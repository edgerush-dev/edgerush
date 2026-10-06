//! One QUIC connection, shared between the task that drives it and the tasks of its streams
//! ([16 §4](../../../../../docs/16-http3.md)).
//!
//! quiche is a state machine that does no I/O, so every task that has something to do with
//! the connection borrows it — only inside a poll, never across an await — and says so by
//! stirring the driver, which then hands on what quiche has for the streams and sends what
//! quiche wants sent. A stream's task waits in its [`Slot`] for what only the driver learns:
//! that its body has more, that its client reset it, that it can take more of its answer.

use crate::connections::Held;
use crate::downstream::h3::head::Refused;
use crate::drain::Drain;
use crate::storage::{Charge, Exhausted, Storage};
use http::HeaderMap;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::task::Waker;

/// What each piece quiche holds received data in costs past its bytes: a 56-byte piece and
/// its key in a slot of a tree node, the node's spare room shared out, and an allocation of
/// its own of 32 bytes at the least (measured with quiche 0.30 on x86-64). A client that
/// sends a byte a frame costs this much each.
const PIECE: usize = 128;

/// A connection, as its driver and its streams' tasks share it.
pub(crate) struct Conn {
    state: RefCell<State>,
    /// The driver's task, to wake when something is to be sent or handed on.
    driver: RefCell<Option<Waker>>,
    /// Something has changed since the driver last looked.
    stirred: Cell<bool>,
    /// What quiche holds for the connection, charged to the worker's storage (16 §6).
    charge: RefCell<Option<Charge>>,
    /// What that charge is.
    charged: Cell<usize>,
    /// The pieces its requests' bodies have read out of quiche and handed on, in bytes, until
    /// each is let go of: charged with what quiche holds, as nothing else pays for them once
    /// read (14 §8).
    in_hand: Cell<usize>,
    /// Closed to make room for the rest: charged nothing from then on.
    shed: Cell<bool>,
    /// Requests the connection has had.
    requests: Cell<u64>,
    /// Of those, the ones the client gave up before their answer's head was sent.
    given_up: Cell<u64>,
    /// Requests the server reset for the client's own errors, or refused 431, in the
    /// connection's life: what h2 counts as `max_local_error_reset_streams` (15 §3).
    provoked: Cell<u64>,
    /// What the connection drains with, and its WebSockets too: its worker's drain, or a
    /// reload replacing the client validation it was accepted under (03 §3).
    pub(crate) drain: Rc<Drain>,
    /// What it counts as among its worker's connections, where the worker counts them:
    /// given back when the last thing holding the connection lets go of it, with quiche's
    /// memory for it (03 §9).
    _held: Option<Held>,
}

/// What the connection holds.
pub(crate) struct State {
    pub(crate) quic: quiche::Connection,
    /// Made once the handshake is done.
    pub(crate) h3: Option<quiche::h3::Connection>,
    /// The request streams whose tasks are running, by stream ID.
    pub(crate) streams: HashMap<u64, Slot>,
    /// Streams whose answers went to quiche whole, until quiche lets them go: what a
    /// draining connection waits to see acknowledged before it closes.
    pub(crate) delivering: Vec<u64>,
    /// The connection has ended; nothing more will come on any stream.
    pub(crate) closed: bool,
}

impl State {
    /// Forgets the answers that have arrived, or that the client stopped: quiche lets a
    /// stream go once both its sides are done, its answer acknowledged. True once none is
    /// left.
    pub(crate) fn delivered(&mut self) -> bool {
        let Self {
            quic, delivering, ..
        } = self;
        delivering.retain(|&id| quic.stream_capacity(id).is_ok());
        delivering.is_empty()
    }
}

/// What one request stream's task waits on.
#[derive(Default)]
pub(crate) struct Slot {
    /// The task reading the request's body, while it waits.
    pub(crate) reader: Option<Waker>,
    /// The task sending the answer, while it waits for room.
    pub(crate) writer: Option<Waker>,
    /// The client's trailers, which quiche hands on once the body before them is read.
    pub(crate) trailers: Option<Result<HeaderMap, Refused>>,
    /// Everything the client sent on the stream has been handed on.
    pub(crate) finished: bool,
    /// The client reset its side of the stream, with this code.
    pub(crate) reset: Option<u64>,
    /// The client asked for no more of the answer, with this code: quiche has reset the
    /// server's side of the stream itself.
    pub(crate) stopped: Option<u64>,
}

impl Slot {
    /// Wakes whichever of the stream's tasks is waiting.
    pub(crate) fn wake(&mut self) {
        if let Some(reader) = self.reader.take() {
            reader.wake();
        }
        if let Some(writer) = self.writer.take() {
            writer.wake();
        }
    }
}

impl Conn {
    /// A connection quiche has accepted, which drains with `drain` and counts as `held`
    /// among its worker's connections.
    pub(crate) fn new(quic: quiche::Connection, drain: Rc<Drain>, held: Option<Held>) -> Rc<Self> {
        Rc::new(Self {
            state: RefCell::new(State {
                quic,
                h3: None,
                streams: HashMap::new(),
                delivering: Vec::new(),
                closed: false,
            }),
            driver: RefCell::new(None),
            stirred: Cell::new(true),
            charge: RefCell::new(None),
            charged: Cell::new(0),
            in_hand: Cell::new(0),
            shed: Cell::new(false),
            requests: Cell::new(0),
            given_up: Cell::new(0),
            provoked: Cell::new(0),
            drain,
            _held: held,
        })
    }

    /// Counts a request the connection has had.
    pub(crate) fn asked(&self) {
        self.requests.set(self.requests.get() + 1);
    }

    /// Counts a request the client gave up, reset or stopped, before its answer's head was
    /// sent.
    pub(crate) fn given_up_early(&self) {
        self.given_up.set(self.given_up.get() + 1);
    }

    /// Whether the connection is a rapid reset (CVE-2023-44487): at least `after` requests,
    /// half or more of them given up by the client before their answer's head was sent.
    /// HTTP/2's rule and its numbers, which are Envoy's (15 §3).
    pub(crate) fn resetting(&self, after: u64) -> bool {
        let requests = self.requests.get();
        requests >= after && self.given_up.get().saturating_mul(2) >= requests
    }

    /// Counts `requests` the server reset for the client's own errors, a malformed head,
    /// body or trailers, or refused 431 for a head too large.
    pub(crate) fn provoked(&self, requests: u32) {
        self.provoked
            .set(self.provoked.get().saturating_add(u64::from(requests)));
    }

    /// Whether the client has made the server reset or refuse more than `most` of its
    /// requests: a client that has its requests acted on and then reset gets its streams
    /// back each time, which the rapid-reset rule does not see (CVE-2025-8671). HTTP/2's
    /// rule and its number, which are h2's (15 §3).
    pub(crate) fn provoking(&self, most: u64) -> bool {
        self.provoked.get() > most
    }

    /// Charges `storage` what quiche holds for the connection now: what it received that
    /// has not been read, `PIECE` bytes more for each piece that is held in, and the HTTP/3
    /// layer's frame buffers, a head held whole until it has all come; and the pieces read
    /// out of it that its bodies have handed on.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if the worker cannot pay for more; the charge is then as it was.
    pub(crate) fn charge(&self, storage: &Rc<Storage>) -> Result<(), Exhausted> {
        if self.shed.get() {
            return Ok(());
        }
        let held = self.with(|state| {
            let quiche::Held { bytes, pieces } = state.quic.received_held();
            let frames = state
                .h3
                .as_ref()
                .map_or(0, quiche::h3::Connection::frame_buffers);
            bytes
                .saturating_add(pieces.saturating_mul(PIECE))
                .saturating_add(frames)
                .saturating_add(self.in_hand.get())
        });
        let charged = self.charged.get();
        let mut charge = self.charge.borrow_mut();
        match charge.as_mut() {
            // Nothing asked for: past the limit, as the worker's own answers may take it,
            // even nothing more would be refused.
            _ if held == charged => {}
            Some(charge) if held > charged => charge.grow(held - charged)?,
            Some(charge) => charge.shrink(charged - held),
            None => *charge = Some(storage.reserve(held)?),
        }
        self.charged.set(held);
        Ok(())
    }

    /// A body has handed on `bytes` it read out of quiche: charged from the driver's next
    /// turn until they are let go of.
    pub(crate) fn hand(&self, bytes: usize) {
        self.in_hand.set(self.in_hand.get().saturating_add(bytes));
    }

    /// A body has let go of `bytes` it handed on: no longer charged once the driver, stirred
    /// for it, has had its turn, which a quiet connection would otherwise not have.
    pub(crate) fn let_go(&self, bytes: usize) {
        self.in_hand.set(self.in_hand.get().saturating_sub(bytes));
        self.stir();
    }

    /// What the connection is charged.
    pub(crate) fn charged(&self) -> usize {
        self.charged.get()
    }

    /// Closes the connection so that the worker has room for the rest, and lets go of its
    /// charge at once: what quiche holds for it goes as the connection does, within its
    /// closing period.
    pub(crate) fn shed(&self) {
        self.shed.set(true);
        self.charge.borrow_mut().take();
        self.charged.set(0);
        self.with(|state| {
            // Fails only for a connection already closing.
            let _closing = state
                .quic
                .close(true, crate::downstream::h3::code::EXCESSIVE_LOAD, b"");
        });
        self.stir();
    }

    /// Does `work` with the connection. Never called across an await, and never from
    /// inside another call of its own.
    pub(crate) fn with<T>(&self, work: impl FnOnce(&mut State) -> T) -> T {
        work(&mut self.state.borrow_mut())
    }

    /// Tells the driver that something has changed: a datagram came, a stream read or wrote.
    pub(crate) fn stir(&self) {
        self.stirred.set(true);
        if let Some(driver) = self.driver.borrow().as_ref() {
            driver.wake_by_ref();
        }
    }

    /// Whether the connection was stirred since this was last asked, and forgets it.
    pub(crate) fn take_stirred(&self) -> bool {
        self.stirred.replace(false)
    }

    /// Whether the connection was stirred since that was last asked, remembering it.
    pub(crate) fn is_stirred(&self) -> bool {
        self.stirred.get()
    }

    /// The driver's task is the one polling with `waker`.
    pub(crate) fn drive_with(&self, waker: &Waker) {
        let mut driver = self.driver.borrow_mut();
        if !driver.as_ref().is_some_and(|kept| kept.will_wake(waker)) {
            *driver = Some(waker.clone());
        }
    }
}

/// A request stream's place in its connection, for as long as the stream's task runs.
/// Dropping it takes the slot away and stops reading whatever the client still sends: an
/// answer may be complete before its request is (RFC 9114 §4.1), and a task that ends
/// early, however it ends, leaves nothing to pile up in quiche. A stream let go of without
/// its answer whole has the answer's side reset too, so that the client does not wait for
/// the rest and the stream ends both ways (§4.1.1): only then does quiche give the client
/// its credit back.
pub(crate) struct Stream {
    pub(crate) conn: Rc<Conn>,
    pub(crate) id: u64,
    /// The answer went to quiche whole, its end included.
    answered: bool,
}

impl Stream {
    /// The place of stream `id` of `conn`, whose slot the driver has made: made while it
    /// held the connection, so that nothing quiche says of the stream meanwhile is lost.
    pub(crate) fn adopt(conn: &Rc<Conn>, id: u64) -> Self {
        Self {
            conn: Rc::clone(conn),
            id,
            answered: false,
        }
    }

    /// Lets the stream go with its answer whole, handed to quiche to its end: quiche sends
    /// it, and sends it again, until the client has it.
    pub(crate) fn answered(mut self) {
        self.answered = true;
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        use crate::downstream::h3::code;
        let code = if self.answered {
            code::NO_ERROR
        } else {
            code::REQUEST_CANCELLED
        };
        self.conn.with(|state| {
            state.streams.remove(&self.id);
            if self.answered {
                state.delivering.push(self.id);
            }
            // One the client stopped, quiche has reset already, with the client's code.
            if !self.answered
                && !matches!(
                    state.quic.stream_capacity(self.id),
                    Err(quiche::Error::StreamStopped(_))
                )
            {
                // Fails only for a side already done: reset by the answer's own failure.
                let _reset = state
                    .quic
                    .stream_shutdown(self.id, quiche::Shutdown::Write, code);
            }
            // Fails only for a side already done, which has nothing more to stop.
            let _stopped = state
                .quic
                .stream_shutdown(self.id, quiche::Shutdown::Read, code);
        });
        self.conn.stir();
    }
}
