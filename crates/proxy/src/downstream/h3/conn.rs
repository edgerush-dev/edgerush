//! One QUIC connection, shared between the task that drives it and the tasks of its streams
//! ([16 §4](../../../../../docs/16-http3.md)).
//!
//! quiche is a state machine that does no I/O, so every task that has something to do with
//! the connection borrows it — only inside a poll, never across an await — and says so by
//! stirring the driver, which then hands on what quiche has for the streams and sends what
//! quiche wants sent. A stream's task waits in its [`Slot`] for what only the driver learns:
//! that its body has more, that its client reset it, that it can take more of its answer.

use crate::downstream::h3::head::Refused;
use http::HeaderMap;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::task::Waker;

/// A connection, as its driver and its streams' tasks share it.
pub(crate) struct Conn {
    state: RefCell<State>,
    /// The driver's task, to wake when something is to be sent or handed on.
    driver: RefCell<Option<Waker>>,
    /// Something has changed since the driver last looked.
    stirred: Cell<bool>,
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
    /// A connection quiche has accepted.
    pub(crate) fn new(quic: quiche::Connection) -> Rc<Self> {
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
        })
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
