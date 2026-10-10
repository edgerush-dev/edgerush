//! One HTTP/2 connection, served over h2 with EdgeRush's own settings
//! ([15 §3, step 2](../../../../../docs/15-http2-and-grpc.md)).
//!
//! The connection is driven by its accept loop, and the streams it accepts run in its queue,
//! polled by the connection's own task so many a turn ([`queue`](super::queue)): the request
//! goes to the request core as any request does, and its answer goes out through
//! [`send_body`], within the room the client grants. A client that resets a stream before it
//! is answered takes the stream's exchange with it: the future answering it is dropped where
//! it stands, and with it whatever it held upstream.
//!
//! A connection with no stream open for its keep-alive time is told to go, with a graceful
//! GOAWAY, and closed once it has finished or its closing time is up
//! ([14 §8](../../../../../docs/14-downstream-server.md)). Its first request is the serving
//! connection's to time, as for any protocol.

use crate::downstream::h1::connection::{Answered, expects_continue};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h2::queue::Queue;
use crate::downstream::h2::writer::{Outgoing, Responder, send_body};
use crate::drain::Drain;
use crate::forwarding::Cut;
use crate::grpc::call::Call;
use crate::h2_stream::H2Stream;
use crate::interim::Interim;
use crate::metrics::Answer;
use crate::received::Received;
use crate::request_body::RequestBody;
use crate::storage::{Charge, Storage};
use crate::way_back::call_answer;
use bytes::Bytes;
use http::header::{DATE, HeaderValue};
use http::{Method, Request, Response, StatusCode, Version};
use http_body::Body;
use std::cell::{Cell, RefCell};
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

/// What an HTTP/2 connection is served with (15 §3).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    /// Streams a client may have open at once.
    pub(crate) streams: u32,
    /// What a client may send on one stream before it is given more. An upload moves at
    /// most this much a round trip: at 4 MiB it went as fast as TCP took it at 20 and
    /// 100 ms, 2.5 and 3 times what 1 MiB moved, and 16 MiB added nothing (15 §3).
    pub(crate) stream_window: u32,
    /// The same for the whole connection.
    pub(crate) connection_window: u32,
    /// The largest header list accepted, by RFC 9113's measure.
    pub(crate) header_list: u32,
    /// What h2 may hold of one stream's answer before it is written. h2's own default:
    /// at 64 KiB, room is granted in smaller pieces and an 8 MiB answer took 17% more CPU
    /// (15 §3).
    pub(crate) send_buffer: usize,
    /// How long a connection may stay with no stream open before it is told to go.
    pub(crate) keep_alive: Duration,
    /// How long a connection stays with no stream open before it gives back the buffers
    /// h2 reads, writes and decodes header blocks in, some 28 KiB, to make them again when
    /// a stream comes (14 §3). Not at once: a client asking one request at a time would
    /// pay for three allocations a request, and a read more.
    pub(crate) release_after: Duration,
    /// How long a connection told to go has to finish before it is closed regardless.
    pub(crate) closing: Duration,
    /// How long a stream's body, or the room to send its answer, may be waited on with
    /// nothing coming.
    pub(crate) idle: Duration,
    /// How many streams a connection must have had before its share reset early is judged.
    pub(crate) reset_judged_after: u64,
    /// How many streams refused in a row for the connection's stream limit close it, once
    /// the client has acknowledged the limit: a client that goes on past the limit it was
    /// given. Refusals for want of the worker's storage are not counted: a shortage is the
    /// worker's, and closes connections by its own rule (15 §3).
    pub(crate) refused_in_a_row: usize,
    /// How many streams refused for the stream limit before the client acknowledged it
    /// close the connection, in its life: a client may open more than the limit before it
    /// has heard of it, h2's own guessing no limit until then, but no more than this.
    pub(crate) refused_before_ack: usize,
    /// How long after the connection starts the client has to acknowledge its settings,
    /// whatever it does meanwhile.
    pub(crate) ack_within: Duration,
    /// How long a draining connection's streams have to finish before it is closed
    /// regardless (03 §10).
    pub(crate) drain_within: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            streams: 100,
            stream_window: 4 << 20,
            connection_window: 16 << 20,
            header_list: 64 * 1024,
            send_buffer: 400 * 1024,
            keep_alive: Duration::from_secs(30),
            release_after: Duration::from_secs(1),
            closing: Duration::from_secs(10),
            idle: Duration::from_secs(30),
            reset_judged_after: 500,
            refused_in_a_row: 100,
            refused_before_ack: 1000,
            ack_within: Duration::from_secs(10),
            drain_within: Duration::from_secs(25),
        }
    }
}

/// The streams a connection has open, and the driver to wake when the last one ends: the
/// keep-alive clock starts then. Today h2 wakes the driver too, as the stream's handles
/// go, so no test can tell this wake from that one; it is here because h2 does not promise
/// it.
struct Streams {
    /// The connection's drain, which its WebSockets drain with too.
    drain: Rc<Drain>,
    open: Cell<usize>,
    driver: RefCell<Option<Waker>>,
    /// Streams accepted in the connection's life.
    seen: Cell<u64>,
    /// Of those, the ones the client reset before their final head was sent.
    premature: Cell<u64>,
}

impl Streams {
    fn new(drain: Rc<Drain>) -> Self {
        Self {
            drain,
            open: Cell::new(0),
            driver: RefCell::new(None),
            seen: Cell::new(0),
            premature: Cell::new(0),
        }
    }

    /// Whether the connection is a rapid reset (CVE-2023-44487): at least `after` streams
    /// seen, half or more of them reset by the client before they were answered. Envoy's
    /// rule and its numbers; h2 bounds only resets that come before a stream is accepted.
    fn resetting(&self, after: u64) -> bool {
        let seen = self.seen.get();
        seen >= after && self.premature.get().saturating_mul(2) >= seen
    }
}

/// One open stream, counted for as long as its task lives.
struct Open(Rc<Streams>);

impl Open {
    fn new(streams: &Rc<Streams>) -> Self {
        streams.open.set(streams.open.get() + 1);
        Self(Rc::clone(streams))
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        let left = self.0.open.get() - 1;
        self.0.open.set(left);
        if left == 0
            && let Some(driver) = self.0.driver.take()
        {
            driver.wake();
        }
    }
}

impl Settings {
    /// h2's server built with these, and every other bound set explicitly rather than
    /// left to h2's defaults, so that a new version of h2 cannot move them unseen.
    fn builder(&self) -> ::h2::server::Builder {
        let mut builder = ::h2::server::Builder::new();
        builder
            .max_concurrent_streams(self.streams)
            .initial_window_size(self.stream_window)
            .initial_connection_window_size(self.connection_window)
            .max_header_list_size(self.header_list)
            .header_table_size(4096)
            .max_frame_size(16_384)
            .max_send_buffer_size(self.send_buffer)
            .max_concurrent_reset_streams(50)
            .reset_stream_duration(std::time::Duration::from_secs(1))
            .max_pending_accept_reset_streams(20)
            .max_local_error_reset_streams(Some(1024))
            // WebSocket over HTTP/2 (RFC 8441), announced from the first SETTINGS: h2 has
            // no way to take it back, and RFC 8441 §3 forbids it ([19 §3]).
            //
            // [19 §3]: ../../../../../docs/19-websocket.md
            .enable_connect_protocol();
        builder
    }
}

/// Serves an HTTP/2 connection, preface included, until it ends. Each request is handed to
/// `respond`, and `date` dates an answer that has no `Date` of its own. What h2 holds of
/// what the client sent is charged in `received`. Once `drain` starts, the client is told to
/// go and the connection closes when its streams have ended or the drain's time is up
/// (03 §10). `cut` is told why, before the streams under way go, when the connection is
/// closed with them: at the drain's time, for the worker's storage, for its resets, for
/// streams opened past its limit, or for settings it did not acknowledge in time.
/// `refused` is told of each stream answered at once for want of storage, with the status
/// sent and whether it was a gRPC call.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve<S, R, F, B, D>(
    socket: S,
    settings: Settings,
    storage: Rc<Storage>,
    received: &Rc<Received>,
    date: Rc<D>,
    drain: Rc<Drain>,
    respond: Rc<R>,
    cut: impl Fn(Cut),
    refused: impl Fn(StatusCode, bool),
) where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Fn(Request<RequestBody>, Interim) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
{
    let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(socket).await else {
        return;
    };
    drive(
        &mut connection,
        settings,
        storage,
        received,
        date,
        drain,
        respond,
        cut,
        refused,
    )
    .await;
}

/// Drives `connection`, once handshaken, until it ends, as [`serve`] says.
#[allow(clippy::too_many_arguments)]
async fn drive<S, R, F, B, D>(
    connection: &mut ::h2::server::Connection<S, Outgoing>,
    settings: Settings,
    storage: Rc<Storage>,
    received: &Rc<Received>,
    date: Rc<D>,
    drain: Rc<Drain>,
    respond: Rc<R>,
    cut: impl Fn(Cut),
    refused: impl Fn(StatusCode, bool),
) where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Fn(Request<RequestBody>, Interim) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
{
    let streams = Rc::new(Streams::new(drain));
    let drain = &streams.drain;
    // Its place among the worker's HTTP/2 connections, charged what h2 holds for it.
    let account = received.open();
    // One timer for the two things a connection with no stream open waits for (`quiet`).
    let mut idle_since = Instant::now();
    let mut released = false;
    let mut idle = std::pin::pin!(tokio::time::sleep_until(
        quiet(idle_since, released, &settings).1
    ));
    let mut idle_from_now = false;
    let mut drain_heard = std::pin::pin!(drain.notified());
    // One timer for the two bounds a connection may be held to: until its client has
    // acknowledged its settings, the time it has to, set as it starts and renewed by nothing;
    // once it drains, the drain's, which takes over from the first if it is still running.
    let mut out_of_time = std::pin::pin!(tokio::time::sleep(settings.ack_within));
    let mut acked = false;
    let mut draining = false;
    // The connection's streams, run by this task, so many a turn (15 §3, `queue`).
    let mut queue = Queue::new();
    let mut turn = Turn::WHOLE;
    // A stream is charged its head as it is handed over and its task as that is made, on
    // its first turn: its future's size is its type's, known without making one, and a
    // stream the worker cannot pay for is answered then, with no task (15 §3).
    let make = |(request, responder, open, charge)| {
        stream_task(
            request,
            responder,
            open,
            Rc::clone(&respond),
            Rc::clone(&storage),
            Rc::clone(&date),
            settings.idle,
            charge,
        )
    };
    let future = size_of_made(&make);
    let mut made = |(request, send, open, mut charge): Waiting| {
        if charge.grow(future).is_err() {
            refuse(request, send, &storage, &*date, &refused);
            return None;
        }
        #[cfg(test)]
        MADE.set(MADE.get() + 1);
        Some(make((request, Responder::new(send), open, charge)))
    };
    // Accepting is what drives the connection: it is polled for as long as the connection
    // lives, its streams run beside it.
    let closing = loop {
        // A stream's head, handed over by h2 with its charge.
        let mut handed = None;
        let accepted = poll_fn(|cx| {
            // Closed to make room by another connection's shortage: h2 reads nothing more.
            if account.is_shed() {
                return Poll::Ready(Next::Shed);
            }
            if streams.resetting(settings.reset_judged_after) {
                return Poll::Ready(Next::Resetting);
            }
            if !acked && !draining {
                acked = connection.settings_acked();
                if !acked && out_of_time.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Next::Unacknowledged);
                }
            }
            if !draining {
                if drain.poll_on(drain_heard.as_mut(), cx).is_ready() {
                    return Poll::Ready(Next::Draining);
                }
            } else if out_of_time.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Next::OutOfTime);
            }
            if streams.open.get() == 0 {
                if idle_from_now {
                    idle_since = Instant::now();
                    released = false;
                    idle.as_mut()
                        .reset(quiet(idle_since, released, &settings).1);
                    idle_from_now = false;
                }
                while idle.as_mut().poll(cx).is_ready() {
                    match quiet(idle_since, released, &settings).0 {
                        Quiet::Close => return Poll::Ready(Next::Idle),
                        Quiet::Release => {
                            connection.release_buffers();
                            released = true;
                            idle.as_mut()
                                .reset(quiet(idle_since, released, &settings).1);
                        }
                    }
                }
            } else {
                idle_from_now = true;
            }
            *streams.driver.borrow_mut() = Some(cx.waker().clone());
            turn.entries -= queue.run(cx.waker(), turn.entries, &mut made);
            // Its allowance spent, with streams perhaps still in line, the turn is given back.
            if turn.spent() {
                return give_back(cx, &mut turn);
            }
            // What h2 may allocate for what the client sends is reserved before it reads, so
            // that h2 refuses what the worker cannot pay for rather than hold it (15 §3).
            let grant = storage.reserve(storage.room().min(GRANT)).ok();
            let granted = grant.as_ref().map_or(0, Charge::bytes);
            connection.set_grant(granted);
            // h2 decodes whatever the client has sent, however much waits: what is left of
            // the turn's decoding goes with it, refused streams and all, and once that is
            // spent h2 reads nothing more and wakes the task for its next turn.
            connection.set_work(turn.work);
            let accepted = connection.poll_accept(cx);
            turn.work = connection.work_left();
            connection.set_grant(0);
            let wanted = connection.wanted();
            account.drive_with(cx.waker());
            // The grant joins the connection's charge, and a head h2 hands over leaves it for
            // its stream's, never let go of between the two; then the connection is charged
            // what h2 holds — uploads not yet read, its streams' places, heads waiting to be
            // taken, a header block half put together — and what the grant did not pay for
            // goes back. A connection closed to make room for others goes now.
            if let Some(grant) = grant {
                account.absorb(grant);
            }
            if let Poll::Ready(Some(Ok((request, _)))) = &accepted {
                handed = account.split(::h2::server::head_held(request));
            }
            account.settle(connection.received_unreleased() + connection.held());
            // Something refused for want of room the worker did not have — not for want of
            // what a full grant gives a poll, which the next poll gets afresh — makes the
            // worker short by what it wanted.
            if wanted > granted && granted < GRANT && wanted <= GRANT {
                account.reclaim(wanted);
            }
            if account.is_shed() {
                return Poll::Ready(Next::Shed);
            }
            // A client that goes on opening streams past its limit: a hundred in a row once it
            // has acknowledged it, a thousand in all before.
            let refused = connection.refused();
            if refused.in_a_row >= settings.refused_in_a_row
                || refused.before_ack >= settings.refused_before_ack
            {
                return Poll::Ready(Next::Refusing);
            }
            match accepted {
                Poll::Ready(accepted) => {
                    turn.accepts -= 1;
                    Poll::Ready(Next::Accepted(accepted))
                }
                Poll::Pending => {
                    turn = Turn::WHOLE;
                    Poll::Pending
                }
            }
        })
        .await;
        // Closing with streams under way, it says why first: their records end as they go.
        match accepted {
            Next::OutOfTime => cut(Cut::Drained),
            Next::Shed => cut(Cut::Exhausted),
            Next::Resetting => cut(Cut::TooManyResets),
            Next::Refusing => cut(Cut::TooManyRefused),
            Next::Unacknowledged => cut(Cut::SettingsTimeout),
            Next::Accepted(_) | Next::Idle | Next::Draining => {}
        }
        let (request, send) = match accepted {
            Next::Accepted(Some(Ok(stream))) => stream,
            // Closed, or failed: either way there is nothing left to serve.
            Next::Accepted(_) => break Closing::Closed,
            Next::Idle => break Closing::Idle,
            Next::Draining => {
                // Told to go, gracefully: what it has sent is still taken and answered, and
                // the connection closes once the last stream ends — or when time is up.
                connection.graceful_shutdown();
                draining = true;
                out_of_time
                    .as_mut()
                    .reset(Instant::now() + settings.drain_within);
                continue;
            }
            Next::OutOfTime => break Closing::Abrupt(::h2::Reason::NO_ERROR),
            Next::Resetting | Next::Refusing => {
                break Closing::Abrupt(::h2::Reason::ENHANCE_YOUR_CALM);
            }
            Next::Unacknowledged => break Closing::Abrupt(::h2::Reason::SETTINGS_TIMEOUT),
            Next::Shed => break Closing::Shed,
        };
        streams.seen.set(streams.seen.get() + 1);
        // Its head's charge, handed over, and what the stream holds beside it.
        let charge = match handed.take() {
            Some(head) => Ok(head),
            None => storage.reserve(0),
        };
        match charge.and_then(|mut charge| charge.grow(STREAM).map(|()| charge)) {
            Ok(charge) => {
                #[cfg(test)]
                TAKEN.set(TAKEN.get() + 1);
                queue.push((request, send, Open::new(&streams), charge));
            }
            Err(_) => refuse(request, send, &storage, &*date, &refused),
        }
    };
    // Its streams still under way go with it, as a reset stream's do: their exchanges are
    // given up at once, and their records say why the connection went (21 §3).
    drop(queue);
    match closing {
        Closing::Closed => {}
        // Idle for its keep-alive time: told to go, gracefully — a request already on its
        // way is still taken — and given its closing time to finish before the socket goes
        // regardless.
        Closing::Idle => {
            connection.graceful_shutdown();
            let _closing = tokio::time::timeout(settings.closing, closed(connection)).await;
        }
        Closing::Abrupt(reason) => {
            connection.abrupt_shutdown(reason);
            let _closing = tokio::time::timeout(settings.closing, closed(connection)).await;
        }
        // Closed to make room: told why if its socket takes the GOAWAY now, and gone either
        // way, its memory with it, without waiting on a client slow to read.
        Closing::Shed => {
            connection.abrupt_shutdown(::h2::Reason::ENHANCE_YOUR_CALM);
            poll_fn(|cx| {
                connection.set_work(WORK);
                let _closing = connection.poll_closed(cx);
                Poll::Ready(())
            })
            .await;
        }
    }
}

/// Gives the worker's turn back with work left: woken again at once, the connection's task
/// goes to the end of the worker's line, behind every other that is ready, and starts its
/// next turn with its allowances whole.
fn give_back<T>(cx: &mut std::task::Context<'_>, turn: &mut Turn) -> Poll<T> {
    *turn = Turn::WHOLE;
    cx.waker().wake_by_ref();
    Poll::Pending
}

/// Polls a connection told to go until it has closed, a turn's decoding at a time: what its
/// client still sends is read, as it may finish streams, but no more of it in one turn than
/// in any other.
async fn closed<S>(connection: &mut ::h2::server::Connection<S, Outgoing>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _closed = poll_fn(|cx| {
        connection.set_work(WORK);
        connection.poll_closed(cx)
    })
    .await;
}

/// What a connection's task may still do in its turn before it gives the turn back.
#[derive(Debug, Clone, Copy)]
struct Turn {
    /// Streams it may take up.
    accepts: usize,
    /// Ready entries it may examine, including refusals and stale wakes.
    entries: usize,
    /// Decoding h2 may do, in frames, a header block's counting one more for each KiB it
    /// carries.
    work: usize,
}

impl Turn {
    const WHOLE: Self = Self {
        accepts: ACCEPTS,
        entries: ENTRIES,
        work: WORK,
    };

    /// Whether the turn is to be given back. Decoding spent ends it in h2, which then reads
    /// nothing and wakes the task.
    fn spent(&self) -> bool {
        self.accepts == 0 || self.entries == 0
    }
}

/// How many streams a connection's task takes up a turn before it gives the turn back.
const ACCEPTS: usize = 16;

/// How many ready entries a connection examines a turn before it gives the turn back,
/// including requests refused before their future is made and wakes of streams already gone.
/// This bounds the count of operations, not the elapsed work of a turn.
const ENTRIES: usize = 64;

/// How much h2 may decode for a connection a turn, in frames, a frame of a header block
/// counting one more for each whole KiB it carries: every frame, whatever becomes of it — a
/// stream refused, a PING, a reset — so that a client with frames waiting cannot keep a worker
/// decoding them while its other connections wait. Some 64 small requests, a hundred streams
/// refused in two turns, a megabyte of uploads in 16 KiB frames; three 16 KiB header blocks.
const WORK: usize = 64;

/// A stream as h2 hands it over, with its count among the open ones and what it is charged
/// until its task is made.
type Waiting = (
    Request<::h2::RecvStream>,
    ::h2::server::SendResponse<Outgoing>,
    Open,
    Charge,
);

/// How the accept loop ended, and so how the connection is closed.
enum Closing {
    /// It is closed already, or failed.
    Closed,
    /// No stream open for its keep-alive time: told to go, gracefully.
    Idle,
    /// Closed at once, with this reason: out of drain time, too many resets.
    Abrupt(::h2::Reason),
    /// Closed to make room in the worker: at once, waiting on nothing.
    Shed,
}

/// What a connection with no stream open waits for next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quiet {
    /// To give back its buffers (14 §3).
    Release,
    /// To be told to go.
    Close,
}

/// What a connection with no stream open since `since` waits for next, and when: its
/// buffers go at its release time unless `released` already, and it at its keep-alive
/// time. A release time no shorter than the keep-alive never comes.
fn quiet(since: Instant, released: bool, settings: &Settings) -> (Quiet, Instant) {
    if released || settings.release_after >= settings.keep_alive {
        (Quiet::Close, since + settings.keep_alive)
    } else {
        (Quiet::Release, since + settings.release_after)
    }
}

/// What the driver was woken for.
enum Next<T> {
    Accepted(T),
    /// No stream open for the keep-alive time.
    Idle,
    /// Too many of its streams reset before their answer.
    Resetting,
    /// Too many of its streams refused for its stream limit.
    Refusing,
    /// Its settings not acknowledged in time.
    Unacknowledged,
    /// The worker has begun to drain.
    Draining,
    /// Draining, and its streams have not finished within the drain's time.
    OutOfTime,
    /// Closed to make room in the worker's storage: it held the most (15 §3).
    Shed,
}

/// How a stream ended, as far as the driver cares.
#[derive(PartialEq, Eq)]
enum Ended {
    /// Reset by the client before its final head was sent.
    ResetEarly,
    /// Anything else.
    Otherwise,
}

/// Sends what waits in `interim` to be passed on, and a `100` of the continue decision's
/// own if it wants one. A `101` is never sent: HTTP/2 has no upgrade (RFC 9113 §8.6).
fn send_interim(responder: &mut Responder, interim: &Interim) {
    while let Some((status, headers)) = interim.next_forwarded() {
        if status == StatusCode::SWITCHING_PROTOCOLS {
            continue;
        }
        let mut head = Response::new(());
        *head.status_mut() = status;
        *head.headers_mut() = headers;
        // Refused only once the final head has gone, when there is nothing to tell.
        let _sent = responder.interim(head);
    }
    if interim.take_local_continue() {
        let mut head = Response::new(());
        *head.status_mut() = StatusCode::CONTINUE;
        let _sent = responder.interim(head);
    }
}

/// What a stream costs the worker besides its future and its head: its place in the
/// connection's queue and the box its future is made in — some 130 bytes on x86-64 — rounded
/// up, with the allocator's own room.
const TASK: usize = 256;

/// What an answer allocates beside its future and the request's head, and nothing else
/// charges: its interim heads' shared state and, where the listener logs, its record's
/// line (160 bytes made, more as it grows).
const ALONGSIDE: usize = 1024;

/// What a stream is charged as it is taken up, beside the head h2 hands over with it.
const STREAM: usize = TASK + ALONGSIDE;

/// A refusal, charged to the worker's provision for its own answers while it is made: its
/// head goes into h2's queue, a few hundred bytes.
const REFUSAL: usize = 1024;

/// The most h2 may allocate for one connection's peer in a poll, reserved before it and
/// settled after: room for decoding several header blocks and taking up a burst of streams.
/// A tuning value: what bounds h2 is that it never allocates past what was reserved, and
/// the grant is let go of after every poll, so a worker's connections never hold one at
/// once.
const GRANT: usize = 4 << 20;

/// What a future `make` would return takes, without one being made.
fn size_of_made<A, F>(_make: &impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[cfg(test)]
thread_local! {
    /// The streams this thread has made a task for.
    static MADE: Cell<usize> = const { Cell::new(0) };
    /// The streams this thread has taken into a connection's queue.
    static TAKEN: Cell<usize> = const { Cell::new(0) };
}

/// Answers a stream the worker cannot pay for, at once and with no task: `503 exhausted`,
/// or `RESOURCE_EXHAUSTED` to a gRPC call, as the core answers one it has no storage for (14 §8),
/// told to `refused` with the status sent and whether it was a call. A worker short of even
/// its provision refuses the stream with REFUSED_STREAM, which a client may send again
/// (RFC 9113 §8.7).
fn refuse(
    request: Request<::h2::RecvStream>,
    mut send: ::h2::server::SendResponse<Outgoing>,
    storage: &Rc<Storage>,
    date: &impl Fn() -> HttpDate,
    refused: &impl Fn(StatusCode, bool),
) {
    let Ok(_making) = storage.reserve_answer(REFUSAL) else {
        send.send_reset(::h2::Reason::REFUSED_STREAM);
        return;
    };
    let call = Call::of(
        Version::HTTP_2,
        request.method(),
        request.headers(),
        Instant::now,
    )
    .is_some();
    let mut head = Response::new(());
    *head.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    if call {
        call_answer(&mut head, Answer::Exhausted);
    }
    if let Ok(now) = HeaderValue::from_bytes(date().as_bytes()) {
        head.headers_mut().insert(DATE, now);
    }
    let status = head.status();
    // A head h2 refuses is not sent, and the stream is reset when its responder goes.
    if Responder::new(send).final_head(head, true).is_ok() {
        refused(status, call);
    }
}

/// A stream's task: its answer, counted open while it lives and holding `charge`, what it
/// costs the worker, until it ends or is dropped. Unboxed: it is measured before it is
/// made, then boxed as it is spawned, so that what Tokio holds and moves is a pointer — the
/// answer's future is several kilobytes, and a task holds its future inline and moves all
/// of it as the task is made and as it finishes.
#[allow(clippy::too_many_arguments)]
async fn stream_task<R, F, B, D>(
    request: Request<::h2::RecvStream>,
    responder: Responder,
    open: Open,
    respond: Rc<R>,
    storage: Rc<Storage>,
    date: Rc<D>,
    idle: Duration,
    charge: Charge,
) where
    R: Fn(Request<RequestBody>, Interim) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    let _paid = charge;
    let answered = answer(
        request,
        responder,
        &*respond,
        &storage,
        &*date,
        idle,
        &open.0.drain,
    )
    .await;
    if answered == Ended::ResetEarly {
        open.0.premature.set(open.0.premature.get() + 1);
    }
    drop(open);
}

/// Answers one stream; a WebSocket it opens drains with the connection's `drain`.
async fn answer<R, F, B, D>(
    request: Request<::h2::RecvStream>,
    mut responder: Responder,
    respond: &R,
    storage: &Rc<Storage>,
    date: &D,
    idle: Duration,
    drain: &Drain,
) -> Ended
where
    R: Fn(Request<RequestBody>, Interim) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    // Read before any filter touches the head, as for HTTP/1 (14 §5).
    let interim = Interim::listened(
        expects_continue(request.headers()),
        Version::HTTP_2,
        request.body().is_end_stream(),
    );
    // An extended CONNECT's stream is a WebSocket's once it is answered, not a body: it is
    // kept here, to be carried, and the request goes to the core with none (19 §3).
    let connect = request.method() == Method::CONNECT
        && request.extensions().get::<::h2::ext::Protocol>().is_some();
    let (request, kept) = if connect {
        let (parts, received) = request.into_parts();
        (
            Request::from_parts(parts, RequestBody::None),
            Some(received),
        )
    } else {
        let request = request
            .map(|body| RequestBody::H2(IncomingH2::new(body, idle).heard_by(interim.clone())));
        (request, None)
    };
    let mut answering = std::pin::pin!(respond(request, interim.clone()));
    let answered = poll_fn(|cx| {
        if responder.poll_reset(cx).is_ready() {
            return Poll::Ready(None);
        }
        let answered = answering.as_mut().poll(cx);
        // What the exchange heard, or the continue decision made, in this turn goes out now.
        send_interim(&mut responder, &interim);
        answered.map(Some)
    })
    .await;
    // Reset by the client: nothing is to be sent, and the exchange has gone.
    let Some(answered) = answered else {
        return Ended::ResetEarly;
    };
    // A local `100` not yet sent is not sent now: the answer says what it would have. What
    // the upstream said before its final answer still goes first, in the order it came.
    interim.final_head();
    send_interim(&mut responder, &interim);
    let (mut head, body) = answered.into_response().into_parts();
    if !head.headers.contains_key(DATE)
        && let Ok(now) = HeaderValue::from_bytes(date().as_bytes())
    {
        head.headers.insert(DATE, now);
    }
    // A WebSocket the core switched: the stream stays open after its 200, and is carried.
    let switched = interim.take_switched().zip(kept);
    let end = body.is_end_stream() && switched.is_none();
    // A head h2 refuses is not sent, and the stream is reset when its responder goes.
    let Ok(mut stream) = responder.final_head(Response::from_parts(head, ()), end) else {
        return Ended::Otherwise;
    };
    if let Some((switched, received)) = switched {
        drop(body);
        let mut tunnel = H2Stream::new(stream, received, Rc::clone(storage), None);
        let _carried = switched.carry(&mut tunnel, None, drain).await;
        return Ended::Otherwise;
    }
    if !end {
        // However the sending ends, there is nobody left to tell: a reset stream or a
        // failed body has been reset already.
        let _sent = send_body(&mut stream, body, storage, idle).await;
    }
    Ended::Otherwise
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downstream::h2::testing::{
        LONG, locally, locally_paused, pair, post, serving, wire, within,
    };
    use http_body_util::{BodyExt, Full};

    /// A stream's future is measured from its type before it is made, and held boxed: what
    /// is held and moved is a pointer, not the future, kilobytes, copied whole as it is made
    /// and as it finishes (14 §3).
    #[test]
    fn a_streams_future_is_measured_unmade_and_held_boxed() {
        type Respond =
            fn(Request<RequestBody>, Interim) -> std::future::Ready<Answered<Full<Bytes>>>;
        type Date = fn() -> HttpDate;
        type Asked = (
            Request<::h2::RecvStream>,
            Responder,
            Open,
            Rc<Respond>,
            Rc<Storage>,
            Rc<Date>,
            Charge,
        );
        let future = size_of_made(
            &|(request, responder, open, respond, storage, date, charge): Asked| {
                stream_task(
                    request, responder, open, respond, storage, date, LONG, charge,
                )
            },
        );
        let boxed = size_of_made(
            &|(request, responder, open, respond, storage, date, charge): Asked| {
                Box::pin(stream_task(
                    request, responder, open, respond, storage, date, LONG, charge,
                ))
            },
        );
        assert_eq!(boxed, std::mem::size_of::<usize>());
        assert!(future > STREAM, "a stream's future of {future} bytes");
    }

    /// A stream whose upload nobody reads holds at most its stream window of the connection
    /// window, so another stream on the same connection uploads megabytes past it: 16 MiB
    /// for the connection against 4 MiB a stream (15 §3).
    #[test]
    fn an_upload_nobody_reads_leaves_room_for_another_on_the_connection() {
        locally(async {
            let (near, far) = wire();
            let (client, server) = tokio::join!(
                ::h2::client::handshake(far),
                Settings::default().builder().handshake::<_, Outgoing>(near)
            );
            let (mut send, connection) = client.unwrap();
            tokio::task::spawn_local(connection);
            let mut accepted = serving(server.unwrap());

            // Stalled: a full stream window sent, and never read.
            let (_stalled, mut stuck) = send.send_request(post(), false).unwrap();
            let window = Settings::default().stream_window as usize;
            stuck
                .send_data(Bytes::from(vec![1u8; window]), false)
                .unwrap();
            let (never_read, _) = within(accepted.recv()).await.unwrap();

            // Beside it, four megabytes, read as they come.
            let (_answer, mut moving) = send.send_request(post(), false).unwrap();
            moving
                .send_data(Bytes::from(vec![2u8; 4 << 20]), true)
                .unwrap();
            let (request, _) = within(accepted.recv()).await.unwrap();
            let body = IncomingH2::new(request.into_body(), LONG);
            let uploaded = within(BodyExt::collect(body)).await.unwrap();
            assert_eq!(uploaded.to_bytes().len(), 4 << 20);
            drop(never_read);
        });
    }

    /// What h2 holds of an upload nobody reads is the worker's storage, as what quiche holds
    /// is for HTTP/3 (16 §6): a stream window received and not read is charged to the worker,
    /// so that the ledger, not the pod's memory, is what runs out.
    #[test]
    fn an_upload_nobody_reads_is_charged_to_the_worker() {
        locally(async {
            let settings = Settings::default();
            let (mut send, mut connection) = pair(&settings.builder()).await;
            let gate = Rc::new(tokio::sync::Notify::new());
            let held = Rc::new(Cell::new(None::<usize>));
            let (opened, told) = (Rc::clone(&gate), Rc::clone(&held));
            // The exchange reads nothing of its body, as with an upstream that takes nothing,
            // until it is let; then it takes what h2 has without waiting, which is what h2
            // held at that moment, and goes on reading nothing.
            let respond = Rc::new(move |request: Request<RequestBody>, _: Interim| {
                let (gate, held) = (Rc::clone(&opened), Rc::clone(&told));
                async move {
                    gate.notified().await;
                    let mut body = request.into_body();
                    let mut cx = std::task::Context::from_waker(Waker::noop());
                    let mut taken = 0;
                    while let Poll::Ready(Some(Ok(frame))) =
                        std::pin::Pin::new(&mut body).poll_frame(&mut cx)
                    {
                        taken += frame.data_ref().map_or(0, Bytes::len);
                    }
                    held.set(Some(taken));
                    std::future::pending::<()>().await;
                    Answered::Map(Response::new(Full::new(Bytes::new())))
                }
            });
            let date = Rc::new(|| HttpDate::from_unix(0));
            let (drain, storage) = (
                Rc::new(Drain::default()),
                Storage::new(crate::storage::LIMIT),
            );
            let received = Received::new(Rc::clone(&storage));
            let window = settings.stream_window as usize;
            let asking = async {
                let (_answer, mut body) = send.send_request(post(), false).unwrap();
                body.send_data(Bytes::from(vec![1u8; window]), false)
                    .unwrap();
                for _ in 0..40 {
                    if storage.used() >= window {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let used = storage.used();
                gate.notify_one();
                for _ in 0..40 {
                    if held.get().is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                used
            };
            let driving = drive(
                &mut connection,
                settings,
                Rc::clone(&storage),
                &received,
                date,
                drain,
                respond,
                |_| {},
                |_, _| {},
            );
            let used = tokio::select! {
                () = driving => panic!("the connection ended"),
                used = asking => used,
            };
            assert_eq!(held.get(), Some(window), "what h2 held of the upload");
            assert!(
                used >= window,
                "{used} charged while h2 held a {window}-byte upload"
            );
        });
    }

    /// When what h2 holds can no longer be paid for, the connection holding the most is told
    /// to calm down and closed, letting go of its charge, and the one that asked is served
    /// on and charged (15 §3).
    #[test]
    fn the_connection_holding_the_most_is_closed_when_storage_runs_out() {
        locally(async {
            let settings = Settings::default();
            // Room for one stream window and some, not for a second.
            let storage = Storage::with_provision(5 << 20, 0);
            let received = Received::new(Rc::clone(&storage));
            let date = Rc::new(|| HttpDate::from_unix(0));
            // Nobody reads any upload: an upstream that takes nothing.
            let respond = Rc::new(|request: Request<RequestBody>, _: Interim| async move {
                let _unread = request;
                std::future::pending::<()>().await;
                Answered::Map(Response::new(Full::new(Bytes::new())))
            });
            // The heavy one's client is kept, to hear how its connection ended.
            let (near, far) = wire();
            let (client, server) = tokio::join!(
                ::h2::client::handshake(far),
                settings.builder().handshake::<_, Outgoing>(near)
            );
            let (mut heavy_send, heavy_client) = client.unwrap();
            let heavy_client = tokio::task::spawn_local(heavy_client);
            let heavy = server.unwrap();
            let (mut light_send, light) = pair(&settings.builder()).await;
            let heavy_ended = Rc::new(Cell::new(false));
            // What each connection's driver says of why it closed it with streams open.
            let (heavy_cut, light_cut) = (Rc::new(Cell::new(None)), Rc::new(Cell::new(None)));
            for (mut connection, ended, told) in [
                (heavy, Some(Rc::clone(&heavy_ended)), Rc::clone(&heavy_cut)),
                (light, None, Rc::clone(&light_cut)),
            ] {
                let (storage, received) = (Rc::clone(&storage), Rc::clone(&received));
                let (date, respond) = (Rc::clone(&date), Rc::clone(&respond));
                tokio::task::spawn_local(async move {
                    drive(
                        &mut connection,
                        settings,
                        storage,
                        &received,
                        date,
                        Rc::new(Drain::default()),
                        respond,
                        |why| told.set(Some(why)),
                        |_, _| {},
                    )
                    .await;
                    if let Some(ended) = ended {
                        ended.set(true);
                    }
                });
            }
            let window = settings.stream_window as usize;
            let (heavy_answer, mut heavy_body) = heavy_send.send_request(post(), false).unwrap();
            heavy_body
                .send_data(Bytes::from(vec![1u8; window]), false)
                .unwrap();
            for _ in 0..100 {
                if storage.used() >= window {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(storage.used() >= window, "{} charged", storage.used());

            light_send = within(light_send.ready()).await.unwrap();
            // The light one's stream is made before its upload comes: once the worker is
            // short, nothing new is made until the heaviest has gone.
            let before = storage.used();
            let (_light_answer, mut light_body) = light_send.send_request(post(), false).unwrap();
            until_held(&storage, |used| used > before + STREAM + 4096).await;
            light_body
                .send_data(Bytes::from(vec![2u8; 2 << 20]), false)
                .unwrap();
            assert!(
                within(heavy_answer).await.is_err(),
                "the heaviest was answered"
            );
            let ended = within(heavy_client).await.unwrap().unwrap_err();
            assert_eq!(
                ended.reason(),
                Some(::h2::Reason::ENHANCE_YOUR_CALM),
                "{ended:?}"
            );
            for _ in 0..100 {
                if heavy_ended.get() && storage.used() >= 2 << 20 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                heavy_ended.get(),
                "the heaviest was told to go but not closed"
            );
            // Its streams' records say the gateway cut them, for its storage (21 §3).
            assert_eq!(heavy_cut.get(), Some(Cut::Exhausted));
            assert_eq!(light_cut.get(), None);
            let used = storage.used();
            assert!(
                (2 << 20..window).contains(&used),
                "{used} charged: the light one's upload alone"
            );
            assert!(
                within(light_send.ready()).await.is_ok(),
                "the light one was closed"
            );
        });
    }

    /// An upload that comes with its request, while the connection closed to make room for
    /// it has not yet gone, finds nothing to make its stream with: the closed one stays
    /// charged until it goes, so the request is answered 503 from the provision, its upload
    /// let go of, and its connection carries on. The connection holding the most is closed
    /// all the same.
    #[test]
    fn an_upload_that_comes_before_room_is_made_is_answered_503_and_let_go() {
        locally(async {
            let settings = Settings::default();
            // Room for one stream window and some, not for a second; a provision for the
            // worker's own answers.
            let storage = Storage::new(5 << 20);
            let received = Received::new(Rc::clone(&storage));
            let date = Rc::new(|| HttpDate::from_unix(0));
            let respond = Rc::new(|request: Request<RequestBody>, _: Interim| async move {
                let _unread = request;
                std::future::pending::<()>().await;
                Answered::Map(Response::new(Full::new(Bytes::new())))
            });
            let (mut heavy_send, heavy) = pair(&settings.builder()).await;
            let (mut light_send, light) = pair(&settings.builder()).await;
            let heavy_ended = Rc::new(Cell::new(false));
            for (mut connection, ended) in [(heavy, Some(Rc::clone(&heavy_ended))), (light, None)] {
                let (storage, received) = (Rc::clone(&storage), Rc::clone(&received));
                let (date, respond) = (Rc::clone(&date), Rc::clone(&respond));
                tokio::task::spawn_local(async move {
                    drive(
                        &mut connection,
                        settings,
                        storage,
                        &received,
                        date,
                        Rc::new(Drain::default()),
                        respond,
                        |_| {},
                        |_, _| {},
                    )
                    .await;
                    if let Some(ended) = ended {
                        ended.set(true);
                    }
                });
            }
            let window = settings.stream_window as usize;
            heavy_send = within(heavy_send.ready()).await.unwrap();
            let (_heavy_answer, mut heavy_body) = heavy_send.send_request(post(), false).unwrap();
            heavy_body
                .send_data(Bytes::from(vec![1u8; window]), false)
                .unwrap();
            until_held(&storage, |used| used >= window).await;

            // The request and its upload together: the upload is read before the stream's
            // first turn, and makes the worker short.
            light_send = within(light_send.ready()).await.unwrap();
            let (light_answer, mut light_body) = light_send.send_request(post(), false).unwrap();
            light_body
                .send_data(Bytes::from(vec![2u8; 2 << 20]), false)
                .unwrap();
            let answer = within(light_answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            until_held(&storage, |_| heavy_ended.get()).await;
            let used = settled(&storage).await;
            assert!(used < 1 << 20, "{used} charged once both are let go of");
            assert!(
                within(light_send.ready()).await.is_ok(),
                "the light one was closed"
            );
        });
    }

    /// With no stream open, a connection gives back its buffers at its release time, then is
    /// told to go at its keep-alive time; once they are back, only the second is waited for.
    #[test]
    fn a_quiet_connection_gives_its_buffers_back_then_goes() {
        let settings = Settings {
            release_after: Duration::from_secs(1),
            keep_alive: Duration::from_secs(30),
            ..Settings::default()
        };
        let since = Instant::now();
        assert_eq!(
            quiet(since, false, &settings),
            (Quiet::Release, since + Duration::from_secs(1))
        );
        assert_eq!(
            quiet(since, true, &settings),
            (Quiet::Close, since + Duration::from_secs(30))
        );
    }

    /// A release time no shorter than the keep-alive never comes: the connection goes first.
    #[test]
    fn a_release_after_the_keep_alive_never_comes() {
        let since = Instant::now();
        for release_after in [Duration::from_secs(30), Duration::from_secs(60)] {
            let settings = Settings {
                release_after,
                keep_alive: Duration::from_secs(30),
                ..Settings::default()
            };
            assert_eq!(
                quiet(since, false, &settings),
                (Quiet::Close, since + Duration::from_secs(30))
            );
        }
    }

    /// A connection quiet past its release time has given its buffers back, and not before;
    /// and it serves the next stream as any other: one whose header block refers to what the
    /// first put in the HPACK table, with a body and an answer larger than a read buffer.
    #[test]
    fn a_quiet_connection_gives_its_buffers_back_and_serves_on() {
        const BODY: usize = 40_000;
        locally_paused(async {
            let (near, far) = wire();
            let settings = Settings {
                release_after: Duration::from_millis(100),
                keep_alive: LONG,
                ..Settings::default()
            };
            let (client, server) = tokio::join!(
                ::h2::client::handshake(far),
                settings.builder().handshake::<_, Outgoing>(near)
            );
            let (mut send, client) = client.unwrap();
            tokio::task::spawn_local(client);
            let mut connection = server.unwrap();
            let respond = Rc::new(|request: Request<RequestBody>, _: Interim| async move {
                let uploaded = request.into_body().collect().await.unwrap().to_bytes();
                Answered::Map(Response::new(Full::new(uploaded)))
            });
            let date = Rc::new(|| HttpDate::from_unix(0));
            let (drain, storage) = (
                Rc::new(Drain::default()),
                Storage::new(crate::storage::LIMIT),
            );
            let received = Received::new(Rc::clone(&storage));
            // Joined, not repeated with a space after each: a value may not end with one
            // (RFC 9113 §8.2.1).
            let long = ["a long header, Huffman-coded"; 40].join(" ");
            // Driven while `waited` passes after a stream, then let go of to be looked at.
            for (round, waited) in [(0u8, 50), (1, 500), (2, 500)] {
                let asking = async {
                    let request = http::Request::builder()
                        .method("POST")
                        .uri("http://example.test/up")
                        .header("x-long", long.as_str())
                        .body(())
                        .unwrap();
                    let (answer, mut body) = send.send_request(request, false).unwrap();
                    let upload = Bytes::from(vec![round; BODY]);
                    body.send_data(upload.clone(), true).unwrap();
                    let mut answer = within(answer).await.unwrap().into_body();
                    let mut received = Vec::new();
                    while let Some(data) = within(answer.data()).await {
                        let data = data.unwrap();
                        let _ = answer.flow_control().release_capacity(data.len());
                        received.extend_from_slice(&data);
                    }
                    assert!(received == upload, "round {round}");
                    tokio::time::sleep(Duration::from_millis(waited)).await;
                };
                let driving = drive(
                    &mut connection,
                    settings,
                    Rc::clone(&storage),
                    &received,
                    Rc::clone(&date),
                    Rc::clone(&drain),
                    Rc::clone(&respond),
                    |_| {},
                    |_, _| {},
                );
                tokio::select! {
                    () = driving => panic!("the connection ended"),
                    () = asking => {}
                }
                let held = connection.buffer_capacity();
                if waited < 100 {
                    assert!(
                        held > 16 * 1024,
                        "round {round}: {held} held before its time"
                    );
                } else {
                    // Room for a frame's header, which a poll that read nothing made again.
                    assert!(held <= 16, "round {round}: {held} held after its time");
                }
            }
        });
    }

    /// What the driver told of the streams it refused: the status sent, and whether it was
    /// a gRPC call.
    type Told = Rc<RefCell<Vec<(StatusCode, bool)>>>;

    /// A connection served against `storage` with `respond`, driven in the background, and
    /// h2's client on the other end; what the driver tells of streams it refuses is kept.
    async fn charged<R, F>(
        storage: &Rc<Storage>,
        respond: R,
    ) -> (::h2::client::SendRequest<Bytes>, Told)
    where
        R: Fn(Request<RequestBody>, Interim) -> F + 'static,
        F: Future<Output = Answered<Full<Bytes>>> + 'static,
    {
        charged_with(Settings::default(), storage, respond).await
    }

    /// The same, served with `settings`.
    async fn charged_with<R, F>(
        settings: Settings,
        storage: &Rc<Storage>,
        respond: R,
    ) -> (::h2::client::SendRequest<Bytes>, Told)
    where
        R: Fn(Request<RequestBody>, Interim) -> F + 'static,
        F: Future<Output = Answered<Full<Bytes>>> + 'static,
    {
        let (send, mut connection) = pair(&settings.builder()).await;
        let told: Told = Rc::new(RefCell::new(Vec::new()));
        let telling = Rc::clone(&told);
        let storage = Rc::clone(storage);
        tokio::task::spawn_local(async move {
            let received = Received::new(Rc::clone(&storage));
            drive(
                &mut connection,
                settings,
                storage,
                &received,
                Rc::new(|| HttpDate::from_unix(0)),
                Rc::new(Drain::default()),
                Rc::new(respond),
                |_| {},
                move |status, call| telling.borrow_mut().push((status, call)),
            )
            .await;
        });
        (send, told)
    }

    /// Waits until `storage` holds what `held` asks of it.
    async fn until_held(storage: &Storage, held: impl Fn(usize) -> bool) -> usize {
        within(async {
            loop {
                let used = storage.used();
                if held(used) {
                    return used;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
    }

    /// What the connection holds with no stream open, once one stream has been taken up and
    /// reset: its streams' places and queues' room, which never shrink, and the decoder's
    /// buffer and table. What a stream is charged comes and goes above it.
    async fn held_between(send: &mut ::h2::client::SendRequest<Bytes>, storage: &Storage) -> usize {
        let made = MADE.get();
        *send = within(send.clone().ready()).await.unwrap();
        let (_answer, mut stream) = send.send_request(get(), false).unwrap();
        within(async {
            while MADE.get() == made {
                tokio::task::yield_now().await;
            }
        })
        .await;
        stream.send_reset(::h2::Reason::CANCEL);
        settled(storage).await
    }

    /// What `storage` holds once nothing has changed it for a while.
    async fn settled(storage: &Storage) -> usize {
        let mut last = storage.used();
        let mut still = 0;
        within(async {
            while still < 20 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let now = storage.used();
                still = if now == last { still + 1 } else { 0 };
                last = now;
            }
        })
        .await;
        last
    }

    /// An answer that never comes, from a core that says when it was asked.
    fn never(
        asked: &Rc<Cell<usize>>,
    ) -> impl Fn(Request<RequestBody>, Interim) -> std::future::Pending<Answered<Full<Bytes>>> + use<>
    {
        let asked = Rc::clone(asked);
        move |_, _| {
            asked.set(asked.get() + 1);
            std::future::pending()
        }
    }

    fn get() -> Request<()> {
        Request::get("http://example.com/")
            .version(Version::HTTP_2)
            .body(())
            .unwrap()
    }

    /// A stream's task is charged from before it is made until it ends: its future, the task
    /// around it and its head, given back once the stream is answered (15 §3).
    #[test]
    fn a_streams_task_is_charged_while_it_lives_and_released_when_it_ends() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let gate = Rc::new(tokio::sync::Notify::new());
            let opening = Rc::clone(&gate);
            let (mut send, _told) = charged(&storage, move |_, _| {
                let opening = Rc::clone(&opening);
                async move {
                    opening.notified().await;
                    Answered::Map(Response::new(Full::new(Bytes::new())))
                }
            })
            .await;
            // One stream answered first: what an answered stream leaves the connection holding
            // — its places, and room in the queue answers wait in — stays, as it never shrinks.
            let (first, _) = send.send_request(get(), true).unwrap();
            gate.notify_one();
            assert_eq!(within(first).await.unwrap().status(), StatusCode::OK);
            let base = settled(&storage).await;
            send = within(send.ready()).await.unwrap();
            let (answer, _) = send.send_request(get(), true).unwrap();
            let held = until_held(&storage, |used| used > base).await;
            assert!(
                held > base + STREAM,
                "{} charged for a stream's task",
                held - base
            );
            gate.notify_one();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            until_held(&storage, |used| used == base).await;
        });
    }

    /// A stream the client resets gives its charge back as its task goes.
    #[test]
    fn a_streams_charge_goes_back_when_the_client_resets_it() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let asked = Rc::new(Cell::new(0));
            let (mut send, _told) = charged(&storage, never(&asked)).await;
            let base = held_between(&mut send, &storage).await;
            let (_answer, mut stream) = send.send_request(get(), false).unwrap();
            until_held(&storage, |used| used > base + STREAM).await;
            stream.send_reset(::h2::Reason::CANCEL);
            until_held(&storage, |used| used == base).await;
            assert_eq!(asked.get(), 2);
        });
    }

    /// A stream is charged its head, handed over by h2 as it is taken up, for as long as it
    /// lives: a stream whose head is kilobytes larger is charged kilobytes more.
    #[test]
    fn a_stream_is_charged_the_head_h2_hands_over_with_it() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let asked = Rc::new(Cell::new(0));
            let (mut send, _told) = charged(&storage, never(&asked)).await;
            let large = || {
                let mut request = get();
                request
                    .headers_mut()
                    .insert("x-large", HeaderValue::from_str(&"v".repeat(8000)).unwrap());
                request
            };
            // What a stream holds while it waits on the core, above what its connection holds
            // between streams; a large head first, so that the decoder's buffer has grown to
            // it before anything is measured.
            let mut charged_for = async |request: Request<()>| {
                send = within(send.clone().ready()).await.unwrap();
                let made = asked.get();
                let (_answer, mut stream) = send.send_request(request, false).unwrap();
                until_held(&storage, |_| asked.get() > made).await;
                let held = settled(&storage).await;
                stream.send_reset(::h2::Reason::CANCEL);
                (held, settled(&storage).await)
            };
            let (_, base) = charged_for(large()).await;
            let (small, after) = charged_for(get()).await;
            assert_eq!(after, base);
            let (large, after) = charged_for(large()).await;
            assert_eq!(after, base);
            assert!(
                large >= small + 8000,
                "{} charged for a small head, {} for a large one",
                small - base,
                large - base
            );
        });
    }

    /// A stream whose connection closes under it gives its charge back as its task goes.
    #[test]
    fn a_streams_charge_goes_back_when_its_connection_closes() {
        locally(async {
            let settings = Settings::default();
            let storage = Storage::new(crate::storage::LIMIT);
            let asked = Rc::new(Cell::new(0));
            let (near, far) = wire();
            let (client, server) = tokio::join!(
                ::h2::client::handshake(far),
                settings.builder().handshake::<_, Outgoing>(near)
            );
            let (mut send, client) = client.unwrap();
            let client = tokio::task::spawn_local(client);
            let mut connection = server.unwrap();
            let serving = Rc::clone(&storage);
            let respond = never(&asked);
            tokio::task::spawn_local(async move {
                let received = Received::new(Rc::clone(&serving));
                drive(
                    &mut connection,
                    settings,
                    serving,
                    &received,
                    Rc::new(|| HttpDate::from_unix(0)),
                    Rc::new(Drain::default()),
                    Rc::new(respond),
                    |_| {},
                    |_, _| {},
                )
                .await;
            });
            let (_answer, _stream) = send.send_request(get(), false).unwrap();
            until_held(&storage, |used| used > 0).await;
            // The client goes, and its socket with it.
            client.abort();
            until_held(&storage, |used| used == 0).await;
        });
    }

    /// A stream's task dropped before it ends — its worker stopping — gives its charge back.
    #[test]
    fn a_streams_charge_goes_back_when_its_task_is_dropped_unfinished() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let storage = Storage::new(crate::storage::LIMIT);
        let asked = Rc::new(Cell::new(0));
        let kept = local.block_on(&runtime, async {
            let (mut send, _told) = charged(&storage, never(&asked)).await;
            let (answer, stream) = send.send_request(get(), false).unwrap();
            until_held(&storage, |used| used > 0).await;
            (send, answer, stream)
        });
        assert!(storage.used() > 0);
        drop(local);
        drop(kept);
        assert_eq!(storage.used(), 0, "what the dropped tasks still held");
    }

    /// A stream the worker has no room to decode is refused by h2 itself with
    /// REFUSED_STREAM, which a client may send again (RFC 9113 §8.7): its head is decoded
    /// to keep HPACK's state and none of it kept, no task is made, the core is never asked,
    /// and nothing stays charged.
    #[test]
    fn a_stream_the_worker_cannot_pay_to_decode_is_refused_by_h2() {
        locally(async {
            // Enough for what the connection holds, not for decoding a head of 64 KiB's
            // header lists, some 410 KiB.
            let storage = Storage::new(64 * 1024);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged(&storage, never(&asked)).await;
            let (made, taken) = (MADE.get(), TAKEN.get());
            let (answer, _) = send.send_request(get(), true).unwrap();
            let refused = within(answer).await.unwrap_err();
            assert_eq!(
                refused.reason(),
                Some(::h2::Reason::REFUSED_STREAM),
                "{refused:?}"
            );
            assert_eq!((MADE.get(), TAKEN.get()), (made, taken));
            assert!(told.borrow().is_empty());
            assert_eq!(asked.get(), 0);
            // A second stream on the same connection is refused as well, not the connection.
            send = within(send.ready()).await.unwrap();
            let (again, _) = send.send_request(get(), true).unwrap();
            let refused = within(again).await.unwrap_err();
            assert_eq!(refused.reason(), Some(::h2::Reason::REFUSED_STREAM));
        });
    }

    /// Settings whose header lists are small, so that what h2 reserves to decode one is too.
    fn small_heads() -> Settings {
        Settings {
            header_list: 512,
            ..Settings::default()
        }
    }

    /// What one stream of `request` leaves charged on a fresh connection once its future is
    /// made, and that future's size: room short of the first by less than the second lets a
    /// stream be decoded and taken in, and not made.
    async fn charged_once(request: impl Fn() -> Request<()>) -> (usize, usize) {
        type Respond =
            fn(Request<RequestBody>, Interim) -> std::future::Pending<Answered<Full<Bytes>>>;
        type Date = fn() -> HttpDate;
        type Asked = (
            Request<::h2::RecvStream>,
            Responder,
            Open,
            Rc<Respond>,
            Rc<Storage>,
            Rc<Date>,
            Charge,
        );
        let future = size_of_made(
            &|(request, responder, open, respond, storage, date, charge): Asked| {
                stream_task(
                    request, responder, open, respond, storage, date, LONG, charge,
                )
            },
        );
        let storage = Storage::new(crate::storage::LIMIT);
        let asked = Rc::new(Cell::new(0));
        let (mut send, _told) = charged_with(small_heads(), &storage, never(&asked)).await;
        let made = MADE.get();
        let (_answer, _stream) = send.send_request(request(), true).unwrap();
        within(async {
            while MADE.get() == made {
                tokio::task::yield_now().await;
            }
        })
        .await;
        (storage.used(), future)
    }

    fn grpc_call() -> Request<()> {
        Request::post("http://example.com/helloworld.Greeter/SayHello")
            .version(Version::HTTP_2)
            .header("content-type", "application/grpc")
            .body(())
            .unwrap()
    }

    /// A stream the worker can decode and take in but cannot pay to make is answered `503`
    /// on its first turn by the driver: no task made, the core never asked, its charges given
    /// back, the refusal told.
    #[test]
    fn a_stream_the_worker_cannot_pay_to_make_is_answered_503_on_its_first_turn() {
        locally(async {
            let (full, future) = charged_once(get).await;
            let room = full - future / 2;
            let storage = Storage::new(room);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged_with(small_heads(), &storage, never(&asked)).await;
            let (made, taken) = (MADE.get(), TAKEN.get());
            let (answer, _) = send.send_request(get(), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(answer.headers()[DATE], "Thu, 01 Jan 1970 00:00:00 GMT");
            assert_eq!(TAKEN.get(), taken + 1, "refused before it was taken in");
            assert_eq!(
                MADE.get(),
                made,
                "a task was made for a stream not paid for"
            );
            assert_eq!(asked.get(), 0);
            assert_eq!(*told.borrow(), [(StatusCode::SERVICE_UNAVAILABLE, false)]);
            assert!(storage.used() <= room);
        });
    }

    /// A gRPC call refused for want of storage is told so as a call: `RESOURCE_EXHAUSTED`,
    /// as the core answers one it has no storage for (15 §6).
    #[test]
    fn a_grpc_call_the_worker_cannot_pay_to_make_is_told_resource_exhausted() {
        locally(async {
            let (full, future) = charged_once(grpc_call).await;
            let room = full - future / 2;
            let storage = Storage::new(room);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged_with(small_heads(), &storage, never(&asked)).await;
            let taken = TAKEN.get();
            let (answer, _) = send.send_request(grpc_call(), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(answer.headers()["grpc-status"], "8");
            assert_eq!(TAKEN.get(), taken + 1);
            assert_eq!(*told.borrow(), [(StatusCode::OK, true)]);
            assert_eq!(asked.get(), 0);
        });
    }

    /// A worker without even its provision left refuses a stream it took in and cannot make
    /// with REFUSED_STREAM, the client's to send again (RFC 9113 §8.7).
    #[test]
    fn a_worker_without_its_provision_refuses_the_stream_it_cannot_make() {
        locally(async {
            // Short of the future by all of it but half a refusal: no room for the refusal
            // under the limit, and no provision above it.
            let (full, future) = charged_once(get).await;
            let room = full - future + REFUSAL / 2;
            let storage = Storage::with_provision(room, 0);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged_with(small_heads(), &storage, never(&asked)).await;
            let (made, taken) = (MADE.get(), TAKEN.get());
            let (answer, _) = send.send_request(get(), true).unwrap();
            let refused = within(answer).await.unwrap_err();
            assert_eq!(
                refused.reason(),
                Some(::h2::Reason::REFUSED_STREAM),
                "{refused:?}"
            );
            assert_eq!(TAKEN.get(), taken + 1, "refused by h2, not by the driver");
            assert_eq!(MADE.get(), made);
            assert!(told.borrow().is_empty());
            assert_eq!(asked.get(), 0);
        });
    }

    /// A connection's streams take its turns, not one each: between two turns of another
    /// task of the worker's, a hundred streams that are always ready are polled no more than
    /// one turn's allowance, where a task a stream would poll every one of them (15 §3,
    /// `queue`).
    #[test]
    fn a_connections_busy_streams_take_its_turns_not_one_each() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let polls = Rc::new(Cell::new(0usize));
            let counting = Rc::clone(&polls);
            // Another task, counting the most stream polls between two of its own turns, from
            // before the first stream comes.
            let (most, turns) = (Rc::new(Cell::new(0usize)), Rc::new(Cell::new(0usize)));
            let (largest, probing, seen) = (Rc::clone(&most), Rc::clone(&turns), Rc::clone(&polls));
            let mut last = 0;
            let probe = tokio::task::spawn_local(poll_fn(move |cx| {
                let now = seen.get();
                largest.set(largest.get().max(now - last));
                last = now;
                probing.set(probing.get() + 1);
                cx.waker().wake_by_ref();
                Poll::<()>::Pending
            }));
            let (mut send, _told) = charged(&storage, move |_, _| {
                let counting = Rc::clone(&counting);
                let mut left = 1000;
                // Ready again as soon as it is polled: back in line at once, not at the end
                // of the worker's tick as `yield_now` would put it. A thousand times, then
                // quiet, so that a queue that never stops polling ends the test.
                poll_fn(move |cx| {
                    counting.set(counting.get() + 1);
                    if left > 0 {
                        left -= 1;
                        cx.waker().wake_by_ref();
                    }
                    Poll::<Answered<Full<Bytes>>>::Pending
                })
            })
            .await;
            let mut answers = Vec::new();
            for _ in 0..100 {
                send = within(send.ready()).await.unwrap();
                answers.push(send.send_request(get(), true).unwrap().0);
            }
            // Every stream polled to its quiet end.
            within(async {
                while polls.get() < 100 * 1001 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            probe.abort();
            // Fewer than one each, whatever a turn's allowance is set to.
            assert!(
                most.get() < 100,
                "{} stream polls between two turns of another task",
                most.get()
            );
            drop(answers);
        });
    }

    /// Streams waiting on their upstreams are not put in line behind each other: a hundred
    /// waiting on as many answers all finish once the answers come.
    #[test]
    fn streams_waiting_on_their_upstreams_all_go_on_when_their_answers_come() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let gate = Rc::new(tokio::sync::Notify::new());
            let waiting = Rc::clone(&gate);
            let (mut send, _told) = charged(&storage, move |_, _| {
                let waiting = Rc::clone(&waiting);
                async move {
                    waiting.notified().await;
                    Answered::Map(Response::new(Full::new(Bytes::new())))
                }
            })
            .await;
            let mut answers = Vec::new();
            for _ in 0..100 {
                send = within(send.ready()).await.unwrap();
                answers.push(send.send_request(get(), true).unwrap().0);
            }
            let peak = until_held(&storage, |used| used > 100 * STREAM).await;
            gate.notify_waiters();
            for answer in answers {
                assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            }
            until_held(&storage, |used| used + 100 * STREAM <= peak).await;
        });
    }

    /// Streams ready past a turn's allowance are polled on the next turn, which the
    /// connection's task gives itself: nothing else may wake it, as here, where a hundred
    /// streams let go at once by a gate that takes none of Tokio's budget (whose own waits
    /// wake a task that runs out of it) do nothing more that h2 would hear of.
    #[test]
    fn streams_ready_past_a_turns_allowance_get_the_next_turn() {
        /// Open or not, and who waits for it to open.
        #[derive(Default)]
        struct Gate {
            open: Cell<bool>,
            waiting: RefCell<Vec<Waker>>,
        }
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let gate = Rc::new(Gate::default());
            let woken = Rc::new(Cell::new(0usize));
            let (waiting, counting) = (Rc::clone(&gate), Rc::clone(&woken));
            let (mut send, _told) = charged(&storage, move |_, _| {
                let (waiting, counting) = (Rc::clone(&waiting), Rc::clone(&counting));
                async move {
                    poll_fn(|cx| {
                        if waiting.open.get() {
                            return Poll::Ready(());
                        }
                        waiting.waiting.borrow_mut().push(cx.waker().clone());
                        Poll::Pending
                    })
                    .await;
                    counting.set(counting.get() + 1);
                    std::future::pending::<Answered<Full<Bytes>>>().await
                }
            })
            .await;
            let made = MADE.get();
            let mut answers = Vec::new();
            for _ in 0..100 {
                send = within(send.ready()).await.unwrap();
                answers.push(send.send_request(get(), true).unwrap().0);
            }
            // Every stream made and waiting at the gate before it opens.
            within(async {
                while MADE.get() < made + 100 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            gate.open.set(true);
            for waker in gate.waiting.take() {
                waker.wake();
            }
            // Two of the connection's turns, within a few of the worker's: no timer of h2's
            // (a second, for reset streams) is let stand in for the turn not given back.
            for _ in 0..50 {
                if woken.get() == 100 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                woken.get(),
                100,
                "streams left in line with nothing to wake them"
            );
            drop(answers);
        });
    }

    /// The most bytes a client reads between two turns of another task of the worker's while
    /// the connection, served with `settings`, answers `pings` PINGs that wait whole in its
    /// socket — with a request before every nine if `asking`, which the core never answers —
    /// once `ready` has done whatever the test puts first.
    async fn acked_between_turns(
        settings: Settings,
        pings: usize,
        asking: bool,
        ready: impl AsyncFnOnce(&mut crate::h2_peer::Peer<tokio::io::DuplexStream>),
    ) -> usize {
        use crate::h2_peer::{self, Peer};
        use tokio::io::{AsyncRead, ReadBuf};
        /// A PING's acknowledgement: its frame's header and eight bytes.
        const ACK: usize = 17;
        let storage = Storage::new(crate::storage::LIMIT);
        let (near, far) = wire();
        let asked = Rc::new(Cell::new(0));
        let respond = never(&asked);
        tokio::task::spawn_local(async move {
            if let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(near).await {
                let received = Received::new(Rc::clone(&storage));
                drive(
                    &mut connection,
                    settings,
                    storage,
                    &received,
                    Rc::new(|| HttpDate::from_unix(0)),
                    Rc::new(Drain::default()),
                    Rc::new(respond),
                    |_| {},
                    |_, _| {},
                )
                .await;
            }
        });
        let mut peer = Peer::open_as_client(far, &[]).await;
        peer.send(&h2_peer::settings_ack()).await;
        within(peer.barrier()).await;
        ready(&mut peer).await;
        let mut flood = Vec::new();
        for n in 0..pings {
            if asking && n % 9 == 0 {
                let id = u32::try_from(2 * n / 9 + 1).unwrap();
                flood.extend(h2_peer::headers(id, h2_peer::request("GET", "/"), true).encode());
            }
            flood.extend(h2_peer::ping(u64::try_from(n).unwrap().to_be_bytes()).encode());
        }
        // All of it in the socket before the connection's task is next polled.
        peer.write_raw(&flood).await;
        // Another task of the worker's, reading what came back between two of its turns: a
        // task, as the test's own future is polled only once the worker has run dozens.
        let (mut read, mut most) = (0, 0);
        let mut buffer = vec![0u8; 1 << 20];
        let reading = tokio::task::spawn_local(poll_fn(move |cx| {
            let mut since = 0;
            loop {
                let mut filled = ReadBuf::new(&mut buffer);
                match std::pin::Pin::new(peer.io_mut()).poll_read(cx, &mut filled) {
                    Poll::Ready(Ok(())) if !filled.filled().is_empty() => {
                        since += filled.filled().len();
                    }
                    _ => break,
                }
            }
            read += since;
            most = most.max(since);
            if read >= pings * ACK {
                return Poll::Ready(most);
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        }));
        within(reading).await.unwrap()
    }

    /// A connection's frames take its turns, not the worker's: with thousands of PINGs
    /// waiting in its socket, and requests among them — taken up, then refused past the
    /// stream limit — it answers no more of them than a turn's decoding between two turns of
    /// another task, where h2 left to itself decodes all that waits in one, and a turn that
    /// takes up streams decodes no more than one that does not (15 §3).
    #[test]
    fn frames_waiting_in_a_connections_socket_are_decoded_a_turn_at_a_time() {
        locally(async {
            const PINGS: usize = 5000;
            // Kept open past its refusals, which would otherwise close it at a hundred.
            let settings = Settings {
                refused_in_a_row: usize::MAX,
                ..Settings::default()
            };
            let most = acked_between_turns(settings, PINGS, true, async |_| {}).await;
            assert!(
                most < PINGS * 17 / 10,
                "{most} bytes of acknowledgements between two turns of another task"
            );
        });
    }

    /// A connection told to go is read a turn's decoding at a time too, while it has its
    /// closing time to finish in, and goes on being read until it closes.
    #[test]
    fn frames_sent_to_a_connection_told_to_go_are_decoded_a_turn_at_a_time() {
        use crate::h2_peer::kind;
        locally(async {
            const PINGS: usize = 5000;
            let settings = Settings {
                keep_alive: Duration::from_millis(50),
                ..Settings::default()
            };
            let most = acked_between_turns(settings, PINGS, false, async |peer| {
                within(peer.until(|frame| frame.kind == kind::GOAWAY)).await;
            })
            .await;
            assert!(
                most < PINGS * 17 / 10,
                "{most} bytes of acknowledgements between two turns of another task"
            );
        });
    }

    /// A connection served beside the test's others, with small header lists, that has sent
    /// the first `block` bytes of a request's header block and nothing more: it holds what
    /// the unfinished block holds until it is closed. `ended` counts those whose driver has
    /// returned.
    async fn withholding(
        storage: &Rc<Storage>,
        received: &Rc<Received>,
        ended: &Rc<Cell<usize>>,
        block: usize,
    ) -> crate::h2_peer::Peer<tokio::io::DuplexStream> {
        use crate::h2_peer::{self, Frame, Peer, flag, kind};
        let (near, far) = wire();
        let settings = small_heads();
        let (storage, received, ended) =
            (Rc::clone(storage), Rc::clone(received), Rc::clone(ended));
        let asked = Rc::new(Cell::new(0));
        let respond = never(&asked);
        tokio::task::spawn_local(async move {
            if let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(near).await {
                drive(
                    &mut connection,
                    settings,
                    storage,
                    &received,
                    Rc::new(|| HttpDate::from_unix(0)),
                    Rc::new(Drain::default()),
                    Rc::new(respond),
                    |_| {},
                    |_, _| {},
                )
                .await;
            }
            ended.set(ended.get() + 1);
        });
        let mut peer = Peer::open_as_client(far, &[]).await;
        peer.send(&h2_peer::settings_ack()).await;
        let mut head = h2_peer::request("GET", "/");
        head.extend_from_slice(&h2_peer::block(&[("x-big", &"a".repeat(4 * block))]));
        head.truncate(block);
        peer.send(&Frame::new(kind::HEADERS, flag::END_STREAM, 1, head))
            .await;
        peer
    }

    /// A client of `settings` served beside the others in `received`, its streams answered
    /// by `respond`.
    async fn served_beside<R, F>(
        settings: Settings,
        storage: &Rc<Storage>,
        received: &Rc<Received>,
        respond: R,
    ) -> ::h2::client::SendRequest<Bytes>
    where
        R: Fn(Request<RequestBody>, Interim) -> F + 'static,
        F: Future<Output = Answered<Full<Bytes>>> + 'static,
    {
        let (send, mut connection) = pair(&settings.builder()).await;
        let (storage, received) = (Rc::clone(storage), Rc::clone(received));
        tokio::task::spawn_local(async move {
            drive(
                &mut connection,
                settings,
                storage,
                &received,
                Rc::new(|| HttpDate::from_unix(0)),
                Rc::new(Drain::default()),
                Rc::new(respond),
                |_| {},
                |_, _| {},
            )
            .await;
        });
        send
    }

    /// Connections holding unfinished header blocks are closed to make room for a newcomer
    /// the worker cannot otherwise pay for, one at a time: a burst of the newcomer's
    /// requests, refused while the worker is short, closes no more of them than the room it
    /// needs, the rest go on holding theirs, and the newcomer's requests then go through.
    #[test]
    fn stalled_header_blocks_are_closed_one_at_a_time_to_let_a_newcomer_in() {
        locally(async {
            const LIMIT: usize = 256 * 1024;
            const BLOCK: usize = 12 * 1024;
            const BURST: usize = 20;
            let storage = Storage::new(LIMIT);
            let received = Received::new(Rc::clone(&storage));
            let ended = Rc::new(Cell::new(0));
            let mut peers = Vec::new();
            for _ in 0..12 {
                peers.push(withholding(&storage, &received, &ended, BLOCK).await);
            }
            let held = settled(&storage).await;
            assert_eq!(ended.get(), 0, "{held} held by twelve blocks of {BLOCK}");
            // The newcomer, warmed up while there is room — its buffers, its decoder's and its
            // streams' places grown to what its requests need — so that what it holds stays
            // put, and only its requests ask for more.
            let ok = |_, _| async { Answered::Map(Response::new(Full::new(Bytes::new()))) };
            let mut send = served_beside(small_heads(), &storage, &received, ok).await;
            for _ in 0..3 {
                send = within(send.ready()).await.unwrap();
                let (answer, _) = send.send_request(get(), true).unwrap();
                assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            }
            settled(&storage).await;
            // The rest is held elsewhere: its requests can be paid for only by closing one.
            let _elsewhere = storage.reserve(storage.room()).unwrap();
            let mut answers = Vec::new();
            for _ in 0..BURST {
                send = within(send.ready()).await.unwrap();
                answers.push(send.send_request(get(), true).unwrap().0);
            }
            let mut refused = 0;
            for answer in answers {
                match within(answer).await {
                    Ok(answer) => assert_eq!(answer.status(), StatusCode::OK),
                    Err(error) => {
                        assert_eq!(error.reason(), Some(::h2::Reason::REFUSED_STREAM));
                        refused += 1;
                    }
                }
            }
            let closed = ended.get();
            assert!(refused > 0, "nothing refused: the worker was never short");
            assert!(
                (1..=2).contains(&closed),
                "{closed} closed for {refused} refused of {BURST}"
            );
            // Sent again, a request gets through.
            let status = within(async {
                loop {
                    send = send.clone().ready().await.unwrap();
                    match send.send_request(get(), true).unwrap().0.await {
                        Ok(answer) => break answer.status(),
                        Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                    }
                }
            })
            .await;
            assert_eq!(status, StatusCode::OK);
            assert!(ended.get() < peers.len(), "every block was closed");
        });
    }

    /// A stream refused for its connection's own stream limit closes nobody, however short
    /// the worker: room made elsewhere would not have let it in.
    #[test]
    fn streams_past_a_connections_own_limit_close_no_other_connection() {
        locally(async {
            const LIMIT: usize = 256 * 1024;
            let storage = Storage::new(LIMIT);
            let received = Received::new(Rc::clone(&storage));
            let ended = Rc::new(Cell::new(0));
            let mut peers = Vec::new();
            for _ in 0..3 {
                peers.push(withholding(&storage, &received, &ended, 12 * 1024).await);
            }
            settled(&storage).await;
            // Short of a whole grant, with room enough for one stream and its future.
            let _elsewhere = storage.reserve(storage.room() - 64 * 1024).unwrap();
            // A client allowed one stream that opens six: the server refuses five for its
            // limit, whatever the client was told.
            use crate::h2_peer::{self, Peer, code};
            let asked = Rc::new(Cell::new(0));
            let settings = Settings {
                streams: 1,
                ..small_heads()
            };
            let (near, far) = wire();
            let (serving, sharing) = (Rc::clone(&storage), Rc::clone(&received));
            let respond = never(&asked);
            tokio::task::spawn_local(async move {
                if let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(near).await
                {
                    drive(
                        &mut connection,
                        settings,
                        serving,
                        &sharing,
                        Rc::new(|| HttpDate::from_unix(0)),
                        Rc::new(Drain::default()),
                        Rc::new(respond),
                        |_| {},
                        |_, _| {},
                    )
                    .await;
                }
            });
            let mut peer = Peer::open_as_client(far, &[]).await;
            peer.send(&h2_peer::settings_ack()).await;
            peer.send(&h2_peer::headers(1, h2_peer::request("GET", "/"), false))
                .await;
            until_held(&storage, |_| asked.get() == 1).await;
            for id in [3, 5, 7, 9, 11] {
                peer.send(&h2_peer::headers(id, h2_peer::request("GET", "/"), true))
                    .await;
            }
            let frames = within(peer.barrier()).await;
            let refused = frames
                .iter()
                .filter(|frame| frame.kind == h2_peer::kind::RST_STREAM)
                .filter(|frame| frame.reset() == code::REFUSED_STREAM)
                .count();
            assert_eq!(refused, 5);
            assert_eq!(
                ended.get(),
                0,
                "a connection was closed for another's stream limit"
            );
        });
    }

    /// A connection allowed one stream, served with defaults otherwise, whose client has
    /// opened it and acknowledged the settings if `ack`; what the driver says of why it closed
    /// is kept.
    async fn limited(
        ack: bool,
    ) -> (
        crate::h2_peer::Peer<tokio::io::DuplexStream>,
        Rc<RefCell<Vec<Cut>>>,
    ) {
        use crate::h2_peer::{self, Peer};
        let storage = Storage::new(crate::storage::LIMIT);
        let asked = Rc::new(Cell::new(0));
        let settings = Settings {
            streams: 1,
            ..Settings::default()
        };
        let (near, far) = wire();
        let told = Rc::new(RefCell::new(Vec::new()));
        let (respond, telling) = (never(&asked), Rc::clone(&told));
        let serving = Rc::clone(&storage);
        tokio::task::spawn_local(async move {
            if let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(near).await {
                let received = Received::new(Rc::clone(&serving));
                drive(
                    &mut connection,
                    settings,
                    serving,
                    &received,
                    Rc::new(|| HttpDate::from_unix(0)),
                    Rc::new(Drain::default()),
                    Rc::new(respond),
                    move |why| telling.borrow_mut().push(why),
                    |_, _| {},
                )
                .await;
            }
        });
        let mut peer = Peer::open_as_client(far, &[]).await;
        if ack {
            peer.send(&h2_peer::settings_ack()).await;
        }
        peer.send(&h2_peer::headers(1, h2_peer::request("GET", "/"), true))
            .await;
        while asked.get() == 0 {
            tokio::task::yield_now().await;
        }
        (peer, told)
    }

    /// Sends `count` requests past a limit of one, from stream `first` on, and says how
    /// many were refused and whether the connection was told to go.
    async fn past_the_limit(
        peer: &mut crate::h2_peer::Peer<tokio::io::DuplexStream>,
        first: u32,
        count: u32,
    ) -> (usize, bool) {
        use crate::h2_peer::{self, code, kind};
        for n in 0..count {
            let id = first + 2 * n;
            peer.send(&h2_peer::headers(id, h2_peer::request("GET", "/"), true))
                .await;
        }
        let frames = within(peer.barrier()).await;
        let refused = frames
            .iter()
            .filter(|frame| frame.kind == kind::RST_STREAM)
            .filter(|frame| frame.reset() == code::REFUSED_STREAM)
            .count();
        let told = frames.iter().any(|frame| frame.kind == kind::GOAWAY);
        (refused, told)
    }

    /// The GOAWAY's code, once it comes.
    async fn goaway(peer: &mut crate::h2_peer::Peer<tokio::io::DuplexStream>) -> u32 {
        use crate::h2_peer::kind;
        let (goaway, _) = within(peer.until(|frame| frame.kind == kind::GOAWAY)).await;
        goaway.goaway().1
    }

    /// A client that goes on opening streams past the limit it was given is closed once a
    /// hundred in a row have been refused after it acknowledged the limit, ENHANCE_YOUR_CALM,
    /// and told why; ninety-nine leave it open (15 §3).
    #[test]
    fn a_client_refused_a_hundred_streams_in_a_row_for_its_limit_is_closed() {
        use crate::h2_peer::{self, code};
        locally(async {
            let (mut peer, told) = limited(true).await;
            assert_eq!(past_the_limit(&mut peer, 3, 99).await, (99, false));
            assert!(told.borrow().is_empty());
            peer.send(&h2_peer::headers(201, h2_peer::request("GET", "/"), true))
                .await;
            assert_eq!(goaway(&mut peer).await, code::ENHANCE_YOUR_CALM);
            assert_eq!(*told.borrow(), [Cut::TooManyRefused]);
        });
    }

    /// A client may open more streams than the limit before it has heard of it: a burst of
    /// hundreds refused before it acknowledges the limit leaves it open, and counts nothing
    /// towards the hundred in a row it may be refused after.
    #[test]
    fn a_burst_past_the_limit_before_it_is_acknowledged_leaves_the_client_open() {
        use crate::h2_peer::{self, code};
        locally(async {
            let (mut peer, told) = limited(false).await;
            assert_eq!(past_the_limit(&mut peer, 3, 299).await, (299, false));
            peer.send(&h2_peer::settings_ack()).await;
            assert_eq!(past_the_limit(&mut peer, 601, 99).await, (99, false));
            assert!(told.borrow().is_empty());
            peer.send(&h2_peer::headers(799, h2_peer::request("GET", "/"), true))
                .await;
            assert_eq!(goaway(&mut peer).await, code::ENHANCE_YOUR_CALM);
            assert_eq!(*told.borrow(), [Cut::TooManyRefused]);
        });
    }

    /// A client that never acknowledges the limit is refused no more than a thousand
    /// streams past it before it is closed.
    #[test]
    fn a_client_that_never_acknowledges_is_closed_after_a_thousand_refused() {
        use crate::h2_peer::{self, code};
        locally(async {
            let (mut peer, told) = limited(false).await;
            assert_eq!(past_the_limit(&mut peer, 3, 999).await, (999, false));
            assert!(told.borrow().is_empty());
            peer.send(&h2_peer::headers(2001, h2_peer::request("GET", "/"), true))
                .await;
            assert_eq!(goaway(&mut peer).await, code::ENHANCE_YOUR_CALM);
            assert_eq!(*told.borrow(), [Cut::TooManyRefused]);
        });
    }

    /// A client that never acknowledges its settings is closed with SETTINGS_TIMEOUT when its
    /// time is up, ten seconds from the start, however busy it keeps the connection
    /// meanwhile; one that acknowledged them is not.
    #[test]
    fn a_client_that_never_acknowledges_is_closed_when_its_time_is_up() {
        use crate::h2_peer::{self, code, kind};
        locally_paused(async {
            let started = Instant::now();
            let (mut silent, told) = limited(false).await;
            let (mut acking, acked_told) = limited(true).await;
            let mut goaway = None;
            // A PING a second, which renews nothing; thirty at most.
            for n in 1..=30u8 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                silent.send_if_open(&h2_peer::ping([n; 8])).await;
                let (frame, _) = silent
                    .until(|frame| frame.kind == kind::GOAWAY || frame.kind == kind::PING)
                    .await;
                if frame.kind == kind::GOAWAY {
                    goaway = Some(frame.goaway().1);
                    break;
                }
            }
            let after = started.elapsed();
            assert_eq!(goaway, Some(code::SETTINGS_TIMEOUT), "after {after:?}");
            assert!(
                (Duration::from_secs(10)..Duration::from_secs(12)).contains(&after),
                "closed after {after:?}"
            );
            assert_eq!(*told.borrow(), [Cut::SettingsTimeout]);
            let frames = acking.barrier().await;
            assert!(frames.iter().all(|frame| frame.kind != kind::GOAWAY));
            assert!(acked_told.borrow().is_empty());
        });
    }

    /// A connection closed to make room goes at once, its memory with it, though its client
    /// reads nothing and the GOAWAY cannot be written: a slow reader does not hold the
    /// worker's storage for the closing time.
    #[test]
    fn a_connection_closed_to_make_room_goes_at_once_though_its_client_reads_nothing() {
        use crate::h2_peer::{self, Frame, Peer, flag, kind};
        locally(async {
            let storage = Storage::new(256 * 1024);
            let received = Received::new(Rc::clone(&storage));
            let ended = Rc::new(Cell::new(false));
            // Little room each way: what the server writes backs up after a few frames.
            let (near, far) = tokio::io::duplex(64);
            let settings = small_heads();
            let (serving, sharing, ending) =
                (Rc::clone(&storage), Rc::clone(&received), Rc::clone(&ended));
            let asked = Rc::new(Cell::new(0));
            let respond = never(&asked);
            tokio::task::spawn_local(async move {
                if let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(near).await
                {
                    drive(
                        &mut connection,
                        settings,
                        serving,
                        &sharing,
                        Rc::new(|| HttpDate::from_unix(0)),
                        Rc::new(Drain::default()),
                        Rc::new(respond),
                        |_| {},
                        |_, _| {},
                    )
                    .await;
                }
                ending.set(true);
            });
            let mut peer = Peer::open_as_client(far, &[]).await;
            peer.send(&h2_peer::settings_ack()).await;
            let mut head = h2_peer::request("GET", "/");
            head.extend_from_slice(&h2_peer::block(&[("x-big", &"a".repeat(8000))]));
            head.truncate(4000);
            within(peer.send(&Frame::new(kind::HEADERS, flag::END_STREAM, 1, head))).await;
            // PINGs whose acknowledgements nobody reads, until the server can write no more:
            // past that, what is sent waits in the wire, and the test stops sending.
            for n in 0..6u8 {
                let ping = h2_peer::ping([n; 8]);
                if tokio::time::timeout(Duration::from_millis(50), peer.send(&ping))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let held = settled(&storage).await;
            assert!(held >= 4000, "{held} held by an unfinished block");
            assert!(!ended.get());
            // Another connection's shortage closes it, the one charged the most.
            let asking = received.open();
            asking.reclaim(1);
            let started = Instant::now();
            until_held(&storage, |_| ended.get()).await;
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "went after {:?}, its closing time is {:?}",
                started.elapsed(),
                Settings::default().closing
            );
            assert_eq!(storage.used(), 0, "its charge outlived it");
            drop(peer);
        });
    }

    /// Connections that each send a large header block and never finish it hold what the
    /// blocks hold, charged to the worker's storage across polls, and never more than the
    /// storage allows: once it is full, a block that cannot be charged closes its connection
    /// with ENHANCE_YOUR_CALM (15 §3).
    #[test]
    fn unfinished_header_blocks_from_many_connections_stay_within_the_workers_storage() {
        use crate::h2_peer::{self, Frame, Peer, code, flag, kind};
        locally(async {
            const LIMIT: usize = 256 * 1024;
            const BLOCK: usize = 12 * 1024;
            const WITHHOLDING: usize = 40;
            let storage = Storage::new(LIMIT);
            let received = Received::new(Rc::clone(&storage));
            let asked = Rc::new(Cell::new(0));
            let mut peers = Vec::new();
            for _ in 0..WITHHOLDING {
                let (near, far) = wire();
                let settings = small_heads();
                let (storage, received) = (Rc::clone(&storage), Rc::clone(&received));
                let respond = never(&asked);
                tokio::task::spawn_local(async move {
                    let Ok(mut connection) =
                        settings.builder().handshake::<_, Outgoing>(near).await
                    else {
                        return;
                    };
                    drive(
                        &mut connection,
                        settings,
                        storage,
                        &received,
                        Rc::new(|| HttpDate::from_unix(0)),
                        Rc::new(Drain::default()),
                        Rc::new(respond),
                        |_| {},
                        |_, _| {},
                    )
                    .await;
                });
                let mut peer = Peer::open_as_client(far, &[]).await;
                peer.send(&h2_peer::settings_ack()).await;
                // A request's head, then a literal value longer than the block's first frame:
                // the decoder can do nothing with what is left until the rest comes.
                let mut block = h2_peer::request("GET", "/");
                block.extend_from_slice(&h2_peer::block(&[("x-big", &"a".repeat(4 * BLOCK))]));
                block.truncate(BLOCK);
                peer.send(&Frame::new(kind::HEADERS, flag::END_STREAM, 1, block))
                    .await;
                peers.push(peer);
            }
            let held = settled(&storage).await;
            // What each connection said, if anything: a GOAWAY, for those it could not hold.
            let mut closed = 0;
            for peer in &mut peers {
                let said = tokio::time::timeout(Duration::from_millis(50), async {
                    loop {
                        match peer.try_next().await {
                            Some(frame) if frame.kind == kind::GOAWAY => {
                                return Some(frame.goaway());
                            }
                            Some(_) => {}
                            None => return None,
                        }
                    }
                })
                .await
                .ok()
                .flatten();
                if let Some((_, said)) = said {
                    assert_eq!(said, code::ENHANCE_YOUR_CALM);
                    closed += 1;
                }
            }
            assert!(held <= LIMIT, "{held} held of {LIMIT}");
            assert!(closed > 0, "every connection held its block: {held} held");
            let open = WITHHOLDING - closed;
            assert!(
                held >= open * BLOCK / 2,
                "{open} connections hold their blocks and {held} is charged"
            );
            assert_eq!(asked.get(), 0, "an unfinished head reached the core");
        });
    }
}
