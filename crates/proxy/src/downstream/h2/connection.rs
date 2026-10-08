//! One HTTP/2 connection, served over h2 with EdgeRush's own settings
//! ([15 §3, step 2](../../../../../docs/15-http2-and-grpc.md)).
//!
//! The connection is driven by its accept loop, and every stream it accepts is a task of its
//! own on the worker: the request goes to the request core as any request does, and its
//! answer goes out through [`send_body`], within the room the client grants. A client that
//! resets a stream before it is answered takes the stream's exchange with it: the future
//! answering it is dropped where it stands, and with it whatever it held upstream.
//!
//! A connection with no stream open for its keep-alive time is told to go, with a graceful
//! GOAWAY, and closed once it has finished or its closing time is up
//! ([14 §8](../../../../../docs/14-downstream-server.md)). Its first request is the serving
//! connection's to time, as for any protocol.

use crate::downstream::h1::connection::{Answered, expects_continue};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h2::body::IncomingH2;
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
/// closed with them: at the drain's time, for the worker's storage, or for its resets.
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
    let mut out_of_time = std::pin::pin!(tokio::time::sleep(settings.drain_within));
    let mut draining = false;
    // Accepting is what drives the connection: it is polled for as long as the connection
    // lives, streams running beside it.
    loop {
        let accepted = poll_fn(|cx| {
            if streams.resetting(settings.reset_judged_after) {
                return Poll::Ready(Next::Resetting);
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
            let accepted = connection.poll_accept(cx);
            // What h2 holds of what the client sent — uploads not yet read — is the worker's
            // storage as well (15 §3); a connection closed to make room for others goes now.
            account.drive_with(cx.waker());
            account.settle(connection.received_unreleased());
            if account.is_shed() {
                return Poll::Ready(Next::Shed);
            }
            accepted.map(Next::Accepted)
        })
        .await;
        // Closing with streams under way, it says why first: their records end as they go.
        match accepted {
            Next::OutOfTime => cut(Cut::Drained),
            Next::Shed => cut(Cut::Exhausted),
            Next::Resetting => cut(Cut::TooManyResets),
            Next::Accepted(_) | Next::Idle | Next::Draining => {}
        }
        let (request, send) = match accepted {
            Next::Accepted(Some(Ok(stream))) => stream,
            // Closed, or failed: either way there is nothing left to serve.
            Next::Accepted(_) => return,
            Next::Idle => break,
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
            Next::OutOfTime => {
                connection.abrupt_shutdown(::h2::Reason::NO_ERROR);
                let _closing = tokio::time::timeout(
                    settings.closing,
                    poll_fn(|cx| connection.poll_closed(cx)),
                )
                .await;
                return;
            }
            Next::Resetting | Next::Shed => {
                connection.abrupt_shutdown(::h2::Reason::ENHANCE_YOUR_CALM);
                let _closing = tokio::time::timeout(
                    settings.closing,
                    poll_fn(|cx| connection.poll_closed(cx)),
                )
                .await;
                return;
            }
        };
        streams.seen.set(streams.seen.get() + 1);
        // The task is paid for before it is made: its future's size is its type's, known
        // without making one, and a stream the worker cannot pay for is answered here,
        // with no task (15 §3).
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
        let cost = stream_cost(size_of_made(&make), &request);
        match storage.reserve(cost) {
            Ok(charge) => {
                #[cfg(test)]
                MADE.set(MADE.get() + 1);
                let task = make((request, Responder::new(send), Open::new(&streams), charge));
                let _detached = tokio::task::spawn_local(Box::pin(task));
            }
            Err(_) => refuse(request, send, &storage, &*date, &refused),
        }
    }
    // Idle for its keep-alive time: told to go, gracefully — a request already on its way is
    // still taken — and given its closing time to finish before the socket goes regardless.
    connection.graceful_shutdown();
    let _closing =
        tokio::time::timeout(settings.closing, poll_fn(|cx| connection.poll_closed(cx))).await;
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

/// What a task costs the worker besides its future: Tokio's cell around the boxed future —
/// its header, scheduler handle, stage and trailer, some 130 bytes on x86-64 — rounded up,
/// and the allocator's own room for the box.
const TASK: usize = 256;

/// What an answer allocates beside its future and the request's head, and nothing else
/// charges: its interim heads' shared state and, where the listener logs, its record's
/// line (160 bytes made, more as it grows).
const ALONGSIDE: usize = 1024;

/// The allocator's own room for one allocation, and what it rounds a small one up by.
const ALLOCATION: usize = 16;

/// What a field takes in a header map besides its bytes: its entry (name, value, hash and
/// links) and its place in the map's index.
const FIELD: usize =
    std::mem::size_of::<http::HeaderName>() + std::mem::size_of::<HeaderValue>() + 16;

/// A refusal, charged to the worker's provision for its own answers while it is made: its
/// head goes into h2's queue, a few hundred bytes.
const REFUSAL: usize = 1024;

/// What a stream's task costs the worker while it lives: its future of `future` bytes, as
/// large as the answer's largest state, the exchange's included (measured at 8.8 KB a
/// stream; 15 §3), the task around it, and the request's head, which h2 hands over when it
/// accepts the stream and which is the task's from then on. A map's capacity is charged, not
/// just what is in it, and every field value is an allocation of its own.
fn stream_cost<T>(future: usize, request: &Request<T>) -> usize {
    let headers = request.headers();
    let fields: usize = headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + 2 * ALLOCATION)
        .sum();
    let uri = request.uri();
    let target = uri.path_and_query().map_or(0, |path| path.as_str().len())
        + uri
            .authority()
            .map_or(0, |authority| authority.as_str().len());
    future
        + TASK
        + ALONGSIDE
        + headers.capacity().saturating_mul(FIELD)
        + fields
        + target
        + 2 * ALLOCATION
}

/// What a future `make` would return takes, without one being made.
fn size_of_made<A, F>(_make: &impl FnOnce(A) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[cfg(test)]
thread_local! {
    /// The streams this thread has made a task for.
    static MADE: Cell<usize> = const { Cell::new(0) };
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

    /// A stream's task is charged its future whole, measured from its type before it is
    /// made, and holds it boxed: what Tokio holds and moves is a pointer, not the future it
    /// would copy whole as the task is made and as it finishes (14 §3).
    #[test]
    fn a_stream_task_is_charged_its_future_and_holds_it_boxed() {
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
        let cost = stream_cost(future, &Request::new(()));
        assert!(
            cost >= future + TASK + ALONGSIDE,
            "{cost} for a future of {future}"
        );
    }

    /// What a stream costs grows with its head: every field's bytes, the map's capacity —
    /// room it was made with, used or not — and the target.
    #[test]
    fn a_streams_cost_grows_with_its_head() {
        let bare = stream_cost(0, &Request::new(()));
        let mut request = Request::get("https://example.com/a/longer/path?with=query")
            .body(())
            .unwrap();
        let target = stream_cost(0, &request);
        // The bare request's target is `/`, a byte already counted.
        let more = "example.com".len() + "/a/longer/path?with=query".len() - "/".len();
        assert!(target >= bare + more, "{bare} then {target}");
        request
            .headers_mut()
            .insert("x-big", HeaderValue::from_static("v"));
        let one = stream_cost(0, &request);
        let value = "x".repeat(4096);
        request
            .headers_mut()
            .insert("x-big", HeaderValue::from_str(&value).unwrap());
        let large = stream_cost(0, &request);
        assert!(large >= one + 4095, "{one} then {large}");
        request.headers_mut().reserve(200);
        let roomy = stream_cost(0, &request);
        assert!(roomy >= large + 100 * FIELD, "{large} then {roomy}");
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
            let (_light_answer, mut light_body) = light_send.send_request(post(), false).unwrap();
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
        let settings = Settings::default();
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
            let (answer, _) = send.send_request(get(), true).unwrap();
            let held = until_held(&storage, |used| used > 0).await;
            assert!(
                held > TASK + ALONGSIDE,
                "{held} charged for a stream's task"
            );
            gate.notify_one();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            until_held(&storage, |used| used == 0).await;
        });
    }

    /// A stream the client resets gives its charge back as its task goes.
    #[test]
    fn a_streams_charge_goes_back_when_the_client_resets_it() {
        locally(async {
            let storage = Storage::new(crate::storage::LIMIT);
            let asked = Rc::new(Cell::new(0));
            let (mut send, _told) = charged(&storage, never(&asked)).await;
            let (_answer, mut stream) = send.send_request(get(), false).unwrap();
            until_held(&storage, |used| used > 0).await;
            stream.send_reset(::h2::Reason::CANCEL);
            until_held(&storage, |used| used == 0).await;
            assert_eq!(asked.get(), 1);
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

    /// A stream the worker cannot pay for is answered `503` at once by the driver: no task is
    /// made for it, the core is never asked, nothing stays charged, and the refusal is told.
    #[test]
    fn a_stream_the_worker_cannot_pay_for_is_answered_503_with_no_task() {
        locally(async {
            // Room for less than any stream's task; the provision for the refusal is there.
            let storage = Storage::new(TASK);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged(&storage, never(&asked)).await;
            let made = MADE.get();
            let (answer, _) = send.send_request(get(), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(answer.headers()[DATE], "Thu, 01 Jan 1970 00:00:00 GMT");
            assert_eq!(
                MADE.get(),
                made,
                "a task was made for a stream not paid for"
            );
            assert_eq!(asked.get(), 0);
            assert_eq!(*told.borrow(), [(StatusCode::SERVICE_UNAVAILABLE, false)]);
            assert_eq!(storage.used(), 0);
        });
    }

    /// A gRPC call refused for want of storage is told so as a call: `RESOURCE_EXHAUSTED`,
    /// as the core answers one it has no storage for (15 §6).
    #[test]
    fn a_grpc_call_the_worker_cannot_pay_for_is_told_resource_exhausted() {
        locally(async {
            let storage = Storage::new(TASK);
            let asked = Rc::new(Cell::new(0));
            let (mut send, told) = charged(&storage, never(&asked)).await;
            let call = Request::post("http://example.com/helloworld.Greeter/SayHello")
                .version(Version::HTTP_2)
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let (answer, _) = send.send_request(call, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert_eq!(answer.headers()["grpc-status"], "8");
            assert_eq!(*told.borrow(), [(StatusCode::OK, true)]);
            assert_eq!(asked.get(), 0);
        });
    }

    /// A worker without even its provision left refuses the stream outright, with
    /// REFUSED_STREAM, which tells the client it may send it again (RFC 9113 §8.7).
    #[test]
    fn a_worker_without_its_provision_refuses_the_stream() {
        locally(async {
            let storage = Storage::with_provision(0, 0);
            let asked = Rc::new(Cell::new(0));
            let made = MADE.get();
            let (mut send, told) = charged(&storage, never(&asked)).await;
            let (answer, _) = send.send_request(get(), true).unwrap();
            let refused = within(answer).await.unwrap_err();
            assert_eq!(
                refused.reason(),
                Some(::h2::Reason::REFUSED_STREAM),
                "{refused:?}"
            );
            assert_eq!(MADE.get(), made);
            assert!(told.borrow().is_empty());
            assert_eq!(asked.get(), 0);
        });
    }
}
