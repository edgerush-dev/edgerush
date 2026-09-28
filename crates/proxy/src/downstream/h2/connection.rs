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
use crate::interim::Interim;
use crate::request_body::RequestBody;
use crate::storage::Storage;
use bytes::Bytes;
use http::header::{DATE, HeaderValue};
use http::{Request, Response, StatusCode, Version};
use http_body::Body;
use std::cell::{Cell, RefCell};
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::pin::Pin;
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
#[derive(Default)]
struct Streams {
    open: Cell<usize>,
    driver: RefCell<Option<Waker>>,
    /// Streams accepted in the connection's life.
    seen: Cell<u64>,
    /// Of those, the ones the client reset before their final head was sent.
    premature: Cell<u64>,
}

impl Streams {
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
            .max_local_error_reset_streams(Some(1024));
        builder
    }
}

/// Serves an HTTP/2 connection, preface included, until it ends. Each request is handed to
/// `respond`, and `date` dates an answer that has no `Date` of its own.
pub(crate) async fn serve<S, R, F, B, D>(
    socket: S,
    settings: Settings,
    storage: Rc<Storage>,
    date: Rc<D>,
    drain: &Drain,
    respond: Rc<R>,
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
    drive(&mut connection, settings, storage, date, drain, respond).await;
}

/// Drives `connection`, once handshaken, until it ends, as [`serve`] says.
async fn drive<S, R, F, B, D>(
    connection: &mut ::h2::server::Connection<S, Outgoing>,
    settings: Settings,
    storage: Rc<Storage>,
    date: Rc<D>,
    drain: &Drain,
    respond: Rc<R>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Fn(Request<RequestBody>, Interim) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
{
    let streams = Rc::new(Streams::default());
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
            connection.poll_accept(cx).map(Next::Accepted)
        })
        .await;
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
            Next::Resetting => {
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
        let _detached = tokio::task::spawn_local(stream_task(
            request,
            Responder::new(send),
            Open::new(&streams),
            Rc::clone(&respond),
            Rc::clone(&storage),
            Rc::clone(&date),
            settings.idle,
        ));
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

/// A stream's task: its answer, boxed, counted open while it lives. The answer's future is
/// as large as its largest state, the exchange's included, several kilobytes; a task holds
/// its future inline and moves all of it as the task is made and as it finishes. Boxed once
/// here, what the task holds and moves is a pointer.
fn stream_task<R, F, B, D>(
    request: Request<::h2::RecvStream>,
    responder: Responder,
    open: Open,
    respond: Rc<R>,
    storage: Rc<Storage>,
    date: Rc<D>,
    idle: Duration,
) -> Pin<Box<impl Future<Output = ()>>>
where
    R: Fn(Request<RequestBody>, Interim) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    Box::pin(async move {
        let answered = answer(request, responder, &*respond, &storage, &*date, idle).await;
        if answered == Ended::ResetEarly {
            open.0.premature.set(open.0.premature.get() + 1);
        }
        drop(open);
    })
}

/// Answers one stream.
async fn answer<R, F, B, D>(
    request: Request<::h2::RecvStream>,
    mut responder: Responder,
    respond: &R,
    storage: &Rc<Storage>,
    date: &D,
    idle: Duration,
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
    let request =
        request.map(|body| RequestBody::H2(IncomingH2::new(body, idle).heard_by(interim.clone())));
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
    let end = body.is_end_stream();
    // A head h2 refuses is not sent, and the stream is reset when its responder goes.
    let Ok(mut stream) = responder.final_head(Response::from_parts(head, ()), end) else {
        return Ended::Otherwise;
    };
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
        LONG, locally, locally_paused, post, serving, wire, within,
    };
    use http_body_util::{BodyExt, Full};

    /// What a future `make` returns takes, without one being made.
    fn size_of_made<A, F>(_make: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    /// A stream's task holds a pointer to its answer's future, not the future itself, which
    /// the task would copy whole as it is made and as it finishes (14 §3).
    #[test]
    fn a_stream_task_holds_its_answer_boxed() {
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
        );
        let task = size_of_made(
            |(request, responder, open, respond, storage, date): Asked| {
                stream_task(request, responder, open, respond, storage, date, LONG)
            },
        );
        let answer = size_of_made(
            |(request, responder, _, respond, storage, date): Asked| async move {
                answer(request, responder, &*respond, &storage, &*date, LONG).await
            },
        );
        assert_eq!(
            task,
            std::mem::size_of::<usize>(),
            "a stream's task holds {task} bytes; its answer's future is {answer}"
        );
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
            let (drain, storage) = (Drain::default(), Storage::new(crate::storage::LIMIT));
            let long = "a long header, Huffman-coded ".repeat(40);
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
                    Rc::clone(&date),
                    &drain,
                    Rc::clone(&respond),
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
}
