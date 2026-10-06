//! What the locked `h2` (0.4.19) actually does, pinned before EdgeRush builds on it.
//!
//! These are probes, not tests of EdgeRush: each one drives the library against the
//! scripted peer in `h2_peer` and asserts what went over the wire, so that the limits in
//! 15 §3 rest on behaviour seen rather than on an API's name
//! ([15 §3](../../../docs/15-http2-and-grpc.md)). A version of `h2` that behaves otherwise
//! fails here first. The client half is the upstream hop, the server half the downstream
//! one; where a probe finds a missing bound, it says which application guard stands in.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

// Kept beside the code whose own tests also drive it.
#[path = "../src/h2_peer.rs"]
mod h2_peer;

use bytes::Bytes;
use h2::client::{self, ResponseFuture, SendRequest};
use h2::server;
use h2::{Reason, RecvStream};
use h2_peer::{Frame, Peer, code, flag, kind, setting};
use http::{Request, Response, StatusCode, Version};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Long enough for a frame the library meant to send to have been sent. Used only to show
/// that something is *not* sent, which has no other way to be observed.
const QUIET: Duration = Duration::from_millis(200);

/// A test that waits for what never comes should fail, not hang.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

/// Two ends of an in-memory connection, with room for everything a probe sends.
fn wire() -> (DuplexStream, DuplexStream) {
    tokio::io::duplex(1 << 20)
}

fn get() -> Request<()> {
    Request::builder()
        .uri("http://example.com/")
        .version(Version::HTTP_2)
        .body(())
        .unwrap()
}

// ===== The client: EdgeRush's upstream hop =====

/// A client connection's driver that, after every poll, publishes the peer's current
/// stream limit. This is how a pool learns of a changed limit: `current_max_send_streams`
/// is a count, not something to wait on, but the driver is polled whenever a frame
/// arrives, so a check after each poll sees every change without a timer or a spin.
struct Observed {
    connection: Pin<Box<client::Connection<DuplexStream, Bytes>>>,
    handle: SendRequest<Bytes>,
    limit: watch::Sender<usize>,
}

impl Future for Observed {
    type Output = Result<(), h2::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.connection.as_mut().poll(cx);
        let now = self.handle.current_max_send_streams();
        self.limit.send_if_modified(|seen| {
            let changed = *seen != now;
            *seen = now;
            changed
        });
        result
    }
}

struct Client {
    send: SendRequest<Bytes>,
    /// The peer's stream limit as the driver last saw it.
    limit: watch::Receiver<usize>,
    driver: JoinHandle<Result<(), h2::Error>>,
}

/// An h2 client on one end of a wire, driven by an [`Observed`] driver, and the scripted
/// server on the other, which has answered with `settings` and read the client's own.
async fn client_with(
    builder: &client::Builder,
    settings: &[(u16, u32)],
) -> (Client, Peer<DuplexStream>, Frame) {
    let (near, far) = wire();
    let (send, connection) = within(builder.handshake::<_, Bytes>(near)).await.unwrap();
    let (limit_tx, limit) = watch::channel(send.current_max_send_streams());
    let driver = tokio::spawn(Observed {
        connection: Box::pin(connection),
        handle: send.clone(),
        limit: limit_tx,
    });
    let (mut peer, first) = Peer::accept_as_server(far, settings).await;
    // Acknowledge the client's settings, as a server does, so that its local settings apply.
    peer.send(&h2_peer::settings_ack()).await;
    (
        Client {
            send,
            limit,
            driver,
        },
        peer,
        first,
    )
}

async fn client(settings: &[(u16, u32)]) -> (Client, Peer<DuplexStream>) {
    let (client, peer, _) = client_with(&client::Builder::new(), settings).await;
    (client, peer)
}

impl Client {
    /// Waits until the driver has seen the peer's limit become `n`.
    async fn limit_is(&mut self, n: usize) {
        within(self.limit.wait_for(|seen| *seen == n))
            .await
            .expect("the driver ended");
    }
}

/// Opens a stream on `handle` that sends no body.
fn open(handle: &mut SendRequest<Bytes>) -> ResponseFuture {
    let (response, _) = handle.send_request(get(), true).unwrap();
    response
}

/// Whether `handle` says it is ready, asked once without waiting.
fn ready_now(handle: &mut SendRequest<Bytes>) -> Poll<Result<(), h2::Error>> {
    let waker = std::task::Waker::noop();
    handle.poll_ready(&mut Context::from_waker(waker))
}

fn headers_on(frames: &[Frame]) -> Vec<u32> {
    frames
        .iter()
        .filter(|f| f.kind == kind::HEADERS)
        .map(|f| f.stream)
        .collect()
}

/// What the scripted server reads until the client has gone quiet.
async fn quiet_frames(peer: &mut Peer<DuplexStream>) -> Vec<Frame> {
    peer.drain_for(QUIET).await
}

/// Waits for the client to open `stream`. A server may answer only a stream it has seen:
/// h2 holds a new stream until its driver sends it, and an answer that overtakes it is a
/// protocol error.
async fn opened(peer: &mut Peer<DuplexStream>, stream: u32) {
    peer.until(|f| f.kind == kind::HEADERS && f.stream == stream)
        .await;
}

/// Answers `stream` with a bodiless 200.
async fn answer(peer: &mut Peer<DuplexStream>, stream: u32) {
    peer.send(&h2_peer::headers(stream, h2_peer::response(200), true))
        .await;
}

/// 15 §3: "Disable push explicitly". A default client leaves push enabled, and advertises
/// none of the limits it enforces; what a client is built with is exactly what it sends.
#[tokio::test]
async fn a_client_sends_the_settings_it_is_built_with_and_no_others() {
    let (_default, _, first) = client_with(&client::Builder::new(), &[]).await;
    assert_eq!(first.settings(), vec![], "a default client says nothing");

    let mut builder = client::Builder::new();
    builder
        .enable_push(false)
        .header_table_size(4096)
        .max_concurrent_streams(0)
        .initial_window_size(262_144)
        .max_frame_size(16_384)
        .max_header_list_size(65_536)
        .initial_connection_window_size(1 << 20);
    let (_client, mut peer, first) = client_with(&builder, &[]).await;
    assert_eq!(
        first.settings(),
        vec![
            (setting::HEADER_TABLE_SIZE, 4096),
            (setting::ENABLE_PUSH, 0),
            (setting::MAX_CONCURRENT_STREAMS, 0),
            (setting::INITIAL_WINDOW_SIZE, 262_144),
            (setting::MAX_FRAME_SIZE, 16_384),
            (setting::MAX_HEADER_LIST_SIZE, 65_536),
        ]
    );
    // The connection window is not a setting: it is raised from 65,535 by a WINDOW_UPDATE.
    let (update, _) = peer.until(|f| f.kind == kind::WINDOW_UPDATE).await;
    assert_eq!((update.stream, update.increment()), (0, (1 << 20) - 65_535));
}

/// 15 §3: "`initial_max_send_streams` is replaced by the peer's SETTINGS". Until they come,
/// a default client assumes no limit at all and opens every stream it is given.
#[tokio::test]
async fn before_the_peer_settings_a_client_opens_as_many_streams_as_its_initial_guess() {
    for (initial, opened) in [(usize::MAX, vec![1, 3, 5]), (1, vec![1])] {
        let (near, far) = wire();
        let mut builder = client::Builder::new();
        builder.initial_max_send_streams(initial);
        let (send, connection) = builder.handshake::<_, Bytes>(near).await.unwrap();
        let driver = tokio::spawn(connection);
        assert_eq!(send.current_max_send_streams(), initial);

        // A server that has not answered with its SETTINGS yet.
        let mut peer = Peer::new(far);
        let mut preface = [0u8; h2_peer::PREFACE.len()];
        within(tokio::io::AsyncReadExt::read_exact(
            peer.io_mut(),
            &mut preface,
        ))
        .await
        .unwrap();
        let _responses: Vec<_> = (0..3).map(|_| open(&mut send.clone())).collect();
        let frames = quiet_frames(&mut peer).await;
        assert_eq!(headers_on(&frames), opened, "initial guess {initial}");

        // The peer's SETTINGS replace the guess, whatever it was, and release what it held.
        peer.send(&h2_peer::settings(&[(setting::MAX_CONCURRENT_STREAMS, 10)]))
            .await;
        peer.barrier().await;
        assert_eq!(send.current_max_send_streams(), 10);
        if initial == 1 {
            let (_, before) = peer
                .until(|f| f.kind == kind::HEADERS && f.stream == 5)
                .await;
            assert_eq!(headers_on(&before), vec![3], "held streams opened");
        }
        driver.abort();
    }
}

/// 15 §3: "Prove how the driver wakes pool waiters on SETTINGS". The peer raises, zeroes
/// and lowers its limit while no stream exists; the driver sees each value as it arrives.
#[tokio::test]
async fn the_driver_sees_every_change_of_the_peer_limit_with_no_stream_open() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 2)]).await;
    client.limit_is(2).await;
    for n in [0, 5, 1] {
        peer.send(&h2_peer::settings(&[(setting::MAX_CONCURRENT_STREAMS, n)]))
            .await;
        client.limit_is(n as usize).await;
    }
    // The peer never set anything else in motion: no stream was opened or finished.
    let frames = peer.settled().await;
    assert!(headers_on(&frames).is_empty());
}

/// 15 §3: "Readiness alone is not our admission counter." At a limit of one, every clone
/// of a handle may still queue a stream of its own: h2 holds them all, unsent, and a fresh
/// clone says it is ready throughout.
#[tokio::test]
async fn readiness_is_not_admission_cloned_handles_queue_past_the_peer_limit() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 1)]).await;
    client.limit_is(1).await;

    let first = open(&mut client.send);
    let mut queued = Vec::new();
    let mut clones = Vec::new();
    for _ in 0..3 {
        let mut clone = client.send.clone();
        assert!(ready_now(&mut clone).is_ready(), "a fresh clone is ready");
        queued.push(open(&mut clone));
        clones.push(clone);
    }
    let frames = quiet_frames(&mut peer).await;
    assert_eq!(
        headers_on(&frames),
        vec![1],
        "one stream on the wire, three held"
    );
    for clone in &mut clones {
        assert!(
            ready_now(clone).is_pending(),
            "a clone holding a queued stream"
        );
    }
    assert!(ready_now(&mut client.send.clone()).is_ready());

    // Each answer lets exactly one held stream out, in the order they were queued.
    for (answered, next) in [(1, 3), (3, 5), (5, 7)] {
        answer(&mut peer, answered).await;
        let (headers, _) = peer.until(|f| f.kind == kind::HEADERS).await;
        assert_eq!(headers.stream, next);
    }
    assert_eq!(within(first).await.unwrap().status(), StatusCode::OK);
    drop(queued);
}

/// A limit of zero holds a new stream until it is raised; the handle holding it is woken
/// then, by the driver, with no other stream having finished.
#[tokio::test]
async fn a_zero_limit_holds_a_stream_and_wakes_its_handle_when_raised() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 0)]).await;
    client.limit_is(0).await;
    let mut handle = client.send.clone();
    let _held = open(&mut handle);
    assert!(headers_on(&quiet_frames(&mut peer).await).is_empty());
    let waiting = tokio::spawn(handle.ready());

    peer.send(&h2_peer::settings(&[(setting::MAX_CONCURRENT_STREAMS, 1)]))
        .await;
    let (headers, _) = peer.until(|f| f.kind == kind::HEADERS).await;
    assert_eq!(headers.stream, 1);
    within(waiting).await.unwrap().unwrap();
}

/// 15 §4, point 5: "A peer limit reduction may leave existing streams above the new limit:
/// let them finish and admit no more until there is room."
#[tokio::test]
async fn a_lowered_limit_lets_open_streams_finish_and_holds_new_ones_until_room() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 3)]).await;
    client.limit_is(3).await;
    let open_streams: Vec<_> = (0..3).map(|_| open(&mut client.send.clone())).collect();
    let (_, _) = peer
        .until(|f| f.kind == kind::HEADERS && f.stream == 5)
        .await;

    peer.send(&h2_peer::settings(&[(setting::MAX_CONCURRENT_STREAMS, 1)]))
        .await;
    client.limit_is(1).await;
    let _new = open(&mut client.send.clone());

    for answered in [1, 3] {
        answer(&mut peer, answered).await;
        assert!(headers_on(&peer.settled().await).is_empty(), "no room yet");
    }
    answer(&mut peer, 5).await;
    let (headers, _) = peer.until(|f| f.kind == kind::HEADERS).await;
    assert_eq!(headers.stream, 7);
    for response in open_streams {
        assert_eq!(within(response).await.unwrap().status(), StatusCode::OK);
    }
}

/// 15 §3 and §6: GOAWAY with accepted and unprocessed streams. Streams at or below its last
/// identifier carry on to their answers; those above it fail at once with a GOAWAY error
/// from the remote, which is how an unprocessed stream is recognised: the public error has
/// no last-stream field. New streams fail, and a fresh clone's readiness reports the error
/// immediately rather than waiting.
#[tokio::test]
async fn goaway_fails_only_the_streams_above_its_last_identifier() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 10)]).await;
    client.limit_is(10).await;
    let mut streams: Vec<_> = (0..3).map(|_| open(&mut client.send.clone())).collect();
    let (_, _) = peer
        .until(|f| f.kind == kind::HEADERS && f.stream == 5)
        .await;

    peer.send(&h2_peer::goaway(3, code::NO_ERROR)).await;
    peer.barrier().await;
    let unprocessed = within(streams.pop().unwrap()).await.unwrap_err();
    assert!(unprocessed.is_go_away() && unprocessed.is_remote());
    assert_eq!(unprocessed.reason(), Some(Reason::NO_ERROR));

    match ready_now(&mut client.send.clone()) {
        Poll::Ready(Err(e)) => assert!(e.is_go_away() && e.is_remote()),
        other => panic!("a fresh clone after GOAWAY: {other:?}"),
    }
    let refused = client.send.send_request(get(), true).unwrap_err();
    assert!(refused.is_go_away());

    answer(&mut peer, 1).await;
    answer(&mut peer, 3).await;
    for response in streams {
        assert_eq!(within(response).await.unwrap().status(), StatusCode::OK);
    }
}

/// The other half of the classification: a stream the GOAWAY counted as processed, whose
/// connection then ends, fails with an I/O error, not a GOAWAY one; and a stream refused
/// with RST_STREAM(REFUSED_STREAM) is a remote reset with that reason. Only these two say
/// "not processed" (RFC 9113 §8.7).
#[tokio::test]
async fn a_processed_stream_on_a_dying_connection_fails_differently_from_an_unprocessed_one() {
    let (mut client, mut peer) = client(&[(setting::MAX_CONCURRENT_STREAMS, 10)]).await;
    client.limit_is(10).await;
    let refused = open(&mut client.send.clone());
    let processed = open(&mut client.send.clone());
    let unprocessed = open(&mut client.send.clone());
    let (_, _) = peer
        .until(|f| f.kind == kind::HEADERS && f.stream == 5)
        .await;

    peer.send(&h2_peer::rst_stream(1, code::REFUSED_STREAM))
        .await;
    let refused = within(refused).await.unwrap_err();
    assert!(refused.is_reset() && refused.is_remote());
    assert_eq!(refused.reason(), Some(Reason::REFUSED_STREAM));

    peer.send(&h2_peer::goaway(3, code::INTERNAL_ERROR)).await;
    let unprocessed = within(unprocessed).await.unwrap_err();
    assert!(unprocessed.is_go_away() && unprocessed.is_remote());
    assert_eq!(unprocessed.reason(), Some(Reason::INTERNAL_ERROR));

    drop(peer);
    let processed = within(processed).await.unwrap_err();
    assert!(processed.is_io(), "{processed:?}");
    assert!(!processed.is_go_away());
}

/// A pool learns of a lost connection from its driver ending, with no stream open to
/// report it, and from a fresh clone's readiness, which fails at once. An idle connection
/// the peer simply closes ends its driver with `Ok`: the end is the signal, not an error.
#[tokio::test]
async fn a_connection_lost_idle_ends_its_driver_and_fails_readiness() {
    let (client, peer) = client(&[]).await;
    drop(peer);
    let ended = within(client.driver).await.unwrap();
    assert!(
        ended.is_ok(),
        "an idle connection closed by its peer: {ended:?}"
    );
    let mut fresh = client.send.clone();
    match ready_now(&mut fresh) {
        Poll::Ready(Err(e)) => assert!(e.is_io(), "{e:?}"),
        other => panic!("readiness on a closed connection: {other:?}"),
    }
}

/// The last handle's drop closes an idle connection with GOAWAY(NO_ERROR); while a stream
/// is still open, the connection waits for it.
#[tokio::test]
async fn dropping_the_last_handle_closes_the_connection_once_its_streams_are_done() {
    let (near, far) = wire();
    let (mut send, connection) = client::Builder::new()
        .handshake::<_, Bytes>(near)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let (mut peer, _) = Peer::accept_as_server(far, &[]).await;
    let response = open(&mut send);
    let (_, _) = peer.until(|f| f.kind == kind::HEADERS).await;
    drop(send);
    let frames = quiet_frames(&mut peer).await;
    assert!(
        frames.iter().all(|f| f.kind != kind::GOAWAY),
        "closed under an open stream: {frames:?}"
    );

    answer(&mut peer, 1).await;
    assert_eq!(within(response).await.unwrap().status(), StatusCode::OK);
    let (goaway, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
    assert_eq!(goaway.goaway(), (0, code::NO_ERROR));
    within(driver).await.unwrap().unwrap();
}

/// Reset/drop order: a stream is reset (CANCEL) only when every handle to it is gone —
/// the response future and the send half both. Once finished both ways, dropping sends
/// nothing.
#[tokio::test]
async fn a_stream_is_cancelled_when_its_last_handle_goes_and_not_before() {
    let (mut client, mut peer) = client(&[]).await;
    let (response, body) = client.send.send_request(get(), false).unwrap();
    let (_, _) = peer.until(|f| f.kind == kind::HEADERS).await;

    drop(response);
    let frames = quiet_frames(&mut peer).await;
    assert!(
        frames.iter().all(|f| f.kind != kind::RST_STREAM),
        "{frames:?}"
    );
    drop(body);
    let (reset, _) = peer.until(|f| f.kind == kind::RST_STREAM).await;
    assert_eq!((reset.stream, reset.reset()), (1, code::CANCEL));

    // A response body dropped part way through cancels the stream too.
    let (response, _) = client.send.send_request(get(), true).unwrap();
    opened(&mut peer, 3).await;
    peer.send(&h2_peer::headers(3, h2_peer::response(200), false))
        .await;
    peer.send(&h2_peer::data(3, b"part", false)).await;
    let body = within(response).await.unwrap().into_body();
    drop(body);
    let (reset, _) = peer.until(|f| f.kind == kind::RST_STREAM).await;
    assert_eq!((reset.stream, reset.reset()), (3, code::CANCEL));

    // A finished stream's handles go quietly.
    let (response, _) = client.send.send_request(get(), true).unwrap();
    opened(&mut peer, 5).await;
    answer(&mut peer, 5).await;
    drop(within(response).await.unwrap());
    let frames = quiet_frames(&mut peer).await;
    assert!(
        frames.iter().all(|f| f.kind != kind::RST_STREAM),
        "{frames:?}"
    );
}

/// 15 §3: "`ResponseFuture::poll_informational` provides upstream 1xx". Interim heads come
/// out in order before the final one; a caller that never asks for them gets the final
/// head alone.
#[tokio::test]
async fn informational_heads_come_before_the_final_one_and_can_be_skipped() {
    let (mut client, mut peer) = client(&[]).await;
    let mut asked = open(&mut client.send);
    let skipped = open(&mut client.send.clone());
    opened(&mut peer, 3).await;
    for stream in [1, 3] {
        peer.send(&h2_peer::headers(
            stream,
            h2_peer::block(&[(":status", "103"), ("link", "</a.css>; rel=preload")]),
            false,
        ))
        .await;
        peer.send(&h2_peer::headers(stream, h2_peer::response(100), false))
            .await;
        answer(&mut peer, stream).await;
    }
    peer.barrier().await;

    let mut interim = Vec::new();
    while let Some(head) = within(poll_fn(|cx| asked.poll_informational(cx))).await {
        interim.push(head.unwrap().status().as_u16());
    }
    assert_eq!(interim, vec![103, 100]);
    assert_eq!(within(asked).await.unwrap().status(), StatusCode::OK);
    assert_eq!(within(skipped).await.unwrap().status(), StatusCode::OK);
}

/// A missing bound: h2 queues every interim head a server sends, with no count. Each is
/// bounded by the header list size; their number is bounded only by the application
/// reading them as they come and refusing past its own limit (14 §8: 16 heads, 128 KiB).
#[tokio::test]
async fn informational_heads_are_queued_without_a_count_limit() {
    let (mut client, mut peer) = client(&[]).await;
    let mut response = open(&mut client.send);
    opened(&mut peer, 1).await;
    for _ in 0..1000 {
        peer.send(&h2_peer::headers(1, h2_peer::response(103), false))
            .await;
    }
    peer.barrier().await;
    let mut queued = 0;
    while let Poll::Ready(Some(head)) =
        response.poll_informational(&mut Context::from_waker(std::task::Waker::noop()))
    {
        head.unwrap();
        queued += 1;
    }
    assert_eq!(queued, 1000);
}

/// 15 §5: "ask for capacity before staging more DATA". `send_data` takes any amount
/// whatever the window, and holds what it cannot send; only capacity bounds staging.
#[tokio::test]
async fn send_data_buffers_beyond_the_window_so_capacity_must_be_asked_for() {
    let (mut client, mut peer) = client(&[]).await;
    let (_response, mut body) = client.send.send_request(get(), false).unwrap();
    assert_eq!(body.capacity(), 0, "nothing reserved, nothing granted");
    body.send_data(Bytes::from(vec![0u8; 1 << 20]), false)
        .unwrap();
    let frames = quiet_frames(&mut peer).await;
    let sent: usize = frames
        .iter()
        .filter(|f| f.kind == kind::DATA)
        .map(|f| f.payload.len())
        .sum();
    assert_eq!(sent, 65_535, "the peer's default window; the rest is held");
}

/// Capacity granted to one stream is at most the smaller of the peer's window and the send
/// buffer: `max_send_buffer_size` is the bound on what a caller that asks first can stage.
#[tokio::test]
async fn granted_capacity_is_bounded_by_the_send_buffer_and_the_window() {
    let mut builder = client::Builder::new();
    builder.max_send_buffer_size(16_384);
    let (mut client, mut peer, _) =
        client_with(&builder, &[(setting::INITIAL_WINDOW_SIZE, 1 << 20)]).await;
    peer.send(&h2_peer::window_update(0, 1 << 20)).await;
    peer.barrier().await;

    let (_response, mut body) = client.send.send_request(get(), false).unwrap();
    body.reserve_capacity(1 << 20);
    let granted = within(poll_fn(|cx| body.poll_capacity(cx)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(granted, 16_384);
    body.send_data(Bytes::from(vec![0u8; granted]), false)
        .unwrap();
    // Once written out, the buffer frees and more is granted.
    let more = within(poll_fn(|cx| body.poll_capacity(cx)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(more, 16_384);
}

/// 15 §3: "`FlowControl::release_capacity` releases both stream and connection receive
/// credit." Nothing is returned when DATA is merely read; releasing it returns credit to
/// the stream and the connection together, once what is unclaimed reaches half of what
/// the peer may still send.
#[tokio::test]
async fn releasing_capacity_returns_stream_and_connection_credit_together() {
    // Default windows, 65,535 on both, so that the two thresholds are the same.
    let (mut client, mut peer) = client(&[]).await;
    let response = open(&mut client.send);
    opened(&mut peer, 1).await;
    peer.send(&h2_peer::headers(1, h2_peer::response(200), false))
        .await;
    peer.send(&h2_peer::data(1, &[0u8; 16_384], false)).await;
    peer.send(&h2_peer::data(1, &[0u8; 16_384], false)).await;
    let mut body: RecvStream = within(response).await.unwrap().into_body();
    let first = within(body.data()).await.unwrap().unwrap();
    let second = within(body.data()).await.unwrap().unwrap();
    assert!(peer.settled().await.is_empty(), "read but not released");

    // 8,192 unclaimed against 32,767 still open to the peer: under half, nothing sent.
    body.flow_control().release_capacity(8_192).unwrap();
    assert!(peer.settled().await.is_empty(), "under half a window");
    body.flow_control()
        .release_capacity(first.len() + second.len() - 8_192)
        .unwrap();
    let mut updates: Vec<_> = peer
        .settled()
        .await
        .into_iter()
        .filter(|f| f.kind == kind::WINDOW_UPDATE)
        .map(|f| (f.stream, f.increment()))
        .collect();
    updates.sort_unstable();
    assert_eq!(updates, vec![(0, 32_768), (1, 32_768)]);
}

/// The receive window is enforced: a peer that sends past it loses the stream, and past
/// the connection window, the connection. This is what makes the window a memory bound.
#[tokio::test]
async fn data_past_the_advertised_window_is_refused() {
    let mut builder = client::Builder::new();
    builder.initial_window_size(16_384);
    let (mut client, mut peer, _) = client_with(&builder, &[]).await;
    let response = open(&mut client.send);
    opened(&mut peer, 1).await;
    peer.send(&h2_peer::headers(1, h2_peer::response(200), false))
        .await;
    peer.send(&h2_peer::data(1, &[0u8; 16_384], false)).await;
    peer.send(&h2_peer::data(1, &[0u8; 1], false)).await;
    let (reset, _) = peer.until(|f| f.kind == kind::RST_STREAM).await;
    assert_eq!((reset.stream, reset.reset()), (1, code::FLOW_CONTROL_ERROR));
    drop(response);

    // The connection window, 65,535 here, spans streams.
    let streams: Vec<_> = (0..5).map(|_| open(&mut client.send.clone())).collect();
    opened(&mut peer, 11).await;
    for stream in [3, 5, 7, 9, 11] {
        peer.send(&h2_peer::headers(stream, h2_peer::response(200), false))
            .await;
        peer.send(&h2_peer::data(stream, &[0u8; 16_384], false))
            .await;
    }
    let (goaway, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
    assert_eq!(goaway.goaway().1, code::FLOW_CONTROL_ERROR);
    drop(streams);
}

/// Stream identifiers run out after 2^30 streams on one client connection, at which point
/// `send_request` fails with a user error. That is not probed here: reaching it takes a
/// thousand million streams, and the only shortcut, `initial_stream_id`, is behind h2's
/// `unstable` feature, which EdgeRush does not turn on. The pool's guard is its own
/// maximum requests per connection, which retires a connection long before (15 §4).
#[test]
fn stream_identifier_exhaustion_is_left_to_the_request_count_retirement() {}

// ===== The server: EdgeRush's downstream hop =====

/// What a probe's server does with each request it accepts.
#[derive(Clone, Copy)]
enum Serve {
    /// Answers 200 at once, with no body.
    Answer,
    /// Hands the request and its responder to the test and does nothing else.
    Hold,
}

type Accepted = (Request<RecvStream>, server::SendResponse<Bytes>);

struct Server {
    accepted: mpsc::UnboundedReceiver<Accepted>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), h2::Error>>,
}

impl Server {
    /// Starts a graceful shutdown of the connection.
    fn shut_down(&mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
    }

    async fn next(&mut self) -> Accepted {
        within(self.accepted.recv())
            .await
            .expect("the server ended")
    }
}

/// Runs a server connection in a task of its own, accepting as `how` says.
fn serve(mut connection: server::Connection<DuplexStream, Bytes>, how: Serve) -> Server {
    let (tx, accepted) = mpsc::unbounded_channel();
    let (shutdown, mut stop) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut stopping = false;
        loop {
            tokio::select! {
                result = &mut stop, if !stopping => {
                    stopping = true;
                    if result.is_ok() {
                        connection.graceful_shutdown();
                    }
                }
                next = connection.accept() => match next {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e),
                    Some(Ok((request, mut respond))) => match how {
                        Serve::Answer => {
                            // A stream the peer has already reset refuses its answer.
                            let _ = respond.send_response(Response::new(()), true);
                            drop(request);
                        }
                        Serve::Hold => {
                            let _ = tx.send((request, respond));
                        }
                    },
                },
            }
        }
    });
    Server {
        accepted,
        shutdown: Some(shutdown),
        task,
    }
}

/// Runs a server connection that is driven but never accepts: what arrives waits in h2's
/// queue of pending accepts.
fn drive_only(mut connection: server::Connection<DuplexStream, Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _ = poll_fn(|cx| connection.poll_closed(cx)).await;
    })
}

/// An h2 server on one end of a wire and the scripted client on the other, which has sent
/// its preface and read the server's SETTINGS, and acknowledged them if `ack`.
async fn server_with(
    builder: &server::Builder,
    ack: bool,
) -> (
    server::Connection<DuplexStream, Bytes>,
    Peer<DuplexStream>,
    Frame,
) {
    let (near, far) = wire();
    let mut peer = Peer::open_as_client(far, &[]).await;
    let connection = within(builder.handshake::<_, Bytes>(near)).await.unwrap();
    let first = peer.next().await;
    assert_eq!(first.kind, kind::SETTINGS, "a server's first frame");
    if ack {
        peer.send(&h2_peer::settings_ack()).await;
    }
    (connection, peer, first)
}

/// Opens stream `id` from the scripted client with a GET.
async fn request(peer: &mut Peer<DuplexStream>, id: u32, end_stream: bool) {
    peer.send(&h2_peer::headers(
        id,
        h2_peer::request("GET", "/"),
        end_stream,
    ))
    .await;
}

fn goaway_in(frames: &[Frame]) -> Option<(u32, u32)> {
    frames
        .iter()
        .find(|f| f.kind == kind::GOAWAY)
        .map(Frame::goaway)
}

fn resets_in(frames: &[Frame]) -> Vec<(u32, u32)> {
    frames
        .iter()
        .filter(|f| f.kind == kind::RST_STREAM)
        .map(|f| (f.stream, f.reset()))
        .collect()
}

/// Provokes the server `attempts` times, one attempt at a time, and says at which attempt
/// (counted from one) it first closed the connection, and with what code.
async fn goaway_after(
    peer: &mut Peer<DuplexStream>,
    attempts: u32,
    mut provoke: impl AsyncFnMut(&mut Peer<DuplexStream>, u32),
) -> Option<(u32, u32)> {
    for attempt in 1..=attempts {
        provoke(peer, attempt).await;
        if let Some((_, code)) = goaway_in(&peer.settled().await) {
            return Some((attempt, code));
        }
    }
    None
}

/// The server's settings, as the client's were: what it is built with is what it sends,
/// and the connection window is raised by a WINDOW_UPDATE.
#[tokio::test]
async fn a_server_sends_the_settings_it_is_built_with_and_no_others() {
    let (_, _, first) = server_with(&server::Builder::new(), true).await;
    assert_eq!(first.settings(), vec![], "a default server says nothing");

    let mut builder = server::Builder::new();
    builder
        .header_table_size(4096)
        .max_concurrent_streams(100)
        .initial_window_size(65_535)
        .max_frame_size(16_384)
        .max_header_list_size(65_536)
        .initial_connection_window_size(1 << 20);
    let (connection, mut peer, first) = server_with(&builder, true).await;
    let _server = serve(connection, Serve::Answer);
    assert_eq!(
        first.settings(),
        vec![
            (setting::HEADER_TABLE_SIZE, 4096),
            (setting::MAX_CONCURRENT_STREAMS, 100),
            (setting::INITIAL_WINDOW_SIZE, 65_535),
            (setting::MAX_FRAME_SIZE, 16_384),
            (setting::MAX_HEADER_LIST_SIZE, 65_536),
        ]
    );
    let (update, _) = peer.until(|f| f.kind == kind::WINDOW_UPDATE).await;
    assert_eq!((update.stream, update.increment()), (0, (1 << 20) - 65_535));
}

/// The stream limit holds from the first frame, before the client has acknowledged the
/// SETTINGS that announce it: a stream past it is refused, never handed to the server.
#[tokio::test]
async fn a_server_refuses_streams_past_its_limit_even_before_it_is_acknowledged() {
    let mut builder = server::Builder::new();
    builder.max_concurrent_streams(2);
    let (connection, mut peer, _) = server_with(&builder, false).await;
    let mut server = serve(connection, Serve::Hold);
    for id in [1, 3, 5] {
        request(&mut peer, id, false).await;
    }
    let frames = peer.settled().await;
    assert_eq!(resets_in(&frames), vec![(5, code::REFUSED_STREAM)]);
    let first = server.next().await;
    let second = server.next().await;
    assert!(server.accepted.try_recv().is_err(), "only two handed over");
    drop((first, second));
}

/// 15 §3: pending accepts and reset retention. Streams the client opens and resets before
/// the server accepts them wait in h2's queue outside the live-stream count; past
/// `max_pending_accept_reset_streams` the connection is closed with ENHANCE_YOUR_CALM.
#[tokio::test]
async fn resets_waiting_to_be_accepted_are_bounded() {
    let mut builder = server::Builder::new();
    builder.max_pending_accept_reset_streams(5);
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let _driver = drive_only(connection);
    let closed = goaway_after(&mut peer, 20, async |peer, attempt| {
        let id = attempt * 2 - 1;
        request(peer, id, false).await;
        peer.send(&h2_peer::rst_stream(id, code::CANCEL)).await;
    })
    .await;
    assert_eq!(closed, Some((6, code::ENHANCE_YOUR_CALM)));
}

/// A missing bound: the same rapid reset against a server that accepts as streams come
/// never fills that queue. Every stream is handed over, already reset, and the connection
/// stays open; whatever work a request starts is the application's to refuse
/// (CVE-2023-44487). The guard has to be ours: admission before an upstream is opened, and
/// a count of streams reset before their answer.
#[tokio::test]
async fn a_rapid_reset_against_a_server_that_accepts_is_not_bounded_by_h2() {
    let mut builder = server::Builder::new();
    builder.max_pending_accept_reset_streams(5);
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let mut server = serve(connection, Serve::Hold);
    let mut handed_over = 0;
    for id in (1..=199).step_by(2) {
        request(&mut peer, id, false).await;
        peer.send(&h2_peer::rst_stream(id, code::CANCEL)).await;
        let frames = peer.settled().await;
        assert_eq!(goaway_in(&frames), None, "closed at stream {id}");
        while let Ok(accepted) = server.accepted.try_recv() {
            handed_over += 1;
            drop(accepted);
        }
    }
    assert_eq!(
        handed_over, 100,
        "every reset stream was handed to the application"
    );
}

/// Local error resets: each malformed request is refused with a stream reset, and past
/// `max_local_error_reset_streams` the connection is closed with ENHANCE_YOUR_CALM.
#[tokio::test]
async fn resets_a_client_provokes_are_bounded() {
    let mut builder = server::Builder::new();
    builder.max_local_error_reset_streams(Some(3));
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let _server = serve(connection, Serve::Answer);
    let closed = goaway_after(&mut peer, 20, async |peer, attempt| {
        // No `:path`: a malformed request, a stream error (RFC 9113 §8.3.1).
        let block = h2_peer::block(&[
            (":method", "GET"),
            (":scheme", "http"),
            (":authority", "example.com"),
        ]);
        peer.send(&h2_peer::headers(attempt * 2 - 1, block, true))
            .await;
    })
    .await;
    assert_eq!(closed, Some((4, code::ENHANCE_YOUR_CALM)));
}

/// 15 §3: "A check after decoding does not bound the allocation before that check." h2
/// stops keeping fields past `max_header_list_size` and answers 431 itself, without the
/// application seeing the request, and the connection carries on. It goes on decoding,
/// though, and only past four times the limit does it close the connection.
#[tokio::test]
async fn a_header_list_past_its_limit_is_answered_431_and_far_past_it_closes() {
    let mut builder = server::Builder::new();
    builder.max_header_list_size(4096);
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let mut server = serve(connection, Serve::Hold);

    let large = "a".repeat(5000);
    let mut fields = vec![
        (":method", "GET"),
        (":scheme", "http"),
        (":authority", "example.com"),
        (":path", "/"),
    ];
    fields.push(("x-large", &large));
    peer.send(&h2_peer::headers(1, h2_peer::block(&fields), true))
        .await;
    let (refused, _) = peer.until(|f| f.kind == kind::HEADERS).await;
    assert_eq!(refused.stream, 1);
    // The answer is h2's own 431 (`recv.rs`, `is_over_size`); its status is Huffman-coded,
    // which this peer does not decode.
    assert!(refused.has(flag::END_STREAM));
    request(&mut peer, 3, true).await;
    let (next, _) = server.next().await;
    assert_eq!(next.uri().path(), "/", "the next request is handed over");

    // 400 small fields: under 16 KiB on the wire, twice as much by the list's measure
    // (RFC 9113 §6.5.2 counts 32 bytes a field).
    let names: Vec<String> = (0..400).map(|n| format!("x-{n:04}")).collect();
    let mut many = fields[..4].to_vec();
    many.extend(names.iter().map(|n| (n.as_str(), "vvvvvvvvvvvvvvvvvvvv")));
    let block = h2_peer::block(&many);
    assert!(block.len() < 16_384, "fits one frame: {}", block.len());
    peer.send(&h2_peer::headers(5, block, true)).await;
    let (goaway, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
    assert_eq!(goaway.goaway().1, code::ENHANCE_YOUR_CALM);
}

/// CONTINUATION without end: the count of frames in one header block is bounded — here
/// five, the floor, since the list limit is under one frame — and one more closes the
/// connection. The frames go back to back: nothing, not even a PING, may come between the
/// pieces of a header block.
#[tokio::test]
async fn continuation_frames_are_bounded() {
    for (continued, closes) in [(5, false), (6, true)] {
        let mut builder = server::Builder::new();
        builder.max_header_list_size(4096);
        let (connection, mut peer, _) = server_with(&builder, true).await;
        let _server = serve(connection, Serve::Answer);
        // The request's block in pieces: one in the HEADERS frame, the others in each
        // CONTINUATION that does not end the block, the rest in the one that does.
        let block = h2_peer::request("GET", "/");
        let (first, rest) = block.split_at(4);
        peer.send(&Frame::new(
            kind::HEADERS,
            flag::END_STREAM,
            1,
            first.to_vec(),
        ))
        .await;
        for byte in &rest[..continued] {
            peer.send_if_open(&Frame::new(kind::CONTINUATION, 0, 1, vec![*byte]))
                .await;
        }
        let last = rest[continued..].to_vec();
        peer.send_if_open(&Frame::new(kind::CONTINUATION, flag::END_HEADERS, 1, last))
            .await;
        let frames = peer.settled().await;
        if closes {
            assert_eq!(
                goaway_in(&frames).map(|(_, code)| code),
                Some(code::ENHANCE_YOUR_CALM)
            );
        } else {
            assert_eq!(goaway_in(&frames), None);
            assert_eq!(headers_on(&frames), vec![1], "answered");
        }
    }
}

/// Empty DATA frames that do not end a stream are limited to 100 for the whole connection;
/// an empty frame that ends a stream is not counted, so a client that ends every request
/// that way is not cut off after a hundred of them.
#[tokio::test]
async fn empty_data_frames_are_limited_unless_they_end_a_stream() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let _server = serve(connection, Serve::Answer);
    for id in (1..=299).step_by(2) {
        request(&mut peer, id, false).await;
        peer.send(&h2_peer::data(id, b"", true)).await;
    }
    assert_eq!(
        goaway_in(&peer.settled().await),
        None,
        "150 empty final frames"
    );

    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let _server = serve(connection, Serve::Hold);
    request(&mut peer, 1, false).await;
    let closed = goaway_after(&mut peer, 150, async |peer, _| {
        peer.send(&h2_peer::data(1, b"", false)).await;
    })
    .await;
    assert_eq!(closed, Some((101, code::ENHANCE_YOUR_CALM)));
}

/// 15 §3: "A DATA budget is not a general CPU-per-poll budget." It bounds what small DATA
/// frames cost to hold: each frame under 256 bytes spends 256 less its length until the
/// application takes it, and past the budget the connection closes.
#[tokio::test]
async fn small_data_frames_spend_a_budget() {
    let mut builder = server::Builder::new();
    builder.data_frame_budget(2550);
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let _server = serve(connection, Serve::Hold);
    request(&mut peer, 1, false).await;
    let closed = goaway_after(&mut peer, 50, async |peer, _| {
        peer.send(&h2_peer::data(1, b"x", false)).await;
    })
    .await;
    // 255 each: ten fit in 2,550 exactly, the eleventh does not.
    assert_eq!(closed, Some((11, code::ENHANCE_YOUR_CALM)));
}

/// Before the client acknowledges the server's SETTINGS, its streams have the protocol's
/// default window of 65,535, whatever smaller one the server announced: a stream window
/// below 65,535 bounds nothing until the acknowledgement.
#[tokio::test]
async fn a_smaller_stream_window_applies_only_once_acknowledged() {
    let mut builder = server::Builder::new();
    builder
        .initial_window_size(16_384)
        .initial_connection_window_size(1 << 20);
    let (connection, mut peer, _) = server_with(&builder, false).await;
    let _server = serve(connection, Serve::Hold);
    request(&mut peer, 1, false).await;
    for _ in 0..3 {
        peer.send(&h2_peer::data(1, &[0u8; 16_384], false)).await;
    }
    peer.send(&h2_peer::data(1, &[0u8; 16_383], false)).await;
    let frames = peer.settled().await;
    assert!(resets_in(&frames).is_empty(), "65,535 accepted: {frames:?}");

    peer.send(&h2_peer::settings_ack()).await;
    request(&mut peer, 3, false).await;
    peer.send(&h2_peer::data(3, &[0u8; 16_384], false)).await;
    assert!(resets_in(&peer.settled().await).is_empty());
    peer.send(&h2_peer::data(3, &[0u8; 1], false)).await;
    assert_eq!(
        resets_in(&peer.settled().await),
        vec![(3, code::FLOW_CONTROL_ERROR)]
    );
}

/// Control frames are answered one at a time: h2 holds one pending PING acknowledgement
/// and one pending SETTINGS acknowledgement, and stops reading until it has written them.
/// A client that floods and never reads is held back by its own socket, not queued for.
#[tokio::test]
async fn ping_and_settings_floods_are_held_back_by_the_socket_not_queued() {
    for flood in [h2_peer::ping(*b"floodfld"), h2_peer::settings(&[])] {
        let (near, far) = tokio::io::duplex(4096);
        let client = Peer::open_as_client(far, &[]).await;
        let connection = within(server::Builder::new().handshake::<_, Bytes>(near))
            .await
            .unwrap();
        let _server = serve(connection, Serve::Answer);
        let (mut reading, mut writing) = tokio::io::split(client.into_inner());
        let frame = flood.encode();
        let flooding = tokio::spawn(async move {
            for _ in 0..10_000 {
                tokio::io::AsyncWriteExt::write_all(&mut writing, &frame).await?;
            }
            Ok::<_, std::io::Error>(())
        });
        tokio::time::sleep(QUIET).await;
        assert!(!flooding.is_finished(), "the server read all it was sent");

        // Once the client reads, every one is answered.
        let mut answered = 0;
        while answered < 10_000 {
            let frame = h2_peer::read_frame(&mut reading).await.expect("an answer");
            if frame.kind == flood.kind && frame.has(flag::ACK) {
                answered += 1;
            }
        }
        within(flooding).await.unwrap().unwrap();
    }
}

/// 15 §3: "`SendResponse::send_informational` provides downstream 1xx." Interim heads go
/// out in order before the final head, carrying their fields; a status that is not 1xx is
/// refused.
#[tokio::test]
async fn a_server_sends_informational_heads_before_the_final_one() {
    let (near, far) = wire();
    let (refused_tx, refused) = oneshot::channel();
    tokio::spawn(async move {
        let mut connection = server::Builder::new()
            .handshake::<_, Bytes>(near)
            .await
            .unwrap();
        let (_request, mut respond) = connection.accept().await.unwrap().unwrap();
        let not_interim = respond.send_informational(Response::new(()));
        let hint = Response::builder()
            .status(StatusCode::EARLY_HINTS)
            .header("link", "</a.css>; rel=preload")
            .body(())
            .unwrap();
        respond.send_informational(hint).unwrap();
        respond
            .send_informational(Response::builder().status(100).body(()).unwrap())
            .unwrap();
        respond.send_response(Response::new(()), true).unwrap();
        let _ = refused_tx.send(not_interim.is_err());
        let _ = poll_fn(|cx| connection.poll_closed(cx)).await;
    });

    let (mut send, connection) = client::handshake(far).await.unwrap();
    tokio::spawn(connection);
    let mut response = open(&mut send);
    let mut interim = Vec::new();
    while let Some(head) = within(poll_fn(|cx| response.poll_informational(cx))).await {
        let head = head.unwrap();
        interim.push((head.status().as_u16(), head.headers().get("link").cloned()));
    }
    assert_eq!(
        interim,
        vec![
            (103, Some("</a.css>; rel=preload".parse().unwrap())),
            (100, None)
        ]
    );
    assert_eq!(within(response).await.unwrap().status(), StatusCode::OK);
    assert!(within(refused).await.unwrap(), "a 200 as an interim head");
}

/// A library defect, and so a rule for our writer: after the final head has ended the
/// stream, `send_informational` still succeeds, and puts a HEADERS frame on the closed
/// stream — a frame a client must treat as a stream error (RFC 9113 §5.1). The response
/// writer must never send an interim head once the final one is out.
#[tokio::test]
async fn an_interim_head_after_the_final_one_is_sent_on_a_closed_stream() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let mut server = serve(connection, Serve::Hold);
    request(&mut peer, 1, true).await;
    let (_, mut respond) = server.next().await;
    respond.send_response(Response::new(()), true).unwrap();
    let late = Response::builder().status(103).body(()).unwrap();
    assert!(
        respond.send_informational(late).is_ok(),
        "h2 refuses it now"
    );
    let heads: Vec<_> = peer
        .settled()
        .await
        .into_iter()
        .filter(|f| f.kind == kind::HEADERS)
        .map(|f| (f.stream, f.has(flag::END_STREAM)))
        .collect();
    assert_eq!(
        heads,
        vec![(1, true), (1, false)],
        "a head after the stream's end"
    );
}

/// Graceful shutdown: a GOAWAY naming the largest identifier and a PING, then, once the
/// PING is answered, a GOAWAY naming the last stream it took. Streams up to that one run to
/// their end; a stream opened after it is never handed over; then the connection closes.
#[tokio::test]
async fn graceful_shutdown_announces_takes_what_was_in_flight_and_closes() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let mut server = serve(connection, Serve::Hold);
    request(&mut peer, 1, true).await;
    let (_, mut first) = server.next().await;
    server.shut_down();
    let (announced, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
    assert_eq!(announced.goaway(), (0x7fff_ffff, code::NO_ERROR));
    let (ping, _) = peer
        .until(|f| f.kind == kind::PING && !f.has(flag::ACK))
        .await;

    // In flight before the client saw the announcement.
    request(&mut peer, 3, true).await;
    peer.send(&Frame::new(kind::PING, flag::ACK, 0, ping.payload.clone()))
        .await;
    let (last, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
    assert_eq!(last.goaway(), (3, code::NO_ERROR));
    request(&mut peer, 5, true).await;
    let (_, mut third) = server.next().await;

    first.send_response(Response::new(()), true).unwrap();
    third.send_response(Response::new(()), true).unwrap();
    let rest = peer.rest().await;
    assert_eq!(headers_on(&rest), vec![1, 3], "{rest:?}");
    assert!(
        server.accepted.try_recv().is_err(),
        "stream 5 was handed over"
    );
    within(server.task).await.unwrap().unwrap();
}

/// Locally reset streams are remembered, up to `max_concurrent_reset_streams` and for
/// `reset_stream_duration`, so that frames still in flight for them are dropped quietly.
/// The count keeps the *first* resets, not the newest: past it a reset stream is forgotten
/// at once, and a late frame for it draws RST_STREAM(STREAM_CLOSED) — a stream error, not
/// the connection error h2's documentation describes. Either way the state held is bounded
/// by the count.
#[tokio::test]
async fn locally_reset_streams_are_remembered_up_to_a_count() {
    let mut builder = server::Builder::new();
    builder
        .max_concurrent_reset_streams(2)
        .reset_stream_duration(Duration::from_secs(60));
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let mut server = serve(connection, Serve::Hold);
    let mut held = Vec::new();
    for id in [1, 3, 5] {
        request(&mut peer, id, false).await;
        held.push(server.next().await);
    }
    for (_, respond) in &mut held {
        respond.send_reset(Reason::CANCEL);
    }
    assert_eq!(
        resets_in(&peer.settled().await),
        vec![(1, code::CANCEL), (3, code::CANCEL), (5, code::CANCEL)]
    );

    let mut late = Vec::new();
    for id in [5, 3, 1] {
        peer.send(&h2_peer::data(id, b"late", false)).await;
        let frames = peer.settled().await;
        assert_eq!(goaway_in(&frames), None);
        late.extend(resets_in(&frames));
    }
    assert_eq!(
        late,
        vec![(5, code::STREAM_CLOSED)],
        "only the third was forgotten"
    );
}

/// A connection says how much DATA it has received and not given back as credit, over all
/// its streams: what it holds of uploads nobody has read, to the byte, and less once one
/// is read and released. Added by the vendored copy (15 §3).
#[tokio::test]
async fn a_server_connection_says_what_it_holds_of_what_was_sent() {
    let (near, far) = wire();
    let (client, server) = tokio::join!(
        client::handshake(far),
        server::Builder::new().handshake::<_, Bytes>(near)
    );
    let (send, connection) = client.unwrap();
    tokio::spawn(connection);
    let mut server = server.unwrap();
    let mut send = within(send.ready()).await.unwrap();
    let mut kept = Vec::new();
    for size in [10_000, 20_000] {
        let request = Request::post("http://example.com/up").body(()).unwrap();
        let (answer, mut body) = send.send_request(request, false).unwrap();
        body.send_data(Bytes::from(vec![1; size]), false).unwrap();
        kept.push((answer, body));
        send = within(send.ready()).await.unwrap();
    }
    // Driven, and nothing read: what was sent is held.
    let mut uploads = Vec::new();
    for _ in 0..100 {
        tokio::select! {
            accepted = server.accept() => {
                let (request, respond) = accepted.unwrap().unwrap();
                uploads.push((request.into_body(), respond));
            }
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        if uploads.len() == 2 && server.received_unreleased() == 30_000 {
            break;
        }
    }
    assert_eq!(server.received_unreleased(), 30_000);

    // One read and released: only the other is held.
    let (first, _) = uploads.first_mut().unwrap();
    let mut read = 0;
    while read < 10_000 {
        let data = within(first.data()).await.unwrap().unwrap();
        read += data.len();
        first.flow_control().release_capacity(data.len()).unwrap();
    }
    assert_eq!(server.received_unreleased(), 20_000);
}

/// EdgeRush's vendored h2 (`vendor/h2/VENDORED.md`, 14 §3): a server connection with nothing
/// in its buffers gives them back — what it reads frames into, writes them from and decodes
/// Huffman-coded strings in — and makes them again for the next request. That request is
/// served as though they had never gone: its header block refers to what the first put in
/// the HPACK table, and its body and its answer are larger than a read buffer.
#[tokio::test]
async fn a_server_connection_gives_back_its_buffers_and_serves_on() {
    const BODY: usize = 40_000;
    let (near, far) = wire();
    // Asked to give its buffers back, the server says what they held and hold.
    let (asked, mut asking) = mpsc::unbounded_channel::<()>();
    let (told, mut capacities) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut connection = server::Builder::new()
            .handshake::<_, Bytes>(near)
            .await
            .unwrap();
        loop {
            tokio::select! {
                // Accepting first: whatever the client sent is read before a release.
                biased;
                next = connection.accept() => {
                    let Some(Ok((request, mut respond))) = next else { return };
                    tokio::spawn(async move {
                        let long = request.headers()["x-long"].len();
                        let mut body = request.into_body();
                        let mut read = 0;
                        while let Some(data) = body.data().await {
                            let data = data.unwrap();
                            read += data.len();
                            let _ = body.flow_control().release_capacity(data.len());
                        }
                        let mut sending = respond.send_response(Response::new(()), false).unwrap();
                        let answer = format!("{long} {read} ").into_bytes();
                        let mut all = answer.clone();
                        all.resize(BODY, b'a');
                        sending.send_data(Bytes::from(all), true).unwrap();
                    });
                }
                command = asking.recv() => {
                    let Some(()) = command else { return };
                    let before = connection.buffer_capacity();
                    connection.release_buffers();
                    told.send((before, connection.buffer_capacity())).unwrap();
                }
            }
        }
    });

    let (mut send, connection) = client::handshake(far).await.unwrap();
    tokio::spawn(connection);
    // Joined, not repeated with a space after each: a value may not end with one
    // (RFC 9113 §8.2.1).
    let long = ["Huffman-coded where it is shorter"; 60].join(" ");
    for round in 0..2 {
        let request = Request::builder()
            .uri("http://example.com/up")
            .method("POST")
            .version(Version::HTTP_2)
            .header("x-long", long.as_str())
            .body(())
            .unwrap();
        let (response, mut body) = send.send_request(request, false).unwrap();
        body.send_data(Bytes::from(vec![b'u'; BODY]), true).unwrap();
        let mut answer = within(response).await.unwrap().into_body();
        let mut received = Vec::new();
        while let Some(data) = within(answer.data()).await {
            let data = data.unwrap();
            let _ = answer.flow_control().release_capacity(data.len());
            received.extend_from_slice(&data);
        }
        assert_eq!(received.len(), BODY, "round {round}");
        assert!(
            received.starts_with(format!("{} {BODY} ", long.len()).as_bytes()),
            "round {round}: {:?}",
            String::from_utf8_lossy(&received[..32])
        );

        asked.send(()).unwrap();
        let (before, after) = within(capacities.recv()).await.unwrap();
        // They were there, made again in the second round: the write buffer at its 16 KiB
        // and what is left of the others, which frames and strings are cut from.
        assert!(before > 16 * 1024, "round {round}: {before} before");
        assert_eq!(after, 0, "round {round}: {after} after, {before} before");
    }
}

/// And what is waiting in them is never given back with them: a server that asks to give
/// its buffers back every time it is polled, over a pipe so narrow that every read and write
/// is part of a frame, still carries a 40 KB upload, a 40 KB answer and header blocks split
/// across frames, both ways, byte for byte. An answer's body goes around the write buffer,
/// but its head goes through it.
#[tokio::test]
async fn a_server_connection_keeps_what_waits_in_its_buffers() {
    const BODY: usize = 40_000;
    let (near, far) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let mut connection = server::Builder::new()
            .handshake::<_, Bytes>(near)
            .await
            .unwrap();
        while let Some(Ok((request, mut respond))) = poll_fn(|cx| {
            connection.release_buffers();
            connection.poll_accept(cx)
        })
        .await
        {
            tokio::spawn(async move {
                let long = request.headers()["x-long"].clone();
                let mut body = request.into_body();
                let mut read = Vec::new();
                while let Some(data) = body.data().await {
                    let data = data.unwrap();
                    let _ = body.flow_control().release_capacity(data.len());
                    read.extend_from_slice(&data);
                }
                let mut head = Response::new(());
                head.headers_mut().insert("x-long", long);
                let mut sending = respond.send_response(head, false).unwrap();
                sending.send_data(Bytes::from(read), true).unwrap();
            });
        }
    });

    let (mut send, connection) = client::handshake(far).await.unwrap();
    tokio::spawn(connection);
    // Past a frame, so that the header block goes as HEADERS and CONTINUATION.
    let long: String = (0..20_000)
        .map(|at| char::from(b'a' + (at % 26) as u8))
        .collect();
    let upload: Vec<u8> = (0..BODY).map(|at| (at % 251) as u8).collect();
    let request = Request::builder()
        .uri("http://example.com/up")
        .method("POST")
        .version(Version::HTTP_2)
        .header("x-long", long.as_str())
        .body(())
        .unwrap();
    let (response, mut body) = send.send_request(request, false).unwrap();
    body.send_data(Bytes::from(upload.clone()), true).unwrap();
    let (head, mut answer) = within(response).await.unwrap().into_parts();
    assert!(
        head.headers["x-long"] == long.as_str(),
        "the answer's long header"
    );
    let mut received = Vec::new();
    while let Some(data) = within(answer.data()).await {
        let data = data.unwrap();
        let _ = answer.flow_control().release_capacity(data.len());
        received.extend_from_slice(&data);
    }
    assert!(received == upload, "{} bytes came back", received.len());
}

// ===== Malformed messages: refused by EdgeRush's copy =====

/// A GET's header block with one more field, `x-env: value`.
fn get_with(value: &str) -> Vec<u8> {
    h2_peer::block(&[
        (":method", "GET"),
        (":scheme", "http"),
        (":authority", "example.com"),
        (":path", "/"),
        ("x-env", value),
    ])
}

/// What `stream` was reset with, once the peer has seen it reset.
async fn reset_of(peer: &mut Peer<DuplexStream>, stream: u32) -> u32 {
    let (reset, _) = within(peer.until(|f| f.kind == kind::RST_STREAM && f.stream == stream)).await;
    reset.reset()
}

/// RFC 9113 §8.2.1: a field value that starts or ends with whitespace makes a message
/// malformed. h2 as published takes it, `HeaderValue` allowing SP and HTAB anywhere; this
/// copy resets the stream with PROTOCOL_ERROR before the application sees the request
/// (vendor/h2/VENDORED.md). An empty value, and whitespace inside one, are well formed.
#[tokio::test]
async fn a_request_field_value_with_whitespace_at_either_end_is_reset() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let mut server = serve(connection, Serve::Hold);
    for (stream, value) in [(1, " a"), (3, "a "), (5, "\ta"), (7, "a\t"), (9, " ")] {
        peer.send(&h2_peer::headers(stream, get_with(value), true))
            .await;
        assert_eq!(
            reset_of(&mut peer, stream).await,
            code::PROTOCOL_ERROR,
            "{value:?}"
        );
    }
    for (stream, value) in [(11, ""), (13, "a \tb")] {
        peer.send(&h2_peer::headers(stream, get_with(value), true))
            .await;
        let (request, _) = server.next().await;
        assert_eq!(request.headers()["x-env"], value);
    }
    assert!(
        server.accepted.try_recv().is_err(),
        "only the well-formed requests were handed over"
    );
}

/// Those resets are h2's own, so they count towards `max_local_error_reset_streams` as
/// any other malformed request does (15 §3's bound on resets a client provokes).
#[tokio::test]
async fn resets_for_whitespace_at_either_end_are_bounded() {
    let mut builder = server::Builder::new();
    builder.max_local_error_reset_streams(Some(3));
    let (connection, mut peer, _) = server_with(&builder, true).await;
    let _server = serve(connection, Serve::Answer);
    let closed = goaway_after(&mut peer, 20, async |peer, attempt| {
        peer.send(&h2_peer::headers(attempt * 2 - 1, get_with(" a"), true))
            .await;
    })
    .await;
    assert_eq!(closed, Some((4, code::ENHANCE_YOUR_CALM)));
}

/// A request's trailers are held to the same rule: the stream is reset, and the reading
/// application told so.
#[tokio::test]
async fn request_trailers_with_whitespace_at_either_end_are_reset() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let mut server = serve(connection, Serve::Hold);
    request(&mut peer, 1, false).await;
    let (request, _respond) = server.next().await;
    peer.send(&h2_peer::headers(
        1,
        h2_peer::block(&[("x-sum", "1 ")]),
        true,
    ))
    .await;
    assert_eq!(reset_of(&mut peer, 1).await, code::PROTOCOL_ERROR);
    let read = within(request.into_body().trailers()).await;
    assert_eq!(
        read.map_err(|error| error.reason()).err(),
        Some(Some(Reason::PROTOCOL_ERROR)),
        "the application reads a reset"
    );
}

/// The client half refuses such an answer (RFC 9113 §8.1.1: "Clients MUST NOT accept a
/// malformed response"), in its head and in its trailers.
#[tokio::test]
async fn an_answer_with_whitespace_at_either_end_of_a_field_value_is_reset() {
    let (mut client, mut peer) = client(&[]).await;
    let head = open(&mut client.send);
    opened(&mut peer, 1).await;
    peer.send(&h2_peer::headers(
        1,
        h2_peer::block(&[(":status", "200"), ("x-env", "\ta")]),
        true,
    ))
    .await;
    let refused = within(head).await.map(|answer| answer.status());
    assert_eq!(
        refused.map_err(|error| error.reason()).err(),
        Some(Some(Reason::PROTOCOL_ERROR))
    );
    assert_eq!(reset_of(&mut peer, 1).await, code::PROTOCOL_ERROR);

    let trailed = open(&mut client.send);
    opened(&mut peer, 3).await;
    peer.send(&h2_peer::headers(3, h2_peer::response(200), false))
        .await;
    peer.send(&h2_peer::headers(
        3,
        h2_peer::block(&[("grpc-message", "a ")]),
        true,
    ))
    .await;
    let mut body = within(trailed).await.unwrap().into_body();
    let read = within(body.trailers()).await;
    assert_eq!(
        read.map_err(|error| error.reason()).err(),
        Some(Some(Reason::PROTOCOL_ERROR))
    );
    assert_eq!(reset_of(&mut peer, 3).await, code::PROTOCOL_ERROR);
}

/// RFC 9113 §8.3.1: `:path` is the target's path and query; a fragment is no part of a
/// valid one. h2 as published builds the URI with `http`, which cuts a fragment off without
/// a word, so `/a#b` would be handed over as `/a`; this copy resets the stream with
/// PROTOCOL_ERROR, as any malformed request, and the application never sees it.
#[tokio::test]
async fn a_path_with_a_fragment_is_reset() {
    let (connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    let mut server = serve(connection, Serve::Hold);
    for (stream, path) in [(1, "/a#b"), (3, "/a?q=1#b"), (5, "/#")] {
        peer.send(&h2_peer::headers(
            stream,
            h2_peer::request("GET", path),
            true,
        ))
        .await;
        assert_eq!(
            reset_of(&mut peer, stream).await,
            code::PROTOCOL_ERROR,
            "{path:?}"
        );
    }
    peer.send(&h2_peer::headers(
        7,
        h2_peer::request("GET", "/a?q=1"),
        true,
    ))
    .await;
    let (request, _) = server.next().await;
    assert_eq!(request.uri().path_and_query().unwrap().as_str(), "/a?q=1");
    assert!(
        server.accepted.try_recv().is_err(),
        "only the well-formed request was handed over"
    );
}
