//! Cooperative deadlines are tested with a monotonic clock independent of Tokio time.

use super::*;
use std::cell::Cell;
use std::rc::Rc;
use std::task::Waker;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

thread_local! {
    static NOW: Cell<Instant> = Cell::new(Instant::now());
    static TICK: Cell<Duration> = const { Cell::new(Duration::ZERO) };
}

fn clock() -> Instant {
    NOW.with(|now| {
        let value = now.get();
        now.set(value + TICK.get());
        value
    })
}

fn budget(ticks: u64, advance: bool) -> h2::TurnBudget {
    TICK.set(if advance {
        Duration::from_micros(1)
    } else {
        Duration::ZERO
    });
    h2::TurnBudget::with_clock(NOW.get() + Duration::from_micros(ticks), clock)
}

#[test]
fn a_turn_copy_keeps_its_absolute_deadline() {
    let turn = budget(2, false);
    let copy = turn;
    assert!(!turn.expired());
    NOW.set(NOW.get() + Duration::from_micros(2));
    assert!(turn.expired(), "equality must exhaust the turn");
    assert!(copy.expired(), "copying must not renew it");
    assert!(h2::TurnBudget::new(Instant::now()).expired());
}

#[tokio::test]
async fn an_exhausted_turn_stays_exhausted_across_accept_send_and_close_polls() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    request(&mut peer, 1, true).await;
    processed(&mut connection, &mut peer).await; // A head already waits for acceptance.
    connection.set_turn(Some(budget(0, false)));
    let counted = std::sync::Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(std::sync::Arc::clone(&counted));
    let mut cx = Context::from_waker(&waker);
    assert!(connection.poll_accept(&mut cx).is_pending());
    assert!(connection.turn_exhausted());
    assert!(connection.poll_send(&mut cx).is_pending());
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert!(counted.0.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    connection.set_turn(Some(budget(100, false)));
    assert!(!connection.turn_exhausted());
    let (request, mut send) = within(connection.accept()).await.unwrap().unwrap();
    assert_eq!(request.uri().path(), "/");
    send.send_response(Response::new(()), true).unwrap();
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    assert!(
        !connection.turn_exhausted(),
        "I/O pending is not budget exhaustion"
    );
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert!(!connection.turn_exhausted());
}

#[tokio::test]
async fn a_new_turn_does_not_replenish_decode_units_or_block_send_only() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    request(&mut peer, 1, true).await;
    let (_, mut send) = within(connection.accept()).await.unwrap().unwrap();
    send.send_response(Response::new(()), true).unwrap();
    connection.set_work(0);
    connection.set_turn(Some(budget(100, false)));
    peer.send(&h2_peer::ping([7; 8])).await;
    let mut cx = Context::from_waker(Waker::noop());
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(connection.work_left(), 0);
    assert!(
        !connection.turn_exhausted(),
        "decode allowance is distinct from time"
    );
    // An owner can give output a turn without granting any additional receive work.
    connection.set_turn(Some(budget(100, false)));
    assert_eq!(connection.work_left(), 0);
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    let frames = peer.drain_for(QUIET).await;
    assert_eq!(frames.iter().filter(|f| f.kind == kind::HEADERS).count(), 1);
    assert_eq!(acks_in(&frames), 0);
    connection.set_work(1);
    assert!(connection.poll_closed(&mut cx).is_pending());
    assert_eq!(connection.work_left(), 0);
    assert_eq!(acks_in(&peer.drain_for(QUIET).await), 1);
}

#[tokio::test]
async fn queued_requests_can_be_taken_without_spending_the_turn_on_more_input() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    request(&mut peer, 1, true).await;
    processed(&mut connection, &mut peer).await;
    for n in 0..100u8 {
        peer.send(&h2_peer::ping([n; 8])).await;
    }
    connection.set_work(1000);
    let mut cx = Context::from_waker(Waker::noop());
    connection.set_turn(Some(budget(0, false)));
    assert!(connection.poll_accept_pending(&mut cx).is_pending());
    assert!(connection.turn_exhausted());
    connection.set_turn(Some(budget(2, true)));
    let Poll::Ready(Some((head, _send))) = connection.poll_accept_pending(&mut cx) else {
        panic!("queued head was not handed over within the fresh turn");
    };
    assert_eq!(head.uri().path(), "/");
    assert_eq!(connection.work_left(), 1000, "admission consumed input");
    assert!(matches!(
        connection.poll_accept_pending(&mut cx),
        Poll::Ready(None)
    ));
    assert!(
        connection.poll_accept_pending(&mut cx).is_pending(),
        "admission renewed the deadline"
    );
    connection.set_turn(None);
    assert_eq!(acks_in(&processed(&mut connection, &mut peer).await), 100);
}

#[tokio::test]
async fn a_turn_ends_between_buffered_continuations_and_resumes_the_same_block() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    let value = "z".repeat(2000);
    let mut block = h2_peer::request("GET", "/");
    block.extend(h2_peer::block(&[("x-split", &value)]));
    let parts: Vec<_> = block.chunks(64).collect();
    for (n, part) in parts.iter().enumerate() {
        peer.send(&Frame::new(
            if n == 0 {
                kind::HEADERS
            } else {
                kind::CONTINUATION
            },
            if n == 0 {
                flag::END_STREAM
            } else if n + 1 == parts.len() {
                flag::END_HEADERS
            } else {
                0
            },
            1,
            part.to_vec(),
        ))
        .await;
    }
    connection.set_work(1000);
    connection.set_turn(Some(budget(12, true)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(connection.poll_accept(&mut cx).is_pending());
    assert!(connection.turn_exhausted());
    let read = 1000 - connection.work_left();
    assert!(
        read > 0 && read < parts.len(),
        "decoded {read} of {} frames",
        parts.len()
    );
    let remaining = connection.work_left();
    assert!(connection.poll_accept(&mut cx).is_pending());
    assert_eq!(connection.work_left(), remaining);
    connection.set_turn(None);
    let (request, _send) = within(connection.accept()).await.unwrap().unwrap();
    assert_eq!(request.headers()["x-split"], value);
    assert!(!connection.turn_exhausted());
}

#[tokio::test]
async fn send_only_progresses_with_input_waiting_and_stops_staging_at_the_deadline() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    for id in (1..=199).step_by(2) {
        request(&mut peer, id, true).await;
    }
    let mut sends = Vec::new();
    for _ in 0..100 {
        let (_, send) = within(connection.accept()).await.unwrap().unwrap();
        sends.push(send);
    }
    for send in &mut sends {
        send.send_response(Response::new(()), true).unwrap();
    }
    for n in 0..100u8 {
        peer.send(&h2_peer::ping([n; 8])).await;
    }
    connection.set_work(1000);
    connection.set_turn(Some(budget(20, true)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        connection.poll_send(&mut cx).is_pending(),
        "staged every response in one turn"
    );
    assert!(connection.turn_exhausted());
    assert_eq!(connection.work_left(), 1000, "send-only consumed input");
    // Expiry before flushing must retain every staged frame. No second budget is granted
    // inside h2; the owner now starts another turn, with input still continuously ready.
    connection.set_turn(Some(budget(1000, false)));
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    assert_eq!(connection.work_left(), 1000);
    let frames = peer.drain_for(QUIET).await;
    let heads: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == kind::HEADERS)
        .map(|f| f.stream)
        .collect();
    assert_eq!(heads, (1..=199).step_by(2).collect::<Vec<_>>());
    assert_eq!(acks_in(&frames), 0);
    connection.set_turn(None);
    let frames = processed(&mut connection, &mut peer).await;
    assert_eq!(acks_in(&frames), 100); // The barrier ACK itself is excluded by the peer.
}

#[tokio::test]
async fn cancelled_send_entries_cannot_bypass_the_turn_checkpoint() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    for id in (1..=63).step_by(2) {
        request(&mut peer, id, true).await;
    }
    let mut sends = Vec::new();
    for _ in 0..32 {
        let (_, send) = within(connection.accept()).await.unwrap().unwrap();
        sends.push(send);
    }
    for (n, send) in sends.iter_mut().enumerate() {
        send.send_response(Response::new(()), true).unwrap();
        peer.send(&h2_peer::rst_stream(1 + n as u32 * 2, code::CANCEL))
            .await;
    }
    let mut cx = Context::from_waker(Waker::noop());
    connection.set_work(1000);
    for n in 1..=32 {
        // Receive one reset and exhaust before the send phase can clean its queue.
        connection.set_turn(Some(budget(2, true)));
        assert!(connection.poll_closed(&mut cx).is_pending());
        assert!(connection.turn_exhausted());
        assert_eq!(connection.work_left(), 1000 - n);
    }
    connection.set_turn(Some(budget(12, true)));
    assert!(
        connection.poll_send(&mut cx).is_pending(),
        "skipped all cancelled entries in one turn"
    );
    assert!(connection.turn_exhausted());
    connection.set_turn(None);
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    assert!(
        peer.drain_for(QUIET).await.is_empty(),
        "sent cancelled responses or echoed resets"
    );
}

/// A transport whose short writes advance the injected clock. This makes stopping
/// within a single encoded frame observable, rather than just between calls to h2.
struct ShortWrites {
    io: DuplexStream,
    short: Rc<Cell<bool>>,
    written: Rc<Cell<usize>>,
}

impl AsyncRead for ShortWrites {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for ShortWrites {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let bytes = if self.short.get() {
            &bytes[..bytes.len().min(3)]
        } else {
            bytes
        };
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            self.written.set(self.written.get() + n);
            if self.short.get() {
                NOW.set(NOW.get() + Duration::from_micros(1));
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

async fn short_server() -> (
    server::Connection<ShortWrites, Bytes>,
    Peer<DuplexStream>,
    Rc<Cell<bool>>,
    Rc<Cell<usize>>,
) {
    let (near, far) = wire();
    let short = Rc::new(Cell::new(false));
    let written = Rc::new(Cell::new(0));
    let io = ShortWrites {
        io: near,
        short: Rc::clone(&short),
        written: Rc::clone(&written),
    };
    let mut peer = Peer::open_as_client(far, &[]).await;
    let connection = within(server::handshake(io)).await.unwrap();
    assert_eq!(peer.next().await.kind, kind::SETTINGS);
    peer.send(&h2_peer::settings_ack()).await;
    (connection, peer, short, written)
}

#[tokio::test]
async fn a_turn_stops_partial_writes_and_resumes_without_losing_data() {
    let (mut connection, mut peer, short, written) = short_server().await;
    request(&mut peer, 1, true).await;
    let (_, mut send) = within(connection.accept()).await.unwrap().unwrap();
    let mut body = send.send_response(Response::new(()), false).unwrap();
    let data = Bytes::from(vec![123; 8192]);
    body.send_data(data.clone(), true).unwrap();
    short.set(true);
    let before = written.get();
    connection.set_turn(Some(budget(2, false)));
    assert!(
        connection
            .poll_send(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(connection.turn_exhausted());
    assert_eq!(
        written.get() - before,
        6,
        "more than two short writes in the turn"
    );
    connection.set_turn(None);
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    let frames = peer.drain_for(QUIET).await;
    let received: Vec<_> = frames
        .iter()
        .filter(|f| f.kind == kind::DATA)
        .flat_map(|f| f.payload.iter().copied())
        .collect();
    assert_eq!(received, data);
    assert_eq!(frames.iter().filter(|f| f.kind == kind::HEADERS).count(), 1);
    assert!(
        frames
            .iter()
            .any(|f| f.kind == kind::DATA && f.has(flag::END_STREAM))
    );
}

#[tokio::test]
async fn closing_keeps_a_partial_goaway_until_the_next_turn() {
    let (mut connection, mut peer, short, written) = short_server().await;
    // Consume the ACK before closing, with no budget installed.
    assert!(
        connection
            .poll_closed(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    connection.abrupt_shutdown(Reason::ENHANCE_YOUR_CALM);
    short.set(true);
    let before = written.get();
    connection.set_turn(Some(budget(2, false)));
    assert!(
        connection
            .poll_closed(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(connection.turn_exhausted());
    assert_eq!(written.get() - before, 6);
    connection.set_turn(Some(budget(100, false)));
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    let frames = peer.drain_for(QUIET).await;
    assert_eq!(frames.iter().filter(|f| f.kind == kind::GOAWAY).count(), 1);
    assert_eq!(
        goaway_in(&frames).map(|(_, reason)| reason),
        Some(code::ENHANCE_YOUR_CALM)
    );
}

#[tokio::test]
async fn corked_output_waits_for_the_turns_end_and_then_leaves_whatever_the_deadline() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    request(&mut peer, 1, true).await;
    let (_, mut send) = within(connection.accept()).await.unwrap().unwrap();
    connection.set_corked(true);
    send.send_response(Response::new(()), true).unwrap();
    connection.set_turn(Some(budget(100, false)));
    within(poll_fn(|cx| connection.poll_send(cx)))
        .await
        .unwrap();
    let frames = peer.drain_for(QUIET).await;
    assert_eq!(
        frames.iter().filter(|f| f.kind == kind::HEADERS).count(),
        0,
        "a corked answer left before the turn's end"
    );
    // The turn is over, and so is its deadline: what was encoded leaves all the same.
    connection.set_turn(Some(budget(0, false)));
    within(poll_fn(|cx| connection.poll_flush_encoded(cx)))
        .await
        .unwrap();
    let frames = peer.drain_for(QUIET).await;
    assert_eq!(frames.iter().filter(|f| f.kind == kind::HEADERS).count(), 1);
}

#[tokio::test]
async fn a_corked_connection_closing_still_says_goaway() {
    let (mut connection, mut peer, _) = server_with(&server::Builder::new(), true).await;
    processed(&mut connection, &mut peer).await;
    connection.set_corked(true);
    connection.abrupt_shutdown(Reason::ENHANCE_YOUR_CALM);
    let _closed = within(poll_fn(|cx| connection.poll_closed(cx))).await;
    let frames = peer.drain_for(QUIET).await;
    assert_eq!(
        goaway_in(&frames).map(|(_, reason)| reason),
        Some(code::ENHANCE_YOUR_CALM)
    );
}
