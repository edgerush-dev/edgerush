//! A worker that drains.

use super::*;

/// A draining worker closes an HTTP/1 connection waiting idle for its next request at
/// once, and takes no new connection (03 §10).
#[tokio::test]
async fn a_draining_worker_closes_idle_http1_connections_and_accepts_nothing() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream.write_all(ASKED).await.unwrap();
            answered(&mut stream).await;
            worker.drain();
            let took = closed_after(&mut stream).await;
            assert!(took < SLACK, "closed {took:?} after the drain began");

            // Nothing new is taken: the listening socket went with the drain, so a
            // connection is refused.
            tokio::task::yield_now().await;
            let late = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(front))
                .await
                .expect("connecting hung");
            assert!(late.is_err(), "a draining worker still listens");
        })
        .await;
}

/// An HTTP/1 request under way when the worker drains is answered, saying the
/// connection closes, and then it does.
#[tokio::test]
async fn an_http1_answer_in_hand_while_draining_says_close() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream
                .write_all(b"GET /held HTTP/1.1\r\nhost: example.test\r\n\r\n")
                .await
                .unwrap();
            until(|| held.borrow().len() == 1).await;
            worker.drain();
            // The upstream goes; the proxy answers for it.
            held.borrow_mut().clear();
            let mut answer = Vec::new();
            tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut answer))
                .await
                .expect("never closed")
                .unwrap();
            let answer = String::from_utf8_lossy(&answer).to_lowercase();
            assert!(answer.starts_with("http/1.1 502"), "{answer}");
            assert!(answer.contains("connection: close\r\n"), "{answer}");
        })
        .await;
}

/// An HTTP/2 connection with a stream under way when the worker drains is told to go,
/// gracefully; the stream is still answered, and then the connection closes.
#[tokio::test]
async fn a_draining_http2_connection_finishes_its_streams_then_closes() {
    use crate::h2_peer::{Frame, code, flag, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            let mut peer = h2_client(front).await;
            h2_get(&mut peer, 1, "/held").await;
            until(|| held.borrow().len() == 1).await;
            worker.drain();
            let (announced, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
            assert_eq!(announced.goaway(), (0x7fff_ffff, code::NO_ERROR));
            let (ping, _) = peer
                .until(|f| f.kind == kind::PING && !f.has(flag::ACK))
                .await;
            peer.send(&Frame::new(kind::PING, flag::ACK, 0, ping.payload.clone()))
                .await;
            let (last, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
            assert_eq!(last.goaway(), (1, code::NO_ERROR));

            held.borrow_mut().clear();
            let rest = peer.rest().await;
            assert!(
                rest.iter()
                    .any(|f| f.stream == 1 && f.has(flag::END_STREAM)),
                "stream 1 was not answered: {rest:?}"
            );
        })
        .await;
}

/// A stream that outlasts the drain's time is not waited for: the connection is closed
/// at the drain's bound.
#[tokio::test]
async fn a_draining_http2_connection_is_closed_at_the_drain_bound() {
    use crate::h2_peer::{Frame, flag, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let (front, worker) = serving_worker_and(upstream).await;
            let mut peer = h2_client(front).await;
            h2_get(&mut peer, 1, "/held").await;
            peer.settled().await;
            worker.drain();
            let began = tokio::time::Instant::now();
            let (ping, _) = peer
                .until(|f| f.kind == kind::PING && !f.has(flag::ACK))
                .await;
            peer.send(&Frame::new(kind::PING, flag::ACK, 0, ping.payload.clone()))
                .await;
            let (_, rest) = peer
                .until(|f| f.kind == kind::GOAWAY && f.goaway().0 == 1)
                .await;
            assert!(rest.iter().all(|f| f.kind != kind::RST_STREAM));
            // Closed at the bound, not after waiting out the time a closing connection
            // is given to flush.
            let _closed = peer.rest().await;
            let took = began.elapsed();
            assert!(
                took + EARLY >= SHORT.drain && took < SHORT.drain + SLACK,
                "closed {took:?} after the drain began"
            );
        })
        .await;
}

/// The data plane's drain reaches a worker at its next sweep.
#[tokio::test]
async fn the_data_planes_drain_reaches_a_worker_at_its_sweep() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            // A sweep far shorter than the time an idle connection is kept, so that it is
            // the drain that closes it.
            let limits = H1Limits {
                sweep: Duration::from_millis(50),
                ..H1Limits::default()
            };
            let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
            let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream.write_all(ASKED).await.unwrap();
            answered(&mut stream).await;
            worker.proxy.drain();
            let took = closed_after(&mut stream).await;
            assert!(
                took < worker.limits.sweep + SLACK && took < SHORT.next_request,
                "closed {took:?} after the data plane began to drain"
            );
        })
        .await;
}
