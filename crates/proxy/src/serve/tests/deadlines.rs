//! A client connection's deadlines, over HTTP/1.

use super::*;

/// A connection that has not finished its first request head is closed at its first
/// request deadline from being accepted, whether it never said anything, stalled part
/// way through the HTTP/2 preface — where protocol detection, the engine's or ours,
/// waits with no deadline of its own — or is trickling a head. Without this each of
/// them holds its connection for ever.
#[tokio::test]
async fn a_connection_without_a_first_request_is_closed_at_its_deadline() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            for said in [
                &b""[..],
                b"PRI * HT",
                // All of the preface but its last byte.
                &crate::downstream::detect::PREFACE[..23],
                b"GET / HTTP/1.1\r\nhost: exa",
            ] {
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream.write_all(said).await.unwrap();
                let took = closed_after(&mut stream).await;
                assert!(
                    took + EARLY >= SHORT.first_request && took < SHORT.first_request + SLACK,
                    "{:?}: closed after {took:?}",
                    String::from_utf8_lossy(said)
                );
            }
        })
        .await;
}

/// A worker started as the data plane starts one, serving and maintained, keeps its
/// deadlines: its maintenance is what waits on its timers, and without it a client
/// that has been answered and asks nothing more would be held for ever.
#[tokio::test]
async fn a_maintained_worker_keeps_its_deadlines() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream.write_all(ASKED).await.unwrap();
            answered(&mut stream).await;
            let took = closed_after(&mut stream).await;
            assert!(
                took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                "closed after {took:?}"
            );
        })
        .await;
}

/// After an answer, a connection waiting for its next request head — idle, or with
/// part of one — is closed at its next request deadline.
#[tokio::test]
async fn a_connection_waiting_for_its_next_request_is_closed_at_its_deadline() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            for then in [&b""[..], b"GET / HT"] {
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream.write_all(ASKED).await.unwrap();
                answered(&mut stream).await;
                stream.write_all(then).await.unwrap();
                let took = closed_after(&mut stream).await;
                assert!(
                    took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                    "{:?}: closed after {took:?}",
                    String::from_utf8_lossy(then)
                );
            }
        })
        .await;
}

/// The deadlines cut off nobody who is keeping to them: a first head sent just inside
/// its deadline is answered, and so is another request well inside the next.
#[tokio::test]
async fn a_connection_that_keeps_to_the_deadlines_is_served() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut stream = TcpStream::connect(front).await.unwrap();
            tokio::time::sleep(SHORT.first_request - Duration::from_millis(150)).await;
            stream.write_all(ASKED).await.unwrap();
            assert!(answered(&mut stream).await.starts_with("HTTP/1.1 200"));
            tokio::time::sleep(SHORT.next_request - Duration::from_millis(250)).await;
            stream.write_all(ASKED).await.unwrap();
            assert!(answered(&mut stream).await.starts_with("HTTP/1.1 200"));
        })
        .await;
}
