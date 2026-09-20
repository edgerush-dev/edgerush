//! What the proxy puts on the wire, byte for byte, on both sides.
//!
//! The upstream here is not a server library but a socket the test writes to: that is the
//! only way to see what the engine really sends and to answer it with things a library
//! would not say — an early final response, an interim head, a body the connection's close
//! delimits. The client side is raw for the same reason.
//!
//! These tests are a **characterisation** of the path as it is today, hyper's client
//! included. They are written before that client is replaced, so that "the same as before"
//! is a measured thing and not an assumption ([13 §8](../../../docs/13-http1-upstream.md)).
//! Where the behaviour is hyper's own and not a promise EdgeRush makes, the test says so.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_config::{Config, compile};
use edgerush_proxy::{Proxy, Upstream, Worker};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// A test that waits for what never comes should fail, not hang.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

/// Starts a proxy whose one listener sends everything to `upstream`, and says where it
/// listens. It is served the way a data plane serves: a worker on a thread of its own.
async fn proxy_to(upstream: SocketAddr) -> SocketAddr {
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends: [{{ upstream: up, weight: 1 }}]
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(
        Proxy::new(
            compile(&config).unwrap(),
            NonZeroUsize::MIN,
            upstream_under_test(),
        )
        .unwrap(),
    );
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let entered = runtime.enter();
        let socket = TcpListener::from_std(socket).unwrap();
        let worker = Worker::new(proxy);
        local.spawn_local(std::rc::Rc::clone(&worker).maintain());
        local.spawn_local(worker.serve(0, socket));
        drop(entered);
        runtime.block_on(local);
    });
    address
}

/// Which way these tests reach an upstream. The suite is the same either way — that is
/// the point of it — so it is told rather than written twice:
///
/// ```text
/// cargo test                                  the engine's client
/// EDGERUSH_TEST_UPSTREAM=ours cargo test      EdgeRush's own
/// ```
fn upstream_under_test() -> Upstream {
    match std::env::var("EDGERUSH_TEST_UPSTREAM").as_deref() {
        Ok("ours") => Upstream::Ours,
        _ => Upstream::Hyper,
    }
}

/// An upstream that is a socket and nothing more: every connection it accepts is handed to
/// `answer`, which reads and writes the bytes the test chose.
fn raw_upstream<F, Fut>(answer: F) -> SocketAddr
where
    F: Fn(Wire) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let socket = TcpListener::from_std(socket).unwrap();
            loop {
                let (stream, _) = socket.accept().await.unwrap();
                tokio::spawn(answer(Wire::new(stream)));
            }
        });
    });
    address
}

/// One end of a connection, read and written as bytes.
struct Wire {
    stream: TcpStream,
    /// What has been read and not yet taken: a read for a head may bring body bytes too.
    buffered: Vec<u8>,
}

impl Wire {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffered: Vec::new(),
        }
    }

    /// Connects to `address` as a client does.
    async fn to(address: SocketAddr) -> Self {
        Self::new(TcpStream::connect(address).await.unwrap())
    }

    async fn write(&mut self, bytes: &str) {
        self.stream.write_all(bytes.as_bytes()).await.unwrap();
    }

    /// Reads up to and including the empty line that ends a head, and returns it.
    async fn head(&mut self) -> String {
        let head = self.until(b"\r\n\r\n").await.expect("a whole head");
        String::from_utf8(head).unwrap()
    }

    /// Reads a chunked body to its end, terminating chunk and trailer section included.
    async fn chunked_body(&mut self) -> String {
        let body = self.until(b"\r\n\r\n").await.expect("a body that ends");
        String::from_utf8(body).unwrap()
    }

    /// Reads exactly `count` bytes of body.
    async fn body(&mut self, count: usize) -> String {
        while self.buffered.len() < count {
            if !self.more().await {
                panic!("the connection ended inside a body");
            }
        }
        let rest = self.buffered.split_off(count);
        let body = std::mem::replace(&mut self.buffered, rest);
        String::from_utf8(body).unwrap()
    }

    /// Reads until `mark` has been seen, and returns everything up to and including it.
    /// `None` if the connection ended first.
    async fn until(&mut self, mark: &[u8]) -> Option<Vec<u8>> {
        loop {
            if let Some(at) = find(&self.buffered, mark) {
                let rest = self.buffered.split_off(at + mark.len());
                return Some(std::mem::replace(&mut self.buffered, rest));
            }
            if !self.more().await {
                return None;
            }
        }
    }

    /// Reads whatever has arrived. False when the peer has closed.
    async fn more(&mut self) -> bool {
        let mut bytes = [0; 4096];
        let read = within(self.stream.read(&mut bytes)).await.unwrap();
        self.buffered.extend_from_slice(&bytes[..read]);
        read > 0
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// A place for an upstream to say what it saw, so the test can look at it.
fn reporter() -> (
    mpsc::UnboundedSender<String>,
    mpsc::UnboundedReceiver<String>,
) {
    mpsc::unbounded_channel()
}
// ---- what the path does today, measured ----

/// A body whose length is not known goes upstream chunked, and a chunked answer comes
/// back chunked.
#[tokio::test]
async fn a_body_of_unknown_length_is_chunked_in_both_directions() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            let body = wire.chunked_body().await;
            saw.send(format!("{head}{body}")).unwrap();
            wire.write("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\n\r\n",
            "5\r\nhello\r\n0\r\n\r\n"
        ))
        .await;

    let sent = seen.recv().await.unwrap();
    assert!(sent.contains("transfer-encoding: chunked\r\n"), "{sent}");
    assert!(sent.ends_with("5\r\nhello\r\n0\r\n\r\n"), "{sent}");
    let head = client.head().await;
    assert!(head.contains("transfer-encoding: chunked\r\n"), "{head}");
    assert_eq!(client.chunked_body().await, "2\r\nhi\r\n0\r\n\r\n");
}

/// **Where the two paths differ, on purpose.** A client's trailers reach the service
/// either way — hyper's server parses them, which
/// [`hypers_server_gives_a_service_the_requests_trailers`] shows. What becomes of them
/// next is the difference: hyper's client puts none on the wire, and EdgeRush's own puts
/// them there.
///
/// This is the one place the candidate is meant to disagree with the baseline, and it
/// disagrees by being right ([13 §5](../../../docs/13-http1-upstream.md)). A differential
/// test that expected these to match would be asking the new path to lose them too.
#[tokio::test]
async fn a_requests_trailers_reach_the_upstream_only_by_our_own_path() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            let _head = wire.head().await;
            let body = wire.chunked_body().await;
            saw.send(body).unwrap();
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\nte: trailers\r\n",
            "\r\n5\r\nhello\r\n0\r\nx-sent: yes\r\n\r\n"
        ))
        .await;

    let body = seen.recv().await.unwrap();
    match upstream_under_test() {
        // Lost between the service and the wire, which is the half being replaced.
        Upstream::Hyper => assert_eq!(body, "5\r\nhello\r\n0\r\n\r\n"),
        Upstream::Ours => assert_eq!(body, "5\r\nhello\r\n0\r\nx-sent: yes\r\n\r\n"),
    }
}

/// Hyper's server does hand the trailers to the service, so what the test above measures
/// is the upstream half losing them and not the downstream half never seeing them. This
/// asks hyper alone, with no proxy in the way, so that the two halves cannot be confused.
#[tokio::test]
async fn hypers_server_gives_a_service_the_requests_trailers() {
    use http_body_util::BodyExt;

    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (saw, mut seen) = reporter();
    tokio::spawn(async move {
        let (stream, _) = socket.accept().await.unwrap();
        let service = hyper::service::service_fn(move |request: hyper::Request<Incoming>| {
            let saw = saw.clone();
            async move {
                let body = request.into_body().collect().await.unwrap();
                let trailers = body.trailers().cloned().unwrap_or_default();
                saw.send(format!("{trailers:?}")).unwrap();
                Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::new())))
            }
        });
        let _closed = auto::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(stream), service)
            .await;
    });

    let mut client = Wire::to(address).await;
    client
        .write(concat!(
            "POST /x HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\n\r\n",
            "5\r\nhello\r\n0\r\nx-sent: yes\r\n\r\n"
        ))
        .await;
    assert_eq!(seen.recv().await.unwrap(), "{\"x-sent\": \"yes\"}");
}

/// A response's trailers reach a client that said it would take them.
#[tokio::test]
async fn a_responses_trailers_reach_a_client_that_asked_for_them() {
    let mut client = Wire::to(proxy_to(trailing_upstream()).await).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\nte: trailers\r\n\r\n")
        .await;

    let head = client.head().await;
    assert!(head.contains("trailer: x-done\r\n"), "{head}");
    assert_eq!(
        client.chunked_body().await,
        "2\r\nhi\r\n0\r\nx-done: yes\r\n\r\n"
    );
}

/// A client that did not ask for them is sent the body without them, though it is still
/// told they were coming: the engine keeps to what the client said it would take. A test
/// of trailers that forgets the `TE` field therefore measures nothing at all.
#[tokio::test]
async fn a_client_that_did_not_ask_for_trailers_is_not_sent_them() {
    let mut client = Wire::to(proxy_to(trailing_upstream()).await).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;

    let head = client.head().await;
    assert!(head.contains("trailer: x-done\r\n"), "{head}");
    assert_eq!(client.chunked_body().await, "2\r\nhi\r\n0\r\n\r\n");
}

/// An upstream whose answer carries a trailer, and announces it.
fn trailing_upstream() -> SocketAddr {
    raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        wire.write(concat!(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ntrailer: x-done\r\n\r\n",
            "2\r\nhi\r\n0\r\nx-done: yes\r\n\r\n"
        ))
        .await;
    })
}

/// The 100 a client is waiting for is hyper's server's own, sent as soon as the body is
/// wanted; the expectation is forwarded to the upstream as well, so an upstream that
/// answers 100 of its own is answering the proxy, not the client, and that answer is
/// consumed here. The client sees exactly one 100.
#[tokio::test]
async fn an_expectation_of_continue_is_answered_here_and_forwarded_too() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            saw.send(head).unwrap();
            wire.write("HTTP/1.1 100 Continue\r\n\r\n").await;
            let body = wire.chunked_body().await;
            saw.send(body).unwrap();
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\nexpect: 100-continue\r\n",
            "transfer-encoding: chunked\r\n\r\n"
        ))
        .await;

    // Before the client has sent a byte of body, and whatever the upstream has said.
    assert_eq!(client.head().await, "HTTP/1.1 100 Continue\r\n\r\n");
    let sent = seen.recv().await.unwrap();
    assert!(sent.contains("expect: 100-continue\r\n"), "{sent}");

    client.write("5\r\nhello\r\n0\r\n\r\n").await;
    assert_eq!(seen.recv().await.unwrap(), "5\r\nhello\r\n0\r\n\r\n");
    let head = client.head().await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert_eq!(client.body(2).await, "ok");
}

/// **A gap, not a promise.** An interim head is consumed and the final response follows
/// it, which is what the client needs; but the interim one is not passed on, so `103
/// Early Hints` never reaches the client. That is hyper's server, which has no way to
/// send one from a service's response ([13 §5](../../../docs/13-http1-upstream.md)), and
/// so is not something an own upstream path would change.
#[tokio::test]
async fn an_interim_response_is_consumed_and_not_passed_on() {
    let upstream = raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        wire.write("HTTP/1.1 103 Early Hints\r\nlink: </s.css>; rel=preload\r\n\r\n")
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await;
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;

    let head = client.head().await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(!head.contains("103"), "{head}");
    assert!(!head.contains("link:"), "{head}");
    assert_eq!(client.body(2).await, "ok");
}

/// An upstream that answers before it has read the body is not waited for: the answer
/// goes to the client while the upload is still in the air.
#[tokio::test]
async fn a_final_response_during_an_upload_reaches_the_client() {
    let upstream = raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        // Not a byte of the body is read.
        wire.write(concat!(
            "HTTP/1.1 413 Payload Too Large\r\ncontent-length: 3\r\n",
            "connection: close\r\n\r\ntoo"
        ))
        .await;
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    // A body that is begun and never finished.
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\n\r\n",
            "5\r\nhello\r\n"
        ))
        .await;

    let head = client.head().await;
    assert!(head.starts_with("HTTP/1.1 413 "), "{head}");
    assert_eq!(client.body(3).await, "too");
}

/// A response whose body only the close of the connection ends is read to that close and
/// framed again for the client, which speaks HTTP/1.1 and can be sent chunks.
#[tokio::test]
async fn a_close_delimited_response_is_framed_again_as_chunks() {
    let upstream = raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        // Neither content-length nor transfer-encoding: the body is whatever follows.
        wire.write("HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\r\nby the close")
            .await;
        drop(wire);
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;

    let head = client.head().await;
    assert!(head.contains("transfer-encoding: chunked\r\n"), "{head}");
    assert!(!head.contains("content-length"), "{head}");
    assert_eq!(
        client.chunked_body().await,
        "C\r\nby the close\r\n0\r\n\r\n"
    );
}

/// **What the upstream parser may not claim.** Two `Content-Length` fields that say the
/// same thing are made one by hyper's server before the request reaches EdgeRush at all,
/// so the upstream sees a single field and nothing here rejected anything. A new upstream
/// *response* parser rejects repeated lengths, equal or not
/// ([13 §4](../../../docs/13-http1-upstream.md)); that is the other direction, and this
/// test is here so that the two are not confused.
#[tokio::test]
async fn equal_repeated_request_lengths_are_made_one_by_the_engine() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            let body = wire.body(5).await;
            saw.send(format!("{head}{body}")).unwrap();
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\ncontent-length: 5\r\n",
            "content-length: 5\r\n\r\nhello"
        ))
        .await;

    let head = client.head().await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    let sent = seen.recv().await.unwrap();
    assert_eq!(sent.matches("content-length:").count(), 1, "{sent}");
    assert!(sent.ends_with("content-length: 5\r\n\r\nhello"), "{sent}");
}

/// What the engine puts on the wire follows what the answer's body says is left of it,
/// so the framing is where that shows: a length the upstream gave is kept and not turned
/// into chunks, and an answer of the data plane's own says it has no body at all rather
/// than leaving the client to wait for one.
#[tokio::test]
async fn an_answers_framing_follows_what_its_body_has_left() {
    let upstream = raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await;
    });
    let proxy = proxy_to(upstream).await;

    let mut client = Wire::to(proxy).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;
    let head = client.head().await;
    assert!(head.contains("content-length: 2\r\n"), "{head}");
    assert!(!head.contains("transfer-encoding"), "{head}");
    assert_eq!(client.body(2).await, "ok");

    // An answer of ours, which no upstream was asked for: a request with no host.
    let mut bare = Wire::to(proxy).await;
    bare.write("GET /up HTTP/1.1\r\n\r\n").await;
    let head = bare.head().await;
    assert!(head.starts_with("HTTP/1.1 400 "), "{head}");
    assert!(head.contains("content-length: 0\r\n"), "{head}");
}
