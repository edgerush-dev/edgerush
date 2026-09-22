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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    proxy_to_with_filters(upstream, "").await
}

async fn proxy_to_with_filters(upstream: SocketAddr, filters: &str) -> SocketAddr {
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
{filters}
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

    /// Everything that arrives from here until the connection ends, however it ends.
    /// A body cut short may be followed by a close or by a reset, and which of the two
    /// arrives is the operating system's business rather than the proxy's.
    async fn rest(&mut self) -> Vec<u8> {
        let mut bytes = [0; 4096];
        loop {
            match within(self.stream.read(&mut bytes)).await {
                Ok(0) | Err(_) => break,
                Ok(read) => self.buffered.extend_from_slice(&bytes[..read]),
            }
        }
        std::mem::take(&mut self.buffered)
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

/// A field the request's own `Connection` named is hop-by-hop for that hop, so it may not
/// travel on — as a trailer no more than as a header, and no more as a name declared in
/// `Trailer` than as the field itself ([13 §4](../../../docs/13-http1-upstream.md)).
///
/// The names have to be read before routing strips the `Connection` that held them:
/// afterwards there is nothing left to read them from, and everything it named looks like
/// an ordinary field.
#[tokio::test]
async fn a_trailer_the_requests_connection_named_does_not_travel_on() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            let body = wire.chunked_body().await;
            saw.send(format!("{head}{body}")).unwrap();
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write(concat!(
            "POST /up HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\n",
            "connection: x-secret\r\ntrailer: x-secret, x-keep\r\nte: trailers\r\n",
            "\r\n5\r\nhello\r\n0\r\nx-secret: leaked\r\nx-keep: fine\r\n\r\n"
        ))
        .await;

    let sent = seen.recv().await.unwrap();
    // Neither as a trailer nor as a name the head declared it would send.
    assert!(
        !sent.to_ascii_lowercase().contains("x-secret"),
        "a field the request's own Connection named crossed the hop:\n{sent}"
    );
    // What the `Connection` did not name is untouched: by our own path the trailer
    // travels and the declaration still names it.
    if matches!(upstream_under_test(), Upstream::Ours) {
        assert!(sent.contains("x-keep: fine"), "{sent}");
        assert!(
            sent.to_ascii_lowercase().contains("trailer: x-keep"),
            "{sent}"
        );
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

/// **A limitation, not a promise.** A 426 reaches the client without the `Upgrade` that
/// [RFC 9110 §15.5.22](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.22) says
/// it MUST carry: `Upgrade` is hop-by-hop and always taken off, and this proxy upgrades no
/// connection, so naming a protocol to the client would offer what the gateway cannot do.
/// The answer itself is still forwarded, status and body as they came (03 §11).
#[tokio::test]
async fn a_426_is_forwarded_without_its_upgrade() {
    let upstream = raw_upstream(move |mut wire| async move {
        let _head = wire.head().await;
        // No `Connection: upgrade`: `Upgrade` goes whether or not anything names it.
        wire.write(concat!(
            "HTTP/1.1 426 Upgrade Required\r\nupgrade: h2c\r\n",
            "content-length: 4\r\n\r\nplea"
        ))
        .await;
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;

    let head = client.head().await;
    assert!(head.starts_with("HTTP/1.1 426 "), "{head}");
    let lower = head.to_ascii_lowercase();
    assert!(!lower.contains("upgrade:"), "{head}");
    assert!(!lower.contains("h2c"), "{head}");
    assert_eq!(client.body(4).await, "plea");
}

/// A 101 to a request that did not ask to switch — none can have asked, since `Upgrade`
/// is taken off every request — is answered 502 on both paths. Passing it on would hand
/// the client a switch it never asked for and tunnel whatever the upstream said next,
/// which this slice does not do ([13 §1](../../../docs/13-http1-upstream.md);
/// linkerd2-proxy's `http1_upgrade_not_requested` tests the same).
#[tokio::test]
async fn a_101_nobody_asked_for_is_answered_502() {
    let (upstream, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write(concat!(
                "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n",
                "connection: upgrade\r\n\r\nTUNNELLED"
            ))
            .await;
            // Held open, as a tunnel would be.
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;

    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    let lower = head.to_ascii_lowercase();
    assert!(!lower.contains("upgrade"), "{head}");
    // Nothing the upstream said after its head reaches the client.
    let length = lower
        .split("\r\n")
        .find_map(|line| line.strip_prefix("content-length: "))
        .map_or(0, |length| length.trim().parse::<usize>().unwrap());
    assert!(!client.body(length).await.contains("TUNNELLED"));

    // The client's connection still carries requests, and the next one goes to a fresh
    // upstream connection rather than into the one that offered to switch.
    let head = asks(&mut client, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(5)).await, "fresh");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        2,
        "the switched connection was reused"
    );
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

/// A connection whose answer finished carries the next request too, which only the
/// upstream can see: it is given one connection for two requests. A body that came
/// back correctly says nothing about whether anything was reused.
#[tokio::test]
async fn a_finished_answer_leaves_its_connection_for_the_next_request() {
    let accepts = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&accepts);
    let backend = raw_upstream(move |mut wire| {
        count.fetch_add(1, Ordering::SeqCst);
        async move {
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    for _ in 0..2 {
        client
            .write("GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .await;
        within(client.head()).await;
        assert_eq!(within(client.body(2)).await, "ok");
    }
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "upstream connection not reused"
    );
}

/// **Where the two paths differ, on purpose.** An interim head that claims a body
/// describes bytes that nothing here will read as one and that the next reader may.
/// Ours refuses the exchange; the engine's client waves it through.
#[tokio::test]
async fn an_interim_answer_claiming_a_body_is_refused_by_our_own_path() {
    let backend = raw_upstream(|mut wire| async move {
        wire.head().await;
        wire.write("HTTP/1.1 100 Continue\r\nContent-Length: 7\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    client
        .write("GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let head = within(client.head()).await;
    match upstream_under_test() {
        // Waved through: the engine's client consumes the interim head without asking
        // what it claimed, and answers with the 200 behind it.
        Upstream::Hyper => assert!(head.starts_with("HTTP/1.1 200"), "{head}"),
        // An interim head that claims a body is a body nobody here will read and
        // something else may: the exchange fails rather than pass it on.
        Upstream::Ours => assert!(head.starts_with("HTTP/1.1 502"), "{head}"),
    }
}

/// A field an answer's own `Connection` names is that hop's business and no further.
/// Forwarding it is something an intermediary may not do (RFC 9110 §7.6.1), and which
/// client read the body has nothing to do with it: the same set is taken off both.
#[tokio::test]
async fn a_trailer_the_answer_nominated_reaches_no_client() {
    let backend = raw_upstream(|mut wire| async move {
        wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: x-secret\r\nTrailer: x-secret\r\n\r\n1\r\na\r\n0\r\nx-secret: hidden\r\n\r\n").await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    client
        .write("GET / HTTP/1.1\r\nHost: example.test\r\nTE: trailers\r\n\r\n")
        .await;
    within(client.head()).await;
    let body = within(client.chunked_body()).await;
    assert!(!body.contains("hidden"), "{body}");
}

/// An upstream that says something into a connection nobody is using has said it to
/// nobody. Handing that to whoever comes next would be answering one request with
/// what was meant for another, which is worth more than the connection it saves.
#[tokio::test]
async fn what_an_idle_connection_was_told_is_never_the_next_answer() {
    let backend = raw_upstream(|mut wire| async move {
        wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n")
            .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        wire.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nzz")
            .await;
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    client
        .write("GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    within(client.head()).await;
    within(client.chunked_body()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    client
        .write("GET /second HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let head = within(client.head()).await;
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        assert!(within(client.chunked_body()).await.contains("ok"));
    } else {
        assert_eq!(
            within(client.body(2)).await,
            "ok",
            "unsolicited idle bytes became next response"
        );
    }
}

// ---- reuse, and the ends of connections ----
//
// Whether a connection was kept is invisible from either end of the proxy: the client
// sees an answer and the upstream sees a request, and a correct body says nothing about
// which socket carried it ([13 §6](../../../docs/13-http1-upstream.md)). So every test
// below counts the connections the upstream accepted, and every one of them sends a
// second request: reuse that is never used again is reuse that was never proved.

/// The plain answer, which only a connection of its own ever gives.
const FRESH: &str = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfresh";

/// The peer deliberately ignores the request's close option. The clients themselves
/// must prevent another downstream client from borrowing a credential-bearing socket.
#[tokio::test]
async fn connection_bound_credentials_are_never_pooled() {
    for (credentials, status, length) in [
        ("Authorization: NTLM token\r\n", 200, 0),
        ("Authorization: nEgOtIaTe ticket\r\n", 200, 2),
        ("Authorization: NTLM token\r\n", 401, 2),
        (
            "Authorization: Basic token\r\nAuthorization: Negotiate ticket\r\n",
            200,
            2,
        ),
    ] {
        let (report, mut reports) = reporter();
        let (backend, accepts) = counted(move |mut wire| {
            let report = report.clone();
            async move {
                while let Some(head) = wire.until(b"\r\n\r\n").await {
                    report.send(String::from_utf8(head).unwrap()).unwrap();
                    let body = if length == 0 { "" } else { "ok" };
                    wire.write(&format!(
                        "HTTP/1.1 {status} Backend Answer\r\nContent-Length: {length}\r\n\r\n{body}"
                    ))
                    .await;
                }
            }
        });
        let front = proxy_to(backend).await;
        let mut first = Wire::to(front).await;
        first
            .write(&format!(
                "GET /first HTTP/1.1\r\nHost: example.test\r\n{credentials}\r\n"
            ))
            .await;
        assert!(
            within(first.head())
                .await
                .starts_with(&format!("HTTP/1.1 {status}"))
        );
        if length > 0 {
            assert_eq!(within(first.body(length)).await, "ok");
        }
        let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
        assert!(sent.contains("connection: close\r\n"), "{sent}");

        let mut second = Wire::to(front).await;
        assert!(
            asks(&mut second, "/second")
                .await
                .starts_with(&format!("HTTP/1.1 {status}"))
        );
        if length > 0 {
            assert_eq!(within(second.body(length)).await, "ok");
        }
        let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
        assert!(!sent.contains("authorization:"), "{sent}");
        assert_eq!(accepts.load(Ordering::SeqCst), 2, "{credentials}");
    }
}

#[tokio::test]
async fn connection_bound_credentials_added_by_a_rule_are_never_pooled() {
    for name in ["Authorization", "Proxy-Authorization"] {
        let (report, mut reports) = reporter();
        let (backend, accepts) = counted(move |mut wire| {
            let report = report.clone();
            async move {
                while let Some(head) = wire.until(b"\r\n\r\n").await {
                    report.send(String::from_utf8(head).unwrap()).unwrap();
                    wire.write(FRESH).await;
                }
            }
        });
        let filters = format!(
            "        filters:\n          - type: request_header_modifier\n            set: [{{ name: {name}, value: 'Negotiate ticket' }}]"
        );
        let front = proxy_to_with_filters(backend, &filters).await;
        for _ in 0..2 {
            let mut client = Wire::to(front).await;
            assert!(asks(&mut client, "/").await.starts_with("HTTP/1.1 200"));
            assert_eq!(within(client.body(5)).await, "fresh");
            let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
            assert!(
                sent.contains(&format!(
                    "{}: negotiate ticket\r\n",
                    name.to_ascii_lowercase()
                )),
                "{sent}"
            );
            assert!(sent.contains("connection: close\r\n"), "{sent}");
        }
        assert_eq!(accepts.load(Ordering::SeqCst), 2, "{name}");
    }
}

#[tokio::test]
async fn connection_bound_challenges_prevent_reuse_on_the_custom_path() {
    for (status, field, challenge) in [
        (401, "WWW-Authenticate", "NTLM"),
        (401, "WWW-Authenticate", "Digest realm=\"a,b\", Negotiate"),
        (407, "Proxy-Authenticate", "Negotiate"),
    ] {
        let (backend, accepts) = counted(move |mut wire| async move {
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(&format!(
                    "HTTP/1.1 {status} Challenge\r\n{field}: {challenge}\r\nContent-Length: 2\r\n\r\nok"
                )).await;
            }
        });
        let front = proxy_to(backend).await;
        let mut clients = Vec::new();
        for _ in 0..2 {
            let mut client = Wire::to(front).await;
            assert!(
                asks(&mut client, "/")
                    .await
                    .starts_with(&format!("HTTP/1.1 {status}"))
            );
            assert_eq!(within(client.body(2)).await, "ok");
            clients.push(client);
        }
        let expected = match upstream_under_test() {
            Upstream::Hyper => 1,
            Upstream::Ours => 2,
        };
        assert_eq!(accepts.load(Ordering::SeqCst), expected, "{challenge}");
    }
}

#[tokio::test]
async fn connection_bound_guard_uses_only_credentials_that_go_upstream() {
    for (credentials, filters) in [
        ("Authorization: Basic token\r\n", ""),
        ("Authorization: Bearer NTLM\r\n", ""),
        ("Proxy-Authorization: NTLM token\r\n", ""),
        (
            "Authorization: Negotiate ticket\r\n",
            "        filters:\n          - type: request_header_modifier\n            remove: [authorization]",
        ),
    ] {
        let (report, mut reports) = reporter();
        let (backend, accepts) = counted(move |mut wire| {
            let report = report.clone();
            async move {
                while let Some(head) = wire.until(b"\r\n\r\n").await {
                    report.send(String::from_utf8(head).unwrap()).unwrap();
                    wire.write(FRESH).await;
                }
            }
        });
        let front = proxy_to_with_filters(backend, filters).await;
        let mut clients = Vec::new();
        for _ in 0..2 {
            let mut client = Wire::to(front).await;
            client
                .write(&format!(
                    "GET / HTTP/1.1\r\nHost: example.test\r\n{credentials}\r\n"
                ))
                .await;
            assert!(within(client.head()).await.starts_with("HTTP/1.1 200"));
            assert_eq!(within(client.body(5)).await, "fresh");
            let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
            assert!(!sent.contains("connection: close"), "{sent}");
            assert!(!sent.contains("proxy-authorization:"), "{sent}");
            if !filters.is_empty() {
                assert!(!sent.contains("authorization:"), "{sent}");
            }
            clients.push(client);
        }
        assert_eq!(accepts.load(Ordering::SeqCst), 1, "{credentials}");
    }
}

/// Closing the credential-bearing upstream hop must leave the downstream H2 connection
/// usable, with the backend's response delivered on each stream.
#[tokio::test]
async fn connection_bound_credentials_from_http2_close_only_the_upstream_hop() {
    use http_body_util::BodyExt;

    let (report, mut reports) = reporter();
    let (backend, accepts) = counted(move |mut wire| {
        let report = report.clone();
        async move {
            while let Some(head) = wire.until(b"\r\n\r\n").await {
                report.send(String::from_utf8(head).unwrap()).unwrap();
                wire.write(FRESH).await;
            }
        }
    });
    let front = proxy_to(backend).await;
    let mut sender = h2_to(front).await;
    for credential in [true, false, false] {
        let mut request = http::Request::get(format!("http://{front}/"));
        if credential {
            request = request.header("authorization", "Negotiate ticket");
        }
        let response = within(sender.send_request(request.body(Upload::None).unwrap()))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(!response.headers().contains_key("connection"));
        assert_eq!(
            within(response.into_body().collect())
                .await
                .unwrap()
                .to_bytes(),
            "fresh"
        );
        let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
        assert_eq!(sent.contains("connection: close\r\n"), credential, "{sent}");
    }
    assert_eq!(accepts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connection_bound_credentials_do_not_make_an_early_answer_stop_the_upload() {
    let (report, mut reports) = reporter();
    let backend = raw_upstream(move |mut wire| {
        let report = report.clone();
        async move {
            let head = wire.head().await;
            report.send(head).unwrap();
            // A refusal alone does not stop the upload. Only the request said close;
            // this response still depends on receiving the whole request body.
            wire.write("HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\n\r\n")
                .await;
            let body = wire.body(5).await;
            report.send(body).unwrap();
            wire.write("ok").await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    client.write("POST / HTTP/1.1\r\nHost: example.test\r\nAuthorization: NTLM token\r\nContent-Length: 5\r\n\r\n").await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 401"), "{head}");
    client.write("hello").await;
    assert_eq!(within(client.body(2)).await, "ok");
    let sent = within(reports.recv()).await.unwrap().to_ascii_lowercase();
    assert!(sent.contains("connection: close\r\n"), "{sent}");
    assert_eq!(within(reports.recv()).await.unwrap(), "hello");
}

/// Answers every request on this connection the same plain way, until it ends.
async fn plainly(mut wire: Wire) {
    while wire.until(b"\r\n\r\n").await.is_some() {
        wire.write(FRESH).await;
    }
}

/// An upstream whose **first** connection does something hostile and whose later ones
/// answer plainly, with a count of how many it has accepted.
///
/// That split is what makes the assertions say something. A second answer of `fresh`
/// could not have come from the hostile connection, and an accept count of one could not
/// have served two requests on two sockets: together they say which socket was used,
/// which no amount of reading the answer can.
fn hostile_first<F, Fut>(hostile: F) -> (SocketAddr, Arc<AtomicUsize>)
where
    F: Fn(Wire) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let hostile = Arc::new(hostile);
    let first = AtomicUsize::new(0);
    counted(move |wire| {
        let hostile = Arc::clone(&hostile);
        let hostile_now = first.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            if hostile_now {
                hostile(wire).await;
            } else {
                plainly(wire).await;
            }
        }
    })
}

/// An upstream that treats every connection alike, with a count of how many it accepted.
fn counted<F, Fut>(answer: F) -> (SocketAddr, Arc<AtomicUsize>)
where
    F: Fn(Wire) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let accepts = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&accepts);
    let answer = Arc::new(answer);
    let address = raw_upstream(move |wire| {
        counting.fetch_add(1, Ordering::SeqCst);
        let answer = Arc::clone(&answer);
        async move { answer(wire).await }
    });
    (address, accepts)
}

/// Whatever followed the first head in `bytes`, or `None` if no whole head arrived.
///
/// **A failure part way through a body has two shapes, and which one arrives is not the
/// proxy's to decide.** The head reaches the downstream server before the body is polled,
/// but whether that server had flushed it to the socket before the body failed is its own
/// buffering. So a client may be left with a head and a body that stops, or with nothing
/// at all. Neither is a message that finished, and that is the whole of what is promised
/// ([13 §7](../../../docs/13-http1-upstream.md)).
fn after_head(bytes: &[u8]) -> Option<&[u8]> {
    find(bytes, b"\r\n\r\n").map(|at| &bytes[at + 4..])
}

/// Sends a request and returns the answer's head.
async fn asks(client: &mut Wire, path: &str) -> String {
    client
        .write(&format!(
            "GET {path} HTTP/1.1\r\nHost: example.test\r\n\r\n"
        ))
        .await;
    within(client.head()).await
}

/// What is left of an upload the upstream refused, or never took, is the body of the
/// request it belonged to and never a request of its own — even when it looks exactly like
/// one. Whether the client's connection is drained or closed after the answer, nothing in
/// it is served ([13 §5](../../../docs/13-http1-upstream.md); linkerd2-proxy's tests of
/// an early answer to a request with a body).
#[tokio::test]
async fn the_rest_of_a_refused_upload_is_never_served_as_a_request() {
    const SMUGGLED: &str = "GET /smuggled HTTP/1.1\r\nhost: a.test\r\n\r\n";
    let refused =
        "HTTP/1.1 413 Payload Too Large\r\ncontent-length: 3\r\nconnection: close\r\n\r\ntoo";
    for (framing, answer) in [
        ("counted", Some(refused)),
        ("chunked", Some(refused)),
        ("counted", None),
        ("chunked", None),
    ] {
        let (upstream, accepts) = hostile_first(move |mut wire| async move {
            if wire.until(b"\r\n\r\n").await.is_some() {
                // Not a byte of the body is read, and then the connection goes.
                if let Some(answer) = answer {
                    wire.write(answer).await;
                }
            }
        });
        let mut client = Wire::to(proxy_to(upstream).await).await;
        let request = match framing {
            "counted" => format!(
                "POST /up HTTP/1.1\r\nhost: a.test\r\ncontent-length: {}\r\n\r\n{SMUGGLED}",
                SMUGGLED.len()
            ),
            _ => format!(
                "POST /up HTTP/1.1\r\nhost: a.test\r\ntransfer-encoding: chunked\r\n\r\n\
                 {:x}\r\n{SMUGGLED}\r\n0\r\n\r\n",
                SMUGGLED.len()
            ),
        };
        client.write(&request).await;

        let head = within(client.head()).await;
        let expected = if answer.is_some() {
            "HTTP/1.1 413"
        } else {
            "HTTP/1.1 502"
        };
        assert!(head.starts_with(expected), "{framing} {answer:?}: {head}");

        // Whatever else the client is sent, for a while, holds no second answer.
        let mut after = Vec::new();
        let mut bytes = [0; 4096];
        let listening = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(listening);
        loop {
            tokio::select! {
                () = &mut listening => break,
                read = client.stream.read(&mut bytes) => match read {
                    Ok(0) | Err(_) => break,
                    Ok(read) => after.extend_from_slice(&bytes[..read]),
                },
            }
        }
        let after = String::from_utf8_lossy(&after);
        assert!(
            !after.contains("HTTP/1.1") && !after.contains("fresh"),
            "{framing} {answer:?}: {after}"
        );
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "{framing} {answer:?}: the rest of the upload reached the upstream as a request"
        );
    }
}

/// Bytes arriving with the answer are a peer saying something nobody asked for. The
/// answer itself is still the answer, but the connection has said a thing this end
/// cannot account for, and a connection like that is not lent out again.
#[tokio::test]
async fn surplus_bytes_behind_an_answer_stop_the_connection_being_kept() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            // The answer, and hard behind it a whole answer nobody asked for.
            wire.write(concat!(
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nsurplus",
            ))
            .await;
        }
        // Still there, so that a connection wrongly kept would have something to give.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    assert!(
        asks(&mut client, "/first")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(within(client.body(2)).await, "ok");

    assert!(
        asks(&mut client, "/second")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(
        within(client.body(5)).await,
        "fresh",
        "the surplus became the next answer"
    );
    assert_eq!(accepts.load(Ordering::SeqCst), 2, "a kept connection");
}

/// An upstream that closes a connection it is not using has closed it, whatever this end
/// thought it was keeping. The next request opens one of its own and is answered; a
/// connection found to be gone is not the request's failure.
#[tokio::test]
async fn a_connection_closed_while_idle_is_not_handed_to_the_next_request() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write(FRESH).await;
        }
        // And gone: the handler returns, which closes it.
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    assert!(
        asks(&mut client, "/first")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(within(client.body(5)).await, "fresh");
    // Long enough for the close to have arrived before the next request wants a socket.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let head = asks(&mut client, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(5)).await, "fresh");
    assert_eq!(accepts.load(Ordering::SeqCst), 2, "a closed connection");
}

/// A connection that closes before it has answered at all fails the request it was
/// carrying. It is not sent again down another socket: a request that has been dispatched
/// has been dispatched, and this end cannot know what the other made of it
/// ([13 §5](../../../docs/13-http1-upstream.md)).
#[tokio::test]
async fn a_connection_that_closes_before_answering_fails_and_is_not_replayed() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        let _request = wire.until(b"\r\n\r\n").await;
        // Nothing said, and gone.
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request was sent a second time"
    );
}

/// The same part way through a head: nothing has been promised to the client yet, so the
/// failure is the proxy's own answer rather than a half-read one passed on.
#[tokio::test]
async fn a_connection_that_closes_inside_a_head_fails_the_request() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\nContent-Len").await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request was replayed"
    );
}

/// A close while the request is still going out is the same failure at a different
/// moment: the upload had nowhere left to go and no answer ever came. Nothing had been
/// promised to the client, so it is told, and the request is not sent anywhere else.
#[tokio::test]
async fn a_connection_that_closes_during_an_upload_fails_the_request() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        // The head and not a byte of the body, and then gone.
        let _head = wire.until(b"\r\n\r\n").await;
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    // Two bytes of a body that says it is ten, so the upload is still in the air when
    // the upstream goes.
    client
        .write("POST /first HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nab")
        .await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request was sent a second time"
    );
}

/// An interim answer is not an answer. A connection that says one and then closes has
/// left the exchange where it was: still waiting for the head that never came.
#[tokio::test]
async fn a_connection_that_closes_after_an_interim_answer_fails_the_request() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 103 Early Hints\r\nLink: </s.css>; rel=preload\r\n\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request was replayed"
    );
}

/// **A body that stops early is lost, not finished.** The head has gone to the client
/// already, so there is no status left to change; what must not happen is the client
/// being handed five bytes of a ten-byte body as though that were all of it.
#[tokio::test]
async fn a_length_delimited_body_cut_short_never_looks_complete() {
    let (backend, _accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    client
        .write("GET /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let all = within(client.rest()).await;
    assert!(
        after_head(&all).is_none_or(|body| body.len() < 10),
        "a cut-short body was completed: {all:?}"
    );
}

/// And chunked: the client is never given the terminating chunk, because there was none.
#[tokio::test]
async fn a_chunked_body_cut_short_never_reaches_its_terminator() {
    let (backend, _accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nshort\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    client
        .write("GET /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let all = within(client.rest()).await;
    assert!(
        after_head(&all).is_none_or(|body| find(body, b"0\r\n\r\n").is_none()),
        "a cut-short body was terminated: {all:?}"
    );
}

/// A last chunk with no trailer section behind it is a message that stopped inside its
/// framing, however complete the data looks. The bytes are all there; the message is not.
#[tokio::test]
async fn a_body_that_stops_inside_its_trailer_section_is_not_an_end() {
    let (backend, _accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            // Every byte of data, the zero chunk, and then nothing: the empty line that
            // ends the trailer section never comes.
            wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n")
                .await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    client
        .write("GET /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let all = within(client.rest()).await;
    assert!(
        after_head(&all).is_none_or(|body| find(body, b"0\r\n\r\n").is_none()),
        "an unterminated message was terminated: {all:?}"
    );
}

/// A body the close of the connection delimits ends with that close, which is the one
/// place EOF is an ending rather than a loss.
///
/// The count here is not evidence of a decision: a connection that has closed could not
/// have been kept whatever this end thought. What it shows is that the next request got
/// a connection of its own and was answered, rather than failing on the remains of one.
/// That such an answer is refused on its own terms is
/// `a_body_the_close_delimited_is_never_kept`, which asks the exchange directly.
#[tokio::test]
async fn a_close_delimited_answer_arrives_whole_and_the_next_request_is_served() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\n\r\nall of it").await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    assert!(
        asks(&mut client, "/first")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(
        within(client.chunked_body()).await.contains("all of it"),
        "a close-delimited body was not delivered whole"
    );

    assert!(
        asks(&mut client, "/second")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(within(client.body(5)).await, "fresh");
    assert_eq!(accepts.load(Ordering::SeqCst), 2, "a spent connection");
}

/// **A refused exchange's socket is never lent again.** The framing stopped making sense
/// part way through, so what state the connection is in is exactly what this end does not
/// know — and a connection nobody can account for is worth less than the one it saves.
#[tokio::test]
async fn a_connection_whose_framing_failed_is_never_lent_again() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            // A chunk size that is not a number, behind a head that was fine.
            wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
                .await;
        }
        // Willing to answer anything else that arrives here, so that a connection wrongly
        // kept would be answered rather than simply hang.
        plainly(wire).await;
    });
    let proxy = proxy_to(backend).await;
    let mut client = Wire::to(proxy).await;

    client
        .write("GET /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let lost = within(client.rest()).await;
    assert!(
        after_head(&lost).is_none_or(|body| find(body, b"0\r\n\r\n").is_none()),
        "a message that stopped making sense was finished off: {lost:?}"
    );

    // The client's connection went with the body, so the second request needs one of its
    // own downstream; what is being counted is the upstream's.
    let mut second = Wire::to(proxy).await;
    let head = asks(&mut second, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(second.body(5)).await, "fresh");
    assert_eq!(accepts.load(Ordering::SeqCst), 2, "a broken connection");
}

// ---- peers that send what this end will not take ----
//
// Most of these are decided by the codec and have a table entry of their own there. What
// they are here for is the path: a bound is reached in a different order when the bytes
// arrive in pieces, and an answer refused must leave the next request able to be served.
//
// **Not all of them are refused for the same kind of reason**, and the tests say which.
// Some are messages that do not parse. Some are shapes the specification names and leaves
// to the recipient, where refusing is this project's choice and reading on is equally
// correct. Some are limits this project sets for itself, one of them against a MUST.
// Where a test differs from the engine's client it says which of the three it is, because
// differing from hyper is not by itself being right
// ([13 §5](../../../docs/13-http1-upstream.md)).

/// Answers the first request with `answer`, then asks a second on a connection of its
/// own, and says what the client was told each time and how many connections it took.
async fn facing(answer: String) -> (SocketAddr, Arc<AtomicUsize>) {
    let answer = Arc::new(answer);
    let (backend, accepts) = hostile_first(move |mut wire| {
        let answer = Arc::clone(&answer);
        async move {
            // Answering every request on this connection the same way, so that a
            // connection wrongly kept shows as an answer from it and not as a hang.
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(&answer).await;
            }
        }
    });
    (proxy_to(backend).await, accepts)
}

/// Answers the first request with `answer`, then asks a second on a connection of its
/// own, and says what the client was told each time and how many connections it took.
async fn told_when_answered(answer: String) -> (String, String, usize) {
    let (proxy, accepts) = facing(answer).await;

    let mut client = Wire::to(proxy).await;
    let first = asks(&mut client, "/first").await;
    let mut again = Wire::to(proxy).await;
    let second = asks(&mut again, "/second").await;
    let served = within(again.body(5)).await;
    (
        first,
        format!("{second}{served}"),
        accepts.load(Ordering::SeqCst),
    )
}

/// A head refused is a request failed and a connection gone, and the next request is
/// served all the same. Every case below is one head away from a head that works.
async fn is_refused(answer: String) {
    let (first, second, accepts) = told_when_answered(answer).await;
    assert!(first.starts_with("HTTP/1.1 502"), "{first}");
    assert!(second.ends_with("fresh"), "{second}");
    assert_eq!(accepts, 2, "a refused answer kept its connection");
}

/// Two lengths that disagree are two framings, and choosing between them is how the same
/// bytes come to be one message here and two somewhere else.
#[tokio::test]
async fn an_answer_with_two_lengths_that_disagree_is_refused() {
    is_refused("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello".into())
        .await;
}

/// **A choice the specification leaves open, and the two paths take different ones.**
/// [RFC 9110 §8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6) says a
/// recipient "MAY either reject the message as invalid or replace that invalid field
/// value with a single instance of the decimal value". Both are named in the one
/// sentence: ours rejects, the engine's client replaces and keeps the connection, and
/// neither is more correct than the other. Ours rejects because a duplicate means some
/// processor upstream has already rewritten this message, and what it meant is not this
/// end's to guess.
///
/// Lengths that *disagree* are an unrecoverable error either way, which is the test
/// above. The request direction is hyper's server and not this rule at all, which
/// `equal_repeated_request_lengths_are_made_one_by_the_engine` measures.
#[tokio::test]
async fn an_answer_with_two_equal_lengths_is_refused_by_our_own_path() {
    let answer = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello";
    match upstream_under_test() {
        Upstream::Ours => is_refused(answer.into()).await,
        Upstream::Hyper => {
            let (first, _second, accepts) = told_when_answered(answer.into()).await;
            assert!(first.starts_with("HTTP/1.1 200"), "{first}");
            assert_eq!(accepts, 1, "the engine's client kept it");
        }
    }
}

/// A sign is not a digit. `+5` is a length only to a parser that was being helpful.
#[tokio::test]
async fn an_answer_with_a_signed_length_is_refused() {
    is_refused("HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\nhello".into()).await;
}

/// And a length no counter can hold is not a length; it is checked, not wrapped.
#[tokio::test]
async fn an_answer_with_a_length_too_big_to_count_is_refused() {
    is_refused("HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999\r\n\r\nhello".into())
        .await;
}

/// **Another choice left open, and neither path forwards the ambiguity.**
/// [RFC 9112 §6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) rule 3 says a
/// message with both "ought to be handled as an error", and that an intermediary which
/// chooses to forward it "MUST first remove the received Content-Length field and process
/// the Transfer-Encoding". Ours takes the error route. The engine's client takes the
/// other one and does remove the length, which is what that rule asks of a forwarder.
///
/// So this is not a hole ours closes; it is the same rule answered two permitted ways.
/// What ours buys is that nothing downstream is asked to trust a head this end could not
/// make sense of ([13 §5](../../../docs/13-http1-upstream.md)).
#[tokio::test]
async fn an_answer_with_both_a_length_and_chunking_is_refused_by_our_own_path() {
    let answer = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    match upstream_under_test() {
        Upstream::Ours => is_refused(answer.into()).await,
        Upstream::Hyper => {
            let (first, _second, accepts) = told_when_answered(answer.into()).await;
            assert!(first.starts_with("HTTP/1.1 200"), "{first}");
            assert_eq!(accepts, 1, "the engine's client kept it");
        }
    }
}

/// Trailers that the gateway does not forward are dropped from the message, not held
/// against the connection. Every byte of the trailer section was still read and accounted
/// for, so the connection has finished cleanly and carries the next request
/// ([13 §4](../../../docs/13-http1-upstream.md)).
#[tokio::test]
async fn an_answer_whose_trailers_were_filtered_still_leaves_its_connection() {
    let accepts = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&accepts);
    let backend = raw_upstream(move |mut wire| {
        counting.fetch_add(1, Ordering::SeqCst);
        async move {
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(concat!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n",
                    "Trailer: x-note, content-length\r\n\r\n",
                    "5\r\nfresh\r\n0\r\nx-note: kept\r\ncontent-length: 9\r\n\r\n",
                ))
                .await;
            }
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    for path in ["/first", "/second"] {
        client
            .write(&format!(
                "GET {path} HTTP/1.1\r\nHost: example.test\r\nTE: trailers\r\n\r\n"
            ))
            .await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let body = within(client.chunked_body()).await;
        assert!(body.contains("fresh"), "{body}");
        assert!(
            !body.to_ascii_lowercase().contains("content-length: 9"),
            "a denied trailer was forwarded: {body}"
        );
    }
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "filtering a trailer cost the connection"
    );
}

/// Answers the first request with `answer`, which stops making sense somewhere after its
/// head, and asks a second afterwards.
///
/// The head has gone to the downstream server by then, so what the client is left with is
/// its buffering and not a promise; what is asserted is that no message was ever finished
/// off, and that the connection this happened on was not kept.
async fn never_finished(answer: String) {
    let (proxy, accepts) = facing(answer).await;

    let mut client = Wire::to(proxy).await;
    client
        .write("GET /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
        .await;
    let all = within(client.rest()).await;
    assert!(
        after_head(&all).is_none_or(|body| find(body, b"0\r\n\r\n").is_none()),
        "a message that stopped making sense was finished off: {all:?}"
    );

    let mut again = Wire::to(proxy).await;
    let second = asks(&mut again, "/second").await;
    assert!(second.starts_with("HTTP/1.1 200"), "{second}");
    assert_eq!(within(again.body(5)).await, "fresh");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        2,
        "a broken connection was kept"
    );
}

/// **A limit of this project's, not a rule.** The line here is valid syntax; it is only
/// long. Nothing in HTTP sets a length for it --
/// [RFC 9112 §7.1.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1.1) says only
/// that a *server* ought to limit chunk extensions in a *request* -- so reading it however
/// long it is, as the engine's client does, is correct. Ours refuses at 4 KiB so that a
/// peer cannot choose how much a worker holds for it
/// ([13 §7](../../../docs/13-http1-upstream.md)).
#[tokio::test]
async fn a_chunk_size_line_past_its_bound_is_refused_by_our_own_path() {
    let padding = ";x=".to_owned() + &"a".repeat(8 * 1024);
    let answer = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5{padding}\r\nhello\r\n0\r\n\r\n"
    );
    match upstream_under_test() {
        Upstream::Ours => never_finished(answer).await,
        Upstream::Hyper => delivered_whole(answer).await,
    }
}

/// **A different kind of refusal from the one above: this one does not parse.**
/// [RFC 9112 §7.1.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1.1) needs a
/// `chunk-ext-name` before any `=`, so `;=novalue` is not an extension at all. Its "a
/// recipient MUST ignore unrecognized chunk extensions" is about names nobody knows, not
/// about input that does not parse, which is why framing bytes are validated here even
/// where their meaning is ignored: what is waved through is parsed by whatever reads
/// these bytes next. The engine's client skips to the CRLF without looking, which
/// nothing forbids.
#[tokio::test]
async fn a_chunk_extension_without_a_name_is_refused_by_our_own_path() {
    let answer =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;=novalue\r\nhello\r\n0\r\n\r\n"
            .to_owned();
    match upstream_under_test() {
        Upstream::Ours => never_finished(answer).await,
        Upstream::Hyper => delivered_whole(answer).await,
    }
}

/// A chunk that does not end where it said it would is a length that meant nothing.
#[tokio::test]
async fn a_chunk_that_does_not_end_where_it_said_is_refused() {
    never_finished(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX0\r\n\r\n".to_owned(),
    )
    .await;
}

/// Whitespace before a field line's colon is what
/// [RFC 9112 §5.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-5.1) singles out,
/// and a trailer is a field line like any other. That section tells a proxy to remove such
/// whitespace from a *response* before forwarding rather than to reject the message;
/// [RFC 9112 §7.1.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-7.1.2) would also
/// allow the field simply to be discarded. Failing the exchange is stricter than either,
/// and is this project's choice: a trailer section that does not parse is a message whose
/// end was never accounted for ([13 §4](../../../docs/13-http1-upstream.md)). Both paths
/// do it, so it is not one of the differences.
#[tokio::test]
async fn a_trailer_with_space_before_its_colon_is_refused() {
    never_finished(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-note : v\r\n\r\n"
            .to_owned(),
    )
    .await;
}

/// And a trailer section past its bound, which is the same bound argument as the head's:
/// the end of a message is not somewhere a peer may put as much as it likes.
#[tokio::test]
async fn a_trailer_section_past_its_bound_is_refused() {
    let fields = "x-note: ".to_owned() + &"a".repeat(32 * 1024) + "\r\n";
    never_finished(format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n{fields}\r\n"
    ))
    .await;
}

/// **A limit of this project's, and the one that sits against a MUST.**
/// [RFC 9110 §15.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.2) says a client
/// "MUST be able to parse one or more 1xx responses received prior to a final response",
/// and names no limit; the engine's client consumes as many as arrive and answers with
/// the final head behind them, which is what the specification asks for.
///
/// Ours stops at sixteen, because without a limit one upstream holds a worker for as long
/// as it cares to keep sending interim heads, and that is judged the worse failure. It is
/// a deliberate deviation and is recorded as one
/// ([13 §5](../../../docs/13-http1-upstream.md)); the test is here so that changing it
/// is a decision and not a drift.
#[tokio::test]
async fn an_upstream_that_floods_interim_heads_is_given_up_on_by_our_own_path() {
    let flood = "HTTP/1.1 103 Early Hints\r\n\r\n".repeat(20);
    let answer = format!("{flood}HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    match upstream_under_test() {
        Upstream::Ours => is_refused(answer).await,
        Upstream::Hyper => answered_anyway(answer).await,
    }
}

/// And the same deviation by the other measure: counting heads alone would let a peer
/// hold as much as it liked in sixteen of them, so there is a bound on what they come to
/// as well. Both are ours; the engine's client has neither.
#[tokio::test]
async fn an_upstream_whose_interim_heads_are_too_long_is_given_up_on_by_our_own_path() {
    let one = "HTTP/1.1 103 Early Hints\r\nLink: ".to_owned() + &"a".repeat(16 * 1024) + "\r\n\r\n";
    let answer = format!(
        "{}HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
        one.repeat(12)
    );
    match upstream_under_test() {
        Upstream::Ours => is_refused(answer).await,
        Upstream::Hyper => answered_anyway(answer).await,
    }
}

/// What the engine's client does with the four above: reads past what the candidate would
/// have stopped at, and delivers the message whole. Recorded so that the difference is a
/// measured thing rather than an assumption; in three of the four it is doing nothing
/// wrong ([13 §5](../../../docs/13-http1-upstream.md)).
async fn delivered_whole(answer: String) {
    let (proxy, accepts) = facing(answer).await;
    let mut client = Wire::to(proxy).await;
    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let body = within(client.chunked_body()).await;
    assert!(body.contains("hello"), "{body}");
    assert_eq!(accepts.load(Ordering::SeqCst), 1, "and kept the connection");
}

/// The same for the interim floods, where what arrives is the final head behind them.
async fn answered_anyway(answer: String) {
    let (proxy, accepts) = facing(answer).await;
    let mut client = Wire::to(proxy).await;
    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(2)).await, "ok");
    assert_eq!(accepts.load(Ordering::SeqCst), 1, "and kept the connection");
}

// ---- peers that stop part way, on either side ----

/// An upstream that refuses an upload it has not read, and says so while the bytes are
/// still arriving. The answer must reach the client rather than wait behind a socket
/// that will never drain ([13 §5](../../../docs/13-http1-upstream.md), RFC 9112 §9.5).
///
/// The upload runs in a task of its own, because a client whose upload is not being read
/// blocks, and a test that blocked with it would be waiting for its own answer.
#[tokio::test]
async fn a_refusal_reaches_the_client_though_the_upload_cannot_go() {
    let (backend, _accepts) = counted(|mut wire| async move {
        // The head, and then not another byte read: the upload backs up behind this.
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\nContent-Length: 7\r\n\r\nrefused").await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let stream = TcpStream::connect(proxy_to(backend).await).await.unwrap();
    let (mut reading, mut writing) = tokio::io::split(stream);

    // Set the moment anything comes back, which is what stops the upload.
    let answered = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&answered);
    let _uploading = tokio::spawn(async move {
        let head = "POST /first HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4194304\r\n\r\n";
        if writing.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        // **Bounded from both sides.** Enough to fill the staging and the socket behind
        // it, or nothing would be blocked and there would be no test here. Little enough
        // that the proxy has taken it all in by the time it answers: a connection closed
        // while unread bytes are still sitting in its receive buffer is reset rather than
        // closed, and a reset takes the answer that had already arrived with it. That is
        // TCP rather than anything this proxy decides, and half a megabyte here failed
        // about one run in two on Windows.
        let block = vec![b'x'; 16 * 1024];
        for _ in 0..10 {
            if stop.load(Ordering::SeqCst) || writing.write_all(&block).await.is_err() {
                return;
            }
        }
    });

    let mut seen = Vec::new();
    let mut bytes = [0; 4096];
    while find(&seen, b"\r\n\r\n").is_none() {
        let read = within(reading.read(&mut bytes)).await.unwrap();
        assert!(read > 0, "the connection ended before the refusal");
        seen.extend_from_slice(&bytes[..read]);
        // Answered, so there is nothing to be gained by sending the rest -- and a client
        // that writes on into a connection the other end has closed has its own receive
        // buffer reset out from under it, answer and all.
        answered.store(true, Ordering::SeqCst);
    }
    let seen = String::from_utf8_lossy(&seen).into_owned();
    assert!(seen.starts_with("HTTP/1.1 413"), "{seen}");
}

/// An upstream that is asked whether to send a body and says nothing is not waited on for
/// ever: the wait is short, and what follows it is the body, because an upstream that
/// does not answer the question is one that means to read it anyway.
#[tokio::test]
async fn a_withheld_body_goes_when_the_continue_wait_runs_out() {
    let (told, mut hears) = reporter();
    let (backend, _accepts) = counted(move |mut wire| {
        let told = told.clone();
        async move {
            // The head is read and nothing is said about it. No 100, no refusal.
            if wire.until(b"\r\n\r\n").await.is_some() {
                let body = wire.body(5).await;
                let _sent = told.send(body);
                wire.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    let mut client = Wire::to(proxy_to(backend).await).await;
    let began = std::time::Instant::now();
    client
        .write("POST /first HTTP/1.1\r\nHost: example.test\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello")
        .await;

    // The downstream server answers the expectation itself, so the client may see a 100
    // of its own before the answer; the upstream's silence is what is being measured.
    let mut head = within(client.head()).await;
    while head.starts_with("HTTP/1.1 1") {
        head = within(client.head()).await;
    }
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(2)).await, "ok");
    assert_eq!(hears.recv().await.unwrap(), "hello", "the body never went");

    // **Where the two paths differ, on purpose.** Ours holds the body back for the wait
    // in [13 §7](../../../docs/13-http1-upstream.md) and sends it when that runs out,
    // so this cannot finish sooner. The engine's client does not hold it back at all,
    // which is why the same test finishes at once there; the body arriving is what both
    // are held to, and the waiting is only ours.
    let waited = began.elapsed();
    match upstream_under_test() {
        Upstream::Ours => assert!(
            waited >= Duration::from_secs(1),
            "the body went without the wait: {waited:?}"
        ),
        Upstream::Hyper => assert!(
            waited < Duration::from_secs(1),
            "the engine's client waited after all: {waited:?}"
        ),
    }
}

/// A client that goes away part way through its upload leaves an upstream part way
/// through a request. Whatever the upstream does about that, the connection is not one
/// this end can account for, so the next request is given one of its own.
#[tokio::test]
async fn a_client_that_abandons_its_upload_costs_the_connection() {
    let (backend, accepts) = counted(|mut wire| async move {
        while wire.until(b"\r\n\r\n").await.is_some() {
            wire.write(FRESH).await;
        }
    });
    let proxy = proxy_to(backend).await;

    let gone = Wire::to(proxy).await;
    let mut gone = gone;
    gone.write("POST /first HTTP/1.1\r\nHost: example.test\r\nContent-Length: 100\r\n\r\nab")
        .await;
    // Two bytes of a hundred, and then the client is gone.
    drop(gone);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = Wire::to(proxy).await;
    let head = asks(&mut client, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(5)).await, "fresh");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        2,
        "an abandoned upload's connection"
    );
}

/// And a client that goes away part way through the answer. The upstream is left with a
/// message half delivered and nobody to deliver it to, which is the same connection in
/// the same unaccountable state.
#[tokio::test]
async fn a_client_that_abandons_the_answer_costs_the_connection() {
    let (backend, accepts) = counted(|mut wire| async move {
        while wire.until(b"\r\n\r\n").await.is_some() {
            wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await;
            // Bounded: a peer that talks for ever is a test run that never ends.
            for _ in 0..200 {
                wire.write("8\r\nxxxxxxxx\r\n").await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            wire.write("0\r\n\r\n").await;
        }
    });
    let proxy = proxy_to(backend).await;

    let mut gone = Wire::to(proxy).await;
    let head = asks(&mut gone, "/first").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    // The head and nothing more: the client leaves the answer where it is.
    drop(gone);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = Wire::to(proxy).await;
    let head = asks(&mut client, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        2,
        "an abandoned answer's connection"
    );
}

// ---- cases taken from other implementations' suites ----
//
// HAProxy's reg-tests, nginx's test suite, Envoy's, hyper's and Pingora's were read for
// cases this suite lacked. The inputs are theirs; the tests are written for this path.

/// A head that describes a body it does not send leaves nothing to wait for, and the
/// connection for the next request. Every one of those suites tests this.
#[tokio::test]
async fn a_body_described_and_not_sent_leaves_the_connection_for_the_next_request() {
    for (method, answer) in [
        ("HEAD", "HTTP/1.1 200 OK\r\nContent-Length: 26\r\n\r\n"),
        (
            "HEAD",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        ),
        (
            "GET",
            "HTTP/1.1 304 Not Modified\r\nContent-Length: 100\r\n\r\n",
        ),
        (
            "GET",
            "HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: chunked\r\n\r\n",
        ),
    ] {
        let (backend, accepts) = counted(move |mut wire| async move {
            if wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(answer).await;
            }
            plainly(wire).await;
        });
        let mut client = Wire::to(proxy_to(backend).await).await;
        client
            .write(&format!(
                "{method} /first HTTP/1.1\r\nHost: example.test\r\n\r\n"
            ))
            .await;
        let head = within(client.head()).await;
        assert!(head.starts_with(&answer[..12]), "{answer:?}: {head}");

        let head = asks(&mut client, "/second").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{answer:?}: {head}");
        assert_eq!(within(client.body(5)).await, "fresh", "{answer:?}");
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "{answer:?}: a finished answer's connection was not kept"
        );
    }
}

/// A body sent to a `HEAD` anyway is not the next answer, and the connection it came on
/// is not lent again (HAProxy's `http_bodyless_response.vtc`, nginx's
/// `proxy_extra_data.t`).
#[tokio::test]
async fn a_body_sent_to_a_head_request_is_never_the_next_answer() {
    for answer in [
        "HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nskipped data",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
    ] {
        let (backend, accepts) = hostile_first(move |mut wire| async move {
            if wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(answer).await;
            }
            // Still there, so that a connection wrongly kept would have something to give.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let mut client = Wire::to(proxy_to(backend).await).await;
        client
            .write("HEAD /first HTTP/1.1\r\nHost: example.test\r\n\r\n")
            .await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{answer:?}: {head}");

        let head = asks(&mut client, "/second").await;
        assert!(head.starts_with("HTTP/1.1 200"), "{answer:?}: {head}");
        assert_eq!(within(client.body(5)).await, "fresh", "{answer:?}");
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            2,
            "{answer:?}: a kept connection"
        );
    }
}

/// An answer that says it closes is delivered at its length, without waiting for a close
/// that may be slow to come, and its connection is not used again (nginx's
/// `proxy_noclose.t` and `proxy_keepalive.t`). The upstream here keeps the socket open and
/// goes on answering, so only the count of connections can tell.
#[tokio::test]
async fn an_answer_that_says_close_is_not_reused_though_the_socket_stays_open() {
    // An HTTP/1.0 answer that asks to be kept alive is kept by the engine's client, which
    // RFC 9112 §9.3 allows; ours never pools a connection that speaks 1.0 (13 §4), one
    // of 13 §5's open choices.
    let kept_alive = match upstream_under_test() {
        Upstream::Ours => 2,
        Upstream::Hyper => 1,
    };
    for (answer, connections) in [
        (
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nfresh",
            2,
        ),
        ("HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\nfresh", 2),
        (
            "HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nfresh",
            kept_alive,
        ),
    ] {
        let (backend, accepts) = counted(move |mut wire| async move {
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write(answer).await;
            }
        });
        let proxy = proxy_to(backend).await;
        for path in ["/first", "/second"] {
            // A client connection each: which version the answer is passed on in is the
            // test below, and not this one's business.
            let mut client = Wire::to(proxy).await;
            let head = asks(&mut client, path).await;
            assert!(head.contains(" 200 OK\r\n"), "{answer:?}: {head}");
            assert_eq!(within(client.body(5)).await, "fresh", "{answer:?}");
        }
        assert_eq!(accepts.load(Ordering::SeqCst), connections, "{answer:?}");
    }
}

/// An intermediary speaks its own version: "Intermediaries that process HTTP messages ...
/// MUST send their own HTTP-version in forwarded messages" (RFC 9110 §6.2). An upstream
/// that answers in HTTP/1.0 is answered on to an HTTP/1.1 client in HTTP/1.1, and one that
/// answers in HTTP/1.1 is answered on to an HTTP/1.0 client in 1.0, which is all that
/// client can be sent. Found while writing the test above.
#[tokio::test]
async fn an_answer_is_passed_on_in_the_proxys_own_version() {
    let proxy = proxy_to(
        counted(|mut wire| async move {
            while wire.until(b"\r\n\r\n").await.is_some() {
                wire.write("HTTP/1.0 200 OK\r\nContent-Length: 5\r\n\r\nfresh")
                    .await;
            }
        })
        .0,
    )
    .await;
    let mut client = Wire::to(proxy).await;
    let head = asks(&mut client, "/first").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    let proxy = proxy_to(counted(plainly).0).await;
    let mut old = Wire::to(proxy).await;
    old.write("GET /first HTTP/1.0\r\nHost: example.test\r\n\r\n")
        .await;
    let head = within(old.head()).await;
    assert!(head.starts_with("HTTP/1.0 200"), "{head}");
}

/// An idle connection the upstream resets, rather than closes, is as gone as one it
/// closed, and the next request is answered on a connection of its own (Pingora's pool
/// tests).
#[tokio::test]
async fn a_connection_reset_while_idle_is_not_handed_to_the_next_request() {
    let (backend, accepts) = hostile_first(|mut wire| async move {
        if wire.until(b"\r\n\r\n").await.is_some() {
            wire.write(FRESH).await;
        }
        // Not before the answer has been read: a reset throws away what the other end has
        // not read yet, on Windows at least, and the answer would go with it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // A linger of nothing makes the close a reset. It cannot block, which is what the
        // deprecation is about.
        #[expect(deprecated, reason = "a zero linger is how a test sends a reset")]
        wire.stream.set_linger(Some(Duration::ZERO)).unwrap();
    });
    let mut client = Wire::to(proxy_to(backend).await).await;

    assert!(
        asks(&mut client, "/first")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert_eq!(within(client.body(5)).await, "fresh");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let head = asks(&mut client, "/second").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.body(5)).await, "fresh");
    assert_eq!(accepts.load(Ordering::SeqCst), 2, "a reset connection");
}

/// A request whose `Connection` names its own framing field still goes upstream with
/// exactly one framing and the whole body: the field is hop-by-hop, but the framing is
/// the serialiser's to choose, not the headers' (Pingora's `test_upstream.rs`).
#[tokio::test]
async fn a_request_that_nominates_its_own_framing_still_arrives_whole() {
    for (named, framing, body) in [
        ("Content-Length", "Content-Length: 5\r\n", "hello"),
        (
            "Transfer-Encoding",
            "Transfer-Encoding: chunked\r\n",
            "5\r\nhello\r\n0\r\n\r\n",
        ),
    ] {
        let (saw, mut seen) = reporter();
        let upstream = raw_upstream(move |mut wire| {
            let saw = saw.clone();
            async move {
                let head = wire.head().await;
                let lower = head.to_ascii_lowercase();
                let body = if lower.contains("transfer-encoding: chunked") {
                    wire.chunked_body().await
                } else if lower.contains("content-length: 5") {
                    wire.body(5).await
                } else {
                    String::new()
                };
                saw.send(format!("{head}{body}")).unwrap();
                wire.write(FRESH).await;
            }
        });
        let mut client = Wire::to(proxy_to(upstream).await).await;
        client
            .write(&format!(
                "POST / HTTP/1.1\r\nHost: example.test\r\nConnection: {named}\r\n{framing}\r\n{body}"
            ))
            .await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{named}: {head}");

        let seen = within(seen.recv()).await.unwrap().to_ascii_lowercase();
        let framings =
            seen.matches("content-length:").count() + seen.matches("transfer-encoding:").count();
        assert_eq!(framings, 1, "{named}: {seen}");
        assert!(!seen.contains("\r\nconnection:"), "{named}: {seen}");
        assert!(seen.contains("hello"), "{named}: {seen}");
    }
}

/// Trailer sections that are not fields fail the exchange, on the wire as in the codec
/// (HAProxy's `http_transfer_encoding.vtc`).
#[tokio::test]
async fn a_trailer_section_that_is_not_fields_is_refused() {
    for trailer in [
        "x tlr: value\r\n",
        ":status: 200\r\n",
        "x-a: val\rue\r\n",
        "x-a: 1\r\n folded\r\n",
        "x-a: \x00\r\n",
    ] {
        never_finished(format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n{trailer}\r\n"
        ))
        .await;
    }
}

/// A `POST` that says it carries nothing still says so upstream. RFC 9110 §8.6 has a user
/// agent send a length for a method that defines a meaning for content, some servers
/// answer 411 without one, and the message that arrived had it (HAProxy's
/// `h1_to_h1.vtc`). The engine's client keeps it.
#[tokio::test]
async fn a_post_of_nothing_still_says_its_length() {
    let (saw, mut seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            saw.send(wire.head().await).unwrap();
            wire.write(FRESH).await;
        }
    });
    let mut client = Wire::to(proxy_to(upstream).await).await;
    client
        .write("POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 0\r\n\r\n")
        .await;
    within(client.head()).await;
    let seen = within(seen.recv()).await.unwrap().to_ascii_lowercase();
    assert!(seen.contains("content-length: 0\r\n"), "{seen}");
}

// ---- HTTP/2 in, HTTP/1.1 out ----

/// A request body for an HTTP/2 client: nothing at all, or nothing after a pause, which
/// is a HEADERS frame without END_STREAM followed by an empty DATA frame that has it.
#[derive(Debug)]
enum Upload {
    None,
    EmptyLater(std::pin::Pin<Box<tokio::time::Sleep>>),
}

impl hyper::body::Body for Upload {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
        match &mut *self {
            Self::None => std::task::Poll::Ready(None),
            Self::EmptyLater(sleep) => sleep.as_mut().poll(cx).map(|()| None),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// An HTTP/2 connection to the proxy, made here rather than taken from a pool.
async fn h2_to(proxy: SocketAddr) -> hyper::client::conn::http2::SendRequest<Upload> {
    let stream = TcpStream::connect(proxy).await.unwrap();
    let (sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(connection);
    sender
}

/// An upstream that reports each request's head, and whatever chunked body followed it
/// within a second, and answers plainly.
fn reporting_upstream() -> (SocketAddr, mpsc::UnboundedReceiver<String>) {
    let (saw, seen) = reporter();
    let upstream = raw_upstream(move |mut wire| {
        let saw = saw.clone();
        async move {
            while let Some(head) = wire.until(b"\r\n\r\n").await {
                let head = String::from_utf8(head).unwrap();
                let chunked = head
                    .to_ascii_lowercase()
                    .contains("transfer-encoding: chunked");
                let body = if chunked {
                    tokio::time::timeout(Duration::from_secs(1), wire.until(b"0\r\n\r\n"))
                        .await
                        .ok()
                        .flatten()
                        .map(|body| String::from_utf8(body).unwrap())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                saw.send(format!("{head}{body}")).unwrap();
                wire.write(FRESH).await;
            }
        }
    });
    (upstream, seen)
}

/// A request whose HEADERS frame ends the stream has no body, and goes upstream with no
/// framing at all (nginx's `h2_request_body.t`).
#[tokio::test]
async fn an_http2_request_that_ends_with_its_headers_goes_up_with_no_framing() {
    let (upstream, mut seen) = reporting_upstream();
    let proxy = proxy_to(upstream).await;
    let mut sender = h2_to(proxy).await;
    let request = http::Request::post(format!("http://{proxy}/x"))
        .body(Upload::None)
        .unwrap();
    let answer = within(sender.send_request(request)).await.unwrap();
    assert_eq!(answer.status(), 200);
    let seen = within(seen.recv()).await.unwrap().to_ascii_lowercase();
    assert!(!seen.contains("content-length"), "{seen}");
    assert!(!seen.contains("transfer-encoding"), "{seen}");
}

/// A request whose body turns out to be empty only when its stream ends: no length was
/// ever said, so it is framed in chunks and ended by one (13 §4: "do not infer absence
/// merely from a missing CL on H2"; nginx's `h2_proxy_request_buffering.t`). The engine's
/// client takes a body already over as an absent one, which is one of 13 §5's differences.
#[tokio::test]
async fn an_http2_request_that_ends_with_an_empty_frame_is_ended_in_chunks() {
    let (upstream, mut seen) = reporting_upstream();
    let proxy = proxy_to(upstream).await;
    let mut sender = h2_to(proxy).await;
    let later = Upload::EmptyLater(Box::pin(tokio::time::sleep(Duration::from_millis(200))));
    let request = http::Request::post(format!("http://{proxy}/x"))
        .body(later)
        .unwrap();
    let answer = within(sender.send_request(request)).await.unwrap();
    assert_eq!(answer.status(), 200);
    let seen = within(seen.recv()).await.unwrap().to_ascii_lowercase();
    assert!(!seen.contains("content-length"), "{seen}");
    match upstream_under_test() {
        Upstream::Ours => {
            assert!(seen.contains("transfer-encoding: chunked\r\n"), "{seen}");
            assert!(seen.ends_with("\r\n\r\n0\r\n\r\n"), "{seen}");
        }
        Upstream::Hyper => assert!(seen.ends_with("\r\n\r\n"), "{seen}"),
    }
}

/// An answer of trailers and no data reaches an HTTP/2 client as HEADERS and trailers,
/// with no DATA frame between them (nginx's `h2_trailers.t`).
#[tokio::test]
async fn an_answer_of_trailers_alone_reaches_an_http2_client_as_trailers() {
    let upstream = raw_upstream(|mut wire| async move {
        wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx-var: v\r\n\r\n")
            .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let proxy = proxy_to(upstream).await;
    let mut sender = h2_to(proxy).await;
    let request = http::Request::get(format!("http://{proxy}/x"))
        .header("te", "trailers")
        .body(Upload::None)
        .unwrap();
    let answer = within(sender.send_request(request)).await.unwrap();
    assert_eq!(answer.status(), 200);
    let mut body = answer.into_body();
    let mut data = 0;
    let mut trailers = None;
    while let Some(frame) = within(http_body_util::BodyExt::frame(&mut body)).await {
        match frame.unwrap().into_data() {
            Ok(bytes) => data += bytes.len(),
            Err(frame) => trailers = frame.into_trailers().ok(),
        }
    }
    assert_eq!(data, 0);
    assert_eq!(trailers.expect("trailers")["x-var"], "v");
}

/// A chunked answer that says it closes, sent a piece at a time and closed straight
/// after its last chunk, reaches an HTTP/2 client whole and ended cleanly (HAProxy's
/// `truncated.vtc`).
#[tokio::test]
async fn a_chunked_answer_closed_right_after_its_end_reaches_an_http2_client_whole() {
    let upstream = raw_upstream(|mut wire| async move {
        wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await;
        let chunk = format!("32f\r\n{}\r\n", "x".repeat(815));
        for _ in 0..20 {
            wire.write(&chunk).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        wire.write("0\r\n\r\n").await;
        // And closed, which the return does.
    });
    let proxy = proxy_to(upstream).await;
    let mut sender = h2_to(proxy).await;
    let request = http::Request::get(format!("http://{proxy}/x"))
        .body(Upload::None)
        .unwrap();
    let answer = within(sender.send_request(request)).await.unwrap();
    assert_eq!(answer.status(), 200);
    let body = within(http_body_util::BodyExt::collect(answer.into_body()))
        .await
        .expect("a body that ended cleanly");
    assert_eq!(body.to_bytes().len(), 20 * 815);
}
