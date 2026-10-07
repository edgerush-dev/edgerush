//! WebSocket over HTTP/1.1, on the wire on both sides
//! ([19](../../../docs/19-websocket.md)).
//!
//! The backend here is a socket the test writes to, so that it can answer a handshake the
//! way a real one does and the ways a broken or hostile one might: a 101 for another key,
//! for another protocol, or none at all. The client is raw for the same reason.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use bytes::Bytes;
use edgerush_config::{Config, compile};
use edgerush_proxy::connections::{Loads, QUIC_MOST};
use edgerush_proxy::{H1Limits, Proxy, Worker};
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// RFC 6455 §1.3's example key, and the Accept a server that read it answers with.
const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
const ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

/// A handshake for `/chat`, as a browser sends it.
fn handshake() -> String {
    format!(
        "GET /chat HTTP/1.1\r\nhost: chat.test\r\nupgrade: websocket\r\nconnection: Upgrade\r\n\
         sec-websocket-key: {KEY}\r\nsec-websocket-version: 13\r\norigin: https://chat.test\r\n\r\n"
    )
}

/// A test that waits for what never comes should fail, not hang.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

/// A gateway whose one rule sends everything to `upstream`, the rule's `forward` given
/// `extra` and the config `upstreams` besides; and the data plane, for its metrics.
fn gateway(upstream: SocketAddr, extra: &str, upstreams: &str) -> (SocketAddr, Arc<Proxy>) {
    gateway_counted(upstream, extra, upstreams, None)
}

/// The same, its worker the first of a process whose connections `connections` counts.
fn gateway_counted(
    upstream: SocketAddr,
    extra: &str,
    upstreams: &str,
    connections: Option<Arc<Loads>>,
) -> (SocketAddr, Arc<Proxy>) {
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
{extra}
upstreams:
  up: {{ load_balancer: p2c, endpoints: ["{upstream}"] }}
{upstreams}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    let serving = Arc::clone(&proxy);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let entered = runtime.enter();
        let socket = TcpListener::from_std(socket).unwrap();
        let worker = match connections {
            Some(connections) => Worker::at(serving, H1Limits::default(), 0, connections),
            None => Worker::new(serving),
        };
        local.spawn_local(std::rc::Rc::clone(&worker).maintain());
        local.spawn_local(worker.serve(0, socket));
        drop(entered);
        runtime.block_on(local);
    });
    (address, proxy)
}

/// The plain rule: everything forwarded to `up`.
const FORWARD: &str = "        forward: { backends: [{ upstream: up, weight: 1 }] }";

/// A backend that is a socket and nothing more: every connection it accepts is handed to
/// `answer`. Says where it listens, and counts the connections it has accepted.
fn backend<F, Fut>(answer: F) -> (SocketAddr, Arc<AtomicUsize>)
where
    F: Fn(Wire) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&accepted);
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
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(answer(Wire::new(stream)));
            }
        });
    });
    (address, accepted)
}

/// One end of a connection, read and written as bytes.
struct Wire {
    stream: TcpStream,
    buffered: Vec<u8>,
}

impl Wire {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffered: Vec::new(),
        }
    }

    async fn to(address: SocketAddr) -> Self {
        Self::new(TcpStream::connect(address).await.unwrap())
    }

    async fn write(&mut self, bytes: &str) {
        self.stream.write_all(bytes.as_bytes()).await.unwrap();
    }

    /// Ends this side's sending, as a WebSocket's clean close ends its TCP connection.
    async fn finish(&mut self) {
        self.stream.shutdown().await.unwrap();
    }

    /// Reads up to and including the empty line that ends a head.
    async fn head(&mut self) -> String {
        let head = self.until(b"\r\n\r\n").await.expect("a whole head");
        String::from_utf8(head).unwrap()
    }

    /// Reads exactly `count` bytes.
    async fn exactly(&mut self, count: usize) -> String {
        while self.buffered.len() < count {
            assert!(self.more().await, "the connection ended early");
        }
        let rest = self.buffered.split_off(count);
        String::from_utf8(std::mem::replace(&mut self.buffered, rest)).unwrap()
    }

    /// Reads until `mark`, and returns everything up to and including it; `None` if the
    /// connection ended first.
    async fn until(&mut self, mark: &[u8]) -> Option<Vec<u8>> {
        loop {
            if let Some(at) = self
                .buffered
                .windows(mark.len())
                .position(|window| window == mark)
            {
                let rest = self.buffered.split_off(at + mark.len());
                return Some(std::mem::replace(&mut self.buffered, rest));
            }
            if !self.more().await {
                return None;
            }
        }
    }

    /// Everything that arrives until the connection ends, however it ends.
    async fn rest(&mut self) -> String {
        let mut bytes = [0; 4096];
        loop {
            match within(self.stream.read(&mut bytes)).await {
                Ok(0) | Err(_) => break,
                Ok(read) => self.buffered.extend_from_slice(&bytes[..read]),
            }
        }
        String::from_utf8_lossy(&std::mem::take(&mut self.buffered)).into_owned()
    }

    /// Reads whatever has arrived. False when the peer has closed.
    async fn more(&mut self) -> bool {
        let mut bytes = [0; 4096];
        let read = within(self.stream.read(&mut bytes)).await.unwrap_or(0);
        self.buffered.extend_from_slice(&bytes[..read]);
        read > 0
    }
}

/// The value of the first field `name` in `head`, which is in lower case.
fn field<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").skip(1).find_map(|line| {
        let (field, value) = line.split_once(':')?;
        field.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// The Accept a server that read `key` answers with (RFC 6455 §4.2.2).
fn accept_of(key: &str) -> String {
    let mut hashed = key.as_bytes().to_vec();
    hashed.extend_from_slice(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64(&boring::sha::sha1(&hashed))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for group in bytes.chunks(3) {
        let at = |index: usize| u32::from(group.get(index).copied().unwrap_or(0));
        let joined = at(0) << 16 | at(1) << 8 | at(2);
        for place in 0..4 {
            out.push(if place <= group.len() {
                char::from(ALPHABET[((joined >> (18 - 6 * place)) & 0x3f) as usize])
            } else {
                '='
            });
        }
    }
    out
}

/// A backend that speaks WebSocket as far as the handshake goes: it tells `saw` the head it
/// was sent, answers with a 101 carrying the Accept of the key in it and `hello` in the same
/// write, then sends back everything it reads until the gateway's side closes, and closes.
fn echoing(saw: mpsc::UnboundedSender<String>) -> (SocketAddr, Arc<AtomicUsize>) {
    backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            let accept = accept_of(field(&head, "sec-websocket-key").unwrap_or_default());
            let _told = saw.send(head);
            wire.write(&format!(
                "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                 connection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\
                 sec-websocket-protocol: chat\r\n\r\nhello"
            ))
            .await;
            let mut bytes = [0; 4096];
            let mut first = true;
            loop {
                let read = match within(wire.stream.read(&mut bytes)).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                let mut said = String::from_utf8_lossy(&bytes[..read]).into_owned();
                if first {
                    // What the client sent before it heard the 101 comes first.
                    let _told = saw.send(format!("first: {said}"));
                    first = false;
                    said = format!("[{said}]");
                }
                wire.stream.write_all(said.as_bytes()).await.unwrap();
            }
            wire.write("bye").await;
            wire.finish().await;
        }
    })
}

/// The value of one sample of the data plane's metrics, by its whole name and labels.
fn sample(proxy: &Proxy, series: &str) -> Option<u64> {
    proxy.metrics().lines().find_map(|line| {
        line.strip_prefix(series)
            .and_then(|rest| rest.trim().parse().ok())
    })
}

/// Waits a little for `series` to reach `value`: a tunnel is counted as it ends, which is
/// after its last byte has gone.
async fn counted(proxy: &Proxy, series: &str, value: u64) {
    let began = Instant::now();
    while sample(proxy, series) != Some(value) {
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "{series} is {:?}, not {value}",
            sample(proxy, series)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_websocket_is_carried_both_ways_once_its_101_has_gone() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, accepted) = echoing(saw);
    let (address, proxy) = gateway(upstream, FORWARD, "");
    let mut client = Wire::to(address).await;
    // Bytes after the handshake, sent before the 101 could have come: RFC 6455 has a client
    // wait, but nothing of them may be lost or read as HTTP.
    client.write(&format!("{}early", handshake())).await;

    // What the backend was asked: the gateway's own handshake, not the client's key.
    let asked = within(seen.recv()).await.unwrap();
    assert!(asked.starts_with("GET /chat HTTP/1.1\r\n"), "{asked}");
    assert_eq!(field(&asked, "upgrade"), Some("websocket"), "{asked}");
    assert_eq!(field(&asked, "connection"), Some("upgrade"), "{asked}");
    let ours = field(&asked, "sec-websocket-key").unwrap();
    assert_ne!(ours, KEY, "the client's key went on");
    assert_eq!(ours.len(), 24, "{asked}");
    assert_eq!(
        field(&asked, "sec-websocket-version"),
        Some("13"),
        "{asked}"
    );
    assert_eq!(
        field(&asked, "origin"),
        Some("https://chat.test"),
        "{asked}"
    );
    assert!(field(&asked, "x-forwarded-for").is_some(), "{asked}");

    let head = within(client.head()).await;
    assert!(
        head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{head}"
    );
    assert_eq!(field(&head, "upgrade"), Some("websocket"), "{head}");
    assert_eq!(field(&head, "connection"), Some("upgrade"), "{head}");
    assert_eq!(field(&head, "sec-websocket-accept"), Some(ACCEPT), "{head}");
    assert_eq!(
        field(&head, "sec-websocket-protocol"),
        Some("chat"),
        "{head}"
    );
    assert!(field(&head, "x-request-id").is_some(), "{head}");
    assert!(field(&head, "content-length").is_none(), "{head}");
    assert!(field(&head, "transfer-encoding").is_none(), "{head}");
    // What the backend sent with its 101 comes first.
    assert_eq!(within(client.exactly(5)).await, "hello");
    assert_eq!(within(seen.recv()).await.unwrap(), "first: early");
    assert_eq!(within(client.exactly(7)).await, "[early]");

    client.write("ping").await;
    assert_eq!(within(client.exactly(4)).await, "ping");
    // A clean end is passed on, and the backend's own goes back.
    client.finish().await;
    assert_eq!(client.rest().await, "bye");
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#,
        1,
    )
    .await;
    assert_eq!(
        sample(
            &proxy,
            r#"edgerush_listener_responses_total{listener="web",class="1xx"}"#
        ),
        Some(1)
    );
}

/// A backend that fetches URLs for its clients can be made to hand back a 101 an attacker
/// wrote, and a gateway that switched on it would give the client a raw pipe past routing
/// (19 §2). The client picks its own key and can work out the Accept of it, so a 101 is
/// held to the Accept of the gateway's key, which the client never saw.
#[tokio::test]
async fn a_101_carrying_the_accept_of_the_clients_key_is_refused() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, _) = backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            let _head = wire.head().await;
            wire.write(&format!(
                "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                 connection: upgrade\r\nsec-websocket-accept: {ACCEPT}\r\n\r\nTUNNELLED"
            ))
            .await;
            // What the gateway sent after the 101, until it closed the connection.
            let _told = saw.send(within(wire.rest()).await);
        }
    });
    let (address, proxy) = gateway(upstream, FORWARD, "");
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;

    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert!(field(&head, "upgrade").is_none(), "{head}");
    let length: usize = field(&head, "content-length").unwrap().parse().unwrap();
    assert!(!within(client.exactly(length)).await.contains("TUNNELLED"));
    // The backend's connection is closed rather than kept or tunnelled: what the client
    // sends now is no WebSocket message of the backend's.
    client
        .write("GET /x HTTP/1.1\r\nhost: chat.test\r\n\r\n")
        .await;
    assert_eq!(
        within(seen.recv()).await.unwrap(),
        "",
        "sent on after the 101"
    );
    assert_eq!(
        sample(
            &proxy,
            r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#
        ),
        Some(0)
    );
}

/// A 101 that does not say it switched to WebSocket, with the Accept of the gateway's key,
/// is the backend failing: 502, and the connection it came on closed.
#[tokio::test]
async fn a_101_that_is_not_the_switch_asked_for_is_answered_502() {
    for (why, answer) in [
        (
            "another protocol",
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: h2c\r\nconnection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n",
        ),
        (
            "no Connection naming upgrade",
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nsec-websocket-accept: {accept}\r\n\r\n",
        ),
        (
            "no Accept",
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\n\r\n",
        ),
        (
            "a body",
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\nsec-websocket-accept: {accept}\r\ncontent-length: 2\r\n\r\nhi",
        ),
    ] {
        let (upstream, _) = backend(move |mut wire| async move {
            let head = wire.head().await;
            let accept = accept_of(field(&head, "sec-websocket-key").unwrap());
            wire.write(&answer.replace("{accept}", &accept)).await;
            let _closed = within(wire.rest()).await;
        });
        let (address, _) = gateway(upstream, FORWARD, "");
        let mut client = Wire::to(address).await;
        client.write(&handshake()).await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 502"), "{why}: {head}");
    }
}

/// A refused handshake is an ordinary answer (19 §2): the client's connection goes on as
/// HTTP, and the backend's goes back to the pool.
#[tokio::test]
async fn a_refused_handshake_leaves_both_connections_to_carry_requests() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, accepted) = backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            let _told = saw.send(wire.head().await);
            wire.write("HTTP/1.1 403 Forbidden\r\ncontent-length: 2\r\n\r\nno")
                .await;
            if let Some(next) = wire.until(b"\r\n\r\n").await {
                let _told = saw.send(String::from_utf8(next).unwrap());
                wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await;
            }
            let _closed = within(wire.rest()).await;
        }
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 403"), "{head}");
    assert!(field(&head, "upgrade").is_none(), "{head}");
    assert_eq!(within(client.exactly(2)).await, "no");

    client
        .write("GET /next HTTP/1.1\r\nhost: chat.test\r\n\r\n")
        .await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(within(client.exactly(2)).await, "ok");
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(field(&asked, "upgrade"), Some("websocket"), "{asked}");
    let next = within(seen.recv()).await.unwrap();
    assert!(next.starts_with("GET /next "), "{next}");
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "the backend's was not reused"
    );
}

/// What follows a refused handshake on the client's connection is read as requests and
/// routed like any other, never carried to the backend as bytes: a proxy that switched on
/// the handshake alone let a refused one carry requests past routing (WebSocket smuggling,
/// Varnish and Envoy to 1.8). And a 426 that asks for WebSocket says so to the client, the
/// one protocol the gateway can switch to (RFC 9110 §15.5.22).
#[tokio::test]
async fn requests_after_a_refused_handshake_are_routed_not_tunnelled() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, _) = backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            let _head = wire.head().await;
            wire.write(concat!(
                "HTTP/1.1 426 Upgrade Required\r\nupgrade: h2c, websocket\r\n",
                "connection: upgrade\r\nsec-websocket-version: 13\r\n",
                "content-length: 0\r\n\r\n"
            ))
            .await;
            let next = wire.head().await;
            let _told = saw.send(next);
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            let _closed = within(wire.rest()).await;
        }
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let mut client = Wire::to(address).await;
    client
        .write(&format!(
            "{}GET /admin HTTP/1.1\r\nhost: chat.test\r\nx-forwarded-for: 10.9.9.9\r\n\r\n",
            handshake().replace("version: 13", "version: 8")
        ))
        .await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 426"), "{head}");
    assert_eq!(field(&head, "upgrade"), Some("websocket"), "{head}");
    assert_eq!(field(&head, "connection"), Some("upgrade"), "{head}");
    assert_eq!(field(&head, "sec-websocket-version"), Some("13"), "{head}");

    // The next request went through the gateway as a request: its forwarding fields are
    // the gateway's, not what the client wrote.
    let next = within(seen.recv()).await.unwrap();
    assert!(next.starts_with("GET /admin HTTP/1.1\r\n"), "{next}");
    assert_eq!(field(&next, "x-forwarded-for"), Some("127.0.0.1"), "{next}");
    assert!(field(&next, "via").is_some(), "{next}");
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
}

/// A 426 that asks for no protocol the gateway can switch to reaches the client without
/// `Upgrade`: naming one would offer what the gateway cannot do.
#[tokio::test]
async fn a_426_for_another_protocol_is_forwarded_without_its_upgrade() {
    let (upstream, _) = backend(|mut wire| async move {
        let _head = wire.head().await;
        wire.write(concat!(
            "HTTP/1.1 426 Upgrade Required\r\nupgrade: h2c\r\n",
            "content-length: 4\r\n\r\nplea"
        ))
        .await;
        let _closed = within(wire.rest()).await;
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let mut client = Wire::to(address).await;
    client
        .write("GET /up HTTP/1.1\r\nhost: a.test\r\n\r\n")
        .await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 426"), "{head}");
    assert!(field(&head, "upgrade").is_none(), "{head}");
    assert!(field(&head, "connection").is_none(), "{head}");
    assert_eq!(within(client.exactly(4)).await, "plea");
}

/// What falls short of a handshake is served as a plain request, its `Upgrade` taken off
/// (RFC 9110 §7.8); the backend refuses it as it sees fit.
#[tokio::test]
async fn what_falls_short_of_a_handshake_goes_as_a_plain_request() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, _) = backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            while let Some(head) = wire.until(b"\r\n\r\n").await {
                let head = String::from_utf8(head).unwrap();
                if let Some(length) = field(&head, "content-length") {
                    let _body = wire.exactly(length.parse().unwrap()).await;
                }
                let _told = saw.send(head);
                wire.write("HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        }
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let good = handshake();
    for (why, request) in [
        ("POST", good.replacen("GET", "POST", 1)),
        (
            "no key",
            good.replace(&format!("sec-websocket-key: {KEY}\r\n"), ""),
        ),
        ("a short key", good.replace(KEY, "dGhlIHNhbXBsZQ==")),
        ("h2c", good.replace("upgrade: websocket", "upgrade: h2c")),
        (
            "no Connection naming it",
            good.replace("connection: Upgrade", "connection: keep-alive"),
        ),
        (
            "a body",
            good.replace("\r\n\r\n", "\r\ncontent-length: 2\r\n\r\nhi"),
        ),
    ] {
        let mut client = Wire::to(address).await;
        client.write(&request).await;
        let asked = within(seen.recv()).await.unwrap();
        assert!(field(&asked, "upgrade").is_none(), "{why}: {asked}");
        assert!(field(&asked, "connection").is_none(), "{why}: {asked}");
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 400"), "{why}: {head}");
    }
    // HTTP/1.0 has no upgrade: RFC 9110 §7.8 has a server ignore one.
    let mut client = Wire::to(address).await;
    client.write(&good.replace("HTTP/1.1", "HTTP/1.0")).await;
    let asked = within(seen.recv()).await.unwrap();
    assert!(field(&asked, "upgrade").is_none(), "HTTP/1.0: {asked}");
}

/// A rule's `request_ms` is for the handshake, not the WebSocket: it runs until the 101,
/// then stops (19 §5).
#[tokio::test]
async fn a_rules_request_timeout_runs_until_the_101_and_not_after() {
    let slow = "        forward: { backends: [{ upstream: up, weight: 1 }], timeouts: { request_ms: 300 } }";
    // A backend that never answers the handshake is out of time.
    let (upstream, _) = backend(|mut wire| async move {
        let _head = wire.head().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
    });
    let (address, _) = gateway(upstream, slow, "");
    let mut client = Wire::to(address).await;
    let began = Instant::now();
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 504"), "{head}");
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );

    // One that answers is not cut off at the same bound once the switch is made.
    let (saw, _seen) = mpsc::unbounded_channel();
    let (upstream, _) = echoing(saw);
    let (address, _) = gateway(upstream, slow, "");
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert_eq!(within(client.exactly(5)).await, "hello");
    tokio::time::sleep(Duration::from_millis(900)).await;
    client.write("still").await;
    assert_eq!(within(client.exactly(7)).await, "[still]");
}

/// An open WebSocket that carries nothing either way for its rule's `tunnel_idle_ms` is
/// closed, and counted as idle (19 §5).
#[tokio::test]
async fn a_quiet_websocket_is_closed_at_its_rules_idle_bound() {
    let quiet = "        forward: { backends: [{ upstream: up, weight: 1 }], timeouts: { tunnel_idle_ms: 300 } }";
    let (saw, _seen) = mpsc::unbounded_channel();
    let (upstream, _) = echoing(saw);
    let (address, proxy) = gateway(upstream, quiet, "");
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert_eq!(within(client.exactly(5)).await, "hello");
    // Traffic keeps it open past its bound.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.write("tick").await;
    }
    assert_eq!(within(client.exactly(18)).await, "[tick]tickticktick");
    let began = Instant::now();
    // Then quiet: closed at the bound, not sooner and not much later.
    assert_eq!(client.rest().await, "");
    let after = began.elapsed();
    assert!(
        after >= Duration::from_millis(250) && after < Duration::from_secs(3),
        "{after:?}"
    );
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="idle"}"#,
        1,
    )
    .await;
}

/// A mirror could only ever be sent a WebSocket's handshake, never its messages, so it is
/// sent nothing, and what it did not get is counted (19 §5).
#[tokio::test]
async fn a_handshake_is_not_mirrored() {
    let (mirrored, mut copies) = mpsc::unbounded_channel();
    let (mirror, _) = backend(move |mut wire| {
        let mirrored = mirrored.clone();
        async move {
            let head = wire.head().await;
            let _told = mirrored.send(head);
            wire.write("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
        }
    });
    let (saw, _seen) = mpsc::unbounded_channel();
    let (upstream, _) = echoing(saw);
    let rule = "        filters: [{ type: request_mirror, upstream: copy, fraction: { numerator: 1, denominator: 1 } }]\n        forward: { backends: [{ upstream: up, weight: 1 }] }";
    let (address, proxy) = gateway(
        upstream,
        rule,
        &format!("  copy: {{ load_balancer: p2c, endpoints: [\"{mirror}\"] }}"),
    );
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    counted(
        &proxy,
        r#"edgerush_upstream_mirrors_given_up_total{upstream="copy",reason="upgrade"}"#,
        1,
    )
    .await;
    // A plain request on another connection is mirrored as ever, and is the first copy.
    let mut plain = Wire::to(address).await;
    plain
        .write("GET /plain HTTP/1.1\r\nhost: chat.test\r\n\r\n")
        .await;
    let copy = within(copies.recv()).await.unwrap();
    assert!(copy.starts_with("GET /plain "), "{copy}");
}

// ---- HTTP/2: extended CONNECT (RFC 8441), 19 §3 and §4 ----

/// A client of the gateway over HTTP/2 with prior knowledge, once the gateway's SETTINGS
/// have been heard; says whether they announced extended CONNECT.
async fn h2_client(address: SocketAddr) -> (h2::client::SendRequest<Bytes>, bool) {
    let socket = TcpStream::connect(address).await.unwrap();
    let (send, connection) = h2::client::handshake(socket).await.unwrap();
    tokio::spawn(async move {
        let _ended = connection.await;
    });
    let mut send = within(send.ready()).await.unwrap();
    // SETTINGS come first, but are read as the connection's task runs.
    let mut announced = false;
    for _ in 0..200 {
        announced = send.is_extended_connect_protocol_enabled();
        if announced {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        send = within(send.ready()).await.unwrap();
    }
    (send, announced)
}

/// An extended CONNECT for `protocol` at `/chat`, as a browser sends one over HTTP/2.
fn connect_for(protocol: &str) -> http::Request<()> {
    let mut request = http::Request::builder()
        .method(http::Method::CONNECT)
        .uri("http://chat.test/chat")
        .header("sec-websocket-version", "13")
        .header("origin", "https://chat.test")
        .body(())
        .unwrap();
    request
        .extensions_mut()
        .insert(h2::ext::Protocol::from(protocol));
    request
}

/// Everything received on `body` until its end.
async fn all_of(body: &mut h2::RecvStream) -> Vec<u8> {
    let mut received = Vec::new();
    while let Some(data) = within(body.data()).await {
        let data = data.unwrap();
        let _released = body.flow_control().release_capacity(data.len());
        received.extend_from_slice(&data);
    }
    received
}

/// The next `count` bytes received on `body`.
async fn next_of(body: &mut h2::RecvStream, count: usize, kept: &mut Vec<u8>) -> Vec<u8> {
    while kept.len() < count {
        let data = within(body.data())
            .await
            .expect("the stream ended")
            .unwrap();
        let _released = body.flow_control().release_capacity(data.len());
        kept.extend_from_slice(&data);
    }
    let rest = kept.split_off(count);
    std::mem::replace(kept, rest)
}

/// An HTTP/2 client's WebSocket is carried to an HTTP/1.1 backend as RFC 6455's handshake,
/// with a key of the gateway's own: the client is told 200 with no Accept, and its stream
/// carries the bytes both ways, END_STREAM each way a half-close (RFC 8441 §5).
#[tokio::test]
async fn an_http2_websocket_is_carried_to_an_http1_backend() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, _) = echoing(saw);
    let (address, proxy) = gateway(upstream, FORWARD, "");
    let (mut send, announced) = h2_client(address).await;
    assert!(announced, "extended CONNECT was not announced");
    let (response, mut stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("sec-websocket-accept").is_none());
    assert!(response.headers().get("upgrade").is_none());

    let asked = within(seen.recv()).await.unwrap();
    assert!(asked.starts_with("GET /chat HTTP/1.1\r\n"), "{asked}");
    assert_eq!(field(&asked, "upgrade"), Some("websocket"), "{asked}");
    assert_eq!(field(&asked, "connection"), Some("upgrade"), "{asked}");
    assert_eq!(
        field(&asked, "sec-websocket-key").map(str::len),
        Some(24),
        "{asked}"
    );
    assert_eq!(
        field(&asked, "sec-websocket-version"),
        Some("13"),
        "{asked}"
    );

    let mut body = response.into_body();
    let mut kept = Vec::new();
    assert_eq!(next_of(&mut body, 5, &mut kept).await, b"hello");
    stream
        .send_data(Bytes::from_static(b"ping"), false)
        .unwrap();
    assert_eq!(next_of(&mut body, 6, &mut kept).await, b"[ping]");
    stream.send_data(Bytes::new(), true).unwrap();
    let mut rest = kept;
    rest.extend(all_of(&mut body).await);
    assert_eq!(rest, b"bye");
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#,
        1,
    )
    .await;
    assert_eq!(
        sample(
            &proxy,
            r#"edgerush_listener_responses_total{listener="web",class="2xx"}"#
        ),
        Some(1)
    );
}

/// A WebSocket an HTTP/2 client asks for counts as one of its worker's connections for as
/// long as it is open, and is answered 503 while there is no room for one, without a
/// backend connection being opened for it; one that closes gives its room back. An HTTP/1.1
/// client's is counted through its own connection, and is not refused (03 §9).
#[tokio::test]
async fn http2_websockets_count_as_connections_of_their_worker() {
    let (saw, _seen) = mpsc::unbounded_channel();
    let (upstream, accepted) = echoing(saw);
    // Room for two connections, on one worker and one listener.
    let connections = Loads::new(1, 2, QUIC_MOST, 1);
    let (address, proxy) = gateway_counted(upstream, FORWARD, "", Some(Arc::clone(&connections)));
    let (mut send, announced) = h2_client(address).await;
    assert!(announced, "extended CONNECT was not announced");
    let mut open = Vec::new();
    let mut statuses = Vec::new();
    for _ in 0..4 {
        send = within(send.ready()).await.unwrap();
        let (response, stream) = send.send_request(connect_for("websocket"), false).unwrap();
        let response = within(response).await.unwrap();
        statuses.push(response.status().as_u16());
        if response.status() == 200 {
            // Kept, so that the tunnel stays open.
            open.push((stream, response.into_body()));
        }
    }
    assert_eq!(statuses, [200, 200, 503, 503]);
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "a refused one went upstream"
    );
    assert_eq!(connections.now(), [2]);
    let refused = r#"edgerush_listener_local_answers_total{listener="web",reason="no_room"}"#;
    assert_eq!(sample(&proxy, refused), Some(2));

    // An HTTP/1.1 client's own connection is what counts for it.
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert_eq!(connections.now(), [2]);

    // One closed, its room is back.
    let (mut stream, mut body) = open.pop().unwrap();
    stream.send_data(Bytes::new(), true).unwrap();
    let _rest = all_of(&mut body).await;
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#,
        1,
    )
    .await;
    assert_eq!(connections.now(), [1]);
    send = within(send.ready()).await.unwrap();
    let (response, _stream) = send.send_request(connect_for("websocket"), false).unwrap();
    assert_eq!(within(response).await.unwrap().status(), 200);
}

/// A backend that answers an extended CONNECT's handshake with a page, 200, has not
/// switched, and to a CONNECT every 2xx opens the tunnel (RFC 9110 §9.3.6): the client is
/// told 502, not 200.
#[tokio::test]
async fn a_page_in_place_of_the_switch_is_answered_502_to_an_http2_client() {
    let (upstream, _) = backend(|mut wire| async move {
        let _head = wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\ncontent-length: 11\r\n\r\n<html>hi</html>")
            .await;
        let _closed = within(wire.rest()).await;
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let (mut send, _) = h2_client(address).await;
    let (response, _stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 502);
}

/// An extended CONNECT for a protocol other than WebSocket is answered 501 (RFC 9220 §3),
/// and nothing goes upstream.
#[tokio::test]
async fn an_extended_connect_for_another_protocol_is_answered_501() {
    let (upstream, accepted) = backend(|mut wire| async move {
        let _head = wire.head().await;
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let (mut send, _) = h2_client(address).await;
    let (response, _stream) = send
        .send_request(connect_for("webtransport"), false)
        .unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 501);
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
}

/// What an HTTP/2 backend was asked, as the test's backend saw it.
#[derive(Debug)]
struct Asked {
    method: http::Method,
    protocol: Option<String>,
    path: String,
    headers: http::HeaderMap,
}

/// A backend spoken to in HTTP/2 that announces extended CONNECT if `announces`. It tells
/// `saw` what each request asked; a CONNECT for `websocket` is answered 200 and its stream
/// then carries `hello`, what it is sent back in brackets the first time, and `bye` once
/// the gateway ends its side. Anything else is answered 426.
fn h2_backend(
    announces: bool,
    saw: mpsc::UnboundedSender<Asked>,
) -> (SocketAddr, Arc<AtomicUsize>) {
    h2_backend_answering(announces, 426, saw)
}

/// The same, answering anything but a CONNECT for `websocket` with `status`.
fn h2_backend_answering(
    announces: bool,
    status: u16,
    saw: mpsc::UnboundedSender<Asked>,
) -> (SocketAddr, Arc<AtomicUsize>) {
    backend(move |wire| {
        let saw = saw.clone();
        async move {
            let mut builder = h2::server::Builder::new();
            if announces {
                builder.enable_connect_protocol();
            }
            let Ok(mut connection) = builder.handshake::<_, Bytes>(wire.stream).await else {
                return;
            };
            while let Some(Ok((request, mut respond))) = connection.accept().await {
                let (parts, mut body) = request.into_parts();
                let asked = Asked {
                    protocol: parts
                        .extensions
                        .get::<h2::ext::Protocol>()
                        .map(|protocol| protocol.as_str().to_owned()),
                    method: parts.method,
                    path: parts.uri.path().to_owned(),
                    headers: parts.headers,
                };
                let websocket = asked.method == http::Method::CONNECT
                    && asked.protocol.as_deref() == Some("websocket");
                let _told = saw.send(asked);
                if !websocket {
                    let refusal = http::Response::builder().status(status).body(()).unwrap();
                    if let Ok(mut sending) = respond.send_response(refusal, false) {
                        let _sent = sending.send_data(Bytes::from_static(b"no"), true);
                    }
                    continue;
                }
                let ok = http::Response::builder().status(200).body(()).unwrap();
                let Ok(mut sending) = respond.send_response(ok, false) else {
                    continue;
                };
                tokio::spawn(async move {
                    let _sent = sending.send_data(Bytes::from_static(b"hello"), false);
                    let mut first = true;
                    while let Some(Ok(data)) = body.data().await {
                        let _released = body.flow_control().release_capacity(data.len());
                        let echo = if first {
                            first = false;
                            format!("[{}]", String::from_utf8_lossy(&data))
                        } else {
                            String::from_utf8_lossy(&data).into_owned()
                        };
                        let _sent = sending.send_data(Bytes::from(echo), false);
                    }
                    let _sent = sending.send_data(Bytes::from_static(b"bye"), true);
                });
            }
        }
    })
}

/// The config's upstreams line for `up` spoken to in HTTP/2 at `address`.
fn h2_upstream(address: SocketAddr) -> String {
    format!("  up2: {{ load_balancer: p2c, endpoints: [\"{address}\"], protocol: http2 }}")
}

const FORWARD_H2: &str = "        forward: { backends: [{ upstream: up2, weight: 1 }] }";

/// An HTTP/1.1 client's WebSocket to an HTTP/2 backend goes as an extended CONNECT, with
/// no key; the backend's 200 becomes the client's 101, with the Accept of the client's own
/// key, and the stream carries the bytes.
#[tokio::test]
async fn an_http1_websocket_is_carried_to_an_http2_backend() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (h2, _) = h2_backend(true, saw);
    let (unused, _) = backend(|_| async {});
    let (address, proxy) = gateway(unused, FORWARD_H2, &h2_upstream(h2));
    let mut client = Wire::to(address).await;
    client.write(&format!("{}early", handshake())).await;
    let head = within(client.head()).await;
    assert!(
        head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{head}"
    );
    assert_eq!(field(&head, "upgrade"), Some("websocket"), "{head}");
    assert_eq!(field(&head, "connection"), Some("upgrade"), "{head}");
    assert_eq!(field(&head, "sec-websocket-accept"), Some(ACCEPT), "{head}");

    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.method, http::Method::CONNECT);
    assert_eq!(asked.protocol.as_deref(), Some("websocket"));
    assert_eq!(asked.path, "/chat");
    assert!(
        asked.headers.get("sec-websocket-key").is_none(),
        "{asked:?}"
    );
    assert_eq!(asked.headers["sec-websocket-version"], "13");

    assert_eq!(within(client.exactly(5)).await, "hello");
    assert_eq!(within(client.exactly(7)).await, "[early]");
    client.write("ping").await;
    assert_eq!(within(client.exactly(4)).await, "ping");
    client.finish().await;
    assert_eq!(client.rest().await, "bye");
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#,
        1,
    )
    .await;
}

/// HTTP/2 both sides: the extended CONNECT goes on as one, and the 200 comes back as one.
#[tokio::test]
async fn an_http2_websocket_is_carried_to_an_http2_backend() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (h2, _) = h2_backend(true, saw);
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(h2));
    let (mut send, _) = h2_client(address).await;
    let (response, mut stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 200);
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.protocol.as_deref(), Some("websocket"));
    let mut body = response.into_body();
    let mut kept = Vec::new();
    assert_eq!(next_of(&mut body, 5, &mut kept).await, b"hello");
    stream
        .send_data(Bytes::from_static(b"ping"), false)
        .unwrap();
    assert_eq!(next_of(&mut body, 6, &mut kept).await, b"[ping]");
    stream.send_data(Bytes::new(), true).unwrap();
    let mut rest = kept;
    rest.extend(all_of(&mut body).await);
    assert_eq!(rest, b"bye");
}

/// An HTTP/2 backend whose connection does not announce extended CONNECT is not sent one:
/// the handshake goes as the plain GET it came as, and its answer goes back as it came —
/// but for a 2xx to an HTTP/2 client, which a CONNECT would take for the switch.
#[tokio::test]
async fn an_http2_backend_that_does_not_announce_it_is_sent_a_plain_request() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (h2, _) = h2_backend(false, saw);
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(h2));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 426"), "{head}");
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.method, http::Method::GET);
    assert_eq!(asked.protocol, None);

    let (mut send, _) = h2_client(address).await;
    let (response, _stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 426);
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.method, http::Method::GET);
}

/// A 2xx to that plain GET is no switch: an HTTP/1.1 client is told it as it came, not a
/// 101, and an HTTP/2 client, which a CONNECT's 2xx would tell of the switch, is told 502.
#[tokio::test]
async fn a_2xx_to_the_plain_request_is_no_switch() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (h2, _) = h2_backend_answering(false, 200, saw);
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(h2));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(field(&head, "upgrade"), None, "{head}");
    assert_eq!(field(&head, "sec-websocket-accept"), None, "{head}");
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.method, http::Method::GET);

    let (mut send, _) = h2_client(address).await;
    let (response, _stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 502);
    let asked = within(seen.recv()).await.unwrap();
    assert_eq!(asked.method, http::Method::GET);
}

/// A socket whose first write waits `delay`: an HTTP/2 server on it sends its SETTINGS late.
struct Late {
    stream: TcpStream,
    delay: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl tokio::io::AsyncRead for Late {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for Late {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Some(delay) = self.delay.as_mut() {
            std::task::ready!(delay.as_mut().poll(cx));
            self.delay = None;
        }
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// The pool lets a new connection's first stream go before the peer's SETTINGS are heard
/// (15 §3); a WebSocket waits for them, and does not take a connection that has yet to say
/// it takes extended CONNECT for one that does not (19 §4).
#[tokio::test]
async fn a_websocket_waits_for_an_http2_backends_settings() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let late = socket.local_addr().unwrap();
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
                let saw = saw.clone();
                tokio::spawn(async move {
                    let late = Late {
                        stream,
                        delay: Some(Box::pin(tokio::time::sleep(Duration::from_millis(300)))),
                    };
                    let mut builder = h2::server::Builder::new();
                    builder.enable_connect_protocol();
                    let Ok(mut connection) = builder.handshake::<_, Bytes>(late).await else {
                        return;
                    };
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let _told = saw.send(
                            request
                                .extensions()
                                .get::<h2::ext::Protocol>()
                                .map(|protocol| protocol.as_str().to_owned()),
                        );
                        let ok = http::Response::builder().status(200).body(()).unwrap();
                        let _sending = respond.send_response(ok, false);
                    }
                });
            }
        });
    });
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(late));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert_eq!(
        within(seen.recv()).await.unwrap().as_deref(),
        Some("websocket"),
        "the handshake went as a plain request"
    );
}

/// A reset ends a WebSocket's tunnel both ways: an HTTP/2 client that resets its stream has
/// the backend's connection closed, and a backend whose connection fails has the client's
/// stream reset; each is counted as a tunnel that failed.
#[tokio::test]
async fn resets_end_an_http2_websocket_both_ways() {
    let (saw, mut seen) = mpsc::unbounded_channel();
    let (upstream, _) = backend(move |mut wire| {
        let saw = saw.clone();
        async move {
            let head = wire.head().await;
            let accept = accept_of(field(&head, "sec-websocket-key").unwrap_or_default());
            wire.write(&format!(
                "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                 connection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\r\nhello"
            ))
            .await;
            // A path that fails: the backend's connection reset under the WebSocket, once
            // the client has been carried to it.
            if head.starts_with("GET /fail ") {
                let _heard = wire.until(b"x").await;
                let _unset = wire.stream.set_zero_linger();
                drop(wire);
                return;
            }
            let _told = saw.send(within(wire.rest()).await);
            // Held open: a reset closes it from the gateway's side, not a half-close
            // that waits for this one's end.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    let (address, proxy) = gateway(upstream, FORWARD, "");

    let (mut send, _) = h2_client(address).await;
    let (response, mut stream) = send.send_request(connect_for("websocket"), false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    let mut kept = Vec::new();
    assert_eq!(next_of(&mut body, 5, &mut kept).await, b"hello");
    let began = Instant::now();
    stream.send_reset(h2::Reason::CANCEL);
    // The backend's connection is closed, with nothing more sent on it, and the tunnel
    // is over at once, not once the backend has closed its side too.
    assert_eq!(within(seen.recv()).await.unwrap(), "");
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="failed"}"#,
        1,
    )
    .await;
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );

    let mut failing = connect_for("websocket");
    *failing.uri_mut() = "http://chat.test/fail".parse().unwrap();
    let (response, mut stream) = send.send_request(failing, false).unwrap();
    let response = within(response).await.unwrap();
    assert_eq!(response.status(), 200);
    stream.send_data(Bytes::from_static(b"x"), false).unwrap();
    let mut body = response.into_body();
    let mut ended = Ok(());
    while let Some(data) = within(body.data()).await {
        if let Err(error) = data {
            ended = Err(error);
            break;
        }
    }
    let reason = ended
        .expect_err("the stream ended as if the backend had")
        .reason();
    assert_eq!(reason, Some(h2::Reason::CANCEL));
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="failed"}"#,
        2,
    )
    .await;
}

/// A rule forwarding everything to `pair`.
const FORWARD_PAIR: &str = "        forward: { backends: [{ upstream: pair, weight: 1 }] }";

/// An open WebSocket is load on its backend for as long as it is open (03 §6): with every
/// one held, `p2c` sends each new one to the backend with fewer, so after every second one
/// both have as many. Were an open WebSocket not counted, each would go either way.
#[tokio::test]
async fn an_open_websocket_counts_against_its_backend() {
    let (saw, _seen) = mpsc::unbounded_channel();
    let (a, at_a) = echoing(saw.clone());
    let (b, at_b) = echoing(saw);
    let (unused, _) = backend(|_| async {});
    let pair = format!("  pair: {{ load_balancer: p2c, endpoints: [\"{a}\", \"{b}\"] }}");
    let (address, _proxy) = gateway(unused, FORWARD_PAIR, &pair);
    let mut held = Vec::new();
    for opened in 1..=20 {
        let mut client = Wire::to(address).await;
        client.write(&handshake()).await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 101 "), "{head}");
        assert_eq!(within(client.exactly(5)).await, "hello");
        held.push(client);
        if opened % 2 == 0 {
            let (a, b) = (at_a.load(Ordering::SeqCst), at_b.load(Ordering::SeqCst));
            assert_eq!(a, b, "after {opened}");
        }
    }
}

/// The same for WebSockets carried as extended CONNECTs to HTTP/2 backends, each a stream
/// of a connection the backend's others share.
#[tokio::test]
async fn an_open_http2_websocket_counts_against_its_backend() {
    let (saw_a, mut seen_a) = mpsc::unbounded_channel();
    let (saw_b, mut seen_b) = mpsc::unbounded_channel();
    let (a, _) = h2_backend(true, saw_a);
    let (b, _) = h2_backend(true, saw_b);
    let (unused, _) = backend(|_| async {});
    let pair =
        format!("  pair: {{ load_balancer: p2c, endpoints: [\"{a}\", \"{b}\"], protocol: http2 }}");
    let (address, _proxy) = gateway(unused, FORWARD_PAIR, &pair);
    let (mut asked_a, mut asked_b) = (0, 0);
    let mut held = Vec::new();
    for opened in 1..=20 {
        let mut client = Wire::to(address).await;
        client.write(&handshake()).await;
        let head = within(client.head()).await;
        assert!(head.starts_with("HTTP/1.1 101 "), "{head}");
        assert_eq!(within(client.exactly(5)).await, "hello");
        held.push(client);
        while seen_a.try_recv().is_ok() {
            asked_a += 1;
        }
        while seen_b.try_recv().is_ok() {
            asked_b += 1;
        }
        if opened % 2 == 0 {
            assert_eq!(asked_a, asked_b, "after {opened}");
        }
    }
}

/// A rule forwarding everything to `up`, trying twice more on a failed exchange (502) or a
/// 503.
const FORWARD_RETRIED: &str = "        forward: { backends: [{ upstream: up, weight: 1 }], retry: { attempts: 2, http_statuses: [502, 503], grpc_statuses: [], on_timeout: false, backoff_base_ms: 1, backoff_max_ms: 1 } }";

/// Any try of a handshake may be the one that switches, not only the first (19 §5): here
/// the backend fails the first, refuses the second with a 503 and switches on the third,
/// and the client is carried to that one.
#[tokio::test]
async fn a_handshake_tried_again_is_carried_once_it_switches() {
    let tries = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&tries);
    let (upstream, accepted) = backend(move |mut wire| {
        let counting = Arc::clone(&counting);
        async move {
            let head = wire.head().await;
            match counting.fetch_add(1, Ordering::SeqCst) {
                // The connection closed with nothing said: a failed exchange.
                0 => {}
                1 => {
                    wire.write("HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")
                        .await;
                }
                _ => {
                    let accept = accept_of(field(&head, "sec-websocket-key").unwrap_or_default());
                    wire.write(&format!(
                        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\r\nhello"
                    ))
                    .await;
                    let heard = wire.until(b"ping").await;
                    assert!(heard.is_some(), "the client was not carried here");
                    wire.write("pong").await;
                    let _rest = wire.rest().await;
                }
            }
        }
    });
    let (address, proxy) = gateway(upstream, FORWARD_RETRIED, "");
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(
        head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{head}"
    );
    assert_eq!(field(&head, "sec-websocket-accept"), Some(ACCEPT), "{head}");
    assert_eq!(within(client.exactly(5)).await, "hello");
    client.write("ping").await;
    assert_eq!(within(client.exactly(4)).await, "pong");
    assert_eq!(tries.load(Ordering::SeqCst), 3);
    assert_eq!(accepted.load(Ordering::SeqCst), 3);
    client.finish().await;
    counted(
        &proxy,
        r#"edgerush_listener_tunnels_total{listener="web",outcome="closed"}"#,
        1,
    )
    .await;
}

/// The same for a backend spoken to in HTTP/2: its first extended CONNECT is refused with
/// a 503, and the second, on the same connection, opens the tunnel.
#[tokio::test]
async fn a_handshake_tried_again_is_carried_to_an_http2_backend_once_it_switches() {
    let tries = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&tries);
    let (h2, _) = backend(move |wire| {
        let counting = Arc::clone(&counting);
        async move {
            let mut builder = h2::server::Builder::new();
            builder.enable_connect_protocol();
            let Ok(mut connection) = builder.handshake::<_, Bytes>(wire.stream).await else {
                return;
            };
            while let Some(Ok((request, mut respond))) = connection.accept().await {
                if counting.fetch_add(1, Ordering::SeqCst) == 0 {
                    let refusal = http::Response::builder().status(503).body(()).unwrap();
                    let _sent = respond.send_response(refusal, true);
                    continue;
                }
                let mut body = request.into_body();
                let ok = http::Response::builder().status(200).body(()).unwrap();
                let Ok(mut sending) = respond.send_response(ok, false) else {
                    continue;
                };
                tokio::spawn(async move {
                    let _sent = sending.send_data(Bytes::from_static(b"hello"), false);
                    while let Some(Ok(data)) = body.data().await {
                        let _released = body.flow_control().release_capacity(data.len());
                        let _sent = sending.send_data(data, false);
                    }
                    let _sent = sending.send_data(Bytes::new(), true);
                });
            }
        }
    });
    let (unused, _) = backend(|_| async {});
    let retried = FORWARD_RETRIED.replace("upstream: up,", "upstream: up2,");
    let (address, _) = gateway(unused, &retried, &h2_upstream(h2));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(
        head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{head}"
    );
    assert_eq!(field(&head, "sec-websocket-accept"), Some(ACCEPT), "{head}");
    assert_eq!(within(client.exactly(5)).await, "hello");
    client.write("ping").await;
    assert_eq!(within(client.exactly(4)).await, "ping");
    assert_eq!(tries.load(Ordering::SeqCst), 2);
}

/// An extended CONNECT is answered 2xx only when its tunnel opens (RFC 9110 §9.3.6, 19 §3,
/// §4), whatever content type its client gave it: a page in place of the switch is 502,
/// and a protocol not carried 501, never the 200 that answers a gRPC call the gateway
/// refuses. A CONNECT is never a gRPC call.
#[tokio::test]
async fn an_extended_connect_that_says_grpc_is_never_answered_2xx() {
    let (upstream, accepted) = backend(|mut wire| async move {
        let _head = wire.head().await;
        wire.write("HTTP/1.1 200 OK\r\ncontent-length: 15\r\n\r\n<html>hi</html>")
            .await;
        let _closed = within(wire.rest()).await;
    });
    let (address, _) = gateway(upstream, FORWARD, "");
    let (mut send, _) = h2_client(address).await;
    let mut statuses = Vec::new();
    for protocol in ["websocket", "webtransport"] {
        send = within(send.ready()).await.unwrap();
        let mut request = connect_for(protocol);
        request.headers_mut().insert(
            "content-type",
            http::HeaderValue::from_static("application/grpc"),
        );
        let (response, _stream) = send.send_request(request, false).unwrap();
        let response = within(response).await.unwrap();
        statuses.push((
            response.status().as_u16(),
            response
                .headers()
                .get("grpc-status")
                .map(|status| status.to_str().unwrap().to_owned()),
        ));
    }
    assert_eq!(statuses, [(502, None), (501, None)]);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

/// An HTTP/2 backend's new connection that ends before its settling PING is answered fails
/// the WebSocket waiting for its SETTINGS at once, as a request on a dead connection fails
/// (502), rather than leaving it to wait out the answer's head deadline (60 s).
#[tokio::test]
async fn a_websocket_waiting_for_settings_fails_when_the_connection_ends() {
    let (dying, opened) = backend(|mut wire| async move {
        // The client's preface and SETTINGS, then SETTINGS announcing extended CONNECT, and
        // no answer to anything after: the connection closes without a PING's answer.
        let mut first = [0; 64];
        let _read = within(wire.stream.read(&mut first)).await;
        let settings = [0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 1];
        let _wrote = wire.stream.write_all(&settings).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(wire);
    });
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(dying));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let began = Instant::now();
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "{:?}",
        began.elapsed()
    );
    assert!(opened.load(Ordering::SeqCst) >= 1);
}

/// The same for a connection that ends before the backend's SETTINGS came at all: having
/// said nothing of extended CONNECT, it is not taken as having refused them, so the
/// handshake is not tried again as a plain GET on a connection of its own.
#[tokio::test]
async fn a_websocket_on_a_connection_that_ends_before_its_settings_is_not_tried_again() {
    let (dying, opened) = backend(|mut wire| async move {
        let mut first = [0; 64];
        let _read = within(wire.stream.read(&mut first)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(wire);
    });
    let (unused, _) = backend(|_| async {});
    let (address, _) = gateway(unused, FORWARD_H2, &h2_upstream(dying));
    let mut client = Wire::to(address).await;
    client.write(&handshake()).await;
    let head = within(client.head()).await;
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}
