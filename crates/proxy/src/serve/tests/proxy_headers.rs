//! PROXY headers, from senders in front and to backends.

use super::*;

/// `config` with its listener `web` reading a PROXY header from `senders`.
fn with_senders(mut config: Config, senders: &[&str]) -> Config {
    let web = config.listeners.get_mut("web").unwrap();
    web.proxy_protocol = Some(edgerush_config::ListenerProxyProtocol::Senders(
        senders.iter().map(|range| (*range).to_owned()).collect(),
    ));
    config
}

/// A v1 header saying the connection is `source`'s, made to `destination`.
fn v1_header(source: &str, destination: &str) -> Vec<u8> {
    let (source, destination) = (source.parse().unwrap(), destination.parse().unwrap());
    proxy_protocol::proxied(proxy_protocol::Version::V1, source, destination)
        .as_bytes()
        .to_vec()
}

/// The same in v2.
fn v2_header(source: &str, destination: &str) -> Vec<u8> {
    let (source, destination) = (source.parse().unwrap(), destination.parse().unwrap());
    proxy_protocol::proxied(proxy_protocol::Version::V2, source, destination)
        .as_bytes()
        .to_vec()
}

/// A v2 `LOCAL` header: the sender's own connection.
const V2_LOCAL: &[u8] = b"\r\n\r\n\0\r\nQUIT\n\x20\x00\x00\x00";

/// Whether the listener `listener` has counted `count` PROXY headers as `outcome`, soon.
async fn headers_counted(worker: &Worker, listener: &str, outcome: &str, count: usize) {
    let line = format!(
        "edgerush_listener_proxy_headers_total{{listener=\"{listener}\",outcome=\"{outcome}\"}} {count}\n"
    );
    until(|| worker.proxy().metrics().contains(&line)).await;
}

/// A stream whose first write is sent with `ahead` in front of it, as one: a PROXY header
/// and the client's first bytes in the one segment, as load balancers send them.
#[derive(Debug)]
struct Fronted {
    stream: TcpStream,
    ahead: Option<Vec<u8>>,
}

impl AsyncRead for Fronted {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(context, buf)
    }
}

impl AsyncWrite for Fronted {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(ahead) = &this.ahead {
            let mut both = ahead.clone();
            both.extend_from_slice(buf);
            return match Pin::new(&mut this.stream).poll_write(context, &both) {
                Poll::Ready(Ok(written)) => {
                    // A first write of a few hundred bytes to loopback goes whole.
                    assert_eq!(written, both.len(), "the first write went in pieces");
                    this.ahead = None;
                    Poll::Ready(Ok(buf.len()))
                }
                other => other,
            };
        }
        Pin::new(&mut this.stream).poll_write(context, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}

/// A sender's header names the client the upstream is told of, in either version and
/// whether or not the request comes in the same segment; a header from anyone else is
/// read and dropped, and whoever connected is the client (20 §3).
#[tokio::test]
async fn a_senders_header_names_the_client_and_a_strangers_is_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            let (front, worker) = serving_config(&config).await;
            let request = b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n";

            let mut sent = v1_header("198.51.100.7:56324", "203.0.113.10:443");
            sent.extend_from_slice(request);
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[0].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 198.51.100.7\r\n"),
                "{head}"
            );
            assert!(!head.contains("proxy"), "{head}");

            // v2, its header and the request in segments of their own.
            {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut stream = TcpStream::connect(front).await.unwrap();
                let header = v2_header("[2001:db8::7]:1", "[2001:db8::1]:443");
                stream.write_all(&header[..7]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
                stream.write_all(&header[7..]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
                stream.write_all(request).await.unwrap();
                let mut answer = Vec::new();
                let _ended = within(stream.read_to_end(&mut answer)).await;
                assert!(answer.starts_with(b"HTTP/1.1 200 OK\r\n"));
            }
            let head = heads.borrow()[1].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 2001:db8::7\r\n"),
                "{head}"
            );
            headers_counted(&worker, "web", "accepted", 2).await;

            // From a listener whose only sender is elsewhere, the header is no one's word.
            let config = with_senders(everything_config(upstream), &["10.0.0.0/8"]);
            let (front, worker) = serving_config(&config).await;
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[2].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 127.0.0.1\r\n"),
                "{head}"
            );
            assert!(!head.contains("198.51.100.7"), "{head}");
            headers_counted(&worker, "web", "untrusted", 1).await;
        })
        .await;
}

/// Sending a header is not trust for forwarding headers: a sender's `LOCAL` makes the
/// sender the client, and the `X-Forwarded-For` it relays is believed only if the
/// sender is a trusted proxy too (20 §3).
#[tokio::test]
async fn a_senders_own_connection_is_no_trusted_proxys() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let mut sent = V2_LOCAL.to_vec();
            sent.extend_from_slice(
                b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nx-forwarded-for: 10.9.9.9\r\n\r\n",
            );

            let config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            let (front, worker) = serving_config(&config).await;
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[0].clone();
            assert!(head.contains("\r\nx-forwarded-for: 127.0.0.1\r\n"), "{head}");
            assert!(!head.contains("10.9.9.9"), "{head}");
            headers_counted(&worker, "web", "local", 1).await;

            let config = with_senders(
                forwarding_config(upstream, &["127.0.0.0/8"]),
                &["127.0.0.0/8"],
            );
            let (front, _worker) = serving_config(&config).await;
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[1].clone();
            assert!(head.contains("\r\nx-forwarded-for: 10.9.9.9\r\n"), "{head}");
        })
        .await;
}

/// HTTP/2 with prior knowledge after a header, the preface in the header's segment.
#[tokio::test]
async fn http2_follows_a_header_in_its_segment() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            let (front, _worker) = serving_config(&config).await;
            let stream = Fronted {
                stream: TcpStream::connect(front).await.unwrap(),
                ahead: Some(v2_header("192.0.2.44:1000", "192.0.2.1:80")),
            };
            let (mut send, connection) = ::h2::client::Builder::new()
                .handshake::<_, Bytes>(stream)
                .await
                .unwrap();
            let _driving = tokio::task::spawn_local(async move {
                let _ended = connection.await;
            });
            let request = Request::get("http://a.test/").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            let head = heads.borrow()[0].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 192.0.2.44\r\n"),
                "{head}"
            );
        })
        .await;
}

/// TLS after a header: the ClientHello in the header's segment, and in one of its own
/// after it.
#[tokio::test]
async fn tls_follows_a_header_in_its_segment_or_after_it() {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let mut config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            secured(
                &mut config,
                vec![crate::tls::testing::certificate(&["example.test"])],
                None,
            );
            let (front, _worker) = serving_config(&config).await;
            let connector = || {
                let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                builder.set_verify(SslVerifyMode::NONE);
                builder.build().configure().unwrap().verify_hostname(false)
            };

            let together = Fronted {
                stream: TcpStream::connect(front).await.unwrap(),
                ahead: Some(v1_header("192.0.2.81:1", "192.0.2.1:443")),
            };
            let secured = within(tokio_boring::connect(connector(), "example.test", together))
                .await
                .unwrap();
            assert!(h1_over(secured).await.starts_with("HTTP/1.1 200 OK\r\n"));
            let head = heads.borrow()[0].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 192.0.2.81\r\n"),
                "{head}"
            );

            let mut apart = TcpStream::connect(front).await.unwrap();
            {
                use tokio::io::AsyncWriteExt;
                apart
                    .write_all(&v2_header("192.0.2.82:1", "192.0.2.1:443"))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            let secured = within(tokio_boring::connect(connector(), "example.test", apart))
                .await
                .unwrap();
            assert!(h1_over(secured).await.starts_with("HTTP/1.1 200 OK\r\n"));
            let head = heads.borrow()[1].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 192.0.2.82\r\n"),
                "{head}"
            );
        })
        .await;
}

/// No header taken closes the connection with nothing said, counted by why: bytes that
/// are no header, a header that is wrong, a client gone before its header was whole,
/// and one that never finished it within the first-request deadline.
#[tokio::test]
async fn a_connection_without_a_header_to_take_is_closed_and_counted() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            let (front, worker) = serving_config(&config).await;
            for (sent, outcome) in [
                (&b"GET / HTTP/1.1\r\nhost: a.test\r\n\r\n"[..], "missing"),
                (
                    b"PROXY TCP4 192.0.2.1 192.0.2.2 01 2\r\nGET / HTTP/1.1\r\n\r\n",
                    "malformed",
                ),
                (b"\r\n\r\n\0\r\nQUIT\n\x22\x11\x00\x0c", "malformed"),
            ] {
                let answer = h1_answer(front, sent).await;
                assert_eq!(answer, "", "{outcome}: {answer}");
                headers_counted(&worker, "web", outcome, 1 + usize::from(sent[0] == b'\r')).await;
            }
            // Gone before saying all of it.
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream.write_all(b"PROXY TCP4 192.0.2.1").await.unwrap();
            stream.shutdown().await.unwrap();
            let mut rest = Vec::new();
            let _ended = within(stream.read_to_end(&mut rest)).await;
            headers_counted(&worker, "web", "closed", 1).await;
            // Never finishing it.
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream
                .write_all(b"\r\n\r\n\0\r\nQUIT\n\x21\x11\x00\x0c\xc0")
                .await
                .unwrap();
            let took = closed_after(&mut stream).await;
            assert!(took + EARLY >= SHORT.first_request, "closed after {took:?}");
            assert!(took < SHORT.first_request + SLACK, "closed after {took:?}");
            headers_counted(&worker, "web", "too_slow", 1).await;
            assert!(heads.borrow().is_empty(), "{:?}", heads.borrow());
        })
        .await;
}

/// A v2 header's TLVs are skipped by count, never held, however many more than a
/// block's worth there are; what follows them is the request, every byte of it.
#[tokio::test]
async fn a_header_longer_than_a_block_is_skipped_to_its_end() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let config = with_senders(everything_config(upstream), &["127.0.0.0/8"]);
            let (front, worker) = serving_config(&config).await;
            // 40,000 bytes of TLVs: NOOPs of 1,000 bytes each, as padding would be.
            let mut tlvs = Vec::new();
            for _ in 0..40 {
                tlvs.push(0x04);
                tlvs.extend_from_slice(&997_u16.to_be_bytes());
                tlvs.extend(std::iter::repeat_n(b'G', 997));
            }
            let mut sent = b"\r\n\r\n\0\r\nQUIT\n\x21\x11".to_vec();
            sent.extend_from_slice(&u16::try_from(12 + tlvs.len()).unwrap().to_be_bytes());
            sent.extend_from_slice(&[192, 0, 2, 9, 192, 0, 2, 1, 0, 1, 0, 80]);
            sent.extend_from_slice(&tlvs);
            sent.extend_from_slice(
                b"GET /after HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
            );
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[0].clone();
            assert!(head.starts_with("get /after http/1.1\r\n"), "{head}");
            assert!(
                head.contains("\r\nx-forwarded-for: 192.0.2.9\r\n"),
                "{head}"
            );
            headers_counted(&worker, "web", "accepted", 1).await;
        })
        .await;
}

/// A listener that reads no header takes one for what it is: bytes of a request that
/// is not one. Nothing is guessed (20 §1).
#[tokio::test]
async fn a_listener_that_reads_no_header_guesses_nothing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let (front, _worker) = serving_config(&everything_config(upstream)).await;
            let mut sent = v1_header("198.51.100.7:1", "192.0.2.1:80");
            sent.extend_from_slice(b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n");
            let answer = h1_answer(front, &sent).await;
            assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
            assert!(heads.borrow().is_empty());
        })
        .await;
}

/// A `tcp` listener with senders carries what came after the header, and only that,
/// whether it came in the header's segment or in pieces.
#[tokio::test]
async fn a_tcp_tunnel_carries_what_follows_the_header() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = tallying_backend().await;
            let yaml = tcp_to(backend, "").replace(
                "proxy_protocol: off",
                "proxy_protocol: { senders: [\"127.0.0.0/8\"] }",
            );
            let (front, worker) = passing(&yaml).await;
            let payload: Vec<u8> = (0..5_000_u32).map(|at| (at % 251) as u8).collect();
            let sum: u64 = payload.iter().map(|&byte| u64::from(byte)).sum();

            let mut client = TcpStream::connect(front).await.unwrap();
            let mut sent = v2_header("192.0.2.5:1", "192.0.2.1:5432");
            sent.extend_from_slice(&payload);
            client.write_all(&sent).await.unwrap();
            client.shutdown().await.unwrap();
            let mut answer = String::new();
            bounded(client.read_to_string(&mut answer)).await.unwrap();
            assert_eq!(answer, format!("{} {sum}", payload.len()));

            let mut client = TcpStream::connect(front).await.unwrap();
            let mut sent = v1_header("192.0.2.5:1", "192.0.2.1:5432");
            sent.extend_from_slice(&payload[..10]);
            // In pieces, each its own segment, well within the first-request deadline.
            for piece in sent.chunks(9) {
                client.write_all(piece).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            client.write_all(&payload[10..]).await.unwrap();
            client.shutdown().await.unwrap();
            let mut answer = String::new();
            bounded(client.read_to_string(&mut answer)).await.unwrap();
            assert_eq!(answer, format!("{} {sum}", payload.len()));
            headers_counted(&worker, "db", "accepted", 2).await;
            let line = "edgerush_listener_tunnels_total{listener=\"db\",outcome=\"closed\"} 2\n";
            until(|| worker.proxy().metrics().contains(line)).await;
        })
        .await;
}

/// A `tls` listener with senders routes by the ClientHello after the header, and the
/// backend's handshake is the client's: the header does not reach it.
#[tokio::test]
async fn a_tls_tunnel_routes_by_the_client_hello_after_the_header() {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = tls_backend("through").await;
            let yaml = format!(
                "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: {{ senders: [\"127.0.0.0/8\"] }} }} }}\n\
                     routes: []\n\
                     tls_routes: [{{ name: a, listeners: [sni], hostnames: [{{ name: a.test, falls_through: true }}], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
                     upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
            );
            let (front, worker) = passing(&yaml).await;
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::NONE);
            let config = builder.build().configure().unwrap().verify_hostname(false);
            let stream = Fronted {
                stream: TcpStream::connect(front).await.unwrap(),
                ahead: Some(v2_header("192.0.2.5:1", "192.0.2.1:443")),
            };
            let mut secured = bounded(tokio_boring::connect(config, "a.test", stream))
                .await
                .unwrap();
            let mut told = String::new();
            let _ended = bounded(secured.read_to_string(&mut told)).await;
            assert_eq!(told, "through");
            drop(secured);
            headers_counted(&worker, "sni", "accepted", 1).await;
            tunnel_ended(&worker, "sni", "closed").await;
        })
        .await;
}

/// A backend that keeps every byte each connection sends until it ends, then closes.
async fn recording_backend() -> (SocketAddr, Rc<RefCell<Vec<Vec<u8>>>>) {
    use tokio::io::AsyncReadExt;
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let heard = Rc::new(RefCell::new(Vec::new()));
    let hearing = Rc::clone(&heard);
    tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let hearing = Rc::clone(&hearing);
            tokio::task::spawn_local(async move {
                let mut came = Vec::new();
                let _ended = stream.read_to_end(&mut came).await;
                hearing.borrow_mut().push(came);
            });
        }
    });
    (address, heard)
}

/// A backend that asks for a header is sent one first, in the version it asks for,
/// naming the connection's own ends where nobody said otherwise, and then exactly the
/// client's bytes (20 §4).
#[tokio::test]
async fn a_backend_that_asks_is_told_who_the_client_is_first() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, heard) = recording_backend().await;
            for (at, version) in ["v1", "v2"].into_iter().enumerate() {
                let (front, _worker) = passing(&sending(&tcp_to(backend, ""), version)).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                let own = client.local_addr().unwrap();
                client.write_all(b"hello there").await.unwrap();
                client.shutdown().await.unwrap();
                let mut rest = Vec::new();
                let _ended =
                    bounded(tokio::io::AsyncReadExt::read_to_end(&mut client, &mut rest)).await;
                until(|| heard.borrow().len() > at).await;
                let came = heard.borrow()[at].clone();
                let version = if version == "v1" {
                    proxy_protocol::Version::V1
                } else {
                    proxy_protocol::Version::V2
                };
                let mut expected = proxy_protocol::proxied(version, own, front)
                    .as_bytes()
                    .to_vec();
                expected.extend_from_slice(b"hello there");
                assert_eq!(came, expected, "{}", came.escape_ascii());
            }
        })
        .await;
}

/// Where a sender's header named the client, a backend that asks is told of that
/// client and the address it connected to, not of the sender (20 §4).
#[tokio::test]
async fn a_backend_is_told_of_the_client_a_sender_named() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, heard) = recording_backend().await;
            let yaml = sending(&tcp_to(backend, ""), "v2").replace(
                "proxy_protocol: off",
                "proxy_protocol: { senders: [\"127.0.0.0/8\"] }",
            );
            let (front, _worker) = passing(&yaml).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let mut sent = v1_header("198.51.100.7:56324", "203.0.113.10:5432");
            sent.extend_from_slice(b"select 1");
            client.write_all(&sent).await.unwrap();
            client.shutdown().await.unwrap();
            let mut rest = Vec::new();
            let _ended =
                bounded(tokio::io::AsyncReadExt::read_to_end(&mut client, &mut rest)).await;
            until(|| !heard.borrow().is_empty()).await;
            let mut expected = v2_header("198.51.100.7:56324", "203.0.113.10:5432");
            expected.extend_from_slice(b"select 1");
            assert_eq!(heard.borrow()[0], expected);
        })
        .await;
}

/// A backend that speaks first, as SMTP, FTP, SSH and MySQL do, is sent its header as
/// soon as it is connected to: the client, which has sent nothing, hears its greeting.
#[tokio::test]
async fn a_backend_that_speaks_first_is_sent_its_header_at_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backend = socket.local_addr().unwrap();
            tokio::task::spawn_local(async move {
                let (mut stream, _) = socket.accept().await.unwrap();
                // Nothing until its header is whole, as a backend that asks for one
                // holds to.
                let mut came = Vec::new();
                let mut chunk = [0; 256];
                while !matches!(
                    proxy_protocol::read(&came),
                    proxy_protocol::Read::Whole { .. }
                ) {
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0, "closed before its header came");
                    came.extend_from_slice(&chunk[..read]);
                }
                stream.write_all(b"220 ready\r\n").await.unwrap();
                let mut rest = Vec::new();
                let _ended = stream.read_to_end(&mut rest).await;
            });
            let (front, _worker) = passing(&sending(&tcp_to(backend, ""), "v2")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let mut greeting = [0; 11];
            bounded(client.read_exact(&mut greeting)).await.unwrap();
            assert_eq!(&greeting, b"220 ready\r\n");
        })
        .await;
}

/// A `tls` listener's backend that asks is sent its header, and after it the
/// ClientHello as the client sent it.
#[tokio::test]
async fn a_tls_backend_that_asks_is_sent_the_header_then_the_client_hello() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (backend, heard) = recording_backend().await;
            let yaml = format!(
                "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: off }} }}\n\
                     routes: []\n\
                     tls_routes: [{{ name: a, listeners: [sni], hostnames: [{{ name: a.test, falls_through: true }}], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
                     upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
            );
            let (front, _worker) = passing(&sending(&yaml, "v1")).await;
            // The handshake goes nowhere: the backend only listens. The client gives up.
            let _nothing = tokio::time::timeout(
                Duration::from_millis(500),
                told_over_tls(front, Some("a.test")),
            )
            .await;
            until(|| !heard.borrow().is_empty()).await;
            let came = heard.borrow()[0].clone();
            let proxy_protocol::Read::Whole { header, length } = proxy_protocol::read(&came) else {
                panic!("no header first: {}", came.escape_ascii());
            };
            assert!(matches!(header, proxy_protocol::Header::Proxied { .. }));
            // A TLS handshake record, holding a ClientHello asking for a.test.
            let hello = &came[length..];
            assert_eq!(hello.first(), Some(&22));
            assert_eq!(
                crate::l4::hello::read(hello),
                crate::l4::hello::Hello::Whole(Some("a.test".to_owned()))
            );
        })
        .await;
}
