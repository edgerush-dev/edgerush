//! WebSockets.

use super::*;

/// A backend over TLS as `certificate` says that speaks WebSocket as far as the
/// handshake goes: a 101 with the Accept of the key it was sent and `hello` after it,
/// then everything it reads sent back until its peer closes.
async fn tls_websocket_backend(certificate: &edgerush_config::Certificate) -> SocketAddr {
    use boring::pkey::PKey;
    use boring::ssl::{SslAcceptor, SslMethod};
    use boring::x509::X509;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    builder
        .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
        .unwrap();
    builder
        .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
        .unwrap();
    let acceptor = Rc::new(builder.build());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((stream, _)) = socket.accept().await {
            let acceptor = Rc::clone(&acceptor);
            let _serving = tokio::task::spawn_local(async move {
                let Ok(mut secured) = tokio_boring::accept(&acceptor, stream).await else {
                    return;
                };
                let mut head = Vec::new();
                let mut byte = [0; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match secured.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => head.push(byte[0]),
                    }
                }
                let head = String::from_utf8(head).unwrap();
                let key = head
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                    .and_then(|key| Key::read(key.as_bytes()))
                    .unwrap();
                let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                let switched = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\nhello"
                );
                if secured.write_all(switched.as_bytes()).await.is_err() {
                    return;
                }
                let mut bytes = [0; 1024];
                loop {
                    match secured.read(&mut bytes).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => {
                            if secured.write_all(&bytes[..read]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                let _ = secured.write_all(b"bye").await;
                let _ = secured.shutdown().await;
            });
        }
    });
    address
}

/// A WebSocket is carried as it is in plaintext when both of its connections are TLS
/// ones: the client's to an `https` listener and the gateway's to a backend it verifies
/// (19 §5).
#[tokio::test]
async fn a_websocket_is_carried_over_tls_on_both_sides() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let server = certificate(&["backend.test"]);
            let upstream = tls_websocket_backend(&server).await;
            let mut config = everything_config(upstream);
            config.upstreams.get_mut("up").unwrap().tls = Some(trusting("backend.test", &server));
            secured(&mut config, vec![certificate(&["a.test"])], None);
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut client = tls_client(front, "a.test", Some(b"\x08http/1.1"), |_| {})
                .await
                .unwrap();
            client
                .write_all(
                    b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                          connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                          sec-websocket-version: 13\r\n\r\nearly",
                )
                .await
                .unwrap();
            let mut said = Vec::new();
            let mut bytes = [0; 1024];
            while !said.ends_with(b"helloearly") {
                let read = within(client.read(&mut bytes)).await.unwrap();
                assert_ne!(read, 0, "{}", String::from_utf8_lossy(&said));
                said.extend_from_slice(&bytes[..read]);
            }
            let said = String::from_utf8(said).unwrap();
            assert!(
                said.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
                "{said}"
            );
            assert!(
                said.contains("\r\nsec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"),
                "{said}"
            );
            client.write_all(b"ping").await.unwrap();
            let mut echoed = [0; 4];
            within(client.read_exact(&mut echoed)).await.unwrap();
            assert_eq!(&echoed, b"ping");
            client.shutdown().await.unwrap();
            let mut rest = Vec::new();
            let _ended = within(client.read_to_end(&mut rest)).await;
            assert_eq!(rest, b"bye");
            tunnel_ended(&worker, "web", "closed").await;
        })
        .await;
}

/// A blocking TLS server as `certificate` says, on a thread of its own, that speaks
/// WebSocket as far as the handshake goes — a 101 with the Accept of the key it was sent —
/// then reads until its peer closes, and says whether a closure alert came before the close.
/// BoringSSL's blocking stream is the one that can say so.
fn alert_heeding_websocket_backend(
    certificate: &edgerush_config::Certificate,
) -> (SocketAddr, std::sync::mpsc::Receiver<bool>) {
    use boring::pkey::PKey;
    use boring::ssl::{ShutdownState, SslAcceptor, SslMethod};
    use boring::x509::X509;
    use std::io::{Read, Write};
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    builder
        .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
        .unwrap();
    builder
        .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
        .unwrap();
    let acceptor = builder.build();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let (heard, hearing) = std::sync::mpsc::channel();
    let _serving = std::thread::spawn(move || {
        let (stream, _) = socket.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut secured = acceptor.accept(stream).unwrap();
        let mut head = Vec::new();
        let mut byte = [0; 1];
        while !head.ends_with(b"\r\n\r\n") {
            secured.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        let key = head
            .split("\r\n")
            .find_map(|line| line.strip_prefix("sec-websocket-key: "))
            .and_then(|key| Key::read(key.as_bytes()))
            .unwrap();
        let accept = String::from_utf8(key.accept().to_vec()).unwrap();
        let switched = format!(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                 connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
        );
        secured.write_all(switched.as_bytes()).unwrap();
        let mut rest = Vec::new();
        let _ended = secured.read_to_end(&mut rest);
        let _ = heard.send(secured.get_shutdown().contains(ShutdownState::RECEIVED));
    });
    (address, hearing)
}

/// A WebSocket over TLS on both sides that the gateway closes — idle for long enough, or
/// drained (`drained`) — ends with a closure alert each way (RFC 8446 §6.1): the tunnel
/// ended in order, and its TLS ends say so before their connections close (19 §5). Says
/// what the client was sent, and whether the client and the backend each heard the alert.
async fn closed_by_the_gateway_over_tls(drained: bool) -> (String, bool, bool) {
    use crate::tls::testing::certificate;
    let server = certificate(&["backend.test"]);
    let (upstream, backend_heard) = alert_heeding_websocket_backend(&server);
    let mut config = everything_config(upstream);
    config.upstreams.get_mut("up").unwrap().tls = Some(trusting("backend.test", &server));
    config.routes[0].rules[0].forward.as_mut().unwrap().timeouts =
        Some(edgerush_config::Timeouts {
            request_ms: None,
            backend_request_ms: None,
            tunnel_idle_ms: Some(if drained { 60_000 } else { 300 }),
        });
    secured(&mut config, vec![certificate(&["a.test"])], None);
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);

    let (switched, switching) = tokio::sync::oneshot::channel();
    let client = tokio::task::spawn_blocking(move || {
        use boring::ssl::{ShutdownState, SslConnector, SslMethod, SslVerifyMode};
        use std::io::{Read, Write};
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        builder.set_alpn_protos(b"\x08http/1.1").unwrap();
        let config = builder.build().configure().unwrap().verify_hostname(false);
        let tcp = std::net::TcpStream::connect(front).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = config.connect("a.test", tcp).unwrap();
        tls.write_all(
            b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
              connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
              sec-websocket-version: 13\r\n\r\n",
        )
        .unwrap();
        let mut said = Vec::new();
        let mut byte = [0; 1];
        while !said.ends_with(b"\r\n\r\n") && tls.read(&mut byte).unwrap_or(0) == 1 {
            said.push(byte[0]);
        }
        let _ = switched.send(());
        // Then until the gateway closes it.
        let _ended = tls.read_to_end(&mut said);
        let heard = tls.get_shutdown().contains(ShutdownState::RECEIVED);
        (String::from_utf8_lossy(&said).into_owned(), heard)
    });
    within(switching).await.unwrap();
    if drained {
        worker.drain();
    }
    let (said, client_heard) = within(client).await.unwrap();
    tunnel_ended(&worker, "web", if drained { "drained" } else { "idle" }).await;
    let backend_heard = within(tokio::task::spawn_blocking(move || {
        backend_heard.recv_timeout(Duration::from_secs(5))
    }))
    .await
    .unwrap();
    (said, client_heard, backend_heard == Ok(true))
}

/// One closed for having been idle.
#[tokio::test]
async fn a_websocket_over_tls_closed_for_idling_ends_with_a_closure_alert_each_way() {
    let local = tokio::task::LocalSet::new();
    let (said, client, backend) = local.run_until(closed_by_the_gateway_over_tls(false)).await;
    assert!(
        said.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{said}"
    );
    assert!(client, "no closure alert to the client");
    assert!(backend, "no closure alert to the backend");
}

/// One closed by a drain, its Close frames gone and unanswered until the drain's bound.
#[tokio::test]
async fn a_drained_websocket_over_tls_ends_with_a_closure_alert_each_way() {
    let local = tokio::task::LocalSet::new();
    let (said, client, backend) = local.run_until(closed_by_the_gateway_over_tls(true)).await;
    assert!(
        said.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "{said}"
    );
    assert!(client, "no closure alert to the client");
    assert!(backend, "no closure alert to the backend");
}

/// A plaintext backend that speaks WebSocket as far as the handshake and the close go:
/// a 101 with the Accept of the key it was sent, then it reads until a Close frame
/// comes, answers it with a Close of its own, and says everything it read once the
/// gateway has closed its connection.
async fn closing_websocket_backend(saw: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let saw = saw.clone();
            let _serving = tokio::task::spawn_local(async move {
                let mut head = Vec::new();
                let mut byte = [0; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => head.push(byte[0]),
                    }
                }
                let head = String::from_utf8(head).unwrap();
                let key = head
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                    .and_then(|key| Key::read(key.as_bytes()))
                    .unwrap();
                let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                let switched = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
                );
                stream.write_all(switched.as_bytes()).await.unwrap();
                let mut read = Vec::new();
                let mut bytes = [0; 1024];
                let mut answered = false;
                loop {
                    match stream.read(&mut bytes).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => read.extend_from_slice(&bytes[..count]),
                    }
                    if !answered && read.first() == Some(&0x88) {
                        answered = true;
                        // 1000, "Normal Closure", as a server answers a Close.
                        let _ = stream.write_all(&[0x88, 0x02, 0x03, 0xe8]).await;
                    }
                }
                let _ = saw.send(read);
            });
        }
    });
    address
}

/// A draining worker closes an open WebSocket with a Close 1001 each way, takes both
/// answers, and counts it drained (19 §6). With the short deadlines' bound of under
/// five seconds, its moment is at once.
#[tokio::test]
async fn a_draining_worker_closes_websockets_with_going_away() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (saw, mut seen) = tokio::sync::mpsc::unbounded_channel();
            let upstream = closing_websocket_backend(saw).await;
            let proxy = Proxy::new(
                compile(&everything_config(upstream)).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(
                    b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                          connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                          sec-websocket-version: 13\r\n\r\n",
                )
                .await
                .unwrap();
            let mut head = Vec::new();
            let mut byte = [0; 1];
            while !head.ends_with(b"\r\n\r\n") {
                within(client.read_exact(&mut byte)).await.unwrap();
                head.push(byte[0]);
            }
            assert!(head.starts_with(b"HTTP/1.1 101 "));

            worker.drain();
            let mut close = [0; 4];
            within(client.read_exact(&mut close)).await.unwrap();
            assert_eq!(close, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
            // The client answers, masked, as a client must.
            client
                .write_all(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9])
                .await
                .unwrap();
            let mut rest = Vec::new();
            let _closed = within(client.read_to_end(&mut rest)).await;
            assert_eq!(rest, b"", "the backend's answer went on");
            let heard = within(seen.recv()).await.unwrap();
            assert_eq!(heard.len(), 8, "{heard:?}");
            assert_eq!(&heard[..2], &[0x88, 0x82], "{heard:?}");
            let code = [heard[6] ^ heard[2], heard[7] ^ heard[3]];
            assert_eq!(u16::from_be_bytes(code), 1001);
            tunnel_ended(&worker, "web", "drained").await;
        })
        .await;
}

/// A WebSocket open across a reload that keeps its route is left alone; one whose route
/// a reload takes away is closed as a draining worker closes it, with a Close 1001 each
/// way, from the worker's next sweep (03 §10, 19 §6).
#[tokio::test]
async fn a_websocket_whose_route_a_reload_takes_away_is_closed_with_going_away() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (saw, mut seen) = tokio::sync::mpsc::unbounded_channel();
            let upstream = closing_websocket_backend(saw).await;
            let mut config = everything_config(upstream);
            let (front, worker) = serving_swept(compile(&config).unwrap()).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(
                    b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                          connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                          sec-websocket-version: 13\r\n\r\n",
                )
                .await
                .unwrap();
            let mut head = Vec::new();
            let mut byte = [0; 1];
            while !head.ends_with(b"\r\n\r\n") {
                within(client.read_exact(&mut byte)).await.unwrap();
                head.push(byte[0]);
            }
            assert!(head.starts_with(b"HTTP/1.1 101 "));

            // Another route beside it: its own is kept.
            let mut other = config.routes[0].clone();
            other.name = "other".to_owned();
            config.routes.push(other);
            worker.proxy().reload(compile(&config).unwrap()).unwrap();
            let quiet = worker.limits.sweep * 4 + SLACK;
            assert!(
                still_open(&mut client, quiet).await,
                "a WebSocket whose route was kept was sent something"
            );

            // Its route gone, another in its place: drained.
            config.routes.remove(0);
            worker.proxy().reload(compile(&config).unwrap()).unwrap();
            let mut close = [0; 4];
            within(client.read_exact(&mut close)).await.unwrap();
            assert_eq!(close, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
            client
                .write_all(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9])
                .await
                .unwrap();
            let mut rest = Vec::new();
            let _closed = within(client.read_to_end(&mut rest)).await;
            assert_eq!(rest, b"", "the backend's answer went on");
            let heard = within(seen.recv()).await.unwrap();
            assert_eq!(&heard[..2], &[0x88, 0x82], "{heard:?}");
            let code = [heard[6] ^ heard[2], heard[7] ^ heard[3]];
            assert_eq!(u16::from_be_bytes(code), 1001);
            tunnel_ended(&worker, "web", "drained").await;
        })
        .await;
}

/// The same over HTTP/2: the Close frames go inside the stream's DATA frames, the
/// connection's graceful GOAWAY lets the stream finish, and the tunnel ends drained
/// once both answers are in (19 §6).
#[tokio::test]
async fn a_draining_worker_closes_http2_websockets_with_going_away() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (saw, mut seen) = tokio::sync::mpsc::unbounded_channel();
            let upstream = closing_websocket_backend(saw).await;
            let proxy = Proxy::new(
                compile(&everything_config(upstream)).unwrap(),
                NonZeroUsize::MIN,
            )
            .unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let stream = TcpStream::connect(front).await.unwrap();
            let (send, connection) = ::h2::client::handshake(stream).await.unwrap();
            let _driving = tokio::task::spawn_local(async move {
                let _ended = connection.await;
            });
            let mut send = within(send.ready()).await.unwrap();
            until(|| send.is_extended_connect_protocol_enabled()).await;
            let mut request = Request::builder()
                .method(Method::CONNECT)
                .uri("http://a.test/chat")
                .header("sec-websocket-version", "13")
                .body(())
                .unwrap();
            request
                .extensions_mut()
                .insert(::h2::ext::Protocol::from_static("websocket"));
            let (response, mut stream) = send.send_request(request, false).unwrap();
            let response = within(response).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = response.into_body();

            worker.drain();
            let mut heard = Vec::new();
            while heard.len() < 4 {
                let data = within(body.data()).await.unwrap().unwrap();
                let _released = body.flow_control().release_capacity(data.len());
                heard.extend_from_slice(&data);
            }
            assert_eq!(heard, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
            stream
                .send_data(
                    Bytes::from_static(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9]),
                    false,
                )
                .unwrap();
            // Nothing more comes, and the stream ends.
            let mut rest = Vec::new();
            while let Some(data) = within(body.data()).await {
                match data {
                    Ok(data) => rest.extend_from_slice(&data),
                    Err(_) => break,
                }
            }
            assert_eq!(rest, b"", "the backend's answer went on");
            let backend_heard = within(seen.recv()).await.unwrap();
            assert_eq!(&backend_heard[..2], &[0x88, 0x82], "{backend_heard:?}");
            tunnel_ended(&worker, "web", "drained").await;
        })
        .await;
}

/// An extended CONNECT's head over HTTP/3, for `protocol` at `/chat` of `a.test`.
fn h3_connect(protocol: &str) -> Vec<(&str, &str)> {
    vec![
        (":method", "CONNECT"),
        (":protocol", protocol),
        (":scheme", "https"),
        (":authority", "a.test"),
        (":path", "/chat"),
        ("sec-websocket-version", "13"),
    ]
}

/// WebSocket over HTTP/3 (RFC 9220): the connection announces extended CONNECT, the
/// client is told 200, and the stream carries the bytes both ways, FIN each way a
/// half-close (19 §3).
#[tokio::test]
async fn an_http3_websocket_is_carried_to_an_http1_backend() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let upstream = echoing_websocket_backend().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, proxy) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            client
                .until(|client| {
                    client
                        .h3
                        .as_ref()
                        .is_some_and(quiche::h3::Connection::extended_connect_enabled_by_peer)
                })
                .await;
            let id = client.request(&h3_connect("websocket"), false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.body.ends_with(b"hello"))
                })
                .await;
            let answer = client.answers.get(&id).unwrap().clone();
            assert_eq!(answer.final_status(), Some("200"), "{answer:?}");
            assert!(
                !answer.heads[0]
                    .iter()
                    .any(|(name, _)| name == "sec-websocket-accept"),
                "{answer:?}"
            );
            client.body(id, b"ping", false).await;
            client
                .until(|client| {
                    client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.body.ends_with(b"[ping]"))
                })
                .await;
            client.body(id, b"", true).await;
            let answer = client.answer(id).await;
            assert!(answer.finished, "{answer:?}");
            assert_eq!(answer.body, b"hello[ping]bye");
            let line = "edgerush_listener_tunnels_total{listener=\"web\",outcome=\"closed\"} 1\n";
            until(|| proxy.metrics().contains(line)).await;
        })
        .await;
}

/// A WebSocket over HTTP/3 counts as one of its worker's connections while it is open,
/// as over HTTP/2: with no room left for one, the next is answered 503 (03 §9).
#[tokio::test]
async fn http3_websockets_count_as_connections_of_their_worker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let upstream = echoing_websocket_backend().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let config = compile(&h3_config(upstream, http3)).unwrap();
            let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN).unwrap());
            // Room for the client's connection, which counts as an HTTP/3 connection
            // does, and one WebSocket.
            let connections = Loads::new(1, 3 + 1, QUIC_MOST, 1);
            let worker = Worker::made(
                Arc::clone(&proxy),
                H1Limits::default(),
                SHORT,
                0,
                Some(Arc::clone(&connections)),
            );
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
            let alone = Forwarding::group(1).remove(0);
            let _serving =
                tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone).unwrap());
            let mut client = Client::connect(front, "a.test").await;
            client
                .until(|client| {
                    client
                        .h3
                        .as_ref()
                        .is_some_and(quiche::h3::Connection::extended_connect_enabled_by_peer)
                })
                .await;
            let first = client.request(&h3_connect("websocket"), false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&first)
                        .is_some_and(|answer| answer.body.ends_with(b"hello"))
                })
                .await;
            assert_eq!(connections.now(), [3 + 1]);
            let second = client.request(&h3_connect("websocket"), false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&second)
                        .is_some_and(|answer| answer.final_status().is_some())
                })
                .await;
            let answer = client.answers.get(&second).unwrap().clone();
            assert_eq!(answer.final_status(), Some("503"), "{answer:?}");
            assert_eq!(connections.now(), [3 + 1]);
        })
        .await;
}

/// An extended CONNECT over HTTP/3 for anything but WebSocket is answered 501 (RFC 9220
/// §3), and nothing goes upstream.
#[tokio::test]
async fn an_http3_extended_connect_for_another_protocol_is_answered_501() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, opened) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            let id = client.request(&h3_connect("connect-udp"), false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.final_status().is_some())
                })
                .await;
            let answer = client.answers.get(&id).unwrap().clone();
            assert_eq!(answer.final_status(), Some("501"), "{answer:?}");
            assert_eq!(opened.load(Ordering::SeqCst), 0);
        })
        .await;
}

/// The same over HTTP/3: an extended CONNECT that says it is gRPC is still answered 501,
/// not 200 with a gRPC status (RFC 9110 §9.3.6).
#[tokio::test]
async fn an_http3_extended_connect_that_says_grpc_is_never_answered_2xx() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, opened) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            let mut head = h3_connect("connect-udp");
            head.push(("content-type", "application/grpc"));
            let id = client.request(&head, false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.final_status().is_some())
                })
                .await;
            let answer = client.answers.get(&id).unwrap().clone();
            assert_eq!(answer.final_status(), Some("501"), "{answer:?}");
            assert!(
                !answer.heads[0]
                    .iter()
                    .any(|(name, _)| name == "grpc-status"),
                "{answer:?}"
            );
            assert_eq!(opened.load(Ordering::SeqCst), 0);
        })
        .await;
}

/// A backend whose connection fails under an HTTP/3 WebSocket has the client's stream
/// reset with `H3_REQUEST_CANCELLED` (RFC 9220 §3), and the tunnel counted as failed.
#[tokio::test]
async fn a_failing_backend_resets_an_http3_websocket() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = socket.local_addr().unwrap();
            let _accepting = tokio::task::spawn_local(async move {
                let (mut stream, _) = socket.accept().await.unwrap();
                let mut head = Vec::new();
                let mut byte = [0; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8(head).unwrap();
                let key = head
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                    .and_then(|key| Key::read(key.as_bytes()))
                    .unwrap();
                let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                let switched = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
                );
                stream.write_all(switched.as_bytes()).await.unwrap();
                // Once the client has been carried to it, the connection fails.
                let _heard = stream.read(&mut byte).await;
                let _unset = stream.set_zero_linger();
                drop(stream);
            });
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, proxy) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            client
                .until(|client| {
                    client
                        .h3
                        .as_ref()
                        .is_some_and(quiche::h3::Connection::extended_connect_enabled_by_peer)
                })
                .await;
            let id = client.request(&h3_connect("websocket"), false);
            client
                .until(|client| {
                    client
                        .answers
                        .get(&id)
                        .is_some_and(|answer| answer.final_status().is_some())
                })
                .await;
            client.body(id, b"x", false).await;
            let answer = client.answer(id).await;
            assert_eq!(
                answer.reset,
                Some(crate::downstream::h3::code::REQUEST_CANCELLED),
                "{answer:?}"
            );
            let line = "edgerush_listener_tunnels_total{listener=\"web\",outcome=\"failed\"} 1\n";
            until(|| proxy.metrics().contains(line)).await;
        })
        .await;
}
