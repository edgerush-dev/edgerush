//! Upstreams and backends for a worker under test to talk to.

use super::*;

/// An upstream that reads a request's head, says `first`, and says `then` only once
/// `gate` opens: what it said first cannot have waited for what it says after.
pub(super) async fn gated_upstream(
    first: &'static [u8],
    gate: tokio::sync::oneshot::Receiver<()>,
    then: &'static [u8],
) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = backend.local_addr().unwrap();
    let _answering = tokio::task::spawn_local(async move {
        let (mut stream, _) = backend.accept().await.unwrap();
        let mut seen = Vec::new();
        let mut byte = [0; 1];
        while !seen.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => return,
                Ok(_) => seen.push(byte[0]),
            }
        }
        let _ = stream.write_all(first).await;
        let _ = gate.await;
        let _ = stream.write_all(then).await;
        let mut rest = Vec::new();
        let _ = stream.read_to_end(&mut rest).await;
    });
    address
}

/// A backend that reads what each connection sends until its end, answers with how
/// many bytes came and their sum, and closes.
pub(super) async fn tallying_backend() -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            tokio::task::spawn_local(async move {
                let mut came = Vec::new();
                stream.read_to_end(&mut came).await.unwrap();
                let sum: u64 = came.iter().map(|&byte| u64::from(byte)).sum();
                let answer = format!("{} {sum}", came.len());
                stream.write_all(answer.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
    });
    address
}

/// A TLS backend that, once its handshake is done, says `says` and closes.
pub(super) async fn tls_backend(says: &'static str) -> SocketAddr {
    use boring::pkey::PKey;
    use boring::ssl::{SslAcceptor, SslMethod};
    use boring::x509::X509;
    use tokio::io::AsyncWriteExt;
    let certificate = crate::tls::testing::certificate(&["backend.test"]);
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    acceptor
        .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
        .unwrap();
    acceptor
        .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
        .unwrap();
    let acceptor = Rc::new(acceptor.build());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        while let Ok((stream, _)) = socket.accept().await {
            let acceptor = Rc::clone(&acceptor);
            tokio::task::spawn_local(async move {
                if let Ok(mut secured) = tokio_boring::accept(&acceptor, stream).await {
                    let _said = secured.write_all(says.as_bytes()).await;
                    let _closed = secured.shutdown().await;
                }
            });
        }
    });
    address
}

/// What an HTTP/2 upstream of the tests saw.
#[derive(Debug, Default)]
pub(super) struct SeenUp {
    /// Connections accepted.
    pub(super) connections: Cell<usize>,
    /// Streams open now.
    pub(super) open: Cell<usize>,
    /// Every request's head, and how many bytes of body came with it.
    pub(super) requests: RefCell<Vec<(Request<()>, usize)>>,
}

/// How an HTTP/2 upstream of the tests behaves.
#[derive(Clone, Copy)]
pub(super) struct UpstreamH2 {
    /// What it announces as its limit on streams.
    pub(super) streams: u32,
    /// Answer only once `gate` lets it: for requests to be held open.
    pub(super) gated: bool,
    /// Send the answer's head before reading the body, then read it, then end the
    /// answer: as a server of a bidirectional stream does.
    pub(super) early: bool,
    /// Tell the client to go away after this many requests on a connection.
    pub(super) away_after: Option<usize>,
}

impl Default for UpstreamH2 {
    fn default() -> Self {
        Self {
            streams: 100,
            gated: false,
            early: false,
            away_after: None,
        }
    }
}

/// An upstream that speaks HTTP/2 by prior knowledge, answering each request `200`
/// with `ok` and the length of the body it read in `x-read`. With `gated`, each
/// answer waits for a permit of `gate`.
pub(super) async fn h2_upstream(
    how: UpstreamH2,
) -> (SocketAddr, Rc<SeenUp>, Rc<tokio::sync::Semaphore>) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let seen = Rc::new(SeenUp::default());
    let gate = Rc::new(tokio::sync::Semaphore::new(0));
    let (seeing, gating) = (Rc::clone(&seen), Rc::clone(&gate));
    let _accepting = tokio::task::spawn_local(async move {
        loop {
            let Ok((stream, _)) = socket.accept().await else {
                return;
            };
            seeing.connections.set(seeing.connections.get() + 1);
            let (seeing, gating) = (Rc::clone(&seeing), Rc::clone(&gating));
            let _serving = tokio::task::spawn_local(async move {
                let mut builder = ::h2::server::Builder::new();
                builder.max_concurrent_streams(how.streams);
                let Ok(mut connection) = builder.handshake::<_, Bytes>(stream).await else {
                    return;
                };
                let mut served = 0;
                while let Some(Ok((request, mut respond))) = connection.accept().await {
                    served += 1;
                    if how.away_after == Some(served) {
                        connection.graceful_shutdown();
                    }
                    let (seeing, gating) = (Rc::clone(&seeing), Rc::clone(&gating));
                    let _answering = tokio::task::spawn_local(async move {
                        seeing.open.set(seeing.open.get() + 1);
                        let (head, mut body) = request.into_parts();
                        let at = seeing.requests.borrow().len();
                        seeing
                            .requests
                            .borrow_mut()
                            .push((Request::from_parts(head, ()), 0));
                        let mut read = 0;
                        // Read in full by `read_all`.
                        if !how.early {
                            read = read_all(&mut body).await;
                        }
                        if how.gated {
                            let _permit = gating.acquire().await.unwrap();
                            _permit.forget();
                        }
                        let answer = Response::builder()
                            .status(200)
                            .header("x-read", read.to_string())
                            .body(())
                            .unwrap();
                        if let Ok(mut sending) = respond.send_response(answer, false) {
                            if how.early {
                                read = read_all(&mut body).await;
                            }
                            let _ = sending.send_data(Bytes::from_static(b"ok"), true);
                        }
                        seeing.requests.borrow_mut()[at].1 = read;
                        seeing.open.set(seeing.open.get() - 1);
                    });
                }
            });
        }
    });
    (address, seen, gate)
}

/// Reads a body h2 received to its end, giving the credit back, and says how much.
pub(super) async fn read_all(body: &mut ::h2::RecvStream) -> usize {
    let mut read = 0;
    while let Some(Ok(data)) = body.data().await {
        read += data.len();
        let _ = body.flow_control().release_capacity(data.len());
    }
    read
}

/// An HTTP/1 upstream answering its first request `first` and the rest `200`, one
/// request to a connection, and every head it was sent, in lower case.
pub(super) fn recording_upstream(first: &'static str) -> (SocketAddr, Rc<RefCell<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    let socket = TcpListener::from_std(socket).unwrap();
    let heads = Rc::new(RefCell::new(Vec::<String>::new()));
    let seen = Rc::clone(&heads);
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let mut read = Vec::new();
            let mut chunk = [0; 4096];
            while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => read.extend_from_slice(&chunk[..n]),
                }
            }
            let status = if seen.borrow().is_empty() {
                first
            } else {
                "200 OK"
            };
            seen.borrow_mut()
                .push(String::from_utf8_lossy(&read).to_lowercase());
            let answer =
                format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    (address, heads)
}

/// How a scripted HTTP/2 upstream answers each request.
pub(super) type Script = Rc<
    dyn Fn(
        Request<::h2::RecvStream>,
        ::h2::server::SendResponse<Bytes>,
    ) -> Pin<Box<dyn Future<Output = ()>>>,
>;

/// An upstream that speaks HTTP/2 by prior knowledge and answers every request as
/// `script` does.
pub(super) async fn scripted_h2_upstream(script: Script) -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((stream, _)) = socket.accept().await {
            // As a real server's: with Nagle's algorithm on, the first answer's frames
            // wait for the ACK of the handshake's, which Linux delays 40 ms and more,
            // longer than a script that times a reset after its head allows.
            stream.set_nodelay(true).unwrap();
            let script = Rc::clone(&script);
            let _serving = tokio::task::spawn_local(async move {
                let Ok(mut connection) = ::h2::server::handshake(stream).await else {
                    return;
                };
                while let Some(Ok((request, respond))) = connection.accept().await {
                    let _answering = tokio::task::spawn_local(script(request, respond));
                }
            });
        }
    });
    address
}

pub(super) fn ok_head() -> Response<()> {
    Response::builder().status(200).body(()).unwrap()
}

/// How a TLS upstream of the tests settles what it speaks.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Agrees {
    /// `h2` if offered, else `http/1.1`; and speaks what it agreed on.
    Either,
    /// Agrees on nothing, and speaks HTTP/2 regardless.
    NothingButSpeaksH2,
}

/// An upstream that speaks TLS with `certificate`, settling what it speaks as `agrees`
/// says. Every request is answered `200` with `ok`.
pub(super) async fn tls_upstream(
    certificate: &edgerush_config::Certificate,
    agrees: Agrees,
) -> SocketAddr {
    tls_upstream_asking(certificate, agrees, None).await
}

/// The same, requiring a client certificate `clients` vouches for, if given.
pub(super) async fn tls_upstream_asking(
    certificate: &edgerush_config::Certificate,
    agrees: Agrees,
    clients: Option<&edgerush_config::Certificate>,
) -> SocketAddr {
    use boring::pkey::PKey;
    use boring::ssl::{AlpnError, SslAcceptor, SslMethod, SslVerifyMode, select_next_proto};
    use boring::x509::X509;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    if let Some(clients) = clients {
        let mut trusted = boring::x509::store::X509StoreBuilder::new().unwrap();
        trusted
            .add_cert(X509::from_pem(clients.chain.as_bytes()).unwrap())
            .unwrap();
        builder.set_verify_cert_store(trusted.build()).unwrap();
        builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    builder
        .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
        .unwrap();
    builder
        .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
        .unwrap();
    builder.set_alpn_select_callback(move |_, client| match agrees {
        Agrees::Either => select_next_proto(b"\x02h2\x08http/1.1", client).ok_or(AlpnError::NOACK),
        Agrees::NothingButSpeaksH2 => Err(AlpnError::NOACK),
    });
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
                if agrees == Agrees::NothingButSpeaksH2
                    || secured.ssl().selected_alpn_protocol() == Some(&b"h2"[..])
                {
                    let Ok(mut connection) = ::h2::server::handshake(secured).await else {
                        return;
                    };
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        // A request for another scheme is not this server's to answer
                        // (RFC 9110 §15.5.20), as a backend that checks it has it; its
                        // health service says so too.
                        let https = request.uri().scheme_str() == Some("https");
                        if request.uri().path() == "/grpc.health.v1.Health/Check" {
                            let _ = read_all(&mut request.into_body()).await;
                            let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                                continue;
                            };
                            let status = if https { 1 } else { 2 };
                            let message = [0, 0, 0, 0, 2, 0x08, status];
                            let _ = sending.send_data(Bytes::copy_from_slice(&message), false);
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", HeaderValue::from_static("0"));
                            let _ = sending.send_trailers(trailers);
                            continue;
                        }
                        if !https {
                            let misdirected = Response::builder().status(421).body(()).unwrap();
                            let _ = respond.send_response(misdirected, true);
                            continue;
                        }
                        if let Ok(mut sending) = respond.send_response(ok_head(), false) {
                            let _ = sending.send_data(Bytes::from_static(b"ok"), true);
                        }
                    }
                    return;
                }
                let mut seen = Vec::new();
                let mut byte = [0; 1];
                loop {
                    match secured.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => seen.push(byte[0]),
                    }
                    if seen.ends_with(b"\r\n\r\n") {
                        seen.clear();
                        let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                        if secured.write_all(answer).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    address
}

pub(super) fn grpc_head() -> Response<()> {
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(())
        .unwrap()
}

/// An HTTP/1 upstream answering its requests with `statuses` in turn, the last of them
/// from then on, one request to a connection; and the bodies it was sent.
pub(super) async fn statuses_upstream(
    statuses: Vec<u16>,
) -> (SocketAddr, Rc<RefCell<Vec<Vec<u8>>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let bodies = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::clone(&bodies);
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let mut read = Vec::new();
            let mut chunk = [0; 16 * 1024];
            let head_end = loop {
                if let Some(at) = read.windows(4).position(|four| four == b"\r\n\r\n") {
                    break at + 4;
                }
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => read.extend_from_slice(&chunk[..n]),
                }
            };
            let head = String::from_utf8_lossy(&read[..head_end]).to_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .map_or(0, |length| length.trim().parse().unwrap());
            while read.len() < head_end + length {
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => read.extend_from_slice(&chunk[..n]),
                }
            }
            let turn = seen.borrow().len();
            seen.borrow_mut().push(read[head_end..].to_vec());
            let status = statuses[turn.min(statuses.len() - 1)];
            let answer =
                format!("HTTP/1.1 {status} X\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok");
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    (address, bodies)
}

/// An upstream that answers by the last part of the path it is asked for, one request
/// after another on a connection: `nothing` with a 204, `ok` with a short body,
/// `endless` with a body that never ends, `short` with a body that stops part way, and
/// anything else not at all, its connection held open in what is returned.
pub(super) async fn scripted_upstream() -> (SocketAddr, Rc<RefCell<Vec<TcpStream>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = backend.local_addr().unwrap();
    let held = Rc::new(RefCell::new(Vec::new()));
    let holding = Rc::clone(&held);
    let _accepting = tokio::task::spawn_local(async move {
        loop {
            let (mut stream, _) = backend.accept().await.unwrap();
            let holding = Rc::clone(&holding);
            let _answering = tokio::task::spawn_local(async move {
                let mut seen = Vec::new();
                let mut byte = [0; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => seen.push(byte[0]),
                    }
                    if !seen.ends_with(b"\r\n\r\n") {
                        continue;
                    }
                    let asked = String::from_utf8_lossy(&seen).into_owned();
                    seen.clear();
                    let target = asked.split(' ').nth(1).unwrap_or_default();
                    let answer: &[u8] = match target.rsplit('/').next() {
                        Some("nothing") => b"HTTP/1.1 204 No Content\r\n\r\n",
                        Some("ok") => b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
                        Some("endless") => {
                            let head = b"HTTP/1.1 200 OK\r\ncontent-length: 1000000000000\r\n\r\n";
                            let mut said = stream.write_all(head).await;
                            while said.is_ok() {
                                said = stream.write_all(&[b'x'; 16 * 1024]).await;
                            }
                            return;
                        }
                        Some("short") => {
                            let head = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nshort";
                            let _said = stream.write_all(head).await;
                            // Long enough for the head to be on its way to the client
                            // before the body stops.
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            return;
                        }
                        _ => {
                            holding.borrow_mut().push(stream);
                            return;
                        }
                    };
                    if stream.write_all(answer).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (address, held)
}

/// What one HTTP/1.1 request for `path` to `address` is answered with.
/// An address that refuses every connection for as long as the socket returned with it
/// is held: bound, so that nothing else can be given its port, and never listening.
///
/// **Not a port read from a socket that was then let go of.** That hands the port back
/// for the operating system to give to whatever asks next, and a test running beside
/// this one was given it and answered 200 where a 502 was expected.
pub(super) fn refusing() -> (tokio::net::TcpSocket, SocketAddr) {
    let held = tokio::net::TcpSocket::new_v4().unwrap();
    held.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = held.local_addr().unwrap();
    (held, address)
}

/// An upstream that answers every request on whatever connection it arrives on, and
/// counts how many connections it was given. Its answers are bytes, so that what is
/// read back is what really went over the wire.
pub(super) async fn counting_upstream() -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counting = Arc::clone(&opened);
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = socket.accept().await.unwrap();
            counting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                // One request after another on the one connection.
                let mut seen = Vec::new();
                let mut byte = [0; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => seen.push(byte[0]),
                    }
                    if seen.ends_with(b"\r\n\r\n") {
                        seen.clear();
                        let answer = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                        if stream.write_all(answer.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    (address, opened)
}

/// A plaintext backend that speaks WebSocket as far as the handshake goes: a 101 with
/// the Accept of the key it was sent and `hello` after it, then what it reads sent back,
/// the first time in brackets, and `bye` once its peer ends its side.
pub(super) async fn echoing_websocket_backend() -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
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
                let Some(key) = head
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                    .and_then(|key| Key::read(key.as_bytes()))
                else {
                    let _ = stream
                        .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                        .await;
                    return;
                };
                let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                let switched = format!(
                    "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\nhello"
                );
                if stream.write_all(switched.as_bytes()).await.is_err() {
                    return;
                }
                let mut bytes = [0; 1024];
                let mut first = true;
                loop {
                    let read = match stream.read(&mut bytes).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => read,
                    };
                    let said = String::from_utf8_lossy(&bytes[..read]).into_owned();
                    let echo = if first {
                        first = false;
                        format!("[{said}]")
                    } else {
                        said
                    };
                    if stream.write_all(echo.as_bytes()).await.is_err() {
                        return;
                    }
                }
                let _ = stream.write_all(b"bye").await;
                let _ = stream.shutdown().await;
            });
        }
    });
    address
}
