//! Listeners that speak TLS: their certificates, the protocol agreed, and client validation.

use super::*;

/// Serves a worker whose listener speaks TLS with `certificates`, sending everything
/// to `upstream`, and says where.
async fn serving_secured_worker(
    upstream: SocketAddr,
    certificates: Vec<edgerush_config::Certificate>,
) -> (SocketAddr, Rc<Worker>) {
    let config = everything_secured_to(upstream, certificates);
    let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A TCP stream that keeps every byte it reads: what arrived on the wire, records and
/// all, under the TLS the client puts over it.
#[derive(Debug)]
struct Tapped {
    stream: TcpStream,
    read: Rc<RefCell<Vec<u8>>>,
}

impl AsyncRead for Tapped {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.stream).poll_read(context, buf);
        this.read
            .borrow_mut()
            .extend_from_slice(&buf.filled()[before..]);
        polled
    }
}

impl AsyncWrite for Tapped {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(context, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}

/// An answer over TLS is one record, its head and body sealed together, as it is one
/// write in plain TCP: sealed a piece at a time it was two records and two sends,
/// which cost more than the rest of TLS. Counted on the second answer of a connection:
/// the first comes with the session tickets.
#[tokio::test]
async fn an_answer_over_tls_is_one_record() {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::NONE);
            let config = builder.build().configure().unwrap().verify_hostname(false);
            let read = Rc::new(RefCell::new(Vec::new()));
            let tapped = Tapped {
                stream: TcpStream::connect(front).await.unwrap(),
                read: Rc::clone(&read),
            };
            let mut client = within(tokio_boring::connect(config, "example.test", tapped))
                .await
                .unwrap();
            let mut answers = Vec::new();
            let mut arrived = 0;
            for _ in 0..2 {
                arrived = read.borrow().len();
                client
                    .write_all(b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n")
                    .await
                    .unwrap();
                let mut answer = Vec::new();
                while !(answer.ends_with(b"ok") && answer.windows(4).any(|w| w == b"\r\n\r\n")) {
                    let mut some = [0; 512];
                    let got = within(client.read(&mut some)).await.unwrap();
                    assert_ne!(got, 0, "{:?}", String::from_utf8_lossy(&answer));
                    answer.extend_from_slice(&some[..got]);
                }
                answers.push(String::from_utf8_lossy(&answer).into_owned());
            }
            assert!(
                answers[1].starts_with("HTTP/1.1 200 OK\r\n"),
                "{}",
                answers[1]
            );
            // The records the second answer came in: a type, a version and a length each.
            let wire = read.borrow()[arrived..].to_vec();
            let mut records = 0;
            let mut at = 0;
            while let Some(header) = wire.get(at..at + 5) {
                records += 1;
                at += 5 + usize::from(u16::from_be_bytes([header[3], header[4]]));
            }
            assert_eq!(at, wire.len(), "records cut short");
            assert_eq!(records, 1, "{} bytes in {records} records", wire.len());
        })
        .await;
}

/// An `https` listener speaks HTTP/2 to a client that agreed on it in the handshake,
/// and HTTP/1.1 to one that agreed on that or on nothing (RFC 9113 §3.2).
#[tokio::test]
async fn an_https_listener_speaks_what_the_handshake_agreed_on() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;

            let offered: &[u8] = b"\x08http/1.1";
            let h1 = tls_client(front, "example.test", Some(offered), |_| {})
                .await
                .unwrap();
            assert_eq!(h1.ssl().selected_alpn_protocol(), Some(&b"http/1.1"[..]));
            let answer = h1_over(h1).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

            let none = tls_client(front, "example.test", None, |_| {})
                .await
                .unwrap();
            assert_eq!(none.ssl().selected_alpn_protocol(), None);
            let answer = h1_over(none).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

            // Offered both, it is given HTTP/2, whatever the client's order.
            let both: &[u8] = b"\x08http/1.1\x02h2";
            let h2 = tls_client(front, "example.test", Some(both), |_| {})
                .await
                .unwrap();
            assert_eq!(h2.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
            let (mut send, connection) = within(::h2::client::handshake(h2)).await.unwrap();
            let _driving = tokio::task::spawn_local(async move {
                let _ended = connection.await;
            });
            let request = Request::get("https://example.test/").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
        })
        .await;
}

/// A listener that validates clients serves only one that shows a certificate its
/// authorities vouch for — whichever of its certificates the client asked for.
#[tokio::test]
async fn a_listener_that_validates_clients_serves_only_those_it_trusts() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let (upstream, _) = counting_upstream().await;
            let trusted = certificate(&["client"]);
            let stranger = certificate(&["client"]);
            let mut config = everything_config(upstream);
            secured(
                &mut config,
                vec![certificate(&["a.test"]), certificate(&["b.test"])],
                Some(edgerush_config::ClientValidation {
                    authorities: vec![trusted.chain.clone()],
                }),
            );
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            // What a client asking for `name`, showing `shown`, is answered: nothing at
            // all if the handshake, or the first read after it, failed.
            let answered = |name: &'static str, shown: Option<edgerush_config::Certificate>| async move {
                let connected = tls_client(front, name, None, |builder| {
                    if let Some(shown) = &shown {
                        let chain = boring::x509::X509::from_pem(shown.chain.as_bytes()).unwrap();
                        let key = boring::pkey::PKey::private_key_from_pem(shown.key.as_bytes()).unwrap();
                        builder.set_certificate(&chain).unwrap();
                        builder.set_private_key(&key).unwrap();
                    }
                })
                .await;
                match connected {
                    Ok(stream) => h1_over_or_nothing(stream).await,
                    Err(_) => String::new(),
                }
            };
            for name in ["a.test", "b.test"] {
                let answer = answered(name, Some(trusted.clone())).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{name}: {answer}");
                let answer = answered(name, None).await;
                assert_eq!(answer, "", "{name}: served a client with no certificate");
                let answer = answered(name, Some(stranger.clone())).await;
                assert_eq!(answer, "", "{name}: served a client nobody vouches for");
            }
        })
        .await;
}

/// A config of one `https` listener, `web`, presenting `served` and validating clients
/// against `authority`, that sends everything to `upstream`.
fn validating_to(
    upstream: SocketAddr,
    served: &edgerush_config::Certificate,
    authority: &edgerush_config::Certificate,
) -> Compiled {
    let mut config = everything_config(upstream);
    secured(
        &mut config,
        vec![served.clone()],
        Some(edgerush_config::ClientValidation {
            authorities: vec![authority.chain.clone()],
        }),
    );
    compile(&config).unwrap()
}

/// Sets a TLS client up to show `shown` when asked who it is.
fn showing(
    shown: &edgerush_config::Certificate,
) -> impl Fn(&mut boring::ssl::SslConnectorBuilder) + Copy + '_ {
    move |builder| {
        let chain = boring::x509::X509::from_pem(shown.chain.as_bytes()).unwrap();
        let key = boring::pkey::PKey::private_key_from_pem(shown.key.as_bytes()).unwrap();
        builder.set_certificate(&chain).unwrap();
        builder.set_private_key(&key).unwrap();
    }
}

/// Asks `client` for `/` on a connection it keeps, and reads the answer, the counting
/// upstream's `ok`.
async fn kept_answer<S>(client: &mut S) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    client
        .write_all(b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    while !answer.ends_with(b"ok") {
        let mut some = [0; 512];
        let got = within(client.read(&mut some)).await.unwrap();
        assert_ne!(got, 0, "{:?}", String::from_utf8_lossy(&answer));
        answer.extend_from_slice(&some[..got]);
    }
    String::from_utf8_lossy(&answer).into_owned()
}

/// A reload that takes away the authority that vouched for a client drains the
/// connections accepted under the validation before, as the worker's drain would, from
/// the worker's next sweep: one kept between requests is closed then, long before its
/// keep-alive would close it, and a new connection is refused (03 §3, §10).
#[tokio::test]
async fn a_reload_that_replaces_a_listeners_client_validation_drains_its_connections() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let (upstream, _) = counting_upstream().await;
            let served = certificate(&["example.test"]);
            let (trusted, other) = (certificate(&["client"]), certificate(&["client"]));
            let (front, worker) = serving_swept(validating_to(upstream, &served, &trusted)).await;
            let mut client = tls_client(front, "example.test", None, showing(&trusted))
                .await
                .unwrap();
            let answer = kept_answer(&mut client).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

            worker
                .proxy()
                .reload(validating_to(upstream, &served, &other))
                .unwrap();
            let reloaded = tokio::time::Instant::now();
            let mut rest = Vec::new();
            let _ended = within(client.read_to_end(&mut rest)).await;
            let took = reloaded.elapsed();
            assert!(
                rest.is_empty(),
                "said more: {}",
                String::from_utf8_lossy(&rest)
            );
            assert!(
                took < worker.limits.sweep + SLACK && took < SHORT.next_request,
                "closed {took:?} after the reload"
            );
            let refused = match tls_client(front, "example.test", None, showing(&trusted)).await {
                Ok(stream) => h1_over_or_nothing(stream).await,
                Err(_) => String::new(),
            };
            assert_eq!(refused, "", "a new connection served after the reload");
        })
        .await;
}

/// The same over HTTP/2: the connection is told to go, and goes, from the next sweep.
#[tokio::test]
async fn an_http2_connection_under_a_replaced_client_validation_is_told_to_go() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let (upstream, _) = counting_upstream().await;
            let served = certificate(&["example.test"]);
            let (trusted, other) = (certificate(&["client"]), certificate(&["client"]));
            let (front, worker) = serving_swept(validating_to(upstream, &served, &trusted)).await;
            let offered: &[u8] = b"\x02h2";
            let stream = tls_client(front, "example.test", Some(offered), showing(&trusted))
                .await
                .unwrap();
            let (mut send, connection) = within(::h2::client::handshake(stream)).await.unwrap();
            let ended = Rc::new(Cell::new(None));
            let ending = Rc::clone(&ended);
            let _driving = tokio::task::spawn_local(async move {
                let _ended = connection.await;
                ending.set(Some(tokio::time::Instant::now()));
            });
            let request = Request::get("https://example.test/").body(()).unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);

            worker
                .proxy()
                .reload(validating_to(upstream, &served, &other))
                .unwrap();
            let reloaded = tokio::time::Instant::now();
            until(|| ended.get().is_some()).await;
            let took = ended.get().unwrap() - reloaded;
            assert!(
                took < worker.limits.sweep + SLACK && took < SHORT.next_request,
                "closed {took:?} after the reload"
            );
            drop(send);
        })
        .await;
}

/// The same over HTTP/3, through a reload that starts validating clients, which is a
/// validation of its own: the QUIC connection accepted before it is told to go from the
/// next sweep, long before its keep-alive would tell it.
#[tokio::test]
async fn an_http3_connection_under_a_replaced_client_validation_is_told_to_go() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, _) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let mut config = h3_config(upstream, http3);
            let limits = H1Limits {
                sweep: Duration::from_millis(50),
                ..H1Limits::default()
            };
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let alone = Forwarding::group(1).remove(0);
            let _serving =
                tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone).unwrap());
            let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let mut client = Client::connect(front, "a.test").await;
            assert_eq!(client.get("a.test", "/").await.final_status(), Some("200"));

            let authority = crate::tls::testing::certificate(&["client"]);
            let web = config.listeners.get_mut("web").unwrap();
            web.tls.as_mut().unwrap().client_validation = Some(edgerush_config::ClientValidation {
                authorities: vec![authority.chain],
            });
            worker.proxy().reload(compile(&config).unwrap()).unwrap();
            let reloaded = tokio::time::Instant::now();
            client.until(|client| client.goaway.is_some()).await;
            let took = reloaded.elapsed();
            assert!(
                took < worker.limits.sweep + SLACK && took < SHORT.next_request,
                "told to go {took:?} after the reload"
            );
        })
        .await;
}

/// New certificates behind the same client validation drain nothing: a connection kept
/// across the reload, and several sweeps after it, is still served.
#[tokio::test]
async fn a_reload_that_rotates_certificates_alone_drains_no_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let (upstream, _) = counting_upstream().await;
            let trusted = certificate(&["client"]);
            let served = certificate(&["example.test"]);
            let (front, worker) = serving_swept(validating_to(upstream, &served, &trusted)).await;
            let mut client = tls_client(front, "example.test", None, showing(&trusted))
                .await
                .unwrap();
            let answer = kept_answer(&mut client).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

            let rotated = certificate(&["example.test"]);
            worker
                .proxy()
                .reload(validating_to(upstream, &rotated, &trusted))
                .unwrap();
            tokio::time::sleep(worker.limits.sweep * 4).await;
            let answer = kept_answer(&mut client).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert!(!answer.contains("connection: close"), "{answer}");
        })
        .await;
}

/// What a worker's connections drain with follows the client validation they were
/// accepted under: one drain for as long as a reload keeps it, certificate rotations
/// included; a reload that replaces it starts it, at the sweep or at the next accept,
/// whichever comes first; the worker's own drain starts them all, and any made after.
#[tokio::test]
async fn connections_drain_with_the_client_validation_they_were_accepted_under() {
    use crate::tls::testing::certificate;
    let upstream: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let served = certificate(&["example.test"]);
    let (trusted, other) = (certificate(&["client"]), certificate(&["client"]));
    let proxy = Proxy::new(
        validating_to(upstream, &served, &trusted),
        NonZeroUsize::MIN,
    )
    .unwrap();
    let worker = Worker::new(Arc::new(proxy));
    let tls_now = || worker.proxy.current.load().tls[0].clone();
    let first = worker.drain_for(0, tls_now().as_ref());
    assert!(Rc::ptr_eq(&first, &worker.drain_for(0, tls_now().as_ref())));

    // New certificates, the same validation: the same drain.
    let rotated = certificate(&["example.test"]);
    worker
        .proxy
        .reload(validating_to(upstream, &rotated, &trusted))
        .unwrap();
    worker.revalidate();
    assert!(!first.is_on());
    assert!(Rc::ptr_eq(&first, &worker.drain_for(0, tls_now().as_ref())));

    // Another validation: started at the sweep, and a new one for what comes after.
    worker
        .proxy
        .reload(validating_to(upstream, &served, &other))
        .unwrap();
    worker.revalidate();
    assert!(first.is_on(), "the validation before was not drained");
    let second = worker.drain_for(0, tls_now().as_ref());
    assert!(!second.is_on());

    // Seen first by a connection accepted before the sweep: started then.
    worker
        .proxy
        .reload(validating_to(upstream, &served, &trusted))
        .unwrap();
    let third = worker.drain_for(0, tls_now().as_ref());
    assert!(second.is_on(), "the validation before was not drained");
    assert!(!third.is_on());
    worker.revalidate();
    assert!(!third.is_on(), "the sweep drained the validation in force");

    // No TLS at all is a validation of its own.
    worker.proxy.reload(everything_to(upstream)).unwrap();
    worker.revalidate();
    assert!(third.is_on());
    let plain = worker.drain_for(0, None);
    assert!(!plain.is_on());

    worker.drain();
    assert!(
        plain.is_on(),
        "the worker's drain did not reach its connections"
    );
    assert!(worker.drain_for(0, None).is_on());
    worker
        .proxy
        .reload(validating_to(upstream, &served, &trusted))
        .unwrap();
    assert!(
        worker.drain_for(0, tls_now().as_ref()).is_on(),
        "a draining worker accepted under a drain not started"
    );
}

/// A reload with new certificates has new handshakes given them at once, behind the
/// front the listener already had.
#[tokio::test]
async fn new_certificates_are_served_from_the_reload_on() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let (upstream, _) = counting_upstream().await;
            let old = certificate(&["example.test"]);
            let (front, worker) = serving_secured_worker(upstream, vec![old.clone()]).await;
            let shown = || async {
                let stream = tls_client(front, "example.test", None, |_| {})
                    .await
                    .unwrap();
                stream.ssl().peer_certificate().unwrap().to_pem().unwrap()
            };
            assert_eq!(shown().await, old.chain.as_bytes());

            let new = certificate(&["example.test"]);
            worker
                .proxy()
                .reload(everything_secured_to(upstream, vec![new.clone()]))
                .unwrap();
            assert_eq!(shown().await, new.chain.as_bytes());
        })
        .await;
}

/// The certificate a client is given is the one whose names cover the name it asked
/// for, and the first when none does.
#[tokio::test]
async fn the_certificate_is_chosen_by_the_name_asked_for() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![
                crate::tls::testing::certificate(&["first.test"]),
                crate::tls::testing::certificate(&["b.test"]),
                crate::tls::testing::certificate(&["*.c.test"]),
            ];
            let leaves: Vec<Vec<u8>> = certificates
                .iter()
                .map(|certificate| {
                    boring::x509::X509::from_pem(certificate.chain.as_bytes())
                        .unwrap()
                        .to_der()
                        .unwrap()
                })
                .collect();
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;
            for (name, expected) in [
                ("b.test", 1),
                ("B.Test", 1),
                ("x.c.test", 2),
                ("first.test", 0),
                ("nobody.test", 0),
            ] {
                let client = tls_client(front, name, None, |_| {}).await.unwrap();
                let given = client.ssl().peer_certificate().unwrap().to_der().unwrap();
                assert_eq!(given, leaves[expected], "asked for {name}");
            }
        })
        .await;
}

/// TLS 1.2 is the oldest spoken, and a client that can exchange keys post-quantum
/// does.
#[tokio::test]
async fn tls_is_1_2_or_later_and_keys_are_exchanged_post_quantum_when_they_can_be() {
    use boring::ssl::SslVersion;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;

            // A client of BoringSSL's will not go below 1.2 unless told to.
            let old = tls_client(front, "example.test", None, |builder| {
                builder
                    .set_min_proto_version(Some(SslVersion::TLS1))
                    .unwrap();
                builder
                    .set_max_proto_version(Some(SslVersion::TLS1_1))
                    .unwrap();
            })
            .await;
            assert!(old.is_err(), "TLS 1.1 was accepted");

            let twelve = tls_client(front, "example.test", None, |builder| {
                builder
                    .set_max_proto_version(Some(SslVersion::TLS1_2))
                    .unwrap();
            })
            .await
            .unwrap();
            assert_eq!(twelve.ssl().version_str(), "TLSv1.2");

            let hybrid = tls_client(front, "example.test", None, |builder| {
                builder.set_curves_list("X25519MLKEM768:X25519").unwrap();
            })
            .await
            .unwrap();
            assert_eq!(hybrid.ssl().version_str(), "TLSv1.3");
            assert_eq!(hybrid.ssl().curve_name(), Some("X25519MLKEM768"));
        })
        .await;
}

/// A handshake is part of the first request's time: one that never finishes is cut
/// off at its deadline, and a client that speaks plain HTTP to a TLS listener is
/// answered with nothing a client could read as HTTP.
#[tokio::test]
async fn a_handshake_is_bounded_by_the_first_request_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;

            let mut silent = TcpStream::connect(front).await.unwrap();
            let took = closed_after(&mut silent).await;
            assert!(
                took + EARLY >= SHORT.first_request && took < SHORT.first_request + SLACK,
                "closed {took:?} after connecting"
            );

            let mut plain = TcpStream::connect(front).await.unwrap();
            plain.write_all(ASKED).await.unwrap();
            let mut answer = Vec::new();
            let _ended = within(plain.read_to_end(&mut answer)).await;
            assert!(!answer.starts_with(b"HTTP/"), "{answer:?}");
        })
        .await;
}

/// A config whose certificates are those of the config before keeps what it served
/// them with, and so the session tickets that were issued with it; other certificates
/// are served afresh; and certificates that cannot be used are refused, with the
/// listener named, and change nothing.
#[test]
fn a_reload_keeps_the_tls_of_certificates_that_have_not_changed() {
    let upstream: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let same = vec![crate::tls::testing::certificate(&["example.test"])];
    let config = everything_secured_to(upstream, same.clone());
    let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
    let tls = |proxy: &Proxy| Arc::clone(proxy.current.load().tls[0].as_ref().unwrap());
    let before = tls(&proxy);

    proxy.reload(everything_secured_to(upstream, same)).unwrap();
    assert!(Arc::ptr_eq(&before, &tls(&proxy)));

    // New certificates are served behind the same front, whose keys seal the tickets:
    // clients keep their resumption.
    let other = vec![crate::tls::testing::certificate(&["example.test"])];
    proxy
        .reload(everything_secured_to(upstream, other))
        .unwrap();
    let after = tls(&proxy);
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(after.same_front(&before));

    // Validating clients otherwise is a new front.
    let mut validating = everything_config(upstream);
    secured(
        &mut validating,
        vec![crate::tls::testing::certificate(&["example.test"])],
        Some(edgerush_config::ClientValidation {
            authorities: vec![crate::tls::testing::certificate(&["ca"]).chain],
        }),
    );
    proxy.reload(compile(&validating).unwrap()).unwrap();
    assert!(!tls(&proxy).same_front(&after));
    proxy
        .reload(everything_secured_to(
            upstream,
            vec![crate::tls::testing::certificate(&["example.test"])],
        ))
        .unwrap();
    let after = tls(&proxy);

    let unusable = vec![edgerush_config::Certificate {
        chain: "not a certificate".to_owned(),
        key: "not a key".to_owned(),
    }];
    let refused = proxy.reload(everything_secured_to(upstream, unusable));
    assert!(
        matches!(&refused, Err(ProxyError::Tls { listener, .. }) if listener == "web"),
        "{refused:?}"
    );
    assert!(Arc::ptr_eq(&after, &tls(&proxy)));
}

/// A TLS ClientHello sent to a plaintext listener is not the HTTP/2 preface, so it
/// goes to HTTP/1, which refuses it as a request line that is not one and closes:
/// nothing hangs waiting for a preface that will never come.
#[tokio::test]
async fn a_tls_hello_on_a_plaintext_listener_is_refused_as_http1() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let front = serving_worker(upstream).await;
            let mut stream = TcpStream::connect(front).await.unwrap();
            stream
                .write_all(b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03")
                .await
                .unwrap();
            let mut answer = Vec::new();
            let _ended =
                tokio::time::timeout(SHORT.first_request / 2, stream.read_to_end(&mut answer))
                    .await
                    .unwrap_or_else(|_| panic!("still open"));
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
        })
        .await;
}

/// Sends `request` to `front` over TLS, as HTTP/1.1, and reads to the end: what came, and
/// whether a closure alert came before the end. The client is BoringSSL's blocking one, on a
/// thread of its own: tokio-boring's reads an end with no alert as an end all the same, and
/// cannot say which it was.
async fn closed_over_tls(front: SocketAddr, request: &[u8]) -> (String, bool) {
    let request = request.to_vec();
    within(tokio::task::spawn_blocking(move || {
        use boring::ssl::{ShutdownState, SslConnector, SslMethod, SslVerifyMode};
        use std::io::{Read, Write};
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        builder.set_alpn_protos(b"\x08http/1.1").unwrap();
        let config = builder.build().configure().unwrap().verify_hostname(false);
        let tcp = std::net::TcpStream::connect(front).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = config.connect("example.test", tcp).unwrap();
        tls.write_all(&request).unwrap();
        let mut answer = Vec::new();
        // An end with no alert is read as an end too, which is why it is asked below.
        let _ended = tls.read_to_end(&mut answer);
        let alerted = tls.get_shutdown().contains(ShutdownState::RECEIVED);
        (String::from_utf8_lossy(&answer).into_owned(), alerted)
    }))
    .await
    .unwrap()
}

/// An HTTP/1 connection over TLS that the server closes with nothing left unfinished ends
/// with a closure alert (RFC 9112 §9.8: "Servers MUST attempt to initiate an exchange of
/// closure alerts with the client before closing the connection"; RFC 8446 §6.1): after an
/// answer the connection does not outlive, closed or ended by the close, after a refused
/// head, and when it has waited for a next request long enough. Without one a client can
/// tell no end of an answer from a cut, and RFC 9112 §9.8 has an answer ended by the close
/// complete only once the alert has come.
#[tokio::test]
async fn an_http1_connection_over_tls_the_server_closes_ends_with_a_closure_alert() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;
            for (request, said) in [
                (
                    &b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n"[..],
                    "HTTP/1.1 200 OK\r\n",
                ),
                (
                    b"GET / HTTP/1.0\r\nhost: example.test\r\n\r\n",
                    "HTTP/1.1 200 OK\r\n",
                ),
                (
                    b"GET / HTTP/1.1\r\nhost: example.test\r\nnot a field\r\n\r\n",
                    "HTTP/1.1 400 ",
                ),
                // Kept, and then closed for asking nothing more.
                (
                    b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n",
                    "HTTP/1.1 200 OK\r\n",
                ),
            ] {
                let (answer, alerted) = closed_over_tls(front, request).await;
                assert!(answer.starts_with(said), "{answer}");
                assert!(
                    alerted,
                    "no closure alert after {:?}: {answer:?}",
                    String::from_utf8_lossy(request)
                );
            }
        })
        .await;
}

/// An answer whose body fails after its head has gone ends with no closure alert: the
/// client is left with a message it can tell is unfinished, and over TLS a close without the
/// alert is how it tells (RFC 9112 §9.8). One ended by the close, to an HTTP/1.0 client,
/// would otherwise be taken as whole.
#[tokio::test]
async fn an_http1_answer_over_tls_cut_short_ends_without_a_closure_alert() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _held) = scripted_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;
            for request in [
                &b"GET /short HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n"[..],
                b"GET /short HTTP/1.0\r\nhost: example.test\r\n\r\n",
            ] {
                let (answer, alerted) = closed_over_tls(front, request).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert!(answer.ends_with("short"), "{answer}");
                assert!(!alerted, "a closure alert after a cut answer: {answer:?}");
            }
        })
        .await;
}

/// An HTTP/2 connection over TLS that the server closes for having been idle long enough
/// ends with a closure alert as well: h2 shuts its socket once it has said GOAWAY. Told from
/// the wire, the client held to TLS 1.2, whose records say their type in the clear: the last
/// the server sends is an alert, which on a connection closed in good order is the closure
/// alert.
#[tokio::test]
async fn an_http2_connection_over_tls_the_server_closes_ends_with_a_closure_alert() {
    use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
            let (front, _worker) = serving_secured_worker(upstream, certificates).await;
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::NONE);
            builder.set_alpn_protos(b"\x02h2").unwrap();
            builder
                .set_max_proto_version(Some(SslVersion::TLS1_2))
                .unwrap();
            let config = builder.build().configure().unwrap().verify_hostname(false);
            let read = Rc::new(RefCell::new(Vec::new()));
            let tapped = Tapped {
                stream: TcpStream::connect(front).await.unwrap(),
                read: Rc::clone(&read),
            };
            let mut stream = within(tokio_boring::connect(config, "example.test", tapped))
                .await
                .unwrap();
            {
                let (mut send, connection) =
                    within(::h2::client::handshake(&mut stream)).await.unwrap();
                let mut connection = std::pin::pin!(connection);
                let request = Request::get("https://example.test/").body(()).unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(async {
                    tokio::select! {
                        answer = answer => answer.unwrap(),
                        ended = &mut connection => panic!("ended before the answer: {ended:?}"),
                    }
                })
                .await;
                assert_eq!(answer.status(), StatusCode::OK);
                // Asked nothing more, the server closes it.
                let _ended = within(connection).await;
            }
            let mut rest = Vec::new();
            let _ended = within(stream.read_to_end(&mut rest)).await;
            // A type, a version and a length each, to the last.
            let wire = read.borrow().clone();
            let (mut at, mut last) = (0, None);
            while let Some(header) = wire.get(at..at + 5) {
                last = Some(header[0]);
                at += 5 + usize::from(u16::from_be_bytes([header[3], header[4]]));
            }
            assert_eq!(at, wire.len(), "records cut short");
            assert_eq!(last, Some(21), "the last record is no alert");
        })
        .await;
}
