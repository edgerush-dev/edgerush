//! Upstreams reached over TLS.

use super::*;

/// A worker whose one upstream `up`, at `upstream`, is reached over TLS as `tls` says
/// and spoken to in `protocol`.
async fn serving_worker_to_tls(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
) -> SocketAddr {
    serving_worker_to_tls_with(protocol, tls, everything_config(upstream)).await
}

/// The same, with `gateway` the certificate `tls` may name to show an endpoint that
/// asks who the data plane is.
async fn serving_worker_to_tls_showing(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
    gateway: &edgerush_config::Certificate,
) -> SocketAddr {
    let mut config = everything_config(upstream);
    config
        .certificates
        .insert("gateway".to_owned(), gateway.clone());
    serving_worker_to_tls_with(protocol, tls, config).await
}

/// A worker whose `config` has its upstream `up` reached over TLS as `tls` says and
/// spoken to in `protocol`.
async fn serving_worker_to_tls_with(
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
    mut config: Config,
) -> SocketAddr {
    let up = config.upstreams.get_mut("up").unwrap();
    up.protocol = protocol;
    up.tls = Some(tls);
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    front
}

/// An upstream that asks who the data plane is is shown the client certificate its
/// TLS names, in either protocol; without one, it will not speak.
#[tokio::test]
async fn an_upstream_that_asks_is_shown_the_client_certificate() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let server = certificate(&["backend.test"]);
            let ours = certificate(&["gateway"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream_asking(&server, Agrees::Either, Some(&ours)).await;
                let mut tls = trusting("backend.test", &server);
                tls.client_certificate = Some("gateway".to_owned());
                let front =
                    serving_worker_to_tls_showing(upstream, protocol, tls.clone(), &ours).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(
                    answer.starts_with("HTTP/1.1 200 OK\r\n"),
                    "{protocol:?}: {answer}"
                );

                tls.client_certificate = None;
                let front = serving_worker_to_tls(upstream, protocol, tls).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(
                    answer.starts_with("HTTP/1.1 502 "),
                    "{protocol:?}: {answer}"
                );
            }
        })
        .await;
}

/// An upstream reached over TLS is spoken to in HTTP/1.1 or in HTTP/2, as configured,
/// once its certificate is found to be the named server's and vouched for by a trusted
/// authority. In HTTP/2 its requests say `https`, which it answers only for.
#[tokio::test]
async fn an_upstream_is_reached_over_tls_in_either_protocol() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream(&certificate, Agrees::Either).await;
                let front = serving_worker_to_tls(
                    upstream,
                    protocol,
                    trusting("backend.test", &certificate),
                )
                .await;
                for _ in 0..2 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );
                    assert!(answer.contains("ok"), "{answer}");
                }
            }
        })
        .await;
}

/// An HTTP/2 upstream reached over TLS is probed as it is sent requests, asking for
/// `https`: its HTTP and gRPC health checks both pass one that answers only for that.
#[tokio::test]
async fn an_http2_upstream_over_tls_is_probed_for_https() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let upstream = tls_upstream(&certificate, Agrees::Either).await;
            let probes = [
                healthz(),
                edgerush_config::Probe::Grpc {
                    service: String::new(),
                },
            ];
            for probe in probes {
                let mut config = everything_config(upstream);
                let up = config.upstreams.get_mut("up").unwrap();
                up.protocol = UpstreamProtocol::Http2;
                up.tls = Some(trusting("backend.test", &certificate));
                up.health_check = Some(every_second_by(probe.clone()));
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let destination = proxy.checked().next().unwrap();
                let check = destination.health_check().unwrap();
                assert!(
                    crate::health::probe::passes(&destination, check).await,
                    "{probe:?}"
                );
            }
        })
        .await;
}

/// An endpoint whose certificate no trusted authority vouches for, or that is not the
/// named server's, is not spoken to: 502.
#[tokio::test]
async fn an_upstream_that_is_not_who_it_should_be_is_not_spoken_to() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let stranger = crate::tls::testing::certificate(&["backend.test"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream(&certificate, Agrees::Either).await;
                for tls in [
                    trusting("backend.test", &stranger),
                    trusting("other.test", &certificate),
                ] {
                    let front = serving_worker_to_tls(upstream, protocol, tls).await;
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 502 "),
                        "{protocol:?}: {answer}"
                    );
                }
            }
        })
        .await;
}

/// An HTTP/2 upstream that does not agree on `h2` in the handshake is a failed
/// connection, never one spoken to in HTTP/1.1 instead.
#[tokio::test]
async fn an_http2_upstream_that_will_not_agree_on_h2_is_not_spoken_to_in_http1() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let upstream = tls_upstream(&certificate, Agrees::NothingButSpeaksH2).await;
            let front = serving_worker_to_tls(
                upstream,
                UpstreamProtocol::Http2,
                trusting("backend.test", &certificate),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
        })
        .await;
}

/// A config whose upstream TLS is the config before's keeps what connections were
/// secured with, and its destinations with it; TLS that changed is a new destination,
/// whose connections are not the old one's; and an authority that is not a certificate
/// is refused, with the upstream named.
#[test]
fn a_reload_keeps_the_tls_to_an_upstream_that_has_not_changed() {
    let certificate = crate::tls::testing::certificate(&["backend.test"]);
    let secured = |tls: edgerush_config::UpstreamTls| {
        let mut config = everything_config("127.0.0.1:9".parse().unwrap());
        config.upstreams.get_mut("up").unwrap().tls = Some(tls);
        compile(&config).unwrap()
    };
    let proxy = Proxy::new(
        secured(trusting("backend.test", &certificate)),
        NonZeroUsize::MIN,
    )
    .unwrap();
    let identity = |proxy: &Proxy| Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
    let before = identity(&proxy);
    let connector = Arc::clone(before.secure().unwrap());

    proxy
        .reload(secured(trusting("backend.test", &certificate)))
        .unwrap();
    assert!(Arc::ptr_eq(&before, &identity(&proxy)));
    let kept = Arc::clone(proxy.current.load().secure[0].as_ref().unwrap());
    assert!(
        Arc::ptr_eq(&connector, &kept),
        "an unchanged TLS was built again"
    );

    proxy
        .reload(secured(trusting("other.test", &certificate)))
        .unwrap();
    let after = identity(&proxy);
    assert_ne!(after.key(), before.key());
    assert!(before.is_retired());

    let unusable = edgerush_config::UpstreamTls {
        server_name: "backend.test".to_owned(),
        authorities: vec![
            "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n".to_owned(),
        ],
        client_certificate: None,
    };
    let refused = proxy.reload(secured(unusable));
    assert!(
        matches!(&refused, Err(ProxyError::UpstreamTls { upstream, .. }) if upstream == "up"),
        "{refused:?}"
    );
}

/// The first byte whatever connects to `endpoint` within two seconds sends, if anything
/// connects and sends one.
async fn first_byte_at(endpoint: &TcpListener) -> Option<u8> {
    use tokio::io::AsyncReadExt;
    let wait = Duration::from_secs(2);
    let (mut socket, _) = tokio::time::timeout(wait, endpoint.accept())
        .await
        .ok()?
        .ok()?;
    let mut byte = [0_u8; 1];
    tokio::time::timeout(wait, socket.read_exact(&mut byte))
        .await
        .ok()?
        .ok()?;
    Some(byte[0])
}

/// The first byte of a TLS handshake record.
const HANDSHAKE: u8 = 0x16;

/// A connection the HTTP/2 client opens for a destination secured with TLS is secured,
/// even when a reload has retired the destination and the worker's sweep has run
/// between the request asking for the connection and the connection being opened —
/// whether the reload came before the request or after (review A06-01).
#[tokio::test]
async fn a_connection_dialled_for_a_retired_destination_is_still_secured() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            for reloaded_first in [false, true] {
                // The endpoint: whatever connects is read for its first byte.
                let endpoint = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = endpoint.local_addr().unwrap();
                let secured = |server_name: &str| {
                    let mut config = everything_config(address);
                    let up = config.upstreams.get_mut("up").unwrap();
                    up.protocol = UpstreamProtocol::Http2;
                    up.tls = Some(trusting(server_name, &certificate));
                    compile(&config).unwrap()
                };
                let proxy =
                    Arc::new(Proxy::new(secured("backend.test"), NonZeroUsize::MIN).unwrap());
                let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
                let destination = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
                if reloaded_first {
                    // A request directed by the config before the reload.
                    proxy.reload(secured("other.test")).unwrap();
                }

                // The request waits for a place, and the pool has a connection dialled
                // for it.
                let mut placing = Box::pin(worker.h2.place(&destination));
                let first = std::future::poll_fn(|cx| Poll::Ready(placing.as_mut().poll(cx))).await;
                assert!(first.is_pending());
                if !reloaded_first {
                    proxy.reload(secured("other.test")).unwrap();
                }
                assert!(destination.is_retired());
                // Before the dial runs.
                worker.h2.sweep();

                let first = first_byte_at(&endpoint).await;
                assert_eq!(
                    first,
                    Some(HANDSHAKE),
                    "reloaded first: {reloaded_first}; the endpoint's first byte {first:x?} is \
                     not a TLS handshake"
                );
                drop(placing);
            }
        })
        .await;
}

/// A retired destination still serves the requests already waiting for it (15 §4): one
/// held back by the worker's bound on connections has a connection opened for it when
/// one is freed, at a sweep after the one that retired the destination, and secured like
/// any other.
#[tokio::test]
async fn a_request_waiting_for_a_retired_destination_gets_a_secured_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let kept = tls_upstream(&certificate, Agrees::Either).await;
            let endpoint = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let dropped = endpoint.local_addr().unwrap();
            let secured = |endpoints: &[SocketAddr]| {
                let mut config = everything_config(kept);
                let up = config.upstreams.get_mut("up").unwrap();
                up.endpoints = endpoints.to_vec();
                up.protocol = UpstreamProtocol::Http2;
                up.tls = Some(trusting("backend.test", &certificate));
                compile(&config).unwrap()
            };
            let proxy = Arc::new(Proxy::new(secured(&[kept, dropped]), NonZeroUsize::MIN).unwrap());
            // One HTTP/2 connection a worker, let go of at any sweep that finds it idle.
            let limits = H1Limits {
                idle_total: 1,
                idle_timeout: Duration::ZERO,
                ..H1Limits::default()
            };
            let worker = Worker::with_deadlines(Arc::clone(&proxy), limits, SHORT);
            let (staying, going) = {
                let current = proxy.current.load();
                let at = |endpoint| Arc::clone(current.destinations.at(0, endpoint).unwrap());
                (at(0), at(1))
            };

            // A request to the endpoint that stays takes the worker's one connection.
            let busy = within(worker.h2.place(&staying)).await.unwrap();
            // A request to the other waits, with nothing opened for it.
            let mut placing = Box::pin(worker.h2.place(&going));
            let first = std::future::poll_fn(|cx| Poll::Ready(placing.as_mut().poll(cx))).await;
            assert!(first.is_pending());
            assert_eq!(worker.h2.connections(), 1);

            // A reload drops the other endpoint, and a sweep retires it.
            proxy.reload(secured(&[kept])).unwrap();
            assert!(going.is_retired() && !staying.is_retired());
            worker.h2.sweep();
            // The connection is done with, and the next sweep lets it go.
            drop(busy);
            worker.h2.sweep();

            let first = first_byte_at(&endpoint).await;
            assert_eq!(
                first,
                Some(HANDSHAKE),
                "the waiting request's endpoint got {first:x?}, not a TLS handshake"
            );
            drop(placing);
        })
        .await;
}

/// A TLS backend as `certificate` says, on a thread of its own, that answers the one
/// request of its one connection `ok` — saying it will carry no other if `closing` — then
/// reads until the gateway closes the connection, and says whether a closure alert came
/// before the close. BoringSSL's blocking stream is the one that can say so.
fn alert_heeding_backend(
    certificate: &edgerush_config::Certificate,
    closing: bool,
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
        let answer: &[u8] = if closing {
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok"
        } else {
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
        };
        secured.write_all(answer).unwrap();
        let mut rest = Vec::new();
        let _ended = secured.read_to_end(&mut rest);
        let _ = heard.send(secured.get_shutdown().contains(ShutdownState::RECEIVED));
    });
    (address, hearing)
}

/// Whether `backend_heard` says a closure alert came, waited for off the runtime's thread.
async fn alerted(backend_heard: std::sync::mpsc::Receiver<bool>) -> bool {
    within(tokio::task::spawn_blocking(move || {
        backend_heard.recv_timeout(Duration::from_secs(10))
    }))
    .await
    .unwrap()
        == Ok(true)
}

/// A connection to an upstream over TLS that the gateway closes in good order ends with a
/// closure alert (RFC 9112 §9.8: "Clients MUST send a closure alert before closing the
/// connection"): here after an exchange that finished, its answer saying the connection
/// would carry no other (13 §6).
#[tokio::test]
async fn an_upstream_connection_closed_after_its_exchange_ends_with_a_closure_alert() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = crate::tls::testing::certificate(&["backend.test"]);
            let (upstream, backend_heard) = alert_heeding_backend(&server, true);
            let front = serving_worker_to_tls(
                upstream,
                UpstreamProtocol::Http1,
                trusting("backend.test", &server),
            )
            .await;
            let answer = h1_answer(
                front,
                b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert!(
                alerted(backend_heard).await,
                "no closure alert to the upstream"
            );
        })
        .await;
}

/// And when the pool lets a kept connection go, here for having been idle too long.
#[tokio::test]
async fn an_upstream_connection_the_pool_lets_go_ends_with_a_closure_alert() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = crate::tls::testing::certificate(&["backend.test"]);
            let (upstream, backend_heard) = alert_heeding_backend(&server, false);
            let mut config = everything_config(upstream);
            let up = config.upstreams.get_mut("up").unwrap();
            up.protocol = UpstreamProtocol::Http1;
            up.tls = Some(trusting("backend.test", &server));
            let limits = H1Limits {
                idle_timeout: Duration::from_millis(200),
                sweep: Duration::from_millis(50),
                ..H1Limits::default()
            };
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let answer = h1_answer(
                front,
                b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert!(
                alerted(backend_heard).await,
                "no closure alert to the upstream"
            );
        })
        .await;
}
