//! Health checks and probes.

use super::*;

/// An HTTP/1 upstream that answers `/healthz` 200 while `serving` says so and 503
/// otherwise, and every other request 200 with its name in `x-upstream`.
async fn checked_upstream(name: &'static str, serving: Rc<Cell<bool>>) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _accepting = tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            let serving = Rc::clone(&serving);
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
                    let probe = seen.starts_with(b"GET /healthz ");
                    seen.clear();
                    let answer = if probe && !serving.get() {
                        "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n".to_owned()
                    } else {
                        format!(
                            "HTTP/1.1 200 OK\r\nx-upstream: {name}\r\ncontent-length: 0\r\n\r\n"
                        )
                    };
                    if stream.write_all(answer.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

/// A worker whose one upstream `up` has `endpoints`, checked as `check` says, with the
/// health checker running.
async fn serving_checked_worker(
    endpoints: Vec<SocketAddr>,
    protocol: UpstreamProtocol,
    check: edgerush_config::HealthCheck,
) -> (SocketAddr, Rc<Worker>) {
    let mut config = everything_config(endpoints[0]);
    let up = config.upstreams.get_mut("up").unwrap();
    up.endpoints = endpoints;
    up.protocol = protocol;
    up.health_check = Some(check);
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let _checking = tokio::task::spawn_local(Arc::clone(&proxy).check_health());
    let worker = Worker::with_deadlines(proxy, H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// How many endpoints of `up` the scrape says serve.
fn serving_now(worker: &Worker) -> String {
    let scrape = worker.proxy().metrics();
    scrape
        .lines()
        .find(|line| line.starts_with("edgerush_upstream_healthy_endpoints{upstream=\"up\"}"))
        .unwrap_or_default()
        .rsplit(' ')
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Until the scrape says `count` endpoints serve, for up to ten seconds.
async fn until_serving(worker: &Worker, count: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while serving_now(worker) != count {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never {count} serving; now {}", serving_now(worker)));
}

/// An endpoint that fails its checks gets no requests while it does, and gets them
/// again once it passes.
#[tokio::test]
async fn an_endpoint_failing_its_checks_is_kept_out_until_it_passes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let steady = checked_upstream("steady", Rc::new(Cell::new(true))).await;
            let shaky_serving = Rc::new(Cell::new(false));
            let shaky = checked_upstream("shaky", Rc::clone(&shaky_serving)).await;
            let (front, worker) = serving_checked_worker(
                vec![steady, shaky],
                UpstreamProtocol::Http1,
                every_second_by(healthz()),
            )
            .await;
            until_serving(&worker, "1").await;
            for _ in 0..20 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.contains("x-upstream: steady\r\n"), "{answer}");
            }
            shaky_serving.set(true);
            until_serving(&worker, "2").await;
            let mut answered_by_shaky = false;
            for _ in 0..40 {
                answered_by_shaky |= h1_answer(front, CLOSING_GET)
                    .await
                    .contains("x-upstream: shaky\r\n");
            }
            assert!(answered_by_shaky, "a healthy endpoint got nothing");
        })
        .await;
}

/// An endpoint a try could not connect to is set aside for every request after it, with
/// no check configured, and counted; once it takes connections again, a connect probe
/// after the data plane's `set_aside_ms` brings it back, ramping up where its upstream
/// has a slow start, as everything that joins the draw does (03 §6).
#[tokio::test]
async fn an_endpoint_that_cannot_be_connected_to_is_set_aside_until_a_probe_gets_through() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (held, nowhere) = refusing();
            let (answering, _) = statuses_upstream(vec![200]).await;
            let mut config = everything_config(nowhere);
            let up = config.upstreams.get_mut("up").unwrap();
            up.endpoints = vec![nowhere, answering];
            up.slow_start = Some(edgerush_config::SlowStart { window_ms: 60_000 });
            config.data_plane.set_aside_ms = Some(200);
            let proxy =
                Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let _checking = tokio::task::spawn_local(Arc::clone(&proxy).check_health());
            let worker = Worker::with_deadlines(proxy, H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            // Drawn at random, the endpoint that refuses is drawn before long, and its
            // request is answered 502: no retry is stated.
            let mut refused = false;
            for _ in 0..50 {
                let answer = h1_answer(front, CLOSING_GET).await;
                if answer.starts_with("HTTP/1.1 502 ") {
                    refused = true;
                    break;
                }
            }
            assert!(refused, "the endpoint that refuses was never drawn");
            for _ in 0..30 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            }
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 1\n";
            assert!(scrape.contains(line), "{scrape}");
            until(|| {
                worker
                    .proxy()
                    .metrics()
                    .contains(
                        "edgerush_upstream_set_asides_total{upstream=\"up\",reason=\"connect\"} 1\n",
                    )
            })
            .await;

            // It takes connections again, and says who it is. A connection that opens
            // and closes having said nothing is one the connect probe should have reset
            // (20 §5).
            let listening = held.listen(64).unwrap();
            let empty = Rc::new(Cell::new(0_usize));
            let counting = Rc::clone(&empty);
            let _answering = tokio::task::spawn_local(async move {
                while let Ok((mut stream, _)) = listening.accept().await {
                    let mut head = [0; 1024];
                    if matches!(stream.read(&mut head).await, Ok(0)) {
                        counting.set(counting.get() + 1);
                    }
                    let answer = b"HTTP/1.1 200 OK\r\nx-upstream: back\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
                    let _written = stream.write_all(answer).await;
                }
            });
            until(|| {
                worker
                    .proxy()
                    .metrics()
                    .contains("edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 0\n")
            })
            .await;
            let snapshot = worker.proxy().current.load();
            let returned = snapshot.destinations.at(0, 0).unwrap();
            assert_eq!(returned.address(), nowhere);
            assert!(returned.is_ramping(), "brought back without a ramp");
            // At a tenth of its share to begin with, but taking requests.
            let mut back = false;
            for _ in 0..400 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                if answer.contains("x-upstream: back\r\n") {
                    back = true;
                    break;
                }
            }
            assert!(back, "the endpoint brought back was never sent a request");
            assert_eq!(empty.get(), 0, "the connect probe closed without a reset");
        })
        .await;
}

/// When fewer than half the endpoints pass, their health is ignored: requests go on
/// to all of them rather than being answered 503.
#[tokio::test]
async fn with_most_endpoints_failing_their_health_is_ignored() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let one = checked_upstream("one", Rc::new(Cell::new(false))).await;
            let two = checked_upstream("two", Rc::new(Cell::new(false))).await;
            let three = checked_upstream("three", Rc::new(Cell::new(true))).await;
            let (front, worker) = serving_checked_worker(
                vec![one, two, three],
                UpstreamProtocol::Http1,
                every_second_by(healthz()),
            )
            .await;
            until_serving(&worker, "1").await;
            let mut unhealthy_answered = false;
            for _ in 0..40 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                unhealthy_answered |= !answer.contains("x-upstream: three\r\n");
            }
            assert!(unhealthy_answered, "health was not ignored below half");
        })
        .await;
}

/// A gRPC health check passes an endpoint whose health service says `SERVING`, and
/// fails one that says anything else.
#[tokio::test]
async fn a_grpc_health_check_passes_only_serving() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let state = Rc::new(Cell::new(1_u8));
            let saying = Rc::clone(&state);
            let script: Script = Rc::new(move |request, mut respond| {
                let saying = Rc::clone(&saying);
                Box::pin(async move {
                    assert_eq!(request.uri().path(), "/grpc.health.v1.Health/Check");
                    let mut body = request.into_body();
                    let _ = read_all(&mut body).await;
                    let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                        return;
                    };
                    let message = [0, 0, 0, 0, 2, 0x08, saying.get()];
                    let _ = sending.send_data(Bytes::copy_from_slice(&message), false);
                    let mut status = http::HeaderMap::new();
                    status.insert("grpc-status", "0".parse().unwrap());
                    let _ = sending.send_trailers(status);
                })
            });
            let upstream = scripted_h2_upstream(script).await;
            let probe = edgerush_config::Probe::Grpc {
                service: String::new(),
            };
            let (_front, worker) = serving_checked_worker(
                vec![upstream],
                UpstreamProtocol::Http2,
                every_second_by(probe),
            )
            .await;
            // NOT_SERVING takes it out, SERVING brings it back.
            state.set(2);
            until_serving(&worker, "0").await;
            state.set(1);
            until_serving(&worker, "1").await;
        })
        .await;
}

/// The one endpoint of `config`'s upstream `up`, checked by `probe`, ready to be probed:
/// its config compiled and its TLS made. With the proxy it is of, to be kept beside it.
fn checked_by(mut config: Config, probe: edgerush_config::Probe) -> (Proxy, Arc<ReuseIdentity>) {
    config.upstreams.get_mut("up").unwrap().health_check = Some(every_second_by(probe));
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let destination = proxy.checked().next().unwrap();
    (proxy, destination)
}

/// Whether the one endpoint of `config`'s upstream `up`, checked by `probe`, passes.
async fn probed(config: Config, probe: edgerush_config::Probe) -> bool {
    let (_proxy, destination) = checked_by(config, probe);
    let check = destination.health_check().unwrap();
    crate::health::probe::passes(&destination, check).await
}

/// An address that takes connections and never says a word on them.
async fn mute() -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = socket.accept().await {
            held.push(stream);
        }
    });
    address
}

/// An HTTP/1 backend that answers `said` to every request, whole, and closes.
async fn answering(said: &'static [u8]) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            tokio::task::spawn_local(async move {
                let mut head = [0; 1024];
                let _read = stream.read(&mut head).await;
                let _written = stream.write_all(said).await;
            });
        }
    });
    address
}

/// An HTTP/1 probe is judged by the final answer, past any informational ones before
/// it (RFC 9110 §15.2): Early Hints and then 200 passes, Early Hints and then 503 fails.
#[tokio::test]
async fn an_http1_probe_is_judged_by_the_final_answer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for (said, passes) in [
                (
                    &b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n"[..],
                    true,
                ),
                (
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 102 Processing\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n",
                    true,
                ),
                (
                    b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\nHTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n",
                    false,
                ),
                (b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n", true),
                // A switch is no answer to a probe that asked for none.
                (
                    b"HTTP/1.1 101 Switching Protocols\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
                    false,
                ),
                // Informational answers without end are not waited through for ever.
                (
                    b"HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
                    false,
                ),
            ] {
                let backend = answering(said).await;
                assert_eq!(
                    probed(everything_config(backend), healthz()).await,
                    passes,
                    "{}",
                    said.escape_ascii()
                );
            }
        })
        .await;
}

/// A TCP probe passes an endpoint it can connect to, and fails one it cannot (20 §5).
#[tokio::test]
async fn a_tcp_probe_passes_what_it_can_connect_to() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listening = mute().await;
            assert!(probed(everything_config(listening), edgerush_config::Probe::Tcp).await);
            let (_held, gone) = refusing();
            assert!(!probed(everything_config(gone), edgerush_config::Probe::Tcp).await);
        })
        .await;
}

/// On an upstream reached over TLS, a TCP probe passes once the handshake does, as
/// traffic's would: not against a certificate nobody trusted vouches for, nor an
/// endpoint that takes the connection and never answers the handshake.
#[tokio::test]
async fn a_tcp_probe_of_a_tls_upstream_needs_the_handshake() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let stranger = crate::tls::testing::certificate(&["backend.test"]);
            let upstream = tls_upstream(&certificate, Agrees::Either).await;
            let secured = |endpoint: SocketAddr, vouched: &edgerush_config::Certificate| {
                let mut config = everything_config(endpoint);
                config.upstreams.get_mut("up").unwrap().tls =
                    Some(trusting("backend.test", vouched));
                config
            };
            let tcp = edgerush_config::Probe::Tcp;
            assert!(probed(secured(upstream, &certificate), tcp.clone()).await);
            assert!(!probed(secured(upstream, &stranger), tcp.clone()).await);
            // Timed from the probe itself: compiling the config and making its TLS are
            // not the probe, and in a loaded run they have taken half a second.
            let (_proxy, destination) = checked_by(secured(mute().await, &certificate), tcp);
            let check = destination.health_check().unwrap();
            let started = tokio::time::Instant::now();
            assert!(!crate::health::probe::passes(&destination, check).await);
            // Given up on at the check's timeout, a second, and not before.
            let took = started.elapsed();
            assert!(took + EARLY >= Duration::from_secs(1), "{took:?}");
            assert!(took < Duration::from_secs(1) + SLACK, "{took:?}");
        })
        .await;
}

/// A plain TCP probe that a backend does see reaches it as a reset, never as a
/// connection that opens and closes having said nothing; on Linux, as a rule, the
/// backend never sees it at all (20 §5). The set-aside reconnect closes the same way.
#[tokio::test]
async fn a_plain_tcp_probe_is_reset_if_it_is_seen() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backend = socket.local_addr().unwrap();
            for probe in 0..2 {
                if probe == 0 {
                    assert!(probed(everything_config(backend), edgerush_config::Probe::Tcp).await);
                } else {
                    crate::health::probe::connect_unseen(backend).await.unwrap();
                }
                // Seen or not: a backend that accepts it finds it reset.
                if let Ok(accepted) =
                    tokio::time::timeout(Duration::from_millis(300), socket.accept()).await
                {
                    let (mut stream, _) = accepted.unwrap();
                    let mut byte = [0; 1];
                    let read = within(stream.read(&mut byte)).await;
                    assert!(
                        read.as_ref()
                            .is_err_and(|error| { error.kind() == io::ErrorKind::ConnectionReset }),
                        "probe {probe}: {read:?}"
                    );
                }
            }
        })
        .await;
}

/// How often, of many plain TCP probes, an accepting backend sees one: a measurement to
/// record (20 §5), not a gate. `cargo test -p edgerush-proxy --lib how_often -- --ignored
/// --nocapture`, on Linux.
#[tokio::test]
#[ignore = "a measurement, not a gate"]
async fn how_often_a_backend_sees_a_plain_tcp_probe() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backend = socket.local_addr().unwrap();
            let seen = Rc::new(Cell::new(0_usize));
            let counting = Rc::clone(&seen);
            tokio::task::spawn_local(async move {
                while socket.accept().await.is_ok() {
                    counting.set(counting.get() + 1);
                }
            });
            let probes = 2_000;
            for _ in 0..probes {
                crate::health::probe::connect_unseen(backend).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("an accepting backend saw {} of {probes} probes", seen.get());
        })
        .await;
}

/// An HTTP probe of an upstream sent a PROXY header sends one first that says the
/// connection is the gateway's own: v2's LOCAL, or a v1 line of the probe's own two
/// ends; the backend, a strict receiver, then answers it (20 §4).
#[tokio::test]
async fn an_http_probe_says_it_is_the_gateways_own() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for version in ["v1", "v2"] {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let backend = socket.local_addr().unwrap();
                let heard = Rc::new(RefCell::new(None));
                let hearing = Rc::clone(&heard);
                tokio::task::spawn_local(async move {
                    let (mut stream, peer) = socket.accept().await.unwrap();
                    let mut came = Vec::new();
                    let mut chunk = [0; 512];
                    let header = loop {
                        if let proxy_protocol::Read::Whole { header, length } =
                            proxy_protocol::read(&came)
                        {
                            break Some((header, came.split_off(length)));
                        }
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => break None,
                            Ok(read) => came.extend_from_slice(&chunk[..read]),
                        }
                    };
                    let Some((header, mut request)) = header else {
                        return;
                    };
                    // The header and the request may come in writes of their own.
                    while !request.windows(4).any(|four| four == b"\r\n\r\n") {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => request.extend_from_slice(&chunk[..read]),
                        }
                    }
                    // No header, or not a strict one: no answer, and the probe fails.
                    let expected = if version == "v1" {
                        proxy_protocol::Header::Proxied {
                            source: peer,
                            destination: stream.local_addr().unwrap(),
                        }
                    } else {
                        proxy_protocol::Header::Local
                    };
                    *hearing.borrow_mut() = Some(header);
                    if header == expected && request.starts_with(b"GET /healthz ") {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await;
                    }
                });
                let mut config: Config =
                    serde_saphyr::from_str(&sending(&tcp_to(backend, ""), version)).unwrap();
                config.upstreams.get_mut("up").unwrap().health_check =
                    Some(every_second_by(healthz()));
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let destination = proxy.checked().next().unwrap();
                let check = destination.health_check().unwrap();
                assert!(
                    crate::health::probe::passes(&destination, check).await,
                    "{version}: {:?}",
                    heard.borrow()
                );
            }
        })
        .await;
}

/// A probe's HTTP/2 client is held to the bounds the data plane's own client is (15 §3):
/// it refuses server push and streams of the peer's, and says how large a header list it
/// takes, so that a backend cannot make the health checker hold more than a request's
/// client would (review A04-02).
#[tokio::test]
async fn a_probes_http2_client_announces_the_request_clients_bounds() {
    use crate::h2_peer::{Peer, setting};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = socket.local_addr().unwrap();
            let asked = tokio::task::spawn_local(async move {
                let (stream, _) = socket.accept().await.unwrap();
                let (_peer, first) = Peer::accept_as_server(stream, &[]).await;
                first.settings()
            });
            let probe = edgerush_config::Probe::Grpc {
                service: String::new(),
            };
            let mut config = everything_config(address);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            // The peer goes once it has read the SETTINGS, and the probe fails with it:
            // only what its client announced is looked at.
            let _passed = probed(config, probe).await;
            let announced = within(asked).await.unwrap();
            assert!(
                announced.contains(&(setting::ENABLE_PUSH, 0)),
                "the probe's client leaves push enabled: it announced {announced:?}"
            );
            assert!(
                announced.contains(&(setting::MAX_CONCURRENT_STREAMS, 0)),
                "the probe's client lets the peer open streams: it announced {announced:?}"
            );
            assert!(
                announced
                    .iter()
                    .any(|&(id, value)| id == setting::MAX_HEADER_LIST_SIZE && value <= 64 * 1024),
                "the probe's client takes header lists past 64 KiB: it announced {announced:?}"
            );
        })
        .await;
}

/// A probe's HTTP/2 connection goes with the probe: a backend that keeps the probe
/// connection's writes blocked — PINGs it never reads the answers to — does not keep the
/// connection open once the probe's timeout has passed (03 §6 Health; review A08-03).
async fn a_probes_http2_connection_ends_with_it(probe: edgerush_config::Probe) {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Small buffers on the backend's side, so that little is needed to fill them.
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(4096).unwrap();
            socket.set_send_buffer_size(4096).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let address = socket.local_addr().unwrap();
            let listener = socket.listen(8).unwrap();
            let backend = tokio::task::spawn_local(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                // A server's SETTINGS, then PINGs, never reading what comes back.
                stream
                    .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
                let ping = [0_u8, 0, 8, 6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8];
                let batch = ping.repeat(1024);
                let mut sent = 0_usize;
                // Bounded: 64 MiB at most; blocked once a write has waited 500 ms. A write
                // that fails means the probe's side has let go of the connection already.
                while sent < 64 << 20 {
                    let wrote =
                        tokio::time::timeout(Duration::from_millis(500), stream.write_all(&batch))
                            .await;
                    match wrote {
                        Ok(Ok(())) => sent += batch.len(),
                        Ok(Err(_)) => return (None, sent),
                        Err(_) => return (Some(stream), sent),
                    }
                }
                panic!("the probe's side read {sent} bytes of PINGs and never stopped");
            });
            let mut config = everything_config(address);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            // The probe times out (1 s) with nothing answered.
            let passed = probed(config, probe).await;
            assert!(!passed);
            let (blocked, sent) = within(backend).await.unwrap();
            // Let go of already.
            let Some(stream) = blocked else { return };
            // Whatever the probe left behind has had time to go.
            tokio::time::sleep(Duration::from_secs(2)).await;
            match stream.try_write(&[0]) {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => panic!(
                    "the probe's connection is still open 2 s after the probe ended (its \
                     writes blocked after {sent} bytes of PINGs)"
                ),
                // Reset: the probe's side let go of the connection with PINGs unread.
                Err(_) => {}
                Ok(_) => panic!("the probe's side read again after the probe ended"),
            }
        })
        .await;
}

#[tokio::test]
async fn an_http_probes_http2_connection_ends_with_it() {
    a_probes_http2_connection_ends_with_it(healthz()).await;
}

#[tokio::test]
async fn a_grpc_probes_http2_connection_ends_with_it() {
    a_probes_http2_connection_ends_with_it(edgerush_config::Probe::Grpc {
        service: String::new(),
    })
    .await;
}

/// A probe that has its answer tells the backend it is going, GOAWAY with no error, and
/// closes the connection, as a request's client does when it lets a connection go.
#[tokio::test]
async fn a_passed_http2_probe_says_goaway_and_closes() {
    use crate::h2_peer::{Peer, code, headers, kind, response};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = socket.local_addr().unwrap();
            let backend = tokio::task::spawn_local(async move {
                let (stream, _) = socket.accept().await.unwrap();
                let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                let (asked, _) = peer.until(|frame| frame.kind == kind::HEADERS).await;
                peer.send(&headers(asked.stream, response(200), true)).await;
                let (away, _) = peer.until(|frame| frame.kind == kind::GOAWAY).await;
                let closed = peer.try_next().await.is_none();
                (away.goaway(), closed)
            });
            let mut config = everything_config(address);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            assert!(probed(config, healthz()).await);
            let ((last, error), closed) = within(backend).await.unwrap();
            assert_eq!((last, error), (0, code::NO_ERROR));
            assert!(closed, "a frame came after GOAWAY");
        })
        .await;
}

/// A worker with no socket to connect with is short of its own, as one with no storage is:
/// the request is answered 503, under the answer reason `exhausted`, and neither its endpoint
/// is set aside nor its upstream counted as failing, whichever protocol it speaks (03 §6,
/// C13). Once sockets can be had again, the same endpoint answers.
#[tokio::test]
async fn a_worker_with_no_socket_to_connect_with_blames_no_endpoint() {
    use crate::upstream::dial::short_of_sockets;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = match protocol {
                    UpstreamProtocol::Http2 => h2_upstream(UpstreamH2::default()).await.0,
                    _ => statuses_upstream(vec![200, 200]).await.0,
                };
                let mut config = everything_config(upstream);
                config.upstreams.get_mut("up").unwrap().protocol = protocol;
                let (front, worker) = serving_config(&config).await;
                short_of_sockets(true);
                let answer = h1_answer(front, CLOSING_GET).await;
                short_of_sockets(false);
                assert!(answer.starts_with("HTTP/1.1 503 "), "{protocol:?}: {answer}");
                let scrape = worker.proxy().metrics();
                for line in [
                    "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 0\n",
                    "edgerush_upstream_failures_total{upstream=\"up\"} 0\n",
                    "edgerush_listener_local_answers_total{listener=\"web\",reason=\"exhausted\"} 1\n",
                ] {
                    assert!(scrape.contains(line), "{protocol:?}: {line}{scrape}");
                }
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{protocol:?}: {answer}");
            }
        })
        .await;
}

/// The same for a tunnel: closed, counted `exhausted`, and its backend not set aside.
#[tokio::test]
async fn a_tunnel_with_no_socket_to_connect_with_blames_no_backend() {
    use crate::upstream::dial::short_of_sockets;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (front, worker) = passing(&tcp_to(backend.local_addr().unwrap(), "")).await;
            short_of_sockets(true);
            let mut client = TcpStream::connect(front).await.unwrap();
            let closed = closed_after(&mut client).await;
            short_of_sockets(false);
            assert!(closed < SLACK, "closed after {closed:?}");
            tunnel_ended(&worker, "db", "exhausted").await;
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 0\n";
            assert!(scrape.contains(line), "{scrape}");
        })
        .await;
}

/// The checker's connect probe of an endpoint set aside, made while the worker has no
/// socket, says nothing of the endpoint: it is neither brought back nor made to wait its
/// time again, and is brought back by the first probe once sockets can be had.
#[tokio::test]
async fn a_probe_with_no_socket_neither_brings_back_nor_waits_again() {
    use crate::upstream::dial::short_of_sockets;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (held, nowhere) = refusing();
            let mut config = everything_config(nowhere);
            config.data_plane.set_aside_ms = Some(200);
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            let aside = || {
                proxy
                    .current
                    .load()
                    .destinations
                    .at(0, 0)
                    .unwrap()
                    .set_aside_for()
            };
            assert!(aside().is_some(), "not set aside");

            // It takes connections again, but the worker has no socket to see so with.
            let _listening = held.listen(64).unwrap();
            short_of_sockets(true);
            let _checking = tokio::task::spawn_local(Arc::clone(&proxy).check_health());
            tokio::time::sleep(Duration::from_millis(800)).await;
            let waited = aside();
            short_of_sockets(false);
            let waited = waited.expect("brought back without a probe that got through");
            assert!(
                waited >= Duration::from_millis(700),
                "its wait was started again: aside for {waited:?}"
            );
            until(|| aside().is_none()).await;
        })
        .await;
}

/// An endpoint set aside for want of a local port to it is counted under a reason of its
/// own, apart from one whose connects failed: port exhaustion is not a dead pod (03 §6).
#[test]
fn an_endpoint_set_aside_for_want_of_a_port_is_counted_as_that() {
    use crate::upstream::destination::Aside;
    let config = everything_config("127.0.0.1:1".parse().unwrap());
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let destination = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
    assert!(destination.set_aside(Aside::NoPort));
    proxy.count_set_aside(&destination);
    let scrape = proxy.metrics();
    for line in [
        "edgerush_upstream_set_asides_total{upstream=\"up\",reason=\"no_port\"} 1\n",
        "edgerush_upstream_set_asides_total{upstream=\"up\",reason=\"connect\"} 0\n",
    ] {
        assert!(scrape.contains(line), "{line}{scrape}");
    }
}

/// A configured probe made while the worker has no socket says nothing of the endpoint: it
/// is neither a pass nor a fail, and an endpoint that one failure would mark down stays in
/// (C25). Probed every second, it is watched for longer than two intervals.
#[tokio::test]
async fn a_probe_with_no_socket_marks_nothing_down() {
    use crate::upstream::dial::short_of_sockets;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let steady = checked_upstream("steady", Rc::new(Cell::new(true))).await;
            let (_front, worker) = serving_checked_worker(
                vec![steady],
                UpstreamProtocol::Http1,
                every_second_by(healthz()),
            )
            .await;
            until_serving(&worker, "1").await;
            short_of_sockets(true);
            let mut seen = Vec::new();
            for _ in 0..25 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                seen.push(serving_now(&worker));
            }
            short_of_sockets(false);
            assert!(
                seen.iter().all(|serving| serving == "1"),
                "marked down for want of a socket: {seen:?}"
            );
        })
        .await;
}

/// A config for `up` at `upstream`, spoken to in HTTP/1, whose rule gives a try `try_ms`.
fn trying(upstream: SocketAddr, try_ms: u64) -> Config {
    let mut config = everything_config(upstream);
    config.routes[0].rules[0]
        .forward
        .as_mut()
        .expect("the rule forwards")
        .timeouts = Some(edgerush_config::Timeouts {
        request_ms: None,
        backend_request_ms: Some(try_ms),
        tunnel_idle_ms: None,
    });
    config
}

/// A worker serving [`trying`], whose connects are bounded by `connect`.
async fn trying_within(
    upstream: SocketAddr,
    try_ms: u64,
    connect: Duration,
) -> (SocketAddr, Rc<Worker>) {
    let limits = H1Limits {
        connect,
        ..H1Limits::default()
    };
    let proxy = Proxy::new(
        compile(&trying(upstream, try_ms)).unwrap(),
        NonZeroUsize::MIN,
    )
    .unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// How many of `up`'s endpoints are set aside, by the worker's scrape.
fn set_aside(worker: &Worker) -> u64 {
    let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} ";
    worker
        .proxy()
        .metrics()
        .lines()
        .find_map(|found| found.strip_prefix(line))
        .and_then(|count| count.parse().ok())
        .unwrap_or(u64::MAX)
}

/// Waits, a few seconds at most, for `done`.
async fn soon(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "waited for something that never happened"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A try whose clock runs out while its connect hangs is answered at its clock, and the
/// connect is carried on until its own bound, which sets the endpoint aside, as a try that
/// waited for it would have (03 §6, C40): over HTTP/1 the connect is no longer the try's to
/// drop.
#[tokio::test]
async fn a_connect_its_try_let_go_of_sets_its_endpoint_aside_at_its_bound() {
    use crate::upstream::dial::hold_connects;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            hold_connects(upstream, Some(Duration::from_secs(60)));
            let (front, worker) = trying_within(upstream, 200, Duration::from_millis(800)).await;
            let began = Instant::now();
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert!(
                began.elapsed() < Duration::from_millis(600),
                "{:?}",
                began.elapsed()
            );
            assert_eq!(set_aside(&worker), 0, "set aside before its bound");
            soon(|| set_aside(&worker) == 1).await;
            assert!(began.elapsed() >= Duration::from_millis(800) - EARLY);
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            hold_connects(upstream, None);
        })
        .await;
}

/// Tries let go of one after another, while the first one's connect is still carried on,
/// leave no more than it: one connect watched for the destination, holding one place, the
/// others ended with theirs. A reload meanwhile changes nothing, and once the bound has
/// passed nothing is held.
#[tokio::test]
async fn connects_let_go_of_to_one_destination_are_watched_once() {
    use crate::upstream::dial::hold_connects;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            hold_connects(upstream, Some(Duration::from_secs(60)));
            let (front, worker) = trying_within(upstream, 100, Duration::from_millis(1_500)).await;
            for _ in 0..3 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(worker.watcher.watched(), 1);
            assert_eq!(worker.places.held(), 1);
            worker
                .proxy()
                .reload(compile(&trying(upstream, 100)).unwrap())
                .unwrap();
            soon(|| set_aside(&worker) == 1).await;
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            hold_connects(upstream, None);
        })
        .await;
}

/// A connect carried on that gets through after all is closed, and sets nothing aside;
/// what it held goes with it.
#[tokio::test]
async fn a_connect_carried_on_that_gets_through_is_closed() {
    use crate::upstream::dial::hold_connects;
    use std::sync::atomic::Ordering;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, opened) = counting_upstream().await;
            hold_connects(upstream, Some(Duration::from_millis(400)));
            let (front, worker) = trying_within(upstream, 100, Duration::from_secs(2)).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            soon(|| opened.load(Ordering::SeqCst) == 1).await;
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            assert_eq!(set_aside(&worker), 0);
            assert_eq!(
                worker.idle_connections(),
                0,
                "kept a connection nobody asked for"
            );
            hold_connects(upstream, None);
        })
        .await;
}

/// What sets nothing aside for a try sets nothing aside carried on: the worker's own
/// shortage of sockets, met by a connect its try let go of, blames no endpoint.
#[tokio::test]
async fn a_connect_carried_on_into_the_workers_own_shortage_blames_no_endpoint() {
    use crate::upstream::dial::{hold_connects, short_of_sockets};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            hold_connects(upstream, Some(Duration::from_millis(300)));
            let (front, worker) = trying_within(upstream, 100, Duration::from_secs(2)).await;
            short_of_sockets(true);
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            short_of_sockets(false);
            assert_eq!(set_aside(&worker), 0);
            hold_connects(upstream, None);
        })
        .await;
}

/// Nor is a try let go of in its TLS handshake, its connect through: nothing is carried
/// on, and an endpoint that took the connection is not set aside.
#[tokio::test]
async fn a_try_let_go_of_in_its_handshake_carries_nothing_on() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = mute().await;
            let mut config = trying(upstream, 200);
            let authority = crate::tls::testing::certificate(&["backend.test"]);
            config.upstreams.get_mut("up").unwrap().tls =
                Some(trusting("backend.test", &authority));
            let (front, worker) = serving_config(&config).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            assert!(worker.watcher.watched() == 0);
            assert_eq!(worker.places.held(), 0);
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(set_aside(&worker), 0);
        })
        .await;
}

/// A worker that ends while a connect is carried on lets go of what it held: the place,
/// and the destination watched.
#[test]
fn a_worker_ending_mid_watch_holds_nothing() {
    use crate::upstream::dial::hold_connects;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let (worker, upstream) = local.block_on(&runtime, async {
        let (upstream, _) = counting_upstream().await;
        hold_connects(upstream, Some(Duration::from_secs(60)));
        let (front, worker) = trying_within(upstream, 100, Duration::from_secs(30)).await;
        let answer = h1_answer(front, CLOSING_GET).await;
        assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(worker.watcher.watched(), 1);
        (worker, upstream)
    });
    drop(local);
    assert!(worker.watcher.watched() == 0);
    assert_eq!(worker.places.held(), 0);
    hold_connects(upstream, None);
}

/// A worker serving `config`, whose connects are bounded by `connect`.
async fn serving_connecting_within(config: &Config, connect: Duration) -> (SocketAddr, Rc<Worker>) {
    let limits = H1Limits {
        connect,
        ..H1Limits::default()
    };
    let proxy = Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// How many of `upstream`'s endpoints are set aside, by the worker's scrape.
fn set_aside_of(worker: &Worker, upstream: &str) -> u64 {
    let line = format!("edgerush_upstream_set_aside_endpoints{{upstream=\"{upstream}\"}} ");
    worker
        .proxy()
        .metrics()
        .lines()
        .find_map(|found| found.strip_prefix(line.as_str()))
        .and_then(|count| count.parse().ok())
        .unwrap_or(u64::MAX)
}

/// A try let go of while its connect hangs, and retried: the connect carried on keeps the
/// first try's place, and the retry, sent to the other endpoint, takes one of its own and
/// is answered. Once the bound has passed only the endpoint set aside is left of it.
#[tokio::test]
async fn a_retry_takes_a_place_of_its_own_beside_a_connect_carried_on() {
    use crate::upstream::dial::hold_connects;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (hanging, _) = counting_upstream().await;
            let (answering, _) = counting_upstream().await;
            hold_connects(hanging, Some(Duration::from_secs(60)));
            let mut config = trying(hanging, 100);
            config.upstreams.get_mut("up").unwrap().endpoints = vec![hanging, answering];
            let mut retry = retrying(1, &[], &[], 10);
            retry.on_timeout = true;
            config.routes[0].rules[0]
                .forward
                .as_mut()
                .expect("the rule forwards")
                .retry = Some(retry);
            let (front, worker) = serving_connecting_within(&config, Duration::from_secs(2)).await;
            let retried = "edgerush_upstream_retries_total{upstream=\"up\"} 1\n";
            // Until a request's first try draws the endpoint that hangs: the other draws
            // it half the time.
            for _ in 0..30 {
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                if worker.proxy().metrics().contains(retried) {
                    break;
                }
            }
            assert!(worker.proxy().metrics().contains(retried), "no try drew it");
            assert_eq!(worker.watcher.watched(), 1);
            soon(|| worker.places.held() == 1).await;
            soon(|| set_aside(&worker) == 1).await;
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            hold_connects(hanging, None);
        })
        .await;
}

/// A mirror's copy given up on while its connect hangs — its request went before its body
/// ended — is let go of, body and all, and its connect carried on alone, with the copy's
/// place, until its bound, which sets the mirror's endpoint aside.
#[tokio::test]
async fn a_mirror_given_up_on_mid_connect_leaves_only_the_connect() {
    use crate::upstream::dial::hold_connects;
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, _) = counting_upstream().await;
            let (shadow, _) = counting_upstream().await;
            hold_connects(shadow, Some(Duration::from_secs(60)));
            let mut config = everything_config(primary);
            let mut copy = config.upstreams["up"].clone();
            copy.endpoints = vec![shadow];
            config.upstreams.insert("shadow".to_owned(), copy);
            config.routes[0].rules[0].filters.push(
                serde_saphyr::from_str(
                    "{ type: request_mirror, upstream: shadow, fraction: { numerator: 1, denominator: 1 } }",
                )
                .unwrap(),
            );
            let (front, worker) =
                serving_connecting_within(&config, Duration::from_millis(1_500)).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(b"POST /a HTTP/1.1\r\nhost: shop.example.com\r\ncontent-length: 100\r\n\r\n0123456789")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(client);
            soon(|| worker.watcher.watched() == 1 && worker.places.held() == 1).await;
            // What the worker stores while the connect is carried on is what it stores
            // after: none of it is the copy's, whose body went with it.
            let stored = worker.blocks.borrow().storage().used();
            assert_eq!(set_aside_of(&worker, "shadow"), 0);
            soon(|| set_aside_of(&worker, "shadow") == 1).await;
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            assert_eq!(worker.blocks.borrow().storage().used(), stored);
            assert_eq!(set_aside_of(&worker, "up"), 0);
            hold_connects(shadow, None);
        })
        .await;
}

/// The destination at `address`, by the identity a compiled config gives it, with a place
/// of `places` held for a try.
fn try_at(
    address: SocketAddr,
    places: &Rc<crate::places::Places>,
) -> (Proxy, Arc<ReuseIdentity>, Option<Admitted>) {
    let proxy = Proxy::new(everything_to(address), NonZeroUsize::MIN).unwrap();
    let identity = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
    let holds = Rc::new(Cell::new(0));
    let admitted = Admitted {
        _place: places.take(&holds, false).unwrap(),
        counted: None,
    };
    (proxy, identity, Some(admitted))
}

/// A connect that ends as its try goes is the try's: nothing is handed on, the place stays
/// with the try, and the connection with whoever has it.
#[tokio::test]
async fn a_connect_that_ended_as_its_try_goes_hands_nothing_on() {
    use crate::serve::watched::{Connecting, Watcher};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let places = crate::places::Places::new(2);
            let (_proxy, identity, mut admitted) = try_at(listener.local_addr().unwrap(), &places);
            let watcher = Rc::new(Watcher::default());
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let connecting = Connecting::new(&identity, deadline, &mut admitted, &watcher);
            let connected = within(connecting).await.unwrap();
            assert!(admitted.is_some(), "the place left the try");
            assert_eq!((watcher.watched(), watcher.queued()), (0, 0));
            drop(connected);
            drop(admitted);
            assert_eq!(places.held(), 0);
        })
        .await;
}

/// Connects let go of before the watcher has taken any up are coalesced as they are
/// handed over, not once watched: the first queued with its place, the second ended at once
/// with its own, so that tries let go of one after another queue no more than one.
#[tokio::test]
async fn connects_let_go_of_while_one_is_queued_are_not_queued_beside_it() {
    use crate::serve::watched::{Connecting, Watcher};
    use crate::upstream::dial::hold_connects;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            hold_connects(upstream, Some(Duration::from_secs(60)));
            let places = crate::places::Places::new(4);
            let (_proxy, identity, mut first) = try_at(upstream, &places);
            let holds = Rc::new(Cell::new(0));
            let mut second = Some(Admitted {
                _place: places.take(&holds, false).unwrap(),
                counted: None,
            });
            let watcher = Rc::new(Watcher::default());
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            {
                let mut one = Box::pin(Connecting::new(&identity, deadline, &mut first, &watcher));
                let mut two = Box::pin(Connecting::new(&identity, deadline, &mut second, &watcher));
                std::future::poll_fn(|cx| {
                    assert!(one.as_mut().poll(cx).is_pending());
                    assert!(two.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                // Both let go of in one go, before the watcher's task has had a turn.
            }
            assert_eq!((watcher.watched(), watcher.queued()), (1, 1));
            assert_eq!(places.held(), 1, "the second's place is held still");
            assert!(first.is_none() && second.is_none());
            hold_connects(upstream, None);
        })
        .await;
}

/// A connect let go of once its worker's watcher has ended is ended at once, all it holds
/// with it: the socket, the place and the destination's registration.
#[test]
fn a_connect_let_go_of_after_its_watcher_ended_holds_nothing() {
    use crate::serve::watched::{Connecting, Watcher};
    use crate::upstream::dial::hold_connects;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let places = crate::places::Places::new(2);
    let watcher = Rc::new(Watcher::default());
    let local = tokio::task::LocalSet::new();
    let (upstream, proxy, identity, mut admitted) = local.block_on(&runtime, async {
        let (upstream, _) = counting_upstream().await;
        hold_connects(upstream, Some(Duration::from_secs(60)));
        let (proxy, identity, admitted) = try_at(upstream, &places);
        (upstream, proxy, identity, admitted)
    });
    let deadline = local.block_on(&runtime, async {
        tokio::time::Instant::now() + Duration::from_secs(30)
    });
    // Begun on the worker, which starts its watcher's task.
    let mut connecting = {
        let _runtime = runtime.enter();
        let _local = local.enter();
        Box::pin(Connecting::new(
            &identity,
            deadline,
            &mut admitted,
            &watcher,
        ))
    };
    local.block_on(&runtime, async {
        std::future::poll_fn(|cx| {
            assert!(connecting.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // The watcher's task has its first turn.
        tokio::task::yield_now().await;
    });
    // The worker ends, and its watcher's task with it.
    drop(local);
    drop(connecting);
    assert_eq!((watcher.watched(), watcher.queued()), (0, 0));
    assert!(admitted.is_none());
    assert_eq!(places.held(), 0);
    drop(proxy);
    hold_connects(upstream, None);
}

/// Connects carried on are seen through side by side: one that hangs to its bound does not
/// keep another, refused meanwhile, from setting its endpoint aside at once.
#[tokio::test]
async fn a_connect_carried_on_does_not_wait_for_another() {
    use crate::upstream::dial::hold_connects;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (hanging, _) = counting_upstream().await;
            let (_held, refusing) = refusing();
            hold_connects(hanging, Some(Duration::from_secs(60)));
            hold_connects(refusing, Some(Duration::from_millis(300)));
            let mut config = trying(hanging, 100);
            config.upstreams.get_mut("up").unwrap().endpoints = vec![hanging, refusing];
            let (front, worker) = serving_connecting_within(&config, Duration::from_secs(3)).await;
            let began = Instant::now();
            // Until a try to each has been let go of: each draws either.
            for _ in 0..40 {
                if worker.watcher.watched() == 2 {
                    break;
                }
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            }
            assert_eq!(worker.watcher.watched(), 2, "no try drew one of them");
            let aside = |address: SocketAddr| {
                let snapshot = worker.proxy().current.load();
                (0..2)
                    .filter_map(|at| snapshot.destinations.at(0, at))
                    .find(|destination| destination.address() == address)
                    .is_some_and(|destination| destination.aside().is_some())
            };
            soon(|| aside(refusing)).await;
            assert!(!aside(hanging), "the hanging one already out of time");
            assert!(
                began.elapsed() < Duration::from_secs(3),
                "{:?}",
                began.elapsed()
            );
            soon(|| aside(hanging)).await;
            soon(|| worker.watcher.watched() == 0 && worker.places.held() == 0).await;
            hold_connects(hanging, None);
            hold_connects(refusing, None);
        })
        .await;
}
