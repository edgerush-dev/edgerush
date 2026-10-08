//! Clients that speak HTTP/3.

use super::*;

/// A worker serving HTTP/3 for `upstream` on a UDP socket of its own, beside its TCP
/// listener's configuration: an `https` listener with HTTP/3, for `a.test`.
async fn serving_h3_worker(upstream: SocketAddr) -> SocketAddr {
    let http3 = edgerush_config::Http3 {
        alt_svc_max_age: 60,
        force_retry: false,
    };
    serving_h3(&h3_config(upstream, http3)).await.0
}

/// A redirect is answered over HTTP/3 as over TCP, and a scheme it does not state is the
/// listener's, `https` (18 §3).
#[tokio::test]
async fn a_redirect_is_answered_over_http3_with_the_listeners_scheme() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Answer, Client};
            let field = |answer: &Answer, name: &str| {
                answer.heads.last().and_then(|head| {
                    head.iter()
                        .find(|(field, _)| field == name)
                        .map(|(_, value)| value.clone())
                })
            };
            let (upstream, opened) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let mut config = h3_config(upstream, http3);
            let redirects = [
                "matches: [{ path: { prefix: /old } }]\n\
                     redirect: { status: 301, path: { replace_prefix: /new }, query: keep }",
                "matches: [{ path: { prefix: /away } }]\n\
                     redirect: { status: 302, host: www.example.org, query: drop }",
            ];
            for (at, rule) in redirects.into_iter().enumerate() {
                config.routes[0]
                    .rules
                    .insert(at, serde_saphyr::from_str(rule).unwrap());
            }
            let (front, _) = serving_h3(&config).await;
            let mut client = Client::connect(front, "a.test").await;

            let moved = client.get("a.test", "/old/a?x=1").await;
            assert_eq!(moved.final_status(), Some("301"));
            assert_eq!(field(&moved, "location").as_deref(), Some("/new/a?x=1"));
            assert!(moved.body.is_empty());
            let away = client.get("a.test", "/away/b?y").await;
            assert_eq!(away.final_status(), Some("302"));
            assert_eq!(
                field(&away, "location").as_deref(),
                Some("https://www.example.org/away/b")
            );
            assert_eq!(opened.load(Ordering::SeqCst), 0);
        })
        .await;
}

/// A listener whose config forces Retry has every client prove its address first,
/// however few handshakes are under way, and is served after; a config that stops
/// forcing it applies to the next client, with no new socket (16 §3).
#[tokio::test]
async fn an_http3_listener_forces_retry_as_the_config_in_force_says() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            // A long header whose type is Retry (RFC 9000 §17.2.5).
            let retry = |datagram: &[u8]| datagram[0] & 0xf0 == 0xf0;
            let (upstream, _) = counting_upstream().await;
            let mut http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: true,
            };
            let (front, proxy) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            assert!(retry(&client.received[0]), "no Retry first");
            assert_eq!(client.get("a.test", "/").await.final_status(), Some("200"));

            http3.force_retry = false;
            proxy
                .reload(compile(&h3_config(upstream, http3)).unwrap())
                .unwrap();
            let mut client = Client::connect(front, "a.test").await;
            assert!(!client.received.iter().any(|datagram| retry(datagram)));
            assert_eq!(client.get("a.test", "/").await.final_status(), Some("200"));
        })
        .await;
}

/// Live HTTP/3 connections carry on across reloads, as TCP ones do (a reload under load
/// drops nothing): a request held open through 51 of them is answered, and a client asking
/// once after each is served every time by the config that reload set, the route moving
/// between two upstreams.
#[tokio::test]
async fn http3_connections_carry_on_across_reloads() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (open, gate) = tokio::sync::oneshot::channel();
            let holding =
                gated_upstream(b"", gate, b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok").await;
            let (old, to_old) = recording_upstream("200 OK");
            let (new, to_new) = recording_upstream("200 OK");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, proxy) = serving_h3(&h3_config(holding, http3)).await;
            let mut held = Client::connect(front, "a.test").await;
            let id = held.request(&get("a.test", "/held"), true);
            held.for_a_while(Duration::from_millis(100)).await;

            let mut asking = Client::connect(front, "a.test").await;
            for round in 1..=51 {
                let upstream = if round % 2 == 1 { new } else { old };
                proxy
                    .reload(compile(&h3_config(upstream, http3)).unwrap())
                    .unwrap();
                let answer = asking.get("a.test", "/").await;
                assert_eq!(answer.final_status(), Some("200"), "round {round}");
                held.for_a_while(Duration::from_millis(5)).await;
            }
            assert_eq!((to_new.borrow().len(), to_old.borrow().len()), (26, 25));

            open.send(()).unwrap();
            let answer = held.answer(id).await;
            assert_eq!(answer.final_status(), Some("200"));
            assert_eq!(answer.body, b"ok");
        })
        .await;
}

/// An HTTP/3 request goes through the request core to an HTTP/1 upstream, and its
/// answer comes back, as any request's does.
#[tokio::test]
async fn an_http3_request_is_answered_by_the_upstream() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, _) = counting_upstream().await;
            let front = serving_h3_worker(upstream).await;
            let mut client = Client::connect(front, "a.test").await;
            for path in ["/", "/again"] {
                let answer = client.get("a.test", path).await;
                assert_eq!(answer.final_status(), Some("200"), "{path}");
                assert_eq!(answer.body, b"ok");
            }
        })
        .await;
}

/// An HTTP/3 connection is open, as a TCP one is, until it has gone: a request held
/// through the drain keeps it so, and a draining process waits on it (03 §10).
#[tokio::test]
async fn an_http3_connection_is_open_until_it_has_gone() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (open, gate) = tokio::sync::oneshot::channel();
            let upstream =
                gated_upstream(b"", gate, b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok").await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let config = h3_config(upstream, http3);
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
            let alone = Forwarding::group(1).remove(0);
            let _serving =
                tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone).unwrap());
            assert_eq!(proxy.open_connections(), 0);

            let mut client = Client::connect(front, "a.test").await;
            let id = client.request(&get("a.test", "/"), true);
            // Sent, and in the upstream's hands, before the drain begins.
            client.for_a_while(Duration::from_millis(100)).await;
            assert_eq!(proxy.open_connections(), 1);
            worker.drain();
            // Draining, with the request still held by the upstream: still open.
            client.for_a_while(Duration::from_millis(200)).await;
            assert_eq!(proxy.open_connections(), 1);
            open.send(()).unwrap();
            let answer = client.answer(id).await;
            assert_eq!(answer.final_status(), Some("200"));
            assert_eq!(answer.body, b"ok");
            // Answered, and drained: the connection goes, and with it the count.
            client.until(|_| proxy.open_connections() == 0).await;
        })
        .await;
}

/// An upstream's 103 reaches an HTTP/3 client before its final answer, as over HTTP/2.
#[tokio::test]
async fn an_upstream_103_reaches_an_http3_client_before_its_answer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (open, gate) = tokio::sync::oneshot::channel();
            let upstream = gated_upstream(
                b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\n",
                gate,
                b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
            )
            .await;
            let front = serving_h3_worker(upstream).await;
            let mut client = Client::connect(front, "a.test").await;
            let id = client.request(&crate::downstream::h3::testing::get("a.test", "/"), true);
            // Heard while the upstream is still working on its answer.
            client
                .until(|client| client.answers.get(&id).is_some_and(|a| !a.heads.is_empty()))
                .await;
            let hint = client.answers[&id].clone();
            assert_eq!(hint.status(0), Some("103"));
            assert!(
                hint.heads[0]
                    .iter()
                    .any(|(name, value)| name == "link" && value == "</a.css>; rel=preload")
            );
            open.send(()).unwrap();
            let answer = client.answer(id).await;
            assert_eq!(answer.final_status(), Some("200"));
            assert_eq!(answer.body, b"ok");
        })
        .await;
}

/// An HTTPS listener that serves HTTP/3 says so on its TCP answers, with the port its
/// config gives and for as long as it says (RFC 7838); one that does not, says nothing.
/// The gateway's own answers say so too: here a 502 for an upstream that refuses.
#[tokio::test]
async fn an_http3_listener_says_so_on_its_tcp_answers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (answering, _) = counting_upstream().await;
            let (_held, refusing) = super::upstreams::refusing();
            for (upstream, status, http3) in [
                (answering, "200 ok", true),
                (answering, "200 ok", false),
                (refusing, "502 bad gateway", true),
            ] {
                let mut config = everything_config(upstream);
                let web = config.listeners.get_mut("web").unwrap();
                // What Alt-Svc names: the port the config gives, whatever socket the
                // test serves on.
                web.address = "127.0.0.1:8443".parse().unwrap();
                web.http3 = http3.then_some(edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                });
                secured(
                    &mut config,
                    vec![crate::tls::testing::certificate(&["a.test"])],
                    None,
                );
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                let stream = tls_client(front, "a.test", None, |_| {}).await.unwrap();
                let answer = h1_over_or_nothing(stream).await.to_ascii_lowercase();
                assert!(
                    answer.starts_with(&format!("http/1.1 {status}\r\n")),
                    "{answer}"
                );
                assert_eq!(
                    answer.contains("\r\nalt-svc: h3=\":8443\"; ma=60\r\n"),
                    http3,
                    "{answer}"
                );
            }
        })
        .await;
}

/// What a listener advertises follows the sockets it has, which a reload does not change
/// (03 §4): one that turns HTTP/3 on for a listener started without it advertises nothing,
/// as no UDP socket is there; one that moves the listener advertises the port it still
/// listens on; one that turns HTTP/3 off stops advertising it at once.
#[tokio::test]
async fn a_reload_advertises_only_the_http3_a_listener_has_a_socket_for() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let with = |http3: bool, port: u16| {
                let mut config = everything_config(upstream);
                let web = config.listeners.get_mut("web").unwrap();
                web.address = format!("127.0.0.1:{port}").parse().unwrap();
                web.http3 = http3.then_some(edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                });
                secured(
                    &mut config,
                    vec![crate::tls::testing::certificate(&["a.test"])],
                    None,
                );
                compile(&config).unwrap()
            };
            let advertised = async |proxy: &Arc<Proxy>| {
                let worker = Worker::with_deadlines(Arc::clone(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                let stream = tls_client(front, "a.test", None, |_| {}).await.unwrap();
                let answer = h1_over_or_nothing(stream).await.to_ascii_lowercase();
                assert!(answer.starts_with("http/1.1 200 ok\r\n"), "{answer}");
                answer
                    .lines()
                    .find_map(|line| line.strip_prefix("alt-svc: "))
                    .map(str::to_owned)
            };

            let started_without =
                Arc::new(Proxy::new(with(false, 8443), NonZeroUsize::MIN).unwrap());
            started_without.reload(with(true, 8443)).unwrap();
            assert_eq!(advertised(&started_without).await, None);

            let started_with = Arc::new(Proxy::new(with(true, 8443), NonZeroUsize::MIN).unwrap());
            started_with.reload(with(true, 8444)).unwrap();
            assert_eq!(
                advertised(&started_with).await.as_deref(),
                Some("h3=\":8443\"; ma=60")
            );
            started_with.reload(with(false, 8443)).unwrap();
            assert_eq!(advertised(&started_with).await, None);
        })
        .await;
}

/// A worker counts the handshakes under way on all its HTTP/3 listeners as one, which
/// is what its Retry threshold is held to (16 §6).
#[tokio::test]
async fn a_workers_http3_listeners_share_its_count_of_handshakes() {
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
            let worker = Worker::made(proxy, H1Limits::default(), SHORT, 0, None);
            let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
            let mut clients = Vec::new();
            for _ in 0..2 {
                let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let alone = Forwarding::group(1).remove(0);
                let _serving = tokio::task::spawn_local(
                    Rc::clone(&worker).serve_h3(0, socket, alone).unwrap(),
                );
                let mut client = Client::new(front, "a.test").await;
                client.flush().await;
                client.hear_for(Duration::from_millis(300)).await;
                clients.push(client);
            }
            assert_eq!(worker.handshakes.get(), 2);
            for client in &mut clients {
                client.until(|client| client.quic.is_established()).await;
            }
            until(|| worker.handshakes.get() == 0).await;
        })
        .await;
}

/// An HTTP/3 connection lives in the half of the pod's memory beside storage, as a TCP
/// connection does, so a worker's connection cap, sized from that half (03 §9), holds its
/// HTTP/3 connections too, each at what it costs: with room for one, a second client is
/// not admitted until the first has gone.
#[tokio::test]
async fn http3_connections_are_held_to_their_workers_connection_cap() {
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
            // Room for one HTTP/3 connection.
            let connections = Loads::new(1, 3, QUIC_MOST, 1);
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
            let mut first = Client::connect(front, "a.test").await;
            assert_eq!(connections.now(), [3]);
            let mut second = Client::new(front, "a.test").await;
            second.for_a_while(Duration::from_secs(1)).await;
            assert!(
                !second.quic.is_established(),
                "a second HTTP/3 connection admitted on a worker with room for one \
                     (the worker counts {:?})",
                connections.now()
            );
            // Each Initial dropped for want of room is counted.
            let dropped = proxy.metrics().lines().find_map(|line| {
                line.strip_prefix(
                    "edgerush_listener_quic_datagrams_total{listener=\"web\",event=\"no_room\"} ",
                )
                .and_then(|count| count.parse::<u64>().ok())
            });
            assert!(dropped.is_some_and(|count| count > 0), "{dropped:?}");

            // The first goes, and with it what it was counted: once its connection has
            // drained, three probe timeouts after the close (RFC 9000 §10.2), which are
            // made of the round trips measured, seconds long each on a loaded machine. So
            // the wait is bounded by far more than `until`'s ten seconds.
            first.quic.close(true, 0x100, b"").unwrap();
            first.flush().await;
            tokio::time::timeout(Duration::from_secs(60), async {
                while connections.now() != [0] {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the first connection was never let go of");
            second.until(|client| client.quic.is_established()).await;
            assert_eq!(connections.now(), [3]);
        })
        .await;
}
