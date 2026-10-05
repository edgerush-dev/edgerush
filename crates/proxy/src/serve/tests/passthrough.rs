//! The connections of `tcp` and `tls` listeners, carried to a backend.

use super::*;

/// A backend that accepts and then says and reads nothing, for as long as the
/// connection lasts.
async fn quiet_backend() -> SocketAddr {
    use tokio::io::AsyncReadExt;
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            tokio::task::spawn_local(async move {
                let mut rest = Vec::new();
                let _ended = stream.read_to_end(&mut rest).await;
            });
        }
    });
    address
}

/// A backend that says `name` to every connection as it accepts it, and then reads
/// until the connection ends.
async fn naming_backend(name: &'static str) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            tokio::task::spawn_local(async move {
                stream.write_all(name.as_bytes()).await.unwrap();
                let mut rest = Vec::new();
                let _ended = stream.read_to_end(&mut rest).await;
            });
        }
    });
    address
}

/// A tunnel is load on its backend for as long as it is open (03 §6): with every tunnel
/// held, `p2c` sends each new one to the backend with fewer, so after every second one
/// both have as many. Were an open tunnel not counted, each would go either way.
#[tokio::test]
async fn an_open_tunnel_counts_against_its_backend() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let a = naming_backend("a").await;
            let b = naming_backend("b").await;
            let yaml = tcp_to(a, "").replace(
                &format!("endpoints: [\"{a}\"]"),
                &format!("endpoints: [\"{a}\", \"{b}\"]"),
            );
            let (front, _worker) = passing(&yaml).await;
            let mut held = Vec::new();
            let mut went = std::collections::BTreeMap::new();
            for opened in 1..=20 {
                let mut client = TcpStream::connect(front).await.unwrap();
                let mut name = [0; 1];
                bounded(client.read_exact(&mut name)).await.unwrap();
                *went.entry(name[0]).or_insert(0) += 1;
                held.push(client);
                if opened % 2 == 0 {
                    assert_eq!(went[&b'a'], went[&b'b'], "after {opened}: {went:?}");
                }
            }
        })
        .await;
}

/// A `tcp` listener's connection is carried to its route's backend and back, byte for
/// byte: the client's end is passed on, so that the backend knows it has everything
/// and answers, and the backend's end is passed back (17 §4).
#[tokio::test]
async fn a_tcp_tunnel_carries_bytes_both_ways_and_each_end() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = tallying_backend().await;
            let (front, worker) = passing(&tcp_to(backend, "")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            // Several blocks' worth, so that each way fills and empties many times.
            let sent: Vec<u8> = (0..300_000_u32).map(|at| (at % 253) as u8).collect();
            client.write_all(&sent).await.unwrap();
            client.shutdown().await.unwrap();
            let mut answer = String::new();
            bounded(client.read_to_string(&mut answer)).await.unwrap();
            let sum: u64 = sent.iter().map(|&byte| u64::from(byte)).sum();
            assert_eq!(answer, format!("{} {sum}", sent.len()));
            tunnel_ended(&worker, "db", "closed").await;
        })
        .await;
}

/// A backend that finishes first has its end passed back, and what the client sends
/// after it still goes on, until the client finishes too.
#[tokio::test]
async fn a_backend_that_finishes_first_still_hears_the_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backend = socket.local_addr().unwrap();
            let heard = Rc::new(RefCell::new(Vec::new()));
            let hearing = Rc::clone(&heard);
            tokio::task::spawn_local(async move {
                let (mut stream, _) = socket.accept().await.unwrap();
                stream.write_all(b"hello").await.unwrap();
                stream.shutdown().await.unwrap();
                let mut came = Vec::new();
                stream.read_to_end(&mut came).await.unwrap();
                *hearing.borrow_mut() = came;
            });
            let (front, worker) = passing(&tcp_to(backend, "")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let mut said = Vec::new();
            bounded(client.read_to_end(&mut said)).await.unwrap();
            assert_eq!(said, b"hello");
            client.write_all(b"and goodbye").await.unwrap();
            client.shutdown().await.unwrap();
            until(|| heard.borrow().as_slice() == b"and goodbye").await;
            tunnel_ended(&worker, "db", "closed").await;
        })
        .await;
}

/// A tunnel that carries nothing either way for its listener's idle bound is closed.
#[tokio::test]
async fn an_idle_tunnel_is_closed_at_its_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = quiet_backend().await;
            let (front, worker) = passing(&tcp_to(backend, ", tunnel_idle_seconds: 1")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let took = closed_after(&mut client).await;
            let bound = Duration::from_secs(1);
            assert!(took + EARLY >= bound, "closed after {took:?}");
            assert!(took < bound + SLACK, "closed after {took:?}");
            tunnel_ended(&worker, "db", "idle").await;
        })
        .await;
}

/// A backend that cannot be reached, or an upstream with no endpoint, closes the
/// client's connection, counted by why.
#[tokio::test]
async fn a_tunnel_with_no_backend_to_reach_is_closed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, gone) = refusing();
            let (front, worker) = passing(&tcp_to(gone, "")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            // Refused at once on Linux; Windows tries again for two seconds or so. The
            // connect bound is what holds either way.
            let bound = H1Limits::default().connect;
            assert!(closed_after(&mut client).await < bound + SLACK);
            tunnel_ended(&worker, "db", "connect_failed").await;
            // Set aside for it, as any try that cannot connect sets its endpoint aside.
            let scrape = worker.proxy().metrics();
            let line = "edgerush_upstream_set_aside_endpoints{upstream=\"up\"} 1
";
            assert!(scrape.contains(line), "{scrape}");

            let yaml = tcp_to(gone, "").replace(&format!("[\"{gone}\"]"), "[]");
            let (front, worker) = passing(&yaml).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            assert!(closed_after(&mut client).await < SLACK);
            tunnel_ended(&worker, "db", "no_backend").await;
        })
        .await;
}

/// A `tls` listener's connection goes to the backend whose route's hostnames cover the
/// name its ClientHello asks for, the most specific first, and the handshake is the
/// backend's own: the ClientHello went on unchanged. A name no route has, or none at
/// all, is refused.
#[tokio::test]
async fn a_tls_tunnel_goes_where_the_name_asked_for_routes_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let exact = tls_backend("exact").await;
            let wildcard = tls_backend("wildcard").await;
            let yaml = format!(
                "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: off }} }}\n\
                     routes: []\n\
                     tls_routes:\n\
                     \x20 - {{ name: exact, listeners: [sni], hostnames: [{{ name: api.example.test, falls_through: true }}], backends: [{{ upstream: exact, weight: 1 }}] }}\n\
                     \x20 - {{ name: rest, listeners: [sni], hostnames: [{{ name: \"*.example.test\", wildcard: any_labels, falls_through: true }}], backends: [{{ upstream: wildcard, weight: 1 }}] }}\n\
                     upstreams: {{ exact: {{ load_balancer: p2c, endpoints: [\"{exact}\"] }}, wildcard: {{ load_balancer: p2c, endpoints: [\"{wildcard}\"] }} }}\n"
            );
            let (front, worker) = passing(&yaml).await;
            assert_eq!(
                told_over_tls(front, Some("api.example.test")).await.as_deref(),
                Some("exact")
            );
            assert_eq!(
                told_over_tls(front, Some("WWW.Example.test")).await.as_deref(),
                Some("wildcard")
            );
            tunnel_ended(&worker, "sni", "closed").await;
            assert_eq!(told_over_tls(front, Some("elsewhere.test")).await, None);
            tunnel_ended(&worker, "sni", "refused").await;
            assert_eq!(told_over_tls(front, None).await, None);
            let line = "edgerush_listener_tunnels_total{listener=\"sni\",outcome=\"refused\"} 2\n";
            until(|| worker.proxy().metrics().contains(line)).await;
        })
        .await;
}

/// A TLS client that has not sent its ClientHello within the first-request deadline is
/// let go of.
#[tokio::test]
async fn a_client_hello_that_never_comes_is_given_up_on() {
    use tokio::io::AsyncWriteExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = quiet_backend().await;
            let yaml = format!(
                "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: off }} }}\n\
                     routes: []\n\
                     tls_routes: [{{ name: a, listeners: [sni], hostnames: [{{ name: a.test, falls_through: true }}], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
                     upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
            );
            let (front, worker) = passing(&yaml).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            // The start of a handshake record, and then nothing.
            client.write_all(&[22, 3, 1, 1, 0]).await.unwrap();
            let took = closed_after(&mut client).await;
            assert!(took + EARLY >= SHORT.first_request, "closed after {took:?}");
            assert!(took < SHORT.first_request + SLACK, "closed after {took:?}");
            tunnel_ended(&worker, "sni", "too_slow").await;
        })
        .await;
}

/// A tunnel carrying on when the worker drains is left the drain's bound, and then
/// closed; no new connection is taken meanwhile.
#[tokio::test]
async fn a_draining_worker_closes_tunnels_at_its_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = quiet_backend().await;
            let (front, worker) = passing(&tcp_to(backend, "")).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            until(|| {
                worker
                    .proxy()
                    .metrics()
                    .contains("edgerush_listener_connections_active{listener=\"db\"} 1\n")
            })
            .await;
            worker.drain();
            let took = closed_after(&mut client).await;
            assert!(took + EARLY >= SHORT.drain, "closed after {took:?}");
            assert!(took < SHORT.drain + SLACK, "closed after {took:?}");
            tunnel_ended(&worker, "db", "drained").await;
        })
        .await;
}

/// A worker serving `yaml`'s one listener, a passthrough one, as `passing` does, with its
/// sweep run every 50 ms, as `serving_swept`'s.
async fn passing_swept(yaml: &str) -> (SocketAddr, Rc<Worker>) {
    let config: Config = serde_saphyr::from_str(yaml).unwrap();
    serving_swept(compile(&config).unwrap()).await
}

/// A tunnel open across a reload that keeps its listener, its route and the route's
/// upstream is left alone, whatever else the reload changes; one that takes the
/// listener and its route away drains it from the worker's next sweep: closed at the
/// drain's bound, as a draining worker's are (03 §10).
#[tokio::test]
async fn a_tunnel_is_drained_once_a_reload_takes_its_route_away() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = naming_backend("a").await;
            let other = naming_backend("b").await;
            let yaml = tcp_to(backend, "");
            let (front, worker) = passing_swept(&yaml).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let mut name = [0; 1];
            bounded(client.read_exact(&mut name)).await.unwrap();
            assert_eq!(&name, b"a");

            // Another weight and another upstream: the route is the same.
            let route = "backends: [{ upstream: up, weight: 1 }]";
            assert!(yaml.contains(route), "{yaml}");
            let reweighted = yaml
                .replace(
                    route,
                    "backends: [{ upstream: up, weight: 3 }, { upstream: spare, weight: 0 }]",
                )
                .replace(
                    "upstreams: {",
                    &format!(
                        "upstreams: {{ spare: {{ load_balancer: p2c, endpoints: [\"{other}\"] }}, "
                    ),
                );
            let reweighted: Config = serde_saphyr::from_str(&reweighted).unwrap();
            worker
                .proxy()
                .reload(compile(&reweighted).unwrap())
                .unwrap();
            let quiet = worker.limits.sweep * 2 + SHORT.drain + SLACK;
            assert!(
                still_open(&mut client, quiet).await,
                "a tunnel whose route was kept ended"
            );

            // No listener, no route: what comes in on its socket has none (03 §3).
            let without: Config = serde_saphyr::from_str(&format!(
                "listeners: {{ web: {{ address: \"127.0.0.1:0\", protocol: http, \
                     proxy_protocol: off, forwarding: {{ trusted_proxies: [], \
                     trusted_only_headers: [] }}, request_id: generate }} }}\n\
                     routes: []\n\
                     upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
            ))
            .unwrap();
            worker.proxy().reload(compile(&without).unwrap()).unwrap();
            let took = closed_after(&mut client).await;
            assert!(took + EARLY >= SHORT.drain, "closed after {took:?}");
            assert!(
                took < worker.limits.sweep + SHORT.drain + SLACK,
                "closed after {took:?}"
            );
            tunnel_ended(&worker, "db", "drained").await;
        })
        .await;
}

/// A ClientHello asking for `api.example.com`, as BoringSSL sends one: the fuzz target's seed.
const HELLO: &[u8] = include_bytes!("../../../../../fuzz/seeds/tls_hello/named");

/// A TLS tunnel is known by its route among its listener's: one open across a reload that
/// takes another route away, and moves its own to another place, is left alone; once a
/// reload takes its own route away, it is drained.
#[tokio::test]
async fn a_tls_tunnel_is_drained_once_a_reload_takes_its_route_away() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = naming_backend("a").await;
            let tls = |routes: &str| {
                format!(
                    "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls, proxy_protocol: off }} }}\n\
                     routes: []\n\
                     tls_routes:\n{routes}\
                     upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
                )
            };
            let route = |name: &str| {
                format!(
                    "\x20 - {{ name: {name}, listeners: [sni], hostnames: [{{ name: {name}.example.com, falls_through: true }}], backends: [{{ upstream: up, weight: 1 }}] }}\n"
                )
            };
            let (api, other) = (route("api"), route("other"));
            let (front, worker) = passing_swept(&tls(&format!("{other}{api}"))).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(HELLO).await.unwrap();
            let mut name = [0; 1];
            bounded(client.read_exact(&mut name)).await.unwrap();
            assert_eq!(&name, b"a");

            let reload = |yaml: String| {
                let config: Config = serde_saphyr::from_str(&yaml).unwrap();
                worker.proxy().reload(compile(&config).unwrap()).unwrap();
            };
            reload(tls(&api));
            let quiet = worker.limits.sweep * 2 + SHORT.drain + SLACK;
            assert!(
                still_open(&mut client, quiet).await,
                "a tunnel whose route was kept ended"
            );
            reload(tls(&other));
            let took = closed_after(&mut client).await;
            assert!(took + EARLY >= SHORT.drain, "closed after {took:?}");
            assert!(
                took < worker.limits.sweep + SHORT.drain + SLACK,
                "closed after {took:?}"
            );
            tunnel_ended(&worker, "sni", "drained").await;
        })
        .await;
}

/// A tunnel whose route a reload sends to another upstream is drained, as the route's
/// listing of the upstream it went to is gone; new connections go to the new one.
#[tokio::test]
async fn a_tunnel_is_drained_once_a_reload_takes_its_upstream_from_its_route() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let backend = naming_backend("a").await;
            let other = naming_backend("b").await;
            let yaml = tcp_to(backend, "");
            let (front, worker) = passing_swept(&yaml).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            let mut name = [0; 1];
            bounded(client.read_exact(&mut name)).await.unwrap();
            assert_eq!(&name, b"a");

            let repointed = yaml
                .replace("{ upstream: up, weight: 1 }", "{ upstream: elsewhere, weight: 1 }")
                .replace(
                    "upstreams: {",
                    &format!(
                        "upstreams: {{ elsewhere: {{ load_balancer: p2c, endpoints: [\"{other}\"] }}, "
                    ),
                );
            let repointed: Config = serde_saphyr::from_str(&repointed).unwrap();
            worker.proxy().reload(compile(&repointed).unwrap()).unwrap();
            let took = closed_after(&mut client).await;
            assert!(took + EARLY >= SHORT.drain, "closed after {took:?}");
            assert!(
                took < worker.limits.sweep + SHORT.drain + SLACK,
                "closed after {took:?}"
            );
            let mut fresh = TcpStream::connect(front).await.unwrap();
            bounded(fresh.read_exact(&mut name)).await.unwrap();
            assert_eq!(&name, b"b");
        })
        .await;
}
