//! [25 §9](../../../../../docs/25-backend-protection.md)'s scenarios on sockets that no test
//! of `protection` is about, so that every row of the table is one. Row by row:
//!
//! | Situation | Test |
//! |---|---|
//! | Within capacity | `customers_within_their_quota_are_admitted_and_nothing_is_refused_or_queued` |
//! | One pod slows down | `a_full_pod_is_passed_over_for_one_with_room` |
//! | The whole backend slows down | `a_full_backend_is_refused_before_its_rate_limits_are_charged`, `a_request_out_of_its_queues_time_is_refused_charged`, `each_pod_is_held_to_its_cap_however_many_ask_at_once` |
//! | A pod dies mid-request | `a_pod_that_dies_or_refuses_gives_its_slots_back_and_its_retry_takes_one_elsewhere` |
//! | Connects to a pod fail | the same, and `every_way_out_of_an_exchange_gives_its_slot_back` |
//! | A pod added | `a_pod_added_starts_at_zero_and_its_cap_ramps_with_slow_start` |
//! | A pod removed, a rolling update | `a_terminating_pod_is_not_picked_and_its_requests_finish` |
//! | The cap lowered or raised, a queue shrunk | `a_lowered_cap_kills_nothing_and_admits_nothing_until_the_pod_drains_below_it`, `a_reload_keeps_an_endpoints_count`, `a_raised_cap_lets_the_requests_waiting_in`, `a_queue_made_smaller_or_taken_away_keeps_its_waiters` |
//! | A data plane restarts | `each_data_plane_holds_each_pod_to_its_cap_for_itself` |
//! | The data planes scale out | the same |
//! | HTTP/2 to the backend | `requests_to_an_http2_backend_are_counted_and_one_waiting_for_a_stream_holds_its_slot` |
//! | WebSockets, long gRPC streams | `a_websocket_holds_a_slot_for_its_life`; a long stream is an exchange held open, as in the HTTP/2 test |
//! | A client gone while queued, or mid-exchange | `a_client_gone_while_queued_leaves_the_line`, `every_way_out_of_an_exchange_gives_its_slot_back` |
//! | Every pod full, no queue | `a_full_backend_is_refused_before_its_rate_limits_are_charged` |
//! | A request waiting on its limits (synchronous mode) | `sync_limits::a_request_waiting_on_its_owner_holds_no_slot_of_its_backend` |
//! | Every pod full, with a queue | `a_full_backend_queues_and_a_slot_given_back_goes_to_the_oldest_waiter`, `a_full_queue_is_refused_before_its_rate_limits_are_charged` |
//! | Others waiting, room found | `a_request_waits_behind_those_already_waiting_whatever_room_it_finds` |
//! | The route's request timeout passing in the queue | `a_routes_request_timeout_ends_a_wait_in_the_queue` |
//! | The local zone full | `a_full_zone_spills_to_the_others_before_the_backend_refuses` |
//! | A retry, every pod full | `a_retry_to_a_full_backend_is_not_sent` |
//! | A mirror's backend full | `a_copy_to_a_full_mirror_is_not_sent` |
//! | An upstream by the cluster IP | `an_upstream_by_the_cluster_ip_holds_the_whole_service_to_the_cap` |
//! | Several routes, Gateways or ports reaching one pod | `a_services_ports_share_each_pods_count` |
//! | Two DataPlanes fronting one Service | `each_data_plane_holds_each_pod_to_its_cap_for_itself` |
//! | A full pod and health | `a_full_pod_is_passed_over_for_one_with_room` |
//! | An operator looks | `protection_is_seen_per_upstream_and_only_where_it_is_set` |

use super::*;
use k8s_openapi::api::discovery::v1::EndpointSlice;

/// The requests in flight to each endpoint of upstream `name`, by its address.
fn counts_of(proxy: &Proxy, name: &str) -> Vec<(SocketAddr, u32)> {
    let snapshot = proxy.current.load();
    let at = snapshot
        .config
        .upstreams()
        .iter()
        .position(|upstream| upstream.name == name)
        .expect("the config has it");
    let protected = snapshot.protected[at].as_ref().expect("it is protected");
    (0..)
        .map_while(|endpoint| snapshot.destinations.at(at, endpoint))
        .zip(protected.counts.iter())
        .map(|(destination, count)| (destination.address(), count.in_flight()))
        .collect()
}

/// The requests in flight to `up`'s endpoint at `address`, if `up` has one there.
fn count_at(proxy: &Proxy, address: SocketAddr) -> Option<u32> {
    counts_of(proxy, "up")
        .into_iter()
        .find_map(|(at, count)| (at == address).then_some(count))
}

/// The requests in flight to every endpoint of `up`.
fn all_in_flight(proxy: &Proxy) -> u32 {
    counts_of(proxy, "up").iter().map(|(_, count)| count).sum()
}

/// A request to `front` for a path the scripted upstream never answers, its connection
/// returned, so that the test ends the request by dropping it.
async fn silent(front: SocketAddr) -> TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut client = TcpStream::connect(front).await.unwrap();
    client.write_all(&asking("/silent")).await.unwrap();
    client
}

/// What a scripted upstream holds: the connections of the requests it never answers.
type Held = Rc<RefCell<Vec<TcpStream>>>;

/// Scripted upstreams at each of `addresses`, all on one port: pods of one Service, for
/// which an EndpointSlice gives one port.
async fn scripted_pods(addresses: &[&str]) -> (u16, Vec<Held>) {
    'port: for _ in 0..20 {
        let first = TcpListener::bind((addresses[0], 0)).await.unwrap();
        let port = first.local_addr().unwrap().port();
        let mut listeners = vec![first];
        for address in &addresses[1..] {
            match TcpListener::bind((*address, port)).await {
                Ok(listener) => listeners.push(listener),
                Err(_) => continue 'port,
            }
        }
        let held = listeners
            .into_iter()
            .map(|listener| scripted_upstream_on(listener).1)
            .collect();
        return (port, held);
    }
    panic!("no port free at every one of {addresses:?}");
}

/// A config whose one rule forwards to `up`, which follows Service `web` in `shop` and is
/// protected to `cap` in flight, with `more` of the data plane's settings. Loopback
/// addresses discovered are sent to, as the tests' pods are at them.
fn following_web(cap: u32, more: &str) -> Compiled {
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
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
upstreams:
  up: {{ load_balancer: p2c, endpoints: {{ kubernetes: "web.shop.svc.cluster.local:http" }}, protection: {{ max_in_flight: {cap} }} }}
data_plane: {{ experimental_unrestricted_egress: true{more} }}
"#
    );
    compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap()
}

/// Namespace `shop`'s slices: one of Service `web`, its port `http` on `port`, of
/// `endpoints` written as YAML is.
fn web_slice(port: u16, endpoints: &[String]) -> (String, Option<Arc<[EndpointSlice]>>) {
    let slice = serde_saphyr::from_str(&format!(
        "metadata: {{ name: web-1, namespace: shop, labels: {{ kubernetes.io/service-name: web }} }}\naddressType: IPv4\nports: [{{ name: http, port: {port} }}]\nendpoints: [{}]\n",
        endpoints.join(", ")
    ))
    .unwrap();
    ("shop".to_owned(), Some(Arc::from(vec![slice])))
}

/// A ready endpoint at `address`, as a slice writes it.
fn ready(address: &str) -> String {
    format!("{{ addresses: [{address}] }}")
}

/// An endpoint at `address` that is terminating, still serving what it has.
fn leaving(address: &str) -> String {
    format!(
        "{{ addresses: [{address}], conditions: {{ ready: false, serving: true, terminating: true }} }}"
    )
}

/// Customers within their quota, to a backend within its capacity, are admitted: nothing
/// is refused or queued, and every request is charged once (25 §9).
#[tokio::test]
async fn customers_within_their_quota_are_admitted_and_nothing_is_refused_or_queued() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (a, _) = counting_upstream().await;
            let (b, _) = counting_upstream().await;
            let mut config = queueing(everything_config(a), 2, 4, 10_000);
            config.upstreams.get_mut("up").unwrap().endpoints =
                edgerush_config::Endpoints::Listed(vec![a, b]);
            let proxy = started(&limited_locally(config, 20));
            let (front, worker) = serving_from(&proxy).await;
            // Four at a time, the pods' two each.
            for _ in 0..5 {
                let answers: Vec<_> = (0..4).map(|_| sent(front, "/ok")).collect();
                for answer in answers {
                    assert_eq!(answer.await.unwrap(), StatusCode::OK);
                }
            }
            let scrape = worker.proxy().metrics();
            for reason in ["backend_full", "backend_queue_full", "backend_queue_timeout"] {
                let refused = format!(
                    r#"edgerush_upstream_protection_refused_total{{upstream="up",reason="{reason}"}}"#
                );
                assert_eq!(metric(&scrape, &refused), Some(0), "{scrape}");
            }
            let queued = r#"edgerush_upstream_protection_queued_total{upstream="up"}"#;
            assert_eq!(metric(&scrape, queued), Some(0), "{scrape}");
            let admitted =
                r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="admitted"}"#;
            assert_eq!(metric(&scrape, admitted), Some(20), "{scrape}");
            assert_eq!(all_in_flight(&proxy), 0);
        })
        .await;
}

/// However many requests arrive at once, from however many workers, each pod takes its cap
/// and no more, and the rest are refused: never more in flight than the pods' caps
/// together (25 §3, §9).
#[tokio::test]
async fn each_pod_is_held_to_its_cap_however_many_ask_at_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (a, held_at_a) = scripted_upstream().await;
            let (b, held_at_b) = scripted_upstream().await;
            let mut config = protecting(everything_config(a), 2);
            config.upstreams.get_mut("up").unwrap().endpoints =
                edgerush_config::Endpoints::Listed(vec![a, b]);
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let (first, _one) = serving_from(&proxy).await;
            let (second, _two) = serving_from(&proxy).await;
            let refused = Rc::new(Cell::new(0));
            for n in 0..10 {
                let front = if n % 2 == 0 { first } else { second };
                let refused = Rc::clone(&refused);
                let _asking = tokio::task::spawn_local(async move {
                    let mut client = TcpStream::connect(front).await.unwrap();
                    client.write_all(&asking("/silent")).await.unwrap();
                    let mut answer = Vec::new();
                    let _read = client.read_to_end(&mut answer).await;
                    if answer.starts_with(b"HTTP/1.1 503 ") {
                        refused.set(refused.get() + 1);
                    }
                });
            }
            until(|| {
                held_at_a.borrow().len() + held_at_b.borrow().len() == 4 && refused.get() == 6
            })
            .await;
            assert_eq!((held_at_a.borrow().len(), held_at_b.borrow().len()), (2, 2));
            assert_eq!(all_in_flight(&proxy), 4);
        })
        .await;
}

/// A pod that dies with a request on it, and one that cannot be connected to: the attempt
/// ends and gives its slot back, the retry takes a slot of its own on the other pod, and
/// nothing is left held; a pod that cannot be connected to is set aside (25 §6, §9).
#[tokio::test]
async fn a_pod_that_dies_or_refuses_gives_its_slots_back_and_its_retry_takes_one_elsewhere() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for dies in [true, false] {
                let (_held, nowhere) = refusing();
                let failing = if dies {
                    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let closing = socket.local_addr().unwrap();
                    let _accepting = tokio::task::spawn_local(async move {
                        while let Ok((mut stream, _)) = socket.accept().await {
                            // The request's head read, and the connection closed on it.
                            let mut head = [0; 1024];
                            let _read = stream.read(&mut head).await;
                        }
                    });
                    closing
                } else {
                    nowhere
                };
                let (answering, _) = statuses_upstream(vec![200]).await;
                let mut config = protecting(everything_config(failing), 1);
                let up = config.upstreams.get_mut("up").unwrap();
                up.endpoints = edgerush_config::Endpoints::Listed(vec![failing, answering]);
                up.load_balancer = edgerush_config::LoadBalancer::RoundRobin;
                config.routes[0].rules[0]
                    .forward
                    .as_mut()
                    .expect("the rule forwards")
                    .retry = Some(retrying(1, &[502], &[], 1));
                let proxy =
                    Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
                let (front, worker) = serving_from(&proxy).await;
                // Each pod in turn: the first try of one or more goes to the failing one.
                for _ in 0..4 {
                    assert_eq!(status_of(front, "/ok").await, StatusCode::OK, "dies {dies}");
                    until(|| all_in_flight(&proxy) == 0).await;
                }
                let scrape = worker.proxy().metrics();
                let retried = r#"edgerush_upstream_retries_total{upstream="up"}"#;
                assert!(
                    metric(&scrape, retried).is_some_and(|retried| retried >= 1),
                    "dies {dies}: {scrape}"
                );
                if !dies {
                    let aside = r#"edgerush_upstream_set_aside_endpoints{upstream="up"}"#;
                    assert_eq!(metric(&scrape, aside), Some(1), "{scrape}");
                }
            }
        })
        .await;
}

/// A pod leaving its Service's EndpointSlice, terminating, is not picked, and its requests
/// finish; its count is kept while they last, so that the pod back again before they end
/// counts them, and goes with the last (25 §7, §9).
#[tokio::test]
async fn a_terminating_pod_is_not_picked_and_its_requests_finish() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (port, held) = scripted_pods(&["127.0.0.2", "127.0.0.3"]).await;
            let a: SocketAddr = format!("127.0.0.2:{port}").parse().unwrap();
            let proxy = Arc::new(Proxy::new(following_web(1, ""), NonZeroUsize::MIN).unwrap());
            let (front, _worker) = serving_from(&proxy).await;
            assert!(
                proxy
                    .rediscover([web_slice(port, &[ready("127.0.0.2")])])
                    .unwrap()
            );
            let mut on_a = silent(front).await;
            until(|| held[0].borrow().len() == 1).await;

            // Terminating: nothing new goes to it, and its request is not ended.
            assert!(
                proxy
                    .rediscover([web_slice(port, &[leaving("127.0.0.2"), ready("127.0.0.3")])])
                    .unwrap()
            );
            assert_eq!(count_at(&proxy, a), None, "a terminating pod is picked");
            for _ in 0..3 {
                let other = silent(front).await;
                until(|| held[1].borrow().len() == 1).await;
                drop(other);
                held[1].borrow_mut().clear();
                until(|| all_in_flight(&proxy) == 0).await;
            }
            assert_eq!(held[0].borrow().len(), 1);
            assert_eq!(
                proxy.current.load().pods.len(),
                2,
                "its count went too soon"
            );

            // Back before its request ends: the count it had.
            assert!(
                proxy
                    .rediscover([web_slice(port, &[ready("127.0.0.2"), ready("127.0.0.3")])])
                    .unwrap()
            );
            assert_eq!(count_at(&proxy, a), Some(1));

            // Gone again, its request finishes, answered; and its count goes with it.
            assert!(
                proxy
                    .rediscover([web_slice(port, &[leaving("127.0.0.2"), ready("127.0.0.3")])])
                    .unwrap()
            );
            let mut upstream_side = held[0].borrow_mut().pop().unwrap();
            upstream_side
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
            let mut answer = Vec::new();
            on_a.read_to_end(&mut answer).await.unwrap();
            assert!(
                answer.starts_with(b"HTTP/1.1 200 OK\r\n"),
                "{}",
                String::from_utf8_lossy(&answer)
            );
            // The next config made, the same again, finds only the pod still served.
            proxy.reload(following_web(1, "")).unwrap();
            assert_eq!(proxy.current.load().pods.len(), 1, "its count outlived it");
        })
        .await;
}

/// A pod added starts at zero, and its cap ramps with slow start: with every other pod
/// full, it takes a tenth of its cap at first, rounded down and never below one, not the
/// whole of it at once; without slow start, the whole (25 §4, §9).
#[tokio::test]
async fn a_pod_added_starts_at_zero_and_its_cap_ramps_with_slow_start() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for slow in [true, false] {
                let (a, _) = scripted_upstream().await;
                let (b, held_at_b) = scripted_upstream().await;
                let config = |endpoints: Vec<SocketAddr>| {
                    let mut config = protecting(everything_config(a), 4);
                    let up = config.upstreams.get_mut("up").unwrap();
                    up.endpoints = edgerush_config::Endpoints::Listed(endpoints);
                    up.slow_start = slow.then_some(edgerush_config::SlowStart {
                        window_ms: 3_600_000,
                    });
                    compile(&config).unwrap()
                };
                let proxy = Arc::new(Proxy::new(config(vec![a]), NonZeroUsize::MIN).unwrap());
                let (front, _worker) = serving_from(&proxy).await;
                let _full: Vec<_> = (0..4).map(|_| hold(&proxy)).collect();

                proxy.reload(config(vec![a, b])).unwrap();
                assert_eq!(count_at(&proxy, b), Some(0), "slow {slow}");
                let taken = if slow { 1 } else { 4 };
                let mut waiting = Vec::new();
                for _ in 0..taken {
                    waiting.push(silent(front).await);
                    until(|| held_at_b.borrow().len() == waiting.len()).await;
                }
                assert_eq!(
                    status_of(front, "/ok").await,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "slow {slow}"
                );
                assert_eq!(count_at(&proxy, b), Some(taken), "slow {slow}");
            }
        })
        .await;
}

/// A cap lowered kills nothing: the pod's requests go on, and it takes nothing new until
/// they drain below the new cap (25 §7).
#[tokio::test]
async fn a_lowered_cap_kills_nothing_and_admits_nothing_until_the_pod_drains_below_it() {
    use tokio::io::AsyncReadExt;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let proxy = Arc::new(Proxy::new(protected(upstream, 2), NonZeroUsize::MIN).unwrap());
            let (front, _worker) = serving_from(&proxy).await;
            let mut first = silent(front).await;
            let second = silent(front).await;
            until(|| held.borrow().len() == 2).await;

            proxy.reload(protected(upstream, 1)).unwrap();
            assert_eq!(in_flight(&proxy), 2);
            let mut byte = [0; 1];
            assert!(
                tokio::time::timeout(Duration::from_millis(100), first.read(&mut byte))
                    .await
                    .is_err(),
                "a request in flight was ended"
            );
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );

            // One ends: at the new cap, still nothing new.
            drop(second);
            until(|| in_flight(&proxy) == 1).await;
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );

            drop(first);
            until(|| in_flight(&proxy) == 0).await;
            assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        })
        .await;
}

/// Each data plane holds each pod to the cap for itself: one at the cap refuses while
/// another, a restarted data plane at zero or one more of a fleet scaled out, still takes
/// up to the cap of its own; two DataPlanes fronting one Service alike (25 §1, §3, §9).
#[tokio::test]
async fn each_data_plane_holds_each_pod_to_its_cap_for_itself() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let one = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
            let (at_one, _first) = serving_from(&one).await;
            let _on_one = silent(at_one).await;
            until(|| held.borrow().len() == 1).await;
            assert_eq!(
                status_of(at_one, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );

            let two = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
            let (at_two, _second) = serving_from(&two).await;
            assert_eq!(in_flight(&two), 0, "a new data plane starts at zero");
            let _on_two = silent(at_two).await;
            until(|| held.borrow().len() == 2).await;
            assert_eq!(
                status_of(at_two, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!((in_flight(&one), in_flight(&two)), (1, 1));
        })
        .await;
}

/// To an HTTP/2 backend requests are counted, not connections: two on one connection hold
/// a slot each, and a request waiting in the pool for a stream, the connection's streams
/// all in use, already holds its own (25 §6).
#[tokio::test]
async fn requests_to_an_http2_backend_are_counted_and_one_waiting_for_a_stream_holds_its_slot() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let how = UpstreamH2 {
                gated: true,
                ..UpstreamH2::default()
            };
            let (upstream, seen, gate) = h2_upstream(how).await;
            let mut config = protecting(everything_config(upstream), 3);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            let proxy =
                Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let limits = H1Limits {
                h2_streams: 2,
                h2_connections: 1,
                h2_waiting: 4,
                ..H1Limits::default()
            };
            let worker = Worker::with_deadlines(Arc::clone(&proxy), limits, SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut answers = Vec::new();
            for _ in 0..2 {
                answers.push(tokio::task::spawn_local(h1_answer(front, CLOSING_GET)));
            }
            until(|| seen.open.get() == 2).await;
            assert_eq!(seen.connections.get(), 1);
            assert_eq!(in_flight(&proxy), 2, "two streams, one connection");

            // The connection's two streams in use: the next waits for one, holding its slot.
            answers.push(tokio::task::spawn_local(h1_answer(front, CLOSING_GET)));
            until(|| in_flight(&proxy) == 3).await;
            assert_eq!(seen.open.get(), 2);
            let refused = h1_answer(front, CLOSING_GET).await;
            assert!(refused.starts_with("HTTP/1.1 503 "), "{refused}");
            let scrape = worker.proxy().metrics();
            let full =
                r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_full"}"#;
            assert_eq!(metric(&scrape, full), Some(1), "{scrape}");

            gate.add_permits(3);
            for answer in answers {
                let answer = answer.await.unwrap();
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            }
            until(|| in_flight(&proxy) == 0).await;
        })
        .await;
}

/// A WebSocket holds its slot for its life, its messages going both ways meanwhile, and
/// gives it back when it closes (25 §6).
#[tokio::test]
async fn a_websocket_holds_a_slot_for_its_life() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = echoing_websocket_backend().await;
            let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
            let (front, _worker) = serving_from(&proxy).await;
            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(
                    b"GET /chat HTTP/1.1\r\nhost: example.test\r\nupgrade: websocket\r\n\
                      connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                      sec-websocket-version: 13\r\n\r\n",
                )
                .await
                .unwrap();
            let mut said = Vec::new();
            let mut bytes = [0; 1024];
            while !said.ends_with(b"hello") {
                let read = client.read(&mut bytes).await.unwrap();
                assert_ne!(read, 0, "{}", String::from_utf8_lossy(&said));
                said.extend_from_slice(&bytes[..read]);
            }
            assert!(said.starts_with(b"HTTP/1.1 101 "));
            assert_eq!(in_flight(&proxy), 1);
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );

            client.write_all(b"hi").await.unwrap();
            let read = client.read(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..read], b"[hi]");
            assert_eq!(in_flight(&proxy), 1);

            drop(client);
            until(|| in_flight(&proxy) == 0).await;
        })
        .await;
}

/// A zone whose pods are all full counts as failing for zone preference: requests spill to
/// the other zones' pods before the backend refuses (25 §4).
#[tokio::test]
async fn a_full_zone_spills_to_the_others_before_the_backend_refuses() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (port, held) = scripted_pods(&["127.0.0.2", "127.0.0.3"]).await;
            let proxy = Arc::new(
                Proxy::new(
                    following_web(1, ", own_service: edgerush.edge.svc.cluster.local"),
                    NonZeroUsize::MIN,
                )
                .unwrap(),
            );
            proxy.deny(
                crate::egress::Denied {
                    own: vec!["10.9.0.1".parse().unwrap()],
                    ..crate::egress::Denied::default()
                },
                |_, _, _| {},
            );
            let hinted = |address: &str, zone: &str| {
                format!(
                    "{{ addresses: [{address}], zone: {zone}, hints: {{ forZones: [{{ name: {zone} }}] }} }}"
                )
            };
            let own: EndpointSlice = serde_saphyr::from_str(
                "metadata: { name: edgerush-1, namespace: edge, labels: { kubernetes.io/service-name: edgerush } }\naddressType: IPv4\nports: [{ name: http, port: 8080 }]\nendpoints: [{ addresses: [10.9.0.1], zone: a }]\n",
            )
            .unwrap();
            assert!(
                proxy
                    .rediscover([
                        web_slice(port, &[hinted("127.0.0.2", "a"), hinted("127.0.0.3", "b")]),
                        ("edge".to_owned(), Some(Arc::from(vec![own]))),
                    ])
                    .unwrap()
            );
            assert_eq!(proxy.current.load().zoned[0], 1, "zone `a` preferred");
            let (front, _worker) = serving_from(&proxy).await;

            // The zone's own pod first; full, the other zone's; both full, refused.
            let _first = silent(front).await;
            until(|| held[0].borrow().len() == 1).await;
            let _second = silent(front).await;
            until(|| held[1].borrow().len() == 1).await;
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        })
        .await;
}

/// An upstream by the Service's cluster IP, a name resolved to one address, is one
/// endpoint to the data plane, and the cap applies to it: the whole Service held to the
/// cap (25 §3).
#[tokio::test]
async fn an_upstream_by_the_cluster_ip_holds_the_whole_service_to_the_cap() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let name = "web.shop.svc.cluster.local";
            let mut config = protecting(everything_config(upstream), 2);
            config.upstreams.get_mut("up").unwrap().endpoints =
                edgerush_config::Endpoints::Dns(format!("{name}:{}", upstream.port()));
            config.data_plane.experimental_unrestricted_egress = Some(true);
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            assert!(
                proxy
                    .reresolve([(name.to_owned(), Some(vec![upstream.ip()]))])
                    .unwrap()
            );
            assert_eq!(counts_of(&proxy, "up"), [(upstream, 0)]);
            let (front, _worker) = serving_from(&proxy).await;
            let _waiting = (silent(front).await, silent(front).await);
            until(|| held.borrow().len() == 2).await;
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        })
        .await;
}

/// A Service's ports share each pod's count: a pod full from one port's upstream is full
/// to the other's, whichever routes reach them (25 §3).
#[tokio::test]
async fn a_services_ports_share_each_pods_count() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (http, held) = scripted_upstream().await;
            let (admin, _) = scripted_upstream().await;
            let yaml = r#"
listeners:
  web: { address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: /admin }
        forward: { backends: [{ upstream: admin, weight: 1 }] }
      - matches:
          - path: { prefix: / }
        forward: { backends: [{ upstream: up, weight: 1 }] }
upstreams:
  admin: { load_balancer: p2c, endpoints: { kubernetes: "web.shop.svc.cluster.local:admin" }, protection: { max_in_flight: 1 } }
  up: { load_balancer: p2c, endpoints: { kubernetes: "web.shop.svc.cluster.local:http" }, protection: { max_in_flight: 1 } }
data_plane: { experimental_unrestricted_egress: true }
"#;
            let config = compile(&serde_saphyr::from_str::<Config>(yaml).unwrap()).unwrap();
            let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN).unwrap());
            let slice: EndpointSlice = serde_saphyr::from_str(&format!(
                "metadata: {{ name: web-1, namespace: shop, labels: {{ kubernetes.io/service-name: web }} }}\naddressType: IPv4\nports: [{{ name: http, port: {} }}, {{ name: admin, port: {} }}]\nendpoints: [{{ addresses: [127.0.0.1] }}]\n",
                http.port(),
                admin.port()
            ))
            .unwrap();
            assert!(
                proxy
                    .rediscover([("shop".to_owned(), Some(Arc::from(vec![slice])))])
                    .unwrap()
            );
            let (front, _worker) = serving_from(&proxy).await;

            let waiting = silent(front).await;
            until(|| held.borrow().len() == 1).await;
            assert_eq!(counts_of(&proxy, "admin"), [(admin, 1)]);
            assert_eq!(
                status_of(front, "/admin/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
            drop(waiting);
            until(|| all_in_flight(&proxy) == 0).await;
            assert_eq!(status_of(front, "/admin/ok").await, StatusCode::OK);
        })
        .await;
}
