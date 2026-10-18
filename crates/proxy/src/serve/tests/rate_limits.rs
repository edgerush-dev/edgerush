//! The rate limits route rules are under, over HTTP/2 and HTTP/3 as over HTTP/1.1
//! (`tests/ratelimit.rs`): the request core decides them whatever a request came by, with
//! three service pods on real sockets, and refuses what is over with 429.

use super::*;
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client as PodClient, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use tonic::transport::server::TcpIncoming;

/// Three service pods, once they agree all three serve: their addresses.
async fn service() -> Vec<String> {
    counted_service(&Arc::new(AtomicUsize::new(0))).await
}

/// The same, each exchange any of its pods is sent counted in `exchanges`.
async fn counted_service(exchanges: &Arc<AtomicUsize>) -> Vec<String> {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let timing = Timing {
        cycle: Duration::from_millis(10),
        heartbeat: Duration::from_millis(20),
        silence: Duration::from_millis(150),
        settle: Duration::from_millis(60),
    };
    let mut pods = Vec::new();
    for (id, listener) in (0..3).zip(listeners) {
        let peers: BTreeMap<PodId, PodClient> = (0..3)
            .filter(|pod| *pod != id)
            .map(|pod| (pod, PodClient::new(&addresses[pod as usize]).unwrap()))
            .collect();
        let pod = Pod::spawn(id, vec![0, 1, 2], 1, timing, peers);
        let server = Counted {
            server: Server::new(pod.clone()),
            exchanges: Arc::clone(exchanges),
        };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await
                .unwrap();
        });
        pods.push(pod);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    for pod in &pods {
        while !matches!(pod.view().await, Some((view, true)) if view.pods == [0, 1, 2]) {
            assert!(Instant::now() < deadline, "the pods never agreed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // A pod taken in makes no key afresh for three heartbeats, in case one is handed to it.
    tokio::time::sleep(Duration::from_millis(80)).await;
    addresses
}

/// A service pod's server that counts the exchanges it is sent.
#[derive(Clone)]
struct Counted {
    server: Server,
    exchanges: Arc<AtomicUsize>,
}

impl tonic::server::NamedService for Counted {
    const NAME: &'static str = <Server as tonic::server::NamedService>::NAME;
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Counted {
    type Response = <Server as tower_service::Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Server as tower_service::Service<http::Request<tonic::body::Body>>>::Error;
    type Future = <Server as tower_service::Service<http::Request<tonic::body::Body>>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.server.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        if request.uri().path().ends_with("/Exchange") {
            self.exchanges.fetch_add(1, Ordering::Relaxed);
        }
        self.server.call(request)
    }
}

/// `config` with its first route's first rule under ten requests an hour for each client,
/// a burst of ten, shared through `service`.
fn limited(mut config: Config, service: &[String]) -> Config {
    config.routes[0].rules[0].rate_limits = vec!["per-client".to_owned()];
    config.limits = Some(
        serde_saphyr::from_str(
            "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }\n",
        )
        .unwrap(),
    );
    config.rate_limit_service = Some(edgerush_config::RateLimitService {
        pods: service.to_vec(),
        cycle_ms: Some(10),
        max_key_bytes: None,
        layered: None,
        layered_keep_ms: None,
        shared: None,
        pooled: None,
        poll_quiet_keys: None,
        ask_cold_keys: None,
    });
    config
}

fn tally(statuses: &[u16]) -> (usize, usize) {
    (
        statuses.iter().filter(|s| **s == 200).count(),
        statuses.iter().filter(|s| **s == 429).count(),
    )
}

/// Fifteen requests over HTTP/2 from one client, on one connection: ten admitted, the
/// rest refused 429, saying when to come back; then a gRPC call, refused as gRPC is.
#[tokio::test]
async fn http2_requests_are_held_to_their_limit() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let config = limited(everything_config(upstream), &service);
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for _ in 0..15 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    assert_eq!(response.headers()["retry-after"], "360");
                    assert_eq!(response.headers()["ratelimit"], r#""per-client";r=0;t=360"#);
                }
                statuses.push(response.status().as_u16());
            }
            assert_eq!(tally(&statuses), (10, 5), "{statuses:?}");
            // A gRPC call is told its status, and nothing of when to come back.
            send = within(send.ready()).await.unwrap();
            let (answer, _) = send.send_request(grpc_call("/", None), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.headers()["grpc-status"], "8");
            assert!(answer.headers().get("retry-after").is_none());
            assert!(answer.headers().get("ratelimit").is_none());
        })
        .await;
}

/// A reload that changes how long an unused slice is kept starts the process layer afresh,
/// with the new keep; one that changes nothing keeps it.
#[tokio::test]
async fn a_changed_keep_starts_the_process_layer_afresh() {
    let mut config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    if let Some(service) = &mut config.rate_limit_service {
        service.layered = Some(true);
    }
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let service = |keep| {
        let mut config = config.clone();
        if let Some(service) = &mut config.rate_limit_service {
            service.layered_keep_ms = keep;
        }
        compile(&config)
            .unwrap()
            .rate_limit_service()
            .unwrap()
            .clone()
    };
    let first = proxy.hub(&service(None)).unwrap();
    let same = proxy.hub(&service(None)).unwrap();
    assert!(Arc::ptr_eq(&first, &same), "nothing changed");
    let kept = proxy.hub(&service(Some(50))).unwrap();
    assert!(!Arc::ptr_eq(&first, &kept), "a new keep");
}

/// Layered, `layered_keep_ms` reaches the process: a slice its worker leaves unused is
/// given again at its sync while the keep lasts, and given back at once without one.
#[tokio::test]
async fn a_layered_keep_reaches_the_process() {
    use crate::ratelimit::{FromHub, Hub, ToHub};
    use edgerush_ratelimit::service::Contract;
    use edgerush_ratelimit::service::layer::Used;
    use std::num::NonZeroU64;

    let service = service().await;
    let contract = Contract {
        limit: NonZeroU64::new(10).unwrap(),
        window: NonZeroU64::new(3_600_000_000_000).unwrap(),
        burst: NonZeroU64::new(10).unwrap(),
    };
    for (keep, kept) in [(0, false), (1_000, true)] {
        let compiled = edgerush_config::CompiledRateLimitService {
            pods: service.clone(),
            cycle: Duration::from_millis(10),
            max_key_bytes: 1 << 20,
            layered: true,
            layered_keep: Duration::from_millis(keep),
            shared: false,
            pooled: false,
            poll_quiet_keys: true,
            ask_cold_keys: true,
        };
        let hub = Hub::start(&compiled, 1, 1 << 20).unwrap();
        let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
        hub.to.send(ToHub::Join { worker: 0, answers }).unwrap();
        let key: Box<[u8]> = format!("kept-{keep}").into_bytes().into();
        // Asked until an answer brings a slice: the first waits on the service, and the
        // copy is fresh once a cycle has bounded it.
        let deadline = Instant::now() + Duration::from_secs(5);
        for id in 0.. {
            assert!(Instant::now() < deadline, "no slice for keep {keep}");
            let keys = vec![(key.clone(), contract)];
            hub.to
                .send(ToHub::Ask {
                    worker: 0,
                    id,
                    keys,
                })
                .unwrap();
            let answer = loop {
                match within(answered.recv()).await.unwrap() {
                    FromHub::Answer {
                        id: answered,
                        answer,
                    } if answered == id => break answer,
                    _ => {}
                }
            };
            if answer
                .slices
                .first()
                .is_some_and(|slice| slice.allowance > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Two syncs with nothing used: the first still counts the ask as use.
        let mut given = Vec::new();
        for _ in 0..2 {
            let used = vec![Used {
                key: key.clone(),
                contract,
                count: 0,
            }];
            hub.to.send(ToHub::Used { worker: 0, used }).unwrap();
            given = loop {
                if let FromHub::Slices(slices) = within(answered.recv()).await.unwrap() {
                    break slices;
                }
            };
        }
        assert_eq!(!given.is_empty(), kept, "keep {keep}: {given:?}");
    }
}

/// Layered (12 item 3), two workers of one data plane share a client's bucket through
/// their process: a client with a connection to each is held to the one bucket, refusals
/// say when to come back, and the process's keys are counted once.
#[tokio::test]
async fn layered_workers_share_a_clients_bucket() {
    two_workers_share_a_clients_bucket(|service| service.layered = Some(true)).await;
}

/// Shared (12 item 3), the same through each key's one room.
#[tokio::test]
async fn shared_workers_share_a_clients_bucket() {
    two_workers_share_a_clients_bucket(|service| service.shared = Some(true)).await;
}

/// Pooled (the shared-atomic spike), the same through each key's one shared entry.
#[tokio::test]
async fn pooled_workers_share_a_clients_bucket() {
    two_workers_share_a_clients_bucket(|service| service.pooled = Some(true)).await;
}

/// Asks that reach the process together go to the service together: a burst of workers'
/// asks about cold keys is sent in a few exchanges, not one for each ask, which under many
/// cold keys would hold an exchange in flight for every one.
#[tokio::test]
async fn asks_that_come_together_are_sent_together() {
    use crate::ratelimit::{FromHub, Hub, ToHub};
    use edgerush_ratelimit::service::Contract;
    use std::num::NonZeroU64;

    let exchanges = Arc::new(AtomicUsize::new(0));
    let service = counted_service(&exchanges).await;
    let contract = Contract {
        limit: NonZeroU64::new(10).unwrap(),
        window: NonZeroU64::new(3_600_000_000_000).unwrap(),
        burst: NonZeroU64::new(10).unwrap(),
    };
    let compiled = edgerush_config::CompiledRateLimitService {
        pods: service,
        // A cycle longer than the test, so that every exchange is an ask's.
        cycle: Duration::from_secs(60),
        max_key_bytes: 1 << 20,
        layered: false,
        layered_keep: Duration::ZERO,
        shared: false,
        pooled: true,
        poll_quiet_keys: true,
        ask_cold_keys: true,
    };
    let hub = Hub::start(&compiled, 1, 1 << 20).unwrap();
    let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
    hub.to.send(ToHub::Join { worker: 0, answers }).unwrap();
    let before = exchanges.load(Ordering::Relaxed);
    const ASKS: u64 = 500;
    for id in 0..ASKS {
        let key: Box<[u8]> = format!("cold-{id}").into_bytes().into();
        hub.to
            .send(ToHub::Ask {
                worker: 0,
                id,
                keys: vec![(key, contract)],
            })
            .unwrap();
    }
    for _ in 0..ASKS {
        assert!(matches!(
            within(answered.recv()).await.unwrap(),
            FromHub::Pooled { admitted: true, .. }
        ));
    }
    let sent = exchanges.load(Ordering::Relaxed) - before;
    assert!(sent < 50, "{sent} exchanges for {ASKS} asks");
    // The process thread counts what it took and answered.
    let stats = hub.render_stats();
    assert!(
        stats.contains(
            "edgerush_rate_limit_hub_asks_total 500
"
        ),
        "{stats}"
    );
    assert!(
        stats.contains(
            "edgerush_rate_limit_hub_asks_answered_total 500
"
        ),
        "{stats}"
    );
}

/// Pooled, a request short only by another request's hold on its key is deferred rather
/// than refused, and decided once the hold settles; with as many deferred as may be, it is
/// the overload answer at once. No service pod answers: asks are given up after their time
/// and keys decided from their copies.
#[tokio::test]
async fn a_pooled_request_short_only_by_a_hold_waits_for_it() {
    use crate::ratelimit::{Hub, Limited, Limiter, MOST_DEFERRED};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = limited(
                everything_config("127.0.0.1:9".parse().unwrap()),
                &["127.0.0.1:9".to_owned()],
            );
            config.routes[0].rules[0].rate_limits = vec!["x".to_owned(), "y".to_owned()];
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:
  - { name: x, key: client, model: { bucket: { rate: 1/h, burst: 2 } } }
  - { name: y, key: client, model: { bucket: { rate: 100/h, burst: 100 } } }
",
                )
                .unwrap(),
            );
            if let Some(service) = &mut config.rate_limit_service {
                service.pooled = Some(true);
            }
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let hub = Hub::start(&service, 1, 1 << 20).unwrap();
            let limiter = Limiter::pooled(&service, hub, 0).unwrap();
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = crate::forwarding::Client::new("203.0.113.7".parse().unwrap());
            let decided = |limited: Limited| async move {
                match limited {
                    Limited::Admit => Some(true),
                    Limited::Refuse => Some(false),
                    Limited::Later(later) => later.await.ok().flatten().map(|d| d.admitted),
                    Limited::Wait(_) | Limited::Overloaded => None,
                }
            };
            // X alone, asked about and given up: decided from its copy, one of its two taken.
            let first = limiter.decide_all(&rule.limits, 0b01, &client);
            assert_eq!(within(decided(first)).await, Some(true));
            // X and Y: X's last token held while Y, cold, is asked about.
            let both = limiter.decide_all(&rule.limits, 0b11, &client);
            assert!(matches!(both, Limited::Later(_)));
            tokio::time::sleep(Duration::from_millis(50)).await;
            // X alone again, short only by that hold: with none to be deferred, overloaded.
            limiter.set_most_deferred(0);
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b01, &client),
                Limited::Overloaded
            ));
            limiter.set_most_deferred(MOST_DEFERRED);
            // Deferred, and decided once the hold settles: X and Y admitted, X is spent.
            let Limited::Later(alone) = limiter.decide_all(&rule.limits, 0b01, &client) else {
                panic!("not deferred");
            };
            assert_eq!(within(decided(both)).await, Some(true));
            let alone = within(alone).await.unwrap();
            assert_eq!(
                alone.map(|d| d.admitted),
                Some(false),
                "decided, not overloaded"
            );
            limiter.stop();
        })
        .await;
}

/// Pooled with cold keys not asked about: a new key is decided at once from a full bucket,
/// no service pod being there to answer, and the next request from the worker's own
/// handle; with them asked about, the first waits on the ask.
#[tokio::test]
async fn a_cold_key_decided_locally_does_not_wait_for_the_service() {
    use crate::ratelimit::{Hub, Limited, Limiter};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for ask_cold in [false, true] {
                let mut config = limited(
                    everything_config("127.0.0.1:9".parse().unwrap()),
                    &["127.0.0.1:9".to_owned()],
                );
                if let Some(service) = &mut config.rate_limit_service {
                    service.pooled = Some(true);
                    service.ask_cold_keys = Some(ask_cold);
                }
                let compiled = compile(&config).unwrap();
                let service = compiled.rate_limit_service().unwrap().clone();
                let hub = Hub::start(&service, 1, 1 << 20).unwrap();
                let limiter = Limiter::pooled(&service, hub, 0).unwrap();
                let rule = Arc::clone(&compiled.rules_of(0)[0]);
                let client = crate::forwarding::Client::new("203.0.113.9".parse().unwrap());
                let started = Instant::now();
                let Limited::Later(first) = limiter.decide_all(&rule.limits, 0b1, &client) else {
                    panic!("the first goes to the process");
                };
                let first = within(first).await.unwrap().unwrap();
                assert!(first.admitted);
                let took = started.elapsed();
                if ask_cold {
                    assert!(
                        took >= Duration::from_millis(200),
                        "waited on the ask: {took:?}"
                    );
                } else {
                    assert!(
                        took < Duration::from_millis(100),
                        "decided at once: {took:?}"
                    );
                    assert!(matches!(
                        limiter.decide_all(&rule.limits, 0b1, &client),
                        Limited::Admit
                    ));
                }
                limiter.stop();
            }
        })
        .await;
}

/// A config with a pooled service is refused where 128-bit atomics are not native, at start
/// and at a reload alike; one that is not pooled runs anywhere.
#[test]
fn a_pooled_service_needs_native_128_bit_atomics() {
    use crate::serve::plane::runs_pooled;
    let mut config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    let apart = compile(&config).unwrap();
    assert_eq!(runs_pooled(&apart, false), Ok(()));
    if let Some(service) = &mut config.rate_limit_service {
        service.pooled = Some(true);
    }
    let pooled = compile(&config).unwrap();
    assert_eq!(runs_pooled(&pooled, true), Ok(()));
    assert_eq!(runs_pooled(&pooled, false), Err(ProxyError::Pooled));
    // A burst that, with two cycles' refill, passes what a pooled key's cycle holds: refused
    // rather than held to less; one that fits runs.
    for (burst, fits) in [(65_000, true), (70_000, false)] {
        config.limits = Some(
            serde_saphyr::from_str(&format!(
                "limits:\n  - {{ name: per-client, key: client, model: {{ bucket: {{ rate: 10/h, burst: {burst} }} }} }}\n"
            ))
            .unwrap(),
        );
        let compiled = compile(&config).unwrap();
        let want = if fits {
            Ok(())
        } else {
            Err(ProxyError::PooledLimit("per-client".to_owned()))
        };
        assert_eq!(runs_pooled(&compiled, true), want, "burst {burst}");
    }
}

/// A reload that turns pooling on or off starts the process's limiter afresh; pooled, it
/// hands its workers the registry of its entries.
#[tokio::test]
async fn a_changed_pooling_starts_the_process_afresh() {
    let config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let service = |pooled| {
        let mut config = config.clone();
        if let Some(service) = &mut config.rate_limit_service {
            service.pooled = pooled;
        }
        compile(&config)
            .unwrap()
            .rate_limit_service()
            .unwrap()
            .clone()
    };
    let not = proxy.hub(&service(None)).unwrap();
    assert!(not.registry().is_none());
    let pooled = proxy.hub(&service(Some(true))).unwrap();
    assert!(!Arc::ptr_eq(&not, &pooled), "pooled now");
    assert!(pooled.registry().is_some());
    let same = proxy.hub(&service(Some(true))).unwrap();
    assert!(Arc::ptr_eq(&pooled, &same), "nothing changed");
    let again = proxy.hub(&service(Some(false))).unwrap();
    assert!(!Arc::ptr_eq(&pooled, &again), "no longer pooled");
    // A lever changed: the process's limiter afresh too.
    let mut levered = service(Some(true));
    levered.ask_cold_keys = false;
    let pooled = proxy.hub(&service(Some(true))).unwrap();
    assert!(
        !Arc::ptr_eq(&pooled, &proxy.hub(&levered).unwrap()),
        "a lever changed"
    );
}

/// Two workers of one data plane sharing as `sharing` sets the service, a client with a
/// connection to each.
async fn two_workers_share_a_clients_bucket(
    sharing: impl Fn(&mut edgerush_config::RateLimitService),
) {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let mut config = limited(everything_config(upstream), &service);
            if let Some(service) = &mut config.rate_limit_service {
                sharing(service);
            }
            let workers = NonZeroUsize::new(2).unwrap();
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), workers).unwrap());
            let limits = H1Limits {
                sweep: Duration::from_millis(20),
                ..H1Limits::default()
            };
            let mut sends = Vec::new();
            for position in 0..2 {
                let worker = Worker::made(Arc::clone(&proxy), limits, SHORT, position, None);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
                sends.push(h2_library_client(front, &::h2::client::Builder::new()).await);
            }
            let mut statuses = Vec::new();
            for turn in 0..16 {
                let send = sends.remove(0);
                let mut send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    assert_eq!(response.headers()["retry-after"], "360", "turn {turn}");
                    assert_eq!(response.headers()["ratelimit"], r#""per-client";r=0;t=360"#);
                }
                statuses.push(response.status().as_u16());
                sends.push(send);
                // The workers sync every millisecond: a few between requests, as a client's
                // come over a network.
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            assert_eq!(tally(&statuses), (10, 6), "{statuses:?}");
            let deadline = Instant::now() + Duration::from_secs(5);
            let scrape = loop {
                let scrape = proxy.metrics();
                if scrape.contains("edgerush_rate_limit_keys 1\n") || Instant::now() > deadline {
                    break scrape;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(scrape.contains("edgerush_rate_limit_keys 1\n"), "{scrape}");
        })
        .await;
}

/// A dry run never refuses: beside a limit that does, the requests that one admits all go
/// on, the dry run charged for those it has room for and counting the rest
/// `dry_run_refused`; the 429s name the limit that refused alone.
#[tokio::test]
async fn a_dry_run_counts_what_it_would_refuse_and_refuses_nothing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let mut config = limited(everything_config(upstream), &service);
            config.routes[0].rules[0].rate_limits =
                vec!["per-client".to_owned(), "trial".to_owned()];
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:
  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }
  - { name: trial, key: client, model: { bucket: { rate: 4/h, burst: 4 } }, dry_run: true }
",
                )
                .unwrap(),
            );
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for _ in 0..12 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    assert_eq!(response.headers()["ratelimit"], r#""per-client";r=0;t=360"#);
                }
                statuses.push(response.status().as_u16());
            }
            assert_eq!(tally(&statuses), (10, 2), "{statuses:?}");
            // A dry run asked about a cold key is counted when the answer comes.
            let line = |limit: &str, outcome: &str, count: u64| {
                format!("edgerush_rate_limit_decisions_total{{limit=\"{limit}\",outcome=\"{outcome}\"}} {count}\n")
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            let scrape = loop {
                let scrape = worker.proxy().metrics();
                if scrape.contains(&line("trial", "dry_run_refused", 6)) || Instant::now() > deadline {
                    break scrape;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            for expected in [
                line("per-client", "admitted", 10),
                line("per-client", "refused", 2),
                line("trial", "admitted", 4),
                line("trial", "refused", 0),
                line("trial", "dry_run_refused", 6),
            ] {
                assert!(scrape.contains(&expected), "{expected}{scrape}");
            }
            assert!(
                !scrape.contains("limit=\"per-client\",outcome=\"dry_run_refused\""),
                "{scrape}"
            );
        })
        .await;
}

/// The scrape shows what the workers' keys hold against the data plane's bound; a bound
/// too small to keep any key evicts each once its requests are over; and the bound is the
/// data plane's, shared among its workers: one and a half keys' worth on two leaves each
/// room for none.
#[tokio::test]
async fn the_keys_held_are_shown_and_kept_within_their_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            // What the one key holds, as the first case's scrape says.
            let mut one_key = 0;
            for (case, workers, held) in [(0, 1, true), (1, 1, false), (2, 2, false)] {
                let max = match case {
                    0 => None,
                    1 => Some(1),
                    _ => Some(one_key * 3 / 2),
                };
                let mut config = limited(everything_config(upstream), &service);
                if let Some(service) = &mut config.rate_limit_service {
                    service.max_key_bytes = max;
                }
                let workers = NonZeroUsize::new(workers).unwrap();
                let proxy = Proxy::new(compile(&config).unwrap(), workers).unwrap();
                // Swept often: the sweep is what says what the keys hold.
                let limits = H1Limits {
                    sweep: Duration::from_millis(20),
                    ..H1Limits::default()
                };
                let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for _ in 0..3 {
                    send = within(send.ready()).await.unwrap();
                    let request = Request::get("http://example.test/")
                        .version(Version::HTTP_2)
                        .body(())
                        .unwrap();
                    let (response, _) = send.send_request(request, true).unwrap();
                    within(response).await.unwrap();
                }
                let sample = |scrape: &str, name: &str| -> u64 {
                    scrape
                        .lines()
                        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0)
                };
                // Held: the one key, never evicted. Bounded below one key: none held, and
                // evicted at least once — a slow run may evict it between the requests and
                // make it again.
                let wanted = |scrape: &str| {
                    let keys = sample(scrape, "edgerush_rate_limit_keys");
                    let evicted = sample(scrape, "edgerush_rate_limit_keys_evicted_total");
                    if held {
                        keys == 1 && evicted == 0
                    } else {
                        keys == 0 && evicted >= 1
                    }
                };
                let deadline = Instant::now() + Duration::from_secs(5);
                let scrape = loop {
                    let scrape = worker.proxy().metrics();
                    if wanted(&scrape) || Instant::now() > deadline {
                        break scrape;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                };
                assert!(wanted(&scrape), "{scrape}");
                let bound = format!(
                    "edgerush_rate_limit_max_key_bytes {}\n",
                    max.unwrap_or(256 << 20)
                );
                assert!(scrape.contains(&bound), "{bound}{scrape}");
                if held {
                    one_key = sample(&scrape, "edgerush_rate_limit_key_bytes");
                    assert!(one_key > 0, "{scrape}");
                }
            }
        })
        .await;
}

/// What a reload does to a client's spent bucket, by what it changed (06 §2): an edit
/// elsewhere, or of what the keys may hold, keeps it — the next request is still refused;
/// a change to the limit itself, its rate, its condition or its name, starts it afresh.
#[tokio::test]
async fn a_reload_keeps_a_limit_as_it_was_and_starts_a_changed_one_afresh() {
    let limits = |yaml: &str| -> edgerush_config::Limits { serde_saphyr::from_str(yaml).unwrap() };
    const PER_CLIENT: &str = "limits:
  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }
";
    // What a reload changed, and the status of the next request after it.
    type Change = fn(&mut Config);
    let cases: [(&str, Change, u16); 5] = [
        (
            "a limit added that no rule names",
            |config| {
                config.limits = Some(serde_saphyr::from_str(&format!(
                "{PER_CLIENT}  - {{ name: other, key: client, model: {{ bucket: {{ rate: 1/s, burst: 1 }} }} }}\n"
            )).unwrap());
            },
            429,
        ),
        (
            "what the keys may hold",
            |config| {
                if let Some(service) = &mut config.rate_limit_service {
                    service.max_key_bytes = Some(128 << 20);
                }
            },
            429,
        ),
        (
            "its rate",
            |config| {
                config.limits =
                    Some(serde_saphyr::from_str(&PER_CLIENT.replace("10/h", "20/h")).unwrap());
            },
            200,
        ),
        (
            "its condition",
            |config| {
                config.limits = Some(
                    serde_saphyr::from_str(
                        &PER_CLIENT
                            .replace("key: client,", "match: { methods: [GET] }, key: client,"),
                    )
                    .unwrap(),
                );
            },
            200,
        ),
        (
            "its name",
            |config| {
                config.limits = Some(
                    serde_saphyr::from_str(&PER_CLIENT.replace("per-client", "per-client-2"))
                        .unwrap(),
                );
                config.routes[0].rules[0].rate_limits = vec!["per-client-2".to_owned()];
            },
            200,
        ),
    ];
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            for (changed, change, after) in cases {
                // Pods of its own: another case's spent bucket is held there for the client.
                let service = service().await;
                let mut config = limited(everything_config(upstream), &service);
                config.limits = Some(limits(PER_CLIENT));
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let ask = async |send: ::h2::client::SendRequest<bytes::Bytes>| {
                    let mut send = within(send.ready()).await.unwrap();
                    let request = Request::get("http://example.test/")
                        .version(Version::HTTP_2)
                        .body(())
                        .unwrap();
                    let (response, _) = send.send_request(request, true).unwrap();
                    (send, within(response).await.unwrap().status().as_u16())
                };
                // The bucket spent: requests until one is refused. A loaded machine may have
                // an ask given up and its request counted twice, refusing one within the ten
                // (06 §4); what is under test is the spent bucket, not how it was spent.
                let mut statuses = Vec::new();
                while !statuses.contains(&429) && statuses.len() < 20 {
                    let status;
                    (send, status) = ask(send).await;
                    statuses.push(status);
                }
                let (admitted, refused) = tally(&statuses);
                assert!(
                    (9..=10).contains(&admitted) && refused == 1,
                    "{changed}: {statuses:?}"
                );
                let mut reloaded = config.clone();
                change(&mut reloaded);
                worker.proxy().reload(compile(&reloaded).unwrap()).unwrap();
                let (_, status) = ask(send).await;
                assert_eq!(status, after, "after a reload that changed {changed}");
            }
        })
        .await;
}

/// Each limit counts what it decided: a request admitted under every limit it was under, a
/// refusal under the limits that were out of room alone, gRPC calls too. A config with no
/// limit shows none.
#[tokio::test]
async fn each_limit_counts_what_it_admitted_and_refused() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let quiet = everything_config(upstream);
            let proxy = Proxy::new(compile(&quiet).unwrap(), NonZeroUsize::MIN).unwrap();
            assert!(!proxy.metrics().contains("edgerush_rate_limit"));
            let mut config = limited(everything_config(upstream), &service);
            config.routes[0].rules[0].rate_limits =
                vec!["per-client".to_owned(), "wide".to_owned()];
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:
  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }
  - { name: wide, key: client, model: { bucket: { rate: 100/h, burst: 100 } } }
  - { name: unused, key: client, model: { bucket: { rate: 1/h, burst: 1 } } }
",
                )
                .unwrap(),
            );
            let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            for _ in 0..12 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                within(response).await.unwrap();
            }
            send = within(send.ready()).await.unwrap();
            let (answer, _) = send.send_request(grpc_call("/", None), true).unwrap();
            assert_eq!(within(answer).await.unwrap().headers()["grpc-status"], "8");
            let scrape = worker.proxy().metrics();
            for line in [
                "edgerush_rate_limit_decisions_total{limit=\"per-client\",outcome=\"admitted\"} 10
",
                "edgerush_rate_limit_decisions_total{limit=\"per-client\",outcome=\"refused\"} 3
",
                "edgerush_rate_limit_decisions_total{limit=\"wide\",outcome=\"admitted\"} 10
",
                "edgerush_rate_limit_decisions_total{limit=\"wide\",outcome=\"refused\"} 0
",
                "edgerush_rate_limit_decisions_total{limit=\"unused\",outcome=\"admitted\"} 0
",
            ] {
                assert!(scrape.contains(line), "{line}{scrape}");
            }
        })
        .await;
}

/// Fifteen requests over HTTP/3 from one client, on one connection: ten admitted, the
/// rest refused 429.
#[tokio::test]
async fn http3_requests_are_held_to_their_limit() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client as H3Client;
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let config = limited(h3_config(upstream, http3), &service);
            let (front, _) = serving_h3(&config).await;
            let mut client = H3Client::connect(front, "a.test").await;
            let mut statuses = Vec::new();
            for _ in 0..15 {
                let answer = client.get("a.test", "/").await;
                let status = answer.final_status().and_then(|s| s.parse().ok());
                statuses.push(status.unwrap_or(0));
            }
            assert_eq!(tally(&statuses), (10, 5), "{statuses:?}");
        })
        .await;
}

/// A refusal a service pod decided is put down to the limits the pod said refused it, by
/// their keys' positions, whatever the worker's copies show: here they know nothing of
/// either key.
#[tokio::test]
async fn a_refusal_is_put_down_to_the_limits_the_pod_named() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut config = limited(
                everything_config("127.0.0.1:9".parse().unwrap()),
                &["127.0.0.1:9".to_owned()],
            );
            config.routes[0].rules[0].rate_limits =
                vec!["per-client".to_owned(), "wide".to_owned()];
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:
  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }
  - { name: wide, key: client, model: { bucket: { rate: 100/h, burst: 100 } } }
",
                )
                .unwrap(),
            );
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let limiter = crate::ratelimit::Limiter::start(&service, 0, u64::MAX).unwrap();
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = crate::forwarding::Client::new("203.0.113.7".parse().unwrap());
            let told = [(1, 5_000_000_000)];
            let refusal = limiter.refusal(&rule.limits, 0b11, &client, &told);
            assert_eq!(refusal.by, 0b10, "wide, the second");
            let (limit, wait) = refusal.longest.unwrap();
            assert_eq!(limit.name, "wide");
            assert_eq!(wait, Duration::from_secs(5));
            let unsaid = limiter.refusal(&rule.limits, 0b11, &client, &[]);
            assert_eq!(unsaid.by, 0, "the copies know nothing");
            limiter.stop();
        })
        .await;
}
