//! The rate limits route rules are under, over HTTP/2 and HTTP/3 as over HTTP/1.1
//! (`tests/ratelimit.rs`): the request core decides them whatever a request came by, with
//! three service pods on real sockets, and refuses what is over with 429.

use super::*;
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client as PodClient, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use tonic::transport::server::TcpIncoming;

/// Three service pods, once they agree all three serve: their addresses.
async fn service() -> Vec<String> {
    counted_service(&Arc::new(AtomicUsize::new(0))).await
}

/// The same, each exchange any of its pods is sent counted in `exchanges`.
async fn counted_service(exchanges: &Arc<AtomicUsize>) -> Vec<String> {
    held_service(exchanges, &Arc::new(Mutex::new(None))).await
}

/// The same, the answer to every exchange that reaches a pod before `hold` held back for
/// [`HELD`].
async fn held_service(
    exchanges: &Arc<AtomicUsize>,
    hold: &Arc<Mutex<Option<Instant>>>,
) -> Vec<String> {
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
            hold: Arc::clone(hold),
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

/// How long an exchange's answer is held back.
const HELD: Duration = Duration::from_millis(600);

/// A service pod's server that counts the exchanges it is sent, and holds back the answers
/// to those that reach it before its `hold`.
#[derive(Clone)]
struct Counted {
    server: Server,
    exchanges: Arc<AtomicUsize>,
    hold: Arc<Mutex<Option<Instant>>>,
}

impl tonic::server::NamedService for Counted {
    const NAME: &'static str = <Server as tonic::server::NamedService>::NAME;
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Counted {
    type Response = <Server as tower_service::Service<http::Request<tonic::body::Body>>>::Response;
    type Error = <Server as tower_service::Service<http::Request<tonic::body::Body>>>::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.server.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let exchange = request.uri().path().ends_with("/Exchange");
        if exchange {
            self.exchanges.fetch_add(1, Ordering::Relaxed);
        }
        let held = exchange
            && self
                .hold
                .lock()
                .unwrap()
                .is_some_and(|until| Instant::now() < until);
        let answer = self.server.call(request);
        Box::pin(async move {
            let answer = answer.await;
            if held {
                tokio::time::sleep(HELD).await;
            }
            answer
        })
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
    config.rate_limiting = Some(edgerush_config::RateLimiting {
        mode: edgerush_config::RateLimitMode::Asynchronous,
        max_key_bytes: None,
        synchronous: None,
        experimental_header_keys: None,
        service: Some(edgerush_config::RateLimitService {
            addresses: service.to_vec(),
            cycle_ms: Some(10),
            poll_quiet_keys: None,
            ask_cold_keys: None,
            schedule_keys: None,
        }),
    });
    config
}

/// The rate-limiting service `config` names, if it names one.
fn service_of(config: &mut Config) -> Option<&mut edgerush_config::RateLimitService> {
    config
        .rate_limiting
        .as_mut()
        .and_then(|limiting| limiting.service.as_mut())
}

/// A refusal under ten an hour says to come back in six minutes — or a second more, a key's
/// wait being padded so that it is never short and rounded up to whole seconds — and the
/// `RateLimit` field says the same.
fn comes_back(headers: &http::HeaderMap) {
    let after = headers["retry-after"].to_str().unwrap();
    assert!(after == "360" || after == "361", "retry-after {after}");
    assert_eq!(
        headers["ratelimit"],
        format!(r#""per-client";r=0;t={after}"#).as_str()
    );
}

/// A data plane for `config` on `workers`, its rate limits started as the binary starts
/// them.
fn started(config: &Config, workers: NonZeroUsize) -> Proxy {
    let proxy = Proxy::new(compile(config).unwrap(), workers).unwrap();
    proxy.start_rate_limits(|| {}).unwrap();
    proxy
}

/// A request's client at `address`, no trusted proxy's.
fn requester(address: &str) -> crate::forwarding::Requester {
    crate::forwarding::Requester {
        address: address.parse().unwrap(),
        peer: None,
    }
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
            let proxy = started(&config, NonZeroUsize::MIN);
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
                    comes_back(response.headers());
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

/// Behind a trusted proxy each of its clients is held to a bucket of its own, the client
/// being whom its `X-Forwarded-For` names, as forwarding works it out (03 §11, 06 §2): the
/// eleventh request of each is refused. From a peer that is no trusted proxy the field is
/// not believed, and every request counts as the peer's. Over HTTP/1.1 and HTTP/2, the
/// clients' requests coming on one connection.
#[tokio::test]
async fn a_trusted_proxys_clients_are_each_held_to_their_own_limit() {
    let mut forwarded_for = vec!["198.51.100.1"; 11];
    forwarded_for.extend(["2001:db8::1"; 11]);
    let mut each_their_own = vec![200; 22];
    each_their_own[10] = 429;
    each_their_own[21] = 429;
    let mut all_the_peers = vec![429; 22];
    all_the_peers[..10].fill(200);
    for version in [Version::HTTP_11, Version::HTTP_2] {
        assert_eq!(
            statuses_from(version, &["127.0.0.0/8"], &forwarded_for).await,
            each_their_own,
            "trusted, {version:?}"
        );
        assert_eq!(
            statuses_from(version, &[], &forwarded_for).await,
            all_the_peers,
            "not trusted, {version:?}"
        );
    }
}

/// The statuses of requests from 127.0.0.1, one for each of `forwarded_for`, which each
/// says as its `X-Forwarded-For`, all on one connection of `version`, to a listener trusting
/// `trusted` under ten requests an hour for each client.
async fn statuses_from(version: Version, trusted: &[&str], forwarded_for: &[&str]) -> Vec<u16> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let config = limited(forwarding_config(upstream, trusted), &service);
            let proxy = started(&config, NonZeroUsize::MIN);
            let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut statuses = Vec::new();
            if version == Version::HTTP_2 {
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for client in forwarded_for {
                    send = within(send.ready()).await.unwrap();
                    let request = Request::get("http://example.test/")
                        .version(Version::HTTP_2)
                        .header("x-forwarded-for", *client)
                        .body(())
                        .unwrap();
                    let (response, _) = send.send_request(request, true).unwrap();
                    statuses.push(within(response).await.unwrap().status().as_u16());
                }
            } else {
                let stream = TcpStream::connect(front).await.unwrap();
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream))
                        .await
                        .unwrap();
                let _driving = tokio::task::spawn_local(async move {
                    let _closed = connection.await;
                });
                for client in forwarded_for {
                    within(sender.ready()).await.unwrap();
                    let request = Request::get("/")
                        .header("host", "example.test")
                        .header("x-forwarded-for", *client)
                        .body(Empty::<Bytes>::new())
                        .unwrap();
                    let response = within(sender.send_request(request)).await.unwrap();
                    statuses.push(response.status().as_u16());
                    // Read to its end, so that the connection is free for the next.
                    let _body = within(response.into_body().collect()).await.unwrap();
                }
            }
            statuses
        })
        .await
}

/// Two workers of one data plane share a client's bucket through each key's one entry: a
/// client with a connection to each is held to the one bucket, refusals say when to come
/// back, and the process's keys are counted once — with the levers left out, and with the
/// protocol's (quiet keys polled, cold keys asked about, every key visited).
#[tokio::test]
async fn workers_share_a_clients_bucket() {
    two_workers_share_a_clients_bucket(|_| {}).await;
    two_workers_share_a_clients_bucket(|service| {
        service.poll_quiet_keys = Some(true);
        service.ask_cold_keys = Some(true);
        service.schedule_keys = Some(false);
    })
    .await;
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
        poll_quiet_keys: true,
        ask_cold_keys: true,
        schedule_keys: false,
    };
    let hub = Hub::start(&compiled, 1, 1 << 20, None).unwrap();
    let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
    hub.to.send(ToHub::Join { worker: 0, answers }).unwrap();
    let before = exchanges.load(Ordering::Relaxed);
    const ASKS: u64 = 500;
    // Each decision waited for, as a request there still waits for its own.
    let mut waiting = Vec::new();
    for id in 0..ASKS {
        let key: Box<[u8]> = format!("cold-{id}").into_bytes().into();
        let (decided, waits) = tokio::sync::oneshot::channel();
        waiting.push(waits);
        hub.to
            .send(ToHub::Ask {
                worker: 0,
                id,
                keys: vec![(key, contract)],
                until: u64::MAX,
                decided,
            })
            .unwrap();
    }
    for _ in 0..ASKS {
        assert!(matches!(
            within(answered.recv()).await.unwrap(),
            FromHub { admitted: true, .. }
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
/// than refused, and decided once the hold settles; with no place left to wait in, it is
/// answered full at once. No service pod answers: asks are given up after their time
/// and keys decided from their copies.
#[tokio::test]
async fn a_pooled_request_short_only_by_a_hold_waits_for_it() {
    use crate::ratelimit::{Hub, Limited, Limiter, MOST_WAITING, Undecided};
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
            if let Some(service) = service_of(&mut config) {
                // Cold keys asked about: a hold waits on an ask.
                service.ask_cold_keys = Some(true);
            }
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let hub = Hub::start(&service, 1, 1 << 20, None).unwrap();
            let limiter = Limiter::new(&service, hub, 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = requester("203.0.113.7");
            let decided = |limited: Limited| async move {
                match limited {
                    Limited::Admit => Some(true),
                    Limited::Refuse(_) => Some(false),
                    Limited::Later(later) => later.decided().await.ok().map(|d| d.admitted),
                    Limited::Undecided(_) => None,
                }
            };
            // X alone, asked about and given up: decided from its copy, one of its two taken.
            let first = limiter.decide_all(&rule.limits, 0b01, client);
            assert_eq!(within(decided(first)).await, Some(true));
            // X and Y: X's last token held while Y, cold, is asked about.
            let both = limiter.decide_all(&rule.limits, 0b11, client);
            assert!(matches!(both, Limited::Later(_)));
            tokio::time::sleep(Duration::from_millis(50)).await;
            // X alone again, short only by that hold: with one place, which the waiting ask
            // holds, none to be deferred in: full.
            limiter.set_most_waiting(1);
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b01, client),
                Limited::Undecided(Undecided::Full)
            ));
            limiter.set_most_waiting(MOST_WAITING);
            // Deferred, and decided once the hold settles: X and Y admitted, X is spent.
            let Limited::Later(alone) = limiter.decide_all(&rule.limits, 0b01, client) else {
                panic!("not deferred");
            };
            assert_eq!(within(decided(both)).await, Some(true));
            let alone = within(alone.decided()).await;
            assert_eq!(
                alone.map(|d| d.admitted),
                Ok(false),
                "decided, not undecided"
            );
            limiter.stop();
        })
        .await;
}

/// Pooled with cold keys not asked about: a new key is decided at once, by the worker,
/// from an entry it makes with a full bucket, no service pod being there to answer; with
/// them asked about, the first waits on the ask.
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
                if let Some(service) = service_of(&mut config) {
                    service.ask_cold_keys = Some(ask_cold);
                }
                let compiled = compile(&config).unwrap();
                let service = compiled.rate_limit_service().unwrap().clone();
                let hub = Hub::start(&service, 1, 1 << 20, None).unwrap();
                let limiter = Limiter::new(&service, hub, 0);
                let rule = Arc::clone(&compiled.rules_of(0)[0]);
                let client = requester("203.0.113.9");
                let started = Instant::now();
                let first = limiter.decide_all(&rule.limits, 0b1, client);
                if ask_cold {
                    let Limited::Later(first) = first else {
                        panic!("the first goes to the process");
                    };
                    assert!(within(first.decided()).await.unwrap().admitted);
                    let took = started.elapsed();
                    assert!(
                        took >= Duration::from_millis(200),
                        "waited on the ask: {took:?}"
                    );
                } else {
                    assert!(
                        matches!(first, Limited::Admit),
                        "decided by the worker at once"
                    );
                    assert!(matches!(
                        limiter.decide_all(&rule.limits, 0b1, client),
                        Limited::Admit
                    ));
                    // The process keeps the worker's entry, to report what it admitted.
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while limiter.usage().1 != 1 {
                        assert!(Instant::now() < deadline, "the process never kept the key");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                limiter.stop();
            }
        })
        .await;
}

/// Pooled, keys visited only when they need it: a key the process has reported and heard
/// back about, nothing polled, is left alone; a worker's next admission under it signals
/// it, and that reaches the process, which reports it.
#[tokio::test]
async fn scheduled_a_workers_admission_reaches_the_service() {
    use crate::ratelimit::{Hub, Limited, Limiter};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let exchanges = Arc::new(AtomicUsize::new(0));
            let service = counted_service(&exchanges).await;
            let mut config = limited(everything_config("127.0.0.1:9".parse().unwrap()), &service);
            if let Some(service) = service_of(&mut config) {
                service.poll_quiet_keys = Some(false);
                service.ask_cold_keys = Some(false);
                service.schedule_keys = Some(true);
            }
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let hub = Hub::start(&service, 1, 1 << 20, None).unwrap();
            let limiter = Limiter::new(&service, Arc::clone(&hub), 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = requester("203.0.113.9");
            let settle = || async {
                // Quiet once a few cycles pass with no exchange.
                let mut last = exchanges.load(Ordering::Relaxed);
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let now = exchanges.load(Ordering::Relaxed);
                    if now == last {
                        return now;
                    }
                    last = now;
                }
            };
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b1, client),
                Limited::Admit
            ));
            let quiet = within(settle()).await;
            assert!(quiet > 0, "the new key reported");
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b1, client),
                Limited::Admit
            ));
            let deadline = Instant::now() + Duration::from_secs(2);
            while exchanges.load(Ordering::Relaxed) == quiet {
                assert!(Instant::now() < deadline, "the admission never reported");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let stats = hub.render_stats();
            for line in [
                "edgerush_rate_limit_hub_visits_total ",
                "edgerush_rate_limit_hub_keys_left_out_total 0\n",
                "edgerush_rate_limit_hub_batches_too_long_total 0\n",
            ] {
                assert!(stats.contains(line), "{line}{stats}");
            }
            limiter.stop();
        })
        .await;
}

/// Pooled, keys visited only when they need it: a signal a worker has no room to keep
/// asks for every key to be visited instead, and gives its key back, so that the key's
/// next signal is sent — not taken as carried by the one that never went.
#[tokio::test]
async fn scheduled_a_signal_with_no_room_leaves_the_next_free_to_go() {
    use crate::ratelimit::{Hub, Limited, Limiter, MOST_SIGNALLED};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let exchanges = Arc::new(AtomicUsize::new(0));
            let service = counted_service(&exchanges).await;
            let mut config = limited(everything_config("127.0.0.1:9".parse().unwrap()), &service);
            if let Some(service) = service_of(&mut config) {
                service.poll_quiet_keys = Some(false);
                service.ask_cold_keys = Some(false);
                service.schedule_keys = Some(true);
            }
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let hub = Hub::start(&service, 1, 1 << 20, None).unwrap();
            let limiter = Limiter::new(&service, Arc::clone(&hub), 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = requester("203.0.113.9");
            let settle = || async {
                // Quiet once a few cycles pass with no exchange.
                let mut last = exchanges.load(Ordering::Relaxed);
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let now = exchanges.load(Ordering::Relaxed);
                    if now == last {
                        return now;
                    }
                    last = now;
                }
            };
            // The new key's signal finds no room: every key visited, the key reported.
            limiter.set_most_signalled(0);
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b1, client),
                Limited::Admit
            ));
            let quiet = within(settle()).await;
            assert!(quiet > 0, "the new key reported");
            limiter.set_most_signalled(MOST_SIGNALLED);
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b1, client),
                Limited::Admit
            ));
            let deadline = Instant::now() + Duration::from_secs(2);
            while exchanges.load(Ordering::Relaxed) == quiet {
                assert!(Instant::now() < deadline, "the next signal never went");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            limiter.stop();
        })
        .await;
}

/// An exchange answered after the one that replaced it: its pod handled it first, so its
/// bucket is the older, and it is not taken over the newer one's. A client that spent its
/// bucket while the first exchange's answer was held back stays refused once that answer
/// comes, and the late answer is counted.
#[tokio::test]
async fn a_late_answer_does_not_give_back_a_spent_bucket() {
    use crate::ratelimit::{Hub, Limited, Limiter};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let hold = Arc::new(Mutex::new(None));
            let service = held_service(&Arc::new(AtomicUsize::new(0)), &hold).await;
            let config = limited(everything_config("127.0.0.1:9".parse().unwrap()), &service);
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let hub = Hub::start(&service, 1, 1 << 20, None).unwrap();
            let limiter = Limiter::new(&service, Arc::clone(&hub), 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = requester("203.0.113.9");
            let admitted = || {
                (0..5)
                    .map(|_| limiter.decide_all(&rule.limits, 0b1, client))
                    .filter(|limited| matches!(limited, Limited::Admit))
                    .count()
            };
            // The exchange reporting the first five is answered late.
            let started = Instant::now();
            *hold.lock().unwrap() = Some(started + Duration::from_millis(100));
            assert_eq!(admitted(), 5);
            // Five more from the copy while it is out, the bucket spent. Given up after
            // 300 ms, the next exchange carries all ten, and its answer says so.
            tokio::time::sleep_until(started + Duration::from_millis(150)).await;
            assert_eq!(admitted(), 5);
            // The held answer, a bucket with five left, has come.
            tokio::time::sleep_until(started + HELD + Duration::from_millis(300)).await;
            assert_eq!(admitted(), 0, "the late answer's bucket taken");
            let stats = hub.render_stats();
            let late = stats
                .lines()
                .find_map(|line| line.strip_prefix("edgerush_rate_limit_hub_answers_late_total "))
                .and_then(|n| n.parse::<u64>().ok());
            assert!(late.is_some_and(|late| late > 0), "{stats}");
            limiter.stop();
        })
        .await;
}

/// A config with rate limits is refused where 128-bit atomics are not native, at start and
/// at a reload alike; one without runs anywhere.
#[test]
fn rate_limits_need_native_128_bit_atomics() {
    use crate::serve::plane::runs_rate_limits;
    let unlimited = compile(&everything_config("127.0.0.1:1".parse().unwrap())).unwrap();
    assert_eq!(runs_rate_limits(&unlimited, false), Ok(()));
    let mut config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    let limited = compile(&config).unwrap();
    assert_eq!(runs_rate_limits(&limited, true), Ok(()));
    assert_eq!(
        runs_rate_limits(&limited, false),
        Err(ProxyError::RateLimitsUnsupported)
    );
    // A burst that, with two cycles' refill, passes what a key's cycle holds: refused rather
    // than held to less; one that fits runs.
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
            Err(ProxyError::LimitTooLarge("per-client".to_owned()))
        };
        assert_eq!(runs_rate_limits(&compiled, true), want, "burst {burst}");
    }
}

/// The process's rate-limiting thread the running config's limits are decided with.
fn hub_of(proxy: &Proxy) -> Arc<crate::ratelimit::Hub> {
    proxy.hub().1.expect("a thread")
}

/// `config` without its limits or its rate-limiting service.
fn unlimited(mut config: Config) -> Config {
    config.routes[0].rules[0].rate_limits.clear();
    config.limits = None;
    config.rate_limiting = None;
    config
}

/// A reload that changes a lever, the cycle or the pods starts the process's thread
/// afresh; one that changes nothing, or only what its keys may hold, keeps it; one with no
/// rate-limiting service lets it go, and one with a service again starts one.
#[tokio::test]
async fn a_changed_service_starts_the_process_afresh() {
    let config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    let proxy = started(&config, NonZeroUsize::MIN);
    let first = hub_of(&proxy);
    proxy.reload(compile(&config).unwrap()).unwrap();
    assert!(Arc::ptr_eq(&first, &hub_of(&proxy)), "nothing changed");
    let mut bigger = config.clone();
    if let Some(limiting) = &mut bigger.rate_limiting {
        limiting.max_key_bytes = Some(1 << 30);
    }
    proxy.reload(compile(&bigger).unwrap()).unwrap();
    assert!(Arc::ptr_eq(&first, &hub_of(&proxy)), "only its keys' bytes");
    let changes: [fn(&mut edgerush_config::RateLimitService); 5] = [
        |service| service.ask_cold_keys = Some(true),
        |service| service.poll_quiet_keys = Some(true),
        |service| service.schedule_keys = Some(false),
        |service| service.cycle_ms = Some(20),
        |service| service.addresses.push("127.0.0.1:2".to_owned()),
    ];
    for (at, change) in changes.into_iter().enumerate() {
        let current = hub_of(&proxy);
        let mut changed = config.clone();
        if let Some(service) = service_of(&mut changed) {
            change(service);
        }
        proxy.reload(compile(&changed).unwrap()).unwrap();
        assert!(!Arc::ptr_eq(&current, &hub_of(&proxy)), "change {at}");
    }
    proxy
        .reload(compile(&unlimited(config.clone())).unwrap())
        .unwrap();
    assert!(proxy.hub().1.is_none(), "no service, no thread");
    proxy.reload(compile(&config).unwrap()).unwrap();
    assert!(proxy.hub().1.is_some(), "a service again");
}

/// Rate limits whose thread cannot be started refuse the config: at start, which the data
/// plane then cannot run, and on a reload, after which it runs on as it was, the thread
/// it had deciding its limits still, and the destinations it has kept. A reload before they
/// are started starts none; starting them starts the thread of the config running then,
/// once.
#[tokio::test]
async fn rate_limits_that_cannot_start_refuse_the_config() {
    use crate::ratelimit::REFUSE_START;

    let config = limited(
        everything_config("127.0.0.1:1".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let mut changed = config.clone();
    if let Some(service) = service_of(&mut changed) {
        service.cycle_ms = Some(20);
    }
    proxy.reload(compile(&changed).unwrap()).unwrap();
    assert!(proxy.hub().1.is_none(), "not started");

    REFUSE_START.with(|refuse| refuse.set(true));
    assert!(matches!(
        proxy.start_rate_limits(|| {}),
        Err(ProxyError::RateLimitThread(_))
    ));
    assert!(proxy.hub().1.is_none(), "refused");
    proxy.start_rate_limits(|| {}).unwrap();
    let running = hub_of(&proxy);
    let service = proxy.current.load().config.rate_limit_service().cloned();
    assert_eq!(
        service.map(|service| service.cycle),
        Some(Duration::from_millis(20))
    );
    let generation = proxy.current.load().generation;
    let destination = proxy.current.load().destinations.at(0, 0).cloned().unwrap();

    // Its upstream elsewhere too: a config taken in would retire where it is now.
    let moved = limited(
        everything_config("127.0.0.1:2".parse().unwrap()),
        &["127.0.0.1:1".to_owned()],
    );
    REFUSE_START.with(|refuse| refuse.set(true));
    assert!(matches!(
        proxy.reload(compile(&moved).unwrap()),
        Err(ProxyError::RateLimitThread(_))
    ));
    assert_eq!(
        proxy.current.load().generation,
        generation,
        "runs on as it was"
    );
    assert!(Arc::ptr_eq(&running, &hub_of(&proxy)), "its thread kept");
    assert!(!destination.is_retired(), "its destination kept");

    // Started already: nothing more is started, though it would be refused.
    REFUSE_START.with(|refuse| refuse.set(true));
    proxy.start_rate_limits(|| {}).unwrap();
    assert!(Arc::ptr_eq(&running, &hub_of(&proxy)));
    REFUSE_START.with(|refuse| refuse.set(false));

    proxy.reload(compile(&moved).unwrap()).unwrap();
    assert!(destination.is_retired(), "taken in, it retires");
}

/// A worker follows the thread a reload starts: its limiter is made afresh on the new one,
/// and the thread replaced, held by nothing then, ends.
#[tokio::test]
async fn a_worker_moves_to_the_thread_a_reload_starts() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let mut config = limited(everything_config(upstream), &service);
            let proxy = Arc::new(started(&config, NonZeroUsize::MIN));
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let first = hub_of(&proxy);
            let ended = first.ended();
            drop(first);
            for at in 0..2 {
                if at == 1 {
                    if let Some(service) = service_of(&mut config) {
                        service.cycle_ms = Some(20);
                    }
                    proxy.reload(compile(&config).unwrap()).unwrap();
                }
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK, "request {at}");
            }
            until(ended).await;
        })
        .await;
}

/// A request under a limit that comes before the data plane's rate limits are started is
/// refused as one they cannot decide, 503, never let through unlimited; once they are
/// started, the limit holds it.
#[tokio::test]
async fn a_limit_not_started_refuses_rather_than_admits() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let config = limited(everything_config(upstream), &service);
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for at in 0..15 {
                if at == 3 {
                    proxy.start_rate_limits(|| {}).unwrap();
                }
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                statuses.push(response.status().as_u16());
            }
            assert_eq!(statuses[..3], [503; 3], "{statuses:?}");
            assert_eq!(tally(&statuses[3..]), (10, 2), "{statuses:?}");
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains(r#"reason="rate_limit_unavailable"} 3"#),
                "{scrape}"
            );
        })
        .await;
}

/// A request that would ask once the rate-limiting thread is gone — its channel closed —
/// is answered 503 `rate_limit_unavailable`, the gateway's own trouble, never 429 as if
/// over its limit.
#[tokio::test]
async fn a_rate_limiting_thread_gone_is_unavailable_not_a_refusal() {
    use crate::ratelimit::ToHub;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let mut config = limited(everything_config(upstream), &["127.0.0.1:9".to_owned()]);
            if let Some(service) = service_of(&mut config) {
                // Every new client asked about.
                service.ask_cold_keys = Some(true);
            }
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let (failed, failures) = told_failures();
            proxy.start_rate_limits(failed).unwrap();
            hub_of(&proxy)
                .to
                .send(ToHub::Panic { in_exchange: false })
                .unwrap();
            until(|| failures() > 0).await;
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for _ in 0..3 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                statuses.push(response.status().as_u16());
            }
            assert_eq!(statuses, [503; 3]);
            let scrape = worker.proxy().metrics();
            assert!(
                scrape.contains(r#"reason="rate_limit_unavailable"} 3"#),
                "{scrape}"
            );
        })
        .await;
}

/// An ask still waiting when the rate-limiting thread goes is told it is unavailable, at
/// once, rather than left waiting for an answer that cannot come; one made after is too.
#[tokio::test]
async fn an_ask_waiting_when_the_thread_goes_is_told_it_is_unavailable() {
    use crate::ratelimit::{Hub, Limited, Limiter, ToHub, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let exchanges = Arc::new(AtomicUsize::new(0));
            // Every answer held back past the end of the test.
            let hold = Arc::new(Mutex::new(Some(Instant::now() + Duration::from_secs(60))));
            let pods = held_service(&exchanges, &hold).await;
            let mut config = limited(everything_config("127.0.0.1:9".parse().unwrap()), &pods);
            if let Some(service) = service_of(&mut config) {
                service.ask_cold_keys = Some(true);
            }
            let compiled = compile(&config).unwrap();
            let service = compiled.rate_limit_service().unwrap().clone();
            let (failed, failures) = told_failures();
            let failed = crate::ratelimit::Failed(Arc::new(failed));
            let hub = Hub::start(&service, 1, 1 << 20, Some(failed)).unwrap();
            let limiter = Limiter::new(&service, Arc::clone(&hub), 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let Limited::Later(waiting) =
                limiter.decide_all(&rule.limits, 0b1, requester("203.0.113.7"))
            else {
                panic!("not asked about");
            };
            until(|| exchanges.load(Ordering::Relaxed) > 0).await;
            hub.to.send(ToHub::Panic { in_exchange: false }).unwrap();
            until(|| failures() > 0).await;
            assert_eq!(within(waiting.decided()).await, Err(Undecided::Unavailable));
            assert!(matches!(
                limiter.decide_all(&rule.limits, 0b1, requester("203.0.113.8")),
                Limited::Undecided(Undecided::Unavailable)
            ));
            limiter.stop();
        })
        .await;
}

/// A limiter of one worker that decides new clients itself, no service pod answering or
/// asked, with a rule under one limit of a single token, and its process's thread.
async fn one_token_limiter() -> (
    Rc<crate::ratelimit::Limiter>,
    Arc<edgerush_config::CompiledRule>,
    Arc<crate::ratelimit::Hub>,
) {
    let mut config = limited(
        everything_config("127.0.0.1:9".parse().unwrap()),
        &["127.0.0.1:9".to_owned()],
    );
    config.routes[0].rules[0].rate_limits = vec!["one".to_owned()];
    config.limits = Some(
        serde_saphyr::from_str(
            "limits:
  - { name: one, key: client, model: { bucket: { rate: 1/h, burst: 1 } } }
",
        )
        .unwrap(),
    );
    if let Some(service) = service_of(&mut config) {
        service.ask_cold_keys = Some(false);
    }
    let compiled = compile(&config).unwrap();
    let service = compiled.rate_limit_service().unwrap().clone();
    let hub = crate::ratelimit::Hub::start(&service, 1, 1 << 20, None).unwrap();
    let limiter = crate::ratelimit::Limiter::new(&service, Arc::clone(&hub), 0);
    (limiter, Arc::clone(&compiled.rules_of(0)[0]), hub)
}

/// A worker makes a new client's key only with room for it — so many keys, and so many
/// bytes of what they hold, made and not yet taken in by its process — and asks its
/// process about the client without: here while its process is held up, the one key it
/// made still queued. The room comes back once the process takes the key in.
#[tokio::test]
async fn a_worker_makes_new_keys_only_with_room_and_asks_without() {
    use crate::ratelimit::{Limited, MOST_MADE, MOST_MADE_BYTES, ToHub};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, hub) = one_token_limiter().await;
            let decide = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let (release, stalled) = std::sync::mpsc::channel();
            hub.to.send(ToHub::Stall(stalled)).unwrap();
            limiter.set_most_made(1, MOST_MADE_BYTES);
            assert!(matches!(decide("203.0.113.1"), Limited::Admit), "made");
            let Limited::Later(asked) = decide("203.0.113.2") else {
                panic!("made with no room");
            };
            release.send(()).unwrap();
            // Its ask given up, the process makes the key itself, from a full bucket.
            assert!(within(asked.decided()).await.unwrap().admitted);
            // The first key taken in: room again.
            within(async {
                let mut client = 3;
                while !matches!(decide(&format!("203.0.113.{client}")), Limited::Admit) {
                    client += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            // Room for keys, none for their bytes.
            limiter.set_most_made(MOST_MADE, 1);
            assert!(
                matches!(decide("203.0.113.200"), Limited::Later(_)),
                "made with no room for its bytes"
            );
            limiter.stop();
        })
        .await;
}

/// A deferred request is let go before it is decided again once nothing waits for it, or
/// once its time is up, never charged for: here gone before its task first ran, and out of
/// time before it did. The key's one token is left for the next request.
#[tokio::test]
async fn a_deferred_request_gone_or_out_of_time_is_not_decided_again() {
    use crate::ratelimit::{Limited, Undecided, WAIT};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = one_token_limiter().await;
            let client = requester("203.0.113.1");
            drop(limiter.defer_regardless(&rule.limits, 0b1, client));
            tokio::time::sleep(Duration::from_millis(20)).await;
            limiter.set_wait(Duration::ZERO);
            let Limited::Later(late) = limiter.defer_regardless(&rule.limits, 0b1, client) else {
                panic!("not deferred");
            };
            assert_eq!(
                within(late.decided()).await.map(|d| d.admitted),
                Err(Undecided::Timeout)
            );
            limiter.set_wait(WAIT);
            assert!(
                matches!(
                    limiter.decide_all(&rule.limits, 0b1, client),
                    Limited::Admit
                ),
                "the token taken for a request no longer waiting"
            );
            limiter.stop();
        })
        .await;
}

/// A decision made in time but taken up once the request's time is up — its worker held up
/// meanwhile — is answered out of time: nothing goes on past its time to wait.
#[tokio::test]
async fn a_decision_taken_up_after_its_time_is_out_of_time() {
    use crate::ratelimit::{Limited, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = one_token_limiter().await;
            limiter.set_wait(Duration::from_millis(100));
            let client = requester("203.0.113.1");
            let Limited::Later(later) = limiter.defer_regardless(&rule.limits, 0b1, client) else {
                panic!("not deferred");
            };
            // Its task decides it at once; its answer is there well before its time is up.
            tokio::time::sleep(Duration::from_millis(10)).await;
            std::thread::sleep(Duration::from_millis(150));
            assert_eq!(
                later.decided().await.map(|d| d.admitted),
                Err(Undecided::Timeout)
            );
            limiter.stop();
        })
        .await;
}

/// A limiter of one worker whose every new client is asked about, of three service pods
/// that hold back every answer for longer than an ask may wait — so that each ask is given
/// up after its 300 ms and decided from the copy — with the rule it limits, and the
/// exchanges the pods were sent.
async fn asking_limiter() -> (
    Rc<crate::ratelimit::Limiter>,
    Arc<edgerush_config::CompiledRule>,
    Arc<AtomicUsize>,
) {
    let exchanges = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Mutex::new(Some(Instant::now() + Duration::from_secs(60))));
    let pods = held_service(&exchanges, &hold).await;
    let mut config = limited(everything_config("127.0.0.1:9".parse().unwrap()), &pods);
    if let Some(service) = service_of(&mut config) {
        service.ask_cold_keys = Some(true);
    }
    let compiled = compile(&config).unwrap();
    let service = compiled.rate_limit_service().unwrap().clone();
    let hub = crate::ratelimit::Hub::start(&service, 1, 1 << 20, None).unwrap();
    let limiter = crate::ratelimit::Limiter::new(&service, hub, 0);
    (limiter, Arc::clone(&compiled.rules_of(0)[0]), exchanges)
}

/// A worker has so many places for requests waiting on its rate limits, asks and deferred
/// alike: past them a request is answered full at once, never queued; a place is given back
/// once its ask is answered.
#[tokio::test]
async fn a_worker_holds_no_more_requests_waiting_than_it_has_places() {
    use crate::ratelimit::{Limited, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = asking_limiter().await;
            limiter.set_most_waiting(2);
            let ask = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let Limited::Later(first) = ask("203.0.113.1") else {
                panic!("not asked about");
            };
            let Limited::Later(second) = ask("203.0.113.2") else {
                panic!("not asked about");
            };
            assert!(matches!(
                ask("203.0.113.3"),
                Limited::Undecided(Undecided::Full)
            ));
            // Given up after their time, and decided from their copies: admitted, and their
            // places given back.
            assert!(within(first.decided()).await.unwrap().admitted);
            assert!(within(second.decided()).await.unwrap().admitted);
            assert!(matches!(ask("203.0.113.4"), Limited::Later(_)));
            limiter.stop();
        })
        .await;
}

/// A place holds the bytes its request carries — its keys and its message — as well as a
/// place: a request whose bytes do not fit is answered full at once, however many places
/// are free, and the bytes come back with the place. Dry runs' bytes are their own too.
#[tokio::test]
async fn a_place_holds_its_requests_bytes_too() {
    use crate::ratelimit::{Limited, MOST_WAITING_BYTES, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = asking_limiter().await;
            let ask = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let Limited::Later(first) = ask("203.0.113.1") else {
                panic!("not asked about");
            };
            // Room for the bytes of one such request alone.
            limiter.set_most_waiting_bytes(limiter.waiting_bytes());
            assert!(matches!(
                ask("203.0.113.2"),
                Limited::Undecided(Undecided::Full)
            ));
            limiter.set_most_dry_runs_waiting_bytes(1);
            assert!(matches!(
                limiter.decide_dry_run(&rule.limits, 0b1, requester("203.0.113.3")),
                Limited::Undecided(Undecided::Full)
            ));
            // Its ask given up and answered: its bytes given back.
            assert!(within(first.decided()).await.unwrap().admitted);
            assert!(matches!(ask("203.0.113.4"), Limited::Later(_)));
            limiter.set_most_waiting_bytes(MOST_WAITING_BYTES);
            limiter.stop();
        })
        .await;
}

/// Dry runs wait in places of their own: however many they fill, the limits that refuse
/// keep every one of theirs, and the other way about.
#[tokio::test]
async fn dry_runs_wait_in_places_of_their_own() {
    use crate::ratelimit::{Limited, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = asking_limiter().await;
            limiter.set_most_waiting(1);
            limiter.set_most_dry_runs_waiting(1);
            let ask = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let dry_run =
                |client: &str| limiter.decide_dry_run(&rule.limits, 0b1, requester(client));
            let Limited::Later(dry) = dry_run("203.0.113.1") else {
                panic!("not asked about");
            };
            assert!(matches!(
                dry_run("203.0.113.2"),
                Limited::Undecided(Undecided::Full)
            ));
            let Limited::Later(enforced) = ask("203.0.113.3") else {
                panic!("a dry run took an enforced limit's place");
            };
            assert!(matches!(
                ask("203.0.113.4"),
                Limited::Undecided(Undecided::Full)
            ));
            assert!(matches!(
                dry_run("203.0.113.5"),
                Limited::Undecided(Undecided::Full)
            ));
            assert!(within(dry.decided()).await.unwrap().admitted);
            assert!(within(enforced.decided()).await.unwrap().admitted);
            limiter.stop();
        })
        .await;
}

/// A request waits on its rate limits no longer than its time, wherever it waits — here on
/// an ask its process has out — and is then answered as out of time; its place is kept until
/// the ask is answered, though nothing waits for it any more, so that what is still queued
/// or asking is within the places too.
#[tokio::test]
async fn a_request_out_of_time_is_answered_and_its_place_kept_until_its_ask_is() {
    use crate::ratelimit::{Limited, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, _) = asking_limiter().await;
            limiter.set_most_waiting(1);
            limiter.set_wait(Duration::from_millis(50));
            let ask = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let Limited::Later(first) = ask("203.0.113.1") else {
                panic!("not asked about");
            };
            let started = Instant::now();
            assert_eq!(within(first.decided()).await, Err(Undecided::Timeout));
            let waited = started.elapsed();
            assert!(
                waited < Duration::from_millis(250),
                "waited {waited:?}, as long as its ask"
            );
            // Its ask still out: no place for another.
            assert!(matches!(
                ask("203.0.113.2"),
                Limited::Undecided(Undecided::Full)
            ));
            // Its ask given up and answered: its place given back.
            within(async {
                while matches!(ask("203.0.113.3"), Limited::Undecided(Undecided::Full)) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            limiter.stop();
        })
        .await;
}

/// A request that goes while its process's participant has its ask out is left to settle,
/// not taken back: its place is held until the ask is answered, though nothing waits for
/// it any more, so that work the participant still has is within the places too.
#[tokio::test]
async fn a_request_gone_keeps_its_place_while_its_ask_is_out() {
    use crate::ratelimit::{Limited, Undecided};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (limiter, rule, exchanges) = asking_limiter().await;
            limiter.set_most_waiting(1);
            let ask = |client: &str| limiter.decide_all(&rule.limits, 0b1, requester(client));
            let Limited::Later(first) = ask("203.0.113.1") else {
                panic!("not asked about");
            };
            until(|| exchanges.load(Ordering::Relaxed) > 0).await;
            // The request goes; its ask is out.
            drop(first);
            assert!(matches!(
                ask("203.0.113.2"),
                Limited::Undecided(Undecided::Full)
            ));
            // Its ask given up and answered: its place given back.
            within(async {
                while matches!(ask("203.0.113.3"), Limited::Undecided(Undecided::Full)) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            limiter.stop();
        })
        .await;
}

/// An ask its process takes once its time is up is answered as out of time, undecided; one
/// whose request has gone is let go, its place given back and nothing told, and counted:
/// neither is asked of a service pod, or charged.
#[tokio::test]
async fn an_ask_gone_or_out_of_time_when_taken_is_not_asked_of_the_service() {
    use crate::ratelimit::{FromHub, Hub, ToHub, Undecided};
    use edgerush_ratelimit::service::Contract;
    use std::num::NonZeroU64;

    let exchanges = Arc::new(AtomicUsize::new(0));
    let service = counted_service(&exchanges).await;
    let compiled = edgerush_config::CompiledRateLimitService {
        pods: service,
        // A cycle longer than the test, so that every exchange is an ask's.
        cycle: Duration::from_secs(60),
        max_key_bytes: 1 << 20,
        poll_quiet_keys: true,
        ask_cold_keys: true,
        schedule_keys: false,
    };
    let hub = Hub::start(&compiled, 1, 1 << 20, None).unwrap();
    let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
    hub.to.send(ToHub::Join { worker: 0, answers }).unwrap();
    let before = exchanges.load(Ordering::Relaxed);
    let ask = |id: u64, until: u64, decided| ToHub::Ask {
        worker: 0,
        id,
        keys: vec![(
            format!("cold-{id}").into_bytes().into(),
            Contract {
                limit: NonZeroU64::new(10).unwrap(),
                window: NonZeroU64::new(3_600_000_000_000).unwrap(),
                burst: NonZeroU64::new(10).unwrap(),
            },
        )],
        until,
        decided,
    };
    // Out of time, its request still waiting for it: told so.
    let (decided, waits) = tokio::sync::oneshot::channel();
    hub.to.send(ask(7, 0, decided)).unwrap();
    let FromHub {
        id, to, undecided, ..
    } = within(answered.recv()).await.unwrap();
    assert_eq!((id, undecided), (7, Some(Undecided::Timeout)));
    assert!(to.is_some(), "not told where to go");
    drop(waits);
    // Its request gone, with time left: let go, nowhere to go.
    let (decided, waits) = tokio::sync::oneshot::channel();
    drop(waits);
    hub.to.send(ask(8, u64::MAX, decided)).unwrap();
    let FromHub { id, to, .. } = within(answered.recv()).await.unwrap();
    assert_eq!(id, 8);
    assert!(to.is_none(), "a request gone was decided");
    assert!(
        hub.render_stats()
            .contains("edgerush_rate_limit_hub_asks_abandoned_total 1\n"),
        "{}",
        hub.render_stats()
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        exchanges.load(Ordering::Relaxed),
        before,
        "asked all the same"
    );
}

/// What a rate-limiting thread tells if it fails, counted.
fn told_failures() -> (
    impl Fn() + Send + Sync + 'static,
    impl Fn() -> usize + 'static,
) {
    let failures = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&failures);
    (
        move || {
            counting.fetch_add(1, Ordering::Relaxed);
        },
        move || failures.load(Ordering::Relaxed),
    )
}

/// Until `done`, looking every few milliseconds.
async fn until(done: impl Fn() -> bool) {
    within(async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// The data plane's rate-limiting thread is watched (03 §2): one that panics tells what the
/// data plane was given to call; one that a reload replaced ends without telling it, though
/// an exchange of its is still in flight.
#[tokio::test]
async fn a_rate_limiting_thread_that_panics_is_told_and_one_replaced_is_not() {
    use crate::ratelimit::ToHub;
    use edgerush_ratelimit::service::Contract;
    use std::num::NonZeroU64;

    let exchanges = Arc::new(AtomicUsize::new(0));
    // Every answer held back past the replacing.
    let hold = Arc::new(Mutex::new(Some(Instant::now() + Duration::from_secs(60))));
    let pods = held_service(&exchanges, &hold).await;
    let mut config = limited(everything_config("127.0.0.1:1".parse().unwrap()), &pods);
    if let Some(service) = service_of(&mut config) {
        // A new key is asked about: an exchange.
        service.ask_cold_keys = Some(true);
    }
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let (failed, failures) = told_failures();
    proxy.start_rate_limits(failed).unwrap();
    let first = hub_of(&proxy);
    let ended = first.ended();
    let (answers, _answered) = tokio::sync::mpsc::unbounded_channel();
    first.to.send(ToHub::Join { worker: 0, answers }).unwrap();
    let before = exchanges.load(Ordering::Relaxed);
    let (decided, _waits) = tokio::sync::oneshot::channel();
    first
        .to
        .send(ToHub::Ask {
            worker: 0,
            id: 0,
            keys: vec![(
                b"cold".as_slice().into(),
                Contract {
                    limit: NonZeroU64::new(10).unwrap(),
                    window: NonZeroU64::new(3_600_000_000_000).unwrap(),
                    burst: NonZeroU64::new(10).unwrap(),
                },
            )],
            until: u64::MAX,
            decided,
        })
        .unwrap();
    until(|| exchanges.load(Ordering::Relaxed) > before).await;
    if let Some(service) = service_of(&mut config) {
        service.cycle_ms = Some(20);
    }
    proxy.reload(compile(&config).unwrap()).unwrap();
    let second = hub_of(&proxy);
    drop(first);
    until(ended).await;
    assert_eq!(failures(), 0, "the replaced thread told");

    second.to.send(ToHub::Panic { in_exchange: false }).unwrap();
    until(|| failures() > 0).await;
    assert_eq!(failures(), 1);
}

/// A thread whose exchanges are answered tells nothing; one that panicked, or whose
/// exchange's task did, tells once: once it has ended, its handles let go of, it has told
/// nothing more.
#[tokio::test]
async fn a_failed_rate_limiting_thread_tells_once() {
    use crate::ratelimit::{Hub, ToHub};
    use edgerush_ratelimit::service::Contract;
    use std::num::NonZeroU64;

    let pods = service().await;
    for in_exchange in [false, true] {
        let compiled = edgerush_config::CompiledRateLimitService {
            pods: pods.clone(),
            cycle: Duration::from_millis(10),
            max_key_bytes: 1 << 20,
            poll_quiet_keys: true,
            // A new key is asked about: an exchange.
            ask_cold_keys: true,
            schedule_keys: false,
        };
        let (failed, failures) = told_failures();
        let failed = crate::ratelimit::Failed(Arc::new(failed));
        let hub = Hub::start(&compiled, 1, 1 << 20, Some(failed)).unwrap();
        let ended = hub.ended();
        let (answers, mut answered) = tokio::sync::mpsc::unbounded_channel();
        hub.to.send(ToHub::Join { worker: 0, answers }).unwrap();
        let contract = Contract {
            limit: NonZeroU64::new(10).unwrap(),
            window: NonZeroU64::new(3_600_000_000_000).unwrap(),
            burst: NonZeroU64::new(10).unwrap(),
        };
        // Each decision waited for, as a request there still waits for its own.
        let waiting = RefCell::new(Vec::new());
        let ask = |id: u64| {
            let (decided, waits) = tokio::sync::oneshot::channel();
            waiting.borrow_mut().push(waits);
            ToHub::Ask {
                worker: 0,
                id,
                keys: vec![(format!("cold-{id}").into_bytes().into(), contract)],
                until: u64::MAX,
                decided,
            }
        };
        hub.to.send(ask(0)).unwrap();
        assert!(within(answered.recv()).await.unwrap().admitted);
        assert_eq!(failures(), 0, "an exchange answered");

        hub.to.send(ToHub::Panic { in_exchange }).unwrap();
        // Not taken once the thread itself has panicked.
        let _gone = hub.to.send(ask(1));
        until(|| failures() > 0).await;
        drop(hub);
        until(ended).await;
        assert_eq!(failures(), 1, "in an exchange: {in_exchange}");
    }
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
            if let Some(service) = service_of(&mut config) {
                sharing(service);
            }
            let workers = NonZeroUsize::new(2).unwrap();
            let proxy = Arc::new(started(&config, workers));
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
            for _ in 0..16 {
                let send = sends.remove(0);
                let mut send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    comes_back(response.headers());
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
            let proxy = started(&config, NonZeroUsize::MIN);
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
                    comes_back(response.headers());
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
            assert!(scrape.contains(&line("trial", "dry_run_uncounted", 0)), "{scrape}");
            for outcome in ["dry_run_refused", "dry_run_uncounted"] {
                assert!(
                    !scrape.contains(&format!("limit=\"per-client\",outcome=\"{outcome}\"")),
                    "{scrape}"
                );
            }
        })
        .await;
}

/// A dry run its rate limits cannot decide — here, before they are started — lets the
/// request go on as ever, and counts it `dry_run_uncounted`, neither admitted nor refused.
#[tokio::test]
async fn a_dry_run_not_decided_lets_the_request_go_on_and_says_so() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            let mut config = limited(everything_config(upstream), &service);
            config.routes[0].rules[0].rate_limits = vec!["trial".to_owned()];
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:
  - { name: trial, key: client, model: { bucket: { rate: 4/h, burst: 4 } }, dry_run: true }
",
                )
                .unwrap(),
            );
            let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
            let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let mut statuses = Vec::new();
            for _ in 0..3 {
                send = within(send.ready()).await.unwrap();
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = within(response).await.unwrap();
                statuses.push(response.status().as_u16());
            }
            assert_eq!(statuses, [200; 3]);
            let line = |outcome: &str, count: u64| {
                format!("edgerush_rate_limit_decisions_total{{limit=\"trial\",outcome=\"{outcome}\"}} {count}\n")
            };
            let scrape = worker.proxy().metrics();
            for expected in [
                line("admitted", 0),
                line("dry_run_refused", 0),
                line("dry_run_uncounted", 3),
            ] {
                assert!(scrape.contains(&expected), "{expected}{scrape}");
            }
        })
        .await;
}

/// The scrape shows what the keys hold against the data plane's bound; a bound too small to
/// keep any key evicts each once its requests are over; and the bound is the process's,
/// its keys held once for all its workers: one and a half keys' worth on two workers keeps
/// the one key. A bound a reload sets before any request is held to as one set at start.
#[tokio::test]
async fn the_keys_held_are_shown_and_kept_within_their_bound() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service = service().await;
            let (upstream, _) = counting_upstream().await;
            // What the one key holds, as the first case's scrape says.
            let mut one_key = 0;
            for (case, workers, held) in [(0, 1, true), (1, 1, false), (2, 2, true), (3, 1, false)]
            {
                let max = match case {
                    0 => None,
                    1 | 3 => Some(1),
                    _ => Some(one_key * 3 / 2),
                };
                let mut config = limited(everything_config(upstream), &service);
                let workers = NonZeroUsize::new(workers).unwrap();
                let proxy = if case == 3 {
                    let proxy = started(&config, workers);
                    if let Some(limiting) = &mut config.rate_limiting {
                        limiting.max_key_bytes = max;
                    }
                    proxy.reload(compile(&config).unwrap()).unwrap();
                    proxy
                } else {
                    if let Some(limiting) = &mut config.rate_limiting {
                        limiting.max_key_bytes = max;
                    }
                    started(&config, workers)
                };
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
                if case == 0 {
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
                if let Some(limiting) = &mut config.rate_limiting {
                    limiting.max_key_bytes = Some(128 << 20);
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
                let proxy = started(&config, NonZeroUsize::MIN);
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
            let proxy = started(&quiet, NonZeroUsize::MIN);
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
            let proxy = started(&config, NonZeroUsize::MIN);
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
            let hub = crate::ratelimit::Hub::start(&service, 1, u64::MAX, None).unwrap();
            let limiter = crate::ratelimit::Limiter::new(&service, hub, 0);
            let rule = Arc::clone(&compiled.rules_of(0)[0]);
            let client = requester("203.0.113.7");
            let told = [(1, 5_000_000_000)];
            let refusal = limiter.refusal(&rule.limits, 0b11, client, &told);
            assert_eq!(refusal.by, 0b10, "wide, the second");
            let (limit, wait) = refusal.longest.unwrap();
            assert_eq!(limit.name, "wide");
            assert_eq!(wait, Duration::from_secs(5));
            let unsaid = limiter.refusal(&rule.limits, 0b11, client, &[]);
            assert_eq!(unsaid.by, 0, "the entries know nothing");
            limiter.stop();
        })
        .await;
}
