//! The rate limits route rules are under, in the synchronous mode (06 §4): every request
//! asked of its keys' owners, the store's pods (`edgerush_ratelimit_service::cluster`) on real
//! sockets, formed in-process as the bench's example program forms them.

use super::*;
use edgerush_certificates::testing::Authority;
use edgerush_cluster::{Config as StoreConfig, Table};
use edgerush_config::KeySecret;
use edgerush_ratelimit_service::cluster::forming::Forming;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::plane::{Answer, Plane, Rule, Told};
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer, decide_stream};
use edgerush_ratelimit_service::secure::{self, PodCredentials, PodTls};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::AtomicUsize;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

/// The secret the tests' data planes hash their keys under, as their config says it.
const SECRET: &str = "00112233445566778899aabbccddeeff";

/// Few shards, so that the tests find keys of every pod.
fn store_config() -> StoreConfig {
    StoreConfig {
        shards: 16,
        ..StoreConfig::defaults()
    }
}

/// The same, slow to take a pod out: a pod stopped keeps its shards for a minute, its keys out
/// of reach rather than moved to another.
fn holding_config() -> StoreConfig {
    StoreConfig {
        silence: 60_000_000_000,
        ..store_config()
    }
}

/// A store pod serving on a socket of its own, and where it is reached, as it gives it.
struct ServedPod {
    pod: StorePod,
    server: JoinHandle<()>,
    address: String,
}

/// Store pods and the `Forming` that formed them, both stopped once dropped.
struct Store {
    pods: Vec<ServedPod>,
    forming: JoinHandle<()>,
}

impl Drop for Store {
    fn drop(&mut self) {
        self.forming.abort();
        for pod in &self.pods {
            pod.server.abort();
        }
    }
}

/// A naming file of the scratch directory's, apart from every other test's.
fn naming_file() -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory =
        std::env::temp_dir().join(format!("edgerush-sync-limits-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(NEXT.fetch_add(1, Ordering::Relaxed).to_string())
}

impl Store {
    /// Pods served on `listeners`, each giving the address beside its listener — its own, or a
    /// relay's in front of it — with `config`, forwarding as the service's binary has them, and
    /// formed into one cluster by `Forming` at those addresses; once every pod is ready on one
    /// table. Served plain, or with `tls`, which `Forming` dials with as a pod.
    async fn formed(
        listeners: Vec<(TcpListener, String)>,
        config: &StoreConfig,
        tls: Option<PodTls>,
    ) -> Self {
        let dial = tls.as_ref().map_or(Dial::Plain, |tls| {
            Dial::Secured(tokio::sync::watch::channel(tls.connector().clone()).1)
        });
        let serving = tls.map(|tls| tokio::sync::watch::channel(Arc::new(tls)).1);
        let mut pods = Vec::new();
        for (id, (listener, address)) in (1..).zip(listeners) {
            let pod = StorePod::spawn(id, address.clone(), *config, dial.clone(), Vec::new());
            pod.set_forwarding(true).await;
            let service = StoreServer::new(pod.clone());
            let server = match &serving {
                Some(tls) => {
                    let tls = tls.clone();
                    tokio::spawn(async move {
                        let _ended = secure::serve(
                            listener,
                            tls,
                            service,
                            |_peer, _why| {},
                            std::future::pending(),
                        )
                        .await;
                    })
                }
                None => tokio::spawn(async move {
                    let _ended = tonic::transport::Server::builder()
                        .serve_with_incoming(service, TcpIncoming::from(listener))
                        .await;
                }),
            };
            pods.push(ServedPod {
                pod,
                server,
                address,
            });
        }
        let addresses = pods.iter().map(|pod| pod.address.clone()).collect();
        let forming = Forming::new(config, naming_file()).unwrap().dialling(dial);
        let forming = tokio::spawn(async move {
            let _stopped = forming.run(addresses).await;
        });
        let store = Self { pods, forming };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !store.settled().await {
            assert!(Instant::now() < deadline, "the pods never formed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        store
    }

    /// `count` pods served plain, each at its own address.
    async fn plain(count: usize) -> Self {
        let mut listeners = Vec::new();
        for _ in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            listeners.push((listener, address));
        }
        Self::formed(listeners, &store_config(), None).await
    }

    /// Whether every pod is ready, on the same table.
    async fn settled(&self) -> bool {
        let mut stamps = Vec::new();
        for pod in &self.pods {
            if !pod.pod.ready().await {
                return false;
            }
            stamps.push(
                pod.pod
                    .with(|node, _| node.committed_stamp())
                    .await
                    .unwrap_or_default(),
            );
        }
        stamps.windows(2).all(|pair| pair[0] == pair[1])
    }

    fn addresses(&self) -> Vec<String> {
        self.pods.iter().map(|pod| pod.address.clone()).collect()
    }

    /// The table the pods have committed.
    async fn table(&self) -> Table {
        self.pods[0]
            .pod
            .with(|node, _| node.committed().cloned())
            .await
            .flatten()
            .unwrap()
    }

    /// The place among the pods of the one owning the data plane's key `key`.
    async fn owner_of(&self, key: &[u8]) -> usize {
        let mut plane = plane();
        assert!(plane.learn(self.table().await));
        let owner = plane.owner(plane.key(key)).unwrap();
        usize::try_from(owner - 1).unwrap()
    }

    /// Stops the pod at `at` as a crash does: its store, its server, its connections.
    async fn stop(&self, at: usize) {
        self.pods[at].pod.stop().await;
        self.pods[at].server.abort();
    }

    /// Stops forming, its pods formed: its connections close.
    fn stop_forming(&self) {
        self.forming.abort();
    }
}

/// A data plane's plane, under the tests' secret.
fn plane() -> Plane {
    Plane::new(KeySecret::parse(SECRET).unwrap().0)
}

/// What the tests' limits of ten an hour and a bucket of five are held to at the store.
const FIVE: Rule = Rule::Bucket {
    per: 10,
    period: 3_600_000_000_000,
    burst: 5,
};

/// Asks the pod at `address` directly, as a data plane does, what `make` makes of a plane, a
/// number and the store's key for `key`, `count` times: each answer.
async fn directly(
    address: &str,
    key: &[u8],
    count: u64,
    make: impl Fn(&Plane, u64, u64) -> Option<edgerush_ratelimit_service::cluster::wire::Asking>,
) -> Vec<Answer> {
    let mut stream = decide_stream(address, &Dial::Plain).await.unwrap();
    let mut plane = plane();
    let key = plane.key(key);
    let mut answers = Vec::new();
    for id in 0..count {
        stream
            .up
            .send(make(&plane, id, key).unwrap())
            .await
            .unwrap();
        loop {
            let told = within(stream.down.message()).await.unwrap().unwrap();
            if let Ok(Told::Answer {
                id: answered,
                answer,
            }) = plane.told(told)
                && answered == id
            {
                answers.push(answer);
                break;
            }
        }
    }
    answers
}

/// Requests under `key` held to [`FIVE`], `count` of them, asked of the pod at `address`
/// directly: how many it admitted.
async fn ask_directly(address: &str, key: &[u8], count: u64) -> usize {
    directly(address, key, count, |plane, id, key| {
        plane.request(id, key, Some(FIVE))
    })
    .await
    .iter()
    .filter(|answer| matches!(answer, Answer::Admitted(_)))
    .count()
}

/// Gives `count` charges under `key` held to [`FIVE`] back to the pod at `address`, directly:
/// a bucket's give-back takes no receipt.
async fn give_back_directly(address: &str, key: &[u8], count: u64) {
    let answers = directly(address, key, count, |plane, id, key| {
        plane.give_back(id, key, Some(FIVE), 0)
    })
    .await;
    assert!(
        answers
            .iter()
            .all(|answer| matches!(answer, Answer::Counted(_))),
        "{answers:?}"
    );
}

/// `config` with its owners' breaker opened by `failures` in a row and probed after
/// `probe_after_ms`.
fn with_breaker(mut config: Config, failures: u32, probe_after_ms: u64) -> Config {
    let synchronous = config
        .rate_limiting
        .as_mut()
        .and_then(|limiting| limiting.synchronous.as_mut())
        .unwrap();
    synchronous.breaker = edgerush_config::RateLimitBreaker {
        failures,
        probe_after_ms,
    };
    config
}

/// The line of `scrape` that begins `name`, and its value.
fn metric(scrape: &str, name: &str) -> Option<u64> {
    scrape
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

/// The name of the metric of what was asked of the pod at `address` and came to `outcome`.
fn asks(address: &str, outcome: &str) -> String {
    format!(r#"edgerush_rate_limit_owner_asks_total{{pod="{address}",outcome="{outcome}"}}"#)
}

/// `config` with its first route's first rule under `limits`, each `(name, burst)` keyed by
/// the client at ten an hour, in the synchronous mode with the pods at `addresses` for seeds,
/// waiting `timeout_ms` for them and `when_unreachable` as said.
fn synchronous_in(
    mut config: Config,
    limits: &[(&str, u64)],
    addresses: &[String],
    timeout_ms: u64,
    when_unreachable: &str,
) -> Config {
    config.routes[0].rules[0].rate_limits =
        limits.iter().map(|(name, _)| (*name).to_owned()).collect();
    let written: String = limits
        .iter()
        .map(|(name, burst)| {
            format!("  - {{ name: {name}, key: client, model: {{ bucket: {{ rate: 10/h, burst: {burst} }} }} }}\n")
        })
        .collect();
    config.limits = Some(serde_saphyr::from_str(&format!("limits:\n{written}")).unwrap());
    let addresses: Vec<String> = addresses.iter().map(|a| format!("\"{a}\"")).collect();
    config.rate_limiting = Some(
        serde_saphyr::from_str(&format!(
            "{{ mode: synchronous, service: {{ addresses: [{}], key_secret: {SECRET} }}, synchronous: {{ timeout_ms: {timeout_ms}, when_unreachable: {when_unreachable}, breaker: {{ failures: 5, probe_after_ms: 1000 }} }} }}",
            addresses.join(", ")
        ))
        .unwrap(),
    );
    config
}

/// `workers` workers of a data plane for `config`, its rate limits started, each serving a
/// connection of its own: the data plane, and a client's connection to each.
async fn serving(
    config: &Config,
    workers: usize,
) -> (Arc<Proxy>, Vec<::h2::client::SendRequest<Bytes>>) {
    let (proxy, sends, _) = serving_at(config, workers).await;
    (proxy, sends)
}

/// The same, and where each worker listens, for a client to connect again.
async fn serving_at(
    config: &Config,
    workers: usize,
) -> (
    Arc<Proxy>,
    Vec<::h2::client::SendRequest<Bytes>>,
    Vec<SocketAddr>,
) {
    let proxy = Proxy::new(
        compile(config).unwrap(),
        NonZeroUsize::new(workers).unwrap(),
    )
    .unwrap();
    proxy.start_rate_limits(|| {}).unwrap();
    let proxy = Arc::new(proxy);
    let mut sends = Vec::new();
    let mut fronts = Vec::new();
    for position in 0..u16::try_from(workers).unwrap() {
        let worker = Worker::made(
            Arc::clone(&proxy),
            H1Limits::default(),
            SHORT,
            position,
            None,
        );
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
        sends.push(h2_library_client(front, &::h2::client::Builder::new()).await);
        fronts.push(front);
    }
    (proxy, sends, fronts)
}

/// `count` requests, alternating between the workers that `sends` reach, one at a time:
/// each one's answer.
async fn send_round(
    sends: &mut Vec<::h2::client::SendRequest<Bytes>>,
    count: usize,
) -> Vec<Response<::h2::RecvStream>> {
    let mut answers = Vec::new();
    for _ in 0..count {
        let send = sends.remove(0);
        let mut send = within(send.ready()).await.unwrap();
        let request = Request::get("http://example.test/")
            .version(Version::HTTP_2)
            .body(())
            .unwrap();
        let (response, _) = send.send_request(request, true).unwrap();
        answers.push(within(response).await.unwrap());
        sends.push(send);
    }
    answers
}

fn statuses(answers: &[Response<::h2::RecvStream>]) -> Vec<u16> {
    answers.iter().map(|a| a.status().as_u16()).collect()
}

/// Each limit of `config`'s first route's first rule, by name, with the key a request from
/// this machine has under it: in the order the data plane builds a request's keys in.
fn keys_of(config: &Config) -> Vec<(String, Vec<u8>)> {
    let compiled = compile(config).unwrap();
    let rule = compiled
        .rule(edgerush_config::RuleId { route: 0, rule: 0 })
        .unwrap();
    let client = crate::forwarding::Requester {
        address: std::net::Ipv4Addr::LOCALHOST.into(),
        peer: Some(std::net::Ipv4Addr::LOCALHOST.into()),
    };
    let every = u64::MAX >> (64 - rule.limits.len());
    let keys = crate::ratelimit::with_keys(
        &RefCell::new(Vec::new()),
        &RefCell::new(Vec::<edgerush_ratelimit::service::Contract>::new()),
        &rule.limits,
        every,
        client.into(),
        |keys| keys.iter().map(|(key, _)| key.to_vec()).collect::<Vec<_>>(),
    );
    rule.limits
        .iter()
        .map(|limit| limit.name.clone())
        .zip(keys)
        .collect()
}

/// A config of two limits of buckets of five whose keys `store` has two different pods own,
/// each with its key, by trying pairs of names; the first's then the second's.
async fn two_owners(
    store: &Store,
    upstream: SocketAddr,
    seeds: &[String],
    timeout_ms: u64,
    when: &str,
) -> (Config, Vec<(String, Vec<u8>)>) {
    let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for first in names {
        for second in names {
            if first == second {
                continue;
            }
            let config = synchronous_in(
                everything_config(upstream),
                &[(first, 5), (second, 5)],
                seeds,
                timeout_ms,
                when,
            );
            let keys = keys_of(&config);
            if store.owner_of(&keys[0].1).await != store.owner_of(&keys[1].1).await {
                return (config, keys);
            }
        }
    }
    panic!("no two limits with different owners");
}

/// Two workers asking one owner hold a client to its one bucket of ten; refusals say when
/// to come back, as the owner told.
#[tokio::test]
async fn the_owner_holds_a_client_to_one_bucket_across_workers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let store = Store::plain(1).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                &store.addresses(),
                1_000,
                "deny",
            );
            let (proxy, mut sends) = serving(&config, 2).await;
            let answers = send_round(&mut sends, 16).await;
            let statuses = statuses(&answers);
            assert_eq!(
                statuses.iter().filter(|s| **s == 200).count(),
                10,
                "{statuses:?}"
            );
            for refused in answers.iter().filter(|a| a.status() == 429) {
                let after = refused.headers()["retry-after"].to_str().unwrap();
                assert!(after == "360" || after == "361", "retry-after {after}");
                assert_eq!(
                    refused.headers()["ratelimit"],
                    format!(r#""per-client";r=0;t={after}"#).as_str()
                );
            }
            assert!(
                proxy.metrics().contains(
                    r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="refused"} 6"#
                ),
                "{}",
                proxy.metrics()
            );
        })
        .await;
}

/// A fixed window is held at its owner: three in the window admitted, the rest refused, told
/// to come back once it ends.
#[tokio::test]
async fn a_fixed_window_is_held_at_its_owner() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let store = Store::plain(1).await;
            let (upstream, _) = counting_upstream().await;
            let mut config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                &store.addresses(),
                1_000,
                "deny",
            );
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:\n  - { name: per-client, key: client, model: { fixed_window: { limit: 3, window: 1h } } }\n",
                )
                .unwrap(),
            );
            let (_proxy, mut sends) = serving(&config, 1).await;
            let answers = send_round(&mut sends, 5).await;
            assert_eq!(statuses(&answers), [200, 200, 200, 429, 429]);
            let after: u64 = answers[3].headers()["retry-after"]
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            assert!((1..=3_600).contains(&after), "retry-after {after}");
        })
        .await;
}

/// A request under limits owned by two pods is asked of both at once; refused by one, the
/// other's charge is given back: the limit that had room is charged only for the requests
/// admitted. The refusal names the limit that refused, whose key is the request's second.
#[tokio::test]
async fn a_charge_admitted_by_one_owner_is_given_back_when_another_refuses() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let store = Store::plain(2).await;
            let (upstream, _) = counting_upstream().await;
            let (config, keys) =
                two_owners(&store, upstream, &store.addresses(), 1_000, "deny").await;
            let ((roomy, roomy_key), (tight, tight_key)) = (&keys[0], &keys[1]);
            let roomy_at = store.pods[store.owner_of(roomy_key).await].address.clone();
            let tight_at = store.pods[store.owner_of(tight_key).await].address.clone();
            // The second has two left, the first all five.
            assert_eq!(ask_directly(&tight_at, tight_key, 3).await, 3);
            let (_proxy, mut sends) = serving(&config, 1).await;
            let answers = send_round(&mut sends, 6).await;
            assert_eq!(statuses(&answers), [200, 200, 429, 429, 429, 429]);
            let named = answers[2].headers()["ratelimit"].to_str().unwrap();
            assert!(named.starts_with(&format!(r#""{tight}";"#)), "{named}");
            // The first's owner has the three it was not charged for, once its give-backs
            // are in.
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                ask_directly(&roomy_at, roomy_key, 5).await,
                3,
                "`{roomy}` charged for the two admitted alone"
            );
        })
        .await;
}

/// A seed out of reach — its port closed, or silent past the wait — leaves each request to
/// `when_unreachable`: `allow` lets it on, `deny` answers 503 with `Retry-After: 1`, never
/// 429; a silent seed after the wait, a closed one at once.
#[tokio::test]
async fn an_owner_out_of_reach_is_allowed_or_denied_as_said() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let closed = {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap().to_string()
            };
            // Accepts and says nothing.
            let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let silent_address = silent.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = silent.accept().await {
                    held.push(socket);
                }
            });
            let (upstream, _) = counting_upstream().await;
            for (address, quick) in [(&closed, true), (&silent_address, false)] {
                for (when, status) in [("allow", 200), ("deny", 503)] {
                    let config = synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 10)],
                        std::slice::from_ref(address),
                        200,
                        when,
                    );
                    let (_proxy, mut sends) = serving(&config, 1).await;
                    let began = Instant::now();
                    let answers = send_round(&mut sends, 2).await;
                    let took = began.elapsed();
                    assert_eq!(statuses(&answers), [status, status], "{address} {when}");
                    if status == 503 {
                        assert_eq!(answers[0].headers()["retry-after"], "1");
                    }
                    if !quick {
                        assert!(took >= Duration::from_millis(400), "waited out: {took:?}");
                    }
                }
            }
        })
        .await;
}

/// An owner that stops is failed over by the others, a majority of the three, which tell the
/// worker's streams of their new table: its keys go to their new owner, and requests are
/// decided again rather than left to `when_unreachable`.
#[tokio::test]
async fn the_keys_of_an_owner_that_stops_go_to_the_next() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let store = Store::plain(3).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                &store.addresses(),
                200,
                "deny",
            );
            let (_, key) = keys_of(&config).remove(0);
            let owner = store.owner_of(&key).await;
            let (_proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            store.stop(owner).await;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let status = statuses(&send_round(&mut sends, 1).await)[0];
                if status == 200 {
                    break;
                }
                assert_eq!(status, 503, "denied while the owner is gone");
                assert!(Instant::now() < deadline, "its keys never moved");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
}

/// An owner that stops and is not taken out — the pods slow to — has its key decided, with
/// the store set to, by the replica its asks go through once it has gone unheard for
/// `stop_serving`, from that replica's copy: the key held to its bucket, what the owner
/// admitted counted, where a store not set to leaves it to `when_unreachable` throughout.
#[tokio::test]
async fn a_replica_decides_the_keys_of_an_owner_gone_unheard_with_the_store_set_to() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for decides in [false, true] {
                let mut listeners = Vec::new();
                for _ in 0..3 {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let address = listener.local_addr().unwrap().to_string();
                    listeners.push((listener, address));
                }
                let store = Store::formed(listeners, &holding_config(), None).await;
                for pod in &store.pods {
                    pod.pod.set_forwarder_decides(decides).await;
                }
                let (upstream, _) = counting_upstream().await;
                let config = synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 5)],
                    &store.addresses(),
                    200,
                    "deny",
                );
                let (_, key) = keys_of(&config).remove(0);
                let owner = store.owner_of(&key).await;
                let (_proxy, mut sends) = serving(&config, 1).await;
                assert_eq!(statuses(&send_round(&mut sends, 2).await), [200, 200]);
                // Copied to the replicas within a cycle or two.
                tokio::time::sleep(Duration::from_millis(100)).await;
                store.stop(owner).await;
                let stopped = Instant::now();
                let mut after = Vec::new();
                while stopped.elapsed() < Duration::from_millis(1_500) {
                    after.extend(statuses(&send_round(&mut sends, 1).await));
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let decided: Vec<u16> = after.iter().copied().filter(|s| *s != 503).collect();
                if decides {
                    // Three left of the five, then refused: the owner's two counted.
                    assert!(decided.len() >= 4, "{after:?}");
                    assert_eq!(decided[..4], [200, 200, 200, 429], "{after:?}");
                    assert!(decided[4..].iter().all(|s| *s == 429), "{after:?}");
                    let decided_from_copy: u64 = {
                        let mut sum = 0;
                        for (at, pod) in store.pods.iter().enumerate() {
                            if at != owner {
                                sum += pod.pod.observed().await.unwrap().decided_from_copy;
                            }
                        }
                        sum
                    };
                    assert_eq!(decided_from_copy, u64::try_from(decided.len()).unwrap());
                } else {
                    assert!(decided.is_empty(), "decided with the owner gone: {after:?}");
                }
            }
        })
        .await;
}

/// A seed that does not answer opens its breaker after `failures` timeouts in a row: from
/// then on its requests are decided by `when_unreachable` at once, nothing asked of it, and
/// the metrics say so, by its address.
#[tokio::test]
async fn an_owner_that_keeps_timing_out_opens_its_breaker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Accepts and says nothing.
            let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = silent.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = silent.accept().await {
                    held.push(socket);
                }
            });
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 10)],
                    std::slice::from_ref(&address),
                    200,
                    "deny",
                ),
                2,
                60_000,
            );
            let (proxy, mut sends) = serving(&config, 1).await;
            let began = Instant::now();
            assert_eq!(statuses(&send_round(&mut sends, 2).await), [503, 503]);
            assert!(began.elapsed() >= Duration::from_millis(400), "each waited");
            let began = Instant::now();
            assert_eq!(
                statuses(&send_round(&mut sends, 5).await),
                [503, 503, 503, 503, 503]
            );
            assert!(
                began.elapsed() < Duration::from_millis(400),
                "none waited: {:?}",
                began.elapsed()
            );
            let scrape = proxy.metrics();
            let line = |name: &str| metric(&scrape, name);
            assert_eq!(line(&asks(&address, "timed_out")), Some(2), "{scrape}");
            assert_eq!(
                line(&format!(
                    r#"edgerush_rate_limit_owner_breaker_trips_total{{pod="{address}"}}"#
                )),
                Some(1)
            );
            assert_eq!(
                line(&format!(
                    r#"edgerush_rate_limit_owner_breaker_open{{pod="{address}"}}"#
                )),
                Some(1)
            );
            assert_eq!(
                line(r#"edgerush_rate_limit_unreachable_total{action="deny"}"#),
                Some(7)
            );
        })
        .await;
}

/// A pod served with mutual TLS is asked by a data plane whose certificate carries the data
/// plane's ID it is given. One whose certificate carries another workload's ID, though their
/// authority issued it, reaches no pod, nor does one that checks the pods for an ID they do
/// not carry: their requests are decided as `when_unreachable` says.
#[tokio::test]
async fn owners_are_asked_with_the_certificate_the_config_shows() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let authority = Authority::new("workloads");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap().to_string();
            let tls = PodTls::new(&PodCredentials {
                certificate: authority.issue(&["127.0.0.1", POD_ID]),
                authorities: authority.pem(),
                data_plane: PLANE_ID.to_owned(),
            })
            .unwrap();
            let _store =
                Store::formed(vec![(listener, owner.clone())], &store_config(), Some(tls)).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                std::slice::from_ref(&owner),
                500,
                "deny",
            );
            let other = "spiffe://cluster.test/ns/edge/sa/other";
            for (shown, pod_id, status, answered) in [
                (PLANE_ID, POD_ID, 200, Some(1)),
                (other, POD_ID, 503, None),
                (PLANE_ID, other, 503, None),
            ] {
                let shown = authority.issue(&["plane", shown]);
                let secured = service_tls(config.clone(), &authority.pem(), pod_id, shown);
                let (proxy, mut sends) = serving(&secured, 1).await;
                assert_eq!(statuses(&send_round(&mut sends, 1).await), [status]);
                assert_eq!(
                    metric(&proxy.metrics(), &asks(&owner, "answered")),
                    answered.or(Some(0)),
                );
            }
        })
        .await;
}

/// A relay to `to` whose connections each have a gate on what `to` sends: open, it passes;
/// shut, it is held until the gate opens again. Says how many connections it accepted and
/// which have ended, since it was last told to forget them.
struct GatedRelay {
    address: String,
    gates: Arc<std::sync::Mutex<Vec<tokio::sync::watch::Sender<bool>>>>,
    /// Whether a connection's gate is open when it is accepted.
    open_at_first: Arc<std::sync::atomic::AtomicBool>,
    accepted: Arc<AtomicUsize>,
    /// The connections that have ended, by the order they were accepted in.
    ended: Arc<std::sync::Mutex<Vec<usize>>>,
    /// Raised at each forgetting: a connection accepted before counts for nothing after.
    epoch: Arc<AtomicUsize>,
}

impl GatedRelay {
    /// One whose gates are open, at `ip`.
    async fn to(to: SocketAddr) -> Self {
        Self::at(IpAddr::V4(Ipv4Addr::LOCALHOST), to).await
    }

    async fn at(ip: IpAddr, to: SocketAddr) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind((ip, 0)).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let gates = Arc::new(std::sync::Mutex::new(Vec::new()));
        let open_at_first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let held_open_at_first = Arc::clone(&open_at_first);
        let accepted = Arc::new(AtomicUsize::new(0));
        let ended = Arc::new(std::sync::Mutex::new(Vec::new()));
        let epoch = Arc::new(AtomicUsize::new(0));
        let (held_gates, held_accepted, held_ended, held_epoch) = (
            Arc::clone(&gates),
            Arc::clone(&accepted),
            Arc::clone(&ended),
            Arc::clone(&epoch),
        );
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(to).await else {
                    continue;
                };
                let (gate, mut open) =
                    tokio::sync::watch::channel(held_open_at_first.load(Ordering::Relaxed));
                held_gates.lock().unwrap().push(gate);
                let nth = held_accepted.fetch_add(1, Ordering::Relaxed);
                let accepted_in = held_epoch.load(Ordering::Relaxed);
                let (mut from_client, mut to_client) = client.into_split();
                let (mut from_server, mut to_server) = server.into_split();
                tokio::spawn(async move {
                    let _ended = tokio::io::copy(&mut from_client, &mut to_server).await;
                });
                let (ended, epoch) = (Arc::clone(&held_ended), Arc::clone(&held_epoch));
                tokio::spawn(async move {
                    let mut buffer = vec![0; 16_384];
                    while let Ok(read) = from_server.read(&mut buffer).await
                        && read > 0
                    {
                        if open.wait_for(|open| *open).await.is_err()
                            || to_client.write_all(&buffer[..read]).await.is_err()
                        {
                            break;
                        }
                    }
                    if epoch.load(Ordering::Relaxed) == accepted_in {
                        ended.lock().unwrap().push(nth);
                    }
                });
            }
        });
        Self {
            address,
            gates,
            open_at_first,
            accepted,
            ended,
            epoch,
        }
    }

    /// Forgets every connection it has accepted, open or ended: those it accepts from now on
    /// are counted from 0. Their gates are let go of, open.
    fn forget(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
        let mut gates = self.gates.lock().unwrap();
        for gate in gates.iter() {
            gate.send_replace(true);
        }
        gates.clear();
        self.accepted.store(0, Ordering::Relaxed);
        self.ended.lock().unwrap().clear();
    }

    /// Shuts every gate, and those of connections accepted from now on: `to` as an owner that
    /// accepts and says nothing, until it is back.
    fn shut_all(&self) {
        self.open_at_first.store(false, Ordering::Relaxed);
        for gate in self.gates.lock().unwrap().iter() {
            gate.send_replace(false);
        }
    }

    /// Opens every gate, and those of connections accepted from now on.
    fn open_all(&self) {
        self.open_at_first.store(true, Ordering::Relaxed);
        for gate in self.gates.lock().unwrap().iter() {
            gate.send_replace(true);
        }
    }

    /// Opens or shuts the gate of the `nth` connection accepted, from 0.
    fn gate(&self, nth: usize, open: bool) {
        self.gates.lock().unwrap()[nth].send_replace(open);
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    fn ended(&self) -> usize {
        self.ended.lock().unwrap().len()
    }

    /// Whether the `nth` connection accepted has ended.
    fn has_ended(&self, nth: usize) -> bool {
        self.ended.lock().unwrap().contains(&nth)
    }
}

/// A pod alone behind a gated relay, the relay's address the one it gives, so that every
/// connection to it goes through the relay: formed, then with every connection forming made
/// forgotten. Served plain, or with `tls`.
async fn behind_a_relay(tls: Option<PodTls>) -> (Store, GatedRelay) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = GatedRelay::to(listener.local_addr().unwrap()).await;
    let store = Store::formed(
        vec![(listener, relay.address.clone())],
        &store_config(),
        tls,
    )
    .await;
    store.stop_forming();
    tokio::time::sleep(Duration::from_millis(50)).await;
    relay.forget();
    (store, relay)
}

/// An open breaker is probed after `probe_after`: once its pod is back and tells its table on
/// the probe's stream, the breaker closes, and requests are asked of the pod again. Plain, and
/// with mutual TLS, which the probe dials with as the streams do.
#[tokio::test]
async fn an_open_breaker_closes_once_a_probe_hears_from_its_owner() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for secured in [false, true] {
                let authority = Authority::new("workloads");
                let tls = secured.then(|| {
                    PodTls::new(&PodCredentials {
                        certificate: authority.issue(&["127.0.0.1", POD_ID]),
                        authorities: authority.pem(),
                        data_plane: PLANE_ID.to_owned(),
                    })
                    .unwrap()
                });
                let (_store, relay) = behind_a_relay(tls).await;
                relay.shut_all();
                let (upstream, _) = counting_upstream().await;
                let mut config = with_breaker(
                    synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 10)],
                        std::slice::from_ref(&relay.address),
                        100,
                        "deny",
                    ),
                    1,
                    200,
                );
                if secured {
                    let shown = authority.issue(&["plane", PLANE_ID]);
                    config = service_tls(config, &authority.pem(), POD_ID, shown);
                }
                let (proxy, mut sends) = serving(&config, 1).await;
                assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
                let open = format!(
                    r#"edgerush_rate_limit_owner_breaker_open{{pod="{}"}}"#,
                    relay.address
                );
                assert_eq!(metric(&proxy.metrics(), &open), Some(1));
                relay.open_all();
                let deadline = Instant::now() + Duration::from_secs(5);
                while metric(&proxy.metrics(), &open) != Some(0) {
                    assert!(
                        Instant::now() < deadline,
                        "the breaker never closed, secured {secured}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
                assert_eq!(
                    metric(&proxy.metrics(), &asks(&relay.address, "answered")),
                    Some(1)
                );
            }
        })
        .await;
}

/// With `when_unreachable: local`, a seed out of reach leaves its keys to the process's own
/// buckets, which its workers share: a client is held to one bucket of ten across two
/// workers, refusals say when to come back, and the metrics count the requests decided
/// there and the process's one key.
#[tokio::test]
async fn with_local_an_owner_out_of_reach_is_decided_by_the_process_buckets() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let closed = {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap().to_string()
            };
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 10)],
                    &[closed],
                    100,
                    "local",
                ),
                2,
                60_000,
            );
            let (proxy, mut sends) = serving(&config, 2).await;
            let answers = send_round(&mut sends, 16).await;
            let statuses = statuses(&answers);
            assert_eq!(
                statuses.iter().filter(|s| **s == 200).count(),
                10,
                "{statuses:?}"
            );
            assert_eq!(
                statuses.iter().filter(|s| **s == 429).count(),
                6,
                "{statuses:?}"
            );
            for refused in answers.iter().filter(|a| a.status() == 429) {
                let after = refused.headers()["retry-after"].to_str().unwrap();
                assert!(after == "360" || after == "361", "retry-after {after}");
                assert_eq!(
                    refused.headers()["ratelimit"],
                    format!(r#""per-client";r=0;t={after}"#).as_str()
                );
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            let scrape = loop {
                let scrape = proxy.metrics();
                if metric(&scrape, "edgerush_rate_limit_keys") == Some(1)
                    || Instant::now() > deadline
                {
                    break scrape;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert_eq!(
                metric(&scrape, "edgerush_rate_limit_keys"),
                Some(1),
                "{scrape}"
            );
            assert_eq!(
                metric(
                    &scrape,
                    r#"edgerush_rate_limit_unreachable_total{action="local"}"#
                ),
                Some(16)
            );
        })
        .await;
}

/// The process's own buckets are kept from one outage to the next: a client that spent its
/// bucket while its owner was out of reach, then was decided by the owner once it was back,
/// finds the bucket still spent when the owner goes again — not a fresh one.
#[tokio::test]
async fn local_buckets_carry_over_from_one_outage_to_the_next() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (store, relay) = behind_a_relay(None).await;
            relay.shut_all();
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 10)],
                    std::slice::from_ref(&relay.address),
                    100,
                    "local",
                ),
                1,
                200,
            );
            let (proxy, mut sends) = serving(&config, 1).await;
            let first = statuses(&send_round(&mut sends, 11).await);
            assert_eq!(first[..10], [200; 10], "{first:?}");
            assert_eq!(first[10], 429, "the local bucket spent");
            relay.open_all();
            let open = format!(
                r#"edgerush_rate_limit_owner_breaker_open{{pod="{}"}}"#,
                relay.address
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), &open) != Some(0) {
                assert!(Instant::now() < deadline, "the breaker never closed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                statuses(&send_round(&mut sends, 1).await),
                [200],
                "the owner's own bucket"
            );
            store.stop(0).await;
            assert_eq!(
                statuses(&send_round(&mut sends, 2).await),
                [429, 429],
                "the local bucket as the last outage left it"
            );
        })
        .await;
}

/// With `when_unreachable: local` and one owner out of reach, only its keys are decided
/// locally, the others asked of their owners: refused locally, the refusal names the limit
/// whose key that was, by its place among the request's, and the owner that admitted is
/// given its charge back.
#[tokio::test]
async fn with_local_only_the_keys_out_of_reach_are_decided_locally() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Slow to take a pod out, so that the one stopped keeps its keys, out of reach;
            // three, so that the two left are a majority and serve.
            let mut listeners = Vec::new();
            for _ in 0..3 {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                listeners.push((listener, address));
            }
            let store = Store::formed(listeners, &holding_config(), None).await;
            let (upstream, _) = counting_upstream().await;
            let (config, keys) =
                two_owners(&store, upstream, &store.addresses(), 200, "local").await;
            let ((_, answered_key), (out_of_reach, out_key)) = (&keys[0], &keys[1]);
            let answering = store.owner_of(answered_key).await;
            store.stop(store.owner_of(out_key).await).await;
            // A breaker slow to open: a key sent through the answering pod to the one
            // stopped times out there, which is not the answering pod's failure.
            let config = with_breaker(config, 100, 60_000);
            let (_proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 5).await), [200; 5]);
            // The answering owner's bucket full again; the local one spent.
            let address = &store.pods[answering].address;
            give_back_directly(address, answered_key, 5).await;
            let answers = send_round(&mut sends, 1).await;
            assert_eq!(statuses(&answers), [429]);
            let named = answers[0].headers()["ratelimit"].to_str().unwrap();
            assert!(
                named.starts_with(&format!(r#""{out_of_reach}";"#)),
                "{named}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                ask_directly(address, answered_key, 5).await,
                5,
                "its charge given back"
            );
        })
        .await;
}

/// The process's own buckets are swept, on the probe thread: a key full and unused for a
/// second is forgotten. Read from the process itself, not from the metrics a worker writes
/// once a sweep: the key could come and go between two of those.
#[tokio::test]
async fn the_local_buckets_are_swept() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let closed = {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap().to_string()
            };
            let (upstream, _) = counting_upstream().await;
            let mut config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 10)],
                    &[closed],
                    100,
                    "local",
                ),
                1,
                60_000,
            );
            // A thousand a second, a bucket of one: full again a millisecond after its
            // request.
            config.limits = Some(
                serde_saphyr::from_str(
                    "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 1000/s, burst: 1 } } }\n",
                )
                .unwrap(),
            );
            let (proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            let keys = |proxy: &Proxy| {
                let (_, process) = proxy.sync_limits();
                process.map(|process| process.usage().keys)
            };
            // Nothing forgets a key before it has gone a second unused.
            assert_eq!(keys(&proxy), Some(1), "the key made");
            let deadline = Instant::now() + Duration::from_secs(5);
            while keys(&proxy) != Some(0) {
                assert!(Instant::now() < deadline, "the key never forgotten");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
}

/// A relay to `to` that holds up everything `to` sends by `delay`: an owner whose answers
/// come late.
async fn slow_relay(to: SocketAddr, delay: Duration) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(to).await else {
                continue;
            };
            let (mut from_client, mut to_client) = client.into_split();
            let (mut from_server, mut to_server) = server.into_split();
            tokio::spawn(async move {
                let _ended = tokio::io::copy(&mut from_client, &mut to_server).await;
            });
            tokio::spawn(async move {
                let mut buffer = vec![0; 16_384];
                while let Ok(read) = from_server.read(&mut buffer).await
                    && read > 0
                {
                    tokio::time::sleep(delay).await;
                    if to_client.write_all(&buffer[..read]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

/// An owner whose answers come after the data plane gave up: its admission of a request
/// the data plane then refused is given back when it comes, and its admission of one the
/// data plane let on is kept, the charge counting a request that went on.
#[tokio::test]
async fn an_admission_that_comes_too_late_is_given_back_if_the_request_was_refused() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for (when, status, left) in [("deny", 503, 5), ("allow", 200, 4)] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let owner = listener.local_addr().unwrap();
                let relay = slow_relay(owner, Duration::from_millis(300)).await;
                let _store =
                    Store::formed(vec![(listener, relay.clone())], &store_config(), None).await;
                let (upstream, _) = counting_upstream().await;
                let config = with_breaker(
                    synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 5)],
                        std::slice::from_ref(&relay),
                        100,
                        when,
                    ),
                    100,
                    60_000,
                );
                let (_, key) = keys_of(&config).remove(0);
                let (proxy, mut sends) = serving(&config, 1).await;
                assert_eq!(
                    statuses(&send_round(&mut sends, 1).await),
                    [status],
                    "{when}"
                );
                assert_eq!(
                    metric(&proxy.metrics(), &asks(&relay, "timed_out")),
                    Some(1),
                    "given up on, not broken: {when}"
                );
                // Its late answer in, and anything given back with it.
                tokio::time::sleep(Duration::from_secs(2)).await;
                assert_eq!(
                    ask_directly(&owner.to_string(), &key, 5).await,
                    left,
                    "{when}"
                );
            }
        })
        .await;
}

/// A reload mid-outage carries its state across: one that changes `timeout_ms` keeps the
/// seed's breaker open, so requests are not made to wait again, and the process's own
/// buckets as they were spent, not full; one that leaves `local` lets them go.
#[tokio::test]
async fn a_reload_mid_outage_keeps_the_breakers_and_the_local_buckets() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Accepts and says nothing.
            let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = silent.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = silent.accept().await {
                    held.push(socket);
                }
            });
            let (upstream, _) = counting_upstream().await;
            let in_mode = |timeout_ms, when| {
                with_breaker(
                    synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 10)],
                        std::slice::from_ref(&address),
                        timeout_ms,
                        when,
                    ),
                    1,
                    60_000,
                )
            };
            let (proxy, mut sends) = serving(&in_mode(200, "local"), 1).await;
            let first = statuses(&send_round(&mut sends, 11).await);
            assert_eq!(first[..10], [200; 10], "{first:?}");
            assert_eq!(first[10], 429);
            proxy
                .reload(compile(&in_mode(300, "local")).unwrap())
                .unwrap();
            let began = Instant::now();
            assert_eq!(
                statuses(&send_round(&mut sends, 1).await),
                [429],
                "still spent"
            );
            assert!(
                began.elapsed() < Duration::from_millis(300),
                "the breaker still open: {:?}",
                began.elapsed()
            );
            let scrape = proxy.metrics();
            assert_eq!(
                metric(
                    &scrape,
                    &format!(r#"edgerush_rate_limit_owner_breaker_trips_total{{pod="{address}"}}"#)
                ),
                Some(1),
                "{scrape}"
            );
            proxy
                .reload(compile(&in_mode(300, "deny")).unwrap())
                .unwrap();
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
        })
        .await;
}

/// A stream let go of while its pod's breaker was open ends only later, after the worker has
/// opened another to the same pod: its end is its own, and leaves the stream that replaced it
/// as it is — no request fails on it, and none has to open yet another.
#[tokio::test]
async fn a_stream_let_go_of_ends_without_touching_the_one_that_replaced_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_store, relay) = behind_a_relay(None).await;
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 100)],
                    std::slice::from_ref(&relay.address),
                    300,
                    "deny",
                ),
                1,
                200,
            );
            let (proxy, mut sends) = serving(&config, 1).await;
            // Connection 0, opened with the limiter, answers.
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            // Then holds its answers: an ask times out and opens the breaker.
            relay.gate(0, false);
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
            // The worker lets go of connection 0, whose end the gate holds.
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
            // The probe (connection 1) closes the breaker; a request opens connection 2.
            let open = format!(
                r#"edgerush_rate_limit_owner_breaker_open{{pod="{}"}}"#,
                relay.address
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), &open) != Some(0) {
                assert!(Instant::now() < deadline, "the breaker never closed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            assert_eq!(
                relay.accepted(),
                3,
                "the first, the probe's, and the new one"
            );
            // Connection 0's end reaches its reader now.
            let ended = relay.ended();
            relay.gate(0, true);
            while relay.ended() == ended {
                assert!(Instant::now() < deadline, "connection 0 never ended");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(statuses(&send_round(&mut sends, 3).await), [200, 200, 200]);
            assert_eq!(relay.accepted(), 3, "connection 2 carried on");
        })
        .await;
}

/// A connection the worker let go of while it was still being opened, opened only later,
/// after the worker had opened another to the same pod: it is closed, and the one that
/// replaced it carries on.
#[tokio::test]
async fn a_stream_let_go_of_while_opening_does_not_replace_the_one_after_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_store, relay) = behind_a_relay(None).await;
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 100)],
                    std::slice::from_ref(&relay.address),
                    300,
                    "deny",
                ),
                1,
                200,
            );
            let (proxy, mut sends) = serving(&config, 1).await;
            // The limiter is made, and connection 0 opened, at the first request; its gate
            // is shut as soon as it is accepted, so its opening hangs and the ask times out.
            let first = send_round(&mut sends, 1);
            let shut_early = async {
                let deadline = Instant::now() + Duration::from_secs(5);
                while relay.accepted() == 0 {
                    assert!(Instant::now() < deadline, "connection 0 never came");
                    tokio::task::yield_now().await;
                }
                relay.gate(0, false);
            };
            let (first, ()) = tokio::join!(first, shut_early);
            assert_eq!(statuses(&first), [503]);
            // The worker lets go of connection 0 while it is still opening.
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
            let open = format!(
                r#"edgerush_rate_limit_owner_breaker_open{{pod="{}"}}"#,
                relay.address
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), &open) != Some(0) {
                assert!(Instant::now() < deadline, "the breaker never closed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            assert_eq!(
                relay.accepted(),
                3,
                "the first, the probe's, and the new one"
            );
            // Connection 0 opens now: it is closed, and connection 2 kept.
            relay.gate(0, true);
            while !relay.has_ended(0) {
                assert!(Instant::now() < deadline, "connection 0 was kept");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!relay.has_ended(2), "connection 2 let go of");
            assert_eq!(statuses(&send_round(&mut sends, 2).await), [200, 200]);
            assert_eq!(relay.accepted(), 3);
        })
        .await;
}

/// A reload that changes the synchronous section, or leaves the mode, has each worker let
/// go of its old limiter at its next sweep, with no request needed: its streams to the pods
/// close, and with them its hold on the old process.
#[tokio::test]
async fn a_reload_lets_go_of_the_old_limiter_and_its_streams() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_store, relay) = behind_a_relay(None).await;
            let (upstream, _) = counting_upstream().await;
            let in_mode = |timeout_ms| {
                with_breaker(
                    synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 100)],
                        std::slice::from_ref(&relay.address),
                        timeout_ms,
                        "deny",
                    ),
                    5,
                    60_000,
                )
            };
            let (proxy, mut sends, fronts) = serving_at(&in_mode(200), 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            assert_eq!(relay.accepted(), 1);
            // The section changed: the old limiter's stream ends, though nothing asks.
            proxy.reload(compile(&in_mode(300)).unwrap()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !relay.has_ended(0) {
                assert!(Instant::now() < deadline, "the old limiter was kept");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // The client's connection went idle meanwhile: another.
            let mut sends = vec![h2_library_client(fronts[0], &::h2::client::Builder::new()).await];
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            assert_eq!(relay.accepted(), 2, "the new limiter's stream");
            // The mode left: the limiter goes, and its stream with it.
            let mut local_mode = in_mode(300);
            local_mode.rate_limiting = Some(edgerush_config::RateLimiting {
                mode: edgerush_config::RateLimitMode::Local,
                max_key_bytes: None,
                service: None,
                synchronous: None,
                experimental_header_keys: None,
            });
            proxy.reload(compile(&local_mode).unwrap()).unwrap();
            while !relay.has_ended(1) {
                assert!(Instant::now() < deadline, "the limiter outlived its mode");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
}

/// A relay in front of a pod, at a loopback IP of its own, that drops every connection a
/// data plane makes to it — from 127.0.0.1, where the pods dial from theirs — once told:
/// a data plane cut off from one pod alone (membership-design.md, F12).
struct CutRelay {
    address: String,
    cut: Arc<std::sync::atomic::AtomicBool>,
}

impl CutRelay {
    async fn at(ip: IpAddr, to: SocketAddr) -> Self {
        let listener = TcpListener::bind((ip, 0)).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cutting = Arc::clone(&cut);
        tokio::spawn(async move {
            while let Ok((mut client, from)) = listener.accept().await {
                if cutting.load(Ordering::Relaxed) && from.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST) {
                    continue;
                }
                tokio::spawn(async move {
                    if let Ok(mut server) = TcpStream::connect(to).await {
                        let _ended = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    }
                });
            }
        });
        Self { address, cut }
    }
}

/// A data plane cut off from a key's owner alone, the pods hearing each other, sends the key
/// through another pod, which forwards it: counted once, at the owner, not left to
/// `when_unreachable`.
#[tokio::test]
async fn a_key_whose_owner_is_cut_off_from_the_data_plane_is_forwarded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut relays = Vec::new();
            let mut listeners = Vec::new();
            let mut direct = Vec::new();
            for at in 0..3_u8 {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let to = listener.local_addr().unwrap();
                let relay = CutRelay::at(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 20 + at)), to).await;
                listeners.push((listener, relay.address.clone()));
                direct.push(to.to_string());
                relays.push(relay);
            }
            let store = Store::formed(listeners, &store_config(), None).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 5)],
                &store.addresses(),
                1_000,
                "deny",
            );
            let (_, key) = keys_of(&config).remove(0);
            let owner = store.owner_of(&key).await;
            relays[owner].cut.store(true, Ordering::Relaxed);
            let (_proxy, mut sends) = serving(&config, 1).await;
            // The first request makes the worker's limiter, whose seeds tell it the table as
            // their streams open; asked of a seed before that, it is forwarded all the same.
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            // The table in, the next is asked of its owner, never reached, and sent another way.
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            // Each counted once, at the owner: three left of its five.
            assert_eq!(ask_directly(&direct[owner], &key, 5).await, 3);
        })
        .await;
}

/// A key whose owner is gone, not yet failed over, is forwarded through a pod that serves,
/// and times out there: no failure of that pod's, whose breaker stays closed for its own keys,
/// while the gone owner's opens.
#[tokio::test]
async fn a_timeout_forwarding_to_an_owner_gone_opens_no_breaker_but_the_owners() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut listeners = Vec::new();
            for _ in 0..3 {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                listeners.push((listener, address));
            }
            // Slow to take a pod out, so that the one stopped keeps its shards.
            let store = Store::formed(listeners, &holding_config(), None).await;
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
                synchronous_in(
                    everything_config(upstream),
                    &[("per-client", 100)],
                    &store.addresses(),
                    200,
                    "deny",
                ),
                2,
                60_000,
            );
            let (_, key) = keys_of(&config).remove(0);
            let owner = store.owner_of(&key).await;
            let (proxy, mut sends) = serving(&config, 1).await;
            // The worker's table in, from its first request.
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            tokio::time::sleep(Duration::from_millis(300)).await;
            store.stop(owner).await;
            assert_eq!(statuses(&send_round(&mut sends, 4).await), [503; 4]);
            let scrape = proxy.metrics();
            let open = |address: &str| {
                metric(
                    &scrape,
                    &format!(r#"edgerush_rate_limit_owner_breaker_open{{pod="{address}"}}"#),
                )
            };
            assert_eq!(open(&store.pods[owner].address), Some(1), "{scrape}");
            let forwarders: Vec<&ServedPod> = (0..3)
                .filter(|at| *at != owner)
                .map(|at| &store.pods[at])
                .collect();
            let timed_out: u64 = forwarders
                .iter()
                .filter_map(|pod| metric(&scrape, &asks(&pod.address, "timed_out")))
                .sum();
            assert!(timed_out >= 2, "forwarded and timed out: {scrape}");
            for pod in forwarders {
                assert_ne!(open(&pod.address), Some(1), "{}: {scrape}", pod.address);
            }
        })
        .await;
}
