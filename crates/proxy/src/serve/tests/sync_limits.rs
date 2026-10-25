//! The rate limits route rules are under, in the synchronous mode (06 §4): every request
//! asked of its keys' owners, synchronous service pods on real sockets.

use super::*;
use edgerush_ratelimit::service::View;
use edgerush_ratelimit_service::grpc::Client;
use edgerush_ratelimit_service::synchronous::wire::{Decide, FromOwner, Key, ToOwner, Upstream};
use edgerush_ratelimit_service::synchronous::{SyncPod, SyncServer, Timing, decide_stream};
use std::collections::BTreeMap;
use tonic::transport::server::TcpIncoming;

/// Faster than a deployment's, so that tests settle quickly.
fn pod_timing() -> Timing {
    Timing {
        copy: Duration::from_millis(10),
        heartbeat: Duration::from_millis(20),
        silence: Duration::from_millis(150),
        settle: Duration::from_millis(60),
    }
}

/// A synchronous service pod alone, serving on `listener`.
fn lone_pod(listener: TcpListener) -> SyncPod {
    let pod = SyncPod::spawn(0, vec![0], 1, pod_timing(), BTreeMap::new());
    let server = SyncServer::new(pod.clone());
    tokio::spawn(async move {
        let _ended = tonic::transport::Server::builder()
            .serve_with_incoming(server, TcpIncoming::from(listener))
            .await;
    });
    pod
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

/// Synchronous service pods, `count` of them, each on a port of its own, once they agree
/// all serve: their addresses.
async fn sync_pods(count: u32) -> (Vec<SyncPod>, Vec<String>) {
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let clients: BTreeMap<u32, Client> = (0..count)
        .map(|pod| (pod, Client::new(&addresses[pod as usize]).unwrap()))
        .collect();
    let timing = pod_timing();
    let mut pods = Vec::new();
    for (id, listener) in (0..count).zip(listeners) {
        let peers = clients
            .iter()
            .filter(|(pod, _)| **pod != id)
            .map(|(pod, client)| (*pod, client.clone()))
            .collect();
        let pod = SyncPod::spawn(id, (0..count).collect(), 1, timing, peers);
        let server = SyncServer::new(pod.clone());
        tokio::spawn(async move {
            let _ended = tonic::transport::Server::builder()
                .serve_with_incoming(server, TcpIncoming::from(listener))
                .await;
        });
        pods.push(pod);
    }
    // Agreed and settled, so that no view changes under a test: a pod just started may say
    // a view without the others for a moment, which would move their keys to it.
    let every: Vec<u32> = (0..count).collect();
    let agreed = |pods: &[SyncPod]| {
        let (first, _) = pods[0].view();
        pods.iter()
            .all(|pod| matches!(pod.view(), (view, true) if view == first && view.pods == every))
            .then_some(first.epoch)
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "the pods never agreed");
        if let Some(epoch) = agreed(&pods) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if agreed(&pods) == Some(epoch) {
                return (pods, addresses);
            }
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// `config` with its first route's first rule under `limits`, each `(name, burst)` keyed by
/// the client at ten an hour, in the synchronous mode on the pods at `addresses`, waiting
/// `timeout_ms` for them and `when_unreachable` as said.
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
            "{{ mode: synchronous, service: {{ addresses: [{}] }}, synchronous: {{ timeout_ms: {timeout_ms}, when_unreachable: {when_unreachable}, breaker: {{ failures: 5, probe_after_ms: 1000 }} }} }}",
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
        &RefCell::new(Vec::new()),
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

/// Two workers asking one owner hold a client to its one bucket of ten; refusals say when
/// to come back, as the owner told.
#[tokio::test]
async fn the_owner_holds_a_client_to_one_bucket_across_workers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_pods, addresses) = sync_pods(1).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                &addresses,
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

/// Requests under `key` with ten an hour and a bucket of five, `count` of them, asked of
/// the pod at `address` directly: how many it admitted.
async fn ask_directly(address: &str, key: &[u8], count: u64) -> usize {
    let mut stream = decide_stream(address).await.unwrap();
    let mut admitted = 0;
    for id in 0..count {
        let decide = Decide {
            id,
            keys: vec![Key {
                key: key.to_vec(),
                contract: Some(edgerush_ratelimit_service::wire::Contract {
                    limit: 10,
                    window_ns: 3_600_000_000_000,
                    burst: 5,
                }),
            }],
        };
        stream
            .up
            .send(Upstream {
                message: Some(ToOwner::Decide(decide)),
            })
            .await
            .unwrap();
        loop {
            let down = within(stream.down.message()).await.unwrap().unwrap();
            if let Some(FromOwner::Decided(decided)) = down.message {
                admitted += usize::from(decided.admitted);
                break;
            }
        }
    }
    admitted
}

/// A request under limits owned by two pods is asked of both at once; refused by one, the
/// other's charge is given back: the limit that had room is charged only for the requests
/// admitted. The refusal names the limit that refused, whose key is the request's second.
#[tokio::test]
async fn a_charge_admitted_by_one_owner_is_given_back_when_another_refuses() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_pods, addresses) = sync_pods(2).await;
            let (upstream, _) = counting_upstream().await;
            let view = View {
                epoch: 1,
                pods: vec![0, 1],
            };
            // Two limits of equal buckets, named from these until their keys have different
            // owners. A rule's limits go smallest bucket first: equal, the second may be
            // the one that refuses, its owner's position mapped to its place among both.
            let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
            let (config, keys) = names
                .iter()
                .flat_map(|first| names.iter().map(move |second| (*first, *second)))
                .filter(|(first, second)| first != second)
                .find_map(|(first, second)| {
                    let config = synchronous_in(
                        everything_config(upstream),
                        &[(first, 5), (second, 5)],
                        &addresses,
                        1_000,
                        "deny",
                    );
                    let keys = keys_of(&config);
                    (view.owners(&keys[0].1).0 != view.owners(&keys[1].1).0)
                        .then_some((config, keys))
                })
                .expect("two limits with different owners");
            let owner_of = |key: &[u8]| addresses[view.owners(key).0.unwrap() as usize].clone();
            let ((roomy, roomy_key), (tight, tight_key)) = (&keys[0], &keys[1]);
            // The second has two left, the first all five.
            assert_eq!(ask_directly(&owner_of(tight_key), tight_key, 3).await, 3);
            let (_proxy, mut sends) = serving(&config, 1).await;
            let answers = send_round(&mut sends, 6).await;
            assert_eq!(statuses(&answers), [200, 200, 429, 429, 429, 429]);
            let named = answers[2].headers()["ratelimit"].to_str().unwrap();
            assert!(named.starts_with(&format!(r#""{tight}";"#)), "{named}");
            // The first's owner has the three it was not charged for.
            assert_eq!(
                ask_directly(&owner_of(roomy_key), roomy_key, 5).await,
                3,
                "`{roomy}` charged for the two admitted alone"
            );
        })
        .await;
}

/// An owner out of reach — its port closed, or silent past the wait — leaves each request
/// to `when_unreachable`: `allow` lets it on, `deny` answers 503 with `Retry-After: 1`,
/// never 429; a silent owner after the wait, a closed one at once.
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

/// An owner that stops is taken out of the view by the other pods — a majority of the
/// three — which say so down the worker's streams to them: its keys go to the next owner, and requests are decided again
/// rather than left to `when_unreachable`.
#[tokio::test]
async fn the_keys_of_an_owner_that_stops_go_to_the_next() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (pods, addresses) = sync_pods(3).await;
            let (upstream, _) = counting_upstream().await;
            let config = synchronous_in(
                everything_config(upstream),
                &[("per-client", 10)],
                &addresses,
                200,
                "deny",
            );
            let (_, key) = keys_of(&config).remove(0);
            let owner = View {
                epoch: 1,
                pods: vec![0, 1, 2],
            }
            .owners(&key)
            .0
            .unwrap();
            let (_proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            pods[owner as usize].stop();
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

/// An owner that does not answer opens its breaker after `failures` timeouts in a row: from
/// then on its requests are decided by `when_unreachable` at once, nothing asked of it, and
/// the metrics say so.
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
            assert_eq!(
                line(r#"edgerush_rate_limit_owner_asks_total{pod="0",outcome="timed_out"}"#),
                Some(2),
                "{scrape}"
            );
            assert_eq!(
                line(r#"edgerush_rate_limit_owner_breaker_trips_total{pod="0"}"#),
                Some(1)
            );
            assert_eq!(
                line(r#"edgerush_rate_limit_owner_breaker_open{pod="0"}"#),
                Some(1)
            );
            assert_eq!(
                line(r#"edgerush_rate_limit_unreachable_total{action="deny"}"#),
                Some(7)
            );
        })
        .await;
}

/// An open breaker is probed after `probe_after`: once its owner is back and sends its
/// view on the probe's stream, the breaker closes, and requests are asked of the owner
/// again.
#[tokio::test]
async fn an_open_breaker_closes_once_a_probe_hears_from_its_owner() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Says nothing until its gates open, the owner back. (A port given up and bound
            // again could be taken in between, as the system hands it out.)
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap();
            let _pod = lone_pod(listener);
            let relay = GatedRelay::shut(owner).await;
            let (upstream, _) = counting_upstream().await;
            let config = with_breaker(
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
            let (proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [503]);
            let open = r#"edgerush_rate_limit_owner_breaker_open{pod="0"}"#;
            assert_eq!(metric(&proxy.metrics(), open), Some(1));
            relay.open_all();
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), open) != Some(0) {
                assert!(Instant::now() < deadline, "the breaker never closed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(statuses(&send_round(&mut sends, 1).await), [200]);
            assert_eq!(
                metric(
                    &proxy.metrics(),
                    r#"edgerush_rate_limit_owner_asks_total{pod="0",outcome="answered"}"#
                ),
                Some(1)
            );
        })
        .await;
}

/// With `when_unreachable: local`, an owner out of reach leaves its keys to the process's
/// own buckets, which its workers share: a client is held to one bucket of ten across two
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
            // Says nothing until its gates open, the owner back.
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap();
            let pod = lone_pod(listener);
            let relay = GatedRelay::shut(owner).await;
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
            let open = r#"edgerush_rate_limit_owner_breaker_open{pod="0"}"#;
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), open) != Some(0) {
                assert!(Instant::now() < deadline, "the breaker never closed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(
                statuses(&send_round(&mut sends, 1).await),
                [200],
                "the owner's own bucket"
            );
            pod.stop();
            assert_eq!(
                statuses(&send_round(&mut sends, 2).await),
                [429, 429],
                "the local bucket as the last outage left it"
            );
        })
        .await;
}

/// Gives `count` charges under `key` back to the pod at `address`, directly.
async fn give_back_directly(address: &str, key: &[u8], count: usize) {
    let stream = decide_stream(address).await.unwrap();
    for _ in 0..count {
        let back = edgerush_ratelimit_service::synchronous::wire::GiveBack {
            keys: vec![Key {
                key: key.to_vec(),
                contract: Some(edgerush_ratelimit_service::wire::Contract {
                    limit: 10,
                    window_ns: 3_600_000_000_000,
                    burst: 5,
                }),
            }],
        };
        stream
            .up
            .send(Upstream {
                message: Some(ToOwner::GiveBack(back)),
            })
            .await
            .unwrap();
    }
    // Asked after them on the same stream, so answered once they are given.
    let mut stream = stream;
    let decide = Decide {
        id: 0,
        keys: vec![Key {
            key: b"settled".to_vec(),
            contract: Some(edgerush_ratelimit_service::wire::Contract {
                limit: 10,
                window_ns: 3_600_000_000_000,
                burst: 5,
            }),
        }],
    };
    stream
        .up
        .send(Upstream {
            message: Some(ToOwner::Decide(decide)),
        })
        .await
        .unwrap();
    loop {
        let down = within(stream.down.message()).await.unwrap().unwrap();
        if matches!(down.message, Some(FromOwner::Decided(_))) {
            return;
        }
    }
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
            let (_pods, live) = sync_pods(2).await;
            let closed = {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap().to_string()
            };
            let (upstream, _) = counting_upstream().await;
            let view = View {
                epoch: 1,
                pods: vec![0, 1],
            };
            // Two limits of equal buckets whose keys have different owners: the first's
            // owner answers, the second's is out of reach.
            let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
            let mut addresses = live.clone();
            let (config, keys) = names
                .iter()
                .flat_map(|first| names.iter().map(move |second| (*first, *second)))
                .filter(|(first, second)| first != second)
                .find_map(|(first, second)| {
                    let config = synchronous_in(
                        everything_config(upstream),
                        &[(first, 5), (second, 5)],
                        &live,
                        200,
                        "local",
                    );
                    let keys = keys_of(&config);
                    (view.owners(&keys[0].1).0 != view.owners(&keys[1].1).0)
                        .then_some((config, keys))
                })
                .expect("two limits with different owners");
            let ((_, answered_key), (out_of_reach, _)) = (&keys[0], &keys[1]);
            let answering = view.owners(answered_key).0.unwrap() as usize;
            addresses[1 - answering] = closed;
            let config = synchronous_in(
                Config {
                    rate_limiting: None,
                    ..config
                },
                &[(&keys[0].0, 5), (out_of_reach, 5)],
                &addresses,
                200,
                "local",
            );
            let config = with_breaker(config, 1, 60_000);
            let (_proxy, mut sends) = serving(&config, 1).await;
            assert_eq!(statuses(&send_round(&mut sends, 5).await), [200; 5]);
            // The answering owner's bucket full again; the local one spent.
            give_back_directly(&addresses[answering], answered_key, 5).await;
            let answers = send_round(&mut sends, 1).await;
            assert_eq!(statuses(&answers), [429]);
            let named = answers[0].headers()["ratelimit"].to_str().unwrap();
            assert!(
                named.starts_with(&format!(r#""{out_of_reach}";"#)),
                "{named}"
            );
            assert_eq!(
                ask_directly(&addresses[answering], answered_key, 5).await,
                5,
                "its charge given back"
            );
        })
        .await;
}

/// The process's own buckets are swept, on the probe thread: a key full and unused for a
/// second is forgotten.
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
            let keys = |proxy: &Proxy| metric(&proxy.metrics(), "edgerush_rate_limit_keys");
            let deadline = Instant::now() + Duration::from_secs(5);
            while keys(&proxy) != Some(1) {
                assert!(Instant::now() < deadline, "the key never counted");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
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
                let _pod = lone_pod(listener);
                let relay = slow_relay(owner, Duration::from_millis(300)).await;
                let (upstream, _) = counting_upstream().await;
                let config = with_breaker(
                    synchronous_in(
                        everything_config(upstream),
                        &[("per-client", 5)],
                        &[relay],
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
                    metric(
                        &proxy.metrics(),
                        r#"edgerush_rate_limit_owner_asks_total{pod="0",outcome="timed_out"}"#
                    ),
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
/// owner's breaker open, so requests are not made to wait again, and the process's own
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
                    r#"edgerush_rate_limit_owner_breaker_trips_total{pod="0"}"#
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

/// A relay to `to` whose connections each have a gate on what `to` sends: open, it passes;
/// shut, it is held until the gate opens again. Says how many connections it accepted and
/// which have ended.
struct GatedRelay {
    address: String,
    gates: Arc<std::sync::Mutex<Vec<tokio::sync::watch::Sender<bool>>>>,
    /// Whether a connection's gate is open when it is accepted.
    open_at_first: Arc<std::sync::atomic::AtomicBool>,
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    /// The connections that have ended, by the order they were accepted in.
    ended: Arc<std::sync::Mutex<Vec<usize>>>,
}

impl GatedRelay {
    async fn to(to: SocketAddr) -> Self {
        Self::opening(to, true).await
    }

    /// One whose gates are all shut until [`open_all`](Self::open_all): `to` as an owner
    /// that accepts and says nothing, until it is back.
    async fn shut(to: SocketAddr) -> Self {
        Self::opening(to, false).await
    }

    async fn opening(to: SocketAddr, open_at_first: bool) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let gates = Arc::new(std::sync::Mutex::new(Vec::new()));
        let open_at_first = Arc::new(std::sync::atomic::AtomicBool::new(open_at_first));
        let held_open_at_first = Arc::clone(&open_at_first);
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ended = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (held_gates, held_accepted, held_ended) = (
            Arc::clone(&gates),
            Arc::clone(&accepted),
            Arc::clone(&ended),
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
                let (mut from_client, mut to_client) = client.into_split();
                let (mut from_server, mut to_server) = server.into_split();
                tokio::spawn(async move {
                    let _ended = tokio::io::copy(&mut from_client, &mut to_server).await;
                });
                let ended = Arc::clone(&held_ended);
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
                    ended.lock().unwrap().push(nth);
                });
            }
        });
        Self {
            address,
            gates,
            open_at_first,
            accepted,
            ended,
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

/// A stream let go of while its owner's breaker was open ends only later, after the worker
/// has opened another to the same owner: its end is its own, and leaves the stream that
/// replaced it as it is — no request fails on it, and none has to open yet another.
#[tokio::test]
async fn a_stream_let_go_of_ends_without_touching_the_one_that_replaced_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap();
            let _pod = lone_pod(listener);
            let relay = GatedRelay::to(owner).await;
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
            let open = r#"edgerush_rate_limit_owner_breaker_open{pod="0"}"#;
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), open) != Some(0) {
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
/// after the worker had opened another to the same owner: it is closed, and the one that
/// replaced it carries on.
#[tokio::test]
async fn a_stream_let_go_of_while_opening_does_not_replace_the_one_after_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap();
            let _pod = lone_pod(listener);
            let relay = GatedRelay::to(owner).await;
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
            let open = r#"edgerush_rate_limit_owner_breaker_open{pod="0"}"#;
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric(&proxy.metrics(), open) != Some(0) {
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
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let owner = listener.local_addr().unwrap();
            let _pod = lone_pod(listener);
            let relay = GatedRelay::to(owner).await;
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
