//! The rate limits route rules are under, over HTTP/2 and HTTP/3 as over HTTP/1.1
//! (`tests/ratelimit.rs`): the request core decides them whatever a request came by, with
//! three service pods on real sockets, and refuses what is over with 429.

use super::*;
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client as PodClient, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use tonic::transport::server::TcpIncoming;

/// Three service pods, once they agree all three serve: their addresses.
async fn service() -> Vec<String> {
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
        let server = Server::new(pod.clone());
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
