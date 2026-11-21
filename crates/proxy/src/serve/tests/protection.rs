//! Backend protection ([25](../../../../docs/25-backend-protection.md)): each endpoint's
//! requests in flight, every worker's together, held to the upstream's bound from the pick
//! to the end of the exchange.

use super::*;

mod scenarios;

/// `everything_config`'s upstream `up` at `upstream`, protected to `cap` in flight.
fn protected(upstream: SocketAddr, cap: u32) -> Compiled {
    let mut config = everything_config(upstream);
    config.upstreams.get_mut("up").unwrap().protection = Some(edgerush_config::Protection {
        max_in_flight: std::num::NonZeroU32::new(cap).unwrap(),
        queue: None,
    });
    compile(&config).unwrap()
}

/// How many requests `up`'s first endpoint has in flight, every worker's together, as the
/// proxy's current config counts them.
fn in_flight(proxy: &Proxy) -> u32 {
    let snapshot = proxy.current.load();
    snapshot.protected[0]
        .as_ref()
        .expect("`up` is protected")
        .counts[0]
        .in_flight()
}

/// A worker of `proxy` serving a listener of its own, and where.
async fn serving_from(proxy: &Arc<Proxy>) -> (SocketAddr, Rc<Worker>) {
    let worker = Worker::with_deadlines(Arc::clone(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A request for `path` as a client would write it, asking for the connection to be
/// closed after the answer.
fn asking(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n").into_bytes()
}

/// An endpoint at its bound takes no more from any worker: the count is every worker's
/// together, and a request that finds it full is answered 503, never 429, named as
/// protection's (25 §5, §8). Its slot given back, the next request goes through.
#[test]
fn an_endpoint_at_its_bound_takes_nothing_more_from_any_worker() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (first, _one) = serving_from(&proxy).await;
        let (second, two) = serving_from(&proxy).await;

        // One request in flight on the first worker, which the upstream never answers.
        let mut waiting = TcpStream::connect(first).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;
        assert_eq!(in_flight(&proxy), 1);

        // The second worker has nothing in flight of its own, and is refused all the same.
        assert_eq!(
            status_of(second, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(in_flight(&proxy), 1, "a refusal takes no slot");
        let line =
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"backend_full\"} 1\n";
        let scrape = two.proxy().metrics();
        assert!(
            scrape.contains(line),
            "the refusal is protection's: {scrape}"
        );

        // The first request ends: its slot comes back, and the next request goes through.
        drop(waiting);
        held.borrow_mut().clear();
        until(|| in_flight(&proxy) == 0).await;
        assert_eq!(status_of(second, "/ok").await, StatusCode::OK);
        assert_eq!(in_flight(&proxy), 0);
    }));
}

/// Every way out of an exchange gives its slot back, as it gives its place back
/// (`resources::every_way_out_of_an_exchange_gives_its_place_back`): an upstream that
/// could not be reached, an answer with no body, one read to its end, one the client
/// stopped reading, one that failed part way, and a client that went before anything came
/// back (25 §6). A slot one of them kept would hold the endpoint full for ever.
#[test]
fn every_way_out_of_an_exchange_gives_its_slot_back() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let given_back = |proxy: &Arc<Proxy>, case: &str| {
            let proxy = Arc::clone(proxy);
            let case = case.to_owned();
            async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while in_flight(&proxy) != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{case}: the slot was kept"));
            }
        };

        // Nothing listening where the upstream should be: a connect that failed held no
        // slot once it failed, and the next request finds the endpoint free (a 502 again,
        // not protection's 503).
        let (_refusing, nowhere) = refusing();
        let proxy = Arc::new(Proxy::new(protected(nowhere, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);
        given_back(&proxy, "unreachable").await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);

        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;

        assert_eq!(status_of(front, "/nothing").await, StatusCode::NO_CONTENT);
        given_back(&proxy, "no body").await;

        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        given_back(&proxy, "a whole body").await;

        // Taken as far as the answer's first bytes, then no further.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/endless")).await.unwrap();
        let mut some = [0; 1024];
        let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut some))
            .await
            .expect("no answer");
        assert!(read.unwrap() > 0, "closed before answering");
        assert_eq!(in_flight(&proxy), 1, "not counted while answering");
        drop(client);
        given_back(&proxy, "a client that stopped reading").await;

        // The head says ten bytes, and five come before the upstream goes.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/short")).await.unwrap();
        let mut rest = Vec::new();
        let _ended = tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
            .await
            .expect("the failed answer was never ended");
        given_back(&proxy, "a body that failed").await;

        // Asked, and gone before the upstream says anything.
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;
        assert_eq!(in_flight(&proxy), 1, "not counted while waiting");
        drop(client);
        given_back(&proxy, "a client that went").await;
    }));
}

/// A client gone while its request's connect is still being made: the worker carries the
/// connect on to its end, with the request's place, but no request will go on it, so its
/// slot comes back at once (25 §6).
#[test]
fn a_client_gone_mid_connect_gives_its_slot_back_at_once() {
    use crate::upstream::dial::hold_connects;
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _) = counting_upstream().await;
        hold_connects(upstream, Some(Duration::from_secs(60)));
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, worker) = serving_from(&proxy).await;
        let mut client = TcpStream::connect(front).await.unwrap();
        client.write_all(&asking("/ok")).await.unwrap();
        until(|| in_flight(&proxy) == 1).await;
        drop(client);
        until(|| worker.watcher.watched() == 1).await;
        assert_eq!(worker.places.held(), 1, "the place goes with the connect");
        assert_eq!(in_flight(&proxy), 0, "the slot does not");
        hold_connects(upstream, None);
    }));
}

/// A reload swaps the config, not the requests in flight: an endpoint the new config keeps
/// keeps its count, so the bound holds across it with no burst, and a raised bound admits
/// at once (25 §3, §7).
#[test]
fn a_reload_keeps_an_endpoints_count() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let proxy = Arc::new(Proxy::new(protected(upstream, 1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        let mut waiting = TcpStream::connect(front).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;

        // The same config again: a new snapshot, the same count.
        proxy.reload(protected(upstream, 1)).unwrap();
        assert_eq!(in_flight(&proxy), 1);
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE,
            "a reload let a second request through"
        );

        // A raised bound admits at once, counted against what is in flight.
        proxy.reload(protected(upstream, 2)).unwrap();
        let mut also = TcpStream::connect(front).await.unwrap();
        also.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 2).await;
        assert_eq!(in_flight(&proxy), 2);
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        drop((waiting, also));
        held.borrow_mut().clear();
        until(|| in_flight(&proxy) == 0).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
    }));
}

/// A pod at its bound is passed over: requests from any worker go to the one with room, and
/// only with both full is the backend refused (25 §4). Neither is set aside for being full:
/// once their requests end, both take requests again.
#[test]
fn a_full_pod_is_passed_over_for_one_with_room() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (a, held_at_a) = scripted_upstream().await;
        let (b, held_at_b) = scripted_upstream().await;
        let held = || held_at_a.borrow().len() + held_at_b.borrow().len();
        let mut config = everything_config(a);
        let up = config.upstreams.get_mut("up").unwrap();
        up.endpoints = edgerush_config::Endpoints::Listed(vec![a, b]);
        up.protection = Some(edgerush_config::Protection {
            max_in_flight: std::num::NonZeroU32::MIN,
            queue: None,
        });
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (first, _one) = serving_from(&proxy).await;
        let (second, _two) = serving_from(&proxy).await;
        let all_in_flight = || {
            let snapshot = proxy.current.load();
            let protected = snapshot.protected[0].as_ref().expect("`up` is protected");
            protected
                .counts
                .iter()
                .map(|count| count.in_flight())
                .sum::<u32>()
        };

        // One pod holds a request from the first worker, which it never answers: each of
        // the second worker's goes to the other.
        let mut waiting = TcpStream::connect(first).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held() == 1).await;
        for _ in 0..10 {
            assert_eq!(status_of(second, "/ok").await, StatusCode::OK);
        }

        // Both full: refused, from either worker.
        let mut also = TcpStream::connect(second).await.unwrap();
        also.write_all(&asking("/silent")).await.unwrap();
        until(|| held() == 2).await;
        assert_eq!((held_at_a.borrow().len(), held_at_b.borrow().len()), (1, 1));
        for front in [first, second] {
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        assert_eq!(all_in_flight(), 2, "a refusal takes no slot");
        let snapshot = proxy.current.load();
        assert!(
            snapshot
                .destinations
                .of(0)
                .iter()
                .all(|destination| destination.serves()),
            "a full pod was set aside"
        );

        // Their requests ended, both take again.
        drop((waiting, also));
        held_at_a.borrow_mut().clear();
        held_at_b.borrow_mut().clear();
        until(|| all_in_flight() == 0).await;
        let mut again: Vec<TcpStream> = Vec::new();
        for front in [first, second] {
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/silent")).await.unwrap();
            again.push(client);
        }
        until(|| held() == 2).await;
        assert_eq!((held_at_a.borrow().len(), held_at_b.borrow().len()), (1, 1));
    }));
}

/// `config` with its upstream `up` protected to `cap` in flight.
fn protecting(mut config: Config, cap: u32) -> Config {
    config.upstreams.get_mut("up").unwrap().protection = Some(edgerush_config::Protection {
        max_in_flight: std::num::NonZeroU32::new(cap).unwrap(),
        queue: None,
    });
    config
}

/// `config` with its first route's first rule under `per-client`, `burst` an hour for each
/// client, in local mode.
fn limited_locally(mut config: Config, burst: u64) -> Config {
    config.routes[0].rules[0].rate_limits = vec!["per-client".to_owned()];
    let limits = format!(
        "limits:\n  - {{ name: per-client, key: client, model: {{ bucket: {{ rate: {burst}/h, burst: {burst} }} }} }}\n"
    );
    config.limits = Some(serde_saphyr::from_str(&limits).unwrap());
    config.rate_limiting = Some(edgerush_config::RateLimiting {
        mode: edgerush_config::RateLimitMode::Local,
        max_key_bytes: None,
        synchronous: None,
        experimental_header_keys: None,
        service: None,
    });
    config
}

/// A data plane of one worker for `config`, its rate limits started.
fn started(config: &Config) -> Arc<Proxy> {
    let proxy = Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap();
    proxy.start_rate_limits(|| {}).unwrap();
    Arc::new(proxy)
}

/// The line of `scrape` that begins `name`, and its value.
fn metric(scrape: &str, name: &str) -> Option<u64> {
    scrape
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

/// A backend with no endpoint with room refuses before the rate limits are charged: the
/// refusal names protection, says nothing of when to come back and nothing of a limit, and
/// leaves the client's quota as it was — two an hour, both still there for the requests
/// that reach the backend (25 §5, §8).
#[test]
fn a_full_backend_is_refused_before_its_rate_limits_are_charged() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let config = limited_locally(protecting(everything_config(upstream), 1), 2);
        let proxy = started(&config);
        let (front, worker) = serving_from(&proxy).await;

        // The first request takes the one slot, and one of the two an hour.
        let mut waiting = TcpStream::connect(front).await.unwrap();
        waiting.write_all(&asking("/silent")).await.unwrap();
        until(|| held.borrow().len() == 1).await;

        let answer = h1_answer(front, &asking("/ok")).await;
        assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
        let head = answer.to_ascii_lowercase();
        assert!(!head.contains("\r\nretry-after:"), "{answer}");
        assert!(!head.contains("\r\nratelimit"), "{answer}");
        let scrape = worker.proxy().metrics();
        let refused =
            r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_full"}"#;
        assert_eq!(metric(&scrape, refused), Some(1), "{scrape}");
        let admitted =
            r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="admitted"}"#;
        assert_eq!(metric(&scrape, admitted), Some(1), "charged: {scrape}");

        // The first ends: the second of the two an hour is still there, and then none is.
        drop(waiting);
        held.borrow_mut().clear();
        until(|| in_flight(&proxy) == 0).await;
        assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        assert_eq!(status_of(front, "/ok").await, StatusCode::TOO_MANY_REQUESTS);
    }));
}

/// A retry that finds every endpoint full is not sent: the client is given the answer the
/// first try had, and the retry is counted as refused for a full backend (25 §5). One
/// endpoint, held to one: the first try's slot is held until a retry has its own. A retry
/// never waits in a queue, if the backend has one.
#[tokio::test]
async fn a_retry_to_a_full_backend_is_not_sent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
          for queued in [false, true] {
            let (upstream, sent) = statuses_upstream(vec![503, 200]).await;
            let mut config = if queued {
                queueing(everything_config(upstream), 1, 4, 10_000)
            } else {
                protecting(everything_config(upstream), 1)
            };
            config.routes[0].rules[0]
                .forward
                .as_mut()
                .expect("the rule forwards")
                .retry = Some(retrying(1, &[503], &[], 1));
            let (front, worker) = serving_config(&config).await;
            assert_eq!(
                status_of(front, "/").await,
                StatusCode::SERVICE_UNAVAILABLE,
                "queued {queued}: the first try's answer"
            );
            assert_eq!(sent.borrow().len(), 1, "queued {queued}: the retry was sent");
            let scrape = worker.proxy().metrics();
            let full =
                r#"edgerush_upstream_retries_refused_total{upstream="up",reason="backend_full"}"#;
            assert_eq!(metric(&scrape, full), Some(1), "queued {queued}: {scrape}");
            let ours =
                r#"edgerush_listener_local_answers_total{listener="web",reason="backend_full"}"#;
            assert_eq!(
                metric(&scrape, ours).unwrap_or(0),
                0,
                "queued {queued}: the 503 was the backend's"
            );
            let waited = r#"edgerush_upstream_protection_queued_total{upstream="up"}"#;
            assert_eq!(metric(&scrape, waited), Some(0), "queued {queued}: {scrape}");
          }
        })
        .await;
}

/// A mirror's copy that finds the mirror's backend full is not sent, and the request goes
/// on untouched; the copy is counted as given up for a full backend (25 §5). Under a rate
/// limit too, where a copy looks first and takes its slot once the request is admitted;
/// and never waiting in the mirror's queue, if it has one.
#[test]
fn a_copy_to_a_full_mirror_is_not_sent() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        for (limited, queued) in [(false, false), (true, false), (false, true)] {
            let (primary, _) = counting_upstream().await;
            let (copies, held) = scripted_upstream().await;
            let mut config = everything_config(primary);
            let mut mirror = config.upstreams["up"].clone();
            mirror.endpoints = edgerush_config::Endpoints::Listed(vec![copies]);
            mirror.protection = Some(edgerush_config::Protection {
                max_in_flight: std::num::NonZeroU32::MIN,
                queue: queued.then_some(edgerush_config::ProtectionQueue {
                    size: std::num::NonZeroU32::MIN,
                    timeout_ms: std::num::NonZeroU64::new(10_000).unwrap(),
                }),
            });
            config.upstreams.insert("copies".to_owned(), mirror);
            config.routes[0].rules[0].filters.push(
                serde_saphyr::from_str(
                    "{ type: request_mirror, upstream: copies, fraction: { numerator: 1, denominator: 1 } }",
                )
                .unwrap(),
            );
            if limited {
                config = limited_locally(config, 10);
            }
            let (front, worker) = serving_from(&started(&config)).await;

            // A copy of the first request holds the mirror's one slot, never answered.
            let mut first = TcpStream::connect(front).await.unwrap();
            first.write_all(&asking("/silent")).await.unwrap();
            until(|| held.borrow().len() == 1).await;

            assert_eq!(
                status_of(front, "/silent").await,
                StatusCode::OK,
                "limited {limited}, queued {queued}"
            );
            let scrape = worker.proxy().metrics();
            let full = r#"edgerush_upstream_mirrors_given_up_total{upstream="copies",reason="backend_full"}"#;
            assert_eq!(
                metric(&scrape, full),
                Some(1),
                "limited {limited}, queued {queued}: {scrape}"
            );
            assert_eq!(
                held.borrow().len(),
                1,
                "limited {limited}, queued {queued}: a second copy was sent"
            );
            let waited = r#"edgerush_upstream_protection_queued_total{upstream="copies"}"#;
            assert_eq!(metric(&scrape, waited), Some(0), "queued {queued}: {scrape}");
        }
    }));
}

/// Protection's own metrics, per upstream and only for one that is protected: the requests
/// in flight to its endpoints, every worker's, the refusals for a full backend, its queue
/// and its queue's wait, the requests queued, and the picks that passed over a full
/// endpoint (25 §8).
#[test]
fn protection_is_seen_per_upstream_and_only_where_it_is_set() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (a, held_at_a) = scripted_upstream().await;
        let (b, held_at_b) = scripted_upstream().await;
        let held = || held_at_a.borrow().len() + held_at_b.borrow().len();
        let mut config = protecting(everything_config(a), 1);
        config.upstreams.get_mut("up").unwrap().endpoints =
            edgerush_config::Endpoints::Listed(vec![a, b]);
        let mut plain = config.upstreams["up"].clone();
        plain.protection = None;
        config.upstreams.insert("plain".to_owned(), plain);
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (front, worker) = serving_from(&proxy).await;
        let seen = |name: &str| metric(&worker.proxy().metrics(), name);
        let in_flight = r#"edgerush_upstream_protection_in_flight{upstream="up"}"#;
        let passed = r#"edgerush_upstream_protection_passed_over_total{upstream="up"}"#;
        let refused =
            r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_full"}"#;
        assert_eq!(
            (seen(in_flight), seen(passed), seen(refused)),
            (Some(0), Some(0), Some(0))
        );
        for series in [
            r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_queue_full"}"#,
            r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_queue_timeout"}"#,
            r#"edgerush_upstream_protection_queued_total{upstream="up"}"#,
            r#"edgerush_upstream_protection_queue_wait_seconds_count{upstream="up"}"#,
        ] {
            assert_eq!(seen(series), Some(0), "{series}");
        }
        let scrape = worker.proxy().metrics();
        assert!(
            !scrape.contains(r#"upstream_protection_in_flight{upstream="plain"}"#),
            "{scrape}"
        );

        let mut waiting = Vec::new();
        for _ in 0..2 {
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/silent")).await.unwrap();
            waiting.push(client);
            until(|| held() == waiting.len()).await;
        }
        assert_eq!(seen(in_flight), Some(2));
        assert_eq!(
            seen(passed),
            Some(1),
            "the second found the first's pod full"
        );
        assert_eq!(
            status_of(front, "/ok").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!((seen(refused), seen(passed)), (Some(1), Some(1)));
    }));
}

/// `config` with its upstream `up` protected to `cap` in flight, a request finding every
/// endpoint full waiting in a queue of `size` for `timeout_ms` at most.
fn queueing(mut config: Config, cap: u32, size: u32, timeout_ms: u64) -> Config {
    config.upstreams.get_mut("up").unwrap().protection = Some(edgerush_config::Protection {
        max_in_flight: std::num::NonZeroU32::new(cap).unwrap(),
        queue: Some(edgerush_config::ProtectionQueue {
            size: std::num::NonZeroU32::new(size).unwrap(),
            timeout_ms: std::num::NonZeroU64::new(timeout_ms).unwrap(),
        }),
    });
    config
}

/// The protection of the upstream named `name` in `proxy`'s current config.
fn protection_of(proxy: &Proxy, name: &str) -> Arc<balancing::Protected> {
    let snapshot = proxy.current.load();
    let at = snapshot
        .config
        .upstreams()
        .iter()
        .position(|upstream| upstream.name == name)
        .expect("the config has it");
    Arc::clone(snapshot.protected[at].as_ref().expect("it is protected"))
}

/// How many requests wait in `up`'s queue, every worker's.
fn waiting(proxy: &Proxy) -> usize {
    protection_of(proxy, "up").line.waiting()
}

/// `up`'s first endpoint's slot, held as an exchange to it would hold it: what a test fills
/// the endpoint with, and gives back by dropping it.
fn hold(proxy: &Proxy) -> edgerush_protection::Slot {
    let protected = protection_of(proxy, "up");
    edgerush_protection::PodCount::take(&protected.counts[0], protected.cap)
        .expect("the endpoint has room")
}

/// What a request for `path` to `front` is answered with, sent from a task of its own.
fn sent(front: SocketAddr, path: &'static str) -> tokio::task::JoinHandle<StatusCode> {
    tokio::task::spawn_local(status_of(front, path))
}

/// A request that finds every endpoint full waits in the backend's queue, whichever worker
/// it came to, and a slot given back on any worker goes to the one that has waited
/// longest: first come, first served (25 §5).
#[test]
fn a_full_backend_queues_and_a_slot_given_back_goes_to_the_oldest_waiter() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, held) = scripted_upstream().await;
        let config = queueing(everything_config(upstream), 1, 4, 10_000);
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (first, _one) = serving_from(&proxy).await;
        let (second, two) = serving_from(&proxy).await;
        let slot = hold(&proxy);

        // The older waits on the first worker, the newer on the second.
        let older = sent(first, "/silent");
        until(|| waiting(&proxy) == 1).await;
        let newer = sent(second, "/ok");
        until(|| waiting(&proxy) == 2).await;
        assert_eq!(in_flight(&proxy), 1, "nothing taken while they wait");

        // The slot comes back: the older takes it, and the upstream holds it in turn.
        drop(slot);
        until(|| held.borrow().len() == 1).await;
        assert_eq!(waiting(&proxy), 1);
        assert!(!newer.is_finished(), "the newer waits on");

        // The older's exchange ends: its slot goes to the newer.
        held.borrow_mut().clear();
        assert_eq!(older.await.unwrap(), StatusCode::BAD_GATEWAY);
        assert_eq!(newer.await.unwrap(), StatusCode::OK);
        assert_eq!(waiting(&proxy), 0);

        let scrape = two.proxy().metrics();
        let queued = r#"edgerush_upstream_protection_queued_total{upstream="up"}"#;
        assert_eq!(metric(&scrape, queued), Some(2), "{scrape}");
        let waits = r#"edgerush_upstream_protection_queue_wait_seconds_count{upstream="up"}"#;
        assert_eq!(metric(&scrape, waits), Some(2), "{scrape}");
    }));
}

/// A full queue refuses at once, before the request's rate limits are charged: 503, named
/// as protection's, nothing of a quota (25 §5, §8). The request that waits was charged as
/// it joined.
#[test]
fn a_full_queue_is_refused_before_its_rate_limits_are_charged() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let config = limited_locally(queueing(everything_config(upstream), 1, 1, 10_000), 10);
        let proxy = started(&config);
        let (front, worker) = serving_from(&proxy).await;
        let slot = hold(&proxy);
        let queued = sent(front, "/ok");
        until(|| waiting(&proxy) == 1).await;

        let answer = h1_answer(front, &asking("/ok")).await;
        assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
        let head = answer.to_ascii_lowercase();
        assert!(!head.contains("\r\nretry-after:"), "{answer}");
        assert!(!head.contains("\r\nratelimit"), "{answer}");
        let scrape = worker.proxy().metrics();
        let refused = r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_queue_full"}"#;
        assert_eq!(metric(&scrape, refused), Some(1), "{scrape}");
        let ours = r#"edgerush_listener_local_answers_total{listener="web",reason="backend_queue_full"}"#;
        assert_eq!(metric(&scrape, ours), Some(1), "{scrape}");
        let admitted =
            r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="admitted"}"#;
        assert_eq!(
            metric(&scrape, admitted),
            Some(1),
            "only the one that waits: {scrape}"
        );

        drop(slot);
        assert_eq!(queued.await.unwrap(), StatusCode::OK);
    }));
}

/// A request that waits as long as the queue lets it is refused, 503 and charged, as the
/// backend's own 503 would be; the wait is seen (25 §5, §8).
#[test]
fn a_request_out_of_its_queues_time_is_refused_charged() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let config = limited_locally(queueing(everything_config(upstream), 1, 4, 200), 10);
        let proxy = started(&config);
        let (front, worker) = serving_from(&proxy).await;
        let _slot = hold(&proxy);

        let began = Instant::now();
        let answer = h1_answer(front, &asking("/ok")).await;
        assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
        assert!(began.elapsed() >= Duration::from_millis(200));
        assert_eq!(waiting(&proxy), 0, "out of line");
        let scrape = worker.proxy().metrics();
        let refused = r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_queue_timeout"}"#;
        assert_eq!(metric(&scrape, refused), Some(1), "{scrape}");
        let ours = r#"edgerush_listener_local_answers_total{listener="web",reason="backend_queue_timeout"}"#;
        assert_eq!(metric(&scrape, ours), Some(1), "{scrape}");
        let admitted =
            r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="admitted"}"#;
        assert_eq!(metric(&scrape, admitted), Some(1), "charged: {scrape}");
        let waits = r#"edgerush_upstream_protection_queue_wait_seconds_count{upstream="up"}"#;
        assert_eq!(metric(&scrape, waits), Some(1), "{scrape}");
    }));
}

/// The wait counts in the route's own request timeout, which ends it if it comes first,
/// answered as that timeout is anywhere: 504 (25 §5).
#[test]
fn a_routes_request_timeout_ends_a_wait_in_the_queue() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let mut config = queueing(everything_config(upstream), 1, 4, 10_000);
        config.routes[0].rules[0]
            .forward
            .as_mut()
            .expect("the rule forwards")
            .timeouts = Some(edgerush_config::Timeouts {
            request_ms: Some(200),
            backend_request_ms: None,
            tunnel_idle_ms: None,
        });
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (front, worker) = serving_from(&proxy).await;
        let _slot = hold(&proxy);

        assert_eq!(status_of(front, "/ok").await, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(waiting(&proxy), 0, "out of line");
        let scrape = worker.proxy().metrics();
        let ours = r#"edgerush_listener_local_answers_total{listener="web",reason="deadline_exceeded"}"#;
        assert_eq!(metric(&scrape, ours), Some(1), "{scrape}");
        let refused = r#"edgerush_upstream_protection_refused_total{upstream="up",reason="backend_queue_timeout"}"#;
        assert_eq!(metric(&scrape, refused), Some(0), "not the queue's: {scrape}");
    }));
}

/// A client gone while its request waits takes it out of line: the next request is the
/// one a slot given back goes to (25 §5).
#[test]
fn a_client_gone_while_queued_leaves_the_line() {
    use tokio::io::AsyncWriteExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let config = queueing(everything_config(upstream), 1, 4, 10_000);
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        let slot = hold(&proxy);

        let mut gone = TcpStream::connect(front).await.unwrap();
        gone.write_all(&asking("/ok")).await.unwrap();
        until(|| waiting(&proxy) == 1).await;
        drop(gone);
        until(|| waiting(&proxy) == 0).await;

        let next = sent(front, "/ok");
        until(|| waiting(&proxy) == 1).await;
        drop(slot);
        assert_eq!(next.await.unwrap(), StatusCode::OK);
    }));
}

/// A reload that makes room with no slot given back, a cap raised, lets the requests
/// waiting in at once (25 §7).
#[test]
fn a_raised_cap_lets_the_requests_waiting_in() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let config = |cap| compile(&queueing(everything_config(upstream), cap, 4, 10_000)).unwrap();
        let proxy = Arc::new(Proxy::new(config(1), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        let _slot = hold(&proxy);
        let waiter = sent(front, "/ok");
        until(|| waiting(&proxy) == 1).await;

        proxy.reload(config(2)).unwrap();
        assert_eq!(waiter.await.unwrap(), StatusCode::OK);
    }));
}

/// A queue made smaller, or taken away, by a reload: the requests waiting keep their
/// places and their terms, and new ones follow the new (25 §7).
#[test]
fn a_queue_made_smaller_or_taken_away_keeps_its_waiters() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        for removed in [false, true] {
            let (upstream, held) = scripted_upstream().await;
            let proxy = Arc::new(
                Proxy::new(
                    compile(&queueing(everything_config(upstream), 1, 4, 10_000)).unwrap(),
                    NonZeroUsize::MIN,
                )
                .unwrap(),
            );
            let (front, worker) = serving_from(&proxy).await;
            let slot = hold(&proxy);
            let first = sent(front, "/silent");
            until(|| waiting(&proxy) == 1).await;
            let second = sent(front, "/ok");
            until(|| waiting(&proxy) == 2).await;

            let next = if removed {
                protecting(everything_config(upstream), 1)
            } else {
                queueing(everything_config(upstream), 1, 1, 10_000)
            };
            proxy.reload(compile(&next).unwrap()).unwrap();
            assert_eq!(waiting(&proxy), 2, "removed {removed}: they wait on");
            assert_eq!(
                status_of(front, "/ok").await,
                StatusCode::SERVICE_UNAVAILABLE,
                "removed {removed}"
            );
            let reason = if removed {
                "backend_full"
            } else {
                "backend_queue_full"
            };
            let ours = format!(
                r#"edgerush_listener_local_answers_total{{listener="web",reason="{reason}"}}"#
            );
            let scrape = worker.proxy().metrics();
            assert_eq!(
                metric(&scrape, &ours),
                Some(1),
                "removed {removed}: {scrape}"
            );

            // In the order they came.
            drop(slot);
            until(|| held.borrow().len() == 1).await;
            assert!(!second.is_finished(), "removed {removed}");
            held.borrow_mut().clear();
            assert_eq!(first.await.unwrap(), StatusCode::BAD_GATEWAY);
            assert_eq!(second.await.unwrap(), StatusCode::OK, "removed {removed}");
        }
    }));
}

/// First come, first served: a request that finds others waiting waits behind them, even
/// with room to take — room a request called and yet to pick, as one ahead may be, is
/// theirs (25 §5).
#[test]
fn a_request_waits_behind_those_already_waiting_whatever_room_it_finds() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (upstream, _held) = scripted_upstream().await;
        let config = queueing(everything_config(upstream), 1, 4, 10_000);
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;

        // One waiting already, its turn yet to come.
        let line = Arc::clone(&protection_of(&proxy, "up").line);
        let ahead = edgerush_protection::Queue::join(&line, std::num::NonZeroU32::MIN).unwrap();

        let newer = sent(front, "/ok");
        until(|| waiting(&proxy) == 2).await;
        assert_eq!(in_flight(&proxy), 0, "the room is left for the one ahead");

        // The one ahead gone, the next slot given back is the newer's.
        drop(ahead);
        drop(hold(&proxy));
        assert_eq!(newer.await.unwrap(), StatusCode::OK);
    }));
}

/// A queued request's mirror copy holds nothing while the request waits: it takes its slot
/// in the mirror's backend once the request has its endpoint, and is sent with it (25 §5).
#[test]
fn a_queued_requests_copy_holds_no_slot_while_it_waits() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (primary, _) = counting_upstream().await;
        let (copies, held) = scripted_upstream().await;
        let mut config = queueing(everything_config(primary), 1, 4, 10_000);
        let mut mirror = config.upstreams["up"].clone();
        mirror.endpoints = edgerush_config::Endpoints::Listed(vec![copies]);
        mirror.protection = Some(edgerush_config::Protection {
            max_in_flight: std::num::NonZeroU32::MIN,
            queue: None,
        });
        config.upstreams.insert("copies".to_owned(), mirror);
        config.routes[0].rules[0].filters.push(
            serde_saphyr::from_str(
                "{ type: request_mirror, upstream: copies, fraction: { numerator: 1, denominator: 1 } }",
            )
            .unwrap(),
        );
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        let slot = hold(&proxy);

        let waiter = sent(front, "/silent");
        until(|| waiting(&proxy) == 1).await;
        let copied = || protection_of(&proxy, "copies").counts[0].in_flight();
        assert_eq!(copied(), 0, "the copy holds nothing while its request waits");

        drop(slot);
        assert_eq!(waiter.await.unwrap(), StatusCode::OK);
        until(|| held.borrow().len() == 1).await;
        assert_eq!(copied(), 1, "the copy went with it");
    }));
}

/// A request queued across a reload is picked on the new config when its turn comes, and
/// sent again, if its rule says to, among the new config's endpoints: not the old ones it
/// was directed among (25 §7).
#[test]
fn a_request_queued_across_a_reload_is_sent_again_among_the_new_endpoints() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let (gone, _held) = scripted_upstream().await;
        let (fresh, sent_to) = statuses_upstream(vec![503, 200]).await;
        let config = |at| {
            // Two a pod: a retry's own, beside its first try's, which it holds until the
            // retry has its pod.
            let mut config = queueing(everything_config(at), 2, 4, 10_000);
            config.routes[0].rules[0]
                .forward
                .as_mut()
                .expect("the rule forwards")
                .retry = Some(retrying(1, &[503], &[], 1));
            compile(&config).unwrap()
        };
        let proxy = Arc::new(Proxy::new(config(gone), NonZeroUsize::MIN).unwrap());
        let (front, _worker) = serving_from(&proxy).await;
        // The old endpoint full, and kept so: its count outlives the config.
        let _slots = (hold(&proxy), hold(&proxy));
        let waiter = sent(front, "/");
        until(|| waiting(&proxy) == 1).await;

        proxy.reload(config(fresh)).unwrap();
        assert_eq!(
            waiter.await.unwrap(),
            StatusCode::OK,
            "the retry went to the new endpoint"
        );
        assert_eq!(sent_to.borrow().len(), 2);
    }));
}
