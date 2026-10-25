//! The rate limits route rules are under, in local mode (06 §4): each process on what it
//! sees, no service; and with rate limiting off, none enforced.

use super::*;

/// `config` with its first route's first rule under ten requests an hour for each client,
/// a burst of ten, enforced as `section` says, or not at all if there is none.
fn limited_in(mut config: Config, section: Option<edgerush_config::RateLimiting>) -> Config {
    config.routes[0].rules[0].rate_limits = vec!["per-client".to_owned()];
    config.limits = Some(
        serde_saphyr::from_str(
            "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 10 } } }\n",
        )
        .unwrap(),
    );
    config.rate_limiting = section;
    config
}

fn local_mode() -> Option<edgerush_config::RateLimiting> {
    Some(edgerush_config::RateLimiting {
        mode: edgerush_config::RateLimitMode::Local,
        max_key_bytes: None,
        synchronous: None,
        experimental_header_keys: None,
        service: None,
    })
}

/// A data plane for `config` on `workers`, its rate limits started as the binary starts
/// them.
fn started(config: &Config, workers: NonZeroUsize) -> Proxy {
    let proxy = Proxy::new(compile(config).unwrap(), workers).unwrap();
    proxy.start_rate_limits(|| {}).unwrap();
    proxy
}

/// `count` requests over HTTP/2, alternating between the workers that `sends` reach: each
/// one's status, and the headers of every refusal.
async fn send_round(
    sends: &mut Vec<::h2::client::SendRequest<Bytes>>,
    count: usize,
) -> (Vec<u16>, Vec<http::HeaderMap>) {
    let mut statuses = Vec::new();
    let mut refusals = Vec::new();
    for _ in 0..count {
        let send = sends.remove(0);
        let mut send = within(send.ready()).await.unwrap();
        let request = Request::get("http://example.test/")
            .version(Version::HTTP_2)
            .body(())
            .unwrap();
        let (response, _) = send.send_request(request, true).unwrap();
        let response = within(response).await.unwrap();
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            refusals.push(response.headers().clone());
        }
        statuses.push(response.status().as_u16());
        sends.push(send);
    }
    (statuses, refusals)
}

/// `workers` workers of a data plane for `config`, each serving a connection of its own:
/// the data plane, and a client's connection to each.
async fn serving_workers(
    config: &Config,
    workers: usize,
) -> (Arc<Proxy>, Vec<::h2::client::SendRequest<Bytes>>) {
    let proxy = Arc::new(started(config, NonZeroUsize::new(workers).unwrap()));
    let limits = H1Limits {
        sweep: Duration::from_millis(20),
        ..H1Limits::default()
    };
    let mut sends = Vec::new();
    for position in 0..u16::try_from(workers).unwrap() {
        let worker = Worker::made(Arc::clone(&proxy), limits, SHORT, position, None);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
        sends.push(h2_library_client(front, &::h2::client::Builder::new()).await);
    }
    (proxy, sends)
}

/// Under ten an hour, a client is told to come back in six minutes, or a second more, and
/// the `RateLimit` field says the same.
fn comes_back(headers: &http::HeaderMap) {
    let after = headers["retry-after"].to_str().unwrap();
    assert!(after == "360" || after == "361", "retry-after {after}");
    assert_eq!(
        headers["ratelimit"],
        format!(r#""per-client";r=0;t={after}"#).as_str()
    );
}

/// Two workers of one process share a client's bucket in local mode: a client with a
/// connection to each is held to the one bucket of ten, refusals say when to come back,
/// and the process's one key is counted once.
#[tokio::test]
async fn local_mode_holds_a_client_to_one_bucket_across_workers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let config = limited_in(everything_config(upstream), local_mode());
            let (proxy, mut sends) = serving_workers(&config, 2).await;
            let (statuses, refusals) = send_round(&mut sends, 16).await;
            let admitted = statuses.iter().filter(|s| **s == 200).count();
            assert_eq!((admitted, refusals.len()), (10, 6), "{statuses:?}");
            refusals.iter().for_each(comes_back);
            let deadline = Instant::now() + Duration::from_secs(5);
            let scrape = loop {
                let scrape = proxy.metrics();
                if scrape.contains("edgerush_rate_limit_keys 1\n") || Instant::now() > deadline {
                    break scrape;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(scrape.contains("edgerush_rate_limit_keys 1\n"), "{scrape}");
            assert!(
                scrape.contains(
                    r#"edgerush_rate_limit_decisions_total{limit="per-client",outcome="refused"} 6"#
                ),
                "{scrape}"
            );
        })
        .await;
}

/// With rate limiting off — its section left out, or its mode `off` — a rule's limits are
/// checked but none is enforced: every request goes on, and no limit is counted.
#[tokio::test]
async fn with_rate_limiting_off_every_request_goes_on() {
    let off = Some(edgerush_config::RateLimiting {
        mode: edgerush_config::RateLimitMode::Off,
        max_key_bytes: None,
        synchronous: None,
        experimental_header_keys: None,
        service: None,
    });
    for section in [None, off] {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let config = limited_in(everything_config(upstream), section.clone());
                let (proxy, mut sends) = serving_workers(&config, 1).await;
                let (statuses, _) = send_round(&mut sends, 15).await;
                assert!(statuses.iter().all(|s| *s == 200), "{statuses:?}");
                assert!(!proxy.metrics().contains("edgerush_rate_limit"));
            })
            .await;
    }
}

/// A reload that stays in local mode keeps the keys, its cap changed or not: a client that
/// spent its bucket is still refused after it.
#[tokio::test]
async fn a_reload_in_local_mode_keeps_the_keys() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let config = limited_in(everything_config(upstream), local_mode());
            let (proxy, mut sends) = serving_workers(&config, 1).await;
            let (statuses, _) = send_round(&mut sends, 10).await;
            assert!(statuses.iter().all(|s| *s == 200), "{statuses:?}");
            let mut bigger = config.clone();
            if let Some(limiting) = &mut bigger.rate_limiting {
                limiting.max_key_bytes = Some(1 << 30);
            }
            proxy.reload(compile(&bigger).unwrap()).unwrap();
            let (statuses, _) = send_round(&mut sends, 1).await;
            assert_eq!(statuses, [429]);
        })
        .await;
}

/// `config` with its first route's first rule under `limits` (each `name: key` with ten an
/// hour and a bucket of `burst`), in local mode with header keys allowed.
fn keyed_by(mut config: Config, limits: &[(&str, &str, u64)]) -> Config {
    config.routes[0].rules[0].rate_limits = limits
        .iter()
        .map(|(name, _, _)| (*name).to_owned())
        .collect();
    let written: String = limits
        .iter()
        .map(|(name, key, burst)| {
            format!("  - {{ name: {name}, key: '{key}', model: {{ bucket: {{ rate: 10/h, burst: {burst} }} }} }}\n")
        })
        .collect();
    config.limits = Some(serde_saphyr::from_str(&format!("limits:\n{written}")).unwrap());
    config.rate_limiting = Some(edgerush_config::RateLimiting {
        mode: edgerush_config::RateLimitMode::Local,
        max_key_bytes: None,
        service: None,
        synchronous: None,
        experimental_header_keys: Some(true),
    });
    config
}

/// One request over `send`, with `x-api-key: key` if there is one: its status, and the
/// limit its `RateLimit` field names if it was refused.
async fn as_key(
    send: ::h2::client::SendRequest<Bytes>,
    key: Option<&str>,
) -> (::h2::client::SendRequest<Bytes>, u16, Option<String>) {
    let mut send = within(send.ready()).await.unwrap();
    let mut request = Request::get("http://example.test/").version(Version::HTTP_2);
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    let (response, _) = send.send_request(request.body(()).unwrap(), true).unwrap();
    let response = within(response).await.unwrap();
    let named = response.headers().get("ratelimit").map(|field| {
        field
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned()
    });
    (send, response.status().as_u16(), named)
}

/// A limit keyed by a header counts each value apart, the same client sending them all: two
/// keys each held to their bucket of two, and a request without the header let through.
#[tokio::test]
async fn a_limit_keyed_by_a_header_counts_each_value_apart() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let config = keyed_by(
                everything_config(upstream),
                &[("per-key", r#"headers["x-api-key"]"#, 2)],
            );
            let (_proxy, mut sends) = serving_workers(&config, 1).await;
            let mut send = sends.remove(0);
            let mut statuses = Vec::new();
            for key in [
                Some("a"),
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                None,
                None,
                None,
            ] {
                let (next, status, _) = as_key(send, key).await;
                send = next;
                statuses.push(status);
            }
            assert_eq!(statuses, [200, 200, 429, 200, 200, 200, 200, 200]);
        })
        .await;
}

/// A request under a limit by client and one by header is decided under both at once: the
/// client's bucket runs out across keys, and its refusal names the limit that refused.
#[tokio::test]
async fn limits_by_client_and_by_header_decide_a_request_together() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, _) = counting_upstream().await;
            let config = keyed_by(
                everything_config(upstream),
                &[
                    ("per-client", "client", 3),
                    ("per-key", r#"headers["x-api-key"]"#, 2),
                ],
            );
            let (_proxy, mut sends) = serving_workers(&config, 1).await;
            let mut send = sends.remove(0);
            let mut seen = Vec::new();
            for key in ["a", "a", "a", "b", "b"] {
                let (next, status, named) = as_key(send, Some(key)).await;
                send = next;
                seen.push((status, named));
            }
            let quoted = |name: &str| Some(format!(r#""{name}""#));
            assert_eq!(
                seen,
                [
                    (200, None),
                    (200, None),
                    // Its own key spent, the client's bucket still has one.
                    (429, quoted("per-key")),
                    (200, None),
                    // The client's three spent: refused by it, though `b` has one left.
                    (429, quoted("per-client")),
                ]
            );
        })
        .await;
}
