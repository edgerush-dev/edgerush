//! The data plane: what a reload keeps, its listeners, and its own answers.

use super::*;

/// A config with the named upstreams, each at the address given.
fn upstreams(named: &[(&str, &str)]) -> Compiled {
    let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
    for (name, address) in named {
        yaml += &format!("  {name}: {{ load_balancer: p2c, endpoints: [\"{address}\"] }}\n");
    }
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    compile(&config).unwrap()
}

/// What a destination of the running config is filed under.
fn filed_under(proxy: &Proxy, upstream: usize) -> u64 {
    proxy
        .current
        .load()
        .destinations
        .at(upstream, 0)
        .expect("a destination")
        .key()
}

/// Reconciling happens where a config is published, so a real reload keeps what has
/// not changed and retires what has gone — not only the reconciler asked on its own.
#[test]
fn a_reload_keeps_what_has_not_changed_and_retires_what_has() {
    let proxy = Proxy::new(
        upstreams(&[("web", "127.0.0.1:1"), ("zed", "127.0.0.1:2")]),
        NonZeroUsize::MIN,
    )
    .unwrap();
    let web = filed_under(&proxy, 0);
    let zed = Arc::clone(proxy.current.load().destinations.at(1, 0).unwrap());

    // `aaa` sorts first, so every upstream after it moves along one, and `zed` goes.
    proxy
        .reload(upstreams(&[("aaa", "127.0.0.1:3"), ("web", "127.0.0.1:1")]))
        .unwrap();

    assert_eq!(filed_under(&proxy, 1), web, "web changed hands on a reload");
    assert_ne!(filed_under(&proxy, 0), web, "the newcomer took web's place");
    assert!(zed.is_retired(), "an upstream that is gone was left live");
}

/// Reads a body to its end and says whether its connection went back.
async fn drain(
    mut body: H1Body<UpstreamSocket, http_body_util::Empty<Bytes>>,
    limits: &H1Limits,
) -> (Vec<u8>, bool) {
    use http_body_util::BodyExt;

    let mut data = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(bytes) = frame.unwrap().into_data() {
            data.extend_from_slice(bytes.as_ref());
        }
    }
    match body.take_if_reusable() {
        Some(kept) => {
            kept.put_back(limits);
            (data, true)
        }
        None => (data, false),
    }
}

/// **The whole path.** A request goes out on a connection opened for it, the answer
/// is read to its end, the connection goes back, and the next request is given the
/// same one — which the upstream can see, because it was only ever accepted once.
#[tokio::test]
async fn a_connection_that_finished_carries_the_next_request_too() {
    let (upstream, opened) = counting_upstream().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let config = upstreams(&[("web", &upstream.to_string())]);
            let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN).unwrap());
            let worker = Worker::new(Arc::clone(&proxy));
            let identity = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());

            for round in 0..3 {
                let (head, body) = worker
                    .through_h1(
                        &identity,
                        &Method::GET,
                        &"/x".parse().unwrap(),
                        &http::HeaderMap::new(),
                        &[],
                        Sending::None,
                        http_body_util::Empty::<Bytes>::new(),
                        None,
                        false,
                        false,
                    )
                    .await
                    .unwrap();
                assert_eq!(head.status(), 200, "round {round}");

                let (data, went_back) = drain(*body, &worker.limits).await;
                assert_eq!(data, b"ok", "round {round}");
                assert!(went_back, "round {round} did not put its connection back");
                assert_eq!(worker.idle_connections(), 1, "round {round}");
            }

            assert_eq!(
                opened.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the upstream was given a new connection for a later request"
            );
        })
        .await;
}

fn authorities(addresses: &[&str]) -> Vec<Authority> {
    addresses
        .iter()
        .map(|address| authority(&address.parse().unwrap()).unwrap())
        .collect()
}

/// A config with nothing but the named listeners.
fn config_with(listeners: &[&str]) -> Compiled {
    let mut yaml = String::from("routes: []\nupstreams: {}\nlisteners:\n");
    for (at, name) in listeners.iter().enumerate() {
        yaml += &format!(
            "  {name}: {{ address: \"127.0.0.1:{at}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
        );
    }
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    compile(&config).unwrap()
}

#[test]
fn a_snapshot_knows_where_it_has_the_listeners_that_have_sockets() {
    let sockets = ["admin".to_owned(), "web".to_owned()];
    let metrics = Metrics::new(NonZeroUsize::MIN, sockets.len());
    let listeners = |names: &[&str]| {
        Snapshot::new(
            config_with(names),
            &sockets,
            &metrics,
            None,
            &Keys::default(),
        )
        .unwrap()
        .listeners
    };

    assert_eq!(listeners(&["admin", "web"]), [Some(0), Some(1)]);
    // Listeners are held in the order of their names, so one more moves the others.
    assert_eq!(
        listeners(&["aaa", "admin", "metrics", "web"]),
        [Some(1), Some(3)]
    );
    assert_eq!(listeners(&["web"]), [None, Some(0)]);
    assert_eq!(listeners(&["other"]), [None, None]);
}

#[test]
fn a_data_plane_serves_the_listeners_it_was_made_with() {
    let proxy = Proxy::new(config_with(&["web", "admin"]), NonZeroUsize::MIN).unwrap();
    assert_eq!(proxy.listeners(), ["admin", "web"]);
    proxy.reload(config_with(&["later"])).unwrap();
    assert_eq!(proxy.listeners(), ["admin", "web"]);
}

#[test]
fn an_endpoint_is_written_as_a_target_would_have_it() {
    assert_eq!(authorities(&["127.0.0.1:80"]), ["127.0.0.1:80"]);
    assert_eq!(authorities(&["[2001:db8::7]:8080"]), ["[2001:db8::7]:8080"]);
}

#[test]
fn the_target_keeps_its_path_and_query_at_the_endpoint() {
    let endpoint = &authorities(&["127.0.0.1:9002"])[0];
    for target in [
        "/cart/items?page=3",
        "http://shop.example.com/cart/items?page=3",
    ] {
        let target: Uri = target.parse().unwrap();
        assert_eq!(
            at_endpoint(&target, endpoint).unwrap(),
            "http://127.0.0.1:9002/cart/items?page=3"
        );
    }
}

#[test]
fn an_answer_of_our_own_is_a_status_and_nothing_else_and_is_counted() {
    let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN).unwrap();
    let answer = proxy.answer(0, Answer::NoRoute);
    assert_eq!(answer.status(), 404);
    assert!(answer.headers().is_empty());
    let counted = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"no_route\"} 1
";
    assert!(proxy.metrics().contains(counted));
}

#[test]
fn a_reload_is_counted_and_resets_nothing() {
    let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN).unwrap();
    let _counted = proxy.answer(0, Answer::NoRoute);
    assert!(proxy.metrics().contains(
        "edgerush_config_reloads_total 0
"
    ));
    assert!(proxy.metrics().contains(
        "edgerush_config_last_reload_timestamp_seconds 0
"
    ));

    proxy.reload(config_with(&["web"])).unwrap();
    let scrape = proxy.metrics();
    assert!(
        scrape.contains(
            "edgerush_config_reloads_total 1
"
        ),
        "{scrape}"
    );
    assert!(
        !scrape.contains(
            "edgerush_config_last_reload_timestamp_seconds 0
"
        ),
        "{scrape}"
    );
    assert!(
        scrape.contains(
            "reason=\"no_route\"} 1
"
        ),
        "{scrape}"
    );
}
