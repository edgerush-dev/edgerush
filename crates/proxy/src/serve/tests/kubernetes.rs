//! The namespaces discovered upstreams are in, followed through a fake API server.

use super::*;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};

/// A config whose upstream `web` follows `service`'s endpoints.
fn discovering(service: &str) -> Compiled {
    let yaml = format!(
        "listeners: {{}}\nroutes: []\nupstreams:\n  web: {{ load_balancer: p2c, endpoints: {{ kubernetes: \"{service}\" }} }}\n"
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    compile(&config).unwrap()
}

/// A list's last page, of one slice of Service `service` in `namespace`, its port `http`
/// on 8080, of a ready endpoint at `address`.
fn page(namespace: &str, service: &str, address: &str) -> Answer {
    answer(
        200,
        &format!(
            r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{{"resourceVersion":"5"}},"items":[{{"metadata":{{"name":"{service}-1","namespace":"{namespace}","resourceVersion":"5","labels":{{"kubernetes.io/service-name":"{service}"}}}},"addressType":"IPv4","ports":[{{"name":"http","port":8080}}],"endpoints":[{{"addresses":["{address}"]}}]}}]}}"#
        ),
    )
}

/// A watch that stays open and quiet.
fn open() -> Answer {
    Answer {
        status: 200,
        chunks: vec![Bytes::from_static(b"\n")],
        pause: Duration::from_secs(60),
    }
}

/// The addresses the running config's upstream `web` is served at.
fn served(proxy: &Proxy) -> Vec<String> {
    let snapshot = proxy.current.load();
    (0..)
        .map_while(|at| snapshot.destinations.at(0, at))
        .map(|identity| identity.address().to_string())
        .collect()
}

/// Waits until `done` holds, or fails the test.
async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never: {what}"));
}

/// The namespace a config's upstream is in is followed, and its endpoints served; a reload
/// that names another namespace follows that one instead, and forgets the first; and what
/// fails is said, with its namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_namespaces_a_config_names_are_followed_and_served() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            open()
        } else if path.contains("/namespaces/shop/") {
            page("shop", "web", "10.0.0.1")
        } else if path.contains("/namespaces/blog/") {
            page("blog", "api", "10.0.1.1")
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("proxy-follows", "secret"), Timing::default());
    let proxy = Arc::new(
        Proxy::new(
            discovering("web.shop.svc.cluster.local:http"),
            NonZeroUsize::MIN,
        )
        .unwrap(),
    );
    assert_eq!(
        proxy.unlisted(),
        ["shop"],
        "a namespace listed before it was"
    );
    let said: Rc<RefCell<Vec<String>>> = Rc::default();
    let heard = Rc::clone(&said);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(Arc::clone(&proxy).follow_kubernetes(
                api,
                move |namespace, failure| {
                    heard.borrow_mut().push(format!("{namespace}: {failure}"));
                },
            ));
            until("shop's endpoints served", || {
                served(&proxy) == ["10.0.0.1:8080"]
            })
            .await;
            assert!(proxy.unlisted().is_empty());

            proxy
                .reload(discovering("api.blog.svc.cluster.local:http"))
                .unwrap();
            until("blog's endpoints served", || {
                served(&proxy) == ["10.0.1.1:8080"]
            })
            .await;
            let followed = proxy
                .discovered
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(followed, ["blog"], "shop was not forgotten");

            proxy
                .reload(discovering("web.broken.svc.cluster.local:http"))
                .unwrap();
            until("broken's failure said", || {
                said.borrow()
                    .iter()
                    .any(|said| said.starts_with("broken: "))
            })
            .await;
            assert!(served(&proxy).is_empty());
        })
        .await;
    let lists: Vec<String> = fake
        .seen()
        .into_iter()
        .filter(|seen| !seen.path.contains("watch=true"))
        .map(|seen| seen.path)
        .collect();
    assert!(lists[0].contains("/namespaces/shop/"), "{lists:?}");
    assert!(lists[1].contains("/namespaces/blog/"), "{lists:?}");
}
