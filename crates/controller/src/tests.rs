use super::*;
use bytes::Bytes;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use k8s_openapi::ByteString;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// A data plane of class `edge` with an `http` and an `https` listener, at `http` and
/// `https`.
fn plane_at(http: &str, https: &str) -> Plane {
    serde_saphyr::from_str(&format!(
        r#"
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: false
listeners:
  http:
    address: "{http}"
    protocol: http
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
  https:
    address: "{https}"
    protocol: https
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
    tls: {{ certificates: [], without_sni: none }}
"#
    ))
    .unwrap()
}

fn plane() -> Plane {
    plane_at("[::]:8080", "[::]:8443")
}

/// IngressClass `edge`, which names this controller.
fn class() -> IngressClass {
    serde_saphyr::from_str(
        "{ metadata: { name: edge }, spec: { controller: edgerush.dev/ingress-controller } }",
    )
    .unwrap()
}

/// Ingress `name` in `namespace` of class `edge`, its `/` on `host` to Service `web`'s port
/// 80, the host secured by Secret `tls`.
fn ingress(namespace: &str, name: &str, host: &str) -> Ingress {
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: {namespace}, resourceVersion: '6' }}
spec:
  ingressClassName: edge
  tls: [{{ hosts: [{host}], secretName: tls }}]
  rules:
    - host: {host}
      http:
        paths:
          - {{ path: /, pathType: Prefix, backend: {{ service: {{ name: web, port: {{ number: 80 }} }} }} }}
"
    ))
    .unwrap()
}

/// Service `web` in `namespace`, its port `http` 80.
fn service(namespace: &str) -> Service {
    serde_saphyr::from_str(&format!(
        "{{ metadata: {{ name: web, namespace: {namespace} }}, spec: {{ ports: [{{ name: http, port: 80 }}] }} }}"
    ))
    .unwrap()
}

/// Secret `tls` in `namespace`, of a certificate for `hosts`, as the API server sends one:
/// in `data`.
fn secret(namespace: &str, hosts: &[&str]) -> Secret {
    let certificate = edgerush_certificates::testing::certificate(hosts);
    let mut secret: Secret = serde_saphyr::from_str(&format!(
        "{{ metadata: {{ name: tls, namespace: {namespace} }}, type: kubernetes.io/tls }}"
    ))
    .unwrap();
    secret.data = Some(BTreeMap::from([
        (
            "tls.crt".to_owned(),
            ByteString(certificate.chain.into_bytes()),
        ),
        (
            "tls.key".to_owned(),
            ByteString(certificate.key.into_bytes()),
        ),
    ]));
    secret
}

/// A list's one page, of `items`.
fn page<K: Kind + Serialize>(items: &[K]) -> Answer {
    let body = serde_json::json!({
        "apiVersion": K::API_VERSION,
        "kind": format!("{}List", K::KIND),
        "metadata": { "resourceVersion": "5" },
        "items": items,
    });
    answer(200, &body.to_string())
}

/// A watch that stays open and quiet.
fn open() -> Answer {
    Answer {
        status: 200,
        chunks: vec![Bytes::from_static(b"\n")],
        pause: Duration::from_secs(60),
    }
}

/// The names of a translation's routes.
fn routes(translation: &Translation) -> Vec<String> {
    translation
        .config
        .routes
        .iter()
        .map(|route| route.name.clone())
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

/// What the control plane said, a line each, and its translations.
#[derive(Default)]
struct Heard {
    lines: Vec<String>,
    translations: Vec<Translation>,
}

fn hear(heard: &Rc<RefCell<Heard>>) -> impl Fn(Said<'_>) + 'static {
    let heard = Rc::clone(heard);
    move |said| {
        let mut heard = heard.borrow_mut();
        let line = match said {
            Said::Waiting(unlisted) => format!("waiting on {}", unlisted.join(", ")),
            Said::Failed { followed, failure } => format!("{followed}: {failure}"),
            Said::Translated(translation) => {
                heard.translations.push(translation.clone());
                "translated".to_owned()
            }
            Said::Refused { problems, .. } => format!("refused: {problems:?}"),
        };
        heard.lines.push(line);
    }
}

#[test]
fn changes_are_translated_once_quiet_and_never_later_than_the_most_after_the_first() {
    let debounce = Debounce::default();
    let start = Instant::now();
    let at = |ms| start + Duration::from_millis(ms);
    let mut waiting = Waiting::default();
    assert_eq!(waiting.due(debounce), None, "due with no change");
    waiting.changed(at(0));
    assert_eq!(waiting.due(debounce), Some(at(100)));
    waiting.changed(at(80));
    assert_eq!(
        waiting.due(debounce),
        Some(at(180)),
        "not waited out afresh"
    );
    for ms in (160..=480).step_by(80) {
        waiting.changed(at(ms));
    }
    assert_eq!(
        waiting.due(debounce),
        Some(at(500)),
        "waited on past the most"
    );
}

/// A follow of `objects`, listed whole or not.
fn follow_of<K>(name: &str, listed: bool, objects: Vec<K>) -> Follow<K> {
    Follow {
        name: name.to_owned(),
        objects: watch::channel(Arc::new(Listed { listed, objects })).1,
    }
}

#[test]
fn every_follows_objects_are_translated_and_its_secrets_looked_at() {
    let followed = Followed {
        ingress_classes: vec![follow_of("classes", true, vec![class()])],
        ingresses: vec![
            follow_of(
                "ingresses in shop",
                true,
                vec![ingress("shop", "web", "shop.example")],
            ),
            follow_of("ingresses in blog", false, Vec::new()),
        ],
        services: vec![
            follow_of("services in shop", false, vec![service("shop")]),
            follow_of("services in blog", true, vec![service("blog")]),
        ],
        secrets: vec![follow_of(
            "secrets in shop",
            true,
            vec![secret("shop", &[])],
        )],
    };
    assert_eq!(
        followed.unlisted(),
        ["ingresses in blog", "services in shop"]
    );
    let cluster = followed.cluster();
    assert!(!cluster.secrets_assumed, "Secrets taken unseen");
    let namespace = |metadata: &ObjectMeta| metadata.namespace.clone().unwrap_or_default();
    assert_eq!(cluster.ingress_classes.len(), 1);
    let ingresses: Vec<String> = cluster
        .ingresses
        .iter()
        .map(|o| namespace(&o.metadata))
        .collect();
    assert_eq!(ingresses, ["shop"]);
    let services: Vec<String> = cluster
        .services
        .iter()
        .map(|o| namespace(&o.metadata))
        .collect();
    assert_eq!(services, ["shop", "blog"]);
    assert_eq!(cluster.secrets.len(), 1);
}

#[test]
fn nothing_is_translated_until_every_follow_is_listed_and_what_is_waited_on_is_said_once() {
    let plane = plane();
    let mut translator = Translator::default();
    let unlisted = || vec!["ingresses in shop".to_owned(), "secrets in shop".to_owned()];
    let untouched = || -> Cluster { panic!("translated before every follow was listed") };
    assert_eq!(
        translator.step(&plane, unlisted(), untouched),
        Step::Waiting(&unlisted())
    );
    assert_eq!(
        translator.step(&plane, unlisted(), untouched),
        Step::Nothing,
        "the same wait said again"
    );
    let secrets = ["secrets in shop".to_owned()];
    assert_eq!(
        translator.step(&plane, secrets.to_vec(), untouched),
        Step::Waiting(&secrets)
    );
    let Step::Translated(translation) = translator.step(&plane, Vec::new(), Cluster::default)
    else {
        panic!("not translated once every follow was listed");
    };
    assert!(translation.config.routes.is_empty());
}

#[test]
fn a_translation_no_different_from_the_last_is_not_said_again() {
    let plane = plane();
    let cluster = |hosts: &[&str]| Cluster {
        ingress_classes: vec![class()],
        ingresses: hosts
            .iter()
            .map(|host| ingress("shop", host, host))
            .collect(),
        services: vec![service("shop")],
        secrets: vec![secret("shop", hosts)],
        secrets_assumed: false,
    };
    let mut translator = Translator::default();
    let shop = cluster(&["shop.example"]);
    let Step::Translated(translation) = translator.step(&plane, Vec::new(), || shop.clone()) else {
        panic!("not translated");
    };
    assert_eq!(
        routes(translation),
        ["ingress/shop/shop.example/shop.example"]
    );
    assert_eq!(
        translator.step(&plane, Vec::new(), || shop.clone()),
        Step::Nothing
    );
    let both = cluster(&["shop.example", "blog.example"]);
    let Step::Translated(translation) = translator.step(&plane, Vec::new(), || both) else {
        panic!("a change not translated");
    };
    assert_eq!(translation.config.routes.len(), 2);
}

/// Each object is compiled alone as it is translated, so only the data plane's own settings
/// make a whole config it would refuse: two listeners at one address.
#[test]
fn a_config_the_data_plane_would_refuse_is_said_once_and_not_handed_on() {
    let plane = plane_at("[::]:8080", "[::]:8080");
    let mut translator = Translator::default();
    let Step::Refused { problems, .. } = translator.step(&plane, Vec::new(), Cluster::default)
    else {
        panic!("a config of two listeners at one address handed on");
    };
    assert!(
        problems
            .iter()
            .any(|problem| problem.to_string().contains("already taken")),
        "{problems:?}"
    );
    assert_eq!(
        translator.step(&plane, Vec::new(), Cluster::default),
        Step::Nothing
    );
}

/// Every kind is followed in the cluster, nothing translated while one is not listed; once
/// all are, the cluster is translated, and translated again when objects change: changes a
/// moment apart, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cluster_is_translated_once_every_kind_is_listed_and_again_when_it_changes() {
    let (secrets_allowed, blog_added) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let (allowed, added) = (Arc::clone(&secrets_allowed), Arc::clone(&blog_added));
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        let watching = path.contains("watch=true");
        if path.contains("/ingresses?") && watching {
            if !added.swap(false, Ordering::SeqCst) {
                // Ended soon, and begun again, so that the change is sent once it is made.
                return Answer {
                    pause: Duration::from_millis(100),
                    ..open()
                };
            }
            let added = |name: &str, host: &str| {
                let event = serde_json::json!({
                    "type": "ADDED",
                    "object": ingress("blog", name, host),
                });
                Bytes::from(format!("{event}\n"))
            };
            return Answer {
                status: 200,
                chunks: vec![added("web", "blog.example"), added("news", "news.example")],
                pause: Duration::from_millis(50),
            };
        }
        if watching {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop"), service("blog")])
        } else if path.starts_with("/api/v1/secrets?") && allowed.load(Ordering::SeqCst) {
            page(&[
                secret("shop", &["shop.example"]),
                secret("blog", &["blog.example", "news.example"]),
            ])
        } else if path.starts_with("/api/v1/secrets?") {
            answer(403, r#"{"kind":"Status","code":403,"message":"forbidden"}"#)
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("controller-cluster", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    // Quiet long enough that changes 50 ms apart are always one.
                    let debounce = Debounce {
                        quiet: Duration::from_secs(1),
                        most: Duration::from_secs(5),
                    };
                    control(api, &[Scope::Cluster], &plane, debounce, said).await;
                }
            });
            until("the Secrets' failure said", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line.starts_with("secrets in the cluster: "))
            })
            .await;
            until("the wait on Secrets alone said", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "waiting on secrets in the cluster")
            })
            .await;
            assert!(
                heard.borrow().translations.is_empty(),
                "translated before every kind was listed: {:?}",
                heard.borrow().lines
            );
            assert_eq!(
                heard.borrow().lines[0],
                "waiting on ingressclasses in the cluster, ingresses in the cluster, \
                 services in the cluster, secrets in the cluster"
            );

            secrets_allowed.store(true, Ordering::SeqCst);
            until("translated", || !heard.borrow().translations.is_empty()).await;
            {
                let heard = heard.borrow();
                let translation = &heard.translations[0];
                assert_eq!(routes(translation), ["ingress/shop/web/shop.example"]);
                assert_eq!(translation.config.certificates.len(), 1, "{translation:?}");
                assert!(translation.report.is_empty(), "{:?}", translation.report);
                assert!(translation.tls.is_empty(), "{:?}", translation.tls);
            }

            blog_added.store(true, Ordering::SeqCst);
            until("both changes translated", || {
                heard
                    .borrow()
                    .translations
                    .last()
                    .is_some_and(|translation| translation.config.routes.len() == 3)
            })
            .await;
            let heard = heard.borrow();
            assert_eq!(
                heard.translations.len(),
                2,
                "changes a moment apart not one"
            );
            assert_eq!(
                routes(&heard.translations[1]),
                [
                    "ingress/blog/news/news.example",
                    "ingress/blog/web/blog.example",
                    "ingress/shop/web/shop.example"
                ]
            );
            controlling.abort();
        })
        .await;
}

/// With namespaces for its scope, each kind is followed in each of them, IngressClasses in
/// the cluster, and the objects of all of them make one translation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_namespace_of_the_scope_is_followed_and_all_are_translated_together() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            return open();
        }
        let namespace = ["shop", "blog"]
            .into_iter()
            .find(|namespace| path.contains(&format!("/namespaces/{namespace}/")));
        match (namespace, path.split('?').next().unwrap_or_default()) {
            (None, "/apis/networking.k8s.io/v1/ingressclasses") => page(&[class()]),
            (Some(namespace), path) if path.ends_with("/ingresses") => {
                page(&[ingress(namespace, "web", &format!("{namespace}.example"))])
            }
            (Some(namespace), path) if path.ends_with("/services") => page(&[service(namespace)]),
            (Some(namespace), path) if path.ends_with("/secrets") => {
                page(&[secret(namespace, &[&format!("{namespace}.example")])])
            }
            _ => answer(500, ""),
        }
    })
    .await;
    let api = fake.client(&token("controller-namespaces", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let scope = [
                Scope::Namespace("shop".to_owned()),
                Scope::Namespace("blog".to_owned()),
            ];
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(api, &scope, &plane, Debounce::default(), said).await;
                }
            });
            until("translated", || !heard.borrow().translations.is_empty()).await;
            let heard = heard.borrow();
            let translation = &heard.translations[0];
            assert_eq!(
                routes(translation),
                [
                    "ingress/blog/web/blog.example",
                    "ingress/shop/web/shop.example"
                ],
                "{:?}",
                heard.lines
            );
            assert_eq!(translation.config.certificates.len(), 2);
            controlling.abort();
        })
        .await;
    let mut lists: Vec<String> = fake
        .seen()
        .into_iter()
        .filter(|seen| !seen.path.contains("watch=true"))
        .map(|seen| seen.path.split('?').next().unwrap_or_default().to_owned())
        .collect();
    lists.sort();
    assert_eq!(
        lists,
        [
            "/api/v1/namespaces/blog/secrets",
            "/api/v1/namespaces/blog/services",
            "/api/v1/namespaces/shop/secrets",
            "/api/v1/namespaces/shop/services",
            "/apis/networking.k8s.io/v1/ingressclasses",
            "/apis/networking.k8s.io/v1/namespaces/blog/ingresses",
            "/apis/networking.k8s.io/v1/namespaces/shop/ingresses",
        ]
    );
}
