use super::*;
use bytes::Bytes;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use edgerush_translate::GATEWAY_CONTROLLER;
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
ingress_https_redirect_max_age: 30
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

/// What no data plane runs: this replica's alone, of none.
fn acks() -> Acks {
    Acks {
        own: watch::channel(acks::Pods::new()).1,
        reports: None,
    }
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
            Said::GatewayApi(served) => {
                let kinds: Vec<String> = served
                    .iter()
                    .map(|(kind, served)| format!("{}: {served}", kind.crd()))
                    .collect();
                format!("gateway api: {}", kinds.join("; "))
            }
            Said::Failed { followed, failure } => format!("{followed}: {failure}"),
            Said::Translated(translation) => {
                heard.translations.push(translation.clone());
                "translated".to_owned()
            }
            Said::Refused { problems, .. } => format!("refused: {problems:?}"),
            Said::GatewayApiStatus { object, written } => match written {
                Ok(()) => format!("status of {object}"),
                Err(error) => format!("status of {object} not written: {error}"),
            },
            Said::Status {
                ingress,
                addresses,
                written,
            } => {
                let ips: Vec<String> = addresses.iter().filter_map(|a| a.ip.clone()).collect();
                match written {
                    Ok(()) => format!("status of {ingress}: [{}]", ips.join(", ")),
                    Err(error) => format!("status of {ingress} not written: {error}"),
                }
            }
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
        _tasks: Tasks(Vec::new()),
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
        status_service: Vec::new(),
        gateway_api: None,
        watched: watched(&[
            Scope::Namespace("shop".to_owned()),
            Scope::Namespace("blog".to_owned()),
        ]),
        reports: Vec::new(),
        ready: Vec::new(),
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
    assert_eq!(
        cluster.watched,
        Some(vec!["shop".to_owned(), "blog".to_owned()])
    );
}

#[test]
fn a_scope_of_the_whole_cluster_watches_every_namespace() {
    assert_eq!(watched(&[Scope::Cluster]), None);
    assert_eq!(
        watched(&[Scope::Namespace("shop".to_owned()), Scope::Cluster]),
        None
    );
    assert_eq!(
        watched(&[Scope::Namespace("shop".to_owned())]),
        Some(vec!["shop".to_owned()])
    );
}

/// The CRD of a Gateway API kind, as the API server sends it, serving `versions`.
fn crd(kind: GatewayKind, versions: &[&str]) -> CustomResourceDefinition {
    let (plural, group) = kind.crd().split_once('.').unwrap();
    let versions: Vec<serde_json::Value> = versions
        .iter()
        .map(|name| serde_json::json!({ "name": name, "served": true, "storage": false }))
        .collect();
    serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": kind.crd(),
            "resourceVersion": "6",
            "annotations": { "gateway.networking.k8s.io/bundle-version": "v1.7.0" },
        },
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": { "kind": format!("{kind:?}"), "plural": plural },
            "versions": versions,
        },
    }))
    .unwrap()
}

/// The data plane of [`plane`], as the Gateway `edge/web`.
fn gateway_plane() -> Plane {
    let mut plane = plane();
    plane.gateway = Some(edgerush_translate::GatewayName::try_from("edge/web".to_owned()).unwrap());
    plane
}

#[test]
fn a_gateway_api_kind_is_waited_on_and_translated_only_while_it_is_followed() {
    let route: HTTPRoute =
        serde_saphyr::from_str("metadata: { name: web, namespace: shop }\nspec: {}").unwrap();
    let gateway_api = GatewayApi {
        crds: vec![
            (
                GatewayKind::HTTPRoute,
                follow_of(
                    "crd httproutes",
                    true,
                    vec![crd(GatewayKind::HTTPRoute, &["v1"])],
                ),
            ),
            (
                GatewayKind::ReferenceGrant,
                follow_of("crd referencegrants", false, Vec::new()),
            ),
        ],
        namespaces: vec![follow_of(
            "namespaces in the cluster",
            true,
            vec![serde_saphyr::from_str("metadata: { name: shop }").unwrap()],
        )],
        namespace: Scope::Namespace("edge".to_owned()),
        last: None,
        gateway_classes: None,
        gateways: None,
        http_routes: Some(vec![follow_of("httproutes in shop", false, vec![route])]),
        reference_grants: None,
    };
    assert_eq!(gateway_api.served(), None, "a CRD not listed yet");
    let unlisted: Vec<String> = gateway_api.unlisted().collect();
    assert_eq!(unlisted, ["crd referencegrants", "httproutes in shop"]);
    // A kind no longer served is no longer followed: none of its objects, none waited on.
    let served = [
        (GatewayKind::HTTPRoute, Served::Missing),
        (GatewayKind::ReferenceGrant, Served::Missing),
    ];
    let followed_with = |gateway_api| Followed {
        ingress_classes: Vec::new(),
        ingresses: Vec::new(),
        services: Vec::new(),
        secrets: Vec::new(),
        status_service: Vec::new(),
        gateway_api: Some(gateway_api),
        watched: None,
        reports: Vec::new(),
        ready: Vec::new(),
    };
    let followed = followed_with(gateway_api);
    assert_eq!(followed.cluster().http_routes.len(), 1);
    assert_eq!(
        followed.cluster().namespaces.len(),
        1,
        "Namespaces, for their labels"
    );
    let Some(mut gateway_api) = followed.gateway_api else {
        unreachable!()
    };
    keep(
        &mut gateway_api.http_routes,
        served[0].1.is_served(),
        || panic!("started though not served"),
    );
    assert!(gateway_api.http_routes.is_none());
    let followed = followed_with(gateway_api);
    assert!(followed.cluster().http_routes.is_empty());
    assert_eq!(followed.unlisted(), ["crd referencegrants"]);
}

#[test]
fn a_kind_followed_is_started_once_and_stopped_when_no_longer_served() {
    let mut slot: Option<Vec<Follow<HTTPRoute>>> = None;
    let mut started = 0;
    for on in [true, true, false, false, true] {
        keep(&mut slot, on, || {
            started += 1;
            vec![follow_of("httproutes", true, Vec::new())]
        });
        assert_eq!(slot.is_some(), on);
    }
    assert_eq!(started, 2, "started again while followed");
}

/// The data plane names a Gateway: each Gateway API kind's CRD is followed by its name,
/// the kinds whose CRDs serve `v1` are followed, Gateways in the Gateway's namespace alone,
/// one served at an older version alone is never asked for, and a CRD deleted and installed
/// again stops its kind's follow and starts a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gateway_apis_kinds_are_followed_as_their_crds_say() {
    // 0: the CRD there; 1: deleted; 2: installed again.
    let stage = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let told = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (at, sent) = (Arc::clone(&stage), Arc::clone(&told));
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        let watching = path.contains("watch=true");
        let named = |kind: GatewayKind| {
            path.starts_with("/apis/apiextensions.k8s.io/v1/customresourcedefinitions?")
                && path.contains(&format!("metadata.name%3D{}", kind.crd()))
        };
        if named(GatewayKind::HTTPRoute) && watching {
            // The change of the stage, sent once on the next watch.
            let now = at.load(Ordering::SeqCst);
            if sent.swap(now, Ordering::SeqCst) == now {
                return Answer {
                    pause: Duration::from_millis(100),
                    ..open()
                };
            }
            let kind = if now == 1 { "DELETED" } else { "ADDED" };
            let event = serde_json::json!({
                "type": kind,
                "object": crd(GatewayKind::HTTPRoute, &["v1", "v1beta1"]),
            });
            return Answer {
                status: 200,
                chunks: vec![Bytes::from(format!("{event}\n"))],
                pause: Duration::from_millis(100),
            };
        }
        if watching {
            return open();
        }
        let gateway_api = "/apis/gateway.networking.k8s.io/v1/";
        if named(GatewayKind::HTTPRoute) && at.load(Ordering::SeqCst) == 1 {
            page::<CustomResourceDefinition>(&[])
        } else if named(GatewayKind::HTTPRoute) {
            page(&[crd(GatewayKind::HTTPRoute, &["v1", "v1beta1"])])
        } else if named(GatewayKind::ReferenceGrant) {
            // An install before Gateway API v1.5.
            page(&[crd(GatewayKind::ReferenceGrant, &["v1beta1"])])
        } else if named(GatewayKind::GatewayClass) {
            page(&[crd(GatewayKind::GatewayClass, &["v1"])])
        } else if named(GatewayKind::Gateway) {
            page(&[crd(GatewayKind::Gateway, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}namespaces/edge/gateways?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"web","namespace":"edge"},"spec":{"gatewayClassName":"edgerush","listeners":[{"name":"http","port":8080,"protocol":"HTTP","allowedRoutes":{"namespaces":{"from":"All"}}}]}}]}"#,
            )
        } else if path.starts_with(&format!("{gateway_api}gatewayclasses?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"edgerush"},"spec":{"controllerName":"edgerush.dev/gateway-controller"}}]}"#,
            )
        } else if path.starts_with(&format!("{gateway_api}httproutes?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"web","namespace":"shop"},"spec":{"parentRefs":[{"name":"web","namespace":"edge"}],"hostnames":["route.example"],"rules":[{"backendRefs":[{"name":"web","port":80}]}]}}]}"#,
            )
        } else if path.starts_with("/api/v1/namespaces?") {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"shop","labels":{"team":"shop"}}}]}"#,
            )
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(
        &token("controller-gateway-api", "secret"),
        Timing::default(),
    );
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let said_of = |heard: &Heard, line: &str| heard.lines.iter().filter(|l| *l == line).count();
    let crd_lines = |httproutes: &str| {
        format!(
            "gateway api: gatewayclasses.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             gateways.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             httproutes.gateway.networking.k8s.io: {httproutes}; \
             referencegrants.gateway.networking.k8s.io: not served at v1 (only v1beta1; Gateway API v1.7.0), \
             so taken as having no objects"
        )
    };
    let served_line = crd_lines("served at v1 (Gateway API v1.7.0)");
    let missing_line = crd_lines("not installed, so taken as having no objects");
    // The kinds' follows waited on, in one line: Namespaces, followed from the start, may
    // still be in it.
    let waited_on_kinds = |heard: &Heard| {
        heard
            .lines
            .iter()
            .filter(|line| {
                line.starts_with("waiting on ")
                    && line.contains("gatewayclasses in the cluster, gateways in edge, httproutes in the cluster")
            })
            .count()
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = gateway_plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &[Scope::Cluster],
                        &plane,
                        Debounce::default(),
                        None,
                        None,
                        acks(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || !heard.borrow().translations.is_empty()).await;
            {
                let heard = heard.borrow();
                assert_eq!(said_of(&heard, &served_line), 1, "{:?}", heard.lines);
                assert_eq!(waited_on_kinds(&heard), 1, "{:?}", heard.lines);
                let translation = &heard.translations[0];
                assert_eq!(
                    routes(translation),
                    ["ingress/shop/web/shop.example", "httproute/shop/web/http"]
                );
                let report: Vec<String> =
                    translation.report.iter().map(ToString::to_string).collect();
                assert!(report.is_empty(), "{report:?}");
            }
            stage.store(1, Ordering::SeqCst);
            until("the CRD's deletion said", || {
                said_of(&heard.borrow(), &missing_line) == 1
            })
            .await;
            stage.store(2, Ordering::SeqCst);
            until("a new follow of HTTPRoutes waited on", || {
                waited_on_kinds(&heard.borrow()) == 1
                    && heard
                        .borrow()
                        .lines
                        .iter()
                        .any(|line| line == "waiting on httproutes in the cluster")
            })
            .await;
            assert_eq!(
                said_of(&heard.borrow(), &served_line),
                2,
                "{:?}",
                heard.borrow().lines
            );
            controlling.abort();
        })
        .await;
    let lists: Vec<String> = fake
        .seen()
        .into_iter()
        .filter(|seen| !seen.path.contains("watch=true"))
        .map(|seen| seen.path)
        .collect();
    assert!(
        !lists.iter().any(|path| path.contains("/referencegrants?")),
        "a kind not served was asked for: {lists:?}"
    );
    let gateways: Vec<&str> = lists
        .iter()
        .filter(|path| path.contains("/gateways?"))
        .map(|path| path.split('?').next().unwrap_or_default())
        .collect();
    assert_eq!(
        gateways,
        ["/apis/gateway.networking.k8s.io/v1/namespaces/edge/gateways"],
        "Gateways followed in the Gateway's namespace alone"
    );
    let routes_listed = lists
        .iter()
        .filter(|path| path.contains("/httproutes?"))
        .count();
    assert_eq!(
        routes_listed, 2,
        "HTTPRoutes listed by each follow: {lists:?}"
    );
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
        ..Cluster::default()
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
                    control(
                        api,
                        &[Scope::Cluster],
                        &plane,
                        debounce,
                        None,
                        None,
                        acks(),
                        said,
                    )
                    .await;
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
                    control(
                        api,
                        &scope,
                        &plane,
                        Debounce::default(),
                        None,
                        None,
                        acks(),
                        said,
                    )
                    .await;
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

/// Ingress `name` in `shop` of [`ingress`]'s making, of class `class`, with `annotations`,
/// its status saying `addresses`.
fn ingress_saying(
    name: &str,
    class: &str,
    addresses: &[&str],
    annotations: &[(&str, &str)],
) -> Ingress {
    let mut made = ingress("shop", name, "shop.example");
    if let Some(spec) = made.spec.as_mut() {
        spec.ingress_class_name = Some(class.to_owned());
    }
    if !annotations.is_empty() {
        made.metadata.annotations = Some(
            annotations
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        );
    }
    if !addresses.is_empty() {
        made.status = Some(saying(
            addresses.iter().map(|ip| status::address(ip)).collect(),
        ));
    }
    made
}

/// A status saying `addresses`.
fn saying(
    addresses: Vec<IngressLoadBalancerIngress>,
) -> k8s_openapi::api::networking::v1::IngressStatus {
    k8s_openapi::api::networking::v1::IngressStatus {
        load_balancer: Some(
            k8s_openapi::api::networking::v1::IngressLoadBalancerStatus {
                ingress: Some(addresses),
            },
        ),
    }
}

fn object(name: &str) -> Object {
    Object {
        kind: "Ingress",
        namespace: "shop".to_owned(),
        name: name.to_owned(),
    }
}

#[test]
fn a_status_is_written_where_it_says_other_than_what_is_served() {
    let addresses = vec![
        status::address("203.0.113.7"),
        status::address("edge.example"),
    ];
    let mut ingresses = vec![
        // Served, saying nothing yet.
        ingress_saying("web", "edge", &[], &[]),
        // Served, saying the same, in another order.
        ingress_saying("same", "edge", &[], &[]),
        // Of the class, not served, saying an address of before.
        ingress_saying("old", "edge", &["192.0.2.1"], &[]),
        // Of the class, not served, saying nothing.
        ingress_saying("quiet", "edge", &[], &[]),
        // Another's.
        ingress_saying("theirs", "other", &["198.51.100.1"], &[]),
    ];
    ingresses[1].status = Some(saying(vec![addresses[1].clone(), addresses[0].clone()]));
    let claimed = [
        object("web"),
        object("same"),
        object("old"),
        object("quiet"),
        object("gone"),
    ];
    let served = [object("web"), object("same")];
    let changes = status::changes(&claimed, &served, &ingresses, &addresses);
    assert_eq!(
        changes,
        [
            status::Change {
                ingress: object("web"),
                addresses: vec![addresses[0].clone(), addresses[1].clone()],
            },
            status::Change {
                ingress: object("old"),
                addresses: Vec::new(),
            },
        ]
    );
    assert_eq!(
        status::patch(&addresses[..1]).unwrap(),
        serde_json::json!({ "status": { "loadBalancer": { "ingress": [{ "ip": "203.0.113.7" }] } } })
    );
    assert_eq!(
        status::patch(&[]).unwrap(),
        serde_json::json!({ "status": { "loadBalancer": { "ingress": null } } })
    );
}

#[test]
fn a_services_addresses_are_its_load_balancers_else_its_external_ips() {
    let service: Service = serde_saphyr::from_str(
        "{ metadata: { name: edgerush, namespace: edge },
           spec: { externalIPs: [192.0.2.9] },
           status: { loadBalancer: { ingress: [{ ip: 203.0.113.7 }, { hostname: lb.example }] } } }",
    )
    .unwrap();
    assert_eq!(
        status::of_service(Some(&service)),
        [
            status::address("203.0.113.7"),
            status::address("lb.example")
        ]
    );
    let mut pending = service.clone();
    pending.status = None;
    assert_eq!(
        status::of_service(Some(&pending)),
        [status::address("192.0.2.9")]
    );
    pending.spec = None;
    assert!(status::of_service(Some(&pending)).is_empty());
    assert!(status::of_service(None).is_empty());
    assert_eq!(
        status::address("2001:db8::1").ip.as_deref(),
        Some("2001:db8::1")
    );
    assert_eq!(
        status::address("lb.example").hostname.as_deref(),
        Some("lb.example")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_ingress_status_is_written_as_the_config_handed_on_serves_it() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.ends_with("/status") {
            return answer(200, "{}");
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[
                ingress_saying("web", "edge", &[], &[]),
                ingress_saying(
                    "old",
                    "edge",
                    &["192.0.2.1"],
                    &[("nginx.ingress.kubernetes.io/rewrite-target", "/")],
                ),
                ingress_saying("theirs", "other", &["198.51.100.1"], &[]),
            ])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            let data_plane: Service = serde_saphyr::from_str(
                "{ metadata: { name: edgerush, namespace: edge }, spec: { externalIPs: [203.0.113.7] } }",
            )
            .unwrap();
            page(&[data_plane])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("controller-status", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    let source = Addresses::Service {
                        namespace: "edge".to_owned(),
                        name: "edgerush".to_owned(),
                    };
                    control(
                        api,
                        &[Scope::Cluster],
                        &plane,
                        Debounce::default(),
                        Some(source),
                        None,
                        acks(),
                        said,
                    )
                    .await;
                }
            });
            let said = |line: &str| heard.borrow().lines.iter().any(|said| said == line);
            until("both statuses written", || {
                said("status of Ingress shop/web: [203.0.113.7]")
                    && said("status of Ingress shop/old: []")
            })
            .await;
            controlling.abort();
        })
        .await;
    let patched: std::collections::BTreeSet<(String, String)> = fake
        .seen()
        .into_iter()
        .filter(|seen| seen.method == "PATCH")
        .map(|seen| (seen.path, seen.body))
        .collect();
    let path =
        |name: &str| format!("/apis/networking.k8s.io/v1/namespaces/shop/ingresses/{name}/status");
    let expected: std::collections::BTreeSet<(String, String)> = [
        (
            path("old"),
            r#"{"status":{"loadBalancer":{"ingress":null}}}"#.to_owned(),
        ),
        (
            path("web"),
            r#"{"status":{"loadBalancer":{"ingress":[{"ip":"203.0.113.7"}]}}}"#.to_owned(),
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(patched, expected);
}

/// The data plane's Service not to be read: translation goes on all the same, and no status
/// is written until it is listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_config_is_not_held_up_by_its_status() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            // Saying an address already, which a status written with no Service to give
            // one would take away.
            page(&[ingress_saying("web", "edge", &["203.0.113.7"], &[])])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            answer(403, r#"{"kind":"Status","code":403,"message":"forbidden"}"#)
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(
        &token("controller-status-held", "secret"),
        Timing::default(),
    );
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    let source = Addresses::Service {
                        namespace: "edge".to_owned(),
                        name: "edgerush".to_owned(),
                    };
                    control(
                        api,
                        &[Scope::Cluster],
                        &plane,
                        Debounce::default(),
                        Some(source),
                        None,
                        acks(),
                        said,
                    )
                    .await;
                }
            });
            until("translated, and the Service's refusal said", || {
                let heard = heard.borrow();
                !heard.translations.is_empty()
                    && heard
                        .lines
                        .iter()
                        .any(|line| line.starts_with("services in edge: "))
            })
            .await;
            controlling.abort();
        })
        .await;
    assert!(
        heard
            .borrow()
            .lines
            .iter()
            .all(|line| !line.starts_with("waiting on services in edge")),
        "the translation waited on the status's Service: {:?}",
        heard.borrow().lines
    );
    assert!(
        fake.seen().iter().all(|seen| seen.method != "PATCH"),
        "a status written with no Service listed"
    );
}

/// No Lease: one is made and held, and renewed; renewals refused, leading stops within the
/// renew deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_makes_the_lease_leads_renews_and_stops_when_it_cannot() {
    use k8s_openapi::api::coordination::v1::Lease;
    let made = Arc::new(AtomicBool::new(false));
    let refusing = Arc::new(AtomicBool::new(false));
    let (was_made, refused) = (Arc::clone(&made), Arc::clone(&refusing));
    let fake =
        edgerush_kubernetes::testing::Fake::start_by_method(&["127.0.0.1"], move |method, path| {
            let held: Lease = serde_json::from_value(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": { "name": "edgerush", "namespace": "edge", "resourceVersion": "2" },
                "spec": { "holderIdentity": "me", "leaseDurationSeconds": 1 },
            }))
            .unwrap();
            let body = serde_json::to_string(&held).unwrap();
            assert!(
                path.starts_with("/apis/coordination.k8s.io/v1/namespaces/edge/leases"),
                "{path}"
            );
            match method {
                "GET" if !was_made.load(Ordering::SeqCst) => {
                    answer(404, r#"{"kind":"Status","code":404}"#)
                }
                "GET" => answer(200, &body),
                "POST" => {
                    was_made.store(true, Ordering::SeqCst);
                    Answer {
                        status: 201,
                        ..answer(200, &body)
                    }
                }
                "PUT" if refused.load(Ordering::SeqCst) => {
                    answer(409, r#"{"kind":"Status","code":409,"message":"modified"}"#)
                }
                _ => answer(200, &body),
            }
        })
        .await;
    let api = fake.client(&token("controller-lease", "secret"), Timing::default());
    let timing = lease::Timing {
        duration: Duration::from_millis(600),
        renew_deadline: Duration::from_millis(400),
        retry: Duration::from_millis(50),
    };
    let (leading, mut leads) = watch::channel(false);
    let electing = tokio::spawn(async move {
        lease::elect(&api, "edge", "edgerush", "me", timing, &leading, |_| {}).await;
    });
    tokio::time::timeout(Duration::from_secs(5), leads.wait_for(|leads| *leads))
        .await
        .expect("never led")
        .unwrap();
    until("renewed", || {
        fake.seen()
            .iter()
            .filter(|seen| seen.method == "PUT")
            .count()
            >= 2
    })
    .await;
    refusing.store(true, Ordering::SeqCst);
    let refused_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), leads.wait_for(|leads| !*leads))
        .await
        .expect("led on, its renewals refused")
        .unwrap();
    // Not before its last renewal's deadline.
    assert!(
        refused_at.elapsed() >= Duration::from_millis(300),
        "stopped after {:?}",
        refused_at.elapsed()
    );
    electing.abort();
    let created = fake
        .seen()
        .into_iter()
        .find(|seen| seen.method == "POST")
        .unwrap();
    let created: Lease = serde_json::from_str(&created.body).unwrap();
    assert_eq!(created.spec.unwrap().holder_identity.as_deref(), Some("me"));
}

/// A replica that does not lead writes no status; as it comes to, it writes each that says
/// otherwise, nothing else having changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_leader_writes_status_and_it_does_as_it_comes_to_lead() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.ends_with("/status") {
            return answer(200, "{}");
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress_saying("web", "edge", &[], &[])])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("controller-leading", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (leads, leading) = watch::channel(false);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    let source = Addresses::Stated(vec![status::address("203.0.113.7")]);
                    control(
                        api,
                        &[Scope::Cluster],
                        &plane,
                        Debounce::default(),
                        Some(source),
                        Some(leading),
                        acks(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || !heard.borrow().translations.is_empty()).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                fake.seen().iter().all(|seen| seen.method != "PATCH"),
                "a status written by a replica that does not lead"
            );
            leads.send_replace(true);
            until("the status written once it leads", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "status of Ingress shop/web: [203.0.113.7]")
            })
            .await;
            controlling.abort();
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gateway_apis_statuses_are_written_where_they_say_otherwise_and_once() {
    let class: GatewayClass = serde_saphyr::from_str(
        "metadata: { name: edgerush, generation: 1, resourceVersion: '5' }
spec: { controllerName: edgerush.dev/gateway-controller }",
    )
    .unwrap();
    let gateway: Gateway = serde_saphyr::from_str(
        "metadata: { name: web, namespace: edge, generation: 2, resourceVersion: '6' }
spec:
  gatewayClassName: edgerush
  listeners: [{ name: http, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }]",
    )
    .unwrap();
    let app: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: app, namespace: shop, generation: 3, resourceVersion: '7' }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    // A route that named the Gateway once: the parent of ours is taken away, another
    // controller's kept.
    let old: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: old, namespace: shop, generation: 1, resourceVersion: '8' }
spec: { parentRefs: [{ name: elsewhere, namespace: edge }] }
status:
  parents:
    - controllerName: edgerush.dev/gateway-controller
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: web, namespace: edge }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]
    - controllerName: example.com/other
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: elsewhere, namespace: edge }
      conditions: []",
    )
    .unwrap();
    let known = [
        "/apis/gateway.networking.k8s.io/v1/gatewayclasses/edgerush/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/edge/gateways/web/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/app/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/old/status",
    ];
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if method == "PUT" && known.contains(&path) {
            answer(200, "{}")
        } else {
            answer(404, r#"{"kind":"Status","code":404,"message":"not found"}"#)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-gateway-status", "secret"),
        Timing::default(),
    );
    let cluster = Cluster {
        services: vec![service("shop")],
        gateway_classes: vec![class.clone()],
        gateways: vec![gateway.clone()],
        http_routes: vec![app.clone(), old.clone()],
        ..Cluster::default()
    };
    let translation = translate(&gateway_plane(), &cluster);
    let handed = Handed::of(&translation).expect("the Gateway translated");
    let served: Vec<(GatewayKind, Served)> = GatewayKind::ALL
        .into_iter()
        .map(|kind| {
            (
                kind,
                Served::Served {
                    release: Some("v1.7.0".to_owned()),
                },
            )
        })
        .collect();
    let following = |classes: Vec<GatewayClass>, gateways: Vec<Gateway>, routes| GatewayApi {
        crds: Vec::new(),
        namespaces: Vec::new(),
        namespace: Scope::Namespace("edge".to_owned()),
        last: Some(served.clone()),
        gateway_classes: Some(vec![follow_of("gatewayclasses", true, classes)]),
        gateways: Some(vec![follow_of("gateways in edge", true, gateways)]),
        http_routes: Some(vec![follow_of("httproutes", true, routes)]),
        reference_grants: None,
    };
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let said = hear(&heard);
    let addresses = gateway_status::addresses(&[status::address("192.0.2.10")]);
    let programmed = Programmed::of(2, &acks::Pods::new(), None, false);
    following(
        vec![class.clone()],
        vec![gateway.clone()],
        vec![app.clone(), old.clone()],
    )
    .write_statuses(&api, &handed, &addresses, &programmed, &said)
    .await;
    assert_eq!(
        heard.borrow().lines,
        [
            "status of GatewayClass edgerush",
            "status of Gateway edge/web",
            "status of HTTPRoute shop/app",
            "status of HTTPRoute shop/old",
        ]
    );
    let bodies: Vec<serde_json::Value> = fake
        .seen()
        .iter()
        .map(|seen| {
            assert_eq!(seen.method, "PUT");
            serde_json::from_str(&seen.body).unwrap()
        })
        .collect();
    let status = |n: usize| &bodies[n]["status"];
    assert_eq!(
        bodies[0]["metadata"]["resourceVersion"], "5",
        "written as of the version read"
    );
    assert_eq!(status(0)["conditions"][0]["type"], "Accepted");
    assert_eq!(status(0)["conditions"][1]["type"], "SupportedVersion");
    assert_eq!(status(0)["conditions"][1]["status"], "True");
    assert_eq!(status(1)["addresses"][0]["value"], "192.0.2.10");
    assert_eq!(status(1)["listeners"][0]["attachedRoutes"], 1);
    assert_eq!(status(1)["conditions"][0]["observedGeneration"], 2);
    assert_eq!(
        status(2)["parents"][0]["controllerName"],
        GATEWAY_CONTROLLER
    );
    assert_eq!(
        status(2)["parents"][0]["conditions"][0]["observedGeneration"],
        3
    );
    assert_eq!(status(3)["parents"].as_array().unwrap().len(), 1);
    assert_eq!(
        status(3)["parents"][0]["controllerName"],
        "example.com/other"
    );
    // Read back as written, every status says what it should: nothing more is written.
    let (mut class, mut gateway, mut app, mut old) = (class, gateway, app, old);
    class.status = serde_json::from_value(bodies[0]["status"].clone()).unwrap();
    gateway.status = serde_json::from_value(bodies[1]["status"].clone()).unwrap();
    app.status = serde_json::from_value(bodies[2]["status"].clone()).unwrap();
    old.status = serde_json::from_value(bodies[3]["status"].clone()).unwrap();
    following(vec![class], vec![gateway], vec![app, old])
        .write_statuses(&api, &handed, &addresses, &programmed, &said)
        .await;
    assert_eq!(fake.seen().len(), 4, "a status written again");
}

#[test]
fn the_leader_merges_every_replicas_report_and_counts_the_ready_pods() {
    use acks::{Pod, Pods, Reporter};
    let pod = |last: u8| IpAddr::from([10, 0, 0, last]);
    let runs = |generation: i64| Pod {
        runs: Some(generation),
        refused: None,
    };
    let report = |pod_name: &str, lease: &str, pods: Pods| {
        Reporter {
            namespace: "edge".to_owned(),
            lease: lease.to_owned(),
            pod: pod_name.to_owned(),
            uid: None,
        }
        .report(&pods)
        .unwrap()
    };
    let reports = vec![
        // This replica's own report, behind what it knows, of a pod it no longer serves
        // among them: not read.
        report(
            "one",
            "control",
            Pods::from([(pod(1), runs(1)), (pod(5), runs(1))]),
        ),
        report(
            "two",
            "control",
            Pods::from([(pod(1), runs(2)), (pod(2), runs(3))]),
        ),
        // Another control plane's.
        report("three", "other", Pods::from([(pod(3), runs(3))])),
    ];
    let slice = |service: &str, endpoints: serde_json::Value| -> EndpointSlice {
        serde_json::from_value(serde_json::json!({
            "metadata": { "name": format!("{service}-x"), "namespace": "edge",
                "labels": { "kubernetes.io/service-name": service } },
            "addressType": "IPv4",
            "endpoints": endpoints,
        }))
        .unwrap()
    };
    let slices = vec![
        slice(
            "edgerush-peers",
            serde_json::json!([
                { "addresses": ["10.0.0.1"], "conditions": { "ready": true } },
                { "addresses": ["10.0.0.2"] },
                { "addresses": ["10.0.0.4"], "conditions": { "ready": false } },
            ]),
        ),
        slice(
            "elsewhere",
            serde_json::json!([{ "addresses": ["10.0.0.9"], "conditions": { "ready": true } }]),
        ),
    ];
    let followed = Followed {
        ingress_classes: Vec::new(),
        ingresses: Vec::new(),
        services: Vec::new(),
        secrets: Vec::new(),
        status_service: Vec::new(),
        gateway_api: None,
        watched: None,
        reports: vec![follow_of("configmaps in edge", true, reports)],
        ready: vec![follow_of("endpointslices in edge", true, slices)],
    };
    let (_own, said) = watch::channel(Pods::from([(pod(1), runs(4))]));
    let acks = Acks {
        own: said,
        reports: Some(Reports {
            namespace: "edge".to_owned(),
            lease: "control".to_owned(),
            own: "control-acks-one".to_owned(),
        }),
    };
    // Its own pods as it knows them, the rest as the other replicas of its Lease say.
    assert_eq!(
        followed.pods(&acks),
        Pods::from([(pod(1), runs(4)), (pod(2), runs(3))])
    );
    let service =
        edgerush_config::ServiceName::parse("edgerush-peers.edge.svc.cluster.local").unwrap();
    assert_eq!(followed.ready(&service), [pod(1), pod(2)]);
}
