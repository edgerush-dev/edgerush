use super::*;
use bytes::Bytes;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use edgerush_planes::objects::ProxyService;
use edgerush_translate::{GATEWAY_CONTROLLER, GatewayName, translate};
use k8s_openapi::ByteString;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// A data plane of class `edge`, of no Gateway, with these more settings.
fn plane_with(more: &str) -> Plane {
    serde_saphyr::from_str(&format!(
        r#"
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: false
ingress_https_redirect_max_age: 30
{more}"#
    ))
    .unwrap()
}

fn plane() -> Plane {
    plane_with("")
}

/// Settings that never change: `plane`, its sender gone, of no Service, as the development
/// harness's.
fn fixed(plane: &Plane) -> watch::Receiver<Planes> {
    watch::channel(only(harness(plane))).1
}

/// `settings` as the one data plane served, as the development harness's is, of no name.
fn only(settings: Settings) -> Planes {
    Planes::from([(String::new(), settings)])
}

/// `plane` as the development harness serves it: of no Service.
fn harness(plane: &Plane) -> Settings {
    Settings {
        plane: plane.clone(),
        service: None,
        generated: false,
        data_plane: None,
    }
}

/// DataPlane `web` of `edge`, of UID `web-uid`, with these more fields in its spec.
fn web_data_plane(more: &str) -> edgerush_planes::data_plane::DataPlane {
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: web, namespace: edge, uid: web-uid }}
spec:
  controlPlaneRef: {{ name: edge }}
{more}"
    ))
    .unwrap()
}

/// `plane` served under the operator: the data plane's Service, `edge/edgerush-proxy-web`,
/// written as DataPlane `web`, of these more fields, shapes it.
fn operated(plane: &Plane, more: &str) -> Settings {
    Settings {
        plane: plane.clone(),
        service: Some(ProxyService::of(&web_data_plane(more)).unwrap()),
        generated: false,
        data_plane: None,
    }
}

/// The path the data plane's Service is applied at.
const SERVICE_APPLIED: &str = "/api/v1/namespaces/edge/services/edgerush-proxy-web?fieldManager=edgerush-controller&force=true";

/// The Service `settings` wants of these ports, as its namespace lists it, with these
/// addresses from its load balancer.
fn as_wanted(settings: &Settings, ports: &[u16], balanced: &[&str]) -> Service {
    let bound = ports
        .iter()
        .map(|port| {
            let transports = edgerush_planes::objects::Transports {
                tcp: true,
                udp: false,
            };
            (*port, transports)
        })
        .collect();
    let mut service = settings.service.as_ref().unwrap().service(&bound).unwrap();
    if !balanced.is_empty() {
        let ingress: Vec<String> = balanced
            .iter()
            .map(|ip| format!("{{ ip: {ip} }}"))
            .collect();
        service.status = Some(
            serde_saphyr::from_str(&format!(
                "loadBalancer: {{ ingress: [{}] }}",
                ingress.join(", ")
            ))
            .unwrap(),
        );
    }
    service
}

/// The bodies of the applies of the data plane's Service the fake was sent.
fn applied(fake: &Fake) -> Vec<serde_json::Value> {
    fake.seen()
        .into_iter()
        .filter(|seen| seen.method == "PATCH" && seen.path == SERVICE_APPLIED)
        .map(|seen| serde_json::from_str(&seen.body).unwrap())
        .collect()
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

/// What a fake answers for a path it is not written for: RateLimitPolicies, which every
/// control plane follows, none, their watch open and quiet; anything else, 500.
fn otherwise(path: &str) -> Answer {
    if !path.contains("/ratelimitpolicies?") {
        answer(500, "")
    } else if path.contains("watch=true") {
        open()
    } else {
        answer(
            200,
            r#"{"kind":"RateLimitPolicyList","metadata":{"resourceVersion":"5"},"items":[]}"#,
        )
    }
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

/// What the control plane said, a line each, and its translations, each with the data
/// plane it is for and that one's ID, and the data planes a translation was refused for.
#[derive(Default)]
struct Heard {
    lines: Vec<String>,
    translations: Vec<Translation>,
    of: Vec<String>,
    ids: Vec<Option<String>>,
    refused: Vec<String>,
    /// The leader's configs handed on in place of a translation, each with its ID.
    kept: Vec<(Config, Option<String>)>,
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
            Said::Namespace { namespace, now } => format!("namespace {namespace}: {now:?}"),
            Said::Translated {
                data_plane,
                translation,
                id,
            } => {
                heard.translations.push(translation.clone());
                heard.of.push(data_plane.to_owned());
                heard.ids.push(id.map(ToOwned::to_owned));
                "translated".to_owned()
            }
            Said::Refused {
                data_plane,
                problems,
                ..
            } => {
                heard.refused.push(data_plane.to_owned());
                format!("refused: {problems:?}")
            }
            Said::Kept {
                data_plane,
                config,
                id,
            } => {
                heard.kept.push((config.clone(), id.map(ToOwned::to_owned)));
                format!("{data_plane}: the leader's kept")
            }
            Said::HeldBack { data_plane, why } => format!("{data_plane} held back: {why}"),
            Said::GatewayApiStatus { object, written } => match written {
                Ok(()) => format!("status of {object}"),
                Err(error) => format!("status of {object} not written: {error}"),
            },
            Said::Service { service, now } => match now {
                ServiceNow::Applied(ports) => format!("Service {service}: {}", ports.join(", ")),
                ServiceNow::Refused(error) => format!("Service {service} refused: {error}"),
                ServiceNow::NotOurs => format!("Service {service} not ours"),
                ServiceNow::NoPort => format!("Service {service} of no port"),
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
            Said::Event { told, written, .. } => match written {
                Ok(written) => format!("event on {} {}: {written:?}", told.object, told.reason),
                Err(error) => format!("event on {} not written: {error}", told.object),
            },
            Said::EventsForbidden(refused) => match refused {
                Some(error) => format!("events forbidden: {error}"),
                None => "events written again".to_owned(),
            },
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
        part: Scope::Cluster,
        joining: false,
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
        rate_limit_policies: vec![follow_of(
            "ratelimitpolicies in shop",
            true,
            vec![RateLimitPolicy {
                metadata: ObjectMeta {
                    name: Some("per-client".to_owned()),
                    namespace: Some("shop".to_owned()),
                    ..ObjectMeta::default()
                },
                ..RateLimitPolicy::default()
            }],
        )],
        own_services: Vec::new(),
        gateway_api: None,
        own_class: false,
        watched: watched(&[
            Scope::Namespace("shop".to_owned()),
            Scope::Namespace("blog".to_owned()),
        ]),
        shape: Shape::default(),
        selected: None,
        parts: Vec::new(),
        scoped: false,
        joining: BTreeSet::new(),
        reports: Vec::new(),
        ready: Vec::new(),
    };
    assert_eq!(
        followed.unlisted().0,
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
    assert_eq!(cluster.rate_limit_policies.len(), 1);
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
        gateway_namespaces: vec![Scope::Namespace("edge".to_owned())],
        last: None,
        gateway_classes: None,
        gateways: None,
        listener_sets: None,
        http_routes: Some(vec![follow_of("httproutes in shop", false, vec![route])]),
        grpc_routes: None,
        tls_routes: None,
        tcp_routes: None,
        udp_routes: None,
        reference_grants: None,
        backend_tls_policies: None,
        config_maps: None,
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
        rate_limit_policies: Vec::new(),
        own_services: Vec::new(),
        gateway_api: Some(gateway_api),
        own_class: false,
        watched: None,
        shape: Shape::default(),
        selected: None,
        parts: Vec::new(),
        scoped: false,
        joining: BTreeSet::new(),
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
    assert_eq!(
        followed.unlisted(),
        (Vec::new(), vec!["crd referencegrants".to_owned()])
    );
}

/// ListenerSets and Secrets are followed where routes are, and in each Gateway's own
/// namespace too, as a Gateway takes ListenerSets `from: Same` there and its listeners'
/// certificateRefs there need no grant, whatever the scope.
#[test]
fn listener_sets_and_secrets_are_followed_in_the_scope_and_the_gateways_namespaces() {
    let edge = Scope::Namespace("edge".to_owned());
    let shop = Scope::Namespace("shop".to_owned());
    let teams = Scope::Namespace("teams".to_owned());
    assert_eq!(
        with_gateway_namespaces(std::slice::from_ref(&shop), std::slice::from_ref(&edge)),
        [shop.clone(), edge.clone()]
    );
    assert_eq!(
        with_gateway_namespaces(&[shop.clone(), edge.clone()], std::slice::from_ref(&edge)),
        [shop.clone(), edge.clone()]
    );
    assert_eq!(
        with_gateway_namespaces(&[Scope::Cluster], std::slice::from_ref(&edge)),
        [Scope::Cluster]
    );
    // Each Gateway's namespace, once.
    assert_eq!(
        with_gateway_namespaces(
            std::slice::from_ref(&shop),
            &[edge.clone(), teams.clone(), shop.clone()]
        ),
        [shop, edge, teams]
    );
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
        } else if named(GatewayKind::ListenerSet) {
            page(&[crd(GatewayKind::ListenerSet, &["v1"])])
        } else if named(GatewayKind::GRPCRoute) {
            page(&[crd(GatewayKind::GRPCRoute, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}grpcroutes?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"calls","namespace":"shop"},"spec":{"parentRefs":[{"name":"web","namespace":"edge"}],"hostnames":["grpc.example"],"rules":[{"backendRefs":[{"name":"calls","port":50051}]}]}}]}"#,
            )
        } else if named(GatewayKind::TLSRoute) {
            page(&[crd(GatewayKind::TLSRoute, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}tlsroutes?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"api","namespace":"shop"},"spec":{"parentRefs":[{"name":"web","namespace":"edge"}],"hostnames":["api.example"],"rules":[{"backendRefs":[{"name":"api","port":8443}]}]}}]}"#,
            )
        } else if named(GatewayKind::TCPRoute) {
            page(&[crd(GatewayKind::TCPRoute, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}tcproutes?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"db","namespace":"shop"},"spec":{"parentRefs":[{"name":"web","namespace":"edge"}],"rules":[{"backendRefs":[{"name":"db","port":5432}]}]}}]}"#,
            )
        } else if named(GatewayKind::UDPRoute) {
            page(&[crd(GatewayKind::UDPRoute, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}udproutes?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"dns","namespace":"shop"},"spec":{"parentRefs":[{"name":"web","namespace":"edge"}],"rules":[{"backendRefs":[{"name":"dns","port":53}]}]}}]}"#,
            )
        } else if named(GatewayKind::BackendTLSPolicy) {
            page(&[crd(GatewayKind::BackendTLSPolicy, &["v1"])])
        } else if path.starts_with(&format!("{gateway_api}backendtlspolicies?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"tls","namespace":"shop"},"spec":{"targetRefs":[{"group":"","kind":"Service","name":"web"}],"validation":{"caCertificateRefs":[{"group":"","kind":"ConfigMap","name":"ca"}],"hostname":"web.example"}}}]}"#,
            )
        } else if path.starts_with("/api/v1/configmaps?") {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"ca","namespace":"shop"},"data":{"ca.crt":"-----BEGIN CERTIFICATE-----"}}]}"#,
            )
        } else if path.starts_with(&format!("{gateway_api}listenersets?")) {
            answer(
                200,
                r#"{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"team","namespace":"shop"},"spec":{"parentRef":{"name":"other","namespace":"edge"},"listeners":[{"name":"team","port":9000,"protocol":"HTTP"}]}}]}"#,
            )
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
            otherwise(path)
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
             listenersets.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             httproutes.gateway.networking.k8s.io: {httproutes}; \
             grpcroutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             tlsroutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             tcproutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             udproutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0); \
             referencegrants.gateway.networking.k8s.io: not served at v1 (only v1beta1; Gateway API v1.7.0), \
             so taken as having no objects; \
             backendtlspolicies.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0)"
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
                    && line.contains(
                        "gatewayclasses in the cluster, gateways in edge, listenersets in the cluster, httproutes in the cluster, grpcroutes in the cluster, tlsroutes in the cluster, tcproutes in the cluster, udproutes in the cluster",
                    )
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
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        fixed(&plane),
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
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
                    // The GRPCRoute followed is translated, first by name; alike in age,
                    // namespace and name, the HTTPRoute before the Ingress (04 §1).
                    [
                        "grpcroute/shop/calls/http",
                        "httproute/shop/web/http",
                        "ingress/shop/web/shop.example"
                    ]
                );
                let report: Vec<String> =
                    translation.report.iter().map(ToString::to_string).collect();
                // Its backend is no Service the cluster has; the TLSRoute, the TCPRoute and
                // the UDPRoute followed reach translation, and the Gateway has no listener to
                // take any; the BackendTLSPolicy and the ConfigMap it names reach it too, the
                // CA read and found no certificate, so that the HTTPRoute's backend fails
                // closed.
                assert_eq!(
                    report,
                    [
                        "TLSRoute shop/api: not attached to the data plane's Gateway: no listener it names takes its kind from its namespace",
                        "TCPRoute shop/db: not attached to the data plane's Gateway: no listener it names takes its kind from its namespace",
                        "UDPRoute shop/dns: not attached to the data plane's Gateway: no listener it names takes its kind from its namespace",
                        "BackendTLSPolicy shop/tls: the connections it covers fail: ConfigMap shop/ca's ca.crt is not PEM certificates",
                        "HTTPRoute shop/web: rule 0: a backend's share is answered 500: Service shop/web port http is covered by BackendTLSPolicy shop/tls, whose TLS cannot be had: ConfigMap shop/ca's ca.crt is not PEM certificates",
                        "GRPCRoute shop/calls: rule 0: a backend's share is answered UNAVAILABLE: Service shop/calls does not exist"
                    ]
                );
            }
            // ListenerSets are listed in the scope routes are, their CRD serving `v1` (this one
            // is another Gateway's, and so says nothing here).
            assert!(
                fake.seen().iter().any(|seen| seen
                    .path
                    .starts_with("/apis/gateway.networking.k8s.io/v1/listenersets?")),
                "ListenerSets never listed"
            );
            // GRPCRoutes likewise, where their CRD serves `v1`.
            assert!(
                fake.seen().iter().any(|seen| seen
                    .path
                    .starts_with("/apis/gateway.networking.k8s.io/v1/grpcroutes?")),
                "GRPCRoutes never listed"
            );
            // TLSRoutes likewise.
            assert!(
                fake.seen().iter().any(|seen| seen
                    .path
                    .starts_with("/apis/gateway.networking.k8s.io/v1/tlsroutes?")),
                "TLSRoutes never listed"
            );
            // TCPRoutes likewise.
            assert!(
                fake.seen().iter().any(|seen| seen
                    .path
                    .starts_with("/apis/gateway.networking.k8s.io/v1/tcproutes?")),
                "TCPRoutes never listed"
            );
            // UDPRoutes likewise.
            assert!(
                fake.seen().iter().any(|seen| seen
                    .path
                    .starts_with("/apis/gateway.networking.k8s.io/v1/udproutes?")),
                "UDPRoutes never listed"
            );
            // BackendTLSPolicies likewise, and the ConfigMaps with them.
            for listed in [
                "/apis/gateway.networking.k8s.io/v1/backendtlspolicies?",
                "/api/v1/configmaps?",
            ] {
                assert!(
                    fake.seen().iter().any(|seen| seen.path.starts_with(listed)),
                    "{listed} never listed"
                );
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
            // The new follow is said waited on before its list goes out: the list counted
            // below is waited for too, or the follow is stopped before it asks.
            until("a new follow of HTTPRoutes listed", || {
                fake.seen()
                    .iter()
                    .filter(|seen| {
                        !seen.path.contains("watch=true") && seen.path.contains("/httproutes?")
                    })
                    .count()
                    == 2
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

/// What is waited on is said once, and again only as it changes; once nothing is, nothing
/// is said, and a wait after it is said again.
#[test]
fn what_is_waited_on_is_said_once() {
    let mut said = None;
    let unlisted = || vec!["ingresses in shop".to_owned(), "secrets in shop".to_owned()];
    let both = unlisted();
    assert_eq!(wait_said(&mut said, unlisted()), Some(both.as_slice()));
    assert_eq!(
        wait_said(&mut said, unlisted()),
        None,
        "the same wait said again"
    );
    let secrets = ["secrets in shop".to_owned()];
    assert_eq!(
        wait_said(&mut said, secrets.to_vec()),
        Some(secrets.as_slice())
    );
    assert_eq!(wait_said(&mut said, Vec::new()), None);
    assert_eq!(
        wait_said(&mut said, secrets.to_vec()),
        Some(secrets.as_slice())
    );
}

#[test]
fn a_translation_of_nothing_routes_nothing() {
    let mut translator = Translator::default();
    let Step::Translated(translation) = translator.step(&plane(), &Cluster::default()) else {
        panic!("not translated");
    };
    assert!(translation.config.routes.is_empty());
}

/// RateLimitPolicy `name` of `shop`, at `generation`, on the Ingress `shop/web`, with a limit
/// of each of `limits`, written `<name> <rate>`.
fn rate_limit_policy(name: &str, generation: i64, limits: &[(&str, &str)]) -> RateLimitPolicy {
    let limits: Vec<String> = limits
        .iter()
        .map(|(limit, rate)| {
            format!("{{ name: {limit}, key: Client, model: {{ bucket: {{ rate: {rate}, burst: 1 }} }} }}")
        })
        .collect();
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: shop, uid: {name}-uid, generation: {generation} }}
spec:
  targetRefs: [{{ group: networking.k8s.io, kind: Ingress, name: web }}]
  limits: [{}]
",
        limits.join(", ")
    ))
    .unwrap()
}

/// A RateLimitPolicy's edit refused keeps its version that applied in the last config handed
/// on applying, translation after translation; a translation the data plane would refuse
/// whole is no config handed on, and what applied stays what the next translates from. A
/// translator afresh, as after a restart, has none to apply (docs/12 item 33).
#[test]
fn a_policy_refused_applies_what_the_last_config_handed_on_applied() {
    let plane = plane_with("rate_limiting: { mode: local }");
    let cluster = |policies: Vec<RateLimitPolicy>| Cluster {
        ingress_classes: vec![class()],
        ingresses: vec![ingress("shop", "web", "shop.example")],
        services: vec![service("shop")],
        secrets: vec![secret("shop", &["shop.example"])],
        rate_limit_policies: policies,
        ..Cluster::default()
    };
    let under = |translation: &Translation| -> Vec<String> {
        translation.config.routes[0].rules[0].rate_limits.clone()
    };
    let mut translator = Translator::default();
    let first = cluster(vec![rate_limit_policy(
        "shop",
        1,
        &[("per-client", "10/s")],
    )]);
    let Step::Translated(translation) = translator.step(&plane, &first) else {
        panic!("not translated");
    };
    assert_eq!(under(translation), ["per-client"]);
    // Its edit renames the limit, translated for settings of 65 global limits, which no
    // policy refused brings under the most a rule takes: the config is refused, so the edit
    // never applied.
    let names: Vec<String> = (0..65).map(|n| format!("global-{n}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let renamed = cluster(vec![rate_limit_policy("shop", 2, &[("renamed", "10/s")])]);
    let Step::Refused { .. } = translator.step(&limiting(&names), &renamed) else {
        panic!("a rule under 65 limits not refused");
    };
    // An edit the data plane would refuse: the version of the last config handed on applies.
    let refused = cluster(vec![rate_limit_policy(
        "shop",
        3,
        &[("per-client", "10/fortnight")],
    )]);
    for _ in 0..2 {
        let Step::Translated(translation) = translator.step(&plane, &refused) else {
            panic!("not translated");
        };
        assert_eq!(under(translation), ["per-client"]);
        let generations: Vec<Option<i64>> = translation
            .rate_limit_policies
            .iter()
            .map(|applied| applied.generation)
            .collect();
        assert_eq!(generations, [Some(1)]);
        // Translated again, as a change elsewhere would have it.
        translator.last = None;
    }
    let mut afresh = Translator::default();
    let Step::Translated(translation) = afresh.step(&plane, &refused) else {
        panic!("not translated");
    };
    assert_eq!(under(translation), Vec::<String>::new());
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
    let Step::Translated(translation) = translator.step(&plane, &shop) else {
        panic!("not translated");
    };
    assert_eq!(
        routes(translation),
        ["ingress/shop/shop.example/shop.example"]
    );
    assert_eq!(translator.step(&plane, &shop), Step::Nothing);
    let both = cluster(&["shop.example", "blog.example"]);
    let Step::Translated(translation) = translator.step(&plane, &both) else {
        panic!("a change not translated");
    };
    assert_eq!(translation.config.routes.len(), 2);
}

/// Each object is compiled alone as it is translated, so only the data plane's own settings
/// make a whole config it would refuse: those of the port an Ingress is served on, trusting
/// a network written wrongly.
#[test]
fn a_config_the_data_plane_would_refuse_is_said_once_and_not_handed_on() {
    let plane = plane_with(
        "ports:
  80: { forwarding: { trusted_proxies: [\"10.1.2.3/8\"], trusted_only_headers: [] } }",
    );
    let cluster = Cluster {
        ingress_classes: vec![class()],
        ingresses: vec![ingress("shop", "shop.example", "shop.example")],
        services: vec![service("shop")],
        ..Cluster::default()
    };
    let mut translator = Translator::default();
    let Step::Refused { problems, .. } = translator.step(&plane, &cluster) else {
        panic!("a config of a port's settings the data plane refuses handed on");
    };
    assert!(
        problems
            .iter()
            .any(|problem| problem.to_string().starts_with("listener `port-80`")),
        "{problems:?}"
    );
    assert_eq!(translator.step(&plane, &cluster), Step::Nothing);
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
            otherwise(path)
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
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        fixed(&plane),
                        None,
                        debounce,
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
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
                 services in the cluster, secrets in the cluster, \
                 ratelimitpolicies in the cluster"
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

/// The RateLimitPolicy `shop/shop` on the Ingress `shop/web`, holding the limit
/// `per-client`, as the API server lists it.
const PER_CLIENT_POLICY: &str = r#"{"kind":"RateLimitPolicyList","metadata":{"resourceVersion":"5"},"items":[{"apiVersion":"gateway.edgerush.dev/v1alpha1","kind":"RateLimitPolicy","metadata":{"name":"shop","namespace":"shop","uid":"shop-uid","generation":1},"spec":{"targetRefs":[{"group":"networking.k8s.io","kind":"Ingress","name":"web"}],"limits":[{"name":"per-client","key":"Client","model":{"bucket":{"rate":"10/s","burst":1}}}]}}]}"#;

/// Local rate limiting, under a global limit of each of `names`.
fn limiting(names: &[&str]) -> Plane {
    let global: Vec<String> = names
        .iter()
        .map(|name| {
            format!(
                "{{ name: {name}, key: client, model: {{ bucket: {{ rate: 100/s, burst: 100 }} }} }}"
            )
        })
        .collect();
    plane_with(&format!(
        "rate_limiting: {{ mode: local, global: [{}] }}",
        global.join(", ")
    ))
}

/// A DataPlane's generation whose global limit takes a name a RateLimitPolicy holds is held
/// back whole, said once, the generation before served (04 §4); once the policy is edited
/// to let the name go, the config handed on without it, the generation held back is taken
/// on, with no other change to bring it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_taking_a_name_a_policy_holds_is_held_back() {
    held_back_then_taken_on(false).await;
}

/// As [`a_data_plane_taking_a_name_a_policy_holds_is_held_back`], of a leader that takes
/// snapshots: it judges the generation held back itself, round after round, never as a
/// follower of its own snapshot would, which takes the leader's verdict and judges none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_taking_snapshots_judges_what_it_holds_back_itself() {
    held_back_then_taken_on(true).await;
}

/// The generation held back, then taken on once its name is free; with `snapshots`, by a
/// leader that takes them.
async fn held_back_then_taken_on(snapshots: bool) {
    let freeing = Arc::new(AtomicBool::new(false));
    let told = Arc::clone(&freeing);
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("/ratelimitpolicies?") && path.contains("watch=true") {
            if !told.swap(false, Ordering::SeqCst) {
                return short();
            }
            let freed = PER_CLIENT_POLICY
                .split_once(r#""items":["#)
                .and_then(|(_, item)| item.strip_suffix("]}"))
                .unwrap()
                .replace(
                    r#""generation":1"#,
                    r#""generation":2,"resourceVersion":"6""#,
                )
                .replace("per-client", "own");
            return Answer {
                chunks: vec![Bytes::from(format!(
                    "{{\"type\":\"MODIFIED\",\"object\":{freed}}}\n"
                ))],
                ..short()
            };
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, PER_CLIENT_POLICY)
        } else if path.starts_with("/apis/apiextensions.k8s.io/v1/customresourcedefinitions?") {
            page::<CustomResourceDefinition>(&[])
        } else if path.starts_with("/api/v1/namespaces?") {
            page::<Namespace>(&[])
        } else {
            otherwise(path)
        }
    })
    .await;
    let crds_listed = || {
        fake.seen()
            .iter()
            .any(|seen| seen.path.contains("/customresourcedefinitions?"))
    };
    // The generation held back is of a Gateway too, which the one before is not: what is
    // followed is what the generation served needs, the Gateway API's kinds only once it is
    // taken on.
    let mut wanted = limiting(&["per-client"]);
    wanted.gateway = Some(GatewayName::try_from("edge/web".to_owned()).unwrap());
    let api = fake.client(&token("controller-held-back", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(only(harness(&limiting(&[]))));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        snapshots.then(|| Snapshots {
                            snapshot: watch::channel(None).0,
                            settings: Rc::new(|_| None),
                        }),
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let under = |at: usize| -> Vec<String> {
                heard.borrow().translations[at].config.routes[0].rules[0]
                    .rate_limits
                    .clone()
            };
            until("translated", || heard.borrow().translations.len() == 1).await;
            assert_eq!(under(0), ["per-client"]);

            settings.send_replace(only(harness(&wanted)));
            let held = "held back: its global limit `per-client` has the name of \
                        RateLimitPolicy shop/shop's, which holds it";
            until("held back", || {
                heard.borrow().lines.iter().any(|line| line.contains(held))
            })
            .await;
            // Sent again, as another DataPlane's change would: said once.
            settings.send_replace(only(harness(&wanted)));
            tokio::time::sleep(Duration::from_millis(300)).await;
            let said = heard
                .borrow()
                .lines
                .iter()
                .filter(|line| line.contains(held))
                .count();
            assert_eq!(said, 1, "held back said once");
            assert_eq!(
                heard.borrow().translations.len(),
                1,
                "the held back translated"
            );
            assert!(!crds_listed(), "followed for the generation held back");

            // The policy lets the name go: handed on without it, then the generation held
            // back taken on.
            freeing.store(true, Ordering::SeqCst);
            until("taken on", || heard.borrow().translations.len() == 3).await;
            assert_eq!(under(1), ["own"]);
            assert_eq!(under(2), ["per-client", "own"]);
            assert!(crds_listed(), "not followed for the generation taken on");
            controlling.abort();
        })
        .await;
}

/// `plane` as DataPlane `edge/web` at `generation` serves it.
fn of_generation(plane: &Plane, generation: i64) -> Settings {
    let mut object = web_data_plane("");
    object.metadata.generation = Some(generation);
    Settings {
        data_plane: Some(DataPlaneOf {
            namespace: "edge".to_owned(),
            name: "web".to_owned(),
            generation,
            object: Arc::new(object),
        }),
        ..harness(plane)
    }
}

/// The path DataPlane `edge/web`'s status is applied at.
const SERVED_APPLIED: &str = "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/dataplanes/web/status?fieldManager=edgerush-controller&force=true";

/// The DataPlane's `Served`, written by the leader alone (05 §1): true of the generation
/// served; false `HeldBack` of one held back, saying why without naming the policy and
/// which generation is served, once however often it is offered again; true of it once it
/// is taken on. Its own condition alone, applied under the control plane's field manager.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_planes_served_says_which_generation_is_served_and_why_one_is_held_back() {
    let freeing = Arc::new(AtomicBool::new(false));
    let told = Arc::clone(&freeing);
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path == SERVED_APPLIED {
            return answer(200, "{}");
        }
        if path.contains("/ratelimitpolicies?") && path.contains("watch=true") {
            if !told.swap(false, Ordering::SeqCst) {
                return short();
            }
            let freed = PER_CLIENT_POLICY
                .split_once(r#""items":["#)
                .and_then(|(_, item)| item.strip_suffix("]}"))
                .unwrap()
                .replace(
                    r#""generation":1"#,
                    r#""generation":2,"resourceVersion":"6""#,
                )
                .replace("per-client", "own");
            return Answer {
                chunks: vec![Bytes::from(format!(
                    "{{\"type\":\"MODIFIED\",\"object\":{freed}}}\n"
                ))],
                ..short()
            };
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, PER_CLIENT_POLICY)
        } else {
            otherwise(path)
        }
    })
    .await;
    let applied = || -> Vec<serde_json::Value> {
        fake.seen()
            .iter()
            .filter(|seen| seen.path == SERVED_APPLIED)
            .map(|seen| {
                assert_eq!(seen.method, "PATCH");
                serde_json::from_str(&seen.body).unwrap()
            })
            .collect()
    };
    let api = fake.client(&token("controller-served", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(only(of_generation(&limiting(&[]), 1)));
            let (leads, leading) = watch::channel(false);
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        Some(leading),
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || heard.borrow().translations.len() == 1).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(applied().len(), 0, "written by a replica not leading");
            leads.send_replace(true);
            until("served written as it leads", || applied().len() == 1).await;
            settings.send_replace(only(of_generation(&limiting(&["per-client"]), 2)));
            until("held back written", || applied().len() == 2).await;
            // Offered again, as another DataPlane's change would: written once.
            settings.send_replace(only(of_generation(&limiting(&["per-client"]), 2)));
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(applied().len(), 2, "held back written again");
            freeing.store(true, Ordering::SeqCst);
            until("taken on written", || applied().len() == 3).await;
            controlling.abort();
        })
        .await;
    let said: Vec<(String, String, String, i64)> = applied()
        .iter()
        .map(|body| {
            let conditions = body["status"]["conditions"].as_array().unwrap();
            assert_eq!(conditions.len(), 1, "its own condition alone: {body}");
            let served = &conditions[0];
            assert_eq!(served["type"], "Served");
            (
                served["status"].as_str().unwrap().to_owned(),
                served["reason"].as_str().unwrap().to_owned(),
                served["message"].as_str().unwrap().to_owned(),
                served["observedGeneration"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        said,
        [
            (
                "True".to_owned(),
                "Served".to_owned(),
                "generation 1 is what its data planes are given".to_owned(),
                1
            ),
            (
                "False".to_owned(),
                "HeldBack".to_owned(),
                "generation 2 is held back, generation 1 served in its place: its global \
                 limit `per-client` has the name of a limit a RateLimitPolicy holds"
                    .to_owned(),
                2
            ),
            (
                "True".to_owned(),
                "Served".to_owned(),
                "generation 2 is what its data planes are given".to_owned(),
                2
            ),
        ]
    );
}

/// The objects of `shop` with `policy` on the Ingress `shop/web`.
fn with_policy(policy: RateLimitPolicy) -> Cluster {
    Cluster {
        ingress_classes: vec![class()],
        ingresses: vec![ingress("shop", "web", "shop.example")],
        services: vec![service("shop")],
        secrets: vec![secret("shop", &["shop.example"])],
        rate_limit_policies: vec![policy],
        ..Cluster::default()
    }
}

/// A generation that adds global limits is not taken on as it is offered but judged; one
/// that adds none, or none any more, is. Judged, it is held back while a limit's name is a
/// policy's, and may be taken on once the config handed on lets go of it.
#[test]
fn a_data_plane_held_back_is_taken_on_once_its_name_is_free() {
    let served_with = harness(&limiting(&[]));
    let mut served = Serving::new(served_with.clone());
    let holding = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    let Step::Translated(_) = served.translator.step(&served.settings.plane, &holding) else {
        panic!("not translated");
    };
    let wanted = harness(&limiting(&["per-client"]));
    served.offer(&wanted);
    assert_eq!(served.settings, served_with, "taken on unjudged");
    assert_eq!(served.wanted.as_ref(), Some(&wanted));
    assert_eq!(
        served
            .judge(&wanted, &holding)
            .map(|why| why.to_string())
            .as_deref(),
        Some(
            "its global limit `per-client` has the name of RateLimitPolicy shop/shop's, which holds it"
        )
    );
    let freed = with_policy(rate_limit_policy("shop", 2, &[("own", "10/s")]));
    let Step::Translated(_) = served.translator.step(&served.settings.plane, &freed) else {
        panic!("not translated");
    };
    assert_eq!(served.judge(&wanted, &freed), None);
    // Offered what adds no global limit to what is served, it is taken on there and then.
    let mut edited = served_with.clone();
    edited.plane.ingress_https_redirect = true;
    served.offer(&edited);
    assert_eq!(served.settings, edited);
    assert_eq!(served.wanted, None);
}

/// A generation whose global limits would put a rule over the most limits a rule takes,
/// where the generation served does not, is held back: whether the config would be refused
/// whole, as no policy's change is left to refuse, or a policy's change refused for it. One
/// that fits is taken on, and so is one where a rule is over under the generation served.
#[test]
fn a_data_plane_putting_a_rule_over_the_most_limits_is_held_back() {
    let names: Vec<String> = (0..63).map(|n| format!("big-{n}")).collect();
    let limits: Vec<(&str, &str)> = names.iter().map(|name| (name.as_str(), "10/s")).collect();
    let too_many = Some(HeldBack::TooManyLimits);
    // A policy as it was accepted: the config would be refused whole.
    let cluster = with_policy(rate_limit_policy("shop", 1, &limits));
    let mut served = Serving::new(harness(&limiting(&[])));
    let Step::Translated(translation) = served.translator.step(&served.settings.plane, &cluster)
    else {
        panic!("not translated");
    };
    assert_eq!(translation.config.routes[0].rules[0].rate_limits.len(), 63);
    let fits = harness(&limiting(&["platform"]));
    assert_eq!(served.judge(&fits, &cluster), None);
    let over = harness(&limiting(&["platform", "more"]));
    assert_eq!(served.judge(&over, &cluster), too_many);
    assert_eq!(
        HeldBack::TooManyLimits.to_string(),
        "its global limits would put a rule over the 64 limits a rule takes"
    );
    // A policy's change, from 60 limits to 62: refused under three global limits, its
    // version last accepted applying.
    let accepted = rate_limit_policy("shop", 1, &limits[..60]);
    let changed = with_policy(rate_limit_policy("shop", 2, &limits[..62]));
    let mut served = Serving::new(harness(&limiting(&[])));
    served.translator.accepted = vec![AcceptedPolicy::of(&accepted)];
    let three = harness(&limiting(&["platform", "more", "most"]));
    assert_eq!(served.judge(&three, &changed), too_many);
    assert_eq!(served.judge(&fits, &changed), None);
    // A rule over under the generation served already is no reason to hold one back.
    let second: Vec<String> = (0..40).map(|n| format!("second-{n}")).collect();
    let second: Vec<(&str, &str)> = second.iter().map(|name| (name.as_str(), "10/s")).collect();
    let both = [
        rate_limit_policy("first", 1, &limits[..40]),
        rate_limit_policy("second", 1, &second),
    ];
    let crowded = Cluster {
        rate_limit_policies: both.to_vec(),
        ..cluster.clone()
    };
    let mut served = Serving::new(harness(&limiting(&[])));
    served.translator.accepted = both.iter().map(AcceptedPolicy::of).collect();
    let Step::Refused { .. } = served.translator.step(&served.settings.plane, &crowded) else {
        panic!("a rule under 81 limits not refused");
    };
    assert_eq!(served.judge(&fits, &crowded), None);
}

/// Settings that change are followed (05 §3): none, and nothing is followed; new settings
/// are translated again, the follows as they were; another data plane's ID has a
/// translation said for that ID, though it is the last one's; and a Gateway has the Gateway
/// API's kinds followed beside the rest, nothing begun again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_settings_are_followed_as_they_change() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?")
            || path.starts_with("/api/v1/namespaces/edge/secrets?")
        {
            page::<Secret>(&[])
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(&token("controller-settings", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(Planes::new());
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let lists = |path: &str| {
                fake.seen()
                    .iter()
                    .filter(|seen| {
                        seen.path.starts_with(&format!("{path}?"))
                            && !seen.path.contains("watch=true")
                    })
                    .count()
            };
            let ingresses = || lists("/apis/networking.k8s.io/v1/ingresses");
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(fake.seen().is_empty(), "followed without settings");

            settings.send_replace(only(harness(&plane())));
            until("translated", || heard.borrow().translations.len() == 1).await;
            assert_eq!(
                routes(&heard.borrow().translations[0]),
                ["ingress/shop/web/shop.example"]
            );
            assert_eq!(ingresses(), 1);

            // Another class: the Ingress is no longer the data plane's, and nothing more is
            // followed or listed again for it.
            let mut other = plane();
            other.ingress_class = Some("other".to_owned());
            settings.send_replace(only(harness(&other)));
            until("translated again", || {
                heard.borrow().translations.len() == 2
            })
            .await;
            assert!(routes(&heard.borrow().translations[1]).is_empty());
            assert_eq!(
                ingresses(),
                1,
                "listed again for settings of the same shape"
            );
            assert_eq!(heard.borrow().ids, [None, None]);

            // Another data plane's ID: what is translated for one is never handed to another.
            let web = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-web";
            other.id = Some(web.to_owned());
            settings.send_replace(only(harness(&other)));
            until("translated for the ID", || {
                heard.borrow().translations.len() == 3
            })
            .await;
            assert_eq!(
                heard.borrow().translations[2],
                heard.borrow().translations[1]
            );
            assert_eq!(heard.borrow().ids[2].as_deref(), Some(web));
            assert_eq!(ingresses(), 1, "followed again for another ID");

            // A Gateway: the Gateway API's kinds are followed beside what was, which goes on
            // as it was (05 §3).
            settings.send_replace(only(harness(&gateway_plane())));
            until("the Gateway API's CRDs listed", || {
                lists("/apis/apiextensions.k8s.io/v1/customresourcedefinitions") > 0
            })
            .await;
            assert_eq!(ingresses(), 1, "the Ingresses followed again for a Gateway");
            controlling.abort();
        })
        .await;
}

/// Secrets are followed in the Gateway's namespace beside the scope's, where its listeners'
/// certificateRefs of their own namespace are; nothing else is followed there but the
/// Gateway's own kinds (04 §1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secrets_are_followed_in_the_gateways_namespace_beside_the_scope() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/api/v1/namespaces/edge/secrets?") {
            page(&[secret("edge", &["secure.example"])])
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-gateway-secrets", "secret"),
        Timing::default(),
    );
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = gateway_plane();
            let scope = WatchScope::Parts(vec![Scope::Namespace("shop".to_owned())]);
            let controlling = tokio::task::spawn_local(async move {
                control(
                    api,
                    &scope,
                    fixed(&plane),
                    None,
                    Debounce::default(),
                    None,
                    acks(),
                    None,
                    "replica".to_owned(),
                    |_| {},
                )
                .await;
            });
            let listed = |path: &str| {
                fake.seen()
                    .iter()
                    .any(|seen| seen.path.starts_with(&format!("{path}?")))
            };
            until("the Gateway's namespace's Secrets listed", || {
                listed("/api/v1/namespaces/edge/secrets")
            })
            .await;
            assert!(listed("/api/v1/namespaces/shop/secrets"));
            for elsewhere in [
                "/api/v1/namespaces/edge/services",
                "/apis/networking.k8s.io/v1/namespaces/edge/ingresses",
            ] {
                assert!(!listed(elsewhere), "{elsewhere} listed");
            }
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
            _ => otherwise(path),
        }
    })
    .await;
    let api = fake.client(&token("controller-namespaces", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let scope = WatchScope::Parts(vec![
                Scope::Namespace("shop".to_owned()),
                Scope::Namespace("blog".to_owned()),
            ]);
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &scope,
                        fixed(&plane),
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
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
            "/apis/gateway.edgerush.dev/v1alpha1/namespaces/blog/ratelimitpolicies",
            "/apis/gateway.edgerush.dev/v1alpha1/namespaces/shop/ratelimitpolicies",
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

/// An Ingress of a class of ControlPlane `edge`'s, `edge.<dataplane>`, that no DataPlane
/// may still serve loses its address (04 §6); every other is left as it is.
#[test]
fn an_ingress_of_a_class_no_data_plane_may_serve_is_let_go_of() {
    let held = ["192.0.2.1"];
    let ingresses = [
        // Of a DataPlane gone, holding an address.
        ingress_saying("gone", "edge.web", &held, &[]),
        // Of one that may still serve it.
        ingress_saying("kept", "edge.api", &held, &[]),
        // Of one gone, but saying nothing already.
        ingress_saying("quiet", "edge.web", &[], &[]),
        // Another ControlPlane's, one of a name that begins as this one's does, and
        // classes no ControlPlane's DataPlane is of.
        ingress_saying("other", "other.web", &held, &[]),
        ingress_saying("longer", "edgeless.web", &held, &[]),
        ingress_saying("bare", "edge", &held, &[]),
        ingress_saying("dotted", "edge.web.more", &held, &[]),
        ingress_saying("empty", "edge.", &held, &[]),
    ];
    // Named by the old annotation alone, as translation reads it.
    let mut annotated = ingress_saying(
        "annotated",
        "",
        &held,
        &[("kubernetes.io/ingress.class", "edge.old")],
    );
    annotated.spec.as_mut().unwrap().ingress_class_name = None;
    // Of no class at all: nothing says whose it was.
    let mut classless = ingress_saying("classless", "", &held, &[]);
    classless.spec.as_mut().unwrap().ingress_class_name = None;
    let ingresses: Vec<Ingress> = ingresses
        .into_iter()
        .chain([annotated, classless])
        .collect();
    let kept = BTreeSet::from(["edge.api".to_owned()]);
    let released = |name: &str| status::Change {
        ingress: object(name),
        addresses: Vec::new(),
    };
    assert_eq!(
        status::released("edge", &kept, &ingresses),
        [released("gone"), released("annotated")]
    );
}

#[test]
fn an_address_is_an_ip_else_a_host_name() {
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
            page(&[as_wanted(
                &operated(&plane(), ""),
                &[80, 443],
                &["203.0.113.7"],
            )])
        } else if path == SERVICE_APPLIED {
            answer(
                200,
                r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"edgerush-proxy-web"}}"#,
            )
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            otherwise(path)
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
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(only(operated(&plane, ""))).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
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
        .filter(|seen| seen.method == "PATCH" && seen.path != SERVICE_APPLIED)
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

/// A RateLimitPolicy's status is written of each ancestor it reaches, here the Ingress it
/// targets, with no Gateway API followed: its `Accepted` alone, under this controller's
/// name, and once, as nothing changes after; this controller's entry of an Ingress of the
/// data plane's class it no longer reaches taken away, another controller's kept (04 §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rate_limit_policys_status_is_written_of_the_ingress_it_reaches_and_once() {
    let entry = |controller: &str, name: &str| {
        format!(
            r#"{{"ancestorRef":{{"group":"networking.k8s.io","kind":"Ingress","name":"{name}","namespace":"shop"}},"controllerName":"{controller}","conditions":[]}}"#
        )
    };
    let theirs = entry("example.com/other", "old");
    let with_status = PER_CLIENT_POLICY.replace(
        r#""spec":"#,
        &format!(
            r#""status":{{"ancestors":[{},{theirs}]}},"spec":"#,
            entry(GATEWAY_CONTROLLER, "old")
        ),
    );
    let theirs: serde_json::Value = serde_json::from_str(&theirs).unwrap();
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
                ingress("shop", "web", "shop.example"),
                ingress("shop", "old", "old.example"),
            ])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, &with_status)
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-limit-status", "secret"),
        Timing::default(),
    );
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        fixed(&limiting(&[])),
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let written = || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .filter(|line| line.starts_with("status of RateLimitPolicy shop/shop"))
                    .count()
            };
            until("the policy's status written", || written() == 1).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(written(), 1, "written again");
            controlling.abort();
        })
        .await;
    let bodies: Vec<serde_json::Value> = fake
        .seen()
        .iter()
        .filter(|seen| seen.path.ends_with("/ratelimitpolicies/shop/status"))
        .map(|seen| {
            assert_eq!(seen.method, "PUT");
            assert_eq!(
                seen.path,
                "/apis/gateway.edgerush.dev/v1alpha1/namespaces/shop/ratelimitpolicies/shop/status"
            );
            serde_json::from_str(&seen.body).unwrap()
        })
        .collect();
    assert_eq!(bodies.len(), 1);
    let ancestors = &bodies[0]["status"]["ancestors"];
    assert_eq!(ancestors.as_array().unwrap().len(), 2, "{ancestors}");
    assert_eq!(ancestors[0], theirs, "another controller's");
    let ancestors = &ancestors.as_array().unwrap()[1..];
    assert_eq!(
        ancestors[0]["ancestorRef"],
        serde_json::json!({ "group": "networking.k8s.io", "kind": "Ingress", "name": "web",
                            "namespace": "shop" })
    );
    assert_eq!(ancestors[0]["controllerName"], GATEWAY_CONTROLLER);
    let conditions = ancestors[0]["conditions"].as_array().unwrap();
    assert_eq!(conditions.len(), 1, "{conditions:?}");
    assert_eq!(
        (
            &conditions[0]["type"],
            &conditions[0]["status"],
            &conditions[0]["observedGeneration"]
        ),
        (
            &serde_json::json!("Accepted"),
            &serde_json::json!("True"),
            &serde_json::json!(1)
        )
    );
}

/// The data plane's Service not to be read: translation goes on all the same, and no status,
/// nor the Service, is written until it is listed.
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
            otherwise(path)
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
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(only(operated(&plane, ""))).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
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
        "a status, or the Service, written with no Service listed"
    );
}

/// A replica that does not lead writes no status, nor the data plane's Service; as it comes
/// to, it writes each that says otherwise, and the Service, nothing else having changed.
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
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            page(&[as_wanted(
                &operated(&plane(), ""),
                &[80, 443],
                &["203.0.113.7"],
            )])
        } else if path == SERVICE_APPLIED {
            answer(
                200,
                r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"edgerush-proxy-web"}}"#,
            )
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            otherwise(path)
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
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(only(operated(&plane, ""))).1,
                        None,
                        Debounce::default(),
                        Some(leading),
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || !heard.borrow().translations.is_empty()).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                fake.seen().iter().all(|seen| seen.method != "PATCH"),
                "a status, or the Service, written by a replica that does not lead"
            );
            leads.send_replace(true);
            until("the Service and the status written once it leads", || {
                let heard = heard.borrow();
                let said = |wanted: &str| heard.lines.iter().any(|line| line == wanted);
                said("status of Ingress shop/web: [203.0.113.7]")
                    && said("Service edge/edgerush-proxy-web: 80/TCP, 443/TCP")
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
  listeners:
    - { name: http, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }
    - { name: tls, port: 8443, protocol: TLS, tls: { mode: Passthrough }, allowedRoutes: { namespaces: { from: All } } }
    - { name: tcp, port: 5432, protocol: TCP, allowedRoutes: { namespaces: { from: All } } }
    - { name: udp, port: 5353, protocol: UDP, allowedRoutes: { namespaces: { from: All } } }",
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
    // A GRPCRoute's written as an HTTPRoute's: one served, and one that named the Gateway
    // once, read through the HTTPRoute's shape and merged.
    let calls: GRPCRoute = serde_saphyr::from_str(
        "metadata: { name: calls, namespace: shop, generation: 4, resourceVersion: '9' }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    let gone: GRPCRoute = serde_saphyr::from_str(
        "metadata: { name: gone, namespace: shop, generation: 1, resourceVersion: '10' }
spec: { parentRefs: [{ name: elsewhere, namespace: edge }] }
status:
  parents:
    - controllerName: example.com/other
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: elsewhere, namespace: edge, sectionName: grpc, port: 9090 }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]
    - controllerName: edgerush.dev/gateway-controller
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: web, namespace: edge }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]",
    )
    .unwrap();
    // A TLSRoute's written as an HTTPRoute's too.
    let tunnel: TLSRoute = serde_saphyr::from_str(
        "metadata: { name: tunnel, namespace: shop, generation: 5, resourceVersion: '11' }
spec:
  parentRefs: [{ name: web, namespace: edge, sectionName: tls }]
  hostnames: [db.example.com]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    // And a TCPRoute's.
    let db: TCPRoute = serde_saphyr::from_str(
        "metadata: { name: db, namespace: shop, generation: 6, resourceVersion: '12' }
spec:
  parentRefs: [{ name: web, namespace: edge, sectionName: tcp }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    // And a UDPRoute's.
    let dns: UDPRoute = serde_saphyr::from_str(
        "metadata: { name: dns, namespace: shop, generation: 8, resourceVersion: '16' }
spec:
  parentRefs: [{ name: web, namespace: edge, sectionName: udp }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    // A BackendTLSPolicy the HTTPRoute reaches, its one CA certificate ref of a kind none
    // can be: its status says so of the Gateway.
    let tls: BackendTLSPolicy = serde_saphyr::from_str(
        "metadata: { name: tls, namespace: shop, generation: 7, resourceVersion: '13' }
spec:
  targetRefs: [{ group: '', kind: Service, name: web }]
  validation:
    hostname: web.shop.example
    caCertificateRefs: [{ group: '', kind: Secret, name: ca }]",
    )
    .unwrap();
    // One no route reaches whose status holds an ancestor of ours: it is taken away, another
    // controller's kept.
    let old_tls: BackendTLSPolicy = serde_saphyr::from_str(
        "metadata: { name: old-tls, namespace: shop, generation: 1, resourceVersion: '14' }
spec:
  targetRefs: [{ group: '', kind: Service, name: elsewhere }]
  validation: { hostname: elsewhere.shop.example, wellKnownCACertificates: System }
status:
  ancestors:
    - controllerName: edgerush.dev/gateway-controller
      ancestorRef: { group: gateway.networking.k8s.io, kind: Gateway, name: web, namespace: edge }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]
    - controllerName: example.com/other
      ancestorRef: { group: gateway.networking.k8s.io, kind: Gateway, name: elsewhere, namespace: edge }
      conditions: []",
    )
    .unwrap();
    // And one no route reaches with no status: none is written.
    let quiet: BackendTLSPolicy = serde_saphyr::from_str(
        "metadata: { name: quiet, namespace: shop, generation: 1, resourceVersion: '15' }
spec:
  targetRefs: [{ group: '', kind: Service, name: elsewhere }]
  validation: { hostname: elsewhere.shop.example, wellKnownCACertificates: System }",
    )
    .unwrap();
    let known = [
        "/apis/gateway.networking.k8s.io/v1/gatewayclasses/edgerush/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/edge/gateways/web/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/app/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/old/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/grpcroutes/calls/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/grpcroutes/gone/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/tlsroutes/tunnel/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/tcproutes/db/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/udproutes/dns/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/backendtlspolicies/tls/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/backendtlspolicies/old-tls/status",
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
        grpc_routes: vec![calls.clone(), gone.clone()],
        tls_routes: vec![tunnel.clone()],
        tcp_routes: vec![db.clone()],
        udp_routes: vec![dns.clone()],
        backend_tls_policies: vec![tls.clone(), old_tls.clone(), quiet.clone()],
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
    let following = |classes: Vec<GatewayClass>,
                     gateways: Vec<Gateway>,
                     routes,
                     grpc_routes,
                     tls_routes,
                     tcp_routes,
                     udp_routes,
                     policies| {
        GatewayApi {
            crds: Vec::new(),
            namespaces: Vec::new(),
            gateway_namespaces: vec![Scope::Namespace("edge".to_owned())],
            last: Some(served.clone()),
            gateway_classes: Some(vec![follow_of("gatewayclasses", true, classes)]),
            gateways: Some(vec![follow_of("gateways in edge", true, gateways)]),
            listener_sets: None,
            http_routes: Some(vec![follow_of("httproutes", true, routes)]),
            grpc_routes: Some(vec![follow_of("grpcroutes", true, grpc_routes)]),
            tls_routes: Some(vec![follow_of("tlsroutes", true, tls_routes)]),
            tcp_routes: Some(vec![follow_of("tcproutes", true, tcp_routes)]),
            udp_routes: Some(vec![follow_of("udproutes", true, udp_routes)]),
            reference_grants: None,
            backend_tls_policies: Some(vec![follow_of("backendtlspolicies", true, policies)]),
            config_maps: None,
        }
    };
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let said = hear(&heard);
    let addresses = gateway_status::addresses(&[status::address("192.0.2.10")]);
    let programmed = Programmed::of(2, &acks::Pods::new(), None, false);
    let kinds = following(
        vec![class.clone()],
        vec![gateway.clone()],
        vec![app.clone(), old.clone()],
        vec![calls.clone(), gone.clone()],
        vec![tunnel.clone()],
        vec![db.clone()],
        vec![dns.clone()],
        vec![tls.clone(), old_tls.clone(), quiet.clone()],
    );
    kinds
        .write_own(&api, &handed, &addresses, &programmed, &said)
        .await;
    kinds.write_shared(&api, &[&handed], None, &said).await;
    assert_eq!(
        heard.borrow().lines,
        [
            "status of Gateway edge/web",
            "status of GatewayClass edgerush",
            "status of HTTPRoute shop/app",
            "status of HTTPRoute shop/old",
            "status of GRPCRoute shop/calls",
            "status of GRPCRoute shop/gone",
            "status of TLSRoute shop/tunnel",
            "status of TCPRoute shop/db",
            "status of UDPRoute shop/dns",
            "status of BackendTLSPolicy shop/tls",
            "status of BackendTLSPolicy shop/old-tls",
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
        bodies[1]["metadata"]["resourceVersion"], "5",
        "written as of the version read"
    );
    assert_eq!(status(1)["conditions"][0]["type"], "Accepted");
    assert_eq!(status(1)["conditions"][1]["type"], "SupportedVersion");
    assert_eq!(status(1)["conditions"][1]["status"], "True");
    assert_eq!(status(0)["addresses"][0]["value"], "192.0.2.10");
    assert_eq!(status(0)["listeners"][0]["attachedRoutes"], 2);
    assert_eq!(status(0)["listeners"][1]["attachedRoutes"], 1);
    assert_eq!(status(0)["listeners"][2]["attachedRoutes"], 1);
    assert_eq!(status(0)["listeners"][3]["attachedRoutes"], 1);
    assert_eq!(status(0)["conditions"][0]["observedGeneration"], 2);
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
    assert_eq!(
        fake.seen()[4].path,
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/grpcroutes/calls/status"
    );
    assert_eq!(bodies[4]["kind"], "GRPCRoute");
    assert_eq!(
        status(4)["parents"][0]["controllerName"],
        GATEWAY_CONTROLLER
    );
    assert_eq!(
        status(4)["parents"][0]["conditions"][0]["observedGeneration"],
        4
    );
    // Another controller's parent kept whole, its section and port among it.
    assert_eq!(
        status(5)["parents"],
        serde_json::to_value(&gone.status.as_ref().unwrap().parents[..1]).unwrap()
    );
    assert_eq!(
        fake.seen()[6].path,
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/tlsroutes/tunnel/status"
    );
    assert_eq!(bodies[6]["kind"], "TLSRoute");
    assert_eq!(status(6)["parents"][0]["parentRef"]["sectionName"], "tls");
    assert_eq!(
        status(6)["parents"][0]["conditions"][0]["observedGeneration"],
        5
    );
    assert_eq!(
        fake.seen()[7].path,
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/tcproutes/db/status"
    );
    assert_eq!(bodies[7]["kind"], "TCPRoute");
    assert_eq!(status(7)["parents"][0]["parentRef"]["sectionName"], "tcp");
    assert_eq!(
        status(7)["parents"][0]["conditions"][0]["observedGeneration"],
        6
    );
    assert_eq!(
        fake.seen()[8].path,
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/udproutes/dns/status"
    );
    assert_eq!(bodies[8]["kind"], "UDPRoute");
    assert_eq!(status(8)["parents"][0]["parentRef"]["sectionName"], "udp");
    assert_eq!(
        status(8)["parents"][0]["conditions"][0]["observedGeneration"],
        8
    );
    assert_eq!(
        fake.seen()[9].path,
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/backendtlspolicies/tls/status"
    );
    assert_eq!(bodies[9]["kind"], "BackendTLSPolicy");
    assert_eq!(
        bodies[9]["apiVersion"], "gateway.networking.k8s.io/v1",
        "written as of the version followed"
    );
    assert_eq!(
        status(9)["ancestors"],
        serde_json::json!([{
            "ancestorRef": {
                "group": "gateway.networking.k8s.io",
                "kind": "Gateway",
                "name": "web",
                "namespace": "edge",
            },
            "conditions": status(9)["ancestors"][0]["conditions"].clone(),
            "controllerName": GATEWAY_CONTROLLER,
        }])
    );
    let conditions: Vec<(&str, &str, &str, i64)> = status(9)["ancestors"][0]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["type"].as_str().unwrap(),
                c["status"].as_str().unwrap(),
                c["reason"].as_str().unwrap(),
                c["observedGeneration"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        conditions,
        [
            ("Accepted", "False", "NoValidCACertificate", 7),
            ("ResolvedRefs", "False", "InvalidKind", 7),
        ]
    );
    assert_eq!(
        status(10)["ancestors"],
        serde_json::to_value(&old_tls.status.as_ref().unwrap().ancestors[1..]).unwrap()
    );
    // Read back as written, every status says what it should: nothing more is written.
    let (mut class, mut gateway, mut app, mut old) = (class, gateway, app, old);
    let (mut calls, mut gone, mut tunnel, mut db, mut dns) = (calls, gone, tunnel, db, dns);
    let (mut tls, mut old_tls) = (tls, old_tls);
    class.status = serde_json::from_value(bodies[1]["status"].clone()).unwrap();
    gateway.status = serde_json::from_value(bodies[0]["status"].clone()).unwrap();
    app.status = serde_json::from_value(bodies[2]["status"].clone()).unwrap();
    old.status = serde_json::from_value(bodies[3]["status"].clone()).unwrap();
    calls.status = serde_json::from_value(bodies[4]["status"].clone()).unwrap();
    gone.status = serde_json::from_value(bodies[5]["status"].clone()).unwrap();
    tunnel.status = serde_json::from_value(bodies[6]["status"].clone()).unwrap();
    db.status = serde_json::from_value(bodies[7]["status"].clone()).unwrap();
    dns.status = serde_json::from_value(bodies[8]["status"].clone()).unwrap();
    tls.status = serde_json::from_value(bodies[9]["status"].clone()).unwrap();
    old_tls.status = serde_json::from_value(bodies[10]["status"].clone()).unwrap();
    let kinds = following(
        vec![class],
        vec![gateway],
        vec![app, old],
        vec![calls, gone],
        vec![tunnel],
        vec![db],
        vec![dns],
        vec![tls, old_tls, quiet],
    );
    kinds
        .write_own(&api, &handed, &addresses, &programmed, &said)
        .await;
    kinds.write_shared(&api, &[&handed], None, &said).await;
    assert_eq!(fake.seen().len(), 11, "a status written again");
}

#[test]
fn the_leader_merges_every_replicas_report_and_counts_the_ready_pods() {
    use acks::{Pod, Pods, Reporter};
    let pod = |last: u8| IpAddr::from([10, 0, 0, last]);
    let runs = |generation: i64| Pod {
        runs: Some(generation),
        ..Pod::default()
    };
    let report = |pod_name: &str, lease: &str, pods: Pods| {
        Reporter {
            namespace: "edge".to_owned(),
            lease: lease.to_owned(),
            pod: pod_name.to_owned(),
            uid: format!("{pod_name}-uid"),
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
        rate_limit_policies: Vec::new(),
        own_services: Vec::new(),
        gateway_api: None,
        own_class: false,
        watched: None,
        shape: Shape::default(),
        selected: None,
        parts: Vec::new(),
        scoped: false,
        joining: BTreeSet::new(),
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

/// Namespace `name`, labelled as a selector of `team=payments` selects it.
fn team_namespace(name: &str, version: &str) -> Namespace {
    serde_saphyr::from_str(&format!(
        "{{ metadata: {{ name: {name}, resourceVersion: '{version}', labels: {{ team: payments }} }} }}"
    ))
    .unwrap()
}

/// One watch event a line, of `kind` (`ADDED`, `MODIFIED`, `DELETED`) and `object`.
fn event<K: Serialize>(kind: &str, object: &K) -> Bytes {
    let event = serde_json::json!({ "type": kind, "object": object });
    Bytes::from(format!("{event}\n"))
}

/// A watch ended soon, with nothing, to be begun again: what a scripted change is sent on.
fn short() -> Answer {
    Answer {
        pause: Duration::from_millis(50),
        ..open()
    }
}

fn forbidden() -> Answer {
    answer(403, r#"{"kind":"Status","code":403,"message":"forbidden"}"#)
}

/// The routes of the last translation, if there is one.
fn last_routes(heard: &Rc<RefCell<Heard>>) -> Option<Vec<String>> {
    heard.borrow().translations.last().map(routes)
}

/// Whether the control plane said `line`.
fn said_line(heard: &Rc<RefCell<Heard>>, line: &str) -> bool {
    heard.borrow().lines.iter().any(|said| said == line)
}

/// Ingress `web` of shop, its rule served on `www.shop.example` too, as a watch sends it
/// changed.
fn shop_changed() -> Ingress {
    let mut changed = ingress("shop", "web", "shop.example");
    changed.metadata.resource_version = Some("9".to_owned());
    let rules = changed
        .spec
        .as_mut()
        .and_then(|spec| spec.rules.as_mut())
        .unwrap();
    let mut www = rules[0].clone();
    www.host = Some("www.shop.example".to_owned());
    rules.push(www);
    changed
}

/// The routes of the last translation, in order of their names.
fn sorted_routes(heard: &Rc<RefCell<Heard>>) -> Vec<String> {
    let mut routes = last_routes(heard).unwrap_or_default();
    routes.sort();
    routes
}

/// The namespaces of a selector scope are followed as they come to match and stop: shop
/// matches from the start; blog, labelled in later while its Secrets cannot yet be read, is
/// held out whole, its Ingress with it, while a change in shop is translated without it,
/// and joins once its Secrets are listed; shop, labelled out, is dropped with its objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selectors_namespaces_are_followed_and_each_that_comes_to_match_taken_in_whole() {
    #[derive(Default)]
    struct Script {
        labelled_in: AtomicBool,
        shop_changed: AtomicBool,
        bound: AtomicBool,
        labelled_out: AtomicBool,
    }
    let script = Arc::new(Script::default());
    let told = Arc::clone(&script);
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        let watching = path.contains("watch=true");
        let collection = path.split('?').next().unwrap_or_default();
        let namespace = ["shop", "blog"]
            .into_iter()
            .find(|namespace| path.contains(&format!("/namespaces/{namespace}/")));
        let sent = |chunks| Answer { chunks, ..short() };
        match (collection, namespace, watching) {
            ("/api/v1/namespaces", None, false) => {
                assert!(path.contains("labelSelector=team%3Dpayments"), "{path}");
                page(&[team_namespace("shop", "5")])
            }
            ("/api/v1/namespaces", None, true) => {
                if told.labelled_out.swap(false, Ordering::SeqCst) {
                    sent(vec![event("DELETED", &team_namespace("shop", "8"))])
                } else if told.labelled_in.swap(false, Ordering::SeqCst) {
                    sent(vec![event("ADDED", &team_namespace("blog", "7"))])
                } else {
                    short()
                }
            }
            (_, Some("shop"), true)
                if collection.ends_with("/ingresses")
                    && told.shop_changed.swap(false, Ordering::SeqCst) =>
            {
                sent(vec![event("MODIFIED", &shop_changed())])
            }
            (_, _, true) => short(),
            ("/apis/networking.k8s.io/v1/ingressclasses", None, false) => page(&[class()]),
            (_, Some(namespace), false) if collection.ends_with("/ingresses") => {
                page(&[ingress(namespace, "web", &format!("{namespace}.example"))])
            }
            (_, Some(namespace), false) if collection.ends_with("/services") => {
                page(&[service(namespace)])
            }
            (_, Some("blog"), false)
                if collection.ends_with("/secrets") && !told.bound.load(Ordering::SeqCst) =>
            {
                forbidden()
            }
            (_, Some(namespace), false) if collection.ends_with("/secrets") => {
                page(&[secret(namespace, &[&format!("{namespace}.example")])])
            }
            _ => otherwise(path),
        }
    })
    .await;
    let api = fake.client(&token("controller-selector", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Selector("team=payments".to_owned()),
                        fixed(&plane),
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("shop translated", || last_routes(&heard).is_some()).await;
            assert_eq!(
                last_routes(&heard).unwrap(),
                ["ingress/shop/web/shop.example"]
            );
            assert_eq!(
                heard.borrow().lines[0],
                "waiting on namespaces labelled team=payments, ingressclasses in the cluster"
            );

            script.labelled_in.store(true, Ordering::SeqCst);
            until("blog's Secrets refused", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line.starts_with("secrets in blog: "))
            })
            .await;
            assert!(
                said_line(&heard, "namespace blog: Joining"),
                "{:?}",
                heard.borrow().lines
            );
            // The rest goes on without it, and nothing of it is taken while a kind of it is
            // not listed: not its Ingress, listed already.
            script.shop_changed.store(true, Ordering::SeqCst);
            until("shop's change translated", || {
                last_routes(&heard).is_some_and(|routes| routes.len() > 1)
            })
            .await;
            assert_eq!(
                sorted_routes(&heard),
                [
                    "ingress/shop/web/shop.example",
                    "ingress/shop/web/www.shop.example"
                ],
                "{:?}",
                heard.borrow().lines
            );

            script.bound.store(true, Ordering::SeqCst);
            until("blog taken in", || {
                last_routes(&heard).is_some_and(|routes| routes.len() == 3)
            })
            .await;
            assert_eq!(
                sorted_routes(&heard),
                [
                    "ingress/blog/web/blog.example",
                    "ingress/shop/web/shop.example",
                    "ingress/shop/web/www.shop.example"
                ]
            );
            assert_eq!(
                heard
                    .borrow()
                    .translations
                    .last()
                    .unwrap()
                    .config
                    .certificates
                    .len(),
                2,
                "taken in without its Secrets"
            );
            assert!(said_line(&heard, "namespace blog: Joined"));

            script.labelled_out.store(true, Ordering::SeqCst);
            until("shop dropped", || {
                last_routes(&heard).is_some_and(|routes| routes.len() == 1)
            })
            .await;
            assert_eq!(
                last_routes(&heard).unwrap(),
                ["ingress/blog/web/blog.example"]
            );
            assert!(said_line(&heard, "namespace shop: Left"));
            assert!(
                !heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line.starts_with("waiting on") && line.contains("blog")),
                "a namespace joining was waited on: {:?}",
                heard.borrow().lines
            );
            controlling.abort();
        })
        .await;
}

/// The namespaces a selector matches at its first list are waited on as every kind is at
/// the start (04 §1): nothing is translated while one of them cannot be read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selectors_first_namespaces_are_waited_on_as_the_start_is() {
    let bound = Arc::new(AtomicBool::new(false));
    let allowed = Arc::clone(&bound);
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        let collection = path.split('?').next().unwrap_or_default();
        let namespace = ["shop", "blog"]
            .into_iter()
            .find(|namespace| path.contains(&format!("/namespaces/{namespace}/")));
        match (collection, namespace) {
            ("/api/v1/namespaces", None) => {
                page(&[team_namespace("blog", "5"), team_namespace("shop", "5")])
            }
            ("/apis/networking.k8s.io/v1/ingressclasses", None) => page(&[class()]),
            (_, Some(namespace)) if collection.ends_with("/ingresses") => {
                page(&[ingress(namespace, "web", &format!("{namespace}.example"))])
            }
            (_, Some(namespace)) if collection.ends_with("/services") => {
                page(&[service(namespace)])
            }
            (_, Some("blog"))
                if collection.ends_with("/secrets") && !allowed.load(Ordering::SeqCst) =>
            {
                forbidden()
            }
            (_, Some(namespace)) if collection.ends_with("/secrets") => {
                page(&[secret(namespace, &[&format!("{namespace}.example")])])
            }
            _ => otherwise(path),
        }
    })
    .await;
    let api = fake.client(
        &token("controller-selector-start", "secret"),
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
                    control(
                        api,
                        &WatchScope::Selector("team=payments".to_owned()),
                        fixed(&plane),
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("the wait on blog's Secrets said", || {
                said_line(&heard, "waiting on secrets in blog")
            })
            .await;
            assert!(
                heard.borrow().translations.is_empty(),
                "translated without blog: {:?}",
                heard.borrow().lines
            );
            assert!(
                !heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line.starts_with("namespace ")),
                "the first list said as joining: {:?}",
                heard.borrow().lines
            );
            bound.store(true, Ordering::SeqCst);
            until("translated", || last_routes(&heard).is_some()).await;
            assert_eq!(
                last_routes(&heard).unwrap(),
                [
                    "ingress/blog/web/blog.example",
                    "ingress/shop/web/shop.example"
                ]
            );
            controlling.abort();
        })
        .await;
}

/// The Gateway's namespace coming to match a selector: its Secrets, followed whatever the
/// scope for the Gateway's certificates, go on being translated, while what is followed
/// there for the scope alone is held out until the namespace joins; one that stops
/// matching takes those with it, the Secrets staying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gateways_namespace_joining_keeps_its_secrets_translated() {
    let fake = Fake::start(&["127.0.0.1"], |_| open()).await;
    let api = Rc::new(fake.client(&token("controller-joining", "secret"), Timing::default()));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let edge = Scope::Namespace("edge".to_owned());
            let mut secrets = follow_of("secrets in edge", true, vec![secret("edge", &[])]);
            secrets.part = edge.clone();
            let mut followed = Followed {
                ingress_classes: Vec::new(),
                ingresses: Vec::new(),
                services: Vec::new(),
                secrets: vec![secrets],
                shape: Shape {
                    gateways: vec![edge.clone()],
                    ..Shape::default()
                },
                rate_limit_policies: Vec::new(),
                own_services: Vec::new(),
                gateway_api: None,
                own_class: false,
                watched: Some(Vec::new()),
                selected: None,
                parts: Vec::new(),
                scoped: false,
                joining: BTreeSet::new(),
                reports: Vec::new(),
                ready: Vec::new(),
            };
            let mut tasks = JoinSet::new();
            let changed = Rc::new(Notify::new());
            let said: Rc<dyn Fn(Said<'_>)> = Rc::new(|_| {});
            let mut start = Start {
                tasks: &mut tasks,
                api: &api,
                changed: &changed,
                said: &said,
                joining: BTreeSet::new(),
            };
            // The first list: shop, waited on as the start is.
            let first = followed.rescope(&BTreeSet::from(["shop".to_owned()]), &mut start);
            assert_eq!(first, []);
            assert_eq!(
                followed.unlisted().0,
                [
                    "ingresses in shop",
                    "services in shop",
                    "secrets in shop",
                    "ratelimitpolicies in edge",
                    "ratelimitpolicies in shop",
                ]
            );
            // Then edge comes to match.
            let both = BTreeSet::from(["edge".to_owned(), "shop".to_owned()]);
            let became = followed.rescope(&both, &mut start);
            assert_eq!(became, [("edge".to_owned(), Membership::Joining)]);
            let joining = |followed: &Followed| -> Vec<(String, bool)> {
                followed
                    .ingresses
                    .iter()
                    .map(|follow| (follow.name.clone(), follow.joining))
                    .chain(
                        followed
                            .secrets
                            .iter()
                            .map(|follow| (follow.name.clone(), follow.joining)),
                    )
                    .collect()
            };
            assert_eq!(
                joining(&followed),
                [
                    ("ingresses in edge".to_owned(), true),
                    ("ingresses in shop".to_owned(), false),
                    ("secrets in edge".to_owned(), false),
                    ("secrets in shop".to_owned(), false),
                ]
            );
            assert_eq!(
                followed.cluster().secrets.len(),
                1,
                "the Gateway's Secrets held out"
            );
            assert_eq!(followed.cluster().watched, Some(vec!["shop".to_owned()]));
            assert_eq!(
                followed.unlisted().0,
                [
                    "ingresses in shop",
                    "services in shop",
                    "secrets in shop",
                    "ratelimitpolicies in edge",
                    "ratelimitpolicies in shop",
                ],
                "a namespace joining waited on"
            );
            assert_eq!(followed.join(), Vec::<String>::new(), "joined unlisted");
            // Edge stops matching: what it was followed for in the scope goes, its Secrets
            // stay for the Gateway.
            let became = followed.rescope(&BTreeSet::from(["shop".to_owned()]), &mut start);
            assert_eq!(became, [("edge".to_owned(), Membership::Left)]);
            assert_eq!(
                joining(&followed),
                [
                    ("ingresses in shop".to_owned(), false),
                    ("secrets in edge".to_owned(), false),
                    ("secrets in shop".to_owned(), false),
                ]
            );
            assert!(followed.joining.is_empty());
        })
        .await;
}

/// A fake API server of Ingress `web` of class `edge` served, and the data plane's Service
/// listed as `own` has it, each apply of it answered `applied`.
async fn serving_web(own: Vec<Service>, applied: u16) -> Fake {
    Fake::start(&["127.0.0.1"], move |path| {
        if path.ends_with("/status") {
            return answer(200, "{}");
        }
        if path.contains("watch=true") {
            return open();
        }
        if path == SERVICE_APPLIED {
            return if applied == 200 {
                answer(
                    200,
                    r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"edgerush-proxy-web"}}"#,
                )
            } else {
                answer(
                    applied,
                    r#"{"kind":"Status","code":422,"reason":"Invalid","message":"spec.loadBalancerClass: may not change once set"}"#,
                )
            };
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress_saying("web", "edge", &["192.0.2.1"], &[])])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            page(&own)
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            otherwise(path)
        }
    })
    .await
}

/// Each said line that starts so.
fn said_of(heard: &Rc<RefCell<Heard>>, start: &str) -> Vec<String> {
    heard
        .borrow()
        .lines
        .iter()
        .filter(|line| line.starts_with(start))
        .cloned()
        .collect()
}

/// The data plane's Service is written as the config binds, Ingress's two ports beside
/// those; not again while it is as wanted, however often translated; and again as its
/// DataPlane changes its shape, what it no longer states taken away by the apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_data_planes_service_is_written_as_wanted_and_not_again_until_that_changes() {
    // Ingress's ports, stated apart from the 80 and 443 the Ingress is bound on, so that
    // both sources are seen.
    let shaped =
        "  ingress: { httpPort: 8080, httpsPort: 8443 }\n  service:\n    annotations: { a: b }\n";
    let first = operated(&plane(), shaped);
    let live = as_wanted(&first, &[80, 443, 8080, 8443], &[]);
    let fake = serving_web(vec![live], 200).await;
    let api = fake.client(&token("controller-service", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(only(first.clone()));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("written once", || {
                !said_of(&heard, "Service edge/edgerush-proxy-web").is_empty()
            })
            .await;
            assert_eq!(
                said_of(&heard, "Service "),
                ["Service edge/edgerush-proxy-web: 80/TCP, 443/TCP, 8080/TCP, 8443/TCP"]
            );
            // Translated again, the Service as wanted: not written.
            let mut redirecting = first.clone();
            redirecting.plane.ingress_https_redirect = true;
            settings.send_replace(only(redirecting));
            until("translated again", || {
                heard.borrow().translations.len() == 2
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(applied(&fake).len(), 1, "{:?}", heard.borrow().lines);
            // Its annotation taken away, which the Service there still carries: written.
            let plain = operated(&plane(), "  ingress: { httpPort: 8080, httpsPort: 8443 }\n");
            settings.send_replace(only(plain));
            until("written again", || applied(&fake).len() == 2).await;
            controlling.abort();
        })
        .await;
    let applies = applied(&fake);
    assert_eq!(applies[0]["metadata"]["annotations"]["a"], "b");
    assert!(
        applies[1]["metadata"].get("annotations").is_none(),
        "{}",
        applies[1]
    );
    assert_eq!(
        applies[1]["metadata"]["ownerReferences"][0]["uid"],
        "web-uid"
    );
    assert_eq!(applies[1]["spec"]["externalTrafficPolicy"], "Local");
}

/// A Service of the data plane's Service's name that its DataPlane does not own is left
/// alone, said once, and no status says its addresses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_of_its_name_not_its_data_planes_is_left_alone() {
    let settings = operated(&plane(), "");
    let mut theirs = as_wanted(&settings, &[80, 443], &["203.0.113.9"]);
    theirs.metadata.owner_references = None;
    let fake = serving_web(vec![theirs], 200).await;
    let api = fake.client(&token("controller-not-ours", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(only(settings)).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("the status said with no address", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "status of Ingress shop/web: []")
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            controlling.abort();
        })
        .await;
    assert!(applied(&fake).is_empty(), "another's Service written");
    assert_eq!(
        said_of(&heard, "Service "),
        ["Service edge/edgerush-proxy-web not ours"]
    );
}

/// A Service the API server refuses is said, and no status claims an address for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_refused_is_said() {
    let settings = operated(&plane(), "");
    let fake = serving_web(Vec::new(), 422).await;
    let api = fake.client(&token("controller-refused", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(only(settings)).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("the refusal said", || {
                said_of(&heard, "Service edge/edgerush-proxy-web refused: ")
                    .iter()
                    .any(|line| line.contains("may not change once set"))
            })
            .await;
            until("the status said with no address", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "status of Ingress shop/web: []")
            })
            .await;
            controlling.abort();
        })
        .await;
}

/// DataPlane `name` of `edge` served under the operator: `plane`, its Service
/// `edge/edgerush-proxy-<name>` as the DataPlane shapes it.
fn operated_as(name: &str, plane: &Plane) -> Settings {
    let data_plane: edgerush_planes::data_plane::DataPlane = serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: edge, uid: {name}-uid }}\nspec:\n  controlPlaneRef: {{ name: edge }}\n"
    ))
    .unwrap();
    Settings {
        plane: plane.clone(),
        service: Some(ProxyService::of(&data_plane).unwrap()),
        generated: false,
        data_plane: None,
    }
}

/// The path DataPlane `name`'s Service is applied at.
fn service_applied(name: &str) -> String {
    format!(
        "/api/v1/namespaces/edge/services/edgerush-proxy-{name}?fieldManager=edgerush-controller&force=true"
    )
}

/// Two DataPlanes of one control plane, each of a class of its own: each is translated on
/// its own from the one set of follows, its Service written, and each Ingress's status says
/// the addresses of the Service of the DataPlane whose class it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_data_plane_is_translated_and_its_statuses_written_on_its_own() {
    let api_plane = || {
        let mut plane = plane();
        plane.ingress_class = Some("other".to_owned());
        plane
    };
    let web = operated_as("web", &plane());
    let api = operated_as("api", &api_plane());
    let (listed_web, listed_api) = (web.clone(), api.clone());
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.ends_with("/status") {
            return answer(200, "{}");
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            let other: IngressClass = serde_saphyr::from_str(
                "{ metadata: { name: other }, spec: { controller: edgerush.dev/ingress-controller } }",
            )
            .unwrap();
            page(&[class(), other])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[
                ingress_saying("web", "edge", &[], &[]),
                ingress_saying("theirs", "other", &[], &[]),
            ])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            page(&[
                as_wanted(&listed_web, &[80, 443], &["203.0.113.7"]),
                as_wanted(&listed_api, &[80, 443], &["203.0.113.9"]),
            ])
        } else if path == service_applied("web") || path == service_applied("api") {
            answer(200, r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"x"}}"#)
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            otherwise(path)
        }
    })
    .await;
    let api_server = fake.client(&token("controller-several", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let planes = Planes::from([
                ("DataPlane edge/api".to_owned(), api),
                ("DataPlane edge/web".to_owned(), web),
            ]);
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api_server,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(planes).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let said = |line: &str| heard.borrow().lines.iter().any(|said| said == line);
            until("each Ingress's status written", || {
                said("status of Ingress shop/web: [203.0.113.7]")
                    && said("status of Ingress shop/theirs: [203.0.113.9]")
            })
            .await;
            controlling.abort();
        })
        .await;
    let heard = heard.borrow();
    let of: Vec<(&str, Vec<String>)> = heard
        .of
        .iter()
        .zip(&heard.translations)
        .map(|(of, translation)| (of.as_str(), routes(translation)))
        .collect();
    assert_eq!(
        of,
        [
            (
                "DataPlane edge/api",
                vec!["ingress/shop/theirs/shop.example".to_owned()]
            ),
            (
                "DataPlane edge/web",
                vec!["ingress/shop/web/shop.example".to_owned()]
            ),
        ]
    );
    let applied: BTreeSet<String> = fake
        .seen()
        .into_iter()
        .filter(|seen| seen.method == "PATCH" && seen.path.contains("/services/"))
        .map(|seen| seen.path)
        .collect();
    assert_eq!(
        applied,
        BTreeSet::from([service_applied("api"), service_applied("web")])
    );
    // Nothing of one written for the other: neither Ingress says both addresses.
    assert!(!said_both(&heard.lines), "{:?}", heard.lines);
}

/// Whether a status was said with two addresses.
fn said_both(lines: &[String]) -> bool {
    lines
        .iter()
        .any(|line| line.starts_with("status of Ingress") && line.contains(", "))
}

/// A fake API server of IngressClass `edge.web` and two Ingresses holding an address: `web`,
/// of that class, and `theirs`, of another ControlPlane's; DataPlane `web`'s Service giving
/// that address; and every status written.
async fn holding_an_address(web: &Settings) -> Fake {
    let listed = web.clone();
    Fake::start(&["127.0.0.1"], move |path| {
        if path.ends_with("/status") {
            return answer(200, "{}");
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            let class: IngressClass = serde_saphyr::from_str(
                "{ metadata: { name: edge.web }, spec: { controller: edgerush.dev/ingress-controller } }",
            )
            .unwrap();
            page(&[class])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[
                ingress_saying("web", "edge.web", &["203.0.113.7"], &[]),
                ingress_saying("theirs", "other.web", &["203.0.113.9"], &[]),
            ])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/namespaces/edge/services?") {
            page(&[as_wanted(&listed, &[80, 443], &["203.0.113.7"])])
        } else if path == service_applied("web") {
            answer(200, r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"x"}}"#)
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else {
            otherwise(path)
        }
    })
    .await
}

/// The paths of the Ingress statuses written.
fn ingress_statuses_written(fake: &Fake) -> Vec<String> {
    fake.seen()
        .into_iter()
        .filter(|seen| seen.method == "PATCH" && seen.path.contains("/ingresses/"))
        .map(|seen| seen.path)
        .collect()
}

/// ControlPlane `edge`'s last DataPlane let go of: the control plane follows on, serving
/// none, and its Ingress loses the address it held, once the class is no longer kept; the
/// other ControlPlane's is left as it is (04 §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_data_planes_ingresses_lose_their_address_once_it_is_let_go_of() {
    let mut plane = plane();
    plane.ingress_class = Some("edge.web".to_owned());
    let web = operated_as("web", &plane);
    let fake = holding_an_address(&web).await;
    let api = fake.client(&token("controller-let-go", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) =
                watch::channel(Planes::from([("DataPlane edge/web".to_owned(), web)]));
            let (keeping, kept) = watch::channel(Some(BTreeSet::from(["edge.web".to_owned()])));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        Some(Classes {
                            control_plane: "edge".to_owned(),
                            namespace: "edge".to_owned(),
                            kept,
                        }),
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("served", || !heard.borrow().translations.is_empty()).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                ingress_statuses_written(&fake),
                Vec::<String>::new(),
                "a status written while its DataPlane serves it as it says"
            );
            // Let go of: no longer kept, as the DataPlanes' follow hands that on first, and
            // then no longer served. Served alone, it is kept.
            keeping.send_replace(Some(BTreeSet::new()));
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                ingress_statuses_written(&fake),
                Vec::<String>::new(),
                "a status taken away while its data plane serves it"
            );
            settings.send_replace(Planes::new());
            until("its address taken away", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "status of Ingress shop/web: []")
            })
            .await;
            controlling.abort();
        })
        .await;
    assert_eq!(
        ingress_statuses_written(&fake),
        ["/apis/networking.k8s.io/v1/namespaces/shop/ingresses/web/status"]
    );
    // Followed on as the last was let go of, not begun again.
    let lists = fake
        .seen()
        .iter()
        .filter(|seen| {
            seen.path
                .starts_with("/apis/networking.k8s.io/v1/ingresses?")
                && !seen.path.contains("watch=true")
        })
        .count();
    assert_eq!(lists, 1, "the Ingresses listed again");
}

/// A control plane of a ControlPlane that serves no DataPlane, as after a restart with its
/// last gone: it follows its scope all the same, writes nothing until the DataPlanes are
/// listed, nor while it does not lead, and then takes the address from each Ingress of a
/// class none keeps (04 §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_plane_serving_no_data_plane_lets_go_of_its_classes_once_they_are_listed() {
    let mut plane = plane();
    plane.ingress_class = Some("edge.web".to_owned());
    let fake = holding_an_address(&operated_as("web", &plane)).await;
    let api = fake.client(&token("controller-none", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (leads, leading) = watch::channel(true);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (keeping, kept) = watch::channel(None);
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(Planes::new()).1,
                        Some(Classes {
                            control_plane: "edge".to_owned(),
                            namespace: "edge".to_owned(),
                            kept,
                        }),
                        Debounce::default(),
                        Some(leading),
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let listed = || {
                fake.seen().iter().any(|seen| {
                    seen.path
                        .starts_with("/apis/networking.k8s.io/v1/ingresses?")
                        && !seen.path.contains("watch=true")
                })
            };
            until("its Ingresses followed", listed).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                ingress_statuses_written(&fake),
                Vec::<String>::new(),
                "a status written before the DataPlanes were listed"
            );
            // Listed, while another replica leads.
            leads.send_replace(false);
            keeping.send_replace(Some(BTreeSet::new()));
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                ingress_statuses_written(&fake),
                Vec::<String>::new(),
                "a status written by a replica that does not lead"
            );
            leads.send_replace(true);
            until("its address taken away", || {
                heard
                    .borrow()
                    .lines
                    .iter()
                    .any(|line| line == "status of Ingress shop/web: []")
            })
            .await;
            controlling.abort();
        })
        .await;
    assert_eq!(
        ingress_statuses_written(&fake),
        ["/apis/networking.k8s.io/v1/namespaces/shop/ingresses/web/status"]
    );
}

/// Of several data planes, one whose config the data plane would refuse is refused alone,
/// the others handed theirs; one added, or let go of, leaves the others as they were: not
/// translated again, and nothing listed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_added_refused_or_let_go_of_leaves_the_others_as_they_were() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page::<Secret>(&[])
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-several-come-and-go", "secret"),
        Timing::default(),
    );
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(Planes::new());
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            let lists = || {
                fake.seen()
                    .iter()
                    .filter(|seen| {
                        seen.path.starts_with("/apis/networking.k8s.io/v1/ingresses?")
                            && !seen.path.contains("watch=true")
                    })
                    .count()
            };
            // A data plane whose own settings the data plane would refuse, beside another.
            let refusing = plane_with(
                "ports:\n  80: { forwarding: { trusted_proxies: [\"10.1.2.3/8\"], trusted_only_headers: [] } }",
            );
            let mut planes = Planes::from([
                ("a".to_owned(), harness(&plane())),
                ("b".to_owned(), harness(&refusing)),
            ]);
            settings.send_replace(planes.clone());
            until("translated and refused", || {
                let heard = heard.borrow();
                heard.of == ["a"] && heard.refused == ["b"]
            })
            .await;
            assert_eq!(lists(), 1);
            // Another added: translated alone.
            planes.insert("c".to_owned(), harness(&plane()));
            settings.send_replace(planes.clone());
            until("the one added translated", || heard.borrow().of.len() == 2).await;
            // Let go of: nothing more of the others.
            planes.remove("b");
            settings.send_replace(planes.clone());
            tokio::time::sleep(Duration::from_millis(300)).await;
            planes.remove("a");
            settings.send_replace(planes);
            tokio::time::sleep(Duration::from_millis(300)).await;
            let heard = heard.borrow();
            assert_eq!(heard.of, ["a", "c"], "{:?}", heard.lines);
            assert_eq!(heard.refused, ["b"], "{:?}", heard.lines);
            assert_eq!(lists(), 1, "listed again as data planes came and went");
            drop(heard);
            controlling.abort();
        })
        .await;
}

/// A data plane of Ingress alone is translated once what it needs is listed, though a
/// Gateway beside it waits on the Gateway API's kinds, here never listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_of_ingress_alone_does_not_wait_on_the_gateway_apis_kinds() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page::<Secret>(&[])
        } else {
            // The CRDs among it: never listed.
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-ingress-alone", "secret"),
        Timing::default(),
    );
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let planes = Planes::from([
                ("gateway".to_owned(), harness(&gateway_plane())),
                ("ingress".to_owned(), harness(&plane())),
            ]);
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        watch::channel(planes).1,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("the Ingress alone translated", || {
                heard.borrow().of == ["ingress"]
            })
            .await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                heard.borrow().of,
                ["ingress"],
                "the Gateway translated unlisted"
            );
            controlling.abort();
        })
        .await;
}

/// A ControlPlane's control plane translates its Gateway only of the class the operator
/// made for it, as its labels say; one of the name made by hand is not taken, and no status
/// of it written, as the operator refuses it (05 §3). The development harness takes either.
#[test]
fn a_control_planes_gateway_is_translated_only_of_the_class_made_for_it() {
    let class = |labels: &str| -> GatewayClass {
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: edgerush, generation: 1, labels: {{ {labels} }} }}
spec: {{ controllerName: edgerush.dev/gateway-controller }}"
        ))
        .unwrap()
    };
    let made = edgerush_planes::objects::control_plane_labels("edge", "edge")
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    let gateway: Gateway = serde_saphyr::from_str(
        "metadata: { name: web, namespace: edge, generation: 1 }
spec:
  gatewayClassName: edgerush
  listeners: [{ name: http, port: 8080, protocol: HTTP }]",
    )
    .unwrap();
    let classes = Classes {
        control_plane: "edge".to_owned(),
        namespace: "edge".to_owned(),
        kept: watch::channel(None).1,
    };
    let translated = |class: GatewayClass, classes: Option<&Classes>| {
        let cluster = of_control_plane(
            Cluster {
                gateway_classes: vec![class],
                gateways: vec![gateway.clone()],
                ..Cluster::default()
            },
            classes,
        );
        Handed::of(&translate(&gateway_plane(), &cluster)).is_some()
    };
    assert!(translated(class(&made), Some(&classes)));
    assert!(!translated(class(""), Some(&classes)));
    let elsewhere = Classes {
        namespace: "teams".to_owned(),
        ..classes.clone()
    };
    assert!(!translated(class(&made), Some(&elsewhere)));
    assert!(translated(class(""), None));
}

/// The statuses data planes share are written once for them all: a route attached to the
/// Gateways of two data planes, both its parents in one write, and the class they share
/// once. Of an entry of this controller's, that of a Gateway gone from a namespace Gateways
/// are followed in is taken away; that of a Gateway of a namespace not followed, which
/// another control plane may serve, is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_statuses_data_planes_share_are_written_once_for_them_all() {
    let class: GatewayClass = serde_saphyr::from_str(
        "metadata: { name: edgerush, generation: 1, resourceVersion: '5' }
spec: { controllerName: edgerush.dev/gateway-controller }",
    )
    .unwrap();
    let gateway = |name: &str| -> Gateway {
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: {name}, namespace: edge, generation: 2, resourceVersion: '6' }}
spec:
  gatewayClassName: edgerush
  listeners:
    - {{ name: http, port: 8080, protocol: HTTP, allowedRoutes: {{ namespaces: {{ from: All }} }} }}"
        ))
        .unwrap()
    };
    let both: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: both, namespace: shop, generation: 3, resourceVersion: '7' }
spec:
  parentRefs: [{ name: web, namespace: edge }, { name: api, namespace: edge }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]",
    )
    .unwrap();
    let left: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: left, namespace: shop, generation: 1, resourceVersion: '8' }
spec: { parentRefs: [{ name: theirs, namespace: teams }] }
status:
  parents:
    - controllerName: edgerush.dev/gateway-controller
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: gone, namespace: edge }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]
    - controllerName: edgerush.dev/gateway-controller
      parentRef: { group: gateway.networking.k8s.io, kind: Gateway, name: theirs, namespace: teams }
      conditions: [{ type: Accepted, status: 'True', reason: Accepted, message: '', lastTransitionTime: '2026-10-01T00:00:00Z', observedGeneration: 1 }]",
    )
    .unwrap();
    let known = [
        "/apis/gateway.networking.k8s.io/v1/gatewayclasses/edgerush/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/both/status",
        "/apis/gateway.networking.k8s.io/v1/namespaces/shop/httproutes/left/status",
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
        &token("controller-shared-status", "secret"),
        Timing::default(),
    );
    let cluster = Cluster {
        services: vec![service("shop")],
        gateway_classes: vec![class.clone()],
        gateways: vec![gateway("web"), gateway("api")],
        http_routes: vec![both.clone(), left.clone()],
        ..Cluster::default()
    };
    let mut api_plane = gateway_plane();
    api_plane.gateway =
        Some(edgerush_translate::GatewayName::try_from("edge/api".to_owned()).unwrap());
    let web_translation = translate(&gateway_plane(), &cluster);
    let api_translation = translate(&api_plane, &cluster);
    let web = Handed::of(&web_translation).expect("web translated");
    let api_handed = Handed::of(&api_translation).expect("api translated");
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
    let kinds = GatewayApi {
        crds: Vec::new(),
        namespaces: Vec::new(),
        gateway_namespaces: vec![Scope::Namespace("edge".to_owned())],
        last: Some(served),
        gateway_classes: Some(vec![follow_of("gatewayclasses", true, vec![class])]),
        gateways: Some(vec![follow_of(
            "gateways in edge",
            true,
            vec![gateway("web"), gateway("api")],
        )]),
        listener_sets: None,
        http_routes: Some(vec![follow_of("httproutes", true, vec![both, left])]),
        grpc_routes: None,
        tls_routes: None,
        tcp_routes: None,
        udp_routes: None,
        reference_grants: None,
        backend_tls_policies: None,
        config_maps: None,
    };
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let said = hear(&heard);
    kinds
        .write_shared(&api, &[&web, &api_handed], None, &said)
        .await;
    assert_eq!(
        heard.borrow().lines,
        [
            "status of GatewayClass edgerush",
            "status of HTTPRoute shop/both",
            "status of HTTPRoute shop/left",
        ]
    );
    let bodies: Vec<serde_json::Value> = fake
        .seen()
        .iter()
        .map(|seen| serde_json::from_str(&seen.body).unwrap())
        .collect();
    let parents = |n: usize| -> Vec<(String, String)> {
        bodies[n]["status"]["parents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|parent| {
                (
                    parent["parentRef"]["namespace"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    parent["parentRef"]["name"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    };
    let named = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(namespace, name)| ((*namespace).to_owned(), (*name).to_owned()))
            .collect()
    };
    assert_eq!(parents(1), named(&[("edge", "web"), ("edge", "api")]));
    assert_eq!(parents(2), named(&[("teams", "theirs")]));
}

/// A ControlPlane's control plane serving no Gateway, as before the first is written of
/// its class, accepts the class the operator made for it all the same, as Gateway API
/// tooling waits for that before it writes one (04 §6): that class alone, not one of its
/// controller made by hand nor one of another controller carrying its labels. The
/// development harness, which has no class of its own, writes none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_planes_own_class_is_accepted_before_any_gateway_of_it_is_served() {
    let own = edgerush_planes::objects::control_plane_labels("edge", "edge");
    let made = own
        .iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    let class = |name: &str, labels: &str, controller: &str| -> GatewayClass {
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: {name}, generation: 2, resourceVersion: '5', labels: {{ {labels} }} }}
spec: {{ controllerName: {controller} }}"
        ))
        .unwrap()
    };
    let classes = vec![
        class("edge", &made, "edgerush.dev/gateway-controller"),
        class("by-hand", "", "edgerush.dev/gateway-controller"),
        class("theirs", &made, "example.com/gateway-controller"),
    ];
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if method == "PUT"
            && path == "/apis/gateway.networking.k8s.io/v1/gatewayclasses/edge/status"
        {
            answer(200, "{}")
        } else {
            answer(404, r#"{"kind":"Status","code":404,"message":"not found"}"#)
        }
    })
    .await;
    let api = fake.client(&token("controller-own-class", "secret"), Timing::default());
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
    let kinds = GatewayApi {
        crds: Vec::new(),
        namespaces: Vec::new(),
        gateway_namespaces: vec![Scope::Cluster],
        last: Some(served),
        gateway_classes: Some(vec![follow_of("gatewayclasses", true, classes)]),
        gateways: Some(vec![follow_of("gateways", true, Vec::new())]),
        listener_sets: None,
        http_routes: None,
        grpc_routes: None,
        tls_routes: None,
        tcp_routes: None,
        udp_routes: None,
        reference_grants: None,
        backend_tls_policies: None,
        config_maps: None,
    };
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let said = hear(&heard);
    kinds.write_shared(&api, &[], None, &said).await;
    assert!(heard.borrow().lines.is_empty(), "the harness wrote a class");
    kinds.write_shared(&api, &[], Some(&own), &said).await;
    assert_eq!(heard.borrow().lines, ["status of GatewayClass edge"]);
    let written: serde_json::Value = serde_json::from_str(&fake.seen()[0].body).unwrap();
    let conditions: Vec<(&str, &str, i64)> = written["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|condition| {
            (
                condition["type"].as_str().unwrap(),
                condition["status"].as_str().unwrap(),
                condition["observedGeneration"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        conditions,
        [("Accepted", "True", 2), ("SupportedVersion", "True", 2)]
    );
}

/// A ControlPlane's control plane serving no DataPlane follows the Gateway API's CRDs and
/// GatewayClasses alone, and accepts its own class (04 §2, §6); the first DataPlane that is
/// a Gateway starts the other kinds' follows, Namespaces among them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_plane_serving_no_gateway_follows_the_classes_alone() {
    let labels = edgerush_planes::objects::control_plane_labels("edge", "edge");
    let class = serde_json::json!({
        "metadata": { "name": "edgerush", "generation": 1, "resourceVersion": "5", "labels": labels },
        "spec": { "controllerName": "edgerush.dev/gateway-controller" },
    });
    let classes = format!(r#"{{"metadata":{{"resourceVersion":"5"}},"items":[{class}]}}"#);
    let gateway_api = "/apis/gateway.networking.k8s.io/v1/";
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if path.contains("watch=true") {
            return open();
        }
        let named = GatewayKind::ALL.into_iter().find(|kind| {
            path.starts_with("/apis/apiextensions.k8s.io/v1/customresourcedefinitions?")
                && path.contains(&format!("metadata.name%3D{}", kind.crd()))
        });
        if let Some(kind) = named {
            page(&[crd(kind, &["v1"])])
        } else if method == "PUT" && path == format!("{gateway_api}gatewayclasses/edgerush/status")
        {
            answer(200, "{}")
        } else if path.starts_with(&format!("{gateway_api}gatewayclasses?")) {
            answer(200, &classes)
        } else if path.starts_with(gateway_api) {
            answer(200, r#"{"metadata":{"resourceVersion":"5"},"items":[]}"#)
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page::<IngressClass>(&[])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page::<Ingress>(&[])
        } else if path.starts_with("/api/v1/services?") {
            page::<Service>(&[])
        } else if path.starts_with("/api/v1/secrets?") {
            page::<Secret>(&[])
        } else if path.starts_with("/api/v1/namespaces?") {
            page::<Namespace>(&[])
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(
        &token("controller-classes-alone", "secret"),
        Timing::default(),
    );
    let asked = |what: &str| {
        fake.seen()
            .iter()
            .filter(|seen| seen.path.starts_with(what) && !seen.path.contains("watch=true"))
            .count()
    };
    let httproutes = format!("{gateway_api}httproutes?");
    let class_status = format!("{gateway_api}gatewayclasses/edgerush/status");
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(Planes::new());
            let controlling = tokio::task::spawn_local({
                let said = hear(&Rc::default());
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        Some(Classes {
                            control_plane: "edge".to_owned(),
                            namespace: "edge".to_owned(),
                            kept: watch::channel(Some(BTreeSet::new())).1,
                        }),
                        Debounce::default(),
                        None,
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("its own class accepted", || asked(&class_status) > 0).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let others: Vec<String> = fake
                .seen()
                .iter()
                .filter(|seen| {
                    seen.path.starts_with(gateway_api)
                        && !seen
                            .path
                            .starts_with(&format!("{gateway_api}gatewayclasses"))
                })
                .map(|seen| seen.path.clone())
                .collect();
            assert_eq!(
                others,
                Vec::<String>::new(),
                "a kind but the classes followed"
            );
            assert_eq!(asked("/api/v1/namespaces?"), 0, "Namespaces followed");
            settings.send_replace(only(harness(&gateway_plane())));
            until("the routes followed", || asked(&httproutes) > 0).await;
            assert!(asked("/api/v1/namespaces?") > 0, "Namespaces not followed");
            controlling.abort();
        })
        .await;
}

/// A Gateway of a generated DataPlane that sets `infrastructure.parametersRef` is held as
/// it was: translated no further, its status the operator's (05 §3). One of a DataPlane
/// written by hand is the operator's own, never held; nor is one not followed.
#[test]
fn a_generated_data_planes_gateway_setting_parameters_is_held() {
    let gateway = |parameters: bool| -> Gateway {
        let infrastructure = if parameters {
            "\n  infrastructure: { parametersRef: { group: '', kind: ConfigMap, name: params } }"
        } else {
            ""
        };
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: web, namespace: shop, generation: 2 }}
spec:
  gatewayClassName: edge
  listeners: [{{ name: http, port: 80, protocol: HTTP }}]{infrastructure}"
        ))
        .unwrap()
    };
    let following = |gateways: Vec<Gateway>| GatewayApi {
        crds: Vec::new(),
        namespaces: Vec::new(),
        gateway_namespaces: vec![Scope::Namespace("shop".to_owned())],
        last: None,
        gateway_classes: None,
        gateways: Some(vec![follow_of("gateways in shop", true, gateways)]),
        listener_sets: None,
        http_routes: None,
        grpc_routes: None,
        tls_routes: None,
        tcp_routes: None,
        udp_routes: None,
        reference_grants: None,
        backend_tls_policies: None,
        config_maps: None,
    };
    let settings = |generated: bool| Settings {
        plane: Plane {
            gateway: Some(edgerush_translate::GatewayName {
                namespace: "shop".to_owned(),
                name: "web".to_owned(),
            }),
            ..plane()
        },
        service: None,
        generated,
        data_plane: None,
    };
    let with = following(vec![gateway(true)]);
    let without = following(vec![gateway(false)]);
    assert!(held(Some(&with), &settings(true)));
    assert!(!held(Some(&without), &settings(true)));
    assert!(!held(Some(&with), &settings(false)));
    assert!(!held(Some(&following(Vec::new())), &settings(true)));
    assert!(!held(None, &settings(true)));
}

/// The versions of the policies `policies` holds, as their generations say them.
fn generations(policies: &[AcceptedPolicy]) -> Vec<Option<i64>> {
    policies.iter().map(|policy| policy.generation).collect()
}

/// A data plane's record holds the version of each policy that applied in the config last
/// handed on, and that config, its certificates with it, only while it serves in another's
/// place: the translation since refused whole, or its Gateway held (04 §2).
#[test]
fn a_record_holds_the_config_handed_on_only_while_it_serves_in_anothers_place() {
    let plane = plane_with("rate_limiting: { mode: local }");
    let mut served = Serving::new(harness(&plane));
    let first = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    let Step::Translated(translation) = served.translator.step(&plane, &first) else {
        panic!("not translated");
    };
    let handed = translation.config.clone();
    assert!(!handed.certificates.is_empty());
    let record = served.record(false);
    assert_eq!(generations(&record.policies), [Some(1)]);
    assert_eq!(
        record.config, None,
        "the config translated is the one handed on"
    );
    assert_eq!(served.record(true).config.as_ref(), Some(&handed), "held");
    // Settings of 65 global limits, which no policy refused brings under the most a rule
    // takes: refused whole, the config handed on serves in its place.
    let names: Vec<String> = (0..65).map(|n| format!("global-{n}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let renamed = with_policy(rate_limit_policy("shop", 2, &[("renamed", "10/s")]));
    let Step::Refused { .. } = served.translator.step(&limiting(&names), &renamed) else {
        panic!("a rule under 65 limits not refused");
    };
    let record = served.record(false);
    assert_eq!(record.config.as_ref(), Some(&handed), "refused whole");
    assert_eq!(generations(&record.policies), [Some(1)]);
    let Step::Translated(_) = served.translator.step(&plane, &renamed) else {
        panic!("not translated");
    };
    let record = served.record(false);
    assert_eq!(record.config, None, "handed on again");
    assert_eq!(generations(&record.policies), [Some(2)]);
}

/// A data plane's record holds its DataPlane as last accepted, and, while that generation
/// is not taken on, the one served in its place; the development harness's, none.
#[test]
fn a_record_holds_the_data_plane_served_while_a_later_one_waits() {
    let object = |settings: &Settings| {
        settings
            .data_plane
            .as_ref()
            .map(|of| DataPlane::clone(&of.object))
    };
    let served_with = of_generation(&limiting(&[]), 1);
    let mut served = Serving::new(served_with.clone());
    let record = served.record(false);
    assert_eq!(record.accepted, object(&served_with));
    assert_eq!(record.served, None);
    // A generation that adds a global limit waits to be judged, the one before served.
    let wanted = of_generation(&limiting(&["per-client"]), 2);
    served.offer(&wanted);
    let record = served.record(false);
    assert_eq!(record.accepted, object(&wanted));
    assert_eq!(record.served, object(&served_with));
    let record = Serving::new(harness(&plane())).record(false);
    assert_eq!((record.accepted, record.served), (None, None));
}

/// A snapshot is taken only where its records differ from the last one's, counted on from
/// it: from one taken from another replica too.
#[test]
fn a_snapshot_is_counted_on_only_as_it_changes() {
    let (snapshots, mut taken) = watch::channel(None);
    let planes = |served: Option<DataPlane>| {
        let record = Record {
            served,
            ..Record::default()
        };
        BTreeMap::from([("DataPlane edge/web".to_owned(), record)])
    };
    let count = |taken: &mut watch::Receiver<Option<Arc<Snapshot>>>| {
        taken.borrow_and_update().as_ref().map(|taken| taken.count)
    };
    take(&snapshots, planes(None));
    assert_eq!(count(&mut taken), Some(1));
    take(&snapshots, planes(None));
    assert!(!taken.has_changed().unwrap(), "the same taken again");
    take(&snapshots, planes(Some(web_data_plane(""))));
    assert_eq!(count(&mut taken), Some(2));
    snapshots.send_replace(Some(Arc::new(Snapshot {
        count: 7,
        planes: planes(None),
    })));
    take(&snapshots, planes(Some(web_data_plane(""))));
    assert_eq!(count(&mut taken), Some(8));
}

/// With a snapshot to take, each round's records are taken into it as they change: the
/// version of each policy that applied, once the objects are translated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_records_are_taken_into_the_snapshot_each_round() {
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, PER_CLIENT_POLICY)
        } else {
            otherwise(path)
        }
    })
    .await;
    let api = fake.client(&token("controller-snapshot", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (snapshots, mut taken) = watch::channel(None);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(only(harness(&limiting(&[]))));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        None,
                        acks(),
                        Some(Snapshots {
                            snapshot: snapshots,
                            settings: Rc::new(|_| None),
                        }),
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || heard.borrow().translations.len() == 1).await;
            let policies = |taken: &mut watch::Receiver<Option<Arc<Snapshot>>>| {
                let taken = taken.borrow_and_update().clone().expect("none taken");
                (taken.count, generations(&taken.planes[""].policies))
            };
            let (count, applied) = policies(&mut taken);
            assert_eq!(applied, [Some(1)]);
            // Settings that change no record, translated again: nothing more taken.
            let mut edited = limiting(&[]);
            edited.ingress_https_redirect = true;
            settings.send_replace(only(harness(&edited)));
            until("translated again", || {
                heard.borrow().translations.len() == 2
            })
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(policies(&mut taken), (count, vec![Some(1)]));
            controlling.abort();
        })
        .await;
}

/// The leader's record of a data plane of `plane`, once it has translated `cluster`.
fn leaders_record(plane: &Plane, cluster: &Cluster) -> Record {
    let mut leader = Serving::new(harness(plane));
    let Step::Translated(_) = leader.translator.step(plane, cluster) else {
        panic!("not translated");
    };
    leader.record(false)
}

/// The limits under the first rule of `translation`.
fn under(translation: &Translation) -> Vec<String> {
    translation.config.routes[0].rules[0].rate_limits.clone()
}

/// A follower translates from the leader's record of the policies that applied rather than
/// its own: started afresh, it applies the version the leader applies of a policy whose
/// edit is refused, where of its own it would have none to apply (04 §2).
#[test]
fn a_follower_translates_from_the_leaders_policies() {
    let plane = plane_with("rate_limiting: { mode: local }");
    let accepted = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    let record = leaders_record(&plane, &accepted);
    let refused = with_policy(rate_limit_policy(
        "shop",
        3,
        &[("per-client", "10/fortnight")],
    ));
    let mut afresh = Serving::new(harness(&plane));
    let Step::Translated(translation) = afresh.translator.step(&plane, &refused) else {
        panic!("not translated");
    };
    assert_eq!(under(translation), Vec::<String>::new());
    let mut follower = Serving::new(harness(&plane));
    assert!(
        !follower.follow(&record, &|_| None),
        "the settings served changed"
    );
    let Step::Translated(translation) = follower.translator.step(&plane, &refused) else {
        panic!("not translated");
    };
    assert_eq!(under(translation), ["per-client"]);
    assert_eq!(generations(&translation.rate_limit_policies), [Some(1)]);
}

/// A follower serves the generation of a DataPlane the leader serves while it holds a later
/// one back, the later waiting, and takes that one on once the leader has: the leader
/// alone judges it.
#[test]
fn a_follower_serves_the_data_plane_the_leader_serves() {
    let served_with = of_generation(&limiting(&[]), 1);
    let wanted = of_generation(&limiting(&["per-client"]), 2);
    let made = {
        let served_with = served_with.clone();
        move |data_plane: &DataPlane| {
            (data_plane.metadata.generation == Some(1)).then(|| served_with.clone())
        }
    };
    let mut leader = Serving::new(served_with.clone());
    leader.offer(&wanted);
    let held_back = leader.record(false);
    // Started afresh, as the generation accepted is all it is given.
    let mut follower = Serving::new(wanted.clone());
    assert!(follower.follow(&held_back, &made));
    assert_eq!(follower.settings, served_with);
    assert_eq!(follower.wanted.as_ref(), Some(&wanted));
    assert!(
        !follower.follow(&held_back, &made),
        "followed again, as it was"
    );
    let taken_on = Record {
        served: None,
        ..held_back
    };
    assert!(follower.follow(&taken_on, &made));
    assert_eq!(follower.settings, wanted);
    assert_eq!(follower.wanted, None);
}

/// A follower hands on the leader's config last handed on, once for each the leader's
/// record holds, and none where it holds none; what it hands on is in its own record, as it
/// would be were it to lead.
#[test]
fn a_follower_keeps_the_leaders_config_once_for_each() {
    let plane = plane_with("rate_limiting: { mode: local }");
    let cluster = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    let mut leader = Serving::new(harness(&plane));
    let Step::Translated(translation) = leader.translator.step(&plane, &cluster) else {
        panic!("not translated");
    };
    let config = translation.config.clone();
    let mut follower = Serving::new(harness(&plane));
    assert_eq!(follower.keep(&leader.record(false)), None, "none held");
    let mut record = leader.record(true);
    assert_eq!(follower.keep(&record), Some(&config));
    assert_eq!(follower.keep(&record), None, "kept again");
    assert_eq!(follower.record(true).config, Some(config.clone()));
    let mut later = config;
    later.routes.clear();
    record.config = Some(later.clone());
    assert_eq!(follower.keep(&record), Some(&later));
}

/// A follower translates each data plane from the leader's record in the snapshot it
/// follows, and hands on the leader's config where its own translation is refused whole;
/// it takes no snapshot of its own, and takes in each the leader pushes as it runs. Once
/// it leads, it takes its records, counted on from the one it followed, which hold what it
/// followed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_translates_from_the_snapshot_and_counts_on_from_it_once_it_leads() {
    let refused_edit = PER_CLIENT_POLICY
        .replace(r#""generation":1"#, r#""generation":3"#)
        .replace("10/s", "10/fortnight");
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, &refused_edit)
        } else {
            otherwise(path)
        }
    })
    .await;
    let plane = limiting(&[]);
    let accepted = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    let mut leader = Serving::new(harness(&plane));
    let Step::Translated(translation) = leader.translator.step(&plane, &accepted) else {
        panic!("not translated");
    };
    let leaders_config = translation.config.clone();
    let record = Record {
        config: Some(leaders_config.clone()),
        ..leader.record(false)
    };
    let followed = Arc::new(Snapshot {
        count: 3,
        planes: BTreeMap::from([(String::new(), record)]),
    });
    let api = fake.client(&token("controller-follower", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (pushed, mut taken) = watch::channel(Some(Arc::clone(&followed)));
    taken.borrow_and_update();
    let (leads, leading) = watch::channel(false);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(only(harness(&plane)));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                let snapshot = pushed.clone();
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        Some(leading),
                        acks(),
                        Some(Snapshots {
                            snapshot,
                            settings: Rc::new(|_| None),
                        }),
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || heard.borrow().translations.len() == 1).await;
            assert_eq!(under(&heard.borrow().translations[0]), ["per-client"]);
            // Settings of 65 global limits: refused whole, the leader's config handed on.
            let names: Vec<String> = (0..65).map(|n| format!("global-{n}")).collect();
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            settings.send_replace(only(harness(&limiting(&names))));
            until("kept", || !heard.borrow().kept.is_empty()).await;
            assert_eq!(heard.borrow().kept, [(leaders_config.clone(), None)]);
            assert!(!taken.has_changed().unwrap(), "a follower took a snapshot");
            // The leader's next, pushed as the follower runs: its config handed on too.
            let mut later = leaders_config.clone();
            later.routes.clear();
            let mut next = Snapshot::clone(&followed);
            next.count = 4;
            next.planes.get_mut("").unwrap().config = Some(later.clone());
            pushed.send_replace(Some(Arc::new(next)));
            taken.borrow_and_update();
            until("kept again", || heard.borrow().kept.len() == 2).await;
            assert_eq!(heard.borrow().kept[1], (later, None));
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!taken.has_changed().unwrap(), "a follower took a snapshot");
            // It leads: its records are those it followed, so nothing new is taken, until
            // one changes, counted on from the one it followed. The settings back, its
            // translation is handed on, of the version of the policy it followed.
            leads.send_replace(true);
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                !taken.has_changed().unwrap(),
                "the records followed taken again"
            );
            settings.send_replace(only(harness(&plane)));
            until("taken", || taken.has_changed().unwrap()).await;
            let snapshot = taken.borrow_and_update().clone().unwrap();
            assert_eq!(snapshot.count, 5);
            let record = &snapshot.planes[""];
            assert_eq!(generations(&record.policies), [Some(1)]);
            assert_eq!(record.config, None);
            assert_eq!(
                under(heard.borrow().translations.last().unwrap()),
                ["per-client"]
            );
            controlling.abort();
        })
        .await;
}

/// A replica that comes to lead leads from the snapshot it holds as it does, one it took from
/// the ready replicas in place of the one it followed among it, though both come at once:
/// it translates from that snapshot's records, not from those it followed, and takes no
/// snapshot of its own over it that says otherwise (04 §2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_coming_to_lead_leads_from_the_snapshot_it_took() {
    let refused_edit = PER_CLIENT_POLICY
        .replace(r#""generation":1"#, r#""generation":3"#)
        .replace("10/s", "10/fortnight");
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[ingress("shop", "web", "shop.example")])
        } else if path.starts_with("/api/v1/services?") {
            page(&[service("shop")])
        } else if path.starts_with("/api/v1/secrets?") {
            page(&[secret("shop", &["shop.example"])])
        } else if path.starts_with("/apis/gateway.edgerush.dev/v1alpha1/ratelimitpolicies?") {
            answer(200, &refused_edit)
        } else {
            otherwise(path)
        }
    })
    .await;
    let plane = limiting(&[]);
    let accepted = with_policy(rate_limit_policy("shop", 1, &[("per-client", "10/s")]));
    // Followed: the policy's first version applies.
    let followed = Arc::new(Snapshot {
        count: 3,
        planes: BTreeMap::from([(String::new(), leaders_record(&plane, &accepted))]),
    });
    // Taken from a ready replica as it comes to lead, newer: no version of it applies.
    let gathered = Arc::new(Snapshot {
        count: 9,
        planes: BTreeMap::from([(String::new(), Record::default())]),
    });
    let api = fake.client(&token("controller-taking", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (pushed, mut taken) = watch::channel(Some(followed));
    let (leads, leading) = watch::channel(false);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_settings, planes) = watch::channel(only(harness(&plane)));
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                let snapshot = pushed.clone();
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        Some(leading),
                        acks(),
                        Some(Snapshots {
                            snapshot,
                            settings: Rc::new(|_| None),
                        }),
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("translated", || heard.borrow().translations.len() == 1).await;
            assert_eq!(under(&heard.borrow().translations[0]), ["per-client"]);
            // Both at once, as the replica taking the lease has them.
            pushed.send_replace(Some(Arc::clone(&gathered)));
            leads.send_replace(true);
            taken.borrow_and_update();
            until("translated again", || {
                heard.borrow().translations.len() == 2
            })
            .await;
            assert_eq!(under(&heard.borrow().translations[1]), Vec::<String>::new());
            tokio::time::sleep(Duration::from_millis(300)).await;
            let held = taken.borrow_and_update().clone().unwrap();
            assert_eq!(
                held.count, 9,
                "a snapshot of its own taken over the one it took"
            );
            assert_eq!(held.planes[""], Record::default());
            controlling.abort();
        })
        .await;
}

/// The leader alone writes an Event of each thing translation tells of an object with no
/// status to say it in, once however many data planes tell it, reported by its pod
/// (04 §6): an Ingress left out for its annotations, and a Service its routes reach whose
/// protection does not read. A replica that follows writes none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_leader_writes_an_event_of_each_thing_told_once_for_every_data_plane() {
    const EVENTS: &str = "/apis/events.k8s.io/v1/namespaces/shop/events";
    let annotated: Ingress = serde_saphyr::from_str(
        "metadata: { name: annotated, namespace: shop, uid: uid-annotated, annotations: { example.com/anything: x } }
spec:
  ingressClassName: edge
  rules:
    - host: other.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
",
    )
    .unwrap();
    let web: Ingress = serde_saphyr::from_str(
        "metadata: { name: web, namespace: shop, uid: uid-web }
spec:
  ingressClassName: edge
  rules:
    - host: shop.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
",
    )
    .unwrap();
    let typo: Service = serde_saphyr::from_str(
        "{ metadata: { name: web, namespace: shop, uid: uid-service, annotations: { gateway.edgerush.dev/max-inflight-per-pod: '3' } }, spec: { ports: [{ name: http, port: 80 }] } }",
    )
    .unwrap();
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if method == "POST" && path == EVENTS {
            return answer(
                201,
                r#"{"apiVersion":"events.k8s.io/v1","kind":"Event","metadata":{"name":"made.1","namespace":"shop"}}"#,
            );
        }
        if path.contains("watch=true") {
            return open();
        }
        if path.starts_with("/apis/networking.k8s.io/v1/ingressclasses?") {
            page(&[class()])
        } else if path.starts_with("/apis/networking.k8s.io/v1/ingresses?") {
            page(&[annotated.clone(), web.clone()])
        } else if path.starts_with("/api/v1/services?") {
            page(std::slice::from_ref(&typo))
        } else if path.starts_with("/api/v1/secrets?") {
            page::<Secret>(&[])
        } else {
            otherwise(path)
        }
    })
    .await;
    let posted = |fake: &Fake| -> Vec<serde_json::Value> {
        fake.seen()
            .into_iter()
            .filter(|seen| seen.method == "POST" && seen.path == EVENTS)
            .map(|seen| serde_json::from_str(&seen.body).unwrap())
            .collect()
    };
    let api = fake.client(&token("controller-events", "secret"), Timing::default());
    let heard: Rc<RefCell<Heard>> = Rc::default();
    let (leading, leads) = watch::channel(false);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let plane = plane();
            let planes = watch::channel(Planes::from([
                ("a".to_owned(), harness(&plane)),
                ("b".to_owned(), harness(&plane)),
            ]))
            .1;
            let controlling = tokio::task::spawn_local({
                let said = hear(&heard);
                async move {
                    control(
                        api,
                        &WatchScope::Parts(vec![Scope::Cluster]),
                        planes,
                        None,
                        Debounce::default(),
                        Some(leads),
                        acks(),
                        None,
                        "replica".to_owned(),
                        said,
                    )
                    .await;
                }
            });
            until("both translated", || heard.borrow().translations.len() >= 2).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(posted(&fake), Vec::<serde_json::Value>::new(), "following");
            leading.send_replace(true);
            until("two Events", || posted(&fake).len() >= 2).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut told: Vec<(String, String, String, String)> = posted(&fake)
                .iter()
                .map(|event| {
                    assert_eq!(event["type"], "Warning", "{event}");
                    assert_eq!(event["action"], "Translate", "{event}");
                    assert_eq!(event["reportingController"], "edgerush.dev/control-plane");
                    assert_eq!(event["reportingInstance"], "replica", "{event}");
                    let regarding = &event["regarding"];
                    (
                        regarding["kind"].as_str().unwrap().to_owned(),
                        regarding["name"].as_str().unwrap().to_owned(),
                        regarding["uid"].as_str().unwrap().to_owned(),
                        event["reason"].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            told.sort();
            let owned = |kind: &str, name: &str, uid: &str, reason: &str| {
                (
                    kind.to_owned(),
                    name.to_owned(),
                    uid.to_owned(),
                    reason.to_owned(),
                )
            };
            assert_eq!(
                told,
                [
                    owned("Ingress", "annotated", "uid-annotated", "NotSupportedYet"),
                    owned("Service", "web", "uid-service", "Unprotected"),
                ],
                "{:?}",
                heard.borrow().lines
            );
            // Following again, it writes nothing, and forgets what it wrote: leading once
            // more, it writes its own afresh, as a new leader does (04 §6).
            leading.send_replace(false);
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(posted(&fake).len(), 2, "following again");
            leading.send_replace(true);
            until("two Events more", || posted(&fake).len() >= 4).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(posted(&fake).len(), 4, "{:?}", heard.borrow().lines);
            controlling.abort();
        })
        .await;
}
