#![allow(
    clippy::unwrap_used,
    reason = "tests: a fixture that cannot be made is a broken test"
)]

use super::*;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, Seen as Asked, answer, token};
use edgerush_planes::data_plane::DataPlaneReplicas;
use k8s_openapi::jiff::Timestamp;
use std::cell::RefCell;

const APPLY: &str = "?fieldManager=edgerush-operator&force=true";

fn install() -> Install {
    Install {
        image: "edgerush:test".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
    }
}

fn time(at: i64) -> Time {
    Time(Timestamp::from_second(1_800_000_000 + at).unwrap())
}

/// ControlPlane `edge` of namespace `edge`, watching `from`.
fn control_plane(from: &str) -> ControlPlane {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "ControlPlane",
        "metadata": {
            "name": "edge", "namespace": "edge", "uid": "cp-uid", "generation": 1,
            "creationTimestamp": "2027-01-15T08:00:00Z",
        },
        "spec": { "watch": { "namespaces": { "from": from } } },
    }))
    .unwrap()
}

/// DataPlane `web` of namespace `edge`, of ControlPlane `edge`, serving Ingress.
fn data_plane() -> DataPlane {
    serde_json::from_value(data_plane_json()).unwrap()
}

/// [`data_plane`], as JSON.
fn data_plane_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "DataPlane",
        "metadata": {
            "name": "web", "namespace": "edge", "uid": "dp-uid", "generation": 1,
            "creationTimestamp": "2027-01-15T08:00:00Z",
        },
        "spec": {
            "controlPlaneRef": { "name": "edge" },
            "ingress": { "httpPort": 80, "httpsPort": 443 },
        },
    })
}

/// DataPlane `web` as [`data_plane`], with a Gateway too.
fn data_plane_with_gateway() -> DataPlane {
    let mut value = data_plane_json();
    value["spec"]["gateway"] = serde_json::json!({
        "listeners": [{ "name": "http", "port": 8080, "protocol": "HTTP" }],
    });
    serde_json::from_value(value).unwrap()
}

/// A cluster with the Gateway API's CRDs.
const SERVED: GatewayApi = GatewayApi {
    gateway_classes: true,
    gateways: true,
};

/// A cluster without the Gateway API's CRDs.
const NO_GATEWAY_API: GatewayApi = GatewayApi {
    gateway_classes: false,
    gateways: false,
};

/// The operator's finalizer on a resource, read at version 4.
fn finalized(metadata: &mut ObjectMeta) {
    metadata.finalizers = Some(vec![FINALIZER.to_owned()]);
    metadata.resource_version = Some("4".to_owned());
}

/// What an apply or a read of `path` is answered with: an object of its kind, of the name
/// it ends in, as little of one as reads as that kind.
fn object_at(path: &str) -> String {
    let path = path.split('?').next().unwrap();
    let mut segments = path.rsplit('/');
    let mut name = segments.next().unwrap();
    let mut plural = segments.next().unwrap();
    if name == "status" {
        (name, plural) = (plural, segments.next().unwrap());
    }
    let role_ref =
        r#""roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"r"}"#;
    let (api_version, kind, rest) = match plural {
        "serviceaccounts" => ("v1", "ServiceAccount", String::new()),
        "services" => ("v1", "Service", String::new()),
        "rolebindings" => (
            "rbac.authorization.k8s.io/v1",
            "RoleBinding",
            format!(",{role_ref}"),
        ),
        "clusterrolebindings" => (
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            format!(",{role_ref}"),
        ),
        "deployments" => ("apps/v1", "Deployment", String::new()),
        "horizontalpodautoscalers" => ("autoscaling/v2", "HorizontalPodAutoscaler", String::new()),
        "poddisruptionbudgets" => ("policy/v1", "PodDisruptionBudget", String::new()),
        "gatewayclasses" => (
            "gateway.networking.k8s.io/v1",
            "GatewayClass",
            r#","spec":{"controllerName":"edgerush.dev/gateway-controller"}"#.to_owned(),
        ),
        "gateways" => (
            "gateway.networking.k8s.io/v1",
            "Gateway",
            r#","spec":{"gatewayClassName":"edge","listeners":[]}"#.to_owned(),
        ),
        "ingressclasses" => ("networking.k8s.io/v1", "IngressClass", String::new()),
        "validatingwebhookconfigurations" => (
            "admissionregistration.k8s.io/v1",
            "ValidatingWebhookConfiguration",
            String::new(),
        ),
        "controlplanes" => (
            "gateway.edgerush.dev/v1alpha1",
            "ControlPlane",
            String::new(),
        ),
        "dataplanes" => ("gateway.edgerush.dev/v1alpha1", "DataPlane", String::new()),
        other => panic!("no kind for {other}"),
    };
    format!(
        r#"{{"apiVersion":"{api_version}","kind":"{kind}","metadata":{{"name":"{name}"}}{rest}}}"#
    )
}

/// A fake API server holding nothing: each read answered 404, each apply and delete done,
/// but where `script` answers a path otherwise.
async fn cluster(
    script: impl Fn(&str, &str) -> Option<Answer> + Send + Sync + 'static,
) -> (Fake, ApiServer) {
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if let Some(scripted) = script(method, path) {
            return scripted;
        }
        match method {
            "GET" => answer(404, r#"{"kind":"Status","code":404}"#),
            "DELETE" => answer(200, r#"{"kind":"Status","code":200}"#),
            _ => answer(200, &object_at(path)),
        }
    })
    .await;
    let api = fake.client(&token("operator", "secret"), Timing::default());
    (fake, api)
}

/// What a step says, kept.
#[derive(Default)]
struct Said(RefCell<Vec<String>>);

impl Said {
    fn say(&self) -> impl Fn(&str) + '_ {
        |said: &str| self.0.borrow_mut().push(said.to_owned())
    }

    fn any(&self, part: &str) -> bool {
        self.0.borrow().iter().any(|said| said.contains(part))
    }
}

/// The writes asked of the fake, as `METHOD path`.
fn writes(asked: &[Asked]) -> Vec<String> {
    asked
        .iter()
        .filter(|asked| asked.method != "GET")
        .map(|asked| format!("{} {}", asked.method, asked.path))
        .collect()
}

/// The conditions the status written to `path` says, as `(type, status, reason)`.
fn status_written(asked: &[Asked], path: &str) -> Vec<(String, String, String)> {
    let written = asked
        .iter()
        .rev()
        .find(|asked| asked.method == "PATCH" && asked.path == format!("{path}/status{APPLY}"))
        .unwrap_or_else(|| panic!("no status written to {path}"));
    let body: serde_json::Value = serde_json::from_str(&written.body).unwrap();
    body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|condition| {
            let text = |field: &str| condition[field].as_str().unwrap().to_owned();
            (text("type"), text("status"), text("reason"))
        })
        .collect()
}

fn conditions_of(said: &[(&str, &str, &str)]) -> Vec<(String, String, String)> {
    said.iter()
        .map(|(a, b, c)| ((*a).to_owned(), (*b).to_owned(), (*c).to_owned()))
        .collect()
}

const CONTROL_PLANE: &str =
    "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/controlplanes/edge";
const DATA_PLANE: &str = "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/dataplanes/web";

/// Where `object` is applied.
fn path_of(object: &Object) -> String {
    let value = serde_json::to_value(object).unwrap();
    let api_version = value["apiVersion"].as_str().unwrap();
    let group = if api_version.contains('/') {
        format!("/apis/{api_version}")
    } else {
        format!("/api/{api_version}")
    };
    let plural = match object.kind() {
        "ServiceAccount" => "serviceaccounts",
        "RoleBinding" => "rolebindings",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Deployment" => "deployments",
        "Service" => "services",
        "HorizontalPodAutoscaler" => "horizontalpodautoscalers",
        "PodDisruptionBudget" => "poddisruptionbudgets",
        "GatewayClass" => "gatewayclasses",
        "IngressClass" => "ingressclasses",
        "Gateway" => "gateways",
        "ValidatingWebhookConfiguration" => "validatingwebhookconfigurations",
        other => panic!("{other}"),
    };
    let metadata = object.metadata();
    let name = metadata.name.as_deref().unwrap();
    match metadata.namespace.as_deref() {
        Some(namespace) => format!("{group}/namespaces/{namespace}/{plural}/{name}"),
        None => format!("{group}/{plural}/{name}"),
    }
}

/// An authority's bundle, as a control plane publishes it; its body is never read here.
const BUNDLE: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

/// `control_plane` with its authority's `bundle` in its status, as its leader publishes it.
fn published(mut control_plane: ControlPlane, bundle: &str) -> ControlPlane {
    let status = control_plane.status.get_or_insert_default();
    status.authority = Some(
        edgerush_planes::control_plane::ControlPlaneStatusAuthority {
            bundle: Some(bundle.to_owned()),
        },
    );
    control_plane
}

/// Every object made for the control plane and data plane of these tests.
fn made() -> (Vec<Object>, Vec<Object>) {
    made_of(&data_plane())
}

/// Every object made for the control plane and data plane of these tests once its bundle is
/// published, as [`after_first`] has them.
fn made_published() -> (Vec<Object>, Vec<Object>) {
    made_for(&published(control_plane("Same"), BUNDLE), &data_plane())
}

/// Every object made for the control plane of these tests, and for `data_plane` of it.
fn made_of(data_plane: &DataPlane) -> (Vec<Object>, Vec<Object>) {
    made_for(&control_plane("Same"), data_plane)
}

/// Every object made for `control_plane`, and for `data_plane` of it.
fn made_for(control_plane: &ControlPlane, data_plane: &DataPlane) -> (Vec<Object>, Vec<Object>) {
    let selected = BTreeSet::new();
    (
        objects::for_control_plane(control_plane, &selected, &install()).unwrap(),
        objects::for_data_plane(data_plane, control_plane, &selected, &install()).unwrap(),
    )
}

/// Every object of `objects` as the follows would see it once made, at `version`.
fn children_of(objects: &[Object], version: &str) -> Vec<Child> {
    objects
        .iter()
        .map(|object| {
            let metadata = object.metadata();
            Child {
                kind: object.kind(),
                namespace: metadata.namespace.clone().unwrap_or_default(),
                name: metadata.name.clone().unwrap(),
                version: version.to_owned(),
                labels: metadata.labels.clone().unwrap(),
                available: (object.kind() == "Deployment").then_some(0),
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_resource_is_finalized_then_made_then_reported() {
    let (fake, api) = cluster(|_, _| None).await;
    let seen = Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("Same")],
        data_planes: vec![data_plane()],
        children: Vec::new(),
        gateway_classes: Vec::new(),
        gateways: Vec::new(),
        gateway_api: SERVED,
    };
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(
        operator.step(&api, &seen, &time(0), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let writes = writes(&asked);
    let (for_control_plane, for_data_plane) = made();
    // Each resource: its finalizer, then each object read and applied, then its status.
    let mut expected = vec![format!("PATCH {CONTROL_PLANE}{APPLY}")];
    expected.extend(
        for_control_plane
            .iter()
            .map(|object| format!("PATCH {}{APPLY}", path_of(object))),
    );
    expected.push(format!("PATCH {CONTROL_PLANE}/status{APPLY}"));
    expected.push(format!("PATCH {DATA_PLANE}{APPLY}"));
    expected.extend(
        for_data_plane
            .iter()
            .map(|object| format!("PATCH {}{APPLY}", path_of(object))),
    );
    expected.push(format!("PATCH {DATA_PLANE}/status{APPLY}"));
    assert_eq!(writes, expected);
    let reads = asked.iter().filter(|asked| asked.method == "GET").count();
    assert_eq!(reads, for_control_plane.len() + for_data_plane.len());
    // What is applied is what edgerush-planes makes, whole.
    let applied = asked
        .iter()
        .find(|asked| asked.path == format!("{}{APPLY}", path_of(&for_data_plane[0])))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&applied.body).unwrap(),
        serde_json::to_value(&for_data_plane[0]).unwrap()
    );
    let finalizer = asked
        .iter()
        .find(|asked| asked.path == format!("{DATA_PLANE}{APPLY}"))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&finalizer.body).unwrap(),
        serde_json::json!({
            "apiVersion": "gateway.edgerush.dev/v1alpha1",
            "kind": "DataPlane",
            "metadata": { "name": "web", "namespace": "edge", "finalizers": [FINALIZER] },
        })
    );
    let pending = conditions_of(&[
        ("Accepted", "True", "Accepted"),
        ("Programmed", "False", "Pending"),
    ]);
    assert_eq!(status_written(&asked, CONTROL_PLANE), pending);
    assert_eq!(status_written(&asked, DATA_PLANE), pending);
    assert!(
        said.any("DataPlane edge/web: Accepted True"),
        "{:?}",
        said.0
    );
}

/// The step after the first, of the resources as the first left them, with every object it
/// made at version `version`, and the proxy's Deployment with `available` replicas; the
/// control plane's bundle published since, and its webhook registered with it.
fn after_first(version: &str, available: i32) -> Seen {
    after_first_of(data_plane(), SERVED, version, available)
}

/// [`after_first`] of `data_plane`, in a cluster that serves `gateway_api`, where nothing
/// of a kind it does not serve was made.
fn after_first_of(
    data_plane: DataPlane,
    gateway_api: GatewayApi,
    version: &str,
    available: i32,
) -> Seen {
    let (for_control_plane, for_data_plane) =
        made_for(&published(control_plane("Same"), BUNDLE), &data_plane);
    let mut children = children_of(&for_control_plane, version);
    children.extend(children_of(&for_data_plane, version));
    children.retain(|child| match child.kind {
        "GatewayClass" => gateway_api.gateway_classes,
        "Gateway" => gateway_api.gateways,
        _ => true,
    });
    for child in &mut children {
        if child.kind == "Deployment" {
            child.available = Some(available);
        }
    }
    let mut cp = control_plane("Same");
    finalized(&mut cp.metadata);
    let mut dp = data_plane;
    finalized(&mut dp.metadata);
    let wanted = |workload: &str| {
        status::conditions(
            Some(1),
            Ok(()),
            &Made::Applied,
            workload,
            available > 0,
            &[],
            &time(0),
        )
    };
    cp.status = Some(edgerush_planes::control_plane::ControlPlaneStatus {
        conditions: Some(wanted("edgerush-controller-edge")),
        authority: None,
        rate_limiting: None,
    });
    let cp = published(cp, BUNDLE);
    dp.status = Some(edgerush_planes::data_plane::DataPlaneStatus {
        conditions: Some(wanted("edgerush-proxy-web")),
    });
    Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![cp],
        data_planes: vec![dp],
        children,
        gateway_classes: Vec::new(),
        gateways: Vec::new(),
        gateway_api,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_changed_is_applied_once_and_a_hand_edit_is_applied_again() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = after_first("1", 1);
    // A new leader applies everything once, as nothing says it was.
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let (for_control_plane, for_data_plane) = made_published();
    assert_eq!(
        writes(&fake.seen()).len(),
        for_control_plane.len() + for_data_plane.len(),
        "every object applied once, and nothing else: {:?}",
        writes(&fake.seen())
    );
    // Then nothing, nothing having changed.
    let before = fake.seen().len();
    assert!(
        operator.step(&api, &seen, &time(20), &said.say()).await,
        "{:?}",
        said.0
    );
    assert_eq!(fake.seen().len(), before, "{:?}", &fake.seen()[before..]);
    // The proxy's Deployment edited by hand: the DataPlane's objects applied again, the
    // ControlPlane's not.
    let mut edited = seen.clone();
    for child in &mut edited.children {
        if child.kind == "Deployment" && child.name == "edgerush-proxy-web" {
            child.version = "2".to_owned();
        }
    }
    assert!(
        operator.step(&api, &edited, &time(30), &said.say()).await,
        "{:?}",
        said.0
    );
    let again = writes(&fake.seen()[before..]);
    let expected: Vec<String> = for_data_plane
        .iter()
        .map(|object| format!("PATCH {}{APPLY}", path_of(object)))
        .collect();
    assert_eq!(again, expected);
    // A Deployment deleted by hand is gone from what is seen: applied again too, and,
    // with no replica of it available, said so.
    let before = fake.seen().len();
    let mut deleted = edited.clone();
    deleted
        .children
        .retain(|child| !(child.kind == "Deployment" && child.name == "edgerush-controller-edge"));
    assert!(
        operator.step(&api, &deleted, &time(40), &said.say()).await,
        "{:?}",
        said.0
    );
    let again = writes(&fake.seen()[before..]);
    let mut expected: Vec<String> = for_control_plane
        .iter()
        .map(|object| format!("PATCH {}{APPLY}", path_of(object)))
        .collect();
    expected.push(format!("PATCH {CONTROL_PLANE}/status{APPLY}"));
    assert_eq!(again, expected);
    assert_eq!(
        status_written(&fake.seen(), CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending")
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_generation_is_applied_and_reported_of_it() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = after_first("1", 1);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let before = fake.seen().len();
    let mut edited = seen.clone();
    edited.data_planes[0].metadata.generation = Some(2);
    assert!(
        operator.step(&api, &edited, &time(20), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen()[before..].to_vec();
    let (_, for_data_plane) = made();
    assert_eq!(writes(&asked).len(), for_data_plane.len() + 1);
    let written = asked.last().unwrap();
    assert_eq!(written.path, format!("{DATA_PLANE}/status{APPLY}"));
    let body: serde_json::Value = serde_json::from_str(&written.body).unwrap();
    assert!(
        body["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|condition| condition["observedGeneration"] == 2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn programmed_once_its_workload_has_a_replica_available() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    // As the first step left them, said pending; then each Deployment has a replica.
    let mut seen = after_first("1", 0);
    for child in &mut seen.children {
        if child.kind == "Deployment" {
            child.available = Some(1);
        }
    }
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let programmed = conditions_of(&[
        ("Accepted", "True", "Accepted"),
        ("Programmed", "True", "Programmed"),
    ]);
    let asked = fake.seen();
    assert_eq!(status_written(&asked, CONTROL_PLANE), programmed);
    assert_eq!(status_written(&asked, DATA_PLANE), programmed);
    // Said programmed, and then no replica of the proxy's is available: pending again.
    let mut lost = after_first("1", 1);
    for child in &mut lost.children {
        if child.name == "edgerush-proxy-web" {
            child.available = Some(0);
        }
    }
    let before = fake.seen().len();
    assert!(
        operator.step(&api, &lost, &time(20), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen()[before..].to_vec();
    assert_eq!(
        status_written(&asked, DATA_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending")
        ])
    );
    assert!(
        !writes(&asked)
            .iter()
            .any(|write| write.contains("controlplanes")),
        "the ControlPlane's status written again: {:?}",
        writes(&asked)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_object_of_its_name_not_the_operators_is_left_alone_and_said() {
    let (_, for_data_plane) = made();
    let theirs = path_of(
        for_data_plane
            .iter()
            .find(|object| object.kind() == "Deployment")
            .unwrap(),
    );
    let class = path_of(
        for_data_plane
            .iter()
            .find(|object| object.kind() == "IngressClass")
            .unwrap(),
    );
    let (deployment, ingress_class) = (theirs.clone(), class.clone());
    let (fake, api) = cluster(move |method, path| {
        if method != "GET" {
            return None;
        }
        // Someone's own Deployment of that name.
        if path == deployment {
            return Some(answer(
                200,
                r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"edgerush-proxy-web","labels":{"app":"theirs"}}}"#,
            ));
        }
        // The operator's IngressClass, its labels taken away by hand: its owner says so.
        if path == ingress_class {
            return Some(answer(
                200,
                r#"{"apiVersion":"networking.k8s.io/v1","kind":"IngressClass","metadata":{"name":"edge.web","ownerReferences":[{"apiVersion":"gateway.edgerush.dev/v1alpha1","kind":"DataPlane","name":"web","uid":"dp-uid"}]}}"#,
            ));
        }
        None
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("Same")],
        data_planes: vec![data_plane()],
        children: Vec::new(),
        gateway_classes: Vec::new(),
        gateways: Vec::new(),
        gateway_api: SERVED,
    };
    assert!(
        !operator.step(&api, &seen, &time(0), &said.say()).await,
        "done, with an object left alone"
    );
    let asked = fake.seen();
    let written = writes(&asked);
    assert!(
        !written.contains(&format!("PATCH {theirs}{APPLY}")),
        "theirs taken over"
    );
    assert!(written.contains(&format!("PATCH {class}{APPLY}")));
    // Every other object of the DataPlane's applied.
    let applied = for_data_plane
        .iter()
        .filter(|object| written.contains(&format!("PATCH {}{APPLY}", path_of(object))))
        .count();
    assert_eq!(applied, for_data_plane.len() - 1);
    assert_eq!(
        status_written(&asked, DATA_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "NotOurs")
        ])
    );
    assert!(
        said.any("Deployment edge/edgerush-proxy-web"),
        "{:?}",
        said.0
    );
    // Tried again at the next step, whatever has changed.
    let before = fake.seen().len();
    operator.step(&api, &seen, &time(10), &said.say()).await;
    assert!(writes(&fake.seen()[before..]).contains(&format!("PATCH {class}{APPLY}")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_apply_refused_is_said_and_tried_again() {
    let (_, for_data_plane) = made();
    let refused = path_of(
        for_data_plane
            .iter()
            .find(|object| object.kind() == "Deployment")
            .unwrap(),
    );
    let refusing = refused.clone();
    let (fake, api) = cluster(move |method, path| {
        (method == "PATCH" && path.starts_with(&format!("{refusing}?"))).then(|| {
            answer(
                422,
                r#"{"kind":"Status","code":422,"message":"denied by a policy"}"#,
            )
        })
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = after_first("1", 0);
    assert!(!operator.step(&api, &seen, &time(10), &said.say()).await);
    let asked = fake.seen();
    let status = status_written(&asked, DATA_PLANE);
    assert_eq!(status[1].2, "ApplyRefused");
    assert!(said.any("denied by a policy"), "{:?}", said.0);
    // The objects after it are not applied while it is refused.
    let tried: Vec<String> = writes(&asked)
        .into_iter()
        .filter(|write| write.contains(&refused))
        .collect();
    assert_eq!(tried.len(), 1);
    // Nothing changed, and yet applied again.
    let before = fake.seen().len();
    assert!(!operator.step(&api, &seen, &time(20), &said.say()).await);
    assert!(
        writes(&fake.seen()[before..])
            .iter()
            .any(|write| write.contains(&refused))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_control_plane_makes_nothing_and_its_data_plane_waits_on_it() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("All")],
        data_planes: vec![data_plane()],
        children: Vec::new(),
        gateway_classes: Vec::new(),
        gateways: Vec::new(),
        gateway_api: SERVED,
    };
    assert!(
        operator.step(&api, &seen, &time(0), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(
        writes(&asked),
        [
            format!("PATCH {CONTROL_PLANE}/status{APPLY}"),
            format!("PATCH {DATA_PLANE}/status{APPLY}"),
        ],
        "anything made, or a finalizer added"
    );
    assert_eq!(
        status_written(&asked, CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "False", "ScopeNotAllowed"),
            ("Programmed", "False", "Pending")
        ])
    );
    // Its DataPlane is accepted, as the control plane keeps serving it, and waits.
    assert_eq!(
        status_written(&asked, DATA_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending")
        ])
    );
    assert!(
        said.any("ControlPlane edge is not accepted"),
        "{:?}",
        said.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_taken_is_refused_and_nothing_is_made_for_it() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut taken = control_plane("Same");
    taken.metadata.namespace = Some("late".to_owned());
    taken.metadata.uid = Some("late-uid".to_owned());
    taken.metadata.creation_timestamp = Some(time(1_000_000_000));
    let mut seen = after_first("1", 1);
    seen.control_planes.push(taken);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let late = "/apis/gateway.edgerush.dev/v1alpha1/namespaces/late/controlplanes/edge";
    assert_eq!(
        status_written(&asked, late),
        conditions_of(&[
            ("Accepted", "False", "NameTaken"),
            ("Programmed", "False", "Pending")
        ])
    );
    assert!(
        !writes(&asked)
            .iter()
            .any(|write| write.contains("namespaces/late/")
                && !write.ends_with(&format!("edge/status{APPLY}"))),
        "made for one refused: {:?}",
        writes(&asked)
    );
}

/// A second DataPlane of one ControlPlane is accepted, and has every object of its own
/// made beside the first's, none of them the first's, whose status is left as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_data_plane_of_a_control_plane_is_accepted_and_has_its_own_objects_made() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut value = data_plane_json();
    value["metadata"]["name"] = "api".into();
    value["metadata"]["uid"] = "api-uid".into();
    value["metadata"]["creationTimestamp"] = "2027-01-15T09:00:00Z".into();
    value["spec"]["gateway"] = serde_json::json!({
        "listeners": [{ "name": "http", "port": 8080, "protocol": "HTTP" }],
    });
    let api_plane: DataPlane = serde_json::from_value(value).unwrap();
    let mut seen = after_first("1", 1);
    seen.data_planes.push(api_plane.clone());
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let second = "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/dataplanes/api";
    assert_eq!(
        status_written(&asked, second),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending")
        ])
    );
    let writes = writes(&asked);
    let (_, for_web) = made_published();
    let (_, for_api) = made_for(&published(control_plane("Same"), BUNDLE), &api_plane);
    for object in &for_api {
        let path = path_of(object);
        assert!(
            writes.contains(&format!("PATCH {path}{APPLY}")),
            "{path} not applied: {writes:?}"
        );
        assert!(
            !for_web.iter().any(|web| path_of(web) == path),
            "{path} is DataPlane web's too"
        );
    }
    assert!(
        !writes.contains(&format!("PATCH {DATA_PLANE}/status{APPLY}")),
        "DataPlane web's status written again: {writes:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_resource_has_what_no_owner_reaches_deleted_then_its_finalizer_taken_away() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first("1", 1);
    seen.data_planes[0].metadata.deletion_timestamp = Some(time(5));
    seen.data_planes[0]
        .metadata
        .finalizers
        .as_mut()
        .unwrap()
        .push("other.example/keep".to_owned());
    seen.control_planes[0].metadata.deletion_timestamp = Some(time(5));
    // A binding of the DataPlane's in another namespace, as a wider scope would have made.
    let elsewhere = seen
        .children
        .iter()
        .find(|child| {
            child.kind == "RoleBinding" && child.labels.contains_key(objects::DATA_PLANE_LABEL)
        })
        .cloned()
        .unwrap();
    seen.children.push(Child {
        namespace: "shop".to_owned(),
        ..elsewhere
    });
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let selector = |labels: &BTreeMap<String, String>| {
        let each: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let mut encoded = String::new();
        for byte in each.join(",").bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                encoded.push(char::from(byte));
            } else {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
        encoded
    };
    let control = selector(&control_plane_labels("edge", "edge"));
    let data = selector(&data_plane_labels("edge", "edge", "web"));
    let background = "propagationPolicy=Background";
    assert_eq!(
        writes(&fake.seen()),
        [
            format!(
                "DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings?labelSelector={control}&{background}"
            ),
            format!(
                "DELETE /apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations?labelSelector={control}&{background}"
            ),
            format!(
                "DELETE /apis/gateway.networking.k8s.io/v1/gatewayclasses?labelSelector={control}&{background}"
            ),
            format!("PATCH {CONTROL_PLANE}"),
            format!(
                "DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings?labelSelector={data}&{background}"
            ),
            format!(
                "DELETE /apis/networking.k8s.io/v1/ingressclasses?labelSelector={data}&{background}"
            ),
            format!(
                "DELETE /apis/rbac.authorization.k8s.io/v1/namespaces/shop/rolebindings?labelSelector={data}&{background}"
            ),
            format!("PATCH {DATA_PLANE}"),
        ]
    );
    // The finalizer taken away: the finalizers written whole without it, another's kept,
    // as of the version read.
    let taken = fake
        .seen()
        .into_iter()
        .find(|asked| asked.path == DATA_PLANE)
        .unwrap();
    assert_eq!(
        taken.content_type.as_deref(),
        Some("application/merge-patch+json")
    );
    let body: serde_json::Value = serde_json::from_str(&taken.body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "metadata": { "resourceVersion": "4", "finalizers": ["other.example/keep"] }
        })
    );
    // Once it is taken away, nothing more is asked.
    let before = fake.seen().len();
    for plane in &mut seen.control_planes {
        plane.metadata.finalizers = None;
    }
    for plane in &mut seen.data_planes {
        plane.metadata.finalizers = None;
    }
    assert!(
        operator.step(&api, &seen, &time(20), &said.say()).await,
        "{:?}",
        said.0
    );
    assert_eq!(fake.seen().len(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finalizer_stays_until_every_delete_has_succeeded() {
    let (fake, api) = cluster(|method, path| {
        (method == "DELETE" && path.contains("ingressclasses"))
            .then(|| answer(403, r#"{"kind":"Status","code":403,"message":"forbidden"}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first("1", 1);
    seen.data_planes[0].metadata.deletion_timestamp = Some(time(5));
    assert!(!operator.step(&api, &seen, &time(10), &said.say()).await);
    assert!(
        !writes(&fake.seen()).contains(&format!("PATCH {DATA_PLANE}")),
        "finalizer taken away with a delete refused"
    );
    assert!(said.any("cannot be deleted"), "{:?}", said.0);
}

/// One refused for a name another holds, given the operator's finalizer by hand and
/// deleted: what it deletes by label is selected by its own namespace too, never the
/// holder's objects of the same names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resource_of_the_same_names_elsewhere_deletes_nothing_of_the_holders() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first("1", 1);
    let mut taken = control_plane("Same");
    taken.metadata.namespace = Some("late".to_owned());
    taken.metadata.uid = Some("late-uid".to_owned());
    taken.metadata.creation_timestamp = Some(time(1_000_000_000));
    finalized(&mut taken.metadata);
    taken.metadata.deletion_timestamp = Some(time(5));
    let mut copied = data_plane();
    copied.metadata.namespace = Some("late".to_owned());
    copied.metadata.uid = Some("copied-uid".to_owned());
    finalized(&mut copied.metadata);
    copied.metadata.deletion_timestamp = Some(time(5));
    seen.control_planes.push(taken);
    seen.data_planes.push(copied);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let deletes: Vec<String> = writes(&fake.seen())
        .into_iter()
        .filter(|write| write.starts_with("DELETE"))
        .collect();
    assert!(!deletes.is_empty());
    for delete in &deletes {
        assert!(
            delete.contains("gateway.edgerush.dev%2Fnamespace%3Dlate"),
            "a delete not of namespace late's: {delete}"
        );
    }
}

/// Its finalizers changed since they were read: nothing undone, and tried again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finalizers_changed_meanwhile_are_read_again_not_undone() {
    let (fake, api) = cluster(|method, path| {
        (method == "PATCH" && path == DATA_PLANE).then(|| {
            answer(
                409,
                r#"{"kind":"Status","code":409,"message":"the object has been modified"}"#,
            )
        })
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first("1", 1);
    seen.data_planes[0].metadata.deletion_timestamp = Some(time(5));
    assert!(!operator.step(&api, &seen, &time(10), &said.say()).await);
    assert!(
        said.any("its finalizer cannot be taken away"),
        "{:?}",
        said.0
    );
    assert_eq!(
        writes(&fake.seen())
            .iter()
            .filter(|write| *write == &format!("PATCH {DATA_PLANE}"))
            .count(),
        1
    );
}

/// The message of the `Programmed` condition last written to `path`'s status.
fn programmed_message(asked: &[Asked], path: &str) -> String {
    let written = asked
        .iter()
        .rev()
        .find(|asked| asked.method == "PATCH" && asked.path == format!("{path}/status{APPLY}"))
        .unwrap_or_else(|| panic!("no status written to {path}"));
    let body: serde_json::Value = serde_json::from_str(&written.body).unwrap();
    let conditions = body["status"]["conditions"].as_array().unwrap();
    let programmed = conditions
        .iter()
        .find(|condition| condition["type"] == "Programmed")
        .unwrap();
    programmed["message"].as_str().unwrap().to_owned()
}

/// Whether a request is of one of the Gateway API's kinds.
fn of_gateway_api(asked: &Asked) -> bool {
    asked.path.starts_with("/apis/gateway.networking.k8s.io/")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_gateway_apis_crds_the_rest_is_made_and_a_gateway_waits_on_them() {
    let (fake, api) = cluster(|_, _| None).await;
    let seen = Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("Same")],
        data_planes: vec![data_plane_with_gateway()],
        children: Vec::new(),
        gateway_classes: Vec::new(),
        gateways: Vec::new(),
        gateway_api: NO_GATEWAY_API,
    };
    let said = Said::default();
    let mut operator = Operator::new(install());
    // Not a step left undone, to be tried again: the CRDs' follows say when they are there.
    assert!(
        operator.step(&api, &seen, &time(0), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let of_its_kinds: Vec<_> = asked.iter().filter(|asked| of_gateway_api(asked)).collect();
    assert!(of_its_kinds.is_empty(), "{of_its_kinds:?}");
    // Everything else made.
    let (for_control_plane, for_data_plane) = made_of(&data_plane_with_gateway());
    let writes = writes(&asked);
    for object in for_control_plane.iter().chain(&for_data_plane) {
        if !matches!(object, Object::GatewayClass(_) | Object::Gateway(_)) {
            let applied = format!("PATCH {}{APPLY}", path_of(object));
            assert!(writes.contains(&applied), "{applied} not in {writes:?}");
        }
    }
    let pending = conditions_of(&[
        ("Accepted", "True", "Accepted"),
        ("Programmed", "False", "Pending"),
    ]);
    assert_eq!(status_written(&asked, DATA_PLANE), pending);
    assert_eq!(
        programmed_message(&asked, DATA_PLANE),
        "Gateway edge/web is not made until the cluster serves the Gateway API's Gateways, \
         their CRD at v1; the rest is made."
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_apis_crds_installed_later_have_what_was_left_out_made() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    // Everything but the GatewayClass and the Gateway made, the workloads available.
    let without = after_first_of(data_plane_with_gateway(), NO_GATEWAY_API, "1", 1);
    assert!(
        operator.step(&api, &without, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    // The ControlPlane is programmed without its GatewayClass, its status left as it is;
    // the DataPlane not, without its Gateway.
    assert!(
        !writes(&asked).contains(&format!("PATCH {CONTROL_PLANE}/status{APPLY}")),
        "{:?}",
        writes(&asked)
    );
    assert_eq!(
        status_written(&asked, DATA_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending"),
        ])
    );
    // Nothing changed, nothing more is asked: waiting on the CRDs is not retried.
    let mut waiting = without.clone();
    waiting.data_planes[0].status = Some(edgerush_planes::data_plane::DataPlaneStatus {
        conditions: Some(status::conditions(
            Some(1),
            Ok(()),
            &Made::Pending(programmed_message(&asked, DATA_PLANE)),
            "edgerush-proxy-web",
            true,
            &[],
            &time(10),
        )),
    });
    let before = fake.seen().len();
    assert!(
        operator.step(&api, &waiting, &time(20), &said.say()).await,
        "{:?}",
        said.0
    );
    assert_eq!(fake.seen().len(), before, "{:?}", &fake.seen()[before..]);
    // The CRDs installed, and nothing else changed: the GatewayClass and the Gateway made,
    // and the DataPlane programmed.
    let installed = Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        gateways: Vec::new(),
        gateway_api: SERVED,
        ..waiting
    };
    assert!(
        operator
            .step(&api, &installed, &time(30), &said.say())
            .await,
        "{:?}",
        said.0
    );
    let asked = fake.seen()[before..].to_vec();
    let made: Vec<_> = writes(&asked)
        .into_iter()
        .filter(|write| write.contains("/gateway.networking.k8s.io/"))
        .collect();
    let (for_control_plane, for_data_plane) = made_of(&data_plane_with_gateway());
    let expected: Vec<String> = for_control_plane
        .iter()
        .chain(&for_data_plane)
        .filter(|object| matches!(object, Object::GatewayClass(_) | Object::Gateway(_)))
        .map(|object| format!("PATCH {}{APPLY}", path_of(object)))
        .collect();
    assert_eq!(made, expected);
    assert_eq!(
        status_written(&asked, DATA_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "True", "Programmed"),
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_plane_deleted_without_gateway_apis_crds_deletes_no_gateway_class() {
    // A cluster without the CRD answers a delete of GatewayClasses 404.
    let (fake, api) = cluster(|method, path| {
        (method == "DELETE" && path.contains("gatewayclasses"))
            .then(|| answer(404, r#"{"kind":"Status","code":404}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first_of(data_plane(), NO_GATEWAY_API, "1", 1);
    seen.control_planes[0].metadata.deletion_timestamp = Some(time(5));
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let of_its_kinds: Vec<_> = asked.iter().filter(|asked| of_gateway_api(asked)).collect();
    assert!(of_its_kinds.is_empty(), "{of_its_kinds:?}");
    assert!(
        writes(&asked).contains(&format!("PATCH {CONTROL_PLANE}")),
        "its finalizer not taken away: {:?}",
        said.0
    );
}

/// A `PlatformDefaults` named `default` letting the ControlPlanes of namespace `edge`
/// watch `namespaces`, as JSON.
fn allowing(namespaces: serde_json::Value) -> PlatformDefaults {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "PlatformDefaults",
        "metadata": { "name": "default" },
        "spec": { "watchScopes": [{
            "controlPlanes": { "namespaceSelector": { "matchLabels": { "kubernetes.io/metadata.name": "edge" } } },
            "namespaces": namespaces,
        }] },
    }))
    .unwrap()
}

/// Namespace `edge`, `pay` and `bill` of team `payments`, and `search` of team `search`,
/// each labelled with its name, as the API server labels them.
fn namespaces() -> BTreeMap<String, Labels> {
    [
        ("edge", None),
        ("pay", Some("payments")),
        ("bill", Some("payments")),
        ("search", Some("search")),
    ]
    .into_iter()
    .map(|(name, team)| {
        let mut labels =
            BTreeMap::from([("kubernetes.io/metadata.name".to_owned(), name.to_owned())]);
        if let Some(team) = team {
            labels.insert("team".to_owned(), team.to_owned());
        }
        (name.to_owned(), labels)
    })
    .collect()
}

/// The allowance of the namespaces of team `payments`.
fn payments() -> PlatformDefaults {
    allowing(serde_json::json!({
        "from": "Selector", "selector": { "matchLabels": { "team": "payments" } },
    }))
}

/// ControlPlane `edge` watching the namespaces of team `payments`.
fn selecting_payments() -> ControlPlane {
    let mut control_plane = control_plane("Same");
    control_plane.spec.watch.namespaces = serde_json::from_value(serde_json::json!({
        "from": "Selector", "selector": { "matchLabels": { "team": "payments" } },
    }))
    .unwrap();
    control_plane
}

/// Where each binding of `objects` is applied.
fn binding_paths(objects: &[Object]) -> BTreeSet<String> {
    objects
        .iter()
        .filter(|object| {
            matches!(
                object,
                Object::RoleBinding(_) | Object::ClusterRoleBinding(_)
            )
        })
        .map(path_of)
        .collect()
}

/// The paths of `asked`'s requests of `method`, without their query.
fn written(asked: &[Asked], method: &str) -> BTreeSet<String> {
    asked
        .iter()
        .filter(|asked| asked.method == method)
        .map(|asked| asked.path.split('?').next().unwrap().to_owned())
        .collect()
}

const ROLE_BINDINGS: &str = "/apis/rbac.authorization.k8s.io/v1/namespaces";
const CLUSTER_ROLE_BINDINGS: &str = "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings";

/// [`after_first`] with its ControlPlane `seen_as`, but everything of it and its DataPlane
/// made as `made_as` and `selected` had it, in a cluster whose namespaces are
/// [`namespaces`] and whose `PlatformDefaults` is `platform`.
fn made_then(
    made_as: &ControlPlane,
    selected: &BTreeSet<String>,
    seen_as: &ControlPlane,
    platform: Option<PlatformDefaults>,
) -> Seen {
    let mut seen = after_first("1", 1);
    let mut control_plane = seen_as.clone();
    control_plane.metadata = seen.control_planes[0].metadata.clone();
    seen.control_planes = vec![control_plane];
    let for_control_plane = objects::for_control_plane(made_as, selected, &install()).unwrap();
    let for_data_plane =
        objects::for_data_plane(&data_plane(), made_as, selected, &install()).unwrap();
    seen.children = children_of(&for_control_plane, "1");
    seen.children.extend(children_of(&for_data_plane, "1"));
    seen.platform_defaults = platform;
    seen.namespaces = namespaces();
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_of_all_allowed_is_bound_over_the_cluster_and_its_own_namespace_no_more() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let (same, all, none) = (control_plane("Same"), control_plane("All"), BTreeSet::new());
    let allowed = allowing(serde_json::json!({ "from": "All" }));
    let seen = made_then(&same, &none, &all, Some(allowed));
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let patched = written(&asked, "PATCH");
    for binding in [
        "edgerush-controller-edge",
        "edgerush-controller-edge-cluster",
        "edgerush-proxy-edge.web",
    ] {
        let path = format!("{CLUSTER_ROLE_BINDINGS}/{binding}");
        assert!(patched.contains(&path), "{path}: {:?}", writes(&asked));
    }
    // The bindings its scope had in its own namespace are no longer asked for: those of
    // its own role stay.
    let mut was = binding_paths(&objects::for_control_plane(&same, &none, &install()).unwrap());
    was.extend(binding_paths(
        &objects::for_data_plane(&data_plane(), &same, &none, &install()).unwrap(),
    ));
    let mut is = binding_paths(&objects::for_control_plane(&all, &none, &install()).unwrap());
    is.extend(binding_paths(
        &objects::for_data_plane(&data_plane(), &all, &none, &install()).unwrap(),
    ));
    let gone: BTreeSet<String> = was.difference(&is).cloned().collect();
    assert_eq!(
        gone,
        BTreeSet::from([
            format!("{ROLE_BINDINGS}/edge/rolebindings/edgerush-controller-edge"),
            format!("{ROLE_BINDINGS}/edge/rolebindings/edgerush-proxy-edge.web"),
        ])
    );
    assert_eq!(written(&asked, "DELETE"), gone);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selector_scope_allowed_is_bound_in_each_namespace_it_selects() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let selecting = selecting_payments();
    let seen = made_then(&selecting, &BTreeSet::new(), &selecting, Some(payments()));
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let patched = written(&asked, "PATCH");
    let role_binding =
        |namespace: &str, name: &str| format!("{ROLE_BINDINGS}/{namespace}/rolebindings/{name}");
    for namespace in ["pay", "bill"] {
        for name in ["edgerush-controller-edge", "edgerush-proxy-edge.web"] {
            let path = role_binding(namespace, name);
            assert!(patched.contains(&path), "{path}: {patched:?}");
        }
    }
    assert!(
        !patched
            .iter()
            .any(|path| path.contains("/namespaces/search/")),
        "{patched:?}"
    );
    // The controller is told the selector, which it follows itself, not the namespaces
    // selected, so that one labelled in or out rolls no controller (04 §1).
    let deployment = asked
        .iter()
        .find(|asked| {
            asked.method == "PATCH" && asked.path.contains("/deployments/edgerush-controller-edge")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&deployment.body).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"].to_string();
    assert!(
        args.contains(r#""--namespace-selector","team=payments""#),
        "{args}"
    );
    assert!(!args.contains(r#""--namespace""#), "{args}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_allowance_narrowed_takes_back_the_rights_it_granted_and_keeps_the_rest() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let all = control_plane("All");
    // Made while All was allowed; now no allowance is for it.
    let seen = made_then(&all, &BTreeSet::new(), &all, None);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(
        status_written(&asked, CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "False", "ScopeNotAllowed"),
            ("Programmed", "False", "Pending")
        ])
    );
    // The rights over the cluster its scope had, its controller's and its data plane's,
    // are deleted; the controller's right to the cluster's own kinds is not its scope's,
    // and stays, as does everything else.
    assert_eq!(
        written(&asked, "DELETE"),
        BTreeSet::from([
            format!("{CLUSTER_ROLE_BINDINGS}/edgerush-controller-edge"),
            format!("{CLUSTER_ROLE_BINDINGS}/edgerush-proxy-edge.web"),
        ]),
        "{:?}",
        said.0
    );
    assert!(
        written(&asked, "PATCH")
            .iter()
            .all(|path| path.ends_with("/status")),
        "nothing applied: {:?}",
        writes(&asked)
    );
    assert!(said.any("no longer granted"), "{:?}", said.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_namespace_no_longer_selected_has_the_bindings_in_it_deleted() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let selecting = selecting_payments();
    // Made while `search` was of team payments too.
    let then = BTreeSet::from(["bill".to_owned(), "pay".to_owned(), "search".to_owned()]);
    let seen = made_then(&selecting, &then, &selecting, Some(payments()));
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    assert_eq!(
        written(&fake.seen(), "DELETE"),
        BTreeSet::from([
            format!("{ROLE_BINDINGS}/search/rolebindings/edgerush-controller-edge"),
            format!("{ROLE_BINDINGS}/search/rolebindings/edgerush-proxy-edge.web"),
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_binding_that_cannot_be_deleted_is_said_and_tried_again() {
    let (_fake, api) = cluster(|method, _| {
        (method == "DELETE").then(|| answer(403, r#"{"kind":"Status","code":403}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let all = control_plane("All");
    let seen = made_then(&all, &BTreeSet::new(), &all, None);
    assert!(!operator.step(&api, &seen, &time(10), &said.say()).await);
    assert!(said.any("cannot be deleted"), "{:?}", said.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_namespace_newly_selected_has_its_bindings_made_with_nothing_else_changed() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let selecting = selecting_payments();
    let selected = BTreeSet::from(["bill".to_owned(), "pay".to_owned()]);
    let seen = made_then(&selecting, &selected, &selecting, Some(payments()));
    assert!(operator.step(&api, &seen, &time(10), &said.say()).await);
    // Applied once; then `search` labelled into team payments, and nothing else.
    let before = fake.seen().len();
    let mut labelled = seen.clone();
    labelled
        .namespaces
        .get_mut("search")
        .unwrap()
        .insert("team".to_owned(), "payments".to_owned());
    assert!(operator.step(&api, &labelled, &time(20), &said.say()).await);
    let patched = written(&fake.seen()[before..], "PATCH");
    let path = format!("{ROLE_BINDINGS}/search/rolebindings/edgerush-controller-edge");
    assert!(patched.contains(&path), "{patched:?}");
}

const REGISTRATION: &str = "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/edgerush-controller-edge";

/// The bundle the last apply of the webhook's registration trusts, decoded.
fn registered_bundle(asked: &[Asked]) -> Option<String> {
    let applied = asked
        .iter()
        .rev()
        .find(|asked| asked.method == "PATCH" && asked.path == format!("{REGISTRATION}{APPLY}"))?;
    let body: serde_json::Value = serde_json::from_str(&applied.body).unwrap();
    let encoded = body["webhooks"][0]["clientConfig"]["caBundle"].clone();
    let bytes: k8s_openapi::ByteString = serde_json::from_value(encoded).unwrap();
    Some(String::from_utf8(bytes.0).unwrap())
}

/// [`after_first`] as it was before its control plane published its bundle: nothing in its
/// status but its conditions, and no registration made.
fn before_bundle() -> Seen {
    let mut seen = after_first("1", 1);
    if let Some(status) = seen.control_planes[0].status.as_mut() {
        status.authority = None;
    }
    seen.children
        .retain(|child| child.kind != "ValidatingWebhookConfiguration");
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_waits_for_its_authority_s_bundle_then_is_registered_with_it() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let waiting = before_bundle();
    // Not a step left undone: the ControlPlane's follow says when its status changes.
    assert!(
        operator.step(&api, &waiting, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(registered_bundle(&asked), None, "{:?}", writes(&asked));
    assert!(
        !writes(&asked)
            .iter()
            .any(|write| write.contains("validatingwebhookconfigurations")),
        "{:?}",
        writes(&asked)
    );
    assert_eq!(
        status_written(&asked, CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "False", "Pending"),
        ])
    );
    assert_eq!(
        programmed_message(&asked, CONTROL_PLANE),
        "ValidatingWebhookConfiguration edgerush-controller-edge is not made until the control \
         plane publishes its authority's bundle in this status; the rest is made."
    );
    // The bundle published, its generation the same: registered, trusting it, and
    // programmed.
    let mut published_now = waiting.clone();
    let status = published_now.control_planes[0].status.as_mut().unwrap();
    status.conditions = Some(status::conditions(
        Some(1),
        Ok(()),
        &Made::Pending(programmed_message(&asked, CONTROL_PLANE)),
        "edgerush-controller-edge",
        true,
        &[],
        &time(10),
    ));
    published_now.control_planes[0] = published(published_now.control_planes[0].clone(), BUNDLE);
    let before = fake.seen().len();
    assert!(
        operator
            .step(&api, &published_now, &time(20), &said.say())
            .await,
        "{:?}",
        said.0
    );
    let asked = fake.seen()[before..].to_vec();
    assert_eq!(registered_bundle(&asked).as_deref(), Some(BUNDLE));
    assert_eq!(
        status_written(&asked, CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "True", "Programmed"),
        ])
    );
    // A later bundle, a root signed again or a provided one replaced, applied the same way.
    let again = format!("{BUNDLE}{BUNDLE}");
    let mut rotated = published_now.clone();
    rotated.control_planes[0] = published(rotated.control_planes[0].clone(), &again);
    let before = fake.seen().len();
    assert!(operator.step(&api, &rotated, &time(30), &said.say()).await);
    assert_eq!(
        registered_bundle(&fake.seen()[before..]).as_deref(),
        Some(again.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_webhook_turned_off_has_its_registration_deleted() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut seen = after_first("1", 1);
    seen.control_planes[0].spec.webhook.enabled = false;
    seen.control_planes[0].metadata.generation = Some(2);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(registered_bundle(&asked), None);
    assert_eq!(
        written(&asked, "DELETE"),
        BTreeSet::from([REGISTRATION.to_owned()])
    );
    assert!(
        said.any(
            "ValidatingWebhookConfiguration edgerush-controller-edge deleted, no longer asked for"
        ),
        "{:?}",
        said.0
    );
    // Programmed without it, as nothing waits.
    assert_eq!(
        status_written(&asked, CONTROL_PLANE),
        conditions_of(&[
            ("Accepted", "True", "Accepted"),
            ("Programmed", "True", "Programmed"),
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_plane_no_longer_accepted_has_its_registration_deleted() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    // Registered while All was allowed; now no allowance is for it, and its webhook would
    // be asked of namespaces it may no longer read.
    let all = published(control_plane("All"), BUNDLE);
    let seen = made_then(&all, &BTreeSet::new(), &all, None);
    assert!(
        seen.children
            .iter()
            .any(|child| child.kind == "ValidatingWebhookConfiguration")
    );
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let deleted = written(&fake.seen(), "DELETE");
    assert!(deleted.contains(REGISTRATION), "{deleted:?}");
}

const INGRESS_CLASS: &str = "/apis/networking.k8s.io/v1/ingressclasses/edge.web";
const GATEWAY: &str = "/apis/gateway.networking.k8s.io/v1/namespaces/edge/gateways/web";

/// [`after_first`] of [`data_plane_with_gateway`], its IngressClass and Gateway made, then
/// its DataPlane's next generation as `change` makes it.
fn served_both_then(change: impl FnOnce(&mut DataPlane)) -> Seen {
    let mut seen = after_first_of(data_plane_with_gateway(), SERVED, "1", 1);
    assert!(
        ["IngressClass", "Gateway"]
            .iter()
            .all(|kind| seen.children.iter().any(|child| child.kind == *kind))
    );
    let data_plane = &mut seen.data_planes[0];
    change(data_plane);
    data_plane.metadata.generation = Some(2);
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingress_no_longer_asked_for_has_its_ingress_class_deleted_and_the_rest_kept() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = served_both_then(|data_plane| data_plane.spec.ingress = None);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(
        written(&asked, "DELETE"),
        BTreeSet::from([INGRESS_CLASS.to_owned()])
    );
    assert!(
        said.any("IngressClass edge.web deleted, no longer asked for"),
        "{:?}",
        said.0
    );
    assert!(
        writes(&asked).contains(&format!("PATCH {GATEWAY}{APPLY}")),
        "{:?}",
        writes(&asked)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_no_longer_asked_for_is_deleted_and_the_rest_kept() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = served_both_then(|data_plane| data_plane.spec.gateway = None);
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(
        written(&asked, "DELETE"),
        BTreeSet::from([GATEWAY.to_owned()])
    );
    assert!(
        said.any("Gateway edge/web deleted, no longer asked for"),
        "{:?}",
        said.0
    );
    assert!(
        writes(&asked).contains(&format!("PATCH {INGRESS_CLASS}{APPLY}")),
        "{:?}",
        writes(&asked)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_that_cannot_be_deleted_is_said_and_tried_again() {
    let (fake, api) = cluster(|method, path| {
        (method == "DELETE" && path.starts_with(GATEWAY))
            .then(|| answer(500, r#"{"kind":"Status","code":500}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = served_both_then(|data_plane| data_plane.spec.gateway = None);
    assert!(!operator.step(&api, &seen, &time(10), &said.say()).await);
    assert!(
        said.any("Gateway edge/web cannot be deleted"),
        "{:?}",
        said.0
    );
    let before = fake.seen().len();
    assert!(!operator.step(&api, &seen, &time(20), &said.say()).await);
    assert!(
        written(&fake.seen()[before..], "DELETE").contains(GATEWAY),
        "not tried again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_of_a_control_plane_not_accepted_keeps_its_ingress_class_and_gateway() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    // Made while All was allowed; now no allowance is for it. Its rights beyond its own
    // namespace are taken back, and nothing else it asks for.
    let all = control_plane("All");
    let mut seen = made_then(&all, &BTreeSet::new(), &all, None);
    let data_plane = data_plane_with_gateway();
    let for_data_plane =
        objects::for_data_plane(&data_plane, &all, &BTreeSet::new(), &install()).unwrap();
    seen.children
        .retain(|child| !child.labels.contains_key(objects::DATA_PLANE_LABEL));
    seen.children.extend(children_of(&for_data_plane, "1"));
    seen.data_planes[0].spec = data_plane.spec;
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let deleted = written(&fake.seen(), "DELETE");
    // Its rights over the cluster taken back: this step withdraws.
    assert!(
        deleted
            .iter()
            .any(|path| path.starts_with(CLUSTER_ROLE_BINDINGS)),
        "{deleted:?}"
    );
    assert!(!deleted.contains(INGRESS_CLASS), "{deleted:?}");
    assert!(!deleted.contains(GATEWAY), "{deleted:?}");
}

const AUTOSCALER: &str =
    "/apis/autoscaling/v2/namespaces/edge/horizontalpodautoscalers/edgerush-proxy-web";
const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/edge/deployments/edgerush-proxy-web";

/// [`after_first`] of [`data_plane`] autoscaled from 2 to 5, its HorizontalPodAutoscaler
/// made, then its DataPlane's next generation with `replicas` as `change` makes them.
fn autoscaled_then(change: impl FnOnce(&mut DataPlaneReplicas)) -> Seen {
    let mut data_plane = data_plane();
    data_plane.spec.replicas = DataPlaneReplicas {
        min: 2,
        max: Some(5),
    };
    let mut seen = after_first_of(data_plane, SERVED, "1", 1);
    assert!(
        seen.children
            .iter()
            .any(|child| child.kind == "HorizontalPodAutoscaler")
    );
    let data_plane = &mut seen.data_planes[0];
    change(&mut data_plane.spec.replicas);
    data_plane.metadata.generation = Some(2);
    seen
}

/// The replicas the last apply of the proxy's Deployment in `asked` states.
fn deployment_replicas(asked: &[Asked]) -> Option<serde_json::Value> {
    let applied = asked
        .iter()
        .rev()
        .find(|asked| asked.method == "PATCH" && asked.path.starts_with(DEPLOYMENT))
        .expect("the Deployment applied");
    let body: serde_json::Value = serde_json::from_str(&applied.body).unwrap();
    body["spec"].get("replicas").cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_no_longer_autoscaled_has_its_autoscaler_deleted() {
    // Its `max` taken away, or made `min`: either is a fixed count.
    for (case, change) in [
        (
            "max taken away",
            (|replicas| replicas.max = None) as fn(&mut DataPlaneReplicas),
        ),
        ("max made min", |replicas| {
            replicas.min = 3;
            replicas.max = Some(3);
        }),
    ] {
        let (fake, api) = cluster(|_, _| None).await;
        let said = Said::default();
        let mut operator = Operator::new(install());
        let seen = autoscaled_then(change);
        assert!(
            operator.step(&api, &seen, &time(10), &said.say()).await,
            "{case}: {:?}",
            said.0
        );
        let asked = fake.seen();
        assert_eq!(
            written(&asked, "DELETE"),
            BTreeSet::from([AUTOSCALER.to_owned()]),
            "{case}"
        );
        assert!(
            said.any(
                "HorizontalPodAutoscaler edge/edgerush-proxy-web deleted, no longer asked for"
            ),
            "{case}: {:?}",
            said.0
        );
        // The Deployment states its count again, which no autoscaler now undoes.
        let min = seen.data_planes[0].spec.replicas.min;
        assert_eq!(
            deployment_replicas(&asked),
            Some(serde_json::json!(min)),
            "{case}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_still_autoscaled_keeps_its_autoscaler() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = autoscaled_then(|replicas| replicas.max = Some(8));
    assert!(
        operator.step(&api, &seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    assert_eq!(written(&asked, "DELETE"), BTreeSet::new());
    assert!(
        writes(&asked).contains(&format!("PATCH {AUTOSCALER}{APPLY}")),
        "{:?}",
        writes(&asked)
    );
    assert_eq!(deployment_replicas(&asked), None);
}

const HANDED_OVER: &str = "?fieldManager=edgerush-operator-handover&force=true";

/// [`after_first`] of [`data_plane`] at a fixed 2 replicas, then its DataPlane's next
/// generation with `replicas` as `change` makes them.
fn fixed_then(change: impl FnOnce(&mut DataPlaneReplicas)) -> Seen {
    let mut data_plane = data_plane();
    data_plane.spec.replicas = DataPlaneReplicas { min: 2, max: None };
    let mut seen = after_first_of(data_plane, SERVED, "1", 2);
    let data_plane = &mut seen.data_planes[0];
    change(&mut data_plane.spec.replicas);
    data_plane.metadata.generation = Some(2);
    seen
}

/// The proxy's Deployment as the API server has it, at 2 replicas, its fields held as
/// `managed` says.
fn deployment_held(managed: serde_json::Value) -> String {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "edgerush-proxy-web",
            "namespace": "edge",
            "ownerReferences": [{
                "apiVersion": "gateway.edgerush.dev/v1alpha1",
                "kind": "DataPlane",
                "name": "web",
                "uid": "dp-uid",
            }],
            "managedFields": managed,
        },
        "spec": { "replicas": 2, "selector": {}, "template": {} },
    })
    .to_string()
}

/// A `managedFields` entry: `manager`'s `operation`, holding `fields` of the spec.
fn held_by(manager: &str, operation: &str, fields: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "manager": manager,
        "operation": operation,
        "apiVersion": "apps/v1",
        "fieldsType": "FieldsV1",
        "fieldsV1": { "f:spec": fields },
    })
}

/// The operator's own apply, holding the replicas and the template.
fn operator_holding_replicas() -> serde_json::Value {
    held_by(
        "edgerush-operator",
        "Apply",
        serde_json::json!({ "f:replicas": {}, "f:template": {} }),
    )
}

/// What a step of `seen` writes, of a cluster whose Deployment is `there`.
async fn step_over(seen: &Seen, there: String) -> Vec<Asked> {
    let (fake, api) = cluster(move |method, path| {
        (method == "GET" && path == DEPLOYMENT).then(|| answer(200, &there))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(
        operator.step(&api, seen, &time(10), &said.say()).await,
        "{:?}",
        said.0
    );
    fake.seen()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_made_autoscaled_hands_its_replicas_over_before_leaving_them_out() {
    let seen = fixed_then(|replicas| replicas.max = Some(5));
    let asked = step_over(
        &seen,
        deployment_held(serde_json::json!([operator_holding_replicas()])),
    )
    .await;
    let written = writes(&asked);
    let handover = format!("PATCH {DEPLOYMENT}{HANDED_OVER}");
    let applied = format!("PATCH {DEPLOYMENT}{APPLY}");
    let at = |write: &str| written.iter().position(|written| written == write);
    let (Some(handed), Some(then)) = (at(&handover), at(&applied)) else {
        panic!("no handover then apply: {written:?}");
    };
    assert!(handed < then, "{written:?}");
    // The count it is at, and nothing else of it.
    let body: serde_json::Value = serde_json::from_str(
        &asked
            .iter()
            .find(|asked| format!("{} {}", asked.method, asked.path) == handover)
            .unwrap()
            .body,
    )
    .unwrap();
    assert_eq!(
        body,
        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "edgerush-proxy-web", "namespace": "edge" },
            "spec": { "replicas": 2 },
        })
    );
    assert_eq!(deployment_replicas(&asked), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicas_the_operator_does_not_leave_out_or_no_longer_holds_are_not_handed_over() {
    for (case, seen, managed) in [
        (
            "still a fixed count",
            fixed_then(|replicas| replicas.min = 3),
            serde_json::json!([operator_holding_replicas()]),
        ),
        (
            "the autoscaler's already",
            autoscaled_then(|replicas| replicas.max = Some(8)),
            serde_json::json!([
                held_by(
                    "edgerush-operator",
                    "Apply",
                    serde_json::json!({ "f:template": {} })
                ),
                held_by(
                    "kube-controller-manager",
                    "Update",
                    serde_json::json!({ "f:replicas": {} })
                ),
            ]),
        ),
        (
            "handed over already",
            autoscaled_then(|replicas| replicas.max = Some(8)),
            serde_json::json!([
                held_by(
                    "edgerush-operator",
                    "Apply",
                    serde_json::json!({ "f:template": {} })
                ),
                held_by(
                    "edgerush-operator-handover",
                    "Apply",
                    serde_json::json!({ "f:replicas": {} })
                ),
            ]),
        ),
    ] {
        let asked = step_over(&seen, deployment_held(managed)).await;
        let written = writes(&asked);
        assert!(
            written.contains(&format!("PATCH {DEPLOYMENT}{APPLY}")),
            "{case}: {written:?}"
        );
        assert!(
            !written.contains(&format!("PATCH {DEPLOYMENT}{HANDED_OVER}")),
            "{case}: {written:?}"
        );
    }
}

/// GatewayClass `name` of generation `generation`, naming `controller`, holding nothing in
/// its status.
fn gateway_class(name: &str, controller: &str, generation: i64) -> GatewayClass {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "GatewayClass",
        "metadata": { "name": name, "generation": generation },
        "spec": { "controllerName": controller },
    }))
    .unwrap()
}

const SUITE_CLASS: &str =
    "/apis/gateway.networking.k8s.io/v1/gatewayclasses/gatewayclass-observed-generation-bump";

/// A cluster of ControlPlane `edge` alone, with three classes: the conformance suite's,
/// naming EdgeRush's controller; `edge`'s, made for it; and another controller's.
fn three_classes() -> Seen {
    Seen {
        platform_defaults: None,
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("Same")],
        data_planes: Vec::new(),
        children: Vec::new(),
        gateway_classes: vec![
            gateway_class(
                "gatewayclass-observed-generation-bump",
                "edgerush.dev/gateway-controller",
                1,
            ),
            {
                let mut edge = gateway_class("edge", "edgerush.dev/gateway-controller", 1);
                edge.metadata.labels = Some(control_plane_labels("edge", "edge"));
                edge
            },
            gateway_class("other", "example.com/gateway-controller", 1),
        ],
        gateways: Vec::new(),
        gateway_api: SERVED,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_class_no_control_plane_made_is_refused_and_no_other_written() {
    let (fake, api) = cluster(|_, _| None).await;
    let seen = three_classes();
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(
        operator.step(&api, &seen, &time(0), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let classes: Vec<String> = writes(&asked)
        .into_iter()
        .filter(|write| write.contains("/gatewayclasses/") && write.contains("/status"))
        .collect();
    assert_eq!(classes, [format!("PATCH {SUITE_CLASS}/status{APPLY}")]);
    assert_eq!(
        status_written(&asked, SUITE_CLASS),
        conditions_of(&[("Accepted", "False", "Unsupported")])
    );
    let written = asked
        .iter()
        .rfind(|asked| asked.path == format!("{SUITE_CLASS}/status{APPLY}"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&written.body).unwrap();
    assert_eq!(body["status"]["conditions"][0]["observedGeneration"], 1);
    // Its conditions alone: the supported features are another's to write.
    assert!(body["status"].get("supportedFeatures").is_none());
    assert!(said.any("GatewayClass gatewayclass-observed-generation-bump: Accepted False"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_class_refused_is_written_again_only_as_its_generation_changes() {
    let (fake, api) = cluster(|_, _| None).await;
    let mut seen = three_classes();
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(operator.step(&api, &seen, &time(0), &said.say()).await);
    let refusal = classes::accepted(&seen.gateway_classes[0], &time(0));
    seen.gateway_classes[0]
        .status
        .get_or_insert_default()
        .conditions = Some(vec![refusal]);
    let status_writes = |asked: &[Asked]| {
        asked
            .iter()
            .filter(|asked| asked.path == format!("{SUITE_CLASS}/status{APPLY}"))
            .count()
    };
    let before = status_writes(&fake.seen());

    // Its status says it: nothing to write.
    assert!(operator.step(&api, &seen, &time(10), &said.say()).await);
    assert_eq!(status_writes(&fake.seen()), before);

    // Edited: said of its new generation.
    seen.gateway_classes[0].metadata.generation = Some(2);
    assert!(operator.step(&api, &seen, &time(20), &said.say()).await);
    let asked = fake.seen();
    assert_eq!(status_writes(&asked), before + 1);
    let written = asked
        .iter()
        .rfind(|asked| asked.path == format!("{SUITE_CLASS}/status{APPLY}"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&written.body).unwrap();
    assert_eq!(body["status"]["conditions"][0]["observedGeneration"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_that_cannot_be_written_is_a_step_left_undone() {
    let (_fake, api) = cluster(|method, path| {
        (method == "PATCH" && path.starts_with(&format!("{SUITE_CLASS}/status")))
            .then(|| answer(500, r#"{"kind":"Status","code":500}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(
        !operator
            .step(&api, &three_classes(), &time(0), &said.say())
            .await
    );
    assert!(
        said.any(
            "GatewayClass gatewayclass-observed-generation-bump: its status cannot be written"
        )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_class_gone_before_its_refusal_is_written_is_nothing_left_undone() {
    let (_fake, api) = cluster(|method, path| {
        (method == "PATCH" && path.starts_with(&format!("{SUITE_CLASS}/status")))
            .then(|| answer(404, r#"{"kind":"Status","code":404}"#))
    })
    .await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    assert!(
        operator
            .step(&api, &three_classes(), &time(0), &said.say())
            .await,
        "{:?}",
        said.0
    );
}

#[test]
fn the_class_the_operator_makes_is_never_refused() {
    let (for_control_plane, _) = made();
    let class = for_control_plane
        .iter()
        .find(|object| object.kind() == "GatewayClass")
        .unwrap();
    let class: GatewayClass = serde_json::from_value(serde_json::to_value(class).unwrap()).unwrap();
    assert_eq!(
        class.spec.controller_name,
        "edgerush.dev/gateway-controller"
    );
    assert!(!classes::refused(&class, &[control_plane("Same")]));
}

/// The class the operator made for [`control_plane`], as followed.
fn made_class() -> GatewayClass {
    let (for_control_plane, _) = made();
    let class = for_control_plane
        .iter()
        .find(|object| object.kind() == "GatewayClass")
        .unwrap();
    serde_json::from_value(serde_json::to_value(class).unwrap()).unwrap()
}

/// Gateway `web` of namespace `edge`, written alone, of the class of [`control_plane`],
/// with `infrastructure`.
fn gateway_alone(infrastructure: serde_json::Value) -> Gateway {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": "web", "namespace": "edge", "uid": "gw-uid", "generation": 2 },
        "spec": {
            "gatewayClassName": "edge",
            "infrastructure": infrastructure,
            "listeners": [{ "name": "http", "port": 80, "protocol": "HTTP" }],
        },
    }))
    .unwrap()
}

/// What a step sees of [`control_plane`] with its class, an entry for it, and `gateways`
/// beside `data_planes`.
fn gateway_first_seen(gateways: Vec<Gateway>, data_planes: Vec<DataPlane>) -> Seen {
    Seen {
        platform_defaults: Some(
            serde_json::from_value(serde_json::json!({
                "metadata": { "name": "default" },
                "spec": { "gatewayClasses": [{ "name": "edge", "dataPlane": {} }] },
            }))
            .unwrap(),
        ),
        namespaces: BTreeMap::new(),
        control_planes: vec![control_plane("Same")],
        data_planes,
        children: Vec::new(),
        gateway_classes: vec![made_class()],
        gateways,
        gateway_api: SERVED,
    }
}

const GENERATED: &str = "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/dataplanes/web-edge";

/// What a generated DataPlane is applied by: a field manager of its own, apart from the
/// finalizer's, whose apply of the metadata alone would otherwise take its spec away.
const GENERATOR_APPLY: &str = "?fieldManager=edgerush-operator-generator&force=true";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_written_alone_gets_its_data_plane_applied_once() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let seen = gateway_first_seen(vec![gateway_alone(serde_json::json!({}))], Vec::new());
    assert!(
        operator.step(&api, &seen, &time(0), &said.say()).await,
        "{:?}",
        said.0
    );
    let asked = fake.seen();
    let applied = asked
        .iter()
        .find(|asked| {
            asked.method == "PATCH" && asked.path == format!("{GENERATED}{GENERATOR_APPLY}")
        })
        .unwrap_or_else(|| panic!("not applied: {:?}", writes(&asked)));
    let body: serde_json::Value = serde_json::from_str(&applied.body).unwrap();
    let wanted = edgerush_planes::gateway_first::data_plane(
        &seen.gateways[0],
        &serde_json::from_value(serde_json::json!({})).unwrap(),
        &seen.control_planes[0],
    )
    .unwrap();
    assert_eq!(
        body,
        serde_json::to_value(edgerush_planes::gateway_first::applied(&wanted)).unwrap()
    );
    assert!(
        said.any("DataPlane edge/web-edge: generated for Gateway edge/web"),
        "{:?}",
        said.0
    );
    // Nothing of it changed: not applied again.
    let (fake, api) = cluster(|_, _| None).await;
    assert!(operator.step(&api, &seen, &time(1), &said.say()).await);
    assert!(
        !fake
            .seen()
            .iter()
            .any(|asked| asked.path.starts_with(GENERATED) && asked.method == "PATCH"),
        "applied again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_of_the_name_its_gateway_does_not_own_is_left_alone() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let mut theirs = data_plane();
    theirs.metadata.name = Some("web-edge".to_owned());
    let seen = gateway_first_seen(vec![gateway_alone(serde_json::json!({}))], vec![theirs]);
    assert!(!operator.step(&api, &seen, &time(0), &said.say()).await);
    assert!(
        !fake
            .seen()
            .iter()
            .any(|asked| asked.path.starts_with(GENERATED) && asked.body.contains("gatewayRef")),
        "taken over"
    );
    assert!(
        said.any("DataPlane edge/web-edge: not generated for Gateway edge/web"),
        "{:?}",
        said.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_with_parameters_gets_no_data_plane_and_is_told_why() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let infrastructure = serde_json::json!({
        "parametersRef": { "group": "", "kind": "ConfigMap", "name": "params" },
    });
    let seen = gateway_first_seen(vec![gateway_alone(infrastructure)], Vec::new());
    assert!(operator.step(&api, &seen, &time(0), &said.say()).await);
    let asked = fake.seen();
    assert!(
        !asked.iter().any(|asked| asked.path.starts_with(GENERATED)),
        "a DataPlane for it: {:?}",
        writes(&asked)
    );
    assert_eq!(
        status_written(&asked, GATEWAY),
        conditions_of(&[
            ("Accepted", "False", "InvalidParameters"),
            ("Programmed", "False", "InvalidParameters"),
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_generated_data_plane_whose_gateway_moved_class_is_deleted() {
    let (fake, api) = cluster(|_, _| None).await;
    let said = Said::default();
    let mut operator = Operator::new(install());
    let gateway = gateway_alone(serde_json::json!({}));
    let mut generated = edgerush_planes::gateway_first::data_plane(
        &gateway,
        &serde_json::from_value(serde_json::json!({})).unwrap(),
        &control_plane("Same"),
    )
    .unwrap();
    generated.metadata.uid = Some("generated-uid".to_owned());
    let mut moved = gateway;
    moved.spec.gateway_class_name = "other".to_owned();
    let seen = gateway_first_seen(vec![moved], vec![generated]);
    operator.step(&api, &seen, &time(0), &said.say()).await;
    assert!(
        writes(&fake.seen())
            .iter()
            .any(|write| write.starts_with(&format!("DELETE {GENERATED}?"))),
        "{:?}",
        said.0
    );
}
