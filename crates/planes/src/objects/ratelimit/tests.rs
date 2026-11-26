#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "tests: a fixture that cannot be made is a broken test"
)]

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PodSpec, Service};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::super::{Error, Install, Object, control_plane_labels, for_control_plane};
use super::{
    CONFIG_DIGEST_ANNOTATION, RATELIMIT_PORT, rate_limiting_labels, ratelimit_identity_secret,
    ratelimit_name, ratelimit_peers,
};
use crate::control_plane::ControlPlane;

fn install() -> Install {
    Install {
        cluster_domain: "cluster.local".to_owned(),
        image: "edgerush:test".to_owned(),
    }
}

/// ControlPlane `payments` of namespace `edge`, its `rateLimiting` as `rate_limiting`
/// says, every default filled in as the API server would.
fn control_plane(rate_limiting: Option<serde_json::Value>) -> ControlPlane {
    let mut spec = serde_json::json!({ "watch": { "namespaces": { "from": "Same" } } });
    if let Some(rate_limiting) = rate_limiting {
        spec["rateLimiting"] = rate_limiting;
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "ControlPlane",
        "metadata": { "name": "payments", "namespace": "edge", "uid": "uid-of-payments" },
        "spec": spec,
    }))
    .unwrap()
}

/// [`control_plane`] declaring the service `service`.
fn declaring(service: serde_json::Value) -> ControlPlane {
    control_plane(Some(serde_json::json!({ "service": service })))
}

fn objects(control_plane: &ControlPlane) -> Vec<Object> {
    for_control_plane(control_plane, &BTreeSet::new(), &install()).unwrap()
}

/// Those of `objects` made for the rate-limiting service, as `(kind, name)`.
fn of_the_service(objects: &[Object]) -> BTreeSet<(&'static str, String)> {
    let labels = rate_limiting_labels("edge", "payments");
    objects
        .iter()
        .filter(|object| {
            let held = object.metadata().labels.clone().unwrap_or_default();
            labels
                .iter()
                .all(|(key, value)| held.get(key) == Some(value))
        })
        .map(|object| (object.kind(), object.metadata().name.clone().unwrap()))
        .collect()
}

fn deployment<'a>(objects: &'a [Object], name: &str) -> &'a Deployment {
    objects
        .iter()
        .find_map(|object| match object {
            Object::Deployment(deployment) if deployment.metadata.name.as_deref() == Some(name) => {
                Some(&**deployment)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no Deployment {name}"))
}

fn pod(objects: &[Object]) -> &PodSpec {
    deployment(objects, "edgerush-ratelimit-payments")
        .spec
        .template
        .spec
        .as_ref()
        .unwrap()
}

fn service<'a>(objects: &'a [Object], name: &str) -> &'a Service {
    objects
        .iter()
        .find_map(|object| match object {
            Object::Service(service) if service.metadata.name.as_deref() == Some(name) => {
                Some(&**service)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no Service {name}"))
}

/// The config file the service's ConfigMap holds, as text.
fn file_text(objects: &[Object]) -> String {
    objects
        .iter()
        .find_map(|object| match object {
            Object::ConfigMap(config_map) => config_map.data.as_ref()?.get("service.yaml").cloned(),
            _ => None,
        })
        .unwrap()
}

/// The config file, read back.
fn file(objects: &[Object]) -> serde_json::Value {
    serde_saphyr::from_str(&file_text(objects)).unwrap()
}

fn digest_of(objects: &[Object]) -> String {
    deployment(objects, "edgerush-ratelimit-payments")
        .spec
        .template
        .metadata
        .as_ref()
        .unwrap()
        .annotations
        .as_ref()
        .unwrap()[CONFIG_DIGEST_ANNOTATION]
        .clone()
}

#[test]
fn a_control_plane_declaring_no_service_makes_none() {
    for rate_limiting in [None, Some(serde_json::json!({}))] {
        let made = objects(&control_plane(rate_limiting));
        assert_eq!(of_the_service(&made), BTreeSet::new());
        assert!(
            !made
                .iter()
                .any(|object| matches!(object, Object::ConfigMap(_)))
        );
    }
}

#[test]
fn a_service_declared_has_its_objects_made_named_after_its_control_plane() {
    let made = objects(&declaring(serde_json::json!({})));
    let name = "edgerush-ratelimit-payments".to_owned();
    assert_eq!(ratelimit_name("payments"), name);
    assert_eq!(
        of_the_service(&made),
        BTreeSet::from([
            ("ServiceAccount", name.clone()),
            ("ConfigMap", name.clone()),
            ("Deployment", name.clone()),
            ("PodDisruptionBudget", name.clone()),
            ("Service", name.clone()),
            ("Service", ratelimit_peers("payments")),
        ])
    );
    for object in made
        .iter()
        .filter(|object| of_the_service(std::slice::from_ref(object)).len() == 1)
    {
        let metadata = object.metadata();
        assert_eq!(metadata.namespace.as_deref(), Some("edge"));
        let owners = metadata.owner_references.as_ref().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(
            (owners[0].kind.as_str(), owners[0].uid.as_str()),
            ("ControlPlane", "uid-of-payments")
        );
    }
}

#[test]
fn its_pods_are_none_of_the_controllers_and_the_controllers_none_of_its() {
    let made = objects(&declaring(serde_json::json!({})));
    let selects = |deployment: &Deployment, pod_labels: &BTreeMap<String, String>| {
        deployment
            .spec
            .selector
            .match_labels
            .as_ref()
            .unwrap()
            .iter()
            .all(|(key, value)| pod_labels.get(key) == Some(value))
    };
    let labels_of = |deployment: &Deployment| {
        deployment
            .spec
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .clone()
            .unwrap()
    };
    let controller = deployment(&made, "edgerush-controller-payments");
    let service = deployment(&made, "edgerush-ratelimit-payments");
    assert!(selects(controller, &labels_of(controller)));
    assert!(selects(service, &labels_of(service)));
    assert!(!selects(controller, &labels_of(service)));
    assert!(!selects(service, &labels_of(controller)));
    assert_ne!(
        rate_limiting_labels("edge", "payments"),
        control_plane_labels("edge", "payments")
    );
}

#[test]
fn its_file_is_what_the_control_plane_declares_with_no_pod_address() {
    let made = objects(&declaring(serde_json::json!({
        "shards": 64, "copies": 1, "forwarderDecides": false, "maxKeyBytes": 1_073_741_824_i64,
    })));
    assert_eq!(
        file(&made),
        serde_json::json!({
            "pod": { "listen": "0.0.0.0:7000", "metrics": "0.0.0.0:9090" },
            "store": {
                "seeds": ["edgerush-ratelimit-payments-peers.edge.svc:7000"],
                "shards": 64,
                "replicas": 1,
                "forwarder_decides": false,
            },
            "keys": { "max_bytes": 1_073_741_824_i64 },
            "tls": {
                "certificate": "/etc/edgerush/identity/tls.crt",
                "key": "/etc/edgerush/identity/tls.key",
                "authorities": "/etc/edgerush/identity/ca.crt",
                "trust_domain": "payments.cluster.local",
            },
        })
    );
}

#[test]
fn a_cap_not_declared_is_half_the_memory_limit() {
    let cap_of = |memory: serde_json::Value| {
        let made = objects(&declaring(serde_json::json!({
            "resources": { "requests": { "memory": "1Gi" }, "limits": { "memory": memory } },
        })));
        file(&made)["keys"]["max_bytes"].as_i64().unwrap()
    };
    assert_eq!(cap_of(serde_json::json!("16Gi")), 8_589_934_592);
    assert_eq!(cap_of(serde_json::json!("1G")), 500_000_000);
    assert_eq!(cap_of(serde_json::json!(1001)), 500);
    // An integer over 32 bits, as the schema lets through.
    assert_eq!(cap_of(serde_json::json!(17_179_869_184_u64)), 8_589_934_592);
    // Never none: the file takes no cap under one byte.
    assert_eq!(cap_of(serde_json::json!(1)), 1);
}

#[test]
fn a_cap_declared_is_kept_whatever_the_memory_limit() {
    let made = objects(&declaring(serde_json::json!({
        "resources": { "limits": { "memory": "16Gi" } }, "maxKeyBytes": 1_048_576,
    })));
    assert_eq!(
        file(&made)["keys"],
        serde_json::json!({ "max_bytes": 1_048_576 })
    );
}

#[test]
fn a_memory_limit_not_read_makes_nothing() {
    let control_plane = declaring(serde_json::json!({
        "resources": { "limits": { "memory": "lots" } },
    }));
    assert_eq!(
        for_control_plane(&control_plane, &BTreeSet::new(), &install()),
        Err(Error::Memory("lots".to_owned()))
    );
}

#[test]
fn with_no_cap_and_no_memory_limit_the_files_own_is_kept() {
    // A memory request is no limit: the pod may use more.
    for service in [
        serde_json::json!({}),
        serde_json::json!({ "resources": { "requests": { "memory": "1Gi" } } }),
    ] {
        let made = objects(&declaring(service));
        let file = file(&made);
        assert_eq!(file.get("keys"), None, "{file}");
    }
}

#[test]
fn the_crds_defaults_are_written_as_the_api_server_fills_them_in() {
    let made = objects(&declaring(serde_json::json!({})));
    let file = file(&made);
    // The CRD's defaults, as the API server fills them in.
    assert_eq!(
        file["store"],
        serde_json::json!({
            "seeds": ["edgerush-ratelimit-payments-peers.edge.svc:7000"],
            "shards": 256,
            "replicas": 2,
            "forwarder_decides": true,
        })
    );
}

#[test]
fn its_deployment_runs_the_service_binary_on_its_file_at_its_own_address() {
    let made = objects(&declaring(serde_json::json!({
        "replicas": 5,
        "resources": { "requests": { "cpu": "2" }, "limits": { "memory": "16Gi" } },
    })));
    let deployment = deployment(&made, "edgerush-ratelimit-payments");
    assert_eq!(deployment.spec.replicas, Some(5));
    let pod = pod(&made);
    let container = &pod.containers[0];
    assert_eq!(container.image.as_deref(), Some("edgerush:test"));
    assert_eq!(
        container.command.as_deref(),
        Some(&["edgerush-ratelimit-service".to_owned()][..])
    );
    assert_eq!(
        container.args.as_deref(),
        Some(
            &[
                "--config".to_owned(),
                "/etc/edgerush/ratelimit/service.yaml".to_owned()
            ][..]
        )
    );
    let env = &container.env.as_ref().unwrap()[0];
    assert_eq!(env.name, "POD_IP");
    assert_eq!(
        env.value_from
            .as_ref()
            .unwrap()
            .field_ref
            .as_ref()
            .unwrap()
            .field_path,
        "status.podIP"
    );
    let resources = container.resources.as_ref().unwrap();
    assert_eq!(resources.requests.as_ref().unwrap()["cpu"].0, "2");
    assert_eq!(resources.limits.as_ref().unwrap()["memory"].0, "16Gi");
    // Its file from its ConfigMap, its certificate from the Secret the control plane writes.
    let volumes: BTreeMap<String, String> = pod
        .volumes
        .as_ref()
        .unwrap()
        .iter()
        .map(|volume| {
            let source = match (&volume.config_map, &volume.secret) {
                (Some(config_map), None) => format!("ConfigMap {}", config_map.name),
                (None, Some(secret)) => {
                    format!("Secret {}", secret.secret_name.as_deref().unwrap())
                }
                _ => panic!("{volume:?}"),
            };
            (volume.name.clone(), source)
        })
        .collect();
    assert_eq!(
        volumes,
        BTreeMap::from([
            (
                "config".to_owned(),
                "ConfigMap edgerush-ratelimit-payments".to_owned()
            ),
            (
                "identity".to_owned(),
                format!("Secret {}", ratelimit_identity_secret("payments"))
            ),
        ])
    );
    assert_eq!(
        pod.service_account_name.as_deref(),
        Some("edgerush-ratelimit-payments")
    );
    assert_eq!(pod.automount_service_account_token, Some(false));
}

#[test]
fn its_pods_are_ready_once_they_serve_and_rolled_one_at_a_time() {
    let made = objects(&declaring(serde_json::json!({})));
    let probe = pod(&made).containers[0]
        .readiness_probe
        .as_ref()
        .unwrap()
        .http_get
        .as_ref()
        .unwrap();
    assert_eq!(probe.path.as_deref(), Some("/ready"));
    assert_eq!(probe.port, IntOrString::String("metrics".to_owned()));
    let rolling = deployment(&made, "edgerush-ratelimit-payments")
        .spec
        .strategy
        .as_ref()
        .unwrap()
        .rolling_update
        .as_ref()
        .unwrap();
    assert_eq!(rolling.max_surge, Some(IntOrString::Int(1)));
    assert_eq!(rolling.max_unavailable, Some(IntOrString::Int(0)));
    let budget = made
        .iter()
        .find_map(|object| match object {
            Object::PodDisruptionBudget(budget)
                if budget.metadata.name.as_deref() == Some("edgerush-ratelimit-payments") =>
            {
                budget.spec.as_ref()
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(budget.max_unavailable, Some(IntOrString::Int(1)));
    assert_eq!(
        budget.selector.as_ref().unwrap().match_labels,
        Some(rate_limiting_labels("edge", "payments"))
    );
}

#[test]
fn its_pods_leave_a_node_gone_after_thirty_seconds_spread_over_zones_and_nodes() {
    let made = objects(&declaring(serde_json::json!({})));
    let pod = pod(&made);
    let tolerations: Vec<(String, String, String, i64)> = pod
        .tolerations
        .as_ref()
        .unwrap()
        .iter()
        .map(|toleration| {
            (
                toleration.key.clone().unwrap(),
                toleration.operator.clone().unwrap(),
                toleration.effect.clone().unwrap(),
                toleration.toleration_seconds.unwrap(),
            )
        })
        .collect();
    let gone = |key: &str| {
        (
            key.to_owned(),
            "Exists".to_owned(),
            "NoExecute".to_owned(),
            30,
        )
    };
    assert_eq!(
        tolerations,
        [
            gone("node.kubernetes.io/not-ready"),
            gone("node.kubernetes.io/unreachable")
        ]
    );
    let spread: Vec<(&str, &str)> = pod
        .topology_spread_constraints
        .as_ref()
        .unwrap()
        .iter()
        .map(|spread| {
            assert_eq!(spread.max_skew, 1);
            assert_eq!(
                spread.label_selector.as_ref().unwrap().match_labels,
                Some(rate_limiting_labels("edge", "payments"))
            );
            (
                spread.topology_key.as_str(),
                spread.when_unsatisfiable.as_str(),
            )
        })
        .collect();
    assert_eq!(
        spread,
        [
            ("topology.kubernetes.io/zone", "ScheduleAnyway"),
            ("kubernetes.io/hostname", "ScheduleAnyway"),
        ]
    );
}

#[test]
fn its_pods_find_each_other_ready_or_not_and_data_planes_reach_the_ready_alone() {
    let made = objects(&declaring(serde_json::json!({})));
    let labels = rate_limiting_labels("edge", "payments");
    for (name, headless) in [
        ("edgerush-ratelimit-payments-peers", true),
        ("edgerush-ratelimit-payments", false),
    ] {
        let spec = service(&made, name).spec.as_ref().unwrap();
        assert_eq!(spec.selector.as_ref(), Some(&labels), "{name}");
        let ports = spec.ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1, "{name}");
        assert_eq!(ports[0].port, RATELIMIT_PORT, "{name}");
        assert_eq!(
            ports[0].target_port,
            Some(IntOrString::String("store".to_owned()))
        );
        assert_eq!(
            spec.cluster_ip.as_deref(),
            headless.then_some("None"),
            "{name}"
        );
        assert_eq!(
            spec.publish_not_ready_addresses,
            headless.then_some(true),
            "{name}"
        );
    }
}

#[test]
fn its_pods_roll_when_their_file_changes_and_only_then() {
    let at = |service: serde_json::Value| digest_of(&objects(&declaring(service)));
    let before = at(serde_json::json!({}));
    assert_eq!(before.len(), 16, "{before}");
    assert_eq!(
        at(serde_json::json!({})),
        before,
        "the same file, the same digest"
    );
    // Not in the file: the Deployment's own fields roll what they roll.
    assert_eq!(at(serde_json::json!({ "replicas": 5 })), before);
    for changed in [
        serde_json::json!({ "copies": 1 }),
        serde_json::json!({ "forwarderDecides": false }),
        serde_json::json!({ "maxKeyBytes": 1_000_000 }),
    ] {
        assert_ne!(at(changed.clone()), before, "{changed}");
    }
}

#[test]
fn the_digest_is_fnv_1a() {
    // The published test vectors of 64-bit FNV-1a: the same from one build to the next.
    assert_eq!(super::digest(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(super::digest(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(super::digest(b"foobar"), 0x8594_4171_f739_67e8);
}
