use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::{Error, Install, Object, ProxyService, Transports, for_control_plane, for_data_plane};
use crate::control_plane::ControlPlane;
use crate::data_plane::{DataPlane, DataPlaneIngress, DataPlaneResources};

fn install() -> Install {
    Install {
        cluster_domain: "cluster.local".to_owned(),
        image: "edgerush:test".to_owned(),
    }
}

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_owned()),
        namespace: Some(namespace.to_owned()),
        uid: Some(format!("uid-of-{name}")),
        ..ObjectMeta::default()
    }
}

fn control_plane() -> ControlPlane {
    ControlPlane {
        metadata: meta("payments", "edge"),
        ..ControlPlane::default()
    }
}

fn data_plane() -> DataPlane {
    let mut dp = DataPlane {
        metadata: meta("public", "edge"),
        ..DataPlane::default()
    };
    dp.spec.control_plane_ref.name = "payments".to_owned();
    // The schema's default, which `Default` does not know.
    dp.spec.ip_family = "IPv4".to_owned();
    dp.spec.ingress = Some(DataPlaneIngress {
        http_port: 80,
        https_port: 443,
        ..DataPlaneIngress::default()
    });
    dp
}

#[test]
fn a_resource_without_a_uid_makes_nothing() {
    let mut cp = control_plane();
    cp.metadata.uid = None;
    assert_eq!(
        for_control_plane(&cp, &BTreeSet::new(), &install()),
        Err(Error::Missing {
            kind: "ControlPlane",
            what: "UID"
        })
    );
    let mut dp = data_plane();
    dp.metadata.namespace = None;
    assert_eq!(
        for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()),
        Err(Error::Missing {
            kind: "DataPlane",
            what: "namespace"
        })
    );
}

#[test]
fn a_data_plane_of_another_control_plane_makes_nothing() {
    let mut dp = data_plane();
    dp.spec.control_plane_ref.name = "orders".to_owned();
    let not_its = Err(Error::NotItsControlPlane {
        data_plane: "edge/public".to_owned(),
        control_plane: "edge/payments".to_owned(),
    });
    assert_eq!(
        for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()),
        not_its
    );
    // One of the same name in another namespace is not it either.
    let mut dp = data_plane();
    dp.metadata.namespace = Some("elsewhere".to_owned());
    assert!(matches!(
        for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()),
        Err(Error::NotItsControlPlane { .. })
    ));
}

#[test]
fn a_cpu_quantity_not_read_makes_nothing() {
    let mut dp = data_plane();
    dp.spec.resources = Some(DataPlaneResources {
        requests: Some(BTreeMap::from([(
            "cpu".to_owned(),
            IntOrString::String("lots".to_owned()),
        )])),
        ..DataPlaneResources::default()
    });
    assert_eq!(
        for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()),
        Err(Error::Cpu("lots".to_owned()))
    );
}

#[test]
fn a_watch_scope_not_known_is_taken_as_the_narrowest() {
    let mut cp = control_plane();
    cp.spec.watch.namespaces.from = "Elsewhere".to_owned();
    let objects = for_control_plane(&cp, &BTreeSet::from(["shop".to_owned()]), &install());
    let watch_bindings: Vec<_> = objects
        .unwrap()
        .into_iter()
        .filter_map(|object| match object {
            Object::RoleBinding(binding) if binding.role_ref.name == super::CONTROL_PLANE_ROLE => {
                binding.metadata.namespace
            }
            Object::ClusterRoleBinding(binding)
                if binding.role_ref.name == super::CONTROL_PLANE_ROLE =>
            {
                Some("the cluster".to_owned())
            }
            _ => None,
        })
        .collect();
    assert_eq!(watch_bindings, ["edge"]);
}

#[test]
fn nothing_is_bound_but_the_four_shipped_roles() {
    let dp = data_plane();
    let every = [
        for_control_plane(&control_plane(), &BTreeSet::new(), &install()),
        for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()),
    ];
    for object in every.into_iter().flat_map(Result::unwrap) {
        let role = match object {
            Object::RoleBinding(binding) => binding.role_ref,
            Object::ClusterRoleBinding(binding) => binding.role_ref,
            _ => continue,
        };
        assert_eq!(role.kind, "ClusterRole");
        assert!(super::ROLES.contains(&role.name.as_str()), "{}", role.name);
    }
}

/// A ControlPlane watching the namespaces `selector` selects, written as YAML.
fn selecting(selector: &str) -> ControlPlane {
    let mut cp = control_plane();
    cp.spec.watch.namespaces.from = "Selector".to_owned();
    cp.spec.watch.namespaces.selector = Some(serde_saphyr::from_str(selector).unwrap());
    cp
}

/// The arguments of the controller of `objects`.
fn controller_args(objects: &[Object]) -> Vec<String> {
    objects
        .iter()
        .find_map(|object| match object {
            Object::Deployment(deployment) => deployment.spec.as_ref()?.template.spec.as_ref(),
            _ => None,
        })
        .and_then(|pod| pod.containers[0].args.clone())
        .unwrap()
}

/// What `--namespace-selector` says in `args`, and whether `--namespace` is among them.
fn selector_arg(args: &[String]) -> (Option<&str>, bool) {
    let selector = args
        .iter()
        .position(|arg| arg == "--namespace-selector")
        .map(|at| args[at + 1].as_str());
    (selector, args.iter().any(|arg| arg == "--namespace"))
}

#[test]
fn a_selector_scope_reaches_the_controller_as_its_selector_not_its_namespaces() {
    let cp = selecting(
        "matchLabels: { tier: web, team: payments }
matchExpressions:
  - { key: zone, operator: NotIn, values: [b, a] }
  - { key: owner, operator: Exists }
  - { key: legacy, operator: DoesNotExist }
  - { key: env, operator: In, values: [prod, canary, prod] }",
    );
    let some = for_control_plane(&cp, &BTreeSet::from(["shop".to_owned()]), &install()).unwrap();
    assert_eq!(
        selector_arg(&controller_args(&some)),
        (
            Some("env in (canary,prod),!legacy,owner,team=payments,tier=web,zone notin (a,b)"),
            false
        )
    );
    // A namespace labelled in or out changes the bindings alone, never the controller, so
    // that it is not rolled (04 §1).
    let more = for_control_plane(
        &cp,
        &BTreeSet::from(["shop".to_owned(), "blog".to_owned()]),
        &install(),
    )
    .unwrap();
    let deployment = |objects: &[Object]| {
        objects
            .iter()
            .find(|object| matches!(object, Object::Deployment(_)))
            .cloned()
    };
    assert_eq!(deployment(&some), deployment(&more));
    assert_ne!(some, more, "the bindings did not follow the namespaces");
    // No requirement is every namespace, followed one at a time as any selector's.
    let every = for_control_plane(&selecting("{}"), &BTreeSet::new(), &install()).unwrap();
    assert_eq!(selector_arg(&controller_args(&every)), (Some(""), false));
}

#[test]
fn a_scope_of_its_own_namespace_or_the_cluster_takes_no_selector() {
    let own = for_control_plane(&control_plane(), &BTreeSet::new(), &install()).unwrap();
    let args = controller_args(&own);
    assert_eq!(selector_arg(&args), (None, true));
    let at = args.iter().position(|arg| arg == "--namespace").unwrap();
    assert_eq!(args[at + 1], "edge");
    let mut cp = control_plane();
    cp.spec.watch.namespaces.from = "All".to_owned();
    let all = for_control_plane(&cp, &BTreeSet::new(), &install()).unwrap();
    assert_eq!(selector_arg(&controller_args(&all)), (None, false));
}

#[test]
fn a_selector_the_api_server_would_not_read_makes_nothing() {
    for (selector, why) in [
        (
            "matchExpressions: [{ key: env, operator: In }]",
            "In of key env has no values",
        ),
        (
            "matchExpressions: [{ key: env, operator: NotIn, values: [] }]",
            "NotIn of key env has no values",
        ),
        (
            "matchExpressions: [{ key: env, operator: Exists, values: [prod] }]",
            "Exists of key env has values",
        ),
        (
            "matchExpressions: [{ key: env, operator: Gt, values: ['1'] }]",
            "Gt of key env is not an operator Kubernetes has",
        ),
    ] {
        assert_eq!(
            for_control_plane(&selecting(selector), &BTreeSet::new(), &install()),
            Err(Error::Selector(why.to_owned())),
            "{selector}"
        );
    }
}

/// The ports of a data plane's Service, as `(name, port, protocol)`.
fn service_ports(
    bound: &[(u16, Transports)],
    dp: &DataPlane,
) -> Option<Vec<(String, i32, String)>> {
    let bound: BTreeMap<u16, Transports> = bound.iter().copied().collect();
    let service = ProxyService::of(dp).unwrap().service(&bound)?;
    Some(
        service
            .spec
            .unwrap()
            .ports
            .unwrap()
            .into_iter()
            .map(|port| (port.name.unwrap(), port.port, port.protocol.unwrap()))
            .collect(),
    )
}

const TCP: Transports = Transports {
    tcp: true,
    udp: false,
};
const UDP: Transports = Transports {
    tcp: false,
    udp: true,
};
const BOTH: Transports = Transports {
    tcp: true,
    udp: true,
};

fn port(name: &str, port: i32, protocol: &str) -> (String, i32, String) {
    (name.to_owned(), port, protocol.to_owned())
}

#[test]
fn ingress_ports_are_listed_whatever_is_bound_and_once_beside_a_listener_of_theirs() {
    let dp = data_plane();
    assert_eq!(
        service_ports(&[], &dp),
        Some(vec![
            port("port-80", 80, "TCP"),
            port("port-443", 443, "TCP")
        ])
    );
    assert_eq!(
        service_ports(&[(443, BOTH), (8080, TCP)], &dp),
        Some(vec![
            port("port-80", 80, "TCP"),
            port("port-443", 443, "TCP"),
            port("port-443-udp", 443, "UDP"),
            port("port-8080", 8080, "TCP"),
        ])
    );
}

#[test]
fn a_data_plane_with_no_port_bound_and_no_ingress_has_no_service() {
    let mut dp = data_plane();
    dp.spec.ingress = None;
    assert_eq!(service_ports(&[], &dp), None);
    assert_eq!(
        service_ports(&[(53, UDP)], &dp),
        Some(vec![port("port-53-udp", 53, "UDP")])
    );
}

#[test]
fn a_service_is_owned_by_its_data_plane_and_named_as_its_deployment() {
    let dp = data_plane();
    let shaped = ProxyService::of(&dp).unwrap();
    assert_eq!(
        (shaped.name(), shaped.namespace(), shaped.owner_uid()),
        ("edgerush-proxy-public", "edge", "uid-of-public")
    );
    let objects = for_data_plane(&dp, &control_plane(), &BTreeSet::new(), &install()).unwrap();
    let pods = objects
        .iter()
        .find_map(|object| match object {
            Object::Deployment(deployment) => deployment
                .spec
                .as_ref()
                .and_then(|spec| spec.template.metadata.as_ref())
                .and_then(|metadata| metadata.labels.clone()),
            _ => None,
        })
        .unwrap();
    let service = shaped.service(&BTreeMap::new()).unwrap();
    assert_eq!(service.spec.unwrap().selector, Some(pods));
}

#[test]
fn an_ip_family_its_schema_does_not_hold_makes_no_service() {
    let mut dp = data_plane();
    dp.spec.ip_family = "IPv5".to_owned();
    assert_eq!(
        ProxyService::of(&dp),
        Err(Error::IpFamily("IPv5".to_owned()))
    );
}
