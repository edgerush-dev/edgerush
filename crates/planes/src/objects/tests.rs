use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::{Error, Install, Object, for_control_plane, for_data_plane};
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
