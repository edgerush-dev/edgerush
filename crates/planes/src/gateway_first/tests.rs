use std::collections::{BTreeMap, BTreeSet};

use edgerush_gateway_api::gateway::Gateway;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use proptest::prelude::*;

use super::{Error, GATEWAY_CLASS_NAME, GATEWAY_NAME, LONGEST, data_plane, data_plane_name};
use crate::control_plane::ControlPlane;
use crate::data_plane::DataPlane;
use crate::objects::{
    self, Install, MANAGED_BY, MANAGED_BY_LABEL, Object, ProxyService, Transports, for_data_plane,
};
use crate::platform_defaults::PlatformDefaultsGatewayClassesDataPlane;

/// Whether `name` is a DNS label: lower-case letters, digits and `-`, starting and ending
/// with a letter or digit, of 1 to 63 characters.
fn is_dns_label(name: &str) -> bool {
    let alphanumeric = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    (1..=63).contains(&name.len())
        && name.chars().all(|c| alphanumeric(c) || c == '-')
        && name.starts_with(alphanumeric)
        && name.ends_with(alphanumeric)
}

#[test]
fn a_name_that_fits_is_the_gateways_and_the_classs() {
    assert_eq!(data_plane_name("web", "edge"), "web-edge");
}

#[test]
fn a_name_of_48_characters_is_kept_and_one_of_49_shortened() {
    let fits = format!("{}-edge", "a".repeat(LONGEST - 5));
    assert_eq!(data_plane_name(&"a".repeat(LONGEST - 5), "edge"), fits);

    let over = data_plane_name(&"a".repeat(LONGEST - 4), "edge");
    assert_eq!(over.len(), LONGEST);
    assert!(over.starts_with(&"a".repeat(39)), "{over}");
    assert!(is_dns_label(&over), "{over}");
}

#[test]
fn a_name_with_a_dot_is_shortened_however_short_its_dots_made_dashes() {
    // A Gateway's name is a DNS subdomain, a DataPlane's a DNS label.
    let name = data_plane_name("payments.example.com", "edge");
    // Its first 39 characters are all of it; the hash is FNV-1a of the whole, pinned, as
    // a DataPlane renamed by an upgrade would be deleted and made again, its address lost.
    assert_eq!(name, "payments-example-com-edge-26810c5d");
    assert!(is_dns_label(&name));
}

#[test]
fn a_gateway_name_of_253_characters_makes_a_label_of_48() {
    let name = data_plane_name(&"a".repeat(253), "edge");
    assert_eq!(name, format!("{}-781408e2", "a".repeat(39)));
    assert!(is_dns_label(&name));
}

#[test]
fn gateways_alike_in_their_first_39_characters_get_names_apart() {
    let one = data_plane_name(&format!("{}-one", "a".repeat(60)), "edge");
    let two = data_plane_name(&format!("{}-two", "a".repeat(60)), "edge");
    assert_ne!(one, two);
}

/// A Gateway's name as the API server admits it: a DNS subdomain of at most 253
/// characters, its labels of at most 63.
fn gateway_name() -> impl Strategy<Value = String> {
    prop::collection::vec("[a-z0-9]([-a-z0-9]{0,20}[a-z0-9])?", 1..12)
        .prop_map(|labels| labels.join("."))
        .prop_filter("at most 253 characters", |name| name.len() <= 253)
}

proptest! {
    #[test]
    fn every_name_is_a_dns_label_of_at_most_48_characters(
        gateway in gateway_name(),
        // A ControlPlane's name, which its class has: a DNS label of at most 38.
        class in "[a-z0-9]([-a-z0-9]{0,36}[a-z0-9])?",
    ) {
        let name = data_plane_name(&gateway, &class);
        prop_assert!(name.len() <= LONGEST, "{name}");
        prop_assert!(is_dns_label(&name), "{name}");
    }
}

fn control_plane() -> ControlPlane {
    ControlPlane {
        metadata: ObjectMeta {
            name: Some("edge".to_owned()),
            namespace: Some("edge-system".to_owned()),
            uid: Some("uid-of-edge".to_owned()),
            ..ObjectMeta::default()
        },
        ..ControlPlane::default()
    }
}

/// A Gateway of `name` in `shop`, of the class `edge`, its `infrastructure` as given.
fn gateway(name: &str, infrastructure: &serde_json::Value) -> Gateway {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": name, "namespace": "shop", "uid": format!("uid-of-{name}") },
        "spec": {
            "gatewayClassName": "edge",
            "infrastructure": infrastructure,
            "listeners": [{ "name": "http", "port": 80, "protocol": "HTTP" }],
        },
    }))
    .unwrap()
}

/// An entry as the API server stores it, its defaults filled in.
fn entry(fields: serde_json::Value) -> PlatformDefaultsGatewayClassesDataPlane {
    serde_json::from_value(fields).unwrap()
}

fn no_infrastructure() -> serde_json::Value {
    serde_json::json!({})
}

#[test]
fn a_generated_data_plane_is_the_entry_named_for_its_gateway_and_owned_by_it() {
    let made = data_plane(
        &gateway("web", &no_infrastructure()),
        &entry(serde_json::json!({ "replicas": { "min": 2 } })),
        &control_plane(),
    )
    .unwrap();
    assert_eq!(made.metadata.name.as_deref(), Some("web-edge"));
    assert_eq!(made.metadata.namespace.as_deref(), Some("shop"));
    let owners = made.metadata.owner_references.unwrap();
    let [owner] = owners.as_slice() else {
        panic!("one owner: {owners:?}");
    };
    assert_eq!(
        (owner.kind.as_str(), owner.name.as_str(), owner.uid.as_str()),
        ("Gateway", "web", "uid-of-web")
    );
    assert_eq!(owner.controller, Some(true));
    // Its ControlPlane by namespace too, its Gateway by name, and the entry's fields with
    // their defaults; nothing of Ingress or a Gateway of its own.
    let spec = &made.spec;
    assert_eq!(spec.control_plane_ref.name, "edge");
    assert_eq!(
        spec.control_plane_ref.namespace.as_deref(),
        Some("edge-system")
    );
    assert_eq!(
        spec.gateway_ref.as_ref().map(|r| r.name.as_str()),
        Some("web")
    );
    assert_eq!(spec.replicas.min, 2);
    assert_eq!(spec.ip_family, "IPv4");
    assert_eq!(spec.service.r#type, "LoadBalancer");
    assert!(spec.gateway.is_none() && spec.ingress.is_none());
}

#[test]
fn a_generated_data_plane_carries_gep_1762s_labels_and_the_gateways_edgerushs_winning() {
    let made = data_plane(
        &gateway(
            "web",
            &serde_json::json!({
                "labels": { "team": "shop", MANAGED_BY_LABEL: "shop", GATEWAY_CLASS_NAME: "x" },
                "annotations": { "example.com/owner": "shop" },
            }),
        ),
        &entry(serde_json::json!({})),
        &control_plane(),
    )
    .unwrap();
    assert_eq!(
        made.metadata.labels.unwrap(),
        BTreeMap::from([
            (GATEWAY_CLASS_NAME.to_owned(), "edge".to_owned()),
            (GATEWAY_NAME.to_owned(), "web".to_owned()),
            (MANAGED_BY_LABEL.to_owned(), MANAGED_BY.to_owned()),
            ("team".to_owned(), "shop".to_owned()),
        ])
    );
    assert_eq!(
        made.metadata.annotations,
        Some(BTreeMap::from([(
            "example.com/owner".to_owned(),
            "shop".to_owned()
        )]))
    );
}

#[test]
fn a_gateway_name_longer_than_a_label_value_is_an_annotation_of_the_label_key() {
    for (length, labelled) in [(63, true), (64, false)] {
        let name = "a".repeat(length);
        let made = data_plane(
            &gateway(&name, &no_infrastructure()),
            &entry(serde_json::json!({})),
            &control_plane(),
        )
        .unwrap();
        let label = made.metadata.labels.unwrap().get(GATEWAY_NAME).cloned();
        let annotation = made
            .metadata
            .annotations
            .and_then(|annotations| annotations.get(GATEWAY_NAME).cloned());
        if labelled {
            assert_eq!((label, annotation), (Some(name), None));
        } else {
            assert_eq!((label, annotation), (None, Some(name)));
        }
    }
}

#[test]
fn a_gateway_without_a_uid_gets_no_data_plane() {
    let mut without = gateway("web", &no_infrastructure());
    without.metadata.uid = None;
    assert_eq!(
        data_plane(&without, &entry(serde_json::json!({})), &control_plane()),
        Err(Error::Missing {
            kind: "Gateway",
            what: "UID"
        })
    );
}

/// The DataPlane generated for `web`, given a UID as the API server would.
fn stored() -> DataPlane {
    let mut made = data_plane(
        &gateway(
            "web",
            &serde_json::json!({
                "labels": { "team": "shop", MANAGED_BY_LABEL: "shop" },
                "annotations": { "example.com/owner": "shop", "example.com/lb": "alb" },
            }),
        ),
        &entry(serde_json::json!({ "service": { "annotations": { "example.com/lb": "nlb" } } })),
        &control_plane(),
    )
    .unwrap();
    made.metadata.uid = Some("uid-of-web-edge".to_owned());
    made
}

fn install() -> Install {
    Install {
        cluster_domain: "cluster.local".to_owned(),
        image: "edgerush:test".to_owned(),
    }
}

#[test]
fn everything_made_for_a_generated_data_plane_carries_its_labels_but_no_selector() {
    let objects =
        for_data_plane(&stored(), &control_plane(), &BTreeSet::new(), &install()).unwrap();
    for object in &objects {
        let metadata = object.metadata();
        let labels = metadata.labels.clone().unwrap_or_default();
        let kind = object.kind();
        assert_eq!(
            labels.get("team").map(String::as_str),
            Some("shop"),
            "{kind}"
        );
        assert_eq!(labels.get(GATEWAY_NAME).map(String::as_str), Some("web"));
        // EdgeRush's own label wins over the Gateway's of the same key.
        assert_eq!(
            labels.get(MANAGED_BY_LABEL).map(String::as_str),
            Some(MANAGED_BY)
        );
        let annotations = metadata.annotations.clone().unwrap_or_default();
        assert_eq!(
            annotations.get("example.com/owner").map(String::as_str),
            Some("shop"),
            "{kind}"
        );
    }
    let deployment = objects
        .iter()
        .find_map(|object| match object {
            Object::Deployment(deployment) => Some(deployment.spec.clone()),
            _ => None,
        })
        .unwrap();
    let selector = deployment.selector.match_labels.unwrap();
    assert!(!selector.contains_key("team"), "{selector:?}");
    let pods = deployment.template.metadata.unwrap();
    assert_eq!(
        pods.labels.unwrap().get("team").map(String::as_str),
        Some("shop")
    );
    // Its pods dial their ControlPlane in its own namespace.
    let args = deployment.template.spec.unwrap().containers[0]
        .args
        .clone()
        .unwrap();
    assert!(
        args.contains(&"edgerush-controller-edge.edge-system.svc:9443".to_owned()),
        "{args:?}"
    );
}

#[test]
fn a_generated_data_planes_service_carries_its_labels_the_entrys_annotations_winning() {
    let service = ProxyService::of(&stored())
        .unwrap()
        .service(&BTreeMap::from([(
            80,
            Transports {
                tcp: true,
                udp: false,
            },
        )]))
        .unwrap();
    let labels = service.metadata.labels.unwrap();
    assert_eq!(labels.get("team").map(String::as_str), Some("shop"));
    assert_eq!(
        labels.get(MANAGED_BY_LABEL).map(String::as_str),
        Some(objects::SERVICE_MANAGED_BY)
    );
    let annotations = service.metadata.annotations.unwrap();
    assert_eq!(
        annotations.get("example.com/lb").map(String::as_str),
        Some("nlb")
    );
    assert_eq!(
        annotations.get("example.com/owner").map(String::as_str),
        Some("shop")
    );
}

#[test]
fn a_data_plane_naming_a_control_plane_of_another_namespace_is_not_this_ones() {
    let mut other = stored();
    other.spec.control_plane_ref.namespace = Some("elsewhere".to_owned());
    assert!(matches!(
        for_data_plane(&other, &control_plane(), &BTreeSet::new(), &install()),
        Err(objects::Error::NotItsControlPlane { .. })
    ));
}
