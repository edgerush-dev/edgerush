#![allow(
    clippy::unwrap_used,
    reason = "tests: a fixture that cannot be made is a broken test"
)]

use super::*;
use edgerush_planes::objects::control_plane_labels;
use k8s_openapi::jiff::Timestamp;

fn time(at: i64) -> Time {
    Time(Timestamp::from_second(1_800_000_000 + at).unwrap())
}

/// ControlPlane `edge` of namespace `edge-system`, watching `from`.
fn control_plane(from: &str) -> ControlPlane {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": "edge", "namespace": "edge-system", "uid": "cp-uid" },
        "spec": { "watch": { "namespaces": { "from": from } } },
    }))
    .unwrap()
}

/// The class the operator made for [`control_plane`].
fn class() -> GatewayClass {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": "edge", "labels": control_plane_labels("edge-system", "edge") },
        "spec": { "controllerName": "edgerush.dev/gateway-controller" },
    }))
    .unwrap()
}

/// The PlatformDefaults, with an entry for each of `classes`.
fn platform(classes: &[&str]) -> PlatformDefaults {
    let entries: Vec<_> = classes
        .iter()
        .map(|class| serde_json::json!({ "name": class, "dataPlane": {} }))
        .collect();
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": "default" },
        "spec": { "gatewayClasses": entries },
    }))
    .unwrap()
}

/// Gateway `name` of namespace `shop`, of class `class`, at generation 3.
fn gateway(name: &str, class: &str) -> Gateway {
    serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": name, "namespace": "shop", "uid": format!("uid-of-{name}"), "generation": 3,
        },
        "spec": {
            "gatewayClassName": class,
            "listeners": [{ "name": "http", "port": 80, "protocol": "HTTP" }],
        },
    }))
    .unwrap()
}

/// `gateway` with a `parametersRef`.
fn with_parameters(mut gateway: Gateway) -> Gateway {
    gateway.spec.infrastructure = Some(
        serde_json::from_value(serde_json::json!({
            "parametersRef": { "group": "", "kind": "ConfigMap", "name": "params" },
        }))
        .unwrap(),
    );
    gateway
}

/// What `gateways` make, of [`control_plane`] watching `from`, accepted or not, its class
/// made, the PlatformDefaults having entries for `entries`, beside `data_planes`.
fn generation_of<'a>(
    gateways: &'a [Gateway],
    data_planes: &'a [DataPlane],
    from: &str,
    accepted: bool,
    entries: &[&str],
) -> Generation<'a> {
    let control_plane = control_plane(from);
    let platform = platform(entries);
    let namespaces = BTreeMap::from([
        (
            "shop".to_owned(),
            Labels::from([("team".to_owned(), "shop".to_owned())]),
        ),
        ("edge-system".to_owned(), Labels::new()),
    ]);
    generation(
        gateways,
        data_planes,
        &[(&control_plane, accepted)],
        &[class()],
        Some(&platform),
        &namespaces,
        &time(0),
    )
}

/// The names of the DataPlanes `generation` applies.
fn names(generation: &Generation<'_>) -> Vec<String> {
    generation
        .data_planes
        .iter()
        .map(|(_, data_plane)| data_plane.metadata.name.clone().unwrap())
        .collect()
}

#[test]
fn a_gateway_of_a_class_with_an_entry_in_scope_gets_a_data_plane() {
    let gateways = [gateway("web", "edge")];
    let made = generation_of(&gateways, &[], "All", true, &["edge"]);
    assert_eq!(names(&made), ["web-edge"]);
    let (of, data_plane) = &made.data_planes[0];
    assert_eq!(of.metadata.name.as_deref(), Some("web"));
    assert_eq!(data_plane.metadata.namespace.as_deref(), Some("shop"));
    assert!(made.refused.is_empty() && made.released.is_empty() && made.stale.is_empty());
}

#[test]
fn a_gateway_gets_none_without_an_entry_scope_its_class_made_or_an_accepted_control_plane() {
    let gateways = [gateway("web", "edge")];
    // No entry: Gateway-first is the platform's to turn on.
    assert!(names(&generation_of(&gateways, &[], "All", true, &["other"])).is_empty());
    // Beyond the scope: `Same` is the ControlPlane's own namespace alone.
    assert!(names(&generation_of(&gateways, &[], "Same", true, &["edge"])).is_empty());
    // A ControlPlane not accepted.
    assert!(names(&generation_of(&gateways, &[], "All", false, &["edge"])).is_empty());
    // Another class.
    let other = [gateway("web", "other")];
    assert!(names(&generation_of(&other, &[], "All", true, &["edge", "other"])).is_empty());
}

#[test]
fn a_selector_scope_takes_the_gateways_of_the_namespaces_it_selects() {
    let mut control_plane = control_plane("Selector");
    control_plane.spec.watch.namespaces.selector = Some(
        serde_json::from_value(serde_json::json!({ "matchLabels": { "team": "shop" } })).unwrap(),
    );
    let namespaces = BTreeMap::from([(
        "shop".to_owned(),
        Labels::from([("team".to_owned(), "shop".to_owned())]),
    )]);
    let gateways = [gateway("web", "edge")];
    let platform = platform(&["edge"]);
    let made = generation(
        &gateways,
        &[],
        &[(&control_plane, true)],
        &[class()],
        Some(&platform),
        &namespaces,
        &time(0),
    );
    assert_eq!(names(&made), ["web-edge"]);
}

#[test]
fn a_class_of_the_control_planes_name_the_operator_did_not_make_gets_none() {
    let mut hand_made = class();
    hand_made.metadata.labels = None;
    let gateways = [gateway("web", "edge")];
    let control_plane = control_plane("All");
    let platform = platform(&["edge"]);
    let made = generation(
        &gateways,
        &[],
        &[(&control_plane, true)],
        &[hand_made],
        Some(&platform),
        &BTreeMap::new(),
        &time(0),
    );
    assert!(names(&made).is_empty());
}

#[test]
fn a_gateway_the_operator_made_from_a_data_plane_gets_none_of_its_own() {
    let mut made_one = gateway("web", "edge");
    made_one.metadata.labels = Some(BTreeMap::from([(
        MANAGED_BY_LABEL.to_owned(),
        MANAGED_BY.to_owned(),
    )]));
    let gateways = [made_one];
    assert!(names(&generation_of(&gateways, &[], "All", true, &["edge"])).is_empty());
}

#[test]
fn a_gateway_with_parameters_gets_none_and_is_refused_of_its_generation_once() {
    let gateways = [with_parameters(gateway("web", "edge"))];
    let made = generation_of(&gateways, &[], "All", true, &["edge"]);
    assert!(names(&made).is_empty());
    let [(refused, conditions)] = made.refused.as_slice() else {
        panic!("one refused: {:?}", made.refused);
    };
    assert_eq!(refused.metadata.name.as_deref(), Some("web"));
    let said: Vec<_> = conditions
        .iter()
        .map(|c| {
            (
                c.type_.as_str(),
                c.status.as_str(),
                c.reason.as_str(),
                c.observed_generation,
            )
        })
        .collect();
    assert_eq!(
        said,
        [
            ("Accepted", "False", INVALID_PARAMETERS, Some(3)),
            ("Programmed", "False", INVALID_PARAMETERS, Some(3)),
        ]
    );
    // Its status saying so, nothing is written again.
    let mut told = gateways[0].clone();
    told.status =
        Some(serde_json::from_value(serde_json::json!({ "conditions": conditions })).unwrap());
    let told = [told];
    let again = generation_of(&told, &[], "All", true, &["edge"]);
    assert!(again.refused.is_empty() && again.released.is_empty());
}

#[test]
fn a_data_plane_generated_before_the_parameters_is_kept_and_not_applied_again() {
    let before = [gateway("web", "edge")];
    let made = generation_of(&before, &[], "All", true, &["edge"]);
    let generated = vec![stored(&made.data_planes[0].1)];
    let after = [with_parameters(gateway("web", "edge"))];
    let now = generation_of(&after, &generated, "All", true, &["edge"]);
    assert!(names(&now).is_empty(), "not applied again");
    assert!(now.stale.is_empty(), "not deleted");
    assert_eq!(now.refused.len(), 1);
}

#[test]
fn a_gateway_whose_parameters_are_taken_away_has_its_refusal_taken_back() {
    let refused = [with_parameters(gateway("web", "edge"))];
    let conditions = generation_of(&refused, &[], "All", true, &["edge"]).refused[0]
        .1
        .clone();
    let mut fixed = gateway("web", "edge");
    fixed.status =
        Some(serde_json::from_value(serde_json::json!({ "conditions": conditions })).unwrap());
    let fixed = [fixed];
    let made = generation_of(&fixed, &[], "All", true, &["edge"]);
    assert_eq!(made.released.len(), 1);
    assert_eq!(names(&made), ["web-edge"]);
}

/// `data_plane` as the API server stores it: with a UID, and, as its spec says, of its
/// namespace.
fn stored(data_plane: &DataPlane) -> DataPlane {
    let mut stored = data_plane.clone();
    stored.metadata.uid = Some("dp-uid".to_owned());
    stored
}

#[test]
fn a_generated_data_plane_is_told_apart_by_its_owner_name_and_control_plane() {
    let gateways = [gateway("web", "edge")];
    let made = generation_of(&gateways, &[], "All", true, &["edge"]);
    let generated = stored(&made.data_planes[0].1);
    assert!(is_generated(&generated, &gateways));

    let mut renamed = generated.clone();
    renamed.metadata.name = Some("web".to_owned());
    assert!(!is_generated(&renamed, &gateways), "of another name");

    let mut other_owner = generated.clone();
    other_owner.metadata.owner_references.as_mut().unwrap()[0].uid = "another".to_owned();
    assert!(
        !is_generated(&other_owner, &gateways),
        "owned by no Gateway there"
    );

    let mut unlabelled = generated.clone();
    unlabelled.metadata.labels = None;
    assert!(
        !is_generated(&unlabelled, &gateways),
        "not made by the operator"
    );

    let mut other_control_plane = generated.clone();
    other_control_plane.spec.control_plane_ref.name = "payments".to_owned();
    assert!(
        !is_generated(&other_control_plane, &gateways),
        "of another ControlPlane than the class's"
    );

    let mut no_gateway = generated;
    no_gateway.spec.gateway_ref = None;
    assert!(!is_generated(&no_gateway, &gateways), "naming no Gateway");
}

#[test]
fn a_generated_data_plane_whose_gateway_moved_to_another_class_is_stale() {
    let gateways = [gateway("web", "edge")];
    let made = generation_of(&gateways, &[], "All", true, &["edge"]);
    let generated = vec![stored(&made.data_planes[0].1)];
    // Its Gateway as it is: not stale.
    assert!(
        generation_of(&gateways, &generated, "All", true, &["edge"])
            .stale
            .is_empty()
    );
    let moved = [gateway("web", "other")];
    let now = generation_of(&moved, &generated, "All", true, &["edge"]);
    assert_eq!(now.stale.len(), 1);
    // Its Gateway gone: its owner reference takes it, not the operator.
    assert!(
        generation_of(&[], &generated, "All", true, &["edge"])
            .stale
            .is_empty()
    );
}
