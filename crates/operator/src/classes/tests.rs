use super::*;
use k8s_openapi::jiff::Timestamp;

fn time(at: i64) -> Time {
    Time(Timestamp::from_second(1_800_000_000 + at).unwrap())
}

/// GatewayClass `name` of generation `generation`, naming `controller`, holding
/// `conditions`.
fn class(name: &str, controller: &str, generation: i64, conditions: &[Condition]) -> GatewayClass {
    let mut class: GatewayClass = serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "GatewayClass",
        "metadata": { "name": name, "generation": generation },
        "spec": { "controllerName": controller },
    }))
    .unwrap();
    if !conditions.is_empty() {
        class.status.get_or_insert_default().conditions = Some(conditions.to_vec());
    }
    class
}

/// `class` labelled as the operator labels the class of ControlPlane `name` of `edge`.
fn made_for(mut class: GatewayClass, name: &str) -> GatewayClass {
    class.metadata.labels = Some(control_plane_labels("edge", name));
    class
}

/// What a class the API server has just taken holds, as Gateway API's CRD defaults it.
fn pending() -> Condition {
    Condition {
        type_: "Accepted".to_owned(),
        status: "Unknown".to_owned(),
        reason: "Pending".to_owned(),
        message: "Waiting for controller".to_owned(),
        observed_generation: None,
        last_transition_time: Time(Timestamp::UNIX_EPOCH),
    }
}

/// ControlPlane `name`, of namespace `edge`.
fn control_plane(name: &str) -> ControlPlane {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "ControlPlane",
        "metadata": { "name": name, "namespace": "edge" },
        "spec": { "watch": { "namespaces": { "from": "Same" } } },
    }))
    .unwrap()
}

#[test]
fn a_class_of_ours_that_no_control_plane_made_is_refused() {
    let suite = class(
        "gatewayclass-observed-generation-bump",
        GATEWAY_CONTROLLER,
        1,
        &[],
    );
    assert!(refused(&suite, &[]));
    assert!(refused(&suite, &[control_plane("edge")]));
}

#[test]
fn a_class_made_for_a_control_plane_is_left_to_it() {
    let edge = made_for(class("edge", GATEWAY_CONTROLLER, 1, &[]), "edge");
    assert!(!refused(&edge, &[control_plane("edge")]));
    assert!(!refused(
        &edge,
        &[control_plane("web"), control_plane("edge")]
    ));
    // Its ControlPlane gone, it is no one's.
    assert!(refused(&edge, &[control_plane("web")]));
}

#[test]
fn a_class_of_a_control_planes_name_not_made_for_it_is_refused() {
    // Left alone by the operator, and said on the ControlPlane's status (05 §2), but not
    // its class: its control plane takes it no more than the operator does.
    let planes = [control_plane("edge")];
    assert!(refused(&class("edge", GATEWAY_CONTROLLER, 1, &[]), &planes));
    // Labelled for a ControlPlane of that name in another namespace.
    let mut elsewhere = made_for(class("edge", GATEWAY_CONTROLLER, 1, &[]), "edge");
    if let Some(labels) = elsewhere.metadata.labels.as_mut() {
        labels.insert(
            edgerush_planes::objects::NAMESPACE_LABEL.to_owned(),
            "teams".to_owned(),
        );
    }
    assert!(refused(&elsewhere, &planes));
    // Some of the labels alone.
    let mut some = class("edge", GATEWAY_CONTROLLER, 1, &[]);
    some.metadata.labels = Some(std::collections::BTreeMap::from([(
        edgerush_planes::objects::CONTROL_PLANE_LABEL.to_owned(),
        "edge".to_owned(),
    )]));
    assert!(refused(&some, &planes));
}

#[test]
fn a_class_of_another_controller_is_not_looked_at() {
    let other = class("other", "example.com/gateway-controller", 1, &[]);
    assert!(!refused(&other, &[]));
    assert!(refusals(&[other], &[], &time(0)).is_empty());
}

#[test]
fn a_refusal_is_unsupported_of_the_class_generation() {
    let suite = class("suite", GATEWAY_CONTROLLER, 3, &[pending()]);
    let accepted = accepted(&suite, &time(5));
    assert_eq!(
        (
            accepted.type_.as_str(),
            accepted.status.as_str(),
            accepted.reason.as_str(),
            accepted.observed_generation,
        ),
        ("Accepted", "False", "Unsupported", Some(3))
    );
    // From the CRD's Unknown, a change: now.
    assert_eq!(accepted.last_transition_time, time(5));
    assert!(!accepted.message.is_empty());
}

#[test]
fn a_refusal_keeps_the_time_it_was_first_refused_at() {
    let first = accepted(&class("suite", GATEWAY_CONTROLLER, 1, &[]), &time(5));
    let edited = class("suite", GATEWAY_CONTROLLER, 2, &[first]);
    let again = accepted(&edited, &time(9));
    assert_eq!(again.observed_generation, Some(2));
    assert_eq!(again.last_transition_time, time(5));
}

#[test]
fn a_class_once_accepted_refused_is_a_change_now() {
    let mut was = pending();
    was.status = "True".to_owned();
    was.reason = "Accepted".to_owned();
    let suite = class("suite", GATEWAY_CONTROLLER, 1, &[was]);
    assert_eq!(accepted(&suite, &time(7)).last_transition_time, time(7));
}

#[test]
fn a_refused_class_is_written_until_its_status_says_so() {
    let suite = class("suite", GATEWAY_CONTROLLER, 1, &[pending()]);
    let edge = made_for(class("edge", GATEWAY_CONTROLLER, 1, &[pending()]), "edge");
    let other = class("other", "example.com/gateway-controller", 1, &[pending()]);
    let classes = [suite, edge, other];
    let planes = [control_plane("edge")];
    let first = refusals(&classes, &planes, &time(0));
    assert_eq!(
        first.iter().map(|refusal| refusal.name).collect::<Vec<_>>(),
        ["suite"]
    );

    // Written, nothing more to write, however later.
    let written = class("suite", GATEWAY_CONTROLLER, 1, &[first[0].accepted.clone()]);
    assert!(refusals(std::slice::from_ref(&written), &planes, &time(30)).is_empty());

    // Edited, a new generation to say, of the time first refused.
    let mut edited = written;
    edited.metadata.generation = Some(2);
    let again = refusals(std::slice::from_ref(&edited), &planes, &time(30));
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].accepted.observed_generation, Some(2));
    assert_eq!(again[0].accepted.last_transition_time, time(0));
}

#[test]
fn a_class_made_for_a_control_plane_later_is_no_longer_written() {
    let suite = class("suite", GATEWAY_CONTROLLER, 1, &[pending()]);
    assert_eq!(
        refusals(std::slice::from_ref(&suite), &[], &time(0)).len(),
        1
    );
    let suite = made_for(suite, "suite");
    assert!(refusals(&[suite], &[control_plane("suite")], &time(0)).is_empty());
}
