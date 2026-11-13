use super::*;
use k8s_openapi::jiff::Timestamp;

/// A ControlPlane of `name` in `namespace`, made `at` seconds in, of UID `uid`, watching
/// `from`.
fn control_plane(namespace: &str, name: &str, at: i64, uid: &str, from: &str) -> ControlPlane {
    let mut control_plane: ControlPlane = serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "ControlPlane",
        "metadata": { "name": name, "namespace": namespace, "uid": uid },
        "spec": { "watch": { "namespaces": { "from": from } } },
    }))
    .unwrap();
    control_plane.metadata.creation_timestamp = Some(time(at));
    control_plane
}

/// A DataPlane of `name` in `namespace`, of ControlPlane `of`, made `at` seconds in.
fn data_plane(namespace: &str, name: &str, of: &str, at: i64) -> DataPlane {
    let mut data_plane: DataPlane = serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "DataPlane",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{namespace}-{name}") },
        "spec": {
            "controlPlaneRef": { "name": of },
            "ingress": { "httpPort": 80, "httpsPort": 443 },
        },
    }))
    .unwrap();
    data_plane.metadata.creation_timestamp = Some(time(at));
    data_plane
}

fn time(at: i64) -> Time {
    Time(Timestamp::from_second(1_800_000_000 + at).unwrap())
}

fn verdicts(judged: &Judged<'_>) -> Vec<(String, Result<(), Refused>)> {
    judged
        .control_planes
        .iter()
        .map(|(control_plane, verdict)| (named(&control_plane.metadata), verdict.clone()))
        .collect()
}

fn data_plane_verdicts(judged: &Judged<'_>) -> Vec<(String, Result<String, Refused>)> {
    judged
        .data_planes
        .iter()
        .map(|(data_plane, verdict)| {
            (
                named(&data_plane.metadata),
                verdict
                    .clone()
                    .map(|control_plane| named(&control_plane.metadata)),
            )
        })
        .collect()
}

#[test]
fn the_oldest_control_plane_of_a_name_holds_it_and_later_ones_are_refused() {
    let control_planes = [
        control_plane("b", "edge", 20, "u2", "Same"),
        control_plane("a", "edge", 10, "u1", "Same"),
        control_plane("c", "other", 30, "u3", "Same"),
    ];
    let judged = judge(&control_planes, &[]);
    assert_eq!(
        verdicts(&judged),
        [
            (
                "b/edge".to_owned(),
                Err(Refused::NameTaken {
                    holder: "a/edge".to_owned()
                })
            ),
            ("a/edge".to_owned(), Ok(())),
            ("c/other".to_owned(), Ok(())),
        ]
    );
}

#[test]
fn of_two_made_at_once_the_lesser_uid_holds_the_name() {
    let control_planes = [
        control_plane("b", "edge", 10, "u2", "Same"),
        control_plane("a", "edge", 10, "u1", "Same"),
    ];
    let judged = judge(&control_planes, &[]);
    assert_eq!(
        judged.control_planes[0].1.as_ref().unwrap_err().reason(),
        "NameTaken"
    );
    assert_eq!(judged.control_planes[1].1, Ok(()));
}

#[test]
fn a_control_plane_watching_beyond_its_own_namespace_is_refused() {
    let control_planes = [
        control_plane("a", "all", 10, "u1", "All"),
        control_plane("a", "picked", 10, "u2", "Selector"),
    ];
    let judged = judge(&control_planes, &[]);
    assert_eq!(
        verdicts(&judged),
        [
            (
                "a/all".to_owned(),
                Err(Refused::ScopeNotAllowed {
                    from: "All".to_owned()
                })
            ),
            (
                "a/picked".to_owned(),
                Err(Refused::ScopeNotAllowed {
                    from: "Selector".to_owned()
                })
            ),
        ]
    );
}

#[test]
fn a_data_plane_is_of_the_control_plane_of_its_namespace_that_holds_the_name() {
    let control_planes = [
        control_plane("a", "edge", 10, "u1", "Same"),
        control_plane("b", "edge", 20, "u2", "Same"),
    ];
    let data_planes = [
        data_plane("a", "web", "edge", 30),
        // Its namespace's ControlPlane of that name does not hold it.
        data_plane("b", "web", "edge", 30),
        // Its namespace has none of that name; another's does.
        data_plane("c", "web", "edge", 30),
        data_plane("a", "lost", "missing", 30),
    ];
    let judged = judge(&control_planes, &data_planes);
    let no_control_plane = || {
        Err(Refused::NoControlPlane {
            name: "edge".to_owned(),
        })
    };
    assert_eq!(
        data_plane_verdicts(&judged),
        [
            ("a/web".to_owned(), Ok("a/edge".to_owned())),
            ("b/web".to_owned(), no_control_plane()),
            ("c/web".to_owned(), no_control_plane()),
            (
                "a/lost".to_owned(),
                Err(Refused::NoControlPlane {
                    name: "missing".to_owned()
                })
            ),
        ]
    );
}

#[test]
fn a_control_plane_serves_its_oldest_data_plane_alone() {
    let control_planes = [control_plane("a", "edge", 10, "u1", "Same")];
    let data_planes = [
        data_plane("a", "second", "edge", 40),
        // Made at once: by name, as the control plane chooses, whichever is listed first.
        data_plane("a", "zeta", "edge", 30),
        data_plane("a", "first", "edge", 30),
    ];
    let judged = judge(&control_planes, &data_planes);
    let one_only = || {
        Err(Refused::OneDataPlaneOnly {
            holder: "first".to_owned(),
        })
    };
    assert_eq!(
        data_plane_verdicts(&judged),
        [
            ("a/second".to_owned(), one_only()),
            ("a/zeta".to_owned(), one_only()),
            ("a/first".to_owned(), Ok("a/edge".to_owned())),
        ]
    );
}

#[test]
fn a_control_plane_refused_for_its_scope_refuses_none_of_its_data_planes() {
    let control_planes = [control_plane("a", "edge", 10, "u1", "All")];
    let data_planes = [data_plane("a", "web", "edge", 30)];
    let judged = judge(&control_planes, &data_planes);
    assert!(judged.control_planes[0].1.is_err());
    assert_eq!(
        data_plane_verdicts(&judged),
        [("a/web".to_owned(), Ok("a/edge".to_owned()))]
    );
}

#[test]
fn each_refusal_has_its_reason_and_says_what_holds_or_is_asked() {
    let cases = [
        (
            Refused::NameTaken {
                holder: "a/edge".to_owned(),
            },
            "NameTaken",
            "a/edge",
        ),
        (
            Refused::ScopeNotAllowed {
                from: "All".to_owned(),
            },
            "ScopeNotAllowed",
            "All",
        ),
        (
            Refused::OneDataPlaneOnly {
                holder: "first".to_owned(),
            },
            "OneDataPlaneOnly",
            "first",
        ),
        (
            Refused::NoControlPlane {
                name: "edge".to_owned(),
            },
            "NoControlPlane",
            "edge",
        ),
    ];
    for (refused, reason, named) in cases {
        assert_eq!(refused.reason(), reason);
        assert!(refused.message().contains(named), "{}", refused.message());
    }
}
