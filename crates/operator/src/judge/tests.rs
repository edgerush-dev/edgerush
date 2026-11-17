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

/// Judged with no `PlatformDefaults`.
fn judged<'a>(control_planes: &'a [ControlPlane], data_planes: &'a [DataPlane]) -> Judged<'a> {
    judge(control_planes, data_planes, None, &BTreeMap::new(), &[])
}

/// `control_plane` watching the namespaces of `selector`, as JSON.
fn selecting(mut control_plane: ControlPlane, selector: serde_json::Value) -> ControlPlane {
    control_plane.spec.watch.namespaces = serde_json::from_value(serde_json::json!({
        "from": "Selector", "selector": selector,
    }))
    .unwrap();
    control_plane
}

/// A `PlatformDefaults` of `name` with `scopes`, as JSON.
fn platform(name: &str, scopes: serde_json::Value) -> PlatformDefaults {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "PlatformDefaults",
        "metadata": { "name": name },
        "spec": { "watchScopes": scopes },
    }))
    .unwrap()
}

/// Namespaces by name, each labelled with its name, as the API server labels them, and
/// with `labels`.
fn namespaces(each: &[(&str, &[(&str, &str)])]) -> BTreeMap<String, Labels> {
    each.iter()
        .map(|(name, labels)| {
            let mut all: Labels = labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            all.insert("kubernetes.io/metadata.name".to_owned(), (*name).to_owned());
            ((*name).to_owned(), all)
        })
        .collect()
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
    let judged = judged(&control_planes, &[]);
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
    let judged = judged(&control_planes, &[]);
    assert_eq!(
        judged.control_planes[0].1.as_ref().unwrap_err().reason(),
        "NameTaken"
    );
    assert_eq!(judged.control_planes[1].1, Ok(()));
}

#[test]
fn without_an_allowance_a_control_plane_watching_beyond_its_own_namespace_is_refused() {
    let control_planes = [
        control_plane("a", "all", 10, "u1", "All"),
        selecting(
            control_plane("a", "picked", 10, "u2", "Same"),
            serde_json::json!({ "matchLabels": { "team": "payments" } }),
        ),
        control_plane("a", "own", 10, "u3", "Same"),
    ];
    let judged = judged(&control_planes, &[]);
    let reasons: Vec<_> = judged
        .control_planes
        .iter()
        .map(|(_, verdict)| verdict.as_ref().err().map(Refused::reason))
        .collect();
    assert_eq!(
        reasons,
        [Some("ScopeNotAllowed"), Some("ScopeNotAllowed"), None]
    );
}

#[test]
fn an_allowance_lets_the_control_planes_of_the_namespaces_it_picks_watch_what_it_allows() {
    let platform = platform(
        "default",
        serde_json::json!([
            {
                "controlPlanes": { "namespaceSelector": { "matchLabels": { "kubernetes.io/metadata.name": "edge" } } },
                "namespaces": { "from": "All" },
            },
            {
                "controlPlanes": { "namespaceSelector": { "matchLabels": { "tenant": "payments" } } },
                "namespaces": { "from": "Selector", "selector": { "matchLabels": { "team": "payments" } } },
            },
        ]),
    );
    let namespaces = namespaces(&[
        ("edge", &[]),
        ("pay", &[("tenant", "payments")]),
        ("other", &[]),
    ]);
    let narrower = serde_json::json!({ "matchLabels": { "team": "payments", "tier": "web" } });
    let control_planes = [
        control_plane("edge", "edge-all", 10, "u1", "All"),
        control_plane("pay", "pay-all", 10, "u2", "All"),
        selecting(
            control_plane("pay", "pay-narrower", 10, "u3", "Same"),
            narrower.clone(),
        ),
        selecting(
            control_plane("pay", "pay-wider", 10, "u4", "Same"),
            serde_json::json!({ "matchLabels": { "tier": "web" } }),
        ),
        selecting(
            control_plane("other", "other-narrower", 10, "u5", "Same"),
            narrower,
        ),
        // A namespace not seen has no labels: no allowance picks it.
        control_plane("gone", "gone-all", 10, "u6", "All"),
    ];
    let judged = judge(&control_planes, &[], Some(&platform), &namespaces, &[]);
    let reasons: Vec<_> = judged
        .control_planes
        .iter()
        .map(|(control_plane, verdict)| {
            (
                named(&control_plane.metadata),
                verdict.as_ref().err().map(Refused::reason),
            )
        })
        .collect();
    let refused = Some("ScopeNotAllowed");
    assert_eq!(
        reasons,
        [
            ("edge/edge-all".to_owned(), None),
            ("pay/pay-all".to_owned(), refused),
            ("pay/pay-narrower".to_owned(), None),
            ("pay/pay-wider".to_owned(), refused),
            ("other/other-narrower".to_owned(), refused),
            ("gone/gone-all".to_owned(), refused),
        ]
    );
}

#[test]
fn a_platform_defaults_of_another_name_allows_nothing() {
    let scopes = serde_json::json!([{
        "controlPlanes": { "namespaceSelector": { "matchLabels": { "kubernetes.io/metadata.name": "edge" } } },
        "namespaces": { "from": "All" },
    }]);
    let control_planes = [control_plane("edge", "all", 10, "u1", "All")];
    let namespaces = namespaces(&[("edge", &[])]);
    let named_so = platform("default", scopes.clone());
    let other = platform("other", scopes);
    assert_eq!(
        judge(&control_planes, &[], Some(&named_so), &namespaces, &[]).control_planes[0].1,
        Ok(())
    );
    assert!(
        judge(&control_planes, &[], Some(&other), &namespaces, &[]).control_planes[0]
            .1
            .is_err()
    );
}

#[test]
fn a_refusal_for_its_scope_says_what_was_asked_and_what_is_allowed() {
    let platform = platform(
        "default",
        serde_json::json!([{
            "controlPlanes": { "namespaceSelector": { "matchLabels": { "tenant": "payments" } } },
            "namespaces": { "from": "Selector", "selector": { "matchLabels": { "team": "payments" } } },
        }]),
    );
    let namespaces = namespaces(&[("pay", &[("tenant", "payments")]), ("other", &[])]);
    let control_planes = [
        control_plane("pay", "pay", 10, "u1", "All"),
        control_plane("other", "other", 10, "u2", "All"),
    ];
    let judged = judge(&control_planes, &[], Some(&platform), &namespaces, &[]);
    let messages: Vec<String> = judged
        .control_planes
        .iter()
        .map(|(_, verdict)| verdict.as_ref().unwrap_err().message())
        .collect();
    assert!(
        messages[0].contains("A watch scope of All")
            && messages[0].contains("or Selector of team=payments"),
        "{}",
        messages[0]
    );
    assert!(
        messages[1].contains("no allowance of PlatformDefaults default is for this namespace"),
        "{}",
        messages[1]
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
    let judged = judged(&control_planes, &data_planes);
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
fn a_control_plane_serves_every_data_plane_naming_it() {
    let control_planes = [
        control_plane("a", "edge", 10, "u1", "Same"),
        control_plane("b", "other", 10, "u2", "Same"),
    ];
    let data_planes = [
        data_plane("a", "second", "edge", 40),
        // Made at once, and one older: each accepted all the same.
        data_plane("a", "zeta", "edge", 30),
        data_plane("a", "first", "edge", 30),
        data_plane("b", "web", "other", 20),
    ];
    let judged = judged(&control_planes, &data_planes);
    assert_eq!(
        data_plane_verdicts(&judged),
        [
            ("a/second".to_owned(), Ok("a/edge".to_owned())),
            ("a/zeta".to_owned(), Ok("a/edge".to_owned())),
            ("a/first".to_owned(), Ok("a/edge".to_owned())),
            ("b/web".to_owned(), Ok("b/other".to_owned())),
        ]
    );
}

#[test]
fn a_control_plane_refused_for_its_scope_refuses_none_of_its_data_planes() {
    let control_planes = [control_plane("a", "edge", 10, "u1", "All")];
    let data_planes = [data_plane("a", "web", "edge", 30)];
    let judged = judged(&control_planes, &data_planes);
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
            Refused::ScopeNotAllowed(Beyond {
                asked: allowance::Scope::All,
                allowed: Vec::new(),
            }),
            "ScopeNotAllowed",
            "All",
        ),
        (
            Refused::NoControlPlane {
                name: "edge".to_owned(),
            },
            "NoControlPlane",
            "edge",
        ),
        (
            Refused::NotGenerated {
                control_plane: "edge-system/edge".to_owned(),
            },
            "NoControlPlane",
            "edge-system/edge",
        ),
    ];
    for (refused, reason, named) in cases {
        assert_eq!(refused.reason(), reason);
        assert!(refused.message().contains(named), "{}", refused.message());
    }
}

/// Gateway `web` of namespace `shop`, of the class of ControlPlane `edge`.
fn gateway() -> Gateway {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": "web", "namespace": "shop", "uid": "gw-uid" },
        "spec": {
            "gatewayClassName": "edge",
            "listeners": [{ "name": "http", "port": 80, "protocol": "HTTP" }],
        },
    }))
    .unwrap()
}

/// The DataPlane the operator generates for [`gateway`], of ControlPlane `edge` of
/// `edge-system`, as the API server stores it.
fn generated() -> DataPlane {
    let control_plane = control_plane("edge-system", "edge", 0, "cp", "All");
    let entry = serde_json::from_value(serde_json::json!({})).unwrap();
    let mut data_plane =
        edgerush_planes::gateway_first::data_plane(&gateway(), &entry, &control_plane).unwrap();
    data_plane.metadata.uid = Some("dp".to_owned());
    data_plane
}

/// Judged with no `PlatformDefaults`, beside [`gateway`].
fn judged_beside_the_gateway<'a>(
    control_planes: &'a [ControlPlane],
    data_planes: &'a [DataPlane],
) -> Judged<'a> {
    judge(
        control_planes,
        data_planes,
        None,
        &BTreeMap::new(),
        &[gateway()],
    )
}

#[test]
fn a_generated_data_plane_is_its_control_planes_of_the_namespace_it_names() {
    let control_planes = [control_plane("edge-system", "edge", 0, "cp", "All")];
    let data_planes = [generated()];
    let beside = judged_beside_the_gateway(&control_planes, &data_planes);
    let verdict = beside.data_planes[0]
        .1
        .as_ref()
        .map(|cp| cp.metadata.uid.clone());
    assert_eq!(verdict, Ok(Some("cp".to_owned())));
    // Without its Gateway, it is no longer the one a Gateway gets.
    assert!(matches!(
        judged(&control_planes, &data_planes).data_planes[0].1,
        Err(Refused::NotGenerated { .. })
    ));
}

#[test]
fn a_generated_data_plane_whose_control_plane_namespace_holds_none_of_its_name_is_refused() {
    // The name is held, but in another namespace than the one named.
    let control_planes = [control_plane("elsewhere", "edge", 0, "cp", "All")];
    let data_planes = [generated()];
    assert_eq!(
        judged_beside_the_gateway(&control_planes, &data_planes).data_planes[0]
            .1
            .as_ref()
            .err(),
        Some(&Refused::NoControlPlane {
            name: "edge".to_owned()
        })
    );
}

#[test]
fn a_data_plane_naming_another_namespace_or_a_gateway_by_hand_is_refused() {
    let control_planes = [control_plane("edge-system", "edge", 0, "cp", "All")];
    // Another namespace's ControlPlane, by hand.
    let mut elsewhere = data_plane("shop", "web", "edge", 0);
    elsewhere.spec.control_plane_ref.namespace = Some("edge-system".to_owned());
    // A Gateway, by hand, in the ControlPlane's own namespace.
    let mut naming_a_gateway = data_plane("edge-system", "web-edge", "edge", 0);
    naming_a_gateway.spec.ingress = None;
    naming_a_gateway.spec.gateway_ref = Some(edgerush_planes::data_plane::DataPlaneGatewayRef {
        name: "web".to_owned(),
    });
    // Its own namespace named outright is as left out.
    let mut own_named = data_plane("edge-system", "own", "edge", 0);
    own_named.spec.control_plane_ref.namespace = Some("edge-system".to_owned());
    let data_planes = [elsewhere, naming_a_gateway, own_named];
    let judged = judged_beside_the_gateway(&control_planes, &data_planes);
    assert!(matches!(
        judged.data_planes[0].1,
        Err(Refused::NotGenerated { .. })
    ));
    assert!(matches!(
        judged.data_planes[1].1,
        Err(Refused::NotGenerated { .. })
    ));
    assert!(judged.data_planes[2].1.is_ok());
}
