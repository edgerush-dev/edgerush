use super::*;

/// A ControlPlane's watch scope, as JSON.
fn scope(written: serde_json::Value) -> ControlPlaneWatchNamespaces {
    serde_json::from_value(written).unwrap()
}

/// A ControlPlane's `Selector` scope of `selector`, as JSON.
fn selecting(selector: serde_json::Value) -> ControlPlaneWatchNamespaces {
    scope(serde_json::json!({ "from": "Selector", "selector": selector }))
}

/// A `PlatformDefaults` named `default`, one allowance for the namespaces labelled
/// `tenant: payments`, of `namespaces`, as JSON.
fn allowing(namespaces: serde_json::Value) -> PlatformDefaults {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "gateway.edgerush.dev/v1alpha1",
        "kind": "PlatformDefaults",
        "metadata": { "name": NAME },
        "spec": { "watchScopes": [{
            "controlPlanes": { "namespaceSelector": { "matchLabels": { "tenant": "payments" } } },
            "namespaces": namespaces,
        }] },
    }))
    .unwrap()
}

/// The allowance's selector of `team: payments`.
fn payments() -> PlatformDefaults {
    allowing(serde_json::json!({
        "from": "Selector", "selector": { "matchLabels": { "team": "payments" } },
    }))
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// The labels of the namespace an allowance picks.
fn picked() -> Labels {
    labels(&[("tenant", "payments")])
}

#[test]
fn its_own_namespace_needs_no_allowance() {
    let same = scope(serde_json::json!({ "from": "Same" }));
    assert_eq!(allows(None, &Labels::new(), &same), Ok(()));
}

#[test]
fn a_scope_beyond_its_own_namespace_needs_an_allowance_that_picks_it() {
    let all = scope(serde_json::json!({ "from": "All" }));
    let platform = allowing(serde_json::json!({ "from": "All" }));
    assert_eq!(allows(Some(&platform), &picked(), &all), Ok(()));
    let unpicked = allows(Some(&platform), &labels(&[("tenant", "search")]), &all);
    assert_eq!(
        unpicked,
        Err(Beyond {
            asked: Scope::All,
            allowed: Vec::new()
        })
    );
    assert!(allows(None, &picked(), &all).is_err());
}

#[test]
fn all_allows_any_scope_and_a_selector_never_allows_all() {
    let all = allowing(serde_json::json!({ "from": "All" }));
    let selector = selecting(serde_json::json!({ "matchLabels": { "tier": "web" } }));
    assert_eq!(allows(Some(&all), &picked(), &selector), Ok(()));
    let whole = scope(serde_json::json!({ "from": "All" }));
    assert!(allows(Some(&payments()), &picked(), &whole).is_err());
}

#[test]
fn a_selector_is_within_one_whose_every_requirement_it_carries() {
    let cases = [
        // The same.
        (
            serde_json::json!({ "matchLabels": { "team": "payments" } }),
            true,
        ),
        // Narrower, by a requirement more.
        (
            serde_json::json!({ "matchLabels": { "team": "payments" },
                "matchExpressions": [{ "key": "tier", "operator": "Exists" }] }),
            true,
        ),
        // `matchLabels` is `In` of one value.
        (
            serde_json::json!({ "matchExpressions": [
                { "key": "team", "operator": "In", "values": ["payments"] }] }),
            true,
        ),
        // Missing the allowance's requirement.
        (
            serde_json::json!({ "matchLabels": { "tier": "web" } }),
            false,
        ),
        // Every namespace.
        (serde_json::json!({}), false),
        // Wider on the same key: `In` of more values is not the requirement.
        (
            serde_json::json!({ "matchExpressions": [
                { "key": "team", "operator": "In", "values": ["payments", "search"] }] }),
            false,
        ),
        // An operator Kubernetes does not have selects nothing and is within nothing.
        (
            serde_json::json!({ "matchLabels": { "team": "payments" }, "matchExpressions": [
                { "key": "tier", "operator": "Near", "values": ["web"] }] }),
            false,
        ),
    ];
    for (selector, within) in cases {
        let verdict = allows(Some(&payments()), &picked(), &selecting(selector.clone()));
        assert_eq!(verdict.is_ok(), within, "{selector}");
    }
}

#[test]
fn values_are_a_set_whatever_their_order() {
    let platform = allowing(serde_json::json!({
        "from": "Selector", "selector": { "matchExpressions": [
            { "key": "team", "operator": "In", "values": ["payments", "billing"] }] },
    }));
    let asked = selecting(serde_json::json!({ "matchExpressions": [
        { "key": "team", "operator": "In", "values": ["billing", "payments", "billing"] }] }));
    assert_eq!(allows(Some(&platform), &picked(), &asked), Ok(()));
}

#[test]
fn a_selector_matches_as_kubernetes_has_it() {
    let selector = Selector::from(
        &serde_json::from_value::<ControlPlaneWatchNamespacesSelector>(serde_json::json!({
            "matchLabels": { "team": "payments" },
            "matchExpressions": [
                { "key": "tier", "operator": "NotIn", "values": ["legacy"] },
                { "key": "frozen", "operator": "DoesNotExist" },
                { "key": "env", "operator": "Exists" },
            ],
        }))
        .unwrap(),
    );
    let cases = [
        (labels(&[("team", "payments"), ("env", "prod")]), true),
        (
            labels(&[("team", "payments"), ("env", "prod"), ("tier", "web")]),
            true,
        ),
        (
            labels(&[("team", "payments"), ("env", "prod"), ("tier", "legacy")]),
            false,
        ),
        (
            labels(&[("team", "payments"), ("env", "prod"), ("frozen", "")]),
            false,
        ),
        (labels(&[("team", "payments")]), false),
        (labels(&[("team", "search"), ("env", "prod")]), false),
    ];
    for (labels, matches) in cases {
        assert_eq!(selector.matches(&labels), matches, "{labels:?}");
    }
}

#[test]
fn a_selector_scope_selects_the_namespaces_its_selector_matches_and_others_none() {
    let namespaces = BTreeMap::from([
        ("pay".to_owned(), labels(&[("team", "payments")])),
        ("bill".to_owned(), labels(&[("team", "payments")])),
        ("search".to_owned(), labels(&[("team", "search")])),
    ]);
    let asked = selecting(serde_json::json!({ "matchLabels": { "team": "payments" } }));
    assert_eq!(
        selected(&asked, &namespaces),
        BTreeSet::from(["bill".to_owned(), "pay".to_owned()])
    );
    let all = scope(serde_json::json!({ "from": "All" }));
    assert!(selected(&all, &namespaces).is_empty());
}

#[test]
fn a_scope_is_said_as_kubectl_writes_a_selector() {
    let asked = Scope::of(&selecting(serde_json::json!({
        "matchLabels": { "team": "payments" },
        "matchExpressions": [
            { "key": "tier", "operator": "In", "values": ["web", "api"] },
            { "key": "frozen", "operator": "DoesNotExist" },
        ],
    })));
    assert_eq!(
        asked.to_string(),
        "Selector of team=payments,tier in (api,web),!frozen"
    );
}
