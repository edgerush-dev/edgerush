use super::*;
use crate::acks::{Pod, Pods};
use edgerush_translate::{ListenerStatus, ParentStatus, RouteStatus};

/// The time `seconds` after the epoch.
fn at(seconds: i64) -> Timestamp {
    Timestamp::from_second(seconds).unwrap()
}

/// A condition as status holds one.
fn condition(kind: &str, status: &str, reason: &str, generation: i64, since: i64) -> Condition {
    Condition {
        last_transition_time: Time(at(since)),
        message: String::new(),
        observed_generation: Some(generation),
        reason: reason.to_owned(),
        status: status.to_owned(),
        type_: kind.to_owned(),
    }
}

/// A condition translation says.
fn said(kind: &'static str, status: bool, reason: &'static str) -> translate::Condition {
    translate::Condition {
        kind,
        status,
        reason,
        message: String::new(),
    }
}

/// The type, status and time of each of `conditions`.
fn seen(conditions: &[Condition]) -> Vec<(String, String, i64)> {
    conditions
        .iter()
        .map(|c| {
            (
                c.type_.clone(),
                c.status.clone(),
                c.last_transition_time.0.as_second(),
            )
        })
        .collect()
}

#[test]
fn a_condition_keeps_its_time_while_its_status_holds_and_takes_now_as_it_changes() {
    let live = [
        condition("Accepted", "True", "Accepted", 1, 10),
        condition("ResolvedRefs", "True", "ResolvedRefs", 1, 10),
    ];
    let wanted = vec![
        Wanted::new("Accepted", "True", "Accepted", String::new()),
        Wanted::new(
            "ResolvedRefs",
            "False",
            "BackendNotFound",
            "gone".to_owned(),
        ),
    ];
    let merged = merged(&live, wanted, &PARENT_TYPES, 2, at(50));
    assert_eq!(
        seen(&merged),
        [
            ("Accepted".to_owned(), "True".to_owned(), 10),
            ("ResolvedRefs".to_owned(), "False".to_owned(), 50),
        ]
    );
    // Each says of the generation written, the reason and message as wanted.
    assert!(merged.iter().all(|c| c.observed_generation == Some(2)));
    assert_eq!(merged[1].reason, "BackendNotFound");
    assert_eq!(merged[1].message, "gone");
}

#[test]
fn others_conditions_are_kept_in_place_and_ours_not_wanted_taken_away() {
    let foreign = Condition {
        message: "theirs".to_owned(),
        ..condition("example.com/Stale", "True", "Stale", 1, 5)
    };
    let live = [
        condition("Conflicted", "True", "HostnameConflict", 1, 10),
        foreign.clone(),
        condition("Accepted", "False", "HostnameConflict", 1, 10),
    ];
    let wanted = vec![
        Wanted::new("Accepted", "True", "Accepted", String::new()),
        Wanted::new("ResolvedRefs", "True", "ResolvedRefs", String::new()),
    ];
    let merged = merged(&live, wanted, &LISTENER_TYPES, 2, at(50));
    assert_eq!(
        seen(&merged),
        [
            ("example.com/Stale".to_owned(), "True".to_owned(), 5),
            ("Accepted".to_owned(), "True".to_owned(), 50),
            ("ResolvedRefs".to_owned(), "True".to_owned(), 50),
        ]
    );
    assert_eq!(merged[0], foreign, "another's condition changed");
}

#[test]
fn a_condition_of_a_newer_generation_is_left_as_it_is() {
    let newer = condition("Accepted", "False", "NoMatchingParent", 7, 30);
    let merged = merged(
        std::slice::from_ref(&newer),
        vec![Wanted::new("Accepted", "True", "Accepted", String::new())],
        &PARENT_TYPES,
        6,
        at(50),
    );
    assert_eq!(merged, [newer]);
}

#[test]
fn the_class_says_whether_every_kinds_release_is_supported() {
    let served = |release: &str| {
        GatewayKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind,
                    Served::Served {
                        release: Some(release.to_owned()),
                    },
                )
            })
            .collect::<Vec<_>>()
    };
    let version = |status: &GatewayClassStatus| {
        let conditions = status.conditions.as_deref().unwrap();
        let accepted = conditions.iter().find(|c| c.type_ == "Accepted").unwrap();
        assert_eq!(
            (accepted.status.as_str(), accepted.observed_generation),
            ("True", Some(3))
        );
        let version = conditions
            .iter()
            .find(|c| c.type_ == "SupportedVersion")
            .unwrap();
        (
            version.status.clone(),
            version.reason.clone(),
            version.message.clone(),
        )
    };
    for release in ["v1.7.0", "v1.6.2", "v1.7.0-rc.1"] {
        let status = class(None, 3, &served(release), at(50));
        assert_eq!(
            version(&status),
            (
                "True".to_owned(),
                "SupportedVersion".to_owned(),
                String::new()
            ),
            "{release}"
        );
        assert_eq!(status.supported_features, None);
    }
    let mut older = served("v1.7.0");
    older[3].1 = Served::Served {
        release: Some("v1.5.1".to_owned()),
    };
    older[4].1 = Served::Missing;
    assert_eq!(
        version(&class(None, 3, &older, at(50))),
        (
            "False".to_owned(),
            "UnsupportedVersion".to_owned(),
            "HTTPRoute served at v1 (Gateway API v1.5.1); ReferenceGrant not installed, so \
             taken as having no objects; supported: Gateway API v1.6, v1.7"
                .to_owned()
        )
    );
    let unnamed = vec![(GatewayKind::Gateway, Served::Served { release: None })];
    assert_eq!(
        version(&class(None, 3, &unnamed, at(50))).0,
        "False",
        "a CRD naming no release"
    );
}

/// What translation says of a Gateway of two listeners, `http` served and `secure` not.
fn wanted_gateway() -> translate::GatewayStatus {
    translate::GatewayStatus {
        generation: 4,
        conditions: vec![said("Accepted", true, "Accepted")],
        programmable: true,
        listeners: vec![
            ListenerStatus {
                name: "http".to_owned(),
                supported_kinds: vec!["HTTPRoute"],
                attached_routes: 2,
                conditions: vec![
                    said("Accepted", true, "Accepted"),
                    said("ResolvedRefs", true, "ResolvedRefs"),
                ],
                programmable: Ok(vec!["port-80".to_owned()]),
            },
            ListenerStatus {
                name: "secure".to_owned(),
                supported_kinds: vec!["HTTPRoute"],
                attached_routes: 1,
                conditions: vec![
                    said("Accepted", true, "Accepted"),
                    said("ResolvedRefs", false, "InvalidCertificateRef"),
                ],
                programmable: Err("no certificateRef of it can be used".to_owned()),
            },
        ],
        attached_listener_sets: 1,
        listener_sets: Vec::new(),
    }
}

#[test]
fn a_gateway_says_its_listeners_as_wanted_and_keeps_what_is_not_ours() {
    let foreign = condition("example.com/Stale", "True", "Stale", 1, 5);
    let live = GatewayStatus {
        addresses: None,
        attached_listener_sets: Some(3),
        conditions: Some(vec![condition("Accepted", "True", "Accepted", 3, 10)]),
        listeners: Some(vec![
            GatewayStatusListeners {
                attached_routes: 1,
                conditions: vec![
                    foreign.clone(),
                    condition("Accepted", "True", "Accepted", 3, 10),
                ],
                name: "http".to_owned(),
                supported_kinds: None,
            },
            GatewayStatusListeners {
                name: "gone".to_owned(),
                ..GatewayStatusListeners::default()
            },
        ]),
    };
    let addresses = [GatewayStatusAddresses {
        r#type: Some("IPAddress".to_owned()),
        value: "192.0.2.10".to_owned(),
    }];
    let unknown = Programmed::of(4, &Pods::new(), None, false);
    let status = gateway(Some(&live), &wanted_gateway(), &addresses, &unknown, at(50));
    assert_eq!(status.addresses.as_deref(), Some(addresses.as_slice()));
    assert_eq!(
        status.attached_listener_sets,
        Some(1),
        "ours to say, as translation counts them"
    );
    assert_eq!(
        seen(status.conditions.as_deref().unwrap()),
        [
            ("Accepted".to_owned(), "True".to_owned(), 10),
            ("Programmed".to_owned(), "Unknown".to_owned(), 50),
        ]
    );
    let listeners = status.listeners.unwrap();
    let names: Vec<&str> = listeners.iter().map(|l| l.name.as_str()).collect();
    assert_eq!(names, ["http", "secure"], "a listener gone is said no more");
    let http = &listeners[0];
    assert_eq!(http.attached_routes, 2);
    assert_eq!(
        http.supported_kinds.as_deref(),
        Some(
            [GatewayStatusListenersSupportedKinds {
                group: Some(GROUP.to_owned()),
                kind: "HTTPRoute".to_owned()
            }]
            .as_slice()
        )
    );
    assert_eq!(http.conditions[0], foreign);
    assert_eq!(
        seen(&http.conditions[1..]),
        [
            ("Accepted".to_owned(), "True".to_owned(), 10),
            ("ResolvedRefs".to_owned(), "True".to_owned(), 50),
            ("Programmed".to_owned(), "Unknown".to_owned(), 50),
        ]
    );
    let programmed = listeners[1]
        .conditions
        .iter()
        .find(|c| c.type_ == "Programmed")
        .unwrap();
    assert_eq!(
        (programmed.status.as_str(), programmed.reason.as_str()),
        ("False", "Invalid")
    );
    assert!(
        listeners
            .iter()
            .flat_map(|l| &l.conditions)
            .filter(|c| c.type_ != "example.com/Stale")
            .all(|c| c.observed_generation == Some(4))
    );
}

/// A listener whose port a ready pod running the generation does not serve is not
/// programmed, saying that pod and why, while the Gateway and its other listeners are, as
/// the config runs on every pod; a pod still on an older generation says nothing of the
/// new one's ports; and a refusal says the same of every listener, as before.
#[test]
fn a_listener_is_programmed_only_where_every_ready_pod_on_the_generation_serves_its_port() {
    let pod = |last: u8| IpAddr::from([10, 0, 0, last]);
    let ready = [pod(1), pod(2)];
    let runs = |generation: i64, unserved: &[&str]| Pod {
        runs: Some(generation),
        unserved: unserved
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    format!("listener \"{name}\": address in use"),
                )
            })
            .collect(),
        ..Pod::default()
    };
    let mut wanted = wanted_gateway();
    wanted.listeners[1] = ListenerStatus {
        name: "metrics".to_owned(),
        programmable: Ok(vec!["port-9100".to_owned()]),
        ..wanted.listeners[0].clone()
    };
    let programmed = |status: &GatewayStatus, listener: Option<usize>| {
        let conditions = match listener {
            Some(at) => &status.listeners.as_ref().unwrap()[at].conditions,
            None => status.conditions.as_ref().unwrap(),
        };
        let c = conditions.iter().find(|c| c.type_ == "Programmed").unwrap();
        (c.status.clone(), c.reason.clone(), c.message.clone())
    };
    let one_not = Pods::from([(pod(1), runs(4, &[])), (pod(2), runs(4, &["port-9100"]))]);
    let status = gateway(
        None,
        &wanted,
        &[],
        &Programmed::of(4, &one_not, Some(&ready), false),
        at(50),
    );
    let all = "2 of 2 data plane pods run generation 4".to_owned();
    assert_eq!(
        programmed(&status, None),
        ("True".to_owned(), "Programmed".to_owned(), all.clone())
    );
    assert_eq!(
        programmed(&status, Some(0)),
        ("True".to_owned(), "Programmed".to_owned(), all)
    );
    assert_eq!(
        programmed(&status, Some(1)),
        (
            "False".to_owned(),
            "Pending".to_owned(),
            "data plane 10.0.0.2: listener \"port-9100\": address in use".to_owned()
        )
    );
    // On an older generation, its ports are its config's: the listener waits as the
    // Gateway does.
    let older = Pods::from([(pod(1), runs(4, &[])), (pod(2), runs(3, &["port-9100"]))]);
    let status = gateway(
        None,
        &wanted,
        &[],
        &Programmed::of(4, &older, Some(&ready), false),
        at(50),
    );
    assert_eq!(programmed(&status, Some(1)), programmed(&status, None));
    assert_eq!(programmed(&status, None).0, "Unknown");
    // A refusal is said of every listener, before any port.
    let mut refusing = one_not.clone();
    refusing.get_mut(&pod(1)).unwrap().refused = Some("an unknown field".to_owned());
    let status = gateway(
        None,
        &wanted,
        &[],
        &Programmed::of(4, &refusing, Some(&ready), false),
        at(50),
    );
    for listener in [None, Some(0), Some(1)] {
        assert_eq!(programmed(&status, listener).1, "Invalid", "{listener:?}");
    }
    // A pod not ready is not counted.
    let status = gateway(
        None,
        &wanted,
        &[],
        &Programmed::of(4, &one_not, Some(&ready[..1]), false),
        at(50),
    );
    assert_eq!(programmed(&status, Some(1)).0, "True");
}

#[test]
fn a_gateway_of_nothing_served_is_not_programmed_and_says_no_address_of_none() {
    let wanted = translate::GatewayStatus {
        programmable: false,
        ..wanted_gateway()
    };
    let unknown = Programmed::of(4, &Pods::new(), None, false);
    let status = gateway(None, &wanted, &[], &unknown, at(50));
    assert_eq!(status.addresses, None);
    let programmed = status
        .conditions
        .as_deref()
        .unwrap()
        .iter()
        .find(|c| c.type_ == "Programmed")
        .unwrap();
    assert_eq!(
        (programmed.status.as_str(), programmed.reason.as_str()),
        ("False", "Invalid")
    );
}

/// The data plane's Gateway, `edge/web`.
fn web() -> Object {
    Object {
        kind: "Gateway",
        namespace: "edge".to_owned(),
        name: "web".to_owned(),
    }
}

/// A route's parent as status holds it, to `namespace`/`name` by `controller`.
fn parent(controller: &str, namespace: Option<&str>, name: &str) -> HTTPRouteStatusParents {
    HTTPRouteStatusParents {
        conditions: vec![condition("Accepted", "True", "Accepted", 1, 10)],
        controller_name: controller.to_owned(),
        parent_ref: HTTPRouteStatusParentsParentRef {
            group: Some(GROUP.to_owned()),
            kind: Some("Gateway".to_owned()),
            name: name.to_owned(),
            namespace: namespace.map(str::to_owned),
            port: None,
            section_name: None,
        },
    }
}

/// What translation says of a route of `shop` with one parent, `edge/web`.
fn wanted_route(generation: i64) -> RouteStatus {
    RouteStatus {
        route: Object {
            kind: "HTTPRoute",
            namespace: "shop".to_owned(),
            name: "app".to_owned(),
        },
        generation,
        parents: vec![ParentStatus {
            kind: "Gateway",
            namespace: Some("edge".to_owned()),
            name: "web".to_owned(),
            section_name: None,
            port: None,
            conditions: vec![
                said("Accepted", true, "Accepted"),
                said("ResolvedRefs", true, "ResolvedRefs"),
            ],
        }],
    }
}

#[test]
fn a_routes_parents_are_ours_of_our_gateway_beside_every_other_kept_whole() {
    let theirs = parent("example.com/other", Some("edge"), "web");
    let another_plane = parent(GATEWAY_CONTROLLER, Some("edge"), "other");
    let live = HTTPRouteStatus {
        parents: vec![
            parent(GATEWAY_CONTROLLER, Some("edge"), "web"),
            theirs.clone(),
            another_plane.clone(),
        ],
    };
    let status = route(
        Some(&live),
        "shop",
        &[web()],
        Some(&wanted_route(2)),
        at(50),
    );
    assert_eq!(status.parents.len(), 3);
    assert_eq!(status.parents[0], theirs, "another controller's changed");
    assert_eq!(
        status.parents[1], another_plane,
        "another data plane's changed"
    );
    let ours = &status.parents[2];
    assert_eq!(ours.controller_name, GATEWAY_CONTROLLER);
    assert_eq!(ours.parent_ref.name, "web");
    assert_eq!(
        seen(&ours.conditions),
        [
            ("Accepted".to_owned(), "True".to_owned(), 10),
            ("ResolvedRefs".to_owned(), "True".to_owned(), 50),
        ]
    );
    // A route that no longer names the Gateway has no parent of ours there, the rest kept.
    let gone = route(Some(&live), "shop", &[web()], None, at(60));
    assert_eq!(gone.parents, [theirs, another_plane]);
}

/// A ListenerSet's status is of its own listeners, its `Programmed` the config's while one
/// is served, and each listener's as a Gateway's; one the Gateway does not take says so in
/// both.
#[test]
fn a_listener_sets_status_is_of_its_own_listeners() {
    let pod = IpAddr::from([10, 0, 0, 1]);
    let running = Pods::from([(
        pod,
        Pod {
            runs: Some(4),
            ..Pod::default()
        },
    )]);
    let programmed = Programmed::of(4, &running, Some(&[pod]), false);
    let set = Object {
        kind: "ListenerSet",
        namespace: "shop".to_owned(),
        name: "team".to_owned(),
    };
    let wanted = translate::ListenerSetStatus {
        set: set.clone(),
        generation: 2,
        conditions: vec![said("Accepted", true, "Accepted")],
        programmable: Ok(()),
        listeners: vec![wanted_gateway().listeners[0].clone()],
    };
    let status = listener_set(None, &wanted, &programmed, at(50));
    let types: Vec<(&str, &str)> = status
        .conditions
        .iter()
        .flatten()
        .map(|c| (c.type_.as_str(), c.status.as_str()))
        .collect();
    assert_eq!(types, [("Accepted", "True"), ("Programmed", "True")]);
    let listeners = status.listeners.unwrap();
    assert_eq!(listeners.len(), 1);
    assert_eq!(listeners[0].attached_routes, 2);
    assert!(
        listeners[0]
            .conditions
            .iter()
            .any(|c| c.type_ == "Programmed" && c.status == "True")
    );
    assert!(
        listeners[0]
            .conditions
            .iter()
            .all(|c| c.observed_generation == Some(2))
    );
    let refused = translate::ListenerSetStatus {
        conditions: vec![said("Accepted", false, "NotAllowed")],
        programmable: Err(said("Programmed", false, "NotAllowed")),
        listeners: Vec::new(),
        ..wanted
    };
    let status = listener_set(None, &refused, &programmed, at(50));
    let reasons: Vec<(&str, &str)> = status
        .conditions
        .iter()
        .flatten()
        .map(|c| (c.type_.as_str(), c.reason.as_str()))
        .collect();
    assert_eq!(
        reasons,
        [("Accepted", "NotAllowed"), ("Programmed", "NotAllowed")]
    );
}

/// A route's parent that is a ListenerSet naming the data plane's Gateway is written of that
/// kind, and is this controller's as the Gateway is.
#[test]
fn a_routes_parent_of_a_listener_set_is_ours_and_said_of_its_kind() {
    let set = Object {
        kind: "ListenerSet",
        namespace: "shop".to_owned(),
        name: "team".to_owned(),
    };
    let mut wanted = wanted_route(2);
    wanted.parents[0].kind = "ListenerSet";
    wanted.parents[0].namespace = None;
    wanted.parents[0].name = "team".to_owned();
    let status = route(None, "shop", &[web(), set.clone()], Some(&wanted), at(50));
    let ours = &status.parents[0].parent_ref;
    assert_eq!(
        (ours.kind.as_deref(), ours.name.as_str()),
        (Some("ListenerSet"), "team")
    );
    // Written again, it is taken for the same parent, and no other added.
    let again = route(Some(&status), "shop", &[web(), set], Some(&wanted), at(60));
    assert_eq!(again.parents.len(), 1);
    assert_eq!(
        again.parents[0].conditions[0].last_transition_time,
        Time(at(50))
    );
}

#[test]
fn a_parent_of_the_routes_own_namespace_is_the_same_written_either_way() {
    // A route of the Gateway's own namespace whose status names it with no namespace, and
    // whose parentRef now names it with its namespace.
    let live = HTTPRouteStatus {
        parents: vec![parent(GATEWAY_CONTROLLER, None, "web")],
    };
    let wanted = wanted_route(2);
    let status = route(Some(&live), "edge", &[web()], Some(&wanted), at(50));
    assert_eq!(status.parents.len(), 1);
    assert_eq!(
        status.parents[0].conditions[0].last_transition_time,
        Time(at(10)),
        "taken for another parent"
    );
}

#[test]
fn a_routes_parents_are_as_many_as_its_status_holds() {
    let live = HTTPRouteStatus {
        parents: (0..MOST_PARENTS)
            .map(|n| parent("example.com/other", Some("edge"), &format!("gateway-{n}")))
            .collect(),
    };
    let status = route(
        Some(&live),
        "shop",
        &[web()],
        Some(&wanted_route(2)),
        at(50),
    );
    assert_eq!(status.parents.len(), MOST_PARENTS);
    assert!(
        status
            .parents
            .iter()
            .all(|p| p.controller_name == "example.com/other")
    );
}

#[test]
fn an_address_is_an_ip_address_or_else_a_host_name() {
    let said = addresses(&[
        crate::status::address("192.0.2.10"),
        crate::status::address("2001:db8::1"),
        crate::status::address("lb.example.com"),
        IngressLoadBalancerIngress::default(),
    ]);
    let said: Vec<(Option<&str>, &str)> = said
        .iter()
        .map(|address| (address.r#type.as_deref(), address.value.as_str()))
        .collect();
    assert_eq!(
        said,
        [
            (Some("IPAddress"), "192.0.2.10"),
            (Some("IPAddress"), "2001:db8::1"),
            (Some("Hostname"), "lb.example.com"),
        ]
    );
}

#[test]
fn programmed_once_every_ready_pod_runs_the_generation_and_false_once_overdue_or_refused() {
    let pod = |last: u8| IpAddr::from([10, 0, 0, last]);
    let running = |generation: i64| Pod {
        runs: Some(generation),
        ..Pod::default()
    };
    let said = |p: &Programmed| (p.status, p.reason, p.message.clone());
    let pods = Pods::from([(pod(1), running(4)), (pod(2), running(3))]);
    let ready = [pod(1), pod(2)];
    assert_eq!(
        said(&Programmed::of(4, &pods, Some(&ready), false)),
        (
            "Unknown",
            "Pending",
            "1 of 2 data plane pods run generation 4".to_owned()
        )
    );
    assert_eq!(
        said(&Programmed::of(4, &pods, Some(&ready), true)).0,
        "False",
        "overdue"
    );
    assert_eq!(
        said(&Programmed::of(4, &pods, Some(&ready[..1]), false)).0,
        "True"
    );
    assert_eq!(
        said(&Programmed::of(4, &pods, Some(&ready[..1]), false)).1,
        "Programmed"
    );
    // A ready pod no replica reports runs nothing; without the ready pods known, those
    // reported are taken.
    assert_eq!(
        said(&Programmed::of(4, &pods, Some(&[pod(1), pod(9)]), false)).2,
        "1 of 2 data plane pods run generation 4"
    );
    let all = Pods::from([(pod(1), running(4)), (pod(2), running(4))]);
    assert_eq!(said(&Programmed::of(4, &all, None, false)).0, "True");
    assert_eq!(
        said(&Programmed::of(4, &Pods::new(), Some(&[]), true)),
        (
            "False",
            "Pending",
            "0 of 0 data plane pods run generation 4".to_owned()
        ),
        "no pod at all is not programmed"
    );
    // A ready pod that refused the latest config makes it invalid; one not ready, nothing.
    let refusing = Pods::from([
        (pod(1), running(4)),
        (
            pod(2),
            Pod {
                runs: Some(3),
                refused: Some("an unknown field".to_owned()),
                ..Pod::default()
            },
        ),
    ]);
    assert_eq!(
        said(&Programmed::of(4, &refusing, Some(&ready), false)),
        (
            "False",
            "Invalid",
            "data plane 10.0.0.2 refused the config: an unknown field".to_owned()
        )
    );
    assert_eq!(
        said(&Programmed::of(4, &refusing, Some(&ready[..1]), false)).0,
        "True"
    );
}
