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
    older[2].1 = Served::Served {
        release: Some("v1.5.1".to_owned()),
    };
    older[3].1 = Served::Missing;
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
                programmable: Ok(()),
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
    assert_eq!(status.attached_listener_sets, Some(3), "not ours, kept");
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
    let status = route(Some(&live), "shop", &web(), Some(&wanted_route(2)), at(50));
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
    let gone = route(Some(&live), "shop", &web(), None, at(60));
    assert_eq!(gone.parents, [theirs, another_plane]);
}

#[test]
fn a_parent_of_the_routes_own_namespace_is_the_same_written_either_way() {
    // A route of the Gateway's own namespace whose status names it with no namespace, and
    // whose parentRef now names it with its namespace.
    let live = HTTPRouteStatus {
        parents: vec![parent(GATEWAY_CONTROLLER, None, "web")],
    };
    let wanted = wanted_route(2);
    let status = route(Some(&live), "edge", &web(), Some(&wanted), at(50));
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
    let status = route(Some(&live), "shop", &web(), Some(&wanted_route(2)), at(50));
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
        refused: None,
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
