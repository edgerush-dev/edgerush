use super::*;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointHints, ForZone};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use proptest::prelude::*;

/// Service `web` of namespace `shop`, port `http`.
fn web() -> KubernetesService {
    KubernetesService::parse("web.shop.svc.cluster.local:http").unwrap()
}

/// A slice of Service `web` in `shop`, of this address type, ports and endpoints, each
/// written as YAML is.
fn slice(address_type: &str, ports: &str, endpoints: &str) -> EndpointSlice {
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: web-1, namespace: shop, labels: {{ kubernetes.io/service-name: web }} }}\naddressType: {address_type}\nports: {ports}\nendpoints:\n{endpoints}"
    ))
    .unwrap()
}

/// A slice of `web` whose port `http` is 8080, of these IPv4 endpoints.
fn http(endpoints: &str) -> EndpointSlice {
    slice(
        "IPv4",
        "[{ name: http, port: 8080, protocol: TCP }]",
        endpoints,
    )
}

fn addresses(set: &BTreeSet<SocketAddr>) -> Vec<String> {
    set.iter().map(ToString::to_string).collect()
}

#[test]
fn conditions_left_out_are_read_as_the_api_asks() {
    // `ready` unknown is ready; `serving` unknown is `ready`'s value; `terminating`
    // unknown is not terminating.
    let endpoints = Endpoints::of(
        &web(),
        &[http(
            "  - { addresses: [10.0.0.1] }
  - { addresses: [10.0.0.2], conditions: {} }
  - { addresses: [10.0.0.3], conditions: { serving: false } }
  - { addresses: [10.0.0.4], conditions: { ready: false, terminating: true } }
",
        )],
    );
    assert_eq!(
        addresses(endpoints.ready()),
        ["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"]
    );
    // Not ready and terminating, with `serving` unknown: it defers to `ready`, so it does
    // not serve.
    assert!(endpoints.terminating().is_empty());
}

#[test]
fn an_endpoint_neither_ready_nor_serving_while_terminating_takes_nothing() {
    let endpoints = Endpoints::of(
        &web(),
        &[http(
            "  - { addresses: [10.0.0.1], conditions: { ready: false } }
  - { addresses: [10.0.0.2], conditions: { ready: false, serving: true } }
  - { addresses: [10.0.0.3], conditions: { ready: false, serving: false, terminating: true } }
",
        )],
    );
    assert_eq!(endpoints, Endpoints::default());
}

#[test]
fn terminating_endpoints_that_serve_are_used_only_while_none_is_ready() {
    let terminating = "  - { addresses: [10.0.0.1], conditions: { ready: false, serving: true, terminating: true } }\n";
    let alone = Endpoints::of(&web(), &[http(terminating)]);
    assert!(alone.ready().is_empty());
    assert_eq!(addresses(alone.terminating()), ["10.0.0.1:8080"]);
    assert_eq!(addresses(alone.in_use()), ["10.0.0.1:8080"]);

    let with_ready = Endpoints::of(
        &web(),
        &[http(&format!(
            "{terminating}  - {{ addresses: [10.0.0.2], conditions: {{ ready: true }} }}\n"
        ))],
    );
    assert_eq!(addresses(with_ready.terminating()), ["10.0.0.1:8080"]);
    assert_eq!(addresses(with_ready.in_use()), ["10.0.0.2:8080"]);
}

#[test]
fn a_ready_endpoint_that_is_terminating_is_ready() {
    // As a Service that publishes addresses not ready has it: `ready` is the endpoint
    // system's word, and is taken.
    let endpoints = Endpoints::of(
        &web(),
        &[http(
            "  - { addresses: [10.0.0.1], conditions: { ready: true, terminating: true } }\n",
        )],
    );
    assert_eq!(addresses(endpoints.ready()), ["10.0.0.1:8080"]);
}

#[test]
fn only_an_endpoints_first_address_is_used() {
    let endpoints = Endpoints::of(&web(), &[http("  - { addresses: [10.0.0.1, 10.0.0.2] }\n")]);
    assert_eq!(addresses(endpoints.ready()), ["10.0.0.1:8080"]);
}

#[test]
fn only_the_services_own_slices_count() {
    let mut other_service = http("  - { addresses: [10.0.0.2] }\n");
    other_service
        .metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(SERVICE_NAME_LABEL.to_owned(), "api".to_owned());
    let mut other_namespace = http("  - { addresses: [10.0.0.3] }\n");
    other_namespace.metadata.namespace = Some("blog".to_owned());
    let mut unlabelled = http("  - { addresses: [10.0.0.4] }\n");
    unlabelled.metadata.labels = None;
    let endpoints = Endpoints::of(
        &web(),
        &[
            http("  - { addresses: [10.0.0.1] }\n"),
            other_service,
            other_namespace,
            unlabelled,
        ],
    );
    assert_eq!(addresses(endpoints.ready()), ["10.0.0.1:8080"]);
}

#[test]
fn the_port_is_each_slices_own_for_the_services_port_name() {
    let endpoint = "  - { addresses: [10.0.0.1] }\n";
    let endpoints = Endpoints::of(
        &web(),
        &[
            slice(
                "IPv4",
                "[{ name: admin, port: 9000 }, { name: http, port: 8080 }]",
                endpoint,
            ),
            // The same pod's slice from before its port changed: both are endpoints.
            slice("IPv4", "[{ name: http, port: 80 }]", endpoint),
        ],
    );
    assert_eq!(
        addresses(endpoints.ready()),
        ["10.0.0.1:80", "10.0.0.1:8080"]
    );
}

#[test]
fn a_slice_without_the_port_over_tcp_with_a_number_gives_nothing() {
    let endpoint = "  - { addresses: [10.0.0.1] }\n";
    for ports in [
        "[{ name: admin, port: 9000 }]",
        "[{ name: http, port: 8080, protocol: UDP }]",
        "[{ name: http }]",
        "[{ name: http, port: 0 }]",
        "[{ name: http, port: 70000 }]",
        "[]",
    ] {
        let endpoints = Endpoints::of(&web(), &[slice("IPv4", ports, endpoint)]);
        assert_eq!(endpoints, Endpoints::default(), "{ports}");
    }
}

#[test]
fn the_services_only_unnamed_port_is_the_slices_port_without_a_name() {
    let only = KubernetesService::parse("web.shop.svc.cluster.local:80").unwrap();
    let endpoint = "  - { addresses: [10.0.0.1] }\n";
    for ports in ["[{ port: 8080 }]", "[{ name: \"\", port: 8080 }]"] {
        let endpoints = Endpoints::of(&only, &[slice("IPv4", ports, endpoint)]);
        assert_eq!(addresses(endpoints.ready()), ["10.0.0.1:8080"], "{ports}");
    }
    let named = Endpoints::of(&only, &[http(endpoint)]);
    assert_eq!(named, Endpoints::default());
}

#[test]
fn ipv4_and_ipv6_slices_both_count_and_fqdn_slices_do_not() {
    let ports = "[{ name: http, port: 8080 }]";
    let endpoints = Endpoints::of(
        &web(),
        &[
            slice("IPv4", ports, "  - { addresses: [10.0.0.1] }\n"),
            slice("IPv6", ports, "  - { addresses: [\"fd00::1\"] }\n"),
            slice("FQDN", ports, "  - { addresses: [web.example.com] }\n"),
            // An address not of its slice's family is not taken for one.
            slice("IPv4", ports, "  - { addresses: [\"fd00::2\"] }\n"),
            slice("IPv6", ports, "  - { addresses: [10.0.0.2] }\n"),
        ],
    );
    assert_eq!(
        addresses(endpoints.ready()),
        ["10.0.0.1:8080", "[fd00::1]:8080"]
    );
}

#[test]
fn an_address_listed_twice_is_one_endpoint_the_readiest() {
    let endpoints = Endpoints::of(
        &web(),
        &[
            http(
                "  - { addresses: [10.0.0.1], conditions: { ready: true } }\n  - { addresses: [10.0.0.2], conditions: { ready: false, serving: true, terminating: true } }\n",
            ),
            http(
                "  - { addresses: [10.0.0.1], conditions: { ready: false, serving: true, terminating: true } }\n  - { addresses: [10.0.0.1], conditions: { ready: false } }\n",
            ),
            http("  - { addresses: [10.0.0.2], conditions: { ready: false } }\n"),
        ],
    );
    assert_eq!(addresses(endpoints.ready()), ["10.0.0.1:8080"]);
    assert_eq!(addresses(endpoints.terminating()), ["10.0.0.2:8080"]);
}

fn zone(endpoints: &Endpoints, zone: &str) -> Option<Vec<String>> {
    endpoints.for_zone(zone).as_ref().map(addresses)
}

#[test]
fn the_zones_endpoints_are_preferred_when_every_one_is_hinted() {
    let endpoints = Endpoints::of(
        &web(),
        &[http(
            "  - { addresses: [10.0.0.1], zone: a, hints: { forZones: [{ name: a }] } }
  - { addresses: [10.0.0.2], zone: b, hints: { forZones: [{ name: b }] } }
  - { addresses: [10.0.0.3], zone: c, hints: { forZones: [{ name: a }, { name: c }] } }
",
        )],
    );
    assert_eq!(
        zone(&endpoints, "a"),
        Some(vec!["10.0.0.1:8080".to_owned(), "10.0.0.3:8080".to_owned()])
    );
    assert_eq!(
        zone(&endpoints, "b"),
        Some(vec!["10.0.0.2:8080".to_owned()])
    );
    // No endpoint hinted for the zone: all are used alike.
    assert_eq!(zone(&endpoints, "d"), None);
}

#[test]
fn one_endpoint_without_a_zone_hint_turns_preference_off() {
    // A Service partly hinted is one whose hints are being written.
    for unhinted in [
        "{ addresses: [10.0.0.2], zone: a }",
        "{ addresses: [10.0.0.2], zone: a, hints: {} }",
        "{ addresses: [10.0.0.2], zone: a, hints: { forZones: [] } }",
    ] {
        let endpoints = Endpoints::of(
            &web(),
            &[http(&format!(
                "  - {{ addresses: [10.0.0.1], zone: a, hints: {{ forZones: [{{ name: a }}] }} }}\n  - {unhinted}\n"
            ))],
        );
        assert_eq!(zone(&endpoints, "a"), None, "{unhinted}");
    }
}

#[test]
fn only_the_endpoints_in_use_count_for_hints() {
    // Hints are judged among those in use: an unhinted endpoint terminating beside ready
    // ones does not count, and the terminating are preferred among while none is ready.
    let terminating = "  - { addresses: [10.0.0.9], conditions: { ready: false, serving: true, terminating: true }, hints: { forZones: [{ name: a }] } }\n";
    let with_ready = Endpoints::of(
        &web(),
        &[http(&format!(
            "{terminating}  - {{ addresses: [10.0.0.1], hints: {{ forZones: [{{ name: a }}] }} }}\n  - {{ addresses: [10.0.0.2], conditions: {{ ready: false, serving: true, terminating: true }} }}\n"
        ))],
    );
    assert_eq!(
        zone(&with_ready, "a"),
        Some(vec!["10.0.0.1:8080".to_owned()])
    );
    let alone = Endpoints::of(&web(), &[http(terminating)]);
    assert_eq!(zone(&alone, "a"), Some(vec!["10.0.0.9:8080".to_owned()]));
}

#[test]
fn an_address_listed_twice_is_hinted_only_if_every_copy_is() {
    let hinted = |zone: &str| {
        http(&format!(
            "  - {{ addresses: [10.0.0.1], hints: {{ forZones: [{{ name: {zone} }}] }} }}\n"
        ))
    };
    let both = Endpoints::of(&web(), &[hinted("a"), hinted("b")]);
    assert_eq!(zone(&both, "a"), Some(vec!["10.0.0.1:8080".to_owned()]));
    assert_eq!(zone(&both, "b"), Some(vec!["10.0.0.1:8080".to_owned()]));
    let unhinted = http("  - { addresses: [10.0.0.1] }\n");
    for slices in [[hinted("a"), unhinted.clone()], [unhinted, hinted("a")]] {
        assert_eq!(zone(&Endpoints::of(&web(), &slices), "a"), None);
    }
}

fn ip(address: &str) -> IpAddr {
    address.parse().unwrap()
}

#[test]
fn the_data_planes_zone_is_that_of_the_endpoint_at_its_own_address() {
    let slices = [
        // Another Service's slice, listing the pod too, is passed over.
        {
            let mut other = http("  - { addresses: [10.0.0.7], zone: wrong }\n");
            other.metadata.labels =
                Some([(SERVICE_NAME_LABEL.to_owned(), "api".to_owned())].into());
            other
        },
        // Not ready yet, and on no port the Service names: still the pod.
        slice(
            "IPv4",
            "[{ name: other, port: 1 }]",
            "  - { addresses: [10.0.0.6], zone: b }\n  - { addresses: [10.0.0.7], zone: a, conditions: { ready: false } }\n",
        ),
        slice("IPv6", "[]", "  - { addresses: [\"fd00::7\"], zone: a6 }\n"),
    ];
    assert_eq!(
        zone_of("shop", "web", &slices, &[ip("10.0.0.7")]),
        Some("a")
    );
    assert_eq!(
        zone_of("shop", "web", &slices, &[ip("fd00::7")]),
        Some("a6")
    );
    assert_eq!(zone_of("shop", "web", &slices, &[ip("10.0.0.8")]), None);
    assert_eq!(zone_of("blog", "web", &slices, &[ip("10.0.0.7")]), None);
    // Listed without a zone: none, as yet.
    let unzoned = [http("  - { addresses: [10.0.0.7] }\n")];
    assert_eq!(zone_of("shop", "web", &unzoned, &[ip("10.0.0.7")]), None);
}

/// Slices of a few addresses, ports and every mix of conditions, some of them `web`'s.
fn slices() -> impl Strategy<Value = Vec<EndpointSlice>> {
    // Each kind of endpoint often, and every mix of conditions too.
    let condition = || prop::option::of(any::<bool>());
    let conditions = prop_oneof![
        Just(None),
        Just(Some((Some(true), None, None))),
        Just(Some((Some(false), Some(true), Some(true)))),
        Just(Some((Some(false), None, None))),
        prop::option::of((condition(), condition(), condition())),
    ];
    // No hint, or hints for zones `a` and `b`, or none of them.
    let hints = prop::option::of(prop::collection::btree_set(
        prop::sample::select(vec!["a", "b"]),
        0..3,
    ));
    let endpoint = (0..4u8, conditions, hints);
    let one = (
        any::<bool>(),
        prop::sample::select(vec![("http", 8080), ("http", 9090), ("admin", 9000)]),
        any::<bool>(),
        prop::collection::vec(endpoint, 0..6),
    );
    prop::collection::vec(one, 0..5).prop_map(|slices| {
        slices
            .into_iter()
            .map(|(v6, (name, port), ours, endpoints)| EndpointSlice {
                metadata: ObjectMeta {
                    namespace: Some("shop".to_owned()),
                    labels: Some(
                        [(
                            SERVICE_NAME_LABEL.to_owned(),
                            if ours { "web" } else { "api" }.to_owned(),
                        )]
                        .into(),
                    ),
                    ..ObjectMeta::default()
                },
                address_type: if v6 { "IPv6" } else { "IPv4" }.to_owned(),
                ports: Some(vec![EndpointPort {
                    name: Some(name.to_owned()),
                    port: Some(port),
                    ..EndpointPort::default()
                }]),
                endpoints: Some(
                    endpoints
                        .into_iter()
                        .map(|(host, conditions, hints)| Endpoint {
                            addresses: vec![if v6 {
                                format!("fd00::{host}")
                            } else {
                                format!("10.0.0.{host}")
                            }],
                            conditions: conditions.map(|(ready, serving, terminating)| {
                                EndpointConditions {
                                    ready,
                                    serving,
                                    terminating,
                                }
                            }),
                            hints: hints.map(|zones| EndpointHints {
                                for_zones: Some(
                                    zones
                                        .into_iter()
                                        .map(|name| ForZone {
                                            name: name.to_owned(),
                                        })
                                        .collect(),
                                ),
                                ..EndpointHints::default()
                            }),
                            ..Endpoint::default()
                        })
                        .collect(),
                ),
            })
            .collect()
    })
}

proptest! {
    /// The slices' order, and a slice seen twice, change nothing: a watch delivers them in
    /// any order, and a slice being replaced may be seen beside its replacement.
    #[test]
    fn order_and_repeats_change_nothing(slices in slices(), seed in any::<u64>()) {
        let endpoints = Endpoints::of(&web(), &slices);
        let mut shuffled = slices.clone();
        let len = shuffled.len().max(1) as u64;
        shuffled.rotate_left((seed % len) as usize);
        shuffled.reverse();
        prop_assert_eq!(&Endpoints::of(&web(), &shuffled), &endpoints);
        let mut repeated = shuffled.clone();
        repeated.extend(slices.iter().cloned());
        prop_assert_eq!(&Endpoints::of(&web(), &repeated), &endpoints);
    }

    /// An address is ready if any copy of it is, terminating if the readiest copy is
    /// terminating and serving, and never both; what is in use is the ready, or the
    /// terminating while none is ready.
    #[test]
    fn the_readiest_copy_decides(slices in slices()) {
        let endpoints = Endpoints::of(&web(), &slices);
        let mut fits: BTreeMap<SocketAddr, Vec<Option<Fit>>> = BTreeMap::new();
        for slice in &slices {
            if !is_of("shop", "web", slice) {
                continue;
            }
            let Some(port) = slice.ports.iter().flatten().find(|p| p.name.as_deref() == Some("http")) else {
                continue;
            };
            for endpoint in slice.endpoints.iter().flatten() {
                let ip = endpoint.addresses[0].parse::<IpAddr>().unwrap();
                let address = SocketAddr::new(ip, u16::try_from(port.port.unwrap()).unwrap());
                fits.entry(address).or_default().push(fit_of(endpoint.conditions.as_ref()));
            }
        }
        for (address, copies) in &fits {
            let ready = copies.contains(&Some(Fit::Ready));
            let terminating = !ready && copies.contains(&Some(Fit::Terminating));
            prop_assert_eq!(endpoints.ready().contains(address), ready);
            prop_assert_eq!(endpoints.terminating().contains(address), terminating);
        }
        for address in endpoints.ready().iter().chain(endpoints.terminating()) {
            prop_assert!(fits.contains_key(address));
        }
        let in_use = if endpoints.ready().is_empty() { endpoints.terminating() } else { endpoints.ready() };
        prop_assert_eq!(endpoints.in_use(), in_use);
    }

    /// A zone's endpoints are preferred exactly when every one in use has a zone hint in
    /// every copy and one names the zone; then they are those in use hinted for it in some
    /// copy.
    #[test]
    fn hints_count_only_when_every_copy_in_use_has_one(
        slices in slices(),
        wanted in prop::sample::select(vec!["a", "b", "c"]),
    ) {
        let endpoints = Endpoints::of(&web(), &slices);
        let mut copies: BTreeMap<SocketAddr, Vec<Option<BTreeSet<String>>>> = BTreeMap::new();
        for slice in &slices {
            if !is_of("shop", "web", slice) {
                continue;
            }
            let Some(port) = slice.ports.iter().flatten().find(|p| p.name.as_deref() == Some("http")) else {
                continue;
            };
            for endpoint in slice.endpoints.iter().flatten() {
                if fit_of(endpoint.conditions.as_ref()).is_none() {
                    continue;
                }
                let ip = endpoint.addresses[0].parse::<IpAddr>().unwrap();
                let address = SocketAddr::new(ip, u16::try_from(port.port.unwrap()).unwrap());
                let zones = endpoint
                    .hints
                    .as_ref()
                    .and_then(|hints| hints.for_zones.as_ref())
                    .map(|zones| zones.iter().map(|zone| zone.name.clone()).collect::<BTreeSet<_>>())
                    .filter(|zones| !zones.is_empty());
                copies.entry(address).or_default().push(zones);
            }
        }
        let in_use = endpoints.in_use();
        let every_hinted = in_use
            .iter()
            .all(|address| copies[address].iter().all(Option::is_some));
        let named: BTreeSet<SocketAddr> = in_use
            .iter()
            .copied()
            .filter(|address| copies[address].iter().flatten().any(|zones| zones.contains(wanted)))
            .collect();
        let expected = (every_hinted && !named.is_empty()).then_some(named);
        prop_assert_eq!(endpoints.for_zone(wanted), expected);
    }
}
