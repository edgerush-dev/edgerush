use super::*;
use k8s_openapi::api::discovery::v1::Endpoint;
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
    let endpoint = (0..4u8, conditions);
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
                        .map(|(host, conditions)| Endpoint {
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
            if !is_of(&web(), slice) {
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
}
