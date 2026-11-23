use super::*;
use proptest::prelude::*;

/// A config of these listeners, as `(port, protocol, http3)`, each on IPv4 and, where
/// `dual`, on IPv6 too.
fn config(listeners: &[(u16, &str, bool)], dual: bool) -> Config {
    // None is an empty mapping: a key with nothing after it is null, not a mapping.
    let mut written = String::from("routes: []\nupstreams: {}\nlisteners:");
    written.push_str(if listeners.is_empty() { " {}\n" } else { "\n" });
    for (index, (port, protocol, http3)) in listeners.iter().enumerate() {
        let http3 = if *http3 { ", http3: {}" } else { "" };
        written.push_str(&format!(
            "  l{index}: {{ address: '0.0.0.0:{port}', protocol: {protocol}{http3} }}\n"
        ));
        if dual {
            written.push_str(&format!(
                "  l{index}v6: {{ address: '[::]:{port}', protocol: {protocol}{http3} }}\n"
            ));
        }
    }
    serde_saphyr::from_str(&written).unwrap()
}

const TCP: Transports = Transports {
    tcp: true,
    udp: false,
};
const UDP: Transports = Transports {
    tcp: false,
    udp: true,
};
const BOTH: Transports = Transports {
    tcp: true,
    udp: true,
};

#[test]
fn http3_and_udp_listeners_are_bound_over_udp_and_the_rest_over_tcp() {
    let config = config(
        &[
            (80, "http", false),
            (443, "https", true),
            (5353, "udp", false),
            (5432, "tcp", false),
            (8443, "tls", false),
        ],
        false,
    );
    assert_eq!(
        bound(&config),
        BTreeMap::from([
            (80, TCP),
            (443, BOTH),
            (5353, UDP),
            (5432, TCP),
            (8443, TCP),
        ])
    );
}

#[test]
fn a_udp_listener_on_an_http_ones_port_is_both() {
    let config = config(&[(53, "udp", false), (53, "http", false)], false);
    assert_eq!(bound(&config), BTreeMap::from([(53, BOTH)]));
}

proptest! {
    /// Each port of a listener is bound once, whatever the family, and only those.
    #[test]
    fn every_listeners_port_is_bound_once(
        listeners in proptest::collection::btree_map(
            1u16..,
            (prop_oneof![Just("http"), Just("https"), Just("tcp"), Just("tls"), Just("udp")], any::<bool>()),
            0..8,
        ),
        dual in any::<bool>(),
    ) {
        let listed: Vec<(u16, &str, bool)> = listeners
            .iter()
            .map(|(port, (protocol, http3))| (*port, *protocol, *http3 && *protocol == "https"))
            .collect();
        let bound = bound(&config(&listed, dual));
        prop_assert_eq!(bound.keys().copied().collect::<Vec<_>>(), listeners.keys().copied().collect::<Vec<_>>());
        for (port, protocol, http3) in listed {
            let transports = bound[&port];
            prop_assert_eq!(transports.udp, protocol == "udp" || http3);
            prop_assert_eq!(transports.tcp, protocol != "udp");
        }
    }
}

fn service(yaml: &str) -> Service {
    serde_saphyr::from_str(yaml).unwrap()
}

/// What the control plane wants of `edgerush-proxy-web`.
fn wanted() -> Service {
    service(
        "metadata:
  name: edgerush-proxy-web
  namespace: edge
  labels: { app.kubernetes.io/name: edgerush, cost-centre: payments }
  annotations: { service.beta.kubernetes.io/aws-load-balancer-type: nlb }
  ownerReferences: [{ apiVersion: gateway.edgerush.dev/v1alpha1, kind: DataPlane, name: web, uid: dp-uid, controller: true }]
spec:
  type: LoadBalancer
  selector: { app.kubernetes.io/name: edgerush }
  ports:
    - { name: port-80, port: 80, protocol: TCP, targetPort: 80 }
    - { name: port-443-udp, port: 443, protocol: UDP, targetPort: 443 }
  ipFamilyPolicy: SingleStack
  ipFamilies: [IPv4]
  externalTrafficPolicy: Local
  sessionAffinity: None
  loadBalancerSourceRanges: [203.0.113.0/24, 198.51.100.0/24]",
    )
}

/// `wanted` as the API server keeps it: with what it fills in, and what others add.
fn live() -> Service {
    let mut live = wanted();
    live.metadata.resource_version = Some("7".to_owned());
    live.metadata
        .labels
        .get_or_insert_default()
        .insert("added-by-hand".to_owned(), "yes".to_owned());
    let spec = live.spec.get_or_insert_default();
    spec.cluster_ip = Some("10.96.0.10".to_owned());
    spec.cluster_ips = Some(vec!["10.96.0.10".to_owned()]);
    spec.health_check_node_port = Some(31000);
    spec.load_balancer_source_ranges = Some(vec![
        "198.51.100.0/24".to_owned(),
        "203.0.113.0/24".to_owned(),
    ]);
    if let Some(ports) = &mut spec.ports {
        for (index, port) in ports.iter_mut().enumerate() {
            port.node_port = Some(30000 + i32::try_from(index).unwrap());
        }
    }
    live
}

#[test]
fn a_service_as_wanted_with_what_the_api_server_and_others_add_is_not_written_again() {
    assert!(!differs(&live(), &wanted()));
}

#[test]
fn a_service_edited_in_what_is_wanted_of_it_differs() {
    // What is edited, and how.
    type Edit = (&'static str, fn(&mut Service));
    let edits: [Edit; 9] = [
        ("a label", |s| {
            s.metadata
                .labels
                .get_or_insert_default()
                .insert("cost-centre".to_owned(), "other".to_owned());
        }),
        ("an annotation", |s| s.metadata.annotations = None),
        ("its owner", |s| s.metadata.owner_references = None),
        ("its type", |s| {
            s.spec.get_or_insert_default().type_ = Some("ClusterIP".to_owned());
        }),
        ("a port", |s| {
            if let Some(ports) = &mut s.spec.get_or_insert_default().ports {
                ports.pop();
            }
        }),
        ("a port's protocol", |s| {
            if let Some(ports) = &mut s.spec.get_or_insert_default().ports {
                ports[1].protocol = Some("TCP".to_owned());
            }
        }),
        ("its traffic policy", |s| {
            s.spec.get_or_insert_default().external_traffic_policy = Some("Cluster".to_owned());
        }),
        ("its affinity", |s| {
            s.spec.get_or_insert_default().session_affinity = Some("ClientIP".to_owned());
        }),
        ("its source ranges", |s| {
            s.spec.get_or_insert_default().load_balancer_source_ranges = None;
        }),
    ];
    for (what, edit) in edits {
        let mut live = live();
        edit(&mut live);
        assert!(differs(&live, &wanted()), "{what} edited is not seen");
    }
}

#[test]
fn a_port_added_to_what_is_wanted_differs() {
    let mut wanted = wanted();
    if let Some(ports) = &mut wanted.spec.get_or_insert_default().ports {
        ports.push(
            serde_saphyr::from_str(
                "{ name: port-8080, port: 8080, protocol: TCP, targetPort: 8080 }",
            )
            .unwrap(),
        );
    }
    assert!(differs(&live(), &wanted));
}

#[test]
fn a_service_is_ours_by_its_owner() {
    assert!(ours(&live(), "dp-uid"));
    assert!(!ours(&live(), "another-uid"));
    let mut by_hand = live();
    by_hand.metadata.owner_references = None;
    assert!(!ours(&by_hand, "dp-uid"));
}

#[test]
fn a_load_balancer_gives_its_balancers_addresses_and_none_while_pending() {
    let mut balanced = live();
    balanced.status = Some(
        serde_saphyr::from_str(
            "loadBalancer: { ingress: [{ ip: 203.0.113.7 }, { hostname: lb.example }] }",
        )
        .unwrap(),
    );
    assert_eq!(addresses(Some(&balanced)), ["203.0.113.7", "lb.example"]);
    assert!(
        addresses(Some(&live())).is_empty(),
        "a pending balancer gave its cluster IP"
    );
    assert!(addresses(None).is_empty());
}

#[test]
fn a_service_of_another_type_gives_its_cluster_ips() {
    for type_ in ["ClusterIP", "NodePort"] {
        let mut inside = live();
        let spec = inside.spec.get_or_insert_default();
        spec.type_ = Some(type_.to_owned());
        spec.cluster_ips = Some(vec!["10.96.0.10".to_owned(), "fd00::10".to_owned()]);
        assert_eq!(
            addresses(Some(&inside)),
            ["10.96.0.10", "fd00::10"],
            "{type_}"
        );
    }
    let mut headless = live();
    let spec = headless.spec.get_or_insert_default();
    spec.type_ = Some("ClusterIP".to_owned());
    spec.cluster_ips = Some(vec!["None".to_owned()]);
    assert!(addresses(Some(&headless)).is_empty());
}

#[test]
fn the_ports_listed_are_said_by_number_and_protocol() {
    assert_eq!(listed(&wanted()), ["80/TCP", "443/UDP"]);
}
