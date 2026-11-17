#![allow(
    clippy::unwrap_used,
    reason = "tests: a DataPlane that cannot be read is a broken test"
)]

use super::*;
use edgerush_config::{AccessLog, Forwarding, Http3, ListenerProxyProtocol, RequestId};
use edgerush_translate::{GatewayName, IpFamily, PortSettings};

/// A DataPlane read from YAML, as the API server would give it.
fn data_plane(yaml: &str) -> DataPlane {
    serde_saphyr::from_str(yaml).unwrap()
}

/// `web` in `edge`, of ControlPlane `edge`, at `generation`, with `spec` and `status`.
fn web(generation: i64, spec: &str, status: &str) -> DataPlane {
    data_plane(&format!(
        "metadata: {{ name: web, namespace: edge, generation: {generation}, uid: u1, creationTimestamp: \"2026-11-12T10:00:00Z\" }}\nspec:\n  controlPlaneRef: {{ name: edge }}\n{spec}{status}"
    ))
}

/// A status of the operator's `Accepted`, `status` as of `generation`.
fn accepted(status: &str, generation: i64) -> String {
    format!(
        "status:\n  conditions:\n    - {{ type: Accepted, status: \"{status}\", reason: R, message: \"\", lastTransitionTime: \"2026-11-12T10:00:00Z\", observedGeneration: {generation} }}\n"
    )
}

fn own() -> Own {
    Own {
        control_plane: "edge".to_owned(),
        namespace: "edge".to_owned(),
        cluster_domain: "cluster.example".to_owned(),
        system_authorities: vec!["SYSTEM".to_owned()],
    }
}

const BOTH: &str = "  ingress: { httpPort: 8080, httpsPort: 8443, hostFallThrough: true, httpsRedirect: false, httpsRedirectMaxAge: 60 }\n  gateway:\n    listeners: [{ name: http, port: 80, protocol: HTTP }]\n    tls: { frontend: { default: {} } }\n  ipFamily: Dual\n";

#[test]
fn a_data_plane_serving_both_is_its_settings_field_by_field() {
    let plane = plane(&web(1, BOTH, ""), &own()).unwrap();
    // The stream knows its data plane by the ID the certificate written for it carries.
    let issuer = crate::issuing::Issuer {
        namespace: "edge".to_owned(),
        scope: crate::WatchScope::Parts(vec![edgerush_kubernetes::Scope::Cluster]),
        control_plane: "edge".to_owned(),
        cluster_domain: "cluster.example".to_owned(),
        authority: "edge-authority".to_owned(),
        kind: edgerush_certificates::authority::Kind::Made,
    };
    assert_eq!(
        plane.id.as_deref(),
        Some("spiffe://edge.cluster.example/ns/edge/sa/edgerush-proxy-web")
    );
    assert_eq!(
        plane.id,
        Some(issuer.data_plane_leaf("edge", "web").spiffe_id)
    );
    assert_eq!(plane.ingress_class.as_deref(), Some("edge.web"));
    assert_eq!(
        plane.gateway,
        Some(GatewayName {
            namespace: "edge".to_owned(),
            name: "web".to_owned()
        })
    );
    assert_eq!(plane.ip_family, IpFamily::Dual);
    assert_eq!(
        (plane.ingress_http_port, plane.ingress_https_port),
        (8080, 8443)
    );
    assert!(plane.ingress_hosts_fall_through);
    assert!(!plane.ingress_https_redirect);
    assert_eq!(plane.ingress_https_redirect_max_age, 60);
    assert_eq!(plane.cluster_domain, "cluster.example");
    assert_eq!(plane.system_authorities, ["SYSTEM"]);
    // What a DataPlane has no field for stays at its default (05 §3), but for the Service
    // of its own pods, its own Service.
    assert!(plane.backend_authorities.is_empty());
    assert_eq!(
        plane.data_plane,
        edgerush_config::DataPlane {
            own_service: Some("edgerush-proxy-web.edge.svc.cluster.example".to_owned()),
            ..edgerush_config::DataPlane::default()
        }
    );
    assert!(plane.ports.is_empty());
}

#[test]
fn a_data_plane_serves_only_the_apis_it_names() {
    let ingress = plane(
        &web(1, "  ingress: { httpPort: 80, httpsPort: 443 }\n", ""),
        &own(),
    )
    .unwrap();
    assert_eq!(ingress.ingress_class.as_deref(), Some("edge.web"));
    assert_eq!(ingress.gateway, None);
    // Of the defaults the CRD gives, as the API server would have written them.
    assert!(!ingress.ingress_hosts_fall_through);
    assert!(ingress.ingress_https_redirect);
    assert_eq!(ingress.ingress_https_redirect_max_age, 30);
    assert_eq!(ingress.ip_family, IpFamily::Ipv4);
    let gateway = plane(
        &web(
            1,
            "  gateway:\n    listeners: [{ name: http, port: 80, protocol: HTTP }]\n    tls: { frontend: { default: {} } }\n",
            "",
        ),
        &own(),
    )
    .unwrap();
    assert_eq!(gateway.ingress_class, None);
    assert!(gateway.gateway.is_some());
}

#[test]
fn each_ports_settings_are_its_listeners() {
    let plane = plane(
        &web(
            1,
            "  ingress: { httpPort: 80, httpsPort: 443 }\n  ports:\n    - port: 443\n      http3: { altSvcMaxAge: 600, forceRetry: true }\n      proxyProtocol: { senders: [10.0.0.0/16] }\n      forwarding: { trustedProxies: [10.1.0.0/16], trustedOnlyHeaders: [X-Real-IP] }\n      requestId: Pass\n      accessLog: { type: File, path: /var/log/edge.log }\n      tunnelIdleSeconds: 120\n    - port: 53\n      accessLog: { type: Stdout }\n      sessionIdleSeconds: 30\n      sessionAnswers: 1\n      requestId: Generate\n",
            "",
        ),
        &own(),
    )
    .unwrap();
    assert_eq!(
        plane.ports.get(&443),
        Some(&PortSettings {
            http3: Some(Http3 {
                alt_svc_max_age: 600,
                force_retry: true
            }),
            proxy_protocol: Some(ListenerProxyProtocol::Senders(vec![
                "10.0.0.0/16".to_owned()
            ])),
            forwarding: Some(Forwarding {
                trusted_proxies: vec!["10.1.0.0/16".to_owned()],
                trusted_only_headers: vec!["X-Real-IP".to_owned()],
            }),
            request_id: Some(RequestId::Pass),
            access_log: Some(AccessLog::File("/var/log/edge.log".into())),
            tunnel_idle_seconds: Some(120),
            session_idle_seconds: None,
            session_answers: None,
        })
    );
    assert_eq!(
        plane.ports.get(&53),
        Some(&PortSettings {
            access_log: Some(AccessLog::Stdout),
            session_idle_seconds: Some(30),
            session_answers: Some(1),
            request_id: Some(RequestId::Generate),
            ..PortSettings::default()
        })
    );
    // HTTP/3 stated empty takes the data plane's own defaults.
    let defaults = super::plane(
        &web(
            1,
            "  ingress: { httpPort: 80, httpsPort: 443 }\n  ports: [{ port: 443, http3: {} }]\n",
            "",
        ),
        &own(),
    )
    .unwrap();
    let http3: Http3 = serde_saphyr::from_str("{}").unwrap();
    assert_eq!(defaults.ports[&443].http3, Some(http3));
}

#[test]
fn a_value_the_data_plane_cannot_take_is_said_by_its_field() {
    let unusable = |spec: &str| plane(&web(1, spec, ""), &own()).unwrap_err().to_string();
    let port = |settings: &str| {
        unusable(&format!(
            "  ingress: {{ httpPort: 80, httpsPort: 443 }}\n  ports: [{{ port: 443, {settings} }}]\n"
        ))
    };
    assert_eq!(
        port("requestId: Invent"),
        "DataPlane edge/web: ports[443].requestId: `Invent` is not Generate or Pass"
    );
    assert_eq!(
        port("accessLog: { type: Syslog }"),
        "DataPlane edge/web: ports[443].accessLog.type: `Syslog` is not Stdout or File"
    );
    assert_eq!(
        port("accessLog: { type: File }"),
        "DataPlane edge/web: ports[443].accessLog.path: a File needs one"
    );
    assert_eq!(
        port("tunnelIdleSeconds: -1"),
        "DataPlane edge/web: ports[443].tunnelIdleSeconds: -1 is out of range"
    );
    assert_eq!(
        port("http3: { altSvcMaxAge: 4294967296 }"),
        "DataPlane edge/web: ports[443].http3.altSvcMaxAge: 4294967296 is out of range"
    );
    assert_eq!(
        unusable("  ingress: { httpPort: 70000, httpsPort: 443 }\n"),
        "DataPlane edge/web: ingress.httpPort: 70000 is out of range"
    );
    assert_eq!(
        unusable("  ingress: { httpPort: 80, httpsPort: 443 }\n  ports: [{ port: 0 }]\n"),
        "DataPlane edge/web: ports[0].port: 0 is out of range"
    );
    assert_eq!(
        unusable("  ingress: { httpPort: 80, httpsPort: 443 }\n  ipFamily: IPv5\n"),
        "DataPlane edge/web: ipFamily: `IPv5` is not IPv4, IPv6 or Dual"
    );
}

#[test]
fn a_generation_is_acted_on_once_the_operator_accepted_it() {
    let spec = "  ingress: { httpPort: 80, httpsPort: 443 }\n";
    assert_eq!(
        verdict(&web(3, spec, &accepted("True", 3))),
        Verdict::Accepted
    );
    // An edit the operator has yet to look at.
    assert_eq!(
        verdict(&web(4, spec, &accepted("True", 3))),
        Verdict::Pending
    );
    assert_eq!(verdict(&web(4, spec, "")), Verdict::Pending);
    // One it refused, at this generation; one it refused before is not looked at yet.
    assert_eq!(
        verdict(&web(4, spec, &accepted("False", 4))),
        Verdict::Refused
    );
    assert_eq!(
        verdict(&web(4, spec, &accepted("False", 3))),
        Verdict::Pending
    );
    assert_eq!(
        verdict(&web(4, spec, &accepted("Unknown", 4))),
        Verdict::Pending
    );
}

/// What follows `data_planes` as they are, after `last`.
fn after(last: &Planes, data_planes: &[DataPlane]) -> Choice {
    let mut chooser = Chooser { last: last.clone() };
    chooser.next(data_planes, &own())
}

const WEB: &str = "DataPlane edge/web";
const API: &str = "DataPlane edge/api";

/// `api` in `edge`, of ControlPlane `edge`, made after `web`, at `generation`, with
/// `status`.
fn api(generation: i64, status: &str) -> DataPlane {
    data_plane(&format!(
        "metadata: {{ name: api, namespace: edge, generation: {generation}, uid: u2, creationTimestamp: \"2026-11-12T11:00:00Z\" }}\nspec:\n  controlPlaneRef: {{ name: edge }}\n  ingress: {{ httpPort: 80, httpsPort: 443 }}\n{status}"
    ))
}

#[test]
fn the_data_plane_served_is_the_last_generation_accepted() {
    let spec = |port: u16| format!("  ingress: {{ httpPort: {port}, httpsPort: 443 }}\n");
    let served = |port: u16, generation: i64| {
        let choice = after(
            &Planes::new(),
            &[web(generation, &spec(port), &accepted("True", generation))],
        );
        assert!(choice.waiting.is_empty(), "{choice:?}");
        choice.served[WEB].clone()
    };
    assert_eq!(served(80, 1).plane.ingress_http_port, 80);
    // An edit not yet accepted keeps the last, which is said as kept.
    let last = Planes::from([(WEB.to_owned(), served(80, 1))]);
    let kept = after(&last, &[web(2, &spec(8080), &accepted("True", 1))]);
    assert_eq!(
        kept,
        Choice {
            served: last.clone(),
            waiting: vec![Waiting::Pending(WEB.to_owned())],
        }
    );
    assert_eq!(
        kept.told(),
        [
            "DataPlane edge/web: the operator has yet to accept it; the DataPlane last accepted is served"
        ]
    );
    // With none before, as after a restart, it is waited on.
    let waited = after(&Planes::new(), &[web(2, &spec(8080), &accepted("True", 1))]);
    assert_eq!(
        waited,
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::Pending(WEB.to_owned())],
        }
    );
    assert_eq!(
        waited.told(),
        [
            "DataPlane edge/web: the operator has yet to accept it; it is not served until it is accepted"
        ]
    );
    // Once accepted, served.
    assert_eq!(
        after(&last, &[web(2, &spec(8080), &accepted("True", 2))]),
        Choice {
            served: Planes::from([(WEB.to_owned(), served(8080, 2))]),
            waiting: Vec::new(),
        }
    );
    // Refused, or gone, and it is not served.
    assert_eq!(
        after(&last, &[web(2, &spec(8080), &accepted("False", 2))]),
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::Refused(WEB.to_owned())],
        }
    );
    let none = after(&last, &[]);
    assert_eq!(
        none,
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::None],
        }
    );
    assert_eq!(
        none.told(),
        ["no DataPlane names this control plane; nothing is served until one is accepted"]
    );
    // Accepted, but of a value it cannot take: the last kept, and why said.
    let unusable = after(
        &last,
        &[web(
            2,
            "  ingress: { httpPort: 80, httpsPort: 443 }\n  ipFamily: IPv5\n",
            &accepted("True", 2),
        )],
    );
    assert_eq!(unusable.served, last);
    let [Waiting::Unusable(why)] = unusable.waiting.as_slice() else {
        panic!("not kept: {unusable:?}");
    };
    assert_eq!(why.data_plane, WEB);
    assert!(why.to_string().contains("ipFamily"), "{why}");
}

#[test]
fn only_a_data_plane_of_its_own_control_plane_is_served() {
    let other = data_plane(
        "metadata: { name: api, namespace: edge, generation: 1, uid: u2, creationTimestamp: \"2026-11-11T10:00:00Z\" }\nspec:\n  controlPlaneRef: { name: other }\n  ingress: { httpPort: 80, httpsPort: 443 }\nstatus:\n  conditions:\n    - { type: Accepted, status: \"True\", reason: R, message: \"\", lastTransitionTime: \"2026-11-12T10:00:00Z\", observedGeneration: 1 }\n",
    );
    assert_eq!(
        after(&Planes::new(), std::slice::from_ref(&other)),
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::None],
        }
    );
    let ours = web(
        1,
        "  ingress: { httpPort: 80, httpsPort: 443 }\n",
        &accepted("True", 1),
    );
    let choice = after(&Planes::new(), &[other, ours]);
    assert_eq!(choice.served.keys().collect::<Vec<_>>(), [WEB]);
    assert_eq!(
        choice.served[WEB].plane.ingress_class.as_deref(),
        Some("edge.web")
    );
}

/// Every DataPlane the operator accepted is served, each by its own last accepted
/// generation: one waiting on the operator, refused or gone changes nothing of another.
#[test]
fn every_data_plane_accepted_is_served_on_its_own() {
    let spec = "  ingress: { httpPort: 80, httpsPort: 443 }\n";
    let both = after(
        &Planes::new(),
        &[
            api(1, &accepted("True", 1)),
            web(1, spec, &accepted("True", 1)),
        ],
    );
    assert_eq!(both.served.keys().collect::<Vec<_>>(), [API, WEB]);
    assert!(both.waiting.is_empty(), "{both:?}");
    assert_eq!(
        both.served[API].plane.ingress_class.as_deref(),
        Some("edge.api")
    );
    assert_eq!(
        both.served[WEB].plane.ingress_class.as_deref(),
        Some("edge.web")
    );
    // An edit of one waiting on the operator keeps its last; the other is as it was.
    let edited = after(
        &both.served,
        &[
            api(2, &accepted("True", 1)),
            web(1, spec, &accepted("True", 1)),
        ],
    );
    assert_eq!(edited.served, both.served);
    assert_eq!(edited.waiting, [Waiting::Pending(API.to_owned())]);
    // One refused, or gone: the other served still. Said in the operator's order, the
    // oldest first.
    let refused = after(
        &both.served,
        &[
            api(2, &accepted("False", 2)),
            web(1, spec, &accepted("True", 1)),
        ],
    );
    assert_eq!(refused.served.keys().collect::<Vec<_>>(), [WEB]);
    assert_eq!(refused.served[WEB], both.served[WEB]);
    assert_eq!(refused.waiting, [Waiting::Refused(API.to_owned())]);
    let gone = after(&both.served, &[web(1, spec, &accepted("True", 1))]);
    assert_eq!(gone.served.keys().collect::<Vec<_>>(), [WEB]);
    assert!(gone.waiting.is_empty(), "{gone:?}");
    let both_waiting = after(&Planes::new(), &[api(1, ""), web(1, spec, "")]);
    assert_eq!(
        both_waiting.waiting,
        [
            Waiting::Pending(WEB.to_owned()),
            Waiting::Pending(API.to_owned())
        ]
    );
}

/// The classes kept are those a DataPlane naming this ControlPlane may still serve (04 §6):
/// each served one's, and that of each listed and not served, whose pods serve their last
/// config; not one served without `ingress`, nor another ControlPlane's.
#[test]
fn the_classes_kept_are_those_a_data_plane_may_still_serve() {
    let kept = |data_planes: &[DataPlane]| {
        let choice = after(&Planes::new(), data_planes);
        super::kept(data_planes, &choice.served, &own())
    };
    let classes = |names: &[&str]| -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    };
    let spec = "  ingress: { httpPort: 80, httpsPort: 443 }\n";
    // Served with `ingress`, waiting on the operator as after a restart, and refused.
    assert_eq!(
        kept(&[api(1, &accepted("True", 1)), web(1, spec, "")]),
        classes(&["edge.api", "edge.web"])
    );
    assert_eq!(
        kept(&[web(2, spec, &accepted("False", 2))]),
        classes(&["edge.web"])
    );
    // Served without `ingress`: no class of its own.
    let gateway_only = web(
        1,
        "  gateway:\n    listeners: [{ name: http, port: 80, protocol: HTTP }]\n    tls: { frontend: { default: {} } }\n",
        &accepted("True", 1),
    );
    assert_eq!(kept(&[gateway_only]), classes(&[]));
    // Another ControlPlane's, and none at all.
    let other = data_plane(
        "metadata: { name: api, namespace: edge, generation: 1, uid: u2 }\nspec:\n  controlPlaneRef: { name: other }\n  ingress: { httpPort: 80, httpsPort: 443 }\n",
    );
    assert_eq!(kept(&[other]), classes(&[]));
    assert_eq!(kept(&[]), classes(&[]));
}

#[test]
fn once_refused_nothing_is_kept_for_a_later_edit() {
    let spec = "  ingress: { httpPort: 80, httpsPort: 443 }\n";
    let mut chooser = Chooser::default();
    let first = chooser.next(&[web(1, spec, &accepted("True", 1))], &own());
    assert!(first.served.contains_key(WEB));
    assert_eq!(
        chooser.next(&[web(2, spec, &accepted("False", 2))], &own()),
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::Refused(WEB.to_owned())],
        }
    );
    // Refused, the last accepted is let go of: an edit the operator has yet to look at is
    // waited on, not served by what was refused before.
    assert_eq!(
        chooser.next(&[web(3, spec, &accepted("False", 2))], &own()),
        Choice {
            served: Planes::new(),
            waiting: vec![Waiting::Pending(WEB.to_owned())],
        }
    );
    assert!(chooser.last.is_empty());
}

/// `web-edge` in `shop`, as the operator generates it for Gateway `shop/web` of class
/// `edge` (05 §3), naming ControlPlane `edge` of namespace `controlPlaneNamespace`, at
/// generation 1, with `status`.
fn generated(control_plane_namespace: Option<&str>, status: &str) -> DataPlane {
    let namespace =
        control_plane_namespace.map_or_else(String::new, |ns| format!(", namespace: {ns}"));
    data_plane(&format!(
        "metadata: {{ name: web-edge, namespace: shop, generation: 1, uid: u3, creationTimestamp: \"2026-11-12T12:00:00Z\" }}\nspec:\n  controlPlaneRef: {{ name: edge{namespace} }}\n  gatewayRef: {{ name: web }}\n{status}"
    ))
}

const GENERATED: &str = "DataPlane shop/web-edge";

/// One generated for a Gateway of another namespace is served the Gateway it names, of its
/// namespace, known by its ServiceAccount there; and is never kept a class.
#[test]
fn a_generated_data_plane_is_served_the_gateway_it_names() {
    let data_planes = [generated(Some("edge"), &accepted("True", 1))];
    let choice = after(&Planes::new(), &data_planes);
    let settings = choice.served.get(GENERATED).unwrap();
    assert!(settings.generated);
    assert_eq!(
        settings.plane.gateway,
        Some(GatewayName {
            namespace: "shop".to_owned(),
            name: "web".to_owned()
        })
    );
    assert_eq!(
        settings.plane.id.as_deref(),
        Some("spiffe://edge.cluster.example/ns/shop/sa/edgerush-proxy-web-edge")
    );
    assert_eq!(settings.plane.ingress_class, None);
    assert_eq!(
        settings.service.as_ref().map(ProxyService::namespace),
        Some("shop")
    );
    assert!(super::kept(&data_planes, &choice.served, &own()).is_empty());
    // Waiting on the operator, it keeps no class either: it has none.
    let waiting = [generated(Some("edge"), "")];
    let choice = after(&Planes::new(), &waiting);
    assert_eq!(choice.waiting, [Waiting::Pending(GENERATED.to_owned())]);
    assert!(super::kept(&waiting, &choice.served, &own()).is_empty());
}

/// A DataPlane names this ControlPlane by its name and namespace: one of another namespace
/// naming no namespace names a ControlPlane of its own namespace, and one naming another
/// namespace another ControlPlane.
#[test]
fn a_data_plane_of_another_namespace_is_ours_only_naming_our_namespace() {
    assert!(names_us(&generated(Some("edge"), ""), &own()));
    assert!(!names_us(&generated(None, ""), &own()));
    assert!(!names_us(&generated(Some("blog"), ""), &own()));
    assert!(names_us(&web(1, "", ""), &own()));
    let elsewhere = data_plane(
        "metadata: { name: web, namespace: edge, generation: 1, uid: u4 }\nspec:\n  controlPlaneRef: { name: edge, namespace: blog }\n",
    );
    assert!(!names_us(&elsewhere, &own()));
    let choice = after(
        &Planes::new(),
        &[
            generated(None, &accepted("True", 1)),
            generated(Some("blog"), &accepted("True", 1)),
        ],
    );
    assert!(choice.served.is_empty());
    assert_eq!(choice.waiting, [Waiting::None]);
}
