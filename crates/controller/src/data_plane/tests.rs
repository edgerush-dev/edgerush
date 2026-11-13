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
        cluster_domain: "cluster.example".to_owned(),
        system_authorities: vec!["SYSTEM".to_owned()],
    }
}

const BOTH: &str = "  ingress: { httpPort: 8080, httpsPort: 8443, hostFallThrough: true, httpsRedirect: false, httpsRedirectMaxAge: 60 }\n  gateway:\n    listeners: [{ name: http, port: 80, protocol: HTTP }]\n    tls: { frontend: { default: {} } }\n  ipFamily: Dual\n";

#[test]
fn a_data_plane_serving_both_is_its_settings_field_by_field() {
    let plane = plane(&web(1, BOTH, ""), &own()).unwrap();
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
fn after(last: Option<Plane>, data_planes: &[DataPlane]) -> Next {
    let mut chooser = Chooser { last };
    chooser.next(data_planes, &own())
}

#[test]
fn the_data_plane_served_is_the_last_generation_accepted() {
    let spec = |port: u16| format!("  ingress: {{ httpPort: {port}, httpsPort: 443 }}\n");
    let served = |port: u16, generation: i64| {
        let Next::Serve(plane) = after(
            None,
            &[web(generation, &spec(port), &accepted("True", generation))],
        ) else {
            panic!("not served");
        };
        plane
    };
    assert_eq!(served(80, 1).ingress_http_port, 80);
    // An edit not yet accepted keeps the last, which is said as kept.
    let last = served(80, 1);
    assert_eq!(
        after(
            Some(last.clone()),
            &[web(2, &spec(8080), &accepted("True", 1))]
        ),
        Next::Keep(Waiting::Pending("DataPlane edge/web".to_owned()))
    );
    // With none before, as after a restart, it is waited on.
    assert_eq!(
        after(None, &[web(2, &spec(8080), &accepted("True", 1))]),
        Next::Wait(Waiting::Pending("DataPlane edge/web".to_owned()))
    );
    // Once accepted, served.
    assert_eq!(
        after(
            Some(last.clone()),
            &[web(2, &spec(8080), &accepted("True", 2))]
        ),
        Next::Serve(served(8080, 2))
    );
    // Refused, or gone, and nothing is served.
    assert_eq!(
        after(
            Some(last.clone()),
            &[web(2, &spec(8080), &accepted("False", 2))]
        ),
        Next::Wait(Waiting::Refused("DataPlane edge/web".to_owned()))
    );
    assert_eq!(after(Some(last.clone()), &[]), Next::Wait(Waiting::None));
    // Accepted, but of a value it cannot take: the last kept, and why said.
    let Next::Keep(Waiting::Unusable(why)) = after(
        Some(last),
        &[web(
            2,
            "  ingress: { httpPort: 80, httpsPort: 443 }\n  ipFamily: IPv5\n",
            &accepted("True", 2),
        )],
    ) else {
        panic!("not kept");
    };
    assert!(why.contains("ipFamily"), "{why}");
}

#[test]
fn only_a_data_plane_of_its_own_control_plane_is_served() {
    let other = data_plane(
        "metadata: { name: api, namespace: edge, generation: 1, uid: u2, creationTimestamp: \"2026-11-11T10:00:00Z\" }\nspec:\n  controlPlaneRef: { name: other }\n  ingress: { httpPort: 80, httpsPort: 443 }\nstatus:\n  conditions:\n    - { type: Accepted, status: \"True\", reason: R, message: \"\", lastTransitionTime: \"2026-11-12T10:00:00Z\", observedGeneration: 1 }\n",
    );
    assert_eq!(
        after(None, std::slice::from_ref(&other)),
        Next::Wait(Waiting::None)
    );
    let ours = web(
        1,
        "  ingress: { httpPort: 80, httpsPort: 443 }\n",
        &accepted("True", 1),
    );
    let Next::Serve(plane) = after(None, &[other, ours]) else {
        panic!("not served");
    };
    assert_eq!(plane.ingress_class.as_deref(), Some("edge.web"));
}

#[test]
fn of_two_accepted_the_oldest_is_served() {
    // The operator accepts one alone; one it accepted before a second came stays first.
    let younger = data_plane(
        "metadata: { name: api, namespace: edge, generation: 1, uid: u2, creationTimestamp: \"2026-11-12T11:00:00Z\" }\nspec:\n  controlPlaneRef: { name: edge }\n  ingress: { httpPort: 80, httpsPort: 443 }\nstatus:\n  conditions:\n    - { type: Accepted, status: \"True\", reason: R, message: \"\", lastTransitionTime: \"2026-11-12T10:00:00Z\", observedGeneration: 1 }\n",
    );
    let older = web(
        1,
        "  ingress: { httpPort: 80, httpsPort: 443 }\n",
        &accepted("True", 1),
    );
    let Next::Serve(plane) = after(None, &[younger, older]) else {
        panic!("not served");
    };
    assert_eq!(plane.ingress_class.as_deref(), Some("edge.web"));
}

#[test]
fn once_refused_nothing_is_kept_for_a_later_edit() {
    let spec = "  ingress: { httpPort: 80, httpsPort: 443 }\n";
    let mut chooser = Chooser::default();
    assert!(matches!(
        chooser.next(&[web(1, spec, &accepted("True", 1))], &own()),
        Next::Serve(_)
    ));
    assert_eq!(
        chooser.next(&[web(2, spec, &accepted("False", 2))], &own()),
        Next::Wait(Waiting::Refused("DataPlane edge/web".to_owned()))
    );
    // Refused, the last accepted is let go of: an edit the operator has yet to look at is
    // waited on, not served by what was refused before.
    assert_eq!(
        chooser.next(&[web(3, spec, &accepted("False", 2))], &own()),
        Next::Wait(Waiting::Pending("DataPlane edge/web".to_owned()))
    );
    assert_eq!(chooser.last, None);
}
