//! The data plane's listeners, as its Gateway's listeners state them
//! ([04 §2](../../../docs/04-control-plane.md)): one for each port they bind, named after it,
//! with the settings the data plane's settings give that port, or the control plane's
//! defaults.

use crate::{IpFamily, Plane};
use edgerush_config::{
    Forwarding, Listener, ListenerProxyProtocol, MOST_LISTENERS, Protocol, RequestId, Tls,
};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The headers only a trusted proxy may send unless a port's settings say otherwise: the
/// control plane's default ([07 §1](../../../docs/07-config-and-dsl.md)).
const TRUSTED_ONLY_HEADERS: [&str; 3] = ["Forwarded", "X-Real-IP", "X-Forwarded-*"];

/// The ports Ingress has of its own: one for plain HTTP, one for its TLS hosts (04 §2).
const INGRESS_PORTS: usize = 2;

/// The most ports of the Gateway API a data plane binds: its slots but those kept for
/// Ingress's own ports, so that a Gateway's every listener, as many as its CRD allows, and
/// Ingress's ports are bound together (04 §2).
pub const MOST_GATEWAY_PORTS: usize = MOST_LISTENERS - INGRESS_PORTS;

/// The name of the data plane's listener bound to `port`: in a cluster a name changes only
/// with its port, so that a listener is kept as long as its port is (03 §10).
#[must_use]
pub fn name(port: u16) -> String {
    format!("port-{port}")
}

/// The data plane's listeners: one for each of `ports`, speaking the protocol it is bound
/// for.
pub(crate) fn bound(plane: &Plane, ports: &BTreeMap<u16, Protocol>) -> BTreeMap<String, Listener> {
    ports
        .iter()
        .map(|(port, protocol)| (name(*port), listener(plane, *port, *protocol)))
        .collect()
}

/// The data plane's listener on `port`, speaking `protocol`, with the settings its
/// settings give the port.
fn listener(plane: &Plane, port: u16, protocol: Protocol) -> Listener {
    let settings = plane.ports.get(&port);
    let address: IpAddr = match plane.ip_family {
        IpFamily::Ipv4 => Ipv4Addr::UNSPECIFIED.into(),
        // A socket that takes IPv4 as well wherever the host has it: the two are told apart
        // only by the Service's families (04 §2).
        IpFamily::Ipv6 | IpFamily::Dual => Ipv6Addr::UNSPECIFIED.into(),
    };
    let https = protocol == Protocol::Https;
    // A passthrough listener reads no request: none of the HTTP listeners' settings is its.
    let http = matches!(protocol, Protocol::Http | Protocol::Https);
    Listener {
        address: SocketAddr::new(address, port),
        protocol,
        // What the Gateway's listeners and the Ingresses' hosts present, put in after.
        tls: https.then(|| Tls {
            certificates: Vec::new(),
            without_sni: "none".to_owned(),
            refused: Vec::new(),
            client_validation: None,
            shared_by: Vec::new(),
        }),
        // An `http` port's is not used: HTTP/3 is TLS's alone.
        http3: settings
            .and_then(|settings| settings.http3)
            .filter(|_| https),
        tunnel_idle_seconds: None,
        forwarding: http.then(|| {
            settings
                .and_then(|settings| settings.forwarding.clone())
                .unwrap_or_else(|| Forwarding {
                    trusted_proxies: Vec::new(),
                    trusted_only_headers: TRUSTED_ONLY_HEADERS.map(str::to_owned).to_vec(),
                })
        }),
        request_id: http.then(|| {
            settings
                .and_then(|settings| settings.request_id)
                .unwrap_or(RequestId::Generate)
        }),
        proxy_protocol: Some(
            settings
                .and_then(|settings| settings.proxy_protocol.clone())
                .unwrap_or(ListenerProxyProtocol::Off),
        ),
        access_log: settings.and_then(|settings| settings.access_log.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A data plane of these more settings.
    fn plane(more: &str) -> Plane {
        serde_saphyr::from_str(&format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\n{more}"
        ))
        .unwrap()
    }

    fn ports() -> BTreeMap<u16, Protocol> {
        [(80, Protocol::Http), (8443, Protocol::Https)].into()
    }

    /// Each port is one listener named after it, on IPv4 unless the data plane says
    /// otherwise, with the control plane's defaults where its settings say nothing.
    #[test]
    fn a_port_is_a_listener_named_after_it() {
        let bound = bound(&plane(""), &ports());
        assert_eq!(bound.keys().collect::<Vec<_>>(), ["port-80", "port-8443"]);
        let http = &bound["port-80"];
        assert_eq!(http.address, "0.0.0.0:80".parse().unwrap());
        assert_eq!(http.protocol, Protocol::Http);
        assert!(http.tls.is_none());
        assert_eq!(http.request_id, Some(RequestId::Generate));
        assert_eq!(http.proxy_protocol, Some(ListenerProxyProtocol::Off));
        let forwarding = http.forwarding.as_ref().unwrap();
        assert!(forwarding.trusted_proxies.is_empty());
        assert_eq!(forwarding.trusted_only_headers, TRUSTED_ONLY_HEADERS);
        let https = &bound["port-8443"];
        assert_eq!(https.protocol, Protocol::Https);
        let tls = https.tls.as_ref().unwrap();
        assert!(tls.certificates.is_empty());
        assert_eq!(tls.without_sni, "none");
    }

    #[test]
    fn ipv6_and_dual_bind_every_address() {
        for family in ["ipv6", "dual"] {
            let bound = bound(&plane(&format!("ip_family: {family}\n")), &ports());
            assert_eq!(
                bound["port-80"].address,
                "[::]:80".parse().unwrap(),
                "{family}"
            );
        }
    }

    /// A port's settings are its listener's; HTTP/3 an `https` port's alone.
    #[test]
    fn a_ports_settings_are_its_listeners() {
        let http3 = "{ http3: { alt_svc_max_age: 60 }, request_id: pass }";
        let bound = bound(
            &plane(&format!("ports:\n  80: {http3}\n  8443: {http3}\n")),
            &ports(),
        );
        assert_eq!(bound["port-80"].http3, None);
        assert_eq!(bound["port-80"].request_id, Some(RequestId::Pass));
        assert_eq!(
            bound["port-8443"].http3.map(|http3| http3.alt_svc_max_age),
            Some(60)
        );
    }
}
