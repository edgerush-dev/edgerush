//! The data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)): found by the name
//! its settings give, its class checked to name this controller, and each of its listeners
//! fitted to the data plane's own listeners bound to its port, or not accepted and why.

use crate::{Cluster, Finding, GATEWAY_CONTROLLER, Object, Plane, Problem};
use edgerush_config::Protocol;
use edgerush_gateway_api::gateway::{Gateway, GatewayListeners};
use std::collections::BTreeMap;
use std::fmt;

/// What translation made of the data plane's Gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayOutcome {
    /// The Gateway.
    pub gateway: Object,
    /// Each of its listeners, in the order it lists them.
    pub listeners: Vec<ListenerOutcome>,
}

/// One of a Gateway's listeners: accepted, and served by the data plane's listeners named,
/// or not, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerOutcome {
    /// Its name.
    pub name: String,
    /// The data plane's listeners bound to its port for its protocol, or why it is not
    /// accepted.
    pub served: Result<Vec<String>, ListenerProblem>,
}

impl fmt::Display for ListenerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.served {
            Ok(listeners) => write!(
                f,
                "listener {}: served on {}",
                self.name,
                listeners.join(", ")
            ),
            Err(problem) => write!(f, "listener {}: not accepted: {problem}", self.name),
        }
    }
}

/// Why a Gateway listener is not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListenerProblem {
    /// A protocol the gateway never serves (the spec's `UnsupportedProtocol`).
    #[error("protocol `{0}` is not one the gateway serves")]
    UnsupportedProtocol(String),
    /// A protocol whose listeners translation does not take yet (`UnsupportedProtocol`).
    #[error("not supported yet: {0} listeners")]
    NotSupportedYet(&'static str),
    /// Another listener of the Gateway has its port, protocol and hostname, so neither can
    /// be told apart from the other and none of them is accepted (`HostnameConflict`).
    #[error("listeners {0} have the same port, protocol and hostname")]
    HostnameConflict(String),
    /// No listener of the data plane is bound to its port (`PortUnavailable`).
    #[error("port {0} is bound by no listener of the data plane")]
    PortUnbound(u16),
    /// The data plane binds its port for another protocol (`PortUnavailable`).
    #[error("port {port} is bound by the data plane for {bound}, not {wanted}")]
    PortOtherProtocol {
        /// The port.
        port: u16,
        /// The protocol the data plane binds it for.
        bound: &'static str,
        /// The listener's.
        wanted: &'static str,
    },
}

/// The data plane's Gateway, its class checked, its listeners fitted: `None`, said in
/// `report`, where the data plane names a Gateway that is not there or not this
/// controller's; `None`, unsaid, where it names none.
pub(crate) fn outcome(
    plane: &Plane,
    cluster: &Cluster,
    report: &mut Vec<Finding>,
) -> Option<GatewayOutcome> {
    let named = plane.gateway.as_ref()?;
    let object = Object {
        kind: "Gateway",
        namespace: named.namespace.clone(),
        name: named.name.clone(),
    };
    let Some(gateway) = cluster.gateways.iter().find(|gateway| {
        gateway.metadata.namespace.as_deref() == Some(named.namespace.as_str())
            && gateway.metadata.name.as_deref() == Some(named.name.as_str())
    }) else {
        report.push(Finding {
            object,
            problem: Problem::GatewayMissing,
        });
        return None;
    };
    if !class_ours(gateway, cluster, report) {
        return None;
    }
    let listeners = listeners(plane, &gateway.spec.listeners);
    for listener in &listeners {
        if let Err(problem) = &listener.served {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::Listener {
                    name: listener.name.clone(),
                    problem: problem.clone(),
                },
            });
        }
    }
    let problem = if listeners.iter().any(|listener| listener.served.is_ok()) {
        // Until routes are attached (the next slice), an accepted Gateway serves nothing.
        Problem::NotSupportedYet("routes of the Gateway API")
    } else {
        Problem::NoListenerAccepted
    };
    report.push(Finding {
        object: object.clone(),
        problem,
    });
    Some(GatewayOutcome {
        gateway: object,
        listeners,
    })
}

/// Whether the Gateway's GatewayClass is there and names this controller; said where not.
fn class_ours(gateway: &Gateway, cluster: &Cluster, report: &mut Vec<Finding>) -> bool {
    let name = &gateway.spec.gateway_class_name;
    let object = Object {
        kind: "GatewayClass",
        namespace: String::new(),
        name: name.clone(),
    };
    let Some(class) = cluster
        .gateway_classes
        .iter()
        .find(|class| class.metadata.name.as_deref() == Some(name.as_str()))
    else {
        report.push(Finding {
            object,
            problem: Problem::GatewayClassMissing,
        });
        return false;
    };
    if class.spec.controller_name != GATEWAY_CONTROLLER {
        report.push(Finding {
            object,
            problem: Problem::GatewayClassNotOurs(class.spec.controller_name.clone()),
        });
        return false;
    }
    true
}

/// Each listener's outcome. A protocol not taken is refused first, and plays no part in
/// what conflicts (the spec: a listener of a protocol not supported is not accepted, and
/// the rules of distinctness are for those that are); then listeners that cannot be told
/// apart are all refused, none chosen; then each is fitted to the data plane's listeners
/// bound to its port.
fn listeners(plane: &Plane, listeners: &[GatewayListeners]) -> Vec<ListenerOutcome> {
    let protocols: Vec<Result<Protocol, ListenerProblem>> = listeners
        .iter()
        .map(|listener| protocol(&listener.protocol))
        .collect();
    // Listeners of a protocol taken, by port, protocol and hostname: two in one place are
    // indistinct. TCP sharing a port with any other, which the spec also refuses, is for
    // when TCP listeners are taken.
    let mut places: BTreeMap<(i32, &'static str, Option<&str>), Vec<&str>> = BTreeMap::new();
    for (listener, protocol) in listeners.iter().zip(&protocols) {
        if let Ok(protocol) = protocol {
            places
                .entry((
                    listener.port,
                    name_of(*protocol),
                    listener.hostname.as_deref(),
                ))
                .or_default()
                .push(&listener.name);
        }
    }
    listeners
        .iter()
        .zip(protocols)
        .map(|(listener, protocol)| {
            let served = protocol.and_then(|protocol| {
                let place = (
                    listener.port,
                    name_of(protocol),
                    listener.hostname.as_deref(),
                );
                match places.get(&place) {
                    Some(names) if names.len() > 1 => {
                        Err(ListenerProblem::HostnameConflict(names.join(", ")))
                    }
                    _ => fitted(plane, listener.port, protocol),
                }
            });
            ListenerOutcome {
                name: listener.name.clone(),
                served,
            }
        })
        .collect()
}

/// The protocol of the data plane's listeners that serve a Gateway listener's, if one is
/// taken.
fn protocol(written: &str) -> Result<Protocol, ListenerProblem> {
    match written {
        "HTTP" => Ok(Protocol::Http),
        "HTTPS" => Err(ListenerProblem::NotSupportedYet("HTTPS")),
        "TLS" => Err(ListenerProblem::NotSupportedYet("TLS")),
        "TCP" => Err(ListenerProblem::NotSupportedYet("TCP")),
        other => Err(ListenerProblem::UnsupportedProtocol(other.to_owned())),
    }
}

/// A protocol's name as the data plane's config writes it.
fn name_of(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Http => "http",
        Protocol::Https => "https",
        Protocol::Tcp => "tcp",
        Protocol::Tls => "tls",
    }
}

/// The data plane's listeners bound to `port` for `protocol`, by name; or why there are none.
fn fitted(plane: &Plane, port: i32, protocol: Protocol) -> Result<Vec<String>, ListenerProblem> {
    // The schema holds a port to 1–65535.
    let port = u16::try_from(port).unwrap_or_default();
    let on_port: Vec<(&String, Protocol)> = plane
        .listeners
        .iter()
        .filter(|(_, listener)| listener.address.port() == port)
        .map(|(name, listener)| (name, listener.protocol))
        .collect();
    let serving: Vec<String> = on_port
        .iter()
        .filter(|(_, bound)| *bound == protocol)
        .map(|(name, _)| (*name).clone())
        .collect();
    if !serving.is_empty() {
        return Ok(serving);
    }
    match on_port.first() {
        Some((_, bound)) => Err(ListenerProblem::PortOtherProtocol {
            port,
            bound: name_of(*bound),
            wanted: name_of(protocol),
        }),
        None => Err(ListenerProblem::PortUnbound(port)),
    }
}
