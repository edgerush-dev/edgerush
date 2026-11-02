//! The data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)): found by the name
//! its settings give, its class checked to name this controller, and each of its listeners
//! served by the data plane's listener bound to its port, or not accepted and why, and the
//! routes each takes: of which kinds, from which namespaces. The ports its accepted
//! listeners state are what the data plane binds.

use crate::http_route::reachable;
use crate::{
    Cluster, Finding, GATEWAY_CONTROLLER, Object, Plane, Problem, RefProblem, RefReason,
    secret_certificate,
};
use edgerush_config::Protocol;
use edgerush_gateway_api::gateway::{
    Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespaces,
    GatewayListenersAllowedRoutesNamespacesSelector,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The API group of the Gateway API's kinds.
const GROUP: &str = "gateway.networking.k8s.io";

/// What translation made of the data plane's Gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayOutcome {
    /// The Gateway.
    pub gateway: Object,
    /// Its generation: what its status says of.
    pub generation: i64,
    /// Each of its listeners, in the order it lists them.
    pub listeners: Vec<ListenerOutcome>,
    /// The ports its accepted listeners state, and the protocol each is bound for: the data
    /// plane's listeners.
    pub ports: BTreeMap<u16, Protocol>,
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
    /// The routes it takes: what its status says of every listener, accepted or not.
    pub allowed: Allowed,
    /// Its hostname, if it has one: none is every host.
    pub hostname: Option<String>,
    /// Its port.
    pub port: u16,
    /// An HTTPS listener's certificates that can be used, by the config's names for them,
    /// `<namespace>/<Secret>`, in the order its `certificateRefs` list them.
    pub certificates: Vec<String>,
    /// Its `certificateRefs` that cannot be used, each with why (`ResolvedRefs` false).
    pub certificate_problems: Vec<RefProblem>,
}

impl fmt::Display for ListenerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.served {
            Ok(listeners) if self.certificates.is_empty() => write!(
                f,
                "listener {}: served on {}, taking {}",
                self.name,
                listeners.join(", "),
                self.allowed
            ),
            Ok(listeners) => write!(
                f,
                "listener {}: served on {}, presenting {}, taking {}",
                self.name,
                listeners.join(", "),
                self.certificates.join(", "),
                self.allowed
            ),
            Err(problem) => write!(
                f,
                "listener {}: {}: {problem}",
                self.name,
                problem.verdict()
            ),
        }
    }
}

impl ListenerOutcome {
    /// Whether it is accepted: served, or refused for its certificates alone, which leaves
    /// it accepted with the routes that attach to it, and serving none of them, as the
    /// spec's conformance has a listener of unresolved `certificateRefs` count its routes
    /// (04 §6).
    #[must_use]
    pub fn accepted(&self) -> bool {
        self.served
            .as_ref()
            .err()
            .is_none_or(ListenerProblem::accepted)
    }
}

/// The routes a listener takes (`allowedRoutes`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allowed {
    /// The kinds it takes, of those its protocol can carry, as its `supportedKinds` says.
    pub kinds: Vec<&'static str>,
    /// The kinds it names that it cannot take, `group/kind` (`InvalidRouteKinds`): of
    /// another group, unknown, or not for its protocol.
    pub invalid_kinds: Vec<String>,
    /// The namespaces its routes may be in.
    pub namespaces: Namespaces,
}

impl fmt::Display for Allowed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kinds = if self.kinds.is_empty() {
            "no route".to_owned()
        } else {
            self.kinds.join(", ")
        };
        write!(f, "{kinds} {}", self.namespaces)
    }
}

/// The namespaces a listener takes routes from (`allowedRoutes.namespaces`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Namespaces {
    /// Its Gateway's own (`Same`, when nothing is said).
    Same(String),
    /// Every namespace (`All`).
    All,
    /// Those whose labels its selector matches (`Selector`).
    Selector(Selector),
    /// None: the listener says what translation cannot read, and that is said.
    None,
}

impl Namespaces {
    /// Whether a route in `namespace`, whose labels are as `cluster` has them, may attach.
    #[must_use]
    pub fn allow(&self, namespace: &str, cluster: &Cluster) -> bool {
        match self {
            Self::Same(own) => own == namespace,
            Self::All => true,
            Self::Selector(selector) => cluster
                .namespaces
                .iter()
                .find(|object| object.metadata.name.as_deref() == Some(namespace))
                .is_some_and(|object| {
                    let none = BTreeMap::new();
                    selector.matches(object.metadata.labels.as_ref().unwrap_or(&none))
                }),
            Self::None => false,
        }
    }
}

impl fmt::Display for Namespaces {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Same(own) => write!(f, "from namespace {own}"),
            Self::All => write!(f, "from every namespace"),
            Self::Selector(selector) => write!(f, "from namespaces labelled {selector}"),
            Self::None => write!(f, "from no namespace"),
        }
    }
}

/// A label selector, as Kubernetes matches one: every label it states, and every
/// requirement, held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    /// Labels a namespace must have, with these values.
    pub labels: BTreeMap<String, String>,
    /// Requirements a namespace's labels must meet.
    pub requirements: Vec<Requirement>,
}

/// One requirement of a label selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requirement {
    /// The label is there, with one of the values.
    In(String, Vec<String>),
    /// The label is not there, or not with any of the values.
    NotIn(String, Vec<String>),
    /// The label is there.
    Exists(String),
    /// The label is not there.
    DoesNotExist(String),
}

impl Selector {
    /// Whether `labels` meet it. A selector that states nothing matches every namespace,
    /// as an empty one does in Kubernetes.
    #[must_use]
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.labels
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
            && self
                .requirements
                .iter()
                .all(|requirement| match requirement {
                    Requirement::In(key, values) => {
                        labels.get(key).is_some_and(|v| values.contains(v))
                    }
                    Requirement::NotIn(key, values) => {
                        labels.get(key).is_none_or(|v| !values.contains(v))
                    }
                    Requirement::Exists(key) => labels.contains_key(key),
                    Requirement::DoesNotExist(key) => !labels.contains_key(key),
                })
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = self
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        parts.extend(
            self.requirements
                .iter()
                .map(|requirement| match requirement {
                    Requirement::In(key, values) => format!("{key} in ({})", values.join(", ")),
                    Requirement::NotIn(key, values) => {
                        format!("{key} notin ({})", values.join(", "))
                    }
                    Requirement::Exists(key) => key.clone(),
                    Requirement::DoesNotExist(key) => format!("!{key}"),
                }),
        );
        if parts.is_empty() {
            write!(f, "with any labels")
        } else {
            write!(f, "{}", parts.join(", "))
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
    /// An HTTPS listener none of whose `certificateRefs` can be used (`ResolvedRefs`
    /// false, `InvalidCertificateRef` or `RefNotPermitted`): accepted, its routes attached,
    /// and not served, its hostname refused over TLS so that no other certificate answers
    /// for it.
    #[error("no certificateRef of it can be used")]
    NoCertificate,
    /// An HTTPS listener of a TLS mode other than `Terminate`, which the spec does not let
    /// one have.
    #[error("TLS mode `{0}` is not one an HTTPS listener takes")]
    TlsMode(String),
    /// Other listeners of the Gateway state its port for another protocol, so that the
    /// port can be bound for none of them, and none on it is accepted (`ProtocolConflict`).
    #[error("port {port} is stated for {protocols}")]
    ProtocolConflict {
        /// The port.
        port: u16,
        /// The protocols stated for it, as the data plane's config writes them.
        protocols: String,
    },
    /// Its port is past the data plane's slots for the Gateway API's ports, taken in the
    /// order listeners state them (`PortUnavailable`).
    #[error(
        "the data plane serves at most {most} ports of the Gateway API; port {port} is past them",
        most = crate::listeners::MOST_GATEWAY_PORTS
    )]
    PortPastSlots {
        /// The port.
        port: u16,
    },
}

impl ListenerProblem {
    /// Whether a listener refused for it is accepted all the same: one of no certificate
    /// that can be used, which serves nothing.
    #[must_use]
    pub fn accepted(&self) -> bool {
        matches!(self, Self::NoCertificate)
    }

    /// What it makes of its listener, as said: `not accepted`, or accepted and serving
    /// nothing.
    #[must_use]
    pub fn verdict(&self) -> &'static str {
        if self.accepted() {
            "accepted, serving nothing"
        } else {
            "not accepted"
        }
    }
}

/// The data plane's Gateway, its class checked, its listeners accepted or refused: `None`,
/// said in `report`, where the data plane names a Gateway that is not there or not this
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
    let mut listeners = listeners(&gateway.spec.listeners);
    // Bound as long as one listener on them is accepted: one whose certificates cannot be
    // used too, which keeps its port refusing its hostname (04 §2).
    let ports: BTreeMap<u16, Protocol> = listeners
        .iter()
        .zip(&gateway.spec.listeners)
        .filter(|(listener, _)| listener.served.is_ok())
        .filter_map(|(listener, written)| Some((listener.port, protocol(&written.protocol).ok()?)))
        .collect();
    for (listener, written) in listeners.iter_mut().zip(&gateway.spec.listeners) {
        if listener.served.is_ok() && written.protocol == "HTTPS" {
            certificates(listener, written, &named.namespace, cluster);
        }
        for why in &listener.certificate_problems {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::ListenerCertificate {
                    name: listener.name.clone(),
                    why: why.why.clone(),
                },
            });
        }
        let (allowed, unread) = allowed(written, &named.namespace);
        if let Err(problem) = &listener.served {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::Listener {
                    name: listener.name.clone(),
                    problem: problem.clone(),
                },
            });
        }
        if !allowed.invalid_kinds.is_empty() {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::ListenerKinds {
                    name: listener.name.clone(),
                    kinds: allowed.invalid_kinds.join(", "),
                },
            });
        }
        if let Some(why) = unread {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::ListenerNamespaces {
                    name: listener.name.clone(),
                    why,
                },
            });
        }
        listener.allowed = allowed;
    }
    if !listeners.iter().any(|listener| listener.served.is_ok()) {
        report.push(Finding {
            object: object.clone(),
            problem: Problem::NoListenerServed,
        });
    }
    Some(GatewayOutcome {
        gateway: object,
        generation: gateway.metadata.generation.unwrap_or_default(),
        listeners,
        ports,
    })
}

/// An HTTPS listener's certificates, put in `listener`: each of its `certificateRefs` that
/// names a Secret, of the Gateway's namespace or of one whose ReferenceGrant lets Gateways
/// of it reach that Secret, asked first as conformance has it, and that holds a certificate
/// and key the data plane can use, read as an Ingress's are (04 §2); each other said why.
/// Of none it can use, the listener is not served.
fn certificates(
    listener: &mut ListenerOutcome,
    written: &GatewayListeners,
    gateway_namespace: &str,
    cluster: &Cluster,
) {
    let mode = written
        .tls
        .as_ref()
        .map_or("Terminate", |tls| tls.mode.as_str());
    if mode != "Terminate" {
        listener.served = Err(ListenerProblem::TlsMode(mode.to_owned()));
        return;
    }
    let refs = written
        .tls
        .iter()
        .flat_map(|tls| tls.certificate_refs.iter().flatten());
    for reference in refs {
        let namespace = reference.namespace.as_deref().unwrap_or(gateway_namespace);
        let name = &reference.name;
        let usable = if !(reference.group.is_empty() && reference.kind == "Secret") {
            Err(RefProblem {
                reason: RefReason::InvalidKind,
                why: format!(
                    "a certificateRef of kind {}/{} is not one translation reads",
                    reference.group, reference.kind
                ),
            })
        } else if let Err(why) = reachable(
            cluster,
            ("Gateway", gateway_namespace),
            ("Secret", namespace, name),
        ) {
            Err(RefProblem {
                reason: RefReason::NotPermitted,
                why,
            })
        } else if cluster.secrets_assumed {
            Ok(())
        } else {
            secret_certificate(cluster, namespace, name)
                .map(|_| ())
                .map_err(|why| RefProblem {
                    reason: RefReason::NotFound,
                    why,
                })
        };
        match usable {
            Ok(()) => listener.certificates.push(format!("{namespace}/{name}")),
            Err(why) => listener.certificate_problems.push(why),
        }
    }
    if listener.certificates.is_empty() {
        if listener.certificate_problems.is_empty() {
            listener.certificate_problems.push(RefProblem {
                reason: RefReason::NotFound,
                why: "it names no certificateRef".to_owned(),
            });
        }
        listener.served = Err(ListenerProblem::NoCertificate);
    }
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
/// apart, or that state one port for two protocols, are all refused, none chosen; each
/// other is served by the data plane's listener bound to its port.
fn listeners(listeners: &[GatewayListeners]) -> Vec<ListenerOutcome> {
    let protocols: Vec<Result<Protocol, ListenerProblem>> = listeners
        .iter()
        .map(|listener| protocol(&listener.protocol))
        .collect();
    // The protocols each port is stated for. Two, and no protocol can win: every listener
    // on the port is conflicted and the port bound for none, as the spec has a conflicted
    // listener's port configured nowhere, and as Envoy Gateway does among a Gateway's own.
    let mut stated: BTreeMap<i32, BTreeSet<&'static str>> = BTreeMap::new();
    for (listener, protocol) in listeners.iter().zip(&protocols) {
        if let Ok(protocol) = protocol {
            stated
                .entry(listener.port)
                .or_default()
                .insert(name_of(*protocol));
        }
    }
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
    // The ports taken, in the order listeners state them, each by the first accepted on it:
    // one past the data plane's slots is refused for every listener on it (04 §2).
    let mut taken: Vec<i32> = Vec::new();
    listeners
        .iter()
        .zip(protocols)
        .map(|(listener, protocol)| {
            // The schema holds a port to 1–65535.
            let port = u16::try_from(listener.port).unwrap_or_default();
            let served = protocol.and_then(|protocol| {
                let place = (
                    listener.port,
                    name_of(protocol),
                    listener.hostname.as_deref(),
                );
                match (stated.get(&listener.port), places.get(&place)) {
                    (Some(protocols), _) if protocols.len() > 1 => {
                        Err(ListenerProblem::ProtocolConflict {
                            port,
                            protocols: protocols.iter().copied().collect::<Vec<_>>().join(", "),
                        })
                    }
                    (_, Some(names)) if names.len() > 1 => {
                        Err(ListenerProblem::HostnameConflict(names.join(", ")))
                    }
                    _ if taken.contains(&listener.port) => Ok(vec![crate::listeners::name(port)]),
                    _ if taken.len() < crate::listeners::MOST_GATEWAY_PORTS => {
                        taken.push(listener.port);
                        Ok(vec![crate::listeners::name(port)])
                    }
                    _ => Err(ListenerProblem::PortPastSlots { port }),
                }
            });
            ListenerOutcome {
                name: listener.name.clone(),
                served,
                allowed: Allowed {
                    kinds: Vec::new(),
                    invalid_kinds: Vec::new(),
                    namespaces: Namespaces::None,
                },
                hostname: listener.hostname.clone(),
                port,
                certificates: Vec::new(),
                certificate_problems: Vec::new(),
            }
        })
        .collect()
}

/// The protocol of the data plane's listeners that serve a Gateway listener's, if one is
/// taken.
fn protocol(written: &str) -> Result<Protocol, ListenerProblem> {
    match written {
        "HTTP" => Ok(Protocol::Http),
        "HTTPS" => Ok(Protocol::Https),
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

/// The route kinds a listener of `protocol` can take: those whose routes its data plane
/// listener serves. GRPCRoute joins HTTP and HTTPS with its slice.
fn kinds_for(protocol: &str) -> &'static [&'static str] {
    match protocol {
        "HTTP" | "HTTPS" => &["HTTPRoute"],
        _ => &[],
    }
}

/// The routes `listener` takes, its Gateway in `namespace`; and why it takes none from any
/// namespace, where it says what translation cannot read: a `from`, or a selector's
/// operator, of a release newer than the types.
fn allowed(listener: &GatewayListeners, namespace: &str) -> (Allowed, Option<String>) {
    let carried = kinds_for(&listener.protocol);
    // `allowedRoutes`, its `namespaces` and a kind's `group` hold the schema's defaults where
    // the listener leaves them out (`from: Same`, the Gateway API's group).
    let routes = &listener.allowed_routes;
    let mut kinds: Vec<&'static str> = Vec::new();
    let mut invalid_kinds: Vec<String> = Vec::new();
    match routes.kinds.as_ref() {
        // None named: every kind its protocol carries.
        None => kinds.extend(carried),
        Some(named) => {
            for named in named {
                let group = named.group.as_str();
                match carried.iter().find(|kind| **kind == named.kind) {
                    Some(kind) if group == GROUP => {
                        if !kinds.contains(kind) {
                            kinds.push(kind);
                        }
                    }
                    _ => invalid_kinds.push(format!("{group}/{}", named.kind)),
                }
            }
        }
    }
    let (namespaces, unread) = namespaces(&routes.namespaces, namespace);
    (
        Allowed {
            kinds,
            invalid_kinds,
            namespaces,
        },
        unread,
    )
}

/// The namespaces `written` allows, its Gateway in `namespace`.
fn namespaces(
    written: &GatewayListenersAllowedRoutesNamespaces,
    namespace: &str,
) -> (Namespaces, Option<String>) {
    match written.from.as_str() {
        "Same" => (Namespaces::Same(namespace.to_owned()), None),
        "All" => (Namespaces::All, None),
        "Selector" => match written.selector.as_ref() {
            // The schema asks for a selector with `Selector`; none, and none is taken.
            None => (
                Namespaces::None,
                Some("`from: Selector` names no selector".to_owned()),
            ),
            Some(selector) => match selector_of(selector) {
                Ok(selector) => (Namespaces::Selector(selector), None),
                Err(why) => (Namespaces::None, Some(why)),
            },
        },
        other => (
            Namespaces::None,
            Some(format!("`from: {other}` is not one translation reads")),
        ),
    }
}

/// A label selector as written, read; or why it cannot be.
fn selector_of(
    written: &GatewayListenersAllowedRoutesNamespacesSelector,
) -> Result<Selector, String> {
    let requirements = written
        .match_expressions
        .iter()
        .flatten()
        .map(|expression| {
            let (key, values) = (
                expression.key.clone(),
                expression.values.clone().unwrap_or_default(),
            );
            match expression.operator.as_str() {
                "In" => Ok(Requirement::In(key, values)),
                "NotIn" => Ok(Requirement::NotIn(key, values)),
                "Exists" => Ok(Requirement::Exists(key)),
                "DoesNotExist" => Ok(Requirement::DoesNotExist(key)),
                other => Err(format!(
                    "a selector's operator `{other}` is not one Kubernetes has"
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Selector {
        labels: written.match_labels.clone().unwrap_or_default(),
        requirements,
    })
}

#[cfg(test)]
mod tests {
    use super::{GatewayListeners, Namespaces, Requirement, Selector, listeners};
    use crate::Cluster;
    use crate::listeners::MOST_GATEWAY_PORTS;
    use std::collections::{BTreeMap, BTreeSet};

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    /// Ports take the data plane's Gateway slots in the order the listeners state them, a
    /// port its first listener's: listeners sharing one share its slot, and every listener
    /// on a port past the slots is refused, so that the config never asks for more.
    #[test]
    fn ports_past_the_gateway_slots_are_refused_in_listing_order() {
        let listener = |name: &str, port: usize| -> GatewayListeners {
            serde_saphyr::from_str(&format!("{{ name: {name}, port: {port}, protocol: HTTP }}"))
                .unwrap()
        };
        let mut written: Vec<GatewayListeners> = (0..MOST_GATEWAY_PORTS)
            .map(|at| listener(&format!("l{at}"), 2000 - at))
            .collect();
        // Sharing a port, told apart by hostname.
        let named = |name: &str, port: usize, host: &str| -> GatewayListeners {
            serde_saphyr::from_str(&format!(
                "{{ name: {name}, port: {port}, protocol: HTTP, hostname: {host} }}"
            ))
            .unwrap()
        };
        written.push(listener("past", 3000));
        written.push(named("shares", 2000, "shares.example.com"));
        written.push(named("past-too", 3000, "past.example.com"));
        let outcomes = listeners(&written);
        let refused: Vec<(&str, String)> = outcomes
            .iter()
            .filter_map(|o| {
                o.served
                    .as_ref()
                    .err()
                    .map(|e| (o.name.as_str(), e.to_string()))
            })
            .collect();
        let why = format!(
            "the data plane serves at most {MOST_GATEWAY_PORTS} ports of the Gateway API; port 3000 is past them"
        );
        assert_eq!(refused, [("past", why.clone()), ("past-too", why)]);
        let served: BTreeSet<&str> = outcomes
            .iter()
            .filter_map(|o| o.served.as_ref().ok())
            .flatten()
            .map(String::as_str)
            .collect();
        assert_eq!(served.len(), MOST_GATEWAY_PORTS);
        // A listener refused for another reason takes no slot.
        let mut unsupported = written[..MOST_GATEWAY_PORTS].to_vec();
        unsupported[0] =
            serde_saphyr::from_str("{ name: tcp, port: 4000, protocol: TCP }").unwrap();
        unsupported.push(listener("fits", 3000));
        let outcomes = listeners(&unsupported);
        assert!(outcomes.last().unwrap().served.is_ok());
    }

    #[test]
    fn a_selector_matches_as_kubernetes_does() {
        let selector = Selector {
            labels: labels(&[("team", "shop")]),
            requirements: vec![
                Requirement::In("stage".to_owned(), strings(&["live", "canary"])),
                Requirement::NotIn("tier".to_owned(), strings(&["free"])),
                Requirement::Exists("owner".to_owned()),
                Requirement::DoesNotExist("frozen".to_owned()),
            ],
        };
        let all = [("team", "shop"), ("stage", "live"), ("owner", "x")];
        assert!(selector.matches(&labels(&all)));
        // NotIn holds where the label is missing, and with another value.
        assert!(selector.matches(&labels(&[
            ("team", "shop"),
            ("stage", "canary"),
            ("owner", "x"),
            ("tier", "paid")
        ])));
        for broken in [
            vec![("team", "blog"), ("stage", "live"), ("owner", "x")],
            vec![("stage", "live"), ("owner", "x")],
            vec![("team", "shop"), ("stage", "test"), ("owner", "x")],
            vec![("team", "shop"), ("owner", "x")],
            vec![
                ("team", "shop"),
                ("stage", "live"),
                ("owner", "x"),
                ("tier", "free"),
            ],
            vec![("team", "shop"), ("stage", "live")],
            vec![
                ("team", "shop"),
                ("stage", "live"),
                ("owner", "x"),
                ("frozen", ""),
            ],
        ] {
            assert!(!selector.matches(&labels(&broken)), "{broken:?}");
        }
        let empty = Selector {
            labels: BTreeMap::new(),
            requirements: Vec::new(),
        };
        assert!(
            empty.matches(&BTreeMap::new()),
            "an empty selector matches everything"
        );
    }

    #[test]
    fn a_route_is_allowed_from_the_namespaces_a_listener_says() {
        let namespace = |name: &str, pairs: &[(&str, &str)]| {
            let mut object = k8s_openapi::api::core::v1::Namespace::default();
            object.metadata.name = Some(name.to_owned());
            object.metadata.labels = Some(labels(pairs));
            object
        };
        let cluster = Cluster {
            namespaces: vec![
                namespace("shop", &[("team", "shop")]),
                namespace("blog", &[]),
            ],
            ..Cluster::default()
        };
        let same = Namespaces::Same("edge".to_owned());
        assert!(same.allow("edge", &cluster));
        assert!(!same.allow("shop", &cluster));
        assert!(Namespaces::All.allow("anywhere", &cluster));
        assert!(!Namespaces::None.allow("edge", &cluster));
        let selected = Namespaces::Selector(Selector {
            labels: labels(&[("team", "shop")]),
            requirements: Vec::new(),
        });
        assert!(selected.allow("shop", &cluster));
        assert!(!selected.allow("blog", &cluster));
        assert!(
            !selected.allow("unseen", &cluster),
            "a namespace not seen has no labels to match"
        );
    }
}
