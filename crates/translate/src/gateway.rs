//! The data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)): found by the name
//! its settings give, its class checked to name this controller, and each of its listeners
//! fitted to the data plane's own listeners bound to its port, or not accepted and why, and
//! the routes each takes: of which kinds, from which namespaces.

use crate::http_route::granted;
use crate::{Cluster, Finding, GATEWAY_CONTROLLER, Object, Plane, Problem, secret_certificate};
use edgerush_config::Protocol;
use edgerush_gateway_api::gateway::{
    Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespaces,
    GatewayListenersAllowedRoutesNamespacesSelector,
};
use std::collections::BTreeMap;
use std::fmt;

/// The API group of the Gateway API's kinds.
const GROUP: &str = "gateway.networking.k8s.io";

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
    pub certificate_problems: Vec<String>,
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
            Err(problem) => write!(f, "listener {}: not accepted: {problem}", self.name),
        }
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
    /// No listener of the data plane is bound to its port (`PortUnavailable`).
    #[error("port {0} is bound by no listener of the data plane")]
    PortUnbound(u16),
    /// An HTTPS listener none of whose `certificateRefs` can be used (`ResolvedRefs`
    /// false, `InvalidCertificateRef` or `RefNotPermitted`): not served, its hostname
    /// refused over TLS so that no other certificate answers for it.
    #[error("no certificateRef of it can be used")]
    NoCertificate,
    /// An HTTPS listener of a TLS mode other than `Terminate`, which the spec does not let
    /// one have.
    #[error("TLS mode `{0}` is not one an HTTPS listener takes")]
    TlsMode(String),
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
    let mut listeners = listeners(plane, &gateway.spec.listeners);
    for (listener, written) in listeners.iter_mut().zip(&gateway.spec.listeners) {
        if listener.served.is_ok() && written.protocol == "HTTPS" {
            certificates(listener, written, &named.namespace, cluster);
        }
        for why in &listener.certificate_problems {
            report.push(Finding {
                object: object.clone(),
                problem: Problem::ListenerCertificate {
                    name: listener.name.clone(),
                    why: why.clone(),
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
            problem: Problem::NoListenerAccepted,
        });
    }
    Some(GatewayOutcome {
        gateway: object,
        listeners,
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
            Err(format!(
                "a certificateRef of kind {}/{} is not one translation reads",
                reference.group, reference.kind
            ))
        } else if namespace != gateway_namespace
            && !granted(
                cluster,
                ("Gateway", gateway_namespace),
                ("Secret", namespace, name),
            )
        {
            Err(format!(
                "Secret {namespace}/{name} is another namespace's, and no ReferenceGrant there \
                 lets Gateways of {gateway_namespace} reach it"
            ))
        } else if cluster.secrets_assumed {
            Ok(())
        } else {
            secret_certificate(cluster, namespace, name).map(|_| ())
        };
        match usable {
            Ok(()) => listener.certificates.push(format!("{namespace}/{name}")),
            Err(why) => listener.certificate_problems.push(why),
        }
    }
    if listener.certificates.is_empty() {
        if listener.certificate_problems.is_empty() {
            listener
                .certificate_problems
                .push("it names no certificateRef".to_owned());
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
                allowed: Allowed {
                    kinds: Vec::new(),
                    invalid_kinds: Vec::new(),
                    namespaces: Namespaces::None,
                },
                hostname: listener.hostname.clone(),
                // The schema holds a port to 1–65535.
                port: u16::try_from(listener.port).unwrap_or_default(),
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
    use super::{Namespaces, Requirement, Selector};
    use crate::Cluster;
    use std::collections::BTreeMap;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
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
