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
use edgerush_gateway_api::listener_set::ListenerSet;
use k8s_openapi::serde_json;
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
    /// Each of its listeners, its own in the order it lists them, then those of each
    /// ListenerSet attached to it, in precedence (04 §2).
    pub listeners: Vec<ListenerOutcome>,
    /// The ports its accepted listeners state, and the protocol each is bound for: the data
    /// plane's listeners. A port HTTPS and TLS listeners share is bound for HTTPS, passing
    /// the TLS ones' names through (17 §2).
    pub ports: BTreeMap<u16, Protocol>,
    /// Each ListenerSet naming it as its parent, in precedence: those it takes, whose
    /// listeners are among `listeners`, and those it does not.
    pub sets: Vec<SetOutcome>,
    /// The client certificate it shows every backend it reaches over TLS
    /// (`spec.tls.backend.clientCertificateRef`), by the config's name for it,
    /// `<namespace>/<Secret>`; or why it cannot be used, which leaves those backends reached
    /// without one (04 §2). None where it names none.
    pub client_certificate: Option<Result<String, RefProblem>>,
}

/// A ListenerSet naming the data plane's Gateway as its parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOutcome {
    /// The ListenerSet.
    pub set: Object,
    /// Its generation: what its status says of.
    pub generation: i64,
    /// Whether the Gateway's `allowedListeners` takes it.
    pub attached: bool,
}

impl GatewayOutcome {
    /// The Gateway's own listeners, those of its `spec.listeners`: what a route naming the
    /// Gateway attaches to, and its status says.
    pub fn own(&self) -> impl Iterator<Item = &ListenerOutcome> {
        self.listeners
            .iter()
            .filter(|listener| listener.set.is_none())
    }
}

/// One of a Gateway's listeners: accepted, and served by the data plane's listeners named,
/// or not, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerOutcome {
    /// Its name, unique within the Gateway or ListenerSet stating it.
    pub name: String,
    /// The ListenerSet stating it, attached to the Gateway; none for the Gateway's own.
    pub set: Option<Object>,
    /// The data plane's listeners bound to its port for its protocol, or why it is not
    /// accepted.
    pub served: Result<Vec<String>, ListenerProblem>,
    /// The routes it takes: what its status says of every listener, accepted or not.
    pub allowed: Allowed,
    /// Its hostname, if it has one: none is every host.
    pub hostname: Option<String>,
    /// Its port.
    pub port: u16,
    /// Whether it passes its connections through, a TLS listener in `Passthrough` mode:
    /// carried by its TLS routes rather than served, on a port it may share with HTTPS
    /// listeners (17 §2).
    pub passes_through: bool,
    /// An HTTPS listener's certificates that can be used, by the config's names for them,
    /// `<namespace>/<Secret>`, in the order its `certificateRefs` list them.
    pub certificates: Vec<String>,
    /// Its `certificateRefs` that cannot be used, each with why (`ResolvedRefs` false).
    pub certificate_problems: Vec<RefProblem>,
}

impl fmt::Display for ListenerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "listener {}", self.name)?;
        if let Some(set) = &self.set {
            write!(f, " of {set}")?;
        }
        match &self.served {
            Ok(listeners) if self.certificates.is_empty() => write!(
                f,
                ": served on {}, taking {}",
                listeners.join(", "),
                self.allowed
            ),
            Ok(listeners) => write!(
                f,
                ": served on {}, presenting {}, taking {}",
                listeners.join(", "),
                self.certificates.join(", "),
                self.allowed
            ),
            Err(problem) => write!(f, ": {}: {problem}", problem.verdict()),
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
    /// A listener before it has its port, protocol and hostname, so the two could not be
    /// told apart: that one is accepted, this not (`HostnameConflict`).
    #[error("{by}, which comes first, has its port, protocol and hostname")]
    HostnameConflict {
        /// The listener before it, as said: by name within one resource, by whose it is
        /// beyond.
        by: String,
    },
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
    /// A TLS listener of a mode other than `Passthrough`: the gateway terminates TLS on
    /// HTTPS listeners alone (`UnsupportedValue`, as conformance has it of an implementation
    /// that does not claim `TLSRouteModeTerminate`).
    #[error("TLS mode `{0}` is not served: a TLS listener passes its connections through")]
    PassthroughOnly(String),
    /// Its Gateway or ListenerSet states its port for TCP and for HTTP, HTTPS or TLS, which
    /// the spec counts not distinct and has none of accepted (`ProtocolConflict`, Q-c1).
    #[error(
        "its {kind} states port {port} for TCP and for HTTP, HTTPS or TLS, which the spec does \
         not let share a port: no listener of it there is accepted"
    )]
    NotDistinct {
        /// The port.
        port: u16,
        /// `Gateway` or `ListenerSet`.
        kind: &'static str,
    },
    /// A listener before it states its port for another protocol, and has the port: that
    /// one is accepted, this not (`ProtocolConflict`).
    #[error("port {port} is taken for {protocol} by {by}, which comes first")]
    ProtocolConflict {
        /// The port.
        port: u16,
        /// The protocol it is taken for, as the data plane's config writes it.
        protocol: &'static str,
        /// The listener that has it, as said: by name within one resource, by whose it is
        /// beyond.
        by: String,
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
    /// A ListenerSet's listener whose port is past the data plane's slots for the Gateway
    /// API's ports, which the Gateway's listeners and the ListenerSets before it take
    /// (`TooManyListeners`, GEP-1713).
    #[error(
        "the data plane serves at most {most} ports of the Gateway API, which the Gateway \
         and the ListenerSets before this one take; port {port} is past them",
        most = crate::listeners::MOST_GATEWAY_PORTS
    )]
    TooManyListeners {
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
    // The Gateway's own listeners, then those of each ListenerSet it takes, in precedence.
    let mut written: Vec<Written> = gateway
        .spec
        .listeners
        .iter()
        .map(|listener| Written {
            listener: listener.clone(),
            set: None,
            namespace: named.namespace.clone(),
        })
        .collect();
    let (sets, theirs) = attached_sets(gateway, &object, cluster, report);
    written.extend(theirs);
    let mut listeners = listeners(&written, &object);
    // Bound as long as one listener on them is accepted: one whose certificates cannot be
    // used too, which keeps its port refusing its hostname (04 §2). For HTTPS where an HTTPS
    // listener shares the port with TLS ones.
    let mut ports: BTreeMap<u16, Protocol> = BTreeMap::new();
    for (listener, written) in listeners.iter().zip(&written) {
        let (Ok(_), Ok(protocol)) = (&listener.served, protocol(&written.listener)) else {
            continue;
        };
        let bound = ports.entry(listener.port).or_insert(protocol);
        if protocol == Protocol::Https {
            *bound = Protocol::Https;
        }
    }
    for (listener, written) in listeners.iter_mut().zip(&written) {
        // Said of the Gateway or the ListenerSet that states it, whose namespace its
        // Secrets and routes are taken from, and whose ReferenceGrants are asked.
        let (stater, kind) = match &written.set {
            Some(set) => (set.clone(), "ListenerSet"),
            None => (object.clone(), "Gateway"),
        };
        if listener.served.is_ok() && written.listener.protocol == "HTTPS" {
            certificates(
                listener,
                &written.listener,
                (kind, &written.namespace),
                cluster,
            );
        }
        for why in &listener.certificate_problems {
            report.push(Finding {
                object: stater.clone(),
                problem: Problem::ListenerCertificate {
                    name: listener.name.clone(),
                    why: why.why.clone(),
                },
            });
        }
        let (allowed, unread) = allowed(&written.listener, &written.namespace);
        if let Err(problem) = &listener.served {
            report.push(Finding {
                object: stater.clone(),
                problem: Problem::Listener {
                    name: listener.name.clone(),
                    problem: problem.clone(),
                },
            });
        }
        if !allowed.invalid_kinds.is_empty() {
            report.push(Finding {
                object: stater.clone(),
                problem: Problem::ListenerKinds {
                    name: listener.name.clone(),
                    kinds: allowed.invalid_kinds.join(", "),
                },
            });
        }
        if let Some(why) = unread {
            report.push(Finding {
                object: stater.clone(),
                problem: Problem::ListenerNamespaces {
                    name: listener.name.clone(),
                    why,
                },
            });
        }
        listener.allowed = allowed;
    }
    // The Gateway's own: its `Accepted` and `Programmed` are of them alone (GEP-1713).
    if !listeners
        .iter()
        .any(|listener| listener.set.is_none() && listener.served.is_ok())
    {
        report.push(Finding {
            object: object.clone(),
            problem: Problem::NoListenerServed,
        });
    }
    let client_certificate = client_certificate(gateway, &named.namespace, cluster);
    if let Some(Err(why)) = &client_certificate {
        report.push(Finding {
            object: object.clone(),
            problem: Problem::ClientCertificate(why.why.clone()),
        });
    }
    Some(GatewayOutcome {
        gateway: object,
        generation: gateway.metadata.generation.unwrap_or_default(),
        listeners,
        ports,
        sets,
        client_certificate,
    })
}

/// The client certificate `gateway`, of `namespace`, shows backends it reaches over TLS, if
/// it names one, by the config's name for it: a Secret of its namespace, or of one whose
/// ReferenceGrant lets Gateways of `namespace` reach it, asked first, as the spec has
/// `RefNotPermitted` before `InvalidClientCertificateRef`; holding a certificate and key the
/// data plane can use, read as a listener's are (04 §2). Or why it cannot be used.
fn client_certificate(
    gateway: &Gateway,
    namespace: &str,
    cluster: &Cluster,
) -> Option<Result<String, RefProblem>> {
    let reference = gateway
        .spec
        .tls
        .as_ref()?
        .backend
        .as_ref()?
        .client_certificate_ref
        .as_ref()?;
    let in_namespace = reference.namespace.as_deref().unwrap_or(namespace);
    let name = &reference.name;
    let kind = if reference.group.is_empty() {
        reference.kind.clone()
    } else {
        format!("{}/{}", reference.group, reference.kind)
    };
    if let Err(why) = reachable(
        cluster,
        ("Gateway", namespace),
        (kind.as_str(), in_namespace, name),
    ) {
        return Some(Err(RefProblem::new(RefReason::NotPermitted, why)));
    }
    if !(reference.group.is_empty() && reference.kind == "Secret") {
        return Some(Err(RefProblem::new(
            RefReason::InvalidKind,
            format!("a clientCertificateRef of kind {kind} is not one translation reads"),
        )));
    }
    if !cluster.secrets_assumed
        && let Err(why) = secret_certificate(cluster, in_namespace, name)
    {
        return Some(Err(RefProblem::new(RefReason::NotFound, why)));
    }
    Some(Ok(format!("{in_namespace}/{name}")))
}

/// An HTTPS listener's certificates, put in `listener`: each of its `certificateRefs` that
/// names a Secret, of the namespace of the `kind` stating it (the Gateway, or a ListenerSet
/// attached to it) or of one whose ReferenceGrant lets that kind of it reach that Secret,
/// neither's grants the other's (GEP-1713), asked first as conformance has it, and that holds a certificate
/// and key the data plane can use, read as an Ingress's are (04 §2); each other said why.
/// Of none it can use, the listener is not served.
fn certificates(
    listener: &mut ListenerOutcome,
    written: &GatewayListeners,
    (kind, stater_namespace): (&'static str, &str),
    cluster: &Cluster,
) {
    let mode = tls_mode(written);
    if mode != "Terminate" {
        listener.served = Err(ListenerProblem::TlsMode(mode.to_owned()));
        return;
    }
    let refs = written
        .tls
        .iter()
        .flat_map(|tls| tls.certificate_refs.iter().flatten());
    for reference in refs {
        let namespace = reference.namespace.as_deref().unwrap_or(stater_namespace);
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
            (kind, stater_namespace),
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

/// Each ListenerSet naming `gateway`, `object`, as its parent, in precedence, oldest first
/// and then by namespace and name, and the listeners of those of a namespace its
/// `allowedListeners` takes (04 §2, GEP-1713). One naming it that it does not take is said.
fn attached_sets(
    gateway: &Gateway,
    object: &Object,
    cluster: &Cluster,
    report: &mut Vec<Finding>,
) -> (Vec<SetOutcome>, Vec<Written>) {
    let mut naming: Vec<&ListenerSet> = cluster
        .listener_sets
        .iter()
        .filter(|set| {
            let parent = &set.spec.parent_ref;
            let namespace = set.metadata.namespace.as_deref().unwrap_or("default");
            parent.group == GROUP
                && parent.kind == "Gateway"
                && parent.name == object.name
                && parent.namespace.as_deref().unwrap_or(namespace) == object.namespace
        })
        .collect();
    naming.sort_by(|a, b| {
        let key = |set: &ListenerSet| {
            (
                set.metadata.creation_timestamp.clone(),
                set.metadata.namespace.clone().unwrap_or_default(),
                set.metadata.name.clone(),
            )
        };
        key(a).cmp(&key(b))
    });
    let mut written = Vec::new();
    let mut sets = Vec::new();
    for set in naming {
        let stater = Object::of("ListenerSet", &set.metadata);
        let attached = takes_sets_of(gateway, &stater.namespace, cluster);
        sets.push(SetOutcome {
            set: stater.clone(),
            generation: set.metadata.generation.unwrap_or_default(),
            attached,
        });
        if !attached {
            report.push(Finding {
                object: stater,
                problem: Problem::ListenerSetNotAllowed(object.clone()),
            });
            continue;
        }
        for listener in &set.spec.listeners {
            // One listener's fields as the other's: the two types are written from one
            // schema of the Gateway API's (`ListenerEntry`), each passing over what it lacks.
            let Ok(listener) = serde_json::to_value(listener).and_then(serde_json::from_value)
            else {
                continue;
            };
            written.push(Written {
                listener,
                set: Some(stater.clone()),
                namespace: stater.namespace.clone(),
            });
        }
    }
    (sets, written)
}

/// Whether `gateway` takes ListenerSets of `namespace`, as its `allowedListeners` says: none
/// where it says nothing, or `None` (GEP-1713).
fn takes_sets_of(gateway: &Gateway, namespace: &str, cluster: &Cluster) -> bool {
    let Some(allowed) = &gateway.spec.allowed_listeners else {
        return false;
    };
    if allowed.namespaces.from == "None" {
        return false;
    }
    // Its namespaces are written as a listener's routes' are, by one schema.
    let Ok(written) = serde_json::to_value(&allowed.namespaces)
        .and_then(serde_json::from_value::<GatewayListenersAllowedRoutesNamespaces>)
    else {
        return false;
    };
    let own = gateway.metadata.namespace.as_deref().unwrap_or("default");
    namespaces(&written, own).0.allow(namespace, cluster)
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

/// A listener as written, with whose it is, the Gateway's own or a ListenerSet's attached
/// to it, and the namespace its references and routes are taken from: its stater's.
pub(crate) struct Written {
    pub(crate) listener: GatewayListeners,
    pub(crate) set: Option<Object>,
    pub(crate) namespace: String,
}

/// Each listener's outcome, in the order listeners merge (04 §2): the Gateway's own, then
/// each attached ListenerSet's. A protocol not taken is refused first, and plays no part in
/// what conflicts (the spec: a listener of a protocol not supported is not accepted, and
/// the rules of distinctness are for those that are); then the first listener on a port
/// takes it for its protocol, HTTPS and TLS together, which share a port told apart by
/// hostname, and a later one stating it for another, or in a place an earlier one has
/// (port, protocol and hostname, HTTPS and TLS as one), is refused, as the spec has a
/// conflicting listener refused in precedence and conformance pins it; each other is served by the
/// data plane's listener bound to its port, a port past the data plane's slots refused for
/// every listener on it. An earlier listener of another resource is said by whose it is,
/// never by name, as nothing of a parent or a sibling is told a ListenerSet (GEP-1713).
///
/// A TCP listener is distinct by port alone (the spec): its `hostname`, which its schema
/// forbids, is not read. Where one resource's listeners state a port for TCP and for HTTP,
/// HTTPS or TLS, none of them on it is accepted, as the spec has it (Q-c1), and, refused,
/// they take no port: a later resource's listener there is judged as any (Q-c1b), so that
/// one team's ListenerSet never keeps a port from another's.
pub(crate) fn listeners(written: &[Written], gateway: &Object) -> Vec<ListenerOutcome> {
    // The ports each resource (the Gateway's own listeners, `None`, or a ListenerSet's)
    // states for TCP and for HTTP, HTTPS or TLS.
    let not_distinct: BTreeSet<(Option<&Object>, i32)> = {
        let mut stated: BTreeMap<(Option<&Object>, i32), (bool, bool)> = BTreeMap::new();
        for listener in written {
            let (tcp, other) = stated
                .entry((listener.set.as_ref(), listener.listener.port))
                .or_default();
            match listener.listener.protocol.as_str() {
                "TCP" => *tcp = true,
                "HTTP" | "HTTPS" | "TLS" => *other = true,
                _ => {}
            }
        }
        stated
            .into_iter()
            .filter(|(_, (tcp, other))| *tcp && *other)
            .map(|(place, _)| place)
            .collect()
    };
    // Each port taken, by the protocols it is for, the protocol of the listener that took
    // it and that listener; each place, by the listener in it: only those accepted, by
    // their place in `written`.
    let mut ports: BTreeMap<i32, (&'static str, &'static str, usize)> = BTreeMap::new();
    let mut places: BTreeMap<(i32, &'static str, Option<&str>), usize> = BTreeMap::new();
    let by = |earlier: usize, now: &Written| {
        let earlier = &written[earlier];
        if earlier.set == now.set {
            format!("listener {}", earlier.listener.name)
        } else if earlier.set.is_none() {
            format!("a listener of {gateway}")
        } else {
            "a listener of another ListenerSet attached to the same Gateway".to_owned()
        }
    };
    written
        .iter()
        .enumerate()
        .map(|(at, stated)| {
            let listener = &stated.listener;
            // The schema holds a port to 1–65535.
            let port = u16::try_from(listener.port).unwrap_or_default();
            let set = stated.set.as_ref();
            let served = protocol(listener).and_then(|protocol| {
                if not_distinct.contains(&(set, listener.port)) {
                    return Err(ListenerProblem::NotDistinct {
                        port,
                        kind: if set.is_some() {
                            "ListenerSet"
                        } else {
                            "Gateway"
                        },
                    });
                }
                let name = name_of(protocol);
                // HTTPS and TLS share a port, one data-plane listener reading each
                // ClientHello (Q-t1): one hostname on both is a conflict of hostnames.
                let family = if protocol == Protocol::Tls {
                    "https"
                } else {
                    name
                };
                let hostname = listener
                    .hostname
                    .as_deref()
                    .filter(|_| protocol != Protocol::Tcp);
                let place = (listener.port, family, hostname);
                match (ports.get(&listener.port), places.get(&place)) {
                    (Some((taken, protocol, earlier)), _) if *taken != family => {
                        Err(ListenerProblem::ProtocolConflict {
                            port,
                            protocol,
                            by: by(*earlier, stated),
                        })
                    }
                    (_, Some(earlier)) => Err(ListenerProblem::HostnameConflict {
                        by: by(*earlier, stated),
                    }),
                    (Some(_), None) => {
                        places.insert(place, at);
                        Ok(vec![crate::listeners::name(port)])
                    }
                    (None, None) if ports.len() < crate::listeners::MOST_GATEWAY_PORTS => {
                        ports.insert(listener.port, (family, name, at));
                        places.insert(place, at);
                        Ok(vec![crate::listeners::name(port)])
                    }
                    (None, None) if stated.set.is_some() => {
                        Err(ListenerProblem::TooManyListeners { port })
                    }
                    (None, None) => Err(ListenerProblem::PortPastSlots { port }),
                }
            });
            ListenerOutcome {
                name: listener.name.clone(),
                set: stated.set.clone(),
                served,
                allowed: Allowed {
                    kinds: Vec::new(),
                    invalid_kinds: Vec::new(),
                    namespaces: Namespaces::None,
                },
                hostname: listener
                    .hostname
                    .clone()
                    .filter(|_| listener.protocol != "TCP"),
                port,
                passes_through: listener.protocol == "TLS",
                certificates: Vec::new(),
                certificate_problems: Vec::new(),
            }
        })
        .collect()
}

/// The protocol of the data plane's listeners that serve a Gateway listener's, if one is
/// taken: a TLS listener's in `Passthrough` mode alone, whose `certificateRefs` are not read
/// (the spec: ignored in that mode).
fn protocol(written: &GatewayListeners) -> Result<Protocol, ListenerProblem> {
    match written.protocol.as_str() {
        "HTTP" => Ok(Protocol::Http),
        "HTTPS" => Ok(Protocol::Https),
        "TLS" => match tls_mode(written) {
            "Passthrough" => Ok(Protocol::Tls),
            other => Err(ListenerProblem::PassthroughOnly(other.to_owned())),
        },
        "TCP" => Ok(Protocol::Tcp),
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
        Protocol::Udp => "udp",
    }
}

/// A listener's TLS mode: `Terminate` where it states none, as its schema defaults it.
fn tls_mode(written: &GatewayListeners) -> &str {
    written
        .tls
        .as_ref()
        .map_or("Terminate", |tls| tls.mode.as_str())
}

/// The route kinds a listener of `protocol` can take: those whose routes its data plane
/// listener serves. One not accepted, a TLS listener not in `Passthrough` mode among them,
/// says it supports none (status).
fn kinds_for(protocol: &str) -> &'static [&'static str] {
    match protocol {
        "HTTP" | "HTTPS" => &["HTTPRoute", "GRPCRoute"],
        "TLS" => &["TLSRoute"],
        "TCP" => &["TCPRoute"],
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
    use super::{GatewayListeners, ListenerOutcome, Namespaces, Requirement, Selector, Written};
    use crate::{Cluster, Object};

    /// The outcomes of a Gateway's own listeners, `written`.
    fn listeners(written: &[GatewayListeners]) -> Vec<ListenerOutcome> {
        let written: Vec<Written> = written
            .iter()
            .map(|listener| Written {
                listener: listener.clone(),
                set: None,
                namespace: "edge".to_owned(),
            })
            .collect();
        let gateway = Object {
            kind: "Gateway",
            namespace: "edge".to_owned(),
            name: "web".to_owned(),
        };
        super::listeners(&written, &gateway)
    }
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

    /// Listeners that clash go to the first: a later one stating a port taken for another
    /// protocol, or in the place of an earlier one, is refused, and the earlier served.
    #[test]
    fn the_first_of_listeners_that_clash_is_served() {
        let written: Vec<GatewayListeners> = [
            "{ name: plain, port: 8443, protocol: HTTP }",
            "{ name: secure, port: 8443, protocol: HTTPS }",
            "{ name: first, port: 8080, protocol: HTTP, hostname: a.example.com }",
            "{ name: second, port: 8080, protocol: HTTP, hostname: a.example.com }",
            "{ name: apart, port: 8080, protocol: HTTP, hostname: b.example.com }",
            "{ name: dns, port: 8080, protocol: UDP }",
        ]
        .iter()
        .map(|yaml| serde_saphyr::from_str(yaml).unwrap())
        .collect();
        let said: Vec<String> = listeners(&written)
            .iter()
            .map(|o| match &o.served {
                Ok(on) => format!("{}: {}", o.name, on.join(", ")),
                Err(why) => format!("{}: {why}", o.name),
            })
            .collect();
        assert_eq!(
            said,
            [
                "plain: port-8443",
                "secure: port 8443 is taken for http by listener plain, which comes first",
                "first: port-8080",
                "second: listener first, which comes first, has its port, protocol and hostname",
                "apart: port-8080",
                "dns: protocol `UDP` is not one the gateway serves",
            ]
        );
    }

    /// A TCP listener is distinct by port alone, its hostname not read; where one resource
    /// states a port for TCP and for HTTP, HTTPS or TLS, none of its listeners there is
    /// accepted (the spec, Q-c1); an earlier resource's listener there keeps it, as GEP-1713
    /// has the first kept, and a later's is judged as any, the refused taking no port (Q-c1b).
    #[test]
    fn listeners_beside_a_tcp_listener_on_its_port_are_not_accepted() {
        let gateway = Object {
            kind: "Gateway",
            namespace: "edge".to_owned(),
            name: "web".to_owned(),
        };
        let set = |name: &str| Object {
            kind: "ListenerSet",
            namespace: "shop".to_owned(),
            name: name.to_owned(),
        };
        let written: Vec<Written> = [
            (None, "{ name: g-http, port: 80, protocol: HTTP }"),
            (None, "{ name: g-tcp, port: 80, protocol: TCP }"),
            (
                None,
                "{ name: g-web, port: 8080, protocol: HTTP, hostname: a.example.com }",
            ),
            (
                None,
                "{ name: g-db, port: 5432, protocol: TCP, hostname: db.example.com }",
            ),
            (None, "{ name: g-db2, port: 5432, protocol: TCP }"),
            (
                None,
                "{ name: g-sni, port: 7000, protocol: TLS, tls: { mode: Passthrough } }",
            ),
            (None, "{ name: g-raw, port: 7000, protocol: TCP }"),
            (None, "{ name: g-secure, port: 7443, protocol: HTTPS }"),
            (None, "{ name: g-raw2, port: 7443, protocol: TCP }"),
            (
                Some("one"),
                "{ name: one-80, port: 80, protocol: HTTP, hostname: b.example.com }",
            ),
            (Some("one"), "{ name: one-8080, port: 8080, protocol: TCP }"),
            (
                Some("one"),
                "{ name: one-8080h, port: 8080, protocol: HTTP, hostname: d.example.com }",
            ),
            (
                Some("one"),
                "{ name: one-9000, port: 9000, protocol: HTTPS, hostname: e.example.com }",
            ),
            (
                Some("one"),
                "{ name: one-9000t, port: 9000, protocol: TCP }",
            ),
            (Some("two"), "{ name: two-9000, port: 9000, protocol: TCP }"),
            (
                Some("two"),
                "{ name: two-8080, port: 8080, protocol: HTTP, hostname: c.example.com }",
            ),
        ]
        .iter()
        .map(|(of, yaml)| Written {
            listener: serde_saphyr::from_str(yaml).unwrap(),
            set: of.map(set),
            namespace: of.map_or("edge", |_| "shop").to_owned(),
        })
        .collect();
        let outcomes = super::listeners(&written, &gateway);
        let said: Vec<String> = outcomes
            .iter()
            .map(|o| match &o.served {
                Ok(on) => format!("{}: {}", o.name, on.join(", ")),
                Err(why) => format!("{}: {why}", o.name),
            })
            .collect();
        let gateway_s = |port: u16| {
            format!(
                "its Gateway states port {port} for TCP and for HTTP, HTTPS or TLS, which the \
                 spec does not let share a port: no listener of it there is accepted"
            )
        };
        let set_s = |port: u16| {
            format!(
                "its ListenerSet states port {port} for TCP and for HTTP, HTTPS or TLS, which the \
                 spec does not let share a port: no listener of it there is accepted"
            )
        };
        assert_eq!(
            said,
            [
                format!("g-http: {}", gateway_s(80)),
                format!("g-tcp: {}", gateway_s(80)),
                "g-web: port-8080".to_owned(),
                "g-db: port-5432".to_owned(),
                "g-db2: listener g-db, which comes first, has its port, protocol and hostname"
                    .to_owned(),
                format!("g-sni: {}", gateway_s(7000)),
                format!("g-raw: {}", gateway_s(7000)),
                format!("g-secure: {}", gateway_s(7443)),
                format!("g-raw2: {}", gateway_s(7443)),
                "one-80: port-80".to_owned(),
                format!("one-8080: {}", set_s(8080)),
                format!("one-8080h: {}", set_s(8080)),
                format!("one-9000: {}", set_s(9000)),
                format!("one-9000t: {}", set_s(9000)),
                "two-9000: port-9000".to_owned(),
                "two-8080: port-8080".to_owned(),
            ]
        );
        // A TCP listener's hostname is not read: none claimed, none intersected.
        assert_eq!(outcomes[3].hostname, None);
    }

    /// A ListenerSet's listener on a port past the slots the Gateway's own listeners take is
    /// refused `TooManyListeners`, the GEP's reason, not the Gateway's `PortUnavailable`.
    #[test]
    fn a_listener_sets_port_past_the_slots_is_too_many_listeners() {
        let gateway = Object {
            kind: "Gateway",
            namespace: "edge".to_owned(),
            name: "web".to_owned(),
        };
        let set = Object {
            kind: "ListenerSet",
            namespace: "shop".to_owned(),
            name: "team".to_owned(),
        };
        let listener = |name: &str, port: usize| -> GatewayListeners {
            serde_saphyr::from_str(&format!("{{ name: {name}, port: {port}, protocol: HTTP }}"))
                .unwrap()
        };
        let mut written: Vec<Written> = (0..MOST_GATEWAY_PORTS)
            .map(|at| Written {
                listener: listener(&format!("l{at}"), 2000 + at),
                set: None,
                namespace: "edge".to_owned(),
            })
            .collect();
        written.push(Written {
            listener: listener("team", 3000),
            set: Some(set),
            namespace: "shop".to_owned(),
        });
        let outcomes = super::listeners(&written, &gateway);
        let last = outcomes.last().unwrap();
        assert_eq!(
            last.served
                .as_ref()
                .err()
                .map(ToString::to_string)
                .as_deref(),
            Some(
                "the data plane serves at most 64 ports of the Gateway API, which the Gateway \
                 and the ListenerSets before this one take; port 3000 is past them"
            )
        );
        assert!(matches!(
            last.served,
            Err(super::ListenerProblem::TooManyListeners { port: 3000 })
        ));
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
            serde_saphyr::from_str("{ name: dns, port: 4000, protocol: UDP }").unwrap();
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

    /// A Gateway's `clientCertificateRef` is asked first whether it may be reached, as the
    /// spec has `RefNotPermitted` before `InvalidClientCertificateRef`; then whether it is a
    /// Secret; then whether the Secret holds a certificate and key the data plane can use,
    /// unless Secrets are assumed.
    #[test]
    fn a_client_certificate_is_granted_then_of_its_kind_then_usable() {
        use crate::RefReason::{InvalidKind, NotFound, NotPermitted};
        use edgerush_gateway_api::gateway::Gateway;
        use k8s_openapi::api::core::v1::Secret;

        // The tests' Secrets of namespace shop, and shop-tls again in namespace certs.
        let mut secrets: Vec<Secret> = include_str!("../tests/golden/shared.yaml")
            .split("\n---\n")
            .filter(|doc| doc.contains("\nkind: Secret\n"))
            .map(|doc| serde_saphyr::from_str(doc).unwrap())
            .collect();
        let mut elsewhere = secrets
            .iter()
            .find(|secret| secret.metadata.name.as_deref() == Some("shop-tls"))
            .unwrap()
            .clone();
        elsewhere.metadata.namespace = Some("certs".to_owned());
        secrets.push(elsewhere);
        let grant = serde_saphyr::from_str(
            "metadata: { name: gateways-of-shop, namespace: certs }
spec:
  from: [{ group: gateway.networking.k8s.io, kind: Gateway, namespace: shop }]
  to: [{ group: \"\", kind: Secret, name: shop-tls }]
",
        )
        .unwrap();
        let cluster = Cluster {
            secrets,
            reference_grants: vec![grant],
            ..Cluster::default()
        };
        let of = |tls: &str, cluster: &Cluster| {
            let gateway: Gateway = serde_saphyr::from_str(&format!(
                "metadata: {{ name: web, namespace: shop }}
spec: {{ gatewayClassName: edgerush, listeners: []{tls} }}
"
            ))
            .unwrap();
            super::client_certificate(&gateway, "shop", cluster)
                .map(|came_to| came_to.map_err(|problem| problem.reason))
        };
        let reference = |reference: &str| {
            format!(", tls: {{ backend: {{ clientCertificateRef: {reference} }} }}")
        };
        let ok = |name: &str| Some(Ok(name.to_owned()));

        assert_eq!(of("", &cluster), None);
        assert_eq!(of(", tls: { backend: {} }", &cluster), None);
        for (written, came_to) in [
            ("{ name: shop-tls }", ok("shop/shop-tls")),
            ("{ name: shop-tls, namespace: shop }", ok("shop/shop-tls")),
            ("{ name: shop-tls, namespace: certs }", ok("certs/shop-tls")),
            ("{ name: other, namespace: certs }", Some(Err(NotPermitted))),
            (
                "{ name: shop-tls, namespace: blog }",
                Some(Err(NotPermitted)),
            ),
            // Of a kind the grant does not name: not permitted, before its kind is asked.
            (
                "{ kind: ConfigMap, name: shop-tls, namespace: certs }",
                Some(Err(NotPermitted)),
            ),
            (
                "{ kind: ConfigMap, name: shop-tls }",
                Some(Err(InvalidKind)),
            ),
            (
                "{ group: example.com, kind: Secret, name: shop-tls }",
                Some(Err(InvalidKind)),
            ),
            ("{ name: absent }", Some(Err(NotFound))),
            ("{ name: keyless }", Some(Err(NotFound))),
            ("{ name: mismatched }", Some(Err(NotFound))),
        ] {
            assert_eq!(of(&reference(written), &cluster), came_to, "{written}");
        }

        // A namespace not watched: no grant there can be seen.
        let unwatched = Cluster {
            watched: Some(vec!["shop".to_owned()]),
            ..cluster.clone()
        };
        assert_eq!(
            of(
                &reference("{ name: shop-tls, namespace: certs }"),
                &unwatched
            ),
            Some(Err(NotPermitted))
        );

        // Assumed, a Secret is taken as named, not looked at; a grant is still asked.
        let assumed = Cluster {
            secrets_assumed: true,
            ..cluster.clone()
        };
        assert_eq!(
            of(&reference("{ name: absent }"), &assumed),
            ok("shop/absent")
        );
        assert_eq!(
            of(&reference("{ name: shop-tls, namespace: blog }"), &assumed),
            Some(Err(NotPermitted))
        );
    }
}
