//! Translation: the Kubernetes objects a control plane watches, into the config one data
//! plane runs ([04 §2](../../../docs/04-control-plane.md)).
//!
//! A pure crate: no I/O, no clocks. [`translate`] is a function of the objects and of the
//! data plane's own settings, so that its tests need no cluster, and the control plane, its
//! admission webhook and `validate --from-manifests` all translate alike.
//!
//! **It never fails as a whole.** What it cannot translate is left out, object by object,
//! and said in its report. So far it translates Ingresses whose rules route paths, of any
//! path type and on any host, and whose default backend takes the rest, to a Service's
//! port, and that carry no annotations but the redirect's below; an Ingress
//! that asks for anything more is left out whole, reported as not supported yet, rather
//! than translated in part. A path whose Service, or port, is not there, or whose port's
//! `appProtocol` the gateway does not speak, is translated with an invalid backend, answered
//! 500, and reported too. Hosts its `tls` lists are served on the `https` listeners as well,
//! with their Secret's certificate, read as the data plane reads one; a host whose Secret
//! is not there, cannot be used or does not cover it is refused over HTTPS, and reported.
//! Over plain HTTP, a host its `tls` lists is redirected there, secured or not, unless the
//! data plane's settings or the Ingress's `gateway.edgerush.dev/https-redirect` annotation
//! say not, and is served over plain HTTP only where they do.
//!
//! Where the data plane is a Gateway, its class is checked to name this controller and
//! each of its listeners fitted to the data plane's own, or refused and why ([`gateway`]);
//! each HTTPRoute that names it is attached to the listeners that take it, and each
//! listener's hostname claimed for isolation ([`http_route`]), and its rules translated:
//! matches and backends, a rule with what translation does not take yet answering 500 by
//! itself, one whose matches cannot be built left out.
//! Every host a claim of one API takes from the other's is said.

use edgerush_certificates::Identity;
use edgerush_config::{
    Backend, Certificate, Config, DataPlane, Endpoints, Filter, Forward, Header, HeaderChanges,
    Hostname, Listener, ListenerCertificate, LoadBalancer, Match, PathMatch, Protocol, Query,
    Redirect, Route, Rule, Scheme, StatedNames, Upstream, UpstreamProtocol, UpstreamTls,
    ValueMatch, Wildcard, compile,
};
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::networking::v1::{
    Ingress, IngressBackend, IngressClass, IngressServiceBackend,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::fmt;

pub mod gateway;
pub mod http_route;

pub use gateway::{
    Allowed, GatewayOutcome, ListenerOutcome, ListenerProblem, Namespaces, Requirement, Selector,
};
pub use http_route::{Attachment, NotAttached, ParentOutcome, RouteOutcome, Taken};

/// The controller an IngressClass names to be served by EdgeRush (02 §2).
pub const CONTROLLER: &str = "edgerush.dev/ingress-controller";

/// The controller a GatewayClass names to be served by EdgeRush (02 §2).
pub const GATEWAY_CONTROLLER: &str = "edgerush.dev/gateway-controller";

/// The data plane's own settings, which in a cluster are its `DataPlane` resource's
/// ([05 §1](../../../docs/05-operator.md)).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plane {
    /// The IngressClass whose Ingresses it serves.
    pub ingress_class: String,
    /// The Gateway it is, if it serves the Gateway API, written `<namespace>/<name>`
    /// ([04 §2](../../../docs/04-control-plane.md)). Only then are the Gateway API's kinds
    /// followed (04 §1).
    #[serde(default)]
    pub gateway: Option<GatewayName>,
    /// Its listeners, as its config states them. An Ingress is served on every `http` one,
    /// and the hosts its `tls` lists on every `https` one too, which names no certificate
    /// of its own: it presents the Ingresses' Secrets'.
    pub listeners: BTreeMap<String, Listener>,
    /// The cluster's DNS domain, which a Service's name ends in: `cluster.local` unless the
    /// cluster was set up otherwise.
    pub cluster_domain: String,
    /// Whether an Ingress host stays a candidate on hosts a more specific claim also covers:
    /// false is nginx's way ([04 §5](../../../docs/04-control-plane.md)).
    pub ingress_hosts_fall_through: bool,
    /// Whether a request over plain HTTP to a host an Ingress secures is redirected to
    /// HTTPS, as ingress-nginx's `ssl-redirect` has it; an Ingress may say otherwise for its
    /// own routes ([04 §5](../../../docs/04-control-plane.md)).
    pub ingress_https_redirect: bool,
    /// How long, in seconds, a client may keep that redirect: stated, as a browser may
    /// otherwise keep a permanent one for ever, and go on to a host no longer served over
    /// HTTPS ([04 §5](../../../docs/04-control-plane.md)).
    pub ingress_https_redirect_max_age: u32,
    /// The certificates, in PEM, of the authorities trusted to vouch for a backend reached
    /// over TLS: a Service port whose `appProtocol` is `kubernetes.io/wss`. None, and such
    /// a backend is answered 500.
    #[serde(default)]
    pub backend_authorities: Vec<String>,
    /// The data plane's own settings, as its config states them
    /// ([07 §1](../../../docs/07-config-and-dsl.md)), written into every config as they are.
    #[serde(default)]
    pub data_plane: DataPlane,
}

/// The Gateway a data plane is, by namespace and name: `<namespace>/<name>` as written.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub struct GatewayName {
    /// Its namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
}

impl TryFrom<String> for GatewayName {
    type Error = String;

    fn try_from(written: String) -> Result<Self, String> {
        let invalid = || format!("`{written}` is not <namespace>/<name>");
        let (namespace, name) = written.split_once('/').ok_or_else(invalid)?;
        if !is_label(namespace) || !name.split('.').all(is_label) || name.len() > 253 {
            return Err(invalid());
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        })
    }
}

impl fmt::Display for GatewayName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// Whether `label` is a DNS label, as a namespace's name is and each of a Gateway's
/// name's parts.
fn is_label(label: &str) -> bool {
    (1..=63).contains(&label.len())
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !label.starts_with('-')
        && !label.ends_with('-')
}

/// The objects translated.
#[derive(Debug, Clone, Default)]
pub struct Cluster {
    /// Every IngressClass seen.
    pub ingress_classes: Vec<IngressClass>,
    /// Every Ingress seen, whatever its class.
    pub ingresses: Vec<Ingress>,
    /// Every Service seen.
    pub services: Vec<Service>,
    /// Every Secret seen: those Ingresses' `tls` names hold their certificates.
    pub secrets: Vec<Secret>,
    /// Whether the Secrets Ingresses' `tls` names are taken as they will be in the
    /// cluster, securing their hosts as named and not looked at: `edgerush validate` unless
    /// asked to check them, as repositories seldom hold them (04 §3). The control plane
    /// always looks.
    pub secrets_assumed: bool,
    /// Every GatewayClass seen, where the data plane is a Gateway: the Gateway API's kinds
    /// are followed only then (04 §1), and those its install does not serve are none.
    pub gateway_classes: Vec<GatewayClass>,
    /// Every Gateway seen.
    pub gateways: Vec<Gateway>,
    /// Every HTTPRoute seen.
    pub http_routes: Vec<HTTPRoute>,
    /// Every ReferenceGrant seen.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Every Namespace seen, where the data plane is a Gateway: their labels are what a
    /// listener's selector of namespaces matches.
    pub namespaces: Vec<Namespace>,
    /// The namespaces the control plane watches, where it watches some and not all: no
    /// object of another is seen, so a reference into one is said as such (04 §9). None for
    /// every namespace, as `edgerush validate` reads them.
    pub watched: Option<Vec<String>>,
}

/// A data plane's config, and what could not go into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translation {
    /// The config.
    pub config: Config,
    /// What was left out, and why, object by object.
    pub report: Vec<Finding>,
    /// How many Ingresses were passed over as another data plane's: of another class, or of
    /// none.
    pub passed_over: usize,
    /// Paths of Ingresses translated whose backend is not there, answered 500 until it is.
    pub unresolved: Vec<Unresolved>,
    /// What of Ingresses translated is not served as their `tls` lists it.
    pub tls: Vec<TlsFinding>,
    /// Paths of Ingresses translated that never take a request: another rule, older, has
    /// the same path on the same host.
    pub shadowed: Vec<Shadowed>,
    /// The Ingresses a route of which is in the config, in the order they rank: those whose
    /// status carries the data plane's addresses (04 §6).
    pub served: Vec<Object>,
    /// Every Ingress of the data plane's class, served or not, in the order they rank:
    /// those whose status is this data plane's to write.
    pub claimed: Vec<Object>,
    /// The data plane's Gateway, where it names one that is there and this controller's:
    /// what its status is to say of it.
    pub gateway: Option<GatewayOutcome>,
    /// Each HTTPRoute that names that Gateway among its parents, and what became of each
    /// such parent: what their status is to say.
    pub http_routes: Vec<RouteOutcome>,
    /// The hosts a claim of one API takes from a claim of the other, which no longer serves
    /// them: said, so that a move from one API to the other meets no surprise (04).
    pub taken: Vec<Taken>,
}

impl Problem {
    /// Whether this is a note on what is served as the spec has it, not a fault: said, but
    /// no reason for `validate` to fail (04 §3). A retry's attempts taken as the data
    /// plane's most are one, and a host an Ingress serves over plain HTTP alone served over
    /// HTTPS by a Gateway listener another.
    #[must_use]
    pub fn is_note(&self) -> bool {
        matches!(self, Self::RetryCapped { .. } | Self::HostOverTls { .. })
    }
}

/// A path whose backend is not there: its Service does not exist, has no such port, or the
/// port's `appProtocol` is one the gateway cannot speak. The path is translated all the
/// same, with an invalid backend in its place, and is answered 500 until the Service is
/// there (04 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    /// The Ingress whose path it is.
    pub object: Object,
    /// The host, as the Ingress names it (`*` for every host).
    pub host: String,
    /// The path and how it is matched, `/api (Prefix)`; none for the Ingress's default
    /// backend.
    pub path: Option<String>,
    /// What is not there: `Service shop/web has no port 81`.
    pub reason: String,
}

impl fmt::Display for Unresolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (object, host, reason) = (&self.object, &self.host, &self.reason);
        match &self.path {
            Some(path) => write!(
                f,
                "{object}: path {path} on {host} is answered 500: {reason}"
            ),
            None => write!(
                f,
                "{object}: the default backend on {host} is answered 500: {reason}"
            ),
        }
    }
}

/// Something of an Ingress's `tls` that is not served as it lists it (04 §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFinding {
    /// The Ingress.
    pub object: Object,
    /// What.
    pub problem: TlsProblem,
}

/// What of an Ingress's `tls` is not served as it lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsProblem {
    /// An entry without hosts, ignored: a certificate is put on no host its Ingress did not
    /// name, though the certificate may carry it.
    WithoutHosts,
    /// A host a later entry lists again, with another Secret or none: ignored there.
    ListedAgain {
        /// The host.
        host: String,
    },
    /// A host its own Secret does not secure: it is not there, names no certificate that
    /// can be used, does not cover the host, or there is no `https` listener.
    Unsecured {
        /// The host.
        host: String,
        /// Why: `Secret shop/tls does not exist`.
        reason: String,
        /// How it is served instead.
        served: Served,
    },
    /// A host an older Ingress states another Secret for, which is the one served.
    Taken {
        /// The host.
        host: String,
        /// The Ingress's own Secret, by namespace and name.
        secret: String,
        /// The Ingress that has the host first.
        by: Object,
        /// Its Secret, by namespace and name.
        by_secret: String,
        /// Whether both carry their creation times, so that `by` is the one the cluster
        /// takes first; otherwise the two are only known to clash.
        by_age: bool,
    },
}

/// How a host whose own Secret does not secure it is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Served {
    /// Over plain HTTP alone.
    OverHttpOnly,
    /// Not at all: refused over HTTPS, and redirected there over plain HTTP, as its Ingress
    /// asked for it secured.
    Redirected,
    /// Not at all: the data plane has no `http` listener.
    Nowhere,
    /// Over HTTPS too, with another Ingress's Secret, which secures the same host.
    With {
        /// That Ingress.
        by: Object,
        /// Its Secret, by namespace and name.
        secret: String,
    },
}

impl fmt::Display for TlsFinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = &self.object;
        match &self.problem {
            TlsProblem::WithoutHosts => {
                write!(f, "{object}: a TLS entry without hosts is ignored")
            }
            TlsProblem::ListedAgain { host } => write!(
                f,
                "{object}: {host} is listed again in a later TLS entry, which is ignored for it"
            ),
            TlsProblem::Unsecured {
                host,
                reason,
                served,
            } => match served {
                Served::OverHttpOnly => {
                    write!(f, "{object}: {host} is served over HTTP only: {reason}")
                }
                Served::Redirected => write!(
                    f,
                    "{object}: {host} is refused over HTTPS and redirected there over plain \
                     HTTP: {reason}"
                ),
                Served::Nowhere => write!(f, "{object}: {host} is not served: {reason}"),
                Served::With { by, secret } => write!(
                    f,
                    "{object}: {host} is served over HTTPS with {by}'s Secret {secret}: {reason}"
                ),
            },
            TlsProblem::Taken {
                host,
                secret,
                by,
                by_secret,
                by_age,
            } => {
                if *by_age {
                    write!(
                        f,
                        "{object}: {host} is served with {by}'s Secret {by_secret}, which comes \
                         first, not Secret {secret}"
                    )
                } else {
                    write!(
                        f,
                        "{object}: {host} is also {by}'s, with Secret {by_secret}; which Secret \
                         is served depends on which the cluster made first"
                    )
                }
            }
        }
    }
}

/// A match that never takes a request: an older Ingress or HTTPRoute, or an earlier rule of
/// the same one, has it on the same listener and host, matched the same way, and the router
/// ranks the older first, whichever API each is of (04 §1, 07 §1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shadowed {
    /// The Ingress or HTTPRoute whose match it is.
    pub object: Object,
    /// The host, as the object names it (`*` for every host).
    pub host: String,
    /// The path and how it is matched, and what else the match asks: `/api (Prefix)`,
    /// `/api (Exact) for POST with header x-beta: on`.
    pub path: String,
    /// The object that has it first: older, or as old and first by namespace and name.
    pub by: Object,
    /// Whether both carry their creation times, so that `by` is the one the cluster takes
    /// first. Manifests written by hand do not, and are ranked by name, which may not be
    /// the cluster's order: then the two are only known to clash.
    pub by_age: bool,
}

impl fmt::Display for Shadowed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (object, host, path) = (&self.object, &self.host, &self.path);
        if self.by == self.object {
            write!(
                f,
                "{object}: path {path} on {host} is already an earlier rule's of its own"
            )
        } else if self.by_age {
            let by = &self.by;
            write!(
                f,
                "{object}: path {path} on {host} is already {by}'s, which comes first"
            )
        } else {
            let by = &self.by;
            write!(
                f,
                "{object}: path {path} on {host} is also {by}'s; which takes it depends on \
                 which the cluster made first"
            )
        }
    }
}

/// Something about one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// Its kind, namespace and name.
    pub object: Object,
    /// What about it.
    pub problem: Problem,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.object, self.problem)
    }
}

/// A Kubernetes object, by kind, namespace and name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Object {
    /// Its kind.
    pub kind: &'static str,
    /// Its namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
}

impl Object {
    fn of(kind: &'static str, metadata: &ObjectMeta) -> Self {
        Self {
            kind,
            namespace: namespace(metadata).to_owned(),
            name: metadata.name.clone().unwrap_or_default(),
        }
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // An object of no namespace, such as an IngressClass, is the cluster's.
        if self.namespace.is_empty() {
            write!(f, "{} {}", self.kind, self.name)
        } else {
            write!(f, "{} {}/{}", self.kind, self.namespace, self.name)
        }
    }
}

/// Why an object was left out.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// It asks for something translation does not do yet.
    #[error("not supported yet: {0}")]
    NotSupportedYet(&'static str),
    /// It is not what Kubernetes allows, which its API server would have refused.
    #[error("invalid: {0}")]
    Invalid(&'static str),
    /// It asks for what is never served, for what it could be made to do.
    #[error("never served: {0}")]
    NeverServed(&'static str),
    /// An annotation that takes `"true"` or `"false"` has another value.
    #[error("annotation `{0}` is neither \"true\" nor \"false\"")]
    NotBoolean(&'static str),
    /// Translated, it is what the data plane would refuse: its problems, as compiling it
    /// says them.
    #[error("the data plane would refuse it: {0}")]
    Refused(String),
    /// The data plane has no listener to serve it on.
    #[error("the data plane has no `http` listener to serve it on")]
    NoListener,
    /// The data plane's IngressClass is not there, so no Ingress is its.
    #[error("not among the objects, so no Ingress of the class is translated")]
    ClassMissing,
    /// The data plane's IngressClass is another controller's, so no Ingress is its.
    #[error(
        "names controller `{0}`, not `edgerush.dev/ingress-controller`, so no Ingress of the \
         class is translated"
    )]
    ClassNotOurs(String),
    /// The data plane's Gateway is not there.
    #[error("not among the objects, so nothing of the Gateway API is translated")]
    GatewayMissing,
    /// The data plane's Gateway's GatewayClass is not there.
    #[error("not among the objects, so its Gateway is not translated")]
    GatewayClassMissing,
    /// The data plane's Gateway's GatewayClass is another controller's.
    #[error(
        "names controller `{0}`, not `edgerush.dev/gateway-controller`, so its Gateway is not \
         translated"
    )]
    GatewayClassNotOurs(String),
    /// A listener of the data plane's Gateway is not accepted, or is and serves nothing.
    #[error("listener {name}: {}: {problem}", problem.verdict())]
    Listener {
        /// The listener's name.
        name: String,
        /// Why.
        problem: ListenerProblem,
    },
    /// No listener of the data plane's Gateway is served.
    #[error("none of its listeners is served, so it serves nothing")]
    NoListenerServed,
    /// A `parentRef` of an HTTPRoute to the data plane's Gateway attaches it to no listener.
    #[error("not attached to the data plane's Gateway: {0}")]
    NotAttached(NotAttached),
    /// A rule of an HTTPRoute has what translation cannot take: it answers 500 for every
    /// request it matches, the rest of the route served (`PartiallyInvalid`).
    #[error("rule {rule} answers 500 for all it matches: {why}")]
    RuleAnswered {
        /// Its position among the route's rules.
        rule: usize,
        /// Why.
        why: String,
    },
    /// A rule of an HTTPRoute whose matches cannot be built is left out, its requests left
    /// to the route's other rules, the rest of the route served (`PartiallyInvalid`).
    #[error("rule {rule} left out: {why}")]
    RuleLeftOut {
        /// Its position among the route's rules.
        rule: usize,
        /// Why.
        why: String,
    },
    /// No rule of an HTTPRoute is served as written: it is not accepted, though those of its
    /// rules that answer 500 still answer.
    #[error("no rule is served as written, so it is not accepted")]
    NoRuleServed,
    /// A backend of an HTTPRoute's rule is invalid: its share of the rule's requests is
    /// answered 500 (`ResolvedRefs` false).
    #[error("rule {rule}: a backend's share is answered 500: {why}")]
    BackendInvalid {
        /// Its rule's position among the route's rules.
        rule: usize,
        /// Why.
        why: String,
    },
    /// A mirror of an HTTPRoute's rule is left off, its backend not there or not permitted,
    /// and the rule served without it, as the spec has it (`ResolvedRefs` false).
    #[error("rule {rule}: a mirror is left off, the rule served without it: {why}")]
    MirrorLeftOff {
        /// Its rule's position among the route's rules.
        rule: usize,
        /// Why.
        why: String,
    },
    /// A retry of an HTTPRoute's rule asks for more attempts than the data plane makes: it
    /// makes its most (04 §2).
    #[error(
        "rule {rule}: a retry of {asked} attempts is taken as {}, the most the data plane makes",
        edgerush_config::MOST_ATTEMPTS
    )]
    RetryCapped {
        /// Its rule's position among the route's rules.
        rule: usize,
        /// The attempts asked for.
        asked: i64,
    },
    /// A `certificateRef` of an HTTPS listener of the data plane's Gateway cannot be used
    /// (`ResolvedRefs` false): the listener is served with the others, or not at all where
    /// none is left.
    #[error("listener {name}: a certificate cannot be used: {why}")]
    ListenerCertificate {
        /// The listener's name.
        name: String,
        /// Why.
        why: String,
    },
    /// A host an Ingress serves over plain HTTP alone is a Gateway HTTPS listener's
    /// hostname: served over HTTPS by that listener's certificate, no longer refused over TLS
    /// (04 §2). A note: a move from Ingress to Gateway API, as it is meant to be made.
    #[error(
        "listener {name}: {host}, which an Ingress serves over plain HTTP alone, is served over HTTPS by it"
    )]
    HostOverTls {
        /// The listener's name.
        name: String,
        /// The host.
        host: String,
    },
    /// A listener of the data plane's Gateway names route kinds it cannot take
    /// (`InvalidRouteKinds`); it takes the others.
    #[error("listener {name}: takes no route of kind {kinds}")]
    ListenerKinds {
        /// The listener's name.
        name: String,
        /// The kinds, `group/kind`.
        kinds: String,
    },
    /// A listener of the data plane's Gateway says which namespaces it takes routes from in
    /// a way translation cannot read: it takes none.
    #[error("listener {name}: takes routes from no namespace, as {why}")]
    ListenerNamespaces {
        /// The listener's name.
        name: String,
        /// Why.
        why: String,
    },
}

/// Translates `cluster`'s objects into the config of the data plane `plane` describes.
#[must_use]
pub fn translate(plane: &Plane, cluster: &Cluster) -> Translation {
    let mut report = Vec::new();
    let gateway = gateway::outcome(plane, cluster, &mut report);
    let http_routes = gateway
        .as_ref()
        .map(|gateway| http_route::attached(cluster, gateway))
        .unwrap_or_default();
    for outcome in &http_routes {
        for parent in &outcome.parents {
            if let Err(why) = parent.attached {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::NotAttached(why),
                });
            }
        }
    }
    // An Ingress is the data plane's when its class is, and its class is when the
    // IngressClass of that name names this controller; one of no class is when that class
    // is the cluster's default.
    let claim = class_claim(plane, cluster, &mut report);
    let ours = |ingress: &&Ingress| match (claim, class_of(ingress)) {
        (None, _) => false,
        (Some(_), Some(class)) => class == plane.ingress_class,
        (Some(default), None) => default,
    };
    // Oldest first, then by namespace and name, as Gateway API ranks routes: the order is
    // precedence's last tie-breaker (07 §1).
    let (mut ingresses, others): (Vec<&Ingress>, Vec<&Ingress>) =
        cluster.ingresses.iter().partition(ours);
    ingresses.sort_by(|a, b| {
        let key = |ingress: &Ingress| {
            let metadata = &ingress.metadata;
            (
                metadata.creation_timestamp.clone(),
                namespace(metadata).to_owned(),
                metadata.name.clone(),
            )
        };
        key(a).cmp(&key(b))
    });
    // Each Ingress is compiled alone, with the listeners it is served on, so that one the
    // data plane would refuse is left out by itself rather than refusing every other's
    // (04 §2). Unless those listeners are refused with no routes at all: then the fault is
    // the data plane's settings, not any Ingress's, and the whole config says so.
    let served_on: BTreeMap<String, Listener> = plane
        .listeners
        .iter()
        .filter(|(_, listener)| matches!(listener.protocol, Protocol::Http | Protocol::Https))
        .map(|(name, listener)| (name.clone(), listener.clone()))
        .collect();
    let checked_alone = compile(&config_of(
        served_on.clone(),
        Vec::new(),
        BTreeMap::new(),
        &[],
        &[],
    ))
    .is_ok();
    // The certificates every `https` listener presents, each for its hosts alone, in the
    // order of the Ingresses that use them, and the Ingress and Secret each host is stated
    // for first: a host two Secrets are stated for is the older Ingress's, as its paths are.
    let mut presented: Vec<(String, Certificate, Vec<String>)> = Vec::new();
    let mut stated: BTreeMap<String, (Object, bool, String)> = BTreeMap::new();
    let mut accepted = Vec::new();
    let of_the_class: Vec<Object> = ingresses
        .iter()
        .map(|ingress| Object::of("Ingress", &ingress.metadata))
        .collect();
    for ingress in ingresses {
        let object = || Object::of("Ingress", &ingress.metadata);
        let dated = ingress.metadata.creation_timestamp.is_some();
        let translated = match ingress_routes(plane, cluster, ingress) {
            Ok(translated) => translated,
            Err(problem) => {
                report.push(Finding {
                    object: object(),
                    problem,
                });
                continue;
            }
        };
        if checked_alone {
            // Checked on every listener it could come to be served on: whether another
            // Ingress secures its hosts is not known yet.
            let mut routes = translated.routes.clone();
            for route in &mut routes {
                route.listeners = served_on.keys().cloned().collect();
            }
            let alone = config_of(
                served_on.clone(),
                routes,
                translated.upstreams.clone(),
                &translated.certificates,
                &[],
            );
            if let Err(problems) = compile(&alone) {
                let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
                report.push(Finding {
                    object: object(),
                    problem: Problem::Refused(problems.join("; ")),
                });
                continue;
            }
        }
        let mut taken = Vec::new();
        for (name, certificate, hosts) in &translated.certificates {
            for host in hosts {
                let first = stated
                    .entry(host.clone())
                    .or_insert_with(|| (object(), dated, name.clone()));
                if first.2 != *name {
                    taken.push(TlsProblem::Taken {
                        host: host.clone(),
                        secret: name.clone(),
                        by: first.0.clone(),
                        by_secret: first.2.clone(),
                        by_age: dated && first.1,
                    });
                    continue;
                }
                match presented.iter_mut().find(|(named, ..)| named == name) {
                    Some((.., named_hosts)) if !named_hosts.contains(host) => {
                        named_hosts.push(host.clone());
                    }
                    Some(_) => {}
                    None => presented.push((name.clone(), certificate.clone(), vec![host.clone()])),
                }
            }
        }
        accepted.push((object(), rank(&ingress.metadata, 0), translated, taken));
    }
    // Every Ingress in, a host some Ingress secures is served over HTTPS whichever Ingress's
    // route it is, so that both kinds of listener rank the same paths for it; a host none
    // secures is refused over HTTPS by name, so that no wildcard's route takes it there.
    // Over plain HTTP, a route on a secured host redirects to HTTPS unless its Ingress says
    // not, as ingress-nginx redirects a path of a host it has a certificate for (04 §5); so
    // does one on a host an Ingress lists under `tls` but cannot secure, refused over HTTPS
    // all the same, as it was asked for secured and is never served unencrypted. A host
    // served on no listener at all is said as such, and nothing else of it.
    let unsecurable: BTreeSet<String> = accepted
        .iter()
        .flat_map(|(_, _, translated, _)| translated.unsecurable.iter().cloned())
        .collect();
    let https: Vec<String> = plane
        .listeners
        .iter()
        .filter(|(_, listener)| listener.protocol == Protocol::Https)
        .map(|(name, _)| name.clone())
        .collect();
    // The port a redirect to HTTPS states: none where an `https` listener binds 443, the
    // default, and else the lowest such listener's, as pods bind ports as written and so
    // are reached on them (05 §2, Lucas).
    let https_ports = plane
        .listeners
        .values()
        .filter(|listener| listener.protocol == Protocol::Https)
        .map(|listener| listener.address.port());
    let https_port = if https_ports.clone().any(|port| port == 443) {
        None
    } else {
        https_ports.min()
    };
    // Each object's config routes, with its rank: those of every Ingress and HTTPRoute are
    // put in the config in one order, oldest first, whichever API each is of (04 §1).
    let mut ranked: Vec<Ranked> = Vec::new();
    let mut upstreams = BTreeMap::new();
    let mut unresolved = Vec::new();
    let mut tls = Vec::new();
    let mut refused: Vec<String> = Vec::new();
    let mut served_ingresses = Vec::new();
    for (object, rank, translated, taken) in accepted {
        let mut routes = Vec::new();
        let mut served = Vec::new();
        let mut redirected = Vec::new();
        for mut route in translated.routes {
            let host = route
                .hostnames
                .first()
                .map(|hostname| hostname.name.clone());
            let host = host.unwrap_or_default();
            let secured = stated.contains_key(&host);
            let mut redirect = None;
            if (secured || unsecurable.contains(&host)) && translated.https_redirect {
                let (http, own): (Vec<String>, Vec<String>) = route
                    .listeners
                    .drain(..)
                    .partition(|listener| !https.contains(listener));
                route.listeners = own;
                if !http.is_empty() {
                    redirect = Some(https_redirect(
                        &route,
                        http,
                        https_port,
                        plane.ingress_https_redirect_max_age,
                    ));
                }
            }
            if secured {
                for listener in &https {
                    if !route.listeners.contains(listener) {
                        route.listeners.push(listener.clone());
                    }
                }
            } else {
                if redirect.is_some() {
                    redirected.push(host.clone());
                }
                if !host.starts_with('*') && !refused.contains(&host) {
                    refused.push(host.clone());
                }
            }
            if route.listeners.is_empty() && redirect.is_none() {
                continue;
            }
            served.push(host);
            // A route whose host cannot be secured has nothing left but its redirect.
            if !route.listeners.is_empty() {
                routes.push(route);
            }
            // In the route's place on the `http` listeners, so that they rank its paths as
            // they would its own; its claims are the route's, said once.
            routes.extend(redirect);
        }
        tls.extend(
            translated
                .notes
                .into_iter()
                .chain(translated.unsecured.into_iter().map(|(host, reason)| {
                    let served = match stated.get(&host) {
                        Some((by, _, secret)) => Served::With {
                            by: by.clone(),
                            secret: secret.clone(),
                        },
                        None if redirected.contains(&host) => Served::Redirected,
                        None if served.contains(&host) => Served::OverHttpOnly,
                        None => Served::Nowhere,
                    };
                    TlsProblem::Unsecured {
                        host,
                        reason,
                        served,
                    }
                }))
                .chain(taken)
                .map(|problem| TlsFinding {
                    object: object.clone(),
                    problem,
                }),
        );
        unresolved.extend(
            translated
                .missing
                .into_iter()
                .filter(|(host, ..)| served.contains(host))
                .map(|(host, path, reason)| Unresolved {
                    object: object.clone(),
                    host,
                    path,
                    reason,
                }),
        );
        upstreams.extend(translated.upstreams);
        if !served.is_empty() {
            served_ingresses.push(object.clone());
        }
        ranked.push(Ranked {
            rank,
            object,
            routes,
        });
    }
    // Each attached HTTPRoute's rules, each compiled alone, with the listeners it could be
    // served on, so that one the data plane would refuse is dropped by itself (04 §2).
    if let Some(gateway) = &gateway {
        let refused = |rule: &Rule, upstreams: &BTreeMap<String, Upstream>| {
            if !checked_alone {
                return None;
            }
            let alone = Route {
                name: "httproute".to_owned(),
                listeners: served_on.keys().cloned().collect(),
                hostnames: vec![http_route::claim("*")],
                rules: vec![rule.clone()],
                rate_limits: Vec::new(),
            };
            compile(&config_of(
                served_on.clone(),
                vec![alone],
                upstreams.clone(),
                &[],
                &[],
            ))
            .err()
            .map(|problems| {
                let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
                // On one line, as a report's finding is: a regex's error runs over several.
                let said: Vec<&str> = problems.iter().flat_map(|p| p.split_whitespace()).collect();
                format!("the data plane would refuse it: {}", said.join(" "))
            })
        };
        for outcome in &http_routes {
            // A route attached to no listener has nothing translated, its parents said.
            if !outcome.parents.iter().any(|parent| parent.attached.is_ok()) {
                continue;
            }
            let Some(route) = cluster
                .http_routes
                .iter()
                .find(|route| Object::of("HTTPRoute", &route.metadata) == outcome.route)
            else {
                continue;
            };
            let translated =
                http_route::translated(route, outcome, gateway, plane, cluster, refused);
            for (rule, why) in &translated.answered {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::RuleAnswered {
                        rule: *rule,
                        why: why.clone(),
                    },
                });
            }
            for (rule, why) in &translated.left_out {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::RuleLeftOut {
                        rule: *rule,
                        why: why.clone(),
                    },
                });
            }
            for (rule, why) in &translated.invalid {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::BackendInvalid {
                        rule: *rule,
                        why: why.clone(),
                    },
                });
            }
            for (rule, asked) in &translated.retries_capped {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::RetryCapped {
                        rule: *rule,
                        asked: *asked,
                    },
                });
            }
            for (rule, why) in &translated.mirrors_left_off {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::MirrorLeftOff {
                        rule: *rule,
                        why: why.clone(),
                    },
                });
            }
            if translated.answered.len() + translated.left_out.len() == route.spec.rules.len() {
                report.push(Finding {
                    object: outcome.route.clone(),
                    problem: Problem::NoRuleServed,
                });
            }
            ranked.push(Ranked {
                rank: rank(&route.metadata, 1),
                object: outcome.route.clone(),
                routes: translated.routes,
            });
            upstreams.extend(translated.upstreams);
        }
    }
    ranked.sort_by(|a, b| a.rank.cmp(&b.rank));
    let (mut routes, shadowed) = in_order(ranked);
    // Each listener hostname of the Gateway is claimed, so that nothing less specific serves
    // it (04 §2, listener isolation).
    if let Some(gateway) = &gateway {
        routes.extend(http_route::claims(gateway));
    }
    let taken = http_route::taken(&routes);
    // An upstream only routes served on no listener led to, or copied to, is left out with
    // them.
    let used: Vec<String> = routes
        .iter()
        .flat_map(|route| &route.rules)
        .flat_map(|rule| {
            let backends = rule
                .forward
                .iter()
                .flat_map(|forward| &forward.backends)
                .filter_map(|backend| backend.upstream.clone());
            let mirrors = rule.filters.iter().filter_map(|filter| match filter {
                Filter::RequestMirror(mirror) => Some(mirror.upstream.clone()),
                _ => None,
            });
            backends.chain(mirrors)
        })
        .collect();
    upstreams.retain(|name, _| used.contains(name));
    let mut config = Config {
        data_plane: plane.data_plane.clone(),
        ..config_of(
            plane.listeners.clone(),
            routes,
            upstreams,
            &presented,
            &refused,
        )
    };
    if let Some(gateway) = &gateway {
        gateway_tls(&mut config, gateway, cluster, &mut report);
    }
    Translation {
        config,
        report,
        passed_over: others.len(),
        unresolved,
        tls,
        shadowed,
        served: served_ingresses,
        claimed: of_the_class,
        gateway,
        http_routes,
        taken,
    }
}

/// Where an object's config routes go among all others: oldest first, then by namespace and
/// name, as Gateway API ranks routes, whichever API each is of (04 §1); an Ingress and an
/// HTTPRoute alike in all three, the Ingress first, as the one more likely there before.
/// The order is precedence's last tie-breaker (07 §1).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    created: Option<Time>,
    namespace: String,
    name: String,
    /// 0 for an Ingress, 1 for an HTTPRoute: the Ingress first.
    api: u8,
}

/// The rank of the object of `metadata`, of API `api` (0 for Ingress, 1 for HTTPRoute).
fn rank(metadata: &ObjectMeta, api: u8) -> Rank {
    Rank {
        created: metadata.creation_timestamp.clone(),
        namespace: namespace(metadata).to_owned(),
        name: metadata.name.clone().unwrap_or_default(),
        api,
    }
}

/// An object's config routes, and its rank.
struct Ranked {
    rank: Rank,
    object: Object,
    routes: Vec<Route>,
}

/// The routes of objects ranked in order, and each match among them that never takes a
/// request: one an earlier route already has on the same listener and host, the same in all
/// the router ranks by. A wildcard hostname of any labels covers one of a single label of
/// the same name, but not the other way about: Ingress's `*.example.com` and Gateway API's
/// claim different hosts.
fn in_order(ranked: Vec<Ranked>) -> (Vec<Route>, Vec<Shadowed>) {
    // Under each key, the first object to have it with each kind of wildcard.
    let mut first: BTreeMap<String, Vec<(Option<Wildcard>, Object, bool)>> = BTreeMap::new();
    let mut shadowed = Vec::new();
    let mut routes = Vec::new();
    for Ranked {
        rank,
        object,
        routes: theirs,
    } in ranked
    {
        let dated = rank.created.is_some();
        for route in &theirs {
            for (key, hostname, path) in matched(route) {
                let earlier = first.entry(key).or_default();
                let wildcard = wildcard_of(hostname);
                match earlier
                    .iter()
                    .find(|(theirs, ..)| covers(*theirs, wildcard))
                {
                    None => earlier.push((wildcard, object.clone(), dated)),
                    Some((_, by, by_dated)) => {
                        let found = Shadowed {
                            object: object.clone(),
                            host: hostname.name.clone(),
                            path,
                            by: by.clone(),
                            by_age: dated && *by_dated,
                        };
                        // Said once, however many listeners it is shadowed on.
                        if !shadowed.contains(&found) {
                            shadowed.push(found);
                        }
                    }
                }
            }
        }
        routes.extend(theirs);
    }
    (routes, shadowed)
}

/// What the `*` of a hostname stands for, where it is a wildcard.
fn wildcard_of(hostname: &Hostname) -> Option<Wildcard> {
    hostname
        .name
        .starts_with("*.")
        .then_some(hostname.wildcard)
        .flatten()
}

/// Whether a hostname of wildcard `theirs` covers every host one of the same name and
/// wildcard `ours` does.
fn covers(theirs: Option<Wildcard>, ours: Option<Wildcard>) -> bool {
    theirs == ours || (theirs == Some(Wildcard::AnyLabels) && ours == Some(Wildcard::OneLabel))
}

/// Each match of a route on each of its listeners and hostnames: a key that is the same for
/// two matches alike in all the router ranks them by but their hostname's wildcard, the
/// hostname, and the match as the report says it.
fn matched(route: &Route) -> impl Iterator<Item = (String, &Hostname, String)> + '_ {
    route.listeners.iter().flat_map(move |listener| {
        route.hostnames.iter().flat_map(move |hostname| {
            route
                .rules
                .iter()
                .flat_map(|rule| &rule.matches)
                .filter_map(move |matched| {
                    let said = described(matched)?;
                    Some((
                        format!("{listener} {} {said}", hostname.name),
                        hostname,
                        said,
                    ))
                })
        })
    })
}

/// A match as the report says it: its path and how it is matched, `/api (Prefix)`, then its
/// method, headers and query parameters, these in order of name, as the router takes them
/// in any order. None for a match of no path.
fn described(matched: &Match) -> Option<String> {
    let mut said = path_said(matched.path.as_ref()?);
    if let Some(method) = &matched.method {
        said.push_str(" for ");
        said.push_str(method);
    }
    // Header names are matched without regard to case.
    let mut headers: Vec<String> = matched
        .headers
        .iter()
        .map(|header| {
            format!(
                "header {}",
                value_said(&header.name.to_ascii_lowercase(), &header.value)
            )
        })
        .collect();
    headers.sort();
    let mut query: Vec<String> = matched
        .query
        .iter()
        .map(|parameter| {
            format!(
                "query parameter {}",
                value_said(&parameter.name, &parameter.value)
            )
        })
        .collect();
    query.sort();
    headers.extend(query);
    if !headers.is_empty() {
        said.push_str(" with ");
        said.push_str(&headers.join(", "));
    }
    Some(said)
}

/// A path and how it is matched, as the report says it: `/api (Prefix)`.
fn path_said(path: &PathMatch) -> String {
    match path {
        PathMatch::Prefix(path) => format!("{path} (Prefix)"),
        PathMatch::Exact(path) => format!("{path} (Exact)"),
        PathMatch::Regex(path) => format!("{path} (regex)"),
    }
}

/// A header's or query parameter's match as the report says it: `x-beta: on`,
/// `tenant matching [a-z]+`.
fn value_said(name: &str, value: &ValueMatch) -> String {
    match value {
        ValueMatch::Exact(value) => format!("{name}: {value}"),
        ValueMatch::Regex(value) => format!("{name} matching {value}"),
    }
}

/// The certificates of the data plane's Gateway's HTTPS listeners, on the data plane's
/// listeners that serve each (04 §2): each stated for its listener's hostname, or, where it
/// has none, for every name no other answers for and for a client that asks for none, the
/// spec's "longest matching SNI"; before an Ingress's on the same listener, the Gateway's
/// listener owning its hostname, and a Secret both name one certificate for both's names.
/// A listener none of whose certificates can be used has its hostname refused, so that no
/// other certificate answers for it; a wildcard cannot be. One that can has its hostname
/// taken off the names refused, where an Ingress serving it over plain HTTP alone put it,
/// and that is said in `report` as a note.
fn gateway_tls(
    config: &mut Config,
    gateway: &GatewayOutcome,
    cluster: &Cluster,
    report: &mut Vec<Finding>,
) {
    // By the data plane's listener: each certificate and the names stated for it.
    let mut gathered: BTreeMap<String, Vec<(String, Vec<String>)>> = BTreeMap::new();
    let mut without_sni: BTreeMap<String, String> = BTreeMap::new();
    for listener in &gateway.listeners {
        let stated = listener.hostname.clone().unwrap_or_else(|| "*".to_owned());
        let serving = match &listener.served {
            Ok(serving) if !listener.certificates.is_empty() => serving,
            Err(gateway::ListenerProblem::NoCertificate) => {
                if !stated.contains('*') {
                    for tls in https_on(config, listener.port) {
                        if !tls.refused.contains(&stated) {
                            tls.refused.push(stated.clone());
                        }
                    }
                }
                continue;
            }
            _ => continue,
        };
        for name in &listener.certificates {
            let certificate = name.split_once('/').and_then(|(namespace, secret)| {
                if cluster.secrets_assumed {
                    // Named, and not looked at: what it holds is the cluster's.
                    return Some(Certificate {
                        chain: String::new(),
                        key: String::new(),
                    });
                }
                secret_certificate(cluster, namespace, secret)
                    .ok()
                    .map(|(certificate, _)| certificate)
            });
            // Read once already, when the listener was fitted.
            let Some(certificate) = certificate else {
                continue;
            };
            config.certificates.insert(name.clone(), certificate);
        }
        let mut taken_off = false;
        for listening in serving {
            if let Some(tls) = config
                .listeners
                .get_mut(listening)
                .and_then(|listener| listener.tls.as_mut())
            {
                let before = tls.refused.len();
                tls.refused.retain(|refused| *refused != stated);
                taken_off |= tls.refused.len() != before;
            }
            let entries = gathered.entry(listening.clone()).or_default();
            for name in &listener.certificates {
                match entries.iter_mut().find(|(named, _)| named == name) {
                    Some((_, names)) if !names.contains(&stated) => names.push(stated.clone()),
                    Some(_) => {}
                    None => entries.push((name.clone(), vec![stated.clone()])),
                }
            }
            if listener.hostname.is_none()
                && let Some(first) = listener.certificates.first()
            {
                without_sni
                    .entry(listening.clone())
                    .or_insert_with(|| first.clone());
            }
        }
        if taken_off {
            report.push(Finding {
                object: gateway.gateway.clone(),
                problem: Problem::HostOverTls {
                    name: listener.name.clone(),
                    host: stated.clone(),
                },
            });
        }
    }
    // Each data plane listener's, in the Gateway's order, before the Ingresses'.
    for (listening, entries) in gathered {
        let Some(tls) = config
            .listeners
            .get_mut(&listening)
            .and_then(|listener| listener.tls.as_mut())
        else {
            continue;
        };
        let mut ours = Vec::with_capacity(entries.len() + tls.certificates.len());
        for (name, mut names) in entries {
            // A Secret an Ingress names too is one certificate, for both's names.
            if let Some(at) = tls
                .certificates
                .iter()
                .position(|presented| presented.name() == name)
            {
                let theirs = tls.certificates.remove(at);
                names.extend(theirs.stated().into_iter().flatten().cloned());
            }
            ours.push(ListenerCertificate::Stated(StatedNames {
                certificate: name,
                names,
            }));
        }
        ours.append(&mut tls.certificates);
        tls.certificates = ours;
        if let Some(name) = without_sni.remove(&listening) {
            tls.without_sni = name;
        }
    }
}

/// The TLS of the data plane's `https` listeners bound to `port`.
fn https_on(config: &mut Config, port: u16) -> impl Iterator<Item = &mut edgerush_config::Tls> {
    config
        .listeners
        .values_mut()
        .filter(move |listener| {
            listener.protocol == Protocol::Https && listener.address.port() == port
        })
        .filter_map(|listener| listener.tls.as_mut())
}

/// A config of these listeners, routes and upstreams, and nothing else but `presented`:
/// certificates each `https` listener presents for the hosts stated with them, after any
/// it names itself; and the host names it refuses.
fn config_of(
    mut listeners: BTreeMap<String, Listener>,
    routes: Vec<Route>,
    upstreams: BTreeMap<String, Upstream>,
    presented: &[(String, Certificate, Vec<String>)],
    refused: &[String],
) -> Config {
    for listener in listeners.values_mut() {
        let Some(tls) = listener.tls.as_mut() else {
            continue;
        };
        if listener.protocol == Protocol::Https {
            tls.refused.extend(refused.iter().cloned());
            tls.certificates
                .extend(presented.iter().map(|(name, _, hosts)| {
                    ListenerCertificate::Stated(StatedNames {
                        certificate: name.clone(),
                        names: hosts.clone(),
                    })
                }));
        }
    }
    Config {
        listeners,
        routes,
        tcp_routes: Vec::new(),
        tls_routes: Vec::new(),
        upstreams,
        data_plane: DataPlane::default(),
        rate_limiting: None,
        certificates: presented
            .iter()
            .map(|(name, certificate, _)| (name.clone(), certificate.clone()))
            .collect(),
        limits: None,
    }
}

/// An object without a namespace is in `default`, as Kubernetes takes it.
fn namespace(metadata: &ObjectMeta) -> &str {
    metadata.namespace.as_deref().unwrap_or("default")
}

/// Whether the data plane's class is this controller's, and if so whether it is the
/// cluster's default; why not, in the report.
fn class_claim(plane: &Plane, cluster: &Cluster, report: &mut Vec<Finding>) -> Option<bool> {
    let object = Object {
        kind: "IngressClass",
        namespace: String::new(),
        name: plane.ingress_class.clone(),
    };
    let Some(class) = cluster
        .ingress_classes
        .iter()
        .find(|class| class.metadata.name.as_deref() == Some(plane.ingress_class.as_str()))
    else {
        report.push(Finding {
            object,
            problem: Problem::ClassMissing,
        });
        return None;
    };
    let controller = class
        .spec
        .as_ref()
        .and_then(|spec| spec.controller.as_deref())
        .unwrap_or_default();
    if controller != CONTROLLER {
        report.push(Finding {
            object,
            problem: Problem::ClassNotOurs(controller.to_owned()),
        });
        return None;
    }
    let default = class
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(DEFAULT_CLASS_ANNOTATION))
        .is_some_and(|value| value == "true");
    Some(default)
}

/// The annotation an IngressClass is made the cluster's default with.
const DEFAULT_CLASS_ANNOTATION: &str = "ingressclass.kubernetes.io/is-default-class";

/// The annotation an Ingress turns the redirect from plain HTTP to HTTPS on or off with, for
/// its own routes, whatever the data plane's setting: `"true"` or `"false"`.
pub const HTTPS_REDIRECT_ANNOTATION: &str = "gateway.edgerush.dev/https-redirect";

/// The annotation an Ingress has its backends reached by their Services' own addresses
/// with, `"true"`, rather than by their endpoints: the cluster IP, which kube-proxy or a
/// mesh then chooses a pod behind ([03 §6](../../../docs/03-data-plane.md)).
pub const SERVICE_UPSTREAM_ANNOTATION: &str = "gateway.edgerush.dev/service-upstream";

/// The annotation that named an Ingress's class before `ingressClassName`, and still does
/// where that is left out.
const CLASS_ANNOTATION: &str = "kubernetes.io/ingress.class";

/// An Ingress's class: its `ingressClassName`, or, without one, the annotation.
fn class_of(ingress: &Ingress) -> Option<&str> {
    let named = ingress
        .spec
        .as_ref()
        .and_then(|spec| spec.ingress_class_name.as_deref());
    named.or_else(|| {
        ingress
            .metadata
            .annotations
            .as_ref()?
            .get(CLASS_ANNOTATION)
            .map(String::as_str)
    })
}

/// An Ingress translated.
#[derive(Default)]
struct IngressRoutes {
    /// Its routes, one a host, each on the listeners its own `tls` gives it: none at all for
    /// a host it would have only over plain HTTP where the data plane has no `http` listener.
    routes: Vec<Route>,
    /// The upstreams they lead to.
    upstreams: BTreeMap<String, Upstream>,
    /// Each path whose backend is not there: its host, the path described (none for the
    /// default backend), and why.
    missing: Vec<(String, Option<String>, String)>,
    /// The certificates its secured hosts are served with: each Secret's, by its config
    /// name, with the hosts it answers for.
    certificates: Vec<(String, Certificate, Vec<String>)>,
    /// What of its `tls` entries is ignored.
    notes: Vec<TlsProblem>,
    /// Each host `tls` lists that its own Secret does not secure, and why.
    unsecured: Vec<(String, String)>,
    /// Of those, the hosts asked for over HTTPS where the data plane serves it, which are
    /// redirected there all the same, never served unencrypted (04 §5).
    unsecurable: Vec<String>,
    /// Whether its routes on a host served over HTTPS redirect there over plain HTTP.
    https_redirect: bool,
}

/// An Ingress's routes, the upstreams they lead to, the certificates of its secured hosts,
/// the paths whose backend is not there and what of its `tls` is not served as listed; or
/// why none.
fn ingress_routes(
    plane: &Plane,
    cluster: &Cluster,
    ingress: &Ingress,
) -> Result<IngressRoutes, Problem> {
    // Ours by the old annotation, with no `ingressClassName`: said apart from other
    // annotations, as the class it names is the only reason it was taken for this data
    // plane. One of no class at all is the default class's, and comes on.
    let named = ingress
        .spec
        .as_ref()
        .is_some_and(|spec| spec.ingress_class_name.is_some());
    let annotated = ingress
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|annotations| annotations.contains_key(CLASS_ANNOTATION));
    if !named && annotated {
        return Err(Problem::NotSupportedYet(
            "the `kubernetes.io/ingress.class` annotation",
        ));
    }
    // Annotations change what an Ingress does (a rewrite, a redirect, a limit), so one that
    // has any is not translated until they are read; all but those read already and
    // kubectl's own record of what it applied, which says nothing of routing and is on
    // whatever `kubectl apply` made.
    let annotations = ingress.metadata.annotations.as_ref();
    if annotations
        .iter()
        .flat_map(|annotations| annotations.keys())
        .any(|name| {
            name != "kubectl.kubernetes.io/last-applied-configuration"
                && name != HTTPS_REDIRECT_ANNOTATION
                && name != SERVICE_UPSTREAM_ANNOTATION
        })
    {
        return Err(Problem::NotSupportedYet("annotations"));
    }
    let https_redirect = match annotations
        .and_then(|annotations| annotations.get(HTTPS_REDIRECT_ANNOTATION))
        .map(String::as_str)
    {
        None => plane.ingress_https_redirect,
        Some("true") => true,
        Some("false") => false,
        Some(_) => return Err(Problem::NotBoolean(HTTPS_REDIRECT_ANNOTATION)),
    };
    let service_upstream = match annotations
        .and_then(|annotations| annotations.get(SERVICE_UPSTREAM_ANNOTATION))
        .map(String::as_str)
    {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => return Err(Problem::NotBoolean(SERVICE_UPSTREAM_ANNOTATION)),
    };
    let Some(spec) = ingress.spec.as_ref().filter(|spec| {
        spec.default_backend.is_some() || spec.rules.as_ref().is_some_and(|rules| !rules.is_empty())
    }) else {
        return Err(Problem::Invalid("neither rules nor a default backend"));
    };
    // The hosts it secures, each with its Secret's name, or none where its entry names
    // none. An entry without hosts secures nothing: putting its certificate on the hosts
    // it happens to carry would serve names nobody listed (04 §5). A host listed again
    // keeps its first entry.
    let mut tls_hosts: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    let mut notes = Vec::new();
    for entry in spec.tls.iter().flatten() {
        let secret = entry.secret_name.as_deref();
        let hosts = entry.hosts.as_deref().unwrap_or_default();
        if hosts.is_empty() {
            notes.push(TlsProblem::WithoutHosts);
        }
        for host in hosts {
            match tls_hosts.entry(host.as_str()) {
                Entry::Vacant(free) => {
                    free.insert(secret);
                }
                Entry::Occupied(first) if *first.get() != secret => {
                    notes.push(TlsProblem::ListedAgain { host: host.clone() });
                }
                Entry::Occupied(_) => {}
            }
        }
    }
    let of_protocol = |protocol: Protocol| -> Vec<String> {
        plane
            .listeners
            .iter()
            .filter(|(_, listener)| listener.protocol == protocol)
            .map(|(name, _)| name.clone())
            .collect()
    };
    let (http, https) = (of_protocol(Protocol::Http), of_protocol(Protocol::Https));
    let namespace = namespace(&ingress.metadata);
    let mut by_host: BTreeMap<String, Vec<Rule>> = BTreeMap::new();
    let mut upstreams = BTreeMap::new();
    let mut missing = Vec::new();
    // A backend that is not there stays in its path, answered 500 until it is, as Gateway
    // API has a missing `backendRef` (04 §2).
    let mut backend_of = |backend: &IngressBackend, host: &str, path: Option<String>| {
        Ok(
            match backend_upstream(plane, cluster, namespace, backend, service_upstream)? {
                Ok((name, upstream)) => {
                    upstreams.insert(name.clone(), upstream);
                    Backend {
                        upstream: Some(name),
                        invalid: None,
                        weight: 1,
                    }
                }
                Err(reason) => {
                    missing.push((host.to_owned(), path, reason.clone()));
                    Backend {
                        upstream: None,
                        invalid: Some(reason),
                        weight: 1,
                    }
                }
            },
        )
    };
    for rule in spec.rules.iter().flatten() {
        // A rule without a host is for every host: the name `*`.
        let host = rule.host.as_deref().unwrap_or("*");
        if host.parse::<std::net::IpAddr>().is_ok() {
            return Err(Problem::Invalid("a host that is an IP address"));
        }
        let Some(http) = &rule.http else {
            // A host named without paths is its default backend's alone, below.
            if spec.default_backend.is_none() {
                return Err(Problem::NotSupportedYet("a rule without paths"));
            }
            by_host.entry(host.to_owned()).or_default();
            continue;
        };
        for path in &http.paths {
            let matched = match (path.path_type.as_str(), &path.path) {
                ("Prefix", Some(prefix)) => PathMatch::Prefix(prefix.clone()),
                ("Exact", Some(exact)) => PathMatch::Exact(exact.clone()),
                // The implementation's to define, and defined as `Prefix`, by whole segments,
                // as ingress-nginx takes it; left out, it is every path (04 §5).
                ("ImplementationSpecific", prefix) => {
                    PathMatch::Prefix(prefix.clone().unwrap_or_else(|| "/".to_owned()))
                }
                ("Prefix" | "Exact", None) => {
                    return Err(Problem::Invalid("a Prefix or Exact path without a path"));
                }
                _ => return Err(Problem::Invalid("a path type Kubernetes does not have")),
            };
            let backend = backend_of(&path.backend, host, Some(path_said(&matched)))?;
            by_host
                .entry(host.to_owned())
                .or_default()
                .push(forward(matched, backend));
        }
    }
    // The default backend takes what no rule does (04 §5): without rules, every host no
    // other Ingress names, as `*`; beside them, the rest of each host they name, as a `/`
    // that ranks below their own paths. A host whose rules have their own `/` keeps it.
    if let Some(default) = &spec.default_backend {
        if by_host.is_empty() {
            by_host.insert("*".to_owned(), Vec::new());
        }
        let every_path = PathMatch::Prefix("/".to_owned());
        for (host, rules) in &mut by_host {
            let has_every_path = rules
                .iter()
                .flat_map(|rule| &rule.matches)
                .any(|matched| matched.path.as_ref() == Some(&every_path));
            if !has_every_path {
                let backend = backend_of(default, host, None)?;
                rules.push(forward(every_path.clone(), backend));
            }
        }
    }
    if by_host.is_empty() {
        return Ok(IngressRoutes::default());
    }
    // Every host is served on the `http` listeners, and one `tls` lists on the `https` ones
    // too, with its Secret's certificate, when that can be used and covers it (04 §5). One
    // that cannot be secured stays on `http`, and is said; one asked for over plain HTTP
    // where the data plane has no `http` listener leaves the Ingress out.
    let mut certificates: Vec<(String, Certificate, Vec<String>)> = Vec::new();
    let mut read: BTreeMap<&str, Result<(Certificate, Option<Identity>), String>> = BTreeMap::new();
    let mut unsecured = Vec::new();
    let mut unsecurable = Vec::new();
    let mut listeners_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for host in by_host.keys() {
        let mut listeners = http.clone();
        match tls_hosts.get(host.as_str()) {
            None if http.is_empty() => return Err(Problem::NoListener),
            None => {}
            Some(None) => {
                unsecured.push((host.clone(), "its TLS entry names no Secret".to_owned()));
                if !https.is_empty() {
                    unsecurable.push(host.clone());
                }
            }
            Some(&Some(secret)) => {
                let checked = read.entry(secret).or_insert_with(|| {
                    if cluster.secrets_assumed {
                        // Named, and not looked at: what it holds is the cluster's.
                        let unread = Certificate {
                            chain: String::new(),
                            key: String::new(),
                        };
                        Ok((unread, None))
                    } else {
                        secret_certificate(cluster, namespace, secret)
                            .map(|(certificate, identity)| (certificate, Some(identity)))
                    }
                });
                let reason = match checked {
                    Err(reason) => Some(reason.clone()),
                    Ok((_, Some(identity))) if !identity.covers(host) => {
                        Some(format!("Secret {namespace}/{secret} does not cover {host}"))
                    }
                    Ok(_) if https.is_empty() => {
                        Some("the data plane has no `https` listener".to_owned())
                    }
                    Ok((certificate, _)) => {
                        let name = format!("{namespace}/{secret}");
                        match certificates.iter_mut().find(|(named, ..)| *named == name) {
                            Some((.., hosts)) => hosts.push(host.clone()),
                            None => {
                                certificates.push((name, certificate.clone(), vec![host.clone()]))
                            }
                        }
                        listeners.extend(https.iter().cloned());
                        None
                    }
                };
                if let Some(reason) = reason {
                    unsecured.push((host.clone(), reason));
                    if !https.is_empty() {
                        unsecurable.push(host.clone());
                    }
                }
            }
        }
        listeners_of.insert(host.clone(), listeners);
    }
    let name = ingress.metadata.name.as_deref().unwrap_or_default();
    let routes: Vec<Route> = by_host
        .into_iter()
        .map(|(host, rules)| Route {
            name: format!("ingress/{namespace}/{name}/{host}"),
            listeners: listeners_of.remove(&host).unwrap_or_default(),
            hostnames: vec![Hostname {
                // An Ingress's `*` stands for exactly one label (04 §5).
                wildcard: host.starts_with("*.").then_some(Wildcard::OneLabel),
                name: host,
                falls_through: plane.ingress_hosts_fall_through,
            }],
            rules,
            rate_limits: Vec::new(),
        })
        .collect();
    Ok(IngressRoutes {
        routes,
        upstreams,
        missing,
        certificates,
        notes,
        unsecured,
        unsecurable,
        https_redirect,
    })
}

/// The certificate a Secret holds, in `tls.crt` and `tls.key` whatever its type, read and
/// checked as the data plane will read it; or why it cannot be used.
fn secret_certificate(
    cluster: &Cluster,
    namespace: &str,
    name: &str,
) -> Result<(Certificate, Identity), String> {
    let Some(secret) = cluster.secrets.iter().find(|secret| {
        secret.metadata.name.as_deref() == Some(name)
            && self::namespace(&secret.metadata) == namespace
    }) else {
        return Err(format!("Secret {namespace}/{name} does not exist"));
    };
    let field = |key: &str| {
        // `stringData` is written over `data`, as the API server does when it stores one.
        if let Some(text) = secret.string_data.as_ref().and_then(|data| data.get(key)) {
            return Ok(text.clone());
        }
        let bytes = secret
            .data
            .as_ref()
            .and_then(|data| data.get(key))
            .ok_or_else(|| format!("Secret {namespace}/{name} has no {key}"))?;
        String::from_utf8(bytes.0.clone())
            .map_err(|_| format!("Secret {namespace}/{name}: {key} is not PEM"))
    };
    let certificate = Certificate {
        chain: field("tls.crt")?,
        key: field("tls.key")?,
    };
    let identity = Identity::read(&certificate)
        .map_err(|error| format!("Secret {namespace}/{name} cannot be used: {error}"))?;
    Ok((certificate, identity))
}

/// The route that answers on `listeners` what `route` takes, each of its paths, with a
/// redirect to the same URL over HTTPS: on `port`, none for its default; 308, so that a
/// client keeps its method and body, as ingress-nginx answers; the query kept; and to be
/// kept `max_age` seconds, as a browser may otherwise keep a permanent redirect for ever
/// (04 §5).
fn https_redirect(route: &Route, listeners: Vec<String>, port: Option<u16>, max_age: u32) -> Route {
    let kept = Filter::ResponseHeaderModifier(HeaderChanges {
        set: vec![Header {
            name: "cache-control".to_owned(),
            value: format!("max-age={max_age}"),
        }],
        add: Vec::new(),
        remove: Vec::new(),
    });
    let rules = route
        .rules
        .iter()
        .map(|rule| Rule {
            matches: rule.matches.clone(),
            filters: vec![kept.clone()],
            forward: None,
            redirect: Some(Redirect {
                status: 308,
                scheme: Some(Scheme::Https),
                host: None,
                port,
                path: None,
                query: Query::Keep,
            }),
            paths: rule.paths.clone(),
            rate_limits: Vec::new(),
        })
        .collect();
    Route {
        name: format!("{}/https-redirect", route.name),
        listeners,
        hostnames: route.hostnames.clone(),
        rules,
        rate_limits: Vec::new(),
    }
}

/// A rule forwarding what `path` matches to `backend`.
fn forward(path: PathMatch, backend: Backend) -> Rule {
    Rule {
        matches: vec![Match {
            path: Some(path),
            grpc: None,
            method: None,
            headers: Vec::new(),
            query: Vec::new(),
        }],
        filters: Vec::new(),
        forward: Some(Forward {
            backends: vec![backend],
            timeouts: None,
            retry: None,
        }),
        redirect: None,
        paths: None,
        rate_limits: Vec::new(),
    }
}

/// The upstream a backend leads to, and its name: one a Service's port, whose endpoints are
/// followed through its EndpointSlices, or, `by_address`, reached by the Service's own name,
/// which resolves to its cluster IP. Or what is not there, when its Service, or that port of
/// it, is not, or it has no cluster IP to be reached by; or why the Ingress is left out.
fn backend_upstream(
    plane: &Plane,
    cluster: &Cluster,
    namespace: &str,
    backend: &IngressBackend,
    by_address: bool,
) -> Result<Result<(String, Upstream), String>, Problem> {
    let Some(backend) = &backend.service else {
        return Err(Problem::NotSupportedYet("a resource backend"));
    };
    let ServicePort {
        label: port,
        number,
        app_protocol,
        headless,
    } = match service_port(cluster, namespace, backend)? {
        Ok(port) => port,
        Err(missing) => return Ok(Err(missing)),
    };
    let service = &backend.name;
    let host = format!("{service}.{namespace}.svc.{}", plane.cluster_domain);
    let (protocol, tls) = match app_protocol {
        None | Some("kubernetes.io/ws") => (UpstreamProtocol::Http1, None),
        Some("kubernetes.io/h2c") => (UpstreamProtocol::Http2, None),
        Some("kubernetes.io/wss") if plane.backend_authorities.is_empty() => {
            return Ok(Err(format!(
                "Service {namespace}/{service} port {port} is kubernetes.io/wss, and the data \
                 plane trusts no authorities for TLS to backends"
            )));
        }
        // The certificate must carry the Service's own name, as a client inside the
        // cluster would ask for it.
        Some("kubernetes.io/wss") => (
            UpstreamProtocol::Http1,
            Some(UpstreamTls {
                server_name: host.clone(),
                authorities: plane.backend_authorities.clone(),
                client_certificate: None,
            }),
        ),
        // A protocol the gateway cannot speak makes the backend invalid, as Gateway API has
        // it (`UnsupportedProtocol`), rather than spoken to in HTTP/1.1 regardless.
        Some(other) => {
            return Ok(Err(format!(
                "Service {namespace}/{service} port {port} has appProtocol {other}, which \
                 the gateway does not speak"
            )));
        }
    };
    // A headless Service's name resolves to its pods, which listen on their own ports, not
    // the Service's.
    if by_address && headless {
        return Ok(Err(format!(
            "Service {namespace}/{service} has no cluster IP for \
             {SERVICE_UPSTREAM_ANNOTATION} to reach it by"
        )));
    }
    let (name, endpoints) = if by_address {
        (
            format!("{namespace}/{service}/{port}/service"),
            Endpoints::Dns(format!("{host}:{number}")),
        )
    } else {
        (
            format!("{namespace}/{service}/{port}"),
            Endpoints::Kubernetes(format!("{host}:{port}")),
        )
    };
    let upstream = Upstream {
        endpoints,
        protocol,
        tls,
        keepalive: None,
        health_check: None,
        load_balancer: LoadBalancer::P2c,
        slow_start: None,
        proxy_protocol: None,
    };
    Ok(Ok((name, upstream)))
}

/// A Service's port, as a backend names it.
struct ServicePort<'a> {
    /// As EndpointSlices know it: its name, or, for a Service's only port left unnamed, its
    /// number (03 §6).
    label: String,
    /// Its number, which the Service's cluster IP is reached on.
    number: i32,
    /// Its `appProtocol`.
    app_protocol: Option<&'a str>,
    /// Whether the Service has no cluster IP.
    headless: bool,
}

/// The Service port a backend names. Or what is not there; or why the Ingress is left out.
fn service_port<'a>(
    cluster: &'a Cluster,
    namespace: &str,
    backend: &IngressServiceBackend,
) -> Result<Result<ServicePort<'a>, String>, Problem> {
    // Kubernetes requires a port, by name or by number, of a Service backend.
    let wanted = match &backend.port {
        Some(wanted) if wanted.name.is_some() || wanted.number.is_some() => wanted,
        _ => return Err(Problem::Invalid("a Service backend without a port")),
    };
    let service_name = &backend.name;
    let Some(service) = cluster.services.iter().find(|service| {
        service.metadata.name.as_deref() == Some(service_name.as_str())
            && self::namespace(&service.metadata) == namespace
    }) else {
        return Ok(Err(format!(
            "Service {namespace}/{service_name} does not exist"
        )));
    };
    let spec = service.spec.as_ref();
    // Its name can be anything, another namespace's Service or the node itself among it,
    // which would let an Ingress reach what its namespace cannot (CVE-2021-25740).
    if spec.and_then(|spec| spec.type_.as_deref()) == Some("ExternalName") {
        return Err(Problem::NeverServed(
            "an ExternalName Service, whose name can lead anywhere",
        ));
    }
    let ports = spec
        .and_then(|spec| spec.ports.as_deref())
        .unwrap_or_default();
    let found = ports
        .iter()
        .find(|port| match (&wanted.name, wanted.number) {
            (Some(name), _) => port.name.as_ref() == Some(name),
            (None, Some(number)) => port.port == number,
            (None, None) => false,
        });
    let Some(port) = found else {
        let wanted = match (&wanted.name, wanted.number) {
            (Some(name), _) => format!("named {name}"),
            (None, number) => number.unwrap_or_default().to_string(),
        };
        return Ok(Err(format!(
            "Service {namespace}/{service_name} has no port {wanted}"
        )));
    };
    let name = match port.name.as_deref() {
        Some(name) if !name.is_empty() => name.to_owned(),
        // Kubernetes names every port of a Service with more than one.
        _ if ports.len() == 1 => port.port.to_string(),
        _ => {
            return Err(Problem::NotSupportedYet(
                "an unnamed port of a Service with several",
            ));
        }
    };
    Ok(Ok(ServicePort {
        label: name,
        number: port.port,
        app_protocol: port.app_protocol.as_deref(),
        headless: spec.and_then(|spec| spec.cluster_ip.as_deref()) == Some("None"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_are_what_is_served_as_the_spec_has_it_and_nothing_else() {
        let notes = [
            Problem::RetryCapped {
                rule: 0,
                asked: 100,
            },
            Problem::HostOverTls {
                name: "secure".to_owned(),
                host: "shop.example.com".to_owned(),
            },
        ];
        assert!(notes.iter().all(Problem::is_note));
        let faults = [
            Problem::NoRuleServed,
            Problem::MirrorLeftOff {
                rule: 0,
                why: String::new(),
            },
            Problem::ListenerCertificate {
                name: "secure".to_owned(),
                why: String::new(),
            },
        ];
        assert!(!faults.iter().any(Problem::is_note));
    }

    #[test]
    fn a_gateway_is_named_by_namespace_and_name() {
        let named = GatewayName::try_from("edge/web.v2".to_owned()).unwrap();
        assert_eq!(
            (named.namespace.as_str(), named.name.as_str()),
            ("edge", "web.v2")
        );
        assert_eq!(named.to_string(), "edge/web.v2");
        for written in [
            "web",
            "/web",
            "edge/",
            "Edge/web",
            "edge/web/x",
            "edge/-web",
            "edge/a..b",
        ] {
            assert_eq!(
                GatewayName::try_from(written.to_owned()),
                Err(format!("`{written}` is not <namespace>/<name>")),
            );
        }
        let long = format!("edge/{}", vec!["a".repeat(63); 4].join("."));
        assert!(GatewayName::try_from(long).is_err(), "longer than 253");
    }

    const HTTP: &str = "  http: { address: \"[::]:8080\", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n";

    /// Ingresses `good`, routing `/`, and `bad`, routing a path that climbs, which the data
    /// plane refuses, translated for a data plane with these listeners.
    fn good_and_bad(listeners: &str) -> Translation {
        let plane: Plane = serde_saphyr::from_str(&format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\nlisteners:\n{listeners}"
        ))
        .unwrap();
        let ingress = |name: &str, path: &str| -> Ingress {
            serde_saphyr::from_str(&format!(
                "metadata: {{ name: {name}, namespace: shop }}\nspec:\n  ingressClassName: edge\n  rules:\n    - host: {name}.example.com\n      http:\n        paths:\n          - {{ path: \"{path}\", pathType: Prefix, backend: {{ service: {{ name: web, port: {{ number: 80 }} }} }} }}\n"
            ))
            .unwrap()
        };
        let service: Service = serde_saphyr::from_str(
            "metadata: { name: web, namespace: shop }\nspec: { ports: [{ name: http, port: 80 }] }\n",
        )
        .unwrap();
        translate(
            &plane,
            &Cluster {
                ingress_classes: vec![
                    serde_saphyr::from_str(
                        "metadata: { name: edge }
spec: { controller: edgerush.dev/ingress-controller }
",
                    )
                    .unwrap(),
                ],
                ingresses: vec![ingress("good", "/"), ingress("bad", "/a/../admin")],
                services: vec![service],
                secrets: Vec::new(),
                secrets_assumed: false,
                ..Cluster::default()
            },
        )
    }

    fn reported(translation: &Translation) -> Vec<String> {
        translation.report.iter().map(ToString::to_string).collect()
    }

    /// An Ingress is compiled alone with the listeners it is served on: a `tcp` listener of
    /// the data plane's, which wants a TCP route no Ingress gives it, neither stops that
    /// check nor is blamed on any Ingress.
    #[test]
    fn an_ingress_is_checked_with_its_own_listeners_only() {
        let tcp = "  db: { address: \"[::]:5432\", protocol: tcp, proxy_protocol: off }\n";
        let translation = good_and_bad(&format!("{HTTP}{tcp}"));
        assert_eq!(
            reported(&translation),
            [
                "Ingress shop/bad: the data plane would refuse it: route `ingress/shop/bad/bad.example.com`, rules[0], matches[0]: path: path contains a `.` or `..` segment"
            ]
        );
        let routes: Vec<&str> = translation
            .config
            .routes
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(routes, ["ingress/shop/good/good.example.com"]);
    }

    /// Secrets taken as they will be in the cluster secure their hosts as named, unread,
    /// though none is among the objects; looked at, the same Secret is not there.
    #[test]
    fn assumed_secrets_secure_their_hosts_unread() {
        let plane: Plane = serde_saphyr::from_str(&format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\nlisteners:\n{HTTP}  https: {{ address: \"[::]:8443\", protocol: https, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, tls: {{ certificates: [], without_sni: none }} }}\n"
        ))
        .unwrap();
        let cluster = |secrets_assumed| {
            Cluster {
            ingress_classes: vec![
                serde_saphyr::from_str(
                    "metadata: { name: edge }\nspec: { controller: edgerush.dev/ingress-controller }\n",
                )
                .unwrap(),
            ],
            ingresses: vec![
                serde_saphyr::from_str(
                    "metadata: { name: web, namespace: shop }\nspec:\n  ingressClassName: edge\n  tls: [{ hosts: [shop.example.com], secretName: shop-tls }]\n  rules:\n    - host: shop.example.com\n      http:\n        paths:\n          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n",
                )
                .unwrap(),
            ],
            services: vec![
                serde_saphyr::from_str(
                    "metadata: { name: web, namespace: shop }\nspec: { ports: [{ name: http, port: 80 }] }\n",
                )
                .unwrap(),
            ],
            secrets: Vec::new(),
            secrets_assumed,
            ..Cluster::default()
        }
        };

        let assumed = translate(&plane, &cluster(true));
        assert!(assumed.tls.is_empty(), "{:?}", assumed.tls);
        assert_eq!(assumed.config.routes[0].listeners, ["https"]);
        assert_eq!(assumed.config.routes[1].listeners, ["http"]);
        assert!(assumed.config.routes[1].rules[0].redirect.is_some());
        let unread = Certificate {
            chain: String::new(),
            key: String::new(),
        };
        assert_eq!(
            assumed.config.certificates.get("shop/shop-tls"),
            Some(&unread)
        );
        let https = &assumed.config.listeners["https"];
        let stated = https.tls.as_ref().unwrap().certificates[0].stated();
        assert_eq!(stated, Some(&["shop.example.com".to_owned()][..]));
        assert!(compile(&assumed.config).is_ok());

        let checked = translate(&plane, &cluster(false));
        let said: Vec<String> = checked.tls.iter().map(ToString::to_string).collect();
        assert_eq!(
            said,
            [
                "Ingress shop/web: shop.example.com is refused over HTTPS and redirected there over plain HTTP: Secret shop/shop-tls does not exist"
            ]
        );
        // Its redirect alone, on `http`: the host is asked for secured, never served unencrypted.
        assert_eq!(checked.config.routes.len(), 1);
        assert_eq!(checked.config.routes[0].listeners, ["http"]);
        assert!(checked.config.routes[0].rules[0].redirect.is_some());
        assert!(checked.config.certificates.is_empty());
    }

    /// Where the data plane's own listeners are what it would refuse, compiling an Ingress
    /// alone would refuse every one for a fault none of them has: each is translated, and
    /// the whole config's problems are the listener's and the Ingresses' together.
    #[test]
    fn a_fault_of_the_data_planes_own_is_not_any_ingresses() {
        let refused = HTTP.replace("trusted_proxies: []", "trusted_proxies: [\"10.1.2.3/8\"]");
        let translation = good_and_bad(&refused);
        assert_eq!(reported(&translation), Vec::<String>::new());
        assert_eq!(translation.config.routes.len(), 2);
        let problems: Vec<String> = compile(&translation.config)
            .unwrap_err()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert!(
            problems
                .iter()
                .any(|problem| problem.starts_with("listener `http`")),
            "{problems:?}"
        );
    }

    /// The data plane's own settings go into its config as they are; one its config
    /// refuses is its own fault, not any Ingress's: each is still judged on its own.
    #[test]
    fn the_data_planes_own_settings_go_into_its_config() {
        let listeners = format!("{HTTP}data_plane: {{ set_aside_ms: 2000 }}\n");
        let translation = good_and_bad(&listeners);
        assert_eq!(translation.config.data_plane.set_aside_ms, Some(2000));
        let refused = format!("{HTTP}data_plane: {{ set_aside_ms: 0 }}\n");
        let translation = good_and_bad(&refused);
        let reported = reported(&translation);
        assert_eq!(reported.len(), 1, "{reported:?}");
        assert!(
            reported[0].starts_with("Ingress shop/bad: "),
            "{reported:?}"
        );
        assert_eq!(translation.config.routes.len(), 1);
        let problems: Vec<String> = compile(&translation.config)
            .unwrap_err()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].starts_with("the data plane: `set_aside_ms` is 0"),
            "{problems:?}"
        );
    }
}
