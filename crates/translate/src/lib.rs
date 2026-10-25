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
//! port, and that carry no annotations; an Ingress
//! that asks for anything more is left out whole, reported as not supported yet, rather
//! than translated in part. A path whose Service, or port, is not there, or whose port's
//! `appProtocol` the gateway does not speak, is translated with an invalid backend, answered
//! 500, and reported too. Hosts its `tls` lists are served on the `https` listeners as well,
//! with their Secret's certificate, read as the data plane reads one; a host whose Secret
//! is not there, cannot be used or does not cover it stays on `http`, and is reported.

use edgerush_certificates::Identity;
use edgerush_config::{
    Backend, Certificate, Config, DataPlane, Endpoints, Forward, Hostname, Listener,
    ListenerCertificate, LoadBalancer, Match, PathMatch, Protocol, Route, Rule, StatedNames,
    Upstream, UpstreamProtocol, UpstreamTls, Wildcard, compile,
};
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::networking::v1::{
    Ingress, IngressBackend, IngressClass, IngressServiceBackend,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;

/// The controller an IngressClass names to be served by EdgeRush (02 §2).
pub const CONTROLLER: &str = "edgerush.dev/ingress-controller";

/// The data plane's own settings, which in a cluster are its `DataPlane` resource's
/// ([05 §1](../../../docs/05-operator.md)).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plane {
    /// The IngressClass whose Ingresses it serves.
    pub ingress_class: String,
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
    /// The certificates, in PEM, of the authorities trusted to vouch for a backend reached
    /// over TLS: a Service port whose `appProtocol` is `kubernetes.io/wss`. None, and such
    /// a backend is answered 500.
    #[serde(default)]
    pub backend_authorities: Vec<String>,
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
    /// Hosts of Ingresses translated that their `tls` lists but are not served over HTTPS.
    pub unsecured: Vec<Unsecured>,
    /// Paths of Ingresses translated that never take a request: another rule, older, has
    /// the same path on the same host.
    pub shadowed: Vec<Shadowed>,
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

/// A host an Ingress's `tls` lists that is not served over HTTPS: its Secret is not there,
/// cannot be used or does not cover it, or the data plane has no `https` listener. It is
/// served over plain HTTP where the data plane has an `http` listener, and nowhere else
/// otherwise (04 §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsecured {
    /// The Ingress whose host it is.
    pub object: Object,
    /// The host.
    pub host: String,
    /// Why: `Secret shop/tls does not exist`.
    pub reason: String,
    /// Whether it is served over plain HTTP.
    pub over_http: bool,
}

impl fmt::Display for Unsecured {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (object, host, reason) = (&self.object, &self.host, &self.reason);
        if self.over_http {
            write!(f, "{object}: {host} is served over HTTP only: {reason}")
        } else {
            write!(f, "{object}: {host} is not served: {reason}")
        }
    }
}

/// A path that never takes a request: an older Ingress, or an earlier rule of the same one,
/// has it on the same host, matched the same way, and the router ranks the older first
/// (07 §1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shadowed {
    /// The Ingress whose path it is.
    pub object: Object,
    /// The host, as the Ingress names it (`*` for every host).
    pub host: String,
    /// The path and how it is matched: `/api (Prefix)`.
    pub path: String,
    /// The Ingress that has it first: older, or as old and first by namespace and name.
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
}

/// Translates `cluster`'s objects into the config of the data plane `plane` describes.
#[must_use]
pub fn translate(plane: &Plane, cluster: &Cluster) -> Translation {
    let mut routes = Vec::new();
    let mut upstreams = BTreeMap::new();
    let mut report = Vec::new();
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
    ))
    .is_ok();
    // The certificates every `https` listener presents, each for its hosts alone, in the
    // order of the Ingresses that use them: a host two Secrets are stated for is the older
    // Ingress's, as its paths are.
    let mut presented: Vec<(String, Certificate, Vec<String>)> = Vec::new();
    let mut unsecured = Vec::new();
    // Each host and path, matched one way, and the Ingress that had it first: oldest first,
    // as the router ranks them.
    let mut claimed: BTreeMap<(String, String), (Object, bool)> = BTreeMap::new();
    let mut shadowed = Vec::new();
    let mut unresolved = Vec::new();
    for ingress in ingresses {
        let object = || Object::of("Ingress", &ingress.metadata);
        let dated = ingress.metadata.creation_timestamp.is_some();
        match ingress_routes(plane, cluster, ingress) {
            Ok(IngressRoutes {
                routes: translated,
                upstreams: used,
                missing,
                certificates,
                unsecured: insecure,
            }) => {
                if checked_alone {
                    let alone = config_of(
                        served_on.clone(),
                        translated.clone(),
                        used.clone(),
                        &certificates,
                    );
                    if let Err(problems) = compile(&alone) {
                        let problems: Vec<String> =
                            problems.iter().map(ToString::to_string).collect();
                        report.push(Finding {
                            object: object(),
                            problem: Problem::Refused(problems.join("; ")),
                        });
                        continue;
                    }
                }
                for route in &translated {
                    for (host, path) in claims(route) {
                        match claimed.entry((host, path)) {
                            Entry::Vacant(free) => {
                                free.insert((object(), dated));
                            }
                            Entry::Occupied(taken) => {
                                let (host, path) = taken.key().clone();
                                let (by, by_dated) = taken.get().clone();
                                shadowed.push(Shadowed {
                                    object: object(),
                                    host,
                                    path,
                                    by,
                                    by_age: dated && by_dated,
                                });
                            }
                        }
                    }
                }
                unresolved.extend(missing.into_iter().map(|(host, path, reason)| Unresolved {
                    object: object(),
                    host,
                    path,
                    reason,
                }));
                unsecured.extend(
                    insecure
                        .into_iter()
                        .map(|(host, reason, over_http)| Unsecured {
                            object: object(),
                            host,
                            reason,
                            over_http,
                        }),
                );
                for (name, certificate, hosts) in certificates {
                    match presented.iter_mut().find(|(named, ..)| *named == name) {
                        Some((.., stated)) => {
                            for host in hosts {
                                if !stated.contains(&host) {
                                    stated.push(host);
                                }
                            }
                        }
                        None => presented.push((name, certificate, hosts)),
                    }
                }
                routes.extend(translated);
                upstreams.extend(used);
            }
            Err(problem) => report.push(Finding {
                object: object(),
                problem,
            }),
        }
    }
    Translation {
        config: config_of(plane.listeners.clone(), routes, upstreams, &presented),
        report,
        passed_over: others.len(),
        unresolved,
        unsecured,
        shadowed,
    }
}

/// The hosts and paths a route claims, each path with how it is matched.
fn claims(route: &Route) -> impl Iterator<Item = (String, String)> + '_ {
    route.hostnames.iter().flat_map(move |hostname| {
        route
            .rules
            .iter()
            .flat_map(|rule| &rule.matches)
            .filter_map(|matched| Some((hostname.name.clone(), described(matched.path.as_ref()?))))
    })
}

/// A path and how it is matched, as the report says it: `/api (Prefix)`.
fn described(path: &PathMatch) -> String {
    match path {
        PathMatch::Prefix(path) => format!("{path} (Prefix)"),
        PathMatch::Exact(path) => format!("{path} (Exact)"),
        PathMatch::Regex(path) => format!("{path} (regex)"),
    }
}

/// A config of these listeners, routes and upstreams, and nothing else but `presented`:
/// certificates each `https` listener presents for the hosts stated with them, after any
/// it names itself.
fn config_of(
    mut listeners: BTreeMap<String, Listener>,
    routes: Vec<Route>,
    upstreams: BTreeMap<String, Upstream>,
    presented: &[(String, Certificate, Vec<String>)],
) -> Config {
    for listener in listeners.values_mut() {
        let Some(tls) = listener.tls.as_mut() else {
            continue;
        };
        if listener.protocol == Protocol::Https {
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
    /// Its routes, one a host.
    routes: Vec<Route>,
    /// The upstreams they lead to.
    upstreams: BTreeMap<String, Upstream>,
    /// Each path whose backend is not there: its host, the path described (none for the
    /// default backend), and why.
    missing: Vec<(String, Option<String>, String)>,
    /// The certificates its secured hosts are served with: each Secret's, by its config
    /// name, with the hosts it answers for.
    certificates: Vec<(String, Certificate, Vec<String>)>,
    /// Each host `tls` lists that is not served over HTTPS: why, and whether it is served
    /// over plain HTTP.
    unsecured: Vec<(String, String, bool)>,
}

/// An Ingress's routes, the upstreams they lead to, the certificates of its secured hosts,
/// the paths whose backend is not there and the hosts that could not be secured; or why
/// none.
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
    // has any is not translated until they are read; all but kubectl's own record of what it
    // applied, which says nothing of routing and is on whatever `kubectl apply` made.
    if ingress
        .metadata
        .annotations
        .iter()
        .flatten()
        .any(|(name, _)| name != "kubectl.kubernetes.io/last-applied-configuration")
    {
        return Err(Problem::NotSupportedYet("annotations"));
    }
    let Some(spec) = ingress.spec.as_ref().filter(|spec| {
        spec.default_backend.is_some() || spec.rules.as_ref().is_some_and(|rules| !rules.is_empty())
    }) else {
        return Err(Problem::Invalid("neither rules nor a default backend"));
    };
    // The hosts it secures, each with its Secret's name; a host listed twice keeps the
    // first.
    let mut tls_hosts: BTreeMap<&str, &str> = BTreeMap::new();
    for entry in spec.tls.iter().flatten() {
        let Some(secret) = entry.secret_name.as_deref() else {
            return Err(Problem::NotSupportedYet("a TLS entry without a Secret"));
        };
        let hosts = entry.hosts.as_deref().unwrap_or_default();
        if hosts.is_empty() {
            return Err(Problem::NotSupportedYet("a TLS entry without hosts"));
        }
        for host in hosts {
            tls_hosts.entry(host.as_str()).or_insert(secret);
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
            match backend_upstream(plane, cluster, namespace, backend)? {
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
            let backend = backend_of(&path.backend, host, Some(described(&matched)))?;
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
    let mut read: BTreeMap<&str, Result<(Certificate, Identity), String>> = BTreeMap::new();
    let mut unsecured = Vec::new();
    let mut listeners_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for host in by_host.keys() {
        let mut listeners = http.clone();
        match tls_hosts.get(host.as_str()) {
            None if http.is_empty() => return Err(Problem::NoListener),
            None => {}
            Some(&secret) => {
                let checked = read
                    .entry(secret)
                    .or_insert_with(|| secret_certificate(cluster, namespace, secret));
                let reason = match checked {
                    Err(reason) => Some(reason.clone()),
                    Ok((_, identity)) if !identity.covers(host) => {
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
                    unsecured.push((host.clone(), reason, !http.is_empty()));
                }
            }
        }
        if !listeners.is_empty() {
            listeners_of.insert(host.clone(), listeners);
        }
    }
    // A host served nowhere is said as such, and nothing else of it.
    missing.retain(|(host, ..)| listeners_of.contains_key(host));
    let name = ingress.metadata.name.as_deref().unwrap_or_default();
    let routes: Vec<Route> = by_host
        .into_iter()
        .filter_map(|(host, rules)| {
            Some(Route {
                name: format!("ingress/{namespace}/{name}/{host}"),
                listeners: listeners_of.remove(&host)?,
                hostnames: vec![Hostname {
                    // An Ingress's `*` stands for exactly one label (04 §5).
                    wildcard: host.starts_with("*.").then_some(Wildcard::OneLabel),
                    name: host,
                    falls_through: plane.ingress_hosts_fall_through,
                }],
                rules,
                rate_limits: Vec::new(),
            })
        })
        .collect();
    let used: Vec<&str> = routes
        .iter()
        .flat_map(|route| &route.rules)
        .filter_map(|rule| rule.forward.as_ref())
        .flat_map(|forward| &forward.backends)
        .filter_map(|backend| backend.upstream.as_deref())
        .collect();
    upstreams.retain(|name, _| used.contains(&name.as_str()));
    Ok(IngressRoutes {
        routes,
        upstreams,
        missing,
        certificates,
        unsecured,
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
/// followed through its EndpointSlices. Or what is not there, when its Service, or that
/// port of it, is not; or why the Ingress is left out.
fn backend_upstream(
    plane: &Plane,
    cluster: &Cluster,
    namespace: &str,
    backend: &IngressBackend,
) -> Result<Result<(String, Upstream), String>, Problem> {
    let Some(backend) = &backend.service else {
        return Err(Problem::NotSupportedYet("a resource backend"));
    };
    let (port, app_protocol) = match service_port(cluster, namespace, backend)? {
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
    let name = format!("{namespace}/{service}/{port}");
    let upstream = Upstream {
        endpoints: Endpoints::Kubernetes(format!("{host}:{port}")),
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

/// The Service port a backend names, as EndpointSlices know it: its name, or, for a
/// Service's only port left unnamed, its number (03 §6); and its `appProtocol`. Or what is
/// not there; or why the Ingress is left out.
fn service_port<'a>(
    cluster: &'a Cluster,
    namespace: &str,
    backend: &IngressServiceBackend,
) -> Result<Result<(String, Option<&'a str>), String>, Problem> {
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
    if spec.and_then(|spec| spec.type_.as_deref()) == Some("ExternalName") {
        return Err(Problem::NotSupportedYet("an ExternalName Service"));
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
    Ok(Ok((name, port.app_protocol.as_deref())))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HTTP: &str = "  http: { address: \"[::]:8080\", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n";

    /// Ingresses `good`, routing `/`, and `bad`, routing a path that climbs, which the data
    /// plane refuses, translated for a data plane with these listeners.
    fn good_and_bad(listeners: &str) -> Translation {
        let plane: Plane = serde_saphyr::from_str(&format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\nlisteners:\n{listeners}"
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
}
