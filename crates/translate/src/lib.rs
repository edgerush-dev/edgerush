//! Translation: the Kubernetes objects a control plane watches, into the config one data
//! plane runs ([04 §2](../../../docs/04-control-plane.md)).
//!
//! A pure crate: no I/O, no clocks. [`translate`] is a function of the objects and of the
//! data plane's own settings, so that its tests need no cluster, and the control plane, its
//! admission webhook and `validate --from-manifests` all translate alike.
//!
//! **It never fails as a whole.** What it cannot translate is left out, object by object,
//! and said in its report. So far it translates Ingresses whose rules each name a host and
//! route paths by `Prefix` to a Service's port, and that carry no annotations; an Ingress
//! that asks for anything more is left out whole, reported as not supported yet, rather
//! than translated in part.

use edgerush_config::{
    Backend, Config, DataPlane, Endpoints, Forward, Hostname, Listener, LoadBalancer, Match,
    PathMatch, Protocol, Route, Rule, Upstream, UpstreamProtocol,
};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::{HTTPIngressPath, Ingress, IngressServiceBackend};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;

/// The data plane's own settings, which in a cluster are its `DataPlane` resource's
/// ([05 §1](../../../docs/05-operator.md)).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plane {
    /// The IngressClass whose Ingresses it serves.
    pub ingress_class: String,
    /// Its listeners, as its config states them. An Ingress is served on every `http` one.
    pub listeners: BTreeMap<String, Listener>,
    /// The cluster's DNS domain, which a Service's name ends in: `cluster.local` unless the
    /// cluster was set up otherwise.
    pub cluster_domain: String,
    /// Whether an Ingress host stays a candidate on hosts a more specific claim also covers:
    /// false is nginx's way ([04 §5](../../../docs/04-control-plane.md)).
    pub ingress_hosts_fall_through: bool,
}

/// The objects translated.
#[derive(Debug, Clone, Default)]
pub struct Cluster {
    /// Every Ingress seen, whatever its class.
    pub ingresses: Vec<Ingress>,
    /// Every Service seen.
    pub services: Vec<Service>,
}

/// A data plane's config, and what could not go into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translation {
    /// The config.
    pub config: Config,
    /// What was left out, and why, object by object.
    pub report: Vec<Finding>,
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
        write!(f, "{} {}/{}", self.kind, self.namespace, self.name)
    }
}

/// Why an object was left out.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// It asks for something translation does not do yet.
    #[error("not supported yet: {0}")]
    NotSupportedYet(&'static str),
    /// The data plane has no listener to serve it on.
    #[error("the data plane has no `http` listener to serve it on")]
    NoListener,
}

/// Translates `cluster`'s objects into the config of the data plane `plane` describes.
#[must_use]
pub fn translate(plane: &Plane, cluster: &Cluster) -> Translation {
    let mut routes = Vec::new();
    let mut upstreams = BTreeMap::new();
    let mut report = Vec::new();
    // Oldest first, then by namespace and name, as Gateway API ranks routes: the order is
    // precedence's last tie-breaker (07 §1).
    let mut ingresses: Vec<&Ingress> = cluster
        .ingresses
        .iter()
        .filter(|ingress| class_of(ingress) == Some(plane.ingress_class.as_str()))
        .collect();
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
    for ingress in ingresses {
        match ingress_routes(plane, cluster, ingress) {
            Ok((translated, used)) => {
                routes.extend(translated);
                upstreams.extend(used);
            }
            Err(problem) => report.push(Finding {
                object: Object::of("Ingress", &ingress.metadata),
                problem,
            }),
        }
    }
    Translation {
        config: Config {
            listeners: plane.listeners.clone(),
            routes,
            tcp_routes: Vec::new(),
            tls_routes: Vec::new(),
            upstreams,
            data_plane: DataPlane::default(),
            rate_limiting: None,
            certificates: BTreeMap::new(),
            limits: None,
        },
        report,
    }
}

/// An object without a namespace is in `default`, as Kubernetes takes it.
fn namespace(metadata: &ObjectMeta) -> &str {
    metadata.namespace.as_deref().unwrap_or("default")
}

fn class_of(ingress: &Ingress) -> Option<&str> {
    ingress.spec.as_ref()?.ingress_class_name.as_deref()
}

/// An Ingress's routes, one a host, and the upstreams they lead to; or why none.
fn ingress_routes(
    plane: &Plane,
    cluster: &Cluster,
    ingress: &Ingress,
) -> Result<(Vec<Route>, BTreeMap<String, Upstream>), Problem> {
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
    let Some(spec) = &ingress.spec else {
        return Ok((Vec::new(), BTreeMap::new()));
    };
    if spec.tls.as_ref().is_some_and(|tls| !tls.is_empty()) {
        return Err(Problem::NotSupportedYet("TLS"));
    }
    if spec.default_backend.is_some() {
        return Err(Problem::NotSupportedYet("a default backend"));
    }
    let listeners: Vec<String> = plane
        .listeners
        .iter()
        .filter(|(_, listener)| listener.protocol == Protocol::Http)
        .map(|(name, _)| name.clone())
        .collect();
    let namespace = namespace(&ingress.metadata);
    let mut by_host: BTreeMap<String, Vec<Rule>> = BTreeMap::new();
    let mut upstreams = BTreeMap::new();
    for rule in spec.rules.iter().flatten() {
        let Some(host) = &rule.host else {
            return Err(Problem::NotSupportedYet("a rule without a host"));
        };
        if host.starts_with('*') {
            return Err(Problem::NotSupportedYet("a wildcard host"));
        }
        let Some(http) = &rule.http else {
            return Err(Problem::NotSupportedYet("a rule without paths"));
        };
        for path in &http.paths {
            let prefix = match (path.path_type.as_str(), &path.path) {
                ("Prefix", Some(prefix)) => prefix.clone(),
                ("Prefix", None) => {
                    return Err(Problem::NotSupportedYet("a Prefix path without a path"));
                }
                _ => return Err(Problem::NotSupportedYet("a path type other than Prefix")),
            };
            let (name, upstream) = path_upstream(plane, cluster, namespace, path)?;
            by_host.entry(host.clone()).or_default().push(Rule {
                matches: vec![Match {
                    path: Some(PathMatch::Prefix(prefix)),
                    grpc: None,
                    method: None,
                    headers: Vec::new(),
                    query: Vec::new(),
                }],
                filters: Vec::new(),
                forward: Some(Forward {
                    backends: vec![Backend {
                        upstream: name.clone(),
                        weight: 1,
                    }],
                    timeouts: None,
                    retry: None,
                }),
                redirect: None,
                paths: None,
                rate_limits: Vec::new(),
            });
            upstreams.insert(name, upstream);
        }
    }
    if by_host.is_empty() {
        return Ok((Vec::new(), BTreeMap::new()));
    }
    if listeners.is_empty() {
        return Err(Problem::NoListener);
    }
    let name = ingress.metadata.name.as_deref().unwrap_or_default();
    let routes = by_host
        .into_iter()
        .map(|(host, rules)| Route {
            name: format!("ingress/{namespace}/{name}/{host}"),
            listeners: listeners.clone(),
            hostnames: vec![Hostname {
                name: host,
                wildcard: None,
                falls_through: plane.ingress_hosts_fall_through,
            }],
            rules,
            rate_limits: Vec::new(),
        })
        .collect();
    Ok((routes, upstreams))
}

/// The upstream a path's backend leads to, and its name: one a Service's port, whose
/// endpoints are followed through its EndpointSlices.
fn path_upstream(
    plane: &Plane,
    cluster: &Cluster,
    namespace: &str,
    path: &HTTPIngressPath,
) -> Result<(String, Upstream), Problem> {
    let Some(backend) = &path.backend.service else {
        return Err(Problem::NotSupportedYet("a resource backend"));
    };
    let port = service_port(cluster, namespace, backend)?;
    let service = &backend.name;
    let name = format!("{namespace}/{service}/{port}");
    let upstream = Upstream {
        endpoints: Endpoints::Kubernetes(format!(
            "{service}.{namespace}.svc.{}:{port}",
            plane.cluster_domain
        )),
        protocol: UpstreamProtocol::default(),
        tls: None,
        keepalive: None,
        health_check: None,
        load_balancer: LoadBalancer::P2c,
        slow_start: None,
        proxy_protocol: None,
    };
    Ok((name, upstream))
}

/// The Service port a backend names, as EndpointSlices know it: its name, or, for a
/// Service's only port left unnamed, its number (03 §6).
fn service_port(
    cluster: &Cluster,
    namespace: &str,
    backend: &IngressServiceBackend,
) -> Result<String, Problem> {
    let missing = Problem::NotSupportedYet("a backend Service or port that does not exist");
    let service = cluster
        .services
        .iter()
        .find(|service| {
            service.metadata.name.as_deref() == Some(backend.name.as_str())
                && self::namespace(&service.metadata) == namespace
        })
        .ok_or_else(|| missing.clone())?;
    let spec = service.spec.as_ref().ok_or_else(|| missing.clone())?;
    if spec.type_.as_deref() == Some("ExternalName") {
        return Err(Problem::NotSupportedYet("an ExternalName Service"));
    }
    let ports = spec.ports.as_deref().unwrap_or_default();
    let wanted = backend.port.as_ref().ok_or_else(|| missing.clone())?;
    let port = ports
        .iter()
        .find(|port| match (&wanted.name, wanted.number) {
            (Some(name), _) => port.name.as_ref() == Some(name),
            (None, Some(number)) => port.port == number,
            (None, None) => false,
        })
        .ok_or(missing)?;
    match port.name.as_deref() {
        Some(name) if !name.is_empty() => Ok(name.to_owned()),
        // Kubernetes names every port of a Service with more than one.
        _ if ports.len() == 1 => Ok(port.port.to_string()),
        _ => Err(Problem::NotSupportedYet(
            "an unnamed port of a Service with several",
        )),
    }
}
