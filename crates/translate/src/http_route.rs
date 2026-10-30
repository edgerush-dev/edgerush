//! HTTPRoutes attached to the data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)):
//! each `parentRef` to it fitted to the listeners it names, those that take the route's kind
//! and namespace, and whose hostnames meet the route's; the hostnames each listener is left
//! with once those of more specific listeners are taken away (listener isolation); the claim
//! of no rules each listener's hostname makes; each route's rules, a rule translation cannot
//! take dropped by itself; and every host a claim of one API takes from a claim of the
//! other.

use crate::gateway::{GatewayOutcome, ListenerOutcome};
use crate::{Cluster, Object, Plane, backend_upstream, namespace};
use edgerush_config::{
    Backend, Forward, Hostname, Match, PathMatch, Route, Rule, Upstream, ValueMatch,
    ValuePredicate, Wildcard,
};
use edgerush_gateway_api::http_route::{
    HTTPRoute, HTTPRouteParentRefs, HTTPRouteRules, HTTPRouteRulesBackendRefs,
};
use k8s_openapi::api::networking::v1::{IngressBackend, IngressServiceBackend, ServiceBackendPort};
use std::collections::BTreeMap;
use std::fmt;

/// The API group of the Gateway API's kinds.
const GROUP: &str = "gateway.networking.k8s.io";

/// What became of an HTTPRoute that names the data plane's Gateway among its parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteOutcome {
    /// The HTTPRoute.
    pub route: Object,
    /// Each of its `parentRefs` to the data plane's Gateway, in the order it lists them.
    pub parents: Vec<ParentOutcome>,
}

/// One `parentRef` of an HTTPRoute to the data plane's Gateway, and what became of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentOutcome {
    /// The listener it names, if it names one.
    pub section_name: Option<String>,
    /// The port it names, if it names one.
    pub port: Option<i32>,
    /// The listeners it is attached to, each with the route's hostnames there; or why none.
    pub attached: Result<Vec<Attachment>, NotAttached>,
}

/// An HTTPRoute attached to one listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    /// The listener.
    pub listener: String,
    /// The route's hostnames there: each the route's own and the listener's met, those of
    /// more specific listeners on its port taken away. None left, and the route serves
    /// nothing through this listener, attached all the same.
    pub hostnames: Vec<String>,
}

/// Why a `parentRef` attaches the route to no listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotAttached {
    /// No accepted listener has the `sectionName` and `port` it names (`NoMatchingParent`).
    #[error("no listener the Gateway serves has the name and port it names")]
    NoMatchingParent,
    /// Every listener it names refuses the route's kind or namespace
    /// (`NotAllowedByListeners`).
    #[error("no listener it names takes an HTTPRoute from its namespace")]
    NotAllowedByListeners,
    /// The route's hostnames meet those of no listener that takes it
    /// (`NoMatchingListenerHostname`).
    #[error("its hostnames meet no hostname of a listener that takes it")]
    NoMatchingListenerHostname,
}

impl fmt::Display for RouteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for parent in &self.parents {
            if !first {
                writeln!(f)?;
            }
            first = false;
            write!(f, "{}", self.route)?;
            if let Some(section) = &parent.section_name {
                write!(f, ", section {section}")?;
            }
            if let Some(port) = parent.port {
                write!(f, ", port {port}")?;
            }
            match &parent.attached {
                Ok(attachments) => {
                    let each: Vec<String> = attachments
                        .iter()
                        .map(|a| {
                            let hosts = if a.hostnames.is_empty() {
                                "no hostname left".to_owned()
                            } else {
                                a.hostnames.join(", ")
                            };
                            format!("{} ({hosts})", a.listener)
                        })
                        .collect();
                    write!(f, ": attached to {}", each.join("; "))?;
                }
                Err(why) => write!(f, ": not attached: {why}")?,
            }
        }
        Ok(())
    }
}

/// Every HTTPRoute that names `gateway` among its parents, oldest first and then by
/// namespace and name, as routes rank, and what became of each such `parentRef`.
pub(crate) fn attached(cluster: &Cluster, gateway: &GatewayOutcome) -> Vec<RouteOutcome> {
    let mut routes: Vec<&HTTPRoute> = cluster.http_routes.iter().collect();
    routes.sort_by_key(|route| {
        let metadata = &route.metadata;
        (
            metadata.creation_timestamp.clone(),
            metadata.namespace.clone(),
            metadata.name.clone(),
        )
    });
    routes
        .into_iter()
        .filter_map(|route| {
            let route_namespace = namespace(&route.metadata);
            let parents: Vec<ParentOutcome> = route
                .spec
                .parent_refs
                .iter()
                .flatten()
                .filter(|parent| names(parent, route_namespace, &gateway.gateway))
                .map(|parent| ParentOutcome {
                    section_name: parent.section_name.clone(),
                    port: parent.port,
                    attached: attach(route, route_namespace, parent, gateway, cluster),
                })
                .collect();
            (!parents.is_empty()).then(|| RouteOutcome {
                route: Object::of("HTTPRoute", &route.metadata),
                parents,
            })
        })
        .collect()
}

/// Whether `parent`, of a route in `route_namespace`, is the Gateway `gateway`.
fn names(parent: &HTTPRouteParentRefs, route_namespace: &str, gateway: &Object) -> bool {
    parent.group == GROUP
        && parent.kind == "Gateway"
        && parent.namespace.as_deref().unwrap_or(route_namespace) == gateway.namespace
        && parent.name == gateway.name
}

/// The listeners `parent` attaches `route` to, or why none.
fn attach(
    route: &HTTPRoute,
    route_namespace: &str,
    parent: &HTTPRouteParentRefs,
    gateway: &GatewayOutcome,
    cluster: &Cluster,
) -> Result<Vec<Attachment>, NotAttached> {
    let accepted = |listener: &&ListenerOutcome| listener.served.is_ok();
    let named: Vec<&ListenerOutcome> = gateway
        .listeners
        .iter()
        .filter(accepted)
        .filter(|l| {
            parent
                .section_name
                .as_ref()
                .is_none_or(|section| *section == l.name)
        })
        .filter(|l| parent.port.is_none_or(|port| port == i32::from(l.port)))
        .collect();
    if named.is_empty() {
        return Err(NotAttached::NoMatchingParent);
    }
    let taking: Vec<&ListenerOutcome> = named
        .into_iter()
        .filter(|l| l.allowed.kinds.contains(&"HTTPRoute"))
        .filter(|l| l.allowed.namespaces.allow(route_namespace, cluster))
        .collect();
    if taking.is_empty() {
        return Err(NotAttached::NotAllowedByListeners);
    }
    let own: &[String] = route.spec.hostnames.as_deref().unwrap_or_default();
    let mut attachments = Vec::new();
    for listener in taking {
        let met: Vec<String> = if own.is_empty() {
            vec![listener.hostname.clone().unwrap_or_else(|| "*".to_owned())]
        } else {
            own.iter()
                .filter_map(|hostname| intersection(listener.hostname.as_deref(), hostname))
                .collect()
        };
        if met.is_empty() {
            continue;
        }
        let others: Vec<&ListenerOutcome> = gateway
            .listeners
            .iter()
            .filter(accepted)
            .filter(|other| other.name != listener.name && other.port == listener.port)
            .collect();
        let left: Vec<String> = met
            .into_iter()
            .filter(|hostname| {
                !others.iter().any(|other| {
                    other.hostname.as_deref().is_some_and(|theirs| {
                        more_specific(theirs, listener.hostname.as_deref())
                            && within(theirs, hostname)
                    })
                })
            })
            .collect();
        let mut hostnames: Vec<String> = Vec::with_capacity(left.len());
        for hostname in left {
            if !hostnames.contains(&hostname) {
                hostnames.push(hostname);
            }
        }
        attachments.push(Attachment {
            listener: listener.name.clone(),
            hostnames,
        });
    }
    if attachments.is_empty() {
        return Err(NotAttached::NoMatchingListenerHostname);
    }
    Ok(attachments)
}

/// A Gateway API hostname's suffix, if it is a wildcard: `example.com` of `*.example.com`.
fn wildcard(hostname: &str) -> Option<&str> {
    hostname.strip_prefix("*.")
}

/// Whether the exact name, or wildcard suffix, `name` lies under the suffix `suffix`, by at
/// least one more label: `a.example.com` under `example.com`.
fn under(suffix: &str, name: &str) -> bool {
    name.len() > suffix.len() + 1
        && name.ends_with(suffix)
        && name.as_bytes()[name.len() - suffix.len() - 1] == b'.'
}

/// The hosts a listener of hostname `listener` (every host, none) and a route of hostname
/// `route` both cover, as one hostname, as the spec meets them; none where they cover none
/// alike. A Gateway API wildcard covers one label or more.
fn intersection(listener: Option<&str>, route: &str) -> Option<String> {
    let Some(listener) = listener else {
        return Some(route.to_owned());
    };
    match (wildcard(listener), wildcard(route)) {
        (None, None) => (listener == route).then(|| listener.to_owned()),
        (None, Some(theirs)) => under(theirs, listener).then(|| listener.to_owned()),
        (Some(ours), None) => under(ours, route).then(|| route.to_owned()),
        (Some(ours), Some(theirs)) => {
            if theirs == ours || under(ours, theirs) {
                Some(route.to_owned())
            } else if under(theirs, ours) {
                Some(listener.to_owned())
            } else {
                None
            }
        }
    }
}

/// Whether the listener hostname `theirs` is more specific than `ours` (none: every host),
/// as the router ranks hostnames: an exact name before any wildcard, a longer wildcard
/// before a shorter, anything before none.
fn more_specific(theirs: &str, ours: Option<&str>) -> bool {
    let Some(ours) = ours else {
        return true;
    };
    match (wildcard(theirs), wildcard(ours)) {
        (None, Some(_)) => true,
        (Some(t), Some(o)) => t.len() > o.len(),
        _ => false,
    }
}

/// Whether every host the hostname `hostname` covers is the listener hostname `theirs`'s:
/// it is that name, or under that wildcard.
fn within(theirs: &str, hostname: &str) -> bool {
    if theirs == hostname {
        return true;
    }
    match (wildcard(theirs), wildcard(hostname)) {
        (Some(suffix), None) => under(suffix, hostname),
        (Some(suffix), Some(inner)) => under(suffix, inner),
        _ => false,
    }
}

/// The claim of no rules each accepted listener's hostname makes, so that no less specific
/// claim serves it (listener isolation): `gateway/<namespace>/<name>/<listener>`, on the
/// data plane's listeners that serve it.
pub(crate) fn claims(gateway: &GatewayOutcome) -> Vec<Route> {
    gateway
        .listeners
        .iter()
        .filter_map(|listener| {
            let served = listener.served.as_ref().ok()?;
            let hostname = listener.hostname.as_ref()?;
            Some(Route {
                name: format!(
                    "gateway/{}/{}/{}",
                    gateway.gateway.namespace, gateway.gateway.name, listener.name
                ),
                listeners: served.clone(),
                hostnames: vec![claim(hostname)],
                rules: Vec::new(),
                rate_limits: Vec::new(),
            })
        })
        .collect()
}

/// A Gateway API hostname as the config claims it: its wildcard one label or more, never
/// falling through (03 §4).
pub(crate) fn claim(hostname: &str) -> Hostname {
    Hostname {
        name: hostname.to_owned(),
        wildcard: wildcard(hostname).map(|_| Wildcard::AnyLabels),
        falls_through: false,
    }
}

/// A host a claim of one API takes from a claim of the other: the second covers it, is less
/// specific and does not fall through, so serves it no longer (04, "one rule ranks them
/// both").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Taken {
    /// The route of the claim that takes the host.
    pub by: String,
    /// The hostname it claims.
    pub hostname: String,
    /// The route of the claim it is taken from.
    pub from: String,
    /// That claim's hostname.
    pub from_hostname: String,
}

impl fmt::Display for Taken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} claims {}, taking it from {}'s {}, which does not fall through",
            described(&self.by),
            self.hostname,
            described(&self.from),
            self.from_hostname
        )
    }
}

/// A route's object, from its name: `Ingress shop/web`, `Gateway edge/web listener api`,
/// `HTTPRoute shop/web`.
fn described(route: &str) -> String {
    let mut parts = route.splitn(4, '/');
    let (api, namespace, name, rest) = (parts.next(), parts.next(), parts.next(), parts.next());
    match (api, namespace, name) {
        (Some("ingress"), Some(namespace), Some(name)) => format!("Ingress {namespace}/{name}"),
        (Some("gateway"), Some(namespace), Some(name)) => format!(
            "Gateway {namespace}/{name} listener {}",
            rest.unwrap_or_default()
        ),
        (Some("httproute"), Some(namespace), Some(name)) => format!("HTTPRoute {namespace}/{name}"),
        _ => route.to_owned(),
    }
}

/// Which API a config route is of, by its name.
fn api(route: &Route) -> Option<&'static str> {
    if route.name.starts_with("ingress/") {
        Some("Ingress")
    } else if route.name.starts_with("gateway/") || route.name.starts_with("httproute/") {
        Some("Gateway API")
    } else {
        None
    }
}

/// Every host a claim of one API takes from a claim of the other among `routes`.
pub(crate) fn taken(routes: &[Route]) -> Vec<Taken> {
    let mut found = Vec::new();
    for by in routes {
        for from in routes {
            if api(by).is_none() || api(from).is_none() || api(by) == api(from) {
                continue;
            }
            for hostname in &by.hostnames {
                for theirs in from.hostnames.iter().filter(|theirs| !theirs.falls_through) {
                    if takes(hostname, theirs) {
                        let taken = Taken {
                            by: by.name.clone(),
                            hostname: hostname.name.clone(),
                            from: from.name.clone(),
                            from_hostname: theirs.name.clone(),
                        };
                        if !found.contains(&taken) {
                            found.push(taken);
                        }
                    }
                }
            }
        }
    }
    found
}

/// Whether the claim `ours` takes hosts from the claim `theirs`: `theirs` covers every host
/// `ours` does, and is less specific, so that the router gives `ours` those hosts alone.
fn takes(ours: &Hostname, theirs: &Hostname) -> bool {
    if theirs.name == "*" {
        return ours.name != "*";
    }
    let Some(suffix) = wildcard(&theirs.name) else {
        return false;
    };
    match wildcard(&ours.name) {
        // An exact name under a wildcard.
        None if ours.name != "*" => match theirs.wildcard {
            Some(Wildcard::OneLabel) => {
                under(suffix, &ours.name)
                    && !ours.name[..ours.name.len() - suffix.len() - 1].contains('.')
            }
            _ => under(suffix, &ours.name),
        },
        // A longer wildcard under one of one label or more; under one of one label, none is.
        Some(inner) => theirs.wildcard != Some(Wildcard::OneLabel) && under(suffix, inner),
        None => false,
    }
}

/// What an HTTPRoute's rules come to: its config routes, one for each listener it is attached
/// to, and the upstreams they lead to; the rules dropped, each with why; and the backends
/// answered 500, each with why.
#[derive(Debug, Default)]
pub(crate) struct Translated {
    pub(crate) routes: Vec<Route>,
    pub(crate) upstreams: BTreeMap<String, Upstream>,
    pub(crate) dropped: Vec<(usize, String)>,
    pub(crate) invalid: Vec<(usize, String)>,
}

/// The rules of `route`, attached as `outcome` says, translated for the listeners of
/// `gateway` that serve each attachment; `refused` says why the data plane would refuse a
/// rule compiled alone with its upstreams, if it would, so that such a rule is dropped by
/// itself.
pub(crate) fn translated(
    route: &HTTPRoute,
    outcome: &RouteOutcome,
    gateway: &GatewayOutcome,
    plane: &Plane,
    cluster: &Cluster,
    refused: impl Fn(&Rule, &BTreeMap<String, Upstream>) -> Option<String>,
) -> Translated {
    let mut translated = Translated::default();
    let route_namespace = namespace(&route.metadata);
    let mut rules = Vec::new();
    for (at, rule) in route.spec.rules.iter().enumerate() {
        let mut upstreams = BTreeMap::new();
        let mut invalid = Vec::new();
        match config_rule(
            rule,
            route_namespace,
            plane,
            cluster,
            &mut upstreams,
            &mut invalid,
        ) {
            Err(why) => translated.dropped.push((at, why)),
            Ok(compiled) => match refused(&compiled, &upstreams) {
                Some(why) => translated.dropped.push((at, why)),
                None => {
                    translated
                        .invalid
                        .extend(invalid.into_iter().map(|why| (at, why)));
                    translated.upstreams.extend(upstreams);
                    rules.push(compiled);
                }
            },
        }
    }
    if rules.is_empty() {
        return translated;
    }
    for parent in &outcome.parents {
        let Ok(attachments) = &parent.attached else {
            continue;
        };
        for attachment in attachments {
            let served = gateway
                .listeners
                .iter()
                .find(|listener| listener.name == attachment.listener)
                .and_then(|listener| listener.served.as_ref().ok());
            let (Some(served), false) = (served, attachment.hostnames.is_empty()) else {
                continue;
            };
            let name = format!(
                "httproute/{}/{}/{}",
                outcome.route.namespace, outcome.route.name, attachment.listener
            );
            if translated.routes.iter().any(|known| known.name == name) {
                continue;
            }
            translated.routes.push(Route {
                name,
                listeners: served.clone(),
                hostnames: attachment.hostnames.iter().map(|h| claim(h)).collect(),
                rules: rules.clone(),
                rate_limits: Vec::new(),
            });
        }
    }
    translated
}

/// A rule as the config has it, its upstreams put in `upstreams` and its backends answered
/// 500 said in `invalid`; or why it is dropped: what translation cannot take yet.
fn config_rule(
    rule: &HTTPRouteRules,
    route_namespace: &str,
    plane: &Plane,
    cluster: &Cluster,
    upstreams: &mut BTreeMap<String, Upstream>,
    invalid: &mut Vec<String>,
) -> Result<Rule, String> {
    if rule
        .filters
        .as_ref()
        .is_some_and(|filters| !filters.is_empty())
    {
        return Err("not supported yet: filters".to_owned());
    }
    if rule.timeouts.is_some() {
        return Err("not supported yet: timeouts".to_owned());
    }
    if rule.retry.is_some() {
        return Err("not supported yet: retry".to_owned());
    }
    let matches = rule
        .matches
        .iter()
        .map(|written| {
            let path = match written.path.r#type.as_str() {
                "Exact" => PathMatch::Exact(written.path.value.clone()),
                "PathPrefix" => PathMatch::Prefix(written.path.value.clone()),
                "RegularExpression" => PathMatch::Regex(written.path.value.clone()),
                other => {
                    return Err(format!(
                        "a path match of type `{other}` is not one translation reads"
                    ));
                }
            };
            let headers = written
                .headers
                .iter()
                .flatten()
                .map(|h| predicate(&h.name, &h.r#type, &h.value, "header"))
                .collect::<Result<Vec<_>, _>>()?;
            let query = written
                .query_params
                .iter()
                .flatten()
                .map(|q| predicate(&q.name, &q.r#type, &q.value, "query parameter"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Match {
                path: Some(path),
                grpc: None,
                method: written.method.clone(),
                headers,
                query,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut backends = Vec::new();
    for written in rule.backend_refs.iter().flatten() {
        if written
            .filters
            .as_ref()
            .is_some_and(|filters| !filters.is_empty())
        {
            return Err("not supported yet: a backend's filters".to_owned());
        }
        // The schema holds a weight to 0–1,000,000.
        let weight = u32::try_from(written.weight).unwrap_or_default();
        match backend(written, route_namespace, plane, cluster) {
            Ok((name, upstream)) => {
                upstreams.insert(name.clone(), upstream);
                backends.push(Backend {
                    upstream: Some(name),
                    invalid: None,
                    weight,
                });
            }
            Err(why) => {
                invalid.push(why.clone());
                backends.push(Backend {
                    upstream: None,
                    invalid: Some(why),
                    weight,
                });
            }
        }
    }
    // A rule of no backend answers 500, as the spec has it: one invalid backend says why.
    if backends.is_empty() {
        backends.push(Backend {
            upstream: None,
            invalid: Some("the rule names no backend".to_owned()),
            weight: 1,
        });
    }
    Ok(Rule {
        matches,
        filters: Vec::new(),
        forward: Some(Forward {
            backends,
            timeouts: None,
            retry: None,
        }),
        redirect: None,
        paths: None,
        rate_limits: Vec::new(),
    })
}

/// A header's or query parameter's match, or why it is not one translation reads.
fn predicate(name: &str, kind: &str, value: &str, what: &str) -> Result<ValuePredicate, String> {
    let value = match kind {
        "Exact" => ValueMatch::Exact(value.to_owned()),
        "RegularExpression" => ValueMatch::Regex(value.to_owned()),
        other => {
            return Err(format!(
                "a {what} match of type `{other}` is not one translation reads"
            ));
        }
    };
    Ok(ValuePredicate {
        name: name.to_owned(),
        value,
    })
}

/// The upstream a backendRef leads to, by name, as an Ingress backend of the same Service
/// and port does; or why it is invalid, its share answered 500.
fn backend(
    written: &HTTPRouteRulesBackendRefs,
    route_namespace: &str,
    plane: &Plane,
    cluster: &Cluster,
) -> Result<(String, Upstream), String> {
    if !(written.group.is_empty() && written.kind == "Service") {
        return Err(format!(
            "a backend of kind {}/{} is not one translation reads",
            written.group, written.kind
        ));
    }
    if written
        .namespace
        .as_deref()
        .is_some_and(|theirs| theirs != route_namespace)
    {
        return Err(format!(
            "Service {}/{} is another namespace's, and ReferenceGrants are not read yet",
            written.namespace.as_deref().unwrap_or_default(),
            written.name
        ));
    }
    let Some(port) = written.port else {
        return Err(format!(
            "Service {route_namespace}/{} is named with no port",
            written.name
        ));
    };
    let reference = IngressBackend {
        service: Some(IngressServiceBackend {
            name: written.name.clone(),
            port: Some(ServiceBackendPort {
                number: Some(port),
                name: None,
            }),
        }),
        resource: None,
    };
    match backend_upstream(plane, cluster, route_namespace, &reference, false) {
        Ok(Ok(found)) => Ok(found),
        Ok(Err(missing)) => Err(missing),
        Err(problem) => Err(problem.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{claim, intersection, takes, within};
    use edgerush_config::{Hostname, Wildcard};

    #[test]
    fn hostnames_meet_as_the_spec_has_them() {
        let cases = [
            (None, "a.example.com", Some("a.example.com")),
            (None, "*.example.com", Some("*.example.com")),
            (
                Some("a.example.com"),
                "a.example.com",
                Some("a.example.com"),
            ),
            (Some("a.example.com"), "b.example.com", None),
            (
                Some("a.example.com"),
                "*.example.com",
                Some("a.example.com"),
            ),
            (
                Some("a.b.example.com"),
                "*.example.com",
                Some("a.b.example.com"),
            ),
            (Some("example.com"), "*.example.com", None),
            (
                Some("*.example.com"),
                "a.example.com",
                Some("a.example.com"),
            ),
            (Some("*.example.com"), "example.com", None),
            (
                Some("*.example.com"),
                "*.example.com",
                Some("*.example.com"),
            ),
            (
                Some("*.example.com"),
                "*.a.example.com",
                Some("*.a.example.com"),
            ),
            (
                Some("*.a.example.com"),
                "*.example.com",
                Some("*.a.example.com"),
            ),
            (Some("*.example.com"), "*.example.org", None),
            (Some("*.example.com"), "a.badexample.com", None),
        ];
        for (listener, route, met) in cases {
            assert_eq!(
                intersection(listener, route).as_deref(),
                met,
                "{listener:?} {route}"
            );
        }
    }

    #[test]
    fn a_hostname_is_within_a_listener_hostname_it_falls_under() {
        assert!(within("abc.foo.example.com", "abc.foo.example.com"));
        assert!(within("*.foo.example.com", "x.foo.example.com"));
        assert!(within("*.foo.example.com", "*.a.foo.example.com"));
        assert!(!within("*.foo.example.com", "*.foo.example.com.x"));
        assert!(!within("*.foo.example.com", "foo.example.com"));
        assert!(!within("abc.foo.example.com", "*.foo.example.com"));
    }

    #[test]
    fn a_claim_takes_hosts_from_a_less_specific_one_that_covers_them() {
        let ingress = |name: &str, wildcard| Hostname {
            name: name.to_owned(),
            wildcard,
            falls_through: false,
        };
        let any = ingress("*", None);
        let one = ingress("*.example.com", Some(Wildcard::OneLabel));
        assert!(takes(&claim("api.example.com"), &any));
        assert!(takes(&claim("api.example.com"), &one));
        assert!(!takes(&claim("a.b.example.com"), &one), "one label alone");
        assert!(!takes(&claim("*.a.example.com"), &one));
        assert!(takes(&claim("*.a.example.com"), &claim("*.example.com")));
        assert!(
            !takes(&claim("*.example.com"), &one),
            "equally specific: merged"
        );
        assert!(!takes(
            &claim("api.example.com"),
            &ingress("api.example.com", None)
        ));
        assert!(!takes(&any, &claim("*.example.com")));
        assert!(takes(
            &ingress("shop.example.com", None),
            &claim("*.example.com")
        ));
    }
}
