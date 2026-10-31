//! HTTPRoutes attached to the data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)):
//! each `parentRef` to it fitted to the listeners it names, those that take the route's kind
//! and namespace, and whose hostnames meet the route's; the hostnames each listener is left
//! with once those of more specific listeners are taken away (listener isolation); the claim
//! of no rules each listener's hostname makes; each route's rules, a rule with what
//! translation cannot take answering 500 for all it matches, one whose matches cannot be
//! built left out; and every host a claim of one API takes from a claim of the other.

use crate::gateway::{GatewayOutcome, ListenerOutcome};
use crate::{Cluster, Object, Plane, RefProblem, RefReason, backend_upstream, namespace};
use edgerush_config::{
    Backend, Filter, Forward, Fraction, Header, HeaderChanges, Hostname, Match, Mirror, PathChange,
    PathMatch, Query, Redirect, Retry, Route, Rule, Scheme, Timeouts, Upstream, UrlRewrite,
    ValueMatch, ValuePredicate, Wildcard,
};
use edgerush_gateway_api::http_route::{
    HTTPRoute, HTTPRouteParentRefs, HTTPRouteRules, HTTPRouteRulesFiltersRequestMirror,
    HTTPRouteRulesFiltersRequestRedirect, HTTPRouteRulesRetry, HTTPRouteRulesTimeouts,
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
    /// Its generation: what its status says of.
    pub generation: i64,
    /// Each of its `parentRefs` to the data plane's Gateway, in the order it lists them.
    pub parents: Vec<ParentOutcome>,
}

/// One `parentRef` of an HTTPRoute to the data plane's Gateway, and what became of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentOutcome {
    /// The Gateway's namespace, as it writes it: none for the route's own.
    pub namespace: Option<String>,
    /// The Gateway's name.
    pub name: String,
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
                    namespace: parent.namespace.clone(),
                    name: parent.name.clone(),
                    section_name: parent.section_name.clone(),
                    port: parent.port,
                    attached: attach(route, route_namespace, parent, gateway, cluster),
                })
                .collect();
            (!parents.is_empty()).then(|| RouteOutcome {
                route: Object::of("HTTPRoute", &route.metadata),
                generation: route.metadata.generation.unwrap_or_default(),
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
    let accepted = |listener: &&ListenerOutcome| listener.accepted();
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
/// to, and the upstreams they lead to; the rules left out, each with why; the rules that
/// answer 500 for all they match, each with why; the backends answered 500, each with why;
/// and the mirrors left off, each with why.
#[derive(Debug, Default)]
pub(crate) struct Translated {
    pub(crate) routes: Vec<Route>,
    pub(crate) upstreams: BTreeMap<String, Upstream>,
    pub(crate) left_out: Vec<(usize, String)>,
    pub(crate) answered: Vec<(usize, String)>,
    pub(crate) invalid: Vec<(usize, RefProblem)>,
    pub(crate) mirrors_left_off: Vec<(usize, RefProblem)>,
    pub(crate) retries_capped: Vec<(usize, i64)>,
}

/// The rules of `route`, attached as `outcome` says, translated for the listeners of
/// `gateway` that serve each attachment; `refused` says why the data plane would refuse a
/// rule compiled alone with its upstreams, if it would, so that such a rule is left out by
/// itself. A rule with what translation cannot take answers 500 for all it matches, never
/// served with part of it ignored nor leaving its requests to another rule; one whose
/// matches cannot be built is left out (04 §2).
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
        let matches = match matches(rule) {
            Ok(matches) => matches,
            Err(why) => {
                translated.left_out.push((at, why));
                continue;
            }
        };
        let mut upstreams = BTreeMap::new();
        let mut invalid = Vec::new();
        let mut left_off = Vec::new();
        let taken = match unsupported(rule) {
            Some(why) => Err(why),
            None => filtered(
                rule,
                route_namespace,
                plane,
                cluster,
                &mut upstreams,
                &mut left_off,
            )
            .and_then(|filtered| Ok((filtered, settings(rule)?))),
        };
        if taken.is_err() {
            upstreams.clear();
            left_off.clear();
        }
        let compiled = match &taken {
            Err(why) => answering(matches.clone(), why),
            Ok((
                Filtered {
                    filters,
                    redirect: Some(redirect),
                },
                _,
            )) => Rule {
                matches: matches.clone(),
                filters: filters.clone(),
                forward: None,
                redirect: Some(redirect.clone()),
                paths: None,
                rate_limits: Vec::new(),
            },
            Ok((
                Filtered {
                    filters,
                    redirect: None,
                },
                settings,
            )) => Rule {
                matches: matches.clone(),
                filters: filters.clone(),
                forward: Some(Forward {
                    backends: backends(
                        rule,
                        route_namespace,
                        plane,
                        cluster,
                        &mut upstreams,
                        &mut invalid,
                    ),
                    timeouts: settings.timeouts.clone(),
                    retry: settings.retry.clone(),
                }),
                redirect: None,
                paths: None,
                rate_limits: Vec::new(),
            },
        };
        if let Some(why) = refused(&compiled, &upstreams) {
            // Refused for what the rule does, its matches answer 500 in its place; refused
            // for its matches, nothing could take its requests, and it is left out.
            let standing = answering(matches, &why);
            if refused(&standing, &BTreeMap::new()).is_some() {
                translated.left_out.push((at, why));
            } else {
                translated.answered.push((at, why));
                rules.push(standing);
            }
            continue;
        }
        match taken {
            Err(why) => translated.answered.push((at, why)),
            Ok((_, settings)) => {
                translated
                    .retries_capped
                    .extend(settings.capped.map(|asked| (at, asked)));
                translated
                    .invalid
                    .extend(invalid.into_iter().map(|why| (at, why)));
                translated
                    .mirrors_left_off
                    .extend(left_off.into_iter().map(|why| (at, why)));
            }
        }
        translated.upstreams.extend(upstreams);
        rules.push(compiled);
    }
    if rules.is_empty() {
        return translated;
    }
    for parent in &outcome.parents {
        let Ok(attachments) = &parent.attached else {
            continue;
        };
        for attachment in attachments {
            let Some(listener) = gateway
                .listeners
                .iter()
                .find(|listener| listener.name == attachment.listener)
            else {
                continue;
            };
            let (Ok(served), false) = (&listener.served, attachment.hostnames.is_empty()) else {
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
                rules: rules
                    .iter()
                    .map(|rule| on_port(rule, listener.port))
                    .collect(),
                rate_limits: Vec::new(),
            });
        }
    }
    translated
}

/// A rule that answers 500 for every request its matches take, saying why.
fn answering(matches: Vec<Match>, why: &str) -> Rule {
    Rule {
        matches,
        filters: Vec::new(),
        forward: Some(Forward {
            backends: vec![Backend {
                upstream: None,
                invalid: Some(why.to_owned()),
                weight: 1,
            }],
            timeouts: None,
            retry: None,
        }),
        redirect: None,
        paths: None,
        rate_limits: Vec::new(),
    }
}

/// `rule` on a listener of `port`: a redirect that states neither scheme nor port sends the
/// client to the listener's port, as Gateway API has it, stated where it is not the
/// listener's scheme's default ([18 §2](../../../docs/18-redirects-and-rewrites.md)).
fn on_port(rule: &Rule, port: u16) -> Rule {
    let mut rule = rule.clone();
    if let Some(redirect) = &mut rule.redirect {
        // Plain HTTP listeners alone are served so far.
        if redirect.scheme.is_none() && redirect.port.is_none() && port != 80 {
            redirect.port = Some(port);
        }
    }
    rule
}

/// A rule's matches as the config has them, or why they cannot be built: a match of a type
/// translation does not read.
fn matches(rule: &HTTPRouteRules) -> Result<Vec<Match>, String> {
    rule.matches
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
        .collect()
}

/// What of a rule, beyond its matches, filters and settings, translation cannot take yet, if
/// anything.
fn unsupported(rule: &HTTPRouteRules) -> Option<String> {
    rule.backend_refs
        .iter()
        .flatten()
        .any(|written| {
            written
                .filters
                .as_ref()
                .is_some_and(|filters| !filters.is_empty())
        })
        .then(|| "not supported yet: a backend's filters".to_owned())
}

/// A rule's filters as the config has them: those done to a request or its answer, in the
/// order written, and a redirect.
#[derive(Debug)]
struct Filtered {
    filters: Vec<Filter>,
    redirect: Option<Redirect>,
}

/// A rule's filters as the config has them, the upstreams its mirrors copy to put in
/// `upstreams` and the mirrors whose backend is not there or not permitted left off and said
/// in `left_off`, as the spec has them not configured; or why translation cannot take them.
/// A request's header changes beside a redirect are left out, as nothing goes upstream for
/// them to change and nothing a client sees differs; a mirror beside one cannot be, as its
/// upstream would be sent nothing ([18 §2](../../../docs/18-redirects-and-rewrites.md)).
fn filtered(
    rule: &HTTPRouteRules,
    route_namespace: &str,
    plane: &Plane,
    cluster: &Cluster,
    upstreams: &mut BTreeMap<String, Upstream>,
    left_off: &mut Vec<RefProblem>,
) -> Result<Filtered, String> {
    let mut filters = Vec::new();
    let mut redirect = None;
    for written in rule.filters.iter().flatten() {
        let missing = || format!("a {} filter of no settings", written.r#type);
        match written.r#type.as_str() {
            "RequestHeaderModifier" => {
                let changes = written
                    .request_header_modifier
                    .as_ref()
                    .ok_or_else(missing)?;
                filters.push(Filter::RequestHeaderModifier(header_changes(
                    changes.set.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.add.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.remove.iter().flatten(),
                )));
            }
            "ResponseHeaderModifier" => {
                let changes = written
                    .response_header_modifier
                    .as_ref()
                    .ok_or_else(missing)?;
                filters.push(Filter::ResponseHeaderModifier(header_changes(
                    changes.set.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.add.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.remove.iter().flatten(),
                )));
            }
            "RequestRedirect" => {
                let written = written.request_redirect.as_ref().ok_or_else(missing)?;
                redirect = Some(redirected(written)?);
            }
            "URLRewrite" => {
                let written = written.url_rewrite.as_ref().ok_or_else(missing)?;
                let path = written
                    .path
                    .as_ref()
                    .map(|path| {
                        path_change(
                            &path.r#type,
                            path.replace_full_path.as_deref(),
                            path.replace_prefix_match.as_deref(),
                        )
                    })
                    .transpose()?;
                filters.push(Filter::UrlRewrite(UrlRewrite {
                    host: written.hostname.clone(),
                    path,
                }));
            }
            "RequestMirror" => {
                let written = written.request_mirror.as_ref().ok_or_else(missing)?;
                let fraction = mirrored(written)?;
                let to = &written.backend_ref;
                let reference = Reference {
                    group: &to.group,
                    kind: &to.kind,
                    name: &to.name,
                    namespace: to.namespace.as_deref(),
                    port: to.port,
                };
                match backend(&reference, route_namespace, plane, cluster) {
                    Ok((name, upstream)) => {
                        upstreams.insert(name.clone(), upstream);
                        filters.push(Filter::RequestMirror(Mirror {
                            upstream: name,
                            fraction,
                        }));
                    }
                    Err(why) => left_off.push(why),
                }
            }
            "CORS" => return Err("not supported yet: CORS".to_owned()),
            "ExtensionRef" => {
                let extension = written.extension_ref.as_ref().ok_or_else(missing)?;
                return Err(format!(
                    "a filter of {}/{} {} is not one EdgeRush has",
                    extension.group, extension.kind, extension.name
                ));
            }
            other => {
                return Err(format!(
                    "a filter of type `{other}` is not one translation reads"
                ));
            }
        }
    }
    if redirect.is_some() {
        if filters
            .iter()
            .any(|filter| matches!(filter, Filter::RequestMirror(_)))
        {
            return Err(
                "a mirror beside a redirect, which sends nothing upstream to copy".to_owned(),
            );
        }
        filters.retain(|filter| !matches!(filter, Filter::RequestHeaderModifier(_)));
    }
    Ok(Filtered { filters, redirect })
}

/// The share of requests a mirror copies: its percent, its fraction, or every one.
fn mirrored(written: &HTTPRouteRulesFiltersRequestMirror) -> Result<Fraction, String> {
    let share = |numerator: i32, denominator: i32| match (
        u32::try_from(numerator),
        u32::try_from(denominator),
    ) {
        (Ok(numerator), Ok(denominator)) => Ok(Fraction {
            numerator,
            denominator,
        }),
        _ => Err(format!("a mirror of {numerator} in {denominator}")),
    };
    match (written.percent, &written.fraction) {
        (Some(percent), _) => share(percent, 100),
        (None, Some(fraction)) => share(fraction.numerator, fraction.denominator),
        (None, None) => share(1, 1),
    }
}

/// Header changes as the config has them.
fn header_changes<'a>(
    set: impl Iterator<Item = (&'a String, &'a String)>,
    add: impl Iterator<Item = (&'a String, &'a String)>,
    remove: impl Iterator<Item = &'a String>,
) -> HeaderChanges {
    let header = |(name, value): (&String, &String)| Header {
        name: name.clone(),
        value: value.clone(),
    };
    HeaderChanges {
        set: set.map(header).collect(),
        add: add.map(header).collect(),
        remove: remove.cloned().collect(),
    }
}

/// A redirect as the config has it: the status written, 302 by the schema's default, and
/// the query kept ([18 §2](../../../docs/18-redirects-and-rewrites.md)).
fn redirected(written: &HTTPRouteRulesFiltersRequestRedirect) -> Result<Redirect, String> {
    let status = u16::try_from(written.status_code)
        .map_err(|_| format!("a redirect of status {}", written.status_code))?;
    let scheme = match written.scheme.as_deref() {
        None => None,
        Some("http") => Some(Scheme::Http),
        Some("https") => Some(Scheme::Https),
        Some(other) => {
            return Err(format!(
                "a redirect to scheme `{other}` is not one translation reads"
            ));
        }
    };
    let port = written
        .port
        .map(|port| u16::try_from(port).map_err(|_| format!("a redirect to port {port}")))
        .transpose()?;
    let path = written
        .path
        .as_ref()
        .map(|path| {
            path_change(
                &path.r#type,
                path.replace_full_path.as_deref(),
                path.replace_prefix_match.as_deref(),
            )
        })
        .transpose()?;
    Ok(Redirect {
        status,
        scheme,
        host: written.hostname.clone(),
        port,
        path,
        query: Query::Keep,
    })
}

/// A path change as the config has it, of its type and the replacement that type names.
fn path_change(kind: &str, full: Option<&str>, prefix: Option<&str>) -> Result<PathChange, String> {
    match (kind, full, prefix) {
        ("ReplaceFullPath", Some(full), _) => Ok(PathChange::ReplaceFull(full.to_owned())),
        ("ReplacePrefixMatch", _, Some(prefix)) => Ok(PathChange::ReplacePrefix(prefix.to_owned())),
        (other, ..) => Err(format!(
            "a path change of type `{other}` with no replacement of that type"
        )),
    }
}

/// A rule's timeouts and retry as the config has them, and the attempts its retry asked for
/// where they were more than the data plane makes.
#[derive(Debug)]
struct Settings {
    timeouts: Option<Timeouts>,
    retry: Option<Retry>,
    capped: Option<i64>,
}

/// The backoff before a first retry where a retry states none: Envoy's.
const BACKOFF_MS: u64 = 25;

/// How many times its base a backoff may double to: Envoy's, which Envoy Gateway leaves.
const BACKOFF_GROWTH: u64 = 10;

/// A rule's timeouts and retry as the config has them, or why translation cannot take them.
fn settings(rule: &HTTPRouteRules) -> Result<Settings, String> {
    let timeouts = rule.timeouts.as_ref().map(timeouts).transpose()?.flatten();
    let (retry, capped) = match &rule.retry {
        Some(written) => retry(written, timeouts.as_ref())?,
        None => (None, None),
    };
    Ok(Settings {
        timeouts,
        retry,
        capped,
    })
}

/// A rule's timeouts as the config has them: none where it states neither.
fn timeouts(written: &HTTPRouteRulesTimeouts) -> Result<Option<Timeouts>, String> {
    let request_ms = written.request.as_deref().map(milliseconds).transpose()?;
    let backend_request_ms = written
        .backend_request
        .as_deref()
        .map(milliseconds)
        .transpose()?;
    Ok(
        (request_ms.is_some() || backend_request_ms.is_some()).then_some(Timeouts {
            request_ms,
            backend_request_ms,
            tunnel_idle_ms: None,
        }),
    )
}

/// A rule's retry as the config has it, and the attempts it asked for where they were more
/// than the data plane makes, which are taken as its most (04 §2). None for no attempts. A
/// try that runs out of its own clock is sent again, as GEP-1731 has it, where it has one:
/// a `backendRequest` above 0, or the data plane's fixed clocks where the rule states no
/// timeout; with neither, the retry is for tries that could not connect and the statuses
/// it names.
fn retry(
    written: &HTTPRouteRulesRetry,
    timeouts: Option<&Timeouts>,
) -> Result<(Option<Retry>, Option<i64>), String> {
    let Ok(asked) = u32::try_from(written.attempts) else {
        return Err(format!("a retry of {} attempts", written.attempts));
    };
    if asked == 0 {
        return Ok((None, None));
    }
    let most = edgerush_config::MOST_ATTEMPTS;
    let capped = (asked > most).then_some(written.attempts);
    let http_statuses = written
        .codes
        .iter()
        .flatten()
        .map(|code| u16::try_from(*code).map_err(|_| format!("a retry on status {code}")))
        .collect::<Result<Vec<_>, _>>()?;
    // A wait of none at all is the data plane's least.
    let backoff_base_ms = written
        .backoff
        .as_deref()
        .map(milliseconds)
        .transpose()?
        .unwrap_or(BACKOFF_MS)
        .max(1);
    let on_timeout = timeouts.is_none_or(|timeouts| {
        timeouts
            .backend_request_ms
            .is_some_and(|backend_request| backend_request > 0)
    });
    Ok((
        Some(Retry {
            attempts: asked.min(most),
            http_statuses,
            grpc_statuses: Vec::new(),
            on_timeout,
            backoff_base_ms,
            backoff_max_ms: backoff_base_ms.saturating_mul(BACKOFF_GROWTH),
        }),
        capped,
    ))
}

/// A Gateway API duration in milliseconds (GEP-2257): one to four parts, each a number of
/// one to five digits and its unit, `h`, `m`, `s` or `ms`, `1h2m3s4ms`; or why it is not
/// one.
fn milliseconds(written: &str) -> Result<u64, String> {
    let bad = || format!("a duration of `{written}` is not one translation reads");
    let mut total: u64 = 0;
    let mut rest = written;
    let mut parts = 0;
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
        if digits == 0 || digits > 5 {
            return Err(bad());
        }
        let (number, after) = rest.split_at(digits);
        let number: u64 = number.parse().map_err(|_| bad())?;
        let (unit, after) = if let Some(after) = after.strip_prefix("ms") {
            (1, after)
        } else if let Some(after) = after.strip_prefix('h') {
            (3_600_000, after)
        } else if let Some(after) = after.strip_prefix('m') {
            (60_000, after)
        } else if let Some(after) = after.strip_prefix('s') {
            (1_000, after)
        } else {
            return Err(bad());
        };
        // Five digits of hours each, four parts at most: far from u64's end.
        total += number * unit;
        parts += 1;
        rest = after;
    }
    if parts == 0 || parts > 4 {
        return Err(bad());
    }
    Ok(total)
}

/// A rule's backends as the config has them, their upstreams put in `upstreams` and those
/// answered 500 said in `invalid`.
fn backends(
    rule: &HTTPRouteRules,
    route_namespace: &str,
    plane: &Plane,
    cluster: &Cluster,
    upstreams: &mut BTreeMap<String, Upstream>,
    invalid: &mut Vec<RefProblem>,
) -> Vec<Backend> {
    let mut backends = Vec::new();
    for written in rule.backend_refs.iter().flatten() {
        // The schema holds a weight to 0–1,000,000.
        let weight = u32::try_from(written.weight).unwrap_or_default();
        let reference = Reference {
            group: &written.group,
            kind: &written.kind,
            name: &written.name,
            namespace: written.namespace.as_deref(),
            port: written.port,
        };
        match backend(&reference, route_namespace, plane, cluster) {
            Ok((name, upstream)) => {
                upstreams.insert(name.clone(), upstream);
                backends.push(Backend {
                    upstream: Some(name),
                    invalid: None,
                    weight,
                });
            }
            Err(problem) => {
                backends.push(Backend {
                    upstream: None,
                    invalid: Some(problem.why.clone()),
                    weight,
                });
                invalid.push(problem);
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
    backends
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

/// A backend a route names: for a request to go to, or a mirror to copy it to.
struct Reference<'a> {
    group: &'a str,
    kind: &'a str,
    name: &'a str,
    namespace: Option<&'a str>,
    port: Option<i32>,
}

/// The upstream a backend leads to, by name, as an Ingress backend of the same Service and
/// port does; or why it is invalid: its share answered 500, or a mirror to it left off. A
/// Service of another namespace is reached only where a ReferenceGrant there allows it.
fn backend(
    written: &Reference<'_>,
    route_namespace: &str,
    plane: &Plane,
    cluster: &Cluster,
) -> Result<(String, Upstream), RefProblem> {
    if !(written.group.is_empty() && written.kind == "Service") {
        return Err(RefProblem {
            reason: RefReason::InvalidKind,
            why: format!(
                "a backend of kind {}/{} is not one translation reads",
                written.group, written.kind
            ),
        });
    }
    let namespace = written.namespace.unwrap_or(route_namespace);
    reachable(
        cluster,
        ("HTTPRoute", route_namespace),
        ("Service", namespace, written.name),
    )
    .map_err(|why| RefProblem {
        reason: RefReason::NotPermitted,
        why,
    })?;
    let Some(port) = written.port else {
        return Err(RefProblem {
            reason: RefReason::NotFound,
            why: format!("Service {namespace}/{} is named with no port", written.name),
        });
    };
    let reference = IngressBackend {
        service: Some(IngressServiceBackend {
            name: written.name.to_owned(),
            port: Some(ServiceBackendPort {
                number: Some(port),
                name: None,
            }),
        }),
        resource: None,
    };
    match backend_upstream(plane, cluster, namespace, &reference, false) {
        Ok(Ok(found)) => Ok(found),
        Ok(Err(missing)) => Err(missing),
        Err(problem) => Err(RefProblem {
            reason: RefReason::Unsupported,
            why: problem.to_string(),
        }),
    }
}

/// Whether an object of the Gateway API's kind and namespace `from` may reach the core
/// object of the kind, namespace and name `to` says, or why not: one of its own namespace
/// always; another's only where a ReferenceGrant there allows it, and never one of a
/// namespace the control plane does not watch, where no grant can be seen, said as such
/// rather than as no grant there (04 §9).
pub(crate) fn reachable(
    cluster: &Cluster,
    (from_kind, from): (&str, &str),
    (kind, namespace, name): (&str, &str, &str),
) -> Result<(), String> {
    if namespace == from {
        return Ok(());
    }
    if cluster
        .watched
        .as_ref()
        .is_some_and(|watched| !watched.iter().any(|ours| ours == namespace))
    {
        return Err(format!(
            "{kind} {namespace}/{name} is in {namespace}, which the control plane does not \
             watch, so no ReferenceGrant there can be seen"
        ));
    }
    if granted(cluster, (from_kind, from), (kind, namespace, name)) {
        Ok(())
    } else {
        Err(format!(
            "{kind} {namespace}/{name} is another namespace's, and no ReferenceGrant there \
             lets {from_kind}s of {from} reach it"
        ))
    }
}

/// Whether a ReferenceGrant in the namespace of `to` lets objects of the Gateway API's kind
/// and namespace `from` reach its core object of the kind and name `to` says: one whose
/// `from` names them and whose `to` names that object, or every object of its kind there.
/// Entries of each list are either-or, as the spec has it.
fn granted(
    cluster: &Cluster,
    (from_kind, from): (&str, &str),
    (kind, namespace, name): (&str, &str, &str),
) -> bool {
    cluster
        .reference_grants
        .iter()
        .filter(|grant| grant.metadata.namespace.as_deref() == Some(namespace))
        .any(|grant| {
            grant.spec.from.iter().any(|theirs| {
                theirs.group == GROUP && theirs.kind == from_kind && theirs.namespace == from
            }) && grant.spec.to.iter().any(|ours| {
                ours.group.is_empty()
                    && ours.kind == kind
                    && ours.name.as_deref().is_none_or(|theirs| theirs == name)
            })
        })
}

#[cfg(test)]
mod tests {
    use super::{claim, intersection, milliseconds, takes, within};
    use edgerush_config::{Hostname, Wildcard};

    #[test]
    fn durations_are_read_as_gep_2257_has_them() {
        for (written, ms) in [
            ("500ms", 500),
            ("0s", 0),
            ("10s", 10_000),
            ("1m", 60_000),
            ("2h", 7_200_000),
            ("1h2m3s4ms", 3_723_004),
            ("1s500ms", 1_500),
            ("99999h", 359_996_400_000),
        ] {
            assert_eq!(milliseconds(written), Ok(ms), "{written}");
        }
        for written in [
            "",
            "10",
            "s",
            "1x",
            "1.5s",
            "-1s",
            "100000s",
            "1h1h1h1h1h",
            " 1s",
            "1S",
        ] {
            assert!(milliseconds(written).is_err(), "{written}");
        }
    }

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
