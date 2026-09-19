//! From a config as the model states it to what a data plane runs: everything is checked,
//! names are resolved to positions, and every problem is reported with its place, not only
//! the first.

use crate::backends::{UpstreamId, WeightedBackends};
use crate::route::{
    Filter, HeaderChanges, Hostname, Match, PathMatch, Route, ValueMatch, ValuePredicate, Wildcard,
};
use crate::{Config, Protocol, Rule};
use edgerush_filters::{HeaderModifier, HeaderModifierError};
use edgerush_router::{
    HeaderPredicate, HeaderPredicateError, HeaderPredicates, HostClaim, HostPattern,
    HostPatternError, PathPattern, PathPatternError, QueryPredicate, QueryPredicateError,
    QueryPredicates, RouteMatch, Router, WildcardLabels,
};
use http::Method;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

/// Which rule a request was routed to: positions in the list of routes and in the route's
/// list of rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleId {
    /// The route's position in [`Config::routes`].
    pub route: usize,
    /// The rule's position in the route.
    pub rule: usize,
}

/// A config compiled: fully resolved and immutable, what a snapshot is made of.
#[derive(Debug)]
pub struct Compiled {
    /// The listeners, in the order of their names, each with the routes that are for it.
    pub listeners: Vec<CompiledListener>,
    /// The upstreams, in the order of their names; an [`UpstreamId`] is a position here.
    pub upstreams: Vec<CompiledUpstream>,
    /// By position of the route, then of the rule. Each is shared on its own, so that a
    /// request can hold on to its rule without holding on to the whole config.
    rules: Vec<Vec<Arc<CompiledRule>>>,
}

impl Compiled {
    /// What to do with a request that was routed to `id`.
    #[must_use]
    pub fn rule(&self, id: RuleId) -> Option<&Arc<CompiledRule>> {
        self.rules.get(id.route)?.get(id.rule)
    }

    /// The upstream a rule's backend stands for.
    #[must_use]
    pub fn upstream(&self, id: UpstreamId) -> Option<&CompiledUpstream> {
        self.upstreams.get(id.0)
    }
}

/// A listener and the router for the requests that come in there.
#[derive(Debug)]
pub struct CompiledListener {
    /// Its name in the config.
    pub name: String,
    /// The address to listen on.
    pub address: SocketAddr,
    /// What is spoken there.
    pub protocol: Protocol,
    /// Finds the rule a request belongs to, among the routes that are for this listener.
    pub router: Router<RuleId>,
}

/// What a rule does with its requests.
#[derive(Debug)]
pub struct CompiledRule {
    /// Changes to the request's headers before it goes to the upstream, if there are any:
    /// a rule that asks for none carries nothing to skip over.
    pub request_headers: Option<HeaderModifier>,
    /// Changes to the response's headers before it goes to the client, if there are any.
    pub response_headers: Option<HeaderModifier>,
    /// Where they go.
    pub backends: WeightedBackends,
}

/// An upstream, with the name it had in the config for logs and metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledUpstream {
    /// Its name in the config.
    pub name: String,
    /// Where to connect.
    pub endpoints: Vec<SocketAddr>,
}

/// Compiles a config. The order of the routes, then of the rules, then of a rule's matches
/// is the last tie-breaker of precedence.
///
/// # Errors
///
/// Returns every problem found, each with its place. Nothing is compiled if there is any:
/// a data plane keeps running what it has, and a harness does not start.
pub fn compile(config: &Config) -> Result<Compiled, Vec<ConfigError>> {
    let mut errors = Vec::new();

    // A `BTreeMap` hands out its names in order, so positions are the same on every pod.
    let upstream_ids: BTreeMap<&str, UpstreamId> = config
        .upstreams
        .keys()
        .enumerate()
        .map(|(position, name)| (name.as_str(), UpstreamId(position)))
        .collect();
    let upstreams = config
        .upstreams
        .iter()
        .map(|(name, upstream)| CompiledUpstream {
            name: name.clone(),
            endpoints: upstream.endpoints.clone(),
        })
        .collect();

    // Until listeners can share a socket, two on one address cannot both be served.
    let mut addresses: BTreeMap<SocketAddr, &str> = BTreeMap::new();
    for (name, listener) in &config.listeners {
        if let Some(other) = addresses.insert(listener.address, name) {
            let problem = Problem::AddressTaken {
                address: listener.address,
                other: other.to_owned(),
            };
            errors.push(Place::listener(name).problem(problem));
        }
    }

    // The matches of every listener, by the listener's name.
    let mut matches: BTreeMap<&str, Vec<RouteMatch<RuleId>>> = config
        .listeners
        .keys()
        .map(|name| (name.as_str(), Vec::new()))
        .collect();
    let mut rules = Vec::new();
    let mut names = HashSet::new();
    for (route_at, route) in config.routes.iter().enumerate() {
        let place = Place::route(&route.name);
        if !names.insert(route.name.as_str()) {
            errors.push(place.problem(Problem::DuplicateName));
        }
        let listeners = listeners_of(route, &matches, &place, &mut errors);
        let hosts = host_claims(route, &place, &mut errors);

        let mut compiled_rules = Vec::new();
        for (rule_at, rule) in route.rules.iter().enumerate() {
            let place = Place {
                rule: Some(rule_at),
                ..place.clone()
            };
            let id = RuleId {
                route: route_at,
                rule: rule_at,
            };
            if rule.matches.is_empty() {
                errors.push(place.problem(Problem::NoMatches));
            }
            for (match_at, matching) in rule.matches.iter().enumerate() {
                let place = Place {
                    matching: Some(match_at),
                    ..place.clone()
                };
                match route_match(matching, &hosts, id) {
                    Ok(route_match) => {
                        for listener in &listeners {
                            if let Some(matches) = matches.get_mut(listener) {
                                matches.push(route_match.clone());
                            }
                        }
                    }
                    Err(problems) => {
                        errors.extend(problems.into_iter().map(|problem| place.problem(problem)));
                    }
                }
            }
            let (request_headers, response_headers) = filters(rule, &place, &mut errors);
            compiled_rules.push(Arc::new(CompiledRule {
                request_headers,
                response_headers,
                backends: backends(rule, &upstream_ids, &place, &mut errors),
            }));
        }
        rules.push(compiled_rules);
    }

    if errors.is_empty() {
        let listeners = config
            .listeners
            .iter()
            .map(|(name, listener)| CompiledListener {
                name: name.clone(),
                address: listener.address,
                protocol: listener.protocol,
                router: Router::new(matches.remove(name.as_str()).unwrap_or_default()),
            })
            .collect();
        Ok(Compiled {
            listeners,
            upstreams,
            rules,
        })
    } else {
        Err(errors)
    }
}

/// The listeners a route is for, each once, as far as they exist.
fn listeners_of<'a>(
    route: &'a Route,
    known: &BTreeMap<&str, Vec<RouteMatch<RuleId>>>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> Vec<&'a str> {
    if route.listeners.is_empty() {
        errors.push(place.problem(Problem::NoListeners));
    }
    let mut listeners: Vec<&str> = Vec::new();
    for name in &route.listeners {
        if !known.contains_key(name.as_str()) {
            errors.push(place.problem(Problem::UnknownListener(name.clone())));
        } else if listeners.contains(&name.as_str()) {
            errors.push(place.problem(Problem::ListenerTwice(name.clone())));
        } else {
            listeners.push(name);
        }
    }
    listeners
}

fn host_claims(route: &Route, place: &Place, errors: &mut Vec<ConfigError>) -> Vec<HostClaim<()>> {
    if route.hostnames.is_empty() {
        errors.push(place.problem(Problem::NoHostnames));
    }
    if route.rules.is_empty() {
        errors.push(place.problem(Problem::NoRules));
    }
    let mut hosts = Vec::new();
    for hostname in &route.hostnames {
        match host_claim(hostname) {
            Ok(claim) => hosts.push(claim),
            Err(problem) => errors.push(place.problem(problem)),
        }
    }
    hosts
}

fn host_claim(hostname: &Hostname) -> Result<HostClaim<()>, Problem> {
    let pattern = if hostname.name == "*" {
        None
    } else {
        let is_wildcard = hostname.name.starts_with('*');
        let labels = match hostname.wildcard {
            Some(Wildcard::OneLabel) => WildcardLabels::One,
            Some(Wildcard::AnyLabels) => WildcardLabels::OneOrMore,
            None if is_wildcard => {
                return Err(Problem::WildcardUnstated {
                    name: hostname.name.clone(),
                });
            }
            // An exact name: the kind has no meaning, so any will do.
            None => WildcardLabels::One,
        };
        let pattern =
            HostPattern::parse(&hostname.name, labels).map_err(|reason| Problem::Hostname {
                name: hostname.name.clone(),
                reason,
            })?;
        Some(pattern)
    };
    Ok(HostClaim {
        pattern,
        falls_through: hostname.falls_through,
        value: (),
    })
}

fn route_match(
    matching: &Match,
    hosts: &[HostClaim<()>],
    value: RuleId,
) -> Result<RouteMatch<RuleId>, Vec<Problem>> {
    let mut problems = Vec::new();

    let path = match &matching.path {
        PathMatch::Exact(path) => PathPattern::exact(path),
        PathMatch::Prefix(path) => PathPattern::prefix(path),
        PathMatch::Regex(pattern) => PathPattern::regex(pattern),
    }
    .map_err(|reason| problems.push(Problem::Path(reason)))
    .ok();

    // Any token is a method to the `http` crate; one in lower case would be a method of its
    // own that no request has.
    let method = matching.method.as_ref().and_then(|method| {
        let parsed = Method::from_bytes(method.as_bytes())
            .ok()
            .filter(|_| !method.bytes().any(|byte| byte.is_ascii_lowercase()));
        if parsed.is_none() {
            problems.push(Problem::Method(method.clone()));
        }
        parsed
    });

    let mut headers = Vec::new();
    for ValuePredicate { name, value } in &matching.headers {
        let predicate = match value {
            ValueMatch::Exact(value) => HeaderPredicate::exact(name, value),
            ValueMatch::Regex(pattern) => HeaderPredicate::regex(name, pattern),
        };
        match predicate {
            Ok(predicate) => headers.push(predicate),
            Err(reason) => problems.push(Problem::Header {
                name: name.clone(),
                reason,
            }),
        }
    }

    let mut query = Vec::new();
    for ValuePredicate { name, value } in &matching.query {
        let predicate = match value {
            ValueMatch::Exact(value) => QueryPredicate::exact(name, value),
            ValueMatch::Regex(pattern) => QueryPredicate::regex(name, pattern),
        };
        match predicate {
            Ok(predicate) => query.push(predicate),
            Err(reason) => problems.push(Problem::Query {
                name: name.clone(),
                reason,
            }),
        }
    }

    match path {
        Some(path) if problems.is_empty() => Ok(RouteMatch {
            hosts: hosts.to_vec(),
            path,
            method,
            headers: HeaderPredicates::new(headers),
            query: QueryPredicates::new(query),
            value,
        }),
        _ => Err(problems),
    }
}

/// The rule's header modifiers, for the request and for the response.
fn filters(
    rule: &Rule,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> (Option<HeaderModifier>, Option<HeaderModifier>) {
    let mut request = None;
    let mut response = None;
    for (at, filter) in rule.filters.iter().enumerate() {
        let place = Place {
            filter: Some(at),
            ..place.clone()
        };
        let (slot, changes, kind) = match filter {
            Filter::RequestHeaderModifier(changes) => {
                (&mut request, changes, "request_header_modifier")
            }
            Filter::ResponseHeaderModifier(changes) => {
                (&mut response, changes, "response_header_modifier")
            }
        };
        if slot.is_some() {
            errors.push(place.problem(Problem::FilterTwice(kind)));
        }
        match header_modifier(changes) {
            Ok(modifier) => *slot = Some(modifier),
            Err(reason) => errors.push(place.problem(Problem::HeaderModifier(reason))),
        }
    }
    let kept = |modifier: Option<HeaderModifier>| modifier.filter(|modifier| !modifier.is_empty());
    (kept(request), kept(response))
}

fn header_modifier(changes: &HeaderChanges) -> Result<HeaderModifier, HeaderModifierError> {
    fn pairs(headers: &[crate::Header]) -> impl Iterator<Item = (&str, &str)> {
        headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_str()))
    }
    HeaderModifier::new(
        pairs(&changes.set),
        pairs(&changes.add),
        changes.remove.iter().map(String::as_str),
    )
}

fn backends(
    rule: &Rule,
    upstream_ids: &BTreeMap<&str, UpstreamId>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> WeightedBackends {
    if rule.backends.is_empty() {
        errors.push(place.problem(Problem::NoBackends));
    }
    let resolved = rule
        .backends
        .iter()
        .enumerate()
        .filter_map(|(at, backend)| {
            let id = upstream_ids.get(backend.upstream.as_str()).copied();
            if id.is_none() {
                let place = Place {
                    backend: Some(at),
                    ..place.clone()
                };
                errors.push(place.problem(Problem::UnknownUpstream(backend.upstream.clone())));
            }
            Some((id?, backend.weight))
        });
    WeightedBackends::new(resolved.collect::<Vec<_>>())
}

/// A problem in a config, and where it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{place}: {problem}")]
pub struct ConfigError {
    /// Where the problem is.
    pub place: Place,
    /// What it is.
    pub problem: Problem,
}

/// A named thing in a config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    /// The listener of this name.
    Listener(String),
    /// The route of this name.
    Route(String),
}

/// A place in a config: a listener, or a route, a rule in it, a match or a backend in the
/// rule. Positions count from zero, as the lists in a file do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    /// The listener or route.
    pub object: Object,
    /// The rule's position in the route, if the problem is in a rule.
    pub rule: Option<usize>,
    /// The match's position in the rule, if the problem is in a match.
    pub matching: Option<usize>,
    /// The filter's position in the rule, if the problem is in a filter.
    pub filter: Option<usize>,
    /// The backend's position in the rule, if the problem is in a backend.
    pub backend: Option<usize>,
}

impl Place {
    fn route(name: &str) -> Self {
        Self::of(Object::Route(name.to_owned()))
    }

    fn listener(name: &str) -> Self {
        Self::of(Object::Listener(name.to_owned()))
    }

    fn of(object: Object) -> Self {
        Self {
            object,
            rule: None,
            matching: None,
            filter: None,
            backend: None,
        }
    }

    fn problem(&self, problem: Problem) -> ConfigError {
        ConfigError {
            place: self.clone(),
            problem,
        }
    }
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.object {
            Object::Listener(name) => write!(f, "listener `{name}`")?,
            Object::Route(name) => write!(f, "route `{name}`")?,
        }
        if let Some(rule) = self.rule {
            write!(f, ", rules[{rule}]")?;
        }
        if let Some(matching) = self.matching {
            write!(f, ", matches[{matching}]")?;
        }
        if let Some(filter) = self.filter {
            write!(f, ", filters[{filter}]")?;
        }
        if let Some(backend) = self.backend {
            write!(f, ", backends[{backend}]")?;
        }
        Ok(())
    }
}

/// What is wrong.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// Another listener has the same address.
    #[error("address {address} is already taken by listener `{other}`")]
    AddressTaken {
        /// The address both want.
        address: SocketAddr,
        /// The listener that has it.
        other: String,
    },
    /// Another route has the same name.
    #[error("another route has the same name")]
    DuplicateName,
    /// The route is for no listener.
    #[error("no listeners")]
    NoListeners,
    /// The route names a listener the config does not have.
    #[error("there is no listener `{0}`")]
    UnknownListener(String),
    /// The route names a listener twice.
    #[error("listener `{0}` is listed twice")]
    ListenerTwice(String),
    /// The route serves no host.
    #[error("no hostnames; every host is the name `*`")]
    NoHostnames,
    /// The route has no rules.
    #[error("no rules")]
    NoRules,
    /// The rule is for no request.
    #[error("no matches; any path is `{{ prefix: / }}`")]
    NoMatches,
    /// The rule has two filters of a kind that it may have once.
    #[error("a rule may have one `{0}`")]
    FilterTwice(&'static str),
    /// The header modifier is not valid.
    #[error("{0}")]
    HeaderModifier(HeaderModifierError),
    /// The rule sends its requests nowhere.
    #[error("no backends")]
    NoBackends,
    /// The backend names an upstream the config does not have.
    #[error("there is no upstream `{0}`")]
    UnknownUpstream(String),
    /// The hostname is not valid.
    #[error("hostname `{name}`: {reason}")]
    Hostname {
        /// The hostname as written.
        name: String,
        /// What is wrong with it.
        reason: HostPatternError,
    },
    /// A wildcard hostname that does not say what its `*` stands for.
    #[error("hostname `{name}` is a wildcard; `wildcard` must say `one_label` or `any_labels`")]
    WildcardUnstated {
        /// The hostname as written.
        name: String,
    },
    /// The path is not valid.
    #[error("path: {0}")]
    Path(PathPatternError),
    /// The method is not an HTTP method in upper case.
    #[error("method `{0}` is not an HTTP method in upper case")]
    Method(String),
    /// The header condition is not valid.
    #[error("header `{name}`: {reason}")]
    Header {
        /// The header name as written.
        name: String,
        /// What is wrong with the condition.
        reason: HeaderPredicateError,
    },
    /// The query parameter condition is not valid.
    #[error("query parameter `{name}`: {reason}")]
    Query {
        /// The parameter name as written.
        name: String,
        /// What is wrong with the condition.
        reason: QueryPredicateError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_router::RequestParts;
    use http::HeaderMap;

    fn config(yaml: &str) -> Config {
        serde_saphyr::from_str(yaml).unwrap()
    }

    fn route(compiled: &Compiled, host: &str, target: &str) -> Option<(usize, usize)> {
        route_with(compiled, "GET", host, target, &[])
    }

    fn route_on(compiled: &Compiled, listener: &str, host: &str) -> Option<(usize, usize)> {
        let listener = compiled
            .listeners
            .iter()
            .find(|l| l.name == listener)
            .unwrap();
        listener
            .router
            .route(&RequestParts {
                host,
                path: "/",
                query: "",
                method: &Method::GET,
                headers: &HeaderMap::new(),
            })
            .map(|id| (id.route, id.rule))
    }

    fn route_with(
        compiled: &Compiled,
        method: &str,
        host: &str,
        target: &str,
        fields: &[(&'static str, &'static str)],
    ) -> Option<(usize, usize)> {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(*name, value.parse().unwrap());
        }
        let web = compiled.listeners.iter().find(|l| l.name == "web").unwrap();
        web.router
            .route(&RequestParts {
                host,
                path,
                query,
                method: &method.parse().unwrap(),
                headers: &headers,
            })
            .map(|id| (id.route, id.rule))
    }

    const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
routes:
  - name: shop
    listeners: [web]
    hostnames:
      - { name: shop.example.com, falls_through: true }
      - { name: "*.shop.example.com", wildcard: any_labels, falls_through: true }
    rules:
      - matches:
          - path: { exact: /checkout }
            method: POST
          - path: { prefix: /cart }
            headers: [{ name: X-Beta, value: { exact: "on" } }]
            query: [{ name: tenant, value: { regex: "[a-z]+" } }]
        backends:
          - { upstream: checkout, weight: 9 }
          - { upstream: checkout-canary, weight: 1 }
      - matches:
          - path: { regex: "/orders/[0-9]+" }
          - path: { prefix: / }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Gateway, value: edgerush }]
            remove: [x-debug]
          - type: response_header_modifier
            add: [{ name: cache-control, value: no-store }]
        backends: [{ upstream: web, weight: 1 }]
  - name: everything-else
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        backends: [{ upstream: web, weight: 1 }]
upstreams:
  web: { endpoints: ["127.0.0.1:9000"] }
  checkout: { endpoints: ["127.0.0.1:9001", "[::1]:9001"] }
  checkout-canary: { endpoints: [] }
"#;

    #[test]
    fn a_config_compiles_into_a_router_that_finds_the_rule() {
        let compiled = compile(&config(SHOP)).unwrap();
        let shop = "shop.example.com";
        assert_eq!(
            route_with(&compiled, "POST", shop, "/checkout", &[]),
            Some((0, 0))
        );
        assert_eq!(route(&compiled, shop, "/checkout"), Some((0, 1)));
        assert_eq!(
            route_with(
                &compiled,
                "GET",
                shop,
                "/cart/1?tenant=acme",
                &[("x-beta", "on")]
            ),
            Some((0, 0))
        );
        assert_eq!(route(&compiled, shop, "/cart/1?tenant=acme"), Some((0, 1)));
        assert_eq!(route(&compiled, shop, "/orders/42"), Some((0, 1)));
        assert_eq!(
            route(&compiled, "eu.west.shop.example.com", "/x"),
            Some((0, 1))
        );
        assert_eq!(route(&compiled, "example.org", "/x"), Some((1, 0)));
    }

    #[test]
    fn a_rule_leads_to_its_upstreams_in_proportion_to_the_weights() {
        let compiled = compile(&config(SHOP)).unwrap();
        let rule = compiled.rule(RuleId { route: 0, rule: 0 }).unwrap();
        let picked: Vec<&str> = (0..10)
            .map(|point| {
                let upstream = rule.backends.pick(point).unwrap();
                compiled.upstream(upstream).unwrap().name.as_str()
            })
            .collect();
        assert_eq!(picked[..9], ["checkout"; 9]);
        assert_eq!(picked[9], "checkout-canary");

        let checkout = compiled
            .upstreams
            .iter()
            .find(|u| u.name == "checkout")
            .unwrap();
        let endpoints: Vec<String> = checkout.endpoints.iter().map(ToString::to_string).collect();
        assert_eq!(endpoints, ["127.0.0.1:9001", "[::1]:9001"]);
        // An upstream may have nowhere to connect: that is a state, not a mistake.
        let canary = compiled
            .upstreams
            .iter()
            .find(|u| u.name == "checkout-canary")
            .unwrap();
        assert!(canary.endpoints.is_empty());

        assert!(compiled.rule(RuleId { route: 0, rule: 2 }).is_none());
        assert!(compiled.rule(RuleId { route: 2, rule: 0 }).is_none());
    }

    #[test]
    fn a_rule_carries_its_header_modifiers_and_nothing_for_none() {
        let compiled = compile(&config(SHOP)).unwrap();
        let plain = compiled.rule(RuleId { route: 0, rule: 0 }).unwrap();
        assert!(plain.request_headers.is_none());
        assert!(plain.response_headers.is_none());

        let filtered = compiled.rule(RuleId { route: 0, rule: 1 }).unwrap();
        let mut request = HeaderMap::new();
        request.insert("x-debug", "1".parse().unwrap());
        request.insert("x-gateway", "spoofed".parse().unwrap());
        filtered
            .request_headers
            .as_ref()
            .unwrap()
            .apply(&mut request);
        assert_eq!(request.get("x-gateway").unwrap(), "edgerush");
        assert!(!request.contains_key("x-debug"));

        let mut response = HeaderMap::new();
        filtered
            .response_headers
            .as_ref()
            .unwrap()
            .apply(&mut response);
        assert_eq!(response.get("cache-control").unwrap(), "no-store");

        // A modifier with nothing in it is not kept.
        let empty = SHOP.replace("add: [{ name: cache-control, value: no-store }]", "add: []");
        let compiled = compile(&config(&empty)).unwrap();
        let filtered = compiled.rule(RuleId { route: 0, rule: 1 }).unwrap();
        assert!(filtered.request_headers.is_some());
        assert!(filtered.response_headers.is_none());
    }

    #[test]
    fn a_modifier_may_not_name_the_gateways_own_headers() {
        let reserved = SHOP.replace("remove: [x-debug]", "remove: [Host]").replace(
            "add: [{ name: cache-control, value: no-store }]",
            "add: [{ name: Transfer-Encoding, value: chunked }]",
        );
        let errors: Vec<String> = compile(&config(&reserved))
            .err()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            errors,
            [
                "route `shop`, rules[1], filters[0]: header `host` is the gateway's own and \
                 cannot be modified",
                "route `shop`, rules[1], filters[1]: header `transfer-encoding` is the \
                 gateway's own and cannot be modified",
            ]
        );
    }

    #[test]
    fn the_order_of_routes_is_the_last_tie_breaker() {
        let twins = r#"
listeners: { web: { address: "[::]:8080", protocol: http } }
routes:
  - name: older
    listeners: [web]
    hostnames: [{ name: a.example.com, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: /api } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: younger
    listeners: [web]
    hostnames: [{ name: a.example.com, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: /api } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(twins)).unwrap();
        assert_eq!(route(&compiled, "a.example.com", "/api/x"), Some((0, 0)));
    }

    #[test]
    fn wildcard_kind_and_fall_through_are_what_the_hostname_says() {
        let ingress_style = r#"
listeners: { web: { address: "[::]:8080", protocol: http } }
routes:
  - name: wildcard
    listeners: [web]
    hostnames: [{ name: "*.example.com", wildcard: one_label, falls_through: false }]
    rules: [{ matches: [{ path: { prefix: /shared } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: exact
    listeners: [web]
    hostnames: [{ name: a.example.com, wildcard: any_labels, falls_through: false }]
    rules: [{ matches: [{ path: { prefix: /own } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(ingress_style)).unwrap();
        assert_eq!(route(&compiled, "b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&compiled, "x.b.example.com", "/shared"), None);
        assert_eq!(route(&compiled, "a.example.com", "/own"), Some((1, 0)));
        assert_eq!(route(&compiled, "a.example.com", "/shared"), None);

        let gateway_style = ingress_style
            .replace("one_label", "any_labels")
            .replace("falls_through: false", "falls_through: true");
        let compiled = compile(&config(&gateway_style)).unwrap();
        assert_eq!(route(&compiled, "x.b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&compiled, "a.example.com", "/shared"), Some((0, 0)));
    }

    #[test]
    fn every_problem_is_reported_with_its_place() {
        let broken = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
  web-again: { address: "[::]:8080", protocol: http }
routes:
  - name: a
    listeners: []
    hostnames: []
    rules: []
  - name: a
    listeners: [web, wbe, web]
    hostnames:
      - { name: "*.example.com", falls_through: true }
      - { name: "exa mple.com", falls_through: true }
    rules:
      - matches: []
        filters:
          - { type: request_header_modifier, set: [{ name: x-a, value: "1" }] }
          - { type: response_header_modifier, set: [{ name: x-a, value: "1" }], remove: [X-A] }
          - { type: request_header_modifier, add: [{ name: "x y", value: "1" }] }
        backends: []
      - matches:
          - path: { prefix: /ok }
          - path: { exact: no-slash }
            method: get
            headers: [{ name: "x y", value: { exact: "1" } }]
            query: [{ name: "", value: { regex: "(" } }]
        backends:
          - { upstream: web, weight: 1 }
          - { upstream: wbe, weight: 1 }
upstreams:
  web: { endpoints: [] }
"#;
        let errors: Vec<String> = compile(&config(broken))
            .err()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            errors,
            [
                "listener `web-again`: address [::]:8080 is already taken by listener `web`",
                "route `a`: no listeners",
                "route `a`: no hostnames; every host is the name `*`",
                "route `a`: no rules",
                "route `a`: another route has the same name",
                "route `a`: there is no listener `wbe`",
                "route `a`: listener `web` is listed twice",
                "route `a`: hostname `*.example.com` is a wildcard; `wildcard` must say \
                 `one_label` or `any_labels`",
                "route `a`: hostname `exa mple.com`: hostname contains invalid character ' '",
                "route `a`, rules[0]: no matches; any path is `{ prefix: / }`",
                "route `a`, rules[0], filters[1]: header `x-a` is named more than once",
                "route `a`, rules[0], filters[2]: a rule may have one `request_header_modifier`",
                "route `a`, rules[0], filters[2]: `x y` is not a header name",
                "route `a`, rules[0]: no backends",
                "route `a`, rules[1], matches[1]: path: path does not start with `/`",
                "route `a`, rules[1], matches[1]: method `get` is not an HTTP method in upper \
                 case",
                "route `a`, rules[1], matches[1]: header `x y`: invalid header name",
                "route `a`, rules[1], matches[1]: query parameter ``: query parameter name is \
                 empty",
                "route `a`, rules[1], backends[1]: there is no upstream `wbe`",
            ]
        );
    }

    #[test]
    fn there_is_no_shorthand_and_nothing_unknown_is_let_through() {
        let parse = |routes: &str| {
            let yaml = format!("listeners: {{}}\nroutes: {routes}\nupstreams: {{}}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(parse("[{ name: a, listeners: [], hostnames: [], rules: [] }]").is_ok());
        // A hostname is always the full statement.
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [a.example.com], rules: [] }]").is_err()
        );
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [{ name: a.example.com }], rules: [] }]")
                .is_err()
        );
        // A match always states its path, a backend its weight.
        let rule =
            |rule: &str| format!("[{{ name: a, listeners: [], hostnames: [], rules: [{rule}] }}]");
        assert!(parse(&rule("{ matches: [], backends: [] }")).is_ok());
        assert!(parse(&rule("{ matches: [{ method: GET }], backends: [] }")).is_err());
        assert!(parse(&rule("{ matches: [], backends: [{ upstream: u }] }")).is_err());
        assert!(parse(&rule("{ matches: [] }")).is_err());
        // Misspelt keys are errors, not silence.
        assert!(
            parse("[{ name: a, listeners: [], hostnames: [], rules: [], rulez: [] }]").is_err()
        );
        assert!(parse(&rule("{ matches: [], backends: [], filter: [] }")).is_err());
        // A filter is of a kind the model knows and has no keys it does not.
        let filter = |filter: &str| {
            rule(&format!(
                "{{ matches: [], filters: [{filter}], backends: [] }}"
            ))
        };
        assert!(parse(&filter("{ type: request_header_modifier }")).is_ok());
        assert!(parse(&filter("{ type: request_header_modifier, remove: [x] }")).is_ok());
        assert!(parse(&filter("{ type: request_header_modifier, removes: [x] }")).is_err());
        assert!(parse(&filter("{ type: header_modifier }")).is_err());
        assert!(parse(&filter("{ set: [] }")).is_err());
        assert!(
            parse(&filter(
                "{ type: request_header_modifier, set: [{ name: x }] }"
            ))
            .is_err()
        );
        assert!(parse(&rule("{ matches: [{ path: { glob: /a } }], backends: [] }")).is_err());
        // An endpoint is an address, not a name.
        let upstream = |endpoint: &str| {
            let yaml = format!(
                "listeners: {{}}\nroutes: []\nupstreams: {{ u: {{ endpoints: [\"{endpoint}\"] }} }}\n"
            );
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(upstream("10.0.0.1:80").is_ok());
        assert!(upstream("localhost:80").is_err());
        assert!(upstream("10.0.0.1").is_err());

        // A listener speaks a protocol the model knows, on an address.
        let listener = |listener: &str| {
            let yaml = format!("listeners: {{ l: {listener} }}\nroutes: []\nupstreams: {{}}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(listener(r#"{ address: "[::]:80", protocol: http }"#).is_ok());
        assert!(listener(r#"{ address: "[::]:80", protocol: gopher }"#).is_err());
        assert!(listener(r#"{ address: "[::]:80" }"#).is_err());
        assert!(listener(r#"{ address: ":80", protocol: http }"#).is_err());
    }

    #[test]
    fn every_listener_has_a_router_of_the_routes_that_are_for_it() {
        let two = r#"
listeners:
  public: { address: "[::]:8080", protocol: http }
  internal: { address: "127.0.0.1:9090", protocol: http }
  idle: { address: "127.0.0.1:9091", protocol: http }
routes:
  - name: site
    listeners: [public]
    hostnames: [{ name: "*", falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: metrics
    listeners: [internal, public]
    hostnames: [{ name: metrics.internal, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], backends: [{ upstream: u, weight: 1 }] }]
upstreams: { u: { endpoints: [] } }
"#;
        let compiled = compile(&config(two)).unwrap();
        let names: Vec<&str> = compiled.listeners.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["idle", "internal", "public"]);
        assert_eq!(compiled.listeners[2].address.to_string(), "[::]:8080");
        assert_eq!(compiled.listeners[2].protocol, Protocol::Http);

        assert_eq!(route_on(&compiled, "public", "example.com"), Some((0, 0)));
        assert_eq!(
            route_on(&compiled, "public", "metrics.internal"),
            Some((1, 0))
        );
        assert_eq!(
            route_on(&compiled, "internal", "metrics.internal"),
            Some((1, 0))
        );
        assert_eq!(route_on(&compiled, "internal", "example.com"), None);
        // A listener no route is for answers nothing, which is valid.
        assert_eq!(route_on(&compiled, "idle", "example.com"), None);
    }
}
