//! From a config as the model states it to what a data plane runs: everything is checked,
//! names are resolved to positions, and every problem is reported with its place, not only
//! the first.

use crate::backends::{UpstreamId, WeightedBackends};
use crate::route::{Hostname, Match, PathMatch, Route, ValueMatch, ValuePredicate, Wildcard};
use crate::{Config, Rule};
use edgerush_router::{
    HeaderPredicate, HeaderPredicateError, HeaderPredicates, HostClaim, HostPattern,
    HostPatternError, PathPattern, PathPatternError, QueryPredicate, QueryPredicateError,
    QueryPredicates, RouteMatch, Router, WildcardLabels,
};
use http::Method;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;

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
    /// Finds the rule a request belongs to.
    pub router: Router<RuleId>,
    /// The upstreams, in the order of their names; an [`UpstreamId`] is a position here.
    pub upstreams: Vec<CompiledUpstream>,
    /// By position of the route, then of the rule.
    rules: Vec<Vec<CompiledRule>>,
}

impl Compiled {
    /// What to do with a request that was routed to `id`.
    #[must_use]
    pub fn rule(&self, id: RuleId) -> Option<&CompiledRule> {
        self.rules.get(id.route)?.get(id.rule)
    }

    /// The upstream a rule's backend stands for.
    #[must_use]
    pub fn upstream(&self, id: UpstreamId) -> Option<&CompiledUpstream> {
        self.upstreams.get(id.0)
    }
}

/// What a rule does with its requests.
#[derive(Debug)]
pub struct CompiledRule {
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

    let mut matches = Vec::new();
    let mut rules = Vec::new();
    let mut names = HashSet::new();
    for (route_at, route) in config.routes.iter().enumerate() {
        let place = Place::route(&route.name);
        if !names.insert(route.name.as_str()) {
            errors.push(place.problem(Problem::DuplicateName));
        }
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
                    Ok(route_match) => matches.push(route_match),
                    Err(problems) => {
                        errors.extend(problems.into_iter().map(|problem| place.problem(problem)));
                    }
                }
            }
            compiled_rules.push(CompiledRule {
                backends: backends(rule, &upstream_ids, &place, &mut errors),
            });
        }
        rules.push(compiled_rules);
    }

    if errors.is_empty() {
        Ok(Compiled {
            router: Router::new(matches),
            upstreams,
            rules,
        })
    } else {
        Err(errors)
    }
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

/// A place in a config: a route, a rule in it, a match or a backend in the rule. Positions
/// count from zero, as the lists in a file do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    /// The route's name.
    pub route: String,
    /// The rule's position in the route, if the problem is in a rule.
    pub rule: Option<usize>,
    /// The match's position in the rule, if the problem is in a match.
    pub matching: Option<usize>,
    /// The backend's position in the rule, if the problem is in a backend.
    pub backend: Option<usize>,
}

impl Place {
    fn route(name: &str) -> Self {
        Self {
            route: name.to_owned(),
            rule: None,
            matching: None,
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
        write!(f, "route `{}`", self.route)?;
        if let Some(rule) = self.rule {
            write!(f, ", rules[{rule}]")?;
        }
        if let Some(matching) = self.matching {
            write!(f, ", matches[{matching}]")?;
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
    /// Another route has the same name.
    #[error("another route has the same name")]
    DuplicateName,
    /// The route serves no host.
    #[error("no hostnames; every host is the name `*`")]
    NoHostnames,
    /// The route has no rules.
    #[error("no rules")]
    NoRules,
    /// The rule is for no request.
    #[error("no matches; any path is `{{ prefix: / }}`")]
    NoMatches,
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
        compiled
            .router
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
routes:
  - name: shop
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
        backends: [{ upstream: web, weight: 1 }]
  - name: everything-else
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
    fn the_order_of_routes_is_the_last_tie_breaker() {
        let twins = r#"
routes:
  - name: older
    hostnames: [{ name: a.example.com, falls_through: true }]
    rules: [{ matches: [{ path: { prefix: /api } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: younger
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
routes:
  - name: wildcard
    hostnames: [{ name: "*.example.com", wildcard: one_label, falls_through: false }]
    rules: [{ matches: [{ path: { prefix: /shared } }], backends: [{ upstream: u, weight: 1 }] }]
  - name: exact
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
routes:
  - name: a
    hostnames: []
    rules: []
  - name: a
    hostnames:
      - { name: "*.example.com", falls_through: true }
      - { name: "exa mple.com", falls_through: true }
    rules:
      - matches: []
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
                "route `a`: no hostnames; every host is the name `*`",
                "route `a`: no rules",
                "route `a`: another route has the same name",
                "route `a`: hostname `*.example.com` is a wildcard; `wildcard` must say \
                 `one_label` or `any_labels`",
                "route `a`: hostname `exa mple.com`: hostname contains invalid character ' '",
                "route `a`, rules[0]: no matches; any path is `{ prefix: / }`",
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
            let yaml = format!("routes: {routes}\nupstreams: {{}}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(parse("[{ name: a, hostnames: [], rules: [] }]").is_ok());
        // A hostname is always the full statement.
        assert!(parse("[{ name: a, hostnames: [a.example.com], rules: [] }]").is_err());
        assert!(parse("[{ name: a, hostnames: [{ name: a.example.com }], rules: [] }]").is_err());
        // A match always states its path, a backend its weight.
        let rule = |rule: &str| format!("[{{ name: a, hostnames: [], rules: [{rule}] }}]");
        assert!(parse(&rule("{ matches: [], backends: [] }")).is_ok());
        assert!(parse(&rule("{ matches: [{ method: GET }], backends: [] }")).is_err());
        assert!(parse(&rule("{ matches: [], backends: [{ upstream: u }] }")).is_err());
        assert!(parse(&rule("{ matches: [] }")).is_err());
        // Misspelt keys are errors, not silence.
        assert!(parse("[{ name: a, hostnames: [], rules: [], rulez: [] }]").is_err());
        assert!(parse(&rule("{ matches: [], backends: [], filter: [] }")).is_err());
        assert!(parse(&rule("{ matches: [{ path: { glob: /a } }], backends: [] }")).is_err());
        // An endpoint is an address, not a name.
        let upstream = |endpoint: &str| {
            let yaml =
                format!("routes: []\nupstreams: {{ u: {{ endpoints: [\"{endpoint}\"] }} }}\n");
            serde_saphyr::from_str::<Config>(&yaml).map(|_| ())
        };
        assert!(upstream("10.0.0.1:80").is_ok());
        assert!(upstream("localhost:80").is_err());
        assert!(upstream("10.0.0.1").is_err());
    }
}
