//! From routes as the model states them to a router: everything is checked, and every
//! problem is reported with its place, not only the first.

use crate::route::{Hostname, Match, PathMatch, Route, ValueMatch, ValuePredicate, Wildcard};
use edgerush_router::{
    HeaderPredicate, HeaderPredicateError, HeaderPredicates, HostClaim, HostPattern,
    HostPatternError, PathPattern, PathPatternError, QueryPredicate, QueryPredicateError,
    QueryPredicates, RouteMatch, Router, WildcardLabels,
};
use http::Method;
use std::collections::HashSet;
use std::fmt;

/// Which rule a request was routed to: positions in the list of routes and in the route's
/// list of rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleId {
    /// The route's position in the list given to [`compile_routes`].
    pub route: usize,
    /// The rule's position in the route.
    pub rule: usize,
}

/// Compiles routes into a router that yields the rule a request belongs to. The order of
/// the routes, then of the rules, then of a rule's matches is the last tie-breaker of
/// precedence.
///
/// # Errors
///
/// Returns every problem found, each with its place. Nothing is compiled if there is any:
/// a data plane keeps running what it has, and a harness does not start.
pub fn compile_routes(routes: &[Route]) -> Result<Router<RuleId>, Vec<RouteError>> {
    let mut errors = Vec::new();
    let mut matches = Vec::new();
    let mut names = HashSet::new();

    for (route_at, route) in routes.iter().enumerate() {
        let place = Place {
            route: route.name.clone(),
            rule: None,
            matching: None,
        };
        let mut report = |place: &Place, problem| {
            errors.push(RouteError {
                place: place.clone(),
                problem,
            });
        };

        if !names.insert(route.name.as_str()) {
            report(&place, Problem::DuplicateName);
        }
        if route.hostnames.is_empty() {
            report(&place, Problem::NoHostnames);
        }
        if route.rules.is_empty() {
            report(&place, Problem::NoRules);
        }
        let mut hosts = Vec::new();
        for hostname in &route.hostnames {
            match host_claim(hostname) {
                Ok(claim) => hosts.push(claim),
                Err(problem) => report(&place, problem),
            }
        }

        for (rule_at, rule) in route.rules.iter().enumerate() {
            let place = Place {
                rule: Some(rule_at),
                ..place.clone()
            };
            if rule.matches.is_empty() {
                report(&place, Problem::NoMatches);
            }
            for (match_at, matching) in rule.matches.iter().enumerate() {
                let place = Place {
                    matching: Some(match_at),
                    ..place.clone()
                };
                let value = RuleId {
                    route: route_at,
                    rule: rule_at,
                };
                match route_match(matching, &hosts, value) {
                    Ok(route_match) => matches.push(route_match),
                    Err(problems) => problems
                        .into_iter()
                        .for_each(|problem| report(&place, problem)),
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(Router::new(matches))
    } else {
        Err(errors)
    }
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
    .map_err(|source| problems.push(Problem::Path(source)))
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

/// A problem in the routes, and where it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{place}: {problem}")]
pub struct RouteError {
    /// Where the problem is.
    pub place: Place,
    /// What it is.
    pub problem: Problem,
}

/// A place in the routes: a route, a rule in it, a match in the rule. Positions count from
/// zero, as the lists in a file do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    /// The route's name.
    pub route: String,
    /// The rule's position in the route, if the problem is in a rule.
    pub rule: Option<usize>,
    /// The match's position in the rule, if the problem is in a match.
    pub matching: Option<usize>,
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
        Ok(())
    }
}

/// What is wrong with a route.
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

    fn routes(yaml: &str) -> Vec<Route> {
        serde_saphyr::from_str(yaml).unwrap()
    }

    fn route(router: &Router<RuleId>, host: &str, target: &str) -> Option<(usize, usize)> {
        route_with(router, "GET", host, target, &[])
    }

    fn route_with(
        router: &Router<RuleId>,
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
        router
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
    - matches:
        - path: { regex: "/orders/[0-9]+" }
        - path: { prefix: / }
- name: everything-else
  hostnames: [{ name: "*", falls_through: true }]
  rules:
    - matches: [{ path: { prefix: / } }]
"#;

    #[test]
    fn routes_compile_into_a_router_that_finds_the_rule() {
        let router = compile_routes(&routes(SHOP)).unwrap();
        let shop = "shop.example.com";
        assert_eq!(
            route_with(&router, "POST", shop, "/checkout", &[]),
            Some((0, 0))
        );
        assert_eq!(route(&router, shop, "/checkout"), Some((0, 1)));
        assert_eq!(
            route_with(
                &router,
                "GET",
                shop,
                "/cart/1?tenant=acme",
                &[("x-beta", "on")]
            ),
            Some((0, 0))
        );
        assert_eq!(route(&router, shop, "/cart/1?tenant=acme"), Some((0, 1)));
        assert_eq!(route(&router, shop, "/orders/42"), Some((0, 1)));
        assert_eq!(
            route(&router, "eu.west.shop.example.com", "/x"),
            Some((0, 1))
        );
        assert_eq!(route(&router, "example.org", "/x"), Some((1, 0)));
    }

    #[test]
    fn the_order_of_routes_is_the_last_tie_breaker() {
        let twins = r#"
- name: older
  hostnames: [{ name: a.example.com, falls_through: true }]
  rules: [{ matches: [{ path: { prefix: /api } }] }]
- name: younger
  hostnames: [{ name: a.example.com, falls_through: true }]
  rules: [{ matches: [{ path: { prefix: /api } }] }]
"#;
        let router = compile_routes(&routes(twins)).unwrap();
        assert_eq!(route(&router, "a.example.com", "/api/x"), Some((0, 0)));
    }

    #[test]
    fn wildcard_kind_and_fall_through_are_what_the_hostname_says() {
        let ingress_style = r#"
- name: wildcard
  hostnames: [{ name: "*.example.com", wildcard: one_label, falls_through: false }]
  rules: [{ matches: [{ path: { prefix: /shared } }] }]
- name: exact
  hostnames: [{ name: a.example.com, wildcard: any_labels, falls_through: false }]
  rules: [{ matches: [{ path: { prefix: /own } }] }]
"#;
        let router = compile_routes(&routes(ingress_style)).unwrap();
        assert_eq!(route(&router, "b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&router, "x.b.example.com", "/shared"), None);
        assert_eq!(route(&router, "a.example.com", "/own"), Some((1, 0)));
        assert_eq!(route(&router, "a.example.com", "/shared"), None);

        let gateway_style = ingress_style
            .replace("one_label", "any_labels")
            .replace("falls_through: false", "falls_through: true");
        let router = compile_routes(&routes(&gateway_style)).unwrap();
        assert_eq!(route(&router, "x.b.example.com", "/shared"), Some((0, 0)));
        assert_eq!(route(&router, "a.example.com", "/shared"), Some((0, 0)));
    }

    #[test]
    fn every_problem_is_reported_with_its_place() {
        let broken = r#"
- name: a
  hostnames: []
  rules: []
- name: a
  hostnames:
    - { name: "*.example.com", falls_through: true }
    - { name: "exa mple.com", falls_through: true }
  rules:
    - matches: []
    - matches:
        - path: { prefix: /ok }
        - path: { exact: no-slash }
          method: get
          headers: [{ name: "x y", value: { exact: "1" } }]
          query: [{ name: "", value: { regex: "(" } }]
"#;
        let errors: Vec<String> = compile_routes(&routes(broken))
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
                "route `a`, rules[1], matches[1]: path: path does not start with `/`",
                "route `a`, rules[1], matches[1]: method `get` is not an HTTP method in upper \
                 case",
                "route `a`, rules[1], matches[1]: header `x y`: invalid header name",
                "route `a`, rules[1], matches[1]: query parameter ``: query parameter name is \
                 empty",
            ]
        );
    }

    #[test]
    fn there_is_no_shorthand_and_nothing_unknown_is_let_through() {
        let parse = |yaml: &str| serde_saphyr::from_str::<Vec<Route>>(yaml).map(|_| ());
        // A hostname is always the full statement.
        assert!(parse("- { name: a, hostnames: [a.example.com], rules: [] }").is_err());
        assert!(parse("- { name: a, hostnames: [{ name: a.example.com }], rules: [] }").is_err());
        // A match always states its path.
        assert!(
            parse("- { name: a, hostnames: [], rules: [{ matches: [{ method: GET }] }] }").is_err()
        );
        // Misspelt keys are errors, not silence.
        assert!(parse("- { name: a, hostnames: [], rules: [], rulez: [] }").is_err());
        assert!(
            parse("- { name: a, hostnames: [], rules: [{ matches: [], filter: [] }] }").is_err()
        );
        assert!(
            parse("- { name: a, hostnames: [], rules: [{ matches: [{ path: { glob: /a } }] }] }")
                .is_err()
        );
        assert!(parse("- { name: a, hostnames: [], rules: [] }").is_ok());
    }
}
