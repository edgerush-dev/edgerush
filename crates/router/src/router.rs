//! The router: the stages joined, and the whole of Gateway API's precedence.
//!
//! Every way a request can reach a route rule — one of the rule's matches, under one of the
//! route's hostnames — is a [`RouteMatch`]. For a request, the one that serves is the first
//! in this order that the request satisfies in full:
//!
//! 1. the most specific host ([`HostIndex`]: groups of equally specific
//!    claims, the next group only when no match of the one before serves);
//! 2. the best path ([`PathIndex`]: exact, then regex, then the longest
//!    prefix);
//! 3. a method predicate before none;
//! 4. the most header predicates;
//! 5. the most query parameter predicates;
//! 6. the order the matches were given in, which is where the caller puts the rest of the
//!    spec's list: the older route, then the name, then the position of the rule and of the
//!    match within it.
//!
//! All of this is laid out when the router is built. A request walks its host's groups and
//! checks predicates of candidates in order; it never sorts and never allocates.

use crate::{
    Fields, HeaderPredicates, HostClaim, HostIndex, PathIndex, PathPattern, QueryPredicates,
};
use http::{HeaderMap, Method};
use std::cmp::Reverse;

/// One way to reach a route rule.
#[derive(Debug, Clone)]
pub struct RouteMatch<T> {
    /// The hosts this match serves, with whether each claim falls through onto more
    /// specific hosts. A claim with no pattern serves every host; without any claim the
    /// match is unreachable.
    pub hosts: Vec<HostClaim<()>>,
    /// The path the request must have.
    pub path: PathPattern,
    /// The method the request must have, if it matters.
    pub method: Option<Method>,
    /// What the request's headers must satisfy.
    pub headers: HeaderPredicates,
    /// What the request's query must satisfy.
    pub query: QueryPredicates,
    /// What routing a request here yields: the rule, or a handle to it.
    pub value: T,
}

/// What routing looks at in a request.
///
/// The headers can be held by anything that gives [`Fields`]; a map is the default.
#[derive(Debug)]
pub struct RequestParts<'a, F: Fields + ?Sized = HeaderMap> {
    /// The bare hostname: no port, no trailing dot.
    pub host: &'a str,
    /// The normalised path ([`normalise_path`](crate::normalise_path)).
    pub path: &'a str,
    /// The query string as it came, without its `?`; empty if there is none.
    pub query: &'a str,
    /// The request method.
    pub method: &'a Method,
    /// The request headers.
    pub headers: &'a F,
}

// By hand: a derive would ask that the headers themselves be `Clone` and `Copy`.
impl<F: Fields + ?Sized> Clone for RequestParts<'_, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F: Fields + ?Sized> Copy for RequestParts<'_, F> {}

/// An immutable router, built once per config snapshot.
#[derive(Debug)]
pub struct Router<T> {
    /// Per group of equally specific host claims, the paths of its members; the values are
    /// positions in `entries`.
    hosts: HostIndex<PathIndex<usize>>,
    entries: Vec<Entry<T>>,
}

/// What is left to check of a match once host and path have led to it.
#[derive(Debug)]
struct Entry<T> {
    method: Option<Method>,
    headers: HeaderPredicates,
    query: QueryPredicates,
    value: T,
}

impl<T> Entry<T> {
    fn serves<F: Fields + ?Sized>(&self, request: &RequestParts<'_, F>) -> bool {
        self.method
            .as_ref()
            .is_none_or(|method| method == request.method)
            && self.headers.matches(request.headers)
            && self.query.matches(request.query)
    }
}

impl<T> Router<T> {
    /// Builds the router. The order of `matches` is the last tie-breaker.
    pub fn new(matches: impl IntoIterator<Item = RouteMatch<T>>) -> Self {
        let mut claims = Vec::new();
        let mut paths = Vec::new();
        let mut entries = Vec::new();
        for (position, route_match) in matches.into_iter().enumerate() {
            claims.extend(route_match.hosts.into_iter().map(|claim| HostClaim {
                pattern: claim.pattern,
                falls_through: claim.falls_through,
                value: position,
            }));
            paths.push(route_match.path);
            entries.push(Entry {
                method: route_match.method,
                headers: route_match.headers,
                query: route_match.query,
                value: route_match.value,
            });
        }

        // Within a group the path decides first, and the path index keeps members with the
        // same path in the order it is given them: so give it the order of what comes
        // after the path. A match that claims a host twice over is a member only once.
        let rank = |position: &usize| {
            let entry = entries.get(*position);
            (
                Reverse(entry.is_some_and(|entry| entry.method.is_some())),
                Reverse(entry.map(|entry| entry.headers.len())),
                Reverse(entry.map(|entry| entry.query.len())),
                *position,
            )
        };
        let hosts = HostIndex::new(claims, |mut members: Vec<usize>| {
            members.sort_by_key(rank);
            members.dedup();
            PathIndex::new(
                members
                    .into_iter()
                    .filter_map(|position| Some((paths.get(position)?.clone(), position))),
            )
        });
        Self { hosts, entries }
    }

    /// The value of the match that serves the request, or `None` if nothing does — which is
    /// a 404. Never allocates, except as the predicates say they do.
    #[must_use]
    pub fn route<F: Fields + ?Sized>(&self, request: &RequestParts<'_, F>) -> Option<&T> {
        self.hosts
            .lookup(request.host)
            .flat_map(|paths| paths.lookup(request.path))
            .filter_map(|&position| self.entries.get(position))
            .find(|entry| entry.serves(request))
            .map(|entry| &entry.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WildcardLabels;
    use crate::reference::{self, PathKind, RequestSpec, RouteSpec};
    use crate::strategies::{compile, get, header_map, on, pairs, request_spec, route_spec};
    use proptest::prelude::*;

    /// Routes the request both ways and checks that they agree, so that every example below
    /// is also an example for the reference.
    fn route(routes: &[RouteSpec], request: &RequestSpec) -> Option<usize> {
        let matches: Vec<RouteMatch<usize>> = routes
            .iter()
            .enumerate()
            .map(|(value, spec)| compile(spec, value))
            .collect();
        let expected = reference::route(routes, request, |position| {
            matches[position].path.matches(&request.path)
        });

        let method: Method = request.method.parse().unwrap();
        let headers = header_map(&request.headers);
        let routed = Router::new(matches)
            .route(&RequestParts {
                host: &request.host,
                path: &request.path,
                query: &request.query,
                method: &method,
                headers: &headers,
            })
            .copied();
        assert_eq!(routed, expected, "router and reference disagree");
        routed
    }

    #[test]
    fn nothing_that_matches_is_nothing_routed() {
        assert_eq!(route(&[], &get("example.com", "/")), None);
        let routes = [on("example.com", PathKind::Prefix, "/api")];
        assert_eq!(route(&routes, &get("example.com", "/api/users")), Some(0));
        assert_eq!(route(&routes, &get("example.com", "/other")), None);
        assert_eq!(route(&routes, &get("example.org", "/api/users")), None);
    }

    #[test]
    fn host_decides_before_path() {
        // The exact host's catch-all beats the wildcard host's exact path.
        let routes = [
            on("*.example.com", PathKind::Exact, "/login"),
            on("a.example.com", PathKind::Prefix, "/"),
        ];
        assert_eq!(route(&routes, &get("a.example.com", "/login")), Some(1));
        assert_eq!(route(&routes, &get("b.example.com", "/login")), Some(0));
    }

    #[test]
    fn less_specific_host_serves_what_the_more_specific_one_does_not() {
        let mut routes = [
            on("*.example.com", PathKind::Prefix, "/shared"),
            on("a.example.com", PathKind::Prefix, "/own"),
        ];
        assert_eq!(route(&routes, &get("a.example.com", "/own/x")), Some(1));
        assert_eq!(route(&routes, &get("a.example.com", "/shared/x")), Some(0));
        // Unless the less specific claim does not fall through, as with Ingress.
        routes[0].hosts[0].1 = false;
        assert_eq!(route(&routes, &get("a.example.com", "/shared/x")), None);
        assert_eq!(route(&routes, &get("b.example.com", "/shared/x")), Some(0));
    }

    #[test]
    fn path_decides_before_predicates() {
        let mut exact = on("example.com", PathKind::Exact, "/users/42");
        let regex = on("example.com", PathKind::Regex, "/users/[0-9]+");
        let mut long = on("example.com", PathKind::Prefix, "/users/42");
        let mut short = on("example.com", PathKind::Prefix, "/users");
        short.method = Some("GET".to_owned());
        short.headers = pairs(&[("x-a", "1"), ("x-b", "2")]);
        long.headers = pairs(&[("x-a", "1")]);
        let mut request = get("example.com", "/users/42");
        request.headers = pairs(&[("x-a", "1"), ("x-b", "2")]);

        let routes = [short.clone(), long.clone(), regex.clone(), exact.clone()];
        assert_eq!(route(&routes, &request), Some(3));
        assert_eq!(route(&routes[..3], &request), Some(2));
        assert_eq!(route(&routes[..2], &request), Some(1));
        assert_eq!(route(&routes[..1], &request), Some(0));

        // A better path that the request does not satisfy in full does not serve it.
        exact.query = pairs(&[("debug", "1")]);
        assert_eq!(route(&[short, long, regex, exact], &request), Some(2));
    }

    /// GRPCRoute: a service's characters before a method's, whatever the order given, and a
    /// method's before a rule that matches every call — headers come after both.
    #[test]
    fn a_grpc_service_outranks_a_method_and_a_method_outranks_the_root() {
        let service = on("example.com", PathKind::Prefix, "/pkg.Svc");
        let method = on("example.com", PathKind::GrpcMethod, "Do");
        let mut root = on("example.com", PathKind::Prefix, "/");
        root.headers = pairs(&[("x-a", "1")]);
        let mut call = get("example.com", "/pkg.Svc/Do");
        call.headers = pairs(&[("x-a", "1")]);
        let mut elsewhere = call.clone();
        elsewhere.path = "/other.Svc/Do".to_owned();

        let routes = [root.clone(), method.clone(), service.clone()];
        assert_eq!(route(&routes, &call), Some(2));
        assert_eq!(route(&routes, &elsewhere), Some(1));
        let routes = [service, method, root];
        assert_eq!(route(&routes, &call), Some(0));
        assert_eq!(route(&routes, &elsewhere), Some(1));
    }

    #[test]
    fn among_equal_paths_method_then_header_count_then_query_count_then_order() {
        let base = on("example.com", PathKind::Prefix, "/api");
        let with = |method: bool, headers: usize, query: usize| {
            let mut spec = base.clone();
            spec.method = method.then(|| "GET".to_owned());
            spec.headers = pairs(&[("x-a", "1"), ("x-b", "2")][..headers]);
            spec.query = pairs(&[("p", "1"), ("q", "2")][..query]);
            spec
        };
        let mut request = get("example.com", "/api/users");
        request.headers = pairs(&[("x-a", "1"), ("x-b", "2")]);
        request.query = "p=1&q=2".to_owned();

        // Each line is beaten by the one after it, whatever the order given.
        let ladder = [
            with(false, 0, 0),
            with(false, 0, 1),
            with(false, 0, 2),
            with(false, 1, 0),
            with(false, 2, 0),
            with(true, 0, 0),
            with(true, 1, 2),
            with(true, 2, 0),
        ];
        for top in 1..=ladder.len() {
            assert_eq!(route(&ladder[..top], &request), Some(top - 1), "{top}");
        }
        // Equal in everything: the first given serves.
        let twins = [with(true, 1, 1), with(true, 1, 1)];
        assert_eq!(route(&twins, &request), Some(0));
    }

    #[test]
    fn method_predicate_must_hold() {
        let mut post = on("example.com", PathKind::Prefix, "/");
        post.method = Some("POST".to_owned());
        let any = on("example.com", PathKind::Prefix, "/");
        assert_eq!(
            route(&[post.clone(), any], &get("example.com", "/")),
            Some(1)
        );
        assert_eq!(route(&[post], &get("example.com", "/")), None);
    }

    #[test]
    fn a_match_under_several_hosts_is_reached_through_each() {
        let mut spec = on("a.example.com", PathKind::Prefix, "/");
        spec.hosts.push((
            Some(("*.example.org".to_owned(), WildcardLabels::One)),
            false,
        ));
        spec.hosts.push((
            Some(("a.example.com".to_owned(), WildcardLabels::One)),
            false,
        ));
        let routes = [spec];
        assert_eq!(route(&routes, &get("a.example.com", "/x")), Some(0));
        assert_eq!(route(&routes, &get("b.example.org", "/x")), Some(0));
        assert_eq!(route(&routes, &get("b.c.example.org", "/x")), None);
    }

    #[test]
    fn a_match_without_hosts_is_unreachable() {
        let mut spec = on("example.com", PathKind::Prefix, "/");
        spec.hosts.clear();
        assert_eq!(route(&[spec], &get("example.com", "/")), None);
    }

    /// Routes on one host and path that differ only in their predicates, all of which the
    /// request below satisfies: nothing but the order of precedence decides between them.
    fn rival() -> impl Strategy<Value = RouteSpec> {
        (
            any::<bool>(),
            prop::sample::subsequence(vec!["x-a", "X-A", "x-b", "x-c"], 0..=4),
            prop::sample::subsequence(vec!["p", "q", "r"], 0..=3),
        )
            .prop_map(|(method, headers, query)| {
                let mut spec = on("a.b", PathKind::Prefix, "/a");
                spec.method = method.then(|| "GET".to_owned());
                spec.headers = headers
                    .iter()
                    .map(|name| ((*name).to_owned(), "1".to_owned()))
                    .collect();
                spec.query = query
                    .iter()
                    .map(|name| ((*name).to_owned(), "1".to_owned()))
                    .collect();
                spec
            })
    }

    proptest! {
        #[test]
        fn rivals_are_ranked_as_the_reference_ranks_them(
            routes in prop::collection::vec(rival(), 1..6),
        ) {
            let mut request = get("a.b", "/a/x");
            request.headers = pairs(&[("x-a", "1"), ("x-b", "1"), ("x-c", "1")]);
            request.query = "p=1&q=1&r=1".to_owned();
            // The comparison is inside `route`; every rival serves, so one must be found.
            prop_assert!(route(&routes, &request).is_some());
        }

        #[test]
        fn routing_agrees_with_the_sort_everything_reference(
            routes in prop::collection::vec(route_spec(), 0..8),
            request in request_spec(),
        ) {
            // The comparison is inside `route`.
            route(&routes, &request);
        }
    }
}
