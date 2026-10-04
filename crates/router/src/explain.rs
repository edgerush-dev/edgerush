//! Why a request goes where it does, for `edgerush explain` and `edgerush test`: every match
//! whose hostnames cover the request's host, in the router's order of precedence, each with
//! what became of it — chosen; failed on a predicate, with the value seen beside the value
//! wanted; outranked by the chosen match, on a key that names why; or kept from the host by
//! a more specific hostname that it does not fall through from.
//!
//! The [`Router`](crate::Router) finds its winner without looking at most matches, so it
//! cannot say why the others lost. This looks at every one, allocating as it likes, and is
//! code the proxy never runs; the tests hold it to the router's winner.

use crate::host::Kind as HostKind;
use crate::path::Kind as PathKind;
use crate::{
    Fields, HeaderPredicate, HostClaim, HostPattern, PathPattern, QueryPredicate, RequestParts,
    RouteMatch,
};
use http::Method;
use std::cmp::Reverse;

/// What became of every match a request's host reaches.
#[derive(Debug)]
pub struct Explanation<'m, T> {
    /// Every match with a hostname that covers the request's host, in order of precedence:
    /// a match under several such hostnames once, where the most specific of them that
    /// makes it a candidate puts it.
    pub considered: Vec<Considered<'m, T>>,
    /// How many matches have no hostname that covers the request's host.
    pub other_hosts: usize,
}

impl<'m, T> Explanation<'m, T> {
    /// The match that serves the request, the one the router finds; `None` if nothing does.
    #[must_use]
    pub fn chosen(&self) -> Option<&'m RouteMatch<T>> {
        self.considered
            .iter()
            .find(|considered| matches!(considered.verdict, Verdict::Chosen))
            .map(|considered| considered.route_match)
    }
}

/// One match, and what became of it.
#[derive(Debug)]
pub struct Considered<'m, T> {
    /// The match.
    pub route_match: &'m RouteMatch<T>,
    /// What became of it.
    pub verdict: Verdict<'m>,
}

/// What became of a match.
#[derive(Debug)]
pub enum Verdict<'m> {
    /// It serves the request.
    Chosen,
    /// The first of its predicates, in the order they are checked, that does not hold.
    Failed(Failure<'m>),
    /// It would serve the request, but the chosen match comes before it, on this key.
    Outranked(Key),
    /// A more specific hostname covers the host, and none of this match's hostnames that
    /// cover it falls through onto hosts that something more specific claims.
    Overshadowed {
        /// The most specific hostname that covers the host.
        by: &'m HostPattern,
    },
}

/// A predicate that does not hold, with what the request has.
#[derive(Debug)]
pub enum Failure<'m> {
    /// The request's path is not one the pattern covers.
    Path(&'m PathPattern),
    /// The request's method is not this one.
    Method(&'m Method),
    /// The request's header does not have the value wanted.
    Header {
        /// The predicate.
        predicate: &'m HeaderPredicate,
        /// What the request has.
        seen: Seen,
    },
    /// The request's query parameter does not have the value wanted.
    Query {
        /// The predicate.
        predicate: &'m QueryPredicate,
        /// What the request has.
        seen: Seen,
    },
}

/// What a request has for a header or query parameter, as a predicate judges it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seen {
    /// Nothing: the request does not have it.
    Absent,
    /// This value: a repeated header's fields joined, a query parameter's first occurrence
    /// decoded.
    Value(Vec<u8>),
    /// A query parameter's first value, as it came, which cannot be decoded and so
    /// satisfies no predicate.
    Undecodable(Vec<u8>),
}

/// What a header or query parameter predicate wants of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wanted<'a> {
    /// This value, byte for byte.
    Exact(&'a [u8]),
    /// A value that this regular expression matches as a whole.
    Regex(&'a str),
}

/// Why one match comes before another that would also serve: the first of these, in the
/// router's order, on which they differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    /// A more specific hostname.
    Host,
    /// A better path: exact before regular expression before prefix, a longer prefix
    /// before a shorter one.
    Path,
    /// A method predicate before none.
    Method,
    /// More header predicates.
    Headers,
    /// More query parameter predicates.
    Query,
    /// Given earlier.
    Order,
}

/// Explains a request against the matches a router is built from, given in the same
/// order: the router's winner is the match chosen here.
#[must_use]
pub fn explain<'m, T, F: Fields + ?Sized>(
    matches: &'m [RouteMatch<T>],
    request: &RequestParts<'_, F>,
) -> Explanation<'m, T> {
    let covers = |claim: &&HostClaim<()>| {
        claim
            .pattern
            .as_ref()
            .is_none_or(|pattern| pattern.matches(request.host))
    };
    // Only claims as specific as the most specific one that covers the host, and those that
    // fall through, are candidates.
    let top = matches
        .iter()
        .flat_map(|route_match| &route_match.hosts)
        .filter(covers)
        .max_by_key(|claim| specificity(claim));
    let most_specific = top.map(specificity);
    let candidate =
        |claim: &&HostClaim<()>| claim.falls_through || Some(specificity(claim)) == most_specific;

    let mut other_hosts = 0;
    let mut placed = Vec::new();
    for (order, route_match) in matches.iter().enumerate() {
        let covering = route_match.hosts.iter().filter(covers);
        let best_candidate = covering.clone().filter(candidate).map(specificity).max();
        let (host, overshadowed) = match (best_candidate, covering.map(specificity).max()) {
            (None, None) => {
                other_hosts += 1;
                continue;
            }
            (Some(host), _) => (host, None),
            // Every claim on every host is as specific as any other, so a claim that is not
            // a candidate is outdone by one with a pattern: `top` has one.
            (None, Some(host)) => (host, top.and_then(|claim| claim.pattern.as_ref())),
        };
        let rank = Rank {
            host: Reverse(host),
            path: path_rank(&route_match.path),
            method: Reverse(route_match.method.is_some()),
            headers: Reverse(route_match.headers.len()),
            query: Reverse(route_match.query.len()),
            order,
        };
        placed.push((rank, route_match, overshadowed));
    }
    placed.sort_by_key(|(rank, ..)| *rank);

    let mut chosen: Option<Rank> = None;
    let considered = placed
        .into_iter()
        .map(|(rank, route_match, overshadowed)| {
            let verdict = if let Some(by) = overshadowed {
                Verdict::Overshadowed { by }
            } else if let Some(failure) = first_failure(route_match, request) {
                Verdict::Failed(failure)
            } else if let Some(winner) = &chosen {
                Verdict::Outranked(winner.key_over(&rank))
            } else {
                chosen = Some(rank);
                Verdict::Chosen
            };
            Considered {
                route_match,
                verdict,
            }
        })
        .collect();
    Explanation {
        considered,
        other_hosts,
    }
}

/// How specific a claim is, as the host index ranks claims: an exact name before any
/// wildcard, a longer wildcard suffix before a shorter one, any pattern before none.
fn specificity(claim: &HostClaim<()>) -> (bool, usize) {
    claim.pattern.as_ref().map_or((false, 0), |pattern| {
        (pattern.kind == HostKind::Exact, pattern.name.len())
    })
}

/// Where a path pattern stands, as the path index orders candidates.
fn path_rank(path: &PathPattern) -> (u8, Reverse<usize>) {
    match path.kind {
        PathKind::Exact => (0, Reverse(0)),
        PathKind::Regex(_) => (1, Reverse(0)),
        PathKind::Prefix => (2, Reverse(path.path.len())),
    }
}

/// A match's place in the order of precedence, lowest first, key by key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    host: Reverse<(bool, usize)>,
    path: (u8, Reverse<usize>),
    method: Reverse<bool>,
    headers: Reverse<usize>,
    query: Reverse<usize>,
    order: usize,
}

impl Rank {
    /// The key on which this rank, the better one, comes before `other`.
    fn key_over(&self, other: &Self) -> Key {
        if self.host != other.host {
            Key::Host
        } else if self.path != other.path {
            Key::Path
        } else if self.method != other.method {
            Key::Method
        } else if self.headers != other.headers {
            Key::Headers
        } else if self.query != other.query {
            Key::Query
        } else {
            Key::Order
        }
    }
}

/// The first predicate of the match that the request does not satisfy, in the order the
/// router checks them: path, method, headers, query.
fn first_failure<'m, T, F: Fields + ?Sized>(
    route_match: &'m RouteMatch<T>,
    request: &RequestParts<'_, F>,
) -> Option<Failure<'m>> {
    if !route_match.path.matches(request.path) {
        return Some(Failure::Path(&route_match.path));
    }
    if let Some(method) = &route_match.method
        && method != request.method
    {
        return Some(Failure::Method(method));
    }
    if let Some(predicate) = route_match
        .headers
        .iter()
        .find(|predicate| !predicate.matches(request.headers))
    {
        return Some(Failure::Header {
            predicate,
            seen: predicate.seen(request.headers),
        });
    }
    route_match
        .query
        .iter()
        .find(|predicate| !predicate.matches(request.query))
        .map(|predicate| Failure::Query {
            predicate,
            seen: predicate.seen(request.query),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::{self, PathKind, RequestSpec, RouteSpec};
    use crate::strategies::{compile, get, header_map, on, pairs, request_spec, route_spec};
    use crate::{Router, WildcardLabels};
    use proptest::prelude::*;
    use std::cmp::Ordering;

    /// What became of a match, owned, as the tests below state it.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Chosen,
        Path,
        Method,
        Header(String, Seen),
        Query(String, Seen),
        Outranked(Key),
        Overshadowed(String),
    }
    use Outcome::{Chosen, Outranked, Overshadowed};

    fn header(name: &str, seen: Seen) -> Outcome {
        Outcome::Header(name.to_owned(), seen)
    }

    fn query(name: &str, seen: Seen) -> Outcome {
        Outcome::Query(name.to_owned(), seen)
    }

    fn value(text: &str) -> Seen {
        Seen::Value(text.as_bytes().to_vec())
    }

    /// Explains the request, checks the explanation against the router, the reference and
    /// the predicates themselves, and gives what became of each match listed, by its
    /// position, with the count of matches for other hosts.
    fn explained(routes: &[RouteSpec], request: &RequestSpec) -> (Vec<(usize, Outcome)>, usize) {
        let matches: Vec<RouteMatch<usize>> = routes
            .iter()
            .enumerate()
            .map(|(position, spec)| compile(spec, position))
            .collect();
        let method: Method = request.method.parse().unwrap();
        let headers = header_map(&request.headers);
        let parts = RequestParts {
            host: &request.host,
            path: &request.path,
            query: &request.query,
            method: &method,
            headers: &headers,
        };
        let explanation = explain(&matches, &parts);

        // The winner is the router's, and the reference's.
        let chosen = explanation.chosen().map(|chosen| chosen.value);
        let path_matches = |position: usize| matches[position].path.matches(&request.path);
        assert_eq!(
            chosen,
            Router::new(matches.clone()).route(&parts).copied(),
            "explain and the router disagree"
        );
        assert_eq!(
            chosen,
            reference::route(routes, request, path_matches),
            "explain and the reference disagree"
        );

        // Every match is listed once if a hostname of its covers the host, and counted if
        // none does.
        let covered = |spec: &RouteSpec| {
            spec.hosts.iter().any(|(pattern, _)| {
                pattern.as_ref().is_none_or(|(text, wildcard)| {
                    reference::host_matches(text, *wildcard, &request.host)
                })
            })
        };
        let listed: Vec<usize> = explanation
            .considered
            .iter()
            .map(|considered| considered.route_match.value)
            .collect();
        let mut once = listed.clone();
        once.sort_unstable();
        once.dedup();
        assert_eq!(once.len(), listed.len(), "a match listed twice");
        let reached: Vec<usize> = (0..routes.len())
            .filter(|&position| covered(&routes[position]))
            .collect();
        assert_eq!(once, reached);
        assert_eq!(explanation.other_hosts, routes.len() - reached.len());

        // Candidates are listed in the reference's order.
        let groups = reference::host_groups(routes, &request.host);
        let rank = |position: usize| (groups[position], reference::precedence(routes, position));
        let candidates: Vec<usize> = listed
            .iter()
            .copied()
            .filter(|&position| groups[position].is_some())
            .collect();
        assert!(
            candidates.is_sorted_by_key(|&position| rank(position)),
            "not in order of precedence: {candidates:?}"
        );

        let mut past_the_winner = false;
        let outcomes = explanation
            .considered
            .iter()
            .map(|considered| {
                let position = considered.route_match.value;
                let spec = &routes[position];
                let is_candidate = groups[position].is_some();
                let outcome = match &considered.verdict {
                    Verdict::Chosen => {
                        assert!(!past_the_winner, "two chosen");
                        past_the_winner = true;
                        Chosen
                    }
                    Verdict::Overshadowed { by } => {
                        assert!(!is_candidate, "{position} is a candidate");
                        assert!(by.matches(&request.host));
                        Overshadowed(by.to_string())
                    }
                    Verdict::Failed(failure) => {
                        assert!(is_candidate, "{position} is no candidate");
                        failed(&matches[position], spec, request, failure)
                    }
                    Verdict::Outranked(key) => {
                        assert!(is_candidate, "{position} is no candidate");
                        assert!(past_the_winner, "{position} is outranked before the winner");
                        assert!(path_matches(position) && reference::serves(spec, request));
                        let (winner, loser) = (rank(chosen.unwrap()), rank(position));
                        let decided = [
                            (Key::Host, winner.0.cmp(&loser.0)),
                            (Key::Path, winner.1.path.cmp(&loser.1.path)),
                            (Key::Method, winner.1.method.cmp(&loser.1.method)),
                            (Key::Headers, winner.1.headers.cmp(&loser.1.headers)),
                            (Key::Query, winner.1.query.cmp(&loser.1.query)),
                            (Key::Order, winner.1.position.cmp(&loser.1.position)),
                        ]
                        .into_iter()
                        .find(|(_, ordering)| ordering.is_ne());
                        assert_eq!(decided, Some((*key, Ordering::Less)), "{position}");
                        Outranked(*key)
                    }
                };
                (position, outcome)
            })
            .collect();
        assert_eq!(past_the_winner, chosen.is_some());
        (outcomes, explanation.other_hosts)
    }

    /// Checks that the predicate named fails and every one checked before it holds.
    fn failed(
        route_match: &RouteMatch<usize>,
        spec: &RouteSpec,
        request: &RequestSpec,
        failure: &Failure<'_>,
    ) -> Outcome {
        let path_holds = route_match.path.matches(&request.path);
        let method_holds = spec
            .method
            .as_ref()
            .is_none_or(|method| *method == request.method);
        let headers_hold =
            |count: usize| reference::exact_headers_match(&spec.headers[..count], &request.headers);
        let query_holds =
            |count: usize| reference::exact_query_matches(&spec.query[..count], &request.query);
        match failure {
            Failure::Path(pattern) => {
                assert!(std::ptr::eq(*pattern, &route_match.path));
                assert!(!path_holds);
                Outcome::Path
            }
            Failure::Method(method) => {
                assert!(path_holds);
                assert_eq!(Some(method.as_str()), spec.method.as_deref());
                assert!(!method_holds);
                Outcome::Method
            }
            Failure::Header { predicate, seen } => {
                assert!(path_holds && method_holds);
                let name = predicate.name().as_str();
                let at = spec
                    .headers
                    .iter()
                    .position(|(given, _)| given.eq_ignore_ascii_case(name))
                    .unwrap();
                assert!(headers_hold(at), "a header before {name} fails");
                let alone = &spec.headers[at..=at];
                assert!(!reference::exact_headers_match(alone, &request.headers));
                assert_eq!(predicate.wanted(), Wanted::Exact(alone[0].1.as_bytes()));
                let fields: Vec<&str> = request
                    .headers
                    .iter()
                    .filter(|(given, _)| given.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value.as_str())
                    .collect();
                let between = if name == "cookie" { "; " } else { "," };
                let expected = if fields.is_empty() {
                    Seen::Absent
                } else {
                    value(&fields.join(between))
                };
                assert_eq!(*seen, expected);
                header(name, seen.clone())
            }
            Failure::Query { predicate, seen } => {
                assert!(path_holds && method_holds && headers_hold(spec.headers.len()));
                let name = predicate.name();
                let at = spec
                    .query
                    .iter()
                    .position(|(given, _)| given.as_bytes() == name)
                    .unwrap();
                assert!(query_holds(at), "a parameter before {name:?} fails");
                let alone = &spec.query[at..=at];
                assert!(!reference::exact_query_matches(alone, &request.query));
                assert_eq!(predicate.wanted(), Wanted::Exact(alone[0].1.as_bytes()));
                let first = reference::query_parameters(&request.query)
                    .into_iter()
                    .find(|(given, _)| given == name);
                match (first, seen) {
                    (None, Seen::Absent) | (Some((_, None)), Seen::Undecodable(_)) => {}
                    (Some((_, Some(expected))), Seen::Value(got)) => assert_eq!(*got, expected),
                    (first, seen) => panic!("{first:?} seen as {seen:?}"),
                }
                query(&String::from_utf8_lossy(name), seen.clone())
            }
        }
    }

    fn with_method(mut spec: RouteSpec, method: &str) -> RouteSpec {
        spec.method = Some(method.to_owned());
        spec
    }

    fn with_headers(mut spec: RouteSpec, headers: &[(&str, &str)]) -> RouteSpec {
        spec.headers = pairs(headers);
        spec
    }

    fn with_query(mut spec: RouteSpec, query: &[(&str, &str)]) -> RouteSpec {
        spec.query = pairs(query);
        spec
    }

    const HOST: &str = "a.example.com";

    #[test]
    fn matches_for_other_hosts_are_counted_and_not_listed() {
        assert_eq!(explained(&[], &get(HOST, "/")), (vec![], 0));
        let mut unreachable = on(HOST, PathKind::Prefix, "/");
        unreachable.hosts.clear();
        let routes = [
            on("b.example.com", PathKind::Prefix, "/"),
            on(HOST, PathKind::Prefix, "/"),
            unreachable,
            on("*.example.org", PathKind::Prefix, "/"),
        ];
        assert_eq!(explained(&routes, &get(HOST, "/x")), (vec![(1, Chosen)], 3));
    }

    #[test]
    fn a_path_outside_the_pattern_fails_on_the_path() {
        let routes = [
            on(HOST, PathKind::Prefix, "/cart"),
            on(HOST, PathKind::Exact, "/closed"),
            on(HOST, PathKind::Regex, "/a/[0-9]+"),
        ];
        let (outcomes, _) = explained(&routes, &get(HOST, "/search"));
        assert_eq!(
            outcomes,
            [(1, Outcome::Path), (2, Outcome::Path), (0, Outcome::Path)]
        );
    }

    #[test]
    fn another_method_fails_on_the_method() {
        let routes = [with_method(on(HOST, PathKind::Prefix, "/"), "POST")];
        let (outcomes, _) = explained(&routes, &get(HOST, "/"));
        assert_eq!(outcomes, [(0, Outcome::Method)]);
    }

    #[test]
    fn a_header_fails_with_the_value_the_request_has() {
        let routes = [with_headers(
            on(HOST, PathKind::Prefix, "/"),
            &[("x-a", "1"), ("X-B", "2")],
        )];
        let outcome = |fields: &[(&str, &str)]| {
            let mut request = get(HOST, "/");
            request.headers = pairs(fields);
            explained(&routes, &request).0.remove(0).1
        };
        assert_eq!(
            outcome(&[("x-a", "1"), ("x-b", "3")]),
            header("x-b", value("3"))
        );
        assert_eq!(outcome(&[("x-a", "1")]), header("x-b", Seen::Absent));
        assert_eq!(
            outcome(&[("x-a", "2"), ("x-b", "3")]),
            header("x-a", value("2"))
        );
        // A repeated header is judged by its fields joined.
        assert_eq!(
            outcome(&[("x-a", "1"), ("x-b", "2"), ("X-B", "2")]),
            header("x-b", value("2,2"))
        );
        assert_eq!(outcome(&[("x-a", "1"), ("x-b", "2")]), Chosen);

        let routes = [with_headers(
            on(HOST, PathKind::Prefix, "/"),
            &[("cookie", "a=1")],
        )];
        let mut request = get(HOST, "/");
        request.headers = pairs(&[("cookie", "a=1"), ("cookie", "b=2")]);
        assert_eq!(
            explained(&routes, &request).0,
            [(0, header("cookie", value("a=1; b=2")))]
        );
    }

    #[test]
    fn a_query_parameter_fails_with_the_value_the_request_has() {
        let routes = [with_query(
            on(HOST, PathKind::Prefix, "/"),
            &[("p", "1"), ("q", "a b")],
        )];
        let outcome = |query: &str| {
            let mut request = get(HOST, "/");
            request.query = query.to_owned();
            explained(&routes, &request).0.remove(0).1
        };
        assert_eq!(outcome("p=1&q=x"), query("q", value("x")));
        assert_eq!(outcome("p=1"), query("q", Seen::Absent));
        assert_eq!(outcome("q=a+b"), query("p", Seen::Absent));
        // The first occurrence decides, decoded; a value that cannot be decoded is given as
        // it came.
        assert_eq!(outcome("p=1&%71=a+c&q=a+b"), query("q", value("a c")));
        assert_eq!(
            outcome("p=1&q=%zz"),
            query("q", Seen::Undecodable(b"%zz".to_vec()))
        );
        assert_eq!(outcome("p=1&q=a%20b"), Chosen);
    }

    #[test]
    fn predicates_are_named_in_the_order_they_are_checked() {
        let route = with_query(
            with_headers(
                with_method(on(HOST, PathKind::Prefix, "/a"), "POST"),
                &[("x-a", "1")],
            ),
            &[("p", "1")],
        );
        let routes = [route];
        let outcome = |path: &str, method: &str, field: &str, query: &str| {
            let mut request = get(HOST, path);
            request.method = method.to_owned();
            request.headers = pairs(&[("x-a", field)]);
            request.query = query.to_owned();
            explained(&routes, &request).0.remove(0).1
        };
        assert_eq!(outcome("/b", "GET", "2", "p=2"), Outcome::Path);
        assert_eq!(outcome("/a", "GET", "2", "p=2"), Outcome::Method);
        assert_eq!(outcome("/a", "POST", "2", "p=2"), header("x-a", value("2")));
        assert_eq!(outcome("/a", "POST", "1", "p=2"), query("p", value("2")));
        assert_eq!(outcome("/a", "POST", "1", "p=1"), Chosen);
    }

    #[test]
    fn a_more_specific_host_outranks() {
        let routes = [
            on("*", PathKind::Exact, "/x"),
            on("*.example.com", PathKind::Exact, "/x"),
            on(HOST, PathKind::Prefix, "/"),
        ];
        let (outcomes, _) = explained(&routes, &get(HOST, "/x"));
        assert_eq!(
            outcomes,
            [
                (2, Chosen),
                (1, Outranked(Key::Host)),
                (0, Outranked(Key::Host))
            ]
        );
    }

    #[test]
    fn a_better_path_outranks() {
        let routes = [
            with_method(on(HOST, PathKind::Prefix, "/"), "GET"),
            on(HOST, PathKind::Prefix, "/a"),
            on(HOST, PathKind::Regex, "/a/.*"),
            on(HOST, PathKind::Exact, "/a/b"),
        ];
        let (outcomes, _) = explained(&routes, &get(HOST, "/a/b"));
        assert_eq!(
            outcomes,
            [
                (3, Chosen),
                (2, Outranked(Key::Path)),
                (1, Outranked(Key::Path)),
                (0, Outranked(Key::Path)),
            ]
        );
    }

    #[test]
    fn a_method_then_more_headers_then_more_parameters_then_order_outrank() {
        let base = on(HOST, PathKind::Prefix, "/");
        let routes = [
            base.clone(),
            base.clone(),
            with_query(base.clone(), &[("p", "1")]),
            with_headers(base.clone(), &[("x-a", "1")]),
            with_method(base, "GET"),
        ];
        let mut request = get(HOST, "/");
        request.headers = pairs(&[("x-a", "1")]);
        request.query = "p=1".to_owned();
        // The first key on which a match falls behind the winner is named, whatever the
        // ones after it say.
        let (outcomes, _) = explained(&routes, &request);
        assert_eq!(
            outcomes,
            [
                (4, Chosen),
                (3, Outranked(Key::Method)),
                (2, Outranked(Key::Method)),
                (0, Outranked(Key::Method)),
                (1, Outranked(Key::Method)),
            ]
        );
        let (outcomes, _) = explained(&routes[..4], &request);
        assert_eq!(
            outcomes,
            [
                (3, Chosen),
                (2, Outranked(Key::Headers)),
                (0, Outranked(Key::Headers)),
                (1, Outranked(Key::Headers)),
            ]
        );
        let (outcomes, _) = explained(&routes[..3], &request);
        assert_eq!(
            outcomes,
            [
                (2, Chosen),
                (0, Outranked(Key::Query)),
                (1, Outranked(Key::Query))
            ]
        );
        let (outcomes, _) = explained(&routes[..2], &request);
        assert_eq!(outcomes, [(0, Chosen), (1, Outranked(Key::Order))]);
    }

    #[test]
    fn a_match_below_the_winner_that_would_not_serve_failed() {
        let base = on(HOST, PathKind::Prefix, "/");
        let routes = [
            with_headers(base.clone(), &[("x-a", "1")]),
            with_method(base.clone(), "GET"),
            with_method(base, "POST"),
        ];
        let (outcomes, _) = explained(&routes, &get(HOST, "/"));
        assert_eq!(
            outcomes,
            [
                (1, Chosen),
                (2, Outcome::Method),
                (0, header("x-a", Seen::Absent)),
            ]
        );
    }

    #[test]
    fn a_hostname_that_does_not_fall_through_is_overshadowed() {
        let mut wildcard = on("*.example.com", PathKind::Prefix, "/");
        wildcard.hosts[0].1 = false;
        let routes = [wildcard, on(HOST, PathKind::Prefix, "/own")];
        assert_eq!(
            explained(&routes, &get(HOST, "/other")),
            (
                vec![(1, Outcome::Path), (0, Overshadowed(HOST.to_owned()))],
                0
            )
        );
        assert_eq!(
            explained(&routes, &get("b.example.com", "/other")),
            (vec![(0, Chosen)], 1)
        );
    }

    #[test]
    fn a_match_under_several_hostnames_is_listed_once_where_it_is_a_candidate() {
        // Reached through both hostnames, and listed at the more specific.
        let mut both = on(HOST, PathKind::Prefix, "/");
        both.hosts.push((
            Some(("*.example.com".to_owned(), WildcardLabels::One)),
            true,
        ));
        let routes = [on("*.example.com", PathKind::Prefix, "/"), both];
        let (outcomes, _) = explained(&routes, &get(HOST, "/"));
        assert_eq!(outcomes, [(1, Chosen), (0, Outranked(Key::Host))]);

        // Kept from the host under its wildcard, and reached as a claim on every host.
        let mut fallback = on("*.example.com", PathKind::Prefix, "/");
        fallback.hosts[0].1 = false;
        fallback.hosts.push((None, true));
        let routes = [
            fallback,
            on("*.example.com", PathKind::Prefix, "/"),
            on(HOST, PathKind::Prefix, "/own"),
        ];
        let (outcomes, _) = explained(&routes, &get(HOST, "/x"));
        assert_eq!(
            outcomes,
            [(2, Outcome::Path), (1, Chosen), (0, Outranked(Key::Host))]
        );
    }

    /// The request every rival below serves.
    fn rivalled() -> RequestSpec {
        let mut request = get("x.a.b", "/a/b");
        request.headers = pairs(&[("x-a", "1"), ("x-b", "1"), ("x-c", "1")]);
        request.query = "p=1&q=1&r=1".to_owned();
        request
    }

    /// Matches that all serve [`rivalled`] if its host reaches them, under hostnames of
    /// every specificity that cover it, so that nearly every one listed is outranked.
    fn rival() -> impl Strategy<Value = RouteSpec> {
        let host = prop_oneof![
            Just(None),
            prop::sample::select(vec!["x.a.b", "*.a.b", "*.b"])
                .prop_map(|text| Some((text.to_owned(), WildcardLabels::OneOrMore))),
        ];
        let path = prop::sample::select(vec![
            ("/a/b", PathKind::Exact),
            ("/a/.*", PathKind::Regex),
            ("/[a]/b", PathKind::Regex),
            ("/a/b", PathKind::Prefix),
            ("/a", PathKind::Prefix),
            ("/", PathKind::Prefix),
        ]);
        (
            prop::collection::vec((host, prop::bool::weighted(0.8)), 1..3),
            path,
            any::<bool>(),
            prop::sample::subsequence(vec!["x-a", "X-A", "x-b", "x-c"], 0..=4),
            prop::sample::subsequence(vec!["p", "q", "r"], 0..=3),
        )
            .prop_map(|(hosts, (path, kind), method, headers, query)| RouteSpec {
                hosts,
                path: (path.to_owned(), kind),
                method: method.then(|| "GET".to_owned()),
                headers: headers
                    .iter()
                    .map(|name| ((*name).to_owned(), "1".to_owned()))
                    .collect(),
                query: query
                    .iter()
                    .map(|name| ((*name).to_owned(), "1".to_owned()))
                    .collect(),
            })
    }

    proptest! {
        #[test]
        fn explanations_agree_with_the_router_the_reference_and_the_predicates(
            routes in prop::collection::vec(route_spec(), 0..8),
            request in request_spec(),
        ) {
            // The checks are inside `explained`.
            explained(&routes, &request);
        }

        #[test]
        fn rivals_are_outranked_on_the_first_key_they_fall_behind_on(
            routes in prop::collection::vec(rival(), 1..8),
        ) {
            // The checks are inside `explained`; nothing here fails a predicate.
            let (outcomes, _) = explained(&routes, &rivalled());
            prop_assert!(outcomes.iter().all(|(_, outcome)| matches!(
                outcome,
                Chosen | Outranked(_) | Overshadowed(_)
            )));
        }
    }
}
