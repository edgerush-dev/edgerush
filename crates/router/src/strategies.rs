//! Proptest strategies shared by this crate's tests, and the helpers that build the real
//! routes and requests they describe.
//!
//! Labels come from a tiny alphabet so that independently generated patterns and hosts
//! still collide, nest and shadow each other often.

use crate::reference::{PathKind, RequestSpec, RouteSpec};
use crate::{
    HeaderPredicate, HeaderPredicates, HostClaim, HostPattern, PathPattern, QueryPredicate,
    QueryPredicates, RouteMatch, WildcardLabels,
};
use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};
use proptest::prelude::*;

/// The match a route spec describes.
pub(crate) fn compile(spec: &RouteSpec, value: usize) -> RouteMatch<usize> {
    let (path, kind) = &spec.path;
    RouteMatch {
        hosts: spec
            .hosts
            .iter()
            .map(|(pattern, falls_through)| HostClaim {
                pattern: pattern
                    .as_ref()
                    .map(|(text, wildcard)| HostPattern::parse(text, *wildcard).unwrap()),
                falls_through: *falls_through,
                value: (),
            })
            .collect(),
        path: match kind {
            PathKind::Exact => PathPattern::exact(path),
            PathKind::Regex => PathPattern::regex(path),
            PathKind::Prefix => PathPattern::prefix(path),
            PathKind::GrpcMethod => PathPattern::grpc_method(path),
        }
        .unwrap(),
        method: spec.method.as_ref().map(|method| method.parse().unwrap()),
        headers: HeaderPredicates::new(
            spec.headers
                .iter()
                .map(|(name, value)| HeaderPredicate::exact(name, value).unwrap()),
        ),
        query: QueryPredicates::new(
            spec.query
                .iter()
                .map(|(name, value)| QueryPredicate::exact(name, value).unwrap()),
        ),
        value,
    }
}

/// Header fields, in order, as a map.
pub(crate) fn header_map(fields: &[(String, String)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in fields {
        headers.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

/// A route on one host (`*` for every host), falling through, with no predicates.
pub(crate) fn on(host: &str, kind: PathKind, path: &str) -> RouteSpec {
    RouteSpec {
        hosts: vec![(
            (host != "*").then(|| (host.to_owned(), WildcardLabels::OneOrMore)),
            true,
        )],
        path: (path.to_owned(), kind),
        method: None,
        headers: Vec::new(),
        query: Vec::new(),
    }
}

pub(crate) fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// A `GET` with no query and no headers.
pub(crate) fn get(host: &str, path: &str) -> RequestSpec {
    RequestSpec {
        host: host.to_owned(),
        path: path.to_owned(),
        query: String::new(),
        method: "GET".to_owned(),
        headers: Vec::new(),
    }
}

/// Routes and requests over a handful of hosts, paths, headers and parameters, so that
/// most requests have several routes to choose from.
pub(crate) fn route_spec() -> impl Strategy<Value = RouteSpec> {
    let host = prop_oneof![
        1 => Just(None),
        6 => (
            prop::sample::select(vec!["a.b", "c.b", "*.b", "*.a.b", "*.c"]),
            prop::sample::select(vec![WildcardLabels::One, WildcardLabels::OneOrMore]),
        )
            .prop_map(|(text, wildcard)| Some((text.to_owned(), wildcard))),
    ];
    let path = prop_oneof![
        (
            prop::sample::select(vec!["/", "/a", "/a/b"]),
            Just(PathKind::Exact)
        ),
        (
            prop::sample::select(vec!["/", "/a", "/a/b"]),
            Just(PathKind::Prefix)
        ),
        (
            prop::sample::select(vec!["/a.*", "/[ab]", "/a/[^/]+"]),
            Just(PathKind::Regex)
        ),
        (
            prop::sample::select(vec!["a", "b"]),
            Just(PathKind::GrpcMethod)
        ),
    ];
    let field = (
        prop::sample::select(vec!["x-a", "X-A", "x-b"]),
        prop::sample::select(vec!["1", "2"]),
    );
    let parameter = (
        prop::sample::select(vec!["p", "q"]),
        prop::sample::select(vec!["1", "2"]),
    );
    (
        prop::collection::vec((host, any::<bool>()), 0..3),
        path,
        prop::option::of(prop::sample::select(vec!["GET", "POST"])),
        prop::collection::vec(field, 0..3),
        prop::collection::vec(parameter, 0..3),
    )
        .prop_map(|(hosts, (path, kind), method, headers, query)| RouteSpec {
            hosts,
            path: (path.to_owned(), kind),
            method: method.map(str::to_owned),
            headers: pairs(&headers),
            query: pairs(&query),
        })
}

pub(crate) fn request_spec() -> impl Strategy<Value = RequestSpec> {
    (
        prop::sample::select(vec!["a.b", "c.b", "x.a.b", "x.y.a.b", "b", "x.c", "z"]),
        prop::sample::select(vec![
            "/", "/a", "/a/", "/a/b", "/a/b/c", "/b", "/b/a", "/ab",
        ]),
        prop::sample::select(vec![
            "",
            "p=1",
            "p=2&q=1",
            "q=2&p=1",
            "p=1&p=2",
            "%70=1&q=1",
        ]),
        prop::sample::select(vec!["GET", "POST", "DELETE"]),
        prop::collection::vec(
            (
                prop::sample::select(vec!["x-a", "x-b", "x-c"]),
                prop::sample::select(vec!["1", "2"]),
            ),
            0..3,
        ),
    )
        .prop_map(|(host, path, query, method, headers)| RequestSpec {
            host: host.to_owned(),
            path: path.to_owned(),
            query: query.to_owned(),
            method: method.to_owned(),
            headers: pairs(&headers),
        })
}

pub(crate) fn valid_label() -> impl Strategy<Value = String> {
    "[ab]{1,2}"
}

/// Host labels may be empty or upper case: hosts are not validated.
pub(crate) fn host_label() -> impl Strategy<Value = String> {
    "[abAB]{0,2}"
}

/// A valid pattern, exact or wildcard, of one to three labels.
pub(crate) fn pattern_text() -> impl Strategy<Value = String> {
    (any::<bool>(), prop::collection::vec(valid_label(), 1..4)).prop_map(|(wildcard, labels)| {
        let name = labels.join(".");
        if wildcard { format!("*.{name}") } else { name }
    })
}

pub(crate) fn wildcard_labels() -> impl Strategy<Value = WildcardLabels> {
    prop_oneof![Just(WildcardLabels::One), Just(WildcardLabels::OneOrMore)]
}

/// A valid path pattern of zero to three segments, with or without a trailing slash.
pub(crate) fn path_pattern_text() -> impl Strategy<Value = String> {
    (prop::collection::vec(valid_label(), 0..4), any::<bool>()).prop_map(
        |(segments, trailing_slash)| {
            let path = format!("/{}", segments.join("/"));
            if trailing_slash && !segments.is_empty() {
                format!("{path}/")
            } else {
                path
            }
        },
    )
}

/// Either an arbitrary path — request paths are not validated, so it may be empty, relative
/// or contain empty segments — or the pattern with something appended: more characters in
/// its last segment, more segments, a trailing slash.
pub(crate) fn path_near(pattern: &str) -> impl Strategy<Value = String> + use<> {
    let pattern = pattern.to_owned();
    let arbitrary = (any::<bool>(), prop::collection::vec(host_label(), 0..4)).prop_map(
        |(absolute, segments)| {
            let path = segments.join("/");
            if absolute { format!("/{path}") } else { path }
        },
    );
    let nearby = (any::<bool>(), "(|/|a|/a|/a/|/a/b)").prop_map(move |(trimmed, appended)| {
        let stem = if trimmed {
            pattern.strip_suffix('/').unwrap_or(&pattern)
        } else {
            &pattern
        };
        format!("{stem}{appended}")
    });
    prop_oneof![arbitrary, nearby]
}

/// Either an arbitrary host, or the pattern's own name under zero to two extra labels —
/// the interesting neighbourhood of every pattern.
pub(crate) fn host_near(pattern: &str) -> impl Strategy<Value = String> + use<> {
    let name = pattern.strip_prefix("*.").unwrap_or(pattern).to_owned();
    let arbitrary = prop::collection::vec(host_label(), 1..5).prop_map(|labels| labels.join("."));
    let nearby = (prop::collection::vec(host_label(), 0..3), any::<bool>()).prop_map(
        move |(mut labels, upper_case)| {
            labels.push(name.clone());
            let host = labels.join(".");
            if upper_case {
                host.to_ascii_uppercase()
            } else {
                host
            }
        },
    );
    prop_oneof![arbitrary, nearby]
}

/// Paths over an alphabet chosen to hit every rule often: separators, dots, the
/// encodings of dots, slashes, backslashes, letters and controls, path parameters, and
/// raw bytes that need encoding.
pub(crate) fn nasty_path() -> impl Strategy<Value = String> {
    let piece = prop_oneof![
        4 => Just("/"),
        3 => Just("."),
        2 => Just("a"),
        1 => Just("B"),
        1 => Just("%2e"),
        1 => Just("%2E"),
        1 => Just("%2f"),
        1 => Just("%5C"),
        1 => Just("%61"),
        1 => Just("%20"),
        1 => Just("%c3"),
        1 => Just("%0a"),
        1 => Just("%"),
        1 => Just("%4"),
        1 => Just(";"),
        1 => Just(";x"),
        1 => Just("%3b"),
        1 => Just("%3Bx"),
        1 => Just("\\"),
        1 => Just(" "),
        1 => Just("é"),
        1 => Just("\u{1}"),
        1 => Just("+"),
    ];
    (any::<bool>(), prop::collection::vec(piece, 0..10)).prop_map(|(absolute, pieces)| {
        let path = pieces.concat();
        if absolute { format!("/{path}") } else { path }
    })
}
