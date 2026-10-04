//! The specification as code: slow, obvious implementations that the real ones are
//! compared with, by this crate's property tests and by the fuzz targets.
//!
//! Only built for tests and with the `reference` feature. Nothing here is shared with the
//! code it checks, and nothing here minds its cost.

use crate::WildcardLabels;
use std::cmp::Reverse;

/// Whether `host` falls under a hostname pattern, worked out label by label. `pattern` is
/// valid pattern text; `wildcard` says what a leading `*` stands for.
#[must_use]
pub fn host_matches(pattern: &str, wildcard: WildcardLabels, host: &str) -> bool {
    // Nothing longer than a DNS name matches anything.
    if host.len() > 253 {
        return false;
    }
    let labels =
        |name: &str| -> Vec<String> { name.split('.').map(str::to_ascii_lowercase).collect() };
    let (pattern, host) = (labels(pattern), labels(host));
    let Some((first, suffix)) = pattern.split_first() else {
        return false;
    };
    if first != "*" {
        return host == pattern;
    }
    if host.len() <= suffix.len() {
        return false;
    }
    let (leading, trailing) = host.split_at(host.len() - suffix.len());
    trailing == suffix
        && !leading.join(".").is_empty()
        && match wildcard {
            WildcardLabels::One => leading.len() == 1,
            WildcardLabels::OneOrMore => true,
        }
}

/// A claim on hosts as text: the pattern (`None` for every host) with the meaning of its
/// wildcard, and whether the claim falls through onto more specific hosts.
pub type HostClaimSpec = (Option<(String, WildcardLabels)>, bool);

/// The positions of the claims that are candidates for `host`, as
/// [`HostIndex::lookup`](crate::HostIndex::lookup) must group and order them: scan every
/// claim, keep the most specific matches plus whatever falls through, put equally specific
/// claims in one group in the order given, most specific group first.
#[must_use]
pub fn host_candidates(claims: &[HostClaimSpec], host: &str) -> Vec<Vec<usize>> {
    let specificity = |claim: &HostClaimSpec| match &claim.0 {
        None => (false, 0),
        Some((text, _)) => (!text.starts_with('*'), text.len()),
    };
    let matching: Vec<(usize, &HostClaimSpec)> = claims
        .iter()
        .enumerate()
        .filter(|(_, claim)| {
            claim
                .0
                .as_ref()
                .is_none_or(|(text, wildcard)| host_matches(text, *wildcard, host))
        })
        .collect();
    let most_specific = matching.iter().map(|(_, claim)| specificity(claim)).max();
    let mut candidates: Vec<(usize, &HostClaimSpec)> = matching
        .into_iter()
        .filter(|(_, claim)| claim.1 || Some(specificity(claim)) == most_specific)
        .collect();
    candidates.sort_by_key(|(position, claim)| (Reverse(specificity(claim)), *position));
    candidates
        .chunk_by(|(_, one), (_, other)| specificity(one) == specificity(other))
        .map(|group| group.iter().map(|(position, _)| *position).collect())
        .collect()
}

/// Whether `path` falls under an exact or prefix pattern, the prefix worked out segment by
/// segment as the Gateway API text describes it. `pattern` is in canonical form.
#[must_use]
pub fn path_matches(pattern: &str, is_prefix: bool, path: &str) -> bool {
    if !is_prefix {
        return pattern == path;
    }
    let mut wanted: Vec<&str> = pattern.split('/').collect();
    if wanted.last() == Some(&"") {
        wanted.pop();
    }
    let given: Vec<&str> = path.split('/').collect();
    path.starts_with('/')
        && given.len() >= wanted.len()
        && wanted.iter().zip(&given).all(|(a, b)| a == b)
}

/// The kinds of path pattern, in order of precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathKind {
    /// The whole path, byte for byte.
    Exact,
    /// A regular expression over the whole path.
    Regex,
    /// A prefix of whole segments.
    Prefix,
}

/// A path pattern as text, in canonical form, with its kind.
pub type PathSpec = (String, PathKind);

/// The positions of the entries that are candidates for a path, in the order
/// [`PathIndex::lookup`](crate::PathIndex::lookup) must give them: exact ones first, then
/// regexes, then the longest prefix first. Whether entry `n` matches the path is for
/// `matches` to say; this is the specification of the order.
#[must_use]
pub fn path_candidates(entries: &[PathSpec], matches: impl Fn(usize) -> bool) -> Vec<usize> {
    let mut candidates: Vec<(usize, &PathSpec)> = entries
        .iter()
        .enumerate()
        .filter(|(position, _)| matches(*position))
        .collect();
    candidates.sort_by_key(|(position, (text, kind))| {
        let length = match kind {
            PathKind::Prefix => text.strip_suffix('/').unwrap_or(text).len(),
            PathKind::Exact | PathKind::Regex => 0,
        };
        (*kind, Reverse(length), *position)
    });
    candidates
        .into_iter()
        .map(|(position, _)| position)
        .collect()
}

/// What [`normalise_path`](crate::normalise_path) must return, as `Some`, or that it must
/// reject the path, as `None` — worked out in separate steps: make every segment's
/// percent-encoding canonical, then resolve the list of segments with a stack.
#[must_use]
pub fn normalise_path(path: &str) -> Option<String> {
    let segments: Vec<(String, bool)> = path
        .strip_prefix('/')?
        .split('/')
        .map(canonical_segment)
        .collect::<Option<_>>()?;

    let mut stack: Vec<&str> = Vec::new();
    let mut trailing_slash = false;
    for (segment, encoded_dot) in &segments {
        // What a server that knows path parameters takes for the name: up to the first
        // `;`, which to a server that decodes first may have been written `%3B`.
        let decoded = segment.replace("%3B", ";");
        let name = decoded.split_once(';').map_or(&*decoded, |(name, _)| name);
        let is_dots = name == "." || name == "..";
        if is_dots && (*encoded_dot || name != segment) {
            return None;
        }
        trailing_slash = is_dots || segment.is_empty();
        if name == ".." {
            stack.pop()?;
        } else if !trailing_slash {
            stack.push(segment);
        }
    }
    let mut normal: String = stack.iter().flat_map(|segment| ["/", segment]).collect();
    if trailing_slash || normal.is_empty() {
        normal.push('/');
    }
    Some(normal)
}

/// The segment with canonical percent-encoding, and whether an encoded dot was decoded.
fn canonical_segment(segment: &str) -> Option<(String, bool)> {
    let mut canonical = String::new();
    let mut encoded_dot = false;
    let mut rest = segment.as_bytes();
    while let Some((&first, tail)) = rest.split_first() {
        let (byte, encoded) = if first == b'%' {
            let (digits, tail) = tail.split_at_checked(2)?;
            if !digits.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            rest = tail;
            let digits = std::str::from_utf8(digits).ok()?;
            (u8::from_str_radix(digits, 16).ok()?, true)
        } else {
            rest = tail;
            (first, false)
        };
        if byte.is_ascii_control() || byte == b'\\' || (encoded && byte == b'/') {
            return None;
        }
        let unreserved = byte.is_ascii_alphanumeric() || b"-._~".contains(&byte);
        if unreserved || (!encoded && b"!$&'()*+,;=:@".contains(&byte)) {
            canonical.push(char::from(byte));
            encoded_dot |= encoded && byte == b'.';
        } else {
            canonical.push_str(&format!("%{byte:02X}"));
        }
    }
    Some((canonical, encoded_dot))
}

/// Whether a request's header fields satisfy a rule's exact header matches, both given as
/// (name, value) in order: only the first match for a name counts, names are compared
/// without regard to case, and a header the request repeats has the one value that joining
/// its fields with commas gives — or, for `Cookie`, with `"; "` (RFC 9113 §8.2.3).
#[must_use]
pub fn exact_headers_match(rule: &[(String, String)], request: &[(String, String)]) -> bool {
    let mut seen: Vec<String> = Vec::new();
    rule.iter().all(|(name, expected)| {
        let name = name.to_ascii_lowercase();
        if seen.contains(&name) {
            return true;
        }
        seen.push(name.clone());
        let values: Vec<&str> = request
            .iter()
            .filter(|(field, _)| field.to_ascii_lowercase() == name)
            .map(|(_, value)| value.as_str())
            .collect();
        let between = if name == "cookie" { "; " } else { "," };
        !values.is_empty() && values.join(between) == *expected
    })
}

/// The parameters of a query string (without its `?`), in order and decoded: split on `&`
/// skipping empty pieces, split each pair at its first `=`, read `+` as a space and decode
/// percent-encoding. A pair whose name cannot be decoded is left out; a value that cannot
/// be decoded is `None`.
#[must_use]
pub fn query_parameters(query: &str) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((form_decode(name)?, form_decode(value)))
        })
        .collect()
}

fn form_decode(text: &str) -> Option<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut rest = text.as_bytes();
    while let Some((&first, tail)) = rest.split_first() {
        rest = tail;
        decoded.push(match first {
            b'+' => b' ',
            b'%' => {
                let (digits, tail) = rest.split_at_checked(2)?;
                rest = tail;
                if !digits.iter().all(u8::is_ascii_hexdigit) {
                    return None;
                }
                u8::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()?
            }
            byte => byte,
        });
    }
    Some(decoded)
}

/// Whether a query string satisfies a rule's exact query parameter matches, given as
/// (name, value) in order: only the first match for a name counts, and a parameter is
/// matched by its first occurrence in the query, which must decode to exactly the value.
#[must_use]
pub fn exact_query_matches(rule: &[(String, String)], query: &str) -> bool {
    let parameters = query_parameters(query);
    let mut seen: Vec<&str> = Vec::new();
    rule.iter().all(|(name, expected)| {
        if seen.contains(&name.as_str()) {
            return true;
        }
        seen.push(name);
        parameters
            .iter()
            .find(|(parameter, _)| parameter == name.as_bytes())
            .is_some_and(|(_, value)| value.as_deref() == Some(expected.as_bytes()))
    })
}

/// One way to reach a route rule, as text: host claims, a path pattern in canonical form,
/// and exact predicates.
#[derive(Debug, Clone)]
pub struct RouteSpec {
    /// The hosts served, as for [`host_candidates`].
    pub hosts: Vec<HostClaimSpec>,
    /// The path pattern.
    pub path: PathSpec,
    /// The method the request must have, if it matters.
    pub method: Option<String>,
    /// Exact header matches, as for [`exact_headers_match`].
    pub headers: Vec<(String, String)>,
    /// Exact query parameter matches, as for [`exact_query_matches`].
    pub query: Vec<(String, String)>,
}

/// A request, as text.
#[derive(Debug, Clone)]
pub struct RequestSpec {
    /// The bare hostname.
    pub host: String,
    /// The normalised path.
    pub path: String,
    /// The query string without its `?`.
    pub query: String,
    /// The method.
    pub method: String,
    /// The header fields in order.
    pub headers: Vec<(String, String)>,
}

/// Where a route stands among routes on equally specific hosts whose path matches, field by
/// field in the order they decide; less is first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Precedence {
    /// The kind of path pattern, and for a prefix its length, the longest first.
    pub path: (PathKind, Reverse<usize>),
    /// A method predicate before none.
    pub method: Reverse<bool>,
    /// The number of header predicates that count, the most first.
    pub headers: Reverse<usize>,
    /// The number of query predicates that count, the most first.
    pub query: Reverse<usize>,
    /// The order given.
    pub position: usize,
}

/// The precedence of route `position`.
#[must_use]
pub fn precedence(routes: &[RouteSpec], position: usize) -> Precedence {
    let distinct = |names: Vec<String>| {
        let mut names = names;
        names.sort();
        names.dedup();
        names.len()
    };
    let route = &routes[position];
    let (path, kind) = &route.path;
    let prefix_length = match kind {
        PathKind::Prefix => path.strip_suffix('/').unwrap_or(path).len(),
        PathKind::Exact | PathKind::Regex => 0,
    };
    let headers = route
        .headers
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase());
    let query = route.query.iter().map(|(name, _)| name.clone());
    Precedence {
        path: (*kind, Reverse(prefix_length)),
        method: Reverse(route.method.is_some()),
        headers: Reverse(distinct(headers.collect())),
        query: Reverse(distinct(query.collect())),
        position,
    }
}

/// Whether the request satisfies the route's method, header and query predicates.
#[must_use]
pub fn serves(route: &RouteSpec, request: &RequestSpec) -> bool {
    route
        .method
        .as_ref()
        .is_none_or(|method| *method == request.method)
        && exact_headers_match(&route.headers, &request.headers)
        && exact_query_matches(&route.query, &request.query)
}

/// For every route, the first of the groups [`host_candidates`] gives for `host` that one
/// of its claims is in, counted from the most specific; `None` for a route that is no
/// candidate for the host.
#[must_use]
pub fn host_groups(routes: &[RouteSpec], host: &str) -> Vec<Option<usize>> {
    // Every host claim of every route, and whose it is.
    let (owners, claims): (Vec<usize>, Vec<HostClaimSpec>) = routes
        .iter()
        .enumerate()
        .flat_map(|(position, route)| {
            route
                .hosts
                .iter()
                .map(move |claim| (position, claim.clone()))
        })
        .unzip();
    let mut groups = vec![None; routes.len()];
    for (at, group) in host_candidates(&claims, host).into_iter().enumerate() {
        for claim in group {
            groups[owners[claim]].get_or_insert(at);
        }
    }
    groups
}

/// The position of the route that [`Router::route`](crate::Router::route) must find: go
/// through the groups of equally specific host claims, most specific first; in each, sort
/// the routes whose path matches by their [`precedence`] and take the first the request
/// satisfies in full. Whether route `n`'s path matches is for `path_matches` to say.
#[must_use]
pub fn route(
    routes: &[RouteSpec],
    request: &RequestSpec,
    path_matches: impl Fn(usize) -> bool,
) -> Option<usize> {
    let groups = host_groups(routes, &request.host);
    // A route in several groups is first tried in the first of them, and what it fails
    // there it fails everywhere.
    let mut candidates: Vec<usize> = (0..routes.len())
        .filter(|&position| groups[position].is_some() && path_matches(position))
        .collect();
    candidates.sort_by_key(|&position| (groups[position], precedence(routes, position)));
    candidates
        .into_iter()
        .find(|&position| serves(&routes[position], request))
}
