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

/// The positions of the claims that are candidates for `host`, in the order
/// [`HostIndex::lookup`](crate::HostIndex::lookup) must give them: scan every claim, keep
/// the most specific matches plus whatever falls through, most specific first.
#[must_use]
pub fn host_candidates(claims: &[HostClaimSpec], host: &str) -> Vec<usize> {
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
        .into_iter()
        .map(|(position, _)| position)
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
        let name = segment.split_once(';').map_or(&**segment, |(name, _)| name);
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
/// its fields with commas gives.
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
        !values.is_empty() && values.join(",") == *expected
    })
}
