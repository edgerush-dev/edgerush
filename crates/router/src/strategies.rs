//! Proptest strategies shared by this crate's tests.
//!
//! Labels come from a tiny alphabet so that independently generated patterns and hosts
//! still collide, nest and shadow each other often.

use crate::WildcardLabels;
use proptest::prelude::*;

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
