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
