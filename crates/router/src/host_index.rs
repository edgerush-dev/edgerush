//! The host stage of routing: from a request's host to the candidates that may serve it.
//!
//! Host specificity is textual — an exact name beats any wildcard, a longer wildcard
//! suffix beats a shorter one, and anything beats a claim on every host. Ingress and
//! Gateway API wildcards with the same suffix are equally specific.
//!
//! Whether less specific claims stay candidates on a host that something more specific
//! also claims is decided per claim ([`HostClaim::falls_through`]): Gateway API requires
//! it, nginx-style Ingress does not want it. Either way it is resolved when the index is
//! built, by copying falling-through values into the more specific candidate lists, so a
//! request pays for one lookup and gets one list.

use crate::WildcardLabels;
use crate::host::{HostPattern, Kind, MAX_NAME_LEN};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher};

/// A fixed-key hasher instead of the randomly seeded default: a pure crate takes no
/// randomness, and benchmarks that count instructions need identical runs. Random seeding
/// defends against chosen colliding keys, but the keys stored here come from
/// configuration, not from requests.
type Map<V> = HashMap<Box<[u8]>, V, BuildHasherDefault<DefaultHasher>>;

/// One claim on a set of hosts, carrying whatever the caller wants to find again.
#[derive(Debug, Clone)]
pub struct HostClaim<T> {
    /// The hosts claimed; `None` claims every host.
    pub pattern: Option<HostPattern>,
    /// Whether `value` stays a candidate, after the more specific ones, on hosts that a
    /// more specific claim also covers. Without it, the claim serves only hosts that
    /// nothing more specific claims.
    pub falls_through: bool,
    /// What a lookup returns for hosts under this claim.
    pub value: T,
}

/// An immutable index from request host to candidates, built once per config snapshot.
#[derive(Debug)]
pub struct HostIndex<T> {
    exact: Map<Box<[T]>>,
    /// Keyed by wildcard suffix, leading dot included (`.example.com`).
    wildcards: Map<WildcardCandidates<T>>,
    /// For hosts no pattern covers.
    any: Box<[T]>,
}

#[derive(Debug)]
struct WildcardCandidates<T> {
    /// For hosts with exactly one label before the suffix: both wildcard kinds apply.
    one_label: Box<[T]>,
    /// For hosts with more labels before the suffix; `None` if only single-label
    /// wildcards claim this suffix, in which case a shorter suffix may still match.
    more_labels: Option<Box<[T]>>,
}

impl<T: Clone> HostIndex<T> {
    /// Builds the index. Among equally specific claims, candidates keep the order the
    /// claims were given in.
    pub fn new(claims: impl IntoIterator<Item = HostClaim<T>>) -> Self {
        let claims: Vec<HostClaim<T>> = claims.into_iter().collect();

        let mut exact: HashMap<&str, Vec<&HostClaim<T>>> = HashMap::new();
        let mut wildcards: HashMap<&str, Vec<&HostClaim<T>>> = HashMap::new();
        let mut any = Vec::new();
        for claim in &claims {
            match &claim.pattern {
                None => any.push(claim),
                Some(pattern) => match pattern.kind {
                    Kind::Exact => exact.entry(&pattern.name).or_default().push(claim),
                    Kind::Wildcard(_) => wildcards.entry(&pattern.name).or_default().push(claim),
                },
            }
        }

        let less_specific = LessSpecific {
            wildcards: &wildcards,
            any: &any,
        };
        Self {
            exact: exact
                .iter()
                .map(|(name, own)| {
                    let candidates = less_specific.after(own.iter().copied(), name);
                    (name.as_bytes().into(), candidates)
                })
                .collect(),
            wildcards: wildcards
                .iter()
                .map(|(suffix, own)| {
                    let more_labels = || own.iter().copied().filter(covers_more_labels);
                    let candidates = WildcardCandidates {
                        one_label: less_specific.after(own.iter().copied(), suffix),
                        more_labels: more_labels()
                            .next()
                            .is_some()
                            .then(|| less_specific.after(more_labels(), suffix)),
                    };
                    (suffix.as_bytes().into(), candidates)
                })
                .collect(),
            any: any.iter().map(|claim| claim.value.clone()).collect(),
        }
    }
}

/// The claims that can fall through onto more specific hosts, grouped for the build.
struct LessSpecific<'a, T> {
    wildcards: &'a HashMap<&'a str, Vec<&'a HostClaim<T>>>,
    any: &'a [&'a HostClaim<T>],
}

impl<'a, T: Clone> LessSpecific<'a, T> {
    /// The candidates for hosts under `name` (an exact name or a wildcard suffix): the
    /// claims on `name` itself, then whatever falls through from less specific claims,
    /// most specific first.
    fn after(&self, own: impl Iterator<Item = &'a HostClaim<T>>, name: &str) -> Box<[T]> {
        let shorter_wildcards = shorter_suffixes(name).flat_map(|(leading, suffix)| {
            // Seen from a shorter suffix, the hosts under `name` have one more label only
            // if `name` is an exact name whose first label is all that was cut off.
            let one_label = !leading.contains('.');
            self.wildcards
                .get(suffix)
                .into_iter()
                .flatten()
                .copied()
                .filter(move |claim| one_label || covers_more_labels(claim))
        });
        let inherited = shorter_wildcards
            .chain(self.any.iter().copied())
            .filter(|claim| claim.falls_through);
        own.chain(inherited)
            .map(|claim| claim.value.clone())
            .collect()
    }
}

impl<T> HostIndex<T> {
    /// The candidates for a request host, most specific claim first; empty if nothing
    /// claims it.
    ///
    /// `host` is the bare hostname, as for [`HostPattern::matches`], and the two always
    /// agree on what matches. Never allocates.
    #[must_use]
    pub fn lookup(&self, host: &str) -> &[T] {
        // Keys are lower case. Anything longer than a DNS name matches no pattern.
        let mut buffer = [0_u8; MAX_NAME_LEN];
        let Some(host) = buffer.get_mut(..host.len()).map(|buffer| {
            buffer.copy_from_slice(host.as_bytes());
            buffer.make_ascii_lowercase();
            &*buffer
        }) else {
            return &self.any;
        };

        if let Some(candidates) = self.exact.get(host) {
            return candidates;
        }
        if !self.wildcards.is_empty() {
            // Dots from the left give suffixes from the longest, so the first hit is the
            // most specific wildcard.
            let mut more_labels = false;
            for at in (0..host.len()).filter(|&at| host.get(at) == Some(&b'.')) {
                // The `*` stands for at least one byte, so a dot at the start is no match.
                if at > 0
                    && let Some(found) =
                        host.get(at..).and_then(|suffix| self.wildcards.get(suffix))
                {
                    if !more_labels {
                        return &found.one_label;
                    }
                    if let Some(candidates) = &found.more_labels {
                        return candidates;
                    }
                }
                more_labels = true;
            }
        }
        &self.any
    }
}

fn covers_more_labels<T>(claim: &&HostClaim<T>) -> bool {
    claim
        .pattern
        .as_ref()
        .is_some_and(|pattern| pattern.kind == Kind::Wildcard(WildcardLabels::OneOrMore))
}

/// Every way to cut `name` at a dot other than its first byte, longest suffix first:
/// `a.b.c` gives (`a`, `.b.c`) and (`a.b`, `.c`). These are the wildcard suffixes less
/// specific than `name` that could also cover its hosts.
fn shorter_suffixes(name: &str) -> impl Iterator<Item = (&str, &str)> {
    name.match_indices('.')
        .filter(|&(at, _)| at > 0)
        .filter_map(|(at, _)| name.split_at_checked(at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WildcardLabels::{One, OneOrMore};
    use crate::strategies::{host_near, pattern_text, wildcard_labels};
    use proptest::prelude::*;
    use std::cmp::Reverse;

    /// A claim as the tests write it: pattern text (`None` for every host), how to read a
    /// wildcard, and whether it falls through. Its value is its position in the list.
    type Spec = (Option<(String, WildcardLabels)>, bool);

    const FALLS_THROUGH: bool = true;
    const STAYS_PUT: bool = false;

    fn on(pattern: &str, wildcard: WildcardLabels, falls_through: bool) -> Spec {
        (Some((pattern.to_owned(), wildcard)), falls_through)
    }

    fn compile((pattern, _): &Spec) -> Option<HostPattern> {
        pattern
            .as_ref()
            .map(|(text, wildcard)| HostPattern::parse(text, *wildcard).unwrap())
    }

    fn index(specs: &[Spec]) -> HostIndex<usize> {
        HostIndex::new(specs.iter().enumerate().map(|(value, spec)| HostClaim {
            pattern: compile(spec),
            falls_through: spec.1,
            value,
        }))
    }

    #[test]
    fn nothing_claimed_means_no_candidates() {
        assert!(index(&[]).lookup("example.com").is_empty());
        let only_example = index(&[on("example.com", One, STAYS_PUT)]);
        assert!(only_example.lookup("example.org").is_empty());
        assert!(only_example.lookup("").is_empty());
    }

    #[test]
    fn most_specific_claim_owns_the_host() {
        let index = index(&[
            (None, STAYS_PUT),
            on("*.com", OneOrMore, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
            on("a.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("a.example.com"), [3]);
        assert_eq!(index.lookup("b.example.com"), [2]);
        assert_eq!(index.lookup("x.b.example.com"), [2]);
        assert_eq!(index.lookup("example.com"), [1]);
        assert_eq!(index.lookup("example.org"), [0]);
    }

    #[test]
    fn lookup_ignores_case() {
        let index = index(&[
            on("a.example.com", One, STAYS_PUT),
            on("*.Example.com", One, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("A.EXAMPLE.COM"), [0]);
        assert_eq!(index.lookup("B.Example.Com"), [1]);
    }

    #[test]
    fn claims_on_the_same_hosts_keep_their_order() {
        let index = index(&[
            on("example.com", One, STAYS_PUT),
            (None, STAYS_PUT),
            on("example.com", One, FALLS_THROUGH),
            (None, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("example.com"), [0, 2]);
        assert_eq!(index.lookup("example.org"), [1, 3]);
    }

    #[test]
    fn ingress_and_gateway_wildcards_with_one_suffix_are_equally_specific() {
        let index = index(&[
            on("*.example.com", OneOrMore, STAYS_PUT),
            on("*.example.com", One, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("a.example.com"), [0, 1, 2]);
        assert_eq!(index.lookup("a.b.example.com"), [0, 2]);
    }

    #[test]
    fn single_label_wildcard_does_not_hide_a_shorter_one_from_deeper_hosts() {
        let index = index(&[
            on("*.b.example.com", One, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("x.b.example.com"), [0]);
        assert_eq!(index.lookup("x.y.b.example.com"), [1]);
    }

    #[test]
    fn falling_through_claims_follow_the_more_specific_ones() {
        let index = index(&[
            (None, FALLS_THROUGH),
            on("*.com", OneOrMore, FALLS_THROUGH),
            on("*.example.com", One, FALLS_THROUGH),
            on("*.example.com", OneOrMore, FALLS_THROUGH),
            on("a.example.com", One, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("a.example.com"), [4, 2, 3, 1, 0]);
        assert_eq!(index.lookup("b.example.com"), [2, 3, 1, 0]);
        assert_eq!(index.lookup("x.b.example.com"), [3, 1, 0]);
        assert_eq!(index.lookup("example.com"), [1, 0]);
        assert_eq!(index.lookup("example.org"), [0]);
    }

    #[test]
    fn claims_that_stay_put_serve_only_hosts_nothing_more_specific_claims() {
        let index = index(&[
            (None, STAYS_PUT),
            on("*.example.com", OneOrMore, FALLS_THROUGH),
            on("*.example.com", One, STAYS_PUT),
            on("a.example.com", One, STAYS_PUT),
        ]);
        assert_eq!(index.lookup("a.example.com"), [3, 1]);
        assert_eq!(index.lookup("b.example.com"), [1, 2]);
        assert_eq!(index.lookup("example.org"), [0]);
    }

    #[test]
    fn hosts_longer_than_any_dns_name_get_only_the_claims_on_every_host() {
        let index = index(&[(None, STAYS_PUT), on("*.com", OneOrMore, STAYS_PUT)]);
        let host = format!("{}.com", "a".repeat(250));
        assert_eq!(index.lookup(&host), [0]);
    }

    #[test]
    fn hosts_that_are_not_hostnames_are_looked_up_without_panicking() {
        let index = index(&[on("*.example.com", OneOrMore, STAYS_PUT)]);
        assert_eq!(index.lookup("ä.example.com"), [0]);
        assert_eq!(index.lookup("..example.com"), [0]);
        assert!(index.lookup(".example.com").is_empty());
        assert!(index.lookup("...").is_empty());
    }

    /// The specification, written the slow and obvious way: scan every claim, keep the
    /// most specific matches plus whatever falls through, most specific first.
    fn reference(specs: &[Spec], host: &str) -> Vec<usize> {
        let specificity = |spec: &Spec| match &spec.0 {
            None => (false, 0),
            Some((text, _)) => (!text.starts_with('*'), text.len()),
        };
        let matching: Vec<(usize, &Spec)> = specs
            .iter()
            .enumerate()
            .filter(|(_, spec)| compile(spec).is_none_or(|pattern| pattern.matches(host)))
            .collect();
        let most_specific = matching.iter().map(|(_, spec)| specificity(spec)).max();
        let mut candidates: Vec<(usize, &Spec)> = matching
            .into_iter()
            .filter(|(_, spec)| spec.1 || Some(specificity(spec)) == most_specific)
            .collect();
        candidates.sort_by_key(|(position, spec)| (Reverse(specificity(spec)), *position));
        candidates
            .into_iter()
            .map(|(position, _)| position)
            .collect()
    }

    fn spec() -> impl Strategy<Value = Spec> {
        let pattern = prop_oneof![
            1 => Just(None),
            6 => (pattern_text(), wildcard_labels()).prop_map(Some),
        ];
        (pattern, any::<bool>())
    }

    /// A handful of claims and a host near one of them (or near nothing at all).
    fn case() -> impl Strategy<Value = (Vec<Spec>, String)> {
        (
            prop::collection::vec(spec(), 0..8),
            any::<prop::sample::Index>(),
        )
            .prop_flat_map(|(specs, pick)| {
                let near = specs
                    .iter()
                    .filter_map(|(pattern, _)| pattern.as_ref())
                    .map(|(text, _)| text.as_str())
                    .collect::<Vec<_>>();
                let near = if near.is_empty() {
                    "a.b"
                } else {
                    *pick.get(&near)
                };
                let host = host_near(near);
                (Just(specs), host)
            })
    }

    proptest! {
        #[test]
        fn lookup_agrees_with_the_scan_everything_reference((specs, host) in case()) {
            let index = index(&specs);
            prop_assert_eq!(index.lookup(&host), reference(&specs, &host));
        }
    }
}
