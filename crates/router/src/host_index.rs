//! The host stage of routing: from a request's host to the groups of candidates that may
//! serve it, most specific host first.
//!
//! Host specificity is textual — an exact name beats any wildcard, a longer wildcard
//! suffix beats a shorter one, and anything beats a claim on every host. Ingress and
//! Gateway API wildcards with the same suffix are equally specific. Claims that are equally
//! specific for a host form one **group**: the later stages of routing choose among the
//! members of a group, and only turn to the next group when none of them serves.
//!
//! Whether less specific claims stay candidates on a host that something more specific
//! also claims is decided per claim ([`HostClaim::falls_through`]): Gateway API requires
//! it, nginx-style Ingress does not want it. Either way it is resolved when the index is
//! built. Every host's list of groups is laid out then, so a request pays for one lookup;
//! and a group that falls through onto many hosts is built once and shared, not copied.

use crate::WildcardLabels;
use crate::hash::Map;
use crate::host::{HostPattern, Kind, MAX_NAME_LEN};
use std::collections::{BTreeMap, HashMap};

/// One claim on a set of hosts, carrying whatever the caller wants to find again.
#[derive(Debug, Clone)]
pub struct HostClaim<T> {
    /// The hosts claimed; `None` claims every host.
    pub pattern: Option<HostPattern>,
    /// Whether `value` stays a candidate, after the more specific ones, on hosts that a
    /// more specific claim also covers. Without it, the claim serves only hosts that
    /// nothing more specific claims.
    pub falls_through: bool,
    /// What the claim contributes to the groups it is a member of.
    pub value: T,
}

/// An immutable index from request host to groups of candidates, built once per config
/// snapshot. What a group is is up to the caller: it is made from the values of its members.
#[derive(Debug)]
pub struct HostIndex<G> {
    groups: Vec<G>,
    /// Positions in `groups`, most specific first.
    exact: Map<Box<[usize]>>,
    /// Keyed by wildcard suffix, leading dot included (`.example.com`).
    wildcards: Map<WildcardGroups>,
    /// For hosts no pattern covers.
    any: Box<[usize]>,
}

#[derive(Debug)]
struct WildcardGroups {
    /// For hosts with exactly one label before the suffix: both wildcard kinds apply.
    one_label: Box<[usize]>,
    /// For hosts with more labels before the suffix; `None` if only single-label
    /// wildcards claim this suffix, in which case a shorter suffix may still match.
    more_labels: Option<Box<[usize]>>,
}

impl<G> HostIndex<G> {
    /// Builds the index. `group` makes a group from the values of its members, which come
    /// in the order the claims were given in; it is called once for every distinct group.
    pub fn new<T: Clone>(
        claims: impl IntoIterator<Item = HostClaim<T>>,
        group: impl FnMut(Vec<T>) -> G,
    ) -> Self {
        let claims: Vec<HostClaim<T>> = claims.into_iter().collect();

        // Claims by what they claim, each with its position: the position is what tells
        // two selections of members apart or shows them to be the same group. In order of
        // the name, so that the groups and the tables built from going through them are laid
        // out the same on every build: a lookup costs what the layout makes it, and a
        // benchmark that counts instructions must see the same index every run.
        let mut exact: BTreeMap<&str, Vec<Claimed<'_, T>>> = BTreeMap::new();
        let mut wildcards: BTreeMap<&str, Vec<Claimed<'_, T>>> = BTreeMap::new();
        let mut any = Vec::new();
        for claimed in claims.iter().enumerate() {
            match &claimed.1.pattern {
                None => any.push(claimed),
                Some(pattern) => match pattern.kind {
                    Kind::Exact => exact.entry(&pattern.name).or_default().push(claimed),
                    Kind::Wildcard(_) => {
                        wildcards.entry(&pattern.name).or_default().push(claimed);
                    }
                },
            }
        }

        let mut groups = Groups {
            wildcards: &wildcards,
            any: &any,
            make: group,
            made: Vec::new(),
            chosen: HashMap::new(),
            known: HashMap::new(),
        };
        let exact = exact
            .iter()
            .map(|(&name, own)| {
                let own = groups.of(name, own, Members::ALL);
                (name.as_bytes().into(), groups.from(own, name))
            })
            .collect();
        let by_suffix = wildcards
            .iter()
            .map(|(&suffix, own)| {
                let one_label = groups.of(suffix, own, Members::ALL);
                let more_labels = groups.of(suffix, own, Members::MORE_LABELS);
                let found = WildcardGroups {
                    one_label: groups.from(one_label, suffix),
                    more_labels: more_labels.map(|own| groups.from(Some(own), suffix)),
                };
                (suffix.as_bytes().into(), found)
            })
            .collect();
        let any = groups.of("", &any, Members::ALL).into_iter().collect();
        Self {
            groups: groups.made,
            exact,
            wildcards: by_suffix,
            any,
        }
    }

    /// The groups of candidates for a request host, most specific claim first; none if
    /// nothing claims it.
    ///
    /// `host` is the bare hostname, as for [`HostPattern::matches`], and the two always
    /// agree on what matches. Never allocates.
    pub fn lookup(&self, host: &str) -> impl Iterator<Item = &G> + use<'_, G> {
        self.positions(host)
            .iter()
            .filter_map(|&position| self.groups.get(position))
    }

    fn positions(&self, host: &str) -> &[usize] {
        // Keys are lower case. Anything longer than a DNS name matches no pattern.
        let mut buffer = [0_u8; MAX_NAME_LEN];
        let Some(host) = buffer.get_mut(..host.len()).map(|buffer| {
            buffer.copy_from_slice(host.as_bytes());
            buffer.make_ascii_lowercase();
            &*buffer
        }) else {
            return &self.any;
        };

        if let Some(groups) = self.exact.get(host) {
            return groups;
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
                    if let Some(groups) = &found.more_labels {
                        return groups;
                    }
                }
                more_labels = true;
            }
        }
        &self.any
    }
}

/// Which of the claims on one name or suffix are members of a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Members {
    /// Only wildcards that stand for one or more labels: hosts further down than one label
    /// are not covered by the other kind.
    more_labels_only: bool,
    /// Only claims that fall through: the group is for hosts something more specific claims.
    falling_through_only: bool,
}

impl Members {
    const ALL: Self = Self {
        more_labels_only: false,
        falling_through_only: false,
    };
    const MORE_LABELS: Self = Self {
        more_labels_only: true,
        falling_through_only: false,
    };
}

/// A claim with its position among all claims.
type Claimed<'a, T> = (usize, &'a HostClaim<T>);

/// The groups made so far while building, each made once however many hosts it serves.
struct Groups<'a, T, G, F> {
    wildcards: &'a BTreeMap<&'a str, Vec<Claimed<'a, T>>>,
    any: &'a [Claimed<'a, T>],
    make: F,
    made: Vec<G>,
    /// What was chosen before, by the name or suffix claimed (empty for every host) and the
    /// choice of members; `None` for a choice that leaves no members.
    chosen: HashMap<(&'a str, Members), Option<usize>>,
    /// The groups made, by the positions of their members: different choices often come to
    /// the same members, and those are one group.
    known: HashMap<Vec<usize>, usize>,
}

impl<'a, T: Clone, G, F: FnMut(Vec<T>) -> G> Groups<'a, T, G, F> {
    /// The position of the group of `members` among the claims on `name`.
    fn of(&mut self, name: &'a str, claims: &[Claimed<'a, T>], members: Members) -> Option<usize> {
        if let Some(&chosen) = self.chosen.get(&(name, members)) {
            return chosen;
        }
        let selected: Vec<Claimed<'a, T>> = claims
            .iter()
            .filter(|(_, claim)| !members.more_labels_only || covers_more_labels(claim))
            .filter(|(_, claim)| !members.falling_through_only || claim.falls_through)
            .copied()
            .collect();
        let group = (!selected.is_empty()).then(|| {
            let positions = selected.iter().map(|(position, _)| *position).collect();
            *self.known.entry(positions).or_insert_with(|| {
                let values = selected.iter().map(|(_, claim)| claim.value.clone());
                self.made.push((self.make)(values.collect()));
                self.made.len() - 1
            })
        });
        self.chosen.insert((name, members), group);
        group
    }

    /// The groups for hosts under `name` (an exact name or a wildcard suffix): the group of
    /// the claims on `name` itself, then whatever falls through from less specific claims,
    /// most specific first.
    fn from(&mut self, own: Option<usize>, name: &'a str) -> Box<[usize]> {
        let wildcards = self.wildcards;
        let any = self.any;
        let mut positions: Vec<usize> = own.into_iter().collect();
        for (leading, suffix) in shorter_suffixes(name) {
            // Seen from a shorter suffix, the hosts under `name` have one more label only
            // if `name` is an exact name whose first label is all that was cut off.
            let members = Members {
                more_labels_only: leading.contains('.'),
                falling_through_only: true,
            };
            if let Some(claims) = wildcards.get(suffix) {
                positions.extend(self.of(suffix, claims, members));
            }
        }
        let members = Members {
            more_labels_only: false,
            falling_through_only: true,
        };
        positions.extend(self.of("", any, members));
        positions.into_boxed_slice()
    }
}

fn covers_more_labels<T>(claim: &HostClaim<T>) -> bool {
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
    use crate::reference;
    use crate::strategies::{host_near, pattern_text, wildcard_labels};
    use proptest::prelude::*;

    /// A claim as the tests write it; its value is its position in the list.
    type Spec = reference::HostClaimSpec;

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

    fn claims(specs: &[Spec]) -> impl Iterator<Item = HostClaim<usize>> {
        specs.iter().enumerate().map(|(value, spec)| HostClaim {
            pattern: compile(spec),
            falls_through: spec.1,
            value,
        })
    }

    /// An index whose groups are simply the positions of their members.
    fn index(specs: &[Spec]) -> HostIndex<Vec<usize>> {
        HostIndex::new(claims(specs), |members| members)
    }

    fn lookup(index: &HostIndex<Vec<usize>>, host: &str) -> Vec<Vec<usize>> {
        index.lookup(host).cloned().collect()
    }

    #[test]
    fn nothing_claimed_means_no_groups() {
        assert!(lookup(&index(&[]), "example.com").is_empty());
        let only_example = index(&[on("example.com", One, STAYS_PUT)]);
        assert!(lookup(&only_example, "example.org").is_empty());
        assert!(lookup(&only_example, "").is_empty());
    }

    #[test]
    fn most_specific_claim_owns_the_host() {
        let index = index(&[
            (None, STAYS_PUT),
            on("*.com", OneOrMore, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
            on("a.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "a.example.com"), [[3]]);
        assert_eq!(lookup(&index, "b.example.com"), [[2]]);
        assert_eq!(lookup(&index, "x.b.example.com"), [[2]]);
        assert_eq!(lookup(&index, "example.com"), [[1]]);
        assert_eq!(lookup(&index, "example.org"), [[0]]);
    }

    #[test]
    fn lookup_ignores_case() {
        let index = index(&[
            on("a.example.com", One, STAYS_PUT),
            on("*.Example.com", One, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "A.EXAMPLE.COM"), [[0]]);
        assert_eq!(lookup(&index, "B.Example.Com"), [[1]]);
    }

    #[test]
    fn claims_on_the_same_hosts_are_one_group_in_the_order_given() {
        let index = index(&[
            on("example.com", One, STAYS_PUT),
            (None, STAYS_PUT),
            on("example.com", One, FALLS_THROUGH),
            (None, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "example.com"), [[0, 2]]);
        assert_eq!(lookup(&index, "example.org"), [[1, 3]]);
    }

    #[test]
    fn ingress_and_gateway_wildcards_with_one_suffix_are_equally_specific() {
        let index = index(&[
            on("*.example.com", OneOrMore, STAYS_PUT),
            on("*.example.com", One, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "a.example.com"), [[0, 1, 2]]);
        assert_eq!(lookup(&index, "a.b.example.com"), [[0, 2]]);
    }

    #[test]
    fn single_label_wildcard_does_not_hide_a_shorter_one_from_deeper_hosts() {
        let index = index(&[
            on("*.b.example.com", One, STAYS_PUT),
            on("*.example.com", OneOrMore, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "x.b.example.com"), [[0]]);
        assert_eq!(lookup(&index, "x.y.b.example.com"), [[1]]);
    }

    #[test]
    fn falling_through_claims_follow_in_groups_of_their_own() {
        let index = index(&[
            (None, FALLS_THROUGH),
            on("*.com", OneOrMore, FALLS_THROUGH),
            on("*.example.com", One, FALLS_THROUGH),
            on("*.example.com", OneOrMore, FALLS_THROUGH),
            on("a.example.com", One, STAYS_PUT),
        ]);
        assert_eq!(
            lookup(&index, "a.example.com"),
            [vec![4], vec![2, 3], vec![1], vec![0]]
        );
        assert_eq!(
            lookup(&index, "b.example.com"),
            [vec![2, 3], vec![1], vec![0]]
        );
        assert_eq!(lookup(&index, "x.b.example.com"), [[3], [1], [0]]);
        assert_eq!(lookup(&index, "example.com"), [[1], [0]]);
        assert_eq!(lookup(&index, "example.org"), [[0]]);
    }

    #[test]
    fn claims_that_stay_put_serve_only_hosts_nothing_more_specific_claims() {
        let index = index(&[
            (None, STAYS_PUT),
            on("*.example.com", OneOrMore, FALLS_THROUGH),
            on("*.example.com", One, STAYS_PUT),
            on("a.example.com", One, STAYS_PUT),
        ]);
        assert_eq!(lookup(&index, "a.example.com"), [[3], [1]]);
        assert_eq!(lookup(&index, "b.example.com"), [[1, 2]]);
        assert_eq!(lookup(&index, "example.org"), [[0]]);
    }

    #[test]
    fn a_group_is_made_once_however_many_hosts_it_serves() {
        let mut specs = vec![
            (None, FALLS_THROUGH),
            on("*.example.com", OneOrMore, FALLS_THROUGH),
        ];
        specs.extend((0..100).map(|n| on(&format!("host-{n}.example.com"), One, STAYS_PUT)));
        let mut made = Vec::new();
        let index = HostIndex::new(claims(&specs), |members| made.push(members));
        assert_eq!(index.lookup("host-7.example.com").count(), 3);
        // One group per exact host; the wildcard's group and the group of every host once
        // each, shared by the hundred hosts and by the hosts they own themselves.
        assert_eq!(made.len(), 102);
        assert_eq!(made.iter().filter(|members| **members == [1]).count(), 1);
        assert_eq!(made.iter().filter(|members| **members == [0]).count(), 1);
    }

    #[test]
    fn an_index_is_laid_out_the_same_however_often_it_is_built() {
        // Enough names that a build following some randomly seeded order would show it in
        // the order of its groups and of its tables, which is what a lookup's cost and a
        // benchmark's count depend on.
        let specs: Vec<Spec> = (0..50)
            .map(|n| on(&format!("host-{n}.example.com"), One, STAYS_PUT))
            .chain(
                (0..10).map(|n| on(&format!("*.wild-{n}.example.com"), OneOrMore, FALLS_THROUGH)),
            )
            .chain([(None, FALLS_THROUGH)])
            .collect();
        let first = format!("{:?}", index(&specs));
        for _ in 0..20 {
            assert_eq!(format!("{:?}", index(&specs)), first);
        }
    }

    #[test]
    fn hosts_longer_than_any_dns_name_get_only_the_claims_on_every_host() {
        let index = index(&[(None, STAYS_PUT), on("*.com", OneOrMore, STAYS_PUT)]);
        let host = format!("{}.com", "a".repeat(250));
        assert_eq!(lookup(&index, &host), [[0]]);
    }

    #[test]
    fn hosts_that_are_not_hostnames_are_looked_up_without_panicking() {
        let index = index(&[on("*.example.com", OneOrMore, STAYS_PUT)]);
        assert_eq!(lookup(&index, "ä.example.com"), [[0]]);
        assert_eq!(lookup(&index, "..example.com"), [[0]]);
        assert!(lookup(&index, ".example.com").is_empty());
        assert!(lookup(&index, "...").is_empty());
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
            prop_assert_eq!(lookup(&index, &host), reference::host_candidates(&specs, &host));
        }
    }
}
