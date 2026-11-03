//! What the synchronous mode keeps under each key, and the rules its owner decides requests by:
//! each part of a value only rises, so that two copies merge by keeping the larger of each, in
//! whatever order they meet.

use edgerush_cluster::table::mix;
use edgerush_cluster::{Held, Time};
use std::collections::{BTreeMap, BTreeSet};

/// A key's value, its shape the rule's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// A count, plain or in a fixed window: the window it counts in, a later one replacing it.
    Count {
        /// The number of the window counted in; 0 for a plain count.
        window: u64,
        /// Requests counted under the key, in that window.
        counted: u64,
        /// Of those, the ones given back, as charged for a request then refused.
        given: u64,
    },
    /// A rolling window's counts, by slot: each slot counted and given back.
    Slots(BTreeMap<u64, (u64, u64)>),
    /// A bucket, as GCRA keeps one: the time its next request is due at, and the requests
    /// given back, each an emission interval taken off it.
    Bucket {
        /// The theoretical arrival time, the give-backs not taken off.
        due: Time,
        /// Requests given back.
        given: u64,
    },
}

impl Default for Value {
    fn default() -> Self {
        Self::counted(0)
    }
}

impl Held for Value {
    fn merge(&mut self, other: &Self) -> bool {
        Self::merge(self, other)
    }

    /// A count's three parts, a slot's number with its two counts each, a bucket's two.
    fn bytes(&self) -> u64 {
        match self {
            Self::Count { .. } => 24,
            Self::Slots(slots) => 8 + 24 * slots.len() as u64,
            Self::Bucket { .. } => 16,
        }
    }

    /// Of a rolling window, the slots `parts` names alone; of any other shape, the whole.
    fn only(&self, parts: &BTreeSet<u64>) -> Self {
        match self {
            Self::Slots(slots) => Self::Slots(
                slots
                    .iter()
                    .filter(|(slot, _)| parts.contains(slot))
                    .map(|(slot, counts)| (*slot, *counts))
                    .collect(),
            ),
            Self::Count { .. } | Self::Bucket { .. } => self.clone(),
        }
    }
}

impl Value {
    /// A count of `counted`, nothing given back.
    #[must_use]
    pub const fn counted(counted: u64) -> Self {
        Self::Count {
            window: 0,
            counted,
            given: 0,
        }
    }

    /// What stands of a count: counted less given back; nothing for any other shape.
    #[must_use]
    pub const fn net(&self) -> u64 {
        match self {
            Self::Count { counted, given, .. } => counted.saturating_sub(*given),
            Self::Slots(_) | Self::Bucket { .. } => 0,
        }
    }

    /// A count's window; 0 for any other shape.
    #[must_use]
    pub const fn window(&self) -> u64 {
        match self {
            Self::Count { window, .. } => *window,
            Self::Slots(_) | Self::Bucket { .. } => 0,
        }
    }

    /// Counts one, under no rule.
    pub fn count_one(&mut self) {
        if let Self::Count { counted, .. } = self {
            *counted += 1;
        } else {
            *self = Self::counted(1);
        }
    }

    /// Gives one back, under no rule.
    pub const fn give_one(&mut self) {
        if let Self::Count { given, .. } = self {
            *given += 1;
        }
    }

    /// Takes in `other`, a copy of the same key: for a count, a later window in place of this
    /// one, else the larger of each part; for slots, the larger of each part of each slot; for
    /// a bucket, the larger of each part. A copy of another shape, the rule having changed,
    /// replaces a value never charged. Whether it raised anything.
    pub fn merge(&mut self, other: &Self) -> bool {
        let before = self.clone();
        match (&mut *self, other) {
            (
                Self::Count {
                    window,
                    counted,
                    given,
                },
                Self::Count {
                    window: theirs,
                    counted: their_counted,
                    given: their_given,
                },
            ) => {
                if *theirs > *window {
                    *self = other.clone();
                } else if *theirs == *window {
                    *counted = (*counted).max(*their_counted);
                    *given = (*given).max(*their_given);
                }
            }
            (Self::Slots(slots), Self::Slots(theirs)) => {
                for (slot, (counted, given)) in theirs {
                    let held = slots.entry(*slot).or_insert((0, 0));
                    held.0 = held.0.max(*counted);
                    held.1 = held.1.max(*given);
                }
                // Only the window's slots back from the newest, as the owner keeps them, or a
                // replica of a key kept busy would hold a slot for every admission ever: in any
                // order, as a slot left out is out of every later window too.
                if let Some(newest) = slots.keys().next_back().copied() {
                    let oldest = newest.saturating_sub(SLOTS - 1);
                    slots.retain(|slot, _| *slot >= oldest);
                }
            }
            (
                Self::Bucket { due, given },
                Self::Bucket {
                    due: their_due,
                    given: their_given,
                },
            ) => {
                *due = (*due).max(*their_due);
                *given = (*given).max(*their_given);
            }
            _ => {
                if *self == Self::default() {
                    *self = other.clone();
                }
            }
        }
        *self != before
    }
}

/// The limit a request is held to, decided by its key's owner by its wall clock (docs/23 §1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// At most `per` in each window of length `window`, aligned to the clock, each key on a
    /// phase of its own from a hash of the key: the model built in stage 4a.
    Window {
        /// Requests admitted a window.
        per: u64,
        /// The window.
        window: Time,
    },
    /// At most `per` in any trailing window of length `window`, to a thousandth of it: each
    /// admission counted in a slot a thousandth of the window long, its time rounded down, so
    /// that it is never stricter than exact, and admits at most `per` in any span of 99.9% of
    /// the window.
    Rolling {
        /// Requests admitted in any window.
        per: u64,
        /// The window.
        window: Time,
    },
    /// A bucket, by GCRA: `per` every `period` on average, `burst` at once from idle — at
    /// most `per` × T / `period` + `burst` in any span T; a new key starts full.
    Bucket {
        /// Requests a period, on average.
        per: u64,
        /// The period.
        period: Time,
        /// Requests at once from idle: the whole bucket, at least 1.
        burst: u64,
    },
}

/// The slots a rolling window counts in.
const SLOTS: u64 = 1_000;

impl Rule {
    /// The number of the window `key` is in at `at` under a fixed window, counted from 1; for
    /// a rolling window the slot; 0 for a bucket.
    #[must_use]
    pub fn window_of(&self, key: u64, at: Time) -> u64 {
        match *self {
            Self::Window { window, .. } => {
                let window = window.max(1);
                (at + mix(key) % window) / window + 1
            }
            Self::Rolling { window, .. } => at / (window / SLOTS).max(1),
            Self::Bucket { .. } => 0,
        }
    }

    /// A bucket's emission interval and tolerance: the time a request takes to drip back, and
    /// how far ahead of now its next request may be due and still be admitted.
    #[must_use]
    pub const fn interval_and_tolerance(per: u64, period: Time, burst: u64) -> (Time, Time) {
        let interval = period / if per == 0 { 1 } else { per };
        let burst = if burst == 0 { 1 } else { burst };
        (interval, interval * (burst - 1))
    }

    /// Charges `value`, under `key`, one at `at` if it has room: the receipt — the window or
    /// slot charged, 0 for a bucket — if so; a refusal charges nothing.
    pub fn admit(&self, key: u64, value: &mut Value, at: Time) -> Option<u64> {
        match *self {
            Self::Window { per, .. } => {
                let now = self.window_of(key, at);
                if !matches!(value, Value::Count { .. }) {
                    *value = Value::default();
                }
                let Value::Count {
                    window,
                    counted,
                    given,
                } = value
                else {
                    return None;
                };
                // A window held later than `at`'s — copied from an owner whose clock runs
                // ahead — is the one charged: a window never goes back.
                if *window < now {
                    *window = now;
                    *counted = 0;
                    *given = 0;
                }
                if counted.saturating_sub(*given) >= per {
                    return None;
                }
                *counted += 1;
                Some(*window)
            }
            Self::Rolling { per, .. } => {
                let slot = self.window_of(key, at);
                if !matches!(value, Value::Slots(_)) {
                    *value = Value::Slots(BTreeMap::new());
                }
                let Value::Slots(slots) = value else {
                    return None;
                };
                // The slots the window covers: this one and those of the rest of the window
                // before it; any older forgotten.
                let oldest = slot.saturating_sub(SLOTS - 1);
                slots.retain(|held, _| *held >= oldest);
                let standing: u64 = slots
                    .values()
                    .map(|(counted, given)| counted.saturating_sub(*given))
                    .sum();
                if standing >= per {
                    return None;
                }
                slots.entry(slot).or_insert((0, 0)).0 += 1;
                Some(slot)
            }
            Self::Bucket { per, period, burst } => {
                let (interval, tolerance) = Self::interval_and_tolerance(per, period, burst);
                if !matches!(value, Value::Bucket { .. }) {
                    *value = Value::Bucket { due: 0, given: 0 };
                }
                let Value::Bucket { due, given } = value else {
                    return None;
                };
                let credit = given.saturating_mul(interval);
                let next = due.saturating_sub(credit).max(at);
                if next - at > tolerance {
                    return None;
                }
                *due = next + interval + credit;
                Some(0)
            }
        }
    }

    /// From when `value`, kept under `key`, may be forgotten, by the wall clock, having the
    /// key behave as a new one from then (docs/23 §3): a window's at its end, a rolling
    /// window's once its newest slot has left the window, a bucket's once it is due, full
    /// again.
    #[must_use]
    pub fn forget_at(&self, key: u64, value: &Value) -> Time {
        match (*self, value) {
            (Self::Window { window, .. }, Value::Count { window: number, .. }) => {
                let window = window.max(1);
                number
                    .saturating_mul(window)
                    .saturating_sub(mix(key) % window)
            }
            (Self::Rolling { window, .. }, Value::Slots(slots)) => {
                let length = (window / SLOTS).max(1);
                slots
                    .keys()
                    .next_back()
                    .map_or(0, |newest| (newest + SLOTS).saturating_mul(length))
            }
            (Self::Bucket { .. }, Value::Bucket { due, .. }) => *due,
            _ => 0,
        }
    }

    /// How long after `at` a request under `key` would be admitted, `value` having just
    /// refused one at `at`, nothing changing it meanwhile: until the window ends; until enough
    /// of a rolling window's oldest slots have left it; until the bucket's next request is due,
    /// less its tolerance. 0 when one would be admitted at `at`.
    #[must_use]
    pub fn wait(&self, key: u64, value: &Value, at: Time) -> Time {
        let until = match (*self, value) {
            (Self::Window { per, .. }, Value::Count { counted, given, .. }) => {
                if counted.saturating_sub(*given) < per {
                    0
                } else {
                    self.forget_at(key, value)
                }
            }
            (Self::Rolling { per, window }, Value::Slots(slots)) => {
                let length = (window / SLOTS).max(1);
                let net = |(counted, given): &(u64, u64)| counted.saturating_sub(*given);
                let standing: u64 = slots.values().map(net).sum();
                // The oldest slots that must leave the window for one more to fit, the last of
                // them leaving it once its slot number and the window's have passed.
                let excess = (standing + 1).saturating_sub(per);
                let mut freed = 0;
                slots
                    .iter()
                    .find_map(|(slot, counts)| {
                        freed += net(counts);
                        (excess > 0 && freed >= excess)
                            .then(|| (slot + SLOTS).saturating_mul(length))
                    })
                    .unwrap_or(0)
            }
            (Self::Bucket { per, period, burst }, Value::Bucket { due, given }) => {
                let (interval, tolerance) = Self::interval_and_tolerance(per, period, burst);
                due.saturating_sub(given.saturating_mul(interval))
                    .saturating_sub(tolerance)
            }
            _ => 0,
        };
        until.saturating_sub(at)
    }

    /// Gives back a charge made under `receipt`: for a window, nothing once its window has
    /// passed, as the window's count went with it; for a rolling window, nothing once its slot
    /// has been forgotten; for a bucket, an emission interval off the bucket, which may give
    /// back up to one request too many if it has refilled since (docs/23 §3).
    pub fn give_back(&self, value: &mut Value, receipt: u64) {
        match value {
            Value::Count { window, given, .. } if *window == receipt => *given += 1,
            Value::Slots(slots) => {
                if let Some((_, given)) = slots.get_mut(&receipt) {
                    *given += 1;
                }
            }
            Value::Bucket { given, .. } if matches!(self, Self::Bucket { .. }) => *given += 1,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::ms;

    fn count(window: u64, counted: u64, given: u64) -> Value {
        Value::Count {
            window,
            counted,
            given,
        }
    }

    /// Refused, a request is admitted once the wait it was told has passed, and not a
    /// nanosecond before, under each rule.
    #[test]
    fn a_refused_request_is_admitted_after_its_wait_and_not_before() {
        let rules = [
            Rule::Window {
                per: 3,
                window: ms(100),
            },
            Rule::Rolling {
                per: 3,
                window: ms(100),
            },
            Rule::Bucket {
                per: 3,
                period: ms(100),
                burst: 2,
            },
        ];
        for rule in rules {
            for key in 0..20 {
                let mut value = Value::default();
                let mut at = ms(1_000) + key * 7_919;
                while rule.admit(key, &mut value, at).is_some() {
                    at += ms(10);
                }
                let wait = rule.wait(key, &value, at);
                assert!(wait > 0, "{rule:?} key {key}: no wait");
                let mut early = value.clone();
                assert_eq!(
                    rule.admit(key, &mut early, at + wait - 1),
                    None,
                    "{rule:?} key {key}: admitted before its wait {wait}"
                );
                let mut late = value.clone();
                assert!(
                    rule.admit(key, &mut late, at + wait).is_some(),
                    "{rule:?} key {key}: refused after its wait {wait}"
                );
            }
        }
    }

    /// From the time a value may be forgotten, it is decided as a key never seen, so forgetting
    /// it then changes nothing; a nanosecond before, it is not, so it is never forgotten early.
    #[test]
    fn a_key_forgotten_from_its_time_behaves_as_a_new_one() {
        let rules = [
            Rule::Window {
                per: 2,
                window: ms(1_000),
            },
            Rule::Rolling {
                per: 2,
                window: ms(1_000),
            },
            Rule::Bucket {
                per: 2,
                period: ms(1_000),
                burst: 2,
            },
        ];
        for rule in rules {
            let mut value = Value::default();
            for at in [ms(5_000), ms(5_100), ms(5_200)] {
                let _ = rule.admit(7, &mut value, at);
            }
            let forget_at = rule.forget_at(7, &value);
            assert!(forget_at > ms(5_200), "{rule:?}");
            // A burst at one instant, decided for the value kept and for a new key.
            let burst = |at: Time| {
                let (mut kept, mut fresh) = (value.clone(), Value::default());
                (0..4).all(|_| {
                    rule.admit(7, &mut kept, at).is_some()
                        == rule.admit(7, &mut fresh, at).is_some()
                })
            };
            assert!(burst(forget_at), "{rule:?}: not as new at its time");
            assert!(!burst(forget_at - 1), "{rule:?}: as new before its time");
        }
    }

    /// A rolling window's copies merged keep only the window's slots back from the newest, as
    /// the owner does: a replica of a key kept busy holds no more.
    #[test]
    fn merged_slots_keep_only_the_window() {
        let slots = |pairs: &[(u64, (u64, u64))]| Value::Slots(pairs.iter().copied().collect());
        // The window ending at slot 1 500 starts at 501.
        let mut held = slots(&[(1, (1, 0)), (500, (1, 0)), (501, (1, 0))]);
        let _ = held.merge(&slots(&[(1_500, (1, 0))]));
        assert_eq!(held, slots(&[(501, (1, 0)), (1_500, (1, 0))]));
        let _ = held.merge(&slots(&[(1, (2, 0))]));
        assert_eq!(held, slots(&[(501, (1, 0)), (1_500, (1, 0))]));
    }

    /// Copies met in any order, or twice, come to the same value, whatever its shape.
    #[test]
    fn copies_merge_to_the_same_value_in_any_order() {
        let slots = |pairs: &[(u64, (u64, u64))]| Value::Slots(pairs.iter().copied().collect());
        for (a, b) in [
            (count(0, 5, 1), count(0, 3, 2)),
            (count(4, 5, 1), count(5, 1, 0)),
            (
                slots(&[(1, (2, 0)), (2, (1, 0))]),
                slots(&[(2, (3, 1)), (3, (1, 0))]),
            ),
            (
                Value::Bucket { due: 9, given: 1 },
                Value::Bucket { due: 7, given: 2 },
            ),
        ] {
            let mut ab = a.clone();
            ab.merge(&b);
            let mut ba = b.clone();
            ba.merge(&a);
            assert_eq!(ab, ba);
            assert!(!ab.merge(&a), "nothing raised the second time");
        }
    }

    /// A window admits its limit and no more, a refusal charging nothing; the next window
    /// starts afresh; a give-back makes room in its own window only.
    #[test]
    fn a_window_admits_its_limit_and_starts_afresh() {
        let rule = Rule::Window {
            per: 2,
            window: ms(100),
        };
        let key = 7;
        let mut value = Value::default();
        let start = (0..ms(100))
            .step_by(1_000_000)
            .find(|at| rule.window_of(key, *at) != rule.window_of(key, at + 1_000_000))
            .map_or(0, |at| at + 1_000_000);
        let first = rule.admit(key, &mut value, start).unwrap();
        assert!(rule.admit(key, &mut value, start + ms(1)).is_some());
        assert_eq!(rule.admit(key, &mut value, start + ms(2)), None);
        assert_eq!(value, count(first, 2, 0), "a refusal charged");
        rule.give_back(&mut value, first);
        assert!(rule.admit(key, &mut value, start + ms(3)).is_some());
        let next = rule.admit(key, &mut value, start + ms(100));
        assert_eq!(next, Some(first + 1));
        rule.give_back(&mut value, first);
        assert_eq!(value.net(), 1, "given back in a window gone by");
    }

    /// An owner whose clock is behind, holding a copy counted in a later window — from an
    /// owner whose clock runs ahead — charges that window, never going back to its own: going
    /// back would forget what was counted, and count the window again.
    #[test]
    fn a_window_never_goes_back() {
        let rule = Rule::Window {
            per: 3,
            window: ms(100),
        };
        let key = 7;
        let behind = rule.window_of(key, ms(1_000));
        let mut value = count(behind + 1, 2, 0);
        assert_eq!(rule.admit(key, &mut value, ms(1_000)), Some(behind + 1));
        assert_eq!(value, count(behind + 1, 3, 0));
        assert_eq!(
            rule.admit(key, &mut value, ms(1_000)),
            None,
            "a window counted again"
        );
    }

    /// A rolling window admits at most its limit in any trailing window, and room comes back
    /// as each admission leaves it — up to a slot early, never late.
    #[test]
    fn a_rolling_window_admits_its_limit_in_any_trailing_window() {
        let rule = Rule::Rolling {
            per: 2,
            window: ms(1_000),
        };
        let mut value = Value::default();
        assert!(rule.admit(1, &mut value, ms(10)).is_some());
        assert!(rule.admit(1, &mut value, ms(500)).is_some());
        assert_eq!(rule.admit(1, &mut value, ms(900)), None);
        assert_eq!(
            rule.admit(1, &mut value, ms(1_008)),
            None,
            "room back early"
        );
        assert!(
            rule.admit(1, &mut value, ms(1_010)).is_some(),
            "room back late"
        );
        assert_eq!(rule.admit(1, &mut value, ms(1_200)), None);
        let receipt = rule.admit(1, &mut value, ms(1_501)).unwrap();
        rule.give_back(&mut value, receipt);
        assert!(
            rule.admit(1, &mut value, ms(1_502)).is_some(),
            "not given back"
        );
    }

    /// A bucket, starting full, admits its burst at once, then one an emission interval; a
    /// refusal charges nothing; a give-back admits one more.
    #[test]
    fn a_bucket_admits_its_burst_then_its_rate() {
        let rule = Rule::Bucket {
            per: 10,
            period: ms(1_000),
            burst: 3,
        };
        let mut value = Value::default();
        let at = ms(5_000);
        for _ in 0..3 {
            assert!(rule.admit(1, &mut value, at).is_some());
        }
        assert_eq!(rule.admit(1, &mut value, at), None);
        let held = value.clone();
        assert_eq!(rule.admit(1, &mut value, at + ms(99)), None);
        assert_eq!(value, held, "a refusal charged");
        assert!(rule.admit(1, &mut value, at + ms(100)).is_some());
        assert_eq!(rule.admit(1, &mut value, at + ms(100)), None);
        rule.give_back(&mut value, 0);
        assert!(
            rule.admit(1, &mut value, at + ms(100)).is_some(),
            "not given back"
        );
    }
}
