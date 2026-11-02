//! What the store holds for each key: a value of the mode's own, which the store copies, hands
//! on, forgets and evicts without reading, and the time from which it may be forgotten.

use crate::Time;
use std::collections::BTreeSet;
use std::fmt::Debug;

/// A mode's value under one key, as the store needs it. Each part only rises, so that two
/// copies merge by keeping the larger of each, in whatever order they meet: the store sends a
/// replica an owner's copies as they come, again after a loss, and from more than one owner
/// across a failover or a handoff.
pub trait Held: Clone + Debug + Default + PartialEq {
    /// Takes in `other`, a copy of the same key: whether it raised anything.
    fn merge(&mut self, other: &Self) -> bool;

    /// About the bytes its parts take, held or sent: what `max_key_bytes` counts, with the key
    /// and its time to forget at.
    fn bytes(&self) -> u64;

    /// Of it, only `parts`, as the mode numbers its value's parts — a rolling window's slots:
    /// what the next copy carries of a key changed only there. Parts merge each by the larger,
    /// so a part is as safe to copy as the whole. The whole, for a value with no parts.
    #[must_use]
    fn only(&self, parts: &BTreeSet<u64>) -> Self {
        let _ = parts;
        self.clone()
    }
}

/// A key's value as a pod keeps it, with the time from which it may be forgotten, by the wall
/// clock: set by its owner, as its mode has it, which a replica never sees, so that every pod
/// forgets it by its own clock, with no message. [`Time::MAX`] for never.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Kept<V> {
    /// The value.
    pub value: V,
    /// From when it may be forgotten; [`Time::MAX`] for never.
    pub forget_at: Time,
}

impl<V> Kept<V> {
    /// `value`, never forgotten.
    pub const fn new(value: V) -> Self {
        Self {
            value,
            forget_at: Time::MAX,
        }
    }
}

impl<V: Held> Kept<V> {
    /// About the bytes it takes with its key, held or sent: the key, the time to forget at and
    /// the value's parts.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        16 + self.value.bytes()
    }

    /// Merges `other` in, the value as [`Held::merge`] has it and the later time to forget at:
    /// whether anything rose.
    pub fn merge(&mut self, other: &Self) -> bool {
        let raised = self.value.merge(&other.value);
        let later = other.forget_at > self.forget_at;
        if later {
            self.forget_at = other.forget_at;
        }
        raised || later
    }
}

/// A count each part of which is a slot of its own, merged by the larger: the store's own tests'
/// value, knowing nothing of rate limiting.
#[cfg(test)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts(pub(crate) std::collections::BTreeMap<u64, u64>);

#[cfg(test)]
impl Counts {
    /// `count` in part 0 alone.
    pub(crate) fn of(count: u64) -> Self {
        Self([(0, count)].into_iter().collect())
    }

    /// One more in part `part`.
    pub(crate) fn add(&mut self, part: u64) {
        *self.0.entry(part).or_default() += 1;
    }

    /// Every part's count, together.
    pub(crate) fn total(&self) -> u64 {
        self.0.values().sum()
    }
}

#[cfg(test)]
impl Held for Counts {
    fn merge(&mut self, other: &Self) -> bool {
        let mut raised = false;
        for (part, count) in &other.0 {
            let held = self.0.entry(*part).or_default();
            if count > held {
                *held = *count;
                raised = true;
            }
        }
        raised
    }

    /// A part's number and count each, and 8 besides: one part takes what a fixed window's
    /// count does.
    fn bytes(&self) -> u64 {
        8 + 16 * self.0.len() as u64
    }

    fn only(&self, parts: &BTreeSet<u64>) -> Self {
        Self(
            self.0
                .iter()
                .filter(|(part, _)| parts.contains(part))
                .map(|(part, count)| (*part, *count))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ms;

    /// Kept values merge as their values do, keeping the later time to forget at.
    #[test]
    fn kept_values_merge_keeping_the_later_time_to_forget() {
        let counts = |pairs: &[(u64, u64)]| Counts(pairs.iter().copied().collect());
        let mut kept = Kept {
            value: counts(&[(1, 2)]),
            forget_at: ms(10),
        };
        let later = Kept {
            value: counts(&[(1, 1)]),
            forget_at: ms(20),
        };
        assert!(kept.merge(&later));
        assert_eq!(kept.value, counts(&[(1, 2)]));
        assert_eq!(kept.forget_at, ms(20));
        assert!(!kept.merge(&later));
        assert_eq!(Kept::new(counts(&[])).forget_at, Time::MAX);
    }
}
