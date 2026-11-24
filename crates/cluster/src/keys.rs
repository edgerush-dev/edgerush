//! The keys a pod holds, by shard, with what its forgetting and its eviction read of them:
//! each key by the second it may be forgotten in, each shard by the soonest of its keys, and
//! the bytes they take — kept as the keys change rather than counted by walking them, so that
//! finding the keys due costs as many steps as there are keys due.

use crate::{Held, KINDS, Kept, Key, ShardId, Time};
use edgerush_memory::btree_entry;
use std::collections::{BTreeMap, BTreeSet};

/// A second, in nanoseconds: what a key's time to be forgotten is rounded up to.
const SECOND: Time = 1_000_000_000;

/// What a key takes in memory in the store's maps, whatever its value holds beside: its
/// share of its shard's B-tree of keys, the key and its [`Kept`] in place, and of the
/// schedule's, its second and the key.
pub(crate) const fn entry_bytes<V>() -> u64 {
    btree_entry::<Key, Kept<V>>() + btree_entry::<(Time, Key), ()>()
}

/// The second `forget_at` falls in, rounded up: a key is looked at once that second has
/// passed, so never before its time, and at most a second after it.
const fn second_of(forget_at: Time) -> Time {
    forget_at.div_ceil(SECOND)
}

/// The second by which a key whose time has come by `wall` may be forgotten: the last whole
/// one, so that every key of it is due.
pub(crate) const fn seconds_passed(wall: Time) -> Time {
    wall / SECOND
}

/// What a key takes toward `max_key_bytes`: what it takes in memory, its share of the maps
/// and what its value holds on the heap.
pub(crate) fn bytes_of<V: Held>(kept: &Kept<V>) -> u64 {
    entry_bytes::<V>().saturating_add(kept.value.heap_bytes())
}

/// A pod's keys, owned and copies, by shard. Every change goes through it, so the keys, their
/// schedule and what they take always agree; a shard dropped takes its schedule with it.
#[derive(Clone, Debug)]
pub(crate) struct Keys<V> {
    shards: BTreeMap<ShardId, ShardKeys<V>>,
    /// Each shard holding a key, by the second of the soonest it may forget: where the soonest
    /// of all its keys is found, whatever the number of shards.
    fronts: BTreeSet<(Time, ShardId)>,
    /// What its keys take, and how many there are.
    bytes: u64,
    count: usize,
}

/// What a set of keys takes, and how many there are.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) bytes: u64,
    pub(crate) count: usize,
}

impl Tally {
    /// With a key of `bytes` added, as [`bytes_of`] counts it: worked out once by the change
    /// that adds it, a value's bytes costing a walk of its parts.
    const fn add(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.count = self.count.saturating_add(1);
    }

    /// With a key of `bytes` taken out.
    const fn take(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_sub(bytes);
        self.count = self.count.saturating_sub(1);
    }
}

impl<V> Default for Keys<V> {
    fn default() -> Self {
        Self {
            shards: BTreeMap::new(),
            fronts: BTreeSet::new(),
            bytes: 0,
            count: 0,
        }
    }
}

impl<V: Held> Keys<V> {
    /// The keys of `shard` and their values, in key order, to read; `None` if it never held
    /// one of it since it last dropped the shard.
    pub(crate) fn shard(&self, shard: ShardId) -> Option<&BTreeMap<Key, Kept<V>>> {
        self.shards.get(&shard).map(|keys| &keys.held)
    }

    /// How many keys it holds of `shard`.
    pub(crate) fn len_of(&self, shard: ShardId) -> usize {
        self.shards.get(&shard).map_or(0, |keys| keys.held.len())
    }

    /// How many keys it holds.
    pub(crate) const fn len(&self) -> usize {
        self.count
    }

    /// What its keys take toward `max_key_bytes`, their places in the schedule with them.
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// What its keys of each kind take, as [`bytes`](Self::bytes) counts them, and how many
    /// there are: a sum over its shards when asked, as a scrape asks, kept by each shard as its
    /// keys change, so that a change costs only its shard's count.
    pub(crate) fn by_kind(&self) -> [Tally; KINDS] {
        let mut total = [Tally::default(); KINDS];
        for keys in self.shards.values() {
            for (total, shard) in total.iter_mut().zip(keys.by_kind) {
                total.bytes = total.bytes.saturating_add(shard.bytes);
                total.count = total.count.saturating_add(shard.count);
            }
        }
        total
    }

    /// Holds `kept` as `key` of `shard`, in place of what it held of it: what that was.
    pub(crate) fn insert(&mut self, shard: ShardId, key: Key, kept: Kept<V>) -> Option<Kept<V>> {
        self.with_shard(shard, |keys| (keys.insert(key, kept), true))
    }

    /// Lets go of `key` of `shard`: what it held of it.
    pub(crate) fn remove(&mut self, shard: ShardId, key: Key) -> Option<Kept<V>> {
        if !self.shards.contains_key(&shard) {
            return None;
        }
        self.with_shard(shard, |keys| (keys.remove(key), true))
    }

    /// Calls `change` on `key` of `shard`, a new one empty, and keeps its place in the
    /// schedule and its bytes as `change` leaves it.
    pub(crate) fn change<R>(
        &mut self,
        shard: ShardId,
        key: Key,
        change: impl FnOnce(&mut Kept<V>) -> R,
    ) -> R {
        self.with_shard(shard, |keys| keys.change(key, change))
    }

    /// Keeps only the keys of `shard` that `keep` says to.
    pub(crate) fn retain(&mut self, shard: ShardId, keep: impl FnMut(&Key, &Kept<V>) -> bool) {
        if self.shards.contains_key(&shard) {
            self.with_shard(shard, |keys| (keys.retain(keep), true));
        }
    }

    /// Drops every key of `shard`.
    pub(crate) fn drop_shard(&mut self, shard: ShardId) {
        let Some(keys) = self.shards.remove(&shard) else {
            return;
        };
        if let Some(second) = keys.front {
            self.fronts.remove(&(second, shard));
        }
        self.bytes = self.bytes.saturating_sub(keys.bytes);
        self.count = self.count.saturating_sub(keys.held.len());
    }

    /// Lets go of the key it may forget soonest of all, if that is due by second `by`: its
    /// shard, the key and what it held of it. Of the keys due in one second, a shard's go
    /// before the next shard's, and within a shard the lowest first.
    pub(crate) fn pop_soonest(&mut self, by: Time) -> Option<(ShardId, Key, Kept<V>)> {
        let (second, shard) = *self.fronts.first()?;
        if second > by {
            return None;
        }
        let (key, kept) = self.with_shard(shard, |keys| (keys.pop_due(second), true))?;
        Some((shard, key, kept))
    }

    /// Calls `change` on the keys of `shard`, made if it holds none, and keeps the totals as
    /// `change` leaves them, and the shard's place among the fronts if `change` says its
    /// schedule may have changed: a key in use, its second the same, leaves it as it was.
    fn with_shard<R>(
        &mut self,
        shard: ShardId,
        change: impl FnOnce(&mut ShardKeys<V>) -> (R, bool),
    ) -> R {
        let Self {
            shards,
            fronts,
            bytes,
            count,
        } = self;
        let keys = shards.entry(shard).or_default();
        let (was_bytes, was_count) = (keys.bytes, keys.held.len());
        let (result, moved) = change(keys);
        *bytes = bytes.saturating_sub(was_bytes).saturating_add(keys.bytes);
        *count = count
            .saturating_sub(was_count)
            .saturating_add(keys.held.len());
        if moved {
            let front = keys.first_due();
            if front != keys.front {
                if let Some(second) = keys.front {
                    fronts.remove(&(second, shard));
                }
                if let Some(second) = front {
                    fronts.insert((second, shard));
                }
                keys.front = front;
            }
        }
        result
    }
}

/// One shard's keys: each key's value, the keys by the second they may be forgotten in, and
/// what they take.
#[derive(Clone, Debug)]
struct ShardKeys<V> {
    held: BTreeMap<Key, Kept<V>>,
    /// Each key held, by the second of its time to be forgotten, soonest first.
    due: BTreeSet<(Time, Key)>,
    /// What the keys take, each as [`bytes_of`] has it; and the same of each kind, with how
    /// many there are.
    bytes: u64,
    by_kind: [Tally; KINDS],
    /// The second of its soonest key, as [`Keys`]'s fronts have it.
    front: Option<Time>,
}

impl<V> Default for ShardKeys<V> {
    fn default() -> Self {
        Self {
            held: BTreeMap::new(),
            due: BTreeSet::new(),
            bytes: 0,
            by_kind: [Tally::default(); KINDS],
            front: None,
        }
    }
}

impl<V: Held> ShardKeys<V> {
    fn insert(&mut self, key: Key, kept: Kept<V>) -> Option<Kept<V>> {
        let (second, bytes) = (second_of(kept.forget_at), bytes_of(&kept));
        self.by_kind[kept.kind()].add(bytes);
        let old = self.held.insert(key, kept);
        if let Some(old) = &old {
            self.take(key, old);
        }
        self.due.insert((second, key));
        self.bytes = self.bytes.saturating_add(bytes);
        old
    }

    fn remove(&mut self, key: Key) -> Option<Kept<V>> {
        let old = self.held.remove(&key)?;
        self.take(key, &old);
        Some(old)
    }

    /// What `change` gives, and whether the key's place in the schedule changed.
    fn change<R>(&mut self, key: Key, change: impl FnOnce(&mut Kept<V>) -> R) -> (R, bool) {
        let mut new = false;
        let kept = self.held.entry(key).or_insert_with(|| {
            new = true;
            Kept::default()
        });
        let before = (!new).then(|| (second_of(kept.forget_at), bytes_of(kept), kept.kind()));
        let result = change(kept);
        let (second, bytes) = (second_of(kept.forget_at), bytes_of(kept));
        if let Some((_, was, kind)) = before {
            self.by_kind[kind].take(was);
        }
        self.by_kind[kept.kind()].add(bytes);
        let moved = match before {
            // Its place unchanged, as for a key in use, its time moving on within a second.
            Some((was, _, _)) if was == second => false,
            Some((was, _, _)) => {
                self.due.remove(&(was, key));
                self.due.insert((second, key));
                true
            }
            None => {
                self.due.insert((second, key));
                true
            }
        };
        let was = before.map_or(0, |(_, bytes, _)| bytes);
        self.bytes = self.bytes.saturating_sub(was).saturating_add(bytes);
        (result, moved)
    }

    fn retain(&mut self, mut keep: impl FnMut(&Key, &Kept<V>) -> bool) {
        let (due, bytes, by_kind) = (&mut self.due, &mut self.bytes, &mut self.by_kind);
        self.held.retain(|key, kept| {
            let kept_on = keep(key, kept);
            if !kept_on {
                let taken = bytes_of(kept);
                due.remove(&(second_of(kept.forget_at), *key));
                *bytes = bytes.saturating_sub(taken);
                by_kind[kept.kind()].take(taken);
            }
            kept_on
        });
    }

    fn first_due(&self) -> Option<Time> {
        self.due.first().map(|(second, _)| *second)
    }

    fn pop_due(&mut self, by: Time) -> Option<(Key, Kept<V>)> {
        let (second, key) = *self.due.first()?;
        if second > by {
            return None;
        }
        let kept = self.remove(key)?;
        Some((key, kept))
    }

    /// Counts out `kept`, held as `key` until now.
    fn take(&mut self, key: Key, kept: &Kept<V>) {
        let taken = bytes_of(kept);
        self.due.remove(&(second_of(kept.forget_at), key));
        self.bytes = self.bytes.saturating_sub(taken);
        self.by_kind[kept.kind()].take(taken);
    }
}

#[cfg(test)]
impl<V: Held> Keys<V> {
    /// Whether its schedules, its fronts and its totals are what a walk of its keys gives.
    pub(crate) fn agrees(&self) -> bool {
        let mut fronts = BTreeSet::new();
        let (mut bytes, mut count) = (0, 0);
        let mut by_kind = [Tally::default(); KINDS];
        for (shard, keys) in &self.shards {
            let mut shard_kinds = [Tally::default(); KINDS];
            for kept in keys.held.values() {
                shard_kinds[kept.kind()].add(bytes_of(kept));
                by_kind[kept.kind()].add(bytes_of(kept));
            }
            if shard_kinds != keys.by_kind {
                return false;
            }
            let due: BTreeSet<(Time, Key)> = keys
                .held
                .iter()
                .map(|(key, kept)| (second_of(kept.forget_at), *key))
                .collect();
            let shard_bytes: u64 = keys.held.values().map(bytes_of).sum();
            if due != keys.due || shard_bytes != keys.bytes || keys.first_due() != keys.front {
                return false;
            }
            if let Some((second, _)) = due.first() {
                fronts.insert((*second, *shard));
            }
            bytes += shard_bytes;
            count += keys.held.len();
        }
        fronts == self.fronts
            && bytes == self.bytes
            && count == self.count
            && by_kind == self.by_kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kept::Counts;
    use crate::ms;
    use proptest::prelude::*;

    fn kept(count: u64, forget_at: Time) -> Kept<Counts> {
        Kept {
            value: Counts::of(count),
            forget_at,
        }
    }

    fn popped(keys: &mut Keys<Counts>, by: Time) -> Option<(ShardId, Key)> {
        keys.pop_soonest(by).map(|(shard, key, _)| (shard, key))
    }

    /// A key due within a second is due once that second has passed, never before its time.
    #[test]
    fn a_key_is_due_once_the_second_of_its_time_has_passed() {
        let mut keys = Keys::default();
        keys.insert(0, 1, kept(1, ms(1_500)));
        keys.insert(0, 2, kept(1, ms(2_000)));
        assert_eq!(
            popped(&mut keys, seconds_passed(ms(1_999))),
            None,
            "1.5 s: not by 1.999"
        );
        assert_eq!(popped(&mut keys, seconds_passed(ms(2_000))), Some((0, 1)));
        assert_eq!(
            popped(&mut keys, seconds_passed(ms(2_000))),
            Some((0, 2)),
            "2 s, due at 2"
        );
        assert_eq!((keys.len(), keys.bytes()), (0, 0));
    }

    /// A key counts its share of the store's maps — of its shard's keys, an entry of 16 bytes
    /// and 32 in a node of 560 bytes as glibc allocates it, and of the schedule, 32 bytes in
    /// one of 384, each node seven entries on average — and what its value holds on the heap.
    #[test]
    fn a_key_counts_its_share_of_the_maps_and_its_values_heap() {
        assert_eq!(entry_bytes::<Counts>(), 560 / 7 + 384 / 7);
        let mut keys = Keys::default();
        keys.insert(0, 1, kept(1, ms(1_000)));
        let one_part = Counts::of(1).heap_bytes();
        assert_eq!(keys.bytes(), entry_bytes::<Counts>() + one_part);
        keys.change(0, 1, |kept| kept.value.add(5));
        assert_eq!(
            keys.bytes(),
            entry_bytes::<Counts>() + edgerush_memory::btree_ascending::<u64, u64>(2)
        );
        keys.insert(0, 2, kept(1, ms(1_000)));
        assert_eq!(keys.bytes(), 2 * entry_bytes::<Counts>() + 2 * one_part);
        assert!(keys.agrees());
    }

    /// The soonest of all goes first, whatever its shard; of one second, by shard.
    #[test]
    fn the_soonest_of_all_shards_comes_first() {
        let mut keys = Keys::default();
        keys.insert(0, 1, kept(1, ms(3_000)));
        keys.insert(1, 2, kept(1, ms(1_000)));
        keys.insert(2, 3, kept(1, ms(3_000)));
        keys.insert(1, 4, kept(1, ms(2_000)));
        let order: Vec<_> = std::iter::from_fn(|| popped(&mut keys, 10)).collect();
        assert_eq!(order, [(1, 2), (1, 4), (0, 1), (2, 3)]);
    }

    /// A key never to be forgotten is never due, and comes off last of any.
    #[test]
    fn a_key_never_forgotten_is_never_due_and_last_of_all() {
        let mut keys = Keys::default();
        keys.insert(0, 1, Kept::new(Counts::of(1)));
        keys.insert(1, 2, kept(1, ms(1_000)));
        assert_eq!(popped(&mut keys, seconds_passed(Time::MAX)), Some((1, 2)));
        assert_eq!(popped(&mut keys, seconds_passed(Time::MAX)), None);
        assert_eq!(popped(&mut keys, Time::MAX), Some((0, 1)));
    }

    /// A key whose time is brought forward is due by its new time, and one put back by its new
    /// time alone.
    #[test]
    fn a_key_changed_is_due_by_its_new_time() {
        let mut keys = Keys::default();
        keys.insert(0, 1, kept(1, ms(60_000)));
        keys.change(0, 1, |kept| kept.forget_at = ms(1_000));
        assert_eq!(popped(&mut keys, 1), Some((0, 1)));
        keys.insert(0, 2, kept(1, ms(1_000)));
        keys.change(0, 2, |kept| kept.forget_at = ms(60_000));
        assert_eq!(popped(&mut keys, 59), None);
        assert_eq!(popped(&mut keys, 60), Some((0, 2)));
    }

    /// A shard dropped takes its keys, its place among the fronts and what it took.
    #[test]
    fn a_shard_dropped_takes_everything_of_it() {
        let mut keys = Keys::default();
        keys.insert(0, 1, kept(1, ms(1_000)));
        keys.insert(1, 2, kept(1, ms(5_000)));
        let before = keys.bytes();
        keys.drop_shard(0);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.bytes(), before / 2);
        assert_eq!(popped(&mut keys, 10), Some((1, 2)));
        assert!(keys.agrees());
    }

    #[derive(Clone, Debug)]
    enum Step {
        Insert(ShardId, Key, u64, Time),
        Remove(ShardId, Key),
        Change(ShardId, Key, u64, Option<Time>),
        Merge(ShardId, Key, u64, Time),
        Retain(ShardId, Key),
        Drop(ShardId),
        Pop(Time),
    }

    fn step() -> impl Strategy<Value = Step> {
        let shard = 0..3u16;
        let key: std::ops::Range<Key> = 0..16;
        let every: std::ops::Range<Key> = 1..4;
        let at = prop_oneof![0..ms(10_000), Just(Time::MAX)];
        prop_oneof![
            (shard.clone(), key.clone(), 0..4u64, at.clone())
                .prop_map(|(s, k, c, t)| Step::Insert(s, k, c, t)),
            (shard.clone(), key.clone()).prop_map(|(s, k)| Step::Remove(s, k)),
            (
                shard.clone(),
                key.clone(),
                0..4u64,
                proptest::option::of(at.clone())
            )
                .prop_map(|(s, k, p, t)| Step::Change(s, k, p, t)),
            (shard.clone(), key, 0..4u64, at).prop_map(|(s, k, p, t)| Step::Merge(s, k, p, t)),
            (shard.clone(), every).prop_map(|(s, every)| Step::Retain(s, every)),
            shard.prop_map(Step::Drop),
            (0..12u64).prop_map(Step::Pop),
        ]
    }

    proptest! {
        /// Whatever is done to them, the schedules, the fronts and the totals are what a walk
        /// of the keys gives; no key comes off before its time, and none comes off while one
        /// due sooner is held.
        #[test]
        fn the_schedule_and_totals_always_agree_with_the_keys(
            steps in proptest::collection::vec(step(), 0..64)
        ) {
            let mut keys: Keys<Counts> = Keys::default();
            for step in steps {
                match step {
                    Step::Insert(shard, key, count, at) => {
                        let _ = keys.insert(shard, key, kept(count, at));
                    }
                    Step::Remove(shard, key) => {
                        let _ = keys.remove(shard, key);
                    }
                    Step::Change(shard, key, part, at) => keys.change(shard, key, |kept| {
                        kept.value.add(part);
                        if let Some(at) = at {
                            kept.forget_at = at;
                        }
                    }),
                    Step::Merge(shard, key, part, at) => {
                        let mut other = kept(0, at);
                        other.value.add(part);
                        let _ = keys.change(shard, key, |kept| kept.merge(&other));
                    }
                    Step::Retain(shard, every) => keys.retain(shard, |key, _| key % every == 0),
                    Step::Drop(shard) => keys.drop_shard(shard),
                    Step::Pop(by) => {
                        let soonest = keys
                            .shards
                            .values()
                            .flat_map(|keys| keys.held.values())
                            .map(|kept| second_of(kept.forget_at))
                            .min();
                        if let Some((_, _, kept)) = keys.pop_soonest(by) {
                            prop_assert!(kept.forget_at <= by * SECOND, "forgotten early");
                            prop_assert_eq!(Some(second_of(kept.forget_at)), soonest);
                        }
                    }
                }
                prop_assert!(keys.agrees(), "{:?}", keys);
            }
        }
    }
}
