//! The worker's deadlines, as a heap that is lazy about them
//! ([03 §2](../../../../docs/03-data-plane.md)).
//!
//! Each owner of a deadline has a slot, holding the deadline as it stands and whatever the
//! owner is woken through. The heap holds entries of a time and a slot. A deadline that
//! moves later leaves its entry where it is, as HAProxy leaves a task in its timer tree: it
//! costs nothing then, and when the entry comes up the slot is looked at and queued again
//! at its deadline. A deadline that moves sooner than its entry is queued again, and the
//! entry it had is left to be thrown away when it comes up. So every deadline falls due at
//! its own time, and a slot's deadline moving on with every bit of progress touches the
//! heap only when it has to.
//!
//! It reads no clock: the times it is told are all it knows.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

/// Which slot, while it is the one given out: a slot's key is no use once it is removed,
/// and the entries it left behind are thrown away when they come up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key {
    index: usize,
    generation: u64,
}

#[derive(Debug)]
struct Slot<T> {
    generation: u64,
    /// Given out and not yet removed.
    taken: bool,
    /// The deadline as it stands.
    due: Option<Instant>,
    /// When the one entry of this slot's that counts comes up. Any other of its entries is
    /// one it has moved on from.
    queued: Option<Instant>,
    /// The latest time its deadline was found to have come.
    rang: Option<Instant>,
    value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    at: Instant,
    index: usize,
    generation: u64,
}

/// What an owner waiting on its deadline found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waited {
    /// The deadline has come.
    Came,
    /// It has not, and nothing needed queueing.
    Waiting,
    /// It has not, and was queued for this time.
    Queued(Instant),
}

/// The deadlines of one worker.
#[derive(Debug)]
pub struct Heap<T> {
    slots: Vec<Slot<T>>,
    free: Vec<usize>,
    queue: BinaryHeap<Reverse<Entry>>,
    /// Slots given out, which is what the entries thrown away are bounded by.
    taken: usize,
}

impl<T> Default for Heap<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            queue: BinaryHeap::new(),
            taken: 0,
        }
    }
}

impl<T> Heap<T> {
    /// A slot, with no deadline yet, holding `value`.
    pub fn add(&mut self, value: T) -> Key {
        self.taken += 1;
        if let Some(index) = self.free.pop()
            && let Some(slot) = self.slots.get_mut(index)
        {
            slot.taken = true;
            slot.due = None;
            slot.queued = None;
            slot.rang = None;
            slot.value = value;
            return Key {
                index,
                generation: slot.generation,
            };
        }
        self.slots.push(Slot {
            generation: 0,
            taken: true,
            due: None,
            queued: None,
            rang: None,
            value,
        });
        Key {
            index: self.slots.len() - 1,
            generation: 0,
        }
    }

    /// Gives the slot up. Its entries are thrown away when they come up.
    pub fn remove(&mut self, key: Key) {
        if let Some(slot) = self.slot_mut(key) {
            slot.taken = false;
            slot.generation += 1;
            slot.due = None;
            slot.queued = None;
            self.free.push(key.index);
            self.taken -= 1;
            self.compact();
        }
    }

    /// What the slot holds.
    #[cfg(test)]
    fn value_mut(&mut self, key: Key) -> Option<&mut T> {
        self.slot_mut(key).map(|slot| &mut slot.value)
    }

    /// What an owner waiting on `due` asks, with one look at its slot: whether `due` has
    /// come, and if it has not, the slot's deadline is set to it as the owner asks: one sooner than the
    /// slot's entry is queued, no later than `by` says, and `keep` is given what the slot holds, to keep what wakes the owner in. Asked on
    /// every poll of every owner, so it is the one the request path takes.
    pub fn wait(
        &mut self,
        key: Key,
        due: Instant,
        by: impl FnOnce() -> Instant,
        keep: impl FnOnce(&mut T),
    ) -> Waited {
        let Some(slot) = self.slot_mut(key) else {
            return Waited::Waiting;
        };
        if slot.rang >= Some(due) {
            return Waited::Came;
        }
        slot.due = Some(due);
        keep(&mut slot.value);
        if slot.queued.is_some_and(|queued| queued <= due) {
            return Waited::Waiting;
        }
        let at = due.min(by());
        slot.queued = Some(at);
        self.queue_at(key, at);
        Waited::Queued(at)
    }

    fn queue_at(&mut self, key: Key, at: Instant) {
        self.queue.push(Reverse(Entry {
            at,
            index: key.index,
            generation: key.generation,
        }));
        self.compact();
    }

    /// Whether `due` had come the last time the slot's deadline was found to have come.
    pub fn reached(&self, key: Key, due: Instant) -> bool {
        self.slot(key).is_some_and(|slot| slot.rang >= Some(due))
    }

    /// When the next entry comes up.
    pub fn soonest(&mut self) -> Option<Instant> {
        while let Some(Reverse(entry)) = self.queue.peek() {
            if self.counts(entry) {
                return Some(entry.at);
            }
            self.queue.pop();
        }
        None
    }

    /// The time is `now`: every slot whose deadline has come is handed to `each`, and one
    /// whose entry came up before its deadline, which moved on since, is queued again at
    /// its deadline.
    pub fn expire(&mut self, now: Instant, mut each: impl FnMut(&mut T)) {
        while let Some(&Reverse(entry)) = self.queue.peek() {
            if entry.at > now {
                break;
            }
            self.queue.pop();
            if !self.counts(&entry) {
                continue;
            }
            let Some(slot) = self.slots.get_mut(entry.index) else {
                continue;
            };
            slot.queued = None;
            match slot.due {
                Some(due) if due <= now => {
                    slot.rang = Some(now);
                    each(&mut slot.value);
                }
                Some(due) => {
                    slot.queued = Some(due);
                    self.queue.push(Reverse(Entry { at: due, ..entry }));
                }
                None => {}
            }
        }
    }

    /// Whether `entry` is the one its slot counts.
    fn counts(&self, entry: &Entry) -> bool {
        self.slots.get(entry.index).is_some_and(|slot| {
            slot.taken && slot.generation == entry.generation && slot.queued == Some(entry.at)
        })
    }

    /// Throws away the entries that no longer count once they outnumber the ones that do,
    /// so that owners moving their deadlines sooner again and again cannot grow the heap
    /// without bound.
    fn compact(&mut self) {
        if self.queue.len() <= 2 * self.taken + 16 {
            return;
        }
        let entries = std::mem::take(&mut self.queue).into_vec();
        let kept: Vec<_> = entries
            .into_iter()
            .filter(|Reverse(entry)| self.counts(entry))
            .collect();
        self.queue = BinaryHeap::from(kept);
    }

    fn slot(&self, key: Key) -> Option<&Slot<T>> {
        self.slots
            .get(key.index)
            .filter(|slot| slot.taken && slot.generation == key.generation)
    }

    fn slot_mut(&mut self, key: Key) -> Option<&mut Slot<T>> {
        self.slots
            .get_mut(key.index)
            .filter(|slot| slot.taken && slot.generation == key.generation)
    }

    #[cfg(test)]
    fn queued(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::time::Duration;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// Everything that `expire` handed over, as the slots' values.
    fn expired(heap: &mut Heap<usize>, now: Instant) -> Vec<usize> {
        let mut out = Vec::new();
        heap.expire(now, |value| out.push(*value));
        out.sort_unstable();
        out
    }

    /// The slot's deadline set to `due`, as waiting on it sets it; when it was queued, if
    /// it was.
    fn set(
        heap: &mut Heap<usize>,
        key: Key,
        due: Option<Instant>,
        by: impl FnOnce() -> Instant,
    ) -> Option<Instant> {
        match heap.wait(key, due?, by, |_| {}) {
            Waited::Queued(at) => Some(at),
            Waited::Came | Waited::Waiting => None,
        }
    }

    #[test]
    fn a_deadline_comes_at_its_time_and_not_before() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(7);
        set(&mut heap, key, Some(start + ms(30)), || start + ms(30));
        assert_eq!(heap.soonest(), Some(start + ms(30)));
        assert!(expired(&mut heap, start + ms(29)).is_empty());
        assert!(!heap.reached(key, start + ms(30)));
        assert_eq!(expired(&mut heap, start + ms(30)), [7]);
        assert!(heap.reached(key, start + ms(30)));
        assert_eq!(heap.soonest(), None);
    }

    /// Moved later, the entry stays where it was; when it comes up the slot goes to its
    /// deadline, and comes then.
    #[test]
    fn a_deadline_moved_later_is_queued_again_when_its_entry_comes_up() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(1);
        set(&mut heap, key, Some(start + ms(10)), || start + ms(10));
        for later in 11..=50 {
            assert_eq!(
                set(&mut heap, key, Some(start + ms(later)), || start
                    + ms(later)),
                None
            );
        }
        assert_eq!(heap.queued(), 1);
        assert!(expired(&mut heap, start + ms(10)).is_empty());
        assert_eq!(heap.soonest(), Some(start + ms(50)));
        assert_eq!(expired(&mut heap, start + ms(50)), [1]);
    }

    #[test]
    fn a_deadline_moved_sooner_is_queued_again_at_once() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(1);
        set(&mut heap, key, Some(start + ms(50)), || start + ms(50));
        assert_eq!(
            set(&mut heap, key, Some(start + ms(20)), || start + ms(60)),
            Some(start + ms(20))
        );
        assert_eq!(heap.soonest(), Some(start + ms(20)));
        assert_eq!(expired(&mut heap, start + ms(20)), [1]);
        // The entry it moved on from is thrown away, not handed over again.
        assert!(expired(&mut heap, start + ms(50)).is_empty());
        assert_eq!(heap.soonest(), None);
    }

    /// Queued no later than `by`: then a deadline set after it, no sooner than that, does
    /// not need queueing.
    #[test]
    fn a_deadline_is_queued_no_later_than_asked() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(1);
        assert_eq!(
            set(&mut heap, key, Some(start + ms(30)), || start + ms(5)),
            Some(start + ms(5))
        );
        assert_eq!(
            set(&mut heap, key, Some(start + ms(8)), || start + ms(13)),
            None
        );
        assert!(expired(&mut heap, start + ms(5)).is_empty());
        assert_eq!(heap.soonest(), Some(start + ms(8)));
        assert_eq!(expired(&mut heap, start + ms(8)), [1]);
    }

    #[test]
    fn a_removed_slot_is_never_handed_over_and_its_key_is_no_use() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let gone = heap.add(1);
        set(&mut heap, gone, Some(start + ms(10)), || start + ms(10));
        heap.remove(gone);
        let again = heap.add(2);
        assert_ne!(again, gone);
        assert_eq!(
            set(&mut heap, gone, Some(start + ms(1)), || start + ms(1)),
            None
        );
        assert!(heap.value_mut(gone).is_none());
        set(&mut heap, again, Some(start + ms(20)), || start + ms(20));
        assert!(expired(&mut heap, start + ms(10)).is_empty());
        assert_eq!(expired(&mut heap, start + ms(20)), [2]);
    }

    /// Waiting looks at the slot once: whether the deadline came, and if not, sets it and
    /// hands over what the slot holds to be kept.
    #[test]
    fn a_wait_says_whether_its_deadline_came_and_keeps_what_wakes() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(0);
        let due = start + ms(30);
        let keep = |value: &mut usize| *value += 1;
        assert_eq!(
            heap.wait(key, due, || start + ms(5), keep),
            Waited::Queued(start + ms(5))
        );
        assert_eq!(heap.wait(key, due, || start + ms(5), keep), Waited::Waiting);
        assert_eq!(heap.value_mut(key).copied(), Some(2));
        assert!(expired(&mut heap, start + ms(5)).is_empty());
        assert_eq!(expired(&mut heap, due), [2]);
        assert_eq!(heap.wait(key, due, || start, keep), Waited::Came);
        assert_eq!(
            heap.value_mut(key).copied(),
            Some(2),
            "nothing kept once it came"
        );
        // A later deadline is a new wait.
        let later = due + ms(1);
        assert_eq!(heap.wait(key, later, || later, keep), Waited::Queued(later));
    }

    /// A deadline moved sooner and sooner leaves an entry behind each time; they are
    /// thrown away once they outnumber the ones that count.
    #[test]
    fn entries_moved_on_from_do_not_pile_up() {
        let start = Instant::now();
        let mut heap = Heap::default();
        let key = heap.add(1);
        for sooner in (1..=100).rev() {
            set(&mut heap, key, Some(start + ms(sooner)), || {
                start + ms(sooner)
            });
        }
        assert!(heap.queued() <= 2 + 16, "{} entries", heap.queued());
        assert_eq!(expired(&mut heap, start + ms(1)), [1]);
        assert!(expired(&mut heap, start + ms(100)).is_empty());
    }

    /// What can happen to the heap: slots come and go, deadlines move either way, and the
    /// time moves on.
    #[derive(Debug, Clone)]
    enum Step {
        Add,
        Remove(usize),
        Set(usize, Option<u64>, u64),
        Expire(u64),
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            Just(Step::Add),
            (0_usize..8).prop_map(Step::Remove),
            (0_usize..8, (0_u64..200).prop_map(Some), 0_u64..200)
                .prop_map(|(slot, due, ahead)| Step::Set(slot, due, ahead)),
            (0_u64..60).prop_map(Step::Expire),
        ]
    }

    proptest! {
        /// Against keeping every deadline in a plain list: whatever is done, a slot is
        /// handed over only once its deadline has come, and every deadline the time has
        /// reached is found to have come; the next entry is never after a deadline still
        /// to come; and the entries thrown away stay bounded.
        #[test]
        fn it_agrees_with_a_plain_list(steps in proptest::collection::vec(step(), 0..120)) {
            let start = Instant::now();
            let mut heap = Heap::default();
            // The reference: each live slot's key, value and deadline.
            let mut live: Vec<(Key, usize, Option<Instant>)> = Vec::new();
            let mut now = start;
            let mut made = 0;
            for step in steps {
                match step {
                    Step::Add => {
                        let key = heap.add(made);
                        live.push((key, made, None));
                        made += 1;
                    }
                    Step::Remove(at) => {
                        if !live.is_empty() {
                            let (key, _, _) = live.remove(at % live.len());
                            heap.remove(key);
                        }
                    }
                    Step::Set(at, due, ahead) => {
                        if !live.is_empty() {
                            let at = at % live.len();
                            let due = due.map(|due| now + ms(due));
                            // Whatever `by` is asked, it is never before now.
                            set(&mut heap, live[at].0, due, || now + ms(ahead));
                            live[at].2 = due;
                        }
                    }
                    Step::Expire(later) => {
                        now += ms(later);
                        for value in expired(&mut heap, now) {
                            let due = live.iter().find(|(_, v, _)| *v == value).map(|(_, _, due)| *due);
                            prop_assert!(due.is_some(), "{} handed over once removed", value);
                            prop_assert!(due.flatten().is_some_and(|due| due <= now), "{} handed over early", value);
                        }
                        for &(key, value, due) in &live {
                            if let Some(due) = due.filter(|&due| due <= now) {
                                prop_assert!(heap.reached(key, due), "{} came late", value);
                            }
                        }
                    }
                }
                for &(key, value, due) in &live {
                    if let Some(due) = due {
                        prop_assert!(!heap.reached(key, due) || due <= now, "{} came early", value);
                    }
                }
                let still = live.iter().filter_map(|(key, _, due)| {
                    due.filter(|&due| !heap.reached(*key, due))
                }).min();
                if let Some(still) = still {
                    let next = heap.soonest();
                    prop_assert!(next.is_some_and(|next| next <= still), "next {:?} after {:?}", next, still);
                }
                prop_assert!(heap.queued() <= 2 * live.len() + 17, "{} entries for {} slots", heap.queued(), live.len());
            }
            // And in the end, with time enough, every deadline comes.
            now += ms(1_000);
            expired(&mut heap, now);
            for &(key, value, due) in &live {
                if let Some(due) = due {
                    prop_assert!(heap.reached(key, due), "{} never came", value);
                }
            }
        }
    }
}
