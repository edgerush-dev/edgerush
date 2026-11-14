//! What the asynchronous mode keeps under each key: its owner's state of it, as today's
//! asynchronous owner keeps it (`edgerush_ratelimit::service`), by the wall clock. Each part
//! only rises — the bucket's arrival time, the hits counted, each data plane's highest batch
//! applied and last admission — so that two copies merge by keeping the larger of each, in
//! whatever order they meet. Batches kept waiting behind a missing one merge as a union, the
//! same batch twice taking the smaller count; records of data planes long silent are retired
//! in the merge as in the applying, by one rule that reads no clock.

use edgerush_cluster::Held;
use edgerush_ratelimit::reference::{BucketState, TokenBucket};
use edgerush_ratelimit::service::{Contract, MOST_UNACKNOWLEDGED, PlaneId, RECORD_KEPT};

/// A key's state as its owner holds it, and every replica a copy of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Value {
    /// The contract it is kept under; `None` for a key never made.
    pub(crate) contract: Option<Contract>,
    /// Its bucket's state, the contract its terms, by the wall clock.
    pub(crate) bucket: BucketState,
    /// Hits applied, each once: for measuring.
    pub(crate) counted: u64,
    /// What it knows of each data plane admitting the key.
    pub(crate) planes: Records,
}

impl Value {
    /// A key made afresh under `contract`: a full bucket, nothing counted, no data plane known.
    #[must_use]
    pub fn new(contract: Contract) -> Self {
        Self {
            contract: Some(contract),
            ..Self::default()
        }
    }

    /// The contract it is kept under; `None` for a key never made.
    #[must_use]
    pub const fn contract(&self) -> Option<Contract> {
        self.contract
    }

    /// Hits applied, each once.
    #[must_use]
    pub const fn counted(&self) -> u64 {
        self.counted
    }

    /// Its bucket, under its contract; `None` for a key never made.
    #[must_use]
    pub fn bucket(&self) -> Option<TokenBucket> {
        self.contract.map(|contract| self.bucket_under(contract))
    }

    /// The highest of `plane`'s batches applied, every one before it too, if it has a record.
    #[must_use]
    pub fn applied(&self, plane: PlaneId) -> Option<u64> {
        self.planes.get(plane).map(|record| record.applied)
    }

    /// When `plane` last admitted the key, by the wall clock, if it has a record and has.
    #[must_use]
    pub fn admitted(&self, plane: PlaneId) -> Option<u64> {
        self.planes
            .get(plane)
            .and_then(|record| record.last_admitted.get())
    }

    /// The data planes it keeps records of, in order.
    pub fn planes(&self) -> impl Iterator<Item = PlaneId> + '_ {
        self.planes.iter().map(|(plane, _)| plane)
    }

    /// The bucket made from its state under `contract`.
    pub(crate) fn bucket_under(&self, contract: Contract) -> TokenBucket {
        let Contract {
            limit,
            window,
            burst,
        } = contract;
        TokenBucket::from_state(limit, window, burst, self.bucket)
    }

    /// `count` reported hits, admitted at `at`, applied at `now`: charged as of `at`, each if
    /// there is room, or as debt while it would be paid back within a refill time of `now`; the
    /// rest forgiven. In one step, whatever the count: it comes from the wire. Nothing for a
    /// key never made.
    pub(crate) fn charge(&mut self, count: u64, at: u64, now: u64) {
        let Some(contract) = self.contract else {
            return;
        };
        self.counted = self.counted.saturating_add(count);
        let mut bucket = self.bucket_under(contract);
        bucket.charge_within(at, now, contract.refill_time(), count);
        self.bucket = bucket.state();
    }

    /// How many data planes besides `plane` it has seen admit the key within `lately` of the
    /// latest admission it knows of: counted back from the last traffic, not from now, so that
    /// data planes that burst together and fall quiet are bounded together when they burst
    /// again.
    pub(crate) fn others(&self, plane: PlaneId, lately: u64) -> u64 {
        let latest = self.planes.latest();
        self.planes
            .iter()
            .filter(|(id, record)| {
                *id != plane
                    && matches!((record.last_admitted.get(), latest), (Some(at), Some(latest)) if latest - at <= lately)
            })
            .count() as u64
    }

    /// How long a silent data plane's record is kept after the latest admission: ten minutes,
    /// or the key's refill time if longer.
    pub(crate) fn record_kept(&self) -> u64 {
        self.contract.map_or(RECORD_KEPT, |contract| {
            contract.refill_time().max(RECORD_KEPT)
        })
    }

    /// Lets go of the records of data planes that last admitted more than
    /// [`record_kept`](Self::record_kept) before the latest admission it knows of, and of
    /// those that never have: how many went.
    pub(crate) fn retire(&mut self) -> usize {
        let kept = self.record_kept();
        self.planes.retire(kept)
    }

    /// Takes in `other`, a copy of the same key: under the same contract, the larger of each
    /// part, then the records of data planes silent for ten minutes (or a refill time, if
    /// longer) before the latest admission gone, as the owner lets them go; under another, it
    /// replaces a value never charged — never made, or made by polls alone — and is otherwise
    /// ignored, as the synchronous mode's value of another shape is. Whether it changed
    /// anything.
    pub fn merge(&mut self, other: &Self) -> bool {
        if self.contract != other.contract {
            // No hit applied, of any count: the bucket was never charged either.
            if self.counted == 0 {
                *self = other.clone();
                return true;
            }
            return false;
        }
        let mut changed = false;
        let bucket = self.bucket.larger(other.bucket);
        if bucket != self.bucket {
            self.bucket = bucket;
            changed = true;
        }
        if other.counted > self.counted {
            self.counted = other.counted;
            changed = true;
        }
        // Only the records of `other`'s that the merged value keeps: one it would retire, taken
        // in, would go again, and must not raise what this one holds of its data plane on the
        // way.
        let latest = self.planes.latest().max(other.planes.latest());
        let kept = self.record_kept();
        for (plane, theirs) in other.planes.iter() {
            if Records::keeps(theirs, latest, kept) {
                changed |= self.planes.entry(plane).merge(theirs);
            }
        }
        changed |= self.planes.retire(kept) > 0;
        changed
    }

    /// The value in its parts, as a wire carries it.
    #[must_use]
    pub fn parts(&self) -> Parts {
        Parts {
            contract: self.contract,
            bucket: self.bucket,
            counted: self.counted,
            planes: self
                .planes
                .iter()
                .map(|(plane, record)| PlaneParts {
                    plane,
                    applied: record.applied,
                    admitted: record.last_admitted.get(),
                    waiting: record.waiting.as_deref().map_or_else(Vec::new, |waiting| {
                        waiting
                            .iter()
                            .map(|(sequence, tally)| (*sequence, tally.count, tally.at))
                            .collect()
                    }),
                })
                .collect(),
        }
    }

    /// The value from its parts, as a wire carried them: `None` if they could not have come
    /// from a value — a data plane given twice, a batch waiting twice, or more batches waiting
    /// than a data plane may have out ([`MOST_UNACKNOWLEDGED`]).
    #[must_use]
    pub fn from_parts(parts: Parts) -> Option<Self> {
        let Parts {
            contract,
            bucket,
            counted,
            mut planes,
        } = parts;
        planes.sort_by_key(|plane| plane.plane);
        if planes.windows(2).any(|two| two[0].plane == two[1].plane) {
            return None;
        }
        let mut records = Vec::with_capacity(planes.len());
        for plane in planes {
            let mut waiting: Vec<(u64, Tally)> = plane
                .waiting
                .into_iter()
                .map(|(sequence, count, at)| (sequence, Tally { count, at }))
                .collect();
            waiting.sort_by_key(|(sequence, _)| *sequence);
            if waiting.len() > MOST_UNACKNOWLEDGED
                || waiting.windows(2).any(|two| two[0].0 == two[1].0)
            {
                return None;
            }
            let record = Record {
                applied: plane.applied,
                waiting: (!waiting.is_empty()).then(|| Box::new(waiting)),
                last_admitted: plane.admitted.map_or_else(Admitted::default, Admitted::at),
            };
            records.push((plane.plane, record));
        }
        Some(Self {
            contract,
            bucket,
            counted,
            planes: Records(records),
        })
    }
}

/// A [`Value`] in its parts, as a wire carries it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Parts {
    /// The contract it is kept under; `None` for a key never made.
    pub contract: Option<Contract>,
    /// Its bucket's state, by the wall clock.
    pub bucket: BucketState,
    /// Hits applied, each once.
    pub counted: u64,
    /// What it knows of each data plane admitting the key.
    pub planes: Vec<PlaneParts>,
}

/// What a [`Value`] knows of one data plane, in its parts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlaneParts {
    /// The data plane.
    pub plane: PlaneId,
    /// The highest of its batches applied, every one before it too.
    pub applied: u64,
    /// When it last admitted the key, by the wall clock, if it has.
    pub admitted: Option<u64>,
    /// Batches that came ahead of one still missing: `(sequence, count, at)`, `at` when their
    /// hits are charged as of, by the wall clock.
    pub waiting: Vec<(u64, u64, u64)>,
}

impl Held for Value {
    fn merge(&mut self, other: &Self) -> bool {
        Self::merge(self, other)
    }

    /// The contract's three parts, the bucket's two, the count, and for each record its data
    /// plane, highest applied and last admission, and each batch waiting's number, count and
    /// time.
    fn bytes(&self) -> u64 {
        let records: u64 = self
            .planes
            .iter()
            .map(|(_, record)| 24 + 24 * record.waiting_len() as u64)
            .sum();
        56 + records
    }
}

/// What the owner knows of one data plane, for one key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Record {
    /// The highest of its batches applied, every one before it too.
    pub(crate) applied: u64,
    /// Batches that came ahead of one still missing, `(sequence, tally)` in order: none, and no
    /// room for any, while batches come in order, as they almost always do. At most
    /// [`MOST_UNACKNOWLEDGED`], as many as a data plane has out, so that one sending more
    /// cannot grow it.
    #[allow(
        clippy::box_collection,
        reason = "a pointer in every record, rather than a list's three words, for what is rarely there"
    )]
    pub(crate) waiting: Option<Box<Vec<(u64, Tally)>>>,
    /// When it last admitted the key, by the wall clock.
    pub(crate) last_admitted: Admitted,
}

/// A batch's hits as its owner applies them: how many, and as of when they are charged, by the
/// wall clock. Ordered by count, then time: of the same batch twice, the smaller is the one
/// sent again emptied, or, as many, the one charged earlier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Tally {
    pub(crate) count: u64,
    pub(crate) at: u64,
}

impl Tally {
    /// Counts come from the wire: as many as a count holds, never wrapped round. Charged as of
    /// the earlier, as one entry's hits are as of its earliest.
    fn add(self, other: Self) -> Self {
        Self {
            count: self.count.saturating_add(other.count),
            at: self.at.min(other.at),
        }
    }
}

/// When a data plane last admitted a key, if it has, in eight bytes rather than an option's
/// sixteen: a record is kept for every data plane admitting every key. One past the time, 0 if
/// never; ordered as the option is, never first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Admitted(u64);

impl Admitted {
    /// Admitted at `at`.
    pub(crate) const fn at(at: u64) -> Self {
        Self(at.saturating_add(1))
    }

    /// When, if ever.
    pub(crate) const fn get(self) -> Option<u64> {
        self.0.checked_sub(1)
    }
}

impl Record {
    /// Batch `sequence` of `tally` hits arrives: the hits now ready to apply, of it and of the
    /// batches kept waiting for it, each batch once. A batch already waiting keeps the smaller
    /// tally of the two, as a merge does.
    pub(crate) fn take(&mut self, sequence: u64, tally: Tally) -> Tally {
        if sequence <= self.applied {
            return Tally::default();
        }
        if sequence > self.applied + 1 {
            let waiting = self.waiting.get_or_insert_default();
            match waiting.binary_search_by_key(&sequence, |(at, _)| *at) {
                Ok(at) => waiting[at].1 = waiting[at].1.min(tally),
                // Answered unapplied, it comes again, as one whose answer was lost does.
                Err(_) if waiting.len() >= MOST_UNACKNOWLEDGED => {}
                Err(at) => waiting.insert(at, (sequence, tally)),
            }
            return Tally::default();
        }
        self.applied = sequence;
        let mut ready = tally;
        if let Some(waiting) = &mut self.waiting {
            // All ahead of it: the first of them follow on from it. One at or below it, left
            // by a merge with a copy that had applied further, was applied already.
            let mut caught_up = 0;
            for &(at, waited) in waiting.iter() {
                if at <= self.applied {
                    caught_up += 1;
                    continue;
                }
                if at != self.applied + 1 {
                    break;
                }
                self.applied = at;
                ready = ready.add(waited);
                caught_up += 1;
            }
            waiting.drain(..caught_up);
            if waiting.is_empty() {
                self.waiting = None;
            }
        }
        ready
    }

    /// The data plane has seen batches up to `seen` applied, and will not send them again.
    pub(crate) fn skip_to(&mut self, seen: u64) {
        self.applied = seen;
        if let Some(waiting) = &mut self.waiting {
            waiting.retain(|(sequence, _)| *sequence > seen);
            if waiting.is_empty() {
                self.waiting = None;
            }
        }
    }

    /// How many batches wait.
    pub(crate) fn waiting_len(&self) -> usize {
        self.waiting.as_ref().map_or(0, |waiting| waiting.len())
    }

    /// Takes in `theirs`, a copy's record of the same data plane: the larger highest applied
    /// and last admission, and of the batches waiting the union of those above the highest
    /// applied, the same batch twice taking the smaller tally, the lowest
    /// [`MOST_UNACKNOWLEDGED`] kept. Whether it changed anything.
    fn merge(&mut self, theirs: &Self) -> bool {
        let mut changed = false;
        if theirs.applied > self.applied {
            self.applied = theirs.applied;
            changed = true;
        }
        if theirs.last_admitted > self.last_admitted {
            self.last_admitted = theirs.last_admitted;
            changed = true;
        }
        if self.waiting.is_none() && theirs.waiting.is_none() {
            return changed;
        }
        let mine: &[(u64, Tally)] = self.waiting.as_deref().map_or(&[], Vec::as_slice);
        let others: &[(u64, Tally)] = theirs.waiting.as_deref().map_or(&[], Vec::as_slice);
        let above = |list: &[(u64, Tally)]| -> Vec<(u64, Tally)> {
            list.iter()
                .copied()
                .filter(|(sequence, _)| *sequence > self.applied)
                .collect()
        };
        let (mine, others) = (above(mine), above(others));
        let mut merged = Vec::with_capacity(mine.len().max(others.len()));
        let (mut a, mut b) = (mine.iter().peekable(), others.iter().peekable());
        while merged.len() < MOST_UNACKNOWLEDGED {
            let next = match (a.peek(), b.peek()) {
                (Some(&&(x, tx)), Some(&&(y, ty))) if x == y => {
                    a.next();
                    b.next();
                    (x, tx.min(ty))
                }
                (Some(&&(x, tx)), Some(&&(y, _))) if x < y => {
                    a.next();
                    (x, tx)
                }
                (_, Some(&&(y, ty))) => {
                    b.next();
                    (y, ty)
                }
                (Some(&&(x, tx)), None) => {
                    a.next();
                    (x, tx)
                }
                (None, None) => break,
            };
            merged.push(next);
        }
        let held = self.waiting.as_deref().map_or(&[][..], Vec::as_slice);
        if merged != held {
            changed = true;
        }
        self.waiting = (!merged.is_empty()).then(|| Box::new(merged));
        changed
    }
}

/// What the owner keeps of the data planes admitting a key, one record each, in order of data
/// plane and each found by halving the list: most keys are admitted by one data plane, and a
/// list has room for no more than it needs where a B-tree's first node has room for eleven.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Records(Vec<(PlaneId, Record)>);

impl Records {
    fn find(&self, plane: PlaneId) -> Result<usize, usize> {
        self.0.binary_search_by_key(&plane, |(id, _)| *id)
    }

    pub(crate) fn get(&self, plane: PlaneId) -> Option<&Record> {
        let at = self.find(plane).ok()?;
        self.0.get(at).map(|(_, record)| record)
    }

    /// The record of `plane`, made if there is none.
    pub(crate) fn entry(&mut self, plane: PlaneId) -> &mut Record {
        let at = match self.find(plane) {
            Ok(at) => at,
            Err(at) => {
                // Room for one at first, as most keys stay at one; then twice the room.
                if self.0.len() == self.0.capacity() {
                    self.0.reserve_exact(self.0.len().max(1));
                }
                self.0.insert(at, (plane, Record::default()));
                at
            }
        };
        // `at` is in the list: found there, or just put there.
        &mut self.0[at].1
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (PlaneId, &Record)> {
        self.0.iter().map(|(plane, record)| (*plane, record))
    }

    /// How many records there is room for.
    #[cfg(test)]
    pub(crate) fn room(&self) -> usize {
        self.0.capacity()
    }

    /// The latest admission any record holds.
    pub(crate) fn latest(&self) -> Option<u64> {
        self.0
            .iter()
            .filter_map(|(_, record)| record.last_admitted.get())
            .max()
    }

    /// Whether a record is kept beside `latest`, the latest admission of the records it is
    /// among: it has admitted, within `kept` of that.
    fn keeps(record: &Record, latest: Option<u64>, kept: u64) -> bool {
        matches!((record.last_admitted.get(), latest), (Some(at), Some(latest)) if latest.saturating_sub(at) <= kept)
    }

    /// Lets go of the records [`keeps`](Self::keeps) does not keep, and of the room they took:
    /// how many went. Read from the records alone, no clock, so that copies retiring in any
    /// order keep the same.
    fn retire(&mut self, kept: u64) -> usize {
        let latest = self.latest();
        let before = self.0.len();
        self.0
            .retain(|(_, record)| Self::keeps(record, latest, kept));
        let gone = before - self.0.len();
        if gone > 0 {
            self.0.shrink_to_fit();
        }
        gone
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner::apply;
    use edgerush_cluster::Kept;
    use edgerush_ratelimit::reference::Model;
    use edgerush_ratelimit::service::Report;
    use proptest::prelude::*;
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;
    const CYCLE: u64 = 10 * MS;

    /// 100 a second, a bucket of ten.
    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        }
    }

    fn hits(sequence: u64, count: u64, seen: u64) -> Report {
        Report::Hits {
            sequence,
            count,
            seen,
            age: 0,
        }
    }

    /// A data plane as the owner sees it: its next batch's number, the highest it was told
    /// applied, and its batches not yet seen applied, each with its count.
    #[derive(Default)]
    struct Plane {
        next: u64,
        seen: u64,
        out: Vec<(u64, u64)>,
    }

    /// What one data plane does: sends a new batch, lost on the way or not; sends its oldest
    /// unseen again, emptied or not; or polls.
    #[derive(Clone, Debug)]
    enum Step {
        Send { count: u64, lost: bool },
        Again { emptied: bool },
        Poll,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            3 => (0u64..20, any::<bool>()).prop_map(|(count, lost)| Step::Send { count, lost }),
            1 => any::<bool>().prop_map(|emptied| Step::Again { emptied }),
            1 => Just(Step::Poll),
        ]
    }

    /// An owner's value after each of `steps`, each by one of four data planes after a gap:
    /// the copies it would send, in the order made.
    fn history(steps: &[(u64, Step, u64)]) -> Vec<Value> {
        let mut planes: Vec<Plane> = (0..4)
            .map(|_| Plane {
                next: 1,
                ..Plane::default()
            })
            .collect();
        let mut kept = Kept::<Value>::default();
        let mut now = 0;
        let mut copies = Vec::new();
        for (plane, step, gap) in steps {
            now += gap;
            let id = *plane;
            let plane = &mut planes[usize::try_from(id).unwrap()];
            let report = match step {
                Step::Send { count, lost } => {
                    let sequence = plane.next;
                    plane.next += 1;
                    plane.out.push((sequence, *count));
                    if *lost {
                        continue;
                    }
                    hits(sequence, *count, plane.seen)
                }
                Step::Again { emptied } => {
                    let Some(&(sequence, count)) = plane.out.first() else {
                        continue;
                    };
                    hits(sequence, if *emptied { 0 } else { count }, plane.seen)
                }
                Step::Poll => Report::Poll,
            };
            let (answer, _) = apply(&mut kept, id, contract(), report, CYCLE, now);
            plane.seen = plane.seen.max(answer.applied);
            let seen = plane.seen;
            plane.out.retain(|(sequence, _)| *sequence > seen);
            copies.push(kept.value.clone());
        }
        copies
    }

    /// The copies in an order of `seed`'s.
    fn shuffled(mut copies: Vec<Value>, mut seed: u64) -> Vec<Value> {
        for at in (1..copies.len()).rev() {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let other = usize::try_from(seed >> 33).unwrap() % (at + 1);
            copies.swap(at, other);
        }
        copies
    }

    /// The copies merged into a key never made, in order, each merge saying whether it
    /// changed anything as it did.
    fn merged(copies: &[Value]) -> Value {
        let mut value = Value::default();
        for copy in copies {
            let before = value.clone();
            let changed = value.merge(copy);
            assert_eq!(changed, value != before, "said changed: {changed}");
        }
        value
    }

    proptest! {
        /// The owner's copies, merged in any order and any number of times, end as the owner's
        /// value: what a replica holds once every copy has reached it, whichever came first.
        #[test]
        fn copies_merged_in_any_order_and_twice_end_as_the_owner(
            steps in proptest::collection::vec((0u64..4, step(), 0u64..30 * MS), 1..60),
            seed in any::<u64>(),
        ) {
            let copies = history(&steps);
            let Some(owner) = copies.last().cloned() else {
                return Ok(());
            };
            prop_assert_eq!(&merged(&copies), &owner, "in order");
            let mut backwards = copies.clone();
            backwards.reverse();
            prop_assert_eq!(&merged(&backwards), &owner, "backwards");
            let mixed = shuffled(copies.clone(), seed);
            let mut twice = merged(&mixed);
            prop_assert_eq!(&twice, &owner, "shuffled");
            for copy in &mixed {
                prop_assert!(!twice.merge(copy), "a copy taken again changes nothing");
            }
            prop_assert_eq!(&twice, &owner);
        }

        /// A merge never lowers a part: the merged bucket admits no more than either, the
        /// count and every record kept are as high as either's.
        #[test]
        fn a_merge_never_lowers_a_part(
            steps in proptest::collection::vec((0u64..4, step(), 0u64..30 * MS), 2..60),
            pick in any::<(prop::sample::Index, prop::sample::Index)>(),
        ) {
            let copies = history(&steps);
            prop_assume!(copies.len() >= 2);
            let (a, b) = (&copies[pick.0.index(copies.len())], &copies[pick.1.index(copies.len())]);
            let mut both = a.clone();
            both.merge(b);
            let now = 2 * 60 * 30 * MS;
            for side in [a, b] {
                prop_assert!(both.bucket().unwrap().remaining(now) <= side.bucket().unwrap().remaining(now));
                prop_assert!(both.counted() >= side.counted());
                for plane in both.planes() {
                    prop_assert!(both.applied(plane) >= side.applied(plane));
                    prop_assert!(both.admitted(plane) >= side.admitted(plane));
                }
            }
        }

        /// Every copy an owner sends, batches waiting among them, is itself again from its
        /// parts, in whatever order the wire gives its records and batches.
        #[test]
        fn a_value_is_itself_again_from_its_parts(
            steps in proptest::collection::vec((0u64..4, step(), 0u64..30 * MS), 1..60),
        ) {
            for copy in history(&steps) {
                let mut parts = copy.parts();
                parts.planes.reverse();
                for plane in &mut parts.planes {
                    plane.waiting.reverse();
                }
                prop_assert_eq!(Value::from_parts(parts), Some(copy));
            }
        }
    }

    /// Parts no value could have had are refused: a data plane twice, a batch waiting twice,
    /// more batches waiting than a data plane may have out.
    #[test]
    fn parts_no_value_could_have_are_refused() {
        let plane = |plane, waiting: Vec<(u64, u64, u64)>| PlaneParts {
            plane,
            applied: 1,
            admitted: Some(5),
            waiting,
        };
        let parts = |planes| Parts {
            contract: Some(contract()),
            bucket: BucketState::default(),
            counted: 3,
            planes,
        };
        assert!(
            Value::from_parts(parts(vec![plane(1, vec![(3, 1, 4)]), plane(2, vec![])])).is_some()
        );
        assert_eq!(
            Value::from_parts(parts(vec![plane(1, vec![]), plane(1, vec![])])),
            None
        );
        assert_eq!(
            Value::from_parts(parts(vec![plane(1, vec![(3, 1, 4), (3, 2, 4)])])),
            None
        );
        let most = u64::try_from(MOST_UNACKNOWLEDGED).unwrap();
        let full: Vec<_> = (3..3 + most).map(|sequence| (sequence, 1, 4)).collect();
        assert!(Value::from_parts(parts(vec![plane(1, full.clone())])).is_some());
        let over: Vec<_> = (3..4 + most).map(|sequence| (sequence, 1, 4)).collect();
        assert_eq!(Value::from_parts(parts(vec![plane(1, over)])), None);
    }

    /// A record the owner has retired, still held in a replica's older copy, does not come
    /// back with the merge: the replica lets it go too, and holds what the owner holds.
    #[test]
    fn a_record_the_owner_retired_goes_from_a_replica_with_the_merge() {
        let mut kept = Kept::<Value>::default();
        apply(&mut kept, 1, contract(), hits(1, 1, 0), CYCLE, 0);
        let mut replica = kept.value.clone();
        apply(
            &mut kept,
            2,
            contract(),
            hits(1, 1, 0),
            CYCLE,
            RECORD_KEPT + CYCLE,
        );
        assert_eq!(kept.value.planes().collect::<Vec<_>>(), [2]);
        assert!(replica.merge(&kept.value));
        assert_eq!(replica, kept.value);
        let mut owner = kept.value.clone();
        let old: Value = {
            let mut old = Kept::<Value>::default();
            apply(&mut old, 1, contract(), hits(1, 1, 0), CYCLE, 0);
            old.value
        };
        assert!(!owner.merge(&old), "the older copy brings nothing back");
        assert_eq!(owner, kept.value);
    }

    /// Under another contract a copy replaces a value never charged — never made, or made by
    /// polls alone — and is otherwise ignored.
    #[test]
    fn a_copy_under_another_contract_replaces_only_a_value_never_charged() {
        let mut bigger = contract();
        bigger.burst = NonZeroU64::new(20).unwrap();
        let charged = |contract, count| {
            let mut kept = Kept::<Value>::default();
            apply(&mut kept, 1, contract, hits(1, count, 0), CYCLE, 0);
            kept.value
        };
        let new = charged(bigger, 3);

        let mut never_made = Value::default();
        assert!(never_made.merge(&new));
        assert_eq!(never_made, new);

        let mut polled = Kept::<Value>::default();
        apply(&mut polled, 1, contract(), Report::Poll, CYCLE, 0);
        assert_eq!(polled.value.contract(), Some(contract()));
        assert!(polled.value.merge(&new));
        assert_eq!(polled.value, new);

        let mut old = charged(contract(), 5);
        let before = old.clone();
        assert!(!old.merge(&new));
        assert_eq!(old, before, "kept under its own contract");
    }

    /// Batches waiting merge as the union of those above the highest applied, the same batch
    /// twice taking the smaller tally, the lowest [`MOST_UNACKNOWLEDGED`] kept.
    #[test]
    fn batches_waiting_merge_as_the_union_above_the_highest_applied() {
        let tally = |count| Tally { count, at: 0 };
        let with = |applied, waiting: Vec<(u64, Tally)>| {
            let mut value = Value::new(contract());
            let record = value.planes.entry(1);
            record.applied = applied;
            record.last_admitted = Admitted::at(0);
            record.waiting = (!waiting.is_empty()).then(|| Box::new(waiting));
            value
        };
        let mut a = with(1, vec![(3, tally(4)), (5, tally(1))]);
        let b = with(3, vec![(5, tally(0)), (6, tally(2))]);
        assert!(a.merge(&b));
        assert_eq!(a, with(3, vec![(5, tally(0)), (6, tally(2))]));

        // A batch waiting alone taken in is a change, for the store to copy on; taken again,
        // none.
        let mut alone = with(3, Vec::new());
        assert!(alone.merge(&with(3, vec![(5, tally(1))])));
        assert!(!alone.merge(&with(3, vec![(5, tally(1))])));
        assert!(
            alone.merge(&with(3, vec![(5, tally(0))])),
            "a smaller tally"
        );

        let most = MOST_UNACKNOWLEDGED as u64;
        let mut low = with(1, (3..3 + most).map(|s| (s, tally(1))).collect());
        let high = with(1, (2..2 + most).step_by(2).map(|s| (s, tally(1))).collect());
        low.merge(&high);
        let waiting = low.planes.get(1).unwrap().waiting.as_ref().unwrap();
        assert_eq!(waiting.len(), MOST_UNACKNOWLEDGED);
        assert_eq!(waiting.first().unwrap().0, 2, "the lowest kept");
        assert_eq!(waiting.last().unwrap().0, 1 + most);
    }

    /// The store counts the contract, the bucket and the count, and each record with each of
    /// its batches waiting.
    #[test]
    fn a_value_counts_its_records_and_batches_waiting() {
        let mut kept = Kept::<Value>::default();
        apply(&mut kept, 1, contract(), Report::Poll, CYCLE, 0);
        assert_eq!(kept.value.bytes(), 56, "a poll leaves no record");
        apply(&mut kept, 1, contract(), hits(1, 1, 0), CYCLE, 0);
        assert_eq!(kept.value.bytes(), 56 + 24);
        apply(&mut kept, 1, contract(), hits(3, 1, 1), CYCLE, 0);
        assert_eq!(kept.value.bytes(), 56 + 24 + 24);
    }

    /// Two states of one bucket merge as the further charged, in either order.
    #[test]
    fn the_larger_of_two_bucket_states_is_the_further_charged() {
        let mut bucket = contract().bucket();
        bucket.charge_many(0, 3);
        let early = bucket.state();
        bucket.charge_many(5 * MS, 4);
        let later = bucket.state();
        assert_eq!(early.larger(later), later);
        assert_eq!(later.larger(early), later);
        assert_eq!(later.larger(later), later);
    }
}
