//! One rate-limit participant per process, its workers deciding warm requests from shared
//! per-key state by compare-and-swap (06 §4).
//!
//! The process is one data plane of the protocol (one [`PlaneId`] for its life). A key's
//! admission state — its bucket, the cycle's allowance and carry, what was admitted for the
//! transport to report, what is held for a request under several keys — is one 128-bit word
//! in an [`Entry`] that every worker holds a handle to. A warm request is decided by the
//! worker with one compare-and-swap on that word, the cycle advanced in the same step if it
//! is due. Nothing on that path waits on another thread.
//!
//! The [`Participant`] is the transport: the process thread's side, single-threaded. It
//! sends what was admitted every cycle, rebases each key's bucket on the owner's answers,
//! and evicts idle keys. It asks no one: a key no process has heard of starts from a full
//! bucket. It is `DataPlane`'s protocol with the per-key state moved into the
//! entries; `DataPlane` itself is kept as the reference it is tested against.
//!
//! No I/O: the time is passed in; the transport's messages are the caller's to carry.

mod chunked;
mod sync;
mod word;

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod tests;

pub use word::fits;

use super::{
    Answers, Batch, Contract, Entry as BatchEntry, PlaneId, PodId, Queued, RESEND_AFTER_CYCLES,
    Report, Room, Unseen, View, encoded_entry_bytes, longest,
};
use chunked::MinHeap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::{Arc, RwLock};
use sync::{AtomicBool, AtomicU32, AtomicU64, AtomicU128, Ordering, fence};
use word::{Advance, Bucket, Fields, Step};

/// A worker's own decision of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Local {
    /// Every key had room: admitted, and taken from each.
    Admit,
    /// A key has no room, and no hold still outstanding could explain it: refused.
    Refuse,
    /// A key is closed, or the worker has none: for the transport to decide.
    Ask,
    /// Not to be decided now without a refusal that may be false or a count that could be
    /// lost: a key's holds or its unreported admissions are at their most, or a key has no
    /// room while another request's holds are outstanding. Nothing of it is held; the
    /// worker waits ([`Entry::add_waiter`]) and decides it again, whole, when the keys'
    /// holds settle or their counts are drained.
    Defer,
}

/// A worker's decision and what it asks of others.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// The decision.
    pub local: Local,
    /// A key's unreported admissions passed half their most: the transport should drain it
    /// ([`Participant::drain`]) before the next cycle. Said once until it does.
    pub drain: bool,
    /// A hold was settled on a key requests wait on: wake them.
    pub wake: bool,
    /// The keys, by position (bit `n` for the `n`th), that now want the transport's
    /// attention: their count started, nothing having been admitted since it was last
    /// drained, or a request brought them back into use after a refill time without one.
    /// Each to be handed to it ([`Participant::on_signals`]) where it visits only the keys
    /// that need it ([`Participant::set_scheduled`]), once claimed
    /// ([`Entry::claim_signal`]): a key's signal already on its way carries the rest.
    pub signal: u64,
}

/// The transport's decision of a request a worker asked about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Admitted under every key.
    Admit,
    /// Refused.
    Refuse,
    /// As [`Local::Defer`].
    Defer,
}

/// What a participant may give up of accuracy for throughput, each the user's to choose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Levers {
    /// Whether a key with nothing to report is polled every cycle while it is in use, to
    /// keep its copy fresh. Off, a quiet key's copy is as fresh as its last answer until it
    /// has something to report again: messages no longer grow with the keys in use, only
    /// with those admitting.
    pub poll_quiet: bool,
}

impl Default for Levers {
    /// The protocol as `DataPlane` keeps it: quiet keys polled.
    fn default() -> Self {
        Self { poll_quiet: true }
    }
}

/// Shards of a [`Registry`]: so many that each holds few of the process's keys. A shard's
/// table grows by rehashing all of it under the shard's lock, some 70 ns a key it holds,
/// which the worker making the key waits for, and any other wanting the shard: at 4,096,
/// some 16 µs for a million keys, half a millisecond for 30 million.
const SHARDS: usize = 4_096;

/// One shard of a [`Registry`]: its entries by key.
type Shard = RwLock<HashSet<Handle>>;

/// A handle to an entry, found in a set by the entry's key: a set of them is a map from
/// keys to entries with no copy of any key.
#[derive(Clone)]
pub struct Handle(Arc<Entry>);

impl Handle {
    /// A handle to `entry`.
    #[must_use]
    pub fn new(entry: Arc<Entry>) -> Self {
        Self(entry)
    }

    /// Its entry.
    #[must_use]
    pub fn entry(&self) -> &Arc<Entry> {
        &self.0
    }
}

impl std::borrow::Borrow<[u8]> for Handle {
    fn borrow(&self) -> &[u8] {
        &self.0.key
    }
}

impl std::hash::Hash for Handle {
    // As the key's own, which `Borrow` requires.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.key.hash(state);
    }
}

impl PartialEq for Handle {
    fn eq(&self, other: &Self) -> bool {
        self.0.key == other.0.key
    }
}

impl Eq for Handle {}

/// Every live entry of a participant, for a worker meeting a key it holds no handle to:
/// it finds the pod's entry here rather than asking the transport, which may be held up.
/// Read only then — once per key per worker — and written by the transport when it makes
/// or evicts an entry: no lock on the request path. Sharded by the participant's keyed
/// hash, so that a client cannot choose keys that crowd one shard and so make its table,
/// and the stall of its growth, as large as all the process's.
pub struct Registry<S> {
    hasher: S,
    shards: Vec<Shard>,
    /// A worker could not hand the transport a key that wanted it: the next cycle visits
    /// every key ([`Self::require_scan`]).
    scan: AtomicBool,
    /// What keys share, by contract: read when an entry is made.
    terms: RwLock<HashMap<Contract, Arc<Terms>>>,
}

impl<S: BuildHasher + Default> Default for Registry<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S: BuildHasher> Registry<S> {
    fn new(hasher: S) -> Self {
        Self {
            hasher,
            shards: (0..SHARDS).map(|_| RwLock::new(HashSet::new())).collect(),
            scan: AtomicBool::new(false),
            terms: RwLock::new(HashMap::new()),
        }
    }

    /// The shard of `key`, by number.
    fn index(&self, key: &[u8]) -> usize {
        // Less than the shard count, which fits in a `usize`.
        (self.hasher.hash_one(key) % SHARDS as u64) as usize
    }

    fn shard(&self, key: &[u8]) -> &Shard {
        // `index` is always less than the shard count.
        &self.shards[self.index(key)]
    }

    /// The live entry of `key` under `contract`, if the participant has made one.
    #[must_use]
    pub fn find(&self, key: &[u8], contract: Contract) -> Option<Arc<Entry>> {
        let shard = self
            .shard(key)
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shard
            .get(key)
            .map(Handle::entry)
            .filter(|entry| entry.terms.contract == contract && !entry.snapshot().closed)
            .cloned()
    }

    /// The live entry of `key` under `contract`, or, if there is none, one made at `now`
    /// for a participant whose cold keys are decided locally — its bucket full, its cycle
    /// `cycle` — and kept: with whether it was made here, for whoever made it to have the
    /// participant adopt it ([`Participant::adopt`]). `None` if a live entry of the key is
    /// under another contract: the participant replaces it. Taken by a worker meeting a key
    /// its process has not, once; under the shard's lock, so that two workers meeting it at
    /// once share one entry.
    #[must_use]
    pub fn find_or_make(
        &self,
        key: &[u8],
        contract: Contract,
        cycle: u64,
        now: u64,
    ) -> Option<(Arc<Entry>, bool)> {
        let mut shard = self
            .shard(key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = shard.get(key).map(Handle::entry)
            && !held.snapshot().closed
        {
            return (held.terms.contract == contract).then(|| (Arc::clone(held), false));
        }
        let entry = Arc::new(Entry::with_terms(
            key.into(),
            self.terms(contract, cycle),
            now,
        ));
        shard.replace(Handle(Arc::clone(&entry)));
        Some((entry, true))
    }

    /// What keys under `contract` share, at `cycle`: made the first time it is asked for.
    fn terms(&self, contract: Contract, cycle: u64) -> Arc<Terms> {
        let terms = self
            .terms
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&contract)
            .cloned();
        terms.unwrap_or_else(|| {
            let mut terms = self
                .terms
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(
                terms
                    .entry(contract)
                    .or_insert_with(|| Arc::new(Terms::new(contract, cycle))),
            )
        })
    }

    /// Asks for every key to be visited at the next cycle: a worker could not hand the
    /// transport a key its signal named ([`Outcome::signal`]), its queue full. Sticky until
    /// the transport takes it, which it does before it visits them, so that a signal lost
    /// during that visit asks for another.
    pub fn require_scan(&self) {
        self.scan.store(true, Ordering::Release);
    }

    fn take_scan(&self) -> bool {
        self.scan.swap(false, Ordering::AcqRel)
    }

    fn insert(&self, entry: Arc<Entry>) {
        let mut shard = self
            .shard(&entry.key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shard.replace(Handle(entry));
    }

    /// Removes `entry`, if it is still the one held for its key.
    fn remove(&self, entry: &Arc<Entry>) {
        let mut shard = self
            .shard(&entry.key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if shard
            .get(&*entry.key)
            .is_some_and(|held| Arc::ptr_eq(&held.0, entry))
        {
            shard.remove(&*entry.key);
        }
    }
}

/// Whether the machine has the native 128-bit compare-and-swap the entries need, found at
/// run time where the build does not assume it: the participant does not start without
/// it rather than put the fallback's lock on the request path.
#[must_use]
pub fn supported() -> bool {
    AtomicU128::is_lock_free()
}

/// One key's shared state, for one incarnation of it: a key evicted and met again is a
/// new entry, made only once the old one is closed.
pub struct Entry {
    key: Box<[u8]>,
    /// What it shares with every key under its contract.
    terms: Arc<Terms>,
    word: AtomicU128,
    /// The cycle the word was last advanced to, coarsely ([`word::record_of`]): written
    /// after the compare-and-swap that advanced it, so never ahead of it, and read before
    /// the word, so that a 16-bit cycle number that wrapped is told from one that did not.
    record: AtomicU32,
    /// Other participants the owner last said admit the key.
    others: AtomicU64,
    last_request: AtomicU64,
    /// When a worker last started the key's count, from none: the earliest of the hits the
    /// transport drains, which they are charged as of. Written before the compare-and-swap
    /// that starts the count, so that the drain taking the count sees it; one taking a count
    /// started after it may see a later one, which errs towards charging late.
    started: AtomicU64,
    /// Compare-and-swap attempts that lost to another and went round again.
    retries: AtomicU64,
    /// The most admitted and not yet drained, holds included: `word::MOST_RESERVED`, or
    /// less in tests.
    most_reserved: u64,
    /// Requests deferred on it, waiting for its holds to settle or its count to drain.
    waiters: AtomicU64,
    /// Its count passed half its most and the transport was told to drain it.
    pressed: AtomicBool,
    /// A signal of it is on its way to the transport: held in a worker's batch, or queued
    /// ([`Self::claim_signal`]).
    signalled: AtomicBool,
}

/// What every key under one contract shares, made once a contract and pointed to by each
/// key's entry rather than copied into it: the contract, its bucket's constants, its refill
/// time, and the cycle and what the contract refills in one.
#[derive(Debug)]
struct Terms {
    contract: Contract,
    bucket: Bucket,
    /// The contract's refill time: how long a copy stays fresh after an answer.
    hold: u64,
    /// The protocol's cycle, and what the contract refills in one, in requests and in units.
    cycle: u64,
    refill: u64,
    refill_units: u64,
}

impl Terms {
    fn new(contract: Contract, cycle: u64) -> Self {
        Self {
            contract,
            bucket: Bucket::new(contract),
            hold: contract.refill_time(),
            cycle,
            refill: contract.refilled(cycle),
            refill_units: Bucket::new(contract).units(cycle),
        }
    }
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("fields", &Fields::unpack(self.word.load(Ordering::Acquire)))
            .finish_non_exhaustive()
    }
}

/// What an operation on a word came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Taken {
    Done,
    /// Done, and its count passed half its most for the first time since drained.
    Pressed,
    /// No room, and nothing held on it.
    Refused,
    /// The entry is closed: ask for the key's new one.
    Closed,
    /// Its holds, or its admitted-and-held count, at their most: nothing taken.
    Full,
    /// No room while holds are outstanding on it, which may yet be given back.
    Contended,
}

impl Taken {
    fn done(self) -> bool {
        matches!(self, Self::Done | Self::Pressed)
    }
}

/// What to do to a word.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Admit,
    Hold,
}

/// Whether an operation advances the cycle first if it is due: always, but for the
/// transport's work before a cycle's advance, which `DataPlane::on_cycle` does first —
/// asks given up and what waited on them decided in the cycle that is ending.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cycle {
    Advance,
    AsItIs,
}

impl Entry {
    /// An entry with terms of its own, for tests of one key.
    #[cfg(test)]
    fn new(key: Box<[u8]>, contract: Contract, cycle: u64, now: u64) -> Self {
        Self::with_terms(key, Arc::new(Terms::new(contract, cycle)), now)
    }

    fn with_terms(key: Box<[u8]>, terms: Arc<Terms>, now: u64) -> Self {
        let cycle = terms.cycle;
        let fields = Fields {
            arrival: Bucket::full(terms.bucket.units(now)),
            // No share bound until a cycle sets one, as a new `DataPlane` key has none.
            left: word::MOST_LEFT,
            spent: 0,
            cycle: word::cycle_of(now, cycle),
            shared: false,
            closed: false,
            unreported: 0,
            held: 0,
            set: false,
        };
        Self {
            key,
            terms,
            word: AtomicU128::new(fields.pack()),
            record: AtomicU32::new(word::record_of(now, cycle)),
            others: AtomicU64::new(0),
            last_request: AtomicU64::new(now),
            started: AtomicU64::new(now),
            retries: AtomicU64::new(0),
            most_reserved: word::MOST_RESERVED,
            waiters: AtomicU64::new(0),
            pressed: AtomicBool::new(false),
            signalled: AtomicBool::new(false),
        }
    }

    /// Claims its signal ([`Outcome::signal`]) for a worker to hand the transport: false if
    /// one is on its way already, which carries this one too — the transport takes the
    /// claim ([`Participant::on_signals`]) before it visits the key, and the swap's acquire
    /// there sees whatever a worker did before it found the claim taken. A worker that
    /// cannot hand its claim over gives it back ([`Self::unclaim_signal`]) before asking for
    /// a scan instead, so that no key stays claimed with nothing on its way.
    pub fn claim_signal(&self) -> bool {
        !self.signalled.swap(true, Ordering::AcqRel)
    }

    /// Gives back a claim on its signal: its signal is no longer on its way.
    pub fn unclaim_signal(&self) {
        self.signalled.swap(false, Ordering::AcqRel);
    }

    /// A request deferred on it waits from now: a hold settled or the count drained after
    /// this is seen by whoever settles or drains it ([`Outcome::wake`],
    /// [`Participant::take_wake`]). Called before the request is decided again, so that no
    /// wake-up falls between the two.
    pub fn add_waiter(&self) {
        wait_on(&self.waiters);
    }

    /// A deferred request waits on it no longer.
    pub fn remove_waiter(&self) {
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }

    /// Whether a change just made to the word is one deferred requests wait for. The fence
    /// pairs with `add_waiter`'s: either the waiter sees the change, or this sees it.
    fn wanted(&self) -> bool {
        waited_on(&self.waiters)
    }

    /// Its key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Its contract.
    #[must_use]
    pub fn contract(&self) -> Contract {
        self.terms.contract
    }

    /// Whether it is closed: evicted or replaced, never to decide again, a worker holding
    /// it to drop it.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.snapshot().closed
    }

    /// Compare-and-swap attempts on it that went round again, all told.
    #[must_use]
    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    fn advance(&self) -> Advance {
        Advance {
            cycle: self.terms.cycle,
            refill: self.terms.refill,
            refill_units: self.terms.refill_units,
            others: self.others.load(Ordering::Acquire),
            record: self.record.load(Ordering::Acquire),
        }
    }

    /// Applies `change` to the word until it holds, the cycle advanced first if due and
    /// `cycle` says so: `change` says the new fields, or none to leave the word as it is.
    fn update(
        &self,
        now: u64,
        cycle: Cycle,
        mut change: impl FnMut(Fields) -> Option<Fields>,
    ) -> Option<Fields> {
        // The record read before the word, so that it is no later than the word's cycle.
        let advance = self.advance();
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            let before = Fields::unpack(current);
            // Whether this advances the cycle: by the step, not by the 16-bit number, which
            // a gap of a multiple of its range leaves as it was.
            let moved = cycle == Cycle::Advance
                && word::step(before.cycle, advance.record, now, advance.cycle) != Step::Same;
            let fields = if moved {
                word::advanced(&self.terms.bucket, before, advance, now)
            } else {
                before
            };
            let changed = change(fields)?;
            match self.word.compare_exchange_weak(
                current,
                changed.pack(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let record = word::record_of(now, advance.cycle);
                    // Changed once every few hundred cycles: read first, written then.
                    if moved && self.record.load(Ordering::Relaxed) < record {
                        self.record.fetch_max(record, Ordering::AcqRel);
                    }
                    return Some(changed);
                }
                Err(seen) => {
                    if seen != current {
                        self.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    current = seen;
                }
            }
        }
    }

    /// Takes one request's worth at `now`: admitted, or held for a request waiting on an
    /// ask about its other keys.
    fn take(&self, op: Op, now: u64) -> (Taken, bool) {
        self.take_in(op, now, now, Cycle::Advance)
    }

    /// Taking does not count as the key's use: a request's arrival does, which the
    /// transport's own takes, settling what waited, are not ([`decide`] marks it). With
    /// whether it started the key's count: an admission counted where none was, read from
    /// the compare-and-swap that took it, never from an attempt. Taken at `now` for a
    /// request that arrived at `at`: the bucket as of then, the cycle as of now.
    fn take_in(&self, op: Op, at: u64, now: u64, cycle: Cycle) -> (Taken, bool) {
        let units = self.terms.bucket.units(at);
        let mut why = Taken::Refused;
        let mut was = 0;
        let taken = self.update(now, cycle, |fields| {
            was = fields.unreported;
            why = if fields.closed {
                Taken::Closed
            } else if fields.unreported + fields.held >= self.most_reserved
                || (op == Op::Hold && fields.held == word::MOST_HELD)
            {
                // Out of places to count it, which says nothing of the client's room.
                Taken::Full
            } else if fields.left == 0 || !self.terms.bucket.admits(fields.arrival, units) {
                if fields.held > 0 {
                    Taken::Contended
                } else {
                    Taken::Refused
                }
            } else {
                if op == Op::Admit && fields.unreported == 0 {
                    self.started.store(at, Ordering::Relaxed);
                }
                return Some(Fields {
                    arrival: Bucket::charge(fields.arrival, units, 1),
                    left: fields.left - 1,
                    spent: fields.spent + 1,
                    unreported: fields.unreported + u64::from(op == Op::Admit),
                    held: fields.held + u64::from(op == Op::Hold),
                    ..fields
                });
            };
            None
        });
        let Some(taken) = taken else {
            return (why, false);
        };
        let started = was == 0 && taken.unreported > 0;
        if taken.unreported + taken.held >= self.most_reserved.div_ceil(2)
            && !self.pressed.swap(true, Ordering::AcqRel)
        {
            (Taken::Pressed, started)
        } else {
            (Taken::Done, started)
        }
    }

    /// A request held, decided: admitted, its hold becomes an admission to report, the
    /// place it reserved kept; refused, its charge is given back, and one of the cycle's
    /// allowance if the cycle under way has spent any, as `DataPlane`'s `spent − 1` does —
    /// all read from the one word. Whether requests wait on it.
    fn settle(&self, admitted: bool, now: u64) -> (bool, bool) {
        self.settle_in(admitted, now, now, Cycle::Advance)
    }

    /// As [`Self::settle`], with whether it started the key's count, as [`Self::take_in`],
    /// for a request that arrived at `at`.
    fn settle_in(&self, admitted: bool, at: u64, now: u64, cycle: Cycle) -> (bool, bool) {
        let mut was = 0;
        let settled = self.update(now, cycle, |fields| {
            was = fields.unreported;
            Some(if admitted {
                if fields.unreported == 0 {
                    self.started.store(at, Ordering::Relaxed);
                }
                Fields {
                    held: fields.held.saturating_sub(1),
                    unreported: fields.unreported + 1,
                    ..fields
                }
            } else {
                let refund = u64::from(fields.spent > 0);
                Fields {
                    held: fields.held.saturating_sub(1),
                    arrival: Bucket::release(fields.arrival),
                    left: fields.left + refund,
                    spent: fields.spent - refund,
                    ..fields
                }
            })
        });
        let started = settled.is_some_and(|fields| was == 0 && fields.unreported > 0);
        (self.wanted(), started)
    }

    /// Marks a request's arrival at `now`, a request being its use whether it is admitted or
    /// not: whether it brought the key back into use, a refill time and more after the last.
    fn arrive(&self, now: u64) -> bool {
        let last = self.last_request.swap(now, Ordering::Relaxed);
        now.saturating_sub(last) > self.terms.hold
    }

    /// Takes what was admitted and not yet drained, in one compare-and-swap that leaves the
    /// rest — holds, the bucket, the allowance — as they are, its cycle advanced first if
    /// `cycle` says so: the count, for the transport's ledger, when the earliest of it was
    /// admitted, and whether requests wait on it.
    fn drain(&self, now: u64, cycle: Cycle) -> (u64, u64, bool) {
        let mut count = 0;
        self.update(now, cycle, |fields| {
            count = fields.unreported;
            Some(Fields {
                unreported: 0,
                ..fields
            })
        });
        // After the compare-and-swap, whose acquire sees the time written before the one
        // that started the count.
        let started = self.started.load(Ordering::Relaxed);
        self.pressed.store(false, Ordering::Release);
        (count, started, count > 0 && self.wanted())
    }

    /// How long from `now` it waits to admit a request, if it would refuse one: its
    /// bucket's refill to one, or, with room but the cycle's allowance spent, the cycle.
    #[must_use]
    pub fn wait(&self, now: u64) -> Option<u64> {
        let fields = word::advanced(
            &self.terms.bucket,
            Fields::unpack(self.word.load(Ordering::Acquire)),
            self.advance(),
            now,
        );
        let units = self.terms.bucket.units(now);
        match self.terms.bucket.wait(fields.arrival, units) {
            Some(wait) => Some(wait),
            None if fields.left == 0 => Some(self.terms.cycle),
            None => None,
        }
    }

    fn snapshot(&self) -> Fields {
        Fields::unpack(self.word.load(Ordering::Acquire))
    }
}

/// One more request waits on what `waiters` counts: the count raised, then a fence, before
/// the request looks at the word again — so that a change to the word made after the look
/// sees the count ([`waited_on`]). The word's own accesses are acquire and release, too
/// weak alone for this: each side writes one location and then reads the other's.
fn wait_on(waiters: &AtomicU64) {
    waiters.fetch_add(1, Ordering::SeqCst);
    fence(Ordering::SeqCst);
}

/// Whether a change just made to the word is one requests wait on: a fence, then the count
/// read, pairing with [`wait_on`]'s, so that the waiter sees the change or this sees it.
fn waited_on(waiters: &AtomicU64) -> bool {
    fence(Ordering::SeqCst);
    waiters.load(Ordering::Relaxed) > 0
}

/// A worker deciding a request under `entries` at `now`, all or none: admitted with one
/// compare-and-swap a key when all have room; refused if any has none and no outstanding
/// hold could explain it; for the transport if any is closed; deferred if not to be decided
/// now ([`Local::Defer`]).
pub fn decide(entries: &[&Entry], now: u64) -> Outcome {
    let mut outcome = Outcome {
        local: Local::Admit,
        drain: false,
        wake: false,
        signal: 0,
    };
    if let [entry] = entries {
        let back = entry.arrive(now);
        let (taken, started) = entry.take(Op::Admit, now);
        outcome.drain = taken == Taken::Pressed;
        outcome.local = local(taken);
        outcome.signal = u64::from(back || started);
        return outcome;
    }
    // Every key counts as used, whichever refuses first, as `DataPlane` has it: a key not
    // marked would not be polled, and its copy would go cold.
    for (at, entry) in entries.iter().enumerate() {
        if entry.arrive(now) {
            outcome.signal |= bit(at);
        }
    }
    // Every key held first, then all settled one way: holds are never reported, so a
    // request given back leaves no trace in what the transport sends.
    for (held, entry) in entries.iter().enumerate() {
        let (taken, _) = entry.take(Op::Hold, now);
        outcome.drain |= taken == Taken::Pressed;
        if !taken.done() {
            for entry in &entries[..held] {
                outcome.wake |= entry.settle(false, now).0;
            }
            outcome.local = local(taken);
            return outcome;
        }
    }
    for (at, entry) in entries.iter().enumerate() {
        let (wake, started) = entry.settle(true, now);
        outcome.wake |= wake;
        if started {
            outcome.signal |= bit(at);
        }
    }
    outcome
}

/// The signal bit of the `at`th key: none past the 64th, which a request never has (the
/// proxy's limits are marked by a 64-bit mask).
fn bit(at: usize) -> u64 {
    u32::try_from(at)
        .ok()
        .and_then(|at| 1u64.checked_shl(at))
        .unwrap_or(0)
}

/// What a worker makes of a take that did not hold.
fn local(taken: Taken) -> Local {
    match taken {
        Taken::Done | Taken::Pressed => Local::Admit,
        Taken::Refused => Local::Refuse,
        Taken::Closed => Local::Ask,
        Taken::Full | Taken::Contended => Local::Defer,
    }
}

/// Those of `entries` that would refuse a request at `now`, by position, each with how
/// long it waits.
pub fn waits<'a>(entries: &'a [&'a Entry], now: u64) -> impl Iterator<Item = (usize, u64)> + 'a {
    entries
        .iter()
        .enumerate()
        .filter_map(move |(at, entry)| Some((at, entry.wait(now)?)))
}

/// What the transport keeps of a key besides its entry: the protocol's bookkeeping, which
/// no worker touches.
struct Tracked {
    entry: Arc<Entry>,
    owner: Option<PodId>,
    sent: u64,
    seen: u64,
    /// Batches sent and not yet seen applied.
    unacknowledged: Unacknowledged,
    /// The ledger: admitted and not yet sent, drained from the entry's count, still to go
    /// to the owner.
    unsent: u64,
    /// When the earliest of it was admitted.
    unsent_since: u64,
    heard: Heard,
    /// Scheduled, whether it is to be visited at the next cycle.
    queued: bool,
    /// Whether it is among the keys left out of their owners' pieces, to be visited first
    /// at the next cycle.
    left_out: bool,
}

impl Tracked {
    /// Its ledger's hits, as a report numbered after the last, sent at `now`: `None` if
    /// there are none.
    fn report_unsent(&mut self, now: u64, spare: &mut Spare) -> Option<BatchEntry> {
        let unsent = self.unsent;
        if unsent == 0 {
            return None;
        }
        self.unsent = 0;
        self.sent += 1;
        let unseen = Unseen {
            sequence: self.sent,
            count: unsent,
            asked: 0,
            refused: 0,
            admitted: self.unsent_since,
            first: now,
            last: now,
        };
        self.unacknowledged.push(unseen, spare);
        Some(BatchEntry {
            key: self.entry.key.clone(),
            contract: self.entry.terms.contract,
            report: unseen.report(self.seen, now),
        })
    }

    /// `count` more in the ledger, the earliest of them admitted at `since`.
    fn unsent_more(&mut self, count: u64, since: u64) {
        if count == 0 {
            return;
        }
        self.unsent_since = if self.unsent == 0 {
            since
        } else {
            self.unsent_since.min(since)
        };
        self.unsent += count;
    }

    fn idle(&self, now: u64) -> bool {
        let fields = self.entry.snapshot();
        let last = self.entry.last_request.load(Ordering::Relaxed);
        fields.unreported == 0
            && self.unsent == 0
            && fields.held == 0
            && self.unacknowledged.is_empty()
            && now.saturating_sub(last) > 2 * self.entry.terms.hold
    }
}

/// A list of a key's unacknowledged batches, behind a pointer.
#[allow(
    clippy::box_collection,
    reason = "a pointer in every key's slot, rather than a list's three words, for what is rarely there"
)]
type List = Box<Vec<Unseen>>;

/// A key's batches sent and not yet seen applied, in the order sent. Between cycles there
/// are almost never any, so a key holds no room for them then: a pointer in its slot, the
/// list taken when a batch is sent and given back once every batch is seen applied, rather
/// than a list's room kept for the key's life.
#[derive(Default)]
struct Unacknowledged(Option<List>);

impl Unacknowledged {
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn as_slice(&self) -> &[Unseen] {
        self.0.as_deref().map_or(&[], Vec::as_slice)
    }

    fn as_mut_slice(&mut self) -> &mut [Unseen] {
        self.0.as_deref_mut().map_or(&mut [], Vec::as_mut_slice)
    }

    /// Adds `unseen`, last, a list taken from `spare` if it has to be made.
    fn push(&mut self, unseen: Unseen, spare: &mut Spare) {
        self.0.get_or_insert_with(|| spare.take()).push(unseen);
    }

    /// Lets go of the batches seen applied, those numbered up to `seen`; of the list too,
    /// to `spare`, if that was all of them.
    fn acknowledge(&mut self, seen: u64, spare: &mut Spare) {
        if let Some(list) = &mut self.0 {
            list.retain(|unseen| unseen.sequence > seen);
            if list.is_empty()
                && let Some(list) = self.0.take()
            {
                spare.give(list);
            }
        }
    }
}

/// Lists of unacknowledged batches given back, for keys to take rather than the allocator
/// making and freeing one for nearly every batch a key sends: at most `SPARE_LISTS`, each
/// with room for one batch, which is almost always all a list holds.
#[derive(Default)]
struct Spare(Vec<List>);

/// The most lists [`Spare`] keeps.
const SPARE_LISTS: usize = 1_024;

impl Spare {
    fn take(&mut self) -> List {
        self.0
            .pop()
            .unwrap_or_else(|| Box::new(Vec::with_capacity(1)))
    }

    /// Keeps `list`, empty, unless it has more room than one batch's or enough are kept.
    fn give(&mut self, list: List) {
        if list.capacity() == 1 && self.0.len() < SPARE_LISTS {
            self.0.push(list);
        }
    }
}

/// When a key's copy was last heard of from its owner, if it has been, in eight bytes rather
/// than an option's sixteen, as the service pod keeps a data plane's last admission: one
/// past the time, 0 if never.
#[derive(Clone, Copy, Debug, Default)]
struct Heard(u64);

impl Heard {
    /// Heard of at `at`.
    fn at(at: u64) -> Self {
        Self(at.saturating_add(1))
    }

    /// When, if ever.
    fn get(self) -> Option<u64> {
        self.0.checked_sub(1)
    }
}

/// The answer whose buckets were last taken from a pod.
struct LastTaken {
    /// Its pod's life and number ([`Answers::life`]).
    life: u64,
    number: u64,
    /// The latest exchange started with the pod when the first answer of that life was
    /// taken: any up to it may have reached an earlier life, none after it can.
    fence: u64,
}

/// Whether `a` comes after `b`, both counted up and wrapping: true while they are less than
/// half the range apart, which `u64` counts never come near.
fn later(a: u64, b: u64) -> bool {
    a.wrapping_sub(b).cast_signed() > 0
}

/// What a participant counts a key as holding: its entry and the counts beside it, its
/// bookkeeping, the key twice (the entry's and the bookkeeping's), and a handle to it in its
/// registry and in each of its `workers`, one byte of a table's control with each slot.
/// The first time `entry`'s key may be idle enough to evict ([`Tracked::idle`]), from its
/// last request.
fn idle_at(entry: &Entry) -> u64 {
    entry
        .last_request
        .load(Ordering::Relaxed)
        .saturating_add(2 * entry.terms.hold)
        .saturating_add(1)
}

fn key_bytes(key: &[u8], workers: u64) -> u64 {
    let tracked = size_of::<(Handle, Tracked)>() + 1;
    let handle = (size_of::<Handle>() + 1) as u64;
    entry_bytes(key) + tracked as u64 + handle * (1 + workers)
}

/// What an entry of `key` holds, counts and key, with its eviction time: all that is left of
/// a key no longer kept while that time keeps its entry, scheduled.
#[must_use]
pub fn entry_bytes(key: &[u8]) -> u64 {
    (2 * size_of::<usize>() + size_of::<Entry>() + key.len() + size_of::<Due>()) as u64
}

/// The process's side of the protocol: one participant, its keys shared with its workers.
pub struct Participant<S> {
    id: PlaneId,
    /// The workers that may hold a handle to each entry, which its bytes count.
    workers: u64,
    registry: Arc<Registry<S>>,
    cycle: u64,
    view: View,
    /// By a handle to the key's entry, so that the key's bytes are kept once, in it.
    keys: HashMap<Handle, Tracked, S>,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    /// A hold settled or a count drained on a key requests wait on.
    wake: bool,
    /// What is to go to each pod.
    queued: BTreeMap<PodId, Queued>,
    /// With one exchange in flight to a pod at a time: the pods that have one, since when.
    one_in_flight: bool,
    sending: BTreeMap<PodId, u64>,
    /// The number of the latest exchange started with each pod: with one in flight, the
    /// one whose answer or failure ends it.
    latest: BTreeMap<PodId, u64>,
    exchanges: u64,
    /// The answer whose buckets were last taken from each pod: an answer's are taken only
    /// if it is later.
    taken: BTreeMap<PodId, LastTaken>,
    /// Answers whose buckets were not taken, a later one from the pod taken already.
    late: u64,
    levers: Levers,
    /// The latest time it was given: an exchange's start.
    last_now: u64,
    polls: u64,
    /// How long an exchange may go unanswered before it is taken as failed.
    exchange_timeout: u64,
    capacity: u64,
    /// The bytes of the keys it keeps.
    holding: u64,
    /// Scheduled, the bytes of entries replaced or evicted that their eviction time, in
    /// `evictions`, keeps until it comes or is dropped ([`Self::drop_stale`]).
    stale: u64,
    evicted: u64,
    /// Lists of unacknowledged batches given back, for keys to take.
    spare: Spare,
    /// Whether each cycle visits only the keys that need it, rather than every key.
    scheduled: bool,
    /// Scheduled, the keys to visit at the next cycle, each once ([`Tracked::queued`]).
    runnable: Vec<Handle>,
    /// Empty, the list `runnable` is swapped with each cycle: two kept, with their room,
    /// rather than one made for every cycle, so that neither is grown from nothing or freed
    /// again, a large block's growth and freeing holding up the workers (06 §4).
    next_runnable: Vec<Handle>,
    /// Scheduled, when each key may next be idle enough to evict: one for each key, a later
    /// one put back in its place when one comes due early; and one for each entry replaced
    /// or evicted, until it comes or is dropped ([`Self::stale`]).
    evictions: MinHeap<Due>,
    /// Keys visited, all told.
    visits: u64,
    /// The keys the last cycle left out of their owners' pieces, to be visited first at the
    /// next ([`PIECE_BYTES`](super::PIECE_BYTES)).
    left_out: Vec<Handle>,
    /// Empty, the list `left_out` is swapped with each cycle, as `next_runnable` is.
    spare_left_out: Vec<Handle>,
    /// Keys left out of their owners' pieces, all told: a visit each.
    keys_left_out: u64,
}

/// When an entry's key is next to be looked at for eviction.
struct Due {
    at: u64,
    entry: Arc<Entry>,
}

impl PartialEq for Due {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Due {}

impl PartialOrd for Due {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Due {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.at.cmp(&other.at)
    }
}

/// What visiting a key in a cycle reads and writes besides the key.
struct Visit<'a> {
    now: u64,
    resend_after: u64,
    poll_quiet: bool,
    sending: &'a BTreeMap<PodId, u64>,
    queued: &'a mut BTreeMap<PodId, Queued>,
    numbered: &'a mut u64,
    spare: &'a mut Spare,
    cut_off: bool,
    wake: bool,
    /// Whether the key last visited was left out of its owner's piece, wholly or in part.
    left_out: bool,
    /// Keys left out so, all told.
    keys_left_out: u64,
}

impl Visit<'_> {
    /// The cycle's work on one key, as `DataPlane::on_cycle` does it on each: its count
    /// drained into the ledger, its reports sent or sent again, a poll if it is in use with
    /// nothing to report. Whether it needs visiting at the next cycle too: a ledger it could
    /// not send, its owner's exchange in flight or its piece full; reports not yet
    /// acknowledged; a poll due every cycle. A key its owner's piece
    /// has no room for is treated as one whose owner is busy: what it has not sent stays in
    /// its ledger, unnumbered, and reports due to go again stay due.
    fn key(&mut self, tracked: &mut Tracked) -> bool {
        let now = self.now;
        // What was admitted drained into the ledger, every key's, with an owner to send it
        // to or not, so that no count fills.
        let (count, started, woken) = tracked.entry.drain(now, Cycle::Advance);
        tracked.unsent_more(count, started);
        self.wake |= woken;
        let entry = &tracked.entry;
        let key = &entry.key;
        let hold = entry.terms.hold;
        let Some(owner) = tracked.owner else {
            return false;
        };
        // Its owner has an exchange in flight: what is admitted stays in the ledger, and
        // the exchange's answer refreshes the copy.
        if self.sending.contains_key(&owner) {
            return true;
        }
        // Ages count back from now, as the batch its entries join is made: every batch waiting
        // was made at a time this data plane was given, and time does not go back.
        let mut entries = Vec::new();
        let mut room = self.queued.get(&owner).map_or_else(Room::new, Queued::room);
        let bytes = encoded_entry_bytes(key.len());
        let mut left_out = false;
        for unseen in tracked.unacknowledged.as_mut_slice() {
            unseen.empty_if_stale(hold, now);
        }
        let mut reported = false;
        for unseen in tracked.unacknowledged.as_mut_slice() {
            if now - unseen.last >= self.resend_after {
                if !room.take(bytes) {
                    left_out = true;
                    break;
                }
                unseen.last = now;
                entries.push(BatchEntry {
                    key: key.clone(),
                    contract: entry.terms.contract,
                    report: unseen.report(tracked.seen, now),
                });
                reported = true;
            }
        }
        let pending = tracked.unsent > 0;
        // Not ahead of its own reports left out.
        if pending && (left_out || !room.take(bytes)) {
            left_out = true;
        } else if let Some(report) = tracked.report_unsent(now, self.spare) {
            *self.numbered += 1;
            entries.push(report);
            reported = true;
        }
        let entry = &tracked.entry;
        let key = &entry.key;
        let last = entry.last_request.load(Ordering::Relaxed);
        let in_use = now.saturating_sub(last) <= hold || !tracked.unacknowledged.is_empty();
        let polled = in_use && self.poll_quiet;
        if !reported && polled {
            if room.take(bytes) {
                entries.push(BatchEntry {
                    key: key.clone(),
                    contract: entry.terms.contract,
                    report: Report::Poll,
                });
            } else {
                left_out = true;
            }
        }
        self.cut_off |= in_use
            && tracked
                .heard
                .get()
                .is_none_or(|heard| now - heard >= self.resend_after);
        if !entries.is_empty() {
            let queued = self.queued.entry(owner).or_default().at(now);
            for entry in entries {
                queued.push_entry(entry);
            }
        }
        self.left_out = left_out;
        self.keys_left_out += u64::from(left_out);
        !tracked.unacknowledged.is_empty() || polled || left_out
    }

    /// Visits `tracked`, `handle`'s, and lists it to go first at the next cycle if it was
    /// left out of its owner's piece; or else, `scheduled`, to visit then if it needs it.
    fn listed(
        &mut self,
        handle: Handle,
        tracked: &mut Tracked,
        scheduled: bool,
        left_out: &mut Vec<Handle>,
        next: &mut Vec<Handle>,
    ) {
        let again = self.key(tracked);
        tracked.left_out = self.left_out;
        tracked.queued = scheduled && again;
        if self.left_out {
            left_out.push(handle);
        } else if tracked.queued {
            next.push(handle);
        }
    }
}

/// `handle`'s key, if it is still the one the handle was made for.
#[expect(
    clippy::mutable_key_type,
    reason = "a handle hashes and compares by its entry's key, which never changes"
)]
fn listed<'a, S: BuildHasher>(
    keys: &'a mut HashMap<Handle, Tracked, S>,
    handle: &Handle,
) -> Option<&'a mut Tracked> {
    keys.get_mut(&*handle.0.key)
        .filter(|tracked| Arc::ptr_eq(&tracked.entry, &handle.0))
}

/// Queues `key` to be visited at the next cycle, unless it is already.
#[expect(
    clippy::mutable_key_type,
    reason = "a handle hashes and compares by its entry's key, which never changes"
)]
fn queue<S: BuildHasher>(
    keys: &mut HashMap<Handle, Tracked, S>,
    runnable: &mut Vec<Handle>,
    key: &[u8],
) {
    if let Some(tracked) = keys.get_mut(key)
        && !tracked.queued
    {
        tracked.queued = true;
        runnable.push(Handle::new(Arc::clone(&tracked.entry)));
    }
}

impl<S: BuildHasher + Clone> Participant<S> {
    /// The participant `id` sending every `cycle` nanoseconds, knowing the service as
    /// `view`, its maps hashed by `hasher`; `None` without native 128-bit atomics.
    #[must_use]
    pub fn new(id: PlaneId, cycle: u64, view: View, hasher: S) -> Option<Self> {
        if !supported() {
            return None;
        }
        Some(Self {
            id,
            workers: 1,
            registry: Arc::new(Registry::new(hasher.clone())),
            cycle: cycle.max(1),
            view,
            keys: HashMap::with_hasher(hasher),
            numbered: 0,
            wake: false,
            queued: BTreeMap::new(),
            one_in_flight: false,
            sending: BTreeMap::new(),
            latest: BTreeMap::new(),
            exchanges: 0,
            taken: BTreeMap::new(),
            late: 0,
            levers: Levers::default(),
            last_now: 0,
            polls: 0,
            exchange_timeout: RESEND_AFTER_CYCLES * cycle.max(1),
            capacity: u64::MAX,
            holding: 0,
            stale: 0,
            evicted: 0,
            spare: Spare::default(),
            scheduled: false,
            runnable: Vec::new(),
            next_runnable: Vec::new(),
            evictions: MinHeap::new(),
            visits: 0,
            left_out: Vec::new(),
            spare_left_out: Vec::new(),
            keys_left_out: 0,
        })
    }

    /// Each cycle visits only the keys that need it — those its workers signalled
    /// ([`Self::on_signals`]), those with work outstanding, and those whose eviction is due
    /// — rather than every key, so that quiet keys cost nothing between their uses. A key
    /// not visited has its cycle advanced when it is next taken from, by the rule for a gap:
    /// one share of the bucket refilled from the time gone by. Off unless set: the
    /// comparison with `DataPlane`, which visits every
    /// key every cycle, is exact only then. Set before it holds any key.
    pub fn set_scheduled(&mut self, on: bool) {
        self.scheduled = on;
    }

    /// Keys its workers' decisions signalled ([`Outcome::signal`]): visited at the next
    /// cycle, each claim taken first ([`Entry::claim_signal`]) — the key queued already, or
    /// no longer kept, alike — so that a signal after it is sent anew. A scan leaves claims
    /// as they are: their signals are still on their way.
    pub fn on_signals(&mut self, entries: impl IntoIterator<Item = Arc<Entry>>) {
        for entry in entries {
            entry.unclaim_signal();
            if let Some(tracked) = self.keys.get_mut(&*entry.key)
                && Arc::ptr_eq(&tracked.entry, &entry)
                && !tracked.queued
            {
                tracked.queued = true;
                self.runnable.push(Handle::new(entry));
            }
        }
    }

    /// Keys visited, all told.
    #[must_use]
    pub fn visits(&self) -> u64 {
        self.visits
    }

    /// Keys left out of their owners' pieces, all told: a visit each that found no room
    /// for what the key had to send ([`PIECE_BYTES`](super::PIECE_BYTES)), which waited
    /// for the next.
    #[must_use]
    pub fn keys_left_out(&self) -> u64 {
        self.keys_left_out
    }

    fn run(&mut self, key: &[u8]) {
        queue(&mut self.keys, &mut self.runnable, key);
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> PlaneId {
        self.id
    }

    /// Its registry of live entries, for its workers.
    #[must_use]
    pub fn registry(&self) -> Arc<Registry<S>> {
        Arc::clone(&self.registry)
    }

    /// How long an exchange may go unanswered before it is taken as failed, and the next
    /// batch goes ([`Self::set_one_in_flight`]): four cycles unless said otherwise. Over TCP
    /// a lost segment is sent again only after some 200 ms.
    pub fn set_exchange_timeout(&mut self, timeout: u64) {
        self.exchange_timeout = timeout.max(1);
    }

    /// What it gives up of accuracy for throughput ([`Levers`]): the defaults unless set.
    /// Set before it holds any key.
    pub fn set_levers(&mut self, levers: Levers) {
        self.levers = levers;
    }

    /// One exchange in flight to a service pod at a time: while a pod has one, nothing new
    /// goes to it — what is admitted gathers in each key's ledger, and its keys are neither
    /// polled nor sent again (the exchange in flight refreshes them) — and the next batch
    /// goes once it is answered, or has failed ([`Self::exchanged`]), or has been out for
    /// the exchange timeout ([`Self::set_exchange_timeout`]). Off unless set: `DataPlane` sends every
    /// cycle, and the comparison with it is of the accounting.
    pub fn set_one_in_flight(&mut self, on: bool) {
        self.one_in_flight = on;
    }

    /// Exchange `exchange` with `pod` failed. Over if it is the one in flight; one given up
    /// already leaves the one that followed it in flight.
    pub fn exchanged(&mut self, pod: PodId, exchange: u64) {
        if self.latest.get(&pod) == Some(&exchange) {
            self.sending.remove(&pod);
        }
    }

    /// Answers that came after a later one from their pod, their buckets not taken, all
    /// told.
    #[must_use]
    pub fn late_answers(&self) -> u64 {
        self.late
    }

    /// As `DataPlane::set_capacity`: the bytes its keys may hold, entries included.
    pub fn set_capacity(&mut self, bytes: u64) {
        self.capacity = bytes;
    }

    /// The workers deciding from its entries, each holding a handle to those it meets,
    /// which a key's bytes count: one unless said. Set before it holds any key.
    pub fn set_workers(&mut self, workers: u64) {
        self.workers = workers.max(1);
    }

    /// The bytes its keys hold, with those of entries no longer kept that their eviction
    /// times keep until they come.
    #[must_use]
    pub fn holding(&self) -> u64 {
        self.holding + self.stale
    }

    /// Keys evicted to stay within its capacity, all told.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// How many keys it holds.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// The entries of `keys` it holds, for a worker to decide from itself from now on.
    #[must_use]
    pub fn entries(&self, keys: &[(&[u8], Contract)]) -> Vec<Arc<Entry>> {
        keys.iter()
            .filter_map(|(key, contract)| {
                let tracked = self.keys.get(*key)?;
                (tracked.entry.terms.contract == *contract).then(|| Arc::clone(&tracked.entry))
            })
            .collect()
    }

    /// Makes sure `key` has a live entry under `contract`: a new key, or one whose contract
    /// changed, starts afresh; the old entry, if any, is closed first.
    fn ensure(&mut self, key: &[u8], contract: Contract, now: u64) {
        if self.keys.get(key).is_some_and(|tracked| {
            tracked.entry.terms.contract == contract && !tracked.entry.snapshot().closed
        }) {
            return;
        }
        if let Some(old) = self.keys.remove(key) {
            close(&old.entry);
            self.registry.remove(&old.entry);
            self.forget(key);
        }
        // A worker may have made the key's entry already: that one.
        let made = self
            .registry
            .find_or_make(key, contract, self.cycle, now)
            .map(|(entry, _)| entry);
        let entry = made.unwrap_or_else(|| {
            Arc::new(Entry::with_terms(
                key.into(),
                self.registry.terms(contract, self.cycle),
                now,
            ))
        });
        self.track(entry);
    }

    /// A worker made `entry` in the registry ([`Registry::find_or_make`]): kept and reported
    /// from now on, as one this participant made — unless it is closed, or already kept.
    pub fn adopt(&mut self, entry: Arc<Entry>) {
        if entry.snapshot().closed
            || self
                .keys
                .get(&*entry.key)
                .is_some_and(|tracked| Arc::ptr_eq(&tracked.entry, &entry))
        {
            return;
        }
        if let Some(old) = self.keys.remove(&*entry.key) {
            close(&old.entry);
            self.forget(&entry.key);
        }
        self.track(entry);
    }

    /// Takes `key`, no longer kept, off what it holds — but for its entry while scheduled,
    /// which its eviction time keeps until it comes or is dropped, and which counts as stale
    /// meanwhile.
    fn forget(&mut self, key: &[u8]) {
        self.holding = self.holding.saturating_sub(key_bytes(key, self.workers));
        if self.scheduled {
            self.stale += entry_bytes(key);
        }
    }

    /// Drops the eviction times of entries no longer kept, freeing the entries: a pass over
    /// every key's, made only past the cap, as eviction's own is.
    #[expect(
        clippy::mutable_key_type,
        reason = "a handle hashes and compares by its entry's key, which never changes"
    )]
    fn drop_stale(&mut self) {
        let keys = &self.keys;
        let mut freed = 0;
        self.evictions.retain(|due| {
            let kept = keys
                .get(&*due.entry.key)
                .is_some_and(|tracked| Arc::ptr_eq(&tracked.entry, &due.entry));
            if !kept {
                freed += entry_bytes(&due.entry.key);
            }
            kept
        });
        debug_assert_eq!(freed, self.stale, "one eviction time an entry");
        self.stale = 0;
    }

    /// Keeps `entry`, a live entry of a key not kept, from now on.
    fn track(&mut self, entry: Arc<Entry>) {
        let owner = self.view.owners(&entry.key).0;
        let tracked = Tracked {
            entry,
            owner,
            sent: self.numbered,
            seen: self.numbered,
            unacknowledged: Unacknowledged::default(),
            unsent: 0,
            unsent_since: 0,
            heard: Heard::default(),
            queued: false,
            left_out: false,
        };
        self.registry.insert(Arc::clone(&tracked.entry));
        self.holding += key_bytes(&tracked.entry.key, self.workers);
        if self.scheduled {
            let at = idle_at(&tracked.entry);
            self.evictions.push(Due {
                at,
                entry: Arc::clone(&tracked.entry),
            });
        }
        let key = tracked.entry.key.clone();
        self.keys
            .insert(Handle::new(Arc::clone(&tracked.entry)), tracked);
        self.run(&key);
    }

    /// Decides at `now` a request a worker asked about, under `keys`: as
    /// `DataPlane::decide_all` does, from the shared entries, a key not met before made
    /// with a full bucket.
    pub fn decide_all(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        self.last_now = now;
        for &(key, contract) in keys {
            self.ensure(key, contract, now);
            let Some(tracked) = self.keys.get(key) else {
                return Decision::Refuse;
            };
            tracked.entry.last_request.store(now, Ordering::Relaxed);
        }
        let entries: Vec<Arc<Entry>> = self.entries(keys);
        let entries: Vec<&Entry> = entries.iter().map(|entry| &**entry).collect();
        let outcome = decide(&entries, now);
        self.wake |= outcome.wake;
        for (at, (key, _)) in keys.iter().enumerate() {
            if outcome.signal & bit(at) != 0 {
                self.run(key);
            }
        }
        if outcome.drain {
            for (key, _) in keys {
                self.drain(key, now);
            }
        }
        match outcome.local {
            Local::Admit => Decision::Admit,
            Local::Defer => Decision::Defer,
            // Closed is impossible: every entry was made live above, on this thread.
            Local::Refuse | Local::Ask => Decision::Refuse,
        }
    }

    /// Drains `key`'s count into its ledger now, at a worker's word that it passed half
    /// its most, rather than at the next cycle.
    pub fn drain(&mut self, key: &[u8], now: u64) {
        let Some(tracked) = self.keys.get_mut(key) else {
            return;
        };
        let (count, started, wake) = tracked.entry.drain(now, Cycle::AsItIs);
        tracked.unsent_more(count, started);
        self.wake |= wake;
        self.run(key);
    }

    /// Whether, since last asked, a hold was settled or a count drained on a key deferred
    /// requests wait on: if so, wake them.
    pub fn take_wake(&mut self) -> bool {
        std::mem::take(&mut self.wake)
    }

    /// The cycle's work at `now`, as `DataPlane::on_cycle`: each key's cycle advanced if no
    /// worker has, what was admitted sent, and idle keys closed and evicted.
    pub fn on_cycle(&mut self, now: u64) {
        self.last_now = now;
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        // An exchange out for its whole timeout is taken as failed.
        let exchange_timeout = self.exchange_timeout;
        self.sending
            .retain(|_, since| now.saturating_sub(*since) < exchange_timeout);
        let mut visit = Visit {
            now,
            resend_after,
            poll_quiet: self.levers.poll_quiet,
            sending: &self.sending,
            queued: &mut self.queued,
            numbered: &mut self.numbered,
            spare: &mut self.spare,
            cut_off: false,
            wake: false,
            left_out: false,
            keys_left_out: 0,
        };
        let mut visits = 0;
        let mut next = std::mem::take(&mut self.next_runnable);
        // The keys the last cycle left out of their owners' pieces go first: visited in the
        // order they otherwise are, the same keys would be left out every cycle.
        let mut previous = std::mem::take(&mut self.left_out);
        let mut left_out = std::mem::take(&mut self.spare_left_out);
        let scheduled = self.scheduled;
        // Taken before the visit, so that a signal lost during it asks for another.
        if !self.scheduled || self.registry.take_scan() {
            for handle in previous.drain(..) {
                if let Some(tracked) = listed(&mut self.keys, &handle) {
                    visits += 1;
                    visit.listed(handle, tracked, scheduled, &mut left_out, &mut next);
                    // Marked visited for the walk below, left out again or not; those
                    // that were are marked so again after it.
                    tracked.left_out = true;
                }
            }
            for (handle, tracked) in &mut self.keys {
                if std::mem::take(&mut tracked.left_out) {
                    continue;
                }
                visits += 1;
                visit.listed(handle.clone(), tracked, scheduled, &mut left_out, &mut next);
            }
            for handle in &left_out {
                if let Some(tracked) = listed(&mut self.keys, handle) {
                    tracked.left_out = true;
                }
            }
        } else {
            for handle in previous.drain(..).chain(self.runnable.drain(..)) {
                if let Some(tracked) = listed(&mut self.keys, &handle) {
                    visits += 1;
                    visit.listed(handle, tracked, scheduled, &mut left_out, &mut next);
                }
            }
        }
        self.spare_left_out = previous;
        self.left_out = left_out;
        let (cut_off, wake) = (visit.cut_off, visit.wake);
        self.keys_left_out += visit.keys_left_out;
        self.visits += visits;
        self.next_runnable = std::mem::replace(&mut self.runnable, next);
        self.next_runnable.clear();
        self.wake |= wake;
        if cut_off && !self.view.pods.is_empty() {
            let at = usize::try_from(self.polls).unwrap_or(0) % self.view.pods.len();
            self.polls += 1;
            if !self.sending.contains_key(&self.view.pods[at]) {
                self.queued.entry(self.view.pods[at]).or_default().at(now);
            }
        }
        if self.scheduled {
            self.evict_due(now);
        } else {
            let mut freed = 0;
            let (registry, workers) = (&self.registry, self.workers);
            self.keys.retain(|_, tracked| {
                let evict = tracked.idle(now) && close_if_quiet(tracked);
                if evict {
                    registry.remove(&tracked.entry);
                    freed += key_bytes(&tracked.entry.key, workers);
                }
                !evict
            });
            self.holding = self.holding.saturating_sub(freed);
        }
        if self.holding() <= self.capacity {
            return;
        }
        // Entries no longer kept go first, before any key that is.
        if self.stale > 0 {
            self.drop_stale();
        }
        let target = self.capacity - self.capacity / 10;
        if self.holding > target {
            let mut quiet: Vec<(u64, Box<[u8]>)> = self
                .keys
                .iter()
                .filter(|(_, t)| t.unacknowledged.is_empty() && t.unsent == 0)
                .map(|(_, t)| {
                    let used = t.entry.last_request.load(Ordering::Relaxed);
                    (used, t.entry.key.clone())
                })
                .collect();
            quiet.sort_unstable_by_key(|(used, _)| *used);
            for (_, key) in quiet {
                if self.holding <= target {
                    break;
                }
                let closed = self.keys.get(&*key).is_some_and(close_if_quiet);
                if let Some(tracked) = self.keys.get(&*key).filter(|_| closed) {
                    self.registry.remove(&tracked.entry);
                }
                if closed && self.keys.remove(&*key).is_some() {
                    self.forget(&key);
                    self.evicted += 1;
                }
            }
            // Freed now, not when the keys would have gone idle: what was evicted is gone.
            if self.stale > 0 {
                self.drop_stale();
            }
        }
    }

    /// Scheduled, the keys whose eviction has come due at `now`: evicted if idle, as an
    /// unscheduled cycle evicts every idle key; otherwise looked at again when they next
    /// may be. A due whose key is gone, or has another entry, is dropped, and its entry with
    /// it.
    fn evict_due(&mut self, now: u64) {
        while self.evictions.peek().is_some_and(|due| due.at <= now) {
            let Some(due) = self.evictions.pop() else {
                break;
            };
            let kept = self
                .keys
                .get(&*due.entry.key)
                .filter(|tracked| Arc::ptr_eq(&tracked.entry, &due.entry));
            let Some(tracked) = kept else {
                self.stale = self.stale.saturating_sub(entry_bytes(&due.entry.key));
                continue;
            };
            if tracked.idle(now) && close_if_quiet(tracked) {
                self.registry.remove(&tracked.entry);
                self.holding = self
                    .holding
                    .saturating_sub(key_bytes(&due.entry.key, self.workers));
                self.keys.remove(&*due.entry.key);
            } else {
                let at = idle_at(&due.entry).max(now + self.cycle);
                self.evictions.push(Due {
                    at,
                    entry: due.entry,
                });
            }
        }
    }

    /// A service pod's answers to the latest exchange started with it reach it at `now`,
    /// as `DataPlane::on_answers`: each copy rebased on the owner's bucket and charged with
    /// what the owner has not seen, in one compare-and-swap with the admissions it races.
    pub fn on_answers(&mut self, answers: Answers, now: u64) {
        let exchange = self.latest.get(&answers.from).copied().unwrap_or(0);
        self.on_answers_to(exchange, answers, now);
    }

    /// A service pod's answers to exchange `exchange` ([`Self::take_exchanges`]) reach it
    /// at `now`. Their buckets rebase the copies only if the pod handled their batch after
    /// that of the answers last taken from it, so the copies never go back to an older
    /// bucket: in one life of the pod, if their number is higher; from another life, if
    /// their exchange started after every exchange started by the time the life last taken
    /// was first heard from, since lives of a pod never serve at once. Answers from
    /// the same life not taken still acknowledge reports, which stays true; another life's
    /// do not, so that the reports go to the life serving, which ignores those its copy
    /// holds already.
    pub fn on_answers_to(&mut self, exchange: u64, answers: Answers, now: u64) {
        if self.latest.get(&answers.from) == Some(&exchange) {
            self.sending.remove(&answers.from);
        }
        let last = self.taken.get(&answers.from);
        let same_life = last.is_some_and(|last| last.life == answers.life);
        let current = match last {
            // Unnumbered: taken as it comes.
            _ if answers.life == 0 => true,
            None => true,
            Some(last) if same_life => later(answers.number, last.number),
            Some(last) => later(exchange, last.fence),
        };
        let acknowledges = current || same_life;
        if current {
            if answers.life != 0 {
                let fence = match last {
                    Some(last) if same_life => last.fence,
                    _ => self.latest.get(&answers.from).copied().unwrap_or(exchange),
                };
                self.taken.insert(
                    answers.from,
                    LastTaken {
                        life: answers.life,
                        number: answers.number,
                        fence,
                    },
                );
            }
        } else {
            self.late += 1;
        }
        self.take_answers(answers, current, acknowledges, now);
    }

    /// `answers` taken, their buckets too if `current`, their acknowledgements if
    /// `acknowledges`.
    fn take_answers(&mut self, answers: Answers, current: bool, acknowledges: bool, now: u64) {
        let Answers {
            from,
            entries,
            view,
            made,
            ..
        } = answers;
        self.last_now = now;
        let made = super::taken_as_of(made, self.cycle, now);
        for answer in entries {
            let Some(tracked) = self.keys.get_mut(&*answer.key) else {
                continue;
            };
            // Only the owner it sends to: an answer from an owner that has gone is stale.
            if !answer.owned || tracked.owner != Some(from) {
                continue;
            }
            if acknowledges {
                let seen = answer.applied.min(tracked.sent);
                tracked.seen = tracked.seen.max(seen);
                tracked.unacknowledged.acknowledge(seen, &mut self.spare);
            }
            if !current {
                continue;
            }
            tracked.entry.others.store(answer.others, Ordering::Release);
            let unseen: u64 = tracked
                .unacknowledged
                .as_slice()
                .iter()
                .map(|unseen| unseen.count)
                .sum();
            // The hits the owner has yet to apply.
            let unapplied = unseen + tracked.unsent;
            let entry = &tracked.entry;
            let units = entry.terms.bucket.units(made);
            entry.update(now, Cycle::Advance, |fields| {
                let extra = unapplied + fields.unreported + fields.held;
                Some(Fields {
                    arrival: entry.terms.bucket.rebased(units, answer.behind, extra),
                    ..fields
                })
            });
            tracked.heard = Heard::at(now);
            queue(&mut self.keys, &mut self.runnable, &answer.key);
        }
        if let Some(view) = view {
            self.on_view(view, now);
        }
    }

    /// The service's pods change at `now`, as `DataPlane::on_view`.
    pub fn on_view(&mut self, view: View, now: u64) {
        if view.epoch <= self.view.epoch {
            return;
        }
        self.view = view;
        for (handle, tracked) in &mut self.keys {
            let key = &tracked.entry.key;
            let owner = self.view.owners(key).0;
            if owner == tracked.owner {
                continue;
            }
            tracked.owner = owner;
            if self.scheduled && !tracked.queued {
                tracked.queued = true;
                self.runnable.push(handle.clone());
            }
            let Some(owner) = owner else {
                continue;
            };
            let queued = self.queued.entry(owner).or_default().at(now);
            for unseen in tracked.unacknowledged.as_mut_slice() {
                unseen.last = now;
                let report = unseen.report(tracked.seen, queued.made);
                queued.push_entry(BatchEntry {
                    key: key.clone(),
                    contract: tracked.entry.terms.contract,
                    report,
                });
            }
        }
    }

    /// Those of `keys` that would refuse at `now`, by position, with how long each waits.
    pub fn waits<'a>(
        &'a self,
        keys: &'a [(&[u8], Contract)],
        now: u64,
    ) -> impl Iterator<Item = (usize, u64)> + 'a {
        keys.iter()
            .enumerate()
            .filter_map(move |(at, (key, contract))| {
                let tracked = self
                    .keys
                    .get(*key)
                    .filter(|t| t.entry.terms.contract == *contract)?;
                Some((at, tracked.entry.wait(now)?))
            })
    }

    /// As `DataPlane::retry_after`.
    #[must_use]
    pub fn retry_after(&self, keys: &[(&[u8], Contract)], now: u64) -> Option<(usize, u64)> {
        longest(self.waits(keys, now))
    }

    /// Messages to send, as `DataPlane::take_outgoing`.
    pub fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
        self.take_exchanges()
            .into_iter()
            .map(|(pod, _, batch)| (pod, batch))
            .collect()
    }

    /// Messages to send, each starting an exchange with its pod, numbered: its answers are
    /// taken under that number ([`Self::on_answers_to`]), and its failure said under it
    /// ([`Self::exchanged`]).
    pub fn take_exchanges(&mut self) -> Vec<(PodId, u64, Batch)> {
        let batches = if self.one_in_flight {
            self.take_outgoing_to_idle()
        } else {
            self.take_outgoing_to_all()
        };
        batches
            .into_iter()
            .map(|(pod, batch)| {
                self.exchanges = self.exchanges.wrapping_add(1);
                self.latest.insert(pod, self.exchanges);
                (pod, self.exchanges, batch)
            })
            .collect()
    }

    /// A piece of what is to go to each pod; the rest kept for the next.
    fn take_outgoing_to_all(&mut self) -> Vec<(PodId, Batch)> {
        let id = self.id;
        let batches = self
            .queued
            .iter_mut()
            .map(|(pod, queued)| (*pod, queued.take_piece(id)))
            .collect();
        self.queued.retain(|_, queued| !queued.is_empty());
        batches
    }

    /// With one exchange in flight, a piece of what is to go to each pod with none, each now
    /// with one; the rest kept until theirs is over. Called with `now` given by the last cycle's time
    /// that `on_cycle` saw, as the start of each exchange.
    fn take_outgoing_to_idle(&mut self) -> Vec<(PodId, Batch)> {
        let pods: Vec<PodId> = self
            .queued
            .keys()
            .filter(|pod| !self.sending.contains_key(pod))
            .copied()
            .collect();
        let mut batches = Vec::new();
        for pod in pods {
            let Some(mut queued) = self.queued.remove(&pod) else {
                continue;
            };
            self.sending.insert(pod, self.last_now);
            batches.push((pod, queued.take_piece(self.id)));
            if !queued.is_empty() {
                self.queued.insert(pod, queued);
            }
        }
        batches
    }
}

/// Closes `tracked`'s entry if nothing was admitted since the last report and nothing is
/// held, in one compare-and-swap: a take after it fails and asks, and none before it is
/// lost. Whether it closed.
fn close_if_quiet(tracked: &Tracked) -> bool {
    let entry = &tracked.entry;
    let mut current = entry.word.load(Ordering::Acquire);
    loop {
        let fields = Fields::unpack(current);
        if fields.closed {
            return true;
        }
        if fields.unreported != 0 || fields.held != 0 {
            return false;
        }
        let closed = Fields {
            closed: true,
            ..fields
        };
        match entry.word.compare_exchange_weak(
            current,
            closed.pack(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(seen) => current = seen,
        }
    }
}

/// Closes an entry being replaced, whatever it holds: its contract changed, and the key
/// starts afresh, what the old one had not reported dropped with it, as `DataPlane` drops
/// a key whose contract changed.
fn close(entry: &Entry) {
    let mut current = entry.word.load(Ordering::Acquire);
    loop {
        let closed = Fields {
            closed: true,
            ..Fields::unpack(current)
        };
        match entry.word.compare_exchange_weak(
            current,
            closed.pack(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(seen) => current = seen,
        }
    }
}
