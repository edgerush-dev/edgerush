//! One rate-limit participant per process, its workers deciding warm requests from shared
//! per-key state by compare-and-swap (06 §4).
//!
//! The process is one data plane of the protocol (one [`PlaneId`] for its life). A key's
//! admission state — its bucket, the cycle's allowance and carry, what was admitted for the
//! transport to report, what is held for requests waiting on an ask — is one 128-bit word
//! in an [`Entry`] that every worker holds a handle to. A warm request is decided by the
//! worker with one compare-and-swap on that word, the cycle advanced in the same step if it
//! is due. Nothing on that path waits on another thread.
//!
//! The [`Participant`] is the transport: the process thread's side, single-threaded. It
//! sends what was admitted every cycle, rebases each key's bucket on the owner's answers,
//! asks the owners about cold keys, coalescing requests under the same list of keys, and
//! evicts idle keys. It is `DataPlane`'s protocol with the per-key state moved into the
//! entries; `DataPlane` itself is kept as the reference it is tested against.
//!
//! No I/O: the time is passed in; the transport's messages are the caller's to carry.

mod sync;
mod word;

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod tests;

pub use word::fits;

use super::{
    Answers, Ask, Batch, Contract, Decided, Entry as BatchEntry, PlaneId, PodId,
    RESEND_AFTER_CYCLES, Report, Ticket, Unseen, View, group_name, longest,
};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::{Arc, RwLock};
use sync::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicU128, Ordering, fence};
use word::{Advance, Bucket, Fields, Step};

/// A worker's own decision of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Local {
    /// Every key had room: admitted, and taken from each.
    Admit,
    /// A key has no room, and no hold still outstanding could explain it: refused.
    Refuse,
    /// A key is cold or closed: for the transport to decide.
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
    /// Waiting on a service pod: decided later, by ticket ([`Participant::take_decisions`],
    /// or [`Participant::take_retries`] for one to decide again).
    Wait(Ticket),
    /// As [`Local::Defer`].
    Defer,
}

/// Flags the transport publishes for workers' warm check.
const OWNED: u8 = 1;
const BOUNDED: u8 = 2;
const UNREACHABLE: u8 = 4;
const ASKING: u8 = 8;
/// The participant decides cold keys from their copies ([`Levers::ask_cold`] off).
const LOCAL: u8 = 16;

/// What a participant may give up of accuracy for throughput, each the user's to choose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Levers {
    /// Whether a key with nothing to report is polled every cycle while it is in use, to
    /// keep its copy fresh. Off, a quiet key's copy goes cold after a refill time and its
    /// next request asks: messages no longer grow with the keys in use, only with those
    /// admitting.
    pub poll_quiet: bool,
    /// Whether a request for a key whose copy is cold — new, stale, or being asked about —
    /// waits for its owner's word. Off, it is decided from the copy as it is, a new key's a
    /// full bucket, and the copy put right by the reports' answers after: no request waits
    /// on the service, at the price of a burst on a new key admitted by every process that
    /// meets it, and copies of keys used on several pods only as fresh as their last answer.
    pub ask_cold: bool,
}

impl Default for Levers {
    /// The protocol as `DataPlane` keeps it: both on.
    fn default() -> Self {
        Self {
            poll_quiet: true,
            ask_cold: true,
        }
    }
}

/// Shards of a [`Registry`]: a few for each core, so that the transport writing one key's
/// shard does not hold up a worker reading another's.
const SHARDS: usize = 64;

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
/// or evicts an entry: no lock on the request path. Sharded by the key's FNV-1a; a client
/// choosing keys to crowd a shard slows only those misses.
pub struct Registry {
    shards: Vec<Shard>,
    /// A worker could not hand the transport a key that wanted it: the next cycle visits
    /// every key ([`Self::require_scan`]).
    scan: AtomicBool,
    /// What keys share, by contract: read when an entry is made.
    terms: RwLock<HashMap<Contract, Arc<Terms>>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| RwLock::new(HashSet::new())).collect(),
            scan: AtomicBool::new(false),
            terms: RwLock::new(HashMap::new()),
        }
    }

    fn shard(&self, key: &[u8]) -> &Shard {
        // The shard count divides the hash's range, so the index is always in it.
        &self.shards[(super::fnv1a(key) % SHARDS as u64) as usize]
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
            false,
            now,
        ));
        entry.set_flag(LOCAL, true);
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
    /// The first time the copy is no longer fresh: `heard + hold + 1`; 0 if never heard.
    warm_until: AtomicU64,
    flags: AtomicU8,
    /// Other participants the owner last said admit the key.
    others: AtomicU64,
    last_request: AtomicU64,
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
    /// The protocol's cycle, and what the contract refills in one.
    cycle: u64,
    refill: u64,
}

impl Terms {
    fn new(contract: Contract, cycle: u64) -> Self {
        Self {
            contract,
            bucket: Bucket::new(contract),
            hold: contract.refill_time(),
            cycle,
            refill: contract.refilled(cycle),
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
    fn new(key: Box<[u8]>, contract: Contract, cycle: u64, owned: bool, now: u64) -> Self {
        Self::with_terms(key, Arc::new(Terms::new(contract, cycle)), owned, now)
    }

    fn with_terms(key: Box<[u8]>, terms: Arc<Terms>, owned: bool, now: u64) -> Self {
        let cycle = terms.cycle;
        let fields = Fields {
            arrival: terms.bucket.units(now),
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
            warm_until: AtomicU64::new(0),
            flags: AtomicU8::new(if owned { OWNED } else { 0 }),
            others: AtomicU64::new(0),
            last_request: AtomicU64::new(now),
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

    /// Whether a request for it must be asked about at `now`, as `DataPlane` decides it: an
    /// owner known and reachable, and no copy fresh enough, none bounded yet, or an ask in
    /// flight. Two loads.
    #[must_use]
    pub fn cold(&self, now: u64) -> bool {
        let flags = self.flags.load(Ordering::Acquire);
        flags & LOCAL == 0
            && flags & OWNED != 0
            && flags & UNREACHABLE == 0
            && (flags & ASKING != 0
                || flags & BOUNDED == 0
                || now >= self.warm_until.load(Ordering::Acquire))
    }

    fn advance(&self) -> Advance {
        Advance {
            cycle: self.terms.cycle,
            refill: self.terms.refill,
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
        self.take_in(op, now, Cycle::Advance)
    }

    /// Taking does not count as the key's use: a request's arrival does, which the
    /// transport's own takes, settling what waited, are not ([`decide`] marks it). With
    /// whether it started the key's count: an admission counted where none was, read from
    /// the compare-and-swap that took it, never from an attempt.
    fn take_in(&self, op: Op, now: u64, cycle: Cycle) -> (Taken, bool) {
        let units = self.terms.bucket.units(now);
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
        self.settle_in(admitted, now, Cycle::Advance)
    }

    /// As [`Self::settle`], with whether it started the key's count, as [`Self::take_in`].
    fn settle_in(&self, admitted: bool, now: u64, cycle: Cycle) -> (bool, bool) {
        let mut was = 0;
        let settled = self.update(now, cycle, |fields| {
            was = fields.unreported;
            Some(if admitted {
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
    /// `cycle` says so: the count, for the transport's ledger, and whether requests wait on
    /// it.
    fn drain(&self, now: u64, cycle: Cycle) -> (u64, bool) {
        let mut count = 0;
        self.update(now, cycle, |fields| {
            count = fields.unreported;
            Some(Fields {
                unreported: 0,
                ..fields
            })
        });
        self.pressed.store(false, Ordering::Release);
        (count, count > 0 && self.wanted())
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

    fn set_flag(&self, flag: u8, on: bool) {
        if on {
            self.flags.fetch_or(flag, Ordering::AcqRel);
        } else {
            self.flags.fetch_and(!flag, Ordering::AcqRel);
        }
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
/// compare-and-swap a key when all are warm and have room; refused if any has none and no
/// outstanding hold could explain it; asked about if any is cold or closed; deferred if
/// not to be decided now ([`Local::Defer`]).
pub fn decide(entries: &[&Entry], now: u64) -> Outcome {
    let mut outcome = Outcome {
        local: Local::Admit,
        drain: false,
        wake: false,
        signal: 0,
    };
    if entries.iter().any(|entry| entry.cold(now)) {
        outcome.local = Local::Ask;
        return outcome;
    }
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
    /// The ledger: admitted and not yet sent — drained from the entry's count, or decided by
    /// a service pod for a key it holds a copy of — still to go to the owner.
    unsent: u64,
    /// Of what is admitted, requests an ask decided, its answer heard or not: those of an
    /// ask given up reach the ledger through the entry's count, at the next drain.
    unsent_asked: u64,
    /// Requests of asks given up refused here, not yet sent.
    unsent_refused: u64,
    heard: Heard,
    bounded: bool,
    unreachable: bool,
    asking: u32,
    /// Scheduled, whether it is to be visited at the next cycle.
    queued: bool,
}

impl Tracked {
    /// Publishes what workers read for their warm check.
    fn publish(&self) {
        let entry = &self.entry;
        entry.set_flag(OWNED, self.owner.is_some());
        entry.set_flag(BOUNDED, self.bounded);
        entry.set_flag(UNREACHABLE, self.unreachable);
        entry.set_flag(ASKING, self.asking > 0);
        let until = self.heard.get().map_or(0, |heard| {
            heard.saturating_add(entry.terms.hold).saturating_add(1)
        });
        entry.warm_until.store(until, Ordering::Release);
    }

    fn cold(&self, now: u64) -> bool {
        self.entry.cold(now)
    }

    /// What it would admit as one request now, from the word, the cycle advanced if due.
    fn room(&self, now: u64) -> bool {
        let entry = &self.entry;
        let fields = word::advanced(&entry.terms.bucket, entry.snapshot(), entry.advance(), now);
        fields.left > 0
            && entry
                .terms
                .bucket
                .admits(fields.arrival, entry.terms.bucket.units(now))
    }

    fn idle(&self, now: u64) -> bool {
        let fields = self.entry.snapshot();
        let last = self.entry.last_request.load(Ordering::Relaxed);
        fields.unreported == 0
            && self.unsent == 0
            && self.unsent_refused == 0
            && fields.held == 0
            && self.asking == 0
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

/// An ask in flight.
struct InFlight {
    id: u64,
    pod: PodId,
    sent: u64,
    resent: bool,
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

/// Requests under one list of keys waiting for a service pod's decision, as `DataPlane`
/// groups them.
struct Group {
    keys: Vec<(Box<[u8]>, Contract)>,
    cold: Vec<usize>,
    asking: Option<InFlight>,
    asked: Vec<Ticket>,
    waiting: Vec<Ticket>,
    follow_up: bool,
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
    (2 * size_of::<usize>() + size_of::<Entry>() + key.len() + size_of::<Reverse<Due>>()) as u64
}

/// The process's side of the protocol: one participant, its keys shared with its workers.
pub struct Participant<S> {
    id: PlaneId,
    /// The workers that may hold a handle to each entry, which its bytes count.
    workers: u64,
    registry: Arc<Registry>,
    cycle: u64,
    view: View,
    /// By a handle to the key's entry, so that the key's bytes are kept once, in it.
    keys: HashMap<Handle, Tracked, S>,
    groups: HashMap<Box<[u8]>, Group, S>,
    /// The requests waiting with their group's keys not asked about held.
    holds: BTreeSet<Ticket>,
    in_flight: BTreeMap<u64, Box<[u8]>>,
    next_ticket: Ticket,
    asks_sent: u64,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    decided: Vec<Decided>,
    /// Requests that waited and are to be decided again, whole, by their workers.
    retries: Vec<Ticket>,
    /// A hold settled or a count drained on a key requests wait on.
    wake: bool,
    outgoing: BTreeMap<PodId, Vec<BatchEntry>>,
    asks: BTreeMap<PodId, Vec<Ask>>,
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
    ask_timeout: u64,
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
    /// Scheduled, when each key may next be idle enough to evict: one for each key, a later
    /// one put back in its place when one comes due early; and one for each entry replaced
    /// or evicted, until it comes or is dropped ([`Self::stale`]).
    evictions: BinaryHeap<Reverse<Due>>,
    /// Keys visited, all told.
    visits: u64,
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
    outgoing: &'a mut BTreeMap<PodId, Vec<BatchEntry>>,
    numbered: &'a mut u64,
    spare: &'a mut Spare,
    cut_off: bool,
    wake: bool,
}

impl Visit<'_> {
    /// The cycle's work on one key, as `DataPlane::on_cycle` does it on each: its count
    /// drained into the ledger, its reports sent or sent again, a poll if it is in use with
    /// nothing to report. Whether it needs visiting at the next cycle too: a ledger it could
    /// not send, its owner's exchange in flight; reports not yet acknowledged; a poll due
    /// every cycle; its owner out of reach.
    fn key(&mut self, tracked: &mut Tracked) -> bool {
        let now = self.now;
        let entry = &tracked.entry;
        let key = &entry.key;
        let hold = entry.terms.hold;
        // What was admitted drained into the ledger, every key's, with an owner to send it
        // to or not, so that no count fills.
        let (count, woken) = entry.drain(now, Cycle::Advance);
        tracked.unsent += count;
        self.wake |= woken;
        tracked.bounded |= tracked.heard.get().is_some();
        tracked.publish();
        let Some(owner) = tracked.owner else {
            return false;
        };
        // Its owner has an exchange in flight: what is admitted stays in the ledger, and
        // the exchange's answer refreshes the copy.
        if self.sending.contains_key(&owner) {
            return true;
        }
        let mut entries = Vec::new();
        for unseen in tracked.unacknowledged.as_mut_slice() {
            unseen.empty_if_stale(hold, now);
        }
        let mut reported = false;
        for unseen in tracked.unacknowledged.as_mut_slice() {
            if now - unseen.last >= self.resend_after {
                unseen.last = now;
                entries.push(BatchEntry {
                    key: key.clone(),
                    contract: entry.terms.contract,
                    report: unseen.report(tracked.seen),
                });
                reported = true;
            }
        }
        let unsent = tracked.unsent;
        if unsent > 0 || tracked.unsent_refused > 0 {
            let asked = tracked.unsent_asked.min(unsent);
            tracked.unsent_asked -= asked;
            let refused = std::mem::take(&mut tracked.unsent_refused);
            tracked.unsent = 0;
            tracked.sent += 1;
            *self.numbered += 1;
            let unseen = Unseen {
                sequence: tracked.sent,
                count: unsent,
                asked,
                refused,
                first: now,
                last: now,
            };
            entries.push(BatchEntry {
                key: key.clone(),
                contract: entry.terms.contract,
                report: unseen.report(tracked.seen),
            });
            tracked.unacknowledged.push(unseen, self.spare);
            reported = true;
        }
        let last = entry.last_request.load(Ordering::Relaxed);
        let in_use = now.saturating_sub(last) <= hold || !tracked.unacknowledged.is_empty();
        let polled = in_use && self.poll_quiet;
        if !reported && polled {
            entries.push(BatchEntry {
                key: key.clone(),
                contract: entry.terms.contract,
                report: Report::Poll,
            });
        }
        self.cut_off |= tracked.unreachable
            || (in_use
                && tracked
                    .heard
                    .get()
                    .is_none_or(|heard| now - heard >= self.resend_after));
        if !entries.is_empty() {
            self.outgoing.entry(owner).or_default().extend(entries);
        }
        !tracked.unacknowledged.is_empty() || polled || tracked.unreachable
    }
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
            registry: Arc::new(Registry::new()),
            cycle: cycle.max(1),
            view,
            keys: HashMap::with_hasher(hasher.clone()),
            groups: HashMap::with_hasher(hasher),
            holds: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            next_ticket: 0,
            asks_sent: 0,
            numbered: 0,
            decided: Vec::new(),
            retries: Vec::new(),
            wake: false,
            outgoing: BTreeMap::new(),
            asks: BTreeMap::new(),
            one_in_flight: false,
            sending: BTreeMap::new(),
            latest: BTreeMap::new(),
            exchanges: 0,
            taken: BTreeMap::new(),
            late: 0,
            levers: Levers::default(),
            last_now: 0,
            polls: 0,
            ask_timeout: RESEND_AFTER_CYCLES * cycle.max(1),
            capacity: u64::MAX,
            holding: 0,
            stale: 0,
            evicted: 0,
            spare: Spare::default(),
            scheduled: false,
            runnable: Vec::new(),
            evictions: BinaryHeap::new(),
            visits: 0,
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
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(&self.registry)
    }

    /// As `DataPlane::set_ask_timeout`.
    pub fn set_ask_timeout(&mut self, timeout: u64) {
        self.ask_timeout = timeout.max(1);
    }

    /// What it gives up of accuracy for throughput ([`Levers`]): the defaults unless set.
    /// Set before it holds any key.
    pub fn set_levers(&mut self, levers: Levers) {
        self.levers = levers;
    }

    /// One exchange in flight to a service pod at a time: while a pod has one, nothing new
    /// goes to it — what is admitted gathers in each key's ledger, its keys are neither
    /// polled nor sent again (the exchange in flight refreshes them), and asks wait — and
    /// the next batch goes once it is answered, or has failed ([`Self::exchanged`]), or has
    /// been out for as long as an ask may wait. Off unless set: `DataPlane` sends every
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
        // Decided locally, a worker may have made the key's entry already: that one.
        let made = if self.levers.ask_cold {
            None
        } else {
            self.registry
                .find_or_make(key, contract, self.cycle, now)
                .map(|(entry, _)| entry)
        };
        let entry = made.unwrap_or_else(|| {
            Arc::new(Entry::with_terms(
                key.into(),
                self.registry.terms(contract, self.cycle),
                self.view.owners(key).0.is_some(),
                now,
            ))
        });
        self.track(entry);
    }

    /// A worker made `entry` in the registry ([`Registry::find_or_make`]), cold keys being
    /// decided locally: kept and reported from now on, as one this participant made — unless
    /// it is closed, or already kept.
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
        self.evictions.retain(|Reverse(due)| {
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
        if !self.levers.ask_cold {
            entry.set_flag(LOCAL, true);
        }
        let tracked = Tracked {
            entry,
            owner,
            sent: self.numbered,
            seen: self.numbered,
            unacknowledged: Unacknowledged::default(),
            unsent: 0,
            unsent_asked: 0,
            unsent_refused: 0,
            heard: Heard::default(),
            bounded: false,
            unreachable: false,
            asking: 0,
            queued: false,
        };
        tracked.publish();
        self.registry.insert(Arc::clone(&tracked.entry));
        self.holding += key_bytes(&tracked.entry.key, self.workers);
        if self.scheduled {
            let at = idle_at(&tracked.entry);
            self.evictions.push(Reverse(Due {
                at,
                entry: Arc::clone(&tracked.entry),
            }));
        }
        let key = tracked.entry.key.clone();
        self.keys
            .insert(Handle::new(Arc::clone(&tracked.entry)), tracked);
        self.run(&key);
    }

    /// Decides at `now` a request a worker asked about, under `keys`: as
    /// `DataPlane::decide_all` does, from the shared entries, waiting on an owner for the
    /// cold ones.
    pub fn decide_all(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        self.last_now = now;
        let mut cold = false;
        for &(key, contract) in keys {
            self.ensure(key, contract, now);
            let Some(tracked) = self.keys.get(key) else {
                return Decision::Refuse;
            };
            tracked.entry.last_request.store(now, Ordering::Relaxed);
            cold |= tracked.cold(now);
        }
        if !cold {
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
            return match outcome.local {
                Local::Admit => Decision::Admit,
                Local::Defer => Decision::Defer,
                // Closed is impossible: every entry was made live above, on this thread.
                Local::Refuse | Local::Ask => Decision::Refuse,
            };
        }
        self.wait(keys, now)
    }

    /// Drains `key`'s count into its ledger now, at a worker's word that it passed half
    /// its most, rather than at the next cycle.
    pub fn drain(&mut self, key: &[u8], now: u64) {
        let Some(tracked) = self.keys.get_mut(key) else {
            return;
        };
        let (count, wake) = tracked.entry.drain(now, Cycle::AsItIs);
        tracked.unsent += count;
        self.wake |= wake;
        self.run(key);
    }

    /// Whether, since last asked, a hold was settled or a count drained on a key deferred
    /// requests wait on: if so, wake them.
    pub fn take_wake(&mut self) -> bool {
        std::mem::take(&mut self.wake)
    }

    /// Requests that waited and, at their settling, could not be decided without a refusal
    /// that may be false (as [`Local::Defer`]): their holds given back, to be decided again,
    /// whole, by the workers that asked.
    pub fn take_retries(&mut self) -> Vec<Ticket> {
        std::mem::take(&mut self.retries)
    }

    fn wait(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        let name = group_name(keys.iter().map(|(key, _)| *key));
        if !self.groups.contains_key(&name) {
            let cold = keys
                .iter()
                .enumerate()
                .filter(|(_, (key, _))| self.keys.get(*key).is_some_and(|t| t.cold(now)))
                .map(|(at, _)| at)
                .collect();
            let group = Group {
                keys: keys.iter().map(|(key, c)| ((*key).into(), *c)).collect(),
                cold,
                asking: None,
                asked: Vec::new(),
                waiting: Vec::new(),
                follow_up: false,
            };
            self.groups.insert(name.clone(), group);
        }
        let Some(group) = self.groups.get(&name) else {
            return Decision::Refuse;
        };
        let held: Vec<usize> = (0..group.keys.len())
            .filter(|at| !group.cold.contains(at))
            .collect();
        let room = held.iter().all(|at| {
            self.keys
                .get(&*group.keys[*at].0)
                .is_some_and(|tracked| tracked.room(now))
        });
        // No room under a key that holds are outstanding on: they may be given back.
        let contended = !room
            && held.iter().any(|at| {
                self.keys
                    .get(&*group.keys[*at].0)
                    .is_some_and(|tracked| !tracked.room(now) && tracked.entry.snapshot().held > 0)
            });
        // Hold the keys not asked about, all or none.
        let mut holding = 0;
        let mut failed = if contended {
            Taken::Contended
        } else {
            Taken::Refused
        };
        if room {
            for at in &held {
                let taken = self
                    .keys
                    .get(&*group.keys[*at].0)
                    .map_or(Taken::Refused, |tracked| {
                        tracked.entry.take(Op::Hold, now).0
                    });
                if !taken.done() {
                    failed = taken;
                    break;
                }
                holding += 1;
            }
        }
        if holding < held.len() {
            for at in &held[..holding] {
                if let Some(tracked) = self.keys.get(&*group.keys[*at].0) {
                    self.wake |= tracked.entry.settle(false, now).0;
                }
            }
            let idle = self
                .groups
                .get(&name)
                .is_some_and(|group| group.asking.is_none() && group.waiting.is_empty());
            if idle {
                self.groups.remove(&name);
            }
            return match local(failed) {
                Local::Defer => Decision::Defer,
                _ => Decision::Refuse,
            };
        }
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.holds.insert(ticket);
        let Some(group) = self.groups.get_mut(&name) else {
            return Decision::Refuse;
        };
        if group.asking.is_some() {
            group.waiting.push(ticket);
        } else {
            group.follow_up = false;
            group.asked.push(ticket);
            self.ask(&name, None, now);
        }
        Decision::Wait(ticket)
    }

    fn ask(&mut self, name: &[u8], again: Option<u64>, now: u64) {
        let Some(group) = self.groups.get_mut(name) else {
            return;
        };
        let Some(pod) = group
            .cold
            .first()
            .and_then(|at| self.keys.get(&*group.keys[*at].0))
            .and_then(|tracked| tracked.owner)
        else {
            return;
        };
        let id = match again {
            Some(id) => id,
            None => {
                self.asks_sent += 1;
                for at in &group.cold {
                    if let Some(tracked) = self.keys.get_mut(&*group.keys[*at].0) {
                        tracked.asking += 1;
                        tracked.publish();
                    }
                }
                self.in_flight.insert(self.asks_sent, name.into());
                self.asks_sent
            }
        };
        let resent = group
            .asking
            .as_ref()
            .is_some_and(|flight| flight.id == id && flight.resent);
        let sent = group
            .asking
            .as_ref()
            .filter(|flight| flight.id == id)
            .map_or(now, |flight| flight.sent);
        group.asking = Some(InFlight {
            id,
            pod,
            sent,
            resent,
        });
        self.asks.entry(pod).or_default().push(Ask {
            id,
            count: group.asked.len() as u64,
            timeout: self.ask_timeout,
            keys: group
                .cold
                .iter()
                .map(|at| group.keys[*at].clone())
                .collect(),
        });
    }

    fn ask_over(&mut self, name: &[u8]) {
        let Some(group) = self.groups.get_mut(name) else {
            return;
        };
        let Some(flight) = group.asking.take() else {
            return;
        };
        self.in_flight.remove(&flight.id);
        for at in &group.cold {
            if let Some(tracked) = self.keys.get_mut(&*group.keys[*at].0) {
                tracked.asking = tracked.asking.saturating_sub(1);
                tracked.publish();
            }
        }
    }

    /// Decides here a request of the group `name` that waited: its asked-about keys from
    /// the entries, its held ones settled with them.
    fn settle_here(&mut self, name: &[u8], ticket: Ticket, of_ask: bool, now: u64, cycle: Cycle) {
        let Some(group) = self.groups.get(name) else {
            return;
        };
        let asked: Vec<Arc<Entry>> = group
            .cold
            .iter()
            .filter_map(|at| {
                self.keys
                    .get(&*group.keys[*at].0)
                    .map(|t| Arc::clone(&t.entry))
            })
            .collect();
        let entries: Vec<&Entry> = asked.iter().map(|entry| &**entry).collect();
        // Decided as a worker would, the copies now warm or unreachable.
        let decided = if entries.len() == group.cold.len() {
            let (decided, wake) = decide_here(&entries, now, cycle);
            self.wake |= wake;
            decided
        } else {
            Local::Refuse
        };
        if decided == Local::Defer {
            // Its holds given back; its worker decides it again, as a request of its own:
            // the owner told the ask's is refused, and counts it if it is admitted then.
            if of_ask {
                for at in &group.cold {
                    if let Some(tracked) = self.keys.get_mut(&*group.keys[*at].0) {
                        tracked.unsent_refused += 1;
                    }
                }
            }
            self.settle_held(name, ticket, false, now, cycle);
            self.retries.push(ticket);
            return;
        }
        let admitted = decided == Local::Admit;
        if of_ask {
            for at in &group.cold {
                if let Some(tracked) = self.keys.get_mut(&*group.keys[*at].0) {
                    if admitted {
                        tracked.unsent_asked += 1;
                    } else {
                        tracked.unsent_refused += 1;
                    }
                }
            }
        }
        let refused_by = if admitted {
            Box::default()
        } else {
            group
                .cold
                .iter()
                .zip(&entries)
                .filter_map(|(at, entry)| Some((*at, entry.wait(now)?)))
                .collect()
        };
        self.settle_held(name, ticket, admitted, now, cycle);
        self.decided.push(Decided {
            ticket,
            admitted,
            refused_by,
        });
    }

    fn settle_held(&mut self, name: &[u8], ticket: Ticket, admitted: bool, now: u64, cycle: Cycle) {
        let Some(group) = self.groups.get(name) else {
            return;
        };
        if !self.holds.remove(&ticket) {
            return;
        }
        let held = (0..group.keys.len()).filter(|at| !group.cold.contains(at));
        for at in held {
            if let Some(tracked) = self.keys.get(&*group.keys[at].0) {
                self.wake |= tracked.entry.settle_in(admitted, now, cycle).0;
            }
        }
        if admitted {
            let keys: Vec<Box<[u8]>> = group.keys.iter().map(|(key, _)| key.clone()).collect();
            for key in keys {
                self.run(&key);
            }
        }
    }

    /// The cycle's work at `now`, as `DataPlane::on_cycle`: asks sent again or given up,
    /// each key's cycle advanced if no worker has, what was admitted sent, and idle keys
    /// closed and evicted.
    pub fn on_cycle(&mut self, now: u64) {
        self.last_now = now;
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        let ask_timeout = self.ask_timeout;
        let mut again = Vec::new();
        let mut given_up = Vec::new();
        for (name, group) in &mut self.groups {
            let Some(flight) = &mut group.asking else {
                continue;
            };
            if now - flight.sent >= ask_timeout {
                given_up.push(name.clone());
            } else if now - flight.sent >= ask_timeout / 2 && !flight.resent {
                flight.resent = true;
                again.push((name.clone(), flight.id));
            }
        }
        for (name, id) in again {
            self.ask(&name, Some(id), now);
        }
        for name in given_up {
            self.ask_over(&name);
            let Some(group) = self.groups.get_mut(&name) else {
                continue;
            };
            let asked = std::mem::take(&mut group.asked);
            let waited: Vec<(Ticket, bool)> = asked
                .into_iter()
                .map(|ticket| (ticket, true))
                .chain(
                    std::mem::take(&mut group.waiting)
                        .into_iter()
                        .map(|ticket| (ticket, false)),
                )
                .collect();
            for at in group.cold.clone() {
                if let Some(tracked) = self.keys.get_mut(&*group.keys[at].0) {
                    tracked.unreachable = true;
                    tracked.publish();
                }
                let key = group.keys[at].0.clone();
                queue(&mut self.keys, &mut self.runnable, &key);
            }
            for (ticket, asked) in waited {
                self.settle_here(&name, ticket, asked, now, Cycle::AsItIs);
            }
            self.groups.remove(&name);
        }
        // An exchange out for as long as an ask may wait is taken as failed.
        self.sending
            .retain(|_, since| now.saturating_sub(*since) < ask_timeout);
        let mut visit = Visit {
            now,
            resend_after,
            poll_quiet: self.levers.poll_quiet,
            sending: &self.sending,
            outgoing: &mut self.outgoing,
            numbered: &mut self.numbered,
            spare: &mut self.spare,
            cut_off: false,
            wake: false,
        };
        let mut visits = 0;
        let mut next = Vec::new();
        // Taken before the visit, so that a signal lost during it asks for another.
        if !self.scheduled || self.registry.take_scan() {
            for (handle, tracked) in &mut self.keys {
                tracked.queued = false;
                visits += 1;
                if visit.key(tracked) && self.scheduled {
                    tracked.queued = true;
                    next.push(handle.clone());
                }
            }
        } else {
            for handle in std::mem::take(&mut self.runnable) {
                let Some(tracked) = self.keys.get_mut(&*handle.0.key) else {
                    continue;
                };
                if !Arc::ptr_eq(&tracked.entry, &handle.0) {
                    continue;
                }
                tracked.queued = false;
                visits += 1;
                if visit.key(tracked) {
                    tracked.queued = true;
                    next.push(handle);
                }
            }
        }
        let (cut_off, wake) = (visit.cut_off, visit.wake);
        self.visits += visits;
        self.runnable = next;
        self.wake |= wake;
        if cut_off && !self.view.pods.is_empty() {
            let at = usize::try_from(self.polls).unwrap_or(0) % self.view.pods.len();
            self.polls += 1;
            if !self.sending.contains_key(&self.view.pods[at]) {
                self.outgoing.entry(self.view.pods[at]).or_default();
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
                .filter(|(_, t)| {
                    t.asking == 0
                        && t.unacknowledged.is_empty()
                        && t.unsent == 0
                        && t.unsent_refused == 0
                })
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
        while self.evictions.peek().is_some_and(|due| due.0.at <= now) {
            let Some(Reverse(due)) = self.evictions.pop() else {
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
                self.evictions.push(Reverse(Due {
                    at,
                    entry: due.entry,
                }));
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
    /// holds already. The asks they decided were given up already: an answer is late only
    /// after a later one has come, and an exchange starts no earlier than the asks it
    /// carries.
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
            decided,
            view,
            ..
        } = answers;
        self.last_now = now;
        let mut untaken: Vec<(Box<[u8]>, u128)> = Vec::new();
        // What the asks decided admitted under each of their keys: still to be sent, as
        // hits, and left out of the owner's bucket, which holds them for those hits.
        let mut admitting: Vec<(Box<[u8]>, u64)> = Vec::new();
        for (id, admitted) in &decided {
            let Some(group) = self
                .in_flight
                .get(id)
                .and_then(|name| self.groups.get(name))
            else {
                continue;
            };
            let admitted = (*admitted).min(group.asked.len() as u64);
            for at in &group.cold {
                admitting.push((group.keys[*at].0.clone(), admitted));
            }
        }
        for answer in entries {
            let Some(tracked) = self.keys.get_mut(&*answer.key) else {
                continue;
            };
            if answer.owned && tracked.owner != Some(from) {
                untaken.push((answer.key, answer.behind));
                continue;
            }
            if answer.owned && acknowledges {
                let seen = answer.applied.min(tracked.sent);
                tracked.seen = tracked.seen.max(seen);
                tracked.unacknowledged.acknowledge(seen, &mut self.spare);
            }
            if !current {
                untaken.push((answer.key, answer.behind));
                continue;
            }
            tracked.entry.others.store(answer.others, Ordering::Release);
            let unseen: u64 = tracked
                .unacknowledged
                .as_slice()
                .iter()
                .map(|unseen| unseen.count)
                .sum();
            let unsent = tracked.unsent;
            let asked: u64 = if answer.owned {
                admitting
                    .iter()
                    .filter(|(key, _)| *key == answer.key)
                    .map(|(_, admitted)| admitted)
                    .sum()
            } else {
                0
            };
            let entry = &tracked.entry;
            let units = entry.terms.bucket.units(now);
            entry.update(now, Cycle::Advance, |fields| {
                let extra = unseen + unsent + asked + fields.unreported + fields.held;
                Some(Fields {
                    arrival: entry.terms.bucket.rebased(units, answer.behind, extra),
                    ..fields
                })
            });
            tracked.heard = Heard::at(now);
            tracked.unreachable = false;
            tracked.publish();
            queue(&mut self.keys, &mut self.runnable, &answer.key);
        }
        for (id, admitted) in decided {
            let Some(name) = self.in_flight.get(&id).cloned() else {
                continue;
            };
            self.ask_over(&name);
            let Some(group) = self.groups.get_mut(&name) else {
                continue;
            };
            let asked = std::mem::take(&mut group.asked);
            let refused_by: Box<[(usize, u64)]> = if (asked.len() as u64) > admitted {
                group
                    .cold
                    .iter()
                    .filter_map(|at| {
                        let key = &group.keys[*at].0;
                        let tracked = self.keys.get(&**key)?;
                        let wait = match untaken.iter().find(|(untaken, _)| untaken == key) {
                            Some((_, behind)) => {
                                let bucket = &tracked.entry.terms.bucket;
                                let units = bucket.units(now);
                                bucket.wait(bucket.rebased(units, *behind, 0), units)
                            }
                            None => tracked.entry.wait(now),
                        }?;
                        Some((*at, wait))
                    })
                    .collect()
            } else {
                Box::default()
            };
            // Every request admitted is reported as a hit under each key asked about: its
            // owner, deciding the ask, holds what it admitted for those hits; a pod deciding
            // from its copy leaves them to the owner to count.
            let charged: Vec<Box<[u8]>> = group
                .cold
                .iter()
                .map(|at| group.keys[*at].0.clone())
                .collect();
            for (index, ticket) in asked.into_iter().enumerate() {
                let admitted = (index as u64) < admitted;
                if admitted {
                    for key in &charged {
                        if let Some(tracked) = self.keys.get_mut(&**key) {
                            tracked.unsent += 1;
                            tracked.unsent_asked += 1;
                        }
                        queue(&mut self.keys, &mut self.runnable, key);
                    }
                }
                self.settle_held(&name, ticket, admitted, now, Cycle::Advance);
                self.decided.push(Decided {
                    ticket,
                    admitted,
                    refused_by: if admitted {
                        Box::default()
                    } else {
                        refused_by.clone()
                    },
                });
            }
            let Some(group) = self.groups.get_mut(&name) else {
                continue;
            };
            if group.waiting.is_empty() {
                self.groups.remove(&name);
                continue;
            }
            let bounded = group.cold.iter().all(|at| {
                self.keys
                    .get(&*group.keys[*at].0)
                    .is_some_and(|t| t.bounded)
            });
            if group.follow_up && bounded {
                let waiting = std::mem::take(&mut group.waiting);
                for ticket in waiting {
                    self.settle_here(&name, ticket, false, now, Cycle::Advance);
                }
                self.groups.remove(&name);
            } else {
                group.follow_up = true;
                group.asked = std::mem::take(&mut group.waiting);
                self.ask(&name, None, now);
            }
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
            tracked.unreachable = false;
            tracked.publish();
            if self.scheduled && !tracked.queued {
                tracked.queued = true;
                self.runnable.push(handle.clone());
            }
            let Some(owner) = owner else {
                continue;
            };
            let entries = self.outgoing.entry(owner).or_default();
            for unseen in tracked.unacknowledged.as_mut_slice() {
                unseen.last = now;
                entries.push(BatchEntry {
                    key: key.clone(),
                    contract: tracked.entry.terms.contract,
                    report: unseen.report(tracked.seen),
                });
            }
        }
        let moved: Vec<(Box<[u8]>, u64)> = self
            .groups
            .iter()
            .filter_map(|(name, group)| {
                let flight = group.asking.as_ref()?;
                let first = group.cold.first()?;
                let owner = self.keys.get(&*group.keys[*first].0)?.owner;
                (owner != Some(flight.pod)).then(|| (name.clone(), flight.id))
            })
            .collect();
        for (name, id) in moved {
            self.ask(&name, Some(id), now);
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

    /// As `DataPlane::oldest_ask`.
    fn oldest_ask(&self) -> u64 {
        self.in_flight
            .keys()
            .next()
            .copied()
            .unwrap_or(self.asks_sent + 1)
    }

    fn take_outgoing_to_all(&mut self) -> Vec<(PodId, Batch)> {
        let oldest_ask = self.oldest_ask();
        let mut asks = std::mem::take(&mut self.asks);
        let mut batches: Vec<(PodId, Batch)> = std::mem::take(&mut self.outgoing)
            .into_iter()
            .map(|(pod, entries)| {
                (
                    pod,
                    Batch {
                        from: self.id,
                        entries,
                        asks: asks.remove(&pod).unwrap_or_default(),
                        oldest_ask,
                    },
                )
            })
            .collect();
        batches.extend(asks.into_iter().map(|(pod, asks)| {
            (
                pod,
                Batch {
                    from: self.id,
                    entries: Vec::new(),
                    asks,
                    oldest_ask,
                },
            )
        }));
        batches.sort_by_key(|(pod, _)| *pod);
        batches
    }

    /// With one exchange in flight, what is to go to the pods with none, each now with one;
    /// the rest kept until theirs is over. Called with `now` given by the last cycle's time
    /// that `on_cycle` saw, as the start of each exchange.
    fn take_outgoing_to_idle(&mut self) -> Vec<(PodId, Batch)> {
        let pods: BTreeSet<PodId> = self
            .outgoing
            .keys()
            .chain(self.asks.keys())
            .filter(|pod| !self.sending.contains_key(pod))
            .copied()
            .collect();
        let oldest_ask = self.oldest_ask();
        let mut batches = Vec::new();
        for pod in pods {
            let entries = self.outgoing.remove(&pod).unwrap_or_default();
            let asks = self.asks.remove(&pod).unwrap_or_default();
            self.sending.insert(pod, self.last_now);
            batches.push((
                pod,
                Batch {
                    from: self.id,
                    entries,
                    asks,
                    oldest_ask,
                },
            ));
        }
        batches
    }

    /// Requests that waited, decided since last asked.
    pub fn take_decisions(&mut self) -> Vec<Decided> {
        std::mem::take(&mut self.decided)
    }

    /// The requests waiting, refused: the participant is going away. Their holds are given
    /// back.
    pub fn abandon(&mut self, now: u64) -> Vec<Ticket> {
        let names: Vec<Box<[u8]>> = self.groups.keys().cloned().collect();
        let mut tickets = Vec::new();
        for name in names {
            let Some(group) = self.groups.get(&name) else {
                continue;
            };
            let waited: Vec<Ticket> = group.asked.iter().chain(&group.waiting).copied().collect();
            for ticket in &waited {
                self.settle_held(&name, *ticket, false, now, Cycle::Advance);
            }
            tickets.extend(waited);
        }
        self.groups.clear();
        self.in_flight.clear();
        tickets
    }
}

/// Decided by the transport for a request that waited and is now settled here: as a
/// worker would, the keys' copies taken as they are, cold or not; and whether a settle
/// wants waiters woken.
fn decide_here(entries: &[&Entry], now: u64, cycle: Cycle) -> (Local, bool) {
    if let [entry] = entries {
        return (
            match local(entry.take_in(Op::Admit, now, cycle).0) {
                Local::Ask => Local::Refuse,
                decided => decided,
            },
            false,
        );
    }
    let mut wake = false;
    for (held, entry) in entries.iter().enumerate() {
        let (taken, _) = entry.take_in(Op::Hold, now, cycle);
        if !taken.done() {
            for entry in &entries[..held] {
                wake |= entry.settle_in(false, now, cycle).0;
            }
            return (
                match local(taken) {
                    Local::Defer => Local::Defer,
                    _ => Local::Refuse,
                },
                wake,
            );
        }
    }
    for entry in entries {
        wake |= entry.settle_in(true, now, cycle).0;
    }
    (Local::Admit, wake)
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
