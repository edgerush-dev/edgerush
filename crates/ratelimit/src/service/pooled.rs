//! One rate-limit participant per process, its workers deciding warm requests from shared
//! per-key state by compare-and-swap (06 §4).
//!
//! The process is one data plane of the protocol (one [`PlaneId`] for its life). A key's
//! admission state — its bucket, the cycle's allowance and carry, what was admitted for the
//! transport to report — is one 128-bit word in an [`Entry`] that every worker holds a
//! handle to. A warm request is decided by the worker with one compare-and-swap on that
//! word, the cycle advanced in the same step if it is due; under several keys, each is read
//! and then, if all have room, each charged, as local mode decides. Nothing on that path
//! waits on another thread.
//!
//! The [`Participant`] is the transport: the process thread's side, single-threaded. It
//! sends what was admitted every cycle, rebases each key's bucket on the owner's answers,
//! and evicts idle keys. It asks no one: a key no process has heard of starts from a full
//! bucket. It is `DataPlane`'s protocol with the per-key state moved into the
//! entries; `DataPlane` itself is kept as the reference it is tested against.
//!
//! No I/O: the time is passed in; the transport's messages are the caller's to carry.

pub(super) mod chunked;
mod keys;
mod routing;
mod sync;
mod word;

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod routing_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod window_tests;

pub use routing::{Replied, Reply, Routing};
pub use word::fits;

use super::{
    Answers, Batch, CUT_OFF_CYCLES, Entry as BatchEntry, Limit, MOST_UNACKNOWLEDGED, PlaneId,
    Queued, RESEND_AFTER_CYCLES, Report, Room, Standing, Unseen, View, cut_off_from_owner,
    encoded_entry_bytes, longest,
};
use crate::rate::MASK;
use chunked::MinHeap;
use keys::Keys;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::{Arc, RwLock};
use sync::{AtomicBool, AtomicU32, AtomicU64, AtomicU128, Ordering};
use word::{Advance, Fields, Rate, Step};

/// A worker's own decision of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Local {
    /// Every key had room: admitted, and taken from each.
    Admit,
    /// A key has no room: refused.
    Refuse,
    /// A key is closed, or the worker has none: for the transport to decide.
    Ask,
}

/// A worker's decision and what it asks of others.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// The decision.
    pub local: Local,
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
    /// What keys share, by limit: read when an entry is made.
    terms: RwLock<HashMap<Limit, Arc<Terms>>>,
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

    /// The live entry of `key` under `limit` at `now`, if the participant has made one: under
    /// a fixed window, one of a window not yet over.
    #[must_use]
    pub fn find(&self, key: &[u8], limit: Limit, now: u64) -> Option<Arc<Entry>> {
        let shard = self
            .shard(key)
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shard
            .get(key)
            .map(Handle::entry)
            .filter(|entry| {
                entry.terms.limit == limit && !entry.snapshot().closed && entry.current(now)
            })
            .cloned()
    }

    /// The live entry of `key` under `limit`, or, if there is none, one made at `now` — its
    /// bucket full, or its window `now`'s with nothing counted; its cycle `cycle` — and kept:
    /// with whether it was made here, for whoever made it to have the participant adopt it
    /// ([`Participant::adopt`]). `None` if a live entry of the key is under another limit:
    /// the participant replaces it. Under a fixed window, an entry whose window is over is
    /// closed and the next made, the others admitting carried over to it, for the participant
    /// to take the key's record on to (06 §4). Taken by a worker meeting a key its process
    /// has not, once, or meeting a window's end; under the shard's lock, so that two workers
    /// meeting it at once share one entry.
    #[must_use]
    pub fn find_or_make(
        &self,
        key: &[u8],
        limit: Limit,
        cycle: u64,
        now: u64,
    ) -> Option<(Arc<Entry>, bool)> {
        let mut shard = self
            .shard(key)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut others = 0;
        if let Some(held) = shard.get(key).map(Handle::entry)
            && !held.snapshot().closed
        {
            if held.terms.limit != limit {
                return None;
            }
            if held.current(now) {
                return Some((Arc::clone(held), false));
            }
            // Its window over: the next, not a cold key.
            close(held);
            others = held.others.load(Ordering::Acquire);
        }
        let entry = Arc::new(self.made(key, self.terms(limit, cycle), now));
        entry.others.store(others, Ordering::Release);
        shard.replace(Handle(Arc::clone(&entry)));
        Some((entry, true))
    }

    /// An entry of `key` made at `now` under `terms`: under a fixed window, of `now`'s
    /// window, each key's windows shifted by an offset from its hash so that their ends
    /// spread, alike on every data plane sharing the key's hash.
    fn made(&self, key: &[u8], terms: Arc<Terms>, now: u64) -> Entry {
        let window = match terms.model {
            Model::Bucket { .. } => None,
            Model::Window { length, .. } => {
                Some(word::window_at(keys::hash(&self.hasher, key), length, now))
            }
        };
        Entry::with_terms(key.into(), terms, now, window)
    }

    /// What keys under `limit` share, at `cycle`: made the first time it is asked for.
    fn terms(&self, limit: Limit, cycle: u64) -> Arc<Terms> {
        let terms = self
            .terms
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&limit)
            .cloned();
        terms.unwrap_or_else(|| {
            let mut terms = self
                .terms
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(
                terms
                    .entry(limit)
                    .or_insert_with(|| Arc::new(Terms::new(limit, cycle))),
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

/// One key's shared state, for one incarnation of it: a key evicted and met again is a
/// new entry, made only once the old one is closed; under a fixed window, one window of it.
pub struct Entry {
    key: Box<[u8]>,
    /// What it shares with every key under its limit.
    terms: Arc<Terms>,
    /// Under a fixed window, its window's number, counted from 1; 0 under a bucket.
    window: u64,
    /// When its window ends, from when it decides nothing more and the next is made;
    /// never, under a bucket.
    ends: u64,
    /// In its low half the bucket's arrival time, or the window's count as the copy holds
    /// it ([`Fields::arrival`]).
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
    /// A signal of it is on its way to the transport: held in a worker's batch, or queued
    /// ([`Self::claim_signal`]).
    signalled: AtomicBool,
}

/// What every key under one limit shares, made once a limit and pointed to by each key's
/// entry rather than copied into it: the limit, its model's constants, its refill time, and
/// the cycle.
#[derive(Debug)]
struct Terms {
    limit: Limit,
    model: Model,
    /// The limit's refill time, a window's length: how long a copy stays fresh after an
    /// answer.
    hold: u64,
    /// The protocol's cycle.
    cycle: u64,
}

/// A limit's arithmetic, by its model.
#[derive(Debug)]
enum Model {
    /// A bucket's constants, and what it refills in a cycle, in requests and in units.
    Bucket {
        bucket: Rate,
        refill: u64,
        refill_units: u64,
    },
    /// A fixed window's limit and length.
    Window { limit: u64, length: u64 },
}

#[cfg(all(test, not(loom)))]
impl Entry {
    /// What an advance of its cycle reads besides the word, by its model, for tests.
    fn advance_now(&self) -> Advance {
        match self.arith() {
            Ariths::Bucket(arith) => self.advance(arith),
            Ariths::Window(arith) => self.advance(arith),
        }
    }
}

#[cfg(all(test, not(loom)))]
#[allow(
    clippy::panic,
    reason = "test set-up: a window's terms asked for a bucket's fails the test"
)]
impl Terms {
    /// A bucket's constants, for tests of buckets.
    fn bucket(&self) -> &Rate {
        match &self.model {
            Model::Bucket { bucket, .. } => bucket,
            Model::Window { .. } => panic!("a fixed window has no bucket"),
        }
    }

    /// What a bucket refills in a cycle, in units, for tests of buckets.
    fn refill_units(&self) -> u64 {
        match &self.model {
            Model::Bucket { refill_units, .. } => *refill_units,
            Model::Window { .. } => panic!("a fixed window refills nothing"),
        }
    }
}

impl Terms {
    fn new(limit: Limit, cycle: u64) -> Self {
        let model = match limit {
            Limit::Bucket(contract) => Model::Bucket {
                bucket: Rate::new(contract),
                refill: contract.refilled(cycle),
                refill_units: Rate::new(contract).units(cycle),
            },
            Limit::FixedWindow(window) => Model::Window {
                limit: window.limit.get(),
                length: window.length.get(),
            },
        };
        Self {
            limit,
            model,
            hold: limit.refill_time(),
            cycle,
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

/// A model's arithmetic on an entry's word: the cycle's refill, its advance, whether the low
/// half has room for a request, and the low half with one charged.
trait Arith: Copy {
    /// What a cycle refills, in requests and in units.
    fn refill(self) -> (u64, u64);
    /// `fields` advanced to the cycle of `now` if that is due.
    fn advanced(self, fields: Fields, advance: Advance, now: u64) -> Fields;
    /// Whether `arrival`, the low half, admits a request at `now`.
    fn has_room(self, arrival: u64, now: u64) -> bool;
    /// `arrival` with one request charged at `now`.
    fn charged(self, arrival: u64, now: u64) -> u64;
}

/// An entry's model's arithmetic, one or the other.
#[derive(Clone, Copy)]
enum Ariths<'a> {
    Bucket(BucketArith<'a>),
    Window(WindowArith),
}

/// A bucket's arithmetic: its arrival time in the low half.
#[derive(Clone, Copy)]
struct BucketArith<'a> {
    rate: &'a Rate,
    refill: u64,
    refill_units: u64,
}

impl Arith for BucketArith<'_> {
    #[inline(always)]
    fn refill(self) -> (u64, u64) {
        (self.refill, self.refill_units)
    }

    #[inline(always)]
    fn advanced(self, fields: Fields, advance: Advance, now: u64) -> Fields {
        word::advanced(self.rate, fields, advance, now)
    }

    #[inline(always)]
    fn has_room(self, arrival: u64, now: u64) -> bool {
        self.rate.admits(arrival, self.rate.units(now))
    }

    #[inline(always)]
    fn charged(self, arrival: u64, now: u64) -> u64 {
        Rate::charge(arrival, self.rate.units(now), 1)
    }
}

/// A fixed window's arithmetic: the copy's count in the low half; no refill within a window.
#[derive(Clone, Copy)]
struct WindowArith {
    limit: u64,
}

impl Arith for WindowArith {
    #[inline(always)]
    fn refill(self) -> (u64, u64) {
        (0, 0)
    }

    #[inline(always)]
    fn advanced(self, fields: Fields, advance: Advance, now: u64) -> Fields {
        word::window_advanced(self.limit, fields, advance, now)
    }

    #[inline(always)]
    fn has_room(self, arrival: u64, _: u64) -> bool {
        arrival < self.limit
    }

    #[inline(always)]
    fn charged(self, arrival: u64, _: u64) -> u64 {
        (arrival + 1).min(MASK)
    }
}

/// What a key says of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Taken {
    /// Room: taken, or there to take.
    Done,
    /// No room.
    Refused,
    /// The entry is closed: ask for the key's new one.
    Closed,
}

impl Entry {
    /// An entry with terms of its own, for tests of one key.
    #[cfg(test)]
    fn new(key: Box<[u8]>, contract: super::Contract, cycle: u64, now: u64) -> Self {
        Self::with_terms(
            key,
            Arc::new(Terms::new(Limit::Bucket(contract), cycle)),
            now,
            None,
        )
    }

    /// An entry made at `now` under `terms`: under a fixed window, of `window`, its number and
    /// when it ends.
    fn with_terms(key: Box<[u8]>, terms: Arc<Terms>, now: u64, window: Option<(u64, u64)>) -> Self {
        let cycle = terms.cycle;
        let arrival = match terms.model {
            Model::Bucket { bucket, .. } => Rate::full(bucket.units(now)),
            Model::Window { .. } => 0,
        };
        let (window, ends) = window.unwrap_or((0, u64::MAX));
        let fields = Fields {
            arrival,
            // No share bound until a cycle sets one, as a new `DataPlane` key has none.
            left: word::MOST_LEFT,
            cycle: word::cycle_of(now, cycle),
            shared: false,
            closed: false,
            unreported: 0,
            set: false,
        };
        Self {
            key,
            terms,
            window,
            ends,
            word: AtomicU128::new(fields.pack()),
            record: AtomicU32::new(word::record_of(now, cycle)),
            others: AtomicU64::new(0),
            last_request: AtomicU64::new(now),
            started: AtomicU64::new(now),
            retries: AtomicU64::new(0),
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

    /// Its key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Its limit.
    #[must_use]
    pub fn limit(&self) -> Limit {
        self.terms.limit
    }

    /// Under a fixed window, its window's number; 0 under a bucket.
    #[must_use]
    pub const fn window(&self) -> u64 {
        self.window
    }

    /// Whether it decides at `now`: always under a bucket; under a fixed window until its
    /// window ends, the next window another entry.
    #[must_use]
    #[inline(always)]
    pub const fn current(&self, now: u64) -> bool {
        now < self.ends
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

    /// Its model's arithmetic: matched on once a decision, so that each model's path is
    /// compiled apart and a bucket's runs as it would with no window beside it.
    #[inline(always)]
    fn arith(&self) -> Ariths<'_> {
        match &self.terms.model {
            Model::Bucket {
                bucket,
                refill,
                refill_units,
            } => Ariths::Bucket(BucketArith {
                rate: bucket,
                refill: *refill,
                refill_units: *refill_units,
            }),
            Model::Window { limit, .. } => Ariths::Window(WindowArith { limit: *limit }),
        }
    }

    #[inline(always)]
    fn advance<A: Arith>(&self, arith: A) -> Advance {
        let (refill, refill_units) = arith.refill();
        Advance {
            cycle: self.terms.cycle,
            refill,
            refill_units,
            others: self.others.load(Ordering::Acquire),
            record: self.record.load(Ordering::Acquire),
        }
    }

    /// Applies `change` to the word until it holds, the cycle advanced first if it is due:
    /// `change` says the new fields, or none to leave the word as it is.
    fn update(&self, now: u64, change: impl FnMut(Fields) -> Option<Fields>) -> Option<Fields> {
        match self.arith() {
            Ariths::Bucket(arith) => self.update_with(arith, now, change),
            Ariths::Window(arith) => self.update_with(arith, now, change),
        }
    }

    /// [`Self::update`] by `arith`.
    #[inline(always)]
    fn update_with<A: Arith>(
        &self,
        arith: A,
        now: u64,
        mut change: impl FnMut(Fields) -> Option<Fields>,
    ) -> Option<Fields> {
        // The record read before the word, so that it is no later than the word's cycle.
        let advance = self.advance(arith);
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            let before = Fields::unpack(current);
            // Whether this advances the cycle: by the step, not by the 16-bit number, which
            // a gap of a multiple of its range leaves as it was.
            let moved = word::step(before.cycle, advance.record, now, advance.cycle) != Step::Same;
            let fields = if moved {
                arith.advanced(before, advance, now)
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

    /// What it says of a request at `now`, from its word as it is, its cycle advanced if
    /// that is due, nothing written: room, none, or closed — its window over among them.
    #[inline(always)]
    fn look(&self, now: u64) -> Taken {
        match self.arith() {
            Ariths::Bucket(arith) => self.look_with(arith, now),
            Ariths::Window(arith) if self.current(now) => self.look_with(arith, now),
            Ariths::Window(_) => Taken::Closed,
        }
    }

    #[inline(always)]
    fn look_with<A: Arith>(&self, arith: A, now: u64) -> Taken {
        let fields = arith.advanced(self.snapshot(), self.advance(arith), now);
        if fields.closed {
            Taken::Closed
        } else if fields.left > 0 && arith.has_room(fields.arrival, now) {
            Taken::Done
        } else {
            Taken::Refused
        }
    }

    /// Takes one request's worth at `now` if it has room, in one compare-and-swap. Taking
    /// does not count as the key's use: a request's arrival does ([`decide`] marks it).
    /// With whether it started the key's count: an admission counted where none was, read
    /// from the compare-and-swap that took it, never from an attempt. A window's entry takes
    /// nothing once its window is over.
    #[inline(always)]
    fn take(&self, now: u64) -> (Taken, bool) {
        match self.arith() {
            Ariths::Bucket(arith) => self.take_with(arith, now),
            Ariths::Window(arith) if self.current(now) => self.take_with(arith, now),
            Ariths::Window(_) => (Taken::Closed, false),
        }
    }

    #[inline(always)]
    fn take_with<A: Arith>(&self, arith: A, now: u64) -> (Taken, bool) {
        let mut why = Taken::Refused;
        let mut was = 0;
        let taken = self.update_with(arith, now, |fields| {
            was = fields.unreported;
            why = if fields.closed {
                Taken::Closed
            } else if fields.left > 0 && arith.has_room(fields.arrival, now) {
                return Some(self.charged(arith, fields, now));
            } else {
                Taken::Refused
            };
            None
        });
        match taken {
            Some(taken) => (Taken::Done, was == 0 && taken.unreported > 0),
            None => (why, false),
        }
    }

    /// Charges one request at `now`, room or none, once a read of every key of the request
    /// found room ([`decide`]): a charge that finds the room taken since admits one more
    /// than the key has, never refuses one within it. A closed entry is left as it is, the
    /// hit never reported: it was idle for two refill times before it closed, or its window
    /// ended as the request was decided. Whether it started the key's count, as
    /// [`Self::take`].
    #[inline(always)]
    fn charge(&self, now: u64) -> bool {
        match self.arith() {
            Ariths::Bucket(arith) => self.charge_with(arith, now),
            Ariths::Window(arith) => self.current(now) && self.charge_with(arith, now),
        }
    }

    #[inline(always)]
    fn charge_with<A: Arith>(&self, arith: A, now: u64) -> bool {
        let mut was = 0;
        let charged = self.update_with(arith, now, |fields| {
            was = fields.unreported;
            (!fields.closed).then(|| self.charged(arith, fields, now))
        });
        charged.is_some_and(|fields| was == 0 && fields.unreported > 0)
    }

    /// `fields` with one request charged at `now`: the bucket or the window's count, the
    /// cycle's allowance and the count to report, the count's start noted if it had none. The
    /// allowance and the count each stop at their ends rather than wrap: a charge past the
    /// allowance only a race gives, and the count is one the transport never lets fill.
    #[inline(always)]
    fn charged<A: Arith>(&self, arith: A, fields: Fields, now: u64) -> Fields {
        if fields.unreported == 0 {
            self.started.store(now, Ordering::Relaxed);
        }
        Fields {
            arrival: arith.charged(fields.arrival, now),
            left: fields.left.saturating_sub(1),
            unreported: (fields.unreported + 1).min(word::MOST_UNREPORTED),
            ..fields
        }
    }

    /// Marks a request's arrival at `now`, a request being its use whether it is admitted or
    /// not: whether it brought the key back into use, a refill time and more after the last.
    fn arrive(&self, now: u64) -> bool {
        let last = self.last_request.swap(now, Ordering::Relaxed);
        now.saturating_sub(last) > self.terms.hold
    }

    /// Takes what was admitted and not yet drained, in one compare-and-swap that leaves the
    /// rest — the bucket, the allowance — as they are, its cycle advanced first if due: the
    /// count, for the transport's ledger, and when the earliest of it was admitted.
    fn drain(&self, now: u64) -> (u64, u64) {
        let mut count = 0;
        self.update(now, |fields| {
            count = fields.unreported;
            Some(Fields {
                unreported: 0,
                ..fields
            })
        });
        // After the compare-and-swap, whose acquire sees the time written before the one
        // that started the count.
        let started = self.started.load(Ordering::Relaxed);
        (count, started)
    }

    /// How long from `now` it waits to admit a request, if it would refuse one: its
    /// bucket's refill to one, or its window's end with the window counted full; or, with
    /// room but the cycle's allowance spent, the cycle. None once its window is over: the
    /// next window's entry decides.
    #[must_use]
    pub fn wait(&self, now: u64) -> Option<u64> {
        let full = match self.arith() {
            Ariths::Bucket(arith) => {
                let fields = arith.advanced(self.snapshot(), self.advance(arith), now);
                let units = arith.rate.units(now);
                (arith.rate.wait(fields.arrival, units), fields.left)
            }
            Ariths::Window(_) if !self.current(now) => return None,
            Ariths::Window(arith) => {
                let fields = arith.advanced(self.snapshot(), self.advance(arith), now);
                let full = fields.arrival >= arith.limit;
                (full.then(|| self.ends.saturating_sub(now)), fields.left)
            }
        };
        match full {
            (Some(wait), _) => Some(wait),
            (None, 0) => Some(self.terms.cycle),
            (None, _) => None,
        }
    }

    fn snapshot(&self) -> Fields {
        Fields::unpack(self.word.load(Ordering::Acquire))
    }
}

/// A worker deciding a request under `entries` at `now`, all or none, as local mode does:
/// one key taken in one compare-and-swap if it has room; several read, then, if every one
/// has room, each charged. Requests racing for a key's last room under several keys may
/// both be admitted, one more than it has; none is refused within its limits. Refused if a
/// key has no room; for the transport if one is closed.
pub fn decide(entries: &[&Entry], now: u64) -> Outcome {
    let mut outcome = Outcome {
        local: Local::Admit,
        signal: 0,
    };
    if let [entry] = entries {
        let back = entry.arrive(now);
        let (taken, started) = entry.take(now);
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
    // Every key read first, nothing written; then, every one with room, each charged.
    for entry in entries {
        let taken = entry.look(now);
        if taken != Taken::Done {
            outcome.local = local(taken);
            return outcome;
        }
    }
    for (at, entry) in entries.iter().enumerate() {
        if entry.charge(now) {
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

/// What a worker makes of what a key says.
fn local(taken: Taken) -> Local {
    match taken {
        Taken::Done => Local::Admit,
        Taken::Refused => Local::Refuse,
        Taken::Closed => Local::Ask,
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
struct Tracked<P> {
    entry: Arc<Entry>,
    owner: Option<P>,
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

/// What an owner's answer rebases a key's copy on.
enum Rebase<'a> {
    /// The bucket, `behind` from full.
    Bucket(&'a Rate, u128),
    /// The window's count.
    Count(u64),
    /// Nothing: another window's answer.
    Nothing,
}

impl<P> Tracked<P> {
    /// Its ledger's hits, as a report numbered after the last, sent at `now`: `None` if
    /// there are none, or [`MOST_UNACKNOWLEDGED`] are out already and they wait.
    fn report_unsent(&mut self, now: u64, spare: &mut Spare) -> Option<BatchEntry> {
        let unsent = self.unsent;
        if unsent == 0 || self.unacknowledged.as_slice().len() >= MOST_UNACKNOWLEDGED {
            return None;
        }
        self.unsent = 0;
        self.sent += 1;
        let unseen = Unseen {
            sequence: self.sent,
            count: unsent,
            admitted: self.unsent_since,
            first: now,
            last: now,
            window: self.entry.window,
        };
        self.unacknowledged.push(unseen, spare);
        Some(BatchEntry {
            key: self.entry.key.clone(),
            limit: self.entry.terms.limit,
            report: unseen.report(self.seen, now),
        })
    }

    /// Hits the owner has yet to apply in the entry's window, or under a bucket: reported and
    /// not seen applied, and not yet reported.
    fn unapplied(&self) -> u64 {
        let window = self.entry.window;
        let unseen: u64 = self
            .unacknowledged
            .as_slice()
            .iter()
            .filter(|unseen| unseen.window == window)
            .map(|unseen| unseen.count)
            .sum();
        unseen + self.unsent
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

fn key_bytes<P>(key: &[u8], workers: u64) -> u64 {
    let tracked = size_of::<Tracked<P>>() + 1;
    let handle = (size_of::<Handle>() + 1) as u64;
    entry_bytes(key) + tracked as u64 + handle * (1 + workers)
}

/// What an entry of `key` holds, counts and key, with its eviction time: all that is left of
/// a key no longer kept while that time keeps its entry, scheduled.
#[must_use]
pub fn entry_bytes(key: &[u8]) -> u64 {
    (2 * size_of::<usize>() + size_of::<Entry>() + key.len() + size_of::<Due>()) as u64
}

/// The process's side of the protocol: one participant, its keys shared with its workers,
/// reaching the service's pods by `R`: today's by the view unless said.
pub struct Participant<S, R: Routing = View> {
    id: PlaneId,
    /// The workers that may hold a handle to each entry, which its bytes count.
    workers: u64,
    registry: Arc<Registry<S>>,
    cycle: u64,
    routing: R,
    /// What its keys are hashed by, as `keys` hashes them: for a visit to hash one while
    /// they are walked.
    hasher: S,
    /// By each key's hash, so that the key's bytes are kept once, in its entry.
    keys: Keys<S, R::Pod>,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    /// What is to go to each pod.
    queued: BTreeMap<R::Pod, Queued>,
    /// With one exchange in flight to a pod at a time: the pods that have one, since when.
    one_in_flight: bool,
    sending: BTreeMap<R::Pod, u64>,
    /// The number of the latest exchange started with each pod: with one in flight, the
    /// one whose answer or failure ends it.
    latest: BTreeMap<R::Pod, u64>,
    exchanges: u64,
    /// The answer whose buckets were last taken from each pod: an answer's are taken only
    /// if it is later.
    taken: BTreeMap<R::Pod, LastTaken>,
    /// Since when each pod has left a batch unanswered, sent with entries: cleared by its
    /// next reply of its own, a relayed one not. Past four cycles, its keys go through
    /// another pod as well ([`Routing::through`]).
    unanswered: BTreeMap<R::Pod, u64>,
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
    /// Hits given up without being seen applied, all told ([`Self::forgiven`]).
    forgiven: u64,
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
struct Visit<'a, S, R: Routing> {
    now: u64,
    resend_after: u64,
    /// How long a key's owner may go unanswered before it is cut off from it
    /// ([`cut_off_from_owner`]).
    cut_off_after: u64,
    poll_quiet: bool,
    hasher: &'a S,
    routing: &'a R,
    unanswered: &'a BTreeMap<R::Pod, u64>,
    sending: &'a BTreeMap<R::Pod, u64>,
    queued: &'a mut BTreeMap<R::Pod, Queued>,
    numbered: &'a mut u64,
    /// Hits given up without being seen applied, all told ([`Participant::forgiven`]).
    forgiven: &'a mut u64,
    spare: &'a mut Spare,
    cut_off: bool,
    /// Whether the key last visited was left out of its owner's piece, wholly or in part.
    left_out: bool,
    /// Keys left out so, all told.
    keys_left_out: u64,
}

impl<S: BuildHasher, R: Routing> Visit<'_, S, R> {
    /// The pod to send `tracked`'s key through as well as to `owner` at `now`: the
    /// routing's, once `owner` has left a batch unanswered for as long as a report waits to
    /// be sent again. Sent both ways, a report is applied once, by its number.
    fn through(&self, tracked: &Tracked<R::Pod>, owner: R::Pod) -> Option<R::Pod> {
        let since = self.unanswered.get(&owner)?;
        if self.now.saturating_sub(*since) < self.resend_after {
            return None;
        }
        let hash = keys::hash(self.hasher, &tracked.entry.key);
        self.routing
            .through(hash, owner)
            .filter(|through| *through != owner)
    }

    /// The cycle's work on one key, as `DataPlane::on_cycle` does it on each: its count
    /// drained into the ledger, its reports sent or sent again, a poll if it is in use with
    /// nothing to report. Whether it needs visiting at the next cycle too: a ledger it could
    /// not send, its owner's exchange in flight or its piece full; reports not yet
    /// acknowledged; a poll due every cycle. A key its owner's piece
    /// has no room for is treated as one whose owner is busy: what it has not sent stays in
    /// its ledger, unnumbered, and reports due to go again stay due.
    fn key(&mut self, tracked: &mut Tracked<R::Pod>) -> bool {
        let now = self.now;
        // What was admitted drained into the ledger, every key's, with an owner to send it
        // to or not, so that no count fills.
        let (count, started) = tracked.entry.drain(now);
        tracked.unsent_more(count, started);
        let entry = &tracked.entry;
        let key = &entry.key;
        let hold = entry.terms.hold;
        let oldest = tracked.unacknowledged.as_slice().first();
        let oldest = oldest.map(|unseen| unseen.first);
        if cut_off_from_owner(tracked.heard.get(), oldest, self.cut_off_after, now) {
            entry.others.store(0, Ordering::Release);
        }
        let Some(owner) = tracked.owner else {
            return false;
        };
        let free = |pod: &R::Pod| !self.sending.contains_key(pod);
        let through = self.through(tracked, owner).filter(free);
        // Its owner has an exchange in flight: what is admitted stays in the ledger, and
        // the exchange's answer refreshes the copy; but for an owner gone unanswered, whose
        // key goes through another pod meanwhile.
        let (to, also) = if free(&owner) {
            (owner, through)
        } else if let Some(through) = through {
            (through, None)
        } else {
            return true;
        };
        // Ages count back from now, as the batch its entries join is made: every batch waiting
        // was made at a time this data plane was given, and time does not go back.
        let mut entries = Vec::new();
        let mut room = self.queued.get(&to).map_or_else(Room::new, Queued::room);
        let bytes = encoded_entry_bytes(key.len());
        let mut left_out = false;
        for unseen in tracked.unacknowledged.as_mut_slice() {
            *self.forgiven += unseen.empty_if_stale(hold, now);
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
                    limit: entry.terms.limit,
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
                    limit: entry.terms.limit,
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
            if let Some(also) = also {
                let queued = self.queued.entry(also).or_default().at(now);
                for entry in &entries {
                    queued.push_entry(entry.clone());
                }
            }
            let queued = self.queued.entry(to).or_default().at(now);
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
        tracked: &mut Tracked<R::Pod>,
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
fn listed<'a, S: BuildHasher, P>(
    keys: &'a mut Keys<S, P>,
    handle: &Handle,
) -> Option<&'a mut Tracked<P>> {
    keys.get_mut(&handle.0.key)
        .filter(|tracked| Arc::ptr_eq(&tracked.entry, &handle.0))
}

/// Queues `tracked` to be visited at the next cycle, unless it is already.
fn queue<P>(tracked: &mut Tracked<P>, runnable: &mut Vec<Handle>) {
    if !tracked.queued {
        tracked.queued = true;
        runnable.push(Handle::new(Arc::clone(&tracked.entry)));
    }
}

impl<S: BuildHasher + Clone, R: Routing> Participant<S, R> {
    /// The participant `id` sending every `cycle` nanoseconds, reaching the service by
    /// `routing`, its maps and keys hashed by `hasher`; `None` without native 128-bit
    /// atomics.
    #[must_use]
    pub fn new(id: PlaneId, cycle: u64, routing: R, hasher: S) -> Option<Self> {
        if !crate::supported() {
            return None;
        }
        Some(Self {
            id,
            workers: 1,
            registry: Arc::new(Registry::new(hasher.clone())),
            cycle: cycle.max(1),
            routing,
            hasher: hasher.clone(),
            keys: Keys::new(hasher),
            numbered: 0,
            queued: BTreeMap::new(),
            one_in_flight: false,
            sending: BTreeMap::new(),
            latest: BTreeMap::new(),
            exchanges: 0,
            taken: BTreeMap::new(),
            unanswered: BTreeMap::new(),
            late: 0,
            levers: Levers::default(),
            last_now: 0,
            polls: 0,
            exchange_timeout: RESEND_AFTER_CYCLES * cycle.max(1),
            capacity: u64::MAX,
            holding: 0,
            stale: 0,
            evicted: 0,
            forgiven: 0,
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
            if let Some(tracked) = self.keys.get_mut(&entry.key)
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
        if let Some(tracked) = self.keys.get_mut(key) {
            queue(tracked, &mut self.runnable);
        }
    }

    /// The hash of the key of `bytes`: what the keys are kept by, and what the cluster
    /// store's batches and answers name the key by. Under a keyed hasher, alike on every
    /// data plane sharing its key.
    #[must_use]
    pub fn hash(&self, bytes: &[u8]) -> u64 {
        self.keys.hash(bytes)
    }

    /// The routing it holds.
    #[must_use]
    pub fn routing(&self) -> &R {
        &self.routing
    }

    /// The pods that have left a batch unanswered, by `now`, for as long as a report waits
    /// to be sent again — four cycles — their keys going through another pod as well
    /// where the routing names one.
    pub fn unanswered(&self, now: u64) -> impl Iterator<Item = R::Pod> + '_ {
        let after = RESEND_AFTER_CYCLES * self.cycle;
        self.unanswered
            .iter()
            .filter(move |(_, since)| now.saturating_sub(**since) >= after)
            .map(|(pod, _)| *pod)
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
    pub fn exchanged(&mut self, pod: R::Pod, exchange: u64) {
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

    /// Hits given up without being seen applied, all told, as `DataPlane::forgiven`: those of
    /// keys evicted for capacity, and those of reports not acknowledged within their key's
    /// refill time. At most what was lost, since an unacknowledged report may have been
    /// applied with its answer lost.
    #[must_use]
    pub fn forgiven(&self) -> u64 {
        self.forgiven
    }

    /// How many keys it holds.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// The entries of `keys` it holds, for a worker to decide from itself from now on.
    #[must_use]
    pub fn entries(&self, keys: &[(&[u8], Limit)]) -> Vec<Arc<Entry>> {
        keys.iter()
            .filter_map(|(key, limit)| {
                let tracked = self.keys.get(key)?;
                (tracked.entry.terms.limit == *limit).then(|| Arc::clone(&tracked.entry))
            })
            .collect()
    }

    /// Makes sure `key` has a live entry under `limit` at `now`: a new key, or one whose limit
    /// changed, starts afresh, the old entry, if any, closed first; under a fixed window whose
    /// entry's window is over, the next window's entry takes the key's record on.
    fn ensure(&mut self, key: &[u8], limit: Limit, now: u64) {
        let held = self.keys.get(key).map(|tracked| {
            let entry = &tracked.entry;
            (
                entry.terms.limit == limit,
                !entry.snapshot().closed && entry.current(now),
            )
        });
        match held {
            Some((true, true)) => return,
            Some((true, false)) if matches!(limit, Limit::FixedWindow(_)) => {
                if let Some((next, _)) = self.registry.find_or_make(key, limit, self.cycle, now) {
                    self.roll(next, now);
                    return;
                }
            }
            _ => {}
        }
        if let Some(old) = self.keys.remove(key) {
            close(&old.entry);
            self.registry.remove(&old.entry);
            self.forget(key);
        }
        // A worker may have made the key's entry already: that one.
        let made = self
            .registry
            .find_or_make(key, limit, self.cycle, now)
            .map(|(entry, _)| entry);
        let entry = made.unwrap_or_else(|| {
            Arc::new(
                self.registry
                    .made(key, self.registry.terms(limit, self.cycle), now),
            )
        });
        self.track(entry);
    }

    /// A worker made `entry` in the registry ([`Registry::find_or_make`]): kept and reported
    /// from now on, as one this participant made — unless it is closed, or already kept. The
    /// next window of a fixed window it keeps takes the key's record on.
    pub fn adopt(&mut self, entry: Arc<Entry>) {
        if entry.snapshot().closed {
            return;
        }
        if let Some(tracked) = self.keys.get(&entry.key) {
            if Arc::ptr_eq(&tracked.entry, &entry) {
                return;
            }
            let held = &tracked.entry;
            if held.terms.limit == entry.terms.limit && held.window > 0 {
                // A window before it, or one made after it for a window later still.
                if entry.window > held.window {
                    self.roll(entry, self.last_now);
                }
                return;
            }
        }
        if let Some(old) = self.keys.remove(&entry.key) {
            close(&old.entry);
            self.forget(&entry.key);
        }
        self.track(entry);
    }

    /// `next`, the key's entry for a later window of its fixed window, takes the key's
    /// record on from the entry it holds at `now` (06 §4): its reports' numbers, those not
    /// yet acknowledged, its owner and when it was heard of, and the others admitting. The
    /// old entry, closed, has what it admitted and the ledger had not sent numbered as a
    /// report of its window, due to go at the next cycle as one sent again is; past
    /// [`MOST_UNACKNOWLEDGED`] out, an outage's, it is forgiven.
    fn roll(&mut self, next: Arc<Entry>, now: u64) {
        let (scheduled, cycle) = (self.scheduled, self.cycle);
        let Some(tracked) = self.keys.get_mut(&next.key) else {
            return;
        };
        let old = std::mem::replace(&mut tracked.entry, Arc::clone(&next));
        close(&old);
        let (count, started) = old.drain(now);
        tracked.unsent_more(count, started);
        let unsent = std::mem::take(&mut tracked.unsent);
        if unsent > 0 && tracked.unacknowledged.as_slice().len() < MOST_UNACKNOWLEDGED {
            tracked.sent += 1;
            self.numbered += 1;
            let unseen = Unseen {
                sequence: tracked.sent,
                count: unsent,
                admitted: tracked.unsent_since,
                first: now,
                // Due at once.
                last: now.saturating_sub(RESEND_AFTER_CYCLES * cycle),
                window: old.window,
            };
            tracked.unacknowledged.push(unseen, &mut self.spare);
        } else {
            self.forgiven += unsent;
        }
        next.others
            .store(old.others.load(Ordering::Acquire), Ordering::Release);
        self.registry.insert(Arc::clone(&next));
        if scheduled {
            self.stale += entry_bytes(&old.key);
            self.evictions.push(Due {
                at: idle_at(&next),
                entry: Arc::clone(&next),
            });
        }
        let key = next.key.clone();
        self.run(&key);
    }

    /// Takes `key`, no longer kept, off what it holds — but for its entry while scheduled,
    /// which its eviction time keeps until it comes or is dropped, and which counts as stale
    /// meanwhile.
    fn forget(&mut self, key: &[u8]) {
        self.holding = self
            .holding
            .saturating_sub(key_bytes::<R::Pod>(key, self.workers));
        if self.scheduled {
            self.stale += entry_bytes(key);
        }
    }

    /// Drops the eviction times of entries no longer kept, freeing the entries: a pass over
    /// every key's, made only past the cap, as eviction's own is.
    fn drop_stale(&mut self) {
        let keys = &self.keys;
        let mut freed = 0;
        self.evictions.retain(|due| {
            let kept = keys
                .get(&due.entry.key)
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
        let owner = self.routing.owner(&entry.key, self.keys.hash(&entry.key));
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
        self.holding += key_bytes::<R::Pod>(&tracked.entry.key, self.workers);
        if self.scheduled {
            let at = idle_at(&tracked.entry);
            self.evictions.push(Due {
                at,
                entry: Arc::clone(&tracked.entry),
            });
        }
        let key = tracked.entry.key.clone();
        self.keys.insert(tracked);
        self.run(&key);
    }

    /// Decides at `now` a request a worker asked about, under `keys`: as
    /// `DataPlane::decide_all` does, from the shared entries, a key not met before made
    /// with a full bucket, or an empty window.
    pub fn decide_all(&mut self, keys: &[(&[u8], Limit)], now: u64) -> Decision {
        self.last_now = now;
        for &(key, limit) in keys {
            self.ensure(key, limit, now);
            let Some(tracked) = self.keys.get(key) else {
                return Decision::Refuse;
            };
            tracked.entry.last_request.store(now, Ordering::Relaxed);
        }
        let entries: Vec<Arc<Entry>> = self.entries(keys);
        let entries: Vec<&Entry> = entries.iter().map(|entry| &**entry).collect();
        let outcome = decide(&entries, now);
        for (at, (key, _)) in keys.iter().enumerate() {
            if outcome.signal & bit(at) != 0 {
                self.run(key);
            }
        }
        match outcome.local {
            Local::Admit => Decision::Admit,
            // Closed is impossible: every entry was made live above, on this thread.
            Local::Refuse | Local::Ask => Decision::Refuse,
        }
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
            cut_off_after: CUT_OFF_CYCLES * self.cycle,
            poll_quiet: self.levers.poll_quiet,
            hasher: &self.hasher,
            routing: &self.routing,
            unanswered: &self.unanswered,
            sending: &self.sending,
            queued: &mut self.queued,
            numbered: &mut self.numbered,
            forgiven: &mut self.forgiven,
            spare: &mut self.spare,
            cut_off: false,
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
            for tracked in self.keys.iter_mut() {
                if std::mem::take(&mut tracked.left_out) {
                    continue;
                }
                visits += 1;
                let handle = Handle::new(Arc::clone(&tracked.entry));
                visit.listed(handle, tracked, scheduled, &mut left_out, &mut next);
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
        let cut_off = visit.cut_off;
        self.keys_left_out += visit.keys_left_out;
        self.visits += visits;
        self.next_runnable = std::mem::replace(&mut self.runnable, next);
        self.next_runnable.clear();
        if cut_off && let Some(pod) = self.routing.polled(self.polls) {
            self.polls += 1;
            if !self.sending.contains_key(&pod) {
                self.queued.entry(pod).or_default().at(now);
            }
        }
        if self.scheduled {
            self.evict_due(now);
        } else {
            let mut freed = 0;
            let (registry, workers) = (&self.registry, self.workers);
            self.keys.retain(|tracked| {
                let evict = tracked.idle(now) && close_if_quiet(tracked);
                if evict {
                    registry.remove(&tracked.entry);
                    freed += key_bytes::<R::Pod>(&tracked.entry.key, workers);
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
            // Every key, whatever it has outstanding: what it has not had applied is
            // forgiven, so that memory stays within the cap however long the service is out
            // of reach.
            let mut used: Vec<(u64, Box<[u8]>)> = self
                .keys
                .iter()
                .map(|t| {
                    let used = t.entry.last_request.load(Ordering::Relaxed);
                    (used, t.entry.key.clone())
                })
                .collect();
            used.sort_unstable_by_key(|(used, _)| *used);
            for (_, key) in used {
                if self.holding <= target {
                    break;
                }
                let Some(tracked) = self.keys.remove(&key) else {
                    continue;
                };
                let unreported = close(&tracked.entry).unreported;
                let unseen: u64 = tracked
                    .unacknowledged
                    .as_slice()
                    .iter()
                    .map(|u| u.count)
                    .sum();
                self.forgiven += unreported + tracked.unsent + unseen;
                self.registry.remove(&tracked.entry);
                self.forget(&key);
                self.evicted += 1;
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
                .get(&due.entry.key)
                .filter(|tracked| Arc::ptr_eq(&tracked.entry, &due.entry));
            let Some(tracked) = kept else {
                self.stale = self.stale.saturating_sub(entry_bytes(&due.entry.key));
                continue;
            };
            if tracked.idle(now) && close_if_quiet(tracked) {
                self.registry.remove(&tracked.entry);
                self.holding = self
                    .holding
                    .saturating_sub(key_bytes::<R::Pod>(&due.entry.key, self.workers));
                self.keys.remove(&due.entry.key);
            } else {
                let at = idle_at(&due.entry).max(now + self.cycle);
                self.evictions.push(Due {
                    at,
                    entry: due.entry,
                });
            }
        }
    }

    /// A pod's reply to the latest exchange started with the pod it came through — `from`
    /// itself, or the pod it was relayed through — reaches it at `now`.
    pub fn on_reply(&mut self, reply: Reply<R>, now: u64) {
        let through = reply.via.unwrap_or(reply.from);
        let exchange = self.latest.get(&through).copied().unwrap_or(0);
        self.on_reply_to(exchange, reply, now);
    }

    /// A pod's reply to exchange `exchange` ([`Self::take_exchanges`]) reaches it at `now`,
    /// as `DataPlane::on_answers` takes answers: each copy rebased on the owner's bucket and
    /// charged with what the owner has not seen, in one compare-and-swap with the admissions
    /// it races. Their buckets rebase the copies only if the pod handled their batch after
    /// that of the answers last taken from it, so the copies never go back to an older
    /// bucket: in one life of the pod, if their number is higher; from another life, if
    /// their exchange started after every exchange started by the time the life last taken
    /// was first heard from, since lives of a pod never serve at once. Answers from
    /// the same life not taken still acknowledge reports, which stays true; another life's
    /// do not, so that the reports go to the life serving, which ignores those its copy
    /// holds already. A reply of the pod's own ends the exchange and its wait
    /// ([`Self::unanswered`]); one relayed ends neither, the pod it came through having
    /// answered for itself.
    pub fn on_reply_to(&mut self, exchange: u64, reply: Reply<R>, now: u64) {
        if reply.via.is_none() {
            if self.latest.get(&reply.from) == Some(&exchange) {
                self.sending.remove(&reply.from);
            }
            self.unanswered.remove(&reply.from);
        }
        let last = self.taken.get(&reply.from);
        let same_life = last.is_some_and(|last| last.life == reply.life);
        let current = match last {
            // Unnumbered: taken as it comes.
            _ if reply.life == 0 => true,
            None => true,
            Some(last) if same_life => later(reply.number, last.number),
            Some(last) => later(exchange, last.fence),
        };
        let acknowledges = current || same_life;
        if current {
            if reply.life != 0 {
                let fence = match last {
                    Some(last) if same_life => last.fence,
                    _ => self.latest.get(&reply.from).copied().unwrap_or(exchange),
                };
                self.taken.insert(
                    reply.from,
                    LastTaken {
                        life: reply.life,
                        number: reply.number,
                        fence,
                    },
                );
            }
        } else {
            self.late += 1;
        }
        self.take_reply(reply, current, acknowledges, now);
    }

    /// `reply` taken, its buckets too if `current`, its acknowledgements if
    /// `acknowledges`.
    fn take_reply(&mut self, reply: Reply<R>, current: bool, acknowledges: bool, now: u64) {
        let Reply {
            from,
            entries,
            routing,
            made,
            ..
        } = reply;
        self.last_now = now;
        let made = super::taken_as_of(made, self.cycle, now);
        for answer in entries {
            let Some(tracked) = self.keys.by_hash_mut(answer.key) else {
                continue;
            };
            // Only the owner it sends to: an answer from an owner that has gone is stale.
            if tracked.owner != Some(from) {
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
            // What the copy is rebased on: a bucket's distance from full, or its own window's
            // count; another window's count rebases nothing (06 §4), and a standing of the
            // other model's is not about this key's copy at all.
            let entry = &tracked.entry;
            let rebase = match (&entry.terms.model, answer.standing) {
                (Model::Bucket { bucket, .. }, Standing::Bucket { behind }) => {
                    Rebase::Bucket(bucket, behind)
                }
                (Model::Window { .. }, Standing::Window { number, count }) => {
                    if number == entry.window {
                        Rebase::Count(count)
                    } else {
                        Rebase::Nothing
                    }
                }
                _ => continue,
            };
            entry.others.store(answer.others, Ordering::Release);
            // The hits the owner has yet to apply.
            let unapplied = tracked.unapplied();
            match rebase {
                Rebase::Bucket(bucket, behind) => {
                    let units = bucket.units(made);
                    entry.update(now, |fields| {
                        let extra = unapplied + fields.unreported;
                        Some(Fields {
                            arrival: bucket.rebased(units, behind, extra),
                            ..fields
                        })
                    });
                }
                Rebase::Count(count) => {
                    entry.update(now, |fields| {
                        let extra = unapplied + fields.unreported;
                        Some(Fields {
                            arrival: count.saturating_add(extra).min(MASK),
                            ..fields
                        })
                    });
                }
                Rebase::Nothing => {}
            }
            tracked.heard = Heard::at(now);
            queue(tracked, &mut self.runnable);
        }
        if let Some(routing) = routing {
            self.on_routing(routing, now);
        }
    }

    /// The service's pods change at `now`, as `DataPlane::on_view`, if `routing` is later
    /// than the one it holds: each key whose owner moved has what it has not seen applied
    /// queued to its new owner at once. A pod the routing names no longer is waited on no
    /// more.
    pub fn on_routing(&mut self, routing: R, now: u64) {
        if !routing.later(&self.routing) {
            return;
        }
        self.routing = routing;
        let routing = &self.routing;
        self.unanswered.retain(|pod, _| routing.names(*pod));
        for (hash, tracked) in self.keys.hashed_mut() {
            let key = &tracked.entry.key;
            let owner = routing.owner(key, hash);
            if owner == tracked.owner {
                continue;
            }
            tracked.owner = owner;
            if self.scheduled && !tracked.queued {
                tracked.queued = true;
                self.runnable.push(Handle::new(Arc::clone(&tracked.entry)));
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
                    limit: tracked.entry.terms.limit,
                    report,
                });
            }
        }
    }

    /// Those of `keys` that would refuse at `now`, by position, with how long each waits.
    pub fn waits<'a>(
        &'a self,
        keys: &'a [(&[u8], Limit)],
        now: u64,
    ) -> impl Iterator<Item = (usize, u64)> + 'a {
        keys.iter()
            .enumerate()
            .filter_map(move |(at, (key, limit))| {
                let tracked = self
                    .keys
                    .get(key)
                    .filter(|t| t.entry.terms.limit == *limit)?;
                Some((at, tracked.entry.wait(now)?))
            })
    }

    /// As `DataPlane::retry_after`.
    #[must_use]
    pub fn retry_after(&self, keys: &[(&[u8], Limit)], now: u64) -> Option<(usize, u64)> {
        longest(self.waits(keys, now))
    }

    /// Messages to send, as `DataPlane::take_outgoing`.
    pub fn take_outgoing(&mut self) -> Vec<(R::Pod, R::Batch)> {
        self.take_exchanges()
            .into_iter()
            .map(|(pod, _, batch)| (pod, batch))
            .collect()
    }

    /// Messages to send, each starting an exchange with its pod, numbered: its answers are
    /// taken under that number ([`Self::on_reply_to`]), and its failure said under it
    /// ([`Self::exchanged`]). A pod sent entries is waited on from now, unless it was already
    /// ([`Self::unanswered`]).
    pub fn take_exchanges(&mut self) -> Vec<(R::Pod, u64, R::Batch)> {
        let batches = if self.one_in_flight {
            self.take_outgoing_to_idle()
        } else {
            self.take_outgoing_to_all()
        };
        let hasher = &self.hasher;
        batches
            .into_iter()
            .map(|(pod, batch)| {
                self.exchanges = self.exchanges.wrapping_add(1);
                self.latest.insert(pod, self.exchanges);
                if !batch.entries.is_empty() {
                    self.unanswered.entry(pod).or_insert(self.last_now);
                }
                let batch = self
                    .routing
                    .batch(pod, batch, |bytes| keys::hash(hasher, bytes));
                (pod, self.exchanges, batch)
            })
            .collect()
    }

    /// A piece of what is to go to each pod; the rest kept for the next.
    fn take_outgoing_to_all(&mut self) -> Vec<(R::Pod, Batch)> {
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
    fn take_outgoing_to_idle(&mut self) -> Vec<(R::Pod, Batch)> {
        let pods: Vec<R::Pod> = self
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

impl<S: BuildHasher + Clone> Participant<S, View> {
    /// Today's pod's answers to the latest exchange started with it reach it at `now`
    /// ([`Self::on_reply`]).
    pub fn on_answers(&mut self, answers: Answers, now: u64) {
        let reply = Reply::of(answers, |bytes| self.hash(bytes));
        self.on_reply(reply, now);
    }

    /// Today's pod's answers to exchange `exchange` reach it at `now`
    /// ([`Self::on_reply_to`]).
    pub fn on_answers_to(&mut self, exchange: u64, answers: Answers, now: u64) {
        let reply = Reply::of(answers, |bytes| self.hash(bytes));
        self.on_reply_to(exchange, reply, now);
    }

    /// Today's pods change at `now` ([`Self::on_routing`]).
    pub fn on_view(&mut self, view: View, now: u64) {
        self.on_routing(view, now);
    }
}

/// Closes `tracked`'s entry if nothing was admitted since the last report, in one
/// compare-and-swap: a take after it fails and asks, and none before it is
/// lost. Whether it closed.
fn close_if_quiet<P>(tracked: &Tracked<P>) -> bool {
    let entry = &tracked.entry;
    let mut current = entry.word.load(Ordering::Acquire);
    loop {
        let fields = Fields::unpack(current);
        if fields.closed {
            return true;
        }
        if fields.unreported != 0 {
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

/// Closes an entry, whatever it holds: replaced, its contract changed, or evicted for
/// capacity; the key starts afresh, what the old one had not reported dropped with it, as
/// `DataPlane` drops such a key. What the entry held as it closed.
fn close(entry: &Entry) -> Fields {
    let mut current = entry.word.load(Ordering::Acquire);
    loop {
        let fields = Fields::unpack(current);
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
            Ok(_) => return fields,
            Err(seen) => current = seen,
        }
    }
}
