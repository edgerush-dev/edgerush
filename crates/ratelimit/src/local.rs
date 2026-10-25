//! Local mode (06 §4): each data-plane process limits on what it sees alone, with no
//! rate-limiting service and nothing shared with other processes.
//!
//! A key is one 64-bit atomic, its bucket's arrival time as GCRA keeps it (governor's way),
//! in a registry the process's workers share. A worker keeps its own handles to the keys it
//! has met, so that a decision takes no lock: it reads the key's word and swaps in the
//! charged one. A request under several limits is admitted only if every one has room, read
//! first and charged after: two racing on the last room of a limit may both be admitted, a
//! request too many, but none is refused within its limits. A sweep, every second or so,
//! forgets keys whose buckets are full and that nothing used lately — nothing is forgiven by
//! that — and past the byte cap evicts the keys whose buckets would be full soonest, the
//! least forgiven.
//!
//! Times are nanoseconds from any origin the caller keeps to, as everywhere in the crate.
//! A bucket's arrival time is kept in units of 1/65,536 of a request's interval, modulo
//! 2^63; the top bit of the word marks a key forgotten.

mod sync;

#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(test)]
mod tests;

pub use crate::service::Contract;

use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use sync::{Arc, AtomicBool, AtomicU64, Ordering, RwLock};

/// Units in a request's interval.
const UNITS: u64 = 1 << 16;
/// The arrival time's range: units modulo 2^63.
const MASK: u64 = (1 << 63) - 1;
/// How far ahead of now an arrival time may be and still be ahead, not behind: a quarter of
/// the range, so that the difference of two times within it is never mistaken.
const AHEAD_BELOW: u64 = 1 << 61;
/// The word's top bit: the key was forgotten or evicted, and a new one takes its place.
const CLOSED: u64 = 1 << 63;
/// The most of a bucket kept: past it, a bucket is this big. Keeps the tolerance far within
/// [`AHEAD_BELOW`].
const MOST_BURST: u64 = 1 << 40;
/// How many shards the registry has: a worker meeting a key it holds no handle to takes one
/// shard's lock, and the sweep takes each in turn. A few under loom, which tracks each lock.
const SHARDS: usize = if cfg!(loom) { 2 } else { 256 };

/// A contract's arithmetic: time to units and back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Rate {
    /// Units a nanosecond, as a 64.64 fixed-point number, rounded up: a time converts to at
    /// most a unit more than it is worth, which errs towards admitting.
    per_ns_high: u64,
    per_ns_low: u64,
    /// How far ahead of now the arrival time may be for a request to be admitted, in units:
    /// the bucket less one.
    tolerance: u64,
    /// The contract's window and limit, for units back to time.
    window: u128,
    limit: u128,
    /// The time a full bucket takes to refill, rounded up: a key unused for longer is full,
    /// whatever its word says once its clock has wrapped round.
    refill: u64,
}

impl Rate {
    fn new(contract: Contract) -> Self {
        let limit = u128::from(contract.limit.get());
        let window = u128::from(contract.window.get());
        let burst = contract.burst.get().min(MOST_BURST);
        let scaled = limit * u128::from(UNITS);
        let whole = scaled / window;
        let rest = ((scaled % window) << 64).div_ceil(window);
        let per_ns = (whole << 64) + rest;
        let refill = (u128::from(burst) * window).div_ceil(limit);
        Self {
            per_ns_high: u64::try_from(per_ns >> 64).unwrap_or(u64::MAX),
            per_ns_low: per_ns as u64,
            tolerance: (burst - 1) * UNITS,
            window,
            limit,
            refill: u64::try_from(refill).unwrap_or(u64::MAX),
        }
    }

    /// `now`, in units, modulo the range.
    fn units(&self, now: u64) -> u64 {
        let high = u128::from(now) * u128::from(self.per_ns_high);
        let low = (u128::from(now) * u128::from(self.per_ns_low)) >> 64;
        (high + low) as u64 & MASK
    }

    /// How far ahead of `now` (units) `arrival` is: zero if it is behind.
    fn ahead(arrival: u64, now: u64) -> u64 {
        let ahead = arrival.wrapping_sub(now) & MASK;
        if ahead < AHEAD_BELOW { ahead } else { 0 }
    }

    /// Nanoseconds that `units` take to pass, rounded up.
    fn nanos(&self, units: u64) -> u64 {
        let nanos = (u128::from(units) * self.window).div_ceil(self.limit * u128::from(UNITS));
        u64::try_from(nanos).unwrap_or(u64::MAX)
    }
}

/// One key: its bucket's arrival time, and when it was last asked for.
#[derive(Debug)]
pub struct Key {
    key: Box<[u8]>,
    contract: Contract,
    rate: Rate,
    /// The arrival time in units, the top bit set once the key is closed.
    word: AtomicU64,
    /// The latest time a request was charged under it, moved before the charge lands.
    last: AtomicU64,
    /// Changed since its state was last copied: set by a decision that changed it, where
    /// the registry tracks changes ([`Local::tracking`]), and cleared as it is copied.
    changed: AtomicBool,
}

/// What a look at a key found.
enum Look {
    /// A request fits, the word as read and the arrival time it would charge from.
    Fits { word: u64, from: u64 },
    /// No room: the time until there is.
    Full { wait: u64 },
    /// The key was closed: its successor in the registry is to be used.
    Closed,
}

impl Key {
    fn new(key: Box<[u8]>, contract: Contract, now: u64) -> Self {
        let rate = Rate::new(contract);
        Self {
            key,
            contract,
            rate,
            word: AtomicU64::new(rate.units(now)),
            last: AtomicU64::new(now),
            changed: AtomicBool::new(false),
        }
    }

    /// One request's charge at `now` given back: the bucket a request's room fuller, never
    /// fuller than full. False if the key was closed.
    fn give_back(&self, now: u64) -> bool {
        let now_units = self.rate.units(now);
        loop {
            let word = self.word.load(Ordering::Acquire);
            if word & CLOSED != 0 {
                return false;
            }
            let arrival = self.arrival(word, now, now_units);
            let back = if Rate::ahead(arrival, now_units) > UNITS {
                arrival.wrapping_sub(UNITS) & MASK
            } else {
                now_units
            };
            if self
                .word
                .compare_exchange_weak(word, back, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Marks it changed where `tracking`: true if this is its first change since its last
    /// copy, for the change's connection to gather it. Always a swap, never a look first: a
    /// look could see the mark a copy has just cleared and gather nothing, while the copy
    /// read the key from before the change, losing it.
    fn mark(&self, tracking: bool) -> bool {
        tracking && !self.changed.swap(true, Ordering::AcqRel)
    }

    /// Its state as a copy at `now`, marked unchanged first, so that a change made while it
    /// is read marks it again, for the next copy. A swap, as the change's mark is: whichever
    /// comes second sees the other, so a change is in this copy or marked for the next.
    fn copy(&self, now: u64) -> Copy {
        self.changed.swap(false, Ordering::AcqRel);
        Copy {
            key: self.key.clone(),
            contract: self.contract,
            full_in: self.full_in(now),
        }
    }

    /// Set to be full in `full_in` nanoseconds from `now`: an owner's copy taken in.
    fn set_full_in(&self, full_in: u64, now: u64) {
        let now_units = self.rate.units(now);
        let ahead = self
            .rate
            .units(now.saturating_add(full_in))
            .wrapping_sub(now_units)
            & MASK;
        let arrival = now_units.wrapping_add(ahead) & MASK;
        self.last.fetch_max(now, Ordering::Relaxed);
        loop {
            let word = self.word.load(Ordering::Acquire);
            if word & CLOSED != 0 {
                return;
            }
            if self
                .word
                .compare_exchange_weak(word, arrival, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn closed(&self) -> bool {
        self.word.load(Ordering::Acquire) & CLOSED != 0
    }

    /// Whether, at `now`, the key went unused for longer than its bucket takes to refill:
    /// full, whatever its word says once its clock has come round. Sound because a charge
    /// moves the last use before it lands ([`charge_from`]): a word read is never older than
    /// the last use read after it.
    ///
    /// [`charge_from`]: Self::charge_from
    fn idle(&self, now: u64) -> bool {
        now.saturating_sub(self.last.load(Ordering::Relaxed)) > self.rate.refill
    }

    /// The arrival time as of `now` (units `now_units`) from `word`: as it is, unless the key
    /// is idle, when it is full.
    fn arrival(&self, word: u64, now: u64, now_units: u64) -> u64 {
        if self.idle(now) { now_units } else { word }
    }

    /// Whether a request at `now` fits.
    fn look(&self, now: u64) -> Look {
        let word = self.word.load(Ordering::Acquire);
        if word & CLOSED != 0 {
            return Look::Closed;
        }
        let now_units = self.rate.units(now);
        self.fits(word, self.arrival(word, now, now_units), now_units)
    }

    /// Whether a request at `now_units` fits, the bucket's arrival time `arrival` as of then.
    fn fits(&self, word: u64, arrival: u64, now_units: u64) -> Look {
        let ahead = Rate::ahead(arrival, now_units);
        if ahead > self.rate.tolerance {
            return Look::Full {
                wait: self.rate.nanos(ahead - self.rate.tolerance),
            };
        }
        let from = if ahead > 0 { arrival } else { now_units };
        Look::Fits { word, from }
    }

    /// The word a charge at `now` (units `now_units`) is to replace, `None` if the key was
    /// closed. An idle key's word is first made to say what the key is, full at `now`,
    /// before a charge moves its last use: a word from before the idle spell, its clock
    /// perhaps come round, is never read beside a recent last use, which would take it as
    /// it is.
    fn word_to_charge(&self, now: u64, now_units: u64) -> Option<u64> {
        loop {
            let word = self.word.load(Ordering::Acquire);
            if word & CLOSED != 0 {
                return None;
            }
            if !self.idle(now) {
                return Some(word);
            }
            if self
                .word
                .compare_exchange(word, now_units, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(now_units);
            }
        }
    }

    /// Replaces `word` with a request charged from `from` at `now`, false if the word
    /// changed. The last use moves first, so that a worker reading the charge reads a last
    /// use at least as recent, and never takes the key for idle and charges over it.
    fn charge_from(&self, word: u64, from: u64, now: u64) -> bool {
        self.last.fetch_max(now, Ordering::Relaxed);
        self.word
            .compare_exchange(
                word,
                from.wrapping_add(UNITS) & MASK,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// One request at `now`, admitted if it fits.
    fn take(&self, now: u64) -> Look {
        let now_units = self.rate.units(now);
        loop {
            let Some(word) = self.word_to_charge(now, now_units) else {
                return Look::Closed;
            };
            match self.fits(word, word, now_units) {
                Look::Fits { word, from } => {
                    if self.charge_from(word, from, now) {
                        return Look::Fits { word, from };
                    }
                }
                other => return other,
            }
        }
    }

    /// One request at `now` charged, whether it fits or not: false if the key was closed.
    fn charge(&self, now: u64) -> bool {
        let now_units = self.rate.units(now);
        loop {
            let Some(word) = self.word_to_charge(now, now_units) else {
                return false;
            };
            let from = if Rate::ahead(word, now_units) > 0 {
                word
            } else {
                now_units
            };
            if self.charge_from(word, from, now) {
                return true;
            }
        }
    }

    /// Closes it if, at `now`, its bucket is full and nothing used it for `quiet`: true if
    /// it did.
    fn close_if_forgettable(&self, now: u64, quiet: u64) -> bool {
        let word = self.word.load(Ordering::Acquire);
        if word & CLOSED != 0 {
            return true;
        }
        if now.saturating_sub(self.last.load(Ordering::Relaxed)) < quiet {
            return false;
        }
        let now_units = self.rate.units(now);
        if Rate::ahead(self.arrival(word, now, now_units), now_units) > 0 {
            return false;
        }
        // Lost to a request just made, the key stays: it is in use.
        self.word
            .compare_exchange(word, word | CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Nanoseconds from `now` until its bucket is full.
    fn full_in(&self, now: u64) -> u64 {
        let word = self.word.load(Ordering::Acquire) & MASK;
        let now_units = self.rate.units(now);
        self.rate
            .nanos(Rate::ahead(self.arrival(word, now, now_units), now_units))
    }

    fn close(&self) {
        self.word.fetch_or(CLOSED, Ordering::AcqRel);
    }
}

/// A key as handles share it: what [`Handles::take_changed`] gathers.
pub type Shared = Arc<Key>;

/// A key in a set, found by its name.
#[derive(Clone, Debug)]
pub struct Handle(Arc<Key>);

impl PartialEq for Handle {
    fn eq(&self, other: &Self) -> bool {
        self.0.key == other.0.key
    }
}

impl Eq for Handle {}

impl Hash for Handle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.key.hash(state);
    }
}

impl Borrow<[u8]> for Handle {
    fn borrow(&self) -> &[u8] {
        &self.0.key
    }
}

/// What a decision is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Admitted, and charged under every limit.
    Admit,
    /// Refused, charged under none: the limit, by its place in the list, that will take the
    /// longest to have room, the first of those that take as long, and how long that is.
    Refuse {
        /// Its place in the list decided.
        position: usize,
        /// Nanoseconds until it has room.
        wait: u64,
    },
}

/// A key's state as its owner copies it to the pods that would take over from it: its
/// contract and how long its bucket takes to be full again, which reads the same on any
/// clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Copy {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract.
    pub contract: Contract,
    /// Nanoseconds from when the copy was taken until its bucket is full.
    pub full_in: u64,
}

/// What a sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Keys forgotten: full, and unused for the time asked.
    pub forgotten: u64,
    /// Keys evicted to come within the cap.
    pub evicted: u64,
}

/// The keys one process's workers share.
#[derive(Debug)]
pub struct Local<S> {
    shards: Box<[RwLock<HashSet<Handle, S>>]>,
    hasher: S,
    /// Workers holding handles to the keys, which a key's bytes count.
    workers: u64,
    holding: AtomicU64,
    capacity: AtomicU64,
    evicted: AtomicU64,
    /// Whether decisions mark the keys they change, for copying them on.
    tracking: bool,
}

/// The bytes a key of `key` is counted as: the key itself, its name, and a slot for its
/// handle in the registry and in each of `workers`' sets, a byte of a table's control with
/// each. An estimate, not what the allocator takes.
#[must_use]
pub fn key_bytes(key: &[u8], workers: u64) -> u64 {
    // An `Arc`'s two counts beside the key.
    let entry = 2 * size_of::<usize>() + size_of::<Key>() + key.len();
    let slot = (size_of::<Handle>() + 1) as u64;
    entry as u64 + slot * (1 + workers)
}

impl<S: BuildHasher + Clone> Local<S> {
    /// No keys yet, hashed by `hasher`, shared by `workers` workers; no cap until one is set.
    #[must_use]
    pub fn new(hasher: S, workers: u64) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| RwLock::new(HashSet::with_hasher(hasher.clone())))
                .collect(),
            hasher,
            workers: workers.max(1),
            holding: AtomicU64::new(0),
            capacity: AtomicU64::new(u64::MAX),
            evicted: AtomicU64::new(0),
            tracking: false,
        }
    }

    /// The same, its decisions marking the keys they change, which each worker's handles
    /// gather ([`Handles::take_changed`]) for their state to be copied on ([`copies`]): what
    /// an owner keeps, for the pods that would take over from it.
    ///
    /// [`copies`]: Self::copies
    #[must_use]
    pub fn tracking(hasher: S, workers: u64) -> Self {
        Self {
            tracking: true,
            ..Self::new(hasher, workers)
        }
    }

    /// Gives one request's charge back under each of `keys` at `now`: a request admitted
    /// under them and then refused under another owner's limit, so charged under none.
    pub fn give_back<K: AsRef<[u8]>>(
        &self,
        handles: &mut Handles<S>,
        keys: &[(K, Contract)],
        now: u64,
    ) {
        for (key, contract) in keys {
            charge_one(self, handles, key.as_ref(), *contract, now, Key::give_back);
        }
    }

    /// The state at `now` of `changed`, keys workers' decisions changed, closed keys left
    /// out. A key is gathered by one worker at a time — the first to change it since its
    /// last copy — so each comes once.
    #[must_use]
    pub fn copies(&self, changed: &[Shared], now: u64) -> Vec<Copy> {
        changed
            .iter()
            .filter(|key| !key.closed())
            .map(|key| key.copy(now))
            .collect()
    }

    /// Takes in an owner's `copies` at `now`: each key made if it is not held, and set to
    /// the owner's state, so that this registry carries on from it if it becomes the owner.
    pub fn install(&self, copies: &[Copy], now: u64) {
        for copy in copies {
            self.find_or_make(&copy.key, copy.contract, now)
                .set_full_in(copy.full_in, now);
        }
    }

    /// The bytes its keys may hold, as [`key_bytes`] counts them: past them, a sweep evicts.
    pub fn set_capacity(&self, bytes: u64) {
        self.capacity.store(bytes, Ordering::Relaxed);
    }

    /// The bytes its keys hold, as [`key_bytes`] counts them.
    #[must_use]
    pub fn holding(&self) -> u64 {
        self.holding.load(Ordering::Relaxed)
    }

    /// Keys evicted for the cap, all told.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    /// How many keys it holds.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.shards.iter().map(|shard| read(shard).len()).sum()
    }

    /// Whether it holds `key`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        read(self.shard(key)).contains(key)
    }

    fn shard(&self, key: &[u8]) -> &RwLock<HashSet<Handle, S>> {
        let at = usize::try_from(self.hasher.hash_one(key)).unwrap_or(0) % self.shards.len();
        // `at` is below the length.
        &self.shards[at]
    }

    /// The live key of `key` under `contract`, made at `now`, full, if there is none; one
    /// closed or under another contract is replaced.
    fn find_or_make(&self, key: &[u8], contract: Contract, now: u64) -> Arc<Key> {
        let shard = self.shard(key);
        if let Some(found) = read(shard).get(key)
            && found.0.contract == contract
            && !found.0.closed()
        {
            return Arc::clone(&found.0);
        }
        let mut set = write(shard);
        if let Some(found) = set.get(key) {
            if found.0.contract == contract && !found.0.closed() {
                return Arc::clone(&found.0);
            }
            found.0.close();
            self.holding
                .fetch_sub(key_bytes(key, self.workers), Ordering::Relaxed);
        }
        let made = Arc::new(Key::new(key.into(), contract, now));
        set.replace(Handle(Arc::clone(&made)));
        self.holding
            .fetch_add(key_bytes(key, self.workers), Ordering::Relaxed);
        made
    }

    /// Decides a request at `now` under `keys`, each with its contract, through a worker's
    /// `handles`: admitted and charged under all of them, or refused and charged under
    /// none.
    pub fn decide<K: AsRef<[u8]>>(
        &self,
        handles: &mut Handles<S>,
        keys: &[(K, Contract)],
        now: u64,
    ) -> Decision {
        if let [(key, contract)] = keys {
            return loop {
                let taken = handles.with_key(self, key.as_ref(), *contract, now, |held| {
                    let look = held.take(now);
                    let first = matches!(look, Look::Fits { .. }) && held.mark(self.tracking);
                    (look, first)
                });
                match taken {
                    (Look::Fits { .. }, first) => {
                        if first {
                            handles.gather(key.as_ref());
                        }
                        break Decision::Admit;
                    }
                    (Look::Full { wait }, _) => break Decision::Refuse { position: 0, wait },
                    (Look::Closed, _) => handles.forget(key.as_ref()),
                }
            };
        }
        let mut longest: Option<(usize, u64)> = None;
        for (position, (key, contract)) in keys.iter().enumerate() {
            let wait = loop {
                match handles.with_key(self, key.as_ref(), *contract, now, |held| held.look(now)) {
                    Look::Fits { .. } => break None,
                    Look::Full { wait } => break Some(wait),
                    Look::Closed => handles.forget(key.as_ref()),
                }
            };
            if let Some(wait) = wait
                && longest.is_none_or(|(_, most)| wait > most)
            {
                longest = Some((position, wait));
            }
        }
        if let Some((position, wait)) = longest {
            return Decision::Refuse { position, wait };
        }
        for (key, contract) in keys {
            charge_one(self, handles, key.as_ref(), *contract, now, Key::charge);
        }
        Decision::Admit
    }

    /// Of `keys`, those without room at `now`, by their place in the list, each with how long
    /// until it has room: what a refusal says. Charges nothing.
    pub fn refused_by<K: AsRef<[u8]>>(
        &self,
        handles: &mut Handles<S>,
        keys: &[(K, Contract)],
        now: u64,
    ) -> Vec<(usize, u64)> {
        let mut refused = Vec::new();
        for (position, (key, contract)) in keys.iter().enumerate() {
            let wait = loop {
                match handles.with_key(self, key.as_ref(), *contract, now, |held| held.look(now)) {
                    Look::Fits { .. } => break None,
                    Look::Full { wait } => break Some(wait),
                    Look::Closed => handles.forget(key.as_ref()),
                }
            };
            if let Some(wait) = wait {
                refused.push((position, wait));
            }
        }
        refused
    }

    /// Forgets the keys whose buckets are full at `now` and that no request used for
    /// `quiet`; then, past the cap, evicts the keys whose buckets will be full soonest, down
    /// to nine tenths of it. Taken every second or so, off the request path.
    pub fn sweep(&self, now: u64, quiet: u64) -> Swept {
        let mut swept = Swept::default();
        let mut freed = 0;
        for shard in &self.shards {
            write(shard).retain(|handle| {
                let forget = handle.0.close_if_forgettable(now, quiet);
                if forget {
                    swept.forgotten += 1;
                    freed += key_bytes(&handle.0.key, self.workers);
                }
                !forget
            });
        }
        self.holding.fetch_sub(freed, Ordering::Relaxed);
        let capacity = self.capacity.load(Ordering::Relaxed);
        if self.holding() <= capacity {
            return swept;
        }
        let mut keys: Vec<(u64, Arc<Key>)> = self
            .shards
            .iter()
            .flat_map(|shard| {
                read(shard)
                    .iter()
                    .map(|handle| (handle.0.full_in(now), Arc::clone(&handle.0)))
                    .collect::<Vec<_>>()
            })
            .collect();
        keys.sort_unstable_by_key(|(full_in, _)| *full_in);
        let target = capacity - capacity / 10;
        for (_, key) in keys {
            if self.holding() <= target {
                break;
            }
            let mut set = write(self.shard(&key.key));
            if set
                .get(&*key.key)
                .is_some_and(|held| Arc::ptr_eq(&held.0, &key))
            {
                key.close();
                set.remove(&*key.key);
                self.holding
                    .fetch_sub(key_bytes(&key.key, self.workers), Ordering::Relaxed);
                self.evicted.fetch_add(1, Ordering::Relaxed);
                swept.evicted += 1;
            }
        }
        swept
    }
}

/// A worker's handles to the keys it has met: found without a lock, by the key's name.
#[derive(Debug)]
pub struct Handles<S> {
    set: HashSet<Handle, S>,
    /// Keys its decisions changed since last taken, each once.
    changed: Vec<Arc<Key>>,
}

impl<S: BuildHasher + Clone> Handles<S> {
    /// None yet.
    #[must_use]
    pub fn new(hasher: S) -> Self {
        Self {
            set: HashSet::with_hasher(hasher),
            changed: Vec::new(),
        }
    }

    /// The keys its decisions changed since this was last asked, for their state to be
    /// copied on ([`Local::copies`]).
    pub fn take_changed(&mut self) -> Vec<Shared> {
        std::mem::take(&mut self.changed)
    }

    /// Gathers `key`, just changed for the first time since its last copy.
    fn gather(&mut self, key: &[u8]) {
        if let Some(handle) = self.set.get(key) {
            self.changed.push(Arc::clone(&handle.0));
        }
    }

    /// How many it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether it holds none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Lets go of the keys forgotten or evicted: taken now and then, as a worker's handles
    /// to keys it never meets again hold them.
    pub fn prune(&mut self) {
        self.set.retain(|handle| !handle.0.closed());
    }

    fn forget(&mut self, key: &[u8]) {
        self.set.remove(key);
    }

    /// `act` on the key of `key` under `contract`: from its handle, found by one lookup; or,
    /// with none for it, found or made in `local` and its handle kept. A key closed since is
    /// for `act` to find: it reads the key's word anyway.
    fn with_key<R>(
        &mut self,
        local: &Local<S>,
        key: &[u8],
        contract: Contract,
        now: u64,
        act: impl Fn(&Key) -> R,
    ) -> R {
        if let Some(handle) = self.set.get(key)
            && handle.0.contract == contract
        {
            return act(&handle.0);
        }
        let made = local.find_or_make(key, contract, now);
        let done = act(&made);
        self.set.replace(Handle(made));
        done
    }
}

/// `change` applied to `key` at `now` through `handles` until it lands on a live key, and
/// the key gathered if that was its first change since its last copy.
fn charge_one<S: BuildHasher + Clone>(
    local: &Local<S>,
    handles: &mut Handles<S>,
    key: &[u8],
    contract: Contract,
    now: u64,
    change: fn(&Key, u64) -> bool,
) {
    loop {
        let (done, first) = handles.with_key(local, key, contract, now, |held| {
            let done = change(held, now);
            (done, done && held.mark(local.tracking))
        });
        if done {
            if first {
                handles.gather(key);
            }
            return;
        }
        handles.forget(key);
    }
}

fn read<T>(lock: &RwLock<T>) -> sync_guard::Read<'_, T> {
    sync_guard::read(lock)
}

fn write<T>(lock: &RwLock<T>) -> sync_guard::Write<'_, T> {
    sync_guard::write(lock)
}

/// The registry's locks, poisoning ignored: what they guard is a set of handles, whole
/// whatever a panicking holder was doing.
mod sync_guard {
    use super::sync::RwLock;

    #[cfg(not(loom))]
    pub(super) type Read<'a, T> = std::sync::RwLockReadGuard<'a, T>;
    #[cfg(not(loom))]
    pub(super) type Write<'a, T> = std::sync::RwLockWriteGuard<'a, T>;
    #[cfg(loom)]
    pub(super) type Read<'a, T> = loom::sync::RwLockReadGuard<'a, T>;
    #[cfg(loom)]
    pub(super) type Write<'a, T> = loom::sync::RwLockWriteGuard<'a, T>;

    pub(super) fn read<T>(lock: &RwLock<T>) -> Read<'_, T> {
        lock.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn write<T>(lock: &RwLock<T>) -> Write<'_, T> {
        lock.write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
