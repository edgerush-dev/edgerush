//! Local mode (06 §4): each data-plane process limits on what it sees alone, with no
//! rate-limiting service and nothing shared with other processes.
//!
//! A key is one 64-bit atomic — its bucket's arrival time as GCRA keeps it (governor's way),
//! or its fixed window's number and count — in a registry the process's workers share. A worker keeps its own handles to the keys it
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
//! 2^63; a window's number modulo 2^23 above its count in 40 bits. The top bit of the word
//! marks a key forgotten.

mod key_set;
mod sync;

#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(test)]
mod tests;

pub use crate::service::Contract;

use key_set::{KeySet, SLOT_BYTES};
use std::hash::BuildHasher;
use std::num::NonZeroU64;
use sync::{Arc, AtomicU64, Ordering, RwLock};

/// The tables a worker's handles are split into, by bits of each key's hash: each doubles
/// on its own, and [`Handles::prune_next`] reads one at a time.
pub const HANDLE_PARTS: usize = 64;

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
/// A window's count: the word's low bits, saturating there.
const COUNT_BITS: u32 = 40;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
/// A window's number, modulo the bits between the count and the top bit: enough that a word
/// is never mistaken for one of a window as many later, which a key unused for longer than
/// a window is never read as.
const NUMBER_MASK: u64 = (1 << (63 - COUNT_BITS)) - 1;
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

/// A fixed window's contract: at most `limit` requests in each window of a key, its
/// windows shifted by an offset from its hash so that keys' boundaries spread (06 §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    /// Requests a window.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
}

/// What a key is held to: a bucket or a fixed window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// A bucket, by GCRA.
    Bucket(Contract),
    /// A fixed window.
    FixedWindow(Window),
}

impl From<Contract> for Limit {
    fn from(contract: Contract) -> Self {
        Self::Bucket(contract)
    }
}

impl From<Window> for Limit {
    fn from(window: Window) -> Self {
        Self::FixedWindow(window)
    }
}

/// A fixed window's arithmetic: times to windows' numbers, and a number and count to a
/// word and back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    limit: u64,
    window: u64,
    /// How far the key's windows are shifted: below the window.
    offset: u64,
}

impl Span {
    fn new(window: Window, hash: u64) -> Self {
        Self {
            limit: window.limit.get(),
            window: window.window.get(),
            offset: hash % window.window.get(),
        }
    }

    /// The number of the window `at` is in.
    fn number(&self, at: u64) -> u64 {
        at.saturating_add(self.offset) / self.window
    }

    /// The word holding `count` in window `number`.
    fn word(number: u64, count: u64) -> u64 {
        ((number & NUMBER_MASK) << COUNT_BITS) | count.min(COUNT_MASK)
    }

    /// What `word` counted in window `number`: none if it holds another window.
    fn count(word: u64, number: u64) -> u64 {
        if (word >> COUNT_BITS) & NUMBER_MASK == number & NUMBER_MASK {
            word & COUNT_MASK
        } else {
            0
        }
    }

    /// Nanoseconds from `at` until its window ends.
    fn left(&self, at: u64) -> u64 {
        self.window - at.saturating_add(self.offset) % self.window
    }
}

/// How a key's word is read.
#[derive(Debug)]
enum Held {
    Bucket(Rate),
    Window(Span),
}

/// One key: its bucket's arrival time or its window's count, and when it was last asked
/// for.
#[derive(Debug)]
pub struct Key {
    key: Box<[u8]>,
    contract: Limit,
    held: Held,
    /// The arrival time in units, or the window's number and count; the top bit set once
    /// the key is closed.
    word: AtomicU64,
    /// The latest time a request was charged under it, moved before the charge lands.
    last: AtomicU64,
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
    fn new(key: Box<[u8]>, contract: Limit, hash: u64, now: u64) -> Self {
        let (held, word) = match contract {
            Limit::Bucket(contract) => {
                let rate = Rate::new(contract);
                (Held::Bucket(rate), rate.units(now))
            }
            Limit::FixedWindow(window) => {
                let span = Span::new(window, hash);
                (Held::Window(span), Span::word(span.number(now), 0))
            }
        };
        Self {
            key,
            contract,
            held,
            word: AtomicU64::new(word),
            last: AtomicU64::new(now),
        }
    }

    fn closed(&self) -> bool {
        self.word.load(Ordering::Acquire) & CLOSED != 0
    }

    /// How long a key goes unused before it is fresh whatever its word says: its bucket's
    /// refill, or its window.
    fn refill(&self) -> u64 {
        match &self.held {
            Held::Bucket(rate) => rate.refill,
            Held::Window(span) => span.window,
        }
    }

    /// Whether, at `now`, the key went unused for longer than its bucket takes to refill or
    /// than its window: fresh, whatever its word says once its clock or its windows' numbers
    /// have come round. Sound because a charge moves the last use before it lands
    /// ([`charge_from`]): a word read is never older than the last use read after it.
    ///
    /// [`charge_from`]: Self::charge_from
    fn idle(&self, now: u64) -> bool {
        now.saturating_sub(self.last.load(Ordering::Relaxed)) > self.refill()
    }

    /// The arrival time as of `now` (units `now_units`) from `word`: as it is, unless the key
    /// is idle, when it is full.
    fn arrival(&self, word: u64, now: u64, now_units: u64) -> u64 {
        if self.idle(now) { now_units } else { word }
    }

    /// Whether a request at `now` fits: as of the key's last charge if `now` is behind it,
    /// as [`word_to_charge`](Self::word_to_charge) decides.
    fn look(&self, now: u64) -> Look {
        let word = self.word.load(Ordering::Acquire);
        if word & CLOSED != 0 {
            return Look::Closed;
        }
        let last = self.last.load(Ordering::Relaxed);
        match &self.held {
            Held::Bucket(rate) => {
                if now.saturating_sub(last) > rate.refill {
                    let now_units = rate.units(now);
                    return Self::fits(rate, word, now_units, now_units);
                }
                Self::fits(rate, word, word, rate.units(now.max(last)))
            }
            Held::Window(span) => Self::window_look(span, word, last, now),
        }
    }

    /// The same for a window, its word and last use read as `word` and `last`.
    fn window_look(span: &Span, word: u64, last: u64, now: u64) -> Look {
        let at = now.max(last);
        let count = if now.saturating_sub(last) > span.window {
            0
        } else {
            Span::count(word, span.number(at))
        };
        Self::window_fits(span, word, count, at)
    }

    /// Whether a request at `now_units` fits, the bucket's arrival time `arrival` as of then.
    fn fits(rate: &Rate, word: u64, arrival: u64, now_units: u64) -> Look {
        let ahead = Rate::ahead(arrival, now_units);
        if ahead > rate.tolerance {
            return Look::Full {
                wait: rate.nanos(ahead - rate.tolerance),
            };
        }
        let from = if ahead > 0 { arrival } else { now_units };
        Look::Fits { word, from }
    }

    /// Whether a request at `at` fits a window that has counted `count`: `from` is the count
    /// it charges from.
    fn window_fits(span: &Span, word: u64, count: u64, at: u64) -> Look {
        if count >= span.limit {
            return Look::Full {
                wait: span.left(at),
            };
        }
        Look::Fits { word, from: count }
    }

    /// The word a charge at `now` (units `now_units`) is to replace, and the time in units it
    /// is decided at; `None` if the key was closed. An idle key's word is first made to say
    /// what the key is, full at `now`, before a charge moves its last use: a word from
    /// before the idle spell, its clock perhaps come round, is never read beside a recent
    /// last use, which would take it as it is.
    ///
    /// A request whose clock is behind the key's last charge, as one worker's can be behind
    /// another's, is decided as of that charge (#108): taken at its own time, it would see
    /// the charge as still to come, the bucket emptier than it is by the refill between.
    fn word_to_charge(&self, rate: &Rate, now: u64, now_units: u64) -> Option<(u64, u64)> {
        loop {
            let word = self.word.load(Ordering::Acquire);
            if word & CLOSED != 0 {
                return None;
            }
            let last = self.last.load(Ordering::Relaxed);
            if last > now {
                return Some((word, rate.units(last)));
            }
            if now - last <= rate.refill {
                return Some((word, now_units));
            }
            if self
                .word
                .compare_exchange(word, now_units, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some((now_units, now_units));
            }
        }
    }

    /// A window's word a charge at `now` is to replace, the time it is decided at, and what
    /// the window has counted then; `None` if the key was closed. As a bucket's
    /// ([`word_to_charge`](Self::word_to_charge)): decided as of the last charge if `now` is
    /// behind it, and an idle key's word first made a fresh window's before a charge moves
    /// its last use, so that no worker reads a word from before the idle spell, its number
    /// perhaps come round, beside a recent last use.
    fn window_to_charge(&self, span: &Span, now: u64) -> Option<(u64, u64, u64)> {
        loop {
            let word = self.word.load(Ordering::Acquire);
            if word & CLOSED != 0 {
                return None;
            }
            let last = self.last.load(Ordering::Relaxed);
            let at = now.max(last);
            if now.saturating_sub(last) <= span.window {
                return Some((word, at, Span::count(word, span.number(at))));
            }
            let fresh = Span::word(span.number(now), 0);
            if self
                .word
                .compare_exchange(word, fresh, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some((fresh, now, 0));
            }
        }
    }

    /// Replaces `word` with `charged`, a charge at `now`, false if the word changed. The last
    /// use moves first, so that a worker reading the charge reads a last use at least as
    /// recent, and never takes the key for idle and charges over it.
    fn charge_from(&self, word: u64, charged: u64, now: u64) -> bool {
        self.last.fetch_max(now, Ordering::Relaxed);
        self.word
            .compare_exchange(word, charged, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// One request at `now`, admitted if it fits. Inlined: called, it costs a decision under
    /// one key a tenth more instructions (Callgrind).
    #[inline]
    fn take(&self, now: u64) -> Look {
        match &self.held {
            Held::Bucket(rate) => self.bucket_take(rate, now),
            Held::Window(span) => self.window_take(span, now),
        }
    }

    #[inline]
    fn bucket_take(&self, rate: &Rate, now: u64) -> Look {
        let now_units = rate.units(now);
        loop {
            let Some((word, at)) = self.word_to_charge(rate, now, now_units) else {
                return Look::Closed;
            };
            match Self::fits(rate, word, word, at) {
                Look::Fits { word, from } => {
                    if self.charge_from(word, from.wrapping_add(UNITS) & MASK, now) {
                        return Look::Fits { word, from };
                    }
                }
                other => return other,
            }
        }
    }

    fn window_take(&self, span: &Span, now: u64) -> Look {
        loop {
            let Some((word, at, count)) = self.window_to_charge(span, now) else {
                return Look::Closed;
            };
            match Self::window_fits(span, word, count, at) {
                Look::Fits { word, from } => {
                    let charged = Span::word(span.number(at), from + 1);
                    if self.charge_from(word, charged, now) {
                        return Look::Fits { word, from };
                    }
                }
                other => return other,
            }
        }
    }

    /// One request at `now` charged, whether it fits or not: false if the key was closed.
    fn charge(&self, now: u64) -> bool {
        match &self.held {
            Held::Bucket(rate) => {
                let now_units = rate.units(now);
                loop {
                    let Some((word, at)) = self.word_to_charge(rate, now, now_units) else {
                        return false;
                    };
                    let from = if Rate::ahead(word, at) > 0 { word } else { at };
                    if self.charge_from(word, from.wrapping_add(UNITS) & MASK, now) {
                        return true;
                    }
                }
            }
            Held::Window(span) => self.window_charge(span, now),
        }
    }

    fn window_charge(&self, span: &Span, now: u64) -> bool {
        loop {
            let Some((word, at, count)) = self.window_to_charge(span, now) else {
                return false;
            };
            if self.charge_from(word, Span::word(span.number(at), count + 1), now) {
                return true;
            }
        }
    }

    /// Closes it if, at `now`, its bucket is full or its window has counted none, and
    /// nothing used it for `quiet`: true if it did.
    fn close_if_forgettable(&self, now: u64, quiet: u64) -> bool {
        let word = self.word.load(Ordering::Acquire);
        if word & CLOSED != 0 {
            return true;
        }
        if now.saturating_sub(self.last.load(Ordering::Relaxed)) < quiet {
            return false;
        }
        if self.full_in_from(word, now) > 0 {
            return false;
        }
        // Lost to a request just made, the key stays: it is in use.
        self.word
            .compare_exchange(word, word | CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Nanoseconds from `now` until its bucket is full or its window has counted none.
    fn full_in(&self, now: u64) -> u64 {
        self.full_in_from(self.word.load(Ordering::Acquire) & MASK, now)
    }

    /// The same, its word read as `word`.
    fn full_in_from(&self, word: u64, now: u64) -> u64 {
        match &self.held {
            Held::Bucket(rate) => {
                let now_units = rate.units(now);
                rate.nanos(Rate::ahead(self.arrival(word, now, now_units), now_units))
            }
            Held::Window(span) => {
                let at = now.max(self.last.load(Ordering::Relaxed));
                if self.idle(now) || Span::count(word, span.number(at)) == 0 {
                    0
                } else {
                    span.left(at)
                }
            }
        }
    }

    fn close(&self) {
        self.word.fetch_or(CLOSED, Ordering::AcqRel);
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
    shards: Box<[RwLock<KeySet<S, 1>>]>,
    hasher: S,
    /// Workers holding handles to the keys, which a key's bytes count.
    workers: u64,
    holding: AtomicU64,
    capacity: AtomicU64,
    evicted: AtomicU64,
}

/// The bytes a key of `key` is counted as: the key itself, its name, and a slot for its
/// handle in the registry and in each of `workers`' sets, a byte of a table's control with
/// each. An estimate, not what the allocator takes.
#[must_use]
pub fn key_bytes(key: &[u8], workers: u64) -> u64 {
    // An `Arc`'s two counts beside the key.
    let entry = 2 * size_of::<usize>() + size_of::<Key>() + key.len();
    entry as u64 + SLOT_BYTES * (1 + workers)
}

impl<S: BuildHasher + Clone> Local<S> {
    /// No keys yet, hashed by `hasher`, shared by `workers` workers; no cap until one is set.
    #[must_use]
    pub fn new(hasher: S, workers: u64) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| RwLock::new(KeySet::with_hasher(hasher.clone())))
                .collect(),
            hasher,
            workers: workers.max(1),
            holding: AtomicU64::new(0),
            capacity: AtomicU64::new(u64::MAX),
            evicted: AtomicU64::new(0),
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
        let hash = self.hasher.hash_one(key);
        read(self.shard(hash)).get_hashed(hash, key).is_some()
    }

    /// The shard of a key whose name's hash is `hash`: chosen by bits of the hash its set
    /// does not place by, so that the keys of one shard spread over its set.
    fn shard(&self, hash: u64) -> &RwLock<KeySet<S, 1>> {
        let at = usize::try_from(hash >> 32).unwrap_or(0) % self.shards.len();
        // `at` is below the length.
        &self.shards[at]
    }

    /// The live key of `key` under `contract`, made at `now`, full, if there is none; one
    /// closed or under another contract is replaced.
    fn find_or_make(&self, key: &[u8], contract: Limit, now: u64) -> Arc<Key> {
        let hash = self.hasher.hash_one(key);
        let shard = self.shard(hash);
        if let Some(found) = read(shard).get_hashed(hash, key)
            && found.contract == contract
            && !found.closed()
        {
            return Arc::clone(found);
        }
        let mut set = write(shard);
        if let Some(found) = set.get_hashed(hash, key) {
            if found.contract == contract && !found.closed() {
                return Arc::clone(found);
            }
            found.close();
            self.holding
                .fetch_sub(key_bytes(key, self.workers), Ordering::Relaxed);
        }
        let made = Arc::new(Key::new(key.into(), contract, hash, now));
        set.replace_hashed(hash, Arc::clone(&made));
        self.holding
            .fetch_add(key_bytes(key, self.workers), Ordering::Relaxed);
        made
    }

    /// Decides a request at `now` under `keys`, each with its contract, through a worker's
    /// `handles`: admitted and charged under all of them, or refused and charged under
    /// none.
    pub fn decide<K: AsRef<[u8]>, L: core::marker::Copy + Into<Limit>>(
        &self,
        handles: &mut Handles<S>,
        keys: &[(K, L)],
        now: u64,
    ) -> Decision {
        self.decide_saying(handles, keys, now).0
    }

    /// The same, saying with a refusal which of `keys` had no room, bit `n` for the `n`th of
    /// the first 64: as the decision read them, which the keys need not bear out a moment
    /// later, refilled or forgotten. Zero with an admission.
    pub fn decide_saying<K: AsRef<[u8]>, L: core::marker::Copy + Into<Limit>>(
        &self,
        handles: &mut Handles<S>,
        keys: &[(K, L)],
        now: u64,
    ) -> (Decision, u64) {
        if let [(key, contract)] = keys {
            return loop {
                match handles.with_key(self, key.as_ref(), (*contract).into(), now, |held| {
                    held.take(now)
                }) {
                    Look::Fits { .. } => break (Decision::Admit, 0),
                    Look::Full { wait } => break (Decision::Refuse { position: 0, wait }, 1),
                    Look::Closed => handles.forget(key.as_ref()),
                }
            };
        }
        let mut longest: Option<(usize, u64)> = None;
        let mut refused = 0;
        for (position, (key, contract)) in keys.iter().enumerate() {
            let wait = loop {
                let contract = (*contract).into();
                match handles.with_key(self, key.as_ref(), contract, now, |held| held.look(now)) {
                    Look::Fits { .. } => break None,
                    Look::Full { wait } => break Some(wait),
                    Look::Closed => handles.forget(key.as_ref()),
                }
            };
            let Some(wait) = wait else {
                continue;
            };
            refused |= u32::try_from(position)
                .ok()
                .and_then(|at| 1_u64.checked_shl(at))
                .unwrap_or(0);
            if longest.is_none_or(|(_, most)| wait > most) {
                longest = Some((position, wait));
            }
        }
        if let Some((position, wait)) = longest {
            return (Decision::Refuse { position, wait }, refused);
        }
        charge_each(self, handles, keys, now);
        (Decision::Admit, 0)
    }

    /// Forgets the keys whose buckets are full at `now` and that no request used for
    /// `quiet`; then, past the cap, evicts the keys whose buckets will be full soonest, down
    /// to nine tenths of it. Taken every second or so, off the request path.
    pub fn sweep(&self, now: u64, quiet: u64) -> Swept {
        let mut swept = Swept::default();
        let mut freed = 0;
        for shard in &self.shards {
            write(shard).retain(|key| {
                let forget = key.close_if_forgettable(now, quiet);
                if forget {
                    swept.forgotten += 1;
                    freed += key_bytes(&key.key, self.workers);
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
                    .map(|key| (key.full_in(now), Arc::clone(key)))
                    .collect::<Vec<_>>()
            })
            .collect();
        keys.sort_unstable_by_key(|(full_in, _)| *full_in);
        let target = capacity - capacity / 10;
        for (_, key) in keys {
            if self.holding() <= target {
                break;
            }
            let hash = self.hasher.hash_one(&*key.key);
            let mut set = write(self.shard(hash));
            if set
                .get_hashed(hash, &key.key)
                .is_some_and(|held| Arc::ptr_eq(held, &key))
            {
                key.close();
                set.remove(&key.key);
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
    set: KeySet<S, HANDLE_PARTS>,
    /// The part [`prune_next`](Self::prune_next) reads next.
    next_part: usize,
}

impl<S: BuildHasher + Clone> Handles<S> {
    /// None yet.
    #[must_use]
    pub fn new(hasher: S) -> Self {
        Self {
            set: KeySet::with_hasher(hasher),
            next_part: 0,
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
        self.set.retain(|key| !key.closed());
    }

    /// The same for one of its [`HANDLE_PARTS`] parts, the next in turn: taken that many
    /// times as often as a whole prune would be, so that no one pass reads every handle —
    /// over a million handles, a whole pass reads them for tens of milliseconds.
    pub fn prune_next(&mut self) {
        self.set.retain_part(self.next_part, |key| !key.closed());
        self.next_part = (self.next_part + 1) % HANDLE_PARTS;
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
        contract: Limit,
        now: u64,
        act: impl FnOnce(&Key) -> R,
    ) -> R {
        let hash = self.set.hash(key);
        let held: &Key = match self.set.get_hashed(hash, key) {
            Some(found) if found.contract == contract => found,
            _ => self.made(local, key, hash, contract, now),
        };
        act(held)
    }

    /// The key of `key` found or made in `local`, its handle kept. Apart from
    /// [`with_key`](Self::with_key) so that `act` is called in one place: called in two, it
    /// is a call the compiler no longer inlines once it holds a window's path beside a
    /// bucket's, a sixth more instructions to a bucket's decision (Callgrind).
    #[cold]
    #[inline(never)]
    fn made(&mut self, local: &Local<S>, key: &[u8], hash: u64, contract: Limit, now: u64) -> &Key {
        let made = local.find_or_make(key, contract, now);
        self.set.replace_hashed(hash, made)
    }
}

/// Each of `keys` charged at `now` through `handles`, each charge made again on the key's
/// successor until it lands on a live key. All the keys in one function: the compiler no
/// longer inlines this once a key holds a window's path beside a bucket's, and called for
/// each key, it costs a decision under three keys an eighth more instructions (Callgrind).
fn charge_each<S: BuildHasher + Clone, K: AsRef<[u8]>, L: core::marker::Copy + Into<Limit>>(
    local: &Local<S>,
    handles: &mut Handles<S>,
    keys: &[(K, L)],
    now: u64,
) {
    for (key, contract) in keys {
        let (key, contract) = (key.as_ref(), (*contract).into());
        while !handles.with_key(local, key, contract, now, |held| held.charge(now)) {
            handles.forget(key);
        }
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
