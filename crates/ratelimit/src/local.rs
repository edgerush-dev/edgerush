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
//! What a limit's keys share — the limit and the arithmetic made from it — is kept once a
//! limit, in terms each of its keys points to, rather than copied into every key: two
//! thirds of a key's bytes otherwise, at the tens of millions of keys a process may hold.
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
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::num::NonZeroU64;
use sync::{Arc, AtomicU64, AtomicU128, Ordering, RwLock};

use crate::rate::{MASK, Rate, UNITS};
use crate::word::{local, local_parts};

/// The tables a worker's handles are split into, by bits of each key's hash: each doubles
/// on its own, and [`Handles::prune_next`] reads one at a time.
pub const HANDLE_PARTS: usize = 64;

/// The cycle a key's last charge is kept to in its word, 10 ms, as the asynchronous mode's:
/// how recently it was used, read by the idle rule and the sweep, which a cycle's grain
/// serves.
const CYCLE: u64 = 10_000_000;
/// The word's top bit: the key was forgotten or evicted, and a new one takes its place.
const CLOSED: u64 = 1 << 63;
/// A window's count: the word's low bits, saturating there.
const COUNT_BITS: u32 = 40;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
/// A window's number, modulo the bits between the count and the top bit: enough that a word
/// is never mistaken for one of a window as many later, which a key unused for longer than
/// a window is never read as.
const NUMBER_MASK: u64 = (1 << (63 - COUNT_BITS)) - 1;
/// How many shards the registry has: a worker meeting a key it holds no handle to takes one
/// shard's lock, and the sweep takes each in turn. So many that each holds few of the
/// process's keys: a shard's table grows by moving all its keys under its lock, which the
/// worker making a key waits out, with any other wanting the shard — at 30 million keys,
/// some 7,300 a shard. A few under loom, which tracks each lock.
const SHARDS: usize = if cfg!(loom) { 2 } else { 4_096 };

/// A fixed window's contract: at most `limit` requests in each window of a key, its
/// windows shifted by an offset from its hash so that keys' boundaries spread (06 §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// Requests a window.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
}

/// What a key is held to: a bucket or a fixed window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
/// word and back. A limit's terms hold it unshifted; a key's own is shifted by its offset
/// ([`Span::at`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    limit: u64,
    window: u64,
    /// How far the key's windows are shifted: below the window.
    offset: u64,
}

impl Span {
    fn new(window: Window) -> Self {
        Self {
            limit: window.limit.get(),
            window: window.window.get(),
            offset: 0,
        }
    }

    /// How far a key's windows are shifted, from its name's hash.
    fn offset_of(&self, hash: u64) -> u64 {
        hash % self.window
    }

    /// The same, shifted by `offset`: a key's.
    fn at(&self, offset: u64) -> Self {
        Self { offset, ..*self }
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

/// What every key under one limit shares: the limit, and the arithmetic its words are read
/// by. Made once a limit and pointed to by each of its keys.
#[derive(Debug)]
struct Terms {
    limit: Limit,
    held: Held,
}

impl Terms {
    fn new(limit: Limit) -> Self {
        let held = match limit {
            Limit::Bucket(contract) => Held::Bucket(Rate::new(contract)),
            Limit::FixedWindow(window) => Held::Window(Span::new(window)),
        };
        Self { limit, held }
    }
}

/// One key: its bucket's arrival time or its window's count, and the cycle it was last
/// charged in, in one word.
#[derive(Debug)]
pub struct Key {
    key: Box<[u8]>,
    terms: Arc<Terms>,
    /// Its state in one word, its low half laid out as the asynchronous mode's entries'
    /// (`word::local`): below, the arrival time in units, or the window's number and count,
    /// and the closed bit; above, the cycle it was last charged in ([`CYCLE`]). A charge is
    /// one compare-and-swap.
    state: AtomicU128,
    /// How far its windows are shifted, under a window: from its name's hash, so that keys'
    /// boundaries spread (06 §3). Nothing under a bucket.
    offset: u64,
}

/// A key's state as read: its word, the low half, and the cycle it was last charged in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    word: u64,
    cycle: u64,
}

impl Seen {
    fn of(state: u128) -> Self {
        let (word, cycle) = local_parts(state);
        Self { word, cycle }
    }

    fn pack(self) -> u128 {
        local(self.word, self.cycle)
    }
}

/// What a look at a key found.
enum Look {
    /// A request fits, the state as read and the arrival time or count it would charge from.
    Fits { seen: Seen, from: u64 },
    /// No room: the time until there is.
    Full { wait: u64 },
    /// The key was closed: its successor in the registry is to be used.
    Closed,
}

impl Key {
    fn new(key: Box<[u8]>, terms: Arc<Terms>, hash: u64, now: u64) -> Self {
        let (word, offset) = match &terms.held {
            Held::Bucket(rate) => (rate.units(now), 0),
            Held::Window(span) => {
                let span = span.at(span.offset_of(hash));
                (Span::word(span.number(now), 0), span.offset)
            }
        };
        let seen = Seen {
            word,
            cycle: Self::cycle_of(now),
        };
        Self {
            key,
            terms,
            state: AtomicU128::new(seen.pack()),
            offset,
        }
    }

    /// The limit it is held to.
    fn limit(&self) -> Limit {
        self.terms.limit
    }

    fn read(&self) -> Seen {
        Seen::of(self.state.load(Ordering::Acquire))
    }

    fn closed(&self) -> bool {
        self.read().word & CLOSED != 0
    }

    /// How long a key goes unused before it is fresh whatever its word says: its bucket's
    /// refill, or its window.
    fn refill(&self) -> u64 {
        match &self.terms.held {
            Held::Bucket(rate) => rate.refill,
            Held::Window(span) => span.window,
        }
    }

    /// `now`'s cycle number.
    fn cycle_of(now: u64) -> u64 {
        now / CYCLE
    }

    /// Whole cycles from `cycle` to `now`'s: none if it is `now`'s or a later one, a clock a
    /// moment behind another's.
    fn since(cycle: u64, now: u64) -> u64 {
        Self::cycle_of(now).saturating_sub(cycle)
    }

    /// Nanoseconds that surely passed since a key `seen` at `now` was last charged: its
    /// charge as late in its cycle as it could have been.
    fn unused_for(seen: Seen, now: u64) -> u64 {
        Self::since(seen.cycle, now).saturating_sub(1) * CYCLE
    }

    /// Whether a key `seen` at `now` went unused for longer than its bucket takes to refill
    /// or than its window: fresh, whatever its word says once its clock or its windows'
    /// numbers have come round. Read in the same word as the charge, so never older than it.
    fn idle(&self, seen: Seen, now: u64) -> bool {
        Self::unused_for(seen, now) > self.refill()
    }

    /// The arrival time as of `now` (units `now_units`) from `seen`: as it is, unless the key
    /// is idle, when it is full.
    fn arrival(&self, seen: Seen, now: u64, now_units: u64) -> u64 {
        if self.idle(seen, now) {
            now_units
        } else {
            seen.word
        }
    }

    /// Whether a request at `now` fits, nothing written, as a charge would decide it.
    // Always, as `take`: called, it costs a decision under two keys some 6% more
    // instructions (Callgrind).
    #[inline(always)]
    fn look(&self, now: u64) -> Look {
        let seen = self.read();
        if seen.word & CLOSED != 0 {
            return Look::Closed;
        }
        match &self.terms.held {
            Held::Bucket(rate) => {
                let now_units = rate.units(now);
                Self::fits(rate, seen, self.arrival(seen, now, now_units), now_units)
            }
            Held::Window(span) => {
                let span = span.at(self.offset);
                let (number, count) = Self::window_at(&span, seen.word, self.idle(seen, now), now);
                Self::window_fits(&span, seen, count, number, now)
            }
        }
    }

    /// The window a request at `now` is decided in, and what it has counted then: `now`'s,
    /// none if the key is `idle`; or, its word holding a later window, that one: a clock
    /// lagging a charge made across one boundary or more, decided in the charge's window, as
    /// the word's numbers say, so that it never writes an earlier window over a later. A word
    /// is told ahead within half the numbers' range; one that far ahead is idle long before.
    fn window_at(span: &Span, word: u64, idle: bool, now: u64) -> (u64, u64) {
        let number = span.number(now);
        if idle {
            return (number, 0);
        }
        let ahead = ((word >> COUNT_BITS) & NUMBER_MASK).wrapping_sub(number) & NUMBER_MASK;
        if ahead > 0 && ahead <= NUMBER_MASK / 2 {
            return (number + ahead, word & COUNT_MASK);
        }
        (number, Span::count(word, number))
    }

    /// Whether a request at `now_units` fits, the bucket's arrival time `arrival` as of then.
    fn fits(rate: &Rate, seen: Seen, arrival: u64, now_units: u64) -> Look {
        let ahead = Rate::ahead(arrival, now_units);
        if ahead > rate.tolerance {
            return Look::Full {
                wait: rate.nanos(ahead - rate.tolerance),
            };
        }
        let from = if ahead > 0 { arrival } else { now_units };
        Look::Fits { seen, from }
    }

    /// Whether a request at `now` fits window `number`, which has counted `count`: `from` is
    /// the count it charges from. A refusal waits, from `now`, until that window ends.
    fn window_fits(span: &Span, seen: Seen, count: u64, number: u64, now: u64) -> Look {
        if count >= span.limit {
            return Look::Full {
                wait: Self::window_left(span, number, now),
            };
        }
        Look::Fits { seen, from: count }
    }

    /// Nanoseconds from `now` until window `number`, `now`'s or a later one, ends.
    fn window_left(span: &Span, number: u64, now: u64) -> u64 {
        span.left(now) + (number - span.number(now)) * span.window
    }

    /// Replaces `seen` with `word` charged at `now`, and the cycle with `now`'s, unless a
    /// clock ahead of this one's charged in a later cycle already: one compare-and-swap,
    /// false if the state changed. A request is decided at its own clock, behind the key's
    /// last charge or not, as one worker's can be behind another's: the slack
    /// ([`SLACK`](crate::SLACK)) keeps a lag of up to it from refusing a request within the
    /// limit.
    fn charge_from(&self, seen: Seen, word: u64, now: u64) -> bool {
        let charged = Seen {
            word,
            cycle: seen.cycle.max(Self::cycle_of(now)),
        };
        self.state
            .compare_exchange(
                seen.pack(),
                charged.pack(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// One request at `now`, admitted if it fits.
    // Always: called, it costs a decision under one key a tenth more instructions
    // (Callgrind), and with one 128-bit word the compiler stopped inlining it on a hint.
    #[inline(always)]
    fn take(&self, now: u64) -> Look {
        match &self.terms.held {
            Held::Bucket(rate) => self.bucket_take(rate, now),
            Held::Window(span) => self.window_take(&span.at(self.offset), now),
        }
    }

    #[inline(always)]
    fn bucket_take(&self, rate: &Rate, now: u64) -> Look {
        let now_units = rate.units(now);
        loop {
            let seen = self.read();
            if seen.word & CLOSED != 0 {
                return Look::Closed;
            }
            let arrival = self.arrival(seen, now, now_units);
            match Self::fits(rate, seen, arrival, now_units) {
                Look::Fits { seen, from } => {
                    if self.charge_from(seen, from.wrapping_add(UNITS) & MASK, now) {
                        return Look::Fits { seen, from };
                    }
                }
                other => return other,
            }
        }
    }

    // Always, as `bucket_take`: called, it costs a window's decision a quarter more
    // instructions (Callgrind).
    #[inline(always)]
    fn window_take(&self, span: &Span, now: u64) -> Look {
        loop {
            let seen = self.read();
            if seen.word & CLOSED != 0 {
                return Look::Closed;
            }
            let (number, count) = Self::window_at(span, seen.word, self.idle(seen, now), now);
            match Self::window_fits(span, seen, count, number, now) {
                Look::Fits { seen, from } => {
                    if self.charge_from(seen, Span::word(number, from + 1), now) {
                        return Look::Fits { seen, from };
                    }
                }
                other => return other,
            }
        }
    }

    /// One request at `now` charged, whether it fits or not: false if the key was closed.
    fn charge(&self, now: u64) -> bool {
        loop {
            let seen = self.read();
            if seen.word & CLOSED != 0 {
                return false;
            }
            let word = match &self.terms.held {
                Held::Bucket(rate) => {
                    let now_units = rate.units(now);
                    let arrival = self.arrival(seen, now, now_units);
                    let from = if Rate::ahead(arrival, now_units) > 0 {
                        arrival
                    } else {
                        now_units
                    };
                    from.wrapping_add(UNITS) & MASK
                }
                Held::Window(span) => {
                    let span = span.at(self.offset);
                    let idle = self.idle(seen, now);
                    let (number, count) = Self::window_at(&span, seen.word, idle, now);
                    Span::word(number, count + 1)
                }
            };
            if self.charge_from(seen, word, now) {
                return true;
            }
        }
    }

    /// Closes it if, at `now`, its bucket is full or its window has counted none, and
    /// nothing used it for `quiet`: true if it did.
    fn close_if_forgettable(&self, now: u64, quiet: u64) -> bool {
        let seen = self.read();
        if seen.word & CLOSED != 0 {
            return true;
        }
        if Self::unused_for(seen, now) < quiet {
            return false;
        }
        if self.full_in_from(seen, now) > 0 {
            return false;
        }
        let closed = Seen {
            word: seen.word | CLOSED,
            ..seen
        };
        // Lost to a request just made, the key stays: it is in use.
        self.state
            .compare_exchange(
                seen.pack(),
                closed.pack(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Nanoseconds from `now` until its bucket is full or its window has counted none.
    fn full_in(&self, now: u64) -> u64 {
        let seen = self.read();
        self.full_in_from(
            Seen {
                word: seen.word & MASK,
                ..seen
            },
            now,
        )
    }

    /// The same, its state read as `seen`.
    fn full_in_from(&self, seen: Seen, now: u64) -> u64 {
        match &self.terms.held {
            Held::Bucket(rate) => {
                let now_units = rate.units(now);
                rate.nanos(Rate::ahead(self.arrival(seen, now, now_units), now_units))
            }
            Held::Window(span) => {
                let span = span.at(self.offset);
                match Self::window_at(&span, seen.word, self.idle(seen, now), now) {
                    (_, 0) => 0,
                    (number, _) => Self::window_left(&span, number, now),
                }
            }
        }
    }

    fn close(&self) {
        self.state.fetch_or(u128::from(CLOSED), Ordering::AcqRel);
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
    /// Each limit's terms, which its keys point to: read when a key is made, and let go of
    /// by the sweep once no key holds them.
    terms: RwLock<HashMap<Limit, Arc<Terms>>>,
}

/// The bytes a key of `key` is counted as: the key itself, its name, and a slot for its
/// handle in the registry and in each of `workers`' sets, a byte of a table's control with
/// each. An estimate, not what the allocator takes. Its limit's terms are not counted:
/// there is one a limit, however many keys it has.
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
            terms: RwLock::new(HashMap::new()),
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

    /// What keys under `limit` share: made the first time it is asked for.
    fn terms(&self, limit: Limit) -> Arc<Terms> {
        if let Some(terms) = read(&self.terms).get(&limit) {
            return Arc::clone(terms);
        }
        Arc::clone(
            write(&self.terms)
                .entry(limit)
                .or_insert_with(|| Arc::new(Terms::new(limit))),
        )
    }

    /// The live key of `key` under `contract`, made at `now`, full, if there is none; one
    /// closed or under another contract is replaced.
    fn find_or_make(&self, key: &[u8], contract: Limit, now: u64) -> Arc<Key> {
        let hash = self.hasher.hash_one(key);
        let shard = self.shard(hash);
        if let Some(found) = read(shard).get_hashed(hash, key)
            && found.limit() == contract
            && !found.closed()
        {
            return Arc::clone(found);
        }
        let mut set = write(shard);
        if let Some(found) = set.get_hashed(hash, key) {
            if found.limit() == contract && !found.closed() {
                return Arc::clone(found);
            }
            found.close();
            self.holding
                .fetch_sub(key_bytes(key, self.workers), Ordering::Relaxed);
        }
        let made = Arc::new(Key::new(key.into(), self.terms(contract), hash, now));
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
    /// to nine tenths of it. Lets go of the terms of limits no key holds any longer, as
    /// after a reload that drops a limit: a key forgotten holds its terms until the workers'
    /// handles let go of it, so they go at a later sweep. Taken every second or so, off the
    /// request path.
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
        // Held by the map alone: no key points to them, and none can be made meanwhile, as
        // making one takes the map's lock to find them.
        write(&self.terms).retain(|_, terms| Arc::strong_count(terms) > 1);
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
            Some(found) if found.limit() == contract => found,
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
