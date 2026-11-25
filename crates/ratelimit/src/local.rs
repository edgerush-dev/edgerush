//! Local mode (06 §4): each data-plane process limits on what it sees alone, with no
//! rate-limiting service and nothing shared with other processes.
//!
//! A key is one 64-bit atomic — its bucket's arrival time as GCRA keeps it (governor's way),
//! or its fixed window's number and count, or a rolling window's total beside a ring of its
//! slots' counts (`rolling`) — in a registry the process's workers share. A worker keeps its own handles to the keys it
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
mod rolling;
mod sync;

#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(test)]
mod tests;

pub use crate::service::Contract;
pub use rolling::MOST_SLOTS;

use crate::chunked::List;
use crate::cutoff::{self, Count, First, Lately, Places};
use edgerush_memory::{allocation, arc};
use key_set::KeySet;
use rolling::Ring;
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

/// A rolling window's contract: at most `limit` requests in any span of `window`, each
/// admission counted in one of `slots` slots a window, its time rounded down to its slot's
/// start, so that room comes back up to a slot early, never late (06 §3). At most
/// [`MOST_SLOTS`]; a key holds a count for each, whatever its limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rolling {
    /// Requests in any window.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
    /// The slots a window is counted in.
    pub slots: NonZeroU64,
}

/// What a key is held to: a bucket, a fixed window or a rolling window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Limit {
    /// A bucket, by GCRA.
    Bucket(Contract),
    /// A fixed window.
    FixedWindow(Window),
    /// A rolling window.
    RollingWindow(Rolling),
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

impl From<Rolling> for Limit {
    fn from(rolling: Rolling) -> Self {
        Self::RollingWindow(rolling)
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
    /// Boxed, so that a bucket's and a window's terms stay their few words.
    Rolling(Box<Ring>),
}

/// What every key under one limit shares: the limit, and the arithmetic its words are read
/// by. Made once a limit and pointed to by each of its keys — but a rolling window's, whose
/// ring is its key's own: each of its keys has terms of its own.
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
            Limit::RollingWindow(rolling) => Held::Rolling(Box::new(Ring::new(rolling))),
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

/// What a look at a key found, or a take did.
// Two words, so that it comes back from a rolling window's out-of-line take in registers:
// larger, it is returned through memory, and a bucket's and a fixed window's decisions,
// merged with it into one value, then keep theirs in memory too — a fixed window's
// decision some 5% more instructions (Callgrind, GitHub #400).
enum Look {
    /// A request fits.
    Fits,
    /// No room: the time until there is.
    Full { wait: u64 },
    /// The key was closed: its successor in the registry is to be used.
    Closed,
}

/// Whether a request fits a bucket or a fixed window, its word as read.
enum Room {
    /// It fits, charged from this arrival time or count.
    From(u64),
    /// No room: the time until there is.
    Full(u64),
}

impl Key {
    fn new(key: Box<[u8]>, terms: Arc<Terms>, hash: u64, now: u64) -> Self {
        let (state, offset) = match &terms.held {
            Held::Bucket(rate) => (Self::fresh(rate.units(now), now), 0),
            Held::Window(span) => {
                let span = span.at(span.offset_of(hash));
                (
                    Self::fresh(Span::word(span.number(now), 0), now),
                    span.offset,
                )
            }
            Held::Rolling(ring) => (ring.word_at(now), 0),
        };
        Self {
            key,
            terms,
            state: AtomicU128::new(state),
            offset,
        }
    }

    /// A bucket's or fixed window's state, its word `word`, charged last in `now`'s cycle.
    fn fresh(word: u64, now: u64) -> u128 {
        Seen {
            word,
            cycle: Self::cycle_of(now),
        }
        .pack()
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

    /// Whether a key `seen` at `now` went unused for longer than `refill`, the time its
    /// bucket takes to refill or its window: fresh, whatever its word says once its clock or
    /// its windows' numbers have come round. Read in the same word as the charge, so never
    /// older than it.
    // Handed the refill by the arm that holds it: read from the key's terms again, it is a
    // match on them in every charge's loop, a bucket's and a window's decisions some 2% more
    // instructions (Callgrind, GitHub #400).
    fn idle(seen: Seen, now: u64, refill: u64) -> bool {
        Self::unused_for(seen, now) > refill
    }

    /// The arrival time as of `now` (units `now_units`) from `seen`: as it is, unless the key
    /// is idle, when it is full.
    fn arrival(rate: &Rate, seen: Seen, now: u64, now_units: u64) -> u64 {
        if Self::idle(seen, now, rate.refill) {
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
        let room = match &self.terms.held {
            Held::Bucket(rate) => {
                let now_units = rate.units(now);
                Self::fits(rate, Self::arrival(rate, seen, now, now_units), now_units)
            }
            Held::Window(span) => {
                let span = span.at(self.offset);
                let (number, count) =
                    Self::window_at(&span, seen.word, Self::idle(seen, now, span.window), now);
                Self::window_fits(&span, count, number, now)
            }
            Held::Rolling(ring) => return ring.look(&self.state, now),
        };
        match room {
            Room::From(_) => Look::Fits,
            Room::Full(wait) => Look::Full { wait },
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
    fn fits(rate: &Rate, arrival: u64, now_units: u64) -> Room {
        let ahead = Rate::ahead(arrival, now_units);
        if ahead > rate.tolerance {
            return Room::Full(rate.nanos(ahead - rate.tolerance));
        }
        Room::From(if ahead > 0 { arrival } else { now_units })
    }

    /// Whether a request at `now` fits window `number`, which has counted `count`, the count
    /// it charges from. A refusal waits, from `now`, until that window ends.
    fn window_fits(span: &Span, count: u64, number: u64, now: u64) -> Room {
        if count >= span.limit {
            return Room::Full(Self::window_left(span, number, now));
        }
        Room::From(count)
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
            Held::Rolling(ring) => ring.take(&self.state, now),
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
            let arrival = Self::arrival(rate, seen, now, now_units);
            match Self::fits(rate, arrival, now_units) {
                Room::From(from) => {
                    if self.charge_from(seen, from.wrapping_add(UNITS) & MASK, now) {
                        return Look::Fits;
                    }
                }
                Room::Full(wait) => return Look::Full { wait },
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
            let (number, count) =
                Self::window_at(span, seen.word, Self::idle(seen, now, span.window), now);
            match Self::window_fits(span, count, number, now) {
                Room::From(from) => {
                    if self.charge_from(seen, Span::word(number, from + 1), now) {
                        return Look::Fits;
                    }
                }
                Room::Full(wait) => return Look::Full { wait },
            }
        }
    }

    /// One request at `now` charged, whether it fits or not: false if the key was closed.
    fn charge(&self, now: u64) -> bool {
        if let Held::Rolling(ring) = &self.terms.held {
            return ring.charge(&self.state, now);
        }
        loop {
            let seen = self.read();
            if seen.word & CLOSED != 0 {
                return false;
            }
            let word = match &self.terms.held {
                Held::Bucket(rate) => {
                    let now_units = rate.units(now);
                    let arrival = Self::arrival(rate, seen, now, now_units);
                    let from = if Rate::ahead(arrival, now_units) > 0 {
                        arrival
                    } else {
                        now_units
                    };
                    from.wrapping_add(UNITS) & MASK
                }
                Held::Window(span) => {
                    let span = span.at(self.offset);
                    let idle = Self::idle(seen, now, span.window);
                    let (number, count) = Self::window_at(&span, seen.word, idle, now);
                    Span::word(number, count + 1)
                }
                // Charged above, in its own way.
                Held::Rolling(_) => return false,
            };
            if self.charge_from(seen, word, now) {
                return true;
            }
        }
    }

    /// Closes it if, at `now`, its bucket is full or its window has counted none, and
    /// nothing used it for `quiet`: true if it did.
    fn close_if_forgettable(&self, now: u64, quiet: u64) -> bool {
        if let Held::Rolling(ring) = &self.terms.held {
            return ring.close_if_forgettable(&self.state, now, quiet);
        }
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
                rate.nanos(Rate::ahead(
                    Self::arrival(rate, seen, now, now_units),
                    now_units,
                ))
            }
            Held::Window(span) => {
                let span = span.at(self.offset);
                match Self::window_at(&span, seen.word, Self::idle(seen, now, span.window), now) {
                    (_, 0) => 0,
                    (number, _) => Self::window_left(&span, number, now),
                }
            }
            Held::Rolling(ring) => ring.full_in(&self.state, now),
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
    /// The keys' own bytes, as [`key_bytes`] counts them.
    holding: AtomicU64,
    /// The keys let go of — forgotten, evicted or replaced — while a worker's handles may
    /// still hold them: kept until none does, so that they count until they are freed
    /// ([`Self::release`]).
    gone: RwLock<Gone>,
    /// What those take ([`Gone::bytes`]), read without the lock.
    gone_bytes: AtomicU64,
    /// What the tables of the keys take, the registry's and the workers' handles', as glibc
    /// allocates them: added to as they grow, and given back as a worker's handles go.
    tables: Arc<AtomicU64>,
    capacity: AtomicU64,
    evicted: AtomicU64,
    /// Each limit's terms, which its keys point to: read when a key is made, and let go of
    /// by the sweep once no key holds them.
    terms: RwLock<HashMap<Limit, Arc<Terms>>>,
}

/// The keys a [`Local`] has let go of that handles may still hold.
#[derive(Debug)]
struct Gone {
    keys: List<Arc<Key>>,
    /// Their bytes, as [`key_bytes`] counts them.
    held: u64,
    /// The bytes of every key let go of, all told: with those held, the bytes of every key
    /// made.
    let_go: u64,
    /// The bytes of keys made lately, which eviction leaves room for.
    lately: Lately,
}

impl Gone {
    /// What they take: their own bytes, and the list's room.
    fn bytes(&self) -> u64 {
        self.held.saturating_add(self.keys.bytes())
    }
}

/// The bytes a key of `key` under `limit` is counted as taking in memory beside its handles:
/// the key itself and its name, in the blocks glibc allocates them in; a rolling window's
/// own terms and ring besides. Its handles are in the tables of the registry and of the
/// workers that met it, counted as the tables are allocated ([`Local::tables`]). Any other
/// limit's terms are not counted: there is one a limit, however many keys it has.
#[must_use]
pub fn key_bytes(key: &[u8], limit: Limit) -> u64 {
    let own = match limit {
        Limit::RollingWindow(rolling) => Ring::bytes(rolling),
        Limit::Bucket(_) | Limit::FixedWindow(_) => 0,
    };
    arc::<Key>() + allocation(key.len()) + own
}

impl<S: BuildHasher + Clone> Local<S> {
    /// No keys yet, hashed by `hasher`; no cap until one is set.
    #[must_use]
    pub fn new(hasher: S) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| RwLock::new(KeySet::with_hasher(hasher.clone())))
                .collect(),
            hasher,
            holding: AtomicU64::new(0),
            gone: RwLock::new(Gone {
                keys: List::new(),
                held: 0,
                let_go: 0,
                lately: Lately::default(),
            }),
            gone_bytes: AtomicU64::new(0),
            tables: Arc::new(AtomicU64::new(0)),
            capacity: AtomicU64::new(u64::MAX),
            evicted: AtomicU64::new(0),
            terms: RwLock::new(HashMap::new()),
        }
    }

    /// The bytes its keys may take, as [`holding`](Self::holding) counts them: past them, a
    /// sweep evicts.
    pub fn set_capacity(&self, bytes: u64) {
        self.capacity.store(bytes, Ordering::Relaxed);
    }

    /// The bytes its keys take in memory: each key's own ([`key_bytes`]), those of keys let go
    /// of until no handle holds them, and the tables holding them ([`tables`](Self::tables)).
    #[must_use]
    pub fn holding(&self) -> u64 {
        self.holding
            .load(Ordering::Relaxed)
            .saturating_add(self.gone_bytes.load(Ordering::Relaxed))
            .saturating_add(self.tables())
    }

    /// The room of its list of keys let go of, beside their own bytes: for the tests.
    #[cfg(test)]
    pub(crate) fn gone_room(&self) -> u64 {
        write(&self.gone).keys.bytes()
    }

    /// Its registry's tables alone, as [`tables`](Self::tables) counts them: a walk, for the
    /// tests.
    #[cfg(test)]
    pub(crate) fn registry_tables(&self) -> u64 {
        self.shards.iter().map(|shard| read(shard).bytes()).sum()
    }

    /// What the tables holding its keys take, as glibc allocates them: its registry's, and
    /// the handles' of every worker deciding from it. A table grows by doubling and never
    /// shrinks, so this falls only as a worker's handles go.
    #[must_use]
    pub fn tables(&self) -> u64 {
        self.tables.load(Ordering::Relaxed)
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
            let old = Arc::clone(found);
            self.let_go([old]);
        }
        let terms = match contract {
            Limit::RollingWindow(_) => Arc::new(Terms::new(contract)),
            Limit::Bucket(_) | Limit::FixedWindow(_) => self.terms(contract),
        };
        let made = Arc::new(Key::new(key.into(), terms, hash, now));
        let (grown, _) = set.replace_hashed(hash, Arc::clone(&made));
        if grown > 0 {
            self.tables.fetch_add(grown, Ordering::Relaxed);
        }
        self.holding
            .fetch_add(key_bytes(key, contract), Ordering::Relaxed);
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
                    Look::Fits => break (Decision::Admit, 0),
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
                    Look::Fits => break None,
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
    /// `quiet`; then, past the cap, [evicts](Self::evict). Lets go of the terms of limits no
    /// key holds any longer, as after a reload that drops a limit: a key forgotten holds its
    /// terms until the workers' handles let go of it, so they go at a later sweep. Taken
    /// every second or so, off the request path.
    pub fn sweep(&self, now: u64, quiet: u64) -> Swept {
        let mut swept = Swept::default();
        let mut forgotten = Vec::new();
        for shard in &self.shards {
            write(shard).retain(|key| {
                let forget = key.close_if_forgettable(now, quiet);
                if forget {
                    forgotten.push(Arc::clone(key));
                }
                !forget
            });
            swept.forgotten += forgotten.len() as u64;
            self.let_go(forgotten.drain(..));
        }
        self.release();
        // Held by the map alone: no key points to them, and none can be made meanwhile, as
        // making one takes the map's lock to find them.
        write(&self.terms).retain(|_, terms| Arc::strong_count(terms) > 1);
        swept.evicted = self.evict(now);
        swept
    }

    /// Frees the keys let go of that no handle holds any longer, and past the cap evicts the
    /// keys whose buckets will be full soonest at `now`, down to nine tenths of it; how many.
    /// Within the cap, reads a few counts and returns, so that it can be taken far more often
    /// than a sweep, keys being made between sweeps faster than the cap allows (GitHub #410).
    /// Off the request path.
    ///
    /// The cap is held by the keys kept and their tables, with room left for the keys made
    /// over the last [`HANDLES_PRUNED`](crate::HANDLES_PRUNED): what workers may go on
    /// holding of keys let go of. Those still held count in [`holding`](Self::holding) until
    /// they are freed but not here, as evicting more frees none of them sooner, and counted
    /// here each look would evict another tenth while they wait (#602). Eviction finds its
    /// keys in three passes, with nothing beside them but a table of 8 KB: one finding the
    /// span of when they will be full, one counting each key's bytes over that span, both
    /// over a sample of the shards when there are many keys, and the last, over every shard
    /// a shard at a time under its lock, evicting those full before the time the bytes needed
    /// end at.
    pub fn evict(&self, now: u64) -> u64 {
        self.release();
        let capacity = self.capacity.load(Ordering::Relaxed);
        let lately = {
            let mut gone = write(&self.gone);
            let made = self.holding.load(Ordering::Relaxed) + gone.let_go;
            gone.lately.at(now, made)
        };
        let holding = self.holding.load(Ordering::Relaxed) + self.tables() + lately;
        if holding <= capacity {
            return 0;
        }
        let target = capacity - capacity / 10;
        let stride = cutoff::stride(self.key_count());
        let mut span = Places::default();
        for shard in self.shards.iter().step_by(stride) {
            for key in read(shard).iter() {
                span.add(key.full_in(now));
            }
        }
        let mut count = Count::new(First::Least, span, stride);
        for shard in self.shards.iter().step_by(stride) {
            for key in read(shard).iter() {
                count.add(key.full_in(now), key_bytes(&key.key, key.limit()));
            }
        }
        let mut stop = count.stop(holding - target);
        let mut evicted = 0;
        let mut evicting = Vec::new();
        for shard in &self.shards {
            write(shard).retain(|key| {
                let evict = stop.takes(key.full_in(now), key_bytes(&key.key, key.limit()));
                if evict {
                    key.close();
                    evicting.push(Arc::clone(key));
                }
                !evict
            });
            evicted += evicting.len() as u64;
            self.let_go(evicting.drain(..));
            if stop.done() {
                break;
            }
        }
        self.evicted.fetch_add(evicted, Ordering::Relaxed);
        self.release();
        evicted
    }

    /// Takes `keys`, closed and out of the registry, off what it holds and onto what it has let
    /// go of, counted there until no handle holds them. No other lock is taken while the
    /// list's is held, so a worker may let go of a key under its shard's lock.
    fn let_go(&self, keys: impl IntoIterator<Item = Arc<Key>>) {
        let mut keys = keys.into_iter().peekable();
        if keys.peek().is_none() {
            return;
        }
        let mut gone = write(&self.gone);
        let mut moved = 0;
        for key in keys {
            moved += key_bytes(&key.key, key.limit());
            gone.keys.push(key);
        }
        gone.held += moved;
        gone.let_go += moved;
        // Counted as let go of before it stops counting as held: never under what is there.
        self.gone_bytes.store(gone.bytes(), Ordering::Relaxed);
        self.holding.fetch_sub(moved, Ordering::Relaxed);
    }

    /// Frees the keys let go of that only its list holds: none is in the registry, so no
    /// worker can take a handle to one again, and one so held stays so.
    fn release(&self) {
        if self.gone_bytes.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut gone = write(&self.gone);
        let mut freed = 0;
        gone.keys.retain(|key| {
            let held = Arc::strong_count(key) > 1;
            if !held {
                freed += key_bytes(&key.key, key.limit());
            }
            held
        });
        gone.held -= freed;
        self.gone_bytes.store(gone.bytes(), Ordering::Relaxed);
    }
}

/// A worker's handles to the keys it has met: found without a lock, by the key's name. What
/// its tables take is counted in the [`Local`] it decides from, one alone, from the first
/// key it meets until it goes.
#[derive(Debug)]
pub struct Handles<S> {
    set: KeySet<S, HANDLE_PARTS>,
    /// The part [`prune_next`](Self::prune_next) reads next.
    next_part: usize,
    /// Where its tables are counted, its [`Local`]'s tables, once it has met a key.
    counted_in: Option<Arc<AtomicU64>>,
}

impl<S> Drop for Handles<S> {
    /// Its tables go with it: no longer counted.
    fn drop(&mut self) {
        if let Some(tables) = &self.counted_in {
            tables.fetch_sub(self.set.bytes(), Ordering::Relaxed);
        }
    }
}

impl<S: BuildHasher + Clone> Handles<S> {
    /// None yet.
    #[must_use]
    pub fn new(hasher: S) -> Self {
        Self {
            set: KeySet::with_hasher(hasher),
            next_part: 0,
            counted_in: None,
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
        let (grown, held) = self.set.replace_hashed(hash, made);
        if grown > 0 {
            self.counted_in
                .get_or_insert_with(|| Arc::clone(&local.tables))
                .fetch_add(grown, Ordering::Relaxed);
        }
        held
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
