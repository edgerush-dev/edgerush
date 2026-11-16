//! The protocol of the rate-limiting service's asynchronous mode (06 §4): the service's pods,
//! on the cluster store, own the keys between them, and data planes — each EdgeRush process,
//! one [participant](pooled::Participant) for all its workers — decide every request from their
//! copy of the key, never talking to each other. Here are its terms and messages, the data
//! plane's side ([`pooled`]) and the owner's applying of a report ([`owner`]), all with no
//! I/O: whatever carries the messages — the cluster simulator, or gRPC between processes — is
//! the caller's.
//!
//! What the two sides do:
//!
//! - A data plane decides a request from its copy: the owner's bucket as last heard,
//!   charged with its own hits the owner has not been seen to apply, and bounded each cycle
//!   to its share of what the copy has left and the cycle refills — the share divided among
//!   the data planes the owner has seen admit the key lately, at least one request, and one
//!   more carried if it left one unspent the cycle before. The cycle after such a share,
//!   while the others' hits under it are still on their way, gets its share of the refill
//!   only, and keeps what it left unspent.
//! - Every cycle it sends each owner, in one batch over all the keys that owner holds, the
//!   hits it admitted, numbered, with how long ago the earliest of them was; or, with none to
//!   send while the key is in use and quiet keys polled, a poll. Each is answered with the
//!   bucket as it stands, which the copy takes as of when the batch was made: the batch's trip
//!   cancels out the lateness of the owner's charge, with no clock shared. A batch not seen
//!   applied is sent again after four cycles; one older than a bucket's refill time is sent
//!   emptied.
//! - A request under several limits is admitted under all of them or none, and charged
//!   to all or none, every one decided from the data plane's copy: a key it has not heard
//!   of from a full bucket. It asks no one.
//! - The owner applies hits in order, once each, as of when the earliest of them was
//!   admitted, charging what does not fit as debt while it would be paid back within one
//!   bucket's refill time. The store copies every change to the key's replicas, one of which
//!   takes over should the owner go.
//!
//! Which pod owns which key is the store's table, which routes a participant
//! ([`pooled::Routing`]).

pub mod keyed;
pub mod owner;
pub mod pooled;

pub use keyed::Keyed;

use crate::reference::TokenBucket;
use std::hash::{BuildHasherDefault, Hasher};
use std::num::NonZeroU64;

/// How many cycles a data plane waits to see a batch applied before sending it again.
pub const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles before the latest admission the owner knows of a data plane counts
/// as admitting the key: 300 ms at a 10 ms cycle, longer than TCP takes to send a lost
/// segment again. A data plane whose batches are held up behind a lost one must not drop out
/// of the count, or every other one's share grows while it is stalled: four cycles let bursts
/// through at 1.44 times the contract with one packet in ten lost (simulator, TCP-like loss).
pub const ACTIVE_CYCLES: u64 = 30;

/// For how many cycles a key's owner may go unanswered, about the key, before a data plane
/// takes itself as cut off from it and drops the share it last heard of: a second
/// at a 10 ms cycle, longer than the store takes to replace a lost owner (about 600 ms), so
/// that a failover is not taken for being cut off. Dropped sooner, at 300 ms, a lost owner's
/// tight bucket among 8 data planes let through 3.1 times its limit in the second after,
/// against 1.24 times keeping the share (cluster simulator).
pub const CUT_OFF_CYCLES: u64 = 100;

/// The most reports a data plane has out for one key, sent and not yet seen applied: past
/// it, new hits wait in the key's ledger, one running count, until one is. Above what a
/// healthy link has out — a report a cycle through the [`ACTIVE_CYCLES`] a lost segment takes
/// TCP to send again — so that only an outage meets it, and it bounds what a busy key holds
/// through one, which eviction at the cap does not reach. A service pod keeps no more of a
/// data plane's batches waiting for a missing one.
pub const MOST_UNACKNOWLEDGED: usize = 32;

/// How long a service pod keeps a data plane's record of a key after the data plane last
/// admitted it, or for the key's refill time if longer: 10 minutes, a fixed margin, not
/// measured. Records of data planes gone, restarted workers among them, go even from a key
/// kept in use; one that comes back says in its next batch which of its batches it saw
/// applied, and a fresh record starts from there. Never under the refill time, so that data
/// planes told they share a key stay told so while they poll it.
pub const RECORD_KEPT: u64 = 10 * 60 * 1_000_000_000;

/// A data plane: one worker of one EdgeRush pod, in one life of it. A restarted worker is a
/// new data plane, with a new id.
pub type PlaneId = u64;

/// A service pod: its ordinal among the service's pods.
pub type PodId = u32;

/// A limit's contract: `limit` requests per `window` nanoseconds on average, at most
/// `burst` at once (the whole bucket, 06 §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Contract {
    /// Requests a window, on average.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
    /// The whole bucket.
    pub burst: NonZeroU64,
}

impl Contract {
    /// A full bucket holding the contract.
    #[must_use]
    pub fn bucket(&self) -> TokenBucket {
        TokenBucket::new(self.limit, self.window, self.burst)
    }

    /// One bucket's refill time, in nanoseconds: how long a copy stays fresh after the
    /// owner last answered, and how much debt the owner keeps.
    #[must_use]
    pub fn refill_time(&self) -> u64 {
        let time = u128::from(self.burst.get()) * u128::from(self.window.get())
            / u128::from(self.limit.get());
        u64::try_from(time).unwrap_or(u64::MAX)
    }

    /// Requests refilled in `span` nanoseconds, rounded down.
    #[must_use]
    pub fn refilled(&self, span: u64) -> u64 {
        let refilled =
            u128::from(self.limit.get()) * u128::from(span) / u128::from(self.window.get());
        u64::try_from(refilled).unwrap_or(u64::MAX)
    }
}

/// FNV-1a, 64 bits: the same on every machine and in every build, as ownership needs.
#[must_use]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// SplitMix64's finaliser: spreads a hash's bits.
#[must_use]
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// A hasher for the nodes' maps with no randomness in it, so that a run is the same every
/// time: the simulator's. A data plane facing keys clients choose takes a seeded one.
#[derive(Default)]
pub struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        mix(self.0)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0 = fnv1a(bytes) ^ self.0.rotate_left(5);
    }
}

/// [`Fnv`] as a map's hasher.
pub type FixedState = BuildHasherDefault<Fnv>;

/// What a data plane tells a service pod about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// Hits it admitted, for the owner to apply.
    Hits {
        /// The batch's number among this data plane's batches for the key, one past the
        /// last; a key evicted and made again numbers on past every number it had before.
        sequence: u64,
        /// How many hits.
        count: u64,
        /// The highest of its batches it has seen applied, always below `sequence`: an owner
        /// taking over from a copy older than that skips to it rather than wait for batches
        /// that will not come.
        seen: u64,
        /// How long before the batch was made ([`Batch::made`]) the earliest of the hits was
        /// admitted: the owner charges them as of the batch's arrival less this, so that a
        /// hit takes the room it took when admitted, not what has refilled since.
        age: u64,
    },
    /// Nothing to report, but the key is in use: a fresh copy, please.
    Poll,
}

/// One key in a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract, as the data plane holds it: the service needs no configuration of its
    /// own, and a contract changed anywhere starts the key afresh.
    pub contract: Contract,
    /// What about it.
    pub report: Report,
}

/// A data plane's message to a service pod: everything it has for the keys that pod owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The data plane.
    pub from: PlaneId,
    /// One entry per key, or a few for one key (batches sent again).
    pub entries: Vec<Entry>,
    /// When the data plane made it, on its own clock: what its hits' ages count back from,
    /// said back in the answers ([`Reply::made`](pooled::Reply::made)).
    pub made: u64,
}

/// The most a data plane's batch to a pod takes, by [`Batch::bytes`]: 1 MiB, a quarter of
/// the longest message the transport takes, so that a batch cut by a bound on its size,
/// and the answers to it, always fit. What does not fit waits for the next exchange (06
/// §4).
pub const PIECE_BYTES: usize = 1 << 20;

/// What a batch or copies take encoded beyond their entries, at most: their own few
/// numbers. This and the bounds below take each number at its longest, ten bytes, with
/// its field's tag and length; that they hold is tested where messages are encoded.
const MESSAGE_BYTES: usize = 64;
/// What an entry takes beyond its key's bytes: its contract, a report's four numbers, and
/// the lengths framing them.
const ENTRY_BYTES: usize = 128;

impl Entry {
    /// At most what it takes encoded.
    #[must_use]
    pub fn bytes(&self) -> usize {
        encoded_entry_bytes(self.key.len())
    }
}

/// At most what an entry for a key of `key` bytes takes encoded.
pub(crate) const fn encoded_entry_bytes(key: usize) -> usize {
    key.saturating_add(ENTRY_BYTES)
}

impl Batch {
    /// At most what it takes encoded.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.entries
            .iter()
            .map(Entry::bytes)
            .fold(MESSAGE_BYTES, usize::saturating_add)
    }
}

/// The decision on a request told to the worker waiting on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decided {
    /// Whether it was admitted.
    pub admitted: bool,
    /// Refused, those of its keys that refused it, by position in its list, each with how
    /// long from the decision it waits to admit one, as whatever decided held them. Empty
    /// when admitted.
    pub refused_by: Box<[(usize, u64)]>,
}

/// When answers that came at `now` are taken as of, their batch made at
/// `made`: then, so that the batch's trip there cancels out, but no further back than a
/// `cycle` — a trip held up longer, a lost packet waiting for TCP to send it again, would
/// be refill the copy never had — and as of `now` from a pod that does not say when.
fn taken_as_of(made: u64, cycle: u64, now: u64) -> u64 {
    if made == 0 {
        return now;
    }
    made.min(now).max(now.saturating_sub(cycle))
}

/// A batch a data plane sent and has not yet seen applied.
#[derive(Clone, Copy, Debug)]
struct Unseen {
    sequence: u64,
    /// Its hits.
    count: u64,
    /// When the earliest of its hits was admitted.
    admitted: u64,
    /// When it was first sent, and last.
    first: u64,
    last: u64,
}

impl Unseen {
    /// Its report in a batch made at `made`, the data plane having seen its batches up to
    /// `seen` applied.
    fn report(&self, seen: u64, made: u64) -> Report {
        Report::Hits {
            sequence: self.sequence,
            count: self.count,
            seen,
            age: made.saturating_sub(self.admitted),
        }
    }

    /// Emptied of its hits if it is not seen applied within `hold` of first being sent, at
    /// `now`, and still sent, as the owner applies in order: the hits it gave up, forgiven.
    fn empty_if_stale(&mut self, hold: u64, now: u64) -> u64 {
        if now - self.first > hold {
            std::mem::take(&mut self.count)
        } else {
            0
        }
    }
}

/// Whether a data plane is cut off from a key's owner at `now`: nothing heard of the key
/// since `heard` for `after` ([`CUT_OFF_CYCLES`]), and its oldest report not seen applied,
/// first sent at `oldest`, out as long. It then stops counting the others the owner last
/// said admit the key and enforces the whole bucket on what it sees (06 §6). A key merely
/// quiet, its oldest report out for less, keeps its share: data planes waking on it together
/// must not each take the whole bucket.
fn cut_off_from_owner(heard: Option<u64>, oldest: Option<u64>, after: u64, now: u64) -> bool {
    let long = |at: Option<u64>| at.is_some_and(|at| now.saturating_sub(at) >= after);
    long(heard) && long(oldest)
}

/// What a data plane has to send one service pod: entries, their hits' ages counting back
/// from `made`; and what they take, by [`Entry::bytes`].
#[derive(Debug, Default)]
pub(crate) struct Queued {
    pub(crate) made: u64,
    pub(crate) entries: Vec<Entry>,
    bytes: usize,
}

/// The room left in a piece being filled, which takes its first entry however big.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Room {
    left: usize,
    empty: bool,
}

impl Room {
    /// A piece with nothing in it.
    pub(crate) const fn new() -> Self {
        Self {
            left: PIECE_BYTES - MESSAGE_BYTES,
            empty: true,
        }
    }

    /// Whether `bytes` more fit, taken from the room if they do.
    pub(crate) const fn take(&mut self, bytes: usize) -> bool {
        if !self.empty && bytes > self.left {
            return false;
        }
        self.left = self.left.saturating_sub(bytes);
        self.empty = false;
        true
    }
}

impl Queued {
    /// The room left in its next piece.
    pub(crate) fn room(&self) -> Room {
        let mut room = Room::new();
        if !self.is_empty() {
            room.take(self.bytes);
        }
        room
    }

    /// Whether it holds nothing to send.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `entry` queued last.
    pub(crate) fn push_entry(&mut self, entry: Entry) {
        self.bytes = self.bytes.saturating_add(entry.bytes());
        self.entries.push(entry);
    }

    /// Its next piece, from data plane `from`: its entries in the order queued, as many as
    /// fit in [`PIECE_BYTES`]; the rest kept for the next. A key's reports stay in order,
    /// and keys depend on no other.
    pub(crate) fn take_piece(&mut self, from: PlaneId) -> Batch {
        if self.bytes <= Room::new().left {
            // All of it, as most often.
            let batch = Batch {
                from,
                entries: std::mem::take(&mut self.entries),
                made: self.made,
            };
            self.bytes = 0;
            return batch;
        }
        let mut room = Room::new();
        let fit = self
            .entries
            .iter()
            .take_while(|entry| room.take(entry.bytes()))
            .count();
        let entries: Vec<Entry> = self.entries.drain(..fit).collect();
        let batch = Batch {
            from,
            entries,
            made: self.made,
        };
        self.bytes = self.bytes.saturating_sub(batch.bytes() - MESSAGE_BYTES);
        batch
    }

    /// Made at `now` from here on, if that is later: the ages of the hits it holds count
    /// back from it, so that every age in a batch counts back from one time, the
    /// latest anything was added at.
    pub(crate) fn at(&mut self, now: u64) -> &mut Self {
        if now > self.made {
            let later = now - self.made;
            for entry in &mut self.entries {
                if let Report::Hits { age, .. } = &mut entry.report {
                    *age = age.saturating_add(later);
                }
            }
            self.made = now;
        }
        self
    }
}

/// The longest of `waits`, by position and wait, the first of those that wait as long.
#[must_use]
pub fn longest(waits: impl Iterator<Item = (usize, u64)>) -> Option<(usize, u64)> {
    waits.fold(None, |longest, (position, wait)| match longest {
        Some((_, most)) if most >= wait => longest,
        _ => Some((position, wait)),
    })
}
