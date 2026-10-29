//! The protocol of the rate-limiting service, a candidate not yet chosen (06 §4): a few
//! service pods own the keys between them, and data planes — each worker of each EdgeRush
//! pod — decide every request from their copy of the key, never talking to each other.
//!
//! Two state machines, with no I/O: a [`DataPlane`] and a [`ServicePod`]. Each is handed
//! the time and the messages that reach it, and hands back the messages it sends; whatever
//! carries them — the simulator, or gRPC between processes — is the caller's.
//!
//! What they do, as measured best in the simulator (06 §4):
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
//!   send while the key is in use, a poll. Each is answered with the bucket as it stands,
//!   which the copy takes as of when the batch was made: the batch's trip cancels out the
//!   lateness of the owner's charge, with no clock shared. A batch not seen applied is sent
//!   again after four cycles; one older than a bucket's refill time is sent emptied.
//! - A request under several limits is admitted under all of them or none, and charged
//!   to all or none, every one decided from the data plane's copy: a key it has not heard
//!   of from a full bucket. It asks no one.
//! - The owner applies hits in order, once each, as of when the earliest of them was
//!   admitted, charging what does not fit as debt while it would be paid back within one
//!   bucket's refill time, and copies every change it makes to every other service pod: the next to own the key takes over from its copy should
//!   the owner go, with nothing to send anyone.
//!
//! Which pods own which keys follows from the [`View`] the service pods agree on: the owner
//! of a key is the pod ranking highest for it (rendezvous hashing), and the next in rank
//! takes it over, so a pod leaving moves only the keys it held.

pub mod keyed;
pub mod membership;
pub mod pooled;

pub use keyed::Keyed;

use crate::reference::{BucketState, Model, TokenBucket};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::num::NonZeroU64;

/// How many cycles a data plane waits to see a batch applied before sending it again.
pub const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles before the latest admission the owner knows of a data plane counts
/// as admitting the key: 300 ms at a 10 ms cycle, longer than TCP takes to send a lost
/// segment again. A data plane whose batches are held up behind a lost one must not drop out
/// of the count, or every other one's share grows while it is stalled: four cycles let bursts
/// through at 1.44 times the contract with one packet in ten lost (simulator, TCP-like loss).
pub const ACTIVE_CYCLES: u64 = 30;

/// The most reports a data plane has out for one key, sent and not yet seen applied: past
/// it, new hits wait in the key's ledger, one running count, until one is. Above what a
/// healthy link has out — a report a cycle through the [`ACTIVE_CYCLES`] a lost segment takes
/// TCP to send again — so that only an outage meets it, and it bounds what a busy key holds
/// through one, which eviction at the cap does not reach.
pub const MOST_UNACKNOWLEDGED: usize = 32;

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

/// Which service pods are serving, as they agree on it, and since when: a later epoch
/// replaces an earlier one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    /// Counted up with each change.
    pub epoch: u64,
    /// The pods serving.
    pub pods: Vec<PodId>,
}

impl View {
    /// The owner of `key` and the pod next in rank for it, which takes it over should the
    /// owner go.
    #[must_use]
    pub fn owners(&self, key: &[u8]) -> (Option<PodId>, Option<PodId>) {
        let hash = fnv1a(key);
        let mut ranked: Vec<(u64, PodId)> = self
            .pods
            .iter()
            .map(|&pod| (mix(hash ^ mix(u64::from(pod) + 1)), pod))
            .collect();
        ranked.sort_unstable_by(|a, b| b.cmp(a));
        (ranked.first().map(|r| r.1), ranked.get(1).map(|r| r.1))
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
    /// said back in the answers ([`Answers::made`]).
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

/// A service pod's answer about one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// The key.
    pub key: Box<[u8]>,
    /// The bucket as the pod holds it: how far from full, as [`TokenBucket::behind`] says.
    pub behind: u128,
    /// The highest of the receiver's batches applied, every one before it too, as far as
    /// the pod knows: a copy is never ahead of its owner.
    pub applied: u64,
    /// How many other data planes the pod has seen admit the key lately.
    pub others: u64,
}

/// A service pod's answer to a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answers {
    /// The service pod.
    pub from: PodId,
    /// One per entry for a key it owns.
    pub entries: Vec<Answer>,
    /// The view it holds: a data plane holding an earlier one takes it over.
    pub view: Option<View>,
    /// The pod's life: drawn at random when it starts, never 0. 0 is a pod that does not
    /// number its answers.
    pub life: u64,
    /// Counted up, wrapping, with each answer the pod gives in this life: the order in
    /// which it handled their batches.
    pub number: u64,
    /// The batch's [`Batch::made`], said back: the data plane takes the buckets as of then.
    /// The pod charged the batch's hits as late as the batch took to reach it, and the
    /// buckets read later by as much: taken as of when the batch was made, the two cancel,
    /// with no clock shared; but no further back than a cycle before the answers came, as a
    /// trip held up longer — a lost packet waiting for TCP to send it again — would be
    /// counted as refill the copy never had. 0 is a pod that does not say it back, charging
    /// hits as they arrive: its buckets are taken as of when the answers came.
    pub made: u64,
}

/// What the owner knows of one data plane, for one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaneRecord {
    /// The data plane.
    pub plane: PlaneId,
    /// The highest of its batches applied, every one before it too.
    pub applied: u64,
    /// How long before the copy was made it last admitted the key, if it has.
    pub admitted_ago: Option<u64>,
}

/// The owner's copy of one key, for the other service pods.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
    /// The key.
    pub key: Box<[u8]>,
    /// Its contract.
    pub contract: Contract,
    /// The bucket, as [`TokenBucket::behind`] says it.
    pub behind: u128,
    /// Hits the count holds, each once: for measuring.
    pub counted: u64,
    /// Every data plane it knows of.
    pub planes: Vec<PlaneRecord>,
}

/// The owner's copies of keys that changed, to another service pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replicas {
    /// The owner, or the pod that was.
    pub from: PodId,
    /// One per key.
    pub entries: Vec<Replica>,
    /// Whether these are keys handed over to their new owner, by the pod that owned them
    /// before a view change, rather than copies.
    pub handover: bool,
    /// The epoch of the view the sender held: a handover can reach a pod joining before
    /// the view that takes it in does, and is kept until it has.
    pub epoch: u64,
}

/// What a copy takes beyond its key's bytes and its records.
const REPLICA_BYTES: usize = 128;
/// What each of a copy's records takes.
const PLANE_RECORD_BYTES: usize = 96;

impl Replica {
    /// At most what it takes encoded.
    #[must_use]
    pub fn bytes(&self) -> usize {
        let records = self.planes.len().saturating_mul(PLANE_RECORD_BYTES);
        self.key
            .len()
            .saturating_add(REPLICA_BYTES)
            .saturating_add(records)
    }
}

impl Replicas {
    /// At most what they take encoded.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.entries
            .iter()
            .map(Replica::bytes)
            .fold(MESSAGE_BYTES, usize::saturating_add)
    }

    /// Cut into pieces of at most [`PIECE_BYTES`], in order, to be sent one after the
    /// other; a copy bigger than a piece goes alone. Copies apply key by key, and keys
    /// handed over too, so that a piece needs none of the others.
    #[must_use]
    pub fn into_pieces(self) -> Vec<Self> {
        let piece = |entries| Self {
            from: self.from,
            entries,
            handover: self.handover,
            epoch: self.epoch,
        };
        let mut pieces = Vec::new();
        let mut entries = Vec::new();
        let mut room = Room::new();
        for replica in self.entries {
            if !room.take(replica.bytes()) {
                pieces.push(piece(std::mem::take(&mut entries)));
                room = Room::new();
                room.take(replica.bytes());
            }
            entries.push(replica);
        }
        if !entries.is_empty() {
            pieces.push(piece(entries));
        }
        pieces
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

/// How a request was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Admitted.
    Admit,
    /// Refused.
    Refuse,
}

/// When answers that came at `now` are taken as of, their batch [`made`](Answers::made) at
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

    /// Its batch, from data plane `from`.
    pub(crate) fn batch(self, from: PlaneId) -> Batch {
        Batch {
            from,
            entries: self.entries,
            made: self.made,
        }
    }
}

/// What a data plane holds of one key.
struct PlaneKey {
    contract: Contract,
    /// Its contract's refill time, worked out once.
    hold: u64,
    /// The service pod it sends to.
    owner: Option<PodId>,
    /// The owner's bucket as last heard, charged with its own hits not yet seen applied,
    /// and with what it has admitted since.
    bucket: TokenBucket,
    /// Hits admitted and not yet sent.
    unsent: u64,
    /// When the earliest of them was admitted.
    unsent_since: u64,
    /// Batches sent to the present owner so far.
    sent: u64,
    /// The highest of them the owner has been seen to apply.
    seen: u64,
    /// Batches sent and not yet seen applied.
    unacknowledged: VecDeque<Unseen>,
    /// Admitted in the cycle under way.
    spent: u64,
    /// The most it may admit in the cycle under way, once a cycle has set it.
    allowance: Option<u64>,
    /// When a cycle last shared out what the copy has left.
    shared: Option<u64>,
    /// How many other data planes the owner last said were admitting the key.
    others: u64,
    /// When a request for the key last came.
    last_request: Option<u64>,
    /// When a pod last answered about it.
    heard: Option<u64>,
}

impl PlaneKey {
    fn new(contract: Contract, owner: Option<PodId>, numbered: u64) -> Self {
        Self {
            contract,
            hold: contract.refill_time(),
            owner,
            bucket: contract.bucket(),
            unsent: 0,
            unsent_since: 0,
            sent: numbered,
            seen: numbered,
            unacknowledged: VecDeque::new(),
            spent: 0,
            allowance: None,
            shared: None,
            others: 0,
            last_request: None,
            heard: None,
        }
    }

    /// Whether a request would be admitted here, from the copy and the cycle's allowance.
    fn room(&self, now: u64) -> bool {
        self.allowance
            .is_none_or(|allowance| self.spent < allowance)
            && self.bucket.admits(now)
    }

    /// How long from `now` the copy waits to admit a request, if it would refuse one: its
    /// bucket's refill to one request, or, with room but its share of the cycle spent, the
    /// next `cycle`.
    fn wait(&self, cycle: u64, now: u64) -> Option<u64> {
        match self.bucket.retry_at(now) {
            Some(retry) => Some(retry.saturating_sub(now)),
            None if self
                .allowance
                .is_some_and(|allowance| self.spent >= allowance) =>
            {
                Some(cycle)
            }
            None => None,
        }
    }

    /// Charges a request admitted here, sent to the owner with the next batch.
    fn take(&mut self, now: u64) {
        self.bucket.charge(now);
        self.spent += 1;
        self.unsent_one(now);
    }

    /// Its hits not yet sent, as a report numbered after the last, sent at `now`: `None` if
    /// there are none, or [`MOST_UNACKNOWLEDGED`] are out already and they wait.
    fn report_unsent(&mut self, key: &[u8], now: u64) -> Option<Entry> {
        if self.unsent == 0 || self.unacknowledged.len() >= MOST_UNACKNOWLEDGED {
            return None;
        }
        self.sent += 1;
        let unseen = Unseen {
            sequence: self.sent,
            count: self.unsent,
            admitted: self.unsent_since,
            first: now,
            last: now,
        };
        self.unacknowledged.push_back(unseen);
        self.unsent = 0;
        Some(Entry {
            key: key.into(),
            contract: self.contract,
            report: unseen.report(self.seen, now),
        })
    }

    /// One more hit to send, admitted at `at`.
    fn unsent_one(&mut self, at: u64) {
        self.unsent_since = if self.unsent == 0 {
            at
        } else {
            self.unsent_since.min(at)
        };
        self.unsent += 1;
    }

    /// Decides a request here, from the copy and the cycle's allowance.
    fn decide(&mut self, now: u64) -> bool {
        self.last_request = Some(now);
        let admitted = self.room(now);
        if admitted {
            self.take(now);
        }
        admitted
    }

    /// Whether nothing is left to do for the key, and nothing has used it for twice its
    /// refill time.
    fn idle(&self, now: u64) -> bool {
        self.unsent == 0
            && self.unacknowledged.is_empty()
            && self
                .last_request
                .is_none_or(|last| now.saturating_sub(last) > 2 * self.hold)
    }
}

/// A data plane's side of the protocol: one worker's copies of the keys it has seen.
pub struct DataPlane<S = FixedState> {
    id: PlaneId,
    cycle: u64,
    view: View,
    keys: HashMap<Box<[u8]>, PlaneKey, S>,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    /// What is to go to each pod.
    queued: BTreeMap<PodId, Queued>,
    /// Views asked for so far, to ask the pods in turn.
    polls: u64,
    /// The bytes its keys may hold, as [`plane_key_bytes`] counts them, and what they hold.
    capacity: u64,
    holding: u64,
    /// Keys it has evicted to stay within its capacity, all told.
    evicted: u64,
    /// Hits given up without being seen applied, all told ([`Self::forgiven`]).
    forgiven: u64,
}

/// What a data plane counts a key as holding: its entry in the table, the table's control
/// byte for it, and the key. An estimate of its own data, not of what the allocator takes.
fn plane_key_bytes(key: &[u8]) -> u64 {
    (size_of::<(Box<[u8]>, PlaneKey)>() + 1 + key.len()) as u64
}

/// What a service pod counts a key as holding: as a data plane does, and the room for its
/// records of the data planes admitting it.
fn pod_key_bytes(key: &[u8], owned: &Owned) -> u64 {
    let records = owned.planes.room() * size_of::<(PlaneId, Record)>();
    (size_of::<(Box<[u8]>, Owned)>() + 1 + key.len() + records) as u64
}

/// Of `keys`, those evictable with when each was last used, the least recently used going
/// first until what is left holds no more than `target` of `holding`: what to evict.
fn least_recently_used(
    mut quiet: Vec<(u64, Box<[u8]>, u64)>,
    holding: u64,
    target: u64,
) -> Vec<Box<[u8]>> {
    quiet.sort_unstable_by_key(|(used, _, _)| *used);
    let mut left = holding;
    quiet
        .into_iter()
        .take_while(|(_, _, bytes)| {
            let over = left > target;
            left = left.saturating_sub(*bytes);
            over
        })
        .map(|(_, key, _)| key)
        .collect()
}

impl<S: BuildHasher + Default + Clone> DataPlane<S> {
    /// A data plane `id` sending every `cycle` nanoseconds, knowing the service as `view`,
    /// its maps hashed by `S` made by default: the simulator's, which runs alike every time.
    #[must_use]
    pub fn new(id: PlaneId, cycle: u64, view: View) -> Self {
        Self::with_hasher(id, cycle, view, S::default())
    }
}

impl<S: BuildHasher + Clone> DataPlane<S> {
    /// A data plane `id` sending every `cycle` nanoseconds, knowing the service as `view`,
    /// its maps hashed by `hasher`: a keyed one ([`Keyed`]) where clients choose the keys.
    #[must_use]
    pub fn with_hasher(id: PlaneId, cycle: u64, view: View, hasher: S) -> Self {
        Self {
            id,
            cycle: cycle.max(1),
            view,
            keys: HashMap::with_hasher(hasher),
            numbered: 0,
            queued: BTreeMap::new(),
            polls: 0,
            capacity: u64::MAX,
            holding: 0,
            evicted: 0,
            forgiven: 0,
        }
    }

    /// The bytes its keys may hold: past them, each cycle evicts the least recently used
    /// keys, down to nine tenths of it, forgiving any hits they have not had applied
    /// ([`Self::forgiven`]). A key evicted starts again full, as one forgotten does, and
    /// numbers its reports past every one sent before. No bound unless set.
    pub fn set_capacity(&mut self, bytes: u64) {
        self.capacity = bytes;
    }

    /// The bytes its keys hold, as it counts them.
    #[must_use]
    pub fn holding(&self) -> u64 {
        self.holding
    }

    /// The keys it has evicted to stay within its capacity, all told.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Hits given up without being seen applied, all told: those of keys evicted for
    /// capacity, unsent or not seen applied, and those of reports not seen applied within
    /// their key's refill time. At most what was lost, since a report not seen applied may
    /// have been applied with its answer lost.
    #[must_use]
    pub fn forgiven(&self) -> u64 {
        self.forgiven
    }

    /// How many keys it holds.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> PlaneId {
        self.id
    }

    /// Decides a request for `key` under `contract` arriving at `now`: [`decide_all`] of
    /// one key.
    ///
    /// [`decide_all`]: Self::decide_all
    pub fn decide(&mut self, key: &[u8], contract: Contract, now: u64) -> Decision {
        // The common case, with one lookup: a key held under the same contract.
        if let Some(at) = self.keys.get_mut(key)
            && at.contract == contract
        {
            return if at.decide(now) {
                Decision::Admit
            } else {
                Decision::Refuse
            };
        }
        self.decide_new(&[(key, contract)], now)
    }

    /// Decides a request arriving at `now` under every limit that applies to it, each a key
    /// under its contract, in an order fixed when the limits were compiled — the smallest
    /// bucket first (06 §4): admitted under all of them or none, and charged to all of them
    /// or none (06 §2).
    ///
    /// Every key is decided here, from its copy — a key it has not heard of from a full
    /// bucket — and nothing waits on a service pod.
    pub fn decide_all(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        // Hashing a key is a third of a decision: the usual few are looked up together,
        // once each.
        match keys {
            [one] => self.decide(one.0, one.1, now),
            [a, b] => self.decide_warm([a, b], now),
            [a, b, c] => self.decide_warm([a, b, c], now),
            [a, b, c, d] => self.decide_warm([a, b, c, d], now),
            _ => self.decide_new(keys, now),
        }
    }

    /// The common case for a few limits, with one lookup a key: every key held under the
    /// same contract.
    fn decide_warm<const N: usize>(&mut self, keys: [&(&[u8], Contract); N], now: u64) -> Decision {
        // Looking the same key up twice at once is refused by the map; limits each have a
        // key of their own, so it is a caller's mistake, decided the long way.
        for (at, (key, _)) in keys.iter().enumerate() {
            if keys[..at].iter().any(|(other, _)| other == key) {
                return self.decide_new(&keys.map(|key| *key), now);
            }
        }
        let mut found = self.keys.get_disjoint_mut(keys.map(|(key, _)| *key));
        let mut held = true;
        let mut room = true;
        for (at, (_, contract)) in found.iter_mut().zip(keys) {
            match at {
                Some(at) if at.contract == *contract => {
                    at.last_request = Some(now);
                    room &= at.room(now);
                }
                _ => {
                    held = false;
                    break;
                }
            }
        }
        if !held {
            return self.decide_new(&keys.map(|key| *key), now);
        }
        if !room {
            return Decision::Refuse;
        }
        for at in found.iter_mut().flatten() {
            at.take(now);
        }
        Decision::Admit
    }

    /// Decides a request with a key new to it, or under a changed contract — made from a
    /// full bucket — or the same key twice: kept out of the common case's way.
    #[cold]
    #[inline(never)]
    fn decide_new(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        for &(key, contract) in keys {
            // New, or its contract changed: any change starts a key afresh (06 §3).
            if !matches!(self.keys.get(key), Some(at) if at.contract == contract) {
                let owner = self.view.owners(key).0;
                if self
                    .keys
                    .insert(key.into(), PlaneKey::new(contract, owner, self.numbered))
                    .is_none()
                {
                    self.holding += plane_key_bytes(key);
                }
            }
            let Some(at) = self.keys.get_mut(key) else {
                return Decision::Refuse;
            };
            at.last_request = Some(now);
        }
        let room = keys
            .iter()
            .all(|(key, _)| self.keys.get(*key).is_some_and(|at| at.room(now)));
        if !room {
            return Decision::Refuse;
        }
        for (key, _) in keys {
            if let Some(at) = self.keys.get_mut(*key) {
                at.take(now);
            }
        }
        Decision::Admit
    }

    /// The cycle's work at `now`: allowances set for the next cycle, batches and polls for
    /// every owner.
    pub fn on_cycle(&mut self, now: u64) {
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        let cycle = self.cycle;
        let mut cut_off = false;
        for (key, at) in &mut self.keys {
            let hold = at.contract.refill_time();
            let refill = at.contract.refilled(cycle);
            let left = at
                .allowance
                .map_or(0, |before| before.saturating_sub(at.spent));
            // What the others admit under the shares given out at a cycle reaches the copy
            // only with the answers to the batches they send at the next, as this one sends
            // its own: sharing out again at once what the copy has left would give it out
            // twice. So the cycle after a share gets its share of the refill only, and keeps
            // what it left unspent; the one after that shares out the copy afresh — its
            // share of what the copy has left and the cycle refills, at least one, and one
            // more if it left one unspent.
            let after_share =
                at.others > 0 && at.shared.is_some_and(|shared| now - shared < 2 * cycle);
            at.allowance = Some(if after_share {
                (refill / (at.others + 1)).max(1) + left
            } else {
                at.shared = Some(now);
                // Rounded once, the room and the refill together: rounded apart, a refill of
                // 6.6 a cycle shares out 6, and a client at exactly its rate is refused.
                let sees = at.bucket.remaining_within(now, cycle, u64::MAX);
                (sees / (at.others + 1)).max(1) + left.min(1)
            });
            at.spent = 0;
            let Some(owner) = at.owner else {
                continue;
            };
            // Ages count back from now, as the batch its entries join is made: every batch waiting
            // was made at a time this data plane was given, and time does not go back.
            let mut entries = Vec::new();
            for unseen in &mut at.unacknowledged {
                self.forgiven += unseen.empty_if_stale(hold, now);
            }
            let mut reported = false;
            for unseen in &mut at.unacknowledged {
                if now - unseen.last >= resend_after {
                    unseen.last = now;
                    entries.push(Entry {
                        key: key.clone(),
                        contract: at.contract,
                        report: unseen.report(at.seen, now),
                    });
                    reported = true;
                }
            }
            if let Some(entry) = at.report_unsent(key, now) {
                self.numbered += 1;
                entries.push(entry);
                reported = true;
            }
            let in_use = at.last_request.is_some_and(|last| now - last <= hold)
                || !at.unacknowledged.is_empty();
            if !reported && in_use {
                entries.push(Entry {
                    key: key.clone(),
                    contract: at.contract,
                    report: Report::Poll,
                });
            }
            // Cut off: a key in use whose owner has not answered for as long as a report is
            // sent again after.
            cut_off |= in_use && at.heard.is_none_or(|heard| now - heard >= resend_after);
            if !entries.is_empty() {
                let queued = self.queued.entry(owner).or_default().at(now);
                for entry in entries {
                    queued.push_entry(entry);
                }
            }
        }
        // An owner out of reach may have gone: the pods are asked for their view in turn,
        // an empty batch each cycle, until one has a later view or the owner answers.
        if cut_off && !self.view.pods.is_empty() {
            let at = usize::try_from(self.polls).unwrap_or(0) % self.view.pods.len();
            self.polls += 1;
            self.queued.entry(self.view.pods[at]).or_default().at(now);
        }
        let mut freed = 0;
        self.keys.retain(|key, at| {
            let idle = at.idle(now);
            if idle {
                freed += plane_key_bytes(key);
            }
            !idle
        });
        self.holding = self.holding.saturating_sub(freed);
        if self.holding > self.capacity {
            // Every key, whatever it has outstanding: memory stays within the cap however
            // long the service is out of reach.
            let used = self
                .keys
                .iter()
                .map(|(key, at)| {
                    let used = at.last_request.unwrap_or(0);
                    (used, key.clone(), plane_key_bytes(key))
                })
                .collect();
            let target = self.capacity - self.capacity / 10;
            for key in least_recently_used(used, self.holding, target) {
                if let Some(at) = self.keys.remove(&key) {
                    let unseen: u64 = at.unacknowledged.iter().map(|u| u.count).sum();
                    self.forgiven += at.unsent + unseen;
                    self.holding = self.holding.saturating_sub(plane_key_bytes(&key));
                    self.evicted += 1;
                }
            }
        }
    }

    /// A service pod's answers reach it at `now`: each copy set to the pod's bucket as of
    /// when the batch was made, and charged with what the pod has not seen.
    pub fn on_answers(&mut self, answers: Answers, now: u64) {
        let Answers {
            from,
            entries,
            view,
            made,
            ..
        } = answers;
        let made = taken_as_of(made, self.cycle, now);
        for answer in entries {
            let Some(at) = self.keys.get_mut(&answer.key) else {
                continue;
            };
            // Only the owner it sends to: an answer from an owner that has gone is stale.
            if at.owner != Some(from) {
                continue;
            }
            at.others = answer.others;
            // No more of this data plane's batches can have been applied than it has sent.
            let seen = answer.applied.min(at.sent);
            at.seen = at.seen.max(seen);
            at.unacknowledged.retain(|unseen| unseen.sequence > seen);
            // Charged on top: the hits the owner has yet to apply.
            let unseen: u64 = at.unacknowledged.iter().map(|unseen| unseen.count).sum();
            at.bucket.rebase(made, answer.behind, unseen + at.unsent);
            at.heard = Some(now);
        }
        if let Some(view) = view {
            self.on_view(view, now);
        }
    }

    /// The service's pods change at `now`. Keys whose owner moved send it again every
    /// batch not seen applied, numbered as before: a pod taking over from its copy knows
    /// what the old owner applied.
    pub fn on_view(&mut self, view: View, now: u64) {
        if view.epoch <= self.view.epoch {
            return;
        }
        self.view = view;
        for (key, at) in &mut self.keys {
            let owner = self.view.owners(key).0;
            if owner == at.owner {
                continue;
            }
            at.owner = owner;
            let Some(owner) = owner else {
                continue;
            };
            let queued = self.queued.entry(owner).or_default().at(now);
            for unseen in &mut at.unacknowledged {
                unseen.last = now;
                let report = unseen.report(at.seen, queued.made);
                queued.push_entry(Entry {
                    key: key.clone(),
                    contract: at.contract,
                    report,
                });
            }
        }
    }

    /// Those of `keys` that would refuse a request at `now`, as the copies have them, each
    /// by its position in `keys` with how long from `now` it waits to admit one: its
    /// bucket's refill to one request, or, a key with room whose share of the cycle is
    /// spent, the next cycle. A key it holds no copy of is not among them.
    pub fn waits<'a>(
        &'a self,
        keys: &'a [(&[u8], Contract)],
        now: u64,
    ) -> impl Iterator<Item = (usize, u64)> + 'a {
        keys.iter()
            .enumerate()
            .filter_map(move |(position, (key, contract))| {
                let at = self.keys.get(*key).filter(|at| at.contract == *contract)?;
                Some((position, at.wait(self.cycle, now)?))
            })
    }

    /// How long, from `now`, a request refused under `keys` should wait before it would be
    /// admitted under all of them, as its copies have it, and which of `keys`, by position,
    /// waits that long: the longest of their [`waits`](Self::waits), the first of those
    /// that wait as long. `None` if every key it holds would admit one now, or it holds
    /// none of them.
    #[must_use]
    pub fn retry_after(&self, keys: &[(&[u8], Contract)], now: u64) -> Option<(usize, u64)> {
        longest(self.waits(keys, now))
    }

    /// Messages to send: one batch per service pod; an empty one asks only for the view.
    /// Whole, not cut to [`PIECE_BYTES`] as the pooled participant's are: as the reference,
    /// it is compared with that over batches that fit.
    pub fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
        std::mem::take(&mut self.queued)
            .into_iter()
            .map(|(pod, queued)| (pod, queued.batch(self.id)))
            .collect()
    }
}

/// What a service pod keeps of one data plane, for one key.
#[derive(Clone, Debug, Default)]
struct Record {
    applied: u64,
    /// Batches that came ahead of one still missing, `(sequence, count)` in order: none,
    /// and no room for any, while batches come in order, as they almost always do.
    #[allow(
        clippy::box_collection,
        reason = "a pointer in every record, rather than a list's three words, for what is rarely there"
    )]
    waiting: Option<Box<Vec<(u64, Tally)>>>,
    last_admitted: Admitted,
}

/// A batch's hits as its owner applies them: how many, and as of when they are charged, on
/// the owner's clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tally {
    count: u64,
    at: u64,
}

impl Tally {
    /// Counts come from the wire: as many as a count holds, never wrapped round. Charged
    /// as of the earlier, as one entry's hits are as of its earliest.
    fn add(self, other: Self) -> Self {
        Self {
            count: self.count.saturating_add(other.count),
            at: self.at.min(other.at),
        }
    }
}

/// When a data plane last admitted a key, if it has, in eight bytes rather than an option's
/// sixteen: a record is kept for every data plane admitting every key. One past the time, 0 if never; ordered as the option is, never first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Admitted(u64);

impl Admitted {
    /// Admitted at `at`.
    fn at(at: u64) -> Self {
        Self(at.saturating_add(1))
    }

    /// When, if ever.
    fn get(self) -> Option<u64> {
        self.0.checked_sub(1)
    }
}

impl From<Option<u64>> for Admitted {
    fn from(at: Option<u64>) -> Self {
        at.map_or(Self(0), Self::at)
    }
}

impl Record {
    /// Batch `sequence` of `count` hits arrives: the hits now ready to apply, of it and of
    /// the batches kept waiting for it, each batch once.
    fn take(&mut self, sequence: u64, tally: Tally) -> Tally {
        if sequence <= self.applied {
            return Tally::default();
        }
        if sequence > self.applied + 1 {
            let waiting = self.waiting.get_or_insert_default();
            match waiting.binary_search_by_key(&sequence, |(at, _)| *at) {
                Ok(at) => waiting[at] = (sequence, tally),
                Err(at) => waiting.insert(at, (sequence, tally)),
            }
            return Tally::default();
        }
        self.applied = sequence;
        let mut ready = tally;
        if let Some(waiting) = &mut self.waiting {
            // All ahead of it: the first of them follow on from it.
            let mut caught_up = 0;
            for &(at, waited) in waiting.iter() {
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
    fn skip_to(&mut self, seen: u64) {
        self.applied = seen;
        if let Some(waiting) = &mut self.waiting {
            waiting.retain(|(sequence, _)| *sequence > seen);
            if waiting.is_empty() {
                self.waiting = None;
            }
        }
    }
}

/// What a service pod keeps of the data planes admitting a key, one record each, in order
/// of data plane and each found by halving the list: most keys are admitted by one data
/// plane, and a list has room for no more than it needs where a B-tree's first node has
/// room for eleven. A record is never removed.
#[derive(Debug, Default)]
struct Records(Vec<(PlaneId, Record)>);

impl Records {
    fn find(&self, plane: PlaneId) -> Result<usize, usize> {
        self.0.binary_search_by_key(&plane, |(id, _)| *id)
    }

    fn get(&self, plane: PlaneId) -> Option<&Record> {
        let at = self.find(plane).ok()?;
        self.0.get(at).map(|(_, record)| record)
    }

    /// The record of `plane`, made if there is none.
    fn entry(&mut self, plane: PlaneId) -> &mut Record {
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

    fn iter(&self) -> impl Iterator<Item = (PlaneId, &Record)> {
        self.0.iter().map(|(plane, record)| (*plane, record))
    }

    /// How many records there is room for.
    fn room(&self) -> usize {
        self.0.capacity()
    }
}

impl FromIterator<(PlaneId, Record)> for Records {
    fn from_iter<I: IntoIterator<Item = (PlaneId, Record)>>(list: I) -> Self {
        let list = list.into_iter();
        let mut records = Self(Vec::with_capacity(list.size_hint().0));
        for (plane, record) in list {
            *records.entry(plane) = record;
        }
        records
    }
}

impl IntoIterator for Records {
    type Item = (PlaneId, Record);
    type IntoIter = std::vec::IntoIter<(PlaneId, Record)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// What a service pod holds of a key, as its owner or as a copy.
struct Owned {
    contract: Contract,
    /// Its bucket's state, the contract its terms: the bucket is made from them to be used.
    bucket: BucketState,
    counted: u64,
    /// What it had counted when its pod last took up a view, or was handed it: a pod left
    /// out and taken back in, still holding the key, is handed it back holding all that,
    /// so only what came after is charged on top ([`merge`](Self::merge)).
    since: u64,
    planes: Records,
    /// When anything last changed.
    touched: u64,
}

impl Owned {
    fn new(contract: Contract, now: u64) -> Self {
        Self {
            contract,
            bucket: BucketState::default(),
            counted: 0,
            since: 0,
            planes: Records::default(),
            touched: now,
        }
    }

    /// `count` reported hits, admitted at `at`, applied at `now`: charged as of `at`, each
    /// if there is room, or as debt while it would be paid back within a refill time of
    /// `now`; the rest forgiven. In one step, whatever the count: it comes from the wire.
    fn apply(&mut self, count: u64, at: u64, now: u64) {
        self.counted = self.counted.saturating_add(count);
        let mut bucket = self.bucket();
        bucket.charge_within(at, now, self.contract.refill_time(), count);
        self.bucket = bucket.state();
    }

    fn bucket(&self) -> TokenBucket {
        let Contract {
            limit,
            window,
            burst,
        } = self.contract;
        TokenBucket::from_state(limit, window, burst, self.bucket)
    }

    /// How many data planes besides `plane` it has seen admit the key within a few cycles
    /// of the latest admission it knows of: counted back from the last traffic, not from
    /// now, so that data planes that burst together and fall quiet are bounded together
    /// when they burst again.
    fn others(&self, plane: PlaneId, lately: u64) -> u64 {
        let latest = self
            .planes
            .iter()
            .filter_map(|(_, r)| r.last_admitted.get())
            .max();
        self.planes
            .iter()
            .filter(|(id, record)| {
                *id != plane
                    && matches!((record.last_admitted.get(), latest), (Some(at), Some(latest)) if latest - at <= lately)
            })
            .count() as u64
    }

    /// Takes in what `mine` applied of the key while this copy was on its way: its count
    /// since its pod took up the view charged on top, and the furthest of each data plane's
    /// marks.
    fn merge(&mut self, mine: Owned, now: u64) {
        self.apply(mine.counted.saturating_sub(mine.since), now, now);
        for (plane, record) in mine.planes {
            let ours = self.planes.entry(plane);
            ours.applied = ours.applied.max(record.applied);
            ours.last_admitted = ours.last_admitted.max(record.last_admitted);
        }
    }

    fn replica(&self, key: &[u8], now: u64) -> Replica {
        Replica {
            key: key.into(),
            contract: self.contract,
            behind: self.bucket().behind(now),
            counted: self.counted,
            planes: self
                .planes
                .iter()
                .map(|(plane, record)| PlaneRecord {
                    plane,
                    applied: record.applied,
                    admitted_ago: record.last_admitted.get().map(|at| now.saturating_sub(at)),
                })
                .collect(),
        }
    }
}

/// A service pod's side of the protocol: the keys it owns, and a copy of every other key
/// the service holds.
pub struct ServicePod<S = FixedState> {
    me: PodId,
    cycle: u64,
    view: View,
    keys: HashMap<Box<[u8]>, Owned, S>,
    replicas: BTreeMap<PodId, Vec<Replica>>,
    handovers: BTreeMap<PodId, Vec<Replica>>,
    /// Whether the view it holds took it in: until then it answers with its view alone.
    serving: bool,
    /// Handovers made under a view later than its own, kept until it holds that view.
    pending: Vec<Replicas>,
    /// How long after joining it makes no key afresh: what it is to own may still be on
    /// its way to it, handed over.
    grace: u64,
    /// Until when it makes no key afresh.
    fresh_from: u64,
    /// The bytes its keys may hold, as [`pod_key_bytes`] counts them, what they held at the
    /// last timer, and the keys it has evicted to stay within it, all told.
    capacity: u64,
    holding: u64,
    evicted: u64,
    /// Its life, and the answers it has given in it ([`Answers::number`]).
    life: u64,
    answered: u64,
}

impl<S: BuildHasher + Default> ServicePod<S> {
    /// Service pod `me`, whose data planes send every `cycle` nanoseconds, knowing the
    /// service as `view`.
    #[must_use]
    pub fn new(me: PodId, cycle: u64, view: View) -> Self {
        Self {
            me,
            cycle: cycle.max(1),
            view,
            keys: HashMap::default(),
            replicas: BTreeMap::new(),
            handovers: BTreeMap::new(),
            serving: true,
            pending: Vec::new(),
            grace: 0,
            fresh_from: 0,
            capacity: u64::MAX,
            holding: 0,
            evicted: 0,
            life: 0,
            answered: 0,
        }
    }

    /// Its life, drawn at random when it starts and never 0, which its answers carry: a
    /// data plane takes an answer's buckets only if it is the pod's latest, and a restart
    /// starts the numbering again. 0, the default, leaves its answers unnumbered, each
    /// taken as it comes.
    pub fn set_life(&mut self, life: u64) {
        self.life = life;
    }

    /// How long after joining a view it makes no key afresh, but answers nothing for a key
    /// it does not hold: data planes go on from their copies meanwhile, and the pod that
    /// held the key has time to hand it over. A few heartbeats, as a view reaches every pod
    /// in one.
    pub fn set_join_grace(&mut self, grace: u64) {
        self.grace = grace;
    }

    /// The bytes its keys may hold, owned and copies alike: past them, its timer evicts the
    /// least recently changed keys down to nine tenths of it. A key evicted starts again
    /// full, as one forgotten does: at most a bucket forgiven. No bound unless set.
    pub fn set_capacity(&mut self, bytes: u64) {
        self.capacity = bytes;
    }

    /// The bytes its keys held at its last timer, as it counts them.
    #[must_use]
    pub fn holding(&self) -> u64 {
        self.holding
    }

    /// How many keys it holds, owned and copies.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// The keys it has evicted to stay within its capacity, all told.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Whether it serves: a pod the agreed view has not taken in, in this life, answers
    /// batches with its view and nothing more.
    pub fn set_serving(&mut self, serving: bool) {
        self.serving = serving;
    }

    /// Requests the count of `key` holds, each once, if it holds the key: hits reported.
    #[must_use]
    pub fn counted(&self, key: &[u8]) -> Option<u64> {
        self.keys.get(key).map(|owned| owned.counted)
    }

    /// A data plane's batch reaches it at `now`: the answers go back to that data plane,
    /// and the copies of what changed to every other pod, from
    /// [`take_replicas`](Self::take_replicas).
    pub fn on_batch(&mut self, batch: Batch, now: u64) -> Answers {
        let lately = ACTIVE_CYCLES * self.cycle;
        let mut answers = Vec::new();
        let from = batch.from;
        let batch_made = batch.made;
        let entries = if self.serving {
            batch.entries
        } else {
            Vec::new()
        };
        for entry in entries {
            let (owner, _) = self.view.owners(&entry.key);
            if owner != Some(self.me) {
                continue;
            }
            if !matches!(self.keys.get(&entry.key), Some(owned) if owned.contract == entry.contract)
            {
                // Just joined: the key may be on its way, handed over.
                if now < self.fresh_from {
                    continue;
                }
                self.keys
                    .insert(entry.key.clone(), Owned::new(entry.contract, now));
            }
            let Some(owned) = self.keys.get_mut(&entry.key) else {
                continue;
            };
            owned.touched = now;
            let latest = owned
                .planes
                .iter()
                .map(|(_, record)| record.last_admitted)
                .max()
                .unwrap_or_default();
            let record = owned.planes.entry(from);
            let mut changed = false;
            if let Report::Hits {
                sequence,
                count,
                seen,
                age,
            } = entry.report
            {
                changed = true;
                // Taken over from a copy that had not caught up: batches the data plane saw
                // applied, and will not send again, are skipped.
                if seen > record.applied {
                    record.skip_to(seen);
                }
                // A batch not applied yet is an admission now. One sent again, its answer
                // lost, keeps its data plane counted with those admitting, as of the latest
                // admission and no later: made the latest itself, long after a burst, it
                // would leave the others that admitted with it uncounted, and each would take
                // too large a share of the next burst.
                if sequence > record.applied {
                    record.last_admitted = Admitted::at(now);
                } else {
                    record.last_admitted = record.last_admitted.max(latest);
                }
                // Charged as of when the earliest was admitted, the batch's trip here counted
                // in with its age, however long ago: charged any later, a batch of more hits than
                // a bucket holds would be charged late, refusing a client within its rate.
                let ready = record.take(
                    sequence,
                    Tally {
                        count,
                        at: now.saturating_sub(age),
                    },
                );
                owned.apply(ready.count, ready.at, now);
            }
            let applied = owned.planes.get(from).map_or(0, |record| record.applied);
            answers.push(Answer {
                key: entry.key.clone(),
                behind: owned.bucket().behind(now),
                applied,
                others: owned.others(from, lately),
            });
            if changed {
                self.copy_out(&entry.key, now);
            }
        }
        self.answered = self.answered.wrapping_add(1);
        Answers {
            from: self.me,
            entries: answers,
            view: Some(self.view.clone()),
            life: self.life,
            number: self.answered,
            made: batch_made,
        }
    }

    /// Copies a key it owns, as it stands at `now`, to every other pod.
    fn copy_out(&mut self, key: &[u8], now: u64) {
        let Some(owned) = self.keys.get(key) else {
            return;
        };
        let replica = owned.replica(key, now);
        for &pod in &self.view.pods {
            if pod == self.me {
                continue;
            }
            let copies = self.replicas.entry(pod).or_default();
            // One copy of a key a batch: the last made holds the others.
            copies.retain(|copy| copy.key != replica.key);
            copies.push(replica.clone());
        }
    }

    /// Copies reach it at `now`: an owner's, kept for every key whose owner it knows that
    /// pod to be; or keys handed over to it as their new owner, taken over — with whatever
    /// it has applied of them meanwhile charged on top.
    pub fn on_replicas(&mut self, replicas: Replicas, now: u64) {
        if replicas.handover && replicas.epoch > self.view.epoch {
            self.pending.push(replicas);
            return;
        }
        for replica in replicas.entries {
            let (owner, _) = self.view.owners(&replica.key);
            let wanted = if replicas.handover {
                owner == Some(self.me)
            } else {
                owner == Some(replicas.from) && owner != Some(self.me)
            };
            if !wanted {
                continue;
            }
            let mut bucket = replica.contract.bucket();
            bucket.set_behind(now, replica.behind);
            let planes = replica
                .planes
                .iter()
                .map(|record| {
                    (
                        record.plane,
                        Record {
                            applied: record.applied,
                            waiting: None,
                            last_admitted: record
                                .admitted_ago
                                .map(|ago| now.saturating_sub(ago))
                                .into(),
                        },
                    )
                })
                .collect();
            let mut copy = Owned {
                contract: replica.contract,
                bucket: bucket.state(),
                counted: replica.counted,
                since: replica.counted,
                planes,
                touched: now,
            };
            if replicas.handover
                && let Some(mine) = self.keys.remove(&replica.key)
                && mine.contract == copy.contract
            {
                copy.merge(mine, now);
            }
            self.keys.insert(replica.key, copy);
        }
    }

    /// The service's pods change at `now`: keys it owns now go on from the copies it holds,
    /// and a pod joining, which holds none, is sent a copy of each at once — or the key
    /// itself, handed over, if the pod joining is its owner now.
    pub fn on_view(&mut self, view: View, now: u64) {
        if view.epoch <= self.view.epoch {
            return;
        }
        let before = std::mem::replace(&mut self.view, view);
        let joining: Vec<PodId> = self
            .view
            .pods
            .iter()
            .copied()
            .filter(|pod| *pod != self.me && !before.pods.contains(pod))
            .collect();
        for (key, owned) in &mut self.keys {
            // Whoever hands it a key from now holds what it had counted until now: it may
            // have been left out of a view it never saw.
            owned.since = owned.counted;
            let (owner, _) = self.view.owners(key);
            // Owned until now, and from now a pod's that was not serving, so holds no copy
            // of it: handed over.
            if before.owners(key).0 == Some(self.me)
                && let Some(owner) = owner
                && joining.contains(&owner)
            {
                self.handovers
                    .entry(owner)
                    .or_default()
                    .push(owned.replica(key, now));
            }
            if owner != Some(self.me) {
                continue;
            }
            for &pod in &joining {
                self.replicas
                    .entry(pod)
                    .or_default()
                    .push(owned.replica(key, now));
            }
        }
        if !before.pods.contains(&self.me) && self.view.pods.contains(&self.me) {
            self.fresh_from = now.saturating_add(self.grace);
        }
        let epoch = self.view.epoch;
        let (ready, later) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|handover| handover.epoch <= epoch);
        self.pending = later;
        for handover in ready {
            self.on_replicas(handover, now);
        }
    }

    /// The pod's timer at `now`: keys full again and quiet for a refill time are dropped,
    /// owned or copies.
    pub fn on_timer(&mut self, now: u64) {
        let mut holding = 0;
        self.keys.retain(|key, owned| {
            let keep = now.saturating_sub(owned.touched) <= owned.contract.refill_time()
                || owned.bucket().forgettable_at() > now;
            if keep {
                holding += pod_key_bytes(key, owned);
            }
            keep
        });
        self.holding = holding;
        if self.holding > self.capacity {
            let all = self
                .keys
                .iter()
                .map(|(key, owned)| (owned.touched, key.clone(), pod_key_bytes(key, owned)))
                .collect();
            let target = self.capacity - self.capacity / 10;
            for key in least_recently_used(all, self.holding, target) {
                if let Some(owned) = self.keys.remove(&key) {
                    self.holding = self.holding.saturating_sub(pod_key_bytes(&key, &owned));
                    self.evicted += 1;
                }
            }
        }
    }

    /// Copies to send: one message per other pod, and one per pod keys are handed over to.
    pub fn take_replicas(&mut self) -> Vec<(PodId, Replicas)> {
        let me = self.me;
        let epoch = self.view.epoch;
        let message = |handover| {
            move |(pod, entries)| {
                (
                    pod,
                    Replicas {
                        from: me,
                        entries,
                        handover,
                        epoch,
                    },
                )
            }
        };
        let handovers = std::mem::take(&mut self.handovers)
            .into_iter()
            .map(message(true));
        std::mem::take(&mut self.replicas)
            .into_iter()
            .map(message(false))
            .chain(handovers)
            .collect()
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

#[cfg(test)]
mod tests;
