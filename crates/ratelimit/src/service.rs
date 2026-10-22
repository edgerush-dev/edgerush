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
//!   hits it admitted, numbered; or, with none to send while the key is in use, a poll. Each
//!   is answered with the bucket as it stands. A batch not seen applied is sent again after
//!   four cycles; one older than a bucket's refill time is sent emptied.
//! - A request under several limits is admitted under all of them or none, and charged
//!   to all or none. Its limits whose keys the data plane holds fresh copies of are decided
//!   there first, and held; the rest wait while the data plane asks one service pod — the
//!   owner of the first, the limits ordered smallest bucket first — which decides the
//!   waiting requests against its own bucket for the keys it owns and its copies for the
//!   others. An ask unanswered for two cycles is sent again once; one unanswered for four
//!   is given up, and requests are decided from the copies until a pod is heard from
//!   again.
//! - The owner applies hits in order, once each, charging what does not fit as debt while
//!   it would be paid back within one bucket's refill time, and copies every change it makes
//!   to every other service pod: the next to own the key takes over from its copy should
//!   the owner go, with nothing to send anyone.
//!
//! Which pods own which keys follows from the [`View`] the service pods agree on: the owner
//! of a key is the pod ranking highest for it (rendezvous hashing), and the next in rank
//! takes it over, so a pod leaving moves only the keys it held.

pub mod keyed;
pub mod membership;
pub mod pooled;

pub use keyed::Keyed;

use crate::reference::{BucketState, Charge, Model, TokenBucket};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::num::NonZeroU64;

/// How many cycles a data plane waits to see a batch applied, or an ask answered, before
/// sending it again (a batch) or giving it up (an ask).
pub const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles before the latest admission the owner knows of a data plane counts
/// as admitting the key: 300 ms at a 10 ms cycle, longer than TCP takes to send a lost
/// segment again. A data plane whose batches are held up behind a lost one must not drop out
/// of the count, or every other one's share grows while it is stalled: four cycles let bursts
/// through at 1.44 times the contract with one packet in ten lost (simulator, TCP-like loss).
pub const ACTIVE_CYCLES: u64 = 30;

/// For how many cycles at least, after a data plane last admitted the key, its owner holds
/// what the data plane's asks admitted, for the hits they are reported as, unless set
/// ([`ServicePod::set_least_hold`]): the key's refill time if that is longer, as a data plane
/// sends its hits for as long, and 400 ms at a 10 ms cycle at least, past the 300 ms the
/// proxy waits for an ask's answer before giving it up and deciding its requests itself,
/// whose hits then still find the hold. What no hits settle by then — the data plane gone,
/// or an ask that arrived again decided twice — lapses, staying charged and counted.
pub const HOLD_CYCLES: u64 = 40;

/// A data plane: one worker of one EdgeRush pod, in one life of it. A restarted worker is a
/// new data plane, with a new id.
pub type PlaneId = u64;

/// A service pod: its ordinal among the service's pods.
pub type PodId = u32;

/// A request waiting for the owner's decision.
pub type Ticket = u64;

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

/// Requests waiting for a service pod to decide them under the keys the data plane holds
/// no fresh copy of, each admitted under all of them or none: sent to the owner of the
/// first, which decides the keys it owns exactly and the others from its copies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ask {
    /// The ask's number among this data plane's asks, from 1: an ask sent again keeps it.
    pub id: u64,
    /// How many requests wait.
    pub count: u64,
    /// The keys, with their contracts, in the order the requests' limits are in: at least
    /// one, and each once, as a request is under each of its limits once.
    pub keys: Vec<(Box<[u8]>, Contract)>,
}

/// A data plane's message to a service pod: everything it has for the keys that pod owns,
/// and the requests it asks that pod to decide.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The data plane.
    pub from: PlaneId,
    /// One entry per key, or a few for one key (batches sent again).
    pub entries: Vec<Entry>,
    /// Requests waiting for a decision.
    pub asks: Vec<Ask>,
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
    /// Whether the pod owns the key, rather than answering an ask from its copy: hits for
    /// a key go to its owner, whoever decided them.
    pub owned: bool,
}

/// A service pod's answer to a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answers {
    /// The service pod.
    pub from: PodId,
    /// One per entry it owns, and one per key of each ask it decided.
    pub entries: Vec<Answer>,
    /// To each ask decided: its id, and how many of the requests it was about were
    /// admitted, the first so many; the rest were refused.
    pub decided: Vec<(u64, u64)>,
    /// The view it holds: a data plane holding an earlier one takes it over.
    pub view: Option<View>,
    /// The pod's life: drawn at random when it starts, never 0. 0 is a pod that does not
    /// number its answers.
    pub life: u64,
    /// Counted up, wrapping, with each answer the pod gives in this life: the order in
    /// which it handled their batches.
    pub number: u64,
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
    /// Its last ask decided under the key, and how many were admitted.
    pub last_ask: (u64, u64),
    /// What the owner's answers to its asks admitted that its hits have not yet settled:
    /// charged on the bucket, held for those hits ([`HOLD_CYCLES`]).
    pub held: u64,
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

/// The decision on a request that waited.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decided {
    /// The request.
    pub ticket: Ticket,
    /// Whether it was admitted.
    pub admitted: bool,
    /// Refused, those of its keys that refused it, by position in its list, each with how
    /// long from the decision it waits to admit one: as the service pod that decided held
    /// them, or, decided here, as the copies have them. Empty when admitted.
    pub refused_by: Box<[(usize, u64)]>,
}

/// How a request was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Admitted.
    Admit,
    /// Refused.
    Refuse,
    /// Waiting for a service pod: its decision comes from [`DataPlane::take_decided`].
    Wait(Ticket),
}

/// What `admitting` says the asks decided admitted under `key`.
fn admitted_under(admitting: &[(Box<[u8]>, u64)], key: &[u8]) -> u64 {
    admitting
        .iter()
        .filter(|(asked, _)| **asked == *key)
        .map(|(_, admitted)| admitted)
        .sum()
}

/// What a data plane holds of one key.
struct PlaneKey {
    contract: Contract,
    /// Its contract's refill time, worked out once: how long a copy stays fresh.
    hold: u64,
    /// The service pod it sends to.
    owner: Option<PodId>,
    /// The owner's bucket as last heard, charged with its own hits not yet seen applied,
    /// with what it has admitted since, and with what it holds.
    bucket: TokenBucket,
    /// Hits admitted and not yet sent.
    unsent: u64,
    /// Batches sent to the present owner so far.
    sent: u64,
    /// The highest of them the owner has been seen to apply.
    seen: u64,
    /// Batches sent and not yet seen applied: `(sequence, count, first sent, last sent)`.
    unacknowledged: VecDeque<(u64, u64, u64, u64)>,
    /// Admitted in the cycle under way.
    spent: u64,
    /// The most it may admit in the cycle under way, once a cycle has set it.
    allowance: Option<u64>,
    /// When a cycle last shared out what the copy has left.
    shared: Option<u64>,
    /// Whether a cycle has set the allowance from a copy a pod answered with: until then
    /// the copy is shared out among no one, and a pod decides.
    bounded: bool,
    /// How many other data planes the owner last said were admitting the key.
    others: u64,
    /// When a request for the key last came.
    last_request: Option<u64>,
    /// When a pod last answered about it: the copy is fresh for a refill time after.
    heard: Option<u64>,
    /// Asks in flight about it.
    asking: u32,
    /// Requests charged here and held while an ask about their other keys is in flight: sent
    /// to the owner if they are admitted, given back if not.
    held: u64,
    /// Whether an ask about it went unanswered since a pod was last heard from about it:
    /// until one is, requests are decided here.
    unreachable: bool,
}

impl PlaneKey {
    fn new(contract: Contract, owner: Option<PodId>, numbered: u64) -> Self {
        Self {
            contract,
            hold: contract.refill_time(),
            owner,
            bucket: contract.bucket(),
            unsent: 0,
            sent: numbered,
            seen: numbered,
            unacknowledged: VecDeque::new(),
            spent: 0,
            allowance: None,
            shared: None,
            bounded: false,
            others: 0,
            last_request: None,
            heard: None,
            asking: 0,
            held: 0,
            unreachable: false,
        }
    }

    /// Whether a pod must decide a request for it: no copy fresh enough, none bounded yet,
    /// or an ask about it in flight. A copy an ask just brought is not bounded until a cycle
    /// after it shares out what it holds among the data planes admitting the key, as the
    /// answers say them; until then another data plane may hold the same copy. Requests come
    /// over a few milliseconds, not all at once: the first ask's answer comes back while the
    /// rest of a burst is still arriving.
    fn cold(&self, now: u64) -> bool {
        self.owner.is_some()
            && !self.unreachable
            && (self.asking > 0
                || !self.bounded
                || self.heard.is_none_or(|heard| now - heard > self.hold))
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

    /// Charges a request held while a pod decides its other keys.
    fn hold(&mut self, now: u64) -> Charge {
        self.spent += 1;
        self.held += 1;
        self.bucket.charge(now)
    }

    /// A request held: admitted, it goes to the owner; refused, its charge is given back.
    fn release(&mut self, charge: Charge, admitted: bool) {
        self.held = self.held.saturating_sub(1);
        if admitted {
            self.unsent += 1;
        } else {
            self.spent = self.spent.saturating_sub(1);
            self.bucket.release(charge);
        }
    }

    /// Whether nothing is left to do for the key and its copy would be cold anyway.
    fn idle(&self, now: u64) -> bool {
        self.unsent == 0
            && self.held == 0
            && self.asking == 0
            && self.unacknowledged.is_empty()
            && self
                .last_request
                .is_none_or(|last| now.saturating_sub(last) > 2 * self.hold)
    }
}

/// An ask in flight.
struct InFlight {
    id: u64,
    /// The pod it went to.
    pod: PodId,
    /// When it was first sent.
    sent: u64,
    /// Whether it has been sent again.
    resent: bool,
}

/// Requests under one list of keys waiting for a service pod's decision, and the ask
/// about them.
struct Group {
    /// The requests' keys, in their limits' order, with their contracts.
    keys: Vec<(Box<[u8]>, Contract)>,
    /// Which of them are asked about, by position; the others are decided here and held.
    /// Fixed for as long as the group waits.
    cold: Vec<usize>,
    asking: Option<InFlight>,
    /// The requests the ask in flight is about.
    asked: Vec<Ticket>,
    /// Requests that came while it was in flight.
    waiting: Vec<Ticket>,
    /// Whether the ask in flight is the follow-up for a cold burst's tail.
    follow_up: bool,
}

/// A group's name: its keys, each after its length, so that no two lists share one.
fn group_name<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Box<[u8]> {
    let mut name = Vec::new();
    for key in keys {
        name.extend_from_slice(&(key.len() as u64).to_le_bytes());
        name.extend_from_slice(key);
    }
    name.into_boxed_slice()
}

/// A data plane's side of the protocol: one worker's copies of the keys it has seen.
pub struct DataPlane<S = FixedState> {
    id: PlaneId,
    cycle: u64,
    view: View,
    keys: HashMap<Box<[u8]>, PlaneKey, S>,
    /// Requests waiting for a pod, by their list of keys.
    groups: HashMap<Box<[u8]>, Group, S>,
    /// The charges held for each request waiting, one per key of its group not asked
    /// about, in the group's order.
    holds: BTreeMap<Ticket, Vec<Charge>>,
    /// The group of each ask in flight, by its id.
    in_flight: BTreeMap<u64, Box<[u8]>>,
    next_ticket: Ticket,
    asks_sent: u64,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    decided: Vec<Decided>,
    outgoing: BTreeMap<PodId, Vec<Entry>>,
    asks: BTreeMap<PodId, Vec<Ask>>,
    /// Views asked for so far, to ask the pods in turn.
    polls: u64,
    /// How long an ask may go unanswered before it is given up and what waits is decided
    /// here; sent again once half way.
    ask_timeout: u64,
    /// The bytes its keys may hold, as [`plane_key_bytes`] counts them, and what they hold.
    capacity: u64,
    holding: u64,
    /// Keys it has evicted to stay within its capacity, all told.
    evicted: u64,
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
            keys: HashMap::with_hasher(hasher.clone()),
            groups: HashMap::with_hasher(hasher),
            holds: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            next_ticket: 0,
            asks_sent: 0,
            numbered: 0,
            decided: Vec::new(),
            outgoing: BTreeMap::new(),
            asks: BTreeMap::new(),
            polls: 0,
            ask_timeout: RESEND_AFTER_CYCLES * cycle.max(1),
            capacity: u64::MAX,
            holding: 0,
            evicted: 0,
        }
    }

    /// How long an ask may go unanswered before it is given up: four cycles unless said
    /// otherwise. Over TCP a lost segment is sent again only after some 200 ms, and an ask
    /// given up sooner leaves its requests to a copy that has heard nothing.
    pub fn set_ask_timeout(&mut self, timeout: u64) {
        self.ask_timeout = timeout.max(1);
    }

    /// The bytes its keys may hold: past them, each cycle evicts the least recently used
    /// keys with nothing in flight, down to nine tenths of it. A key evicted starts again
    /// full, as one forgotten does. No bound unless set.
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
        // The common case, with one lookup: a key held under the same contract, warm.
        if let Some(at) = self.keys.get_mut(key)
            && at.contract == contract
            && !at.cold(now)
        {
            return if at.decide(now) {
                Decision::Admit
            } else {
                Decision::Refuse
            };
        }
        self.decide_held_or_asked(&[(key, contract)], now)
    }

    /// Decides a request arriving at `now` under every limit that applies to it, each a key
    /// under its contract, in an order fixed when the limits were compiled — the smallest
    /// bucket first (06 §4): admitted under all of them or none, and charged to all of them
    /// or none (06 §2).
    ///
    /// Keys it holds fresh copies of are decided here, and any refusing refuses at once.
    /// If some are cold, it holds the charges of the others and waits while it asks the
    /// owner of the first cold key to decide the cold ones; the ask is among the messages
    /// [`take_outgoing`](Self::take_outgoing) gives, to be sent at once.
    pub fn decide_all(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        // Hashing a key is a third of a decision: the usual few are looked up together,
        // once each.
        match keys {
            [one] => self.decide(one.0, one.1, now),
            [a, b] => self.decide_warm([a, b], now),
            [a, b, c] => self.decide_warm([a, b, c], now),
            [a, b, c, d] => self.decide_warm([a, b, c, d], now),
            _ => self.decide_held_or_asked(keys, now),
        }
    }

    /// The common case for a few limits, with one lookup a key: every key held under the
    /// same contract, warm, decided here.
    fn decide_warm<const N: usize>(&mut self, keys: [&(&[u8], Contract); N], now: u64) -> Decision {
        // Looking the same key up twice at once is refused by the map; limits each have a
        // key of their own, so it is a caller's mistake, decided the long way.
        for (at, (key, _)) in keys.iter().enumerate() {
            if keys[..at].iter().any(|(other, _)| other == key) {
                return self.decide_held_or_asked(&keys.map(|key| *key), now);
            }
        }
        let mut found = self.keys.get_disjoint_mut(keys.map(|(key, _)| *key));
        let mut warm = true;
        let mut room = true;
        for (at, (_, contract)) in found.iter_mut().zip(keys) {
            match at {
                Some(at) if at.contract == *contract && !at.cold(now) => {
                    at.last_request = Some(now);
                    room &= at.room(now);
                }
                _ => {
                    warm = false;
                    break;
                }
            }
        }
        if !warm {
            return self.decide_held_or_asked(&keys.map(|key| *key), now);
        }
        if !room {
            return Decision::Refuse;
        }
        for at in found.iter_mut().flatten() {
            at.take(now);
        }
        Decision::Admit
    }

    /// Decides a request with a key new to it, under a changed contract, or cold: kept out
    /// of the common case's way.
    #[cold]
    #[inline(never)]
    fn decide_held_or_asked(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        let mut cold = false;
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
            cold |= at.cold(now);
        }
        if !cold {
            let room = keys
                .iter()
                .all(|(key, _)| self.keys.get(*key).is_some_and(|at| at.room(now)));
            if room {
                for (key, _) in keys {
                    if let Some(at) = self.keys.get_mut(*key) {
                        at.take(now);
                    }
                }
                return Decision::Admit;
            }
            return Decision::Refuse;
        }
        self.wait(keys, now)
    }

    /// A request with a cold key: the keys its group does not ask about decided and held
    /// here, then waiting for the ask in flight, or asking.
    fn wait(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        let name = group_name(keys.iter().map(|(key, _)| *key));
        if !self.groups.contains_key(&name) {
            let cold = keys
                .iter()
                .enumerate()
                .filter(|(_, (key, _))| self.keys.get(*key).is_some_and(|at| at.cold(now)))
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
        let Some(group) = self.groups.get_mut(&name) else {
            return Decision::Refuse;
        };
        let held = |at: &usize| !group.cold.contains(at);
        let room = (0..group.keys.len()).filter(held).all(|at| {
            self.keys
                .get(&group.keys[at].0)
                .is_some_and(|key| key.room(now))
        });
        if !room {
            if group.asking.is_none() && group.waiting.is_empty() {
                self.groups.remove(&name);
            }
            return Decision::Refuse;
        }
        let charges = (0..group.keys.len())
            .filter(held)
            .filter_map(|at| {
                self.keys
                    .get_mut(&group.keys[at].0)
                    .map(|key| key.hold(now))
            })
            .collect();
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.holds.insert(ticket, charges);
        if group.asking.is_some() {
            group.waiting.push(ticket);
        } else {
            group.follow_up = false;
            group.asked.push(ticket);
            self.ask(&name, None, now);
        }
        Decision::Wait(ticket)
    }

    /// Sends the ask about the requests the group `name` has in `asked` to the owner of its
    /// first cold key: under a new id, or `again` under the one it had.
    fn ask(&mut self, name: &[u8], again: Option<u64>, now: u64) {
        let Some(group) = self.groups.get_mut(name) else {
            return;
        };
        let Some(pod) = group
            .cold
            .first()
            .and_then(|at| self.keys.get(&group.keys[*at].0))
            .and_then(|key| key.owner)
        else {
            return;
        };
        let id = match again {
            Some(id) => id,
            None => {
                self.asks_sent += 1;
                for at in &group.cold {
                    if let Some(key) = self.keys.get_mut(&group.keys[*at].0) {
                        key.asking += 1;
                    }
                }
                self.in_flight.insert(self.asks_sent, name.into());
                self.asks_sent
            }
        };
        let resent = group
            .asking
            .as_ref()
            .is_some_and(|f| f.id == id && f.resent);
        let sent = group
            .asking
            .as_ref()
            .filter(|f| f.id == id)
            .map_or(now, |f| f.sent);
        group.asking = Some(InFlight {
            id,
            pod,
            sent,
            resent,
        });
        self.asks.entry(pod).or_default().push(Ask {
            id,
            count: group.asked.len() as u64,
            keys: group
                .cold
                .iter()
                .map(|at| group.keys[*at].clone())
                .collect(),
        });
    }

    /// The ask in flight for the group `name` is over — answered, or given up: its keys are
    /// no longer asked about by it.
    fn ask_over(&mut self, name: &[u8]) -> Option<InFlight> {
        let group = self.groups.get_mut(name)?;
        let flight = group.asking.take()?;
        self.in_flight.remove(&flight.id);
        for at in &group.cold {
            if let Some(key) = self.keys.get_mut(&group.keys[*at].0) {
                key.asking = key.asking.saturating_sub(1);
            }
        }
        Some(flight)
    }

    /// Decides here a request of the group `name` that waited: its asked-about keys from
    /// their copies, its held ones admitted or given back with them.
    fn settle_here(&mut self, name: &[u8], ticket: Ticket, now: u64) {
        let Some(group) = self.groups.get(name) else {
            return;
        };
        let admitted = group.cold.iter().all(|at| {
            self.keys
                .get(&group.keys[*at].0)
                .is_some_and(|key| key.room(now))
        });
        if admitted {
            for at in &group.cold {
                if let Some(key) = self.keys.get_mut(&group.keys[*at].0) {
                    key.take(now);
                }
            }
        }
        let refused_by = if admitted {
            Box::default()
        } else {
            group
                .cold
                .iter()
                .filter_map(|at| {
                    let wait = self.keys.get(&group.keys[*at].0)?.wait(self.cycle, now)?;
                    Some((*at, wait))
                })
                .collect()
        };
        self.settle_held(name, ticket, admitted);
        self.decided.push(Decided {
            ticket,
            admitted,
            refused_by,
        });
    }

    /// The charges held for `ticket`, of the group `name`, as its decision says.
    fn settle_held(&mut self, name: &[u8], ticket: Ticket, admitted: bool) {
        let Some(group) = self.groups.get(name) else {
            return;
        };
        let charges = self.holds.remove(&ticket).unwrap_or_default();
        let held = (0..group.keys.len()).filter(|at| !group.cold.contains(at));
        for (at, charge) in held.zip(charges) {
            if let Some(key) = self.keys.get_mut(&group.keys[at].0) {
                key.release(charge, admitted);
            }
        }
    }

    /// The cycle's work at `now`: allowances set for the next cycle, asks sent again or
    /// given up, batches and polls for every owner.
    pub fn on_cycle(&mut self, now: u64) {
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        let ask_timeout = self.ask_timeout;
        let cycle = self.cycle;
        // Asks: one unanswered half its timeout is sent again once, as it was; one
        // unanswered for all of it is given up — its pod gone or out of reach — and what
        // waits is decided here, and so is every request for its keys after, without
        // asking, until a pod is heard from about them again.
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
            let waited: Vec<Ticket> = std::mem::take(&mut group.asked)
                .into_iter()
                .chain(std::mem::take(&mut group.waiting))
                .collect();
            for at in group.cold.clone() {
                if let Some(key) = self.keys.get_mut(&group.keys[at].0) {
                    key.unreachable = true;
                }
            }
            for ticket in waited {
                self.settle_here(&name, ticket, now);
            }
            self.groups.remove(&name);
        }
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
                let sees = at.bucket.remaining(now).saturating_add(refill);
                (sees / (at.others + 1)).max(1) + left.min(1)
            });
            at.bounded |= at.heard.is_some();
            at.spent = 0;
            let Some(owner) = at.owner else {
                continue;
            };
            let mut entries = Vec::new();
            // A batch not seen applied within a refill time is emptied, and still sent, as
            // the owner applies in order.
            for (_, count, first, _) in &mut at.unacknowledged {
                if now - *first > hold {
                    *count = 0;
                }
            }
            let mut reported = false;
            for (sequence, count, _, last) in &mut at.unacknowledged {
                if now - *last >= resend_after {
                    *last = now;
                    entries.push(Entry {
                        key: key.clone(),
                        contract: at.contract,
                        report: Report::Hits {
                            sequence: *sequence,
                            count: *count,
                            seen: at.seen,
                        },
                    });
                    reported = true;
                }
            }
            if at.unsent > 0 {
                at.sent += 1;
                self.numbered += 1;
                entries.push(Entry {
                    key: key.clone(),
                    contract: at.contract,
                    report: Report::Hits {
                        sequence: at.sent,
                        count: at.unsent,
                        seen: at.seen,
                    },
                });
                at.unacknowledged.push_back((at.sent, at.unsent, now, now));
                at.unsent = 0;
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
            // Cut off: an ask given up, or a key in use whose owner has not answered for as
            // long as an ask is given up after.
            cut_off |= at.unreachable
                || (in_use && at.heard.is_none_or(|heard| now - heard >= resend_after));
            if !entries.is_empty() {
                self.outgoing.entry(owner).or_default().extend(entries);
            }
        }
        // An owner out of reach may have gone: the pods are asked for their view in turn,
        // an empty batch each cycle, until one has a later view or the owner answers.
        if cut_off && !self.view.pods.is_empty() {
            let at = usize::try_from(self.polls).unwrap_or(0) % self.view.pods.len();
            self.polls += 1;
            self.outgoing.entry(self.view.pods[at]).or_default();
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
            let quiet = self
                .keys
                .iter()
                .filter(|(_, at)| {
                    at.unsent == 0 && at.held == 0 && at.asking == 0 && at.unacknowledged.is_empty()
                })
                .map(|(key, at)| {
                    let used = at.last_request.unwrap_or(0);
                    (used, key.clone(), plane_key_bytes(key))
                })
                .collect();
            let target = self.capacity - self.capacity / 10;
            for key in least_recently_used(quiet, self.holding, target) {
                if self.keys.remove(&key).is_some() {
                    self.holding = self.holding.saturating_sub(plane_key_bytes(&key));
                    self.evicted += 1;
                }
            }
        }
    }

    /// Of the asks `decided`, what each admitted under each of its keys asked about.
    fn admitting(&self, decided: &[(u64, u64)]) -> Vec<(Box<[u8]>, u64)> {
        let mut admitting = Vec::new();
        for (id, admitted) in decided {
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
        admitting
    }

    /// A service pod's answers reach it at `now`.
    pub fn on_answers(&mut self, answers: Answers, now: u64) {
        let Answers {
            from,
            entries,
            decided,
            view,
            ..
        } = answers;
        // Keys the pod decided asks under without owning them: the requests it admitted
        // are charged on its copy, and still to be sent to their owners.
        // Keys whose state the pod sent but the copies did not take, for saying which of
        // them refused the asks it decided.
        let mut untaken: Vec<(Box<[u8]>, u128)> = Vec::new();
        // What the asks decided admitted under each of their keys: still to be sent, as
        // hits, and left out of the owner's bucket, which holds them for those hits.
        let admitting = self.admitting(&decided);
        for answer in entries {
            let Some(at) = self.keys.get_mut(&answer.key) else {
                continue;
            };
            // Only the owner it sends to, or a pod answering an ask from its copy: an answer
            // from an owner that has gone is stale.
            if answer.owned && at.owner != Some(from) {
                untaken.push((answer.key, answer.behind));
                continue;
            }
            at.bucket.set_behind(now, answer.behind);
            at.others = answer.others;
            // Only the owner says which batches it applied: another pod's copy may be fresher
            // than the owner's own state, which after a failover went on from an older one.
            if answer.owned {
                // No more of this data plane's batches can have been applied than it has sent.
                let seen = answer.applied.min(at.sent);
                at.seen = at.seen.max(seen);
                at.unacknowledged
                    .retain(|(sequence, _, _, _)| *sequence > seen);
            }
            let unseen: u64 = at.unacknowledged.iter().map(|(_, count, _, _)| count).sum();
            let asked = if answer.owned {
                admitted_under(&admitting, &answer.key)
            } else {
                0
            };
            for _ in 0..unseen + at.unsent + at.held + asked {
                at.bucket.charge(now);
            }
            at.heard = Some(now);
            at.unreachable = false;
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
            // Those of the keys asked about that refused, as the pod held them: from the copy,
            // which took its state, or from the state the copy did not take.
            let refused_by: Box<[(usize, u64)]> = if (asked.len() as u64) > admitted {
                group
                    .cold
                    .iter()
                    .filter_map(|at| {
                        let key = &group.keys[*at].0;
                        let copy = self.keys.get(key)?;
                        let wait = match untaken.iter().find(|(untaken, _)| untaken == key) {
                            Some((_, behind)) => {
                                let mut held = copy.bucket.clone();
                                held.set_behind(now, *behind);
                                held.retry_at(now).map(|retry| retry.saturating_sub(now))
                            }
                            None => copy.wait(self.cycle, now),
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
                        if let Some(at) = self.keys.get_mut(key) {
                            at.unsent += 1;
                        }
                    }
                }
                self.settle_held(&name, ticket, admitted);
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
            // Those that came during the first ask are asked about once more — the tail of
            // a cold burst — and, once a cycle has bounded the copies, any after that are
            // decided here: asking again and again would keep a data plane whose requests
            // come faster than a round trip waiting on every one.
            let Some(group) = self.groups.get_mut(&name) else {
                continue;
            };
            if group.waiting.is_empty() {
                self.groups.remove(&name);
                continue;
            }
            let bounded = group.cold.iter().all(|at| {
                self.keys
                    .get(&group.keys[*at].0)
                    .is_some_and(|key| key.bounded)
            });
            if group.follow_up && bounded {
                let waiting = std::mem::take(&mut group.waiting);
                for ticket in waiting {
                    self.settle_here(&name, ticket, now);
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

    /// The service's pods change at `now`. Keys whose owner moved send it again every
    /// batch not seen applied, numbered as before — a pod taking over from its copy knows
    /// what the old owner applied — and asks whose pod no longer owns their first key are
    /// sent again to the one that does.
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
            at.unreachable = false;
            let Some(owner) = owner else {
                continue;
            };
            let entries = self.outgoing.entry(owner).or_default();
            for (sequence, count, _, last) in &mut at.unacknowledged {
                *last = now;
                entries.push(Entry {
                    key: key.clone(),
                    contract: at.contract,
                    report: Report::Hits {
                        sequence: *sequence,
                        count: *count,
                        seen: at.seen,
                    },
                });
            }
        }
        let moved: Vec<(Box<[u8]>, u64)> = self
            .groups
            .iter()
            .filter_map(|(name, group)| {
                let flight = group.asking.as_ref()?;
                let first = group.cold.first()?;
                let owner = self.keys.get(&group.keys[*first].0)?.owner;
                (owner != Some(flight.pod)).then(|| (name.clone(), flight.id))
            })
            .collect();
        for (name, id) in moved {
            self.ask(&name, Some(id), now);
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
    pub fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
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
                },
            )
        }));
        batches.sort_by_key(|(pod, _)| *pod);
        batches
    }

    /// Requests that waited, decided since last asked: `(ticket, admitted)`.
    pub fn take_decided(&mut self) -> Vec<(Ticket, bool)> {
        self.take_decisions()
            .into_iter()
            .map(|decided| (decided.ticket, decided.admitted))
            .collect()
    }

    /// The same, with which keys refused those refused.
    pub fn take_decisions(&mut self) -> Vec<Decided> {
        std::mem::take(&mut self.decided)
    }

    /// The requests waiting, refused: the data plane is going away.
    pub fn abandon(&mut self) -> Vec<Ticket> {
        self.holds.clear();
        self.in_flight.clear();
        self.groups
            .drain()
            .flat_map(|(_, group)| group.asked.into_iter().chain(group.waiting))
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
    waiting: Option<Box<Vec<(u64, u64)>>>,
    last_admitted: Admitted,
    last_ask: (u64, u64),
    /// What its asks were admitted, charged and counted, that its hits have not yet
    /// settled: its next hits settle it rather than being charged, until it lapses
    /// ([`Owned::lapse`]).
    held: u64,
}

/// When a data plane last admitted a key, if it has, in eight bytes rather than an option's
/// sixteen: a record's room, kept for every data plane admitting every key, holds what is
/// held for it beside. One past the time, 0 if never; ordered as the option is, never first.
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
    fn take(&mut self, sequence: u64, count: u64) -> u64 {
        if sequence <= self.applied {
            return 0;
        }
        if sequence > self.applied + 1 {
            let waiting = self.waiting.get_or_insert_default();
            match waiting.binary_search_by_key(&sequence, |(at, _)| *at) {
                Ok(at) => waiting[at] = (sequence, count),
                Err(at) => waiting.insert(at, (sequence, count)),
            }
            return 0;
        }
        self.applied = sequence;
        let mut ready = count;
        if let Some(waiting) = &mut self.waiting {
            // All ahead of it: the first of them follow on from it.
            let mut caught_up = 0;
            for &(at, waited) in waiting.iter() {
                if at != self.applied + 1 {
                    break;
                }
                self.applied = at;
                // Counts come from the wire: as many as a count holds, never wrapped round.
                ready = ready.saturating_add(waited);
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

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Record> {
        self.0.iter_mut().map(|(_, record)| record)
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
            planes: Records::default(),
            touched: now,
        }
    }

    /// `count` reported hits, each charged if there is room, or as debt while it would be
    /// paid back within a refill time; the rest forgiven. In one step, whatever the count:
    /// it comes from the wire.
    fn apply(&mut self, count: u64, now: u64) {
        self.counted = self.counted.saturating_add(count);
        let mut bucket = self.bucket();
        let owed = bucket.remaining_within(now, self.contract.refill_time(), count);
        bucket.charge_many(now, owed);
        self.bucket = bucket.state();
    }

    /// Ends what is held for data planes that have admitted nothing for the key's refill
    /// time, or `least_hold` if longer, at `now`: it stays charged and counted, and hits
    /// arriving after are charged as any other — a request counted twice if they were its
    /// own. Not given back: taken off a bucket that had since refilled to full, it would free
    /// room later charges took; and a data plane gives up sending hits it has not seen applied
    /// within a refill time, whose requests would then go uncounted.
    fn lapse(&mut self, least_hold: u64, now: u64) {
        let hold_for = self.contract.refill_time().max(least_hold);
        for record in self.planes.iter_mut() {
            let due = record
                .last_admitted
                .get()
                .is_none_or(|at| now.saturating_sub(at) >= hold_for);
            if due {
                record.held = 0;
            }
        }
    }

    /// The bucket as `plane` is to take it: without what is held for it, which its own
    /// ledger carries as hits still to report.
    fn behind_for(&self, plane: PlaneId, now: u64) -> u128 {
        let held = self.planes.get(plane).map_or(0, |record| record.held);
        self.bucket()
            .behind(now)
            .saturating_sub(u128::from(held) * u128::from(self.contract.window.get()))
    }

    fn bucket(&self) -> TokenBucket {
        let Contract {
            limit,
            window,
            burst,
        } = self.contract;
        TokenBucket::from_state(limit, window, burst, self.bucket)
    }

    fn charge(&mut self, now: u64, count: u64) {
        let mut bucket = self.bucket();
        bucket.charge_many(now, count);
        self.bucket = bucket.state();
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

    /// Takes in what `mine` applied of the key while this copy was on its way: its count —
    /// hits, and what it holds for the asks it decided — charged on top, what it holds kept
    /// for the hits to come, and the furthest of each data plane's marks.
    fn merge(&mut self, mine: Owned, now: u64) {
        self.apply(mine.counted, now);
        for (plane, record) in mine.planes {
            let ours = self.planes.entry(plane);
            ours.applied = ours.applied.max(record.applied);
            ours.last_admitted = ours.last_admitted.max(record.last_admitted);
            if record.last_ask.0 > ours.last_ask.0 {
                ours.last_ask = record.last_ask;
            }
            // Charged and counted with the rest of its count, above.
            ours.held = ours.held.saturating_add(record.held);
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
                    last_ask: record.last_ask,
                    held: record.held,
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
    /// The least a key's asked requests are held for their data plane's hits.
    least_hold: u64,
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
            least_hold: HOLD_CYCLES * cycle.max(1),
        }
    }

    /// The least time after a data plane last admitted a key that what its asks were
    /// admitted is held, charged and counted, for its hits to settle rather than be charged
    /// again; the key's refill time if that is longer ([`HOLD_CYCLES`]).
    pub fn set_least_hold(&mut self, least_hold: u64) {
        self.least_hold = least_hold;
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

    /// Requests the count of `key` holds, each once, if it holds the key: hits reported, and
    /// what asks were admitted that their data planes have not yet reported, held for it.
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
        let (entries, asks) = if self.serving {
            (batch.entries, batch.asks)
        } else {
            (Vec::new(), Vec::new())
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
            owned.lapse(self.least_hold, now);
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
                let ready = record.take(sequence, count);
                // What is held for the data plane settles first: counted and charged when
                // its ask was decided. Any hit settles it, not only those of the requests
                // its ask admitted — a hold is a charge like any other — and the bucket ends
                // charged once for every hit either way.
                let settled = ready.min(record.held);
                record.held -= settled;
                owned.apply(ready - settled, now);
            }
            let applied = owned.planes.get(from).map_or(0, |record| record.applied);
            answers.push(Answer {
                key: entry.key.clone(),
                behind: owned.behind_for(from, now),
                applied,
                others: owned.others(from, lately),
                owned: true,
            });
            if changed {
                self.copy_out(&entry.key, now);
            }
        }
        let mut decided = Vec::new();
        for ask in asks {
            if let Some(admitted) = self.decide_ask(from, &ask, now) {
                decided.push((ask.id, admitted));
                for (key, _) in &ask.keys {
                    let Some(held) = self.keys.get(key) else {
                        continue;
                    };
                    let owned = self.view.owners(key).0 == Some(self.me);
                    answers.push(Answer {
                        key: key.clone(),
                        behind: if owned {
                            held.behind_for(from, now)
                        } else {
                            held.bucket().behind(now)
                        },
                        applied: held.planes.get(from).map_or(0, |record| record.applied),
                        others: held.others(from, lately),
                        owned,
                    });
                }
            }
        }
        self.answered = self.answered.wrapping_add(1);
        Answers {
            from: self.me,
            entries: answers,
            decided,
            view: Some(self.view.clone()),
            life: self.life,
            number: self.answered,
        }
    }

    /// Decides the requests of `ask`, from data plane `from`, each under all its keys or
    /// none: the keys it owns exactly, counted, held for the hits the data plane will report
    /// them as, and copied to the other pods; the others from its copies, charged on them so
    /// that the asks it decides one after another see each other, and counted by their
    /// owners when the data plane's hits reach them. A key it holds nothing of is new to the
    /// service: a full bucket. An ask decided already is answered as it was. `None` if it
    /// cannot decide: it has just joined, and a key it owns may still be on its way, handed
    /// over.
    fn decide_ask(&mut self, from: PlaneId, ask: &Ask, now: u64) -> Option<u64> {
        let least_hold = self.least_hold;
        for (key, contract) in &ask.keys {
            if !matches!(self.keys.get(key), Some(held) if held.contract == *contract) {
                if now < self.fresh_from && self.view.owners(key).0 == Some(self.me) {
                    return None;
                }
                self.keys.insert(key.clone(), Owned::new(*contract, now));
            }
            if let Some(held) = self.keys.get_mut(key) {
                held.lapse(least_hold, now);
            }
        }
        let before = ask.keys.iter().find_map(|(key, _)| {
            let record = self.keys.get(key)?.planes.get(from)?;
            (record.last_ask.0 == ask.id).then_some(record.last_ask.1)
        });
        if let Some(admitted) = before {
            return Some(admitted);
        }
        // As many as every key has room for, each charged to all of them: worked out in one
        // step, whatever the count, as it comes from the wire.
        let admitted = ask
            .keys
            .iter()
            .map(|(key, _)| {
                self.keys
                    .get(key)
                    .map_or(0, |held| held.bucket().remaining_within(now, 0, ask.count))
            })
            .fold(ask.count, u64::min);
        for (key, _) in &ask.keys {
            let owned = self.view.owners(key).0 == Some(self.me);
            let Some(held) = self.keys.get_mut(key) else {
                continue;
            };
            held.charge(now, admitted);
            held.touched = now;
            let record = held.planes.entry(from);
            record.last_ask = (ask.id, admitted);
            if owned {
                record.last_admitted = Admitted::at(now);
                record.held = record.held.saturating_add(admitted);
                held.counted = held.counted.saturating_add(admitted);
                self.copy_out(key, now);
            }
        }
        Some(admitted)
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
                            last_ask: record.last_ask,
                            held: record.held,
                        },
                    )
                })
                .collect();
            let mut copy = Owned {
                contract: replica.contract,
                bucket: bucket.state(),
                counted: replica.counted,
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
        for (key, owned) in &self.keys {
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
