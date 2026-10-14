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
//!   more carried if it left one unspent the cycle before.
//! - Every cycle it sends each owner, in one batch over all the keys that owner holds, the
//!   hits it admitted, numbered; or, with none to send while the key is in use, a poll. Each
//!   is answered with the bucket as it stands. A batch not seen applied is sent again after
//!   four cycles; one older than a bucket's refill time is sent emptied.
//! - A request for a key it holds no fresh copy of waits while the data plane asks the
//!   owner, which decides the waiting requests against its bucket. An ask unanswered for two
//!   cycles is sent again once; one unanswered for four is given up, and requests are decided
//!   from the copy until the owner is heard from again.
//! - The owner applies hits in order, once each, charging what does not fit as debt while
//!   it would be paid back within one bucket's refill time, and copies every change it makes
//!   to the key's secondary, which takes over from that copy should the owner go.
//!
//! Which pods own which keys follows from the [`View`] the service pods agree on: the owner
//! and the secondary of a key are the two pods ranking highest for it (rendezvous hashing),
//! so a pod leaving moves only the keys it held.

pub mod membership;

use crate::reference::{Model, TokenBucket};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::num::NonZeroU64;

/// How many cycles a data plane waits to see a batch applied, or an ask answered, before
/// sending it again (a batch) or giving it up (an ask).
pub const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles before the latest admission the owner knows of a data plane counts
/// as admitting the key.
pub const ACTIVE_CYCLES: u64 = 4;

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
    /// The owner of `key` and its secondary: the pods ranking highest and next for it.
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
        /// The batch's number among this data plane's batches for the key, from 1.
        sequence: u64,
        /// How many hits.
        count: u64,
        /// The highest of its batches it has seen applied: an owner taking over from a copy
        /// older than that skips to it rather than wait for batches that will not come.
        seen: u64,
    },
    /// Requests waiting for the owner to decide them.
    Ask {
        /// The ask's number among this data plane's asks for the key, from 1.
        sequence: u64,
        /// How many requests wait.
        count: u64,
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
    /// One entry per key, or a few for one key (an ask and a batch).
    pub entries: Vec<Entry>,
}

/// The owner's answer about one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// The key.
    pub key: Box<[u8]>,
    /// The owner's bucket as it stands: how far from full, as [`TokenBucket::behind`] says.
    pub behind: u128,
    /// The highest of the receiver's batches applied, every one before it too.
    pub applied: u64,
    /// How many other data planes the owner has seen admit the key lately.
    pub others: u64,
    /// To an ask: which ask, and how many of the requests it was about were admitted, the
    /// first so many; the rest were refused.
    pub decided: Option<(u64, u64)>,
}

/// A service pod's answer to a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answers {
    /// The service pod.
    pub from: PodId,
    /// One per entry it owns.
    pub entries: Vec<Answer>,
    /// The view it holds: a data plane holding an earlier one takes it over.
    pub view: Option<View>,
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
    /// Its last ask decided, and how many were admitted.
    pub last_ask: (u64, u64),
}

/// The owner's copy of one key, for its secondary.
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

/// The owner's copies of keys that changed, to their secondary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replicas {
    /// The owner, or the pod that was.
    pub from: PodId,
    /// One per key.
    pub entries: Vec<Replica>,
    /// Whether these are keys handed over to their new owner, by the pod that owned them
    /// before a view change, rather than copies for a secondary.
    pub handover: bool,
    /// The epoch of the view the sender held: a handover can reach a pod joining before
    /// the view that takes it in does, and is kept until it has.
    pub epoch: u64,
}

/// How a request was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Admitted.
    Admit,
    /// Refused.
    Refuse,
    /// Waiting for the owner: its decision comes from [`DataPlane::take_decided`].
    Wait(Ticket),
}

/// What a data plane holds of one key.
struct PlaneKey {
    contract: Contract,
    /// Its contract's refill time, worked out once: how long a copy stays fresh.
    hold: u64,
    /// The service pod it sends to.
    owner: Option<PodId>,
    /// The owner's bucket as last heard, charged with its own hits not yet seen applied,
    /// and with what it has admitted since.
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
    /// Whether a cycle has set the allowance from a copy the owner answered with: until
    /// then the copy is shared out among no one, and the owner decides.
    bounded: bool,
    /// How many other data planes the owner last said were admitting the key.
    others: u64,
    /// When a request for the key last came.
    last_request: Option<u64>,
    /// When the owner last answered: the copy is fresh for a refill time after.
    heard: Option<u64>,
    /// When the ask in flight was sent, if one is.
    asking: Option<u64>,
    /// Asks sent so far: the one in flight is the last.
    asks: u64,
    /// Whether the ask in flight has been sent again.
    ask_resent: bool,
    /// Whether an ask went unanswered since the owner was last heard from: until it is,
    /// requests are decided here.
    unreachable: bool,
    /// The requests the ask in flight is about.
    asked: Vec<Ticket>,
    /// Requests that came while it was in flight.
    waiting: Vec<Ticket>,
    /// Whether the ask in flight is the follow-up for a cold burst's tail.
    follow_up: bool,
}

impl PlaneKey {
    fn new(contract: Contract, owner: Option<PodId>) -> Self {
        Self {
            contract,
            hold: contract.refill_time(),
            owner,
            bucket: contract.bucket(),
            unsent: 0,
            sent: 0,
            seen: 0,
            unacknowledged: VecDeque::new(),
            spent: 0,
            allowance: None,
            bounded: false,
            others: 0,
            last_request: None,
            heard: None,
            asking: None,
            asks: 0,
            ask_resent: false,
            unreachable: false,
            asked: Vec::new(),
            waiting: Vec::new(),
            follow_up: false,
        }
    }

    /// Decides a request here, from the copy and the cycle's allowance.
    fn decide(&mut self, now: u64) -> bool {
        self.last_request = Some(now);
        if self
            .allowance
            .is_some_and(|allowance| self.spent >= allowance)
        {
            return false;
        }
        let admitted = self.bucket.decide(now);
        if admitted {
            self.spent += 1;
            self.unsent += 1;
        }
        admitted
    }

    /// Whether nothing is left to do for the key and its copy would be cold anyway.
    fn idle(&self, now: u64) -> bool {
        let hold = self.contract.refill_time();
        self.unsent == 0
            && self.unacknowledged.is_empty()
            && self.asking.is_none()
            && self
                .last_request
                .is_none_or(|last| now.saturating_sub(last) > 2 * hold)
    }
}

/// A data plane's side of the protocol: one worker's copies of the keys it has seen.
pub struct DataPlane<S = FixedState> {
    id: PlaneId,
    cycle: u64,
    view: View,
    keys: HashMap<Box<[u8]>, PlaneKey, S>,
    next_ticket: Ticket,
    decided: Vec<(Ticket, bool)>,
    outgoing: BTreeMap<PodId, Vec<Entry>>,
    /// Views asked for so far, to ask the pods in turn.
    polls: u64,
    /// How long an ask may go unanswered before it is given up and what waits is decided
    /// here; sent again once half way.
    ask_timeout: u64,
}

impl<S: BuildHasher + Default> DataPlane<S> {
    /// A data plane `id` sending every `cycle` nanoseconds, knowing the service as `view`.
    #[must_use]
    pub fn new(id: PlaneId, cycle: u64, view: View) -> Self {
        Self {
            id,
            cycle: cycle.max(1),
            view,
            keys: HashMap::default(),
            next_ticket: 0,
            decided: Vec::new(),
            outgoing: BTreeMap::new(),
            polls: 0,
            ask_timeout: RESEND_AFTER_CYCLES * cycle.max(1),
        }
    }

    /// How long an ask may go unanswered before it is given up: four cycles unless said
    /// otherwise. Over TCP a lost segment is sent again only after some 200 ms, and an ask
    /// given up sooner leaves its requests to a copy that has heard nothing.
    pub fn set_ask_timeout(&mut self, timeout: u64) {
        self.ask_timeout = timeout.max(1);
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> PlaneId {
        self.id
    }

    /// Decides a request for `key` under `contract` arriving at `now`. A request for a key
    /// with no fresh copy waits while the owner is asked; the ask is among the messages
    /// [`take_outgoing`](Self::take_outgoing) gives, to be sent at once.
    pub fn decide(&mut self, key: &[u8], contract: Contract, now: u64) -> Decision {
        // The common case, with one lookup: a key held under the same contract.
        if let Some(at) = self.keys.get_mut(key)
            && at.contract == contract
        {
            return decide_held(at, key, now, &mut self.outgoing, &mut self.next_ticket);
        }
        // New, or its contract changed: any change starts a key afresh (06 §3).
        let owner = self.view.owners(key).0;
        self.keys.insert(key.into(), PlaneKey::new(contract, owner));
        match self.keys.get_mut(key) {
            Some(at) => decide_held(at, key, now, &mut self.outgoing, &mut self.next_ticket),
            None => Decision::Refuse,
        }
    }

    /// The cycle's work at `now`: allowances set for the next cycle, asks sent again or
    /// given up, batches and polls for every owner.
    pub fn on_cycle(&mut self, now: u64) {
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        let ask_timeout = self.ask_timeout;
        let cycle = self.cycle;
        let mut decided = Vec::new();
        let mut cut_off = false;
        for (key, at) in &mut self.keys {
            let hold = at.contract.refill_time();
            // Its share of what the copy has left and the cycle refills, at least one, and
            // one more if it left one unspent.
            let sees = at
                .bucket
                .remaining(now)
                .saturating_add(at.contract.refilled(cycle));
            let share = (sees / (at.others + 1)).max(1);
            let left = at
                .allowance
                .map_or(0, |before| before.saturating_sub(at.spent));
            at.allowance = Some(share + left.min(1));
            at.bounded |= at.heard.is_some();
            at.spent = 0;
            let Some(owner) = at.owner else {
                continue;
            };
            let mut entries = Vec::new();
            // An ask unanswered half as long is sent again once, as it was.
            if at
                .asking
                .is_some_and(|asked| now - asked >= ask_timeout / 2)
                && !at.ask_resent
            {
                at.ask_resent = true;
                entries.push(Entry {
                    key: key.clone(),
                    contract: at.contract,
                    report: Report::Ask {
                        sequence: at.asks,
                        count: at.asked.len() as u64,
                    },
                });
            }
            // Unanswered as long as a batch waits to be sent again: the owner is gone or out
            // of reach, and what waits is decided here.
            if at.asking.is_some_and(|asked| now - asked >= ask_timeout) {
                at.asking = None;
                at.unreachable = true;
                let waited: Vec<Ticket> = std::mem::take(&mut at.asked)
                    .into_iter()
                    .chain(std::mem::take(&mut at.waiting))
                    .collect();
                for ticket in waited {
                    decided.push((ticket, at.decide(now)));
                }
            }
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
        self.decided.extend(decided);
        self.keys.retain(|_, at| !at.idle(now));
    }

    /// An owner's answers reach it at `now`.
    pub fn on_answers(&mut self, answers: Answers, now: u64) {
        let Answers {
            from,
            entries,
            view,
        } = answers;
        for answer in entries {
            let Some(at) = self.keys.get_mut(&answer.key) else {
                continue;
            };
            // Only the owner it sends to: an answer from one that has gone is stale.
            if at.owner != Some(from) {
                continue;
            }
            at.bucket.set_behind(now, answer.behind);
            at.others = answer.others;
            // No more of this data plane's batches can have been applied than it has sent.
            let seen = answer.applied.min(at.sent);
            at.seen = at.seen.max(seen);
            at.unacknowledged
                .retain(|(sequence, _, _, _)| *sequence > seen);
            let unseen: u64 = at.unacknowledged.iter().map(|(_, count, _, _)| count).sum();
            for _ in 0..unseen + at.unsent {
                at.bucket.charge(now);
            }
            at.heard = Some(now);
            at.unreachable = false;
            let Some((sequence, admitted)) = answer.decided else {
                continue;
            };
            if at.asking.is_none() || sequence != at.asks {
                continue;
            }
            at.asking = None;
            for (index, ticket) in std::mem::take(&mut at.asked).into_iter().enumerate() {
                self.decided.push((ticket, (index as u64) < admitted));
            }
            // Those that came during the first ask are asked about once more — the tail of
            // a cold burst — and, once a cycle has bounded the copy, any after that are
            // decided here: asking again and again would keep a data plane whose requests
            // come faster than a round trip waiting on every one.
            if at.waiting.is_empty() {
                at.follow_up = false;
            } else if at.follow_up && at.bounded {
                at.follow_up = false;
                for ticket in std::mem::take(&mut at.waiting) {
                    let admitted = at.decide(now);
                    self.decided.push((ticket, admitted));
                }
            } else {
                at.follow_up = true;
                at.asked = std::mem::take(&mut at.waiting);
                ask(&mut self.outgoing, &answer.key, at, now);
            }
        }
        if let Some(view) = view {
            self.on_view(view, now);
        }
    }

    /// The service's pods change at `now`. Keys whose owner moved send it again every
    /// batch not seen applied, numbered as before — a secondary taking over knows what the
    /// old owner applied — and ask again about requests waiting.
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
            if at.asking.is_some() {
                ask(&mut self.outgoing, key, at, now);
            }
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
    }

    /// Messages to send: one batch per service pod; an empty one asks only for the view.
    pub fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
        std::mem::take(&mut self.outgoing)
            .into_iter()
            .map(|(pod, entries)| {
                (
                    pod,
                    Batch {
                        from: self.id,
                        entries,
                    },
                )
            })
            .collect()
    }

    /// Requests that waited, decided since last asked: `(ticket, admitted)`.
    pub fn take_decided(&mut self) -> Vec<(Ticket, bool)> {
        std::mem::take(&mut self.decided)
    }

    /// The requests waiting, refused: the data plane is going away.
    pub fn abandon(&mut self) -> Vec<Ticket> {
        self.keys
            .values_mut()
            .flat_map(|at| {
                let mut gone = std::mem::take(&mut at.asked);
                gone.append(&mut at.waiting);
                gone
            })
            .collect()
    }
}

/// Decides a request for a key `at` holds: from its copy, or, cold, by asking the owner.
fn decide_held(
    at: &mut PlaneKey,
    key: &[u8],
    now: u64,
    outgoing: &mut BTreeMap<PodId, Vec<Entry>>,
    next_ticket: &mut Ticket,
) -> Decision {
    // Cold: no copy fresh enough, or none bounded yet. A copy an ask just brought is not
    // bounded until a cycle after it shares out what it holds among the data planes
    // admitting the key, as the owner's answers say them; until then another data plane
    // may hold the same copy, and the owner decides. Requests come over a few milliseconds,
    // not all at once: the first ask's answer comes back while the rest of a burst is still
    // arriving.
    let cold = at.owner.is_some()
        && !at.unreachable
        && (at.asking.is_some()
            || !at.bounded
            || at.heard.is_none_or(|heard| now - heard > at.hold));
    if !cold {
        return if at.decide(now) {
            Decision::Admit
        } else {
            Decision::Refuse
        };
    }
    at.last_request = Some(now);
    let ticket = *next_ticket;
    *next_ticket += 1;
    if at.asking.is_some() {
        at.waiting.push(ticket);
    } else {
        at.follow_up = false;
        at.asked.push(ticket);
        ask(outgoing, key, at, now);
    }
    Decision::Wait(ticket)
}

/// Sends the owner an ask about the requests in `asked`.
fn ask(outgoing: &mut BTreeMap<PodId, Vec<Entry>>, key: &[u8], at: &mut PlaneKey, now: u64) {
    let Some(owner) = at.owner else {
        return;
    };
    at.asking = Some(now);
    at.ask_resent = false;
    at.asks += 1;
    outgoing.entry(owner).or_default().push(Entry {
        key: key.into(),
        contract: at.contract,
        report: Report::Ask {
            sequence: at.asks,
            count: at.asked.len() as u64,
        },
    });
}

/// What a service pod keeps of one data plane, for one key.
#[derive(Clone, Debug, Default)]
struct Record {
    applied: u64,
    /// Batches that came ahead of one still missing.
    waiting: BTreeMap<u64, u64>,
    last_admitted: Option<u64>,
    last_ask: (u64, u64),
}

/// What a service pod holds of a key, as its owner or its secondary.
struct Owned {
    contract: Contract,
    bucket: TokenBucket,
    counted: u64,
    planes: BTreeMap<PlaneId, Record>,
    /// When anything last changed.
    touched: u64,
}

impl Owned {
    fn new(contract: Contract, now: u64) -> Self {
        Self {
            contract,
            bucket: contract.bucket(),
            counted: 0,
            planes: BTreeMap::new(),
            touched: now,
        }
    }

    /// `count` reported hits, each charged if there is room, or as debt while it would be
    /// paid back within a refill time; the rest forgiven.
    fn apply(&mut self, count: u64, now: u64) {
        let most = self.contract.refill_time();
        self.counted += count;
        for _ in 0..count {
            let owe = self
                .bucket
                .retry_at(now)
                .is_none_or(|retry| retry - now <= most);
            if owe {
                self.bucket.charge(now);
            }
        }
    }

    /// How many data planes besides `plane` it has seen admit the key within a few cycles
    /// of the latest admission it knows of: counted back from the last traffic, not from
    /// now, so that data planes that burst together and fall quiet are bounded together
    /// when they burst again.
    fn others(&self, plane: PlaneId, lately: u64) -> u64 {
        let latest = self.planes.values().filter_map(|r| r.last_admitted).max();
        self.planes
            .iter()
            .filter(|(id, record)| {
                **id != plane
                    && matches!((record.last_admitted, latest), (Some(at), Some(latest)) if latest - at <= lately)
            })
            .count() as u64
    }

    /// Takes in what `mine` applied of the key while this copy was on its way: its hits
    /// charged on top, and the furthest of each data plane's marks.
    fn merge(&mut self, mine: Owned, now: u64) {
        self.apply(mine.counted, now);
        for (plane, record) in mine.planes {
            let ours = self.planes.entry(plane).or_default();
            ours.applied = ours.applied.max(record.applied);
            ours.last_admitted = ours.last_admitted.max(record.last_admitted);
            if record.last_ask.0 > ours.last_ask.0 {
                ours.last_ask = record.last_ask;
            }
        }
    }

    fn replica(&self, key: &[u8], now: u64) -> Replica {
        Replica {
            key: key.into(),
            contract: self.contract,
            behind: self.bucket.behind(now),
            counted: self.counted,
            planes: self
                .planes
                .iter()
                .map(|(plane, record)| PlaneRecord {
                    plane: *plane,
                    applied: record.applied,
                    admitted_ago: record.last_admitted.map(|at| now.saturating_sub(at)),
                    last_ask: record.last_ask,
                })
                .collect(),
        }
    }
}

/// A service pod's side of the protocol: the keys it owns, and the copies it keeps as a
/// secondary.
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
        }
    }

    /// How long after joining a view it makes no key afresh, but answers nothing for a key
    /// it does not hold: data planes go on from their copies meanwhile, and the pod that
    /// held the key has time to hand it over. A few heartbeats, as a view reaches every pod
    /// in one.
    pub fn set_join_grace(&mut self, grace: u64) {
        self.grace = grace;
    }

    /// Whether it serves: a pod the agreed view has not taken in, in this life, answers
    /// batches with its view and nothing more.
    pub fn set_serving(&mut self, serving: bool) {
        self.serving = serving;
    }

    /// Hits the count of `key` holds, each once, if it holds the key.
    #[must_use]
    pub fn counted(&self, key: &[u8]) -> Option<u64> {
        self.keys.get(key).map(|owned| owned.counted)
    }

    /// A data plane's batch reaches it at `now`: the answers go back to that data plane,
    /// and the copies of what changed to secondaries, from
    /// [`take_replicas`](Self::take_replicas).
    pub fn on_batch(&mut self, batch: Batch, now: u64) -> Answers {
        let lately = ACTIVE_CYCLES * self.cycle;
        let mut answers = Vec::new();
        let from = batch.from;
        let entries = if self.serving {
            batch.entries
        } else {
            Vec::new()
        };
        for entry in entries {
            let (owner, secondary) = self.view.owners(&entry.key);
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
            let record = owned.planes.entry(from).or_default();
            let mut decided = None;
            let mut changed = false;
            match entry.report {
                Report::Hits {
                    sequence,
                    count,
                    seen,
                } => {
                    record.last_admitted = Some(now);
                    changed = true;
                    // Taken over from a copy that had not caught up: batches the data plane
                    // saw applied, and will not send again, are skipped.
                    if seen > record.applied {
                        record.applied = seen;
                        record.waiting.retain(|sequence, _| *sequence > seen);
                    }
                    if sequence > record.applied {
                        record.waiting.insert(sequence, count);
                    }
                    let mut ready = 0;
                    while let Some(count) = record.waiting.remove(&(record.applied + 1)) {
                        record.applied += 1;
                        ready += count;
                    }
                    owned.apply(ready, now);
                }
                Report::Ask { sequence, count } => {
                    let (last, admitted) = record.last_ask;
                    let admitted = if sequence == last {
                        admitted
                    } else {
                        let admitted =
                            (0..count).filter(|_| owned.bucket.decide(now)).count() as u64;
                        owned.counted += admitted;
                        let record = owned.planes.entry(from).or_default();
                        record.last_admitted = Some(now);
                        record.last_ask = (sequence, admitted);
                        changed = true;
                        admitted
                    };
                    decided = Some((sequence, admitted));
                }
                Report::Poll => {}
            }
            let applied = owned.planes.get(&from).map_or(0, |record| record.applied);
            answers.push(Answer {
                key: entry.key.clone(),
                behind: owned.bucket.behind(now),
                applied,
                others: owned.others(from, lately),
                decided,
            });
            if changed && let Some(secondary) = secondary {
                let replica = owned.replica(&entry.key, now);
                let copies = self.replicas.entry(secondary).or_default();
                // One copy of a key a batch: the last made holds the others.
                copies.retain(|copy| copy.key != replica.key);
                copies.push(replica);
            }
        }
        Answers {
            from: self.me,
            entries: answers,
            view: Some(self.view.clone()),
        }
    }

    /// Copies reach it at `now`: an owner's, kept for the keys it is the secondary of from
    /// the owner it knows of; or keys handed over to it as their new owner, taken over —
    /// with whatever it has applied of them meanwhile charged on top.
    pub fn on_replicas(&mut self, replicas: Replicas, now: u64) {
        if replicas.handover && replicas.epoch > self.view.epoch {
            self.pending.push(replicas);
            return;
        }
        for replica in replicas.entries {
            let (owner, secondary) = self.view.owners(&replica.key);
            let wanted = if replicas.handover {
                owner == Some(self.me)
            } else {
                owner == Some(replicas.from) && secondary == Some(self.me)
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
                            waiting: BTreeMap::new(),
                            last_admitted: record.admitted_ago.map(|ago| now.saturating_sub(ago)),
                            last_ask: record.last_ask,
                        },
                    )
                })
                .collect();
            let mut copy = Owned {
                contract: replica.contract,
                bucket,
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
    /// and each gets its new secondary a copy at once.
    pub fn on_view(&mut self, view: View, now: u64) {
        if view.epoch <= self.view.epoch {
            return;
        }
        let before = std::mem::replace(&mut self.view, view);
        for (key, owned) in &self.keys {
            let (owner, secondary) = self.view.owners(key);
            // Owned until now, and from now another pod's that held no copy of it — one
            // joining, not the secondary taking over: handed over.
            let (owned_before, secondary_before) = before.owners(key);
            if owned_before == Some(self.me)
                && let Some(owner) = owner
                && owner != self.me
                && secondary_before != Some(owner)
            {
                self.handovers
                    .entry(owner)
                    .or_default()
                    .push(owned.replica(key, now));
            }
            if owner != Some(self.me) {
                continue;
            }
            let Some(secondary) = secondary else {
                continue;
            };
            if before.owners(key) != (owner, Some(secondary)) {
                self.replicas
                    .entry(secondary)
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

    /// The pod's timer at `now`: keys it neither owns nor keeps a copy of any more, and
    /// keys full again and quiet for a refill time, are dropped.
    pub fn on_timer(&mut self, now: u64) {
        let view = &self.view;
        let me = self.me;
        self.keys.retain(|key, owned| {
            let (owner, secondary) = view.owners(key);
            let mine = owner == Some(me) || secondary == Some(me);
            let quiet = now.saturating_sub(owned.touched) > owned.contract.refill_time()
                && owned.bucket.forgettable_at() <= now;
            mine && !quiet
        });
    }

    /// Copies to send: one message per secondary, and one per pod keys are handed over to.
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

#[cfg(test)]
mod tests;
