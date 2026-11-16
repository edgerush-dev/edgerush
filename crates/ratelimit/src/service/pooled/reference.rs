//! The protocol's own data plane, kept only as the reference the participant is tested
//! against (`a_participant_decides_as_a_data_plane`): one worker's copies of the keys it has
//! seen, each its own state in a plain struct rather than a word workers share, routed by a
//! [`View`] of the pods and answered as [`Answers`]. Test-only: nothing outside the tests runs
//! it.

use crate::reference::{Model, TokenBucket};
use crate::service::{
    Batch, CUT_OFF_CYCLES, Contract, Entry, FixedState, MOST_UNACKNOWLEDGED, PlaneId, PodId,
    Queued, RESEND_AFTER_CYCLES, Report, Unseen, cut_off_from_owner, fnv1a, mix, taken_as_of,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::BuildHasher;

/// Which service pods are serving, as whoever runs them sets it, and since when: a later
/// epoch replaces an earlier one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct View {
    /// Counted up with each change.
    pub(super) epoch: u64,
    /// The pods serving.
    pub(super) pods: Vec<PodId>,
}

impl View {
    /// The owner of `key` and the pod next in rank for it, which takes it over should the
    /// owner go.
    #[must_use]
    pub(super) fn owners(&self, key: &[u8]) -> (Option<PodId>, Option<PodId>) {
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

/// A service pod's answer about one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Answer {
    /// The key.
    pub(super) key: Box<[u8]>,
    /// The bucket as the pod holds it: how far from full, as [`TokenBucket::behind`] says.
    pub(super) behind: u128,
    /// The highest of the receiver's batches applied, every one before it too, as far as
    /// the pod knows: a copy is never ahead of its owner.
    pub(super) applied: u64,
    /// How many other data planes the pod has seen admit the key lately.
    pub(super) others: u64,
}

/// A service pod's answer to a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Answers {
    /// The service pod.
    pub(super) from: PodId,
    /// One per entry for a key it owns.
    pub(super) entries: Vec<Answer>,
    /// The view it holds: a data plane holding an earlier one takes it over.
    pub(super) view: Option<View>,
    /// The pod's life: drawn at random when it starts, never 0. 0 is a pod that does not
    /// number its answers.
    pub(super) life: u64,
    /// Counted up, wrapping, with each answer the pod gives in this life: the order in
    /// which it handled their batches.
    pub(super) number: u64,
    /// The batch's [`Batch::made`], said back: the data plane takes the buckets as of then.
    /// The pod charged the batch's hits as late as the batch took to reach it, and the
    /// buckets read later by as much: taken as of when the batch was made, the two cancel,
    /// with no clock shared; but no further back than a cycle before the answers came, as a
    /// trip held up longer — a lost packet waiting for TCP to send it again — would be
    /// counted as refill the copy never had. 0 is a pod that does not say it back, charging
    /// hits as they arrive: its buckets are taken as of when the answers came.
    pub(super) made: u64,
}

/// How a request was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Decision {
    /// Admitted.
    Admit,
    /// Refused.
    Refuse,
}

/// What a data plane holds of one key.
pub(super) struct PlaneKey {
    pub(super) contract: Contract,
    /// Its contract's refill time, worked out once.
    pub(super) hold: u64,
    /// The service pod it sends to.
    pub(super) owner: Option<PodId>,
    /// The owner's bucket as last heard, charged with its own hits not yet seen applied,
    /// and with what it has admitted since.
    pub(super) bucket: TokenBucket,
    /// Hits admitted and not yet sent.
    pub(super) unsent: u64,
    /// When the earliest of them was admitted.
    pub(super) unsent_since: u64,
    /// Batches sent to the present owner so far.
    pub(super) sent: u64,
    /// The highest of them the owner has been seen to apply.
    pub(super) seen: u64,
    /// Batches sent and not yet seen applied.
    pub(super) unacknowledged: VecDeque<Unseen>,
    /// Admitted in the cycle under way.
    pub(super) spent: u64,
    /// The most it may admit in the cycle under way, once a cycle has set it.
    pub(super) allowance: Option<u64>,
    /// When a cycle last shared out what the copy has left.
    pub(super) shared: Option<u64>,
    /// How many other data planes the owner last said were admitting the key.
    pub(super) others: u64,
    /// When a request for the key last came.
    pub(super) last_request: Option<u64>,
    /// When a pod last answered about it.
    pub(super) heard: Option<u64>,
}

impl PlaneKey {
    fn new(contract: Contract, owner: Option<PodId>, numbered: u64) -> Self {
        Self {
            contract,
            hold: contract.refill_time(),
            owner,
            bucket: contract.bucket().with_slack(crate::SLACK),
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

impl Queued {
    /// Its batch, from data plane `from`, whole.
    pub(super) fn batch(self, from: PlaneId) -> Batch {
        Batch {
            from,
            entries: self.entries,
            made: self.made,
        }
    }
}

/// A data plane's side of the protocol: one worker's copies of the keys it has seen.
pub(super) struct DataPlane<S = FixedState> {
    id: PlaneId,
    cycle: u64,
    view: View,
    pub(super) keys: HashMap<Box<[u8]>, PlaneKey, S>,
    /// Batches numbered, over every key: a key made again numbers its own from here, past
    /// every number an earlier life of it used.
    numbered: u64,
    /// What is to go to each pod.
    queued: BTreeMap<PodId, Queued>,
    /// Views asked for so far, to ask the pods in turn.
    polls: u64,
}

impl<S: BuildHasher + Default + Clone> DataPlane<S> {
    /// A data plane `id` sending every `cycle` nanoseconds, knowing the service as `view`,
    /// its maps hashed by `S` made by default: the simulator's, which runs alike every time.
    #[must_use]
    pub(super) fn new(id: PlaneId, cycle: u64, view: View) -> Self {
        Self::with_hasher(id, cycle, view, S::default())
    }
}

impl<S: BuildHasher + Clone> DataPlane<S> {
    /// A data plane `id` sending every `cycle` nanoseconds, knowing the service as `view`,
    /// its maps hashed by `hasher`: a keyed one ([`Keyed`]) where clients choose the keys.
    #[must_use]
    pub(super) fn with_hasher(id: PlaneId, cycle: u64, view: View, hasher: S) -> Self {
        Self {
            id,
            cycle: cycle.max(1),
            view,
            keys: HashMap::with_hasher(hasher),
            numbered: 0,
            queued: BTreeMap::new(),
            polls: 0,
        }
    }

    /// Decides a request for `key` under `contract` arriving at `now`: [`decide_all`] of
    /// one key.
    ///
    /// [`decide_all`]: Self::decide_all
    pub(super) fn decide(&mut self, key: &[u8], contract: Contract, now: u64) -> Decision {
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
    pub(super) fn decide_all(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
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
                self.keys
                    .insert(key.into(), PlaneKey::new(contract, owner, self.numbered));
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
    pub(super) fn on_cycle(&mut self, now: u64) {
        let resend_after = RESEND_AFTER_CYCLES * self.cycle;
        let cycle = self.cycle;
        let cut_off_after = CUT_OFF_CYCLES * cycle;
        let mut cut_off = false;
        for (key, at) in &mut self.keys {
            let hold = at.contract.refill_time();
            let refill = at.contract.refilled(cycle);
            let oldest = at.unacknowledged.front().map(|unseen| unseen.first);
            if cut_off_from_owner(at.heard, oldest, cut_off_after, now) {
                at.others = 0;
            }
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
            // What a stale report gives up is forgiven: the participant counts it, the
            // reference has no need to.
            for unseen in &mut at.unacknowledged {
                let _forgiven = unseen.empty_if_stale(hold, now);
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
        self.keys.retain(|_, at| !at.idle(now));
    }

    /// A service pod's answers reach it at `now`: each copy set to the pod's bucket as of
    /// when the batch was made, and charged with what the pod has not seen.
    pub(super) fn on_answers(&mut self, answers: Answers, now: u64) {
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
    pub(super) fn on_view(&mut self, view: View, now: u64) {
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

    /// Messages to send: one batch per service pod; an empty one asks only for the view.
    /// Whole, not cut to [`PIECE_BYTES`] as the pooled participant's are: as the reference,
    /// it is compared with that over batches that fit.
    pub(super) fn take_outgoing(&mut self) -> Vec<(PodId, Batch)> {
        std::mem::take(&mut self.queued)
            .into_iter()
            .map(|(pod, queued)| (pod, queued.batch(self.id)))
            .collect()
    }
}

/// The reference's own decisions, held to what a plain token bucket admits over round trips:
/// what makes it a reference the participant can be compared with.
mod tests {
    use super::*;
    use crate::service::pooled::testing::Owner;
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;
    const CYCLE: u64 = 10 * MS;

    /// One data plane and its pod, applying with the real owner, batches taking `there` to reach
    /// the pod and answers `back`
    /// to return, a cycle every `CYCLE`: requests at `times`, each decided at once from the
    /// copy. Whether each was admitted.
    fn decided_over_round_trips(
        c: Contract,
        times: &[u64],
        there: u64,
        back: u64,
    ) -> Vec<Option<bool>> {
        let one = View {
            epoch: 1,
            pods: vec![0],
        };
        let mut plane: DataPlane = DataPlane::new(100, CYCLE, one);
        let mut pod = Owner::new(0, CYCLE);
        let mut decided = vec![None; times.len()];
        // In order of arrival: answers, and when. Each takes as long, so sent in order, they
        // arrive in order.
        let mut answers: VecDeque<(u64, Answers)> = VecDeque::new();
        let mut next_cycle = CYCLE;
        let last = times.last().copied().unwrap_or(0);
        let mut at = 0;
        loop {
            let request = times.get(at).copied();
            let answer = answers.front().map(|(when, _)| *when);
            // Requests first at the same instant, as the harder order.
            let now = [request, Some(next_cycle), answer]
                .into_iter()
                .flatten()
                .min()
                .unwrap();
            if request == Some(now) {
                decided[at] = Some(plane.decide(b"k", c, now) == Decision::Admit);
                at += 1;
            } else if answer == Some(now) {
                let (_, answer) = answers.pop_front().unwrap();
                plane.on_answers(answer, now);
            } else {
                if next_cycle > last + 4 * CYCLE && at == times.len() && answers.is_empty() {
                    return decided;
                }
                plane.on_cycle(now);
                next_cycle += CYCLE;
            }
            for (_, batch) in plane.take_outgoing() {
                let answer = pod.on_batch(batch, now + there);
                answers.push_back((now + there + back, answer));
            }
        }
    }

    proptest::proptest! {
        /// Traffic a plain token bucket admits all of, over many rates and bursts, is admitted
        /// all by one data plane however long its messages take: its hits are charged as of
        /// when it admitted them, its copy taken as of when it sent them, and requests that
        /// wait — the copy cold between answers — decided as of when they came. One request
        /// warms the key a refill time and a round trip before the traffic, and no gap is
        /// longer than a refill time, after which a bucket is full however long the gap. The
        /// bucket holds two cycles of its rate or more, as config insists, and a round trip
        /// fits in a cycle. A bucket that refills faster refuses a client within its rate: a
        /// copy answered full charges hits that waited from the answer's batch rather than from
        /// when they came (issue #54); so does a cycle shorter than the round trip, whose
        /// answers are taken as of a cycle before they come (issue #56).
        #[test]
        fn what_a_plain_bucket_admits_a_data_plane_admits_over_round_trips(
            limit in 1u64..200, interval in 1u64..50, burst in 1u64..12,
            gaps in proptest::collection::vec(
                proptest::prop_oneof![2 => proptest::strategy::Just(1u64), 1 => 0u64..3, 1 => 0u64..40],
                1..80,
            ),
            jitter in proptest::collection::vec(0u64..1_000_000, 80),
            there in 0u64..5_000_000, back in 0u64..5_000_000,
        ) {
            let interval = interval * MS + 1;
            let burst = burst.max((2 * CYCLE).div_ceil(interval));
            let n = |n| NonZeroU64::new(n).unwrap();
            let c = Contract { limit: n(limit), window: n(limit * interval), burst: n(burst) };
            // Gaps of whole intervals, exactly at the edge, some a little more.
            let refill = burst * interval;
            let mut plain = c.bucket();
            proptest::prop_assert!(plain.decide(0));
            let mut times = vec![0];
            let mut now = refill + there + back;
            for (gap, jitter) in gaps.iter().zip(&jitter) {
                now += (gap * interval + if *gap > 2 { *jitter } else { 0 }).min(refill);
                if plain.decide(now) {
                    times.push(now);
                }
            }
            let decided = decided_over_round_trips(c, &times, there, back);
            let refused: Vec<(u64, Option<bool>)> = times
                .iter()
                .zip(&decided)
                .filter(|(_, admitted)| **admitted != Some(true))
                .map(|(at, admitted)| (*at, *admitted))
                .collect();
            proptest::prop_assert!(refused.is_empty(), "refused {:?} of {:?}", refused, times);
        }
    }

    /// The issue's case: a client at exactly one a second, a bucket of one, its hits reaching
    /// the owner a cycle and a millisecond on, the answers a millisecond after.
    #[test]
    fn a_client_at_exactly_its_rate_is_admitted_after_a_round_trip() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let c = Contract {
            limit: n(1),
            window: n(1_000 * MS),
            burst: n(1),
        };
        let times: Vec<u64> = (0..5).map(|k| k * 1_000 * MS).collect();
        let decided = decided_over_round_trips(c, &times, MS, MS);
        assert_eq!(decided, [Some(true); 5]);
    }

    /// Exchanges overlapping, a bucket of one that refills faster than a round trip: requests
    /// at exactly the rate ask, and the owner holds what it admits for their hits. Its answers
    /// say what it holds rather than taking it off the bucket, which a partial refill since
    /// would leave short, so the copy is not left further behind than the owner.
    #[test]
    fn a_client_at_exactly_its_rate_is_admitted_with_exchanges_overlapping() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let interval = 11 * MS + 1;
        let c = Contract {
            limit: n(1),
            window: n(interval),
            burst: n(1),
        };
        let times = [
            interval,
            2 * interval,
            3 * interval,
            4 * interval,
            176_494_783,
            309_000_000,
            309_000_000 + interval,
        ];
        let decided = decided_over_round_trips(c, &times, 4_773_750, 266_466);
        assert_eq!(decided, [Some(true); 7]);
    }

    /// The issue's case (#49): a bucket of one refilling faster than a round trip, so the copy
    /// is cold for every request and each asks. The requests that come while the first ask is
    /// out go in one ask after it; the owner decides them at one instant, against a bucket of
    /// one, though they came an interval apart.
    #[test]
    fn a_client_at_exactly_its_rate_is_admitted_when_every_request_asks() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let interval = MS + 1;
        let c = Contract {
            limit: n(1),
            window: n(interval),
            burst: n(1),
        };
        let times = [interval, 2 * interval, 3 * interval];
        let decided = decided_over_round_trips(c, &times, 3_270_000, 1_620_000);
        assert_eq!(decided, [Some(true); 3]);
    }
}
