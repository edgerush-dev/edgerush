//! Ownership as Gubernator shares counts (06 §4): one owner applies every pod's hits to the
//! bucket that counts, and broadcasts it — and, as settings, the hit ledger and the per-cycle
//! bound 06 §4 proposes, and what pods do with their batches when the owner changes. Each
//! pod has its own idea of who the owner is, so that a partition can leave two.

use crate::{
    Bound, Cluster, Contract, Excess, FaultEvent, Message, OnOwnerChange, Outbox, Sharing,
};
use edgerush_ratelimit::reference::{Model, TokenBucket};
use std::collections::{BTreeMap, VecDeque};

/// The pod that owns the key at the start. With one key there is one owner; which pod it
/// is changes nothing, so it is the first.
const OWNER: usize = 0;

/// How many cycles a pod waits to see a batch applied before sending it again.
const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles a pod counts as admitting the key after it last did.
const ACTIVE_CYCLES: u64 = 4;

/// A fleet sharing one key by ownership.
pub(crate) struct Ownership {
    sharing: Sharing,
    /// A new bucket, full: what a pod comes back with from a restart.
    fresh: TokenBucket,
    /// What a cycle refills, in requests, rounded down.
    refill: u64,
    pods: Vec<Pod>,
    /// What each pod keeps as an owner, used while it is one.
    owned: Vec<Owned>,
    /// Which pods are still there.
    alive: Vec<bool>,
    /// Hits admitted anywhere.
    admitted: u64,
}

/// What a pod keeps while it owns the key.
#[derive(Clone, Default)]
struct Owned {
    /// For each pod: when it was last seen to admit the key.
    last_admitted: Vec<Option<u64>>,
    /// For each pod: the highest of its batches applied, every one before it applied too.
    applied: Vec<u64>,
    /// For each pod: batches that came ahead of one still missing, held until it comes,
    /// so that a pod's batches are applied in order.
    waiting: Vec<BTreeMap<u64, u64>>,
    /// Hits its count holds: its own, and those reported to it, each once — and those it
    /// held on taking over.
    counted: u64,
    /// For each pod: the latest of its instances heard from. A batch from an earlier one is
    /// from a life that has ended, and is ignored.
    instances: Vec<u64>,
}

impl Owned {
    fn new(pods: usize) -> Self {
        Self {
            last_admitted: vec![None; pods],
            applied: vec![0; pods],
            waiting: vec![BTreeMap::new(); pods],
            counted: 0,
            instances: vec![0; pods],
        }
    }
}

/// What one pod holds of the key.
struct Pod {
    /// Which life of the pod this is: a restart begins another.
    instance: u64,
    /// The pod it takes to own the key.
    owner: usize,
    /// Its own bucket while it owns the key; otherwise its view, answered from at once.
    bucket: TokenBucket,
    /// Hits admitted here and not yet sent to the owner.
    unsent: u64,
    /// Batches sent to the present owner so far.
    sent: u64,
    /// With the ledger: batches sent and not yet seen applied in a broadcast, as
    /// `(sequence, count, last sent)`.
    unacknowledged: VecDeque<(u64, u64, u64)>,
    /// As owner: whether the bucket changed since the last broadcast.
    changed: bool,
    /// Admitted in the cycle under way.
    spent: u64,
    /// The most it may admit in the cycle under way, if bounded.
    allowance: Option<u64>,
    /// Which pods the owner last said were admitting the key.
    active: Vec<bool>,
    /// What the owner's count held at its last broadcast to this pod.
    counted: u64,
}

impl Ownership {
    pub(crate) fn new(contract: &Contract, pods: usize, sharing: Sharing) -> Self {
        let pods = pods.max(1);
        let cycle = sharing.cycle.max(1);
        let pod = || Pod {
            instance: 0,
            owner: OWNER,
            bucket: TokenBucket::new(contract.limit, contract.window, contract.burst),
            unsent: 0,
            sent: 0,
            unacknowledged: VecDeque::new(),
            changed: false,
            spent: 0,
            allowance: None,
            active: vec![false; pods],
            counted: 0,
        };
        let refill = u128::from(contract.limit.get()) * u128::from(cycle)
            / u128::from(contract.window.get());
        Self {
            sharing: Sharing { cycle, ..sharing },
            fresh: TokenBucket::new(contract.limit, contract.window, contract.burst),
            refill: u64::try_from(refill).unwrap_or(u64::MAX),
            pods: (0..pods).map(|_| pod()).collect(),
            owned: vec![Owned::new(pods); pods],
            alive: vec![true; pods],
            admitted: 0,
        }
    }

    /// The pod a request for `pod` is decided on: itself, or, if it has gone, the next that
    /// is still there, as a load balancer sends its traffic on.
    fn serving(&self, pod: usize) -> usize {
        let pods = self.pods.len();
        (0..pods)
            .map(|step| (pod + step) % pods)
            .find(|candidate| self.alive[*candidate])
            .unwrap_or(pod)
    }

    /// The owner of the pods that take the first pod still there to own the key: the count
    /// the run is judged by as it ends.
    fn main_owner(&self) -> usize {
        self.pods[self.serving(OWNER)].owner
    }

    /// What `pod` may admit in the cycle starting at `now`.
    fn allowance(&self, pod: usize, now: u64) -> Option<u64> {
        let at = &self.pods[pod];
        let share = |pods: usize| {
            let sees = at.bucket.remaining(now).saturating_add(self.refill);
            Some((sees / pods.max(1) as u64).max(1))
        };
        match self.sharing.bound {
            Bound::Unbounded => None,
            Bound::NewPods if at.active[pod] => None,
            Bound::NewPods => share(self.pods.len()),
            Bound::Every => {
                let others = (0..at.active.len())
                    .filter(|p| *p != pod && at.active[*p])
                    .count();
                share(others + 1)
            }
        }
    }

    /// Which pods `owner` has seen admit the key within the last few cycles.
    fn active(&self, owner: usize, now: u64) -> Vec<bool> {
        let lately = ACTIVE_CYCLES * self.sharing.cycle;
        self.owned[owner]
            .last_admitted
            .iter()
            .map(|last| last.is_some_and(|at| now - at <= lately))
            .collect()
    }

    /// `owner` applies `count` reported hits, each charged if there is room, and beyond
    /// that as its handling of excess says.
    fn apply(&mut self, owner: usize, count: u64, now: u64) {
        self.owned[owner].counted += count;
        let bucket = &mut self.pods[owner].bucket;
        for _ in 0..count {
            let room = bucket.admits(now);
            let owe = match self.sharing.excess {
                Excess::Drain => false,
                Excess::Debt => true,
                Excess::DebtUpTo(most) => {
                    bucket.retry_at(now).is_none_or(|retry| retry - now <= most)
                }
            };
            if room || owe {
                bucket.charge(now);
            }
        }
        self.pods[owner].changed = true;
    }

    /// The pods of `group` take `new` to own the key from `now`. If `new` was not their
    /// owner already, it carries on from its own view, which already counts its own hits,
    /// and every other pod of the group numbers its batches afresh for it, and drops or
    /// sends again those its old owner was not seen to apply.
    fn change_owner(&mut self, group: &[usize], new: usize, now: u64, outbox: &mut Outbox) {
        let pods = self.pods.len();
        if self.pods[new].owner != new {
            let at = &mut self.pods[new];
            let held: u64 = at.unacknowledged.iter().map(|(_, count, _)| count).sum();
            let mut owned = Owned::new(pods);
            owned.counted = at.counted + held + at.unsent;
            self.owned[new] = owned;
            at.unacknowledged.clear();
            at.unsent = 0;
            at.changed = true;
            at.owner = new;
        }
        let followers: Vec<usize> = group
            .iter()
            .copied()
            .filter(|pod| *pod != new && self.alive[*pod] && self.pods[*pod].owner != new)
            .collect();
        for pod in followers {
            self.follow(pod, new, now, outbox);
        }
    }

    /// `pod` takes `owner` to own the key, as a new owner or one that has restarted: it
    /// numbers its batches afresh, and drops or sends again those not yet seen applied.
    fn follow(&mut self, pod: usize, owner: usize, now: u64, outbox: &mut Outbox) {
        let at = &mut self.pods[pod];
        at.owner = owner;
        at.sent = 0;
        // Numbered afresh, so an owner this pod had before forgets its old numbers, or it
        // would take the new batches for ones it had applied.
        self.owned[owner].applied[pod] = 0;
        self.owned[owner].waiting[pod].clear();
        let batches: Vec<(u64, u64, u64)> = at.unacknowledged.drain(..).collect();
        if self.sharing.on_owner_change == OnOwnerChange::SendAgain {
            for (_, count, _) in batches {
                at.sent += 1;
                let hits = Message::Hits {
                    from: pod,
                    instance: at.instance,
                    sequence: at.sent,
                    count,
                };
                outbox.send(owner, hits);
                at.unacknowledged.push_back((at.sent, count, now));
            }
        }
    }

    /// `pod` restarts: it comes back as another instance, holding nothing — a full view,
    /// no hits unsent or unacknowledged. If it owned the key it owns it still, from
    /// nothing, and the pods that took it as owner number their batches afresh for it.
    fn restart(&mut self, pod: usize, now: u64, outbox: &mut Outbox) {
        let pods = self.pods.len();
        let fresh = self.fresh.clone();
        let at = &mut self.pods[pod];
        at.instance += 1;
        at.bucket = fresh;
        at.unsent = 0;
        at.sent = 0;
        at.unacknowledged.clear();
        at.spent = 0;
        at.allowance = None;
        at.active = vec![false; pods];
        at.counted = 0;
        at.changed = false;
        if at.owner == pod {
            at.changed = true;
            self.owned[pod] = Owned::new(pods);
            let followers: Vec<usize> = (0..pods)
                .filter(|f| *f != pod && self.alive[*f] && self.pods[*f].owner == pod)
                .collect();
            for follower in followers {
                self.follow(follower, pod, now, outbox);
            }
        }
    }
}

impl Cluster for Ownership {
    fn decide(&mut self, pod: usize, now: u64, _: &mut Outbox) -> bool {
        let pod = self.serving(pod);
        let at = &mut self.pods[pod];
        if at.allowance.is_some_and(|allowance| at.spent >= allowance) {
            return false;
        }
        let admitted = at.bucket.decide(now);
        if admitted {
            at.spent += 1;
            self.admitted += 1;
            if at.owner == pod {
                at.changed = true;
                self.owned[pod].counted += 1;
                self.owned[pod].last_admitted[pod] = Some(now);
            } else {
                at.unsent += 1;
            }
        }
        admitted
    }

    fn cycle(&self) -> Option<u64> {
        Some(self.sharing.cycle)
    }

    fn on_timer(&mut self, pod: usize, now: u64, outbox: &mut Outbox) {
        if !self.alive[pod] {
            return;
        }
        let owner = self.pods[pod].owner;
        let resend_after = RESEND_AFTER_CYCLES * self.sharing.cycle;
        let active = self.active(owner, now);
        if pod == owner {
            self.pods[pod].active = active.clone();
        }
        let allowance = self.allowance(pod, now);
        let followers: Vec<usize> = (0..self.pods.len())
            .filter(|to| *to != pod && self.alive[*to] && self.pods[*to].owner == pod)
            .collect();
        let (applied, counted) = (self.owned[owner].applied.clone(), self.owned[owner].counted);
        let at = &mut self.pods[pod];
        at.spent = 0;
        at.allowance = allowance;
        if pod != owner {
            for (sequence, count, sent) in &mut at.unacknowledged {
                if now - *sent >= resend_after {
                    *sent = now;
                    let again = Message::Hits {
                        from: pod,
                        instance: at.instance,
                        sequence: *sequence,
                        count: *count,
                    };
                    outbox.send(owner, again);
                }
            }
            if at.unsent > 0 {
                at.sent += 1;
                let hits = Message::Hits {
                    from: pod,
                    instance: at.instance,
                    sequence: at.sent,
                    count: at.unsent,
                };
                outbox.send(owner, hits);
                if self.sharing.ledger {
                    at.unacknowledged.push_back((at.sent, at.unsent, now));
                }
                at.unsent = 0;
            }
        } else if at.changed {
            at.changed = false;
            for to in followers {
                let state = Message::State {
                    from: pod,
                    bucket: at.bucket.clone(),
                    applied: applied.clone(),
                    active: active.clone(),
                    counted,
                    decided: None,
                };
                outbox.send(to, state);
            }
        }
    }

    fn on_message(&mut self, pod: usize, message: Message, now: u64, _: &mut Outbox) {
        if !self.alive[pod] {
            return;
        }
        match message {
            // Only a pod that takes itself to own the key applies hits.
            Message::Hits {
                from,
                instance,
                sequence,
                count,
            } if self.pods[pod].owner == pod => {
                let owned = &mut self.owned[pod];
                // A batch from a life of the pod that has ended is ignored; the first from a
                // new one starts its numbering again (10 §2, identity safety).
                if instance < owned.instances[from] {
                    return;
                }
                if instance > owned.instances[from] {
                    owned.instances[from] = instance;
                    owned.applied[from] = 0;
                    owned.waiting[from].clear();
                }
                owned.last_admitted[from] = Some(now);
                if !self.sharing.ledger {
                    self.apply(pod, count, now);
                    return;
                }
                // In order, once each: a batch already applied is ignored, and one that came
                // ahead of a missing one waits for it. Either way the next broadcast goes
                // out, so that a pod whose earlier one was lost learns what is applied.
                if sequence > self.owned[pod].applied[from] {
                    self.owned[pod].waiting[from].insert(sequence, count);
                }
                loop {
                    let next = self.owned[pod].applied[from] + 1;
                    let Some(count) = self.owned[pod].waiting[from].remove(&next) else {
                        break;
                    };
                    self.owned[pod].applied[from] = next;
                    self.apply(pod, count, now);
                }
                self.pods[pod].changed = true;
            }
            // Only from the owner this pod takes to be the owner: any other is stale.
            Message::State {
                from,
                bucket,
                applied,
                active,
                counted,
                ..
            } if self.pods[pod].owner == from && from != pod => {
                let at = &mut self.pods[pod];
                at.bucket = bucket;
                at.active = active;
                at.counted = counted;
                if self.sharing.ledger {
                    // The owner's bucket, plus what this pod admitted that it has not seen:
                    // batches the broadcast's applied mark does not cover, and hits unsent.
                    let seen = applied.get(pod).copied().unwrap_or(0);
                    at.unacknowledged
                        .retain(|(sequence, _, _)| *sequence > seen);
                    let unseen: u64 = at.unacknowledged.iter().map(|(_, count, _)| count).sum();
                    for _ in 0..unseen + at.unsent {
                        at.bucket.charge(now);
                    }
                }
                // Without it, Gubernator's overwrite: what this pod admitted since its last
                // report is no longer counted in its view.
            }
            _ => {}
        }
    }

    fn on_fault(&mut self, event: FaultEvent, now: u64, outbox: &mut Outbox) {
        let pods = self.pods.len();
        match event {
            FaultEvent::OwnerLost => {
                let old = self.main_owner();
                self.alive[old] = false;
                let new = self.serving(old);
                let group: Vec<usize> = (0..pods).collect();
                self.change_owner(&group, new, now, outbox);
            }
            // The pods from `split` on cannot reach the others: they take the first of them
            // to own the key — two owners, each counting its side alone (06 §4).
            FaultEvent::PartitionStarts { split } => {
                let side: Vec<usize> = (split.min(pods)..pods).collect();
                if let Some(&first) = side.iter().find(|pod| self.alive[**pod]) {
                    self.change_owner(&side, first, now, outbox);
                }
            }
            // They meet again and take the others' owner back. Nothing merges: what the side
            // counted alone stays with the owner it gave up.
            FaultEvent::PartitionHeals { split } => {
                let side: Vec<usize> = (split.min(pods)..pods).collect();
                let owner = self.main_owner();
                self.change_owner(&side, owner, now, outbox);
            }
            FaultEvent::Restart { pod } if pod < pods && self.alive[pod] => {
                self.restart(pod, now, outbox);
            }
            FaultEvent::Restart { .. } => {}
            FaultEvent::OwnerRestarts => self.restart(self.main_owner(), now, outbox),
        }
    }

    fn unapplied(&self) -> u64 {
        self.admitted
            .saturating_sub(self.owned[self.main_owner()].counted)
    }

    fn counted_twice(&self) -> u64 {
        self.owned[self.main_owner()]
            .counted
            .saturating_sub(self.admitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sharing(cycle: u64, excess: Excess, ledger: bool, bound: Bound) -> Sharing {
        Sharing {
            cycle,
            excess,
            ledger,
            bound,
            on_owner_change: OnOwnerChange::Drop,
        }
    }
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;

    fn fleet(pods: usize) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        Ownership::new(
            &contract,
            pods,
            sharing(10 * MS, Excess::Drain, false, Bound::Unbounded),
        )
    }

    /// Delivers what `outbox` holds, as the network would with no delay.
    fn deliver(fleet: &mut Ownership, outbox: &mut Outbox, now: u64) {
        let sent = std::mem::take(&mut outbox.sent);
        for (to, message) in sent {
            fleet.on_message(to, message, now, outbox);
        }
    }

    fn admits(fleet: &mut Ownership, pod: usize, count: u64, now: u64) -> u64 {
        let mut outbox = Outbox::default();
        (0..count)
            .filter(|_| fleet.decide(pod, now, &mut outbox))
            .count() as u64
    }

    #[test]
    fn a_pods_hits_reach_the_owner_and_its_bucket_comes_back() {
        let mut fleet = fleet(2);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, 1, 10, 0), 10);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(outbox.sent.len(), 1, "one batch of hits");
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(
            admits(&mut fleet, OWNER, 1, 0),
            0,
            "the owner's bucket holds the hits"
        );
        fleet.on_timer(OWNER, 0, &mut outbox);
        assert_eq!(outbox.sent.len(), 1, "one broadcast, to the one other pod");
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.pods[1].bucket, fleet.pods[OWNER].bucket);
    }

    #[test]
    fn hits_that_do_not_fit_are_forgiven() {
        let mut fleet = fleet(3);
        let mut outbox = Outbox::default();
        assert_eq!(
            admits(&mut fleet, 1, 10, 0) + admits(&mut fleet, 2, 10, 0),
            20
        );
        fleet.on_timer(1, 0, &mut outbox);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        // Twenty hits on a bucket of ten: empty, not ten owed. A tenth of a second later,
        // ten more fit.
        assert_eq!(admits(&mut fleet, OWNER, 20, 100 * MS), 10);
    }

    fn owned(excess: Excess) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        Ownership::new(
            &contract,
            3,
            sharing(10 * MS, excess, false, Bound::Unbounded),
        )
    }

    /// Twenty hits reported on a bucket of ten, then what the owner admits a tenth of a
    /// second later, when ten have refilled.
    fn after_twenty_reported(excess: Excess) -> u64 {
        let mut fleet = owned(excess);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 10, 0);
        admits(&mut fleet, 2, 10, 0);
        fleet.on_timer(1, 0, &mut outbox);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        admits(&mut fleet, OWNER, 20, 100 * MS)
    }

    #[test]
    fn debt_is_paid_back_before_anything_more_is_admitted() {
        assert_eq!(after_twenty_reported(Excess::Drain), 10);
        // Ten owed: the tenth of a second's refill pays them back, and nothing more fits.
        assert_eq!(after_twenty_reported(Excess::Debt), 0);
        // Owing at most what 50 ms pays back: five owed, five admitted.
        assert_eq!(after_twenty_reported(Excess::DebtUpTo(50 * MS)), 5);
    }

    /// Gubernator's flaw that 06 §4 sets out to change: the owner's bucket replaces the
    /// copy, and what the pod admitted since its last report is forgotten.
    #[test]
    fn a_broadcast_forgets_what_the_pod_admitted_since_its_last_report() {
        let mut fleet = fleet(2);
        let mut outbox = Outbox::default();
        fleet.on_timer(OWNER, 0, &mut outbox);
        assert!(outbox.sent.is_empty(), "nothing changed, nothing broadcast");
        assert_eq!(admits(&mut fleet, OWNER, 1, 0), 1);
        fleet.on_timer(OWNER, 0, &mut outbox);
        assert_eq!(
            admits(&mut fleet, 1, 10, 0),
            10,
            "the pod spends its whole copy"
        );
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(
            admits(&mut fleet, 1, 10, 0),
            9,
            "and, overwritten, spends it again"
        );
    }

    fn ledger(pods: usize) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        Ownership::new(
            &contract,
            pods,
            sharing(10 * MS, Excess::Drain, true, Bound::Unbounded),
        )
    }

    /// The same as the flaw above, with the ledger: the pod's ten unsent hits are counted
    /// on top of the owner's bucket, and nothing more fits.
    #[test]
    fn with_the_ledger_a_broadcast_forgets_nothing() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, OWNER, 1, 0), 1);
        fleet.on_timer(OWNER, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 1, 10, 0), 10);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(admits(&mut fleet, 1, 10, 0), 0);
    }

    #[test]
    fn a_batch_delivered_twice_is_applied_once() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, 1, 4, 0), 4);
        fleet.on_timer(1, 0, &mut outbox);
        let batch = outbox.sent[0].clone();
        outbox.sent.push(batch);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(admits(&mut fleet, OWNER, 10, 0), 6, "four hits, not eight");
    }

    #[test]
    fn a_batch_ahead_of_a_missing_one_waits_for_it() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 3, 0);
        fleet.on_timer(1, 0, &mut outbox);
        admits(&mut fleet, 1, 2, 0);
        fleet.on_timer(1, 0, &mut outbox);
        let (first, second) = (outbox.sent.remove(0), outbox.sent.remove(0));
        fleet.on_message(first.0, second.1, 0, &mut outbox);
        assert_eq!(fleet.owned[OWNER].applied[1], 0, "the second batch waits");
        assert_eq!(fleet.pods[OWNER].bucket.remaining(0), 10);
        fleet.on_message(first.0, first.1, 0, &mut outbox);
        assert_eq!(
            fleet.owned[OWNER].applied[1], 2,
            "then both are applied, in order"
        );
        assert_eq!(fleet.pods[OWNER].bucket.remaining(0), 5);
    }

    /// A batch the broadcast says is applied is counted once, in the owner's bucket, and no
    /// longer on top of it.
    #[test]
    fn an_applied_batch_is_no_longer_counted_apart() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert!(fleet.pods[1].unacknowledged.is_empty());
        assert_eq!(fleet.pods[1].bucket.remaining(0), 6);
    }

    /// A broadcast made before the owner applied a batch the pod has sent: the batch is not
    /// covered by its applied marks, and is counted on top.
    #[test]
    fn a_batch_sent_and_not_yet_applied_is_still_counted() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, OWNER, 1, 0), 1);
        admits(&mut fleet, 1, 9, 0);
        fleet.on_timer(1, 0, &mut outbox);
        let batch = outbox.sent.remove(0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(
            admits(&mut fleet, 1, 10, 0),
            0,
            "nine sent and one the owner's own"
        );
        fleet.on_message(batch.0, batch.1, 0, &mut outbox);
        assert_eq!(fleet.owned[OWNER].applied[1], 1);
    }

    #[test]
    fn a_lost_batch_is_sent_again_and_applied_once() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        outbox.sent.clear();
        fleet.on_timer(1, 40 * MS - 1, &mut outbox);
        assert!(outbox.sent.is_empty(), "not before four cycles");
        fleet.on_timer(1, 40 * MS, &mut outbox);
        assert_eq!(outbox.sent.len(), 1, "sent again");
        deliver(&mut fleet, &mut outbox, 40 * MS);
        assert_eq!((fleet.owned[OWNER].applied[1], fleet.unapplied()), (1, 0));
    }

    /// The broadcast that would have told the pod its batch was applied is lost: the pod
    /// sends it again, the owner ignores it but broadcasts, and the pod stops counting it.
    #[test]
    fn a_batch_sent_again_after_it_was_applied_is_acknowledged() {
        let mut fleet = ledger(2);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        outbox.sent.clear();
        fleet.on_timer(1, 40 * MS, &mut outbox);
        deliver(&mut fleet, &mut outbox, 40 * MS);
        assert_eq!(
            fleet.pods[OWNER].bucket.remaining(40 * MS),
            10,
            "applied once"
        );
        fleet.on_timer(OWNER, 40 * MS, &mut outbox);
        deliver(&mut fleet, &mut outbox, 40 * MS);
        assert!(fleet.pods[1].unacknowledged.is_empty());
    }

    fn bounded(pods: usize, bound: Bound) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        Ownership::new(
            &contract,
            pods,
            sharing(10 * MS, Excess::Drain, true, bound),
        )
    }

    /// Tells every pod, as the owner's broadcast would, that `active` pods are admitting.
    fn say_active(fleet: &mut Ownership, active: &[bool]) {
        for pod in &mut fleet.pods {
            pod.active = active.to_vec();
        }
    }

    /// Two pods admitting: each may spend half of what it sees in a cycle — the ten in the
    /// bucket and the one a 10 ms cycle refills — and again in the next cycle.
    #[test]
    fn every_pod_holds_to_its_share_each_cycle() {
        let mut fleet = bounded(3, Bound::Every);
        let mut outbox = Outbox::default();
        say_active(&mut fleet, &[false, true, true]);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 1, 10, 0), 5);
        fleet.on_timer(1, 10 * MS, &mut outbox);
        assert_eq!(
            admits(&mut fleet, 1, 10, 10 * MS),
            3,
            "half of the six it sees"
        );
    }

    /// A pod that is the only one admitting gets the whole of what it sees: skewed traffic
    /// is not shaved.
    #[test]
    fn a_pod_admitting_alone_is_not_held_back() {
        let mut fleet = bounded(3, Bound::Every);
        let mut outbox = Outbox::default();
        say_active(&mut fleet, &[false, true, false]);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 1, 20, 0), 10);
    }

    /// 06 §4 as written: a pod already admitting may spend all it sees; one new to the key
    /// its share of the fleet.
    #[test]
    fn as_written_only_a_pod_new_to_the_key_is_held_back() {
        let mut fleet = bounded(3, Bound::NewPods);
        let mut outbox = Outbox::default();
        say_active(&mut fleet, &[false, true, false]);
        fleet.on_timer(1, 0, &mut outbox);
        fleet.on_timer(2, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 2, 20, 0), 3, "eleven over three pods");
        assert_eq!(
            admits(&mut fleet, 1, 20, 0),
            10,
            "all it sees: its own view, which has not yet heard of the other's three"
        );
    }

    #[test]
    fn the_owner_says_which_pods_admitted_lately() {
        let mut fleet = bounded(3, Bound::Every);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 2, 1, 0);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.pods[1].active, vec![false, false, true]);
        fleet.on_timer(OWNER, 41 * MS, &mut outbox);
        assert_eq!(
            fleet.pods[OWNER].active,
            vec![false; 3],
            "forgotten after four cycles"
        );
    }

    /// At 1,000 a second a 10 ms cycle refills ten: two pods admitting each get half of the
    /// ten left and the ten to come, not half of the ten left alone.
    #[test]
    fn a_share_counts_what_the_cycle_refills() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(1_000),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let mut fleet = Ownership::new(
            &contract,
            3,
            sharing(10 * MS, Excess::Drain, true, Bound::Every),
        );
        let mut outbox = Outbox::default();
        say_active(&mut fleet, &[false, true, true]);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 1, 20, 0), 10);
        fleet.on_timer(1, 10 * MS, &mut outbox);
        assert_eq!(admits(&mut fleet, 1, 20, 10 * MS), 10);
    }

    fn changing(on_owner_change: OnOwnerChange) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let sharing = Sharing {
            on_owner_change,
            ..sharing(10 * MS, Excess::Drain, true, Bound::Unbounded)
        };
        Ownership::new(&contract, 3, sharing)
    }

    #[test]
    fn when_the_owner_goes_the_next_pod_owns_the_key_and_takes_its_traffic() {
        let mut fleet = changing(OnOwnerChange::Drop);
        let mut outbox = Outbox::default();
        fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
        assert_eq!(fleet.pods[2].owner, 1);
        assert_eq!(admits(&mut fleet, OWNER, 3, 0), 3, "decided on pod 1");
        assert_eq!(fleet.pods[1].bucket.remaining(0), 7);
        admits(&mut fleet, 2, 2, 0);
        fleet.on_timer(2, 0, &mut outbox);
        assert_eq!(outbox.sent[0].0, 1, "hits go to the new owner");
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.pods[1].bucket.remaining(0), 5);
        fleet.on_timer(1, 0, &mut outbox);
        assert!(
            outbox.sent.iter().all(|(to, _)| *to == 2),
            "broadcast to the pod left"
        );
        fleet.on_timer(OWNER, 0, &mut outbox);
        assert_eq!(outbox.sent.len(), 1, "the old owner does nothing");
    }

    /// A batch pod 2 sent the old owner, never seen applied: dropped, it is lost; sent
    /// again, numbered afresh, it reaches the new owner.
    #[test]
    fn unapplied_batches_are_dropped_or_sent_again() {
        for (on_owner_change, lost) in [(OnOwnerChange::Drop, 4), (OnOwnerChange::SendAgain, 0)] {
            let mut fleet = changing(on_owner_change);
            let mut outbox = Outbox::default();
            admits(&mut fleet, 2, 4, 0);
            fleet.on_timer(2, 0, &mut outbox);
            outbox.sent.clear();
            fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
            deliver(&mut fleet, &mut outbox, 0);
            assert_eq!(fleet.unapplied(), lost, "{on_owner_change:?}");
            assert_eq!(fleet.counted_twice(), 0, "{on_owner_change:?}");
        }
    }

    /// The old owner applied pod 2's batch and broadcast it to pod 1, but the broadcast
    /// saying so never reached pod 2: sent again, the batch is counted twice.
    #[test]
    fn a_batch_sent_again_can_be_counted_twice() {
        let mut fleet = changing(OnOwnerChange::SendAgain);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 2, 4, 0);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        let to_pod_1 = outbox.sent.remove(0);
        outbox.sent.clear();
        fleet.on_message(to_pod_1.0, to_pod_1.1, 0, &mut outbox);
        fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!((fleet.unapplied(), fleet.counted_twice()), (0, 4));
    }

    #[test]
    fn a_broadcast_from_an_owner_that_has_gone_is_ignored() {
        let mut fleet = changing(OnOwnerChange::Drop);
        let mut outbox = Outbox::default();
        admits(&mut fleet, OWNER, 5, 0);
        fleet.on_timer(OWNER, 0, &mut outbox);
        let stale = outbox.sent.pop().unwrap();
        outbox.sent.clear();
        fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
        fleet.on_message(stale.0, stale.1, 0, &mut outbox);
        assert_eq!(fleet.pods[2].bucket.remaining(0), 10);
    }

    /// Pods 2 and 3 are cut off from 0 and 1: they take pod 2 to own the key and count
    /// alone; when they meet the others again they take pod 0 back, and what they counted
    /// alone is not merged.
    #[test]
    fn a_partition_makes_two_owners_and_healing_merges_nothing() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let mut fleet = Ownership::new(
            &contract,
            4,
            sharing(10 * MS, Excess::Drain, true, Bound::Unbounded),
        );
        let mut outbox = Outbox::default();
        fleet.on_fault(FaultEvent::PartitionStarts { split: 2 }, 0, &mut outbox);
        let owners: Vec<usize> = fleet.pods.iter().map(|pod| pod.owner).collect();
        assert_eq!(owners, vec![0, 0, 2, 2]);
        assert_eq!(admits(&mut fleet, 3, 6, 0), 6);
        fleet.on_timer(3, 0, &mut outbox);
        assert_eq!(outbox.sent[0].0, 2, "hits go to the side's own owner");
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_timer(2, 0, &mut outbox);
        assert!(
            outbox.sent.iter().all(|(to, _)| *to == 3),
            "it broadcasts to its side"
        );
        outbox.sent.clear();
        assert_eq!(
            admits(&mut fleet, OWNER, 10, 0),
            10,
            "the other side has all of it"
        );
        fleet.on_fault(FaultEvent::PartitionHeals { split: 2 }, 0, &mut outbox);
        let owners: Vec<usize> = fleet.pods.iter().map(|pod| pod.owner).collect();
        assert_eq!(owners, vec![0; 4]);
        assert_eq!(fleet.unapplied(), 6, "what the side counted alone");
    }

    /// Pod 3's first batch was applied by pod 0 before the partition; after it, its batches
    /// are numbered afresh, and pod 0 must not take the new first for the old.
    #[test]
    fn an_owner_taken_back_applies_batches_numbered_afresh() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let mut fleet = Ownership::new(
            &contract,
            4,
            sharing(10 * MS, Excess::Drain, true, Bound::Unbounded),
        );
        let mut outbox = Outbox::default();
        admits(&mut fleet, 3, 2, 0);
        fleet.on_timer(3, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_fault(FaultEvent::PartitionStarts { split: 2 }, 0, &mut outbox);
        fleet.on_fault(FaultEvent::PartitionHeals { split: 2 }, 0, &mut outbox);
        outbox.sent.clear();
        admits(&mut fleet, 3, 3, 0);
        fleet.on_timer(3, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(
            fleet.pods[OWNER].bucket.remaining(0),
            5,
            "two before, three after"
        );
    }

    /// Pod 2 restarts with its old life's second batch still on its way: the owner ignores
    /// it, though its number is above any the new life has sent, and takes the new life's
    /// first batch as its first.
    #[test]
    fn a_batch_from_a_life_that_has_ended_is_ignored() {
        let mut fleet = ledger(3);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 2, 1, 0);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        admits(&mut fleet, 2, 4, 0);
        fleet.on_timer(2, 0, &mut outbox);
        let old = outbox.sent.remove(0);
        fleet.on_fault(FaultEvent::Restart { pod: 2 }, 0, &mut outbox);
        assert_eq!(
            fleet.pods[2].bucket.remaining(0),
            10,
            "it comes back with nothing"
        );
        admits(&mut fleet, 2, 3, 0);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_message(old.0, old.1, 0, &mut outbox);
        assert_eq!(
            fleet.pods[OWNER].bucket.remaining(0),
            6,
            "one and three, not the four"
        );
        assert_eq!(fleet.unapplied(), 4, "the old life's four are lost");
    }

    /// The owner restarts: it owns the key still, from nothing, and the other pods number
    /// their batches afresh for it.
    #[test]
    fn an_owner_that_restarts_starts_from_nothing() {
        let mut fleet = ledger(3);
        let mut outbox = Outbox::default();
        admits(&mut fleet, OWNER, 6, 0);
        admits(&mut fleet, 1, 2, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        fleet.on_fault(FaultEvent::Restart { pod: OWNER }, 0, &mut outbox);
        assert_eq!(fleet.pods[OWNER].bucket.remaining(0), 10);
        assert_eq!((fleet.pods[1].owner, fleet.pods[1].sent), (OWNER, 0));
        admits(&mut fleet, 1, 1, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.pods[OWNER].bucket.remaining(0), 9);
        assert_eq!(fleet.unapplied(), 8, "the count from before is gone");
    }
}
