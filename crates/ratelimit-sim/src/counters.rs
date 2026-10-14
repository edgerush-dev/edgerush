//! Per-pod counters (06 §5): no owner. Every pod counts what it admits, a total that only
//! grows, and sends it to every other pod each cycle; each pod charges its own view of the
//! key with what it learns the others admitted.

use crate::{Bound, Cluster, Contract, Counting, Excess, FaultEvent, Message, Outbox};
use edgerush_ratelimit::reference::{Model, TokenBucket};

/// For how many cycles before the latest growth a pod knows of another counts as admitting
/// the key.
const ACTIVE_CYCLES: u64 = 4;

/// A fleet sharing one key by per-pod counters.
pub(crate) struct Counters {
    counting: Counting,
    /// What a cycle refills, in requests, rounded down.
    refill: u64,
    /// A new bucket, full: what a pod starts from, and comes back with from a restart.
    fresh: TokenBucket,
    pods: Vec<Pod>,
    /// Which pods are still there.
    alive: Vec<bool>,
    /// Hits admitted anywhere.
    admitted: u64,
}

/// What one pod holds of the key.
struct Pod {
    /// Which life of the pod this is: a restart begins another.
    instance: u64,
    /// Its view of the key: its own admissions, and the others' as it learns of them.
    bucket: TokenBucket,
    /// How many it has admitted in this life.
    total: u64,
    /// For each pod: the life last heard from, and the highest total heard in it; none
    /// until this life of the pod has heard from it.
    heard: Vec<Option<(u64, u64)>>,
    /// For each pod: when its total last grew.
    grew: Vec<Option<u64>>,
    /// Admitted in the cycle under way.
    spent: u64,
    /// The most it may admit in the cycle under way, if bounded.
    allowance: Option<u64>,
    /// Hits counted in its view from lives that have since restarted, by pod: kept so that
    /// what the fleet's count holds can be told at the end.
    earlier_lives: Vec<u64>,
}

impl Counters {
    pub(crate) fn new(contract: &Contract, pods: usize, counting: Counting) -> Self {
        let pods = pods.max(1);
        let cycle = counting.cycle.max(1);
        let fresh = TokenBucket::new(contract.limit, contract.window, contract.burst);
        let pod = || Pod {
            instance: 0,
            bucket: fresh.clone(),
            total: 0,
            heard: vec![Some((0, 0)); pods],
            grew: vec![None; pods],
            spent: 0,
            allowance: None,
            earlier_lives: vec![0; pods],
        };
        let refill = u128::from(contract.limit.get()) * u128::from(cycle)
            / u128::from(contract.window.get());
        Self {
            counting: Counting { cycle, ..counting },
            refill: u64::try_from(refill).unwrap_or(u64::MAX),
            pods: (0..pods).map(|_| pod()).collect(),
            fresh: fresh.clone(),
            alive: vec![true; pods],
            admitted: 0,
        }
    }

    /// The pod a request for `pod` is decided on: itself, or the next still there.
    fn serving(&self, pod: usize) -> usize {
        let pods = self.pods.len();
        (0..pods)
            .map(|step| (pod + step) % pods)
            .find(|candidate| self.alive[*candidate])
            .unwrap_or(pod)
    }

    /// What `pod` may admit in the cycle starting at `now`.
    fn allowance(&self, pod: usize, now: u64) -> Option<u64> {
        let at = &self.pods[pod];
        let lately = ACTIVE_CYCLES * self.counting.cycle;
        // Who was admitting is judged as of the last traffic this pod knows of, not as of
        // now: pods that burst together and then fall quiet are still sharing the bucket
        // when they burst again. An owner's followers do the same, keeping the active pods
        // of the last broadcast until another comes.
        let last = at.grew.iter().flatten().max().copied().unwrap_or(now);
        let active = |p: usize| at.grew[p].is_some_and(|grew| last - grew <= lately);
        let share = |pods: usize| {
            let sees = at.bucket.remaining(now).saturating_add(self.refill);
            Some((sees / pods.max(1) as u64).max(1))
        };
        match self.counting.bound {
            Bound::Unbounded => None,
            Bound::NewPods if active(pod) => None,
            Bound::NewPods => share(self.pods.len()),
            Bound::Every | Bound::EveryCarrying | Bound::Granted | Bound::Credit => {
                let others = (0..self.pods.len())
                    .filter(|p| *p != pod && active(*p))
                    .count();
                share(others + 1)
            }
        }
    }

    /// `pod` charges its view with `count` hits another pod admitted, each if there is
    /// room, and beyond that as the handling of excess says.
    fn charge_others(&mut self, pod: usize, count: u64, now: u64) {
        let bucket = &mut self.pods[pod].bucket;
        for _ in 0..count {
            let room = bucket.admits(now);
            let owe = match self.counting.excess {
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
    }

    /// How many admissions `pod`'s view holds: its own and every other pod's it has heard.
    fn known_by(&self, pod: usize) -> u64 {
        let at = &self.pods[pod];
        let others: u64 = (0..self.pods.len())
            .filter(|p| *p != pod)
            .map(|p| at.heard[p].map_or(0, |(_, total)| total) + at.earlier_lives[p])
            .sum();
        at.total + at.earlier_lives[pod] + others
    }

    /// `pod` restarts: it comes back as another life of itself, holding nothing.
    fn restart(&mut self, pod: usize) {
        let pods = self.pods.len();
        let fresh = self.fresh.clone();
        let at = &mut self.pods[pod];
        at.instance += 1;
        at.bucket = fresh;
        at.total = 0;
        at.heard = vec![None; pods];
        at.grew = vec![None; pods];
        at.spent = 0;
        at.allowance = None;
        at.earlier_lives = vec![0; pods];
    }

    /// The fleet's count: what the best-informed pod still there holds.
    fn known(&self) -> u64 {
        (0..self.pods.len())
            .filter(|pod| self.alive[*pod])
            .map(|pod| self.known_by(pod))
            .max()
            .unwrap_or(0)
    }
}

impl Cluster for Counters {
    fn decide(&mut self, pod: usize, now: u64, _: &mut Outbox) -> bool {
        let pod = self.serving(pod);
        let at = &mut self.pods[pod];
        if at.allowance.is_some_and(|allowance| at.spent >= allowance) {
            return false;
        }
        let admitted = at.bucket.decide(now);
        if admitted {
            at.spent += 1;
            at.total += 1;
            at.grew[pod] = Some(now);
            self.admitted += 1;
        }
        admitted
    }

    fn cycle(&self) -> Option<u64> {
        Some(self.counting.cycle)
    }

    fn on_timer(&mut self, pod: usize, now: u64, outbox: &mut Outbox) {
        if !self.alive[pod] {
            return;
        }
        let allowance = self.allowance(pod, now);
        let bound = self.counting.bound;
        let pods = self.pods.len();
        let at = &mut self.pods[pod];
        at.allowance = bound.carry(allowance, at.allowance, at.spent);
        at.spent = 0;
        if at.total == 0 {
            return;
        }
        for to in (0..pods).filter(|to| *to != pod && self.alive[*to]) {
            let counts = Message::Counts {
                from: pod,
                instance: at.instance,
                total: at.total,
            };
            outbox.send(to, counts);
        }
    }

    fn on_message(&mut self, pod: usize, message: Message, now: u64, _: &mut Outbox) {
        if !self.alive[pod] {
            return;
        }
        let Message::Counts {
            from,
            instance,
            total,
        } = message
        else {
            return;
        };
        let at = &mut self.pods[pod];
        // A pod that has restarted takes the first total it hears from each other pod as
        // where it starts: what that total holds was admitted before, mostly long enough
        // ago to have refilled, and charging it now would refuse traffic that conforms.
        let Some((life, heard)) = at.heard[from] else {
            at.heard[from] = Some((instance, total));
            return;
        };
        // A total from a life that has ended is stale; a new life counts from nothing. A
        // total no higher than one already heard adds nothing: totals only grow.
        let grown = if instance < life {
            return;
        } else if instance > life {
            at.earlier_lives[from] += heard;
            at.heard[from] = Some((instance, total));
            total
        } else if total > heard {
            at.heard[from] = Some((instance, total));
            total - heard
        } else {
            return;
        };
        at.grew[from] = Some(now);
        self.charge_others(pod, grown, now);
    }

    fn on_fault(&mut self, event: FaultEvent, _: u64, _: &mut Outbox) {
        match event {
            // No owner to lose: the first pod goes, and its traffic goes to the next.
            FaultEvent::OwnerLost => {
                let first = self.serving(0);
                self.alive[first] = false;
            }
            // Nothing to elect: messages across the cut are lost, and the totals heal it.
            FaultEvent::PartitionStarts { .. } | FaultEvent::PartitionHeals { .. } => {}
            // No owner to restart either: the first pod does.
            FaultEvent::OwnerRestarts => self.restart(self.serving(0)),
            FaultEvent::Restart { pod } if pod < self.pods.len() && self.alive[pod] => {
                self.restart(pod);
            }
            FaultEvent::Restart { .. } => {}
        }
    }

    fn unapplied(&self) -> u64 {
        self.admitted.saturating_sub(self.known())
    }

    fn counted_twice(&self) -> u64 {
        self.known().saturating_sub(self.admitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;

    fn fleet(pods: usize, excess: Excess) -> Counters {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let counting = Counting {
            cycle: 10 * MS,
            excess,
            bound: Bound::Unbounded,
        };
        Counters::new(&contract, pods, counting)
    }

    fn admits(fleet: &mut Counters, pod: usize, count: u64, now: u64) -> u64 {
        let mut outbox = Outbox::default();
        (0..count)
            .filter(|_| fleet.decide(pod, now, &mut outbox))
            .count() as u64
    }

    fn counts(from: usize, instance: u64, total: u64) -> Message {
        Message::Counts {
            from,
            instance,
            total,
        }
    }

    #[test]
    fn a_pod_charges_what_others_admitted_once() {
        let mut fleet = fleet(3, Excess::Drain);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, 1, 4, 0), 4);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(outbox.sent.len(), 2, "to each other pod");
        for (to, message) in std::mem::take(&mut outbox.sent) {
            fleet.on_message(to, message.clone(), 0, &mut outbox);
            fleet.on_message(to, message, 0, &mut outbox);
        }
        assert_eq!(
            fleet.pods[0].bucket.remaining(0),
            6,
            "four, though told twice"
        );
        fleet.on_message(0, counts(1, 0, 3), 0, &mut outbox);
        assert_eq!(
            fleet.pods[0].bucket.remaining(0),
            6,
            "a lower total adds nothing"
        );
    }

    /// A total lost on the way is made good by the next: it carries everything.
    #[test]
    fn a_lost_total_heals_with_the_next() {
        let mut fleet = fleet(2, Excess::Drain);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 3, 0);
        admits(&mut fleet, 1, 2, 0);
        fleet.on_message(0, counts(1, 0, 5), 0, &mut outbox);
        assert_eq!(fleet.pods[0].bucket.remaining(0), 5);
        assert_eq!((fleet.unapplied(), fleet.counted_twice()), (0, 0));
    }

    /// Pod 1 restarts: its new life counts from nothing, a total from the old life still on
    /// its way is ignored, and what the old life admitted stays counted.
    #[test]
    fn a_new_life_counts_from_nothing_and_an_old_one_is_ignored() {
        let mut fleet = fleet(2, Excess::Drain);
        let mut outbox = Outbox::default();
        fleet.on_message(0, counts(1, 0, 4), 0, &mut outbox);
        fleet.on_message(0, counts(1, 1, 2), 0, &mut outbox);
        assert_eq!(fleet.pods[0].bucket.remaining(0), 4, "four and two");
        fleet.on_message(0, counts(1, 0, 9), 0, &mut outbox);
        assert_eq!(fleet.pods[0].bucket.remaining(0), 4);
        assert_eq!(fleet.known_by(0), 6);
    }

    /// Pods that burst together are bounded together the next time, however long they were
    /// quiet in between.
    #[test]
    fn pods_quiet_since_they_last_admitted_still_share_the_bucket() {
        let mut fleet = fleet(3, Excess::Drain);
        fleet.counting.bound = Bound::Every;
        let mut outbox = Outbox::default();
        fleet.on_message(0, counts(1, 0, 1), 0, &mut outbox);
        fleet.on_message(0, counts(2, 0, 1), 0, &mut outbox);
        let later = 5_000 * MS;
        assert_eq!(fleet.allowance(0, later), Some(3), "a third of ten and one");
        fleet.on_message(0, counts(1, 0, 1), later, &mut outbox);
        fleet.on_message(0, counts(2, 0, 2), later, &mut outbox);
        assert_eq!(
            fleet.allowance(0, later),
            Some(5),
            "pod 1 has gone quiet since: the same total again is not growth"
        );
    }

    /// A pod sends its total to every other pod still there, once it has admitted any.
    #[test]
    fn a_total_goes_to_every_pod_still_there_once_there_is_one() {
        let mut fleet = fleet(3, Excess::Drain);
        let mut outbox = Outbox::default();
        fleet.on_timer(1, 0, &mut outbox);
        assert!(outbox.sent.is_empty(), "nothing admitted, nothing to say");
        fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
        admits(&mut fleet, 1, 1, 0);
        fleet.on_timer(1, 0, &mut outbox);
        let to: Vec<usize> = outbox.sent.iter().map(|(to, _)| *to).collect();
        assert_eq!(to, vec![2], "pod 0 has gone");
    }

    /// The bound is on each cycle: what a pod admitted in one does not count against the
    /// next.
    #[test]
    fn a_bounded_pod_admits_its_share_again_each_cycle() {
        let mut fleet = fleet(2, Excess::Drain);
        fleet.counting.bound = Bound::Every;
        let mut outbox = Outbox::default();
        fleet.on_message(0, counts(1, 0, 1), 0, &mut outbox);
        fleet.on_timer(0, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 0, 9, 0), 5, "half of nine and one");
        fleet.on_timer(0, 10 * MS, &mut outbox);
        assert_eq!(
            admits(&mut fleet, 0, 9, 10 * MS),
            3,
            "half of the five left and one"
        );
    }

    /// Pod 0 restarts: the first total it hears from pod 1 is where it starts, not hits to
    /// charge, and only what pod 1 admits after that is charged.
    #[test]
    fn a_restarted_pod_starts_from_the_first_total_it_hears() {
        let mut fleet = fleet(2, Excess::Drain);
        let mut outbox = Outbox::default();
        fleet.on_message(0, counts(1, 0, 4), 0, &mut outbox);
        fleet.on_fault(FaultEvent::Restart { pod: 0 }, 0, &mut outbox);
        fleet.on_message(0, counts(1, 0, 50), 0, &mut outbox);
        assert_eq!(fleet.pods[0].bucket.remaining(0), 10, "nothing charged");
        fleet.on_message(0, counts(1, 0, 53), 0, &mut outbox);
        assert_eq!(fleet.pods[0].bucket.remaining(0), 7, "the three since");
        assert_eq!(fleet.known_by(0), 53);
    }

    /// What a restarted pod forgot of its own earlier life is still in the fleet's count:
    /// the others heard it.
    #[test]
    fn the_fleet_counts_what_a_restarted_pod_forgot() {
        let mut fleet = fleet(2, Excess::Drain);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, 0, 4, 0), 4);
        fleet.on_message(1, counts(0, 0, 4), 0, &mut outbox);
        fleet.on_fault(FaultEvent::Restart { pod: 0 }, 0, &mut outbox);
        assert_eq!(fleet.known_by(0), 0);
        assert_eq!((fleet.unapplied(), fleet.counted_twice()), (0, 0));
    }

    /// Two pods each spent the whole bucket while cut off: when they hear of each other the
    /// other's ten are owed, as far as the cap allows, or forgiven with drain.
    #[test]
    fn healing_merges_the_counts_and_owes_up_to_the_cap() {
        for (excess, retry_ms) in [(Excess::Drain, 10), (Excess::DebtUpTo(50 * MS), 60)] {
            let mut fleet = fleet(2, excess);
            let mut outbox = Outbox::default();
            assert_eq!(
                admits(&mut fleet, 0, 10, 0) + admits(&mut fleet, 1, 10, 0),
                20
            );
            fleet.on_message(0, counts(1, 0, 10), 0, &mut outbox);
            assert_eq!(
                fleet.pods[0].bucket.retry_at(0),
                Some(retry_ms * MS),
                "{excess:?}"
            );
        }
    }
}
