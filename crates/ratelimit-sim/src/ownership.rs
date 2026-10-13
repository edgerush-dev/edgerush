//! Ownership as Gubernator shares counts (06 §4): one owner applies every pod's hits to the
//! bucket that counts, and broadcasts it — and, as a setting, the hit ledger 06 §4 proposes
//! in place of Gubernator's overwrite.

use crate::{Cluster, Contract, Excess, Message, Outbox};
use edgerush_ratelimit::reference::{Model, TokenBucket};
use std::collections::{BTreeMap, VecDeque};

/// The pod that owns the key. With one key there is one owner; which pod it is changes
/// nothing, so it is the first.
const OWNER: usize = 0;

/// A fleet sharing one key by ownership.
pub(crate) struct Ownership {
    cycle: u64,
    excess: Excess,
    ledger: bool,
    pods: Vec<Pod>,
    /// On the owner, for each pod: the highest of its batches applied, every one before it
    /// applied too.
    applied: Vec<u64>,
    /// On the owner, for each pod: batches that came ahead of one still missing, held until
    /// it comes, so that a pod's batches are applied in order.
    waiting: Vec<BTreeMap<u64, u64>>,
}

/// What one pod holds of the key.
struct Pod {
    /// The owner's own bucket on the owner; elsewhere the pod's view, answered from at once.
    bucket: TokenBucket,
    /// Hits admitted here and not yet sent to the owner.
    unsent: u64,
    /// Batches sent so far.
    sent: u64,
    /// With the ledger: batches sent and not yet seen applied in a broadcast, as
    /// `(sequence, count)`.
    unacknowledged: VecDeque<(u64, u64)>,
    /// On the owner: whether the bucket changed since the last broadcast.
    changed: bool,
}

impl Ownership {
    pub(crate) fn new(
        contract: &Contract,
        pods: usize,
        cycle: u64,
        excess: Excess,
        ledger: bool,
    ) -> Self {
        let pods = pods.max(1);
        let pod = || Pod {
            bucket: TokenBucket::new(contract.limit, contract.window, contract.burst),
            unsent: 0,
            sent: 0,
            unacknowledged: VecDeque::new(),
            changed: false,
        };
        Self {
            cycle: cycle.max(1),
            excess,
            ledger,
            pods: (0..pods).map(|_| pod()).collect(),
            applied: vec![0; pods],
            waiting: vec![BTreeMap::new(); pods],
        }
    }

    /// The owner applies `count` reported hits, each charged if there is room, and beyond
    /// that as its handling of excess says.
    fn apply(&mut self, count: u64, now: u64) {
        let bucket = &mut self.pods[OWNER].bucket;
        for _ in 0..count {
            let room = bucket.admits(now);
            let owe = match self.excess {
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
        self.pods[OWNER].changed = true;
    }
}

impl Cluster for Ownership {
    fn decide(&mut self, pod: usize, now: u64, _: &mut Outbox) -> bool {
        let at = &mut self.pods[pod];
        let admitted = at.bucket.decide(now);
        if admitted {
            if pod == OWNER {
                at.changed = true;
            } else {
                at.unsent += 1;
            }
        }
        admitted
    }

    fn cycle(&self) -> Option<u64> {
        Some(self.cycle)
    }

    fn on_timer(&mut self, pod: usize, _: u64, outbox: &mut Outbox) {
        let pods = self.pods.len();
        let at = &mut self.pods[pod];
        if pod != OWNER {
            if at.unsent > 0 {
                at.sent += 1;
                let hits = Message::Hits {
                    from: pod,
                    sequence: at.sent,
                    count: at.unsent,
                };
                outbox.send(OWNER, hits);
                if self.ledger {
                    at.unacknowledged.push_back((at.sent, at.unsent));
                }
                at.unsent = 0;
            }
        } else if at.changed {
            at.changed = false;
            for to in (0..pods).filter(|to| *to != OWNER) {
                let state = Message::State {
                    bucket: at.bucket.clone(),
                    applied: self.applied.clone(),
                };
                outbox.send(to, state);
            }
        }
    }

    fn on_message(&mut self, pod: usize, message: Message, now: u64, _: &mut Outbox) {
        match message {
            Message::Hits {
                from,
                sequence,
                count,
            } if pod == OWNER => {
                if !self.ledger {
                    self.apply(count, now);
                    return;
                }
                // In order, once each: a batch already applied is ignored, and one that came
                // ahead of a missing one waits for it.
                if sequence > self.applied[from] {
                    self.waiting[from].insert(sequence, count);
                }
                while let Some(count) = self.waiting[from].remove(&(self.applied[from] + 1)) {
                    self.applied[from] += 1;
                    self.apply(count, now);
                }
            }
            Message::State { bucket, applied } if pod != OWNER => {
                let at = &mut self.pods[pod];
                at.bucket = bucket;
                if self.ledger {
                    // The owner's bucket, plus what this pod admitted that it has not seen:
                    // batches the broadcast's applied mark does not cover, and hits unsent.
                    let seen = applied.get(pod).copied().unwrap_or(0);
                    at.unacknowledged.retain(|(sequence, _)| *sequence > seen);
                    let unseen: u64 = at.unacknowledged.iter().map(|(_, count)| count).sum();
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;

    fn fleet(pods: usize) -> Ownership {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        Ownership::new(&contract, pods, 10 * MS, Excess::Drain, false)
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
        Ownership::new(&contract, 3, 10 * MS, excess, false)
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
        Ownership::new(&contract, pods, 10 * MS, Excess::Drain, true)
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
        assert_eq!(fleet.applied[1], 0, "the second batch waits");
        assert_eq!(fleet.pods[OWNER].bucket.remaining(0), 10);
        fleet.on_message(first.0, first.1, 0, &mut outbox);
        assert_eq!(fleet.applied[1], 2, "then both are applied, in order");
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
        assert_eq!(fleet.applied[1], 1);
    }
}
