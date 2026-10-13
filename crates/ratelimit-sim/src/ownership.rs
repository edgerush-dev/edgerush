//! Ownership as Gubernator shares counts (06 §4): one owner applies every pod's hits to the
//! bucket that counts, and broadcasts it.

use crate::{Cluster, Contract, Excess, Message, Outbox};
use edgerush_ratelimit::reference::{Model, TokenBucket};

/// The pod that owns the key. With one key there is one owner; which pod it is changes
/// nothing, so it is the first.
const OWNER: usize = 0;

/// A fleet sharing one key by ownership.
pub(crate) struct Ownership {
    cycle: u64,
    excess: Excess,
    pods: Vec<Pod>,
}

/// What one pod holds of the key.
struct Pod {
    /// The owner's own bucket on the owner; elsewhere a copy, answered from at once.
    bucket: TokenBucket,
    /// Hits admitted here and not yet sent to the owner.
    unsent: u64,
    /// Batches sent so far.
    sent: u64,
    /// On the owner: whether the bucket changed since the last broadcast.
    changed: bool,
}

impl Ownership {
    pub(crate) fn new(contract: &Contract, pods: usize, cycle: u64, excess: Excess) -> Self {
        let pod = || Pod {
            bucket: TokenBucket::new(contract.limit, contract.window, contract.burst),
            unsent: 0,
            sent: 0,
            changed: false,
        };
        Self {
            cycle: cycle.max(1),
            excess,
            pods: (0..pods.max(1)).map(|_| pod()).collect(),
        }
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
                at.unsent = 0;
            }
        } else if at.changed {
            at.changed = false;
            for to in (0..pods).filter(|to| *to != OWNER) {
                let state = Message::State {
                    bucket: at.bucket.clone(),
                    applied: Vec::new(),
                };
                outbox.send(to, state);
            }
        }
    }

    fn on_message(&mut self, pod: usize, message: Message, now: u64, _: &mut Outbox) {
        let at = &mut self.pods[pod];
        match message {
            Message::Hits { count, .. } if pod == OWNER => {
                for _ in 0..count {
                    let room = at.bucket.admits(now);
                    let owe = match self.excess {
                        Excess::Drain => false,
                        Excess::Debt => true,
                        Excess::DebtUpTo(most) => at
                            .bucket
                            .retry_at(now)
                            .is_none_or(|retry| retry - now <= most),
                    };
                    if room || owe {
                        at.bucket.charge(now);
                    }
                }
                at.changed = true;
            }
            // Taken over in place of the copy, as Gubernator does: what this pod admitted
            // since its last report is no longer counted in its view.
            Message::State { bucket, .. } if pod != OWNER => at.bucket = bucket,
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
        Ownership::new(&contract, pods, 10 * MS, Excess::Drain)
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
        Ownership::new(&contract, 3, 10 * MS, excess)
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
}
