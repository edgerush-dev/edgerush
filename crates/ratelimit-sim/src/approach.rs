//! Ways of limiting one key across several pods.

use crate::counters::Counters;
use crate::ownership::Ownership;
use crate::service::Service;
use crate::{Contract, FaultEvent, Message, Outbox};
use edgerush_ratelimit::reference::{Model, TokenBucket};
use std::num::NonZeroU64;

/// The pods of a fleet limiting one key, however they share what they know. A pod never
/// waits on another: it decides at once, and what it learns from its peers comes later, in
/// messages.
pub trait Cluster {
    /// Decides a request arriving at `pod` at `now`: `true` admits it.
    fn decide(&mut self, pod: usize, now: u64, outbox: &mut Outbox) -> bool;

    /// Request `request` arrives at `pod` at `now`: decided at once, or `None` if it waits
    /// — its decision then comes later, from [`take_decided`](Self::take_decided).
    fn arrive(
        &mut self,
        request: usize,
        pod: usize,
        now: u64,
        outbox: &mut Outbox,
    ) -> Option<bool> {
        let _ = request;
        Some(self.decide(pod, now, outbox))
    }

    /// Decisions made since last asked for requests that waited, as `(request, admitted)`.
    fn take_decided(&mut self) -> Vec<(usize, bool)> {
        Vec::new()
    }

    /// How often each pod's timer fires, if the pods have work to do in the background.
    fn cycle(&self) -> Option<u64> {
        None
    }

    /// Nodes beyond the `pods` that take traffic which have timers too: a service's own pods.
    fn helpers(&self) -> usize {
        0
    }

    /// `pod`'s timer fired at `now`.
    fn on_timer(&mut self, _pod: usize, _now: u64, _outbox: &mut Outbox) {}

    /// `message` reached `pod` at `now`.
    fn on_message(&mut self, _pod: usize, _message: Message, _now: u64, _outbox: &mut Outbox) {}

    /// Hits admitted that never reached the count that decides for the whole fleet — lost
    /// with a message, or never sent — as the run ends: usage no bucket saw (10 §2,
    /// property 3), apart from any excess a bucket saw and forgave.
    fn unapplied(&self) -> u64 {
        0
    }

    /// Hits the count deciding for the fleet holds more than once, as the run ends.
    fn counted_twice(&self) -> u64 {
        0
    }

    /// Part of the run's fault happens at `now`.
    fn on_fault(&mut self, _event: FaultEvent, _now: u64, _outbox: &mut Outbox) {}
}

/// How the pods limit a key between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Approach {
    /// One bucket for the whole fleet, as if every pod saw every other's admissions at
    /// once: the contract kept exactly, which sharing aims at and never quite reaches.
    Central,
    /// Every pod a bucket of its own holding the whole contract, sharing nothing: what each
    /// pod does with its peers gone (06 §6), and the baseline any sharing has to beat.
    Independent,
    /// Every pod a share of the contract, the rate and the burst divided by the pods (the
    /// burst rounded up): never far over the contract, at the price of refusing traffic
    /// that lands unevenly (06 §4).
    Split,
    /// Gubernator's `GLOBAL` behaviour (06 §4): the first pod owns the key; every other pod
    /// answers from a copy of the owner's bucket and, every cycle, sends the owner the hits
    /// it admitted; the owner applies them, and every cycle sends its bucket to every pod,
    /// which takes it over in place of its copy — with the changes 06 §4 proposes as
    /// settings.
    Ownership(Sharing),
    /// Per-pod counters (06 §5): no owner; every pod sends the others the total it has
    /// admitted every cycle, and charges its own view with what it learns of theirs.
    Counters(Counting),
    /// A rate-limiting service of its own, a candidate not yet chosen: a few service pods
    /// own the keys between them, and the data planes, which never talk to each other,
    /// decide from copies of the owner's bucket; every cycle each sends the owner its hits,
    /// or asks for a copy, and is answered with the bucket as it stands.
    Service(Serving),
}

/// How data planes share a key through a rate-limiting service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Serving {
    /// The time between a data plane's messages to the service.
    pub cycle: u64,
    /// What the owner does with hits that do not fit.
    pub excess: Excess,
    /// How much of what it sees a data plane may admit in one cycle.
    pub bound: Bound,
    /// Whether requests for a key a data plane holds no fresh copy of wait while it asks
    /// the owner, which decides them against the one bucket (06 §4), rather than being
    /// decided from a full bucket.
    pub ask_when_cold: bool,
    /// Whether a request that the copy has room for but the per-cycle bound would refuse
    /// waits while the owner decides it, rather than being refused: a fleet with more data
    /// planes than a cycle refills cannot share it out in whole requests.
    pub ask_when_short: bool,
    /// How the owning service pod keeps a secondary's copy of what it holds, so that the
    /// secondary can take over from it.
    pub replicate: Replicate,
    /// How far, in nanoseconds, the data planes' clocks are off the service's, spread
    /// evenly from this much behind to this much ahead: a copy of the owner's bucket is
    /// read with the data plane's own clock.
    pub skew: u64,
}

/// How the service pod owning a key keeps a copy of its state on a secondary, the next
/// service pod, which owns the key should the owner go or restart; a new secondary is then
/// the next after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Replicate {
    /// No copy: the next service pod starts from a full bucket, and an owner that restarts
    /// owns the key still, from nothing.
    None,
    /// Every cycle, if anything changed: a takeover loses up to a cycle.
    EveryCycle,
    /// With every change, before the next is made: a takeover loses what was on its way.
    EveryChange,
}

/// How pods share a key by per-pod counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counting {
    /// The time between a pod's sending its total.
    pub cycle: u64,
    /// What a pod does with others' hits that do not fit in its view.
    pub excess: Excess,
    /// How much of what it sees a pod may admit in one cycle.
    pub bound: Bound,
}

/// How pods share a key by ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sharing {
    /// The time between a pod's reports, and between the owner's broadcasts.
    pub cycle: u64,
    /// What the owner does with hits that do not fit.
    pub excess: Excess,
    /// Whether pods keep their own hits apart with the hit ledger (06 §4) — a pod's view
    /// the owner's bucket plus its hits the owner has not yet applied — rather than taking
    /// the broadcast over as it comes. With it, a batch not seen applied within four cycles
    /// is sent again.
    pub ledger: bool,
    /// How much of what it sees a pod may admit in one cycle.
    pub bound: Bound,
    /// What pods do, when the owner changes, with batches it had not yet been seen to apply.
    pub on_owner_change: OnOwnerChange,
}

/// What a pod does, when the owner changes, with batches the old owner was not yet seen to
/// apply (06 §4: what the ledger cannot settle).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnOwnerChange {
    /// Forgets them: never counted twice, but lost if the old owner never applied them.
    Drop,
    /// Sends them to the new owner: never lost, but counted twice if the new owner's copy
    /// already held them.
    SendAgain,
}

/// How much of what it sees a pod may admit in one cycle, against bursts that land on every
/// pod in the same instant, before any of them can hear of the others (06 §4). A pod's
/// share is what its view has left plus what the cycle refills, divided by the pods the
/// owner has seen admit the key lately — this one counted — and never less than one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    /// No bound: whatever the pod's view admits.
    Unbounded,
    /// 06 §4 as written: a pod the owner has not seen admit the key lately is held to its
    /// share; one that has been admitting it may spend all it sees.
    NewPods,
    /// Every pod held to its share.
    Every,
    /// Every pod held to its share, plus one request more if it left one unspent in the
    /// cycle before: traffic spread at random gives a pod two requests in one cycle and
    /// none in the next more often than a share of one allows. Carrying a whole share
    /// would double the first cycle of a burst.
    EveryCarrying,
    /// With a rate-limiting service: every data plane held to what the owner grants it in
    /// each answer — a share of what the owner's bucket has, weighted by the data plane's
    /// part in the hits of the last few cycles; one that has not been admitting gets an
    /// equal share, as if joining. Elsewhere, as [`Bound::Every`].
    Granted,
    /// With a rate-limiting service: every data plane earns its share each cycle as a
    /// fraction, with no floor of one request, and admits as many whole requests as it has
    /// earned, banking at most one ahead: a fleet larger than a cycle's refill is held to
    /// it, not to a request a pod. Elsewhere, as [`Bound::Every`].
    Credit,
}

impl Bound {
    /// The allowance for a cycle: `share`, and with [`Bound::EveryCarrying`] one request
    /// more if `before` was not all `spent`.
    #[must_use]
    pub fn carry(self, share: Option<u64>, before: Option<u64>, spent: u64) -> Option<u64> {
        match (self, share) {
            (Self::EveryCarrying, Some(share)) => {
                let left = before.map_or(0, |before| before.saturating_sub(spent));
                Some(share + left.min(1))
            }
            _ => share,
        }
    }
}

/// What the owner of a key does with reported hits that do not fit in its bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Excess {
    /// Forgives them: the bucket is left empty, and refills from there (06 §4, Gubernator).
    Drain,
    /// Charges them all: the bucket owes them, and admits nothing until they are paid back.
    Debt,
    /// Charges them while what is owed would be paid back within this many nanoseconds,
    /// and forgives the rest.
    DebtUpTo(u64),
}

impl Approach {
    /// The pods of a fleet of `pods` limiting a key to `contract` this way.
    #[must_use]
    pub fn build(&self, contract: &Contract, pods: usize) -> Box<dyn Cluster> {
        let pods = pods.max(1);
        let bucket = || TokenBucket::new(contract.limit, contract.window, contract.burst);
        match self {
            Self::Central => Box::new(Pods(vec![bucket()])),
            Self::Independent => Box::new(Pods((0..pods).map(|_| bucket()).collect())),
            Self::Split => {
                let count = pods as u64;
                // `limit / pods` a window is `limit` every `pods` windows, kept exact.
                let window = NonZeroU64::new(contract.window.get().saturating_mul(count))
                    .unwrap_or(contract.window);
                let burst = NonZeroU64::new(contract.burst.get().div_ceil(count))
                    .unwrap_or(NonZeroU64::MIN);
                let share = || TokenBucket::new(contract.limit, window, burst);
                Box::new(Pods((0..pods).map(|_| share()).collect()))
            }
            Self::Ownership(sharing) => Box::new(Ownership::new(contract, pods, *sharing)),
            Self::Counters(counting) => Box::new(Counters::new(contract, pods, *counting)),
            Self::Service(serving) => Box::new(Service::new(contract, pods, *serving)),
        }
    }
}

/// A bucket per pod, or one for all of them.
struct Pods(Vec<TokenBucket>);

impl Cluster for Pods {
    fn decide(&mut self, pod: usize, now: u64, _: &mut Outbox) -> bool {
        let count = self.0.len();
        self.0[pod % count].decide(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn contract(limit: u64, burst: u64) -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(limit),
            window: n(SECOND),
            burst: n(burst),
        }
    }

    fn at_once(cluster: &mut dyn Cluster, pod: usize, count: u64) -> u64 {
        (0..count)
            .filter(|_| cluster.decide(pod, 0, &mut Outbox::default()))
            .count() as u64
    }

    /// Carrying, a pod's allowance is its share plus what it left of the last, at most one
    /// request more; without, its share alone.
    #[test]
    fn a_carrying_bound_adds_at_most_one_request_left_unspent() {
        let carry = Bound::EveryCarrying;
        assert_eq!(carry.carry(Some(1), Some(1), 0), Some(2));
        assert_eq!(carry.carry(Some(1), Some(1), 1), Some(1), "nothing left");
        assert_eq!(carry.carry(Some(12), Some(12), 0), Some(13), "at most one");
        assert_eq!(carry.carry(Some(3), None, 0), Some(3), "nothing before");
        assert_eq!(carry.carry(None, Some(5), 0), None, "unbounded");
        assert_eq!(Bound::Every.carry(Some(1), Some(1), 0), Some(1));
    }

    #[test]
    fn central_holds_the_whole_fleet_to_one_bucket() {
        let mut cluster = Approach::Central.build(&contract(100, 20), 4);
        assert_eq!(
            at_once(&mut *cluster, 0, 15) + at_once(&mut *cluster, 3, 15),
            20
        );
    }

    #[test]
    fn independent_pods_each_spend_the_whole_bucket() {
        let mut cluster = Approach::Independent.build(&contract(100, 20), 4);
        let total: u64 = (0..4).map(|pod| at_once(&mut *cluster, pod, 30)).sum();
        assert_eq!(total, 80);
    }

    #[test]
    fn split_pods_each_spend_a_share_rounded_up() {
        let mut cluster = Approach::Split.build(&contract(100, 22), 4);
        let each: Vec<u64> = (0..4).map(|pod| at_once(&mut *cluster, pod, 30)).collect();
        assert_eq!(each, vec![6, 6, 6, 6]);
        // A share of the rate too: a quarter of 100 a second is one every 40 ms.
        assert!(!cluster.decide(0, 39_999_999, &mut Outbox::default()));
        assert!(cluster.decide(0, 40_000_000, &mut Outbox::default()));
    }
}
