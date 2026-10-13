//! A rate-limiting service of its own, a candidate not yet chosen: a few service pods own
//! the keys between them, Gubernator's way, and the data planes are replicas that never
//! talk to each other. A data plane decides every request from its copy of the key — the
//! owner's bucket as last heard, plus its own hits not yet seen applied — and never waits.
//! Every cycle it sends the owner the hits it admitted or, with none to send while the key
//! is in use, asks for a copy; the owner applies hits in order, once each, and answers
//! every message with its bucket as it stands.

use crate::{Bound, Cluster, Contract, Excess, FaultEvent, Message, Outbox, Serving};
use edgerush_ratelimit::reference::{Model, TokenBucket};
use std::collections::{BTreeMap, VecDeque};

/// The service's pods. Keys are spread over them by hashing; with one key, one owns it —
/// the first — and the next takes it over if it goes.
const SERVICE_PODS: usize = 3;

/// How many cycles a data plane waits to see a batch applied before sending it again.
const RESEND_AFTER_CYCLES: u64 = 4;

/// For how many cycles before the latest admission the owner knows of a data plane counts
/// as admitting the key.
const ACTIVE_CYCLES: u64 = 4;

/// A fleet of data planes sharing one key through the service.
pub(crate) struct Service {
    serving: Serving,
    /// A new bucket, full: what a pod starts from, and comes back with from a restart.
    fresh: TokenBucket,
    /// What a cycle refills, in requests, rounded down.
    refill: u64,
    /// One bucket's refill time: how long a data plane goes on asking for copies after its
    /// last request, and keeps sending a batch it has not seen applied. A hit older than
    /// that no longer weighs on a bucket whose debt is capped at it.
    hold: u64,
    planes: Vec<Plane>,
    /// Which data planes are still there.
    alive: Vec<bool>,
    owners: Vec<Owner>,
    /// Which service pods are still there.
    serving_pods: Vec<bool>,
    /// The service pod that owns the key, as every data plane knows it.
    owner: usize,
    /// Hits admitted anywhere.
    admitted: u64,
}

/// What one data plane holds of the key.
struct Plane {
    /// Which life of the data plane this is: a restart begins another.
    instance: u64,
    /// Its copy: the owner's bucket as last heard, charged with its own hits not yet seen
    /// applied, and with what it has admitted since.
    bucket: TokenBucket,
    /// Hits admitted and not yet sent.
    unsent: u64,
    /// Batches sent to the present owner so far.
    sent: u64,
    /// Batches sent and not yet seen applied, as `(sequence, count, first sent, last
    /// sent)`.
    unacknowledged: VecDeque<(u64, u64, u64, u64)>,
    /// Admitted in the cycle under way.
    spent: u64,
    /// The most it may admit in the cycle under way, if bounded.
    allowance: Option<u64>,
    /// Which data planes the owner last said were admitting the key.
    active: Vec<bool>,
    /// When a request for the key last came.
    last_request: Option<u64>,
}

/// What a service pod keeps of the key while it owns it.
struct Owner {
    /// The bucket that counts.
    bucket: TokenBucket,
    /// For each data plane: when it was last seen to admit the key.
    last_admitted: Vec<Option<u64>>,
    /// For each data plane: the highest of its batches applied, every one before it too.
    applied: Vec<u64>,
    /// For each data plane: batches that came ahead of one still missing.
    waiting: Vec<BTreeMap<u64, u64>>,
    /// Hits the bucket holds, each once.
    counted: u64,
    /// For each data plane: the latest of its lives heard from.
    instances: Vec<u64>,
}

impl Owner {
    fn new(fresh: &TokenBucket, planes: usize) -> Self {
        Self {
            bucket: fresh.clone(),
            last_admitted: vec![None; planes],
            applied: vec![0; planes],
            waiting: vec![BTreeMap::new(); planes],
            counted: 0,
            instances: vec![0; planes],
        }
    }

    /// Which data planes it has seen admit the key within a few cycles of the latest
    /// admission it knows of: counted back from the last traffic, not from now, so that
    /// data planes that burst together and then fall quiet are bounded together when they
    /// burst again (06 §4).
    fn active(&self, lately: u64) -> Vec<bool> {
        let last = self.last_admitted.iter().flatten().max().copied();
        self.last_admitted
            .iter()
            .map(|at| matches!((at, last), (Some(at), Some(last)) if last - at <= lately))
            .collect()
    }
}

impl Service {
    pub(crate) fn new(contract: &Contract, planes: usize, serving: Serving) -> Self {
        let planes = planes.max(1);
        let cycle = serving.cycle.max(1);
        let fresh = TokenBucket::new(contract.limit, contract.window, contract.burst);
        let plane = || Plane {
            instance: 0,
            bucket: fresh.clone(),
            unsent: 0,
            sent: 0,
            unacknowledged: VecDeque::new(),
            spent: 0,
            allowance: None,
            active: vec![false; planes],
            last_request: None,
        };
        let per = |span: u64| {
            u128::from(contract.limit.get()) * u128::from(span) / u128::from(contract.window.get())
        };
        let hold = u128::from(contract.burst.get()) * u128::from(contract.window.get())
            / u128::from(contract.limit.get());
        Self {
            serving: Serving { cycle, ..serving },
            refill: u64::try_from(per(cycle)).unwrap_or(u64::MAX),
            hold: u64::try_from(hold).unwrap_or(u64::MAX),
            planes: (0..planes).map(|_| plane()).collect(),
            alive: vec![true; planes],
            owners: (0..SERVICE_PODS)
                .map(|_| Owner::new(&fresh, planes))
                .collect(),
            serving_pods: vec![true; SERVICE_PODS],
            owner: 0,
            fresh,
            admitted: 0,
        }
    }

    /// The node a service pod is reached at: after the data planes.
    fn address(&self, service_pod: usize) -> usize {
        self.planes.len() + service_pod
    }

    /// The data plane a request for `plane` is decided on: itself, or the next still there.
    fn serving_plane(&self, plane: usize) -> usize {
        let planes = self.planes.len();
        (0..planes)
            .map(|step| (plane + step) % planes)
            .find(|candidate| self.alive[*candidate])
            .unwrap_or(plane)
    }

    /// What `plane` may admit in the cycle starting at `now`.
    fn allowance(&self, plane: usize, now: u64) -> Option<u64> {
        let at = &self.planes[plane];
        let share = |planes: usize| {
            let sees = at.bucket.remaining(now).saturating_add(self.refill);
            Some((sees / planes.max(1) as u64).max(1))
        };
        match self.serving.bound {
            Bound::Unbounded => None,
            Bound::NewPods if at.active[plane] => None,
            Bound::NewPods => share(self.planes.len()),
            Bound::Every => {
                let others = (0..at.active.len())
                    .filter(|p| *p != plane && at.active[*p])
                    .count();
                share(others + 1)
            }
        }
    }

    /// Service pod `pod` applies `count` reported hits, each charged if there is room, and
    /// beyond that as its handling of excess says.
    fn apply(&mut self, pod: usize, count: u64, now: u64) {
        let owner = &mut self.owners[pod];
        owner.counted += count;
        for _ in 0..count {
            let room = owner.bucket.admits(now);
            let owe = match self.serving.excess {
                Excess::Drain => false,
                Excess::Debt => true,
                Excess::DebtUpTo(most) => owner
                    .bucket
                    .retry_at(now)
                    .is_none_or(|retry| retry - now <= most),
            };
            if room || owe {
                owner.bucket.charge(now);
            }
        }
    }

    /// Service pod `pod` answers `plane` with its bucket as it stands.
    fn answer(&self, pod: usize, plane: usize, outbox: &mut Outbox) {
        let owner = &self.owners[pod];
        let state = Message::State {
            from: self.address(pod),
            bucket: owner.bucket.clone(),
            applied: owner.applied.clone(),
            active: owner.active(ACTIVE_CYCLES * self.serving.cycle),
            counted: owner.counted,
        };
        outbox.send(plane, state);
    }

    /// Every data plane takes a new owner, or one that has restarted: it numbers its
    /// batches afresh and sends again those it has not seen applied.
    fn follow(&mut self, now: u64, outbox: &mut Outbox) {
        let to = self.address(self.owner);
        for (plane, at) in self.planes.iter_mut().enumerate() {
            if !self.alive[plane] {
                continue;
            }
            at.sent = 0;
            for (sequence, count, _, last) in &mut at.unacknowledged {
                at.sent += 1;
                *sequence = at.sent;
                *last = now;
                let hits = Message::Hits {
                    from: plane,
                    instance: at.instance,
                    sequence: at.sent,
                    count: *count,
                };
                outbox.send(to, hits);
            }
        }
    }

    /// The owner's handling of a message from data plane `from`'s life `instance`: whether
    /// it is from a life that has ended, and so ignored. The first from a new life starts
    /// its numbering again (10 §2, identity safety).
    fn current_life(&mut self, pod: usize, from: usize, instance: u64) -> bool {
        let owner = &mut self.owners[pod];
        if instance < owner.instances[from] {
            return false;
        }
        if instance > owner.instances[from] {
            owner.instances[from] = instance;
            owner.applied[from] = 0;
            owner.waiting[from].clear();
        }
        true
    }

    /// Service pod `pod` hears from data plane `from`. Every data plane learns of a new
    /// owner at once, so only one that has gone hears from them late.
    fn at_service(&mut self, pod: usize, message: Message, now: u64, outbox: &mut Outbox) {
        if !self.serving_pods[pod] {
            return;
        }
        match message {
            Message::Hits {
                from,
                instance,
                sequence,
                count,
            } if from < self.planes.len() => {
                if !self.current_life(pod, from, instance) {
                    return;
                }
                let owner = &mut self.owners[pod];
                owner.last_admitted[from] = Some(now);
                // In order, once each: a batch already applied is ignored, and one that came
                // ahead of a missing one waits for it. Either way the answer goes back, so
                // that a data plane whose earlier answer was lost learns what is applied.
                if sequence > owner.applied[from] {
                    owner.waiting[from].insert(sequence, count);
                }
                loop {
                    let next = self.owners[pod].applied[from] + 1;
                    let Some(count) = self.owners[pod].waiting[from].remove(&next) else {
                        break;
                    };
                    self.owners[pod].applied[from] = next;
                    self.apply(pod, count, now);
                }
                self.answer(pod, from, outbox);
            }
            Message::Poll { from, instance }
                if from < self.planes.len() && self.current_life(pod, from, instance) =>
            {
                self.answer(pod, from, outbox);
            }
            _ => {}
        }
    }

    /// Data plane `plane` hears from the service.
    fn at_plane(&mut self, plane: usize, message: Message, now: u64) {
        let owner = self.address(self.owner);
        let Message::State {
            from,
            bucket,
            applied,
            active,
            ..
        } = message
        else {
            return;
        };
        // Only the owner every data plane knows of: an answer from one that has gone is
        // stale.
        if from != owner {
            return;
        }
        let at = &mut self.planes[plane];
        at.bucket = bucket;
        at.active = active;
        // The owner's bucket, plus what this data plane admitted that it has not seen:
        // batches the answer's applied mark does not cover, and hits unsent.
        let seen = applied.get(plane).copied().unwrap_or(0);
        at.unacknowledged
            .retain(|(sequence, _, _, _)| *sequence > seen);
        let unseen: u64 = at.unacknowledged.iter().map(|(_, count, _, _)| count).sum();
        for _ in 0..unseen + at.unsent {
            at.bucket.charge(now);
        }
    }
}

impl Cluster for Service {
    fn decide(&mut self, plane: usize, now: u64, _: &mut Outbox) -> bool {
        let plane = self.serving_plane(plane);
        let at = &mut self.planes[plane];
        at.last_request = Some(now);
        if at.allowance.is_some_and(|allowance| at.spent >= allowance) {
            return false;
        }
        let admitted = at.bucket.decide(now);
        if admitted {
            at.spent += 1;
            at.unsent += 1;
            self.admitted += 1;
        }
        admitted
    }

    fn cycle(&self) -> Option<u64> {
        Some(self.serving.cycle)
    }

    /// Only data planes have timers: the service only answers.
    fn on_timer(&mut self, plane: usize, now: u64, outbox: &mut Outbox) {
        if plane >= self.planes.len() || !self.alive[plane] {
            return;
        }
        let allowance = self.allowance(plane, now);
        let (to, hold) = (self.address(self.owner), self.hold);
        let resend_after = RESEND_AFTER_CYCLES * self.serving.cycle;
        let at = &mut self.planes[plane];
        at.spent = 0;
        at.allowance = allowance;
        // A batch not seen applied within a bucket's refill time is emptied: its hits no
        // longer weigh on the bucket, and applied now they would only refuse traffic that
        // came since. It goes on being sent, empty, as the owner applies batches in order
        // and would otherwise hold back every one after it.
        for (_, count, first, _) in &mut at.unacknowledged {
            if now - *first > hold {
                *count = 0;
            }
        }
        let mut asked = false;
        for (sequence, count, _, last) in &mut at.unacknowledged {
            if now - *last >= resend_after {
                *last = now;
                let again = Message::Hits {
                    from: plane,
                    instance: at.instance,
                    sequence: *sequence,
                    count: *count,
                };
                outbox.send(to, again);
                asked = true;
            }
        }
        if at.unsent > 0 {
            at.sent += 1;
            let hits = Message::Hits {
                from: plane,
                instance: at.instance,
                sequence: at.sent,
                count: at.unsent,
            };
            outbox.send(to, hits);
            at.unacknowledged.push_back((at.sent, at.unsent, now, now));
            at.unsent = 0;
            asked = true;
        }
        let in_use =
            at.last_request.is_some_and(|last| now - last <= hold) || !at.unacknowledged.is_empty();
        if !asked && in_use {
            let poll = Message::Poll {
                from: plane,
                instance: at.instance,
            };
            outbox.send(to, poll);
        }
    }

    fn on_message(&mut self, to: usize, message: Message, now: u64, outbox: &mut Outbox) {
        let planes = self.planes.len();
        if to >= planes {
            self.at_service(to - planes, message, now, outbox);
        } else if self.alive[to] {
            self.at_plane(to, message, now);
        }
    }

    fn on_fault(&mut self, event: FaultEvent, now: u64, outbox: &mut Outbox) {
        match event {
            // The service pod owning the key goes for good: the key's hash leads to the next,
            // which starts from a full bucket.
            FaultEvent::OwnerLost => {
                self.serving_pods[self.owner] = false;
                if let Some(next) = (0..SERVICE_PODS).find(|pod| self.serving_pods[*pod]) {
                    self.owner = next;
                    self.follow(now, outbox);
                }
            }
            // It restarts, holding nothing.
            FaultEvent::OwnerRestarts => {
                self.owners[self.owner] = Owner::new(&self.fresh, self.planes.len());
                self.follow(now, outbox);
            }
            // Nothing to elect: data planes cut off from the service go on from their copies,
            // and their messages are lost until it heals.
            FaultEvent::PartitionStarts { .. } | FaultEvent::PartitionHeals { .. } => {}
            FaultEvent::Restart { pod } if pod < self.planes.len() && self.alive[pod] => {
                let planes = self.planes.len();
                let at = &mut self.planes[pod];
                at.instance += 1;
                at.bucket = self.fresh.clone();
                at.unsent = 0;
                at.sent = 0;
                at.unacknowledged.clear();
                at.spent = 0;
                at.allowance = None;
                at.active = vec![false; planes];
                at.last_request = None;
            }
            FaultEvent::Restart { .. } => {}
        }
    }

    fn unapplied(&self) -> u64 {
        self.admitted
            .saturating_sub(self.owners[self.owner].counted)
    }

    fn counted_twice(&self) -> u64 {
        self.owners[self.owner]
            .counted
            .saturating_sub(self.admitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;

    fn fleet(planes: usize, bound: Bound) -> Service {
        let n = |n| NonZeroU64::new(n).unwrap();
        let contract = Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        };
        let serving = Serving {
            cycle: 10 * MS,
            excess: Excess::DebtUpTo(100 * MS),
            bound,
        };
        Service::new(&contract, planes, serving)
    }

    fn admits(fleet: &mut Service, plane: usize, count: u64, now: u64) -> u64 {
        let mut outbox = Outbox::default();
        (0..count)
            .filter(|_| fleet.decide(plane, now, &mut outbox))
            .count() as u64
    }

    /// Delivers what `outbox` holds, as the network would with no delay, and what that
    /// sends in turn.
    fn deliver(fleet: &mut Service, outbox: &mut Outbox, now: u64) {
        while !outbox.sent.is_empty() {
            for (to, message) in std::mem::take(&mut outbox.sent) {
                fleet.on_message(to, message, now, outbox);
            }
        }
    }

    #[test]
    fn hits_go_to_the_service_and_a_copy_comes_back() {
        let mut fleet = fleet(3, Bound::Unbounded);
        let mut outbox = Outbox::default();
        assert_eq!(admits(&mut fleet, 1, 4, 0), 4);
        fleet.on_timer(1, 0, &mut outbox);
        let to: Vec<usize> = outbox.sent.iter().map(|(to, _)| *to).collect();
        assert_eq!(to, vec![fleet.address(0)], "to the owning service pod only");
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.owners[0].bucket.remaining(0), 6);
        assert_eq!(fleet.planes[1].bucket.remaining(0), 6, "the copy, answered");
        assert_eq!(
            fleet.planes[2].bucket.remaining(0),
            10,
            "a data plane that did not ask hears nothing"
        );
    }

    /// With no hits to send, a data plane still asks for a copy while the key is in use —
    /// for one bucket's refill time after its last request — and then stops.
    #[test]
    fn a_data_plane_asks_for_a_copy_while_the_key_is_in_use() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        fleet.on_timer(0, 0, &mut outbox);
        assert!(outbox.sent.is_empty(), "never asked for: nothing to ask");
        admits(&mut fleet, 1, 3, 0);
        fleet.on_timer(1, 0, &mut outbox);
        assert_eq!(admits(&mut fleet, 0, 1, 0), 1);
        fleet.on_timer(0, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        admits(&mut fleet, 1, 6, 40 * MS);
        fleet.on_timer(1, 40 * MS, &mut outbox);
        deliver(&mut fleet, &mut outbox, 40 * MS);
        fleet.on_timer(0, 50 * MS, &mut outbox);
        let service = fleet.address(0);
        assert!(
            matches!(outbox.sent.as_slice(), [(to, Message::Poll { from: 0, .. })] if *to == service),
            "{:?}",
            outbox.sent
        );
        deliver(&mut fleet, &mut outbox, 50 * MS);
        assert_eq!(
            fleet.planes[0].bucket.remaining(50 * MS),
            5,
            "pod 1's six since, in the answer"
        );
        fleet.on_timer(0, 100 * MS, &mut outbox);
        assert_eq!(
            outbox.sent.len(),
            1,
            "a refill time to the nanosecond: still in use"
        );
        outbox.sent.clear();
        fleet.on_timer(0, 101 * MS, &mut outbox);
        assert!(
            outbox.sent.is_empty(),
            "a refill time since its last request"
        );
    }

    /// Its own hits stay in a data plane's copy until an answer shows them applied.
    #[test]
    fn a_copy_keeps_its_own_hits_until_the_service_has_them() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        outbox.sent.clear();
        admits(&mut fleet, 1, 2, 0);
        fleet.on_timer(0, 0, &mut outbox);
        fleet.on_timer(1, 0, &mut outbox);
        // The first batch is lost; the second arrives ahead of it and waits.
        let mut answers = Outbox::default();
        for (to, message) in std::mem::take(&mut outbox.sent) {
            fleet.on_message(to, message, 0, &mut answers);
        }
        deliver(&mut fleet, &mut answers, 0);
        assert_eq!(fleet.owners[0].counted, 0, "the second waits for the first");
        assert_eq!(
            fleet.planes[1].bucket.remaining(0),
            4,
            "six of its own, none seen applied"
        );
        fleet.on_timer(1, 40 * MS, &mut outbox);
        deliver(&mut fleet, &mut outbox, 40 * MS);
        assert_eq!(
            fleet.owners[0].counted, 6,
            "sent again, both applied in order"
        );
        assert!(fleet.planes[1].unacknowledged.is_empty());
    }

    /// Hits admitted since the last batch are charged into the copy an answer brings.
    #[test]
    fn a_copy_keeps_hits_admitted_since_the_last_batch() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        admits(&mut fleet, 1, 2, 0);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.owners[0].counted, 4);
        assert_eq!(
            fleet.planes[1].bucket.remaining(0),
            4,
            "four applied, two unsent"
        );
    }

    /// The owner owes hits that do not fit, up to its cap, and forgives the rest.
    #[test]
    fn the_owner_owes_excess_up_to_its_cap() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        assert_eq!(
            admits(&mut fleet, 0, 10, 0) + admits(&mut fleet, 1, 10, 0),
            20
        );
        fleet.on_timer(0, 0, &mut outbox);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(fleet.owners[0].bucket.retry_at(0), Some(110 * MS));
    }

    /// A batch not seen applied within a bucket's refill time is emptied, and goes on being
    /// sent, empty.
    #[test]
    fn a_batch_is_emptied_after_a_refill_time() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        fleet.on_timer(1, 100 * MS, &mut outbox);
        assert_eq!(fleet.planes[1].unacknowledged[0].1, 4, "still held");
        outbox.sent.clear();
        fleet.on_timer(1, 100 * MS + 1, &mut outbox);
        assert_eq!(fleet.planes[1].unacknowledged[0].1, 0, "emptied");
        outbox.sent.clear();
        fleet.on_timer(1, 140 * MS, &mut outbox);
        assert!(
            matches!(
                outbox.sent.as_slice(),
                [(
                    _,
                    Message::Hits {
                        sequence: 1,
                        count: 0,
                        ..
                    }
                )]
            ),
            "{:?}",
            outbox.sent
        );
    }

    /// A batch emptied does not hold back those after it: the owner applies them in order
    /// all the same.
    #[test]
    fn a_batch_emptied_does_not_hold_back_the_next() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        outbox.sent.clear();
        let later = 100 * MS + 1;
        admits(&mut fleet, 1, 2, later);
        fleet.on_timer(1, later, &mut outbox);
        deliver(&mut fleet, &mut outbox, later);
        assert_eq!(fleet.owners[0].counted, 2, "the next batch applied");
        assert!(fleet.planes[1].unacknowledged.is_empty());
    }

    /// The owner says which data planes have admitted the key, counted back from the
    /// latest admission it knows of.
    #[test]
    fn the_owner_says_who_was_admitting_as_of_the_last_traffic() {
        let mut fleet = fleet(3, Bound::Every);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 1, 0);
        admits(&mut fleet, 2, 1, 0);
        fleet.on_timer(1, 0, &mut outbox);
        fleet.on_timer(2, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        let later = 5_000 * MS;
        assert!(fleet.decide(0, later, &mut outbox));
        fleet.on_timer(0, later, &mut outbox);
        deliver(&mut fleet, &mut outbox, later);
        assert_eq!(fleet.planes[0].active, vec![true, false, false]);
        fleet.on_timer(1, later, &mut outbox);
        admits(&mut fleet, 1, 1, later);
        fleet.on_timer(1, later + 10 * MS, &mut outbox);
        deliver(&mut fleet, &mut outbox, later + 10 * MS);
        assert_eq!(fleet.planes[1].active, vec![true, true, false]);
        assert_eq!(
            fleet.allowance(1, later + 10 * MS),
            Some(5),
            "half of the nine left and a cycle's refill"
        );
    }

    /// A data plane restarts: the service counts its new life afresh and ignores its old.
    #[test]
    fn a_restarted_data_plane_is_numbered_afresh_and_its_old_life_ignored() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 3, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        admits(&mut fleet, 1, 4, 0);
        fleet.on_timer(1, 0, &mut outbox);
        let late = std::mem::take(&mut outbox.sent);
        fleet.on_fault(FaultEvent::Restart { pod: 1 }, 0, &mut outbox);
        assert_eq!(fleet.planes[1].bucket.remaining(0), 10, "it holds nothing");
        admits(&mut fleet, 1, 2, 0);
        fleet.on_timer(1, 0, &mut outbox);
        deliver(&mut fleet, &mut outbox, 0);
        assert_eq!(
            fleet.owners[0].counted, 5,
            "the new life's first batch applied"
        );
        for (to, message) in late {
            fleet.on_message(to, message, 0, &mut outbox);
        }
        assert_eq!(
            fleet.owners[0].counted, 5,
            "the old life's late batch ignored"
        );
    }

    /// The owner goes or restarts: data planes number their batches afresh for it and send
    /// again those not seen applied; it starts from a full bucket.
    #[test]
    fn data_planes_send_again_to_a_new_or_restarted_owner() {
        for event in [FaultEvent::OwnerLost, FaultEvent::OwnerRestarts] {
            let mut fleet = fleet(2, Bound::Unbounded);
            let mut outbox = Outbox::default();
            admits(&mut fleet, 1, 3, 0);
            fleet.on_timer(1, 0, &mut outbox);
            deliver(&mut fleet, &mut outbox, 0);
            admits(&mut fleet, 1, 2, 0);
            fleet.on_timer(1, 0, &mut outbox);
            outbox.sent.clear();
            fleet.on_fault(event, 0, &mut outbox);
            let (owner, service) = (fleet.owner, fleet.address(fleet.owner));
            assert!(
                matches!(outbox.sent.as_slice(), [(to, Message::Hits { sequence: 1, count: 2, .. })] if *to == service),
                "{event:?}: {:?}",
                outbox.sent
            );
            deliver(&mut fleet, &mut outbox, 0);
            assert_eq!(fleet.owners[owner].counted, 2, "{event:?}");
            assert_eq!(fleet.owners[owner].bucket.remaining(0), 8, "{event:?}");
        }
    }

    /// An answer from an owner that has gone, arriving late, is ignored: its marks belong to
    /// the old numbering.
    #[test]
    fn an_answer_from_an_owner_that_has_gone_is_ignored() {
        let mut fleet = fleet(2, Bound::Unbounded);
        let mut outbox = Outbox::default();
        admits(&mut fleet, 1, 3, 0);
        fleet.on_timer(1, 0, &mut outbox);
        let mut answers = Outbox::default();
        for (to, message) in std::mem::take(&mut outbox.sent) {
            fleet.on_message(to, message, 0, &mut answers);
        }
        fleet.on_fault(FaultEvent::OwnerLost, 0, &mut outbox);
        outbox.sent.clear();
        deliver(&mut fleet, &mut answers, 0);
        assert_eq!(
            fleet.planes[1].unacknowledged.len(),
            1,
            "still waiting for the new owner"
        );
        assert_eq!(fleet.planes[1].bucket.remaining(0), 7);
    }

    /// Data planes never talk to each other: every message goes to the service, and every
    /// answer to the data plane that asked.
    #[test]
    fn data_planes_talk_only_to_the_service() {
        let mut fleet = fleet(4, Bound::Every);
        let mut outbox = Outbox::default();
        for plane in 0..4 {
            admits(&mut fleet, plane, 2, 0);
            fleet.on_timer(plane, 0, &mut outbox);
        }
        let service = fleet.address(0);
        assert!(outbox.sent.iter().all(|(to, _)| *to == service));
        let mut answers = Outbox::default();
        for (to, message) in std::mem::take(&mut outbox.sent) {
            let from = message.sender();
            fleet.on_message(to, message, 0, &mut answers);
            assert!(matches!(answers.sent.last(), Some((to, _)) if *to == from));
        }
    }
}
