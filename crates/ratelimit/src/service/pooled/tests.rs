//! Tests of the pooled participant: against `DataPlane` as the semantic reference for one
//! participant, the races between workers, and between workers and the transport. Not
//! under loom, whose atomics work only inside its model (`loom_tests`).

#![cfg(not(loom))]

use super::*;
use crate::service::{Answer, DataPlane, ENTRY_BYTES, FixedState, Keyed, PIECE_BYTES, ServicePod};
use proptest::prelude::*;
use std::collections::VecDeque;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
/// An interval of a whole number of units: 160 ns a unit, 65,536 an interval.
const INTERVAL: u64 = 65_536 * 160;
/// The protocol's cycle, a whole number of units too.
const CYCLE: u64 = 10 * MS;

/// `limit` a window of `limit` intervals, a bucket of `burst`: one every 10.49 ms.
fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(limit * INTERVAL),
        burst: n(burst),
    }
}

fn view() -> View {
    View {
        epoch: 1,
        pods: vec![0],
    }
}

fn participant() -> Participant<FixedState> {
    Participant::new(7, CYCLE, view(), FixedState::default()).unwrap()
}

/// The participant and its pod, a worker's handles to the entries it was given, and the
/// pod's answers not yet delivered: each with its exchange and its place among the answers
/// the pod gave.
struct Pooled {
    participant: Participant<FixedState>,
    pod: ServicePod,
    handles: HashMap<Box<[u8]>, Arc<Entry>>,
    pending: Vec<(u64, u64, Answers)>,
    /// The answers the pod has given.
    given: u64,
    /// The place of the latest answer whose buckets the participant took.
    taken: u64,
}

impl Pooled {
    /// Its pod's answers unnumbered, each taken as it comes, as `DataPlane` takes them.
    fn new() -> Self {
        Self {
            participant: participant(),
            pod: ServicePod::new(0, CYCLE, view()),
            handles: HashMap::new(),
            pending: Vec::new(),
            given: 0,
            taken: 0,
        }
    }

    /// `batch` answered by the pod: the answers and their place among those it gave.
    fn answer(&mut self, batch: Batch, now: u64) -> (u64, Answers) {
        self.given += 1;
        (self.given, self.pod.on_batch(in_order(batch), now))
    }

    /// The pod's `place`th answers, to `exchange`, reach the participant. Numbered, no
    /// answer's buckets are taken over a later one's: a copy never goes back to an older
    /// bucket.
    fn take(&mut self, exchange: u64, place: u64, answers: Answers, now: u64) {
        let late = self.participant.late_answers();
        let numbered = answers.life != 0;
        self.participant.on_answers_to(exchange, answers, now);
        if numbered && self.participant.late_answers() == late {
            assert!(
                place > self.taken,
                "answer {place} taken after answer {}",
                self.taken
            );
            self.taken = place;
        }
    }

    /// Its pod numbering its answers, and its participant visiting only the keys that need
    /// it if `scheduled`, its workers' signals handed to it as they decide.
    fn numbered(scheduled: bool) -> Self {
        let mut pooled = Self::new();
        pooled.pod.set_life(1);
        pooled.participant.set_scheduled(scheduled);
        pooled
    }

    /// What it has to send, answered by the pod and held back.
    fn send(&mut self, now: u64) {
        for (_, exchange, batch) in self.participant.take_exchanges() {
            let (place, answers) = self.answer(batch, now);
            self.pending.push((exchange, place, answers));
        }
    }

    /// The `at`th answer held back (counting round), delivered, and kept to deliver again
    /// if `twice`.
    fn deliver(&mut self, at: usize, twice: bool, now: u64) {
        if self.pending.is_empty() {
            return;
        }
        let at = at % self.pending.len();
        let (exchange, place, answers) = if twice {
            self.pending[at].clone()
        } else {
            self.pending.remove(at)
        };
        self.take(exchange, place, answers, now);
    }

    /// A worker's decision: from its handles if it holds them all and they decide it;
    /// otherwise asked of the participant, whose entries it then holds.
    fn decide(&mut self, keys: &[(&[u8], Contract)], now: u64) -> Decision {
        let held: Option<Vec<Arc<Entry>>> = keys
            .iter()
            .map(|(key, contract)| {
                self.handles
                    .get(*key)
                    .filter(|entry| entry.contract() == *contract)
                    .cloned()
            })
            .collect();
        if let Some(held) = held {
            let entries: Vec<&Entry> = held.iter().map(|entry| &**entry).collect();
            let outcome = decide(&entries, now);
            let signalled = held
                .iter()
                .enumerate()
                .filter(|(at, _)| outcome.signal & (1 << at) != 0)
                .map(|(_, entry)| Arc::clone(entry));
            self.participant.on_signals(signalled);
            if outcome.drain {
                for (key, _) in keys {
                    self.participant.drain(key, now);
                }
            }
            match outcome.local {
                Local::Admit => return Decision::Admit,
                Local::Refuse => return Decision::Refuse,
                Local::Defer => return Decision::Defer,
                Local::Ask => {}
            }
        }
        let decision = self.participant.decide_all(keys, now);
        for entry in self.participant.entries(keys) {
            self.handles.insert(entry.key().into(), entry);
        }
        decision
    }

    fn exchange(&mut self, now: u64) {
        for _ in 0..10 {
            let outgoing = self.participant.take_exchanges();
            if outgoing.is_empty() {
                break;
            }
            for (_, exchange, batch) in outgoing {
                let (place, answers) = self.answer(batch, now);
                self.take(exchange, place, answers, now);
            }
        }
    }
}

/// `batch` as the pod is given it, its reports numbered as the wire takes them: from 1, and
/// seeing only earlier reports applied, whatever was resent, answered late or answered
/// twice before it.
fn in_order(batch: Batch) -> Batch {
    for entry in &batch.entries {
        if let Report::Hits { sequence, seen, .. } = entry.report {
            assert!(seen < sequence, "report {sequence} seeing {seen} applied");
        }
    }
    batch
}

/// The reference: one `DataPlane`, its pod, and the answers not yet delivered.
struct Reference {
    plane: DataPlane,
    pod: ServicePod,
    pending: Vec<Answers>,
}

impl Reference {
    fn new() -> Self {
        Self {
            plane: DataPlane::new(7, CYCLE, view()),
            pod: ServicePod::new(0, CYCLE, view()),
            pending: Vec::new(),
        }
    }

    fn send(&mut self, now: u64) {
        for (_, batch) in self.plane.take_outgoing() {
            self.pending.push(self.pod.on_batch(in_order(batch), now));
        }
    }

    fn deliver(&mut self, at: usize, twice: bool, now: u64) {
        if self.pending.is_empty() {
            return;
        }
        let at = at % self.pending.len();
        let answers = if twice {
            self.pending[at].clone()
        } else {
            self.pending.remove(at)
        };
        self.plane.on_answers(answers, now);
    }

    fn exchange(&mut self, now: u64) {
        for _ in 0..10 {
            let outgoing = self.plane.take_outgoing();
            if outgoing.is_empty() {
                break;
            }
            for (_, batch) in outgoing {
                let answers = self.pod.on_batch(in_order(batch), now);
                self.plane.on_answers(answers, now);
            }
        }
    }
}

/// What happens next in a schedule: a request under one key, the cycle, or the messages
/// carried both ways.
#[derive(Clone, Debug)]
enum Event {
    Request(usize),
    /// A request under two limits, both keys charged or neither.
    Pair(usize, usize),
    Cycle,
    Exchange,
    /// What is to be sent, answered by the pod and held back.
    Send,
    /// An answer held back delivered — any of them, so late and out of order — and, if
    /// so, kept to come again.
    Deliver(usize, bool),
}

fn event() -> impl Strategy<Value = Event> {
    prop_oneof![
        6 => (0usize..3).prop_map(Event::Request),
        2 => (0usize..3, 1usize..3).prop_map(|(a, b)| Event::Pair(a, (a + b) % 3)),
        1 => Just(Event::Cycle),
        2 => Just(Event::Exchange),
        1 => Just(Event::Send),
        2 => (0usize..8, prop::bool::weighted(0.1)).prop_map(|(at, twice)| Event::Deliver(at, twice)),
    ]
}

proptest! {
    // 2,048 schedules unless PROPTEST_CASES says more, for a longer run.
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_048)
    ))]

    /// One participant decides every request — under one limit or two, all or none — as one
    /// `DataPlane` does, the cycle and the answers — late, out of order, twice — included:
    /// the per-key state moved into shared words changes no decision. Requests come a unit
    /// apart or more, cycles on their boundary.
    ///
    /// But one: where `DataPlane` refuses a request because another's hold is outstanding,
    /// pooled defers it, rather than refuse what the hold may yet give back. Such a request
    /// is set aside — the reference's refusal of it, which changed nothing, put aside too —
    /// and given to both again after every event, compared when pooled decides it. Pooled
    /// defers only where the reference refuses.
    #[test]
    fn a_participant_decides_as_a_data_plane(
        events in prop::collection::vec((event(), 0u64..4), 1..300),
    ) {
        let keys: [(&[u8], Contract); 3] = [
            (b"a", contract(100, 10)),
            (b"b", contract(10, 3)),
            (b"c", contract(1_000, 50)),
        ];
        let mut comparison = Comparison::new(keys);
        let mut now = 0;
        let mut next_cycle = CYCLE;
        for (at, (event, step)) in events.into_iter().enumerate() {
            now += step * 40_000 * 25;
            while now >= next_cycle {
                comparison.cycle(next_cycle);
                next_cycle += CYCLE;
            }
            comparison.event(at, &event, now)?;
            comparison.again(at, now)?;
        }
        // What is still set aside, decided once the holds it waits on have settled.
        for round in 0..20 {
            if comparison.aside.is_empty() {
                break;
            }
            now = next_cycle;
            comparison.cycle(next_cycle);
            next_cycle += CYCLE;
            comparison.event(1_000 + round, &Event::Exchange, now)?;
            comparison.again(1_000 + round, now)?;
        }
        prop_assert!(comparison.aside.is_empty(), "never decided: {:?}", comparison.aside);
        for (key, _) in keys {
            prop_assert_eq!(comparison.pooled.pod.counted(key), comparison.reference.pod.counted(key));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_048)
    ))]

    /// No admission goes uncounted, and no answer's buckets are taken over a later
    /// answer's ([`Pooled::take`]), whether the participant visits every
    /// key each cycle or only those that need it, quiet keys polled or not, one exchange in
    /// flight or more:
    /// over a schedule of requests, cycles and answers — late, out
    /// of order, twice — and then everything delivered and settled, each key's count at its
    /// owner is what was admitted under it. Now
    /// and then the next cycle runs with no room for any key, evicting every
    /// key it may, to be made again by its next request: its reports then number on past
    /// those of its earlier life, which the pod, never forgetting the key, still holds.
    #[test]
    fn no_admission_goes_uncounted_scheduled_or_not(
        events in prop::collection::vec((event(), 0u64..4, prop::bool::weighted(0.1)), 1..300),
        poll_quiet: bool,
        one_in_flight: bool,
    ) {
        let keys: [(&[u8], Contract); 3] = [
            (b"a", contract(100, 80)),
            (b"b", contract(10, 70)),
            (b"c", contract(1_000, 100)),
        ];
        for scheduled in [false, true] {
            let mut pooled = Pooled::numbered(scheduled);
            pooled.participant.set_levers(Levers { poll_quiet });
            pooled.participant.set_one_in_flight(one_in_flight);
            let mut admitted = [0u64; 3];
            let mut now = 0;
            let mut next_cycle = CYCLE;
            let mut evicting = false;
            for (event, step, evict) in &events {
                now += step * 40_000 * 25;
                if *evict {
                    pooled.participant.set_capacity(1);
                    evicting = true;
                }
                while now >= next_cycle {
                    pooled.participant.on_cycle(next_cycle);
                    next_cycle += CYCLE;
                    if evicting {
                        pooled.participant.set_capacity(u64::MAX);
                        evicting = false;
                    }
                }
                let places = match event {
                    Event::Request(key) => vec![*key],
                    Event::Pair(first, second) => vec![*first, *second],
                    Event::Cycle => continue,
                    Event::Exchange => {
                        pooled.exchange(now);
                        continue;
                    }
                    Event::Send => {
                        pooled.send(now);
                        continue;
                    }
                    Event::Deliver(index, twice) => {
                        pooled.deliver(*index, *twice, now);
                        continue;
                    }
                };
                let request: Vec<(&[u8], Contract)> =
                    places.iter().map(|place| keys[*place]).collect();
                if pooled.decide(&request, now) == Decision::Admit {
                    for place in &places {
                        admitted[*place] += 1;
                    }
                }
            }
            for _ in 0..50 {
                now = next_cycle;
                pooled.participant.on_cycle(now);
                next_cycle += CYCLE;
                while !pooled.pending.is_empty() {
                    pooled.deliver(0, false, now);
                }
                pooled.exchange(now);
            }
            for (place, (key, _)) in keys.iter().enumerate() {
                let counted = pooled.pod.counted(key).unwrap_or(0);
                prop_assert_eq!(
                    counted,
                    admitted[place],
                    "scheduled {}, key {}",
                    scheduled,
                    place
                );
            }
        }
    }
}

/// Pooled against the reference over one schedule, with the requests pooled set aside.
struct Comparison {
    keys: [(&'static [u8], Contract); 3],
    pooled: Pooled,
    reference: Reference,
    /// Requests deferred, by their keys' places.
    aside: Vec<Vec<usize>>,
}

impl Comparison {
    fn new(keys: [(&'static [u8], Contract); 3]) -> Self {
        Self {
            keys,
            pooled: Pooled::new(),
            reference: Reference::new(),
            aside: Vec::new(),
        }
    }

    fn cycle(&mut self, now: u64) {
        self.pooled.participant.on_cycle(now);
        self.reference.plane.on_cycle(now);
    }

    /// A request under the keys at `places`: decided alike, or set aside where pooled
    /// defers and the reference refuses. Whether pooled decided it.
    fn request(&mut self, at: usize, places: &[usize], now: u64) -> Result<bool, TestCaseError> {
        let request: Vec<(&[u8], Contract)> =
            places.iter().map(|place| self.keys[*place]).collect();
        let got = self.pooled.decide(&request, now);
        if got == Decision::Defer {
            return Ok(false);
        }
        let want = self.reference.plane.decide_all(&request, now);
        prop_assert_eq!(
            discriminant(got),
            discriminant(want),
            "event {} at {}: {:?} against {:?}",
            at,
            now,
            got,
            want
        );
        Ok(true)
    }

    fn event(&mut self, at: usize, event: &Event, now: u64) -> Result<(), TestCaseError> {
        match event {
            Event::Request(key) => {
                if !self.request(at, &[*key], now)? {
                    self.set_aside(at, &[*key], now)?;
                }
            }
            Event::Pair(first, second) => {
                if !self.request(at, &[*first, *second], now)? {
                    self.set_aside(at, &[*first, *second], now)?;
                }
            }
            Event::Cycle => {}
            Event::Exchange => {
                self.pooled.exchange(now);
                self.reference.exchange(now);
            }
            Event::Send => {
                self.pooled.send(now);
                self.reference.send(now);
                prop_assert_eq!(
                    self.pooled.pending.len(),
                    self.reference.pending.len(),
                    "event {}: batches",
                    at
                );
            }
            Event::Deliver(index, twice) => {
                self.pooled.deliver(*index, *twice, now);
                self.reference.deliver(*index, *twice, now);
            }
        }
        Ok(())
    }

    /// Pooled deferred a request: the reference must have refused it, which changes nothing
    /// it keeps but the keys' use.
    fn set_aside(&mut self, at: usize, places: &[usize], now: u64) -> Result<(), TestCaseError> {
        let request: Vec<(&[u8], Contract)> =
            places.iter().map(|place| self.keys[*place]).collect();
        let want = self.reference.plane.decide_all(&request, now);
        prop_assert_eq!(
            want,
            crate::service::Decision::Refuse,
            "event {}: deferred where the reference does not refuse",
            at
        );
        self.aside.push(places.to_vec());
        Ok(())
    }

    /// Every request set aside, decided again on both sides.
    fn again(&mut self, at: usize, now: u64) -> Result<(), TestCaseError> {
        for places in std::mem::take(&mut self.aside) {
            if !self.request(at, &places, now)? {
                self.aside.push(places);
            }
        }
        Ok(())
    }
}

/// Which decision: pooled's and the reference's compared.
fn discriminant(decision: impl Into<Kind>) -> Kind {
    decision.into()
}

#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Admit,
    Refuse,
    Defer,
}

impl From<Decision> for Kind {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Admit => Self::Admit,
            Decision::Refuse => Self::Refuse,
            Decision::Defer => Self::Defer,
        }
    }
}

impl From<crate::service::Decision> for Kind {
    fn from(decision: crate::service::Decision) -> Self {
        match decision {
            crate::service::Decision::Admit => Self::Admit,
            crate::service::Decision::Refuse => Self::Refuse,
        }
    }
}

/// Room for `burst` at once and nothing more: many threads taking at once, started
/// together, get the bucket exactly, the final token to one of them. 16,000: within what a
/// key may hold unreported, as nothing drains it here.
#[test]
fn threads_racing_for_the_last_tokens_admit_the_bucket_exactly() {
    let entry = Entry::new(b"k"[..].into(), contract(1, 16_000), CYCLE, 0);
    let start = std::sync::Barrier::new(8);
    let admitted: u64 = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    (0..40_000)
                        .filter(|_| decide(&[&entry], 0).local == Local::Admit)
                        .count() as u64
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    });
    assert_eq!(admitted, 16_000);
    assert_eq!(entry.snapshot().unreported, 16_000);
}

/// A cycle's allowance is spent once however many threads spend it, and the cycle advances
/// once however many meet it: a share of 50 a cycle, eight threads, three cycles.
#[test]
fn threads_spend_a_cycles_allowance_once_and_advance_it_once() {
    // A bucket deep enough that only the allowance bounds: the share is what the copy has
    // left plus the refill, among `others + 1`, and `others` is set so that it is 50.
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 2_000), CYCLE, 0);
    entry.others.store(39, Ordering::Release);
    for cycle in 1..=3u64 {
        let now = cycle * CYCLE;
        let admitted: u64 = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..100)
                            .filter(|_| decide(&[&entry], now).local == Local::Admit)
                            .count() as u64
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .sum()
        });
        let fields = entry.snapshot();
        assert_eq!(fields.left, 0, "cycle {cycle}: spent");
        assert_eq!(fields.cycle, word::cycle_of(now, CYCLE));
        // What one advance allows, as the word worked it out: the first a full share, the
        // second its share of the refill, alternating while others admit.
        assert!((1..=60).contains(&admitted), "cycle {cycle}: {admitted}");
    }
}

/// The transport draining while workers admit loses no admission and counts none twice:
/// what it sent and what is left add up to what the workers admitted — with the count's
/// most the real one, and with one of 8, so that admissions meet a full count and wait for
/// a drain rather than wrap it.
#[test]
fn reports_racing_admissions_count_each_once() {
    for most in [word::MOST_RESERVED, 8] {
        let mut participant = participant();
        let key: &[u8] = b"k";
        let contract = contract(1_000_000, 1_000_000);
        participant.ensure(key, contract, 0);
        set_most_reserved(&mut participant, key, most);
        let entry = participant.entries(&[(key, contract)]).remove(0);
        // Drained until every worker is done, however late the threads start: a fixed
        // number of drains could end before they filled the small count.
        let done = std::sync::atomic::AtomicUsize::new(0);
        let (admitted, full, sent) = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let entry = Arc::clone(&entry);
                    let done = &done;
                    scope.spawn(move || {
                        let (mut admitted, mut full) = (0u64, 0u64);
                        while admitted < 20_000 {
                            match entry.take(Op::Admit, 1).0 {
                                Taken::Done | Taken::Pressed => admitted += 1,
                                Taken::Full => full += 1,
                                // Time standing still, the bucket runs out: done.
                                Taken::Refused | Taken::Closed => break,
                                Taken::Contended => {}
                            }
                        }
                        done.fetch_add(1, Ordering::Relaxed);
                        (admitted, full)
                    })
                })
                .collect();
            let mut sent = 0u64;
            while done.load(Ordering::Relaxed) < 4 {
                participant.on_cycle(1);
                for (_, batch) in participant.take_outgoing() {
                    for batch_entry in batch.entries {
                        if let Report::Hits { count, .. } = batch_entry.report {
                            sent += count;
                        }
                    }
                }
            }
            let (admitted, full) = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .fold((0, 0), |(a, f), (admitted, full)| (a + admitted, f + full));
            (admitted, full, sent)
        });
        let tracked = &participant.keys[key];
        let fields = entry.snapshot();
        assert!(fields.unreported <= most, "never past its most");
        assert_eq!(
            sent + fields.unreported + tracked.unsent,
            admitted,
            "most {most}"
        );
        if most == 8 {
            assert!(full > 0, "the small count filled");
        }
    }
}

/// Sets the most `key`'s entry may hold unreported, holds included: small, to fill.
fn set_most_reserved(participant: &mut Participant<FixedState>, key: &[u8], most: u64) {
    let tracked = participant.keys.get_mut(key).unwrap();
    let entry = Arc::get_mut(&mut tracked.entry);
    match entry {
        Some(entry) => entry.most_reserved = most,
        None => {
            // Shared with the registry: made again with the most set, and registered.
            let old = &tracked.entry;
            let mut fresh = Entry::new(old.key.clone(), old.terms.contract, old.terms.cycle, 0);
            fresh.most_reserved = most;
            participant.registry.remove(&tracked.entry);
            tracked.entry = Arc::new(fresh);
            participant.registry.insert(Arc::clone(&tracked.entry));
        }
    }
}

/// With every place in the count reserved — admitted and held — holds still settle either
/// way, and a request more defers rather than counts past its most; drained, it is taken.
#[test]
fn a_full_count_defers_and_holds_still_settle() {
    let mut entry = Entry::new(b"k"[..].into(), contract(100, 100), CYCLE, 0);
    entry.most_reserved = 4;
    for _ in 0..2 {
        assert!(entry.take(Op::Hold, 0).0.done());
    }
    for _ in 0..2 {
        assert!(entry.take(Op::Admit, 0).0.done());
    }
    assert_eq!(entry.take(Op::Admit, 0).0, Taken::Full);
    assert_eq!(decide(&[&entry], 0).local, Local::Defer);
    entry.settle(true, 0);
    entry.settle(false, 0);
    let fields = entry.snapshot();
    assert_eq!((fields.unreported, fields.held), (3, 0));
    assert!(
        entry.take(Op::Admit, 0).0.done(),
        "the given-back place free again"
    );
    assert_eq!(entry.take(Op::Admit, 0).0, Taken::Full);
    let (drained, _, _) = entry.drain(0, Cycle::AsItIs);
    assert_eq!(drained, 4);
    assert_eq!(decide(&[&entry], 0).local, Local::Admit);
}

/// Passing half its count's most, a key says once that it wants draining; drained, it can
/// say so again.
#[test]
fn a_key_asks_for_a_drain_once_at_half_its_count() {
    let mut entry = Entry::new(b"k"[..].into(), contract(100, 100), CYCLE, 0);
    entry.most_reserved = 8;
    let drains: Vec<bool> = (0..6).map(|_| decide(&[&entry], 0).drain).collect();
    assert_eq!(drains, [false, false, false, true, false, false]);
    entry.drain(0, Cycle::AsItIs);
    let drains: Vec<bool> = (0..4).map(|_| decide(&[&entry], 0).drain).collect();
    assert_eq!(drains, [false, false, false, true]);
}

/// The refund of a refused hold, from the word alone: given to a cycle that has spent
/// some — the hold's own, or a later one another worker advanced to and spent from — and
/// not to one that has spent nothing, which would raise it past its allowance.
#[test]
fn a_refused_hold_gives_back_to_a_cycle_only_what_it_spent() {
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 1_000), CYCLE, 0);
    // Its own cycle: held, refused, all back.
    assert!(entry.take(Op::Hold, CYCLE).0.done());
    let held = entry.snapshot();
    entry.settle(false, CYCLE);
    let back = entry.snapshot();
    assert_eq!((back.left, back.spent), (held.left + 1, held.spent - 1));
    // Held in one cycle; the next advanced by another worker's admission, which spends one;
    // the hold refused after: the new cycle gets its one back.
    assert!(entry.take(Op::Hold, 2 * CYCLE).0.done());
    assert!(entry.take(Op::Admit, 3 * CYCLE).0.done());
    let spent = entry.snapshot();
    assert_eq!(spent.spent, 1);
    entry.settle(false, 3 * CYCLE);
    let after = entry.snapshot();
    assert_eq!((after.left, after.spent), (spent.left + 1, 0));
    // Held in one cycle, refused in the next, which has spent nothing: nothing given to it.
    assert!(entry.take(Op::Hold, 4 * CYCLE).0.done());
    let fresh = word::advanced(
        &entry.terms.bucket,
        entry.snapshot(),
        entry.advance(),
        5 * CYCLE,
    );
    entry.settle(false, 5 * CYCLE);
    let after = entry.snapshot();
    assert_eq!(
        (after.left, after.spent),
        (fresh.left, 0),
        "its allowance, no more"
    );
}

/// A request that finds a key's last room taken by another's hold defers rather than be
/// refused; the hold given back, it is admitted. The three steps of the race: A holds the
/// last token on X, B needs X, A fails on Y and gives X back.
#[test]
fn a_request_short_only_by_anothers_hold_waits_for_it() {
    let x = Entry::new(b"x"[..].into(), contract(100, 1), CYCLE, 0);
    assert!(x.take(Op::Hold, 0).0.done(), "A holds X's last token");
    let b = decide(&[&x], 0);
    assert_eq!(b.local, Local::Defer, "B is not refused");
    x.add_waiter();
    assert!(x.settle(false, 0).0, "A gives X back, and B is to be woken");
    assert_eq!(decide(&[&x], 0).local, Local::Admit, "B, again");
    x.remove_waiter();
    // With nothing held, no room is a refusal.
    assert_eq!(decide(&[&x], 0).local, Local::Refuse);
    assert!(!x.settle(true, 0).0 || x.snapshot().held == 0);
}

/// A key's holds at their most defer a request that would hold one more, its earlier holds
/// given back; one settled, it is admitted.
#[test]
fn holds_at_their_most_defer_a_request_rather_than_refuse_it() {
    let x = Entry::new(b"x"[..].into(), contract(1_000, 1_000), CYCLE, 0);
    let y = Entry::new(b"y"[..].into(), contract(1_000, 1_000), CYCLE, 0);
    for _ in 0..word::MOST_HELD {
        assert!(x.take(Op::Hold, 0).0.done());
    }
    let before = y.snapshot();
    assert_eq!(decide(&[&y, &x], 0).local, Local::Defer);
    given_back_as_it_was(&y, before);
    x.settle(true, 0);
    assert_eq!(decide(&[&y, &x], 0).local, Local::Admit);
}

/// Settling or draining wakes only where requests wait: none waiting, nothing to wake.
#[test]
fn only_a_key_with_waiters_wants_waking() {
    let x = Entry::new(b"x"[..].into(), contract(100, 10), CYCLE, 0);
    assert!(x.take(Op::Hold, 0).0.done());
    assert!(!x.settle(false, 0).0);
    assert!(x.take(Op::Admit, 0).0.done());
    x.add_waiter();
    assert!(x.drain(0, Cycle::AsItIs).2);
    x.remove_waiter();
    assert!(x.take(Op::Admit, 0).0.done());
    assert!(!x.drain(0, Cycle::AsItIs).2);
}

/// An answer racing admissions rebases the bucket on the owner's view and charges every
/// admission the owner has not seen exactly once: what was admitted since the report,
/// what was reported and not acknowledged, and what is held.
#[test]
fn a_rebase_charges_what_the_owner_has_not_seen_once() {
    let mut participant = participant();
    let key: &[u8] = b"k";
    let c = contract(100, 100);
    participant.ensure(key, c, 0);
    let entry = participant.entries(&[(key, c)]).remove(0);
    // Three admitted and reported, two admitted since, one held.
    for _ in 0..3 {
        assert!(entry.take(Op::Admit, 0).0.done());
    }
    participant.on_cycle(0);
    let _batches = participant.take_outgoing();
    for _ in 0..2 {
        assert!(entry.take(Op::Admit, 0).0.done());
    }
    assert!(entry.take(Op::Hold, 0).0.done());
    // The owner has seen none of it, and its own bucket is full.
    participant.on_answers(
        Answers {
            from: 0,
            entries: vec![crate::service::Answer {
                key: key.into(),
                behind: 0,
                applied: 0,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 0,
        },
        0,
    );
    let bucket = entry.terms.bucket;
    assert_eq!(
        bucket.remaining(entry.snapshot().arrival, bucket.units(0)),
        100 - 6,
        "three reported, two since, one held"
    );
    // Seen applied, the three reported leave the count of what the owner has not seen.
    participant.on_answers(
        Answers {
            from: 0,
            entries: vec![crate::service::Answer {
                key: key.into(),
                behind: 3 * u128::from(c.window.get()),
                applied: 1,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 0,
        },
        0,
    );
    assert_eq!(
        bucket.remaining(entry.snapshot().arrival, bucket.units(0)),
        100 - 6,
        "the owner's three, and the three it has not seen"
    );
}

/// Answers from a pod that does not say back when their batch was made — `made` 0 — come
/// from one that charges hits as they arrive: their buckets are taken as of when the
/// answers came, not as of time 0 and refilled since.
#[test]
fn answers_that_do_not_say_when_are_taken_as_they_come() {
    let mut participant = participant();
    let key: &[u8] = b"k";
    let c = contract(100, 10);
    participant.ensure(key, c, 0);
    let entry = participant.entries(&[(key, c)]).remove(0);
    let now = 50 * INTERVAL;
    participant.on_answers(
        Answers {
            from: 0,
            entries: vec![crate::service::Answer {
                key: key.into(),
                behind: 5 * u128::from(c.window.get()),
                applied: 0,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 0,
        },
        now,
    );
    let bucket = entry.terms.bucket;
    assert_eq!(
        bucket.remaining(entry.snapshot().arrival, bucket.units(now)),
        5
    );
}

/// Answers whose batch was made long before they came — a lost packet waiting for TCP to
/// send it again — are taken as of a cycle before they came, not as of then: the trip's
/// refill counted no further than a cycle.
#[test]
fn answers_held_up_are_taken_as_of_no_more_than_a_cycle_before() {
    let mut participant = participant();
    let key: &[u8] = b"k";
    let c = contract(100, 10);
    participant.ensure(key, c, 0);
    let entry = participant.entries(&[(key, c)]).remove(0);
    let now = 50 * INTERVAL;
    participant.on_answers(
        Answers {
            from: 0,
            entries: vec![crate::service::Answer {
                key: key.into(),
                behind: 5 * u128::from(c.window.get()),
                applied: 0,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 20 * INTERVAL,
        },
        now,
    );
    let bucket = entry.terms.bucket;
    assert_eq!(
        entry.snapshot().arrival,
        bucket.units(now - CYCLE) + 5 * 65_536
    );
}

/// A client at exactly its rate is admitted, however long its hits take to reach the owner
/// and its answer to come back: the owner charges a hit as of when it was admitted, not as
/// of when the batch arrives, and the copy taken from the answer loses no time either.
#[test]
fn a_client_at_exactly_its_rate_is_admitted_after_a_round_trip() {
    use crate::reference::Model as _;
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", contract(1, 1));
    let interval = c.window.get();
    let mut exact = c.bucket();
    assert!(exact.decide(0));
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (exchange, batch) = only_exchange(&mut participant);
    let answers = pod.on_batch(batch, CYCLE + MS);
    participant.on_answers_to(exchange, answers, CYCLE + 2 * MS);
    assert!(exact.decide(interval));
    assert_eq!(
        participant.decide_all(&[(key, c)], interval),
        Decision::Admit,
        "one interval after the last, as a plain bucket admits it"
    );
}

/// A participant as the proxy runs it — one exchange in flight to a pod, cold keys decided
/// from their copies, what it has to send sent every turn — and its pod, batches taking
/// `there` to reach the pod and answers `back` to return, a cycle every `CYCLE`: requests at
/// `times`. Whether each was admitted, `None` if never decided.
fn decided_over_round_trips(
    c: Contract,
    times: &[u64],
    there: u64,
    back: u64,
) -> Vec<Option<bool>> {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let key: &[u8] = b"k";
    let mut decided = vec![None; times.len()];
    // In order of arrival, each taking as long: exchange, answers, and when.
    let mut answers: VecDeque<(u64, u64, Answers)> = VecDeque::new();
    let mut next_cycle = CYCLE;
    let last = times.last().copied().unwrap_or(0);
    let mut at = 0;
    loop {
        let request = times.get(at).copied();
        let answer = answers.front().map(|(when, _, _)| *when);
        // Requests first at the same instant, as the harder order.
        let now = [request, Some(next_cycle), answer]
            .into_iter()
            .flatten()
            .min()
            .unwrap();
        if request == Some(now) {
            decided[at] = match participant.decide_all(&[(key, c)], now) {
                Decision::Admit => Some(true),
                Decision::Refuse => Some(false),
                waits => panic!("decided from the copy, not {waits:?}"),
            };
            at += 1;
        } else if answer == Some(now) {
            let (_, exchange, answer) = answers.pop_front().unwrap();
            participant.on_answers_to(exchange, answer, now);
        } else {
            if next_cycle > last + 4 * CYCLE && at == times.len() && answers.is_empty() {
                return decided;
            }
            participant.on_cycle(now);
            next_cycle += CYCLE;
        }
        for (_, exchange, batch) in participant.take_exchanges() {
            let answer = pod.on_batch(batch, now + there);
            answers.push_back((now + there + back, exchange, answer));
        }
    }
}

proptest! {
    /// Traffic a plain token bucket admits all of, over many rates and bursts, is admitted
    /// all by a participant however long its messages take: its hits are charged as of when
    /// it admitted them, and its copies taken as of when it sent them.
    #[test]
    fn what_a_plain_bucket_admits_a_participant_admits_over_round_trips(
        limit in 1u64..200, units in 16u64..800, burst in 1u64..12,
        gaps in prop::collection::vec(prop_oneof![2 => Just(1u64), 1 => 0u64..3, 1 => 0u64..40], 1..80),
        jitter in prop::collection::vec(0u64..1_000_000, 80),
        there in 0u64..5_000_000, back in 0u64..5_000_000,
    ) {
        use crate::reference::Model as _;
        // A whole number of nanoseconds a unit, so that a request at the edge is at it in
        // units too.
        let interval = units * 65_536;
        let n = |n| NonZeroU64::new(n).unwrap();
        let c = Contract { limit: n(limit), window: n(limit * interval), burst: n(burst) };
        // Gaps of whole intervals, exactly at the edge, some a little more.
        let mut plain = c.bucket();
        let mut times = Vec::new();
        let mut now = 0;
        for (gap, jitter) in gaps.iter().zip(&jitter) {
            now += gap * interval + if *gap > 2 { *jitter } else { 0 };
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
        prop_assert!(refused.is_empty(), "refused {:?} of {:?}", refused, times);
    }
}

/// `entry` as it was `before` a hold taken and given back at 0: its bucket as full at 0, but
/// full from then rather than from long before, as the reference's is left.
fn given_back_as_it_was(entry: &Entry, before: Fields) {
    let after = entry.snapshot();
    assert_eq!(
        Fields {
            arrival: before.arrival,
            ..after
        },
        before,
        "given back as it was"
    );
    let bucket = &entry.terms.bucket;
    assert_eq!(
        bucket.remaining(after.arrival, bucket.units(0)),
        bucket.remaining(before.arrival, bucket.units(0)),
        "as much room"
    );
}

/// A request under two keys is held on both or neither: the second without room gives
/// the first back as it was.
#[test]
fn a_two_key_request_is_charged_on_both_or_neither() {
    let roomy = Entry::new(b"a"[..].into(), contract(100, 10), CYCLE, 0);
    let empty = Entry::new(b"b"[..].into(), contract(100, 1), CYCLE, 0);
    assert_eq!(decide(&[&empty], 0).local, Local::Admit);
    let before = roomy.snapshot();
    assert_eq!(decide(&[&roomy, &empty], 0).local, Local::Refuse);
    given_back_as_it_was(&roomy, before);
    let fresh = Entry::new(b"c"[..].into(), contract(100, 1), CYCLE, 0);
    assert_eq!(decide(&[&roomy, &fresh], 0).local, Local::Admit);
    let after = roomy.snapshot();
    assert_eq!((after.held, after.unreported), (0, 1));
}

/// An entry closed for eviction refuses nothing and admits nothing more: a worker holding
/// it asks, and the key's new entry is a fresh one. One with admissions not yet reported is
/// not closed.
#[test]
fn an_evicted_entry_sends_its_holders_to_the_new_one() {
    // No service pods: a key has no owner and is decided here, warm from the start.
    let mut participant = Participant::new(
        7,
        CYCLE,
        View {
            epoch: 1,
            pods: Vec::new(),
        },
        FixedState::default(),
    )
    .unwrap();
    let key: &[u8] = b"k";
    let c = contract(100, 10);
    participant.ensure(key, c, 0);
    let old = participant.entries(&[(key, c)]).remove(0);
    assert!(old.take(Op::Admit, 0).0.done());
    assert!(
        !close_if_quiet(&participant.keys[key]),
        "an admission not drained"
    );
    // Drained into the ledger: nothing left in the word to lose.
    participant.drain(key, 0);
    assert!(close_if_quiet(&participant.keys[key]));
    assert_eq!(decide(&[&old], 0).local, Local::Ask);
    participant.keys.remove(key);
    participant.ensure(key, c, 0);
    let new = participant.entries(&[(key, c)]).remove(0);
    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(decide(&[&new], 0).local, Local::Admit);
}

/// Workers left alone through several cycles — the transport stalled — advance each key's
/// cycle themselves, once, with one cycle's refill: a long pause adds no burst.
#[test]
fn a_stalled_transport_leaves_workers_advancing_cycles_with_one_refill() {
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 20), CYCLE, 0);
    entry.others.store(1, Ordering::Release);
    while decide(&[&entry], 0).local == Local::Admit {}
    // Five cycles later: one advance, sharing out what the bucket refilled (capped at the
    // bucket) and one cycle's refill, among two — not five refills.
    let now = 5 * CYCLE;
    let fields = word::advanced(&entry.terms.bucket, entry.snapshot(), entry.advance(), now);
    let sees = entry.terms.bucket.remaining_within(
        fields.arrival,
        entry.terms.bucket.units(now),
        entry.terms.refill_units,
    );
    // No cycle had set an allowance before: nothing is carried.
    assert_eq!(
        fields.left,
        (sees / 2).max(1),
        "one share of what is left, one cycle's refill"
    );
    let admitted = (0..100)
        .filter(|_| decide(&[&entry], now).local == Local::Admit)
        .count() as u64;
    let remaining = entry
        .terms
        .bucket
        .remaining(fields.arrival, entry.terms.bucket.units(now));
    assert_eq!(admitted, fields.left.min(remaining));
}

/// A worker meeting a key the pod already holds finds its entry in the registry, the
/// transport not asked — so a transport held up holds up no warm request — and an entry
/// evicted or replaced is no longer found.
#[test]
fn a_worker_finds_the_pods_entry_without_asking() {
    let mut participant = Participant::new(
        7,
        CYCLE,
        View {
            epoch: 1,
            pods: Vec::new(),
        },
        FixedState::default(),
    )
    .unwrap();
    let registry = participant.registry();
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 10));
    assert!(registry.find(key, c).is_none(), "nothing made yet");
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    let found = registry.find(key, c).unwrap();
    assert!(Arc::ptr_eq(&found, &participant.entries(&[(key, c)])[0]));
    assert_eq!(
        decide(&[&found], 0).local,
        Local::Admit,
        "decided from it alone"
    );
    assert!(
        registry.find(key, contract(100, 11)).is_none(),
        "another contract"
    );
    // Replaced by a changed contract: the old one is closed and gone from the registry.
    participant.ensure(key, contract(50, 10), 0);
    assert!(registry.find(key, c).is_none());
    assert!(registry.find(key, contract(50, 10)).is_some());
    // Evicted, idle: no longer found.
    let later = 100 * 1_000 * MS;
    participant.on_cycle(later);
    assert!(registry.find(key, contract(50, 10)).is_none(), "evicted");
}

/// A key's bytes count a handle to its entry for each worker that may hold one, so that a
/// process's cap bounds its workers' handles too; and the registry's and workers' handles
/// find an entry by its key without a copy of it.
#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "a handle hashes and compares by its entry's key, which never changes"
)]
fn a_keys_bytes_count_every_workers_handle() {
    let holding = |workers| {
        let mut participant = Participant::new(
            7,
            CYCLE,
            View {
                epoch: 1,
                pods: Vec::new(),
            },
            FixedState::default(),
        )
        .unwrap();
        participant.set_workers(workers);
        participant.ensure(b"k", contract(100, 10), 0);
        participant.holding()
    };
    let handle = (size_of::<Handle>() + 1) as u64;
    assert_eq!(holding(8) - holding(1), 7 * handle);
    assert_eq!(size_of::<Handle>(), size_of::<usize>(), "a pointer, no key");
    let mut set = HashSet::new();
    let made = Arc::new(Entry::new(b"key"[..].into(), contract(100, 10), CYCLE, 0));
    set.insert(Handle::new(Arc::clone(&made)));
    assert!(Arc::ptr_eq(set.get(&b"key"[..]).unwrap().entry(), &made));
    assert!(!set.contains(&b"kez"[..]));
}

/// With one exchange in flight to a pod, nothing more goes to it until that one is
/// answered: what is admitted meanwhile gathers in the ledger and goes as one report, and
/// the key is not polled while it waits. An exchange never answered is taken as failed once
/// its time has passed.
#[test]
fn one_exchange_in_flight_gathers_what_comes_meanwhile() {
    let mut participant = participant();
    participant.set_one_in_flight(true);
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view());
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 100));
    // A new key, made and decided here.
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    let entry = participant.entries(&[(key, c)]).remove(0);
    // One admitted, sent with a cycle, and not answered.
    assert!(entry.take(Op::Admit, CYCLE).0.done());
    participant.on_cycle(CYCLE);
    let first = participant.take_outgoing();
    assert_eq!(first.len(), 1, "the report");
    // Three cycles more, an admission each: nothing goes while the first is out.
    for cycle in 2..5 {
        assert!(entry.take(Op::Admit, cycle * CYCLE).0.done());
        participant.on_cycle(cycle * CYCLE);
        assert!(participant.take_outgoing().is_empty(), "cycle {cycle}");
    }
    // Answered: the next cycle sends the three as one report.
    for (_, batch) in first {
        participant.on_answers(pod.on_batch(batch, 4 * CYCLE), 4 * CYCLE);
    }
    participant.on_cycle(5 * CYCLE);
    let next = participant.take_outgoing();
    let hits: Vec<u64> = next
        .iter()
        .flat_map(|(_, batch)| &batch.entries)
        .filter_map(|e| match e.report {
            Report::Hits { count, .. } => Some(count),
            _ => None,
        })
        .collect();
    assert_eq!(hits, [3]);
    // Never answered: after the exchange's time, taken as failed, and the next goes.
    assert!(entry.take(Op::Admit, 6 * CYCLE).0.done());
    let later = 5 * CYCLE + RESEND_AFTER_CYCLES * CYCLE;
    participant.on_cycle(later);
    assert!(
        !participant.take_outgoing().is_empty(),
        "after the exchange's time"
    );
}

/// One request every 10 s, a bucket of 10: nothing refills within a test.
fn slow() -> Contract {
    Contract {
        limit: NonZeroU64::new(1).unwrap(),
        window: NonZeroU64::new(10_000 * MS).unwrap(),
        burst: NonZeroU64::new(10).unwrap(),
    }
}

/// A participant with one exchange in flight, quiet keys not polled.
fn one_in_flight() -> Participant<FixedState> {
    let mut participant = participant();
    participant.set_one_in_flight(true);
    participant.set_levers(Levers { poll_quiet: false });
    participant
}

/// Another data plane's `count` hits on `key`, its first report.
fn others_hits(key: &[u8], c: Contract, count: u64) -> Batch {
    Batch {
        from: 8,
        entries: vec![BatchEntry {
            key: key.into(),
            contract: c,
            report: Report::Hits {
                sequence: 1,
                count,
                seen: 0,
                age: 0,
            },
        }],
        made: 0,
    }
}

/// Service pod 0 in its life `life`.
fn pod(life: u64) -> ServicePod {
    let mut pod = ServicePod::new(0, CYCLE, view());
    pod.set_life(life);
    pod
}

/// The one exchange `participant` starts: its number and batch.
fn only_exchange(participant: &mut Participant<FixedState>) -> (u64, Batch) {
    let mut started = participant.take_exchanges();
    assert_eq!(started.len(), 1, "one exchange");
    let (_, exchange, batch) = started.remove(0);
    (exchange, batch)
}

/// An exchange given up and its pod's answer to it arriving after the next one's: the
/// pod handled the next one later, so its bucket, numbered higher, is the newer, and the
/// late answer's is not taken over it. Here the late one would have the bucket nearly full
/// where another data plane has emptied it since.
#[test]
fn a_late_answer_does_not_put_back_an_older_bucket() {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let older = pod.on_batch(batch, CYCLE);
    // Unanswered for the exchange's time: given up, its report sent again in the next.
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    assert!(next > first);
    let _ = pod.on_batch(others_hits(key, c, 9), 5 * CYCLE);
    let newer = pod.on_batch(batch, 5 * CYCLE);
    participant.on_answers_to(next, newer, 6 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse
    );
    participant.on_answers_to(first, older, 6 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse,
        "the older bucket put back"
    );
    assert_eq!(participant.late_answers(), 1);
}

/// A late answer, or a late failure, of an exchange given up does not end the one that
/// followed it: that one is still in flight, and nothing more goes to the pod until it is
/// over.
#[test]
fn a_late_answer_leaves_the_next_exchange_in_flight() {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let late = pod.on_batch(batch, CYCLE);
    participant.on_cycle(5 * CYCLE);
    let (next, _) = only_exchange(&mut participant);
    participant.on_answers_to(first, late, 6 * CYCLE);
    participant.exchanged(0, first);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Admit
    );
    participant.on_cycle(7 * CYCLE);
    assert!(
        participant.take_exchanges().is_empty(),
        "the next is still in flight"
    );
    participant.exchanged(0, next);
    participant.on_cycle(8 * CYCLE);
    assert_eq!(only_exchange(&mut participant).0, next + 1);
}

/// A late answer's buckets are not taken, but its acknowledgements are: the reports it
/// says the pod applied are not sent again, though the later answer taken carried none of
/// the key's.
#[test]
fn a_late_answer_still_acknowledges() {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let late = pod.on_batch(batch, CYCLE);
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    assert_eq!(batch.entries.len(), 1, "the report sent again");
    participant.on_answers_to(
        next,
        Answers {
            from: 0,
            entries: Vec::new(),
            view: None,
            life: 1,
            number: late.number + 1,
            made: 5 * CYCLE,
        },
        6 * CYCLE,
    );
    participant.on_answers_to(first, late, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 1);
    participant.on_cycle(10 * CYCLE);
    let again: Vec<Batch> = participant
        .take_outgoing()
        .into_iter()
        .map(|(_, batch)| batch)
        .collect();
    assert!(
        again.iter().all(|batch| batch.entries.is_empty()),
        "the acknowledged report sent again: {again:?}"
    );
}

/// An exchange given up with nothing started after it: its answer is still the latest
/// the pod gave, and its buckets are taken.
#[test]
fn the_answer_to_the_latest_exchange_is_taken_though_given_up() {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let _ = pod.on_batch(others_hits(key, c, 9), CYCLE);
    let answer = pod.on_batch(batch, CYCLE);
    participant.on_cycle(5 * CYCLE);
    participant.exchanged(0, first);
    participant.on_answers_to(first, answer, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 0);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse
    );
}

/// An exchange given up whose batch the pod handled after the next one's: its answer,
/// though late, is the newer, and its buckets are taken.
#[test]
fn a_late_answer_the_pod_handled_later_is_taken() {
    let mut participant = one_in_flight();
    let mut pod = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, first_batch) = only_exchange(&mut participant);
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    let earlier = pod.on_batch(batch, 5 * CYCLE);
    let _ = pod.on_batch(others_hits(key, c, 9), 5 * CYCLE);
    let later = pod.on_batch(first_batch, 5 * CYCLE);
    participant.on_answers_to(next, earlier, 6 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Admit
    );
    participant.on_answers_to(first, later, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 0);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse,
        "the newer bucket not taken"
    );
}

/// A restarted pod numbers its answers from the start again: its first, in a new life and
/// to a later exchange, is taken though numbered lower than the last taken.
#[test]
fn a_restarted_pods_answers_are_taken() {
    let mut participant = one_in_flight();
    let mut before = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    for _ in 0..3 {
        let _ = before.on_batch(others_hits(b"other", c, 1), 0);
    }
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let answer = before.on_batch(batch, CYCLE);
    assert!(answer.number > 1);
    participant.on_answers_to(first, answer, CYCLE);
    // Restarted, it has met another data plane emptying the bucket.
    let mut after = pod(2);
    let _ = after.on_batch(others_hits(key, c, 10), 2 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 2 * CYCLE),
        Decision::Admit
    );
    participant.on_cycle(3 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    let answer = after.on_batch(batch, 3 * CYCLE);
    assert!(answer.number <= 2);
    participant.on_answers_to(next, answer, 3 * CYCLE);
    assert_eq!(participant.late_answers(), 0);
    assert_eq!(
        participant.decide_all(&[(key, c)], 3 * CYCLE),
        Decision::Refuse,
        "the restarted pod's bucket not taken"
    );
}

/// After a restart, an answer from the life before arriving late is not taken as another
/// restart: it answers an exchange started before the one the new life answered.
#[test]
fn an_earlier_lifes_late_answer_is_not_taken() {
    let mut participant = one_in_flight();
    let mut before = pod(1);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let old = before.on_batch(batch, CYCLE);
    // Unanswered for an ask's time: given up, and the next goes to the pod restarted, which
    // has met another data plane emptying the bucket.
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    let mut after = pod(2);
    let _ = after.on_batch(others_hits(key, c, 9), 5 * CYCLE);
    let new = after.on_batch(batch, 5 * CYCLE);
    participant.on_answers_to(next, new, 6 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse
    );
    participant.on_answers_to(first, old, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 1);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse,
        "the earlier life's bucket put back"
    );
}

/// After a restart, an earlier life's answer to an exchange started later than the one the
/// new life answered is not taken either: the two may reach the pod in either order, and
/// every exchange started before the new life was first heard from is fenced off.
#[test]
fn an_earlier_lifes_answer_to_a_later_exchange_is_not_taken() {
    let mut participant = one_in_flight();
    let mut before = pod(1);
    let mut after = pod(2);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    participant.on_cycle(CYCLE);
    let (first, delayed) = only_exchange(&mut participant);
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    // The later exchange reaches the life before; the earlier one, held up, the life after,
    // which has met another data plane emptying the bucket.
    let old = before.on_batch(batch, 5 * CYCLE);
    let _ = after.on_batch(others_hits(key, c, 9), 6 * CYCLE);
    let new = after.on_batch(delayed, 6 * CYCLE);
    participant.on_answers_to(first, new, 6 * CYCLE);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse
    );
    participant.on_answers_to(next, old, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 1);
    assert_eq!(
        participant.decide_all(&[(key, c)], 6 * CYCLE),
        Decision::Refuse,
        "the earlier life's bucket put back"
    );
}

/// An earlier life's answer passed over acknowledges nothing: the reports it applied go
/// again to the life serving, which may not hold them. Its own copy holding them, it would
/// ignore them.
#[test]
fn an_earlier_lifes_late_answer_acknowledges_nothing() {
    let mut participant = one_in_flight();
    participant.set_levers(Levers { poll_quiet: true });
    let mut before = pod(1);
    let mut after = pod(2);
    let (key, c): (&[u8], Contract) = (b"k", slow());
    for _ in 0..10 {
        assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    }
    participant.on_cycle(CYCLE);
    let (first, batch) = only_exchange(&mut participant);
    let old = before.on_batch(batch, CYCLE);
    // Given up; the next reaches the life after while it joins, answering nothing.
    participant.on_cycle(5 * CYCLE);
    let (next, batch) = only_exchange(&mut participant);
    after.set_serving(false);
    let joining = after.on_batch(batch, 5 * CYCLE);
    assert!(joining.entries.is_empty());
    participant.on_answers_to(next, joining, 5 * CYCLE);
    participant.on_answers_to(first, old, 6 * CYCLE);
    assert_eq!(participant.late_answers(), 1);
    after.set_serving(true);
    participant.on_cycle(10 * CYCLE);
    let (third, batch) = only_exchange(&mut participant);
    let new = after.on_batch(batch, 10 * CYCLE);
    participant.on_answers_to(third, new, 10 * CYCLE);
    assert_eq!(after.counted(key), Some(10), "the reports not sent again");
    assert_eq!(
        participant.decide_all(&[(key, c)], 10 * CYCLE),
        Decision::Refuse
    );
}

/// Answer and exchange numbers wrap: one counted past the top is still the later.
#[test]
fn later_holds_across_the_wrap() {
    assert!(later(0, u64::MAX));
    assert!(later(u64::MAX, u64::MAX - 1));
    assert!(!later(u64::MAX, 0));
    assert!(!later(5, 5));
}

/// A new key's request is decided at once from a full bucket and nothing is sent for it
/// until the cycle, which reports its admissions as any key's.
#[test]
fn a_cold_key_is_decided_locally_and_reported_with_the_cycle() {
    let mut participant = participant();
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 3));
    for _ in 0..3 {
        assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    }
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Refuse);
    assert!(participant.take_outgoing().is_empty());
    participant.on_cycle(CYCLE);
    let reported: u64 = participant
        .take_outgoing()
        .iter()
        .flat_map(|(_, batch)| &batch.entries)
        .filter_map(|e| match e.report {
            Report::Hits { count, .. } => Some(count),
            _ => None,
        })
        .sum();
    assert_eq!(reported, 3);
}

/// A worker makes a key's entry itself, once however many meet it; the participant adopts
/// it, reports what it admitted, and keeps it as the key's.
#[test]
fn a_worker_makes_a_local_key_and_the_participant_adopts_it() {
    let mut participant = participant();
    let registry = participant.registry();
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 10));
    let (made, fresh) = registry.find_or_make(key, c, CYCLE, 0).unwrap();
    assert!(fresh);
    let (again, fresh) = registry.find_or_make(key, c, CYCLE, 0).unwrap();
    assert!(!fresh && Arc::ptr_eq(&made, &again), "one entry");
    assert!(
        registry
            .find_or_make(key, contract(100, 11), CYCLE, 0)
            .is_none(),
        "another contract"
    );
    assert_eq!(decide(&[&made], 0).local, Local::Admit, "decided at once");
    participant.adopt(Arc::clone(&made));
    participant.adopt(Arc::clone(&made));
    assert_eq!(participant.key_count(), 1);
    assert!(Arc::ptr_eq(&participant.entries(&[(key, c)])[0], &made));
    // The participant meeting the key itself keeps the worker's entry.
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    assert!(Arc::ptr_eq(&participant.entries(&[(key, c)])[0], &made));
    participant.on_cycle(CYCLE);
    let reported: u64 = participant
        .take_outgoing()
        .iter()
        .flat_map(|(_, batch)| &batch.entries)
        .filter_map(|e| match e.report {
            Report::Hits { count, .. } => Some(count),
            _ => None,
        })
        .sum();
    assert_eq!(reported, 2, "the worker's and the participant's");
}

/// With quiet keys not polled, a key in use with nothing to report sends nothing with the
/// cycle; with them polled, a poll.
#[test]
fn quiet_keys_are_polled_only_when_asked_to_be() {
    for poll_quiet in [true, false] {
        let mut participant = participant();
        participant.set_levers(Levers { poll_quiet });
        let mut pod: ServicePod = ServicePod::new(0, CYCLE, view());
        let (key, c): (&[u8], Contract) = (b"k", contract(100, 100));
        assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
        // Its admission reported and answered; then a cycle with nothing to report.
        participant.on_cycle(CYCLE);
        for (_, batch) in participant.take_outgoing() {
            participant.on_answers(pod.on_batch(batch, CYCLE), CYCLE);
        }
        participant.on_cycle(2 * CYCLE);
        let polls = participant
            .take_outgoing()
            .iter()
            .flat_map(|(_, batch)| &batch.entries)
            .filter(|e| matches!(e.report, Report::Poll))
            .count();
        assert_eq!(polls, usize::from(poll_quiet), "poll_quiet {poll_quiet}");
    }
}

/// A request whose clock read a moment before another's, and whose compare-and-swap comes
/// after, finds the cycle the other advanced the key to: it does not move it back, and gets
/// no allowance from doing so, nor does the next request from moving it on again.
#[test]
fn an_earlier_clock_does_not_move_a_cycle_back() {
    // No pods: the key decided here, nothing asked.
    let view = View {
        epoch: 1,
        pods: Vec::new(),
    };
    let mut participant = Participant::new(7, CYCLE, view, FixedState::default()).unwrap();
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 10));
    participant.ensure(key, c, 0);
    let entry = participant.entries(&[(key, c)]).remove(0);
    // Nine others: a share of one a cycle.
    entry.others.store(9, Ordering::Release);
    let at = 5 * CYCLE;
    assert_eq!(decide(&[&entry], at).local, Local::Admit);
    assert_eq!(
        decide(&[&entry], at).local,
        Local::Refuse,
        "the share spent"
    );
    assert_eq!(
        decide(&[&entry], at - 1).local,
        Local::Refuse,
        "a clock behind"
    );
    assert_eq!(
        decide(&[&entry], at).local,
        Local::Refuse,
        "and the cycle as it was"
    );
}

/// The cycle number is kept modulo 2^16: a key untouched for exactly that many cycles has
/// the number it had. Its record tells the gap, so it gets a new cycle's share, whether the
/// transport advanced it every cycle or nothing did — the process paused, or the key off
/// the transport's schedule.
#[test]
fn a_key_untouched_for_the_cycle_numbers_range_gets_a_new_share() {
    let wrap = (1u64 << 16) * CYCLE;
    for transport in [true, false] {
        let mut participant = Participant::new(
            7,
            CYCLE,
            View {
                epoch: 1,
                pods: Vec::new(),
            },
            FixedState::default(),
        )
        .unwrap();
        let (key, c): (&[u8], Contract) = (b"k", contract(100, 10));
        participant.ensure(key, c, 0);
        let entry = participant.entries(&[(key, c)]).remove(0);
        // Nine others: a share of one a cycle, spent at once.
        entry.others.store(9, Ordering::Release);
        assert_eq!(decide(&[&entry], CYCLE).local, Local::Admit);
        assert_eq!(
            decide(&[&entry], CYCLE).local,
            Local::Refuse,
            "the share spent"
        );
        if transport {
            for cycle in 2..=(1u64 << 16) + 1 {
                participant.on_cycle(cycle * CYCLE);
            }
        }
        let admitted = (0..10)
            .take_while(|_| decide(&[&entry], wrap + CYCLE).local == Local::Admit)
            .count();
        // A share of one; advanced every cycle, one more carried from the cycle before,
        // which left its place unspent.
        assert_eq!(
            admitted,
            if transport { 2 } else { 1 },
            "transport {transport}"
        );
    }
}

/// The participant finds a key by a handle to its entry, holding no copy of its bytes; a
/// key whose contract changed is found by its new entry, not the closed one.
#[test]
fn the_participant_keeps_a_key_once() {
    let mut participant = participant();
    let key: &[u8] = b"k";
    participant.ensure(key, contract(100, 10), 0);
    for c in [contract(100, 10), contract(100, 11)] {
        participant.ensure(key, c, 0);
        let (handle, tracked) = participant.keys.get_key_value(key).unwrap();
        assert!(Arc::ptr_eq(handle.entry(), &tracked.entry));
        assert_eq!(tracked.entry.terms.contract, c);
    }
    assert_eq!(participant.key_count(), 1);
}

/// What keys under one contract share is made once and pointed to, not copied into each
/// key's entry: an entry is its key, a pointer, its word and its counts.
#[test]
fn keys_under_one_contract_share_their_terms() {
    let registry = Registry::new(FixedState::default());
    let c = contract(100, 10);
    let (a, _) = registry.find_or_make(b"a", c, CYCLE, 0).unwrap();
    let (b, _) = registry.find_or_make(b"b", c, CYCLE, 0).unwrap();
    assert!(Arc::ptr_eq(&a.terms, &b.terms));
    let (other, _) = registry
        .find_or_make(b"c", contract(100, 11), CYCLE, 0)
        .unwrap();
    assert!(!Arc::ptr_eq(&a.terms, &other.terms));
    assert!(size_of::<Entry>() <= 128, "{}", size_of::<Entry>());
}

/// A client choosing its keys cannot crowd one of the registry's shards: keys found to share
/// a shard under one process's hash are spread over the shards under another's, as keys
/// would be by chance. A shard's table grows by rehashing all of it under its lock, so one
/// holding all the process's keys would hold a worker up for as long as they take.
#[test]
fn keys_chosen_to_share_a_shard_spread_under_another_processs_hash() {
    let known = Registry::new(Keyed::new([1, 2]));
    let unknown = Registry::new(Keyed::new([3, 4]));
    let crowded: Vec<Vec<u8>> = (0_u64..)
        .map(|k| format!("api/0\0client-{k}").into_bytes())
        .filter(|key| known.index(key) == 0)
        .take(200)
        .collect();
    let shards: HashSet<usize> = crowded.iter().map(|key| unknown.index(key)).collect();
    // 200 keys over 4,096 shards land on some 195 by chance.
    assert!(shards.len() > 180, "{} shards", shards.len());
}

/// A key's slot in the participant's table takes 80 bytes, the table holding up to twice
/// the room its keys take: the batches it has not seen acknowledged, which between cycles
/// it almost never has, kept apart behind a pointer, and when its copy was last heard of
/// in eight bytes.
#[test]
fn a_keys_slot_is_small() {
    assert_eq!(size_of::<(Handle, Tracked)>(), 80);
}

/// When a key's copy was last heard of, kept in eight bytes, reads back as it was set:
/// never, time 0 among the times.
#[test]
fn when_a_copy_was_heard_of_reads_back() {
    assert_eq!(Heard::default().get(), None);
    for at in [0, 1, 5 * MS] {
        assert_eq!(Heard::at(at).get(), Some(at));
    }
}

/// A batch numbered `sequence`, its count telling it apart.
fn unseen(sequence: u64) -> Unseen {
    Unseen {
        sequence,
        count: sequence,
        admitted: sequence,
        first: sequence,
        last: sequence,
    }
}

proptest! {
    /// Keys' unacknowledged batches behave as lists of them, sent in order and let go of as
    /// they are seen applied; a key holds no list once there are none, and the lists given
    /// back are kept empty, with room for one batch, for keys to take.
    #[test]
    fn unacknowledged_batches_are_lists_given_back_when_empty(
        steps in proptest::collection::vec((0usize..3, proptest::option::of(0u64..4)), 0..96)
    ) {
        let mut spare = Spare::default();
        let mut lists: [Unacknowledged; 3] = Default::default();
        let mut models: [VecDeque<Unseen>; 3] = Default::default();
        let (mut sent, mut seen) = ([0; 3], [0; 3]);
        for (key, step) in steps {
            let (list, model) = (&mut lists[key], &mut models[key]);
            match step {
                None => {
                    sent[key] += 1;
                    list.push(unseen(sent[key]), &mut spare);
                    model.push_back(unseen(sent[key]));
                }
                Some(more) => {
                    seen[key] = (seen[key] + more).min(sent[key]);
                    list.acknowledge(seen[key], &mut spare);
                    model.retain(|unseen| unseen.sequence > seen[key]);
                }
            }
            let held: Vec<(u64, u64)> = list
                .as_slice()
                .iter()
                .map(|unseen| (unseen.sequence, unseen.count))
                .collect();
            let want: Vec<(u64, u64)> = model
                .iter()
                .map(|unseen| (unseen.sequence, unseen.count))
                .collect();
            prop_assert_eq!(held, want);
            prop_assert_eq!(list.is_empty(), model.is_empty());
            prop_assert_eq!(list.0.is_none(), model.is_empty(), "no list kept for none");
            prop_assert!(
                spare.0.iter().all(|list| list.is_empty() && list.capacity() == 1),
                "kept empty, room for one"
            );
        }
    }
}

/// Lists given back are kept up to `SPARE_LISTS`, and only those with room for one batch:
/// one grown while a key's batches went unacknowledged goes back to the allocator.
#[test]
fn spare_lists_are_kept_up_to_a_bound_with_room_for_one() {
    let mut spare = Spare::default();
    let mut grown = Unacknowledged::default();
    for sequence in 1..=3 {
        grown.push(unseen(sequence), &mut spare);
    }
    grown.acknowledge(3, &mut spare);
    assert!(grown.is_empty());
    assert_eq!(spare.0.len(), 0, "room for more than one: not kept");
    for _ in 0..SPARE_LISTS + 5 {
        let mut list = Unacknowledged::default();
        list.push(unseen(1), &mut spare);
        list.acknowledge(1, &mut spare);
    }
    assert_eq!(spare.0.len(), 1, "the one list taken back each time");
    let mut lists: Vec<Unacknowledged> = (0..SPARE_LISTS + 5)
        .map(|_| Unacknowledged::default())
        .collect();
    for list in &mut lists {
        list.push(unseen(1), &mut spare);
    }
    assert_eq!(spare.0.len(), 0, "taken before any is made");
    for list in &mut lists {
        list.acknowledge(1, &mut spare);
    }
    assert_eq!(spare.0.len(), SPARE_LISTS, "no more kept");
}

#[test]
fn the_platform_has_native_128_bit_atomics() {
    assert!(supported(), "the participant would not start here");
}

/// Runs `schedule` on both sides, printing what each decided and every key's room after
/// each event: for explaining a failure of the comparison above.
fn replay(schedule: &[(Event, u64)]) {
    use crate::reference::Model as _;
    let keys: [(&[u8], Contract); 3] = [
        (b"a", contract(100, 10)),
        (b"b", contract(10, 3)),
        (b"c", contract(1_000, 50)),
    ];
    let mut pooled = Pooled::new();
    let mut reference = Reference::new();
    let mut now = 0;
    let mut next_cycle = CYCLE;
    for (at, (event, step)) in schedule.iter().enumerate() {
        now += step * 40_000 * 25;
        while now >= next_cycle {
            pooled.participant.on_cycle(next_cycle);
            reference.plane.on_cycle(next_cycle);
            next_cycle += CYCLE;
        }
        let decided = match event {
            Event::Request(key) => {
                let request = [keys[*key]];
                format!(
                    "{:?} {:?}",
                    pooled.decide(&request, now),
                    reference.plane.decide_all(&request, now)
                )
            }
            Event::Pair(first, second) => {
                let request = [keys[*first], keys[*second]];
                format!(
                    "{:?} {:?}",
                    pooled.decide(&request, now),
                    reference.plane.decide_all(&request, now)
                )
            }
            Event::Exchange => format!("{:?} {:?}", pooled.exchange(now), reference.exchange(now)),
            Event::Send => {
                pooled.send(now);
                reference.send(now);
                String::new()
            }
            Event::Deliver(index, twice) => format!(
                "{:?} {:?}",
                pooled.deliver(*index, *twice, now),
                reference.deliver(*index, *twice, now)
            ),
            Event::Cycle => String::new(),
        };
        let rooms: Vec<String> = keys
            .iter()
            .map(|(key, contract)| {
                // What the reference's copy would admit: what is left of its bucket and of
                // its share of the cycle.
                let want = reference
                    .plane
                    .keys
                    .get(*key)
                    .filter(|at| at.contract == *contract)
                    .map(|at| {
                        let left = at.bucket.remaining(now);
                        at.allowance.map_or(left, |allowance| {
                            left.min(allowance.saturating_sub(at.spent))
                        })
                    });
                let got = pooled.participant.keys.get(*key).map(|tracked| {
                    let entry = &tracked.entry;
                    let fields =
                        word::advanced(&entry.terms.bucket, entry.snapshot(), entry.advance(), now);
                    let left = entry
                        .terms
                        .bucket
                        .remaining(fields.arrival, entry.terms.bucket.units(now));
                    (fields.left, left, fields.held, tracked.heard.get())
                });
                format!("{want:?}/{got:?}")
            })
            .collect();
        eprintln!("{at:2} {now:>9} {event:?}: {decided}  {}", rooms.join("  "));
        if std::env::var("REPLAY_KEY").is_ok() {
            let key: &[u8] = b"b";
            let want = reference.plane.keys.get(key).map(|at| {
                (
                    at.last_request,
                    at.heard,
                    at.unacknowledged.len(),
                    at.unsent,
                    at.sent,
                )
            });
            let got = pooled.participant.keys.get(key).map(|t| {
                (
                    t.entry.last_request.load(Ordering::Relaxed),
                    t.heard.get(),
                    t.unacknowledged.as_slice().len(),
                    t.entry.snapshot().unreported + t.unsent,
                    t.sent,
                )
            });
            eprintln!("     b: reference {want:?}  pooled {got:?}");
        }
    }
}

#[test]
#[ignore = "a replay for debugging"]
fn replay_a_failure() {
    use Event::{Cycle, Deliver, Exchange, Pair, Request, Send};
    let r = Request;
    let d = |at| Deliver(at, false);
    replay(&[
        (r(1), 3),
        (r(0), 1),
        (r(0), 2),
        (d(0), 2),
        (r(1), 3),
        (r(0), 1),
        (r(0), 2),
        (r(0), 3),
        (r(1), 1),
        (d(0), 1),
        (r(2), 3),
        (Send, 3),
        (d(0), 3),
        (r(1), 1),
        (Exchange, 1),
        (Cycle, 3),
        (Pair(2, 0), 1),
        (Cycle, 1),
        (Pair(0, 2), 1),
        (Pair(0, 2), 1),
        (Pair(2, 0), 3),
        (d(0), 1),
        (d(0), 2),
        (Cycle, 3),
        (d(0), 3),
        (d(0), 2),
        (Send, 3),
        (r(0), 1),
        (r(0), 1),
        (r(2), 1),
        (Pair(2, 0), 2),
        (Exchange, 1),
        (r(1), 1),
    ]);
}

/// A scheduled participant, nothing polled: what each test of scheduling starts from, with `keys` keys made at 0 and their first cycle's visit done.
fn scheduled(keys: u64) -> (Participant<FixedState>, Vec<Arc<Entry>>) {
    scheduled_under(keys, contract(100, 10))
}

/// As [`scheduled`], the keys under `c`.
fn scheduled_under(keys: u64, c: Contract) -> (Participant<FixedState>, Vec<Arc<Entry>>) {
    let mut participant = participant();
    participant.set_scheduled(true);
    participant.set_levers(Levers { poll_quiet: false });
    let names: Vec<Vec<u8>> = (0..keys).map(|n| format!("k{n}").into_bytes()).collect();
    for name in &names {
        participant.ensure(name, c, 0);
    }
    participant.on_cycle(CYCLE);
    let _ = participant.take_outgoing();
    let entries = names
        .iter()
        .map(|name| participant.entries(&[(name, c)]).remove(0))
        .collect();
    (participant, entries)
}

/// The hits each key was reported with in `batches`, by key.
fn hits(batches: &[(PodId, Batch)]) -> HashMap<Box<[u8]>, u64> {
    let mut hits = HashMap::new();
    for (_, batch) in batches {
        for entry in &batch.entries {
            if let Report::Hits { count, .. } = entry.report {
                *hits.entry(entry.key.clone()).or_default() += count;
            }
        }
    }
    hits
}

/// Scheduled, a cycle visits the keys that need it and no other: none while all are quiet;
/// a key a worker signalled, once, and its admission reported. A second admission before
/// the count is drained signals nothing; one after it signals again.
#[test]
fn a_scheduled_participant_visits_only_the_keys_that_need_it() {
    let (mut participant, entries) = scheduled(100);
    let quiet = participant.visits();
    participant.on_cycle(2 * CYCLE);
    assert_eq!(participant.visits(), quiet, "nothing to do");
    let key = &entries[5];
    let outcome = decide(&[key], 2 * CYCLE + 1);
    assert_eq!((outcome.local, outcome.signal), (Local::Admit, 1));
    assert_eq!(
        decide(&[key], 2 * CYCLE + 2).signal,
        0,
        "the count started already"
    );
    participant.on_signals([Arc::clone(key)]);
    participant.on_cycle(3 * CYCLE);
    assert_eq!(participant.visits(), quiet + 1, "the one signalled");
    let sent = hits(&participant.take_outgoing());
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[key.key()], 2);
    assert_eq!(
        decide(&[key], 3 * CYCLE + 1).signal,
        1,
        "drained, it starts again"
    );
}

/// A count no worker could signal, its queue full, is found by the scan it asked for:
/// the next cycle visits every key, and the one after only those that need it.
#[test]
fn a_required_scan_finds_a_count_no_signal_named() {
    let (mut participant, entries) = scheduled(50);
    assert_eq!(decide(&[&entries[7]], 2 * CYCLE).signal, 1);
    participant.on_cycle(2 * CYCLE + 1);
    assert!(
        hits(&participant.take_outgoing()).is_empty(),
        "not signalled"
    );
    participant.registry().require_scan();
    let before = participant.visits();
    participant.on_cycle(3 * CYCLE);
    assert_eq!(participant.visits(), before + 50, "every key");
    assert_eq!(hits(&participant.take_outgoing())[entries[7].key()], 1);
    let before = participant.visits();
    participant.on_cycle(4 * CYCLE);
    assert!(participant.visits() - before <= 1, "the scan taken");
}

/// A key's signal on its way to the transport carries any other worker's for it: a second
/// claim finds it taken, until the transport takes it — before it visits the key, and even
/// with the key queued already or the entry stale — after which the key's next signal is
/// claimed afresh.
#[test]
fn a_signal_on_its_way_carries_the_others_until_the_transport_takes_it() {
    let (mut participant, entries) = scheduled(2);
    let key = &entries[0];
    assert!(key.claim_signal(), "the first");
    assert!(!key.claim_signal(), "a second, carried by the first");
    participant.on_signals([Arc::clone(key)]);
    assert!(key.claim_signal(), "taken, claimed afresh");
    participant.on_signals([Arc::clone(key)]);
    assert!(key.claim_signal(), "taken with the key queued already");
    let stale = Arc::new(Entry::new(b"gone"[..].into(), contract(100, 10), CYCLE, 0));
    assert!(stale.claim_signal());
    participant.on_signals([Arc::clone(&stale)]);
    assert!(stale.claim_signal(), "taken, though no longer kept");
}

/// A scan visits every key but takes no signal still on its way: the key stays claimed
/// until the transport takes that signal, so no other worker sends it again meanwhile.
#[test]
fn a_scan_leaves_a_signal_on_its_way_claimed() {
    let (mut participant, entries) = scheduled(3);
    assert!(entries[1].claim_signal());
    participant.registry().require_scan();
    participant.on_cycle(2 * CYCLE);
    assert!(!entries[1].claim_signal(), "the scan took it");
    participant.on_signals([Arc::clone(&entries[1])]);
    assert!(entries[1].claim_signal());
}

/// With one exchange in flight, a key drained while its owner's exchange is out keeps its
/// ledger and is visited until the exchange is over, then reported, with no new signal.
#[test]
fn a_count_drained_while_its_owner_is_busy_goes_once_the_exchange_is_over() {
    let (mut participant, entries) = scheduled(2);
    participant.set_one_in_flight(true);
    let (a, b) = (&entries[0], &entries[1]);
    assert_eq!(decide(&[a], 2 * CYCLE).signal, 1);
    participant.on_signals([Arc::clone(a)]);
    participant.on_cycle(2 * CYCLE + 1);
    let first = participant.take_exchanges();
    let exchange = first[0].1;
    let first: Vec<(PodId, Batch)> = first.into_iter().map(|(pod, _, b)| (pod, b)).collect();
    assert_eq!(hits(&first)[a.key()], 1, "a's exchange goes");
    assert_eq!(decide(&[b], 2 * CYCLE + 2).signal, 1);
    participant.on_signals([Arc::clone(b)]);
    for cycle in 3..6 {
        participant.on_cycle(cycle * CYCLE);
        assert!(participant.take_outgoing().is_empty(), "the pod is busy");
    }
    participant.exchanged(0, exchange);
    participant.on_cycle(6 * CYCLE);
    assert_eq!(hits(&participant.take_outgoing())[b.key()], 1);
}

/// Scheduled, idle keys are evicted when their time comes, without a cycle visiting them;
/// a key used again in the meantime is kept until it is idle in turn. One eviction time a
/// key at most.
#[test]
fn idle_keys_are_evicted_at_their_time_without_a_scan() {
    let (mut participant, entries) = scheduled(50);
    // A refill time of some 105 ms: idle after twice that.
    let used = 200 * MS;
    assert_eq!(decide(&[&entries[3]], used).local, Local::Admit);
    participant.on_signals([Arc::clone(&entries[3])]);
    participant.on_cycle(used + 1);
    let _ = participant.take_outgoing();
    let before = participant.visits();
    participant.on_cycle(300 * MS);
    assert_eq!(participant.key_count(), 1, "all but the one used");
    assert!(
        participant.visits() - before <= 1,
        "evicted without a visit"
    );
    assert!(participant.evictions.len() <= participant.key_count());
    // Its report acknowledged, the used key is evicted once idle in turn: from 200 ms, some
    // 210 ms on.
    participant.on_answers(
        Answers {
            from: 0,
            entries: vec![Answer {
                key: entries[3].key().into(),
                behind: 0,
                applied: 1,
                others: 0,
            }],
            view: None,
            life: 0,
            number: 0,
            made: 300 * MS,
        },
        300 * MS,
    );
    let mut at = 300 * MS;
    while participant.key_count() > 0 && at < 2_000 * MS {
        at += CYCLE;
        participant.on_cycle(at);
        let _ = participant.take_outgoing();
    }
    assert_eq!(participant.key_count(), 0);
    assert!(
        (410 * MS..=440 * MS).contains(&at),
        "idle in turn at {} ms",
        at / MS
    );
}

/// Ten an hour: a key idle only two hours after its last request.
fn hourly() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(10),
        window: n(3_600_000 * MS),
        burst: n(10),
    }
}

/// Scheduled, a key evicted for capacity frees its entry then, not when its eviction time
/// comes: under an hourly limit that is hours of memory the cap would not see.
#[test]
fn a_key_evicted_for_capacity_is_freed_at_once() {
    let (mut participant, entries) = scheduled_under(2, hourly());
    let held: Vec<_> = entries.iter().map(Arc::downgrade).collect();
    drop(entries);
    participant.set_capacity(1);
    participant.on_cycle(2 * CYCLE);
    assert_eq!(participant.key_count(), 0);
    assert_eq!(participant.holding(), 0);
    assert!(held.iter().all(|entry| entry.upgrade().is_none()), "freed");
    assert!(participant.evictions.is_empty());
}

/// Scheduled, the two lists of keys to visit keep their room from cycle to cycle, however
/// few keys later cycles visit: a list made for each cycle grows from nothing and is freed,
/// which, for a large one, holds up the workers (06 §4).
#[test]
fn the_lists_of_keys_to_visit_keep_their_room() {
    const KEYS: usize = 1_000;
    let mut participant = one_in_flight();
    participant.set_scheduled(true);
    let mut pod = pod(1);
    let c = contract(100, 10);
    for n in 0..KEYS {
        let name = format!("k{n}").into_bytes();
        assert_eq!(participant.decide_all(&[(&name, c)], 0), Decision::Admit);
    }
    let lists = |participant: &Participant<FixedState>| {
        let (one, other) = (&participant.runnable, &participant.next_runnable);
        let mut blocks = [one.as_ptr() as usize, other.as_ptr() as usize];
        blocks.sort_unstable();
        (blocks, one.capacity().min(other.capacity()), one.len())
    };
    let mut first = None;
    for cycle in 1..8 {
        let now = cycle * CYCLE;
        participant.on_cycle(now);
        for (_, exchange, batch) in participant.take_exchanges() {
            participant.on_answers_to(exchange, pod.on_batch(batch, now), now);
        }
        let (blocks, room, _) = lists(&participant);
        match first {
            None => first = Some(blocks),
            Some(first) => assert_eq!(blocks, first, "cycle {cycle}: the same two lists"),
        }
        assert!(room >= KEYS, "cycle {cycle}: room for {room} keys");
    }
    assert!(
        lists(&participant).2 < KEYS / 10,
        "later cycles visit few keys"
    );
}

/// A key evicted and made again numbers its reports past those its earlier life sent,
/// scheduled or not: its owner, which still holds that life's numbers, counts them rather
/// than taking them for reports it has applied.
#[test]
fn a_key_made_again_is_counted_from_its_first_report() {
    for scheduled in [false, true] {
        let mut participant = one_in_flight();
        participant.set_scheduled(scheduled);
        let mut pod = pod(1);
        let keys: [(&[u8], Contract); 1] = [(b"k", contract(100, 10))];
        let mut exchange = |participant: &mut Participant<FixedState>, now: u64| {
            participant.on_cycle(now);
            for (_, exchange, batch) in participant.take_exchanges() {
                participant.on_answers_to(exchange, pod.on_batch(batch, now), now);
            }
            pod.counted(b"k")
        };
        assert_eq!(participant.decide_all(&keys, 0), Decision::Admit);
        assert_eq!(exchange(&mut participant, CYCLE), Some(1));
        participant.set_capacity(1);
        participant.on_cycle(2 * CYCLE);
        assert_eq!(participant.key_count(), 0, "evicted");
        participant.set_capacity(u64::MAX);
        assert_eq!(participant.decide_all(&keys, 2 * CYCLE), Decision::Admit);
        assert_eq!(
            exchange(&mut participant, 3 * CYCLE),
            Some(2),
            "scheduled: {scheduled}"
        );
    }
}

/// Scheduled, an entry replaced by a changed contract is kept by its eviction time until
/// that comes: counted against the cap meanwhile, and freed first when the cap is reached,
/// before any key in use is evicted.
#[test]
fn a_replaced_entry_counts_until_it_is_freed_and_is_freed_first() {
    let (mut participant, entries) = scheduled_under(1, hourly());
    let one = participant.holding();
    let old = Arc::downgrade(&entries[0]);
    drop(entries);
    participant.ensure(b"k0", contract(100, 10), 2 * CYCLE);
    assert!(old.upgrade().is_some(), "kept by its eviction time");
    let stale = participant.holding() - one;
    assert!(stale > 0, "and counted");
    // Past the cap by a byte, and within nine tenths of it once the old entry is gone.
    participant.set_capacity(one + stale - 1);
    assert!(one <= (one + stale - 1) / 10 * 9);
    participant.on_cycle(3 * CYCLE);
    assert!(old.upgrade().is_none(), "freed under the cap");
    assert_eq!(participant.key_count(), 1, "the key in use kept");
    assert_eq!(participant.holding(), one);
    assert_eq!(participant.evictions.len(), 1);
}

/// Scheduled, an entry replaced is freed, and stops counting, when its eviction time comes,
/// with no cap reached.
#[test]
fn a_replaced_entry_stops_counting_when_its_time_comes() {
    let (mut participant, entries) = scheduled_under(1, hourly());
    let old = Arc::downgrade(&entries[0]);
    drop(entries);
    participant.ensure(b"k0", contract(100, 10), 2 * CYCLE);
    participant.on_cycle(3 * CYCLE);
    assert!(old.upgrade().is_some(), "not yet due");
    participant.on_cycle(3 * 3_600_000 * MS);
    assert!(old.upgrade().is_none());
    assert_eq!(participant.key_count(), 0, "the new one idle too");
    assert_eq!(participant.holding(), 0);
    assert!(participant.evictions.is_empty());
}

/// A request refused still brings a key back into use: signalled, so that a scheduled
/// transport polls it again; a refusal under a key in use signals nothing.
#[test]
fn a_refused_request_signals_a_key_it_brings_back_into_use() {
    let (_participant, entries) = scheduled(2);
    let (busy, quiet) = (&entries[0], &entries[1]);
    // The busy key's bucket spent at 300 ms; the quiet key unused since it was made.
    let at = 300 * MS;
    while decide(&[busy], at).local == Local::Admit {}
    let outcome = decide(&[busy, quiet], at);
    assert_eq!(outcome.local, Local::Refuse);
    assert_eq!(outcome.signal, 0b10, "the quiet key, back in use");
    assert_eq!(decide(&[busy, quiet], at + 1).signal, 0);
}

/// With quiet keys polled, a key in use is visited every cycle while it is, and no longer
/// once it is not.
#[test]
fn a_polled_key_is_visited_while_in_use() {
    let mut participant = participant();
    participant.set_scheduled(true);
    let c = contract(100, 10);
    participant.ensure(b"k", c, 0);
    participant.on_cycle(CYCLE);
    let _ = participant.take_outgoing();
    let mut visited = Vec::new();
    for cycle in 2..40 {
        let before = participant.visits();
        participant.on_cycle(cycle * CYCLE);
        let _ = participant.take_outgoing();
        visited.push(participant.visits() - before);
    }
    // In use for its refill time (some 105 ms) from its making: polled every cycle until
    // then, not after.
    assert!(visited[..8].iter().all(|v| *v == 1), "{visited:?}");
    assert!(visited[12..].iter().all(|v| *v == 0), "{visited:?}");
}

/// Scheduled, a report never acknowledged is sent again, as it is unscheduled: its key is
/// visited every cycle until it is.
#[test]
fn an_unacknowledged_report_is_sent_again_scheduled() {
    let (mut participant, entries) = scheduled(1);
    let key = &entries[0];
    assert_eq!(decide(&[key], 2 * CYCLE).signal, 1);
    participant.on_signals([Arc::clone(key)]);
    participant.on_cycle(2 * CYCLE + 1);
    assert_eq!(hits(&participant.take_outgoing())[key.key()], 1);
    // Its answer lost: sent again within a few cycles, unasked.
    let again = (3..20).find(|cycle| {
        participant.on_cycle(cycle * CYCLE);
        hits(&participant.take_outgoing()).get(key.key()) == Some(&1)
    });
    assert!(again.is_some(), "never sent again");
}

/// Keys too many for one piece, each admitting a request every cycle, one exchange in
/// flight: every exchange carries a piece no bigger than [`PIECE_BYTES`]; a key left out
/// goes ahead of the rest at the next cycle, so that none waits more cycles than it takes
/// to send them all; and every hit reaches the pod once.
fn a_backlog_goes_a_piece_an_exchange_and_none_starves(scheduled: bool) {
    const KEYS: usize = 6_000;
    let c = contract(100, 1_000);
    let mut participant = one_in_flight();
    participant.set_scheduled(scheduled);
    participant.set_levers(Levers { poll_quiet: false });
    let mut pod = pod(1);
    // 300 bytes a key: some 2,400 to a piece, two and a half pieces for every key.
    let names: Vec<Vec<u8>> = (0..KEYS)
        .map(|n| {
            let mut name = format!("k{n}").into_bytes();
            name.resize(300, b'.');
            name
        })
        .collect();
    for name in &names {
        participant.ensure(name, c, 0);
    }
    let entries: Vec<Arc<Entry>> = names
        .iter()
        .map(|name| participant.entries(&[(name, c)]).remove(0))
        .collect();
    let piece = PIECE_BYTES / (300 + ENTRY_BYTES);
    let rounds = KEYS.div_ceil(piece);
    assert_eq!(rounds, 3);
    let mut admitted = vec![0_u64; KEYS];
    let mut reported_at = vec![0_u64; KEYS];
    let at: HashMap<&[u8], usize> = names
        .iter()
        .enumerate()
        .map(|(n, name)| (name.as_slice(), n))
        .collect();
    let mut exchange = |participant: &mut Participant<FixedState>,
                        reported_at: &mut [u64],
                        cycle: u64,
                        now: u64| {
        let started = participant.take_exchanges();
        assert!(started.len() <= 1, "one pod, one exchange");
        for (_, number, batch) in started {
            assert!(batch.bytes() <= PIECE_BYTES, "{}", batch.bytes());
            for entry in &batch.entries {
                reported_at[at[&*entry.key]] = cycle;
            }
            let answers = pod.on_batch(in_order(batch), now);
            participant.on_answers_to(number, answers, now);
        }
    };
    for cycle in 1..=12 {
        let now = cycle * CYCLE;
        let mut signalled = Vec::new();
        for (n, entry) in entries.iter().enumerate() {
            let outcome = decide(&[entry], now + 1);
            assert_eq!(outcome.local, Local::Admit, "key {n}, cycle {cycle}");
            admitted[n] += 1;
            if outcome.signal != 0 {
                signalled.push(Arc::clone(entry));
            }
        }
        participant.on_signals(signalled);
        let visits = participant.visits();
        participant.on_cycle(now + 2);
        assert_eq!(
            participant.visits() - visits,
            KEYS as u64,
            "each key once, cycle {cycle}"
        );
        exchange(&mut participant, &mut reported_at, cycle, now + 3);
        if cycle > rounds as u64 {
            for (n, reported) in reported_at.iter().enumerate() {
                assert!(
                    cycle - reported < rounds as u64,
                    "key {n} last reported at cycle {reported}, now {cycle}"
                );
            }
        }
    }
    assert!(participant.keys_left_out() > 0);
    // What is left drains, with nothing more admitted.
    for cycle in 13..=13 + rounds as u64 {
        let now = cycle * CYCLE;
        participant.on_cycle(now + 2);
        exchange(&mut participant, &mut reported_at, cycle, now + 3);
    }
    for (n, name) in names.iter().enumerate() {
        assert_eq!(pod.counted(name), Some(admitted[n]), "key {n}");
    }
}

#[test]
fn a_scheduled_backlog_goes_a_piece_an_exchange_and_none_starves() {
    a_backlog_goes_a_piece_an_exchange_and_none_starves(true);
}

#[test]
fn a_scanned_backlog_goes_a_piece_an_exchange_and_none_starves() {
    a_backlog_goes_a_piece_an_exchange_and_none_starves(false);
}
