//! Tests of the pooled participant: against `DataPlane` as the semantic reference for one
//! participant, the races between workers, and between workers and the transport.

use super::*;
use crate::service::{Answer, DataPlane, FixedState, ServicePod};
use proptest::prelude::*;
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
/// pod's answers not yet delivered.
struct Pooled {
    participant: Participant<FixedState>,
    pod: ServicePod,
    handles: HashMap<Box<[u8]>, Arc<Entry>>,
    pending: Vec<Answers>,
}

impl Pooled {
    fn new() -> Self {
        Self {
            participant: participant(),
            pod: ServicePod::new(0, CYCLE, view()),
            handles: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// Its participant visiting only the keys that need it, its workers' signals handed to
    /// it as they decide.
    fn scheduled() -> Self {
        let mut pooled = Self::new();
        pooled.participant.set_scheduled(true);
        pooled
    }

    /// What it has to send, answered by the pod and held back.
    fn send(&mut self, now: u64) {
        for (_, batch) in self.participant.take_outgoing() {
            self.pending.push(self.pod.on_batch(in_order(batch), now));
        }
    }

    /// The `at`th answer held back (counting round), delivered, and kept to deliver again
    /// if `twice`: the decisions it brought, and the requests handed back to decide again.
    fn deliver(&mut self, at: usize, twice: bool, now: u64) -> Settled {
        if self.pending.is_empty() {
            return Settled::default();
        }
        let at = at % self.pending.len();
        let answers = if twice {
            self.pending[at].clone()
        } else {
            self.pending.remove(at)
        };
        self.participant.on_answers(answers, now);
        self.settled()
    }

    fn settled(&mut self) -> Settled {
        Settled {
            decided: self
                .participant
                .take_decisions()
                .into_iter()
                .map(|decided| (decided.ticket, decided.admitted))
                .collect(),
            retries: self.participant.take_retries(),
        }
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

    fn exchange(&mut self, now: u64) -> Settled {
        for _ in 0..10 {
            let outgoing = self.participant.take_outgoing();
            if outgoing.is_empty() {
                break;
            }
            for (_, batch) in outgoing {
                let answers = self.pod.on_batch(in_order(batch), now);
                self.participant.on_answers(answers, now);
            }
        }
        self.settled()
    }
}

/// What came of requests that waited: decided, by ticket, or handed back to decide again.
#[derive(Debug, Default)]
struct Settled {
    decided: Vec<(Ticket, bool)>,
    retries: Vec<Ticket>,
}

/// `batch` with its asks in the order of their numbers, as both sides' pods are given them.
/// A pod remembers only the last ask a data plane made about a key, so two asks about one
/// key sent again count once more or twice more as their order has it (an inherited defect
/// of the service pod, apart's alike; results note): the order a side happens to list its
/// waiting groups in, which is not what this comparison is about.
fn in_order(mut batch: Batch) -> Batch {
    batch.asks.sort_by_key(|ask| ask.id);
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

    fn deliver(&mut self, at: usize, twice: bool, now: u64) -> Vec<(Ticket, bool)> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let at = at % self.pending.len();
        let answers = if twice {
            self.pending[at].clone()
        } else {
            self.pending.remove(at)
        };
        self.plane.on_answers(answers, now);
        self.plane.take_decided()
    }

    fn exchange(&mut self, now: u64) -> Vec<(Ticket, bool)> {
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
        self.plane.take_decided()
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
    /// `DataPlane` does, the cycle, the answers — late, out of order, twice — and the asks
    /// included: the per-key state moved into shared words changes no decision. Requests
    /// come a unit apart or more, cycles on their boundary.
    ///
    /// But one: where `DataPlane` refuses a request because another's hold is outstanding,
    /// pooled defers it, or hands a waited one back, rather than refuse what the hold may yet
    /// give back. Such a request is set aside — the reference's refusal of it, which changed
    /// nothing, put aside too — and given to both again after every event, compared when
    /// pooled decides it. Pooled defers only where the reference refuses.
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

    /// No admission goes uncounted, whether the participant visits every key each cycle or
    /// only those that need it, quiet keys polled or not, one exchange in flight or more:
    /// over a schedule of requests, cycles and answers — late, out
    /// of order, twice — and then everything delivered and settled, each key's count at its
    /// owner is what was admitted under it, with cold keys decided locally. With them asked
    /// about, at least that: the pod counts an ask sent again twice where another ask about
    /// the key came between (the service pod's one-ask memory; results note), which neither
    /// mode causes. Buckets refilling over longer than a schedule, so that no key is idle
    /// long enough to evict: a key evicted and made again starts its reports afresh, which
    /// a pod that never forgot the key, as this one never does, takes for ones it applied.
    #[test]
    fn no_admission_goes_uncounted_scheduled_or_not(
        events in prop::collection::vec((event(), 0u64..4), 1..300),
        ask_cold: bool,
        poll_quiet: bool,
        one_in_flight: bool,
    ) {
        let keys: [(&[u8], Contract); 3] = [
            (b"a", contract(100, 80)),
            (b"b", contract(10, 70)),
            (b"c", contract(1_000, 100)),
        ];
        for scheduled in [false, true] {
            let mut pooled = if scheduled { Pooled::scheduled() } else { Pooled::new() };
            pooled.participant.set_levers(Levers {
                ask_cold,
                poll_quiet,
            });
            pooled.participant.set_one_in_flight(one_in_flight);
            let mut admitted = [0u64; 3];
            let mut waiting: HashMap<Ticket, Vec<usize>> = HashMap::new();
            let settle = |settled: Settled,
                          admitted: &mut [u64; 3],
                          waiting: &mut HashMap<Ticket, Vec<usize>>| {
                for (ticket, ok) in settled.decided {
                    let places = waiting.remove(&ticket).unwrap_or_default();
                    if ok {
                        for place in places {
                            admitted[place] += 1;
                        }
                    }
                }
                for ticket in settled.retries {
                    waiting.remove(&ticket);
                }
            };
            let mut now = 0;
            let mut next_cycle = CYCLE;
            for (event, step) in &events {
                now += step * 40_000 * 25;
                while now >= next_cycle {
                    pooled.participant.on_cycle(next_cycle);
                    next_cycle += CYCLE;
                }
                let places = match event {
                    Event::Request(key) => vec![*key],
                    Event::Pair(first, second) => vec![*first, *second],
                    Event::Cycle => continue,
                    Event::Exchange => {
                        settle(pooled.exchange(now), &mut admitted, &mut waiting);
                        continue;
                    }
                    Event::Send => {
                        pooled.send(now);
                        continue;
                    }
                    Event::Deliver(index, twice) => {
                        settle(pooled.deliver(*index, *twice, now), &mut admitted, &mut waiting);
                        continue;
                    }
                };
                let request: Vec<(&[u8], Contract)> =
                    places.iter().map(|place| keys[*place]).collect();
                match pooled.decide(&request, now) {
                    Decision::Admit => {
                        for place in &places {
                            admitted[*place] += 1;
                        }
                    }
                    Decision::Wait(ticket) => {
                        waiting.insert(ticket, places);
                    }
                    Decision::Refuse | Decision::Defer => {}
                }
            }
            for _ in 0..50 {
                now = next_cycle;
                pooled.participant.on_cycle(now);
                next_cycle += CYCLE;
                while !pooled.pending.is_empty() {
                    settle(pooled.deliver(0, false, now), &mut admitted, &mut waiting);
                }
                settle(pooled.exchange(now), &mut admitted, &mut waiting);
            }
            prop_assert!(waiting.is_empty(), "never decided: {:?}", waiting);
            for (place, (key, _)) in keys.iter().enumerate() {
                let counted = pooled.pod.counted(key).unwrap_or(0);
                if ask_cold {
                    prop_assert!(
                        counted >= admitted[place],
                        "scheduled {}, key {}: {} counted of {}",
                        scheduled, place, counted, admitted[place]
                    );
                } else {
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
}

/// Pooled against the reference over one schedule, with the requests pooled set aside.
struct Comparison {
    keys: [(&'static [u8], Contract); 3],
    pooled: Pooled,
    reference: Reference,
    /// Requests deferred or handed back, by their keys' places.
    aside: Vec<Vec<usize>>,
    /// The keys of requests waiting on both sides, by ticket: the same ticket on both, as
    /// each numbers its waiting requests in turn.
    waiting: HashMap<Ticket, Vec<usize>>,
}

impl Comparison {
    fn new(keys: [(&'static [u8], Contract); 3]) -> Self {
        Self {
            keys,
            pooled: Pooled::new(),
            reference: Reference::new(),
            aside: Vec::new(),
            waiting: HashMap::new(),
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
        if let (Decision::Wait(ours), crate::service::Decision::Wait(theirs)) = (got, want) {
            prop_assert_eq!(ours, theirs, "event {}: tickets", at);
            self.waiting.insert(ours, places.to_vec());
        }
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
                let got = self.pooled.exchange(now);
                let want = self.reference.exchange(now);
                self.waited(at, now, got, want)?;
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
                let got = self.pooled.deliver(*index, *twice, now);
                let want = self.reference.deliver(*index, *twice, now);
                self.waited(at, now, got, want)?;
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

    /// Requests that waited, decided alike by ticket; those pooled handed back the
    /// reference refused, and are set aside.
    fn waited(
        &mut self,
        at: usize,
        now: u64,
        got: Settled,
        want: Vec<(Ticket, bool)>,
    ) -> Result<(), TestCaseError> {
        let got_decided: HashMap<Ticket, bool> = got.decided.iter().copied().collect();
        prop_assert_eq!(
            got.decided.len() + got.retries.len(),
            want.len(),
            "event {} at {}: waited decisions {:?} against {:?}",
            at,
            now,
            got,
            want
        );
        for (ticket, admitted) in want {
            let places = self.waiting.remove(&ticket).unwrap_or_default();
            if got.retries.contains(&ticket) {
                prop_assert!(
                    !admitted,
                    "event {}: handed back where the reference admits",
                    at
                );
                self.aside.push(places);
            } else {
                prop_assert_eq!(
                    got_decided.get(&ticket),
                    Some(&admitted),
                    "event {} at {}: ticket {}",
                    at,
                    now,
                    ticket
                );
            }
        }
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

/// Which decision, its ticket aside: pooled's and the reference's compared.
fn discriminant(decision: impl Into<Kind>) -> Kind {
    decision.into()
}

#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Admit,
    Refuse,
    Wait,
    Defer,
}

impl From<Decision> for Kind {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Admit => Self::Admit,
            Decision::Refuse => Self::Refuse,
            Decision::Wait(_) => Self::Wait,
            Decision::Defer => Self::Defer,
        }
    }
}

impl From<crate::service::Decision> for Kind {
    fn from(decision: crate::service::Decision) -> Self {
        match decision {
            crate::service::Decision::Admit => Self::Admit,
            crate::service::Decision::Refuse => Self::Refuse,
            crate::service::Decision::Wait(_) => Self::Wait,
        }
    }
}

/// Room for `burst` at once and nothing more: many threads taking at once, started
/// together, get the bucket exactly, the final token to one of them. 16,000: within what a
/// key may hold unreported, as nothing drains it here.
#[test]
fn threads_racing_for_the_last_tokens_admit_the_bucket_exactly() {
    let entry = Entry::new(b"k"[..].into(), contract(1, 16_000), CYCLE, false, 0);
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
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 2_000), CYCLE, false, 0);
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
            let mut fresh = Entry::new(
                old.key.clone(),
                old.terms.contract,
                old.terms.cycle,
                true,
                0,
            );
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
    let mut entry = Entry::new(b"k"[..].into(), contract(100, 100), CYCLE, false, 0);
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
    let (drained, _) = entry.drain(0, Cycle::AsItIs);
    assert_eq!(drained, 4);
    assert_eq!(decide(&[&entry], 0).local, Local::Admit);
}

/// Passing half its count's most, a key says once that it wants draining; drained, it can
/// say so again.
#[test]
fn a_key_asks_for_a_drain_once_at_half_its_count() {
    let mut entry = Entry::new(b"k"[..].into(), contract(100, 100), CYCLE, false, 0);
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
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 1_000), CYCLE, false, 0);
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
    let x = Entry::new(b"x"[..].into(), contract(100, 1), CYCLE, false, 0);
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
    let x = Entry::new(b"x"[..].into(), contract(1_000, 1_000), CYCLE, false, 0);
    let y = Entry::new(b"y"[..].into(), contract(1_000, 1_000), CYCLE, false, 0);
    for _ in 0..word::MOST_HELD {
        assert!(x.take(Op::Hold, 0).0.done());
    }
    let before = y.snapshot();
    assert_eq!(decide(&[&y, &x], 0).local, Local::Defer);
    assert_eq!(y.snapshot(), before, "its hold on Y given back as it was");
    x.settle(true, 0);
    assert_eq!(decide(&[&y, &x], 0).local, Local::Admit);
}

/// Settling or draining wakes only where requests wait: none waiting, nothing to wake.
#[test]
fn only_a_key_with_waiters_wants_waking() {
    let x = Entry::new(b"x"[..].into(), contract(100, 10), CYCLE, false, 0);
    assert!(x.take(Op::Hold, 0).0.done());
    assert!(!x.settle(false, 0).0);
    assert!(x.take(Op::Admit, 0).0.done());
    x.add_waiter();
    assert!(x.drain(0, Cycle::AsItIs).1);
    x.remove_waiter();
    assert!(x.take(Op::Admit, 0).0.done());
    assert!(!x.drain(0, Cycle::AsItIs).1);
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
                owned: true,
            }],
            decided: Vec::new(),
            view: None,
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
                owned: true,
            }],
            decided: Vec::new(),
            view: None,
        },
        0,
    );
    assert_eq!(
        bucket.remaining(entry.snapshot().arrival, bucket.units(0)),
        100 - 6,
        "the owner's three, and the three it has not seen"
    );
}

/// A request under two keys is held on both or neither: the second without room gives
/// the first back as it was.
#[test]
fn a_two_key_request_is_charged_on_both_or_neither() {
    let roomy = Entry::new(b"a"[..].into(), contract(100, 10), CYCLE, false, 0);
    let empty = Entry::new(b"b"[..].into(), contract(100, 1), CYCLE, false, 0);
    assert_eq!(decide(&[&empty], 0).local, Local::Admit);
    let before = roomy.snapshot();
    assert_eq!(decide(&[&roomy, &empty], 0).local, Local::Refuse);
    assert_eq!(roomy.snapshot(), before, "given back as it was");
    let fresh = Entry::new(b"c"[..].into(), contract(100, 1), CYCLE, false, 0);
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
    let entry = Entry::new(b"k"[..].into(), contract(1_000, 20), CYCLE, false, 0);
    entry.others.store(1, Ordering::Release);
    while decide(&[&entry], 0).local == Local::Admit {}
    // Five cycles later: one advance, sharing out what the bucket refilled (capped at the
    // bucket) and one cycle's refill, among two — not five refills.
    let now = 5 * CYCLE;
    let fields = word::advanced(&entry.terms.bucket, entry.snapshot(), entry.advance(), now);
    let remaining = entry
        .terms
        .bucket
        .remaining(fields.arrival, entry.terms.bucket.units(now));
    // No cycle had set an allowance before: nothing is carried.
    assert_eq!(
        fields.left,
        ((remaining + entry.terms.refill) / 2).max(1),
        "one share of what is left, one cycle's refill"
    );
    let admitted = (0..100)
        .filter(|_| decide(&[&entry], now).local == Local::Admit)
        .count() as u64;
    assert_eq!(admitted, fields.left.min(remaining));
}

/// Cold requests under the same keys wait on one ask; under different lists that share a
/// key, on one ask each; and an owner admitting one of two admits only the first.
#[test]
fn cold_requests_are_coalesced_by_their_list_of_keys() {
    let mut participant = participant();
    let a: (&[u8], Contract) = (b"a", contract(100, 1));
    let b: (&[u8], Contract) = (b"b", contract(100, 10));
    let first = participant.decide_all(&[a], 0);
    let second = participant.decide_all(&[a], 0);
    let other = participant.decide_all(&[a, b], 0);
    assert!(matches!(
        (first, second, other),
        (Decision::Wait(_), Decision::Wait(_), Decision::Wait(_))
    ));
    let batches = participant.take_outgoing();
    let asks: Vec<(u64, usize)> = batches
        .iter()
        .flat_map(|(_, batch)| batch.asks.iter().map(|ask| (ask.count, ask.keys.len())))
        .collect();
    assert_eq!(
        asks,
        [(1, 1), (1, 2)],
        "the second waits behind the first's ask"
    );
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view());
    for (_, batch) in batches {
        let answers = pod.on_batch(batch, 0);
        participant.on_answers(answers, 0);
    }
    let decided: Vec<bool> = participant
        .take_decisions()
        .iter()
        .map(|decided| decided.admitted)
        .collect();
    assert_eq!(
        decided.iter().filter(|admitted| **admitted).count(),
        1,
        "a bucket of one: {decided:?}"
    );
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
    let made = Arc::new(Entry::new(
        b"key"[..].into(),
        contract(100, 10),
        CYCLE,
        false,
        0,
    ));
    set.insert(Handle::new(Arc::clone(&made)));
    assert!(Arc::ptr_eq(set.get(&b"key"[..]).unwrap().entry(), &made));
    assert!(!set.contains(&b"kez"[..]));
}

/// With one exchange in flight to a pod, nothing more goes to it until that one is
/// answered: what is admitted meanwhile gathers in the ledger and goes as one report, and
/// the key is not polled while it waits. An exchange never answered is taken as failed once
/// an ask's time has passed.
#[test]
fn one_exchange_in_flight_gathers_what_comes_meanwhile() {
    let mut participant = participant();
    participant.set_one_in_flight(true);
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view());
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 100));
    // Cold: asked, and answered, which leaves the pod free.
    assert!(matches!(
        participant.decide_all(&[(key, c)], 0),
        Decision::Wait(_)
    ));
    for (_, batch) in participant.take_outgoing() {
        participant.on_answers(pod.on_batch(batch, 0), 0);
    }
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
    // Never answered: after an ask's time, taken as failed, and the next goes.
    assert!(entry.take(Op::Admit, 6 * CYCLE).0.done());
    let later = 5 * CYCLE + RESEND_AFTER_CYCLES * CYCLE;
    participant.on_cycle(later);
    assert!(
        !participant.take_outgoing().is_empty(),
        "after the exchange's time"
    );
}

/// With cold keys not asked about, a new key's request is decided at once from a full
/// bucket and nothing is asked; its admissions are reported with the cycle, as any key's.
#[test]
fn a_cold_key_decided_locally_asks_nothing() {
    let mut participant = participant();
    participant.set_levers(Levers {
        ask_cold: false,
        ..Levers::default()
    });
    let (key, c): (&[u8], Contract) = (b"k", contract(100, 3));
    for _ in 0..3 {
        assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Admit);
    }
    assert_eq!(participant.decide_all(&[(key, c)], 0), Decision::Refuse);
    let asked: usize = participant
        .take_outgoing()
        .iter()
        .map(|(_, batch)| batch.asks.len())
        .sum();
    assert_eq!(asked, 0);
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

/// Cold keys decided locally, a worker makes a key's entry itself, once however many meet
/// it; the participant adopts it, reports what it admitted, and keeps it as the key's.
#[test]
fn a_worker_makes_a_local_key_and_the_participant_adopts_it() {
    let mut participant = participant();
    participant.set_levers(Levers {
        ask_cold: false,
        ..Levers::default()
    });
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
        participant.set_levers(Levers {
            poll_quiet,
            ..Levers::default()
        });
        let mut pod: ServicePod = ServicePod::new(0, CYCLE, view());
        let (key, c): (&[u8], Contract) = (b"k", contract(100, 100));
        assert!(matches!(
            participant.decide_all(&[(key, c)], 0),
            Decision::Wait(_)
        ));
        for (_, batch) in participant.take_outgoing() {
            participant.on_answers(pod.on_batch(batch, 0), 0);
        }
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
    let registry = Registry::new();
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

#[test]
fn the_platform_has_native_128_bit_atomics() {
    assert!(supported(), "the participant would not start here");
}

/// Runs `schedule` on both sides, printing what each decided and every key's room after
/// each event: for explaining a failure of the comparison above.
fn replay(schedule: &[(Event, u64)]) {
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
                let want = reference.plane.room(key, *contract, now);
                let got = pooled.participant.keys.get(*key).map(|tracked| {
                    let entry = &tracked.entry;
                    let fields =
                        word::advanced(&entry.terms.bucket, entry.snapshot(), entry.advance(), now);
                    let left = entry
                        .terms
                        .bucket
                        .remaining(fields.arrival, entry.terms.bucket.units(now));
                    (
                        fields.left,
                        left,
                        fields.held,
                        entry.cold(now),
                        tracked.asking,
                        tracked.bounded,
                        tracked.heard,
                    )
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
                    t.heard,
                    t.unacknowledged.len(),
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

/// A scheduled participant deciding cold keys locally, nothing polled: what each test of
/// scheduling starts from, with `keys` keys made at 0 and their first cycle's visit done.
fn scheduled(keys: u64) -> (Participant<FixedState>, Vec<Arc<Entry>>) {
    let mut participant = participant();
    participant.set_scheduled(true);
    participant.set_levers(Levers {
        poll_quiet: false,
        ask_cold: false,
    });
    let c = contract(100, 10);
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
    assert_eq!(
        hits(&participant.take_outgoing())[a.key()],
        1,
        "a's exchange goes"
    );
    assert_eq!(decide(&[b], 2 * CYCLE + 2).signal, 1);
    participant.on_signals([Arc::clone(b)]);
    for cycle in 3..6 {
        participant.on_cycle(cycle * CYCLE);
        assert!(participant.take_outgoing().is_empty(), "the pod is busy");
    }
    participant.exchanged(0);
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
                owned: true,
            }],
            decided: Vec::new(),
            view: None,
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
