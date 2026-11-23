//! The asynchronous mode as the world runs it: its clients are data planes — the proxy's
//! participant (`pooled::Participant`), routed by the table it was told (`Routed`) — deciding
//! every request from their own copies and reporting their hits in batches, every cycle, to
//! each key's owner as the table names it; its pods apply them on the store and answer with
//! the buckets. A data plane runs as the proxy runs it: one exchange in flight to each pod, an
//! exchange unanswered for 300 ms taken as failed, only the keys that need it visited each
//! cycle, quiet keys not polled. An owner that has left a batch unanswered for four cycles
//! has its keys' entries sent through one of the shard's replicas too, which forwards them,
//! until the owner answers directly again.

use super::both::{Caller, asynchronous_of};
use super::{Event, World, store_kind, store_size};
use crate::Rng;
use crate::workload::Workload;
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::table::{Table, shard_of};
use edgerush_cluster::{KEY_BYTES, Key, NodeId, Time, ms};
use edgerush_cluster_asynchronous::{Answers, Routed};
use edgerush_ratelimit::service::pooled::{
    self, Decision, Entry, Levers, Local, Participant, Routing,
};
use edgerush_ratelimit::service::{Contract, Keyed};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// The mode's messages, over the store of both modes' values.
type Message = edgerush_cluster_asynchronous::Message<edgerush_cluster_modes::Value>;

/// The time between a data plane's batches.
pub const CYCLE: Time = ms(10);

/// How long a data plane waits for an exchange's answer before taking it as failed: the
/// proxy's.
const EXCHANGE_TIMEOUT: Time = ms(300);

/// What every data plane hashes its keys under, as the proxy's share `key_secret`.
const SECRET: [u64; 2] = [1, 2];

/// The data planes' routing, under the secret they share, by `table` if there is one.
fn routed(table: Option<Table>) -> Routed {
    let none = Routed::new(SECRET);
    match table {
        Some(table) => none.told(table),
        None => none,
    }
}

/// A data plane as the world runs it: the proxy's participant, under keys it shares with the
/// others, taking requests steadily or as a workload brings them.
pub struct Plane {
    data: Participant<Keyed, Routed>,
    feed: Feed,
    /// When each key's requests were admitted, by the world's clock.
    admitted: BTreeMap<u64, Vec<Time>>,
    /// Requests it decided, and refused.
    requests: u64,
    refused: u64,
    /// Batches it sent.
    batches: u64,
    /// Messages it took from the pods: answers, answers relayed and tables.
    answers: u64,
    /// Whether this life of it has ended, its process restarted (`restart_plane`): it decides,
    /// sends and takes nothing more, and what it did stays counted.
    gone: bool,
}

/// Where a data plane's requests come from.
enum Feed {
    /// A steady rate, under its keys in turn, each held to one contract.
    Steady {
        keys: Vec<u64>,
        next: usize,
        contract: Contract,
        interval: Time,
    },
    /// A workload's arrivals, made beforehand, to its workers.
    Workload(Box<Fed>),
}

/// A data plane taking a workload: its workers decide each request from the entries they hold
/// handles to, as the proxy's do, asking the participant for those they lack.
struct Fed {
    /// Arrivals still to come, in order: when, at which of its workers, and of which kind.
    arrivals: VecDeque<(Time, usize, usize)>,
    /// The limits of each kind of request: its keys, each with its contract.
    kinds: Arc<[Vec<(u64, Contract)>]>,
    /// Each worker's handles to the entries it decides from, by key.
    handles: Vec<BTreeMap<u64, Arc<Entry>>>,
    /// Until when its participant's background is held, if it is: no cycles, its answers and
    /// its workers' asks waiting.
    paused_until: Option<Time>,
    /// Answers that came while it was held, from whom, in order.
    held: Vec<(NodeId, Message)>,
    /// Asks that came while it was held: when, from which worker, of which kind.
    pending: Vec<(Time, usize, usize)>,
    /// Requests whose decision waited for the participant, and the time they waited in all.
    waited: u64,
    waited_for: Time,
    /// Its participant's levers, for a process restarted to start alike: quiet keys polled,
    /// keys scheduled.
    poll_quiet: bool,
    scheduled: bool,
}

/// A second limit each request of a workload is under, beside the run's: decided with it, all
/// or none, its key owned by another pod than the run's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Beside {
    /// Every request under one more limit, over every data plane.
    Global(Contract),
    /// Requests alternate between two routes, each with a limit of its own, `Contract`; the
    /// run's is shared by both.
    Routes(Contract),
}

/// A workload as data planes take it.
#[derive(Clone, Copy, Debug)]
pub struct Fleet {
    /// The data planes.
    pub planes: usize,
    /// Each data plane's workers.
    pub workers: usize,
    /// The run's limit: every request is under it.
    pub contract: Contract,
    /// A limit beside it, if any.
    pub beside: Option<Beside>,
    /// The traffic, landing on every data plane's workers in turn.
    pub workload: Workload,
    /// What the traffic is drawn from, apart from the world's own draws.
    pub seed: u64,
    /// Whether each participant visits only the keys that need it each cycle, as the proxy's
    /// do unless `schedule_keys` is off, rather than every key.
    pub scheduled: bool,
    /// Whether each participant polls a key in use that has nothing to report
    /// ([`Levers::poll_quiet`]).
    pub poll_quiet: bool,
}

/// A key's bytes as a client sends it: the world's key, little-endian. The data plane hashes
/// them into the store's key.
fn bytes_of(key: u64) -> [u8; 8] {
    key.to_le_bytes()
}

/// The name of `message`'s kind.
pub(super) const fn kind(message: &Message) -> &'static str {
    match message {
        Message::Store(message) => store_kind(message),
        Message::Batch(_) => "batch",
        Message::Answers(Answers { table: Some(_), .. }) => "answers with a table",
        Message::Answers(_) => "answers",
        Message::Forwarded { .. } => "batch forwarded",
        Message::Relayed { .. } => "answers relayed",
    }
}

/// A batch's entry as today's wire bounds it, its key the store's ([`KEY_BYTES`]) in place of
/// the key's own bytes, an answer a key and three numbers, an unserved key a key and a flag,
/// and a table as the synchronous mode's reply carries it.
pub(super) fn size(message: &Message) -> u64 {
    match message {
        Message::Store(message) => store_size(message),
        Message::Batch(batch) | Message::Forwarded { batch, .. } => {
            64 + (128 + KEY_BYTES) * batch.entries.len() as u64
        }
        Message::Answers(answers) | Message::Relayed { answers, .. } => {
            64 + (40 + KEY_BYTES) * answers.entries.len() as u64
                + (8 + KEY_BYTES) * answers.unserved.len() as u64
                + answers
                    .table
                    .as_ref()
                    .map_or(0, |table| table.shards.len() as u64 * 24)
        }
    }
}

/// The keys whose hits `message` brings a pod to apply.
pub(super) fn reported(message: &Message) -> Vec<Key> {
    match message {
        Message::Batch(batch) | Message::Forwarded { batch, .. } => {
            let mut keys: Vec<Key> = batch.entries.iter().map(|entry| entry.key).collect();
            keys.sort_unstable();
            keys.dedup();
            keys
        }
        _ => Vec::new(),
    }
}

impl World {
    /// `count` data planes, each taking `rate` requests a second, spread in turn over `keys`
    /// shared by all, every key held to `contract`; each knowing the latest table, as a data
    /// plane is told it when its stream to a pod opens.
    ///
    /// # Panics
    ///
    /// Where 128-bit atomics are not native, which the participant needs.
    #[expect(
        clippy::expect_used,
        reason = "the simulation runs only where the proxy's participant does"
    )]
    pub fn add_planes(&mut self, count: u64, keys: &[u64], rate: u64, contract: Contract) {
        let interval = 1_000_000_000 / rate.max(1);
        let table = self.table().cloned();
        for _ in 0..count {
            let id = self.next_client;
            self.next_client += 1;
            let mut data = Participant::new(id, CYCLE, routed(table.clone()), Keyed::new(SECRET))
                .expect("native 128-bit atomics");
            data.set_one_in_flight(true);
            data.set_exchange_timeout(EXCHANGE_TIMEOUT);
            data.set_levers(Levers { poll_quiet: false });
            data.set_scheduled(true);
            let plane = Plane {
                data,
                feed: Feed::Steady {
                    keys: keys.to_vec(),
                    next: usize::try_from(self.rng.below(keys.len() as u64)).unwrap_or(0),
                    contract,
                    interval,
                },
                admitted: BTreeMap::new(),
                requests: 0,
                refused: 0,
                batches: 0,
                answers: 0,
                gone: false,
            };
            self.clients.insert(id, Caller::Asynchronous(plane));
            self.asked.extend(keys.iter().copied().map(Key::from));
            let first = self.now + self.rng.below(interval.max(1));
            self.schedule(first, Event::ClientSend(id));
            let cycle = self.now + self.rng.below(CYCLE);
            self.schedule(cycle, Event::ClientCheck(id));
        }
    }

    /// Requests the data planes decided, and refused.
    #[must_use]
    pub fn decided(&self) -> (u64, u64) {
        self.clients
            .values()
            .filter_map(Caller::asynchronous)
            .fold((0, 0), |(requests, refused), plane| {
                (requests + plane.requests, refused + plane.refused)
            })
    }

    /// Hits the data planes gave up reporting, their batches older than a refill time unseen
    /// applied (`Participant::forgiven`).
    #[must_use]
    pub fn forgiven(&self) -> u64 {
        self.clients
            .values()
            .filter_map(Caller::asynchronous)
            .map(|plane| plane.data.forgiven())
            .sum()
    }

    /// The pods data plane `id` has, by now, left a batch unanswered for four cycles
    /// (`Participant::unanswered`).
    #[must_use]
    pub fn unanswered(&self, id: NodeId) -> Vec<NodeId> {
        self.clients
            .get(&id)
            .and_then(Caller::asynchronous)
            .map(|plane| plane.data.unanswered(self.now).collect())
            .unwrap_or_default()
    }

    /// When each key's requests were admitted, every data plane's together, in order.
    #[must_use]
    pub fn admissions(&self) -> BTreeMap<u64, Vec<Time>> {
        let mut all: BTreeMap<u64, Vec<Time>> = BTreeMap::new();
        for plane in self.clients.values().filter_map(Caller::asynchronous) {
            for (key, times) in &plane.admitted {
                all.entry(*key).or_default().extend(times);
            }
        }
        for times in all.values_mut() {
            times.sort_unstable();
        }
        all
    }

    /// A request reaches data plane `id`: steadily, under its next key, decided from its copy;
    /// or each of a workload's arrivals now due, at its worker.
    pub(super) fn on_request(&mut self, id: NodeId) {
        if self.clients_stopped || self.gone(id) {
            return;
        }
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        let Feed::Steady {
            keys,
            next,
            contract,
            interval,
        } = &mut plane.feed
        else {
            self.on_arrivals(id);
            return;
        };
        let interval = *interval;
        if let Some(&key) = keys.get(*next % keys.len().max(1)) {
            *next += 1;
            plane.requests += 1;
            let bytes = bytes_of(key);
            match plane.data.decide_all(&[(&bytes, *contract)], now) {
                Decision::Admit => plane.admitted.entry(key).or_default().push(now),
                Decision::Refuse => plane.refused += 1,
            }
        }
        self.stats.sent += 1;
        self.schedule(now + interval, Event::ClientSend(id));
    }

    /// `fleet`'s data planes, each its participant and workers, every request of its workload
    /// under the run's key and the one beside it, if any, each owned by another pod: its keys,
    /// the run's first. Once the cluster has a table, as a data plane is told it.
    ///
    /// # Panics
    ///
    /// Where 128-bit atomics are not native, which the participant needs.
    pub fn add_fleet(&mut self, fleet: &Fleet) -> Vec<u64> {
        let table = self.table().cloned();
        let workers = fleet.workers.max(1);
        let mut planes = Vec::new();
        for _ in 0..fleet.planes.max(1) {
            let id = self.next_client;
            self.next_client += 1;
            let data = fed_participant(
                id,
                table.clone(),
                fleet.poll_quiet,
                fleet.scheduled,
                workers,
            );
            planes.push((id, data));
        }
        // The run's key, and each beside it the first after it whose owner is another pod.
        let owner = |key: u64| {
            let bytes = bytes_of(key);
            let data = &planes[0].1;
            let key = data.routing().store_key(&bytes, data.hash(&bytes));
            table
                .as_ref()
                .and_then(|table| table.owner(shard_of(key, table.shards.len() as u16)))
        };
        let run = 1;
        let apart = |after: u64| {
            (after..)
                .take(10_000)
                .find(|key| owner(*key) != owner(run))
                .unwrap_or(after)
        };
        let kinds: Vec<Vec<(u64, Contract)>> = match fleet.beside {
            None => vec![vec![(run, fleet.contract)]],
            Some(Beside::Global(global)) => {
                vec![vec![(run, fleet.contract), (apart(2), global)]]
            }
            Some(Beside::Routes(each)) => {
                let a = apart(2);
                let b = apart(a + 1);
                vec![
                    vec![(a, each), (run, fleet.contract)],
                    vec![(b, each), (run, fleet.contract)],
                ]
            }
        };
        let mut keys: Vec<u64> = kinds.iter().flatten().map(|(key, _)| *key).collect();
        keys.sort_unstable();
        keys.dedup();
        keys.retain(|key| *key != run);
        keys.insert(0, run);
        let kinds: Arc<[Vec<(u64, Contract)>]> = kinds.into();
        // Every data plane's arrivals, by its workers; each request's kind by its place among
        // them all, so that kinds alternate across the fleet.
        let arrivals = fleet.workload.arrivals(
            &fleet.contract,
            fleet.planes.max(1) * workers,
            &mut Rng::new(fleet.seed),
        );
        let mut each: Vec<VecDeque<(Time, usize, usize)>> = vec![VecDeque::new(); planes.len()];
        for (n, arrival) in arrivals.iter().enumerate() {
            each[arrival.worker / workers].push_back((
                self.now + arrival.at,
                arrival.worker % workers,
                n % kinds.len(),
            ));
        }
        for ((id, data), arrivals) in planes.into_iter().zip(each) {
            let first = arrivals.front().map(|(at, _, _)| *at);
            let fed = Fed {
                arrivals,
                kinds: Arc::clone(&kinds),
                handles: vec![BTreeMap::new(); workers],
                paused_until: None,
                held: Vec::new(),
                pending: Vec::new(),
                waited: 0,
                waited_for: 0,
                poll_quiet: fleet.poll_quiet,
                scheduled: fleet.scheduled,
            };
            self.clients.insert(
                id,
                Caller::Asynchronous(Plane {
                    data,
                    feed: Feed::Workload(Box::new(fed)),
                    admitted: BTreeMap::new(),
                    requests: 0,
                    refused: 0,
                    batches: 0,
                    answers: 0,
                    gone: false,
                }),
            );
            self.asked.extend(keys.iter().copied().map(Key::from));
            if let Some(first) = first {
                self.schedule(first, Event::ClientSend(id));
            }
            let cycle = self.now + self.rng.below(CYCLE);
            self.schedule(cycle, Event::ClientCheck(id));
        }
        keys
    }

    /// Data plane `id`'s process restarts now, as a proxy's does: a new participant under a new
    /// id, at a new address, holding nothing, told the latest table, its workers holding no
    /// handles; the arrivals still to come go to it. The old life ends: what it had admitted
    /// and not reported is lost with it, requests waiting for it fail (counted refused), and
    /// answers on their way to it reach no one, as over the connections that died with it.
    /// The new life's id.
    pub fn restart_plane(&mut self, id: NodeId) -> Option<NodeId> {
        let table = self.table().cloned();
        let new = self.next_client;
        let old = self
            .clients
            .get_mut(&id)
            .and_then(Caller::asynchronous_mut)?;
        if old.gone {
            return None;
        }
        let Feed::Workload(fed) = &mut old.feed else {
            return None;
        };
        let failed = fed.pending.len() as u64;
        let workers = fed.handles.len();
        let data = fed_participant(new, table, fed.poll_quiet, fed.scheduled, workers);
        let fresh = Fed {
            arrivals: std::mem::take(&mut fed.arrivals),
            kinds: Arc::clone(&fed.kinds),
            handles: vec![BTreeMap::new(); workers],
            paused_until: None,
            held: Vec::new(),
            pending: Vec::new(),
            waited: 0,
            waited_for: 0,
            poll_quiet: fed.poll_quiet,
            scheduled: fed.scheduled,
        };
        fed.pending.clear();
        fed.held.clear();
        old.requests += failed;
        old.refused += failed;
        old.gone = true;
        self.next_client += 1;
        let next = fresh.arrivals.front().map(|(at, _, _)| *at);
        self.clients.insert(
            new,
            Caller::Asynchronous(Plane {
                data,
                feed: Feed::Workload(Box::new(fresh)),
                admitted: BTreeMap::new(),
                requests: 0,
                refused: 0,
                batches: 0,
                answers: 0,
                gone: false,
            }),
        );
        if let Some(next) = next {
            self.schedule(next.max(self.now), Event::ClientSend(new));
        }
        let cycle = self.now + self.rng.below(CYCLE);
        self.schedule(cycle, Event::ClientCheck(new));
        Some(new)
    }

    /// Whether data plane `id` is a life that has ended, or none.
    fn gone(&self, id: NodeId) -> bool {
        self.clients
            .get(&id)
            .and_then(Caller::asynchronous)
            .is_none_or(|plane| plane.gone)
    }

    /// Holds data plane `id`'s participant for `length` from now: no cycles, its answers and
    /// its workers' asks waiting until it goes on, as when its thread is held up. Its workers
    /// go on deciding from the entries they hold.
    pub fn pause_plane(&mut self, id: NodeId, length: Time) {
        let until = self.now + length;
        if let Some(Plane {
            feed: Feed::Workload(fed),
            ..
        }) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut)
        {
            fed.paused_until = Some(until);
        }
    }

    /// Requests whose decision waited for a participant held up, and the time they waited in
    /// all.
    #[must_use]
    pub fn waited(&self) -> (u64, Time) {
        self.clients.values().filter_map(Caller::asynchronous).fold(
            (0, 0),
            |(waited, wait), plane| match &plane.feed {
                Feed::Workload(fed) => (waited + fed.waited, wait + fed.waited_for),
                Feed::Steady { .. } => (waited, wait),
            },
        )
    }

    /// Batches the data planes sent.
    #[must_use]
    pub fn batches(&self) -> u64 {
        self.clients
            .values()
            .filter_map(Caller::asynchronous)
            .map(|plane| plane.batches)
            .sum()
    }

    /// Messages between the data planes and the pods: the batches sent and what the data
    /// planes took back.
    #[must_use]
    pub fn plane_messages(&self) -> u64 {
        self.clients
            .values()
            .filter_map(Caller::asynchronous)
            .map(|plane| plane.batches + plane.answers)
            .sum()
    }

    /// The pod the table names `key`'s owner, and the key as the store keeps it: hashed as the
    /// data planes hash it.
    fn owned(&self, key: u64) -> Option<(NodeId, Key)> {
        let plane = self
            .clients
            .values()
            .filter_map(Caller::asynchronous)
            .next()?;
        let bytes = bytes_of(key);
        let key = plane
            .data
            .routing()
            .store_key(&bytes, plane.data.hash(&bytes));
        let table = self.table()?;
        let owner = table.owner(shard_of(key, table.shards.len() as u16))?;
        Some((owner, key))
    }

    /// The pod the table names `key`'s owner.
    #[must_use]
    pub fn owner_of(&self, key: u64) -> Option<NodeId> {
        self.owned(key).map(|(owner, _)| owner)
    }

    /// Hits applied to `key`, each once by whichever pod applied them, over every life of the key
    /// and every owner ([`World::applied`]).
    #[must_use]
    pub fn counted_ever(&self, key: u64) -> u64 {
        self.owned(key).map_or(0, |(_, hash)| self.applied(hash))
    }

    /// Hits `key`'s owner, as the table names it, has applied, each once, if it holds the key.
    #[must_use]
    pub fn counted(&self, key: u64) -> Option<u64> {
        let (owner, hash) = self.owned(key)?;
        let pod = self.pods.get(&owner)?;
        pod.node
            .value(hash)
            .and_then(asynchronous_of)
            .map(|value| value.counted())
    }

    /// Data plane `id` stops, as one whose DataPlane leaves the mode: it decides, sends and takes
    /// nothing more, and what it did stays counted.
    pub fn end_plane(&mut self, id: NodeId) {
        if let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) {
            plane.gone = true;
        }
    }

    /// The data planes' ids, in order: of the lives running, a data plane restarted under its
    /// new id.
    #[must_use]
    pub fn plane_ids(&self) -> Vec<NodeId> {
        self.clients
            .iter()
            .filter_map(|(id, caller)| Some((id, caller.asynchronous()?)))
            .filter(|(_, plane)| !plane.gone)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The workload's arrivals now due at data plane `id`, each decided at its worker, and the
    /// next arrival's turn set.
    fn on_arrivals(&mut self, id: NodeId) {
        let now = self.now;
        self.resume(id);
        let mut due = Vec::new();
        let mut next = None;
        if let Some(Plane {
            feed: Feed::Workload(fed),
            ..
        }) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut)
        {
            while let Some(&(at, worker, kind)) = fed.arrivals.front() {
                if at > now {
                    next = Some(at);
                    break;
                }
                fed.arrivals.pop_front();
                due.push((at, worker, kind));
            }
        }
        for (at, worker, kind) in due {
            self.stats.sent += 1;
            self.decide_at_worker(id, at, worker, kind);
        }
        self.flush(id);
        if let Some(next) = next {
            self.schedule(next, Event::ClientSend(id));
        }
    }

    /// Data plane `id`'s worker `worker` decides a request of kind `kind` that came at `at`:
    /// from the entries it holds handles to, each it lacks found in its participant's registry
    /// or made there; the participant asked when an entry cannot decide it, the ask waiting
    /// while the participant is held.
    fn decide_at_worker(&mut self, id: NodeId, at: Time, worker: usize, kind: usize) {
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        let Feed::Workload(fed) = &mut plane.feed else {
            return;
        };
        let limits = fed.kinds[kind].clone();
        let registry = plane.data.registry();
        let mut made = Vec::new();
        for &(key, contract) in &limits {
            let held = fed.handles[worker]
                .get(&key)
                .is_some_and(|entry| entry.contract() == contract);
            if held {
                continue;
            }
            if let Some((entry, fresh)) =
                registry.find_or_make(&bytes_of(key), contract, CYCLE, now)
            {
                if fresh {
                    made.push(Arc::clone(&entry));
                }
                fed.handles[worker].insert(key, entry);
            }
        }
        let held: Option<Vec<Arc<Entry>>> = limits
            .iter()
            .map(|(key, contract)| {
                fed.handles[worker]
                    .get(key)
                    .filter(|entry| entry.contract() == *contract)
                    .cloned()
            })
            .collect();
        let paused = fed.paused_until.is_some_and(|until| until > now);
        for entry in made {
            plane.data.adopt(entry);
        }
        if let Some(held) = held {
            let entries: Vec<&Entry> = held.iter().map(|entry| &**entry).collect();
            let outcome = pooled::decide(&entries, now);
            // Signals reach the participant at once, as the proxy's do within a tick.
            if outcome.signal != 0 {
                let signalled = held
                    .iter()
                    .enumerate()
                    .filter(|(n, _)| outcome.signal & (1 << n) != 0)
                    .map(|(_, entry)| Arc::clone(entry));
                plane.data.on_signals(signalled);
            }
            match outcome.local {
                Local::Admit => {
                    plane.requests += 1;
                    for (key, _) in &limits {
                        plane.admitted.entry(*key).or_default().push(at);
                    }
                    return;
                }
                Local::Refuse => {
                    plane.requests += 1;
                    plane.refused += 1;
                    return;
                }
                Local::Ask => {}
            }
        }
        if paused {
            // Held: the ask waits in its queue, as on the participant's channel.
            if let Feed::Workload(fed) = &mut plane.feed {
                fed.pending.push((at, worker, kind));
            }
            return;
        }
        Self::ask(plane, at, worker, &limits, now);
    }

    /// `plane`'s participant decides a request of `limits` that came at `at` to `worker`, and
    /// gives the worker its entries to decide from itself from now on.
    fn ask(plane: &mut Plane, at: Time, worker: usize, limits: &[(u64, Contract)], now: Time) {
        let bytes: Vec<[u8; 8]> = limits.iter().map(|(key, _)| bytes_of(*key)).collect();
        let keys: Vec<(&[u8], Contract)> = bytes
            .iter()
            .zip(limits)
            .map(|(bytes, (_, contract))| (&bytes[..], *contract))
            .collect();
        let decision = plane.data.decide_all(&keys, now);
        let given = plane.data.entries(&keys);
        plane.requests += 1;
        match decision {
            Decision::Admit => {
                for (key, _) in limits {
                    plane.admitted.entry(*key).or_default().push(at);
                }
            }
            Decision::Refuse => plane.refused += 1,
        }
        if let Feed::Workload(fed) = &mut plane.feed {
            for entry in given {
                let mut key = [0; 8];
                key.copy_from_slice(entry.key());
                fed.handles[worker].insert(u64::from_le_bytes(key), entry);
            }
        }
    }

    /// Data plane `id`'s participant goes on, if it was held and its time is up: the answers it
    /// held taken, then the asks that waited.
    fn resume(&mut self, id: NodeId) {
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        let Feed::Workload(fed) = &mut plane.feed else {
            return;
        };
        if fed.paused_until.is_none_or(|until| until > now) {
            return;
        }
        fed.paused_until = None;
        let held = std::mem::take(&mut fed.held);
        let pending = std::mem::take(&mut fed.pending);
        let kinds = Arc::clone(&fed.kinds);
        fed.waited += pending.len() as u64;
        fed.waited_for += pending.iter().map(|(at, _, _)| now - at).sum::<Time>();
        for (from, message) in held {
            Self::take_answers(plane, from, message, now);
        }
        for (at, worker, kind) in pending {
            Self::ask(plane, at, worker, &kinds[kind], now);
        }
        self.flush(id);
    }

    /// Whether data plane `id`'s participant is held now.
    fn held(&self, id: NodeId) -> bool {
        matches!(
            self.clients.get(&id).and_then(Caller::asynchronous),
            Some(Plane { feed: Feed::Workload(fed), .. })
                if fed.paused_until.is_some_and(|until| until > self.now)
        )
    }

    /// What data plane `id` has to send, sent: each batch to the pod the participant routed
    /// it to.
    fn flush(&mut self, id: NodeId) {
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        let sends = plane.data.take_outgoing();
        plane.batches += sends.len() as u64;
        for (to, batch) in sends {
            self.send(id, to, Message::Batch(batch).into());
        }
    }

    /// A data plane's cycle: its batches, each to the pod the participant routed it to. First,
    /// the latest table committed, if later than the one it holds: what its stream to a seed
    /// tells it as a pod commits one, the proxy keeping that stream open to the service's
    /// address whatever its keys are doing, and opening it again on a pod alive should the one
    /// it reached go. Without it, a data plane whose keys sent nothing through a rollout would
    /// never hear of the new pods.
    pub(super) fn on_cycle(&mut self, id: NodeId) {
        let now = self.now;
        if self.gone(id) {
            return;
        }
        if self.held(id) {
            // Held up: no cycle, until it goes on.
            self.schedule(now + CYCLE, Event::ClientCheck(id));
            return;
        }
        self.resume(id);
        let latest = self.table().cloned();
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        if let Some(table) = latest {
            let told = plane.data.routing().told(table);
            if told.later(plane.data.routing()) {
                plane.data.on_routing(told, now);
            }
        }
        plane.data.on_cycle(now);
        self.flush(id);
        self.schedule(now + CYCLE, Event::ClientCheck(id));
    }

    /// Pod `from`'s message reaches a data plane: its own answers, answers relayed from their
    /// owner, or a table.
    pub(super) fn on_answers(
        &mut self,
        from: NodeId,
        id: NodeId,
        message: edgerush_cluster_modes::Message,
    ) {
        let message = match message {
            edgerush_cluster_modes::Message::Asynchronous(message) => message,
            edgerush_cluster_modes::Message::Store(message) => Message::Store(message),
            edgerush_cluster_modes::Message::Synchronous(_) => return,
        };
        let now = self.now;
        if self.gone(id) {
            return;
        }
        if self.held(id) {
            if let Some(Plane {
                feed: Feed::Workload(fed),
                ..
            }) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut)
            {
                fed.held.push((from, message));
            }
            return;
        }
        self.resume(id);
        let Some(plane) = self.clients.get_mut(&id).and_then(Caller::asynchronous_mut) else {
            return;
        };
        plane.answers += 1;
        Self::take_answers(plane, from, message, now);
        // Answered, what waited for the exchange goes now, as the proxy sends it.
        self.flush(id);
    }

    /// `from`'s answers, answers relayed from their owner, or a table, taken by `plane`'s
    /// participant.
    fn take_answers(plane: &mut Plane, from: NodeId, message: Message, now: Time) {
        let reply = match message {
            Message::Answers(answers) => plane.data.routing().reply(from, None, answers),
            Message::Relayed { owner, answers, .. } => {
                plane.data.routing().reply(owner, Some(from), answers)
            }
            Message::Store(StoreMessage::TableReply { table }) => {
                let told = plane.data.routing().told(table);
                plane.data.on_routing(told, now);
                return;
            }
            _ => return,
        };
        plane.data.on_reply(reply, now);
    }
}

/// A data plane's participant as the proxy sets it up, for a workload on `workers` workers.
///
/// # Panics
///
/// Where 128-bit atomics are not native, which the participant needs.
#[expect(
    clippy::expect_used,
    reason = "the simulation runs only where the proxy's participant does"
)]
fn fed_participant(
    id: NodeId,
    table: Option<edgerush_cluster::table::Table>,
    poll_quiet: bool,
    scheduled: bool,
    workers: usize,
) -> Participant<Keyed, Routed> {
    let mut data = Participant::new(id, CYCLE, routed(table), Keyed::new(SECRET))
        .expect("native 128-bit atomics");
    data.set_one_in_flight(true);
    data.set_exchange_timeout(EXCHANGE_TIMEOUT);
    data.set_levers(Levers { poll_quiet });
    data.set_scheduled(scheduled);
    data.set_workers(workers as u64);
    data
}
