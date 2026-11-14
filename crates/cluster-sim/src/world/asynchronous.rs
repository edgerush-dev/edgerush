//! The asynchronous mode as the world runs it: its clients are data planes — the proxy's
//! participant (`pooled::Participant`), routed by the table it was told (`Routed`) — deciding
//! every request from their own copies and reporting their hits in batches, every cycle, to
//! each key's owner as the table names it; its pods apply them on the store and answer with
//! the buckets. A data plane runs as the proxy runs it: one exchange in flight to each pod, an
//! exchange unanswered for 300 ms taken as failed, only the keys that need it visited each
//! cycle, quiet keys not polled. An owner that has left a batch unanswered for four cycles
//! has its keys' entries sent through one of the shard's replicas too, which forwards them,
//! until the owner answers directly again.

use super::{Event, Mode, Sim, address, store_kind, store_size};
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::{Config, Node, NodeId, Time, ms};
use edgerush_cluster_asynchronous::{Answers, Message, Pod, Routed, Value};
use edgerush_ratelimit::service::pooled::{Decision, Levers, Participant, Routing};
use edgerush_ratelimit::service::{Contract, Keyed};
use std::collections::BTreeMap;

/// The time between a data plane's batches.
pub const CYCLE: Time = ms(10);

/// How long a data plane waits for an exchange's answer before taking it as failed: the
/// proxy's.
const EXCHANGE_TIMEOUT: Time = ms(300);

/// What every data plane hashes its keys under, as the proxy's share `key_secret`.
const SECRET: [u64; 2] = [1, 2];

/// The asynchronous mode: data planes decide, owners apply.
#[derive(Clone, Copy, Debug)]
pub struct Asynchronous;

/// A data plane as the world runs it: the proxy's participant, under keys it shares with the
/// others.
pub struct Plane {
    data: Participant<Keyed, Routed>,
    /// The keys it takes requests under, in turn.
    keys: Vec<u64>,
    next: usize,
    contract: Contract,
    interval: Time,
    /// When each key's requests were admitted, by the world's clock.
    admitted: BTreeMap<u64, Vec<Time>>,
    /// Requests it decided, and refused.
    requests: u64,
    refused: u64,
}

/// A key's bytes as a client sends it: the world's key, little-endian. The data plane hashes
/// them into the store's key.
fn bytes_of(key: u64) -> [u8; 8] {
    key.to_le_bytes()
}

impl Mode for Asynchronous {
    type Value = Value;
    type Pod = Pod;
    type Message = Message;
    type Client = Plane;

    fn pod(id: NodeId, config: Config, peers: Vec<NodeId>, _: bool, _: bool) -> Pod {
        let mut pod = Pod::new(id, address(id), config, peers, CYCLE);
        // A pod is a member in one life only, and no pod is 0: its id serves as its life.
        pod.set_life(id);
        pod
    }

    fn store(pod: &Pod) -> &Node<Value> {
        pod.store()
    }

    fn store_mut(pod: &mut Pod) -> &mut Node<Value> {
        pod.store_mut()
    }

    fn tick(pod: &mut Pod, now: Time) -> Vec<(NodeId, Message)> {
        pod.tick(now)
    }

    fn receive(pod: &mut Pod, from: NodeId, message: Message, now: Time) -> Vec<(NodeId, Message)> {
        pod.receive(from, message, now)
    }

    fn as_store(message: &Message) -> Option<&StoreMessage<Value>> {
        match message {
            Message::Store(message) => Some(message),
            _ => None,
        }
    }

    fn into_store(message: Message) -> Option<StoreMessage<Value>> {
        match message {
            Message::Store(message) => Some(message),
            _ => None,
        }
    }

    fn kind(message: &Message) -> &'static str {
        match message {
            Message::Store(message) => store_kind(message),
            Message::Batch(_) => "batch",
            Message::Answers(Answers { table: Some(_), .. }) => "answers with a table",
            Message::Answers(_) => "answers",
            Message::Forwarded { .. } => "batch forwarded",
            Message::Relayed { .. } => "answers relayed",
        }
    }

    /// A batch's entry as today's wire bounds it less its key's bytes (an eight-byte key), an
    /// answer a key and three numbers, and a table as the synchronous mode's reply carries it.
    fn size(message: &Message) -> u64 {
        match message {
            Message::Store(message) => store_size(message),
            Message::Batch(batch) | Message::Forwarded { batch, .. } => {
                64 + 136 * batch.entries.len() as u64
            }
            Message::Answers(answers) | Message::Relayed { answers, .. } => {
                64 + 48 * answers.entries.len() as u64
                    + 16 * answers.unserved.len() as u64
                    + answers
                        .table
                        .as_ref()
                        .map_or(0, |table| table.shards.len() as u64 * 24)
            }
        }
    }

    fn on_send(_: &mut Sim<Self>, _: NodeId, _: NodeId, _: &Message) {}

    /// Lost, as a batch is when its exchange fails: the data plane sends it again after four
    /// cycles unseen.
    fn unreachable(_: &mut Sim<Self>, _: NodeId, _: NodeId, _: &Message) {}

    fn client_send(world: &mut Sim<Self>, id: NodeId) {
        world.on_request(id);
    }

    fn client_check(world: &mut Sim<Self>, id: NodeId) {
        world.on_cycle(id);
    }

    fn client_unreachable(_: &mut Sim<Self>, _: NodeId, _: u64, _: NodeId) {}

    fn deliver_to_client(world: &mut Sim<Self>, from: NodeId, id: NodeId, message: Message) {
        world.on_answers(from, id, message);
    }
}

impl Sim<Asynchronous> {
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
            let mut data =
                Participant::new(id, CYCLE, Routed::new(table.clone()), Keyed::new(SECRET))
                    .expect("native 128-bit atomics");
            data.set_one_in_flight(true);
            data.set_exchange_timeout(EXCHANGE_TIMEOUT);
            data.set_levers(Levers { poll_quiet: false });
            data.set_scheduled(true);
            let plane = Plane {
                data,
                keys: keys.to_vec(),
                next: usize::try_from(self.rng.below(keys.len() as u64)).unwrap_or(0),
                contract,
                interval,
                admitted: BTreeMap::new(),
                requests: 0,
                refused: 0,
            };
            self.clients.insert(id, plane);
            self.asked.extend(keys.iter().copied());
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
            .map(|plane| plane.data.forgiven())
            .sum()
    }

    /// The pods data plane `id` has, by now, left a batch unanswered for four cycles
    /// (`Participant::unanswered`).
    #[must_use]
    pub fn unanswered(&self, id: NodeId) -> Vec<NodeId> {
        self.clients
            .get(&id)
            .map(|plane| plane.data.unanswered(self.now).collect())
            .unwrap_or_default()
    }

    /// When each key's requests were admitted, every data plane's together, in order.
    #[must_use]
    pub fn admissions(&self) -> BTreeMap<u64, Vec<Time>> {
        let mut all: BTreeMap<u64, Vec<Time>> = BTreeMap::new();
        for plane in self.clients.values() {
            for (key, times) in &plane.admitted {
                all.entry(*key).or_default().extend(times);
            }
        }
        for times in all.values_mut() {
            times.sort_unstable();
        }
        all
    }

    /// A request reaches data plane `id`, under its next key: decided from its copy.
    fn on_request(&mut self, id: NodeId) {
        if self.clients_stopped {
            return;
        }
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id) else {
            return;
        };
        let interval = plane.interval;
        if let Some(&key) = plane.keys.get(plane.next % plane.keys.len().max(1)) {
            plane.next += 1;
            plane.requests += 1;
            let bytes = bytes_of(key);
            match plane.data.decide_all(&[(&bytes, plane.contract)], now) {
                Decision::Admit => plane.admitted.entry(key).or_default().push(now),
                Decision::Refuse => plane.refused += 1,
            }
        }
        self.stats.sent += 1;
        self.schedule(now + interval, Event::ClientSend(id));
    }

    /// A data plane's cycle: its batches, each to the pod the participant routed it to. First,
    /// the latest table committed, if later than the one it holds: what its stream to a seed
    /// tells it as a pod commits one, the proxy keeping that stream open to the service's
    /// address whatever its keys are doing, and opening it again on a pod alive should the one
    /// it reached go. Without it, a data plane whose keys sent nothing through a rollout would
    /// never hear of the new pods.
    fn on_cycle(&mut self, id: NodeId) {
        let now = self.now;
        let latest = self.table().cloned();
        let Some(plane) = self.clients.get_mut(&id) else {
            return;
        };
        if let Some(table) = latest {
            let told = Routed::new(Some(table));
            if told.later(plane.data.routing()) {
                plane.data.on_routing(told, now);
            }
        }
        plane.data.on_cycle(now);
        let sends = plane.data.take_outgoing();
        for (to, batch) in sends {
            self.send(id, to, Message::Batch(batch));
        }
        self.schedule(now + CYCLE, Event::ClientCheck(id));
    }

    /// Pod `from`'s message reaches a data plane: its own answers, answers relayed from their
    /// owner, or a table.
    fn on_answers(&mut self, from: NodeId, id: NodeId, message: Message) {
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id) else {
            return;
        };
        let reply = match message {
            Message::Answers(answers) => Routed::reply(from, None, answers),
            Message::Relayed { owner, answers, .. } => Routed::reply(owner, Some(from), answers),
            Message::Store(StoreMessage::TableReply { table }) => {
                plane.data.on_routing(Routed::new(Some(table)), now);
                return;
            }
            _ => return,
        };
        plane.data.on_reply(reply, now);
        // Answered, what waited for the exchange goes now, as the proxy sends it.
        let sends = plane.data.take_outgoing();
        for (to, batch) in sends {
            self.send(id, to, Message::Batch(batch));
        }
    }
}
