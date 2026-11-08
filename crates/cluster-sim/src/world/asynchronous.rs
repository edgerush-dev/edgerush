//! The asynchronous mode as the world runs it: its clients are data planes — today's
//! `DataPlane`, unchanged — deciding every request from their own copies and reporting their
//! hits in batches, every cycle, to each key's owner as the table names it; its pods apply them
//! on the store and answer with the buckets.
//!
//! Today's data plane routes by the view, which the store has none of: here it is given a view
//! of one stand-in pod, and its client does what A3 brings into the data planes themselves. It
//! splits each batch the data plane makes for that pod by the owner its table names for each
//! key, and hands every answer back as the stand-in's: a pod answers only the keys it serves
//! under its lease, so an answer is its owner's as it stood, where today's data plane drops one
//! from a pod its view no longer names. An owner that has left a batch unanswered for four
//! cycles has that key's entries sent through one of the shard's replicas too, chosen by the
//! key, which forwards them, until the owner answers directly again. An owner moved is found
//! by the data plane's sending again, after four cycles, rather than at once as a view's change
//! has it.

use super::{Event, Mode, Sim, address, store_kind, store_size};
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::table::{Table, shard_of};
use edgerush_cluster::{Config, Node, NodeId, Time, ms};
use edgerush_cluster_asynchronous::{Answers, Batch, Entry, Message, Pod, Value};
use edgerush_ratelimit::service::{self, Contract, DataPlane, Decision, View};
use std::collections::BTreeMap;

/// The time between a data plane's batches.
pub const CYCLE: Time = ms(10);

/// How long an owner leaves a batch unanswered before its keys are sent through another pod
/// too: four cycles, as long as a data plane waits to send a batch again.
const UNANSWERED: Time = 4 * CYCLE;

/// The one pod today's data plane is told of, standing for whichever owns each key.
const STAND_IN: service::PodId = 0;

/// The asynchronous mode: data planes decide, owners apply.
#[derive(Clone, Copy, Debug)]
pub struct Asynchronous;

/// A data plane as the world runs it: today's, under keys it shares with the others.
pub struct Plane {
    data: DataPlane,
    /// The keys it takes requests under, in turn.
    keys: Vec<u64>,
    next: usize,
    contract: Contract,
    interval: Time,
    /// The table it has learnt, from answers and the store's replies.
    table: Option<Table>,
    shards: u16,
    /// Pods it asks, in turn, when it has no owner to send to.
    asked: usize,
    /// When each key's requests were admitted, by the world's clock.
    admitted: BTreeMap<u64, Vec<Time>>,
    /// Requests it decided, and refused.
    requests: u64,
    refused: u64,
    /// Since when each pod has left a batch unanswered, cleared by its next direct answer.
    waiting: BTreeMap<NodeId, Time>,
}

impl Plane {
    /// The owner its table names for `key`.
    fn owner(&self, key: u64) -> Option<NodeId> {
        self.table
            .as_ref()
            .and_then(|table| table.owner(shard_of(key, self.shards)))
    }

    /// The pod to send `key` through as well as to its owner at `now`, if its owner has left a
    /// batch unanswered for four cycles: one of its shard's other replicas, chosen by the key,
    /// as the synchronous mode's data plane chooses (`Plane::through`), to be forwarded. Sent
    /// both ways, a batch is applied once, by its number.
    fn through(&self, key: u64, now: Time) -> Option<NodeId> {
        let owner = self.owner(key)?;
        let since = self.waiting.get(&owner).copied()?;
        if now.saturating_sub(since) < UNANSWERED {
            return None;
        }
        let table = self.table.as_ref()?;
        let shard = table.shards.get(usize::from(shard_of(key, self.shards)))?;
        let others: Vec<NodeId> = shard
            .replicas
            .iter()
            .copied()
            .filter(|replica| *replica != owner)
            .collect();
        let index = usize::try_from(key % others.len().max(1) as u64).unwrap_or(0);
        others.get(index).copied()
    }

    /// Takes `table` if later than its own.
    fn learn(&mut self, table: Table) {
        if self
            .table
            .as_ref()
            .is_none_or(|held| held.stamp() < table.stamp())
        {
            self.table = Some(table);
        }
    }

    /// The next pod to ask when it has no owner to send to: its table's members in turn, or
    /// with none, the pods alive.
    fn someone(&mut self, pods: &[NodeId]) -> Option<NodeId> {
        let members: Vec<NodeId> = self
            .table
            .as_ref()
            .map(|table| table.members.keys().copied().collect())
            .filter(|members: &Vec<NodeId>| !members.is_empty())
            .unwrap_or_else(|| pods.to_vec());
        let pod = members.get(self.asked % members.len().max(1)).copied();
        self.asked += 1;
        pod
    }
}

/// A key's bytes as today's data plane holds it, and back: the store's key, little-endian.
fn bytes_of(key: u64) -> [u8; 8] {
    key.to_le_bytes()
}

fn key_of(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
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
    pub fn add_planes(&mut self, count: u64, keys: &[u64], rate: u64, contract: Contract) {
        let interval = 1_000_000_000 / rate.max(1);
        let table = self.table().cloned();
        for _ in 0..count {
            let id = self.next_client;
            self.next_client += 1;
            let view = View {
                epoch: 1,
                pods: vec![STAND_IN],
            };
            let plane = Plane {
                data: DataPlane::new(id, CYCLE, view),
                keys: keys.to_vec(),
                next: usize::try_from(self.rng.below(keys.len() as u64)).unwrap_or(0),
                contract,
                interval,
                table: table.clone(),
                shards: self.config.shards,
                asked: 0,
                admitted: BTreeMap::new(),
                requests: 0,
                refused: 0,
                waiting: BTreeMap::new(),
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
    /// applied (`DataPlane::forgiven`).
    #[must_use]
    pub fn forgiven(&self) -> u64 {
        self.clients
            .values()
            .map(|plane| plane.data.forgiven())
            .sum()
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
            match plane.data.decide(&bytes_of(key), plane.contract, now) {
                Decision::Admit => plane.admitted.entry(key).or_default().push(now),
                Decision::Refuse => plane.refused += 1,
            }
        }
        self.stats.sent += 1;
        self.schedule(now + interval, Event::ClientSend(id));
    }

    /// A data plane's cycle: its batch for the stand-in split by the owner its table names for
    /// each key; a batch for no owner, or an empty one asking for the table, to a pod in turn.
    fn on_cycle(&mut self, id: NodeId) {
        let now = self.now;
        let pods = self.live_pods();
        let Some(plane) = self.clients.get_mut(&id) else {
            return;
        };
        plane.data.on_cycle(now);
        let stamp = plane.table.as_ref().map_or((0, 0), Table::stamp);
        let mut sends: Vec<(NodeId, Batch)> = Vec::new();
        for (_, batch) in plane.data.take_outgoing() {
            let mut by_owner: BTreeMap<Option<NodeId>, Vec<Entry>> = BTreeMap::new();
            for entry in batch.entries {
                let Some(key) = key_of(&entry.key) else {
                    continue;
                };
                let entry = Entry {
                    key,
                    contract: entry.contract,
                    report: entry.report,
                };
                if let Some(through) = plane.through(key, now) {
                    by_owner.entry(Some(through)).or_default().push(entry);
                }
                by_owner.entry(plane.owner(key)).or_default().push(entry);
            }
            if by_owner.is_empty() {
                by_owner.insert(None, Vec::new());
            }
            for (owner, entries) in by_owner {
                let Some(to) = owner.or_else(|| plane.someone(&pods)) else {
                    continue;
                };
                if !entries.is_empty() {
                    plane.waiting.entry(to).or_insert(now);
                }
                sends.push((
                    to,
                    Batch {
                        from: id,
                        stamp,
                        made: batch.made,
                        entries,
                    },
                ));
            }
        }
        for (to, batch) in sends {
            self.send(id, to, Message::Batch(batch));
        }
        self.schedule(now + CYCLE, Event::ClientCheck(id));
    }

    /// Pod `from`'s answers reach a data plane, its own or relayed: the table taken if later,
    /// then the answers handed on as the stand-in's. A pod answering directly is no longer
    /// waited on.
    fn on_answers(&mut self, from: NodeId, id: NodeId, message: Message) {
        let now = self.now;
        let Some(plane) = self.clients.get_mut(&id) else {
            return;
        };
        let answers = match message {
            Message::Answers(answers) => {
                plane.waiting.remove(&from);
                answers
            }
            Message::Relayed { answers, .. } => answers,
            Message::Store(StoreMessage::TableReply { table }) => {
                plane.learn(table);
                return;
            }
            _ => return,
        };
        if let Some(table) = answers.table {
            plane.learn(table);
        }
        let entries = answers
            .entries
            .iter()
            .map(|answer| service::Answer {
                key: Box::from(bytes_of(answer.key)),
                behind: answer.answered.behind,
                applied: answer.answered.applied,
                others: answer.answered.others,
            })
            .collect();
        plane.data.on_answers(
            service::Answers {
                from: STAND_IN,
                entries,
                view: None,
                life: answers.life,
                number: answers.number,
                made: answers.made,
            },
            now,
        );
    }
}
