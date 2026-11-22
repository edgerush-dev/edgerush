//! The synchronous mode's part of the world: its clients send requests to keys' owners, are
//! answered admitted, refused or counted, and give back what a refused request was charged.

use super::both::{Caller, synchronous_of};
use super::{CLIENT_BASE, Event, Sim, Synchronous, store_kind, store_size};
use crate::client::{Client, Multi};
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::{Key, NodeId, ShardId, Time, ms};
use edgerush_cluster_synchronous::{Answer, Value};
use std::collections::BTreeMap;

/// The mode's messages, over the store of both modes' values.
type Message = edgerush_cluster_synchronous::Message<edgerush_cluster_modes::Value>;

impl Sim<Synchronous> {
    /// What the mode watches of every message sent: a give-back sent, as many times as it is;
    /// when an answer under a rule first leaves its owner, by its wall clock; and a request
    /// answered "counted" twice.
    pub(super) fn watch_sent(&mut self, from: NodeId, to: NodeId, message: &Message) {
        if let Message::Request {
            client,
            id,
            give_back: true,
            ..
        } = message
        {
            self.give_backs.insert((*client, *id));
            self.stats.give_back_sends += 1;
        }
        // Decided where an answer under a rule first leaves its owner, by its wall clock.
        if let Message::Reply {
            client,
            id,
            answer: Answer::Admitted(_) | Answer::Limited { .. },
        } = message
        {
            let skew = self.pods.get(&from).map_or(0, |pod| pod.skew);
            let at = self.now.saturating_add_signed(skew);
            self.decided.entry((*client, *id)).or_insert((at, self.now));
        }
        if let Message::Reply {
            client,
            id,
            answer: Answer::Counted(_) | Answer::Admitted(_),
        } = message
            && to >= CLIENT_BASE
            && !self.give_backs.contains(&(*client, *id))
            && !self.answered_counted.insert((*client, *id))
        {
            self.stats.counted_twice += 1;
        }
    }

    /// Whether pods started from now forward a request for a shard not theirs.
    pub fn set_forwarding(&mut self, forwarding: bool) {
        self.forwarding = forwarding;
        for pod in self.pods.values_mut() {
            pod.node.set_forwarding(forwarding);
        }
    }

    /// Whether pods started from now, forwarding, decide a request they would forward from
    /// their copy, its owner unheard.
    pub fn set_forwarder_decides(&mut self, decides: bool) {
        self.forwarder_decides = decides;
        for pod in self.pods.values_mut() {
            pod.node.set_forwarder_decides(decides);
        }
    }

    /// Requests every pod decided from its copy, their owner unheard.
    #[must_use]
    pub fn decided_from_copy(&self) -> u64 {
        self.pods
            .values()
            .map(|pod| pod.node.decided_from_copy())
            .sum()
    }
    /// A client under `keys`, a request every `interval`, each refused after `deadline`.
    pub fn add_client(&mut self, keys: Vec<Key>, interval: Time, deadline: Time) -> NodeId {
        let id = self.next_client;
        self.next_client += 1;
        let mut client = Client::new(id, keys, interval, deadline, self.config.shards);
        if let Some((rule, _)) = self.limit {
            client.set_rule(rule);
        }
        self.clients.insert(id, Caller::Synchronous(client));
        let first = self.now + self.rng.below(interval.max(1));
        self.schedule(first, Event::ClientSend(id));
        self.schedule(self.now + ms(1), Event::ClientCheck(id));
        id
    }
    /// `count` clients, each under 32 keys of its own, `rate` requests a second each.
    pub fn add_clients(&mut self, count: u64, rate: u64, deadline: Time) {
        for client in 0..count {
            let keys = (0..32).map(|key| Key::from(client * 1_000 + key)).collect();
            self.add_client(keys, 1_000_000_000 / rate.max(1), deadline);
        }
    }
    /// `count` clients as [`add_clients`](Self::add_clients) makes, each request also under
    /// `shared`, a key every client shares, asked as `multi` says.
    pub fn add_clients_multi(
        &mut self,
        count: u64,
        rate: u64,
        deadline: Time,
        multi: Multi,
        shared: Key,
    ) {
        let first = self.next_client;
        self.add_clients(count, rate, deadline);
        for id in first..self.next_client {
            if let Some(client) = self.clients.get_mut(&id).and_then(Caller::synchronous_mut) {
                client.set_multi(multi, shared);
            }
        }
        self.asked.insert(shared);
    }
    /// Admissions the shards' owners count beyond what clients were told: parts counted for
    /// requests refused, and requests counted twice.
    #[must_use]
    pub fn overcount(&self) -> u64 {
        let Some(table) = self.table() else {
            return 0;
        };
        let shards = self.config.shards;
        self.asked
            .iter()
            .map(|key| {
                let owner = table
                    .owner(edgerush_cluster::table::shard_of(*key, shards))
                    .and_then(|owner| self.node(owner));
                // Under a window, the owner holds the window it last counted in.
                if let Some(oracle) = &self.oracle {
                    let held = owner
                        .and_then(|node| node.value(*key))
                        .and_then(synchronous_of)
                        .unwrap_or_default();
                    return oracle.beyond_told(*key, held.window(), held.net());
                }
                let told = self.truth.get(key).copied().unwrap_or(0);
                let held = owner
                    .and_then(|node| {
                        node.value(*key)
                            .and_then(synchronous_of)
                            .map(|value| value.net())
                    })
                    .unwrap_or(0);
                held.saturating_sub(told)
            })
            .sum()
    }
    /// Parts counted for requests then refused, as clients and gathering pods saw them.
    #[must_use]
    pub fn orphaned(&self) -> u64 {
        self.clients
            .values()
            .filter_map(Caller::synchronous)
            .map(|client| client.orphaned)
            .sum::<u64>()
            + self
                .pods
                .values()
                .map(|pod| pod.node.orphaned())
                .sum::<u64>()
    }
    /// `per_shard` keys in every shard, each counted once, held by its owner and its replicas
    /// as the table says, as if the cluster had served them long: for failovers and moves that
    /// carry many keys. They are no client's, and count in nothing measured.
    pub fn preload(&mut self, per_shard: u64) {
        let Some(table) = self.table().cloned() else {
            return;
        };
        let shards = u64::from(self.config.shards);
        let mut key: Key = 1 << 40;
        let mut placed = 0;
        while placed < per_shard * shards {
            key += 1;
            let shard = edgerush_cluster::table::shard_of(key, self.config.shards);
            let Some(entry) = table.shards.get(usize::from(shard)) else {
                continue;
            };
            for holder in entry.owner.iter().chain(&entry.replicas) {
                if let Some(pod) = self.pods.get_mut(holder) {
                    pod.node.store_mut().load(
                        key,
                        edgerush_cluster_modes::Value::Synchronous(Value::counted(1)),
                    );
                }
            }
            placed += 1;
        }
    }
    /// Copies behind their owner: over every key asked under, each live replica of its shard
    /// holding less for it than the owner does.
    #[must_use]
    pub fn stale_copies(&self) -> u64 {
        let Some(table) = self.table() else {
            return 0;
        };
        let shards = self.config.shards;
        self.asked
            .iter()
            .map(|key| {
                let shard = edgerush_cluster::table::shard_of(*key, shards);
                let Some(entry) = table.shards.get(usize::from(shard)) else {
                    return 0;
                };
                let held = entry
                    .owner
                    .and_then(|owner| self.node(owner))
                    .and_then(|node| {
                        node.value(*key)
                            .and_then(synchronous_of)
                            .map(|value| value.net())
                    })
                    .unwrap_or(0);
                let behind = entry.replicas.iter().filter(|replica| {
                    self.is_alive(**replica)
                        && self.node(**replica).is_some_and(|node| {
                            node.value(*key)
                                .and_then(synchronous_of)
                                .map(|value| value.net())
                                .unwrap_or(0)
                                < held
                        })
                });
                behind.count() as u64
            })
            .sum()
    }
    /// Admissions clients were told of that the shards' owners do not now count: how far a
    /// limit would be overshot.
    #[must_use]
    pub fn lost(&self) -> u64 {
        let Some(table) = self.table() else {
            return self.truth.values().sum();
        };
        let shards = self.config.shards;
        self.truth
            .iter()
            .map(|(key, told)| {
                let owner = table.owner(edgerush_cluster::table::shard_of(*key, shards));
                let held = owner
                    .and_then(|owner| self.node(owner))
                    .and_then(|node| {
                        node.value(*key)
                            .and_then(synchronous_of)
                            .map(|value| value.net())
                    })
                    .unwrap_or(0);
                told.saturating_sub(held)
            })
            .sum()
    }
    /// For each shard where admissions were lost: the shard, its owner, admissions told and
    /// what the owner holds.
    #[must_use]
    pub fn lost_by_shard(&self) -> Vec<(ShardId, Option<NodeId>, u64, u64)> {
        let Some(table) = self.table() else {
            return Vec::new();
        };
        let shards = self.config.shards;
        let mut by: BTreeMap<ShardId, (u64, u64)> = BTreeMap::new();
        for (key, told) in &self.truth {
            let shard = edgerush_cluster::table::shard_of(*key, shards);
            let held = table
                .owner(shard)
                .and_then(|owner| self.node(owner))
                .and_then(|node| {
                    node.value(*key)
                        .and_then(synchronous_of)
                        .map(|value| value.net())
                })
                .unwrap_or(0);
            let entry = by.entry(shard).or_insert((0, 0));
            entry.0 += told;
            entry.1 += held.min(*told);
        }
        by.into_iter()
            .filter(|(_, (told, held))| told > held)
            .map(|(shard, (told, held))| (shard, table.owner(shard), told, held))
            .collect()
    }
    pub(super) fn on_client_send(&mut self, id: NodeId) {
        if self.clients_stopped {
            return;
        }
        let pods = self.live_pods();
        let now = self.now;
        let Some(client) = self.clients.get_mut(&id).and_then(Caller::synchronous_mut) else {
            return;
        };
        let interval = client.interval;
        let before = client.requests;
        let sent: Vec<(NodeId, Message)> = if client.splits() {
            client.send_split(now, &pods, &mut self.rng)
        } else {
            client.send(now, &pods, &mut self.rng).into_iter().collect()
        };
        // Made, whether sent or waiting for a stream to some pod.
        self.stats.sent += client.requests - before;
        for (to, message) in sent {
            if let Message::Request { key, .. } = &message {
                self.asked.insert(*key);
            }
            if let Message::Gather { keys, .. } = &message {
                self.asked.extend(keys.iter().copied());
            }
            self.send(id, to, message.into());
        }
        self.schedule(now + interval, Event::ClientSend(id));
    }
    pub(super) fn on_client_check(&mut self, id: NodeId) {
        let pods = self.live_pods();
        let now = self.now;
        let Some(client) = self.clients.get_mut(&id).and_then(Caller::synchronous_mut) else {
            return;
        };
        let (ended, sends) = client.check(now, &pods, &mut self.rng);
        for (keys, outcome) in ended {
            self.record(id, outcome, &keys);
        }
        for (to, message) in sends {
            self.send(id, to, message.into());
        }
        self.schedule(now + ms(1), Event::ClientCheck(id));
    }
    /// `from`, a client, told soon that `message` could not reach pod `to`.
    pub(super) fn unreachable(&mut self, from: NodeId, to: NodeId, message: &Message) {
        if from < CLIENT_BASE || to >= CLIENT_BASE {
            return;
        }
        let id = match message {
            Message::Request { id, .. } | Message::Gather { id, .. } => *id,
            _ => return,
        };
        self.stats.unreachable += 1;
        let at = self.now + self.link(from, to).delay;
        self.schedule(
            at,
            Event::Unreachable {
                client: from,
                id,
                pod: to,
            },
        );
    }
    pub(super) fn on_unreachable(&mut self, client: NodeId, id: u64, pod: NodeId) {
        let pods = self.live_pods();
        let now = self.now;
        let Some(entry) = self
            .clients
            .get_mut(&client)
            .and_then(Caller::synchronous_mut)
        else {
            return;
        };
        if let Some((to, message)) = entry.unreachable(id, pod, now, &pods, &mut self.rng) {
            self.send(client, to, message.into());
        }
    }
    pub(super) fn deliver_to_client(
        &mut self,
        id: NodeId,
        message: edgerush_cluster_modes::Message,
    ) {
        let message = match message {
            edgerush_cluster_modes::Message::Synchronous(message) => message,
            edgerush_cluster_modes::Message::Store(message) => Message::Store(message),
            edgerush_cluster_modes::Message::Asynchronous(_) => return,
        };
        let pods = self.live_pods();
        let now = self.now;
        let Some(client) = self.clients.get_mut(&id).and_then(Caller::synchronous_mut) else {
            return;
        };
        match message {
            Message::Reply {
                id: request,
                answer,
                ..
            } => {
                let (ended, sends) = client.answered(request, answer, now, &pods, &mut self.rng);
                for (to, message) in sends {
                    self.send(id, to, message.into());
                }
                if let Some((keys, outcome)) = ended {
                    self.record(id, outcome, &keys);
                }
            }
            Message::Store(StoreMessage::TableReply { table }) => client.learn(table),
            _ => {}
        }
    }
}

/// The name of `message`'s kind, and for an answer, which answer.
pub(super) const fn kind(message: &Message) -> &'static str {
    match message {
        Message::Store(message) => store_kind(message),
        Message::Request { .. } | Message::Gather { .. } => "request",
        Message::Reply {
            answer: Answer::Elsewhere(Some(_)),
            ..
        } => "reply with a table",
        Message::Reply { .. } => "reply",
    }
}

/// About how many bytes `message` takes on the wire: what a link's bandwidth is spent on.
pub(super) fn size(message: &Message) -> u64 {
    match message {
        Message::Store(message) => store_size(message),
        Message::Reply {
            answer: Answer::Elsewhere(Some(table)),
            ..
        } => 32 + table.shards.len() as u64 * 24,
        Message::Request { .. } | Message::Gather { .. } | Message::Reply { .. } => 32,
    }
}
