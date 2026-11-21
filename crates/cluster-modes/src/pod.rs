//! A pod of both modes in one life: one store, the synchronous mode's requests decided by its
//! `Decider` and the asynchronous mode's batches applied by its `Applier`, each over the store,
//! the store's messages its own.

use crate::{Kind, Store, Value};
use edgerush_cluster::{Config, NodeId, Time};
use edgerush_cluster_asynchronous::{self as asynchronous, Applier, TurnedAway};
use edgerush_cluster_synchronous::{self as synchronous, Answer, Decider};

/// One message between pods, or between a data plane and a pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// The store's: membership, tables, copies and handoffs; to a data plane, a table.
    Store(edgerush_cluster::Message<Value>),
    /// The synchronous mode's: requests and their answers.
    Synchronous(synchronous::Message<Value>),
    /// The asynchronous mode's: batches and their answers.
    Asynchronous(asynchronous::Message<Value>),
}

impl From<edgerush_cluster::Message<Value>> for Message {
    fn from(message: edgerush_cluster::Message<Value>) -> Self {
        Self::Store(message)
    }
}

impl From<synchronous::Message<Value>> for Message {
    /// The store's messages among the mode's as the store's.
    fn from(message: synchronous::Message<Value>) -> Self {
        match message {
            synchronous::Message::Store(message) => Self::Store(message),
            message => Self::Synchronous(message),
        }
    }
}

impl From<asynchronous::Message<Value>> for Message {
    /// The store's messages among the mode's as the store's.
    fn from(message: asynchronous::Message<Value>) -> Self {
        match message {
            asynchronous::Message::Store(message) => Self::Store(message),
            message => Self::Asynchronous(message),
        }
    }
}

/// What a pod gives back to be sent: each message with whom it is for.
pub type Outbox = Vec<(NodeId, Message)>;

/// One pod of both modes in one life: its store, and each mode's part over it.
#[derive(Clone, Debug)]
pub struct Pod {
    node: Store,
    decider: Decider,
    applier: Applier,
    /// Requests and entries not served, their keys of the other mode's.
    wrong_kind: u64,
}

impl Pod {
    /// Pod `id`, reached at `address`, new, knowing how to reach `peers`, its asynchronous data
    /// planes sending every `cycle` nanoseconds.
    #[must_use]
    pub fn new(
        id: NodeId,
        address: String,
        config: Config,
        peers: impl IntoIterator<Item = NodeId>,
        cycle: Time,
    ) -> Self {
        Self {
            node: Store::new(id, address, config, peers),
            decider: Decider::default(),
            applier: Applier::new(cycle),
            wrong_kind: 0,
        }
    }

    /// Its store.
    #[must_use]
    pub const fn store(&self) -> &Store {
        &self.node
    }

    /// Its store, to change: told to drain, its wall clock set, keys loaded.
    pub const fn store_mut(&mut self) -> &mut Store {
        &mut self.node
    }

    /// As the synchronous mode's [`Decider::set_forwarding`].
    pub const fn set_forwarding(&mut self, forwarding: bool) {
        self.decider.set_forwarding(forwarding);
    }

    /// As the synchronous mode's [`Decider::set_forwarder_decides`].
    pub const fn set_forwarder_decides(&mut self, decides: bool) {
        self.decider.set_forwarder_decides(decides);
    }

    /// As the asynchronous mode's [`Applier::set_life`].
    pub const fn set_life(&mut self, life: u64) {
        self.applier.set_life(life);
    }

    /// Synchronous requests it decided from its copy, their owner unheard.
    #[must_use]
    pub const fn decided_from_copy(&self) -> u64 {
        self.decider.decided_from_copy()
    }

    /// As the synchronous mode's [`Decider::orphaned`].
    #[must_use]
    pub const fn orphaned(&self) -> u64 {
        self.decider.orphaned()
    }

    /// What it has answered the asynchronous mode's data planes unserved in this life, by why.
    #[must_use]
    pub const fn turned_away(&self) -> TurnedAway {
        self.applier.turned_away()
    }

    /// Requests, and entries of batches, it did not serve, their keys carrying the other
    /// mode's bit: a data plane's mistake, answered as not served — a request
    /// [`Answer::NotServing`], an entry unserved — so that nothing is held under a key of the
    /// wrong kind.
    #[must_use]
    pub const fn wrong_kind(&self) -> u64 {
        self.wrong_kind
    }

    /// The value it holds for `key`, owned or a copy.
    #[must_use]
    pub fn value(&self, key: edgerush_cluster::Key) -> Option<Value> {
        self.node.value(key)
    }

    /// Its timer at `now`: its store's, then the synchronous mode's requests it gathers; the
    /// asynchronous mode keeps no timer of its own.
    pub fn tick(&mut self, now: Time) -> Outbox {
        let mut out = from_store(self.node.tick(now));
        out.extend(
            self.decider
                .tick(&mut self.node, now)
                .into_iter()
                .map(|(to, message)| (to, message.into())),
        );
        out
    }

    /// `message` from `from` at `now`: the store's taken by the store, each mode's by that
    /// mode, a data plane's whose keys are of the other mode's not served.
    pub fn receive(&mut self, from: NodeId, message: impl Into<Message>, now: Time) -> Outbox {
        match message.into() {
            Message::Store(message) => from_store(self.node.receive(from, message, now)),
            Message::Synchronous(message) => {
                if let Some(refused) = self.refused_synchronous(&message) {
                    return vec![refused];
                }
                self.decider
                    .receive(&mut self.node, from, message, now)
                    .into_iter()
                    .map(|(to, message)| (to, message.into()))
                    .collect()
            }
            Message::Asynchronous(message) => self.receive_asynchronous(from, message, now),
        }
    }

    /// A synchronous request or request to gather under a key of the asynchronous mode's,
    /// answered not served, to whom its answer goes; none for one under keys of its own mode.
    fn refused_synchronous(
        &mut self,
        message: &synchronous::Message<Value>,
    ) -> Option<(NodeId, Message)> {
        let (client, id, via, wrong) = match message {
            synchronous::Message::Request {
                client,
                id,
                key,
                via,
                ..
            } => (*client, *id, *via, Kind::of(*key) != Kind::Synchronous),
            synchronous::Message::Gather { client, id, keys } => (
                *client,
                *id,
                None,
                keys.iter().any(|key| Kind::of(*key) != Kind::Synchronous),
            ),
            synchronous::Message::Reply { .. } | synchronous::Message::Store(_) => return None,
        };
        if !wrong {
            return None;
        }
        self.wrong_kind = self.wrong_kind.saturating_add(1);
        let reply = synchronous::Message::Reply {
            client,
            id,
            answer: Answer::NotServing,
        };
        Some((via.unwrap_or(client), Message::Synchronous(reply)))
    }

    /// An asynchronous message: a data plane's batch with its entries under keys of the
    /// synchronous mode's taken out and answered unserved, the rest as the mode takes them.
    fn receive_asynchronous(
        &mut self,
        from: NodeId,
        message: asynchronous::Message<Value>,
        now: Time,
    ) -> Outbox {
        let mut wrong = Vec::new();
        let message = match message {
            asynchronous::Message::Batch(mut batch) => {
                batch.entries.retain(|entry| {
                    let right = Kind::of(entry.key) == Kind::Asynchronous;
                    if !right {
                        wrong.push(entry.key);
                    }
                    right
                });
                asynchronous::Message::Batch(batch)
            }
            message => message,
        };
        self.wrong_kind = self.wrong_kind.saturating_add(wrong.len() as u64);
        let mut out: Outbox = self
            .applier
            .receive(&mut self.node, from, message, now)
            .into_iter()
            .map(|(to, message)| (to, message.into()))
            .collect();
        if !wrong.is_empty() {
            // The batch's answers, to the data plane that sent it, say them unserved.
            if let Some(answers) = out.iter_mut().find_map(|(to, message)| match message {
                Message::Asynchronous(asynchronous::Message::Answers(answers)) if *to == from => {
                    Some(answers)
                }
                _ => None,
            }) {
                answers
                    .unserved
                    .extend(wrong.into_iter().map(|key| asynchronous::Unserved {
                        key,
                        why: asynchronous::Why::NotServing,
                    }));
            }
        }
        out
    }
}

/// The store's messages as the pod's.
fn from_store(out: edgerush_cluster::Outbox<Value>) -> Outbox {
    out.into_iter()
        .map(|(to, message)| (to, Message::Store(message)))
        .collect()
}
