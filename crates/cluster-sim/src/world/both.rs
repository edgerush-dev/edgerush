//! The world's pods: the service's pod of both modes over one store
//! (`edgerush_cluster_modes::Pod`), its clients either mode's — the synchronous mode's, each
//! request decided by its key's owner, and the asynchronous mode's data planes, deciding from
//! their copies and reporting their hits — each driven by that mode's part of the world, the
//! one beside the other. A world of one kind of client is a pod of both serving one mode.

use super::asynchronous::Plane;
use super::{World, store_kind, store_size};
use crate::client::Client;
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::{Config, Key, NodeId};
use edgerush_cluster_asynchronous as asynchronous;
use edgerush_cluster_modes::{Message, Pod, Value};
use edgerush_cluster_synchronous as synchronous;

/// A client of the world: either mode's.
#[allow(
    clippy::large_enum_variant,
    reason = "a world holds a few clients, each for a whole run"
)]
pub enum Caller {
    /// The synchronous mode's, standing for a data plane asking each request of its owner.
    Synchronous(Client),
    /// The asynchronous mode's: a data plane.
    Asynchronous(Plane),
}

impl Caller {
    /// The synchronous mode's client it is, if it is one.
    pub(crate) const fn synchronous(&self) -> Option<&Client> {
        match self {
            Self::Synchronous(client) => Some(client),
            Self::Asynchronous(_) => None,
        }
    }

    /// The same, to change.
    pub(crate) const fn synchronous_mut(&mut self) -> Option<&mut Client> {
        match self {
            Self::Synchronous(client) => Some(client),
            Self::Asynchronous(_) => None,
        }
    }

    /// The asynchronous mode's data plane it is, if it is one.
    pub(crate) const fn asynchronous(&self) -> Option<&Plane> {
        match self {
            Self::Asynchronous(plane) => Some(plane),
            Self::Synchronous(_) => None,
        }
    }

    /// The same, to change.
    pub(crate) const fn asynchronous_mut(&mut self) -> Option<&mut Plane> {
        match self {
            Self::Asynchronous(plane) => Some(plane),
            Self::Synchronous(_) => None,
        }
    }
}

/// The synchronous mode's value of `value`, if it holds one.
pub(crate) fn synchronous_of(value: Value) -> Option<synchronous::Value> {
    match value {
        Value::Synchronous(value) => Some(value),
        Value::Fresh | Value::Asynchronous(_) => None,
    }
}

/// The asynchronous mode's value of `value`, if it holds one.
pub(crate) fn asynchronous_of(value: Value) -> Option<asynchronous::Value> {
    match value {
        Value::Asynchronous(value) => Some(value),
        Value::Fresh | Value::Synchronous(_) => None,
    }
}

impl World {
    /// Requests and batch entries every pod did not serve, their keys of the other mode's: none
    /// while each data plane keys its requests as its mode does.
    #[must_use]
    pub fn wrong_kind(&self) -> u64 {
        self.pods.values().map(|pod| pod.node.wrong_kind()).sum()
    }
}

/// Pod `id`, new, knowing how to reach `peers`, forwarding requests for shards not its own if
/// `forwarding`, and deciding those it would forward from its copy, their owner unheard, if
/// `decides`.
pub(super) fn pod(
    id: NodeId,
    config: Config,
    peers: Vec<NodeId>,
    forwarding: bool,
    decides: bool,
) -> Pod {
    let mut pod = Pod::new(id, super::address(id), config, peers);
    pod.set_forwarding(forwarding);
    pod.set_forwarder_decides(decides);
    // A pod is a member in one life only, and no pod is 0: its id serves as its life.
    pod.set_life(id);
    pod
}

/// The store's message `message` carries, if it is one.
pub(super) const fn as_store(message: &Message) -> Option<&StoreMessage<Value>> {
    match message {
        Message::Store(message) => Some(message),
        Message::Synchronous(_) | Message::Asynchronous(_) => None,
    }
}

/// The same, taken.
pub(super) fn into_store(message: Message) -> Option<StoreMessage<Value>> {
    match message {
        Message::Store(message) => Some(message),
        Message::Synchronous(_) | Message::Asynchronous(_) => None,
    }
}

/// The name of `message`'s kind, for the bytes sent by kind.
pub(super) fn kind(message: &Message) -> &'static str {
    match message {
        Message::Store(message) => store_kind(message),
        Message::Synchronous(message) => super::synchronous::kind(message),
        Message::Asynchronous(message) => super::asynchronous::kind(message),
    }
}

/// About how many bytes `message` takes on the wire.
pub(super) fn size(message: &Message) -> u64 {
    match message {
        Message::Store(message) => store_size(message),
        Message::Synchronous(message) => super::synchronous::size(message),
        Message::Asynchronous(message) => super::asynchronous::size(message),
    }
}

/// The keys whose hits `message` brings a pod to apply, as its store keeps them: an
/// asynchronous batch's.
pub(super) fn reported(message: &Message) -> Vec<Key> {
    match message {
        Message::Asynchronous(message) => super::asynchronous::reported(message),
        Message::Store(_) | Message::Synchronous(_) => Vec::new(),
    }
}

/// Hits `pod` has applied to `key`, each once, in the value it holds of it now.
pub(super) fn counted(pod: &Pod, key: Key) -> u64 {
    pod.value(key)
        .and_then(asynchronous_of)
        .map_or(0, |value| value.counted())
}

impl World {
    /// What the world watches of every message sent, before the network takes it: the
    /// synchronous mode's.
    pub(super) fn on_send(&mut self, from: NodeId, to: NodeId, message: &Message) {
        if let Message::Synchronous(message) = message {
            self.watch_sent(from, to, message);
        }
    }

    /// A client's `message` to pod `to` found unreachable, its stream down or the pod gone. A
    /// synchronous request is told its pod is unreachable; an asynchronous batch is lost, as
    /// one is when its exchange fails, the data plane sending it again after four cycles
    /// unseen.
    pub(super) fn found_unreachable(&mut self, from: NodeId, to: NodeId, message: &Message) {
        if let Message::Synchronous(message) = message {
            self.unreachable(from, to, message);
        }
    }

    /// Client `id`'s turn to send.
    pub(super) fn client_send(&mut self, id: NodeId) {
        match self.clients.get(&id) {
            Some(Caller::Synchronous(_)) => self.on_client_send(id),
            Some(Caller::Asynchronous(_)) => self.on_request(id),
            None => {}
        }
    }

    /// Client `id`'s check of what it has in flight, every millisecond.
    pub(super) fn client_check(&mut self, id: NodeId) {
        match self.clients.get(&id) {
            Some(Caller::Synchronous(_)) => self.on_client_check(id),
            Some(Caller::Asynchronous(_)) => self.on_cycle(id),
            None => {}
        }
    }

    /// Client `client` told its request `id` could not reach `pod`.
    pub(super) fn client_unreachable(&mut self, client: NodeId, id: u64, pod: NodeId) {
        if let Some(Caller::Synchronous(_)) = self.clients.get(&client) {
            self.on_unreachable(client, id, pod);
        }
    }

    /// `message` from `from` reaching client `id`.
    pub(super) fn deliver_to_caller(&mut self, from: NodeId, id: NodeId, message: Message) {
        match self.clients.get(&id) {
            Some(Caller::Synchronous(_)) => self.deliver_to_client(id, message),
            Some(Caller::Asynchronous(_)) => self.on_answers(from, id, message),
            None => {}
        }
    }
}
