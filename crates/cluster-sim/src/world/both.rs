//! The world's one mode: the service's pod of both modes over one store
//! (`edgerush_cluster_modes::Pod`), its clients either mode's — the synchronous mode's, each
//! request decided by its key's owner, and the asynchronous mode's data planes, deciding from
//! their copies and reporting their hits — each driven by that mode's part of the world, the
//! one beside the other. A world of one kind of client is a pod of both serving one mode.

use super::asynchronous::Plane;
use super::{Mode, Sim, store_kind, store_size};
use crate::client::Client;
use edgerush_cluster::Message as StoreMessage;
use edgerush_cluster::{Config, Key, Node, NodeId, Time};
use edgerush_cluster_asynchronous as asynchronous;
use edgerush_cluster_modes::{Message, Pod, Value};
use edgerush_cluster_synchronous as synchronous;

/// Both modes, over one store in each pod.
#[derive(Clone, Copy, Debug)]
pub struct Both;

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

impl Sim<Both> {
    /// Requests and batch entries every pod did not serve, their keys of the other mode's: none
    /// while each data plane keys its requests as its mode does.
    #[must_use]
    pub fn wrong_kind(&self) -> u64 {
        self.pods.values().map(|pod| pod.node.wrong_kind()).sum()
    }
}

impl Mode for Both {
    type Value = Value;
    type Pod = Pod;
    type Message = Message;
    type Client = Caller;

    fn pod(id: NodeId, config: Config, peers: Vec<NodeId>, forwarding: bool, decides: bool) -> Pod {
        let mut pod = Pod::new(id, super::address(id), config, peers, super::CYCLE);
        pod.set_forwarding(forwarding);
        pod.set_forwarder_decides(decides);
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
            Message::Synchronous(_) | Message::Asynchronous(_) => None,
        }
    }

    fn into_store(message: Message) -> Option<StoreMessage<Value>> {
        match message {
            Message::Store(message) => Some(message),
            Message::Synchronous(_) | Message::Asynchronous(_) => None,
        }
    }

    fn kind(message: &Message) -> &'static str {
        match message {
            Message::Store(message) => store_kind(message),
            Message::Synchronous(message) => super::synchronous::kind(message),
            Message::Asynchronous(message) => super::asynchronous::kind(message),
        }
    }

    fn size(message: &Message) -> u64 {
        match message {
            Message::Store(message) => store_size(message),
            Message::Synchronous(message) => super::synchronous::size(message),
            Message::Asynchronous(message) => super::asynchronous::size(message),
        }
    }

    fn on_send(world: &mut Sim<Self>, from: NodeId, to: NodeId, message: &Message) {
        if let Message::Synchronous(message) = message {
            world.watch_sent(from, to, message);
        }
    }

    /// A synchronous request is told its pod is unreachable; an asynchronous batch is lost, as
    /// one is when its exchange fails, the data plane sending it again after four cycles unseen.
    fn unreachable(world: &mut Sim<Self>, from: NodeId, to: NodeId, message: &Message) {
        if let Message::Synchronous(message) = message {
            world.unreachable(from, to, message);
        }
    }

    fn client_send(world: &mut Sim<Self>, id: NodeId) {
        match world.clients.get(&id) {
            Some(Caller::Synchronous(_)) => world.on_client_send(id),
            Some(Caller::Asynchronous(_)) => world.on_request(id),
            None => {}
        }
    }

    fn client_check(world: &mut Sim<Self>, id: NodeId) {
        match world.clients.get(&id) {
            Some(Caller::Synchronous(_)) => world.on_client_check(id),
            Some(Caller::Asynchronous(_)) => world.on_cycle(id),
            None => {}
        }
    }

    fn client_unreachable(world: &mut Sim<Self>, client: NodeId, id: u64, pod: NodeId) {
        if let Some(Caller::Synchronous(_)) = world.clients.get(&client) {
            world.on_unreachable(client, id, pod);
        }
    }

    fn deliver_to_client(world: &mut Sim<Self>, from: NodeId, id: NodeId, message: Message) {
        match world.clients.get(&id) {
            Some(Caller::Synchronous(_)) => world.deliver_to_client(id, message),
            Some(Caller::Asynchronous(_)) => world.on_answers(from, id, message),
            None => {}
        }
    }

    fn reported(message: &Message) -> Vec<Key> {
        match message {
            Message::Asynchronous(message) => super::asynchronous::reported(message),
            Message::Store(_) | Message::Synchronous(_) => Vec::new(),
        }
    }

    fn counted(pod: &Pod, key: Key) -> u64 {
        pod.value(key)
            .and_then(asynchronous_of)
            .map_or(0, |value| value.counted())
    }
}
