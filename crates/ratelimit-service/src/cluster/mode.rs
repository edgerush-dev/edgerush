//! What a pod of the store on sockets ([`StorePod`]) needs of the mode it runs, beside the
//! store within it: one task drives any mode's pod, its links, clients and server the same.
//! Each mode names the data planes' streams it serves ([`Mode::plane`]): the synchronous
//! mode's [`Decide`], the asynchronous mode's [`Exchange`], and both on the pod of both modes.
//! Each mode's own switches, and its data planes' side, stay its own, outside the trait.

use super::link::DECIDE;
use super::pod::{StorePod, serve_plane};
use super::wire::{
    self, Asking, Invalid, Outgoing, Synchronous as SyncWire, Telling, ToCopy, ToPeer, Value,
    asynchronous, both,
};
use edgerush_cluster::{Message, Node, NodeId, Time};
use edgerush_cluster_asynchronous::{
    Message as AsyncMessage, Pod as AsyncPod, TurnedAway, Value as AsyncValue,
};
use edgerush_cluster_modes::{Message as BothMessage, Pod as BothPod, Value as BothValue};
use edgerush_cluster_synchronous::{Message as SyncMessage, Pod as SyncPod, Value as SyncValue};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

/// The data planes' stream to a pod serving the asynchronous mode.
pub const EXCHANGE: &str = "/edgerush.cluster.v1.Cluster/Exchange";

/// A mode's value as the wire carries it.
pub type Wire<M> = <<M as Mode>::Value as Value>::Wire;

/// A data plane's call, served: what goes back to it.
pub type Served = Pin<Box<dyn Future<Output = http::Response<tonic::body::Body>> + Send>>;

/// How a pod serves one of the data planes' streams: the call, served as the stream's.
pub type PlaneServer<M> = fn(StorePod<M>, http::Request<tonic::body::Body>) -> Served;

/// A mode's pod, over the store, as a pod on sockets runs it.
pub trait Mode: Send + Sized + 'static {
    /// What it keeps under each key.
    type Value: Value<Wire: Send + 'static> + Send + 'static;
    /// Its messages, the store's among them.
    type Message: From<Message<Self::Value>> + Send + 'static;
    /// What it counts of itself, for its scrape.
    type Counted: Clone + Copy + Debug + Default + PartialEq + Send + 'static;

    /// The data planes' stream of `method`, if it serves it, as it serves it.
    fn plane(method: &str) -> Option<PlaneServer<Self>>;

    /// Its store.
    fn store(&self) -> &Node<Self::Value>;

    /// Its store, to change.
    fn store_mut(&mut self) -> &mut Node<Self::Value>;

    /// Its timer at `now`: what it sends, each with whom it is for.
    fn tick(&mut self, now: Time) -> Vec<(NodeId, Self::Message)>;

    /// `message` from `from` at `now`: what it sends.
    fn receive(
        &mut self,
        from: NodeId,
        message: Self::Message,
        now: Time,
    ) -> Vec<(NodeId, Self::Message)>;

    /// What it counts of itself now.
    fn counted(&self) -> Self::Counted;

    /// The store's message `message` holds, or `message` again if it is a mode's own.
    ///
    /// # Errors
    ///
    /// `message`, a mode's own.
    fn into_store(message: Self::Message) -> Result<Message<Self::Value>, Self::Message>;

    /// Whether `message`, asked by client `client`, is one a client asks, naming it.
    fn asks_as(message: &Self::Message, client: NodeId) -> bool;

    /// `message`, from `from`, as the wire carries it between pods; none for one that never
    /// goes between them.
    fn to_wire(from: NodeId, message: Self::Message) -> Option<Outgoing<Wire<Self>>>;

    /// The sender and the message, from the stream apart from copies.
    ///
    /// # Errors
    ///
    /// A message the mode or its store cannot use.
    fn from_peer(sent: ToPeer<Wire<Self>>) -> Result<(NodeId, Self::Message), Invalid>;

    /// The sender and the message, from the copy stream.
    ///
    /// # Errors
    ///
    /// A message the store cannot use.
    fn from_copy(sent: ToCopy<Wire<Self>>) -> Result<(NodeId, Self::Message), Invalid>;
}

/// One of the data planes' streams, as mode `M` serves it: what goes up it and down it, as
/// the mode's messages.
pub trait Stream<M: Mode>: Send + 'static {
    /// What a data plane sends up it.
    type Asking: prost::Message + Default + Send + 'static;
    /// What it is told down it.
    type Telling: prost::Message + Default + Send + 'static;

    /// What a data plane sends up, as the message of `client`, the stream's.
    ///
    /// # Errors
    ///
    /// A message the mode cannot use.
    fn asked_from(asking: Self::Asking, client: NodeId) -> Result<M::Message, Invalid>;

    /// What the pod tells a client, as the stream carries it down; none for anything else.
    fn telling(message: M::Message) -> Option<Self::Telling>;
}

/// The synchronous mode's stream (`/edgerush.cluster.v1.Cluster/Decide`): requests and requests to gather up, answers
/// and tables down.
#[derive(Debug, Clone, Copy)]
pub struct Decide;

/// The asynchronous mode's stream ([`EXCHANGE`]): batches up, answers and tables down, an
/// owner's relayed by a pod it forwarded entries to among them.
#[derive(Debug, Clone, Copy)]
pub struct Exchange;

/// A pod running the synchronous mode, alone or beside the other: its switches for requests
/// of keys not its own.
pub trait Forwards {
    /// Whether it forwards a request for a key not its own to the owner its table names.
    fn set_forwarding(&mut self, forwarding: bool);

    /// Whether, forwarding, it decides a request itself from its copy while the owner is
    /// unheard.
    fn set_forwarder_decides(&mut self, decides: bool);
}

impl Forwards for SyncPod {
    fn set_forwarding(&mut self, forwarding: bool) {
        Self::set_forwarding(self, forwarding);
    }

    fn set_forwarder_decides(&mut self, decides: bool) {
        Self::set_forwarder_decides(self, decides);
    }
}

impl Forwards for BothPod {
    fn set_forwarding(&mut self, forwarding: bool) {
        Self::set_forwarding(self, forwarding);
    }

    fn set_forwarder_decides(&mut self, decides: bool) {
        Self::set_forwarder_decides(self, decides);
    }
}

/// What a synchronous pod counts of itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecidedFromCopy {
    /// Requests it decided from its copy, their owner unheard.
    pub decided_from_copy: u64,
}

impl Mode for SyncPod {
    type Value = SyncValue;
    type Message = SyncMessage;
    type Counted = DecidedFromCopy;

    fn plane(method: &str) -> Option<PlaneServer<Self>> {
        (method == DECIDE).then_some(serve_plane::<Self, Decide>)
    }

    fn store(&self) -> &Node<SyncValue> {
        Self::store(self)
    }

    fn store_mut(&mut self) -> &mut Node<SyncValue> {
        Self::store_mut(self)
    }

    fn tick(&mut self, now: Time) -> Vec<(NodeId, SyncMessage)> {
        Self::tick(self, now)
    }

    fn receive(
        &mut self,
        from: NodeId,
        message: SyncMessage,
        now: Time,
    ) -> Vec<(NodeId, SyncMessage)> {
        Self::receive(self, from, message, now)
    }

    fn counted(&self) -> DecidedFromCopy {
        DecidedFromCopy {
            decided_from_copy: self.decided_from_copy(),
        }
    }

    fn into_store(message: SyncMessage) -> Result<Message<SyncValue>, SyncMessage> {
        match message {
            SyncMessage::Store(message) => Ok(message),
            message => Err(message),
        }
    }

    fn asks_as(message: &SyncMessage, client: NodeId) -> bool {
        synchronous_asks_as(message, client)
    }

    fn to_wire(from: NodeId, message: SyncMessage) -> Option<Outgoing<SyncWire>> {
        wire::mode_wire(from, message)
    }

    fn from_peer(sent: ToPeer<SyncWire>) -> Result<(NodeId, SyncMessage), Invalid> {
        wire::mode_from_peer(sent)
    }

    fn from_copy(sent: ToCopy<SyncWire>) -> Result<(NodeId, SyncMessage), Invalid> {
        wire::mode_from_copy(sent)
    }
}

impl Stream<SyncPod> for Decide {
    type Asking = Asking;
    type Telling = Telling;

    fn asked_from(asking: Asking, client: NodeId) -> Result<SyncMessage, Invalid> {
        wire::asked_from(asking, client)
    }

    fn telling(message: SyncMessage) -> Option<Telling> {
        wire::telling(message)
    }
}

impl Mode for AsyncPod {
    type Value = AsyncValue;
    type Message = AsyncMessage;
    type Counted = TurnedAway;

    fn plane(method: &str) -> Option<PlaneServer<Self>> {
        (method == EXCHANGE).then_some(serve_plane::<Self, Exchange>)
    }

    fn store(&self) -> &Node<AsyncValue> {
        Self::store(self)
    }

    fn store_mut(&mut self) -> &mut Node<AsyncValue> {
        Self::store_mut(self)
    }

    fn tick(&mut self, now: Time) -> Vec<(NodeId, AsyncMessage)> {
        Self::tick(self, now)
    }

    fn receive(
        &mut self,
        from: NodeId,
        message: AsyncMessage,
        now: Time,
    ) -> Vec<(NodeId, AsyncMessage)> {
        Self::receive(self, from, message, now)
    }

    fn counted(&self) -> TurnedAway {
        self.turned_away()
    }

    fn into_store(message: AsyncMessage) -> Result<Message<AsyncValue>, AsyncMessage> {
        match message {
            AsyncMessage::Store(message) => Ok(message),
            message => Err(message),
        }
    }

    fn asks_as(message: &AsyncMessage, _: NodeId) -> bool {
        asynchronous_asks(message)
    }

    fn to_wire(
        from: NodeId,
        message: AsyncMessage,
    ) -> Option<Outgoing<asynchronous::Asynchronous>> {
        asynchronous::mode_wire(from, message)
    }

    fn from_peer(
        sent: ToPeer<asynchronous::Asynchronous>,
    ) -> Result<(NodeId, AsyncMessage), Invalid> {
        asynchronous::mode_from_peer(sent)
    }

    fn from_copy(
        sent: ToCopy<asynchronous::Asynchronous>,
    ) -> Result<(NodeId, AsyncMessage), Invalid> {
        asynchronous::mode_from_copy(sent)
    }
}

impl Stream<AsyncPod> for Exchange {
    type Asking = asynchronous::Batch;
    type Telling = asynchronous::Answering;

    fn asked_from(batch: asynchronous::Batch, _: NodeId) -> Result<AsyncMessage, Invalid> {
        asynchronous::batch_from(batch).map(AsyncMessage::Batch)
    }

    fn telling(message: AsyncMessage) -> Option<asynchronous::Answering> {
        asynchronous::answering(message)
    }
}

/// What a pod of both modes counts of itself: each mode's own counts, its keys of each mode,
/// and what it did not serve, the keys of the other mode's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BothCounted {
    /// The synchronous mode's requests it decided from its copy, their owner unheard.
    pub decided_from_copy: u64,
    /// What it answered the asynchronous mode's data planes unserved, by why.
    pub turned_away: TurnedAway,
    /// Requests, and entries of batches, it did not serve, their keys of the other mode's.
    pub wrong_kind: u64,
    /// Its synchronous mode's keys.
    pub synchronous: KindCounted,
    /// Its asynchronous mode's keys.
    pub asynchronous: KindCounted,
}

/// What a pod's store counts of one mode's keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KindCounted {
    /// The keys it holds, owned and copies.
    pub keys: u64,
    /// About the bytes they take.
    pub key_bytes: u64,
    /// Keys forgotten, their time come.
    pub forgotten: u64,
    /// Keys evicted past the cap.
    pub evicted: u64,
}

/// The store's number for the synchronous mode's keys, as the value of both modes gives it
/// (`Held::kind`): a key not yet given to a mode counts with them.
const SYNCHRONOUS: usize = 0;
/// The store's number for the asynchronous mode's keys.
const ASYNCHRONOUS: usize = 1;

impl Mode for BothPod {
    type Value = BothValue;
    type Message = BothMessage;
    type Counted = BothCounted;

    fn plane(method: &str) -> Option<PlaneServer<Self>> {
        match method {
            DECIDE => Some(serve_plane::<Self, Decide>),
            EXCHANGE => Some(serve_plane::<Self, Exchange>),
            _ => None,
        }
    }

    fn store(&self) -> &Node<BothValue> {
        Self::store(self)
    }

    fn store_mut(&mut self) -> &mut Node<BothValue> {
        Self::store_mut(self)
    }

    fn tick(&mut self, now: Time) -> Vec<(NodeId, BothMessage)> {
        Self::tick(self, now)
    }

    fn receive(
        &mut self,
        from: NodeId,
        message: BothMessage,
        now: Time,
    ) -> Vec<(NodeId, BothMessage)> {
        Self::receive(self, from, message, now)
    }

    fn counted(&self) -> BothCounted {
        let store = self.store();
        let keys = store.keys_by_kind();
        let (forgotten, evicted) = (store.forgotten_by_kind(), store.evicted_by_kind());
        let of = |kind: usize| KindCounted {
            keys: u64::try_from(keys[kind].0).unwrap_or(u64::MAX),
            key_bytes: keys[kind].1,
            forgotten: forgotten[kind],
            evicted: evicted[kind],
        };
        BothCounted {
            decided_from_copy: self.decided_from_copy(),
            turned_away: self.turned_away(),
            wrong_kind: self.wrong_kind(),
            synchronous: of(SYNCHRONOUS),
            asynchronous: of(ASYNCHRONOUS),
        }
    }

    fn into_store(message: BothMessage) -> Result<Message<BothValue>, BothMessage> {
        match message {
            BothMessage::Store(message) => Ok(message),
            message => Err(message),
        }
    }

    /// As each mode's pod has it, of that mode's messages.
    fn asks_as(message: &BothMessage, client: NodeId) -> bool {
        match message {
            BothMessage::Synchronous(message) => synchronous_asks_as(message, client),
            BothMessage::Asynchronous(message) => asynchronous_asks(message),
            BothMessage::Store(_) => false,
        }
    }

    fn to_wire(from: NodeId, message: BothMessage) -> Option<Outgoing<both::Both>> {
        both::mode_wire(from, message)
    }

    fn from_peer(sent: ToPeer<both::Both>) -> Result<(NodeId, BothMessage), Invalid> {
        both::mode_from_peer(sent)
    }

    fn from_copy(sent: ToCopy<both::Both>) -> Result<(NodeId, BothMessage), Invalid> {
        both::mode_from_copy(sent)
    }
}

impl Stream<BothPod> for Decide {
    type Asking = Asking;
    type Telling = Telling;

    fn asked_from(asking: Asking, client: NodeId) -> Result<BothMessage, Invalid> {
        wire::asked_from::<BothValue>(asking, client).map(BothMessage::Synchronous)
    }

    /// The synchronous mode's answers, and the store's tables.
    fn telling(message: BothMessage) -> Option<Telling> {
        match message {
            BothMessage::Synchronous(message) => wire::telling(message),
            BothMessage::Store(message) => wire::telling(SyncMessage::<BothValue>::Store(message)),
            BothMessage::Asynchronous(_) => None,
        }
    }
}

impl Stream<BothPod> for Exchange {
    type Asking = asynchronous::Batch;
    type Telling = asynchronous::Answering;

    fn asked_from(batch: asynchronous::Batch, _: NodeId) -> Result<BothMessage, Invalid> {
        asynchronous::batch_from(batch)
            .map(|batch| BothMessage::Asynchronous(AsyncMessage::Batch(batch)))
    }

    /// The asynchronous mode's answers, an owner's relayed among them, and the store's tables.
    fn telling(message: BothMessage) -> Option<asynchronous::Answering> {
        match message {
            BothMessage::Asynchronous(message) => asynchronous::answering(message),
            BothMessage::Store(message) => {
                asynchronous::answering(AsyncMessage::<BothValue>::Store(message))
            }
            BothMessage::Synchronous(_) => None,
        }
    }
}

/// A request or a request to gather, not one forwarded: what a synchronous data plane asks.
fn synchronous_asks_as<V>(message: &SyncMessage<V>, client: NodeId) -> bool {
    match message {
        SyncMessage::Request {
            client: asker,
            via: None,
            ..
        }
        | SyncMessage::Gather { client: asker, .. } => *asker == client,
        _ => false,
    }
}

/// A batch alone: whatever data plane it names, it is answered on the stream it came up.
const fn asynchronous_asks<V>(message: &AsyncMessage<V>) -> bool {
    matches!(message, AsyncMessage::Batch(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::Held;
    use edgerush_cluster_asynchronous::{Answers, Batch};

    /// A data plane's stream asks the asynchronous mode a batch alone: never what pods send
    /// each other, nor the store's.
    #[test]
    fn a_data_plane_asks_the_asynchronous_mode_a_batch_alone() {
        let batch = Batch {
            from: 1,
            stamp: (1, 1),
            made: 0,
            cycle: 10_000_000,
            entries: Vec::new(),
        };
        let answers = Answers {
            life: 1,
            number: 1,
            made: 0,
            entries: Vec::new(),
            unserved: Vec::new(),
            table: None,
        };
        let client = 1 << 63;
        assert!(AsyncPod::asks_as(
            &AsyncMessage::Batch(batch.clone()),
            client
        ));
        for not in [
            AsyncMessage::Forwarded {
                client,
                via: 2,
                batch,
            },
            AsyncMessage::Relayed {
                client,
                owner: 2,
                answers: answers.clone(),
            },
            AsyncMessage::Answers(answers),
            AsyncMessage::Store(Message::TableRequest),
        ] {
            assert!(!AsyncPod::asks_as(&not, client), "{not:?}");
        }
    }

    /// Each pod serves its mode's stream, and the pod of both modes serves both.
    #[test]
    fn each_pod_serves_its_modes_streams() {
        let methods = [DECIDE, EXCHANGE, "/edgerush.cluster.v1.Cluster/Other"];
        assert_eq!(
            methods.map(|method| SyncPod::plane(method).is_some()),
            [true, false, false]
        );
        assert_eq!(
            methods.map(|method| AsyncPod::plane(method).is_some()),
            [false, true, false]
        );
        assert_eq!(
            methods.map(|method| BothPod::plane(method).is_some()),
            [true, true, false]
        );
    }

    /// The numbers the pod of both modes reads each mode's keys under are the ones its value
    /// counts them under.
    #[test]
    fn each_modes_keys_are_counted_under_its_own_number() {
        let synchronous = BothValue::Synchronous(SyncValue::Bucket { due: 0, given: 0 });
        let asynchronous = BothValue::Asynchronous(AsyncValue::default());
        assert_eq!(synchronous.kind(), SYNCHRONOUS);
        assert_eq!(asynchronous.kind(), ASYNCHRONOUS);
        assert_eq!(BothValue::Fresh.kind(), SYNCHRONOUS);
    }
}
