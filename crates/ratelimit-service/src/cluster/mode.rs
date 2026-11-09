//! What a pod of the store on sockets ([`StorePod`]) needs of the mode it runs, beside the
//! store within it: one task drives either mode's pod, its links, clients and server the same.
//! Each mode's own switches, and its data planes' side, stay its own, outside the trait.
//!
//! [`StorePod`]: super::pod::StorePod

use super::link::DECIDE;
use super::wire::{
    self, Asking, Invalid, Outgoing, Synchronous as SyncWire, Telling, ToCopy, ToPeer, Value,
    asynchronous,
};
use edgerush_cluster::{Message, Node, NodeId, Time};
use edgerush_cluster_asynchronous::{
    Message as AsyncMessage, Pod as AsyncPod, TurnedAway, Value as AsyncValue,
};
use edgerush_cluster_synchronous::{Message as SyncMessage, Pod as SyncPod, Value as SyncValue};
use std::fmt::Debug;

/// The data planes' stream to a pod of the asynchronous mode.
pub const EXCHANGE: &str = "/edgerush.cluster.v1.Cluster/Exchange";

/// A mode's value as the wire carries it.
pub type Wire<M> = <<M as Mode>::Value as Value>::Wire;

/// A mode's pod, over the store, as a pod on sockets runs it.
pub trait Mode: Send + 'static {
    /// What it keeps under each key.
    type Value: Value<Wire: Send + 'static> + Send + 'static;
    /// Its messages, the store's among them.
    type Message: From<Message<Self::Value>> + Send + 'static;
    /// What a data plane sends up its stream.
    type Asking: prost::Message + Default + Send + 'static;
    /// What a data plane is told down it.
    type Telling: prost::Message + Default + Send + 'static;
    /// What it counts of itself, for its scrape.
    type Counted: Clone + Copy + Debug + Default + PartialEq + Send + 'static;

    /// The method of the data planes' stream.
    const STREAM: &'static str;

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

    /// The store's message `message` holds, or `message` again if it is the mode's own.
    ///
    /// # Errors
    ///
    /// `message`, the mode's own.
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

    /// What a data plane sends up, as the message of `client`, its stream's.
    ///
    /// # Errors
    ///
    /// A message the mode cannot use.
    fn asked_from(asking: Self::Asking, client: NodeId) -> Result<Self::Message, Invalid>;

    /// What the pod tells a client, as its stream carries it down; none for anything else.
    fn telling(message: Self::Message) -> Option<Self::Telling>;
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
    type Asking = Asking;
    type Telling = Telling;
    type Counted = DecidedFromCopy;

    const STREAM: &'static str = DECIDE;

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

    /// A request or a request to gather, not one forwarded.
    fn asks_as(message: &SyncMessage, client: NodeId) -> bool {
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

    fn to_wire(from: NodeId, message: SyncMessage) -> Option<Outgoing<SyncWire>> {
        wire::mode_wire(from, message)
    }

    fn from_peer(sent: ToPeer<SyncWire>) -> Result<(NodeId, SyncMessage), Invalid> {
        wire::mode_from_peer(sent)
    }

    fn from_copy(sent: ToCopy<SyncWire>) -> Result<(NodeId, SyncMessage), Invalid> {
        wire::mode_from_copy(sent)
    }

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
    type Asking = asynchronous::Batch;
    type Telling = asynchronous::Answering;
    type Counted = TurnedAway;

    const STREAM: &'static str = EXCHANGE;

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

    /// A batch alone: whatever data plane it names, it is answered on the stream it came up.
    fn asks_as(message: &AsyncMessage, _: NodeId) -> bool {
        matches!(message, AsyncMessage::Batch(_))
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

    fn asked_from(batch: asynchronous::Batch, _: NodeId) -> Result<AsyncMessage, Invalid> {
        asynchronous::batch_from(batch).map(AsyncMessage::Batch)
    }

    fn telling(message: AsyncMessage) -> Option<asynchronous::Answering> {
        asynchronous::answering(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster_asynchronous::{Answers, Batch};

    /// A data plane's stream asks the asynchronous mode a batch alone: never what pods send
    /// each other, nor the store's.
    #[test]
    fn a_data_plane_asks_the_asynchronous_mode_a_batch_alone() {
        let batch = Batch {
            from: 1,
            stamp: (1, 1),
            made: 0,
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
                answers: answers.clone(),
            },
            AsyncMessage::Answers(answers),
            AsyncMessage::Store(Message::TableRequest),
        ] {
            assert!(!AsyncPod::asks_as(&not, client), "{not:?}");
        }
    }
}
