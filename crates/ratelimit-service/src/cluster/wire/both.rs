//! The pod of both modes on the wire (`edgerush_cluster_modes`): its value under each key,
//! either mode's, and its messages between pods, the store's and each mode's, as each mode's
//! own wire carries them ([`super::mode_wire`], [`super::asynchronous`]).
//!
//! A value is either mode's message, in a `oneof`. One of the mode other than its key's bit
//! says is refused with the copy or handoff it came in ([`Invalid::Kind`]): no owner could
//! have held it, as each mode's data planes set the bit of their own.

use super::asynchronous::{self, Asynchronous};
use super::{
    Control, Invalid, Outgoing, Synchronous, ToCopy, ToPeer, Value, from_copy, from_peer, wire,
};
use edgerush_cluster::{Key, Kind, NodeId};
use edgerush_cluster_asynchronous::Value as AsyncValue;
use edgerush_cluster_modes::{Message, Value as BothValue};
use edgerush_cluster_synchronous::Value as SyncValue;

/// A key's value on a pod of both modes: either mode's, or none yet.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Both {
    /// The mode's value; none for a key not yet given to a mode, which takes whatever copy
    /// comes to it.
    #[prost(oneof = "Either", tags = "1, 2")]
    pub value: Option<Either>,
}

/// The mode a value is of, with the value.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Either {
    /// The synchronous mode's.
    #[prost(message, tag = "1")]
    Synchronous(Synchronous),
    /// The asynchronous mode's.
    #[prost(message, tag = "2")]
    Asynchronous(Asynchronous),
}

impl Value for BothValue {
    type Wire = Both;

    fn to_wire(&self) -> Both {
        let value = match self {
            Self::Fresh => None,
            Self::Synchronous(value) => Some(Either::Synchronous(value.to_wire())),
            Self::Asynchronous(value) => Some(Either::Asynchronous(value.to_wire())),
        };
        Both { value }
    }

    fn from_wire(wire: Both) -> Result<Self, Invalid> {
        Ok(match wire.value {
            None => Self::Fresh,
            Some(Either::Synchronous(value)) => Self::Synchronous(SyncValue::from_wire(value)?),
            Some(Either::Asynchronous(value)) => Self::Asynchronous(AsyncValue::from_wire(value)?),
        })
    }

    /// A mode's value under a key whose bit says that mode; a key not yet given to a mode under
    /// any.
    fn fits(&self, key: Key) -> bool {
        match self {
            Self::Fresh => true,
            Self::Synchronous(_) => Kind::of(key) == Kind::Synchronous,
            Self::Asynchronous(_) => Kind::of(key) == Kind::Asynchronous,
        }
    }
}

/// `message`, from `from`, as the wire carries it between pods: the store's as [`wire`] has
/// it, each mode's as its own wire does. None for what a data plane alone asks, or is told.
#[must_use]
pub fn mode_wire(from: NodeId, message: Message) -> Option<Outgoing<Both>> {
    match message {
        Message::Store(message) => Some(wire(from, message)),
        Message::Synchronous(message) => super::mode_wire(from, message),
        Message::Asynchronous(message) => asynchronous::mode_wire(from, message),
    }
}

/// The sender and the message, from the stream apart from copies: each mode's own as that
/// mode's, everything else as the store's.
///
/// # Errors
///
/// A message neither mode nor the store can use: see [`Invalid`].
pub fn mode_from_peer(sent: ToPeer<Both>) -> Result<(NodeId, Message), Invalid> {
    match sent.message {
        Some(Control::Request(_) | Control::Reply(_)) => super::mode_from_peer::<BothValue>(sent)
            .map(|(from, message)| (from, Message::from(message))),
        Some(Control::Forwarded(_) | Control::Relayed(_)) => {
            asynchronous::mode_from_peer::<BothValue>(sent)
                .map(|(from, message)| (from, Message::from(message)))
        }
        _ => from_peer::<BothValue>(sent).map(|(from, message)| (from, Message::Store(message))),
    }
}

/// The sender and the store's message, from the copy stream.
///
/// # Errors
///
/// A message the store cannot use: see [`Invalid`].
pub fn mode_from_copy(sent: ToCopy<Both>) -> Result<(NodeId, Message), Invalid> {
    from_copy::<BothValue>(sent).map(|(from, message)| (from, Message::Store(message)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::{Kept, Message as StoreMessage};
    use edgerush_cluster_asynchronous::{Batch, Message as AsyncMessage};
    use edgerush_cluster_synchronous::Message as SyncMessage;

    /// A whole copy of shard 3 holding `entries`.
    fn copy(entries: Vec<(Key, Kept<BothValue>)>) -> Message {
        Message::Store(StoreMessage::Replicate {
            shard: 3,
            term: 1,
            assignment: 1,
            after: None,
            version: 1,
            under: (1, 1),
            entries,
            dropped: Vec::new(),
            part: 0,
            more: false,
        })
    }

    /// `message` from pod 1, through the wire and back.
    fn again(message: Message) -> Result<(NodeId, Message), Invalid> {
        match mode_wire(1, message).expect("a message between pods") {
            Outgoing::Coordinate(sent) | Outgoing::Peer(sent) => mode_from_peer(sent),
            Outgoing::Copy(sent) => mode_from_copy(sent),
        }
    }

    fn synchronous() -> BothValue {
        BothValue::Synchronous(SyncValue::Bucket { due: 7, given: 2 })
    }

    fn asynchronous() -> BothValue {
        BothValue::Asynchronous(AsyncValue::default())
    }

    /// A copy carries either mode's value under a key of its mode, and a key not yet given to
    /// a mode, each as it was.
    #[test]
    fn a_copy_carries_either_modes_value_under_a_key_of_its_mode() {
        let sent = copy(vec![
            (
                Kind::Synchronous.key(5),
                Kept {
                    value: synchronous(),
                    forget_at: 9,
                },
            ),
            (
                Kind::Asynchronous.key(6),
                Kept {
                    value: asynchronous(),
                    forget_at: 10,
                },
            ),
            (
                Kind::Synchronous.key(7),
                Kept {
                    value: BothValue::Fresh,
                    forget_at: 11,
                },
            ),
        ]);
        assert_eq!(again(sent.clone()), Ok((1, sent)));
    }

    /// A copy holding a value of the other mode than its key's bit says is refused whole: no
    /// owner could have held it.
    #[test]
    fn a_copy_of_a_value_under_a_key_of_the_other_mode_is_refused() {
        for (key, value) in [
            (Kind::Asynchronous.key(5), synchronous()),
            (Kind::Synchronous.key(6), asynchronous()),
        ] {
            let wrong = copy(vec![(
                key,
                Kept {
                    value,
                    forget_at: 0,
                },
            )]);
            assert_eq!(again(wrong), Err(Invalid::Kind), "{key:#x}");
        }
    }

    /// Each mode's messages between pods come back as that mode's, and the store's as the
    /// store's; what a data plane alone asks, or is told, goes between no pods.
    #[test]
    fn each_modes_messages_between_pods_come_back_as_its_own() {
        let batch = Batch {
            from: 9,
            stamp: (1, 1),
            made: 5,
            cycle: 10_000_000,
            entries: Vec::new(),
        };
        for message in [
            Message::Store(StoreMessage::TableRequest),
            Message::Synchronous(SyncMessage::Request {
                client: 9,
                id: 4,
                key: Kind::Synchronous.key(5),
                stamp: (1, 1),
                hops: 1,
                via: Some(2),
                give_back: false,
                rule: None,
                receipt: 0,
            }),
            Message::Asynchronous(AsyncMessage::Forwarded {
                client: 9,
                via: 2,
                batch: batch.clone(),
            }),
        ] {
            assert_eq!(again(message.clone()), Ok((1, message)));
        }
        for alone in [
            Message::Synchronous(SyncMessage::Gather {
                client: 9,
                id: 4,
                keys: Vec::new(),
            }),
            Message::Asynchronous(AsyncMessage::Batch(batch)),
        ] {
            assert_eq!(mode_wire(1, alone.clone()), None, "{alone:?}");
        }
    }
}
