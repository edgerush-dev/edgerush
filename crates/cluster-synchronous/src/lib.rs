//! The synchronous rate-limiting mode, over the cluster store (`membership-design.md`): each
//! request decided by its key's owner under its rule, by the owner's wall clock; a refused
//! request's counted parts given back, three sends at most; requests under several keys
//! gathered by the pod they reach. The store holds the keys, copies them, hands them on and
//! forgets them; this decides what they hold ([`Value`], under a [`Rule`]).
//!
//! Like the store, a pure state machine: messages and the time in, messages out.

#![forbid(unsafe_code)]

pub mod message;
pub mod pod;
pub mod value;

pub use message::{Answer, Message, Outbox};
pub use pod::Pod;
pub use value::{Rule, Value};

/// The store as this mode keeps it.
pub type Store = edgerush_cluster::Node<Value>;

impl From<edgerush_cluster::Message<Value>> for Message {
    fn from(message: edgerush_cluster::Message<Value>) -> Self {
        Self::Store(message)
    }
}
