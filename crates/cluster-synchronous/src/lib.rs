//! The synchronous rate-limiting mode, over the cluster store (`membership-design.md`): each
//! request decided by its key's owner under its rule, by the owner's wall clock; a refused
//! request's counted parts given back, three sends at most; requests under several keys
//! gathered by the pod they reach. The store holds the keys, copies them, hands them on and
//! forgets them; this decides what they hold ([`Value`], under a [`Rule`]), over any store
//! whose values hold this mode's ([`Holds`]): the pod of both modes' (`edgerush_cluster_modes`).
//!
//! Like the store, a pure state machine: messages and the time in, messages out.

#![forbid(unsafe_code)]

pub mod message;
pub mod pod;
pub mod value;

pub use message::{Answer, Message, Outbox};
pub use pod::{Decider, GIVE_BACK_SENDS, Holds, PART_FOR};
pub use value::{MOST_SLOTS, Rule, Value};

impl<V> From<edgerush_cluster::Message<V>> for Message<V> {
    fn from(message: edgerush_cluster::Message<V>) -> Self {
        Self::Store(message)
    }
}
