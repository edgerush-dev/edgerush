//! The asynchronous rate-limiting mode, over the cluster store (06 §4): data planes decide every
//! request from their own copy of a key's bucket and report their hits to the key's owner in
//! numbered batches; the owner applies each batch once, in order, and answers with the bucket.
//! Today's asynchronous mode (`edgerush_ratelimit::service`) carried onto the store: the owner's
//! state of a key is the store's value ([`Value`]), which the store copies to the shard's
//! replicas, hands on and forgets; its applying of a batch's entry ([`owner::apply`]) runs on
//! `Node::update`, in the mode's [`Pod`], which answers a data plane's [`Batch`].
//!
//! Like the store, a pure state machine: messages and the time in, messages out.

#![forbid(unsafe_code)]

pub mod message;
pub mod owner;
pub mod pod;
pub mod value;

pub use message::{Answer, Answers, Batch, Entry, Message, Outbox, Unserved, Why};
pub use owner::{Answered, apply};
pub use pod::{Pod, Turned, TurnedAway};
pub use value::{Parts, PlaneParts, Value};

/// The store as this mode keeps it.
pub type Store = edgerush_cluster::Node<Value>;
