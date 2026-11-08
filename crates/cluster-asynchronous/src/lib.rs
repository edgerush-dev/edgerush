//! The asynchronous rate-limiting mode, over the cluster store (06 §4): data planes decide every
//! request from their own copy of a key's bucket and report their hits to the key's owner in
//! numbered batches; the owner applies each batch once, in order, and answers with the bucket.
//! Today's asynchronous mode (`edgerush_ratelimit::service`) carried onto the store: the owner's
//! state of a key is the store's value ([`Value`]), which the store copies to the shard's
//! replicas, hands on and forgets, and its applying of a batch's entry ([`owner::apply`]) runs
//! on `Node::update`.
//!
//! Like the store, a pure state machine: the time in, nothing sent of its own.

#![forbid(unsafe_code)]

pub mod owner;
pub mod value;

pub use owner::{Answered, apply};
pub use value::Value;

/// The store as this mode keeps it.
pub type Store = edgerush_cluster::Node<Value>;
