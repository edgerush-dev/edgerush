//! A pod of the rate-limiting service serving both modes over one cluster store
//! (`edgerush_cluster`): each key holds the synchronous mode's value or the asynchronous mode's
//! ([`Value`]), the mode its key's top bit says ([`Kind`]), and each mode's logic — the
//! synchronous mode's `Decider`, the asynchronous mode's `Applier` — runs over the one store,
//! one membership, table and memory cap ([`Pod`]). The store counts each kind's keys, bytes,
//! forgotten and evicted apart.
//!
//! The two kinds never share a key: a data plane sets the bit as it hashes, so a request whose
//! key carries the other mode's bit is not served ([`Pod::wrong_kind`]), and a copy of a value
//! of the wrong kind is refused where it comes in, on the wire.
//!
//! Like the store, a pure state machine: messages and the time in, messages out.

#![forbid(unsafe_code)]

pub mod pod;
pub mod value;

pub use pod::{Message, Outbox, Pod};
pub use value::{KIND_BIT, Kind, Value};

/// The store as a pod of both modes keeps it.
pub type Store = edgerush_cluster::Node<Value>;
