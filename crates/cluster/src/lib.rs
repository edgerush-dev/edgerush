//! The rate-limiting service's cluster layer, as each pod's part of it: a pure state machine
//! fed messages and the time, giving back the messages to send (`membership-design.md`,
//! candidate B). It knows nothing of rate limiting: each mode keeps a value of its own under
//! each key ([`Held`]) and decides what it holds, in a layer of its own over the store.
//!
//! - **Members.** Every pod, in every life, is a member with an identity of its own: a pod
//!   restarted is a new member, voted in again, so nothing need outlive it.
//! - **The voting set.** The members whose votes count, kept odd by the leader, changed so that
//!   every change needs a majority of the old set and of the new one.
//! - **The leader.** Elected by the voting set — a term per election, one vote per member per
//!   term, a pre-vote first that a voter refuses while it still hears a live leader — and
//!   standing down once it hears no majority.
//! - **The table.** Published by the leader, numbered by term and version, counting once a
//!   majority of the voting set has accepted it: the members, each with its address, the
//!   voting set, and a fixed number of shards, each with an owner, replicas and a shard term
//!   raised whenever its owner changes.
//! - **Serving.** An owner answers for its shards while the table it holds names it and it
//!   has heard a majority of the voting set within `stop_serving`: a lease. No leader is
//!   needed to serve; only changing the table waits for one.
//! - **Replication.** Each owner sends its replicas what changed, every cycle; a copy carries
//!   its shard term, and a pod refuses one older than it knows.
//! - **Keys.** A key's owner updates it ([`Node::update`]) while it serves the key's shard;
//!   every pod forgets a key once its time comes, by its own wall clock, and past
//!   `max_key_bytes` evicts those it may forget soonest.
//! - **Forming.** Pods never form a cluster on their own: the control plane names the pods to
//!   form each generation of it, the first and any after a majority of the voting set is gone
//!   ([`authority`]). A generation is the upper half of every term, so a cluster formed again
//!   outnumbers the old one in every term and table.
//!
//! Nothing here touches a socket, a thread or a clock: the time is passed in, in nanoseconds
//! of the pod's own clock, and what to send is given back, so that a simulation can drive many
//! pods deterministically.

#![forbid(unsafe_code)]

pub mod authority;
pub mod config;
pub mod kept;
pub mod message;
pub mod node;
pub mod table;

pub use authority::Authority;
pub use config::{Config, ConfigError};
pub use kept::{Held, Kept};
pub use message::{Message, Stamp};
pub use node::{Changed, Node, Outbox, Role};
pub use table::{Shard, Table};

/// A member: one pod in one life.
pub type NodeId = u64;

/// One of the table's shards, by number.
pub type ShardId = u16;

/// A time on a pod's own clock, in nanoseconds.
pub type Time = u64;

/// The generation of the cluster a term is in: its upper half.
#[must_use]
pub const fn generation_of(term: u64) -> u64 {
    term >> 32
}

/// The first term of `generation`.
#[must_use]
pub const fn first_term(generation: u64) -> u64 {
    (generation << 32) | 1
}

/// Milliseconds as [`Time`].
#[must_use]
pub const fn ms(millis: u64) -> Time {
    millis * 1_000_000
}
