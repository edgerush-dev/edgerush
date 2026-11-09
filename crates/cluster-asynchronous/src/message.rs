//! What data planes and the asynchronous mode's pods send each other: a data plane's batch of
//! reports and the owner's answers to it, entries forwarded to their owner and its answers
//! relayed back, beside the store's own messages, which travel inside them.

use crate::Value;
use crate::owner::Answered;
use edgerush_cluster::{NodeId, Stamp, Table};
use edgerush_ratelimit::service::{Contract, PlaneId, Report};

/// One message between pods, or between a data plane and a pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// The store's: membership, tables, copies and handoffs; to a data plane, a table.
    Store(edgerush_cluster::Message<Value>),
    /// A data plane's reports on keys it believes the pod owns.
    Batch(Batch),
    /// The pod's answers to a batch.
    Answers(Answers),
    /// The entries of a data plane's batch for keys its table names another pod the owner of,
    /// forwarded by the pod they reached to that owner, once: for a data plane that cannot
    /// reach the owner itself.
    Forwarded {
        /// The data plane, to be answered through `via`.
        client: NodeId,
        /// The pod that forwarded them.
        via: NodeId,
        /// The entries, as a batch of the data plane's.
        batch: Batch,
    },
    /// The owner's answers to entries forwarded, relayed back to the data plane by the pod
    /// that forwarded them.
    Relayed {
        /// The data plane.
        client: NodeId,
        /// The owner's answers.
        answers: Answers,
    },
}

/// A data plane's message to a pod: everything it has for the keys its table names that pod
/// the owner of, as today's batch to a pod is (`edgerush_ratelimit::service::Batch`), keyed by
/// the store's 64-bit keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The data plane, in one life of it.
    pub from: PlaneId,
    /// The table the data plane holds: the pod answers with its own if later.
    pub stamp: Stamp,
    /// When the data plane made it, on its own clock: what its hits' ages count back from,
    /// said back in the answers.
    pub made: u64,
    /// One entry a key, or a few for one key (batches sent again).
    pub entries: Vec<Entry>,
}

/// One key in a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The key, as the data plane hashed it.
    pub key: u64,
    /// Its contract, as the data plane holds it: a contract changed anywhere starts the key
    /// afresh.
    pub contract: Contract,
    /// What about it.
    pub report: Report,
}

/// A pod's answers to a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answers {
    /// The pod's life, never 0 when set; 0 for a pod that does not number its answers.
    pub life: u64,
    /// Counted up, wrapping, with each answer the pod gives in this life: the order in which
    /// it handled their batches.
    pub number: u64,
    /// The batch's [`Batch::made`], said back: the data plane takes the buckets as of then.
    pub made: u64,
    /// One for each entry of a key it serves.
    pub entries: Vec<Answer>,
    /// One for each entry of a key it does not serve, nothing of it applied.
    pub unserved: Vec<Unserved>,
    /// Its table, if later than the batch's.
    pub table: Option<Table>,
}

/// The owner's answer about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answer {
    /// The key.
    pub key: u64,
    /// The bucket, the data plane's highest batch applied, and the others admitting.
    pub answered: Answered,
}

/// A key a pod does not serve, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unserved {
    /// The key.
    pub key: u64,
    /// Why.
    pub why: Why,
}

/// Why a pod does not serve a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// Its table names another pod the key's owner, or none.
    Elsewhere,
    /// Its table names it the owner, but it is not serving: no lease, or handing the shard on.
    NotServing,
}

/// What a pod gives back to be sent: each message with whom it is for.
pub type Outbox = Vec<(NodeId, Message)>;

impl From<edgerush_cluster::Message<Value>> for Message {
    fn from(message: edgerush_cluster::Message<Value>) -> Self {
        Self::Store(message)
    }
}
