//! What clients and the synchronous mode's pods send each other: requests and their answers,
//! beside the store's own messages, which travel inside them.

use crate::{Rule, Value};
use edgerush_cluster::{NodeId, Stamp, Table};

/// One message between pods, or between a client and a pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// The store's: membership, tables, copies and handoffs; to a client, a table.
    Store(edgerush_cluster::Message<Value>),
    /// A client's request under `key`: count one.
    Request {
        /// The client, to be answered whoever answers.
        client: NodeId,
        /// The client's number for it.
        id: u64,
        /// The key.
        key: u64,
        /// The client's table.
        stamp: Stamp,
        /// Forwarded this many times already.
        hops: u8,
        /// The pod that forwarded it, if one did: answered through it, as the client may reach
        /// that pod alone.
        via: Option<NodeId>,
        /// Whether it gives back one counted before, for a request then refused: routed to the
        /// key's owner now, as any request is, whoever counted it.
        give_back: bool,
        /// The limit it is held to, decided by the key's owner; none for a plain count.
        rule: Option<Rule>,
        /// For a give-back under a rule, the receipt the charge was answered with.
        receipt: u64,
    },
    /// A client's request under several `keys`, to be counted under all or reported not
    /// counted: the pod it reaches gathers the parts from their owners (Q5).
    Gather {
        /// The client, answered by the gathering pod.
        client: NodeId,
        /// The client's number for it.
        id: u64,
        /// The keys.
        keys: Vec<u64>,
    },
    /// The answer to a [`Message::Request`].
    Reply {
        /// The client it is for: a pod receiving it passes it on.
        client: NodeId,
        /// The client's number for the request.
        id: u64,
        /// The answer.
        answer: Answer,
    },
}

/// What a pod answers a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// Counted: the key's count now.
    Counted(u64),
    /// Admitted under its rule and charged, with the receipt a give-back of it names.
    Admitted(u64),
    /// Refused under its rule, its limit reached; charged nothing.
    Limited,
    /// Not this pod's: the table it has, newer than the client's, if it is.
    Elsewhere(Option<Table>),
    /// This pod's, but it is not serving: it has no lease, or is handing the shard on.
    NotServing,
}

/// What a pod gives back to be sent: each message with whom it is for.
pub type Outbox = Vec<(NodeId, Message)>;
