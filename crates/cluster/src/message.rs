//! What pods send each other about the store: membership, tables, copies and handoffs. A mode's
//! messages, its requests and their answers, are its own.

use crate::table::{Changes, Table};
use crate::{Kept, NodeId, ShardId, Time};
use std::collections::BTreeMap;

/// A table's number, `(term, version)`: `(0, 0)` for no table. The term's upper half is the
/// cluster's generation, so a cluster formed again outnumbers every table before it.
pub type Stamp = (u64, u64);

/// One message between pods, or a table to a client, its values `V` the mode's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<V> {
    /// To every other pod, every heartbeat: who it is following, and which table it has.
    Heartbeat {
        /// Its term.
        term: u64,
        /// The leader it follows in that term, if any; itself if it leads.
        leader: Option<NodeId>,
        /// The table it has applied.
        committed: Stamp,
        /// Whether it is new and has never had a table.
        fresh: bool,
        /// When it was sent, on the sender's own clock.
        sent: Time,
        /// The pods it has heard within `stop_serving`, each with when that pod sent the latest
        /// heartbeat it heard, on that pod's clock: so that each knows who hears it, and since
        /// when, timed on its own clock.
        hears: Vec<(NodeId, Time)>,
        /// Whether it holds a lease: it and a majority of its table's voting set hear each
        /// other within `stop_serving`. So that a leader it does not hear can tell one that
        /// serves through the others from one that serves nothing.
        leased: bool,
    },
    /// Asks for the sender's committed table.
    TableRequest,
    /// A committed table.
    TableReply {
        /// The table.
        table: Table,
    },
    /// Would the receiver vote for the sender in `term`? Changes nothing.
    PreVote {
        /// The term it would stand in.
        term: u64,
        /// The table it has accepted.
        accepted: Stamp,
    },
    /// The answer to a [`Message::PreVote`].
    PreVoteReply {
        /// The term asked about.
        term: u64,
        /// Whether it would.
        granted: bool,
    },
    /// The sender stands in `term`: a receiver that has not yet gone past it takes the term,
    /// and votes for it, its one vote in that term, unless it has accepted a later table.
    StartJoin {
        /// The term.
        term: u64,
        /// The table the candidate has accepted.
        accepted: Stamp,
    },
    /// A vote for the receiver in `term`.
    Join {
        /// The term.
        term: u64,
    },
    /// A table from the leader, to be accepted.
    Publish {
        /// The table.
        table: Table,
    },
    /// A leader's next table as what changed from the one numbered `base`, to a pod whose
    /// latest heartbeat says it holds that one: in place of the whole table, nearly all of
    /// which is its shards.
    PublishChanges {
        /// The table it changes.
        base: Stamp,
        /// What changed.
        changes: Changes,
    },
    /// The table numbered `stamp` is accepted.
    PublishAck {
        /// Which.
        stamp: Stamp,
    },
    /// The table numbered `stamp` is committed: apply it.
    Commit {
        /// Which.
        stamp: Stamp,
    },
    /// A new member asks to be taken in.
    JoinCluster {
        /// Where it is reached, for the table to carry.
        address: String,
    },
    /// From the control plane, every heartbeat: what table does the pod hold now? Numbered, so
    /// that an answer shows it was written after the question was asked.
    Probe {
        /// The control plane's life, apart from every other's.
        life: u64,
        /// The question's number in that life.
        round: u64,
    },
    /// The answer to a [`Message::Probe`]: the table the pod has applied.
    Report {
        /// The life and round of the question answered.
        life: u64,
        /// See `life`.
        round: u64,
        /// The table's number, `(0, 0)` for none.
        committed: Stamp,
        /// Its voting set.
        voting: Vec<NodeId>,
    },
    /// From the control plane: the pods named are to form `generation` of the cluster, a
    /// majority of them together, led by one holding the latest table any of them had.
    Form {
        /// The generation.
        generation: u64,
        /// The pods named, in order, each with its address.
        voters: BTreeMap<NodeId, String>,
        /// The latest table any of them had applied when named, `(0, 0)` for none: a pod
        /// that has accepted none as late does not stand.
        latest: Stamp,
    },
    /// A member asks to be let go: its shards handed on, then it may stop.
    Drain,
    /// An owner's copy of what changed in `shard` since its copy numbered `after`, or of all
    /// it holds if none, to a replica: numbered `version`. One part of it: a copy goes in
    /// parts of at most [`Config::part_bytes`](crate::Config::part_bytes), each numbered the
    /// same, so that no message grows with the keys a shard holds.
    Replicate {
        /// The shard.
        shard: ShardId,
        /// Its term, as the owner knows it.
        term: u64,
        /// The replica's assignment to the shard, by the owner's number for it.
        assignment: u64,
        /// The copy it follows, or none for all the shard holds.
        after: Option<u64>,
        /// Its number.
        version: u64,
        /// The number of the owner's table it was sent under.
        under: Stamp,
        /// Each key, with its value and when it may be forgotten.
        entries: Vec<(u64, Kept<V>)>,
        /// Keys the owner no longer holds, evicted: dropped by a replica holding the copies
        /// before this one, as a deletion is not to be taken out of order.
        dropped: Vec<u64>,
        /// The part's number in the copy, from 0.
        part: u32,
        /// Whether more parts follow: the copy is held, and said held, only once its last
        /// part is in with every part before it.
        more: bool,
    },
    /// A replica's answer to a [`Message::Replicate`]: how far it holds `shard`, under `term`,
    /// without a gap; 0 for not at all.
    Replicated {
        /// The shard.
        shard: ShardId,
        /// Its term.
        term: u64,
        /// The assignment the copies were for.
        assignment: u64,
        /// The number of the last copy it holds every one up to.
        version: u64,
    },
    /// A replica's answer to each part of a [`Message::Replicate`] in more than one: it has
    /// taken, in turn, every part of the copy up to `part`. An owner sends a copy of many parts
    /// only a few unanswered parts ahead, so that neither end holds more than those of it. How
    /// far the replica then holds the shard it says, as for any copy, by
    /// [`Message::Replicated`].
    Took {
        /// The shard.
        shard: ShardId,
        /// Its term.
        term: u64,
        /// The assignment the copy was for.
        assignment: u64,
        /// The copy's number.
        version: u64,
        /// The part it has every part up to.
        part: u32,
    },
    /// A replica's word to the owner its table names that it holds nothing of `shard` from
    /// it: said every heartbeat until it is sent the shard.
    Lacking {
        /// The shard.
        shard: ShardId,
        /// Its term, as the replica knows it.
        term: u64,
    },
    /// An owner handing `shard` on stops answering for it and sends everything it holds. One
    /// part of it, as a [`Message::Replicate`] is.
    Handoff {
        /// The shard.
        shard: ShardId,
        /// Its term, as the owner knows it.
        term: u64,
        /// Every key it holds, with its value and when it may be forgotten.
        entries: Vec<(u64, Kept<V>)>,
        /// Keys it no longer holds, evicted, that the taker may hold as a replica.
        dropped: Vec<u64>,
        /// The part's number, from 0.
        part: u32,
        /// Whether more parts follow: the changes are in only once the last is, with every
        /// part before it.
        more: bool,
    },
    /// From an owner to the taker of `shard`: it has caught up on the move planned from table
    /// `from`; it tells the leader so itself, which only a taker alive can.
    CaughtUp {
        /// The shard.
        shard: ShardId,
        /// See [`Message::HandedOff`].
        from: Stamp,
    },
    /// From a taker, to the leader and every voter: it has caught up on the move of `shard`
    /// planned from table `from`, and the table may name it the owner.
    HandedOff {
        /// The shard.
        shard: ShardId,
        /// The taker.
        to: NodeId,
        /// The number of the table the move was planned from: a report on a move since
        /// dropped, or asked for again, completes nothing.
        from: Stamp,
    },
}
