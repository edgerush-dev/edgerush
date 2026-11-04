//! The cluster store's messages on the wire (`edgerush_cluster::Message`): protobuf, derived
//! from these structs, and the conversions both ways.
//!
//! Pods speak on two streams (`membership-design.md`, "Requirements it found for the real
//! transport"): copies to replicas on one ([`ToCopy`]); everything else — heartbeats, tables,
//! votes, handoffs, the control plane's questions, and the answers to copies — on the other
//! ([`ToPeer`]), so that a pod joining a cluster of many keys, sent a snapshot of each shard
//! it is to keep, holds up no heartbeat. [`wire`] says which a message goes on.
//!
//! A mode's value travels as a message of the mode's own, through [`Value`]. As for the
//! service's other messages ([`crate::wire`]), a field a reader does not know is skipped, one
//! it misses takes its default, and a message the store cannot use — a part it needs missing,
//! a shard past 16 bits — is refused whole.
//!
//! The synchronous mode's own messages between pods — a request forwarded to its key's owner,
//! a part of one gathered, and their answers — go on the stream apart from copies too, beside
//! the store's ([`mode_wire`]).
//!
//! A data plane asks a pod on a stream of its own ([`Asking`]), and is told its answers and the
//! pod's table on it ([`Telling`]): plain messages, a table at 256 shards a few KB.
//!
//! Every message goes as one or more [`Piece`]s: a shard's whole copy can pass what one gRPC
//! message may hold, and the store sends it as one message, so the wire cuts it ([`pieces`])
//! and the receiver puts it together again ([`Joining`]) before the store sees it.

use bytes::Bytes;
use edgerush_cluster::table::{Changes as StoreChanges, Shard as StoreShard, Table as StoreTable};
use edgerush_cluster::{Held, Kept, Message, NodeId, ShardId, Stamp as StoreStamp};
use edgerush_cluster_synchronous::{
    Answer as SyncAnswer, Message as SyncMessage, Rule as SyncRule, Value as SyncValue,
};
use std::collections::BTreeMap;
use std::fmt::Debug;

/// Why a message on the wire cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    /// A part the message needs is not there: what it is, a table's number, a table, a
    /// shard's entry, a key's value.
    #[error("a message without a part it needs")]
    Missing,
    /// A shard numbered past what 16 bits hold.
    #[error("a shard numbered past 16 bits")]
    Shard,
    /// A rolling window's slot given twice.
    #[error("a slot given twice")]
    SlotTwice,
    /// A member named twice, in a table or a naming.
    #[error("a member named twice")]
    MemberTwice,
    /// A mode's message where the store's was asked for.
    #[error("a mode's message, not the store's")]
    Mode,
    /// A request forwarded more times than a byte counts.
    #[error("a request forwarded past 255 hops")]
    Hops,
}

/// A mode's value as the wire carries it: a protobuf message of the mode's own.
pub trait Value: Held {
    /// The message.
    type Wire: prost::Message + Default + Clone + PartialEq + Debug;

    /// The value as its message.
    fn to_wire(&self) -> Self::Wire;

    /// The value from its message, if the mode can use it.
    ///
    /// # Errors
    ///
    /// A message the mode cannot use.
    fn from_wire(wire: Self::Wire) -> Result<Self, Invalid>;
}

/// A table's number, `(term, version)`.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Stamp {
    /// The term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// The version.
    #[prost(uint64, tag = "2")]
    pub version: u64,
}

/// One shard of a table.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Shard {
    /// Its owner, if any.
    #[prost(uint64, optional, tag = "1")]
    pub owner: Option<u64>,
    /// Its replicas.
    #[prost(uint64, repeated, tag = "2")]
    pub replicas: Vec<u64>,
    /// Its term.
    #[prost(uint64, tag = "3")]
    pub term: u64,
}

/// A shard to be handed on.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Move {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// The member to take it.
    #[prost(uint64, tag = "2")]
    pub to: u64,
    /// The table it was planned from.
    #[prost(message, optional, tag = "3")]
    pub from: Option<Stamp>,
}

/// A member, and where it is reached.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Member {
    /// The member.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Its address, `host:port`.
    #[prost(string, tag = "2")]
    pub address: String,
}

/// A table.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Table {
    /// Its leader's term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// Its version.
    #[prost(uint64, tag = "2")]
    pub version: u64,
    /// The members, in order, each with its address.
    #[prost(message, repeated, tag = "3")]
    pub members: Vec<Member>,
    /// The voting set, in order.
    #[prost(uint64, repeated, tag = "4")]
    pub voting: Vec<u64>,
    /// The members draining.
    #[prost(uint64, repeated, tag = "5")]
    pub draining: Vec<u64>,
    /// The shards, by number.
    #[prost(message, repeated, tag = "6")]
    pub shards: Vec<Shard>,
    /// The moves.
    #[prost(message, repeated, tag = "7")]
    pub moves: Vec<Move>,
}

/// A shard changed, with its number.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Changed {
    /// The shard's number.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// The shard.
    #[prost(message, optional, tag = "2")]
    pub entry: Option<Shard>,
}

/// A table as what changed from the one before it.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Changes {
    /// The table's term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// Its version.
    #[prost(uint64, tag = "2")]
    pub version: u64,
    /// Its members, each with its address.
    #[prost(message, repeated, tag = "3")]
    pub members: Vec<Member>,
    /// Its voting set.
    #[prost(uint64, repeated, tag = "4")]
    pub voting: Vec<u64>,
    /// Its members draining.
    #[prost(uint64, repeated, tag = "5")]
    pub draining: Vec<u64>,
    /// Its moves.
    #[prost(message, repeated, tag = "6")]
    pub moves: Vec<Move>,
    /// The shards changed.
    #[prost(message, repeated, tag = "7")]
    pub shards: Vec<Changed>,
}

/// A pod heard, and when it sent the latest heartbeat heard, on its own clock.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Heard {
    /// The pod.
    #[prost(uint64, tag = "1")]
    pub pod: u64,
    /// When, in nanoseconds.
    #[prost(uint64, tag = "2")]
    pub sent: u64,
}

/// A heartbeat.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Heartbeat {
    /// Its term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// The leader it follows, if any.
    #[prost(uint64, optional, tag = "2")]
    pub leader: Option<u64>,
    /// The table it has applied.
    #[prost(message, optional, tag = "3")]
    pub committed: Option<Stamp>,
    /// Whether it has never had a table.
    #[prost(bool, tag = "4")]
    pub fresh: bool,
    /// When it was sent, on the sender's clock, in nanoseconds.
    #[prost(uint64, tag = "5")]
    pub sent: u64,
    /// The pods it hears.
    #[prost(message, repeated, tag = "6")]
    pub hears: Vec<Heard>,
    /// Whether it holds a lease.
    #[prost(bool, tag = "7")]
    pub leased: bool,
    /// The sender's wall clock when it sent it, in nanoseconds since the Unix epoch: 0 for
    /// none.
    #[prost(uint64, tag = "8")]
    pub wall: u64,
}

/// Nothing more than what the message is.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Empty {}

/// A candidate's term and the table it has accepted: a pre-vote or a stand.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Candidacy {
    /// The term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// The table accepted.
    #[prost(message, optional, tag = "2")]
    pub accepted: Option<Stamp>,
}

/// The answer to a pre-vote.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct PreVoteReply {
    /// The term asked about.
    #[prost(uint64, tag = "1")]
    pub term: u64,
    /// Whether it would.
    #[prost(bool, tag = "2")]
    pub granted: bool,
}

/// A vote.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Join {
    /// Its term.
    #[prost(uint64, tag = "1")]
    pub term: u64,
}

/// A table as what changed from the one numbered `base`.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct PublishChanges {
    /// The table it changes.
    #[prost(message, optional, tag = "1")]
    pub base: Option<Stamp>,
    /// What changed.
    #[prost(message, optional, tag = "2")]
    pub changes: Option<Changes>,
}

/// The control plane's question.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Probe {
    /// The control plane's life.
    #[prost(uint64, tag = "1")]
    pub life: u64,
    /// The question's number in it.
    #[prost(uint64, tag = "2")]
    pub round: u64,
}

/// The answer to a probe.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Report {
    /// The life asked in.
    #[prost(uint64, tag = "1")]
    pub life: u64,
    /// The round.
    #[prost(uint64, tag = "2")]
    pub round: u64,
    /// The table applied.
    #[prost(message, optional, tag = "3")]
    pub committed: Option<Stamp>,
    /// Its voting set.
    #[prost(uint64, repeated, tag = "4")]
    pub voting: Vec<u64>,
}

/// A new member asking in.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct JoinCluster {
    /// Where it is reached.
    #[prost(string, tag = "1")]
    pub address: String,
}

/// The control plane's naming of the pods to form a generation.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Form {
    /// The generation.
    #[prost(uint64, tag = "1")]
    pub generation: u64,
    /// The pods, in order, each with its address.
    #[prost(message, repeated, tag = "2")]
    pub voters: Vec<Member>,
    /// The latest table any had applied.
    #[prost(message, optional, tag = "3")]
    pub latest: Option<Stamp>,
}

/// What a pod says of itself as each of its streams to another opens, and to a seed it asks
/// and from one: who it is, where it is reached and how many shards its cluster has.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Hello {
    /// Its member id.
    #[prost(uint64, tag = "1")]
    pub from: u64,
    /// Where it is reached.
    #[prost(string, tag = "2")]
    pub address: String,
    /// Its cluster's shard count.
    #[prost(uint32, tag = "3")]
    pub shards: u32,
}

/// A key, its value, and when it may be forgotten.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Entry<W: prost::Message + Default + Debug> {
    /// The key.
    #[prost(fixed64, tag = "1")]
    pub key: u64,
    /// Its value, as the mode's message.
    #[prost(message, optional, tag = "2")]
    pub value: Option<W>,
    /// From when it may be forgotten, in nanoseconds of the wall clock.
    #[prost(uint64, tag = "3")]
    pub forget_at: u64,
}

/// A copy to a replica.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Replicate<W: prost::Message + Default + Debug> {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// Its term.
    #[prost(uint64, tag = "2")]
    pub term: u64,
    /// The replica's assignment.
    #[prost(uint64, tag = "3")]
    pub assignment: u64,
    /// The copy it follows; none for all the shard holds.
    #[prost(uint64, optional, tag = "4")]
    pub after: Option<u64>,
    /// Its number.
    #[prost(uint64, tag = "5")]
    pub version: u64,
    /// The table it was sent under.
    #[prost(message, optional, tag = "6")]
    pub under: Option<Stamp>,
    /// The keys.
    #[prost(message, repeated, tag = "7")]
    pub entries: Vec<Entry<W>>,
    /// The keys evicted.
    #[prost(fixed64, repeated, tag = "8")]
    pub dropped: Vec<u64>,
    /// The part's number in the copy, from 0.
    #[prost(uint32, tag = "9")]
    pub part: u32,
    /// Whether more parts follow.
    #[prost(bool, tag = "10")]
    pub more: bool,
}

/// A replica's answer to a copy.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Replicated {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// Its term.
    #[prost(uint64, tag = "2")]
    pub term: u64,
    /// The assignment.
    #[prost(uint64, tag = "3")]
    pub assignment: u64,
    /// The last copy held without a gap.
    #[prost(uint64, tag = "4")]
    pub version: u64,
}

/// A replica's answer to a part of a copy in more than one.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Took {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// Its term.
    #[prost(uint64, tag = "2")]
    pub term: u64,
    /// The assignment.
    #[prost(uint64, tag = "3")]
    pub assignment: u64,
    /// The copy's number.
    #[prost(uint64, tag = "4")]
    pub version: u64,
    /// The part it has every part up to.
    #[prost(uint32, tag = "5")]
    pub part: u32,
}

/// A replica's word that it holds nothing of a shard.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Lacking {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// Its term.
    #[prost(uint64, tag = "2")]
    pub term: u64,
}

/// A shard handed on, everything its owner holds.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Handoff<W: prost::Message + Default + Debug> {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// Its term.
    #[prost(uint64, tag = "2")]
    pub term: u64,
    /// The keys.
    #[prost(message, repeated, tag = "3")]
    pub entries: Vec<Entry<W>>,
    /// The keys evicted.
    #[prost(fixed64, repeated, tag = "4")]
    pub dropped: Vec<u64>,
    /// The part's number, from 0.
    #[prost(uint32, tag = "5")]
    pub part: u32,
    /// Whether more parts follow.
    #[prost(bool, tag = "6")]
    pub more: bool,
}

/// An owner's word to a taker that it has caught up.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct CaughtUp {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// The table the move was planned from.
    #[prost(message, optional, tag = "2")]
    pub from: Option<Stamp>,
}

/// A taker's word that a move is complete.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct HandedOff {
    /// The shard.
    #[prost(uint32, tag = "1")]
    pub shard: u32,
    /// The taker.
    #[prost(uint64, tag = "2")]
    pub to: u64,
    /// The table the move was planned from.
    #[prost(message, optional, tag = "3")]
    pub from: Option<Stamp>,
}

/// What goes on the stream apart from copies.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Control<W: prost::Message + Default + Debug> {
    /// A heartbeat.
    #[prost(message, tag = "2")]
    Heartbeat(Heartbeat),
    /// A request for the committed table.
    #[prost(message, tag = "3")]
    TableRequest(Empty),
    /// A committed table.
    #[prost(message, tag = "4")]
    TableReply(Table),
    /// A pre-vote.
    #[prost(message, tag = "5")]
    PreVote(Candidacy),
    /// Its answer.
    #[prost(message, tag = "6")]
    PreVoteReply(PreVoteReply),
    /// A stand.
    #[prost(message, tag = "7")]
    StartJoin(Candidacy),
    /// A vote.
    #[prost(message, tag = "8")]
    Join(Join),
    /// A table to accept.
    #[prost(message, tag = "9")]
    Publish(Table),
    /// A table to accept, as what changed.
    #[prost(message, tag = "10")]
    PublishChanges(PublishChanges),
    /// A table accepted.
    #[prost(message, tag = "11")]
    PublishAck(Stamp),
    /// A table to apply.
    #[prost(message, tag = "12")]
    Commit(Stamp),
    /// A new member asking in.
    #[prost(message, tag = "13")]
    JoinCluster(JoinCluster),
    /// The control plane's question.
    #[prost(message, tag = "14")]
    Probe(Probe),
    /// Its answer.
    #[prost(message, tag = "15")]
    Report(Report),
    /// The control plane's naming.
    #[prost(message, tag = "16")]
    Form(Form),
    /// A member asking to be let go.
    #[prost(message, tag = "17")]
    Drain(Empty),
    /// A replica holding nothing of a shard.
    #[prost(message, tag = "18")]
    Lacking(Lacking),
    /// A shard handed on.
    #[prost(message, tag = "19")]
    Handoff(Handoff<W>),
    /// An owner caught up on a move.
    #[prost(message, tag = "20")]
    CaughtUp(CaughtUp),
    /// A move complete.
    #[prost(message, tag = "21")]
    HandedOff(HandedOff),
    /// A part of a copy taken.
    #[prost(message, tag = "22")]
    Took(Took),
    /// A replica's answer to a copy: here rather than on the copy stream, so that it never
    /// waits behind bulk copies past the time an owner repairs a copy unanswered.
    #[prost(message, tag = "23")]
    Replicated(Replicated),
    /// The synchronous mode's request, forwarded to its key's owner or asked of it as a part.
    #[prost(message, tag = "24")]
    Request(Request),
    /// Its answer.
    #[prost(message, tag = "25")]
    Reply(Reply),
}

/// The synchronous mode's request under one key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Request {
    /// The client, answered whoever answers.
    #[prost(uint64, tag = "1")]
    pub client: u64,
    /// The client's number for it.
    #[prost(uint64, tag = "2")]
    pub id: u64,
    /// The key.
    #[prost(uint64, tag = "3")]
    pub key: u64,
    /// The client's table.
    #[prost(message, optional, tag = "4")]
    pub stamp: Option<Stamp>,
    /// Forwarded this many times already.
    #[prost(uint32, tag = "5")]
    pub hops: u32,
    /// The pod that forwarded it, if one did.
    #[prost(uint64, optional, tag = "6")]
    pub via: Option<u64>,
    /// Whether it gives one back.
    #[prost(bool, tag = "7")]
    pub give_back: bool,
    /// The limit it is held to; none for a plain count.
    #[prost(message, optional, tag = "8")]
    pub rule: Option<Rule>,
    /// For a give-back under a rule, the receipt its charge was answered with.
    #[prost(uint64, tag = "9")]
    pub receipt: u64,
}

/// A limit a request is held to.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Rule {
    /// Which, and its numbers.
    #[prost(oneof = "Limit", tags = "1, 2, 3")]
    pub limit: Option<Limit>,
}

/// A limit's model.
#[derive(Clone, Copy, PartialEq, Eq, prost::Oneof)]
pub enum Limit {
    /// A fixed window.
    #[prost(message, tag = "1")]
    Window(WindowLimit),
    /// A rolling window.
    #[prost(message, tag = "2")]
    Rolling(WindowLimit),
    /// A bucket.
    #[prost(message, tag = "3")]
    Bucket(BucketLimit),
}

/// A window's limit: `per` in each `window`, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct WindowLimit {
    /// Requests a window.
    #[prost(uint64, tag = "1")]
    pub per: u64,
    /// The window.
    #[prost(uint64, tag = "2")]
    pub window: u64,
}

/// A bucket's limit: `per` every `period`, in nanoseconds, `burst` at once.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct BucketLimit {
    /// Requests a period.
    #[prost(uint64, tag = "1")]
    pub per: u64,
    /// The period.
    #[prost(uint64, tag = "2")]
    pub period: u64,
    /// Requests at once from idle.
    #[prost(uint64, tag = "3")]
    pub burst: u64,
}

/// The answer to a [`Request`].
#[derive(Clone, PartialEq, prost::Message)]
pub struct Reply {
    /// The client it is for.
    #[prost(uint64, tag = "1")]
    pub client: u64,
    /// The client's number for the request.
    #[prost(uint64, tag = "2")]
    pub id: u64,
    /// The answer.
    #[prost(message, optional, tag = "3")]
    pub answer: Option<Answer>,
}

/// What an owner answers a request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Answer {
    /// Which.
    #[prost(oneof = "Answered", tags = "1, 2, 3, 4, 5")]
    pub answered: Option<Answered>,
}

/// What an owner answers, and what it says.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Answered {
    /// Counted: the key's count now.
    #[prost(uint64, tag = "1")]
    Counted(u64),
    /// Admitted under its rule, with the receipt a give-back of it names.
    #[prost(uint64, tag = "2")]
    Admitted(u64),
    /// Refused under its rule: how long until the key would admit, in nanoseconds.
    #[prost(uint64, tag = "3")]
    Limited(u64),
    /// Not this pod's: the newer table, if there is one.
    #[prost(message, tag = "4")]
    Elsewhere(Elsewhere),
    /// This pod's, but it is not serving.
    #[prost(message, tag = "5")]
    NotServing(Empty),
}

/// What a data plane asks a pod on its stream: a request under one key, or one under several
/// to gather. The stream names the client, so neither need say who asks.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Asking {
    /// Which.
    #[prost(oneof = "Asked", tags = "1, 2")]
    pub asked: Option<Asked>,
}

/// What a data plane asks.
#[derive(Clone, PartialEq, Eq, prost::Oneof)]
pub enum Asked {
    /// A request under one key, or a give-back.
    #[prost(message, tag = "1")]
    Request(Request),
    /// A request under several keys, to be counted under all or reported not counted.
    #[prost(message, tag = "2")]
    Gather(Gather),
}

/// A request under several keys.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Gather {
    /// The data plane's number for it.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// The keys.
    #[prost(uint64, repeated, tag = "2")]
    pub keys: Vec<u64>,
}

/// What a pod tells a data plane on its stream: an answer, or the table it has committed.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Telling {
    /// Which.
    #[prost(oneof = "Told", tags = "1, 2")]
    pub told: Option<Told>,
}

/// What a pod tells.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Told {
    /// Its table, on the stream's opening and on each commit.
    #[prost(message, tag = "1")]
    Table(Table),
    /// An answer, by the data plane's number.
    #[prost(message, tag = "2")]
    Reply(Reply),
}

/// Not this pod's key.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Elsewhere {
    /// The table the asker should have, newer than its own; none if it is not.
    #[prost(message, optional, tag = "1")]
    pub table: Option<Table>,
}

/// One message on the stream apart from copies.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ToPeer<W: prost::Message + Default + Debug> {
    /// The sender.
    #[prost(uint64, tag = "1")]
    pub from: u64,
    /// What it is.
    #[prost(
        oneof = "Control",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25"
    )]
    pub message: Option<Control<W>>,
}

/// What goes on the copy stream.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Copying<W: prost::Message + Default + Debug> {
    /// A copy.
    #[prost(message, tag = "2")]
    Replicate(Replicate<W>),
}

/// One message on the copy stream.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ToCopy<W: prost::Message + Default + Debug> {
    /// The sender.
    #[prost(uint64, tag = "1")]
    pub from: u64,
    /// What it is.
    #[prost(oneof = "Copying", tags = "2")]
    pub message: Option<Copying<W>>,
}

/// A store's message from `from`, as the wire carries it, on the stream it goes on.
#[derive(Clone, Debug, PartialEq)]
pub enum Outgoing<W: prost::Message + Default + Debug> {
    /// Apart from copies.
    Peer(ToPeer<W>),
    /// A copy, or an answer to one.
    Copy(ToCopy<W>),
}

/// `message`, from `from`, as the wire carries it.
pub fn wire<V: Value>(from: NodeId, message: Message<V>) -> Outgoing<V::Wire> {
    let peer = |message| {
        Outgoing::Peer(ToPeer {
            from,
            message: Some(message),
        })
    };
    match message {
        Message::Replicate {
            shard,
            term,
            assignment,
            after,
            version,
            under,
            entries,
            dropped,
            part,
            more,
        } => Outgoing::Copy(ToCopy {
            from,
            message: Some(Copying::Replicate(Replicate {
                shard: u32::from(shard),
                term,
                assignment,
                after,
                version,
                under: Some(stamp(under)),
                entries: entries_to_wire(entries),
                dropped,
                part,
                more,
            })),
        }),
        Message::Replicated {
            shard,
            term,
            assignment,
            version,
        } => peer(Control::Replicated(Replicated {
            shard: u32::from(shard),
            term,
            assignment,
            version,
        })),
        Message::Heartbeat {
            term,
            leader,
            committed,
            fresh,
            sent,
            wall,
            hears,
            leased,
        } => peer(Control::Heartbeat(Heartbeat {
            term,
            leader,
            committed: Some(stamp(committed)),
            fresh,
            sent,
            hears: hears
                .into_iter()
                .map(|(pod, sent)| Heard { pod, sent })
                .collect(),
            leased,
            wall,
        })),
        Message::TableRequest => peer(Control::TableRequest(Empty {})),
        Message::TableReply { table } => peer(Control::TableReply(table_to_wire(table))),
        Message::PreVote { term, accepted } => peer(Control::PreVote(Candidacy {
            term,
            accepted: Some(stamp(accepted)),
        })),
        Message::PreVoteReply { term, granted } => {
            peer(Control::PreVoteReply(PreVoteReply { term, granted }))
        }
        Message::StartJoin { term, accepted } => peer(Control::StartJoin(Candidacy {
            term,
            accepted: Some(stamp(accepted)),
        })),
        Message::Join { term } => peer(Control::Join(Join { term })),
        Message::Publish { table } => peer(Control::Publish(table_to_wire(table))),
        Message::PublishChanges { base, changes } => {
            peer(Control::PublishChanges(PublishChanges {
                base: Some(stamp(base)),
                changes: Some(Changes {
                    term: changes.term,
                    version: changes.version,
                    members: members_to_wire(changes.members),
                    voting: changes.voting,
                    draining: changes.draining,
                    moves: moves_to_wire(changes.moves),
                    shards: changes
                        .shards
                        .into_iter()
                        .map(|(shard, entry)| Changed {
                            shard: u32::from(shard),
                            entry: Some(shard_to_wire(entry)),
                        })
                        .collect(),
                }),
            }))
        }
        Message::PublishAck { stamp: acked } => peer(Control::PublishAck(stamp(acked))),
        Message::Commit { stamp: committed } => peer(Control::Commit(stamp(committed))),
        Message::JoinCluster { address } => peer(Control::JoinCluster(JoinCluster { address })),
        Message::Probe { life, round } => peer(Control::Probe(Probe { life, round })),
        Message::Report {
            life,
            round,
            committed,
            voting,
        } => peer(Control::Report(Report {
            life,
            round,
            committed: Some(stamp(committed)),
            voting,
        })),
        Message::Form {
            generation,
            voters,
            latest,
        } => peer(Control::Form(Form {
            generation,
            voters: members_to_wire(voters),
            latest: Some(stamp(latest)),
        })),
        Message::Drain => peer(Control::Drain(Empty {})),
        Message::Took {
            shard,
            term,
            assignment,
            version,
            part,
        } => peer(Control::Took(Took {
            shard: u32::from(shard),
            term,
            assignment,
            version,
            part,
        })),
        Message::Lacking { shard, term } => peer(Control::Lacking(Lacking {
            shard: u32::from(shard),
            term,
        })),
        Message::Handoff {
            shard,
            term,
            entries,
            dropped,
            part,
            more,
        } => peer(Control::Handoff(Handoff {
            shard: u32::from(shard),
            term,
            entries: entries_to_wire(entries),
            dropped,
            part,
            more,
        })),
        Message::CaughtUp {
            shard,
            from: planned,
        } => peer(Control::CaughtUp(CaughtUp {
            shard: u32::from(shard),
            from: Some(stamp(planned)),
        })),
        Message::HandedOff {
            shard,
            to,
            from: planned,
        } => peer(Control::HandedOff(HandedOff {
            shard: u32::from(shard),
            to,
            from: Some(stamp(planned)),
        })),
    }
}

/// The sender and the store's message, from the stream apart from copies.
///
/// # Errors
///
/// A message the store cannot use: see [`Invalid`].
pub fn from_peer<V: Value>(sent: ToPeer<V::Wire>) -> Result<(NodeId, Message<V>), Invalid> {
    let message = match sent.message.ok_or(Invalid::Missing)? {
        Control::Request(_) | Control::Reply(_) => return Err(Invalid::Mode),
        Control::Heartbeat(beat) => Message::Heartbeat {
            term: beat.term,
            leader: beat.leader,
            committed: stamp_from(beat.committed)?,
            fresh: beat.fresh,
            sent: beat.sent,
            wall: beat.wall,
            hears: beat
                .hears
                .into_iter()
                .map(|heard| (heard.pod, heard.sent))
                .collect(),
            leased: beat.leased,
        },
        Control::TableRequest(_) => Message::TableRequest,
        Control::TableReply(table) => Message::TableReply {
            table: table_from(table)?,
        },
        Control::PreVote(candidacy) => Message::PreVote {
            term: candidacy.term,
            accepted: stamp_from(candidacy.accepted)?,
        },
        Control::PreVoteReply(reply) => Message::PreVoteReply {
            term: reply.term,
            granted: reply.granted,
        },
        Control::StartJoin(candidacy) => Message::StartJoin {
            term: candidacy.term,
            accepted: stamp_from(candidacy.accepted)?,
        },
        Control::Join(join) => Message::Join { term: join.term },
        Control::Publish(table) => Message::Publish {
            table: table_from(table)?,
        },
        Control::PublishChanges(publish) => {
            let changes = publish.changes.ok_or(Invalid::Missing)?;
            Message::PublishChanges {
                base: stamp_from(publish.base)?,
                changes: StoreChanges {
                    term: changes.term,
                    version: changes.version,
                    members: members_from(changes.members)?,
                    voting: changes.voting,
                    draining: changes.draining,
                    moves: moves_from(changes.moves)?,
                    shards: changes
                        .shards
                        .into_iter()
                        .map(|changed| {
                            Ok((
                                shard_id(changed.shard)?,
                                shard_from(changed.entry.ok_or(Invalid::Missing)?),
                            ))
                        })
                        .collect::<Result<_, Invalid>>()?,
                },
            }
        }
        Control::PublishAck(acked) => Message::PublishAck {
            stamp: (acked.term, acked.version),
        },
        Control::Commit(committed) => Message::Commit {
            stamp: (committed.term, committed.version),
        },
        Control::JoinCluster(join) => Message::JoinCluster {
            address: join.address,
        },
        Control::Probe(probe) => Message::Probe {
            life: probe.life,
            round: probe.round,
        },
        Control::Report(report) => Message::Report {
            life: report.life,
            round: report.round,
            committed: stamp_from(report.committed)?,
            voting: report.voting,
        },
        Control::Form(form) => Message::Form {
            generation: form.generation,
            voters: members_from(form.voters)?,
            latest: stamp_from(form.latest)?,
        },
        Control::Drain(_) => Message::Drain,
        Control::Took(took) => Message::Took {
            shard: shard_id(took.shard)?,
            term: took.term,
            assignment: took.assignment,
            version: took.version,
            part: took.part,
        },
        Control::Lacking(lacking) => Message::Lacking {
            shard: shard_id(lacking.shard)?,
            term: lacking.term,
        },
        Control::Handoff(handoff) => Message::Handoff {
            shard: shard_id(handoff.shard)?,
            term: handoff.term,
            entries: entries_from(handoff.entries)?,
            dropped: handoff.dropped,
            part: handoff.part,
            more: handoff.more,
        },
        Control::CaughtUp(caught) => Message::CaughtUp {
            shard: shard_id(caught.shard)?,
            from: stamp_from(caught.from)?,
        },
        Control::HandedOff(handed) => Message::HandedOff {
            shard: shard_id(handed.shard)?,
            to: handed.to,
            from: stamp_from(handed.from)?,
        },
        Control::Replicated(answer) => Message::Replicated {
            shard: shard_id(answer.shard)?,
            term: answer.term,
            assignment: answer.assignment,
            version: answer.version,
        },
    };
    Ok((sent.from, message))
}

/// The sender and the store's message, from the copy stream.
///
/// # Errors
///
/// A message the store cannot use: see [`Invalid`].
pub fn from_copy<V: Value>(sent: ToCopy<V::Wire>) -> Result<(NodeId, Message<V>), Invalid> {
    let message = match sent.message.ok_or(Invalid::Missing)? {
        Copying::Replicate(copy) => Message::Replicate {
            shard: shard_id(copy.shard)?,
            term: copy.term,
            assignment: copy.assignment,
            after: copy.after,
            version: copy.version,
            under: stamp_from(copy.under)?,
            entries: entries_from(copy.entries)?,
            dropped: copy.dropped,
            part: copy.part,
            more: copy.more,
        },
    };
    Ok((sent.from, message))
}

/// The synchronous mode's `message`, from `from`, as the wire carries it between pods: the
/// store's as [`wire`] has it, the mode's own on the stream apart from copies. None for a
/// request to gather, which a client alone asks, of the pod it reaches.
pub fn mode_wire(from: NodeId, message: SyncMessage) -> Option<Outgoing<Synchronous>> {
    let peer = |message| {
        Outgoing::Peer(ToPeer {
            from,
            message: Some(message),
        })
    };
    Some(match message {
        SyncMessage::Store(message) => wire(from, message),
        SyncMessage::Request {
            client,
            id,
            key,
            stamp: asked,
            hops,
            via,
            give_back,
            rule,
            receipt,
        } => peer(Control::Request(Request {
            client,
            id,
            key,
            stamp: Some(stamp(asked)),
            hops: u32::from(hops),
            via,
            give_back,
            rule: rule.map(rule_to_wire),
            receipt,
        })),
        SyncMessage::Reply { client, id, answer } => peer(Control::Reply(Reply {
            client,
            id,
            answer: Some(answer_to_wire(answer)),
        })),
        SyncMessage::Gather { .. } => return None,
    })
}

/// The sender and the synchronous mode's message, from the stream apart from copies.
///
/// # Errors
///
/// A message the mode or its store cannot use: see [`Invalid`].
pub fn mode_from_peer(sent: ToPeer<Synchronous>) -> Result<(NodeId, SyncMessage), Invalid> {
    let from = sent.from;
    match sent.message {
        Some(Control::Request(request)) => Ok((from, request_from(request)?)),
        Some(Control::Reply(reply)) => Ok((
            from,
            SyncMessage::Reply {
                client: reply.client,
                id: reply.id,
                answer: answer_from(reply.answer.ok_or(Invalid::Missing)?)?,
            },
        )),
        message => from_peer::<SyncValue>(ToPeer { from, message })
            .map(|(from, message)| (from, SyncMessage::Store(message))),
    }
}

/// The sender and the synchronous mode's message, from the copy stream: the store's alone.
///
/// # Errors
///
/// A message the store cannot use: see [`Invalid`].
pub fn mode_from_copy(sent: ToCopy<Synchronous>) -> Result<(NodeId, SyncMessage), Invalid> {
    from_copy::<SyncValue>(sent).map(|(from, message)| (from, SyncMessage::Store(message)))
}

/// What a data plane asks, as the mode's message from `client`, the stream's: whoever the
/// request says asks, it is the stream's client, and its first hop.
///
/// # Errors
///
/// A message the mode cannot use: see [`Invalid`].
pub fn asked_from(asking: Asking, client: NodeId) -> Result<SyncMessage, Invalid> {
    match asking.asked.ok_or(Invalid::Missing)? {
        Asked::Request(request) => {
            let mut message = request_from(request)?;
            if let SyncMessage::Request {
                client: asker,
                hops,
                via,
                ..
            } = &mut message
            {
                *asker = client;
                *hops = 0;
                *via = None;
            }
            Ok(message)
        }
        Asked::Gather(gather) => Ok(SyncMessage::Gather {
            client,
            id: gather.id,
            keys: gather.keys,
        }),
    }
}

/// A data plane's `message` as its stream carries it up: a request or a request to gather;
/// none for anything else.
#[must_use]
pub fn asking(message: SyncMessage) -> Option<Asking> {
    let asked = match message {
        message @ SyncMessage::Request { .. } => match mode_wire(0, message) {
            Some(Outgoing::Peer(ToPeer {
                message: Some(Control::Request(request)),
                ..
            })) => Asked::Request(request),
            _ => return None,
        },
        SyncMessage::Gather { id, keys, .. } => Asked::Gather(Gather { id, keys }),
        _ => return None,
    };
    Some(Asking { asked: Some(asked) })
}

/// What the mode tells a client, as a data plane's stream carries it down: an answer, or a
/// table; none for anything else.
#[must_use]
pub fn telling(message: SyncMessage) -> Option<Telling> {
    let told = match message {
        SyncMessage::Reply { client, id, answer } => Told::Reply(Reply {
            client,
            id,
            answer: Some(answer_to_wire(answer)),
        }),
        SyncMessage::Store(Message::TableReply { table }) => Told::Table(table_to_wire(table)),
        _ => return None,
    };
    Some(Telling { told: Some(told) })
}

/// What a pod told, as the mode's message: an answer, or its table.
///
/// # Errors
///
/// A message the mode cannot use: see [`Invalid`].
pub fn told_from(telling: Telling) -> Result<SyncMessage, Invalid> {
    Ok(match telling.told.ok_or(Invalid::Missing)? {
        Told::Table(table) => SyncMessage::Store(Message::TableReply {
            table: table_from(table)?,
        }),
        Told::Reply(reply) => SyncMessage::Reply {
            client: reply.client,
            id: reply.id,
            answer: answer_from(reply.answer.ok_or(Invalid::Missing)?)?,
        },
    })
}

fn request_from(request: Request) -> Result<SyncMessage, Invalid> {
    Ok(SyncMessage::Request {
        client: request.client,
        id: request.id,
        key: request.key,
        stamp: stamp_from(request.stamp)?,
        hops: u8::try_from(request.hops).map_err(|_| Invalid::Hops)?,
        via: request.via,
        give_back: request.give_back,
        rule: request.rule.map(rule_from).transpose()?,
        receipt: request.receipt,
    })
}

const fn rule_to_wire(rule: SyncRule) -> Rule {
    let limit = match rule {
        SyncRule::Window { per, window } => Limit::Window(WindowLimit { per, window }),
        SyncRule::Rolling { per, window } => Limit::Rolling(WindowLimit { per, window }),
        SyncRule::Bucket { per, period, burst } => {
            Limit::Bucket(BucketLimit { per, period, burst })
        }
    };
    Rule { limit: Some(limit) }
}

fn rule_from(rule: Rule) -> Result<SyncRule, Invalid> {
    Ok(match rule.limit.ok_or(Invalid::Missing)? {
        Limit::Window(WindowLimit { per, window }) => SyncRule::Window { per, window },
        Limit::Rolling(WindowLimit { per, window }) => SyncRule::Rolling { per, window },
        Limit::Bucket(BucketLimit { per, period, burst }) => {
            SyncRule::Bucket { per, period, burst }
        }
    })
}

fn answer_to_wire(answer: SyncAnswer) -> Answer {
    let answered = match answer {
        SyncAnswer::Counted(count) => Answered::Counted(count),
        SyncAnswer::Admitted(receipt) => Answered::Admitted(receipt),
        SyncAnswer::Limited { wait } => Answered::Limited(wait),
        SyncAnswer::Elsewhere(table) => Answered::Elsewhere(Elsewhere {
            table: table.map(table_to_wire),
        }),
        SyncAnswer::NotServing => Answered::NotServing(Empty {}),
    };
    Answer {
        answered: Some(answered),
    }
}

fn answer_from(answer: Answer) -> Result<SyncAnswer, Invalid> {
    Ok(match answer.answered.ok_or(Invalid::Missing)? {
        Answered::Counted(count) => SyncAnswer::Counted(count),
        Answered::Admitted(receipt) => SyncAnswer::Admitted(receipt),
        Answered::Limited(wait) => SyncAnswer::Limited { wait },
        Answered::Elsewhere(elsewhere) => {
            SyncAnswer::Elsewhere(elsewhere.table.map(table_from).transpose()?)
        }
        Answered::NotServing(_) => SyncAnswer::NotServing,
    })
}

const fn stamp((term, version): StoreStamp) -> Stamp {
    Stamp { term, version }
}

fn stamp_from(stamp: Option<Stamp>) -> Result<StoreStamp, Invalid> {
    let stamp = stamp.ok_or(Invalid::Missing)?;
    Ok((stamp.term, stamp.version))
}

fn shard_id(shard: u32) -> Result<ShardId, Invalid> {
    ShardId::try_from(shard).map_err(|_| Invalid::Shard)
}

fn shard_to_wire(shard: StoreShard) -> Shard {
    Shard {
        owner: shard.owner,
        replicas: shard.replicas,
        term: shard.term,
    }
}

fn shard_from(shard: Shard) -> StoreShard {
    StoreShard {
        owner: shard.owner,
        replicas: shard.replicas,
        term: shard.term,
    }
}

fn moves_to_wire(moves: Vec<(ShardId, NodeId, StoreStamp)>) -> Vec<Move> {
    moves
        .into_iter()
        .map(|(shard, to, from)| Move {
            shard: u32::from(shard),
            to,
            from: Some(stamp(from)),
        })
        .collect()
}

fn moves_from(moves: Vec<Move>) -> Result<Vec<(ShardId, NodeId, StoreStamp)>, Invalid> {
    moves
        .into_iter()
        .map(|planned| {
            Ok((
                shard_id(planned.shard)?,
                planned.to,
                stamp_from(planned.from)?,
            ))
        })
        .collect()
}

fn members_to_wire(members: BTreeMap<NodeId, String>) -> Vec<Member> {
    members
        .into_iter()
        .map(|(id, address)| Member { id, address })
        .collect()
}

/// Members from the wire: one named twice is refused, rather than one of its addresses kept.
fn members_from(members: Vec<Member>) -> Result<BTreeMap<NodeId, String>, Invalid> {
    let mut named = BTreeMap::new();
    for member in members {
        if named.insert(member.id, member.address).is_some() {
            return Err(Invalid::MemberTwice);
        }
    }
    Ok(named)
}

fn table_to_wire(table: StoreTable) -> Table {
    Table {
        term: table.term,
        version: table.version,
        members: members_to_wire(table.members),
        voting: table.voting,
        draining: table.draining,
        shards: table.shards.into_iter().map(shard_to_wire).collect(),
        moves: moves_to_wire(table.moves),
    }
}

fn table_from(table: Table) -> Result<StoreTable, Invalid> {
    Ok(StoreTable {
        term: table.term,
        version: table.version,
        members: members_from(table.members)?,
        voting: table.voting,
        draining: table.draining,
        shards: table.shards.into_iter().map(shard_from).collect(),
        moves: moves_from(table.moves)?,
    })
}

fn entries_to_wire<V: Value>(entries: Vec<(u64, Kept<V>)>) -> Vec<Entry<V::Wire>> {
    entries
        .into_iter()
        .map(|(key, kept)| Entry {
            key,
            value: Some(kept.value.to_wire()),
            forget_at: kept.forget_at,
        })
        .collect()
}

fn entries_from<V: Value>(entries: Vec<Entry<V::Wire>>) -> Result<Vec<(u64, Kept<V>)>, Invalid> {
    entries
        .into_iter()
        .map(|entry| {
            let value = V::from_wire(entry.value.ok_or(Invalid::Missing)?)?;
            Ok((
                entry.key,
                Kept {
                    value,
                    forget_at: entry.forget_at,
                },
            ))
        })
        .collect()
}

/// A count, plain or in a fixed window.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Count {
    /// The window's number; 0 for a plain count.
    #[prost(uint64, tag = "1")]
    pub window: u64,
    /// Requests counted.
    #[prost(uint64, tag = "2")]
    pub counted: u64,
    /// Of those, given back.
    #[prost(uint64, tag = "3")]
    pub given: u64,
}

/// One slot of a rolling window.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Slot {
    /// The slot's number.
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    /// Requests counted in it.
    #[prost(uint64, tag = "2")]
    pub counted: u64,
    /// Of those, given back.
    #[prost(uint64, tag = "3")]
    pub given: u64,
}

/// A rolling window's slots, each once.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Slots {
    /// The slots.
    #[prost(message, repeated, tag = "1")]
    pub slots: Vec<Slot>,
}

/// A bucket, as GCRA keeps one.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Bucket {
    /// When its next request is due, in nanoseconds of the wall clock.
    #[prost(uint64, tag = "1")]
    pub due: u64,
    /// Requests given back.
    #[prost(uint64, tag = "2")]
    pub given: u64,
}

/// A value's shape.
#[derive(Clone, PartialEq, Eq, prost::Oneof)]
pub enum Shape {
    /// A count.
    #[prost(message, tag = "1")]
    Count(Count),
    /// A rolling window's slots.
    #[prost(message, tag = "2")]
    Slots(Slots),
    /// A bucket.
    #[prost(message, tag = "3")]
    Bucket(Bucket),
}

/// The synchronous mode's value under one key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Synchronous {
    /// Its shape.
    #[prost(oneof = "Shape", tags = "1, 2, 3")]
    pub shape: Option<Shape>,
}

impl Value for SyncValue {
    type Wire = Synchronous;

    fn to_wire(&self) -> Synchronous {
        let shape = match self {
            Self::Count {
                window,
                counted,
                given,
            } => Shape::Count(Count {
                window: *window,
                counted: *counted,
                given: *given,
            }),
            Self::Slots(slots) => Shape::Slots(Slots {
                slots: slots
                    .iter()
                    .map(|(slot, (counted, given))| Slot {
                        slot: *slot,
                        counted: *counted,
                        given: *given,
                    })
                    .collect(),
            }),
            Self::Bucket { due, given } => Shape::Bucket(Bucket {
                due: *due,
                given: *given,
            }),
        };
        Synchronous { shape: Some(shape) }
    }

    fn from_wire(wire: Synchronous) -> Result<Self, Invalid> {
        Ok(match wire.shape.ok_or(Invalid::Missing)? {
            Shape::Count(count) => Self::Count {
                window: count.window,
                counted: count.counted,
                given: count.given,
            },
            Shape::Slots(slots) => {
                let mut held = BTreeMap::new();
                for slot in slots.slots {
                    if held.insert(slot.slot, (slot.counted, slot.given)).is_some() {
                        return Err(Invalid::SlotTwice);
                    }
                }
                Self::Slots(held)
            }
            Shape::Bucket(bucket) => Self::Bucket {
                due: bucket.due,
                given: bucket.given,
            },
        })
    }
}

/// The most of a message one piece carries: a quarter of what a gRPC message may hold
/// ([`crate::grpc::MAX_MESSAGE`]), as the service's other copies are cut.
pub const PIECE: usize = crate::grpc::MAX_MESSAGE / 4;

/// Part of an encoded message, in order on its stream: one gRPC message.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Piece {
    /// The part.
    #[prost(bytes = "bytes", tag = "1")]
    pub part: Bytes,
    /// Whether more of the message follow.
    #[prost(bool, tag = "2")]
    pub more: bool,
}

/// An encoded message cut into pieces of at most `size` bytes, `size` at least 1: one piece
/// for a message that fits, empty or not.
#[must_use]
pub fn pieces(encoded: Bytes, size: usize) -> Vec<Piece> {
    let size = size.max(1);
    let count = encoded.len().div_ceil(size).max(1);
    (0..count)
        .map(|index| {
            let start = index * size;
            let end = encoded.len().min(start + size);
            Piece {
                part: encoded.slice(start..end),
                more: index + 1 < count,
            }
        })
        .collect()
}

/// A message held past what a receiver takes while it waits for the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a message longer than {most} bytes")]
pub struct TooLong {
    /// The most it takes.
    pub most: usize,
}

/// The pieces of one message, put together as they come on one stream.
#[derive(Debug)]
pub struct Joining {
    held: Vec<u8>,
    most: usize,
}

impl Joining {
    /// Nothing held yet, a message of at most `most` bytes taken.
    #[must_use]
    pub const fn new(most: usize) -> Self {
        Self {
            held: Vec::new(),
            most,
        }
    }

    /// Takes `piece`: the whole message, once its last piece is in.
    ///
    /// # Errors
    ///
    /// The message passing `most` bytes: what was held of it is dropped, and the stream it
    /// came on is to be closed, as what follows on it is the rest of a message not taken.
    pub fn take(&mut self, piece: &Piece) -> Result<Option<Vec<u8>>, TooLong> {
        if self.held.len() + piece.part.len() > self.most {
            self.held = Vec::new();
            return Err(TooLong { most: self.most });
        }
        self.held.extend_from_slice(&piece.part);
        Ok((!piece.more).then(|| std::mem::take(&mut self.held)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message as _;

    fn stamps() -> impl Strategy<Value = StoreStamp> {
        any::<(u64, u64)>()
    }

    fn nodes() -> impl Strategy<Value = Vec<NodeId>> {
        prop::collection::vec(any::<u64>(), 0..5)
    }

    fn value() -> impl Strategy<Value = SyncValue> {
        prop_oneof![
            any::<(u64, u64, u64)>().prop_map(|(window, counted, given)| SyncValue::Count {
                window,
                counted,
                given,
            }),
            prop::collection::btree_map(any::<u64>(), any::<(u64, u64)>(), 0..6)
                .prop_map(SyncValue::Slots),
            any::<(u64, u64)>().prop_map(|(due, given)| SyncValue::Bucket { due, given }),
        ]
    }

    fn entries() -> impl Strategy<Value = Vec<(u64, Kept<SyncValue>)>> {
        prop::collection::vec(
            (any::<u64>(), value(), any::<u64>())
                .prop_map(|(key, value, forget_at)| (key, Kept { value, forget_at })),
            0..5,
        )
    }

    fn shard() -> impl Strategy<Value = StoreShard> {
        (prop::option::of(any::<u64>()), nodes(), any::<u64>()).prop_map(
            |(owner, replicas, term)| StoreShard {
                owner,
                replicas,
                term,
            },
        )
    }

    /// Members with addresses, any text among them.
    fn members() -> impl Strategy<Value = BTreeMap<NodeId, String>> {
        prop::collection::btree_map(any::<u64>(), any::<String>(), 0..4)
    }

    fn moves() -> impl Strategy<Value = Vec<(ShardId, NodeId, StoreStamp)>> {
        prop::collection::vec((any::<ShardId>(), any::<u64>(), stamps()), 0..4)
    }

    fn table() -> impl Strategy<Value = StoreTable> {
        (
            any::<(u64, u64)>(),
            members(),
            nodes(),
            nodes(),
            prop::collection::vec(shard(), 0..6),
            moves(),
        )
            .prop_map(
                |((term, version), members, voting, draining, shards, moves)| StoreTable {
                    term,
                    version,
                    members,
                    voting,
                    draining,
                    shards,
                    moves,
                },
            )
    }

    fn changes() -> impl Strategy<Value = StoreChanges> {
        (
            any::<(u64, u64)>(),
            members(),
            nodes(),
            nodes(),
            moves(),
            prop::collection::vec((any::<ShardId>(), shard()), 0..4),
        )
            .prop_map(
                |((term, version), members, voting, draining, moves, shards)| StoreChanges {
                    term,
                    version,
                    members,
                    voting,
                    draining,
                    moves,
                    shards,
                },
            )
    }

    /// Every one of the store's messages, any of its fields.
    fn message() -> impl Strategy<Value = Message<SyncValue>> {
        prop_oneof![
            (
                any::<u64>(),
                prop::option::of(any::<u64>()),
                stamps(),
                any::<bool>(),
                any::<(u64, u64)>(),
                prop::collection::vec(any::<(u64, u64)>(), 0..4),
                any::<bool>(),
            )
                .prop_map(
                    |(term, leader, committed, fresh, (sent, wall), hears, leased)| {
                        Message::Heartbeat {
                            term,
                            leader,
                            committed,
                            fresh,
                            sent,
                            wall,
                            hears,
                            leased,
                        }
                    },
                ),
            Just(Message::TableRequest),
            table().prop_map(|table| Message::TableReply { table }),
            (any::<u64>(), stamps())
                .prop_map(|(term, accepted)| Message::PreVote { term, accepted }),
            (any::<u64>(), any::<bool>())
                .prop_map(|(term, granted)| Message::PreVoteReply { term, granted }),
            (any::<u64>(), stamps())
                .prop_map(|(term, accepted)| Message::StartJoin { term, accepted }),
            any::<u64>().prop_map(|term| Message::Join { term }),
            table().prop_map(|table| Message::Publish { table }),
            (stamps(), changes())
                .prop_map(|(base, changes)| Message::PublishChanges { base, changes }),
            stamps().prop_map(|stamp| Message::PublishAck { stamp }),
            stamps().prop_map(|stamp| Message::Commit { stamp }),
            any::<String>().prop_map(|address| Message::JoinCluster { address }),
            any::<(u64, u64)>().prop_map(|(life, round)| Message::Probe { life, round }),
            (any::<(u64, u64)>(), stamps(), nodes()).prop_map(
                |((life, round), committed, voting)| Message::Report {
                    life,
                    round,
                    committed,
                    voting,
                }
            ),
            (any::<u64>(), members(), stamps()).prop_map(|(generation, voters, latest)| {
                Message::Form {
                    generation,
                    voters,
                    latest,
                }
            }),
            Just(Message::Drain),
            (
                any::<ShardId>(),
                any::<(u64, u64)>(),
                prop::option::of(any::<u64>()),
                any::<u64>(),
                stamps(),
                entries(),
                prop::collection::vec(any::<u64>(), 0..4),
                any::<(u32, bool)>(),
            )
                .prop_map(
                    |(
                        shard,
                        (term, assignment),
                        after,
                        version,
                        under,
                        entries,
                        dropped,
                        (part, more),
                    )| {
                        Message::Replicate {
                            shard,
                            term,
                            assignment,
                            after,
                            version,
                            under,
                            entries,
                            dropped,
                            part,
                            more,
                        }
                    }
                ),
            (any::<ShardId>(), any::<(u64, u64, u64)>()).prop_map(
                |(shard, (term, assignment, version))| Message::Replicated {
                    shard,
                    term,
                    assignment,
                    version,
                }
            ),
            (any::<ShardId>(), any::<(u64, u64, u64)>(), any::<u32>()).prop_map(
                |(shard, (term, assignment, version), part)| Message::Took {
                    shard,
                    term,
                    assignment,
                    version,
                    part,
                }
            ),
            (any::<ShardId>(), any::<u64>())
                .prop_map(|(shard, term)| Message::Lacking { shard, term }),
            (
                any::<ShardId>(),
                any::<u64>(),
                entries(),
                prop::collection::vec(any::<u64>(), 0..4),
                any::<(u32, bool)>(),
            )
                .prop_map(|(shard, term, entries, dropped, (part, more))| {
                    Message::Handoff {
                        shard,
                        term,
                        entries,
                        dropped,
                        part,
                        more,
                    }
                }),
            (any::<ShardId>(), stamps())
                .prop_map(|(shard, from)| Message::CaughtUp { shard, from }),
            (any::<ShardId>(), any::<u64>(), stamps())
                .prop_map(|(shard, to, from)| Message::HandedOff { shard, to, from }),
        ]
    }

    /// `message` from `from` through the wire: encoded, decoded, and taken back.
    fn through(from: NodeId, message: Message<SyncValue>) -> (NodeId, Message<SyncValue>) {
        match wire(from, message) {
            Outgoing::Peer(sent) => {
                let decoded = ToPeer::decode(sent.encode_to_vec().as_slice()).unwrap();
                from_peer(decoded).unwrap()
            }
            Outgoing::Copy(sent) => {
                let decoded = ToCopy::decode(sent.encode_to_vec().as_slice()).unwrap();
                from_copy(decoded).unwrap()
            }
        }
    }

    fn rule() -> impl Strategy<Value = SyncRule> {
        prop_oneof![
            any::<(u64, u64)>().prop_map(|(per, window)| SyncRule::Window { per, window }),
            any::<(u64, u64)>().prop_map(|(per, window)| SyncRule::Rolling { per, window }),
            any::<(u64, u64, u64)>().prop_map(|(per, period, burst)| SyncRule::Bucket {
                per,
                period,
                burst
            }),
        ]
    }

    fn answer() -> impl Strategy<Value = SyncAnswer> {
        prop_oneof![
            any::<u64>().prop_map(SyncAnswer::Counted),
            any::<u64>().prop_map(SyncAnswer::Admitted),
            any::<u64>().prop_map(|wait| SyncAnswer::Limited { wait }),
            prop::option::of(table()).prop_map(SyncAnswer::Elsewhere),
            Just(SyncAnswer::NotServing),
        ]
    }

    /// Every message the synchronous mode's pods send each other, any of its fields.
    fn mode_message() -> impl Strategy<Value = SyncMessage> {
        prop_oneof![
            message().prop_map(SyncMessage::Store),
            (
                any::<(u64, u64, u64)>(),
                stamps(),
                any::<u8>(),
                prop::option::of(any::<u64>()),
                any::<bool>(),
                prop::option::of(rule()),
                any::<u64>(),
            )
                .prop_map(
                    |((client, id, key), stamp, hops, via, give_back, rule, receipt)| {
                        SyncMessage::Request {
                            client,
                            id,
                            key,
                            stamp,
                            hops,
                            via,
                            give_back,
                            rule,
                            receipt,
                        }
                    }
                ),
            (any::<(u64, u64)>(), answer()).prop_map(|((client, id), answer)| SyncMessage::Reply {
                client,
                id,
                answer
            }),
        ]
    }

    fn mode_through(from: NodeId, message: SyncMessage) -> (NodeId, SyncMessage) {
        match mode_wire(from, message).unwrap() {
            Outgoing::Peer(sent) => {
                let decoded = ToPeer::decode(sent.encode_to_vec().as_slice()).unwrap();
                mode_from_peer(decoded).unwrap()
            }
            Outgoing::Copy(sent) => {
                let decoded = ToCopy::decode(sent.encode_to_vec().as_slice()).unwrap();
                mode_from_copy(decoded).unwrap()
            }
        }
    }

    /// The store refuses the mode's messages, and a request to gather, a client's alone, is
    /// never sent between pods.
    #[test]
    fn the_store_refuses_the_modes_messages_and_a_gathering_goes_between_no_pods() {
        let request = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Request(Request::default())),
        };
        assert_eq!(from_peer::<SyncValue>(request), Err(Invalid::Mode));
        let gather = SyncMessage::Gather {
            client: 9,
            id: 1,
            keys: vec![1, 2],
        };
        assert!(mode_wire(1, gather).is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Every message of the store's comes through the wire as it was sent, from its
        /// sender.
        #[test]
        fn a_message_comes_through_the_wire_as_it_was_sent(from in any::<u64>(), message in message()) {
            prop_assert_eq!(through(from, message.clone()), (from, message));
        }

        /// A data plane's request comes up its stream as it was asked, but for whom it names:
        /// the stream's client, on its first hop.
        #[test]
        fn a_data_planes_request_comes_up_its_stream_as_the_streams_client(
            message in mode_message(),
            client in any::<u64>(),
        ) {
            let Some(asked) = asking(message.clone()) else {
                let asked = matches!(message, SyncMessage::Request { .. } | SyncMessage::Gather { .. });
                prop_assert!(!asked);
                return Ok(());
            };
            let decoded = Asking::decode(asked.encode_to_vec().as_slice()).unwrap();
            let mut expected = message;
            if let SyncMessage::Request { client: asker, hops, via, .. } = &mut expected {
                *asker = client;
                *hops = 0;
                *via = None;
            }
            prop_assert_eq!(asked_from(decoded, client).unwrap(), expected);
        }

        /// What a pod tells a data plane comes down its stream as it was told: an answer, or a
        /// table.
        #[test]
        fn what_a_pod_tells_comes_down_the_stream_as_it_was_told(
            message in prop_oneof![
                mode_message(),
                table().prop_map(|table| SyncMessage::Store(Message::TableReply { table })),
            ],
        ) {
            let Some(told) = telling(message.clone()) else {
                let tells = matches!(
                    message,
                    SyncMessage::Reply { .. } | SyncMessage::Store(Message::TableReply { .. })
                );
                prop_assert!(!tells);
                return Ok(());
            };
            let decoded = Telling::decode(told.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(told_from(decoded).unwrap(), message);
        }

        /// A request to gather comes up a data plane's stream as the stream's client asked it.
        #[test]
        fn a_request_to_gather_comes_up_as_the_streams_client(
            id in any::<u64>(),
            keys in prop::collection::vec(any::<u64>(), 0..5),
            client in any::<u64>(),
        ) {
            let gather = SyncMessage::Gather { client: 0, id, keys: keys.clone() };
            let asked = asking(gather).unwrap();
            prop_assert_eq!(
                asked_from(asked, client).unwrap(),
                SyncMessage::Gather { client, id, keys }
            );
        }

        /// Every message the synchronous mode's pods send each other comes through the wire as
        /// it was sent, from its sender.
        #[test]
        fn a_modes_message_comes_through_the_wire_as_it_was_sent(
            from in any::<u64>(),
            message in mode_message(),
        ) {
            prop_assert_eq!(mode_through(from, message.clone()), (from, message));
        }

        /// Any bytes, as the mode's message, are refused or taken without a panic; and what is
        /// taken comes back the same through an encoding of its own.
        #[test]
        fn any_bytes_are_refused_or_taken_by_the_mode_without_a_panic(
            bytes in prop::collection::vec(any::<u8>(), 0..300),
        ) {
            if let Ok(sent) = ToPeer::<Synchronous>::decode(bytes.as_slice())
                && let Ok((from, message)) = mode_from_peer(sent)
            {
                prop_assert_eq!(mode_through(from, message.clone()), (from, message));
            }
        }

        /// Copies go on the copy stream, everything else — their answers too — on the other.
        #[test]
        fn copies_alone_go_on_the_copy_stream(message in message()) {
            let copy = matches!(message, Message::Replicate { .. });
            prop_assert_eq!(matches!(wire(1, message), Outgoing::Copy(_)), copy);
        }

        /// Any bytes, as either stream's message, are refused or taken without a panic; and
        /// what is taken comes back the same through an encoding of its own.
        #[test]
        fn any_bytes_are_refused_or_taken_without_a_panic(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
            if let Ok(sent) = ToPeer::<Synchronous>::decode(bytes.as_slice())
                && let Ok((from, message)) = from_peer::<SyncValue>(sent)
            {
                prop_assert_eq!(through(from, message.clone()), (from, message));
            }
            if let Ok(sent) = ToCopy::<Synchronous>::decode(bytes.as_slice())
                && let Ok((from, message)) = from_copy::<SyncValue>(sent)
            {
                prop_assert_eq!(through(from, message.clone()), (from, message));
            }
        }

        /// Any message, cut into pieces of any size, is put together again as it was, each
        /// piece no longer than the size and only the last saying no more follow.
        #[test]
        fn a_message_cut_into_pieces_is_put_together_as_it_was(
            bytes in prop::collection::vec(any::<u8>(), 0..2_000),
            size in 1..600_usize,
        ) {
            let cut = pieces(Bytes::from(bytes.clone()), size);
            prop_assert!(cut.iter().all(|piece| piece.part.len() <= size));
            prop_assert!(cut.iter().rev().skip(1).all(|piece| piece.more));
            prop_assert!(cut.last().is_some_and(|piece| !piece.more));
            let mut joining = Joining::new(bytes.len());
            let mut whole = None;
            for piece in &cut {
                prop_assert!(whole.is_none(), "a message ended before its last piece");
                whole = joining.take(piece).unwrap();
            }
            prop_assert_eq!(whole, Some(bytes));
        }
    }

    /// A message with a shard numbered past 16 bits, in any of its places, is refused.
    #[test]
    fn a_shard_past_16_bits_is_refused() {
        let past = u32::from(ShardId::MAX) + 1;
        let lacking = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Lacking(Lacking {
                shard: past,
                term: 1,
            })),
        };
        assert_eq!(from_peer::<SyncValue>(lacking), Err(Invalid::Shard));
        let moved = Table {
            moves: vec![Move {
                shard: past,
                to: 2,
                from: Some(Stamp::default()),
            }],
            ..Table::default()
        };
        let publish = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Publish(moved)),
        };
        assert_eq!(from_peer::<SyncValue>(publish), Err(Invalid::Shard));
        let copy = ToCopy::<Synchronous> {
            from: 1,
            message: Some(Copying::Replicate(Replicate {
                shard: past,
                ..Replicate::default()
            })),
        };
        assert_eq!(from_copy::<SyncValue>(copy), Err(Invalid::Shard));
        // The largest that fits is taken.
        let fits = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Lacking(Lacking {
                shard: past - 1,
                term: 1,
            })),
        };
        assert_eq!(
            from_peer::<SyncValue>(fits),
            Ok((
                1,
                Message::Lacking {
                    shard: ShardId::MAX,
                    term: 1
                }
            ))
        );
    }

    /// A message saying nothing, a table's number missing, a key without its value, a value
    /// of no shape: each refused.
    #[test]
    fn a_message_missing_a_part_it_needs_is_refused() {
        let nothing = ToPeer::<Synchronous> {
            from: 1,
            message: None,
        };
        assert_eq!(from_peer::<SyncValue>(nothing), Err(Invalid::Missing));
        let no_copy = ToCopy::<Synchronous> {
            from: 1,
            message: None,
        };
        assert_eq!(from_copy::<SyncValue>(no_copy), Err(Invalid::Missing));
        let unnumbered = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Heartbeat(Heartbeat::default())),
        };
        assert_eq!(from_peer::<SyncValue>(unnumbered), Err(Invalid::Missing));
        let no_changes = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::PublishChanges(PublishChanges {
                base: Some(Stamp::default()),
                changes: None,
            })),
        };
        assert_eq!(from_peer::<SyncValue>(no_changes), Err(Invalid::Missing));
        let handoff = |value| ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Handoff(Handoff {
                shard: 0,
                term: 1,
                entries: vec![Entry {
                    key: 7,
                    value,
                    forget_at: 0,
                }],
                dropped: Vec::new(),
                part: 0,
                more: false,
            })),
        };
        assert_eq!(from_peer::<SyncValue>(handoff(None)), Err(Invalid::Missing));
        assert_eq!(
            from_peer::<SyncValue>(handoff(Some(Synchronous { shape: None }))),
            Err(Invalid::Missing)
        );
    }

    /// A rolling window with a slot given twice is refused, rather than one of the two kept.
    #[test]
    fn a_slot_given_twice_is_refused() {
        let slot = |counted| Slot {
            slot: 4,
            counted,
            given: 0,
        };
        let twice = Synchronous {
            shape: Some(Shape::Slots(Slots {
                slots: vec![slot(1), slot(2)],
            })),
        };
        assert_eq!(SyncValue::from_wire(twice), Err(Invalid::SlotTwice));
    }

    /// A member named twice, in a table, what changed of one or a naming, is refused, rather
    /// than one of its addresses kept.
    #[test]
    fn a_member_named_twice_is_refused() {
        let twice = || {
            vec![
                Member {
                    id: 3,
                    address: "pod-3:7000".to_owned(),
                },
                Member {
                    id: 3,
                    address: "elsewhere:7000".to_owned(),
                },
            ]
        };
        let sent = |message| ToPeer::<Synchronous> {
            from: 1,
            message: Some(message),
        };
        let table = Table {
            members: twice(),
            ..Table::default()
        };
        let changes = PublishChanges {
            base: Some(Stamp::default()),
            changes: Some(Changes {
                members: twice(),
                ..Changes::default()
            }),
        };
        let form = Form {
            generation: 1,
            voters: twice(),
            latest: Some(Stamp::default()),
        };
        for message in [
            Control::Publish(table),
            Control::PublishChanges(changes),
            Control::Form(form),
        ] {
            assert_eq!(
                from_peer::<SyncValue>(sent(message)),
                Err(Invalid::MemberTwice)
            );
        }
    }

    /// A message past what a receiver takes is refused once it passes, what was held
    /// dropped; one at exactly the bound is taken.
    #[test]
    fn a_message_past_the_bound_is_refused() {
        let cut = pieces(Bytes::from(vec![7; 10]), 4);
        let mut joining = Joining::new(9);
        assert_eq!(joining.take(&cut[0]), Ok(None));
        assert_eq!(joining.take(&cut[1]), Ok(None));
        assert_eq!(joining.take(&cut[2]), Err(TooLong { most: 9 }));
        let mut exact = Joining::new(10);
        let whole: Vec<_> = cut.iter().map(|piece| exact.take(piece)).collect();
        assert_eq!(whole, [Ok(None), Ok(None), Ok(Some(vec![7; 10]))]);
    }

    /// A shard's whole copy too long for one gRPC message goes in pieces each within one,
    /// and comes through as it was sent.
    #[test]
    fn a_copy_too_long_for_one_message_goes_in_pieces() {
        let slots = (0..1_000)
            .map(|slot| (slot, (u64::MAX, u64::MAX)))
            .collect();
        let kept = Kept {
            value: SyncValue::Slots(slots),
            forget_at: u64::MAX,
        };
        let copy = Message::Replicate {
            shard: 3,
            term: 1,
            assignment: 1,
            after: None,
            version: 1,
            under: (1, 1),
            entries: (0..300).map(|key| (key, kept.clone())).collect(),
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let Outgoing::Copy(sent) = wire(2, copy.clone()) else {
            panic!("a copy goes on the copy stream");
        };
        let encoded = Bytes::from(sent.encode_to_vec());
        assert!(
            encoded.len() > crate::grpc::MAX_MESSAGE,
            "{}",
            encoded.len()
        );
        let cut = pieces(encoded, PIECE);
        assert!(
            cut.iter()
                .all(|piece| piece.encoded_len() <= crate::grpc::MAX_MESSAGE)
        );
        let mut joining = Joining::new(usize::MAX);
        let whole = cut
            .iter()
            .find_map(|piece| joining.take(piece).unwrap())
            .unwrap();
        let decoded = ToCopy::decode(whole.as_slice()).unwrap();
        assert_eq!(from_copy(decoded), Ok((2, copy)));
    }

    /// A part as large as the store cuts one, of the values costliest to encode — every
    /// number at its longest — or of keys dropped alone, is one message a receiver takes: the
    /// service fixes the store's part size, so this holds for every config.
    #[test]
    fn a_part_of_the_most_a_part_carries_fits_what_a_receiver_takes() {
        let most = edgerush_cluster::Config::defaults().part_bytes;
        let slots = (0..1_000)
            .map(|slot| (u64::MAX - slot, (u64::MAX, u64::MAX)))
            .collect();
        let costliest = [
            SyncValue::Count {
                window: u64::MAX,
                counted: u64::MAX,
                given: u64::MAX,
            },
            SyncValue::Bucket {
                due: u64::MAX,
                given: u64::MAX,
            },
            SyncValue::Slots(slots),
        ];
        let part = |entries: Vec<(u64, Kept<SyncValue>)>, dropped: Vec<u64>| {
            let Outgoing::Copy(sent) = wire(
                u64::MAX,
                Message::Replicate {
                    shard: u16::MAX,
                    term: u64::MAX,
                    assignment: u64::MAX,
                    after: Some(u64::MAX),
                    version: u64::MAX,
                    under: (u64::MAX, u64::MAX),
                    entries,
                    dropped,
                    part: u32::MAX,
                    more: true,
                },
            ) else {
                panic!("a copy goes on the copy stream");
            };
            sent.encoded_len()
        };
        for value in costliest {
            let kept = Kept {
                value,
                forget_at: u64::MAX,
            };
            let count = most / kept.bytes();
            let entries = (0..count)
                .map(|key| (u64::MAX - key, kept.clone()))
                .collect();
            let encoded = part(entries, Vec::new());
            assert!(encoded <= crate::grpc::MAX_MESSAGE, "{encoded} bytes");
        }
        let dropped = (0..most / 8).map(|key| u64::MAX - key).collect();
        let encoded = part(Vec::new(), dropped);
        assert!(encoded <= crate::grpc::MAX_MESSAGE, "{encoded} bytes");
    }
}
