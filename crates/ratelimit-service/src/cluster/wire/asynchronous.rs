//! The asynchronous mode's messages on the wire (`edgerush_cluster_asynchronous`): its value
//! under each key, a data plane's batch and the owner's answers, the entries a pod forwards to
//! their owner and the answers it relays back, and what a data plane's stream carries.
//!
//! An entry's contract and report are the service's today ([`crate::wire`]): the same
//! messages, refused alike; its key is the store's, 64 bits.
//!
//! A data plane sends its batches up its stream ([`Batch`]), and is told down it the answers
//! — the pod's own, and an owner's it relays, with the owner — and the pod's table as it
//! commits one ([`Answering`]).

use super::{
    Control, Invalid, Outgoing, Stamp, Table, ToCopy, ToPeer, Value, from_copy, from_peer, stamp,
    stamp_from, table_from, table_to_wire, wire,
};
use crate::grpc::MAX_MESSAGE;
use crate::wire::{Contract, Report, contract, narrow, report, report_to_wire};
use edgerush_cluster::{Message, NodeId};
use edgerush_cluster_asynchronous::{
    Answer as AsyncAnswer, Answered as AsyncAnswered, Answers as AsyncAnswers, Batch as AsyncBatch,
    Entry as AsyncEntry, Message as AsyncMessage, Parts, PlaneParts, Unserved as AsyncUnserved,
    Value as AsyncValue, Why,
};
use edgerush_ratelimit::reference::BucketState;

/// One batch waiting behind one missing, in a data plane's record.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Waiting {
    // Tag 4, a fixed window's number, is retired with the asynchronous mode's windows (#389): never
    // reuse it.
    /// The batch's number.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    /// Its hits.
    #[prost(uint64, tag = "2")]
    pub count: u64,
    /// When they are charged as of, in nanoseconds of the wall clock.
    #[prost(uint64, tag = "3")]
    pub at: u64,
}

/// What an owner knows of one data plane admitting a key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Record {
    /// The data plane.
    #[prost(uint64, tag = "1")]
    pub plane: u64,
    /// The highest of its batches applied, every one before it too.
    #[prost(uint64, tag = "2")]
    pub applied: u64,
    /// When it last admitted the key, in nanoseconds of the wall clock, if it has.
    #[prost(uint64, optional, tag = "3")]
    pub admitted: Option<u64>,
    /// Batches that came ahead of one still missing.
    #[prost(message, repeated, tag = "4")]
    pub waiting: Vec<Waiting>,
}

/// The asynchronous mode's value under one key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Asynchronous {
    // Tags 7 to 9, a fixed window and its count, are retired with the asynchronous mode's windows
    // (#389): never reuse them.
    /// The contract it is kept under; none for a key never made.
    #[prost(message, optional, tag = "1")]
    pub contract: Option<Contract>,
    /// Its bucket's arrival time, its high 64 bits; 0 with the low for none.
    #[prost(uint64, tag = "2")]
    pub arrival_high: u64,
    /// Its low 64 bits.
    #[prost(uint64, tag = "3")]
    pub arrival_low: u64,
    /// When its bucket was last charged, in nanoseconds of the wall clock.
    #[prost(uint64, tag = "4")]
    pub last: u64,
    /// Hits applied, each once.
    #[prost(uint64, tag = "5")]
    pub counted: u64,
    /// What it knows of each data plane admitting the key.
    #[prost(message, repeated, tag = "6")]
    pub planes: Vec<Record>,
}

impl Value for AsyncValue {
    type Wire = Asynchronous;

    fn to_wire(&self) -> Asynchronous {
        let Parts {
            contract,
            bucket,
            counted,
            planes,
        } = self.parts();
        let arrival = bucket.arrival();
        Asynchronous {
            contract: contract.map(Contract::from),
            // The arrival time's two halves, each 64 bits.
            arrival_high: u64::try_from(arrival >> 64).unwrap_or(u64::MAX),
            arrival_low: u64::try_from(arrival & u128::from(u64::MAX)).unwrap_or(u64::MAX),
            last: bucket.last(),
            counted,
            planes: planes
                .into_iter()
                .map(|plane| Record {
                    plane: plane.plane,
                    applied: plane.applied,
                    admitted: plane.admitted,
                    waiting: plane
                        .waiting
                        .into_iter()
                        .map(|(sequence, count, at)| Waiting {
                            sequence,
                            count,
                            at,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    fn from_wire(wire: Asynchronous) -> Result<Self, Invalid> {
        let contract = wire
            .contract
            .map(|held| contract(Some(held)))
            .transpose()
            .map_err(Invalid::Entry)?;
        let arrival = (u128::from(wire.arrival_high) << 64) | u128::from(wire.arrival_low);
        let parts = Parts {
            contract,
            bucket: BucketState::from_parts(arrival, wire.last),
            counted: wire.counted,
            planes: wire
                .planes
                .into_iter()
                .map(|record| PlaneParts {
                    plane: record.plane,
                    applied: record.applied,
                    admitted: record.admitted,
                    waiting: record
                        .waiting
                        .into_iter()
                        .map(|waiting| (waiting.sequence, waiting.count, waiting.at))
                        .collect(),
                })
                .collect(),
        };
        Self::from_parts(parts).ok_or(Invalid::Value)
    }
}

/// One key in a batch.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Entry {
    // Tag 6, a fixed window, is retired with the asynchronous mode's windows (#389): never reuse
    // it.
    /// The key, as the data plane hashed it.
    #[prost(uint64, tag = "1")]
    pub key: u64,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
    /// What about it.
    #[prost(oneof = "Report", tags = "3, 5")]
    pub report: Option<Report>,
}

/// A data plane's batch: all it has for the keys its table names one pod the owner of.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Batch {
    /// The data plane, in one life of it.
    #[prost(uint64, tag = "1")]
    pub from: u64,
    /// The table the data plane holds.
    #[prost(message, optional, tag = "2")]
    pub stamp: Option<Stamp>,
    /// When the data plane made it, on its own clock, in nanoseconds: said back with the
    /// answers.
    #[prost(uint64, tag = "3")]
    pub made_ns: u64,
    /// One entry a key, or a few.
    #[prost(message, repeated, tag = "4")]
    pub entries: Vec<Entry>,
}

/// The owner's answer about one key.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Answer {
    // Tag 5, a fixed window's standing, is retired with the asynchronous mode's windows (#389):
    // never reuse it.
    /// The key.
    #[prost(uint64, tag = "1")]
    pub key: u64,
    /// How far the owner's bucket is from full, in time multiplied by the limit; past what 64
    /// bits hold, the most they do, as today's answers have it.
    #[prost(uint64, tag = "2")]
    pub behind: u64,
    /// The highest of the data plane's batches applied.
    #[prost(uint64, tag = "3")]
    pub applied: u64,
    /// How many other data planes are admitting the key.
    #[prost(uint64, tag = "4")]
    pub others: u64,
}

/// A key the pod does not serve.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Unserved {
    /// The key.
    #[prost(uint64, tag = "1")]
    pub key: u64,
    /// Whether its table names it the owner while it is not serving; else the table names
    /// another, or none.
    #[prost(bool, tag = "2")]
    pub not_serving: bool,
}

/// A pod's answers to a batch.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Answers {
    /// The pod's life; 0 leaves the answers unnumbered.
    #[prost(uint64, tag = "1")]
    pub life: u64,
    /// Counted up with each answer the pod gives in its life.
    #[prost(uint64, tag = "2")]
    pub number: u64,
    /// The batch's `made_ns`, said back.
    #[prost(uint64, tag = "3")]
    pub made_ns: u64,
    /// One per entry of a key it serves.
    #[prost(message, repeated, tag = "4")]
    pub entries: Vec<Answer>,
    /// One per entry of a key it does not, nothing of it applied.
    #[prost(message, repeated, tag = "5")]
    pub unserved: Vec<Unserved>,
    /// Its table, if later than the batch's.
    #[prost(message, optional, tag = "6")]
    pub table: Option<Table>,
}

/// A batch's entries forwarded by the pod they reached to their keys' owner.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Forwarded {
    /// The data plane, as the forwarding pod numbers its clients.
    #[prost(uint64, tag = "1")]
    pub client: u64,
    /// The pod that forwarded them.
    #[prost(uint64, tag = "2")]
    pub via: u64,
    /// The entries, as a batch of the data plane's.
    #[prost(message, optional, tag = "3")]
    pub batch: Option<Batch>,
}

/// The owner's answers to entries forwarded, relayed back through the pod that forwarded them.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Relayed {
    /// The data plane, as the forwarding pod numbers its clients.
    #[prost(uint64, tag = "1")]
    pub client: u64,
    /// The owner's answers.
    #[prost(message, optional, tag = "2")]
    pub answers: Option<Answers>,
    /// The owner that answered.
    #[prost(uint64, tag = "3")]
    pub owner: u64,
}

/// What a pod tells a data plane on its stream: its answers, an owner's it relays, or the
/// table it has committed.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Answering {
    /// Which.
    #[prost(oneof = "Answered", tags = "1, 2, 3")]
    pub answered: Option<Answered>,
}

/// What a pod tells a data plane.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Answered {
    /// Its table, on the stream's opening and on each commit.
    #[prost(message, tag = "1")]
    Table(Table),
    /// The pod's answers to a batch.
    #[prost(message, tag = "2")]
    Answers(Answers),
    /// An owner's answers to entries the pod forwarded, relayed: the data plane takes them as
    /// that owner's.
    #[prost(message, tag = "3")]
    Relayed(Relayed),
}

fn entry_to_wire(entry: AsyncEntry) -> Entry {
    Entry {
        key: entry.key,
        contract: Some(entry.contract.into()),
        report: Some(report_to_wire(entry.report)),
    }
}

fn entry_from(entry: Entry) -> Result<AsyncEntry, Invalid> {
    Ok(AsyncEntry {
        key: entry.key,
        contract: contract(entry.contract).map_err(Invalid::Entry)?,
        report: report(entry.report).map_err(Invalid::Entry)?,
    })
}

/// A data plane's batch as the wire carries it.
#[must_use]
pub fn batch_to_wire(batch: AsyncBatch) -> Batch {
    Batch {
        from: batch.from,
        stamp: Some(stamp(batch.stamp)),
        made_ns: batch.made,
        entries: batch.entries.into_iter().map(entry_to_wire).collect(),
    }
}

/// Whether `batch` is longer than a stream's message may be ([`MAX_MESSAGE`]): sent, it would
/// end the stream rather than go.
#[must_use]
pub fn too_long(batch: &Batch) -> bool {
    prost::Message::encoded_len(batch) > MAX_MESSAGE
}

/// A data plane's batch from the wire: refused whole if any entry cannot be used.
///
/// # Errors
///
/// A batch the mode cannot use: see [`Invalid`].
pub fn batch_from(batch: Batch) -> Result<AsyncBatch, Invalid> {
    Ok(AsyncBatch {
        from: batch.from,
        stamp: stamp_from(batch.stamp)?,
        made: batch.made_ns,
        entries: batch
            .entries
            .into_iter()
            .map(entry_from)
            .collect::<Result<_, _>>()?,
    })
}

/// A pod's answers as the wire carries them.
#[must_use]
pub fn answers_to_wire(answers: AsyncAnswers) -> Answers {
    Answers {
        life: answers.life,
        number: answers.number,
        made_ns: answers.made,
        entries: answers
            .entries
            .into_iter()
            .map(|answer| Answer {
                key: answer.key,
                behind: narrow(answer.answered.behind),
                applied: answer.answered.applied,
                others: answer.answered.others,
            })
            .collect(),
        unserved: answers
            .unserved
            .into_iter()
            .map(|unserved| Unserved {
                key: unserved.key,
                not_serving: unserved.why == Why::NotServing,
            })
            .collect(),
        table: answers.table.map(table_to_wire),
    }
}

/// A pod's answers from the wire.
///
/// # Errors
///
/// Answers whose table the store cannot use: see [`Invalid`].
pub fn answers_from(answers: Answers) -> Result<AsyncAnswers, Invalid> {
    Ok(AsyncAnswers {
        life: answers.life,
        number: answers.number,
        made: answers.made_ns,
        entries: answers
            .entries
            .into_iter()
            .map(|answer| AsyncAnswer {
                key: answer.key,
                answered: AsyncAnswered {
                    behind: u128::from(answer.behind),
                    applied: answer.applied,
                    others: answer.others,
                },
            })
            .collect(),
        unserved: answers
            .unserved
            .into_iter()
            .map(|unserved| AsyncUnserved {
                key: unserved.key,
                why: if unserved.not_serving {
                    Why::NotServing
                } else {
                    Why::Elsewhere
                },
            })
            .collect(),
        table: answers.table.map(table_from).transpose()?,
    })
}

/// The asynchronous mode's `message`, from `from`, as the wire carries it between pods: the
/// store's as [`wire`] has it, the mode's own on the stream apart from copies. None for a
/// batch or its answers, which go between a data plane and a pod alone.
#[must_use]
pub fn mode_wire(from: NodeId, message: AsyncMessage) -> Option<Outgoing<Asynchronous>> {
    let peer = |message| {
        Outgoing::Peer(ToPeer {
            from,
            message: Some(message),
        })
    };
    Some(match message {
        AsyncMessage::Store(message) => wire(from, message),
        AsyncMessage::Forwarded { client, via, batch } => peer(Control::Forwarded(Forwarded {
            client,
            via,
            batch: Some(batch_to_wire(batch)),
        })),
        AsyncMessage::Relayed {
            client,
            owner,
            answers,
        } => peer(Control::Relayed(Relayed {
            client,
            answers: Some(answers_to_wire(answers)),
            owner,
        })),
        AsyncMessage::Batch(_) | AsyncMessage::Answers(_) => return None,
    })
}

/// The sender and the asynchronous mode's message, from the stream apart from copies.
///
/// # Errors
///
/// A message the mode or its store cannot use: see [`Invalid`].
pub fn mode_from_peer(sent: ToPeer<Asynchronous>) -> Result<(NodeId, AsyncMessage), Invalid> {
    let from = sent.from;
    match sent.message {
        Some(Control::Forwarded(forwarded)) => Ok((
            from,
            AsyncMessage::Forwarded {
                client: forwarded.client,
                via: forwarded.via,
                batch: batch_from(forwarded.batch.ok_or(Invalid::Missing)?)?,
            },
        )),
        Some(Control::Relayed(relayed)) => Ok((
            from,
            AsyncMessage::Relayed {
                client: relayed.client,
                owner: relayed.owner,
                answers: answers_from(relayed.answers.ok_or(Invalid::Missing)?)?,
            },
        )),
        message => from_peer::<AsyncValue>(ToPeer { from, message })
            .map(|(from, message)| (from, AsyncMessage::Store(message))),
    }
}

/// The sender and the asynchronous mode's message, from the copy stream: the store's alone.
///
/// # Errors
///
/// A message the store cannot use: see [`Invalid`].
pub fn mode_from_copy(sent: ToCopy<Asynchronous>) -> Result<(NodeId, AsyncMessage), Invalid> {
    from_copy::<AsyncValue>(sent).map(|(from, message)| (from, AsyncMessage::Store(message)))
}

/// What the mode tells a client, as a data plane's stream carries it down: the pod's answers,
/// an owner's relayed with the owner, or a table; none for anything else.
#[must_use]
pub fn answering(message: AsyncMessage) -> Option<Answering> {
    let answered = match message {
        AsyncMessage::Answers(answers) => Answered::Answers(answers_to_wire(answers)),
        AsyncMessage::Relayed {
            client,
            owner,
            answers,
        } => Answered::Relayed(Relayed {
            client,
            answers: Some(answers_to_wire(answers)),
            owner,
        }),
        AsyncMessage::Store(Message::TableReply { table }) => Answered::Table(table_to_wire(table)),
        _ => return None,
    };
    Some(Answering {
        answered: Some(answered),
    })
}

/// What a pod told a data plane, as the mode's message: its answers, an owner's relayed, or
/// its table.
///
/// # Errors
///
/// A message the mode cannot use: see [`Invalid`].
pub fn answered_from(answering: Answering) -> Result<AsyncMessage, Invalid> {
    Ok(match answering.answered.ok_or(Invalid::Missing)? {
        Answered::Table(table) => AsyncMessage::Store(Message::TableReply {
            table: table_from(table)?,
        }),
        Answered::Answers(answers) => AsyncMessage::Answers(answers_from(answers)?),
        Answered::Relayed(relayed) => AsyncMessage::Relayed {
            client: relayed.client,
            owner: relayed.owner,
            answers: answers_from(relayed.answers.ok_or(Invalid::Missing)?)?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::wire::{Request, Synchronous};
    use edgerush_cluster::table::first;
    use edgerush_ratelimit::service::{
        Contract as ServiceContract, MOST_UNACKNOWLEDGED, Report as ServiceReport,
    };
    use proptest::prelude::*;
    use prost::Message as _;
    use std::num::NonZeroU64;

    fn contracts() -> impl Strategy<Value = ServiceContract> {
        (1u64.., 1u64.., 1u64..).prop_map(|(limit, window, burst)| {
            let n = |n| NonZeroU64::new(n).unwrap();
            ServiceContract {
                limit: n(limit),
                window: n(window),
                burst: n(burst),
            }
        })
    }

    fn reports() -> impl Strategy<Value = ServiceReport> {
        prop_oneof![
            (1u64.., any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(sequence, count, seen, age)| ServiceReport::Hits {
                    sequence,
                    count,
                    seen: seen % sequence,
                    age,
                }
            ),
            Just(ServiceReport::Poll),
        ]
    }

    fn batches() -> impl Strategy<Value = AsyncBatch> {
        let entry =
            (any::<u64>(), contracts(), reports()).prop_map(|(key, contract, report)| AsyncEntry {
                key,
                contract,
                report,
            });
        (
            any::<u64>(),
            any::<(u64, u64)>(),
            any::<u64>(),
            prop::collection::vec(entry, 0..5),
        )
            .prop_map(|(from, stamp, made, entries)| AsyncBatch {
                from,
                stamp,
                made,
                entries,
            })
    }

    /// A table of three pods.
    fn table() -> edgerush_cluster::Table {
        let members = [1, 2, 3].map(|id| (id, format!("pod-{id}:7000"))).into();
        first(&members, 4, 2, 1)
    }

    fn answers() -> impl Strategy<Value = AsyncAnswers> {
        (
            any::<(u64, u64, u64)>(),
            prop::collection::vec(any::<(u64, u64, u64, u64)>(), 0..4),
            prop::collection::vec((any::<u64>(), any::<bool>()), 0..4),
            any::<bool>(),
        )
            .prop_map(
                |((life, number, made), entries, unserved, told)| AsyncAnswers {
                    life,
                    number,
                    made,
                    entries: entries
                        .into_iter()
                        .map(|(key, behind, applied, others)| AsyncAnswer {
                            key,
                            answered: AsyncAnswered {
                                behind: u128::from(behind),
                                applied,
                                others,
                            },
                        })
                        .collect(),
                    unserved: unserved
                        .into_iter()
                        .map(|(key, not_serving)| AsyncUnserved {
                            key,
                            why: if not_serving {
                                Why::NotServing
                            } else {
                                Why::Elsewhere
                            },
                        })
                        .collect(),
                    table: told.then(table),
                },
            )
    }

    /// A value any owner could hold: records of data planes each once, batches waiting each
    /// once and up to as many as a data plane may have out, its arrival time any of 128 bits.
    fn values() -> impl Strategy<Value = AsyncValue> {
        let waiting =
            prop::collection::btree_map(any::<u64>(), any::<(u64, u64)>(), 0..=MOST_UNACKNOWLEDGED);
        let plane = (any::<u64>(), prop::option::of(any::<u64>()), waiting);
        (
            prop::option::of(contracts()),
            any::<u128>(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::btree_map(any::<u64>(), plane, 0..4),
        )
            .prop_map(|(contract, arrival, last, counted, planes)| {
                AsyncValue::from_parts(Parts {
                    contract,
                    bucket: BucketState::from_parts(arrival, last),
                    counted,
                    planes: planes
                        .into_iter()
                        .map(|(plane, (applied, admitted, waiting))| PlaneParts {
                            plane,
                            applied,
                            // Kept one past the time: the latest a record holds is one short of
                            // the most.
                            admitted: admitted.map(|at| at.min(u64::MAX - 1)),
                            waiting: waiting
                                .into_iter()
                                .map(|(sequence, (count, at))| (sequence, count, at))
                                .collect(),
                        })
                        .collect(),
                })
                .unwrap()
            })
    }

    fn mode_through(from: NodeId, message: AsyncMessage) -> (NodeId, AsyncMessage) {
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

    proptest! {
        /// A value comes through the wire as it was sent: its contract, its bucket's whole
        /// arrival time, its count, and every record with its batches waiting.
        #[test]
        fn a_value_comes_through_the_wire_as_it_was_sent(value in values()) {
            let sent = value.to_wire();
            let decoded = Asynchronous::decode(sent.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(AsyncValue::from_wire(decoded), Ok(value));
        }

        /// A data plane's batch comes up its stream as it was sent.
        #[test]
        fn a_batch_comes_up_the_stream_as_it_was_sent(batch in batches()) {
            let sent = batch_to_wire(batch.clone());
            let decoded = Batch::decode(sent.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(batch_from(decoded), Ok(batch));
        }

        /// Entries forwarded and answers relayed come through the wire between pods as they
        /// were sent, from their sender.
        #[test]
        fn forwarded_and_relayed_come_through_the_wire_as_they_were_sent(
            from in any::<u64>(),
            (client, via, owner) in any::<(u64, u64, u64)>(),
            batch in batches(),
            answers in answers(),
        ) {
            let forwarded = AsyncMessage::Forwarded { client, via, batch };
            prop_assert_eq!(mode_through(from, forwarded.clone()), (from, forwarded));
            let relayed = AsyncMessage::Relayed { client, owner, answers };
            prop_assert_eq!(mode_through(from, relayed.clone()), (from, relayed));
        }

        /// What a pod tells a data plane comes down its stream as it was told: its answers,
        /// an owner's relayed with the owner, or a table.
        #[test]
        fn answers_come_down_the_stream_as_they_were_told(
            answers in answers(),
            (client, owner) in any::<(u64, u64)>(),
        ) {
            for told in [
                AsyncMessage::Answers(answers.clone()),
                AsyncMessage::Relayed { client, owner, answers: answers.clone() },
            ] {
                let sent = answering(told.clone()).unwrap();
                let decoded = Answering::decode(sent.encode_to_vec().as_slice()).unwrap();
                prop_assert_eq!(answered_from(decoded), Ok(told));
            }
            let table = AsyncMessage::Store(Message::TableReply { table: table() });
            let sent = answering(table.clone()).unwrap();
            prop_assert_eq!(answered_from(sent), Ok(table));
        }

        /// Any bytes, as the mode's message between pods, are refused or taken without a panic;
        /// and what is taken comes back the same through an encoding of its own.
        #[test]
        fn any_bytes_are_refused_or_taken_by_the_mode_without_a_panic(
            bytes in prop::collection::vec(any::<u8>(), 0..300),
        ) {
            if let Ok(sent) = ToPeer::<Asynchronous>::decode(bytes.as_slice())
                && let Ok((from, message)) = mode_from_peer(sent)
            {
                prop_assert_eq!(mode_through(from, message.clone()), (from, message));
            }
        }
    }

    /// What a pod tells a data plane, missing what it is, is refused: nothing told, or a
    /// relayed answer without its answers.
    #[test]
    fn a_word_down_the_stream_missing_its_answers_is_refused() {
        assert_eq!(answered_from(Answering::default()), Err(Invalid::Missing));
        let relayed = Answering {
            answered: Some(Answered::Relayed(Relayed {
                client: 1,
                answers: None,
                owner: 2,
            })),
        };
        assert_eq!(answered_from(relayed), Err(Invalid::Missing));
    }

    /// A batch and its answers go between a data plane and a pod alone, never between pods.
    #[test]
    fn a_batch_and_its_answers_go_between_no_pods() {
        let batch = AsyncBatch {
            from: 1,
            stamp: (1, 1),
            made: 0,
            entries: Vec::new(),
        };
        assert!(mode_wire(1, AsyncMessage::Batch(batch)).is_none());
        let answers = AsyncAnswers {
            life: 1,
            number: 1,
            made: 0,
            entries: Vec::new(),
            unserved: Vec::new(),
            table: None,
        };
        assert!(mode_wire(1, AsyncMessage::Answers(answers)).is_none());
    }

    /// Each mode refuses the other's messages between pods, as the store refuses both.
    #[test]
    fn each_mode_refuses_the_others_messages() {
        let forwarded = ToPeer::<Synchronous> {
            from: 1,
            message: Some(Control::Forwarded(Forwarded::default())),
        };
        assert_eq!(
            crate::cluster::wire::mode_from_peer(forwarded),
            Err(Invalid::Mode)
        );
        let relayed = ToPeer::<Asynchronous> {
            from: 1,
            message: Some(Control::Relayed(Relayed::default())),
        };
        assert_eq!(from_peer::<AsyncValue>(relayed), Err(Invalid::Mode));
        let request = ToPeer::<Asynchronous> {
            from: 1,
            message: Some(Control::Request(Request::default())),
        };
        assert_eq!(mode_from_peer(request).map(|_| ()), Err(Invalid::Mode));
    }

    /// A value no owner could have held is refused, and so is one under a contract of zero;
    /// an entry without a contract, or reporting hits it says were seen applied, refuses its
    /// whole batch.
    #[test]
    fn values_and_batches_the_mode_cannot_use_are_refused() {
        let record = |plane| Record {
            plane,
            applied: 1,
            admitted: Some(2),
            waiting: Vec::new(),
        };
        let twice = Asynchronous {
            planes: vec![record(4), record(4)],
            ..Asynchronous::default()
        };
        assert_eq!(AsyncValue::from_wire(twice), Err(Invalid::Value));
        let zero = Asynchronous {
            contract: Some(Contract {
                limit: 0,
                window_ns: 1,
                burst: 1,
            }),
            ..Asynchronous::default()
        };
        assert_eq!(
            AsyncValue::from_wire(zero),
            Err(Invalid::Entry(crate::wire::Invalid::ZeroContract))
        );
        let one = NonZeroU64::new(1).unwrap();
        let fine = AsyncBatch {
            from: 1,
            stamp: (1, 1),
            made: 0,
            entries: vec![AsyncEntry {
                key: 3,
                contract: ServiceContract {
                    limit: one,
                    window: one,
                    burst: one,
                },
                report: ServiceReport::Poll,
            }],
        };
        assert!(batch_from(batch_to_wire(fine.clone())).is_ok());
        let mut no_contract = batch_to_wire(fine.clone());
        no_contract.entries[0].contract = None;
        assert_eq!(
            batch_from(no_contract),
            Err(Invalid::Entry(crate::wire::Invalid::NoContract))
        );
        let mut seen = batch_to_wire(fine);
        seen.entries[0].report = Some(Report::Hits(crate::wire::Hits {
            sequence: 2,
            count: 1,
            seen: 2,
            age_ns: 0,
        }));
        assert_eq!(
            batch_from(seen),
            Err(Invalid::Entry(crate::wire::Invalid::ReportNumbers))
        );
    }
}
