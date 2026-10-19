//! The protocol's messages on the wire: protobuf, derived from these structs, and the
//! conversions to and from the protocol's own types (`edgerush_ratelimit::service`).
//!
//! Protobuf is for version skew: a field a reader does not know is skipped, one it misses
//! takes its default. A message the protocol cannot use — a key with no contract, a
//! contract of zero — is refused whole rather than guessed at.

use edgerush_ratelimit::service::{self, membership};
use std::num::NonZeroU64;

/// A limit's contract.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Contract {
    /// Requests a window, on average.
    #[prost(uint64, tag = "1")]
    pub limit: u64,
    /// The window, in nanoseconds.
    #[prost(uint64, tag = "2")]
    pub window_ns: u64,
    /// The whole bucket.
    #[prost(uint64, tag = "3")]
    pub burst: u64,
}

/// Hits a data plane admitted.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Hits {
    /// The batch's number for the key, from 1.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    /// How many hits.
    #[prost(uint64, tag = "2")]
    pub count: u64,
    /// The highest of its batches the data plane has seen applied.
    #[prost(uint64, tag = "3")]
    pub seen: u64,
}

/// Nothing to report; a fresh copy, please.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Poll {}

/// What an entry reports.
#[derive(Clone, Copy, PartialEq, Eq, prost::Oneof)]
pub enum Report {
    /// Hits.
    #[prost(message, tag = "3")]
    Hits(Hits),
    /// A poll.
    #[prost(message, tag = "5")]
    Poll(Poll),
}

/// One key in a batch.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Entry {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
    /// What about it.
    #[prost(oneof = "Report", tags = "3, 5")]
    pub report: Option<Report>,
}

/// A key an ask is about.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AskKey {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
}

/// Requests waiting for a service pod to decide them, each under all its keys or none.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Ask {
    /// The ask's number among the data plane's asks, from 1.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// How many requests wait.
    #[prost(uint64, tag = "2")]
    pub count: u64,
    /// The keys, in their limits' order: at least one, each once.
    #[prost(message, repeated, tag = "3")]
    pub keys: Vec<AskKey>,
}

/// A data plane's batch.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Batch {
    /// The data plane.
    #[prost(uint64, tag = "1")]
    pub from: u64,
    /// One entry per key, or a few.
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<Entry>,
    /// Requests waiting for a decision.
    #[prost(message, repeated, tag = "3")]
    pub asks: Vec<Ask>,
}

/// An ask's decision.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Decided {
    /// Which ask.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// How many were admitted, the first so many.
    #[prost(uint64, tag = "2")]
    pub admitted: u64,
}

/// A service pod's answer about one key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Answer {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// How far the pod's bucket is from full, in time multiplied by the limit. Beyond
    /// what 64 bits hold it is sent as the most they do, which only a bucket owing for
    /// centuries reaches.
    #[prost(uint64, tag = "2")]
    pub behind: u64,
    /// The highest of the receiver's batches applied.
    #[prost(uint64, tag = "3")]
    pub applied: u64,
    /// How many other data planes are admitting the key.
    #[prost(uint64, tag = "4")]
    pub others: u64,
    /// Whether the pod owns the key, rather than answering an ask from its copy.
    #[prost(bool, tag = "6")]
    pub owned: bool,
}

/// Which service pods serve, as they agree.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct View {
    /// Counted up with each change.
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// The pods serving.
    #[prost(uint32, repeated, tag = "2")]
    pub pods: Vec<u32>,
}

/// A service pod's answers to a batch.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Answers {
    /// The service pod.
    #[prost(uint32, tag = "1")]
    pub from: u32,
    /// One per entry it owns, and one per key of each ask it decided.
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<Answer>,
    /// The view it holds.
    #[prost(message, optional, tag = "3")]
    pub view: Option<View>,
    /// To each ask decided, its decision.
    #[prost(message, repeated, tag = "4")]
    pub decided: Vec<Decided>,
}

/// What an owner knows of one data plane.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct PlaneRecord {
    /// The data plane.
    #[prost(uint64, tag = "1")]
    pub plane: u64,
    /// The highest of its batches applied.
    #[prost(uint64, tag = "2")]
    pub applied: u64,
    /// How long ago it last admitted the key, if it has.
    #[prost(uint64, optional, tag = "3")]
    pub admitted_ago: Option<u64>,
    /// Its last ask decided.
    #[prost(uint64, tag = "4")]
    pub last_ask: u64,
    /// How many that ask admitted.
    #[prost(uint64, tag = "5")]
    pub last_ask_admitted: u64,
}

/// An owner's copy of one key.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Replica {
    /// The key.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Its contract.
    #[prost(message, optional, tag = "2")]
    pub contract: Option<Contract>,
    /// The bucket, as [`Answer::behind`].
    #[prost(uint64, tag = "3")]
    pub behind: u64,
    /// Hits the count holds.
    #[prost(uint64, tag = "4")]
    pub counted: u64,
    /// Every data plane it knows of.
    #[prost(message, repeated, tag = "5")]
    pub planes: Vec<PlaneRecord>,
}

/// An owner's copies, to another pod; or keys handed over to a new owner.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Replicas {
    /// The owner, or the pod that was.
    #[prost(uint32, tag = "1")]
    pub from: u32,
    /// One per key.
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<Replica>,
    /// Whether the keys are handed over.
    #[prost(bool, tag = "3")]
    pub handover: bool,
    /// The epoch of the view the sender held.
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
}

/// A pod in one of its lives.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Life {
    /// The pod.
    #[prost(uint32, tag = "1")]
    pub pod: u32,
    /// Its incarnation.
    #[prost(uint64, tag = "2")]
    pub incarnation: u64,
}

/// A view, and the life of each pod it took in.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Agreed {
    /// The view.
    #[prost(message, optional, tag = "1")]
    pub view: Option<View>,
    /// The lives taken in.
    #[prost(message, repeated, tag = "2")]
    pub incarnations: Vec<Life>,
}

/// A pod's heartbeat.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Heartbeat {
    /// The pod.
    #[prost(uint32, tag = "1")]
    pub from: u32,
    /// Its life.
    #[prost(uint64, tag = "2")]
    pub incarnation: u64,
    /// The view it holds.
    #[prost(message, optional, tag = "3")]
    pub agreed: Option<Agreed>,
    /// The pods it hears from.
    #[prost(message, repeated, tag = "4")]
    pub hears: Vec<Life>,
}

/// Nothing.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct Empty {}

/// Why a message on the wire cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    /// An entry or a copy without a contract.
    #[error("a key without a contract")]
    NoContract,
    /// A contract with a limit, window or burst of zero.
    #[error("a contract with a limit, window or burst of zero")]
    ZeroContract,
    /// An entry that reports nothing.
    #[error("an entry that reports nothing")]
    NoReport,
    /// A heartbeat without a view.
    #[error("a heartbeat without a view")]
    NoView,
    /// An ask under no keys.
    #[error("an ask under no keys")]
    AskWithoutKeys,
    /// An ask under one key more than once.
    #[error("an ask under one key more than once")]
    KeyTwice,
}

impl From<service::View> for View {
    fn from(view: service::View) -> Self {
        Self {
            epoch: view.epoch,
            pods: view.pods,
        }
    }
}

impl From<View> for service::View {
    fn from(view: View) -> Self {
        Self {
            epoch: view.epoch,
            pods: view.pods,
        }
    }
}

fn lives(lives: Vec<(u32, u64)>) -> Vec<Life> {
    lives
        .into_iter()
        .map(|(pod, incarnation)| Life { pod, incarnation })
        .collect()
}

fn pairs(lives: Vec<Life>) -> Vec<(u32, u64)> {
    lives
        .into_iter()
        .map(|life| (life.pod, life.incarnation))
        .collect()
}

impl From<membership::Heartbeat> for Heartbeat {
    fn from(beat: membership::Heartbeat) -> Self {
        Self {
            from: beat.from,
            incarnation: beat.incarnation,
            agreed: Some(Agreed {
                view: Some(beat.agreed.view.into()),
                incarnations: lives(beat.agreed.incarnations),
            }),
            hears: lives(beat.hears),
        }
    }
}

impl TryFrom<Heartbeat> for membership::Heartbeat {
    type Error = Invalid;

    fn try_from(beat: Heartbeat) -> Result<Self, Invalid> {
        let agreed = beat.agreed.ok_or(Invalid::NoView)?;
        Ok(Self {
            from: beat.from,
            incarnation: beat.incarnation,
            agreed: membership::Agreed {
                view: agreed.view.ok_or(Invalid::NoView)?.into(),
                incarnations: pairs(agreed.incarnations),
            },
            hears: pairs(beat.hears),
        })
    }
}

impl From<service::Contract> for Contract {
    fn from(contract: service::Contract) -> Self {
        Self {
            limit: contract.limit.get(),
            window_ns: contract.window.get(),
            burst: contract.burst.get(),
        }
    }
}

/// The protocol's contract from the wire's, which must be there and hold no zero.
fn contract(contract: Option<Contract>) -> Result<service::Contract, Invalid> {
    let contract = contract.ok_or(Invalid::NoContract)?;
    let n = |n| NonZeroU64::new(n).ok_or(Invalid::ZeroContract);
    Ok(service::Contract {
        limit: n(contract.limit)?,
        window: n(contract.window_ns)?,
        burst: n(contract.burst)?,
    })
}

/// `behind` in 64 bits, the most they hold beyond.
fn narrow(behind: u128) -> u64 {
    u64::try_from(behind).unwrap_or(u64::MAX)
}

impl From<service::Batch> for Batch {
    fn from(batch: service::Batch) -> Self {
        Self {
            from: batch.from,
            entries: batch
                .entries
                .into_iter()
                .map(|entry| Entry {
                    key: entry.key.into_vec(),
                    contract: Some(entry.contract.into()),
                    report: Some(match entry.report {
                        service::Report::Hits {
                            sequence,
                            count,
                            seen,
                        } => Report::Hits(Hits {
                            sequence,
                            count,
                            seen,
                        }),
                        service::Report::Poll => Report::Poll(Poll {}),
                    }),
                })
                .collect(),
            asks: batch
                .asks
                .into_iter()
                .map(|ask| Ask {
                    id: ask.id,
                    count: ask.count,
                    keys: ask
                        .keys
                        .into_iter()
                        .map(|(key, contract)| AskKey {
                            key: key.into_vec(),
                            contract: Some(contract.into()),
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl TryFrom<Batch> for service::Batch {
    type Error = Invalid;

    fn try_from(batch: Batch) -> Result<Self, Invalid> {
        let entries = batch
            .entries
            .into_iter()
            .map(|entry| {
                let report = match entry.report.ok_or(Invalid::NoReport)? {
                    Report::Hits(hits) => service::Report::Hits {
                        sequence: hits.sequence,
                        count: hits.count,
                        seen: hits.seen,
                    },
                    Report::Poll(_) => service::Report::Poll,
                };
                Ok(service::Entry {
                    key: entry.key.into_boxed_slice(),
                    contract: contract(entry.contract)?,
                    report,
                })
            })
            .collect::<Result<_, Invalid>>()?;
        let asks = batch
            .asks
            .into_iter()
            .map(|ask| {
                let keys: Vec<(Box<[u8]>, service::Contract)> = ask
                    .keys
                    .into_iter()
                    .map(|at| Ok((at.key.into_boxed_slice(), contract(at.contract)?)))
                    .collect::<Result<_, Invalid>>()?;
                if keys.is_empty() {
                    return Err(Invalid::AskWithoutKeys);
                }
                // Sorted rather than compared pair by pair, so that an ask under many keys
                // costs no more than sorting them; most asks are under one.
                if keys.len() > 1 {
                    let mut sorted: Vec<&[u8]> = keys.iter().map(|(key, _)| &**key).collect();
                    sorted.sort_unstable();
                    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
                        return Err(Invalid::KeyTwice);
                    }
                }
                Ok(service::Ask {
                    id: ask.id,
                    count: ask.count,
                    keys,
                })
            })
            .collect::<Result<_, Invalid>>()?;
        Ok(Self {
            from: batch.from,
            entries,
            asks,
        })
    }
}

impl From<service::Answers> for Answers {
    fn from(answers: service::Answers) -> Self {
        Self {
            from: answers.from,
            entries: answers
                .entries
                .into_iter()
                .map(|answer| Answer {
                    key: answer.key.into_vec(),
                    behind: narrow(answer.behind),
                    applied: answer.applied,
                    others: answer.others,
                    owned: answer.owned,
                })
                .collect(),
            view: answers.view.map(View::from),
            decided: answers
                .decided
                .into_iter()
                .map(|(id, admitted)| Decided { id, admitted })
                .collect(),
        }
    }
}

impl From<Answers> for service::Answers {
    fn from(answers: Answers) -> Self {
        Self {
            from: answers.from,
            entries: answers
                .entries
                .into_iter()
                .map(|answer| service::Answer {
                    key: answer.key.into_boxed_slice(),
                    behind: u128::from(answer.behind),
                    applied: answer.applied,
                    others: answer.others,
                    owned: answer.owned,
                })
                .collect(),
            decided: answers
                .decided
                .into_iter()
                .map(|decided| (decided.id, decided.admitted))
                .collect(),
            view: answers.view.map(service::View::from),
        }
    }
}

impl From<service::Replicas> for Replicas {
    fn from(replicas: service::Replicas) -> Self {
        Self {
            from: replicas.from,
            entries: replicas
                .entries
                .into_iter()
                .map(|replica| Replica {
                    key: replica.key.into_vec(),
                    contract: Some(replica.contract.into()),
                    behind: narrow(replica.behind),
                    counted: replica.counted,
                    planes: replica
                        .planes
                        .into_iter()
                        .map(|record| PlaneRecord {
                            plane: record.plane,
                            applied: record.applied,
                            admitted_ago: record.admitted_ago,
                            last_ask: record.last_ask.0,
                            last_ask_admitted: record.last_ask.1,
                        })
                        .collect(),
                })
                .collect(),
            handover: replicas.handover,
            epoch: replicas.epoch,
        }
    }
}

impl TryFrom<Replicas> for service::Replicas {
    type Error = Invalid;

    fn try_from(replicas: Replicas) -> Result<Self, Invalid> {
        let entries = replicas
            .entries
            .into_iter()
            .map(|replica| {
                Ok(service::Replica {
                    key: replica.key.into_boxed_slice(),
                    contract: contract(replica.contract)?,
                    behind: u128::from(replica.behind),
                    counted: replica.counted,
                    planes: replica
                        .planes
                        .into_iter()
                        .map(|record| service::PlaneRecord {
                            plane: record.plane,
                            applied: record.applied,
                            admitted_ago: record.admitted_ago,
                            last_ask: (record.last_ask, record.last_ask_admitted),
                        })
                        .collect(),
                })
            })
            .collect::<Result<_, Invalid>>()?;
        Ok(Self {
            from: replicas.from,
            entries,
            handover: replicas.handover,
            epoch: replicas.epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;

    fn contract() -> impl Strategy<Value = service::Contract> {
        (1..u64::MAX, 1..u64::MAX, 1..u64::MAX).prop_map(|(limit, window, burst)| {
            let n = |n| NonZeroU64::new(n).unwrap();
            service::Contract {
                limit: n(limit),
                window: n(window),
                burst: n(burst),
            }
        })
    }

    fn report() -> impl Strategy<Value = service::Report> {
        prop_oneof![
            (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(sequence, count, seen)| {
                service::Report::Hits {
                    sequence,
                    count,
                    seen,
                }
            }),
            Just(service::Report::Poll),
        ]
    }

    fn key() -> impl Strategy<Value = Box<[u8]>> {
        prop::collection::vec(any::<u8>(), 0..40).prop_map(Vec::into_boxed_slice)
    }

    fn batch() -> impl Strategy<Value = service::Batch> {
        let entry =
            (key(), contract(), report()).prop_map(|(key, contract, report)| service::Entry {
                key,
                contract,
                report,
            });
        let ask = (
            any::<u64>(),
            any::<u64>(),
            prop::collection::btree_map(key(), contract(), 1..4),
        )
            .prop_map(|(id, count, keys)| service::Ask {
                id,
                count,
                keys: keys.into_iter().collect(),
            });
        (
            any::<u64>(),
            prop::collection::vec(entry, 0..8),
            prop::collection::vec(ask, 0..3),
        )
            .prop_map(|(from, entries, asks)| service::Batch {
                from,
                entries,
                asks,
            })
    }

    fn answers() -> impl Strategy<Value = service::Answers> {
        let answer = (
            key(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<bool>(),
        )
            .prop_map(|(key, behind, applied, others, owned)| service::Answer {
                key,
                behind: u128::from(behind),
                applied,
                others,
                owned,
            });
        let view = prop::option::of(
            (any::<u64>(), prop::collection::vec(any::<u32>(), 0..4))
                .prop_map(|(epoch, pods)| service::View { epoch, pods }),
        );
        (
            any::<u32>(),
            prop::collection::vec(answer, 0..8),
            prop::collection::vec(any::<(u64, u64)>(), 0..3),
            view,
        )
            .prop_map(|(from, entries, decided, view)| service::Answers {
                from,
                entries,
                decided,
                view,
            })
    }

    fn replicas() -> impl Strategy<Value = service::Replicas> {
        let record = (
            any::<u64>(),
            any::<u64>(),
            prop::option::of(any::<u64>()),
            any::<(u64, u64)>(),
        )
            .prop_map(
                |(plane, applied, admitted_ago, last_ask)| service::PlaneRecord {
                    plane,
                    applied,
                    admitted_ago,
                    last_ask,
                },
            );
        let replica = (
            key(),
            contract(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(record, 0..4),
        )
            .prop_map(
                |(key, contract, behind, counted, planes)| service::Replica {
                    key,
                    contract,
                    behind: u128::from(behind),
                    counted,
                    planes,
                },
            );
        (
            any::<u32>(),
            prop::collection::vec(replica, 0..4),
            any::<bool>(),
            any::<u64>(),
        )
            .prop_map(|(from, entries, handover, epoch)| service::Replicas {
                from,
                entries,
                handover,
                epoch,
            })
    }

    fn heartbeat() -> impl Strategy<Value = membership::Heartbeat> {
        let lives = || prop::collection::vec(any::<(u32, u64)>(), 0..4);
        (
            any::<u32>(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(any::<u32>(), 0..4),
            lives(),
            lives(),
        )
            .prop_map(|(from, incarnation, epoch, pods, incarnations, hears)| {
                membership::Heartbeat {
                    from,
                    incarnation,
                    agreed: membership::Agreed {
                        view: service::View { epoch, pods },
                        incarnations,
                    },
                    hears,
                }
            })
    }

    proptest! {
        #[test]
        fn a_heartbeat_comes_through_the_wire_as_it_was_sent(beat in heartbeat()) {
            let bytes = Heartbeat::from(beat.clone()).encode_to_vec();
            let decoded = Heartbeat::decode(bytes.as_slice()).unwrap();
            prop_assert_eq!(membership::Heartbeat::try_from(decoded).unwrap(), beat);
        }

        #[test]
        fn a_batch_comes_through_the_wire_as_it_was_sent(batch in batch()) {
            let bytes = Batch::from(batch.clone()).encode_to_vec();
            let decoded = Batch::decode(bytes.as_slice()).unwrap();
            prop_assert_eq!(service::Batch::try_from(decoded).unwrap(), batch);
        }

        #[test]
        fn answers_come_through_the_wire_as_they_were_sent(answers in answers()) {
            let bytes = Answers::from(answers.clone()).encode_to_vec();
            let decoded = Answers::decode(bytes.as_slice()).unwrap();
            prop_assert_eq!(service::Answers::from(decoded), answers);
        }

        #[test]
        fn copies_come_through_the_wire_as_they_were_sent(replicas in replicas()) {
            let bytes = Replicas::from(replicas.clone()).encode_to_vec();
            let decoded = Replicas::decode(bytes.as_slice()).unwrap();
            prop_assert_eq!(service::Replicas::try_from(decoded).unwrap(), replicas);
        }

        /// Whatever bytes arrive, decoding and converting them never panics.
        #[test]
        fn any_bytes_are_refused_or_taken_without_a_panic(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            if let Ok(batch) = Batch::decode(bytes.as_slice()) {
                let _ = service::Batch::try_from(batch);
            }
            if let Ok(replicas) = Replicas::decode(bytes.as_slice()) {
                let _ = service::Replicas::try_from(replicas);
            }
        }
    }

    #[test]
    fn a_key_without_a_contract_or_with_a_zero_in_it_is_refused() {
        let entry = |contract| Entry {
            key: b"k".to_vec(),
            contract,
            report: Some(Report::Poll(Poll {})),
        };
        let batch = |entry| Batch {
            from: 1,
            entries: vec![entry],
            asks: Vec::new(),
        };
        assert_eq!(
            service::Batch::try_from(batch(entry(None))),
            Err(Invalid::NoContract)
        );
        for zero in [
            Contract {
                limit: 0,
                window_ns: 1,
                burst: 1,
            },
            Contract {
                limit: 1,
                window_ns: 0,
                burst: 1,
            },
            Contract {
                limit: 1,
                window_ns: 1,
                burst: 0,
            },
        ] {
            assert_eq!(
                service::Batch::try_from(batch(entry(Some(zero)))),
                Err(Invalid::ZeroContract)
            );
        }
        let mut nothing = entry(Some(Contract {
            limit: 1,
            window_ns: 1,
            burst: 1,
        }));
        nothing.report = None;
        assert_eq!(
            service::Batch::try_from(batch(nothing)),
            Err(Invalid::NoReport)
        );
    }

    /// An ask under no keys, or under one key twice, is refused: the first would admit every
    /// request it is about, the second charge a key twice for each.
    #[test]
    fn an_ask_under_no_keys_or_a_key_twice_is_refused() {
        let contract = Some(Contract {
            limit: 1,
            window_ns: 1,
            burst: 1,
        });
        let at = |key: &[u8]| AskKey {
            key: key.to_vec(),
            contract,
        };
        let ask = |keys| Batch {
            from: 1,
            entries: Vec::new(),
            asks: vec![Ask {
                id: 1,
                count: u64::MAX,
                keys,
            }],
        };
        assert_eq!(
            service::Batch::try_from(ask(Vec::new())),
            Err(Invalid::AskWithoutKeys)
        );
        assert_eq!(
            service::Batch::try_from(ask(vec![at(b"a"), at(b"b"), at(b"a")])),
            Err(Invalid::KeyTwice)
        );
        assert!(service::Batch::try_from(ask(vec![at(b"a"), at(b"b")])).is_ok());
    }

    #[test]
    fn a_bucket_owing_beyond_64_bits_is_sent_as_the_most_they_hold() {
        assert_eq!(narrow(u128::from(u64::MAX) + 5), u64::MAX);
        assert_eq!(narrow(7), 7);
    }
}
