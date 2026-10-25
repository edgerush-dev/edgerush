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
    /// The batch's number for the key, one past the last, and never 0.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    /// How many hits.
    #[prost(uint64, tag = "2")]
    pub count: u64,
    /// The highest of its batches the data plane has seen applied, below `sequence`.
    #[prost(uint64, tag = "3")]
    pub seen: u64,
    /// How many of the hits are requests an ask decided.
    #[prost(uint64, tag = "4")]
    pub asked: u64,
    /// Requests of asks given up that the data plane refused itself.
    #[prost(uint64, tag = "5")]
    pub refused: u64,
    /// How long before the batch was made the earliest of them was admitted, in
    /// nanoseconds; 0, from a data plane that does not say, charges them as they arrive.
    #[prost(uint64, tag = "6")]
    pub age_ns: u64,
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
    /// How long the data plane waits for the answer, in nanoseconds.
    #[prost(uint64, tag = "4")]
    pub timeout_ns: u64,
    /// How long before the batch was made the earliest of the requests arrived, in
    /// nanoseconds; 0, from a data plane that does not say, decides them as the ask arrives.
    #[prost(uint64, tag = "5")]
    pub age_ns: u64,
    /// How long after the earliest the latest of them arrived, in nanoseconds.
    #[prost(uint64, tag = "6")]
    pub span_ns: u64,
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
    /// The oldest of the data plane's asks still in flight, or one past its last.
    #[prost(uint64, tag = "4")]
    pub oldest_ask: u64,
    /// When the data plane made it, on its own clock, in nanoseconds: said back with the
    /// answers.
    #[prost(uint64, tag = "5")]
    pub made_ns: u64,
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
    /// Of the receiver's hits still to reach the pod, how many `behind` holds charged.
    #[prost(uint64, tag = "7")]
    pub held: u64,
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
    /// The pod's life; 0 leaves the answers unnumbered.
    #[prost(uint64, tag = "5")]
    pub life: u64,
    /// Counted up with each answer the pod gives in its life.
    #[prost(uint64, tag = "6")]
    pub number: u64,
    /// The batch's `made_ns`, said back; 0 from a pod that does not.
    #[prost(uint64, tag = "7")]
    pub made_ns: u64,
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
    /// What its asks were admitted that its hits have not yet settled.
    #[prost(uint64, tag = "6")]
    pub held: u64,
    /// How long after the copy was made that lapses, in nanoseconds.
    #[prost(uint64, tag = "7")]
    pub held_for_ns: u64,
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
    /// A report of hits numbered 0, or saying it or a later one was applied already.
    #[error("a report of hits numbered 0, or seen applied already")]
    ReportNumbers,
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
pub(crate) fn contract(contract: Option<Contract>) -> Result<service::Contract, Invalid> {
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
                            asked,
                            refused,
                            seen,
                            age,
                        } => Report::Hits(Hits {
                            sequence,
                            count,
                            seen,
                            asked,
                            refused,
                            age_ns: age,
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
                    timeout_ns: ask.timeout,
                    age_ns: ask.age,
                    span_ns: ask.span,
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
            oldest_ask: batch.oldest_ask,
            made_ns: batch.made,
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
                    // Numbered from 1, and only earlier ones seen applied: a report saying
                    // it or a later one was would have the pod skip every report to come.
                    Report::Hits(hits) if hits.seen >= hits.sequence => {
                        return Err(Invalid::ReportNumbers);
                    }
                    Report::Hits(hits) => service::Report::Hits {
                        sequence: hits.sequence,
                        count: hits.count,
                        asked: hits.asked,
                        refused: hits.refused,
                        seen: hits.seen,
                        age: hits.age_ns,
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
                    age: ask.age_ns,
                    span: ask.span_ns,
                    timeout: ask.timeout_ns,
                    keys,
                })
            })
            .collect::<Result<_, Invalid>>()?;
        Ok(Self {
            from: batch.from,
            entries,
            asks,
            oldest_ask: batch.oldest_ask,
            made: batch.made_ns,
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
                    held: answer.held,
                })
                .collect(),
            view: answers.view.map(View::from),
            decided: answers
                .decided
                .into_iter()
                .map(|(id, admitted)| Decided { id, admitted })
                .collect(),
            life: answers.life,
            number: answers.number,
            made_ns: answers.made,
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
                    held: answer.held,
                })
                .collect(),
            decided: answers
                .decided
                .into_iter()
                .map(|decided| (decided.id, decided.admitted))
                .collect(),
            view: answers.view.map(service::View::from),
            life: answers.life,
            number: answers.number,
            made: answers.made_ns,
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
                            held: record.held,
                            held_for_ns: record.held_for,
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
                            held: record.held,
                            held_for: record.held_for_ns,
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
            (
                1..=u64::MAX,
                any::<u64>(),
                any::<u64>(),
                any::<u64>(),
                any::<u64>(),
                any::<u64>()
            )
                .prop_map(|(sequence, count, asked, refused, seen, age)| {
                    service::Report::Hits {
                        sequence,
                        count,
                        asked,
                        refused,
                        seen: seen % sequence,
                        age,
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
            any::<(u64, u64)>(),
            any::<(u64, u64)>(),
            any::<u64>(),
            prop::collection::btree_map(key(), contract(), 1..4),
        )
            .prop_map(|((id, count), (age, span), timeout, keys)| service::Ask {
                id,
                count,
                age,
                span,
                timeout,
                keys: keys.into_iter().collect(),
            });
        (
            any::<u64>(),
            prop::collection::vec(entry, 0..8),
            prop::collection::vec(ask, 0..3),
            any::<(u64, u64)>(),
        )
            .prop_map(|(from, entries, asks, (oldest_ask, made))| service::Batch {
                from,
                entries,
                asks,
                oldest_ask,
                made,
            })
    }

    fn answers() -> impl Strategy<Value = service::Answers> {
        let answer = (
            key(),
            any::<u64>(),
            any::<u64>(),
            any::<(u64, bool)>(),
            any::<u64>(),
        )
            .prop_map(
                |(key, behind, applied, (others, owned), held)| service::Answer {
                    key,
                    behind: u128::from(behind),
                    applied,
                    others,
                    owned,
                    held,
                },
            );
        let view = prop::option::of(
            (any::<u64>(), prop::collection::vec(any::<u32>(), 0..4))
                .prop_map(|(epoch, pods)| service::View { epoch, pods }),
        );
        (
            any::<u32>(),
            prop::collection::vec(answer, 0..8),
            prop::collection::vec(any::<(u64, u64)>(), 0..3),
            view,
            any::<(u64, u64, u64)>(),
        )
            .prop_map(|(from, entries, decided, view, (life, number, made))| {
                service::Answers {
                    from,
                    entries,
                    decided,
                    view,
                    life,
                    number,
                    made,
                }
            })
    }

    fn replicas() -> impl Strategy<Value = service::Replicas> {
        let record = (
            any::<u64>(),
            any::<u64>(),
            prop::option::of(any::<u64>()),
            any::<(u64, u64)>(),
            any::<(u64, u64)>(),
        )
            .prop_map(
                |(plane, applied, admitted_ago, last_ask, (held, held_for))| service::PlaneRecord {
                    plane,
                    applied,
                    admitted_ago,
                    last_ask,
                    held,
                    held_for,
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
        /// What a batch is cut by holds: it never takes more than [`service::Batch::bytes`]
        /// says, nor does any part of it.
        #[test]
        fn a_batch_takes_no_more_than_its_bound(batch in batch()) {
            prop_assert!(within_bounds(&batch));
        }

        /// As for a batch, for copies.
        #[test]
        fn copies_take_no_more_than_their_bound(replicas in replicas()) {
            prop_assert!(copies_within_bounds(&replicas));
        }

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

    /// Every number at its longest and keys long enough for their lengths to take three
    /// bytes, an ask with a hundred of them: still within the bound.
    #[test]
    fn a_batch_at_its_longest_takes_no_more_than_its_bound() {
        let most = NonZeroU64::MAX;
        let contract = service::Contract {
            limit: most,
            window: most,
            burst: most,
        };
        let key: Box<[u8]> = vec![b'k'; 100_000].into();
        let batch = service::Batch {
            from: u64::MAX,
            entries: vec![service::Entry {
                key,
                contract,
                report: service::Report::Hits {
                    sequence: u64::MAX,
                    count: u64::MAX,
                    asked: u64::MAX,
                    refused: u64::MAX,
                    seen: u64::MAX - 1,
                    age: u64::MAX,
                },
            }],
            asks: vec![service::Ask {
                id: u64::MAX,
                count: u64::MAX,
                age: u64::MAX,
                span: u64::MAX,
                timeout: u64::MAX,
                keys: (0..100_u8)
                    .map(|n| {
                        let mut key = vec![b'k'; 20_000];
                        key[0] = n;
                        (key.into(), contract)
                    })
                    .collect(),
            }],
            oldest_ask: u64::MAX,
            made: u64::MAX,
        };
        assert!(within_bounds(&batch));
    }

    /// Whether `batch` takes no more than its bound, and each entry and ask no more than
    /// its own: what each adds to the batch without it, its fields' framing included.
    fn within_bounds(batch: &service::Batch) -> bool {
        let encoded = |batch: &service::Batch| Batch::from(batch.clone()).encoded_len();
        let empty = service::Batch {
            entries: Vec::new(),
            asks: Vec::new(),
            ..batch.clone()
        };
        let alone = encoded(&empty);
        let entries = batch.entries.iter().all(|entry| {
            let with = service::Batch {
                entries: vec![entry.clone()],
                ..empty.clone()
            };
            encoded(&with) - alone <= entry.bytes()
        });
        let asks = batch.asks.iter().all(|ask| {
            let with = service::Batch {
                asks: vec![ask.clone()],
                ..empty.clone()
            };
            encoded(&with) - alone <= ask.bytes()
        });
        entries && asks && alone <= empty.bytes() && encoded(batch) <= batch.bytes()
    }

    /// Every number at its longest, keys long enough for their lengths to take three bytes,
    /// one copy with a hundred records and one with none: still within the bound.
    #[test]
    fn copies_at_their_longest_take_no_more_than_their_bound() {
        let most = NonZeroU64::MAX;
        let record = service::PlaneRecord {
            plane: u64::MAX,
            applied: u64::MAX,
            admitted_ago: Some(u64::MAX),
            last_ask: (u64::MAX, u64::MAX),
            held: u64::MAX,
            held_for: u64::MAX,
        };
        let replica = |planes| service::Replica {
            key: vec![b'k'; 100_000].into(),
            contract: service::Contract {
                limit: most,
                window: most,
                burst: most,
            },
            behind: u128::from(u64::MAX),
            counted: u64::MAX,
            planes,
        };
        let replicas = service::Replicas {
            from: u32::MAX,
            entries: vec![replica(vec![record; 100]), replica(Vec::new())],
            handover: true,
            epoch: u64::MAX,
        };
        assert!(copies_within_bounds(&replicas));
    }

    /// Whether `replicas` take no more than their bound, and each copy no more than its
    /// own, as [`within_bounds`] for a batch.
    fn copies_within_bounds(replicas: &service::Replicas) -> bool {
        let encoded = |replicas: &service::Replicas| Replicas::from(replicas.clone()).encoded_len();
        let empty = service::Replicas {
            entries: Vec::new(),
            ..replicas.clone()
        };
        let alone = encoded(&empty);
        let each = replicas.entries.iter().all(|replica| {
            let with = service::Replicas {
                entries: vec![replica.clone()],
                ..empty.clone()
            };
            encoded(&with) - alone <= replica.bytes()
        });
        each && alone <= empty.bytes() && encoded(replicas) <= replicas.bytes()
    }

    /// The answers to a full piece are no bigger than it: a piece of asks, each about one
    /// short key, as answers say most for what a batch holds; and a piece of hits.
    #[test]
    fn the_answers_to_a_piece_are_no_bigger_than_it() {
        let one = NonZeroU64::MIN;
        let contract = service::Contract {
            limit: one,
            window: NonZeroU64::new(1_000_000_000).unwrap(),
            burst: one,
        };
        let key = |n: u64| -> Box<[u8]> { n.to_be_bytes().into() };
        let full =
            |batch: &service::Batch, more: usize| batch.bytes() + more <= service::PIECE_BYTES;
        let mut asks = service::Batch {
            from: 7,
            entries: Vec::new(),
            asks: Vec::new(),
            oldest_ask: 1,
            made: 0,
        };
        let mut hits = asks.clone();
        let mut n = 0;
        loop {
            let ask = service::Ask {
                id: n + 1,
                count: 3,
                age: 0,
                span: 0,
                timeout: 300_000_000,
                keys: vec![(key(n), contract)],
            };
            if !full(&asks, ask.bytes()) {
                break;
            }
            asks.asks.push(ask);
            n += 1;
        }
        loop {
            let entry = service::Entry {
                key: key(n),
                contract,
                report: service::Report::Hits {
                    sequence: 1,
                    count: 1,
                    asked: 0,
                    refused: 0,
                    seen: 0,
                    age: 0,
                },
            };
            if !full(&hits, entry.bytes()) {
                break;
            }
            hits.entries.push(entry);
            n += 1;
        }
        let view = service::View {
            epoch: 1,
            pods: vec![0],
        };
        let mut pod: service::ServicePod = service::ServicePod::new(0, 10_000_000, view);
        pod.set_life(u64::MAX);
        for piece in [asks, hits] {
            let bound = piece.bytes();
            let answers = Answers::from(pod.on_batch(piece, 1_000_000_000));
            assert!(!answers.entries.is_empty());
            assert!(
                answers.encoded_len() <= bound,
                "{} {bound}",
                answers.encoded_len()
            );
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
            oldest_ask: 0,
            made_ns: 0,
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
    /// A report of hits numbered 0, or saying it or a later one was applied already, is
    /// refused: a data plane numbers its reports from 1 and has seen only earlier ones
    /// applied, and a pod taking such a report would skip past every report still to come.
    #[test]
    fn a_report_numbered_zero_or_seen_applied_is_refused() {
        let hits = |sequence, seen| Batch {
            from: 1,
            oldest_ask: 0,
            made_ns: 0,
            entries: vec![Entry {
                key: b"k".to_vec(),
                contract: Some(Contract {
                    limit: 1,
                    window_ns: 1,
                    burst: 1,
                }),
                report: Some(Report::Hits(Hits {
                    sequence,
                    count: 1,
                    seen,
                    asked: 0,
                    refused: 0,
                    age_ns: 0,
                })),
            }],
            asks: Vec::new(),
        };
        for (sequence, seen) in [(0, 0), (1, 1), (5, 5), (5, 6), (5, u64::MAX)] {
            assert_eq!(
                service::Batch::try_from(hits(sequence, seen)),
                Err(Invalid::ReportNumbers),
                "{sequence} seeing {seen}"
            );
        }
        for (sequence, seen) in [(1, 0), (5, 4), (5, 0), (u64::MAX, u64::MAX - 1)] {
            assert!(
                service::Batch::try_from(hits(sequence, seen)).is_ok(),
                "{sequence} seeing {seen}"
            );
        }
    }

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
            oldest_ask: 0,
            made_ns: 0,
            entries: Vec::new(),
            asks: vec![Ask {
                id: 1,
                count: u64::MAX,
                keys,
                timeout_ns: 0,
                age_ns: 0,
                span_ns: 0,
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
