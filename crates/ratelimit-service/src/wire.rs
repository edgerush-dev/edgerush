//! What an entry of the asynchronous mode's carries on the wire ([`crate::cluster::wire`]): its
//! contract and its report, as protobuf derived from these structs, and their conversions to
//! and from the protocol's own types (`edgerush_ratelimit::service`).
//!
//! Protobuf is for version skew: a field a reader does not know is skipped, one it misses
//! takes its default. What the protocol cannot use — a key with no contract, a contract of
//! zero — is refused whole rather than guessed at.

use edgerush_ratelimit::service;
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
    // Tag 7, the window hits were admitted in, is retired with the asynchronous mode's windows
    // (#389): never reuse it.
    /// The batch's number for the key, one past the last, and never 0.
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    /// How many hits.
    #[prost(uint64, tag = "2")]
    pub count: u64,
    /// The highest of its batches the data plane has seen applied, below `sequence`.
    #[prost(uint64, tag = "3")]
    pub seen: u64,
    // Tags 4 and 5 are not to be used again.
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
    /// A request to decide under no keys.
    #[error("a request under no keys")]
    NoKeys,
    /// A request to decide under one key more than once.
    #[error("a request under one key more than once")]
    KeyTwice,
    /// A report of hits numbered 0, or saying it or a later one was applied already.
    #[error("a report of hits numbered 0, or seen applied already")]
    ReportNumbers,
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

/// The protocol's report as the wire carries it.
pub(crate) const fn report_to_wire(report: service::Report) -> Report {
    match report {
        service::Report::Hits {
            sequence,
            count,
            seen,
            age,
        } => Report::Hits(Hits {
            sequence,
            count,
            seen,
            age_ns: age,
        }),
        service::Report::Poll => Report::Poll(Poll {}),
    }
}

/// The protocol's report from the wire's, which must be there: hits numbered from 1, and only
/// earlier ones seen applied, as a report saying it or a later one was would have the owner
/// skip every report to come.
pub(crate) fn report(report: Option<Report>) -> Result<service::Report, Invalid> {
    Ok(match report.ok_or(Invalid::NoReport)? {
        Report::Hits(hits) if hits.seen >= hits.sequence => return Err(Invalid::ReportNumbers),
        Report::Hits(hits) => service::Report::Hits {
            sequence: hits.sequence,
            count: hits.count,
            seen: hits.seen,
            age: hits.age_ns,
        },
        Report::Poll(_) => service::Report::Poll,
    })
}

/// `behind` in 64 bits, the most they hold beyond.
pub(crate) fn narrow(behind: u128) -> u64 {
    u64::try_from(behind).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_without_a_contract_or_with_a_zero_in_it_is_refused() {
        assert_eq!(contract(None), Err(Invalid::NoContract));
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
            assert_eq!(contract(Some(zero)), Err(Invalid::ZeroContract));
        }
        let one = Contract {
            limit: 1,
            window_ns: 1,
            burst: 1,
        };
        assert_eq!(contract(Some(one)).map(Contract::from), Ok(one));
        assert_eq!(report(None), Err(Invalid::NoReport));
    }

    /// A report of hits numbered 0, or saying it or a later one was applied already, is
    /// refused: a data plane numbers its reports from 1 and has seen only earlier ones
    /// applied, and a pod taking such a report would skip past every report still to come.
    #[test]
    fn a_report_numbered_zero_or_seen_applied_is_refused() {
        let hits = |sequence, seen| {
            Some(Report::Hits(Hits {
                sequence,
                count: 1,
                seen,
                age_ns: 0,
            }))
        };
        for (sequence, seen) in [(0, 0), (1, 1), (5, 5), (5, 6), (5, u64::MAX)] {
            assert_eq!(
                report(hits(sequence, seen)),
                Err(Invalid::ReportNumbers),
                "{sequence} seeing {seen}"
            );
        }
        for (sequence, seen) in [(1, 0), (5, 4), (5, 0), (u64::MAX, u64::MAX - 1)] {
            let taken = report(hits(sequence, seen)).unwrap();
            assert_eq!(
                report_to_wire(taken),
                hits(sequence, seen).unwrap(),
                "{sequence} seeing {seen}"
            );
        }
    }

    #[test]
    fn a_bucket_owing_beyond_64_bits_is_sent_as_the_most_they_hold() {
        assert_eq!(narrow(u128::from(u64::MAX) + 5), u64::MAX);
        assert_eq!(narrow(7), 7);
    }
}
