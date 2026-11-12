//! EdgeRush rate limiting.
//!
//! A pure crate: it decides whether a request is admitted and does nothing else — no
//! sockets, clocks, randomness or async runtime. The time of every decision is passed in,
//! in nanoseconds from any fixed origin, which keeps it deterministic and easy to test.
//!
//! It holds [`reference`](mod@reference): the admission models of docs/06 §3, each written
//! as plainly as it can be, so that what each one promises can be read, tested and compared
//! before any of them is built for the request path; and [`service`](mod@service): the
//! protocol of the rate-limiting service, a candidate, as two state machines that the
//! simulator and the prototype both run; and [`local`](mod@local): local mode's limiter, each
//! process on what it sees alone. The synchronous mode's owner is the cluster store's
//! (`edgerush_cluster_synchronous`).

/// How far, in nanoseconds, a worker's clock may lag the latest charge of a key without a
/// request within the limit being refused on a data plane: a bucket admits a request whose
/// arrival time is up to its burst and this much refill ahead (06 §4). Workers read their own
/// clocks, so one may decide a moment after another that read a later time; a key at 100 a
/// second refills a request every 10 ms, so microseconds of lag refuse nothing below about
/// 100,000 a second, and this covers the rest. The burst grows by it times the rate: 0.01 of a
/// request at 100 a second, 100 at a million. The rate is unchanged.
pub const SLACK: u64 = 100_000;

pub mod local;
mod rate;
pub mod reference;
pub mod service;
