//! The EdgeRush rate-limit simulator.
//!
//! It runs ways of limiting one key across several pods against the contract they are meant
//! to keep, deterministically: the same run gives the same decisions on every machine. No
//! sockets, clocks or threads — time is a number, and randomness comes from a seed. It is
//! where the ways of sharing counts between pods (docs 06 §4, 10 §2) are compared, before
//! any networking exists.
//!
//! A [`Run`] is a contract, a number of pods, a [`Workload`], an [`Approach`] and a seed;
//! [`run`] plays it and measures the [`Outcome`] against the contract itself, not against
//! another limiter's decisions: how many requests were refused, and how far admissions went
//! over what the contract allows in any span of time.

mod approach;
mod measure;
mod rng;
mod workload;

pub use approach::{Approach, Cluster};
pub use measure::{Outcome, worst_excess};
pub use rng::Rng;
pub use workload::{Arrival, Shape, Workload};

use std::num::NonZeroU64;

/// The contract of a token bucket: `limit` requests per `window` nanoseconds on average,
/// at most `burst` at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contract {
    /// Requests a window, on average.
    pub limit: NonZeroU64,
    /// The window, in nanoseconds.
    pub window: NonZeroU64,
    /// The whole bucket: how many may come at once, from idle.
    pub burst: NonZeroU64,
}

/// One simulation: everything that decides its outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
    /// What the pods together are meant to keep.
    pub contract: Contract,
    /// How many pods share the key.
    pub pods: usize,
    /// The traffic.
    pub workload: Workload,
    /// How the pods limit it.
    pub approach: Approach,
    /// The seed every random choice comes from.
    pub seed: u64,
}

/// Every request of a run with the decision it got, in the order they were decided: what a
/// failed check is replayed from.
#[must_use]
pub fn decisions(run: &Run) -> Vec<(Arrival, bool)> {
    let mut rng = Rng::new(run.seed);
    let arrivals = run.workload.arrivals(&run.contract, run.pods, &mut rng);
    let mut cluster = run.approach.build(&run.contract, run.pods);
    arrivals
        .into_iter()
        .map(|arrival| {
            let admitted = cluster.decide(arrival.pod, arrival.at);
            (arrival, admitted)
        })
        .collect()
}

/// Plays a run and measures it against its contract.
#[must_use]
pub fn run(run: &Run) -> Outcome {
    Outcome::of(&run.contract, run.pods, &decisions(run))
}
