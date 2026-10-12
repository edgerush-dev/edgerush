//! Ways of limiting one key across several pods.

use crate::Contract;
use edgerush_ratelimit::reference::{Model, TokenBucket};
use std::num::NonZeroU64;

/// The pods of a fleet limiting one key, however they share what they know.
pub trait Cluster {
    /// Decides a request arriving at `pod` at `now`: `true` admits it.
    fn decide(&mut self, pod: usize, now: u64) -> bool;
}

/// How the pods limit a key between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Approach {
    /// One bucket for the whole fleet, as if every pod saw every other's admissions at
    /// once: the contract kept exactly, which sharing aims at and never quite reaches.
    Central,
    /// Every pod a bucket of its own holding the whole contract, sharing nothing: what each
    /// pod does with its peers gone (06 §6), and the baseline any sharing has to beat.
    Independent,
    /// Every pod a share of the contract, the rate and the burst divided by the pods (the
    /// burst rounded up): never far over the contract, at the price of refusing traffic
    /// that lands unevenly (06 §4).
    Split,
}

impl Approach {
    /// The pods of a fleet of `pods` limiting a key to `contract` this way.
    #[must_use]
    pub fn build(&self, contract: &Contract, pods: usize) -> Box<dyn Cluster> {
        let pods = pods.max(1);
        let bucket = || TokenBucket::new(contract.limit, contract.window, contract.burst);
        match self {
            Self::Central => Box::new(Pods(vec![bucket()])),
            Self::Independent => Box::new(Pods((0..pods).map(|_| bucket()).collect())),
            Self::Split => {
                let count = pods as u64;
                // `limit / pods` a window is `limit` every `pods` windows, kept exact.
                let window = NonZeroU64::new(contract.window.get().saturating_mul(count))
                    .unwrap_or(contract.window);
                let burst = NonZeroU64::new(contract.burst.get().div_ceil(count))
                    .unwrap_or(NonZeroU64::MIN);
                let share = || TokenBucket::new(contract.limit, window, burst);
                Box::new(Pods((0..pods).map(|_| share()).collect()))
            }
        }
    }
}

/// A bucket per pod, or one for all of them.
struct Pods(Vec<TokenBucket>);

impl Cluster for Pods {
    fn decide(&mut self, pod: usize, now: u64) -> bool {
        let count = self.0.len();
        self.0[pod % count].decide(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn contract(limit: u64, burst: u64) -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(limit),
            window: n(SECOND),
            burst: n(burst),
        }
    }

    fn at_once(cluster: &mut dyn Cluster, pod: usize, count: u64) -> u64 {
        (0..count).filter(|_| cluster.decide(pod, 0)).count() as u64
    }

    #[test]
    fn central_holds_the_whole_fleet_to_one_bucket() {
        let mut cluster = Approach::Central.build(&contract(100, 20), 4);
        assert_eq!(
            at_once(&mut *cluster, 0, 15) + at_once(&mut *cluster, 3, 15),
            20
        );
    }

    #[test]
    fn independent_pods_each_spend_the_whole_bucket() {
        let mut cluster = Approach::Independent.build(&contract(100, 20), 4);
        let total: u64 = (0..4).map(|pod| at_once(&mut *cluster, pod, 30)).sum();
        assert_eq!(total, 80);
    }

    #[test]
    fn split_pods_each_spend_a_share_rounded_up() {
        let mut cluster = Approach::Split.build(&contract(100, 22), 4);
        let each: Vec<u64> = (0..4).map(|pod| at_once(&mut *cluster, pod, 30)).collect();
        assert_eq!(each, vec![6, 6, 6, 6]);
        // A share of the rate too: a quarter of 100 a second is one every 40 ms.
        assert!(!cluster.decide(0, 39_999_999));
        assert!(cluster.decide(0, 40_000_000));
    }
}
