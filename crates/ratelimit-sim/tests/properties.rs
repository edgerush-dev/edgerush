//! What 10 §2 asks the simulation to show, for the baselines every way of sharing is
//! measured against.

#![allow(
    clippy::unwrap_used,
    reason = "test set-up: the helpers make contracts from constants known not to be zero"
)]

use edgerush_ratelimit_sim::{Approach, Contract, Run, Shape, Workload, decisions, run};
use proptest::prelude::*;
use std::num::NonZeroU64;

const SECOND: u64 = 1_000_000_000;

fn shape() -> impl Strategy<Value = Shape> {
    prop_oneof![
        (1u64..400).prop_map(|per_window| Shape::Spread { per_window }),
        (1u64..400).prop_map(|per_window| Shape::Skewed { per_window }),
        (1u64..60).prop_map(|each| Shape::Bursts { each }),
        (1u64..400, 1u64..60)
            .prop_map(|(per_window, each)| Shape::SkewedThenBursts { per_window, each }),
    ]
}

fn contract() -> impl Strategy<Value = Contract> {
    (1u64..200, 1u64..200).prop_map(|(limit, burst)| Contract {
        limit: NonZeroU64::new(limit).unwrap(),
        window: NonZeroU64::new(SECOND).unwrap(),
        burst: NonZeroU64::new(burst).unwrap(),
    })
}

fn runs(approach: Approach, conforming: bool) -> impl Strategy<Value = Run> {
    (contract(), 1usize..8, shape(), any::<u64>()).prop_map(move |(contract, pods, shape, seed)| {
        Run {
            contract,
            pods,
            workload: Workload {
                shape,
                span: 3 * SECOND,
                conforming,
            },
            approach,
            seed,
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Property 1: traffic that keeps the contract is not refused, however it lands on
    /// the pods — by one bucket for the fleet, nor by pods each holding the whole contract.
    #[test]
    fn conforming_traffic_is_never_refused(run in runs(Approach::Central, true), independent in any::<bool>()) {
        let run = Run { approach: if independent { Approach::Independent } else { Approach::Central }, ..run };
        let outcome = edgerush_ratelimit_sim::run(&run);
        prop_assert_eq!(outcome.refused, 0, "{:?}", run);
    }

    /// One bucket for the fleet keeps the contract exactly, whatever comes.
    #[test]
    fn central_never_goes_over(run in runs(Approach::Central, false)) {
        prop_assert!(edgerush_ratelimit_sim::run(&run).worst_excess <= 0, "{:?}", run);
    }

    /// Pods sharing nothing each keep the contract alone; together they can go over by as
    /// much as the other pods' whole contracts.
    #[test]
    fn independent_pods_each_keep_the_contract(run in runs(Approach::Independent, false)) {
        let outcome = edgerush_ratelimit_sim::run(&run);
        prop_assert!(outcome.worst_pod_excess <= 0, "{:?}", run);
    }

    /// Shares never go over by more than the rounding up of the burst, one request a pod.
    #[test]
    fn split_pods_go_over_by_their_rounding_at_most(run in runs(Approach::Split, false)) {
        let outcome = edgerush_ratelimit_sim::run(&run);
        let rounding = i64::try_from(run.pods).unwrap() - 1;
        prop_assert!(outcome.worst_excess <= rounding, "{} over: {:?}", outcome.worst_excess, run);
    }

    /// Property 6: the same run decides the same way every time.
    #[test]
    fn a_run_is_deterministic(run in runs(Approach::Independent, false)) {
        prop_assert_eq!(decisions(&run), decisions(&run));
    }
}

fn fixed(approach: Approach, pods: usize, shape: Shape, conforming: bool) -> Run {
    let n = |n| NonZeroU64::new(n).unwrap();
    Run {
        contract: Contract {
            limit: n(100),
            window: n(SECOND),
            burst: n(100),
        },
        pods,
        workload: Workload {
            shape,
            span: 10 * SECOND,
            conforming,
        },
        approach,
        seed: 1,
    }
}

/// Synchronised bursts of a whole bucket on every pod, every window: pods sharing nothing
/// admit every pod's bucket each time, `pods` times the contract, sustained.
#[test]
fn synchronised_bursts_take_independent_pods_to_pods_times_the_contract() {
    let outcome = run(&fixed(
        Approach::Independent,
        4,
        Shape::Bursts { each: 100 },
        false,
    ));
    assert_eq!(outcome.admitted, 4 * 100 * 10);
    // Ten bursts of 400 against 100 a second and a bucket of 100: 4,000 against 1,000.
    assert_eq!(outcome.worst_excess, 3_000);
    let central = run(&fixed(
        Approach::Central,
        4,
        Shape::Bursts { each: 100 },
        false,
    ));
    assert_eq!(central.admitted, 1_000);
}

/// All of a customer's traffic on one pod, within the contract: a share of it refuses
/// most of what the customer is entitled to — why a bound sized for an even spread is
/// wrong (06 §4).
#[test]
fn skewed_conforming_traffic_is_shaved_by_shares() {
    let outcome = run(&fixed(
        Approach::Split,
        4,
        Shape::Skewed { per_window: 100 },
        true,
    ));
    assert!(outcome.sent >= 900);
    assert!(outcome.refused * 2 > outcome.sent, "{outcome:?}");
    let independent = run(&fixed(
        Approach::Independent,
        4,
        Shape::Skewed { per_window: 100 },
        true,
    ));
    assert_eq!(independent.refused, 0);
}
