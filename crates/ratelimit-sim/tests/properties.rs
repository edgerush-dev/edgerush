//! What 10 §2 asks the simulation to show, for the baselines every way of sharing is
//! measured against.

#![allow(
    clippy::unwrap_used,
    reason = "test set-up: the helpers make contracts from constants known not to be zero"
)]

use edgerush_ratelimit_sim::{
    Approach, Bound, Contract, Excess, Fault, Network, OnOwnerChange, Run, Shape, Sharing,
    Workload, decisions, run,
};
use proptest::prelude::*;
use std::num::NonZeroU64;

const SECOND: u64 = 1_000_000_000;
/// A millisecond, give or take half of one.
const NETWORK: Network = Network {
    delay: 1_000_000,
    jitter: 500_000,
    loss_per_mille: 0,
};

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
            network: NETWORK,
            fault: Fault::None,
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

    /// Property 6: the same run decides the same way every time, messages and their
    /// delays included.
    #[test]
    fn a_run_is_deterministic(run in runs(Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::Drain,
            ledger: true,
            bound: Bound::Every,
            on_owner_change: OnOwnerChange::Drop,
        }), false)) {
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
        network: NETWORK,
        fault: Fault::None,
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

/// Pods that keep sharing their counts, cycle after cycle, admit clearly less under steady
/// overload than pods that share nothing — though, as Gubernator's model forgives each
/// cycle's excess, far more than the contract.
#[test]
fn ownership_keeps_sharing_through_a_run() {
    let shape = Shape::Spread { per_window: 300 };
    let owned = run(&fixed(
        Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::Drain,
            ledger: false,
            bound: Bound::Unbounded,
            on_owner_change: OnOwnerChange::Drop,
        }),
        2,
        shape,
        false,
    ));
    let nothing = run(&fixed(Approach::Independent, 2, shape, false));
    let central = run(&fixed(Approach::Central, 2, shape, false));
    assert!(
        owned.admitted * 10 < nothing.admitted * 8,
        "{owned:?} against {nothing:?}"
    );
    assert!(
        owned.admitted > central.admitted,
        "{owned:?} against {central:?}"
    );
}

fn lossy(ledger: bool, loss_per_mille: u64, seed: u64) -> Run {
    Run {
        approach: Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::Drain,
            ledger,
            bound: Bound::Unbounded,
            on_owner_change: OnOwnerChange::Drop,
        }),
        network: Network {
            loss_per_mille,
            ..NETWORK
        },
        seed,
        ..fixed(
            Approach::Central,
            4,
            Shape::Spread { per_window: 300 },
            false,
        )
    }
}

/// Property 3: with the ledger, every hit reaches the owner in the end, though one message
/// in ten is lost; without it, as in Gubernator, hits lost with a message stay lost.
#[test]
fn with_the_ledger_no_hit_is_lost_for_good() {
    for seed in 0..5 {
        assert_eq!(run(&lossy(true, 100, seed)).lost, 0, "seed {seed}");
        assert!(run(&lossy(false, 100, seed)).lost > 0, "seed {seed}");
    }
}

/// With no message lost, no hit is either.
#[test]
fn a_reliable_network_loses_nothing() {
    for ledger in [false, true] {
        assert_eq!(run(&lossy(ledger, 0, 1)).lost, 0);
    }
}

fn shared(bound: Bound, shape: Shape, conforming: bool) -> Run {
    Run {
        approach: Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::DebtUpTo(SECOND),
            ledger: true,
            bound,
            on_owner_change: OnOwnerChange::Drop,
        }),
        ..fixed(Approach::Central, 8, shape, conforming)
    }
}

/// Bounding every pod's share of a cycle holds bursts that land on every pod at once to a
/// fraction of what they reach unbounded, and costs a client on one pod nothing.
#[test]
fn a_bound_on_every_pod_holds_simultaneous_bursts_without_shaving_skew() {
    let bursts = Shape::Bursts { each: 100 };
    let unbounded = run(&shared(Bound::Unbounded, bursts, false));
    let every = run(&shared(Bound::Every, bursts, false));
    assert!(
        every.admitted * 2 < unbounded.admitted,
        "{every:?} against {unbounded:?}"
    );
    let skewed = run(&shared(
        Bound::Every,
        Shape::Skewed { per_window: 300 },
        true,
    ));
    assert!(skewed.refused * 100 < skewed.sent, "{skewed:?}");
}

fn owner_lost(on_owner_change: OnOwnerChange, seed: u64) -> Run {
    Run {
        approach: Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::DebtUpTo(SECOND),
            ledger: true,
            bound: Bound::Every,
            on_owner_change,
        }),
        fault: Fault::OwnerLost { at: 5 * SECOND },
        seed,
        ..fixed(
            Approach::Central,
            4,
            Shape::Spread { per_window: 300 },
            false,
        )
    }
}

/// The owner goes half way through: dropping what it had not been seen to apply loses a
/// few cycles of hits and counts none twice; sending them again loses none.
#[test]
fn an_owner_lost_costs_at_most_a_few_cycles_of_hits() {
    for seed in 0..5 {
        let dropped = run(&owner_lost(OnOwnerChange::Drop, seed));
        assert!(dropped.lost <= 30, "{dropped:?}");
        assert_eq!(dropped.counted_twice, 0);
        assert_eq!(run(&owner_lost(OnOwnerChange::SendAgain, seed)).lost, 0);
    }
}

/// Four seconds of half the pods cut off from the others under steady overload: each side
/// counts alone, so the fleet admits up to twice the contract while it lasts; healed, the
/// side gives up what it counted, and nothing is owed afterwards — the last two seconds are
/// back at the contract.
#[test]
fn a_partition_doubles_the_contract_while_it_lasts_and_leaves_nothing_owed() {
    let shape = Shape::Spread { per_window: 300 };
    let run_with = |fault| Run {
        approach: Approach::Ownership(Sharing {
            cycle: 10_000_000,
            excess: Excess::DebtUpTo(SECOND),
            ledger: true,
            bound: Bound::Every,
            on_owner_change: OnOwnerChange::SendAgain,
        }),
        fault,
        ..fixed(Approach::Central, 4, shape, false)
    };
    let partition = Fault::Partition {
        from: 3 * SECOND,
        until: 7 * SECOND,
        split: 2,
    };
    let whole = decisions(&run_with(Fault::None));
    let cut = decisions(&run_with(partition));
    let admitted = |decided: &[(edgerush_ratelimit_sim::Arrival, bool)], from: u64, until: u64| {
        decided
            .iter()
            .filter(|(arrival, admitted)| *admitted && (from..until).contains(&arrival.at))
            .count()
    };
    let (during, during_whole) = (
        admitted(&cut, 4 * SECOND, 7 * SECOND),
        admitted(&whole, 4 * SECOND, 7 * SECOND),
    );
    assert!(
        during * 10 > during_whole * 18,
        "{during} against {during_whole}"
    );
    let (after, after_whole) = (
        admitted(&cut, 8 * SECOND, 10 * SECOND),
        admitted(&whole, 8 * SECOND, 10 * SECOND),
    );
    assert!(after <= after_whole + 10, "{after} against {after_whole}");
    assert!(
        run(&run_with(partition)).lost > 0,
        "what one side counted alone"
    );
}
