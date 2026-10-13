//! Ways of limiting one key across pods, side by side over many seeds, printed as Markdown:
//! the baselines (one bucket for the fleet, which keeps the contract exactly; pods sharing
//! nothing; pods with a share each) and the ways of sharing counts.
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example compare [seeds]`

use edgerush_ratelimit_sim::{
    Approach, Bound, Contract, Excess, Fault, Network, OnOwnerChange, Run, Shape, Sharing,
    Workload, run,
};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

fn owned(cycle: u64, excess: Excess) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: false,
        bound: Bound::Unbounded,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn ledger(cycle: u64, excess: Excess) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: true,
        bound: Bound::Unbounded,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn bounded(cycle: u64, excess: Excess, bound: Bound) -> Approach {
    Approach::Ownership(Sharing {
        cycle,
        excess,
        ledger: true,
        bound,
        on_owner_change: OnOwnerChange::Drop,
    })
}

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(200);
    let loss_per_mille: u64 = std::env::args()
        .nth(2)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(0);
    // `owner-lost`: the pod owning the key goes away half way through each run.
    // `partition`: half the pods are cut off from the others from 3 s to 7 s.
    let fault_name = std::env::args().nth(3).unwrap_or_default();
    let fault = |pods: usize| match fault_name.as_str() {
        "owner-lost" => Fault::OwnerLost { at: 5 * SECOND },
        "partition" => Fault::Partition {
            from: 3 * SECOND,
            until: 7 * SECOND,
            split: pods / 2,
        },
        _ => Fault::None,
    };
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    let contract = Contract {
        limit: n(100),
        window: n(SECOND),
        burst: n(100),
    };
    let network = Network {
        loss_per_mille,
        ..Network::reliable(MS, MS / 2)
    };
    let approaches = [
        ("central", Approach::Central),
        ("sharing nothing", Approach::Independent),
        ("a share each", Approach::Split),
        ("ownership, drain, 100 ms", owned(100 * MS, Excess::Drain)),
        ("ownership, drain, 10 ms", owned(10 * MS, Excess::Drain)),
        ("ownership, debt, 100 ms", owned(100 * MS, Excess::Debt)),
        ("ownership, debt, 10 ms", owned(10 * MS, Excess::Debt)),
        (
            "ownership, debt up to 1 s, 100 ms",
            owned(100 * MS, Excess::DebtUpTo(SECOND)),
        ),
        (
            "ownership, debt up to 1 s, 10 ms",
            owned(10 * MS, Excess::DebtUpTo(SECOND)),
        ),
        ("ledger, drain, 100 ms", ledger(100 * MS, Excess::Drain)),
        ("ledger, drain, 10 ms", ledger(10 * MS, Excess::Drain)),
        (
            "ledger, debt up to 1 s, 100 ms",
            ledger(100 * MS, Excess::DebtUpTo(SECOND)),
        ),
        (
            "ledger, debt up to 1 s, 10 ms",
            ledger(10 * MS, Excess::DebtUpTo(SECOND)),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, new pods bounded",
            bounded(10 * MS, Excess::DebtUpTo(SECOND), Bound::NewPods),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, every pod bounded",
            bounded(10 * MS, Excess::DebtUpTo(SECOND), Bound::Every),
        ),
        (
            "ledger, drain, 10 ms, every pod bounded",
            bounded(10 * MS, Excess::Drain, Bound::Every),
        ),
        (
            "ledger, debt up to 1 s, 10 ms, every pod bounded, sent again to a new owner",
            Approach::Ownership(Sharing {
                cycle: 10 * MS,
                excess: Excess::DebtUpTo(SECOND),
                ledger: true,
                bound: Bound::Every,
                on_owner_change: OnOwnerChange::SendAgain,
            }),
        ),
    ];
    let workloads = [
        (
            "Within the contract, spread",
            Shape::Spread { per_window: 100 },
            true,
        ),
        (
            "Within the contract, all on one pod",
            Shape::Skewed { per_window: 100 },
            true,
        ),
        (
            "On the contract's edge, spread (three times the limit, kept to what one bucket admits)",
            Shape::Spread { per_window: 300 },
            true,
        ),
        (
            "On the contract's edge, all on one pod",
            Shape::Skewed { per_window: 300 },
            true,
        ),
        (
            "Three times the limit, spread",
            Shape::Spread { per_window: 300 },
            false,
        ),
        (
            "A whole bucket on every pod at once, every second",
            Shape::Bursts { each: 100 },
            false,
        ),
        (
            "One pod at the limit, then such bursts",
            Shape::SkewedThenBursts {
                per_window: 100,
                each: 100,
            },
            false,
        ),
    ];
    let pods = [2usize, 4, 8];

    println!(
        "Contract: 100 a second, burst 100. Runs of 10 s, {seeds} seeds each; messages take \
         1 ms, give or take 0.5 ms, {loss_per_mille} in 1,000 lost. Each cell: share of \
         requests refused (for conforming traffic every refusal is false) / admitted against \
         one bucket for the fleet / the most over the contract in any span, worst seed / \
         share of admitted requests the fleet's count never saw / share it counted twice. \
         Fault: {fault_name}.\n"
    );
    for (title, shape, conforming) in workloads {
        println!("**{title}**\n");
        print!("| Approach |");
        for p in pods {
            print!(" {p} pods |");
        }
        println!();
        println!("|---|{}", "---|".repeat(pods.len()));
        for (name, approach) in approaches {
            print!("| {name} |");
            for p in pods {
                let (mut sent, mut refused, mut admitted, mut central, mut over) =
                    (0, 0, 0, 0, i64::MIN);
                let (mut lost, mut twice) = (0, 0);
                for seed in 0..seeds {
                    let workload = Workload {
                        shape,
                        span: 10 * SECOND,
                        conforming,
                    };
                    let this = Run {
                        contract,
                        pods: p,
                        workload,
                        approach,
                        network,
                        fault: fault(p),
                        seed,
                    };
                    let outcome = run(&this);
                    let baseline = run(&Run {
                        approach: Approach::Central,
                        ..this
                    });
                    sent += outcome.sent;
                    refused += outcome.refused;
                    admitted += outcome.admitted;
                    central += baseline.admitted;
                    over = over.max(outcome.worst_excess);
                    lost += outcome.lost;
                    twice += outcome.counted_twice;
                }
                let refused = 100.0 * refused as f64 / sent.max(1) as f64;
                let ratio = admitted as f64 / central.max(1) as f64;
                let lost = 100.0 * lost as f64 / admitted.max(1) as f64;
                let twice = 100.0 * twice as f64 / admitted.max(1) as f64;
                print!(" {refused:.1}% / {ratio:.2}× / {over} / {lost:.1}% / {twice:.1}% |");
            }
            println!();
        }
        println!();
    }
}
