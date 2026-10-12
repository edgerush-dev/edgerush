//! The baselines every way of sharing counts between pods is measured against, over many
//! seeds, printed as Markdown: one bucket for the fleet (the contract kept exactly), pods
//! that share nothing, and pods that each hold a share.
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example baselines [seeds]`

use edgerush_ratelimit_sim::{Approach, Contract, Run, Shape, Workload, run};
use std::num::NonZeroU64;

const SECOND: u64 = 1_000_000_000;

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(200);
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    let contract = Contract {
        limit: n(100),
        window: n(SECOND),
        burst: n(100),
    };
    let approaches = [
        ("central", Approach::Central),
        ("independent", Approach::Independent),
        ("split", Approach::Split),
    ];
    let workloads = [
        (
            "spread at the limit, conforming",
            Shape::Spread { per_window: 100 },
            true,
        ),
        (
            "all on one pod at the limit, conforming",
            Shape::Skewed { per_window: 100 },
            true,
        ),
        (
            "spread at 3× the limit",
            Shape::Spread { per_window: 300 },
            false,
        ),
        (
            "a bucket on every pod at once, every second",
            Shape::Bursts { each: 100 },
            false,
        ),
        (
            "one pod at the limit, then bursts",
            Shape::SkewedThenBursts {
                per_window: 100,
                each: 100,
            },
            false,
        ),
    ];
    let pods = [1usize, 2, 4, 8];

    println!("Contract: 100 a second, burst 100. Runs of 10 s, {seeds} seeds each.");
    println!(
        "Refused: share of requests refused (for conforming traffic, every refusal is false)."
    );
    println!(
        "Admitted: against what one bucket for the fleet admits. Over: the most admitted over"
    );
    println!("the contract in any span, worst seed.\n");
    for (title, shape, conforming) in workloads {
        println!("**{title}**\n");
        print!("| Approach |");
        for p in pods {
            print!(" {p} pod{} |", if p == 1 { "" } else { "s" });
        }
        println!();
        println!("|---|{}", "---|".repeat(pods.len()));
        for (name, approach) in approaches {
            print!("| {name} |");
            for p in pods {
                let (mut sent, mut refused, mut admitted, mut central, mut over) =
                    (0, 0, 0, 0, i64::MIN);
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
                }
                let refused = 100.0 * refused as f64 / sent.max(1) as f64;
                let ratio = admitted as f64 / central.max(1) as f64;
                print!(" refused {refused:.1}%, admitted {ratio:.2}×, over {over} |");
            }
            println!();
        }
        println!();
    }
}
