//! A data-plane pod's workers as data planes of their own, as the prototype runs them;
//! layered, the pod's process one data plane syncing its workers' allowances every
//! millisecond; and one data plane a pod, its workers sharing its keys at no cost: the same
//! traffic on the same pods, over many seeds, printed as Markdown (12 item 3).
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example workers [seeds] [pods]`
//!
//! Four pods unless said, of 1, 4, 8 and 16 workers. Each worker is a data plane of the
//! protocol (`Approach::Nodes`, the state machines the proxy runs) when it keeps its own
//! keys; one data plane stands for the whole pod when they share them. What goes to a pod
//! goes to one of its workers at random, as connections land; a client on one connection
//! stays on one worker. Contract: 96 a second, a bucket of 96, so that a pod's burst splits
//! evenly among its workers.

use edgerush_ratelimit_sim::{Approach, Contract, Fault, Network, Run, Shape, Workload, run};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
const CYCLE: u64 = 10 * MS;

/// What one configuration did, over every seed.
#[derive(Default)]
struct Totals {
    sent: u64,
    refused: u64,
    waited: u64,
    waited_for: u64,
    messages: u64,
    worst_excess: i64,
    excess_sum: i64,
    runs: u64,
}

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(200);
    let pods: usize = std::env::args()
        .nth(2)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(4);
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    let contract = Contract {
        limit: n(96),
        window: n(SECOND),
        burst: n(96),
    };
    let limit = contract.limit.get();
    let burst = contract.burst.get();
    let span = 10 * contract.window.get();
    let workers = [1_usize, 4, 8, 16];
    let network = Network::reliable(MS, MS / 2);
    let approach = Approach::Nodes {
        cycle: CYCLE,
        ask_timeout: 0,
    };

    // A case: its name, whether its traffic keeps the contract (every refusal then a false
    // one), and its shape on the pods sharing their keys and on `w` workers a pod.
    type Shaped = fn(u64, u64, usize) -> (Shape, Shape);
    let cases: [(&str, bool, Shaped); 6] = [
        (
            "spread over the fleet, at the limit",
            true,
            |limit, _, _| {
                let shape = Shape::Spread { per_window: limit };
                (shape, shape)
            },
        ),
        (
            "spread over the fleet, 3× the limit",
            false,
            |limit, _, _| {
                let shape = Shape::Spread {
                    per_window: 3 * limit,
                };
                (shape, shape)
            },
        ),
        (
            "one client, many connections to one pod, at the limit",
            true,
            |limit, _, w| {
                (
                    Shape::Skewed { per_window: limit },
                    Shape::Within {
                        per_window: limit,
                        last: w,
                    },
                )
            },
        ),
        (
            "one client, many connections to one pod, 3× the limit",
            false,
            |limit, _, w| {
                (
                    Shape::Skewed {
                        per_window: 3 * limit,
                    },
                    Shape::Within {
                        per_window: 3 * limit,
                        last: w,
                    },
                )
            },
        ),
        (
            "one client, one connection, 3× the limit",
            false,
            |limit, _, _| {
                let shape = Shape::Skewed {
                    per_window: 3 * limit,
                };
                (shape, shape)
            },
        ),
        (
            "a bucket at once on every pod, every second",
            false,
            |_, burst, w| {
                (
                    Shape::Bursts { each: burst },
                    Shape::Bursts {
                        each: burst / w as u64,
                    },
                )
            },
        ),
    ];

    println!(
        "Contract: {limit} a second, a bucket of {burst}. {pods} pods; runs of ten seconds, \
         {seeds} seeds each; messages take 1 ms, give or take 0.5 ms; a 10 ms cycle. \
         \"apart\": each worker a data plane; \"layered\": the process one, its workers \
         synced every millisecond; \"shared\": one a pod, at no cost.\n"
    );
    let mut table: Vec<(String, Vec<[Totals; 3]>)> = Vec::new();
    for (name, conforming, shaped) in cases {
        let mut row = Vec::new();
        for &w in &workers {
            let (shared_shape, apart_shape) = shaped(limit, burst, w);
            let layered = Approach::Layered {
                cycle: CYCLE,
                ask_timeout: 0,
                workers: w,
                sync: MS,
            };
            let mut cell: [Totals; 3] = Default::default();
            let [apart, layers, shared] = &mut cell;
            for (totals, shape, planes, approach) in [
                (apart, apart_shape, pods * w, approach),
                (layers, apart_shape, pods * w, layered),
                (shared, shared_shape, pods, approach),
            ] {
                for seed in 0..seeds {
                    let outcome = run(&Run {
                        contract,
                        pods: planes,
                        workload: Workload {
                            shape,
                            span,
                            conforming,
                        },
                        approach,
                        network,
                        fault: Fault::None,
                        seed,
                    });
                    totals.sent += outcome.sent;
                    totals.refused += outcome.refused;
                    totals.waited += outcome.waited;
                    totals.waited_for += outcome.waited_for;
                    totals.messages += outcome.messages;
                    totals.worst_excess = totals.worst_excess.max(outcome.worst_excess);
                    totals.excess_sum += outcome.worst_excess;
                    totals.runs += 1;
                }
            }
            row.push(cell);
        }
        table.push((
            format!("{name}{}", if conforming { " (conforming)" } else { "" }),
            row,
        ));
    }

    let header = |what: &str| {
        print!("| {what} |");
        for w in workers {
            print!(" {w} apart | {w} layered | {w} shared |");
        }
        println!();
        println!("|---|{}", "---|---|---|".repeat(workers.len()));
    };
    let print = |what: &str, cell: &dyn Fn(&Totals) -> String| {
        header(what);
        for (name, row) in &table {
            print!("| {name} |");
            for [apart, layered, shared] in row {
                print!(" {} | {} | {} |", cell(apart), cell(layered), cell(shared));
            }
            println!();
        }
        println!();
    };
    print("Refused, per 10,000 sent", &|t| {
        format!("{:.1}", t.refused as f64 * 10_000.0 / t.sent.max(1) as f64)
    });
    print("Most over the contract in any span: mean, worst", &|t| {
        format!(
            "{:.1}, {}",
            t.excess_sum as f64 / t.runs.max(1) as f64,
            t.worst_excess
        )
    });
    print("Waited for the service: %, mean ms", &|t| {
        format!(
            "{:.2}%, {:.1}",
            t.waited as f64 * 100.0 / t.sent.max(1) as f64,
            t.waited_for as f64 / t.waited.max(1) as f64 / MS as f64
        )
    });
    let seconds = (span / SECOND) as f64;
    print("Messages a second", &|t| {
        format!("{:.0}", t.messages as f64 / t.runs.max(1) as f64 / seconds)
    });
}
