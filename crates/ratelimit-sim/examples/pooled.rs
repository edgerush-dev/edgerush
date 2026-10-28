//! The shared-atomic spike's matrix (workspace `rate-limiting-shared-atomic-spike.md`,
//! phase 3): each worker apart, as the prototype runs them; pooled, the pod's process one
//! participant whose workers decide warm requests from shared entries
//! (`service::pooled`); and one `DataPlane` a pod, the semantic reference for sharing — the
//! same traffic, over many seeds, printed as Markdown.
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example pooled [seeds] [workers]`
//!
//! Four pods of 8 workers unless said. What goes to a pod goes to one of its workers, as
//! connections land. A pause holds the pooled process's background (its cycles, the
//! answers to it and its workers' asks) for 100 ms; apart and the reference have no
//! background apart from deciding, and go on. Restarts and pauses strike a whole pod.

use edgerush_ratelimit_sim::{
    Approach, Beside, Contract, Fault, Network, Order, Run, Shape, Workload, run,
};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
/// Pooled's levers: the fourth argument, `no-poll` for quiet keys not polled; the
/// protocol's own unless said. `both` is taken for `no-poll`, as runs from when cold keys
/// could be asked about named it.
fn levers() -> edgerush_ratelimit::service::pooled::Levers {
    let chosen = std::env::args().nth(4).unwrap_or_default();
    edgerush_ratelimit::service::pooled::Levers {
        poll_quiet: !matches!(chosen.as_str(), "no-poll" | "both"),
    }
}

/// Whether each participant visits only the keys that need it each cycle: a fifth argument
/// `scheduled`; every key unless said.
fn scheduled() -> bool {
    std::env::args().nth(5).as_deref() == Some("scheduled")
}

/// The protocol's cycle: the third argument, in milliseconds, 10 unless said.
fn cycle() -> u64 {
    std::env::args()
        .nth(3)
        .and_then(|arg| arg.parse::<u64>().ok())
        .unwrap_or(10)
        * MS
}
const PODS: usize = 4;

/// What one configuration did, over every seed.
#[derive(Default, Clone, Copy)]
struct Totals {
    sent: u64,
    refused: u64,
    waited: u64,
    waited_for: u64,
    messages: u64,
    worst_excess: i64,
    excess_sum: i64,
    lost: u64,
    twice: u64,
    runs: u64,
}

/// A case: its name, contract, whether its traffic keeps the contract, the shape for one
/// data plane a pod and for `w` workers a pod, and the fault for each.
struct Case {
    name: &'static str,
    contract: Contract,
    conforming: bool,
    shapes: fn(&Contract, usize) -> (Shape, Shape),
    faults: fn(u64, usize) -> (Fault, Fault),
    network: Network,
    /// Limits beside the run's on every request, decided all or none.
    beside: Option<(Beside, Order)>,
}

fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(limit),
        window: n(SECOND),
        burst: n(burst),
    }
}

fn none(_: u64, _: usize) -> (Fault, Fault) {
    (Fault::None, Fault::None)
}

/// The pooled process of the last pod held 100 ms, from 50 ms before the client moves.
fn paused(span: u64, w: usize) -> (Fault, Fault) {
    let at = 2 * span / 3 - 50 * MS;
    (
        Fault::Pause {
            at,
            length: 100 * MS,
            pod: PODS - 1,
        },
        Fault::Pause {
            at,
            length: 100 * MS,
            pod: (PODS - 1) * w,
        },
    )
}

/// A client spread over half of the last pod's workers, then on one, then on a worker of
/// the same pod it had not reached; one data plane a pod sees it on one pod throughout.
fn migrating(per_window: u64, w: usize) -> (Shape, Shape) {
    (
        Shape::Skewed { per_window },
        Shape::Migrating {
            per_window,
            last: (w / 2).max(1),
            to: w,
        },
    )
}

/// The same, ending on the last worker of the pod before: to another pod.
fn migrating_away(per_window: u64, w: usize) -> (Shape, Shape) {
    (
        Shape::Migrating {
            per_window,
            last: 1,
            to: 2,
        },
        Shape::Migrating {
            per_window,
            last: (w / 2).max(1),
            to: w + 1,
        },
    )
}

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(200);
    let w: usize = std::env::args()
        .nth(2)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(8);
    let reliable = Network::reliable(MS, MS / 2);
    let tcp = Network {
        loss_per_mille: 100,
        retransmit: Some(200 * MS),
        ..reliable
    };
    let cases = [
        Case {
            name: "client moving to a worker of its pod it had not reached, half the limit",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, w| migrating(c.limit.get() / 2, w),
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "the same, background held 100 ms across the move",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, w| migrating(c.limit.get() / 2, w),
            faults: paused,
            network: reliable,
            beside: None,
        },
        Case {
            name: "client moving to a worker it had not reached, at the limit",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, w| migrating(c.limit.get(), w),
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "the same at the limit, background held 100 ms across the move",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, w| migrating(c.limit.get(), w),
            faults: paused,
            network: reliable,
            beside: None,
        },
        Case {
            name: "client moving to another pod, at the limit, background held 100 ms across the move",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, w| migrating_away(c.limit.get(), w),
            faults: paused,
            network: reliable,
            beside: None,
        },
        Case {
            name: "tiny limit: 1 a second, a bucket of 1, spread, at the limit",
            contract: contract(1, 1),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "small limit: 50 a second (half a request a cycle), a bucket of 5, spread, at the limit",
            contract: contract(50, 5),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "small limit: 50 a second, a bucket of 5, spread, 3× the limit",
            contract: contract(50, 5),
            conforming: false,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: 3 * c.limit.get(),
                };
                (s, s)
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "steady overload, spread, 3× the limit",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: 3 * c.limit.get(),
                };
                (s, s)
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "a bucket at once on every pod, every second",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::Bursts {
                        each: c.burst.get(),
                    },
                    Shape::Bursts {
                        each: c.burst.get() / w as u64,
                    },
                )
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "a bucket on every pod over 30 ms, every second",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::SpreadBursts {
                        each: c.burst.get(),
                        over: 30 * MS,
                    },
                    Shape::SpreadBursts {
                        each: c.burst.get() / w as u64,
                        over: 30 * MS,
                    },
                )
            },
            faults: none,
            network: reliable,
            beside: None,
        },
        Case {
            name: "bursts, the key's owner lost half way",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::Bursts {
                        each: c.burst.get(),
                    },
                    Shape::Bursts {
                        each: c.burst.get() / w as u64,
                    },
                )
            },
            faults: |span, _| {
                let f = Fault::OwnerLost { at: span / 2 };
                (f, f)
            },
            network: reliable,
            beside: None,
        },
        Case {
            name: "bursts, the key's owner restarted half way",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::Bursts {
                        each: c.burst.get(),
                    },
                    Shape::Bursts {
                        each: c.burst.get() / w as u64,
                    },
                )
            },
            faults: |span, _| {
                let f = Fault::OwnerRestart { at: span / 2 };
                (f, f)
            },
            network: reliable,
            beside: None,
        },
        Case {
            name: "spread at the limit, half the pods cut off from the rest and the service for 4 s",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: |span, w| {
                let (from, until) = (span / 3, span / 3 + 4 * SECOND);
                (
                    Fault::Partition {
                        from,
                        until,
                        split: PODS / 2,
                    },
                    Fault::Partition {
                        from,
                        until,
                        split: PODS / 2 * w,
                    },
                )
            },
            network: reliable,
            beside: None,
        },
        Case {
            name: "bursts over TCP, one packet in ten lost",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::Bursts {
                        each: c.burst.get(),
                    },
                    Shape::Bursts {
                        each: c.burst.get() / w as u64,
                    },
                )
            },
            faults: none,
            network: tcp,
            beside: None,
        },
        Case {
            name: "spread at the limit, a pod restarted half way",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: |span, w| {
                (
                    Fault::Restart {
                        at: span / 2,
                        pod: 1,
                    },
                    Fault::Restart {
                        at: span / 2,
                        pod: w,
                    },
                )
            },
            network: reliable,
            beside: None,
        },
        Case {
            name: "spread at the limit, a rolling restart, a pod a second",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: |span, w| {
                (
                    Fault::Rolling {
                        from: span / 4,
                        every: SECOND,
                    },
                    Fault::Rolling {
                        from: span / 4,
                        every: SECOND / w as u64,
                    },
                )
            },
            network: reliable,
            beside: None,
        },
        Case {
            name: "two routes, each under its own limit and one over the data plane (different owners), bursts",
            contract: contract(96, 96),
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::Bursts {
                        each: c.burst.get(),
                    },
                    Shape::Bursts {
                        each: c.burst.get() / w as u64,
                    },
                )
            },
            faults: none,
            network: reliable,
            beside: Some((Beside::Routes(contract(80, 80)), Order::SmallestFirst)),
        },
        Case {
            name: "two routes under their own limits and one shared, spread at the shared limit",
            contract: contract(96, 96),
            conforming: true,
            shapes: |c, _| {
                let s = Shape::Spread {
                    per_window: c.limit.get(),
                };
                (s, s)
            },
            faults: none,
            network: reliable,
            beside: Some((Beside::Routes(contract(200, 200)), Order::SmallestFirst)),
        },
    ];

    println!(
        "{PODS} pods of {w} workers; runs of ten seconds, {seeds} seeds each; messages take \
         1 ms, give or take 0.5 ms (TCP rows: 10% lost, sent again after 200 ms); a {} ms \
         cycle. \"pooled\": each pod's process one participant, its workers deciding from shared entries \
         (`Approach::Pooled`, levers {:?}, {}); \"one\": one `DataPlane` a pod, the \
         reference.\n",
        cycle() / MS,
        levers(),
        if scheduled() {
            "visiting only the keys that need it"
        } else {
            "visiting every key every cycle"
        }
    );
    let mut table: Vec<(String, [Totals; 2])> = Vec::new();
    for case in &cases {
        let span = 10 * case.contract.window.get();
        let (one_shape, workers_shape) = (case.shapes)(&case.contract, w);
        let (one_fault, workers_fault) = (case.faults)(span, w);
        let mut cell = [Totals::default(); 2];
        let (pooled, one) = match case.beside {
            None => (
                Approach::PooledWith {
                    cycle: cycle(),
                    exchange_timeout: 0,
                    workers: w,
                    levers: levers(),
                    scheduled: scheduled(),
                },
                Approach::Nodes { cycle: cycle() },
            ),
            Some((beside, order)) => {
                let under = Approach::NodesUnder {
                    cycle: cycle(),
                    beside,
                    order,
                };
                let pooled = Approach::PooledUnder {
                    cycle: cycle(),
                    exchange_timeout: 0,
                    workers: w,
                    beside,
                    order,
                    levers: levers(),
                    scheduled: scheduled(),
                };
                (pooled, under)
            }
        };
        let setups = [
            (pooled, PODS * w, workers_shape, workers_fault),
            (one, PODS, one_shape, one_fault),
        ];
        for (totals, (approach, pods, shape, fault)) in cell.iter_mut().zip(setups) {
            for seed in 0..seeds {
                let outcome = run(&Run {
                    contract: case.contract,
                    pods,
                    workload: Workload {
                        shape,
                        span,
                        conforming: case.conforming,
                    },
                    approach,
                    network: case.network,
                    fault,
                    seed,
                });
                totals.sent += outcome.sent;
                totals.refused += outcome.refused;
                totals.waited += outcome.waited;
                totals.waited_for += outcome.waited_for;
                totals.messages += outcome.messages;
                totals.worst_excess = totals.worst_excess.max(outcome.worst_excess);
                totals.excess_sum += outcome.worst_excess;
                totals.lost += outcome.lost;
                totals.twice += outcome.counted_twice;
                totals.runs += 1;
            }
        }
        let name = format!(
            "{}{}",
            case.name,
            if case.conforming { " (conforming)" } else { "" }
        );
        table.push((name, cell));
    }

    let print = |what: &str, cell: &dyn Fn(&Totals) -> String| {
        println!("| {what} | pooled | one |");
        println!("|---|---|---|");
        for (name, [pooled, one]) in &table {
            println!("| {name} | {} | {} |", cell(pooled), cell(one));
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
    print("Waited for a decision: %, mean ms", &|t| {
        format!(
            "{:.2}%, {:.1}",
            t.waited as f64 * 100.0 / t.sent.max(1) as f64,
            t.waited_for as f64 / t.waited.max(1) as f64 / MS as f64
        )
    });
    print("Messages a second", &|t| {
        format!("{:.0}", t.messages as f64 / t.runs.max(1) as f64 / 10.0)
    });
    print("Admitted never counted, counted twice (all runs)", &|t| {
        format!("{}, {}", t.lost, t.twice)
    });
}
