//! Requests under two limits through the prototype's nodes, decided all or none (06 §4),
//! against the run's limit alone, over many seeds, printed as Markdown, with a request's
//! limits in either order — smallest bucket first, or the limit over the whole data plane
//! first. A cold request's ask goes to the first limit's owner, which decides that limit
//! exactly and the other from its copy:
//!
//! - **a limit over the data plane beside**: every request under the run's limit, 20 a
//!   second with a bucket of 20, and one over the whole data plane, 200 a second;
//! - **two routes, both binding**: requests alternating between two routes, each with a
//!   limit of its own of 80 a second, under the run's limit over the whole data plane, 100;
//! - **two routes, the routes binding**: the same with routes of 20 under 200.
//!
//! The limits' keys are owned by different service pods. Besides bursts, a client steadily
//! a quarter over the limit shows whether what goes over in a burst is made up for over a
//! run.
//!
//! `cargo run --release -p edgerush-ratelimit-sim --example two_limits [seeds]
//! [none|owner-lost|owner-restart|restart|rolling|partition] [pods, as 2,4,8] [lost in
//! 1,000]`; `RETRANSMIT_MS=200` carries messages as TCP does; `CASES=routes:500/625,
//! global:400/5000` runs other limits in place of those above, and `ONLY=steady` the steady
//! client alone.

use edgerush_ratelimit_sim::{
    Approach, Arrival, Beside, Contract, Fault, Network, Order, Run, Shape, Workload, decisions,
    run, worst_excess,
};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
const CYCLE: u64 = 10 * MS;

fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(limit),
        window: n(SECOND),
        burst: n(burst),
    }
}

/// Totals over the seeds of one cell.
#[derive(Default)]
struct Cell {
    runs: u64,
    sent: u64,
    refused: u64,
    admitted: u64,
    central: u64,
    over: Option<i64>,
    route_over: Option<i64>,
    lost: u64,
    twice: u64,
}

impl Cell {
    fn print(&self, routes: bool) {
        let refused = 100.0 * self.refused as f64 / self.sent.max(1) as f64;
        let ratio = self.admitted as f64 / self.central.max(1) as f64;
        let lost = 100.0 * self.lost as f64 / self.admitted.max(1) as f64;
        let twice = 100.0 * self.twice as f64 / self.admitted.max(1) as f64;
        let over = self.over.unwrap_or(0);
        // Over a run, on average, how many more than one bucket holding the limit admits.
        let surplus = (self.admitted as f64 - self.central as f64) / self.runs.max(1) as f64;
        print!(" {refused:.1}% / {ratio:.3}× ({surplus:+.0}) / {over}");
        if routes {
            print!(" / {}", self.route_over.unwrap_or(0));
        }
        print!(" / {lost:.2}% / {twice:.2}% |");
    }
}

/// The admissions of route `route` of two alternating, at the times they arrived.
fn route_times(decided: &[(Arrival, bool)], route: usize) -> Vec<u64> {
    decided
        .iter()
        .enumerate()
        .filter(|(index, (_, admitted))| index % 2 == route && *admitted)
        .map(|(_, (arrival, _))| arrival.at)
        .collect()
}

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(100);
    let fault_name = std::env::args().nth(2).unwrap_or_default();
    let pods: Vec<usize> = std::env::args()
        .nth(3)
        .map(|arg| arg.split(',').filter_map(|n| n.parse().ok()).collect())
        .filter(|pods: &Vec<usize>| !pods.is_empty())
        .unwrap_or_else(|| vec![2, 8]);
    let loss_per_mille: u64 = std::env::args()
        .nth(4)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(0);
    let network = Network {
        loss_per_mille,
        retransmit: std::env::var("RETRANSMIT_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
            .map(|ms| ms * MS),
        ..Network::reliable(MS, MS / 2)
    };
    let span = 10 * SECOND;
    let fault = |pods: usize| match fault_name.as_str() {
        "owner-lost" => Fault::OwnerLost { at: span / 2 },
        "owner-restart" => Fault::OwnerRestart { at: span / 2 },
        "restart" => Fault::Restart {
            at: span / 2,
            pod: pods - 1,
        },
        "rolling" => Fault::Rolling {
            from: span * 3 / 10,
            every: span * 4 / 10 / pods as u64,
        },
        "partition" => Fault::Partition {
            from: span * 3 / 10,
            until: span * 7 / 10,
            split: pods / 2,
        },
        _ => Fault::None,
    };
    let mut scenarios = vec![
        (
            "20 a second, bucket 20, under 200 a second over the data plane".to_owned(),
            contract(20, 20),
            Beside::Global(contract(200, 200)),
        ),
        (
            "Two routes of 80 a second each under 100 a second over the data plane (both bind)"
                .to_owned(),
            contract(100, 100),
            Beside::Routes(contract(80, 80)),
        ),
        (
            "Two routes of 20 a second each under 200 a second over the data plane (the routes bind)"
                .to_owned(),
            contract(200, 200),
            Beside::Routes(contract(20, 20)),
        ),
    ];
    // `CASES=routes:500/625,global:400/5000`: in place of those, two routes of so many a
    // second under so many over the data plane, or one limit of so many under so many over
    // the data plane; each bucket a second's worth.
    if let Ok(cases) = std::env::var("CASES") {
        scenarios = cases
            .split(',')
            .filter_map(|case| {
                let (kind, pair) = case.split_once(':')?;
                let (one, global) = pair.split_once('/')?;
                let (one, global): (u64, u64) = (one.parse().ok()?, global.parse().ok()?);
                Some(match kind {
                    "routes" => (
                        format!("Two routes of {one} a second each under {global} a second over the data plane"),
                        contract(global, global),
                        Beside::Routes(contract(one, one)),
                    ),
                    _ => (
                        format!("{one} a second under {global} a second over the data plane"),
                        contract(one, one),
                        Beside::Global(contract(global, global)),
                    ),
                })
            })
            .collect();
    }
    // `ONLY=steady`: only a client steadily over the limit.
    let only_steady = std::env::var("ONLY").is_ok_and(|only| only == "steady");
    println!(
        "Runs of ten seconds, {seeds} seeds each; messages take 1 ms, give or take 0.5 ms, \
         {loss_per_mille} in 1,000 lost; fault: {fault_name}. Each cell, for the run's limit: \
         share of requests refused / admitted against one bucket holding it, over the whole \
         run (how many more a run, on average) / the most over it \
         in any span, worst seed / (two routes: the most over a route's own limit) / share of \
         admissions some owner never counted / share counted twice, over every limit.\n"
    );
    for (title, limit, beside) in scenarios {
        let routes = matches!(beside, Beside::Routes(_));
        let each = match beside {
            Beside::Routes(each) => each,
            Beside::Global(_) => limit,
        };
        let l = limit.limit.get();
        let b = limit.burst.get();
        let workloads = [
            (
                "within the limit, spread",
                Shape::Spread { per_window: l },
                true,
            ),
            (
                "on its edge, uneven",
                Shape::Uneven {
                    per_window: 3 * l,
                    hot: 50,
                },
                true,
            ),
            (
                "three times it, spread",
                Shape::Spread { per_window: 3 * l },
                false,
            ),
            (
                "a whole bucket on every pod at once",
                Shape::Bursts { each: b },
                false,
            ),
            (
                "a whole bucket on every pod over 30 ms",
                Shape::SpreadBursts {
                    each: b,
                    over: 30 * MS,
                },
                false,
            ),
            (
                "a quarter over it, steadily, spread",
                Shape::Spread {
                    per_window: 5 * l / 4,
                },
                false,
            ),
            (
                "a quarter over it, steadily, all on one pod",
                Shape::Skewed {
                    per_window: 5 * l / 4,
                },
                false,
            ),
            (
                "a quarter over it, steadily, uneven",
                Shape::Uneven {
                    per_window: 5 * l / 4,
                    hot: 50,
                },
                false,
            ),
        ];
        let workloads = workloads
            .into_iter()
            .filter(|(name, _, _)| !only_steady || name.contains("steadily"));
        println!("**{title}**\n");
        print!("| Workload | Limits |");
        for &p in &pods {
            print!(" {p} pods |");
        }
        println!();
        println!("|---|---|{}", "---|".repeat(pods.len()));
        for (name, shape, conforming) in workloads {
            let under = |order| Approach::NodesUnder {
                cycle: CYCLE,
                ask_timeout: 0,
                beside,
                order,
            };
            for (rows, approach) in [
                (
                    "alone",
                    Approach::Nodes {
                        cycle: CYCLE,
                        ask_timeout: 0,
                    },
                ),
                ("both, smallest first", under(Order::SmallestFirst)),
                ("both, data plane's first", under(Order::SharedFirst)),
            ] {
                print!("| {name} | {rows} |");
                for &p in &pods {
                    let mut cell = Cell::default();
                    for seed in 0..seeds {
                        let this = Run {
                            contract: limit,
                            pods: p,
                            workload: Workload {
                                shape,
                                span,
                                conforming,
                            },
                            approach,
                            network,
                            fault: fault(p),
                            seed,
                        };
                        let outcome = run(&this);
                        let central = run(&Run {
                            approach: Approach::Central,
                            ..this
                        });
                        cell.runs += 1;
                        cell.sent += outcome.sent;
                        cell.refused += outcome.refused;
                        cell.admitted += outcome.admitted;
                        cell.central += central.admitted;
                        cell.over = cell.over.max(Some(outcome.worst_excess));
                        cell.lost += outcome.lost;
                        cell.twice += outcome.counted_twice;
                        if routes && rows != "alone" {
                            let decided = decisions(&this);
                            for route in 0..2 {
                                let over = worst_excess(&each, &route_times(&decided, route));
                                cell.route_over = cell.route_over.max(Some(over));
                            }
                        }
                    }
                    cell.print(routes);
                }
                println!();
            }
        }
        println!();
    }
}
