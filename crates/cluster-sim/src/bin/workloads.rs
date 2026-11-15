//! The asynchronous mode on the store's three pods under the rate-limit workloads, over many
//! seeds, printed as Markdown: the participant on data planes of many workers against one
//! worker each (`pooled`), and requests under two limits against the run's alone
//! (`two-limits`).
//!
//! `cargo run --release -p edgerush-cluster-sim --bin workloads -- pooled [seeds] [workers]
//! [scheduled|every] [no-poll|poll]`: four data planes of 8 workers unless said, the proxy's
//! levers unless said (keys scheduled, quiet keys not polled).
//!
//! `cargo run --release -p edgerush-cluster-sim --bin workloads -- two-limits [seeds]
//! [none|owner-lost|owner-restart|restart|rolling|partition] [data planes, as 2,8] [lost in
//! 1,000] [no-poll|poll]`: one worker a data plane, keys scheduled, quiet keys not polled
//! unless said; `RETRANSMIT_MS=200` carries messages as TCP does.
//!
//! Lost packets fall between the data planes and the pods only: lost between the pods too,
//! admissions go uncounted (#421).

use edgerush_cluster::{Time, ms};
use edgerush_cluster_sim::runs::{self, Fault, Run};
use edgerush_cluster_sim::{Beside, Fleet, Link, Shape, Workload};
use edgerush_ratelimit::service::Contract;
use std::num::NonZeroU64;

const SECOND: Time = 1_000_000_000;

/// The data planes of `pooled`'s runs.
const PLANES: usize = 4;

fn contract(limit: u64, burst: u64) -> Contract {
    let n = |n: u64| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(limit),
        window: n(SECOND),
        burst: n(burst),
    }
}

/// Links of a millisecond, give or take half of one.
fn reliable() -> Link {
    Link {
        delay: ms(1),
        jitter: ms(1) / 2,
        ..Link::lan()
    }
}

fn arg(n: usize) -> Option<String> {
    std::env::args().nth(n)
}

fn main() {
    match arg(1).as_deref() {
        Some("pooled") => pooled(),
        Some("two-limits") => two_limits(),
        _ => eprintln!("usage: workloads pooled|two-limits [arguments]"),
    }
}

/// What one configuration did, over every seed.
#[derive(Default, Clone, Copy)]
struct Totals {
    sent: u64,
    refused: u64,
    waited: u64,
    waited_for: Time,
    messages: u64,
    worst_excess: i64,
    excess_sum: i64,
    lost: u64,
    forgiven: u64,
    twice: u64,
    unheld: u64,
    runs: u64,
    never_formed: u64,
}

impl Totals {
    fn add(&mut self, run: Option<Run>) {
        let Some(run) = run else {
            self.never_formed += 1;
            return;
        };
        self.sent += run.sent;
        self.refused += run.refused;
        self.waited += run.waited.0;
        self.waited_for += run.waited.1;
        self.messages += run.messages;
        let excess = run.worst_excess.first().copied().unwrap_or(0);
        self.worst_excess = if self.runs == 0 {
            excess
        } else {
            self.worst_excess.max(excess)
        };
        self.excess_sum += excess;
        self.forgiven += run.forgiven;
        for (admitted, counted) in run.admitted.iter().zip(&run.counted) {
            self.lost += admitted.saturating_sub(*counted);
            self.twice += counted.saturating_sub(*admitted);
        }
        for (admitted, held) in run.admitted.iter().zip(&run.held) {
            self.unheld += admitted.saturating_sub(*held);
        }
        self.runs += 1;
    }
}

/// A case of `pooled`: its name, contract, whether its traffic keeps the contract, the shape
/// on `w` workers a data plane and on one, the fault for each, whether its links lose
/// packets, and a limit beside the run's.
struct Case {
    name: &'static str,
    contract: Contract,
    conforming: bool,
    shapes: fn(&Contract, usize) -> (Shape, Shape),
    faults: fn(Time, usize) -> (Fault, Fault),
    lossy: bool,
    beside: Option<Beside>,
}

fn none(_: Time, _: usize) -> (Fault, Fault) {
    (Fault::None, Fault::None)
}

fn spread_at(c: &Contract, _: usize) -> (Shape, Shape) {
    let s = Shape::Spread {
        per_window: c.limit.get(),
    };
    (s, s)
}

fn spread_thrice(c: &Contract, _: usize) -> (Shape, Shape) {
    let s = Shape::Spread {
        per_window: 3 * c.limit.get(),
    };
    (s, s)
}

/// A whole bucket on every data plane at once, every second: each worker its share.
fn bursts(c: &Contract, w: usize) -> (Shape, Shape) {
    (
        Shape::Bursts {
            each: c.burst.get() / w as u64,
        },
        Shape::Bursts {
            each: c.burst.get(),
        },
    )
}

/// The last data plane's participant held 100 ms, from 50 ms before the client moves.
fn paused(span: Time, _: usize) -> (Fault, Fault) {
    let f = Fault::Pause {
        at: 2 * span / 3 - ms(50),
        length: ms(100),
        plane: PLANES - 1,
    };
    (f, f)
}

/// A client spread over half of the last data plane's workers, then on one, then on a worker
/// of the same data plane it had not reached; on one worker a data plane, on the last
/// throughout.
fn migrating(per_window: u64, w: usize) -> (Shape, Shape) {
    (
        Shape::Migrating {
            per_window,
            last: (w / 2).max(1),
            to: w,
        },
        Shape::Skewed { per_window },
    )
}

/// The same, ending on the last worker of the data plane before: to another data plane.
fn migrating_away(per_window: u64, w: usize) -> (Shape, Shape) {
    (
        Shape::Migrating {
            per_window,
            last: (w / 2).max(1),
            to: w + 1,
        },
        Shape::Migrating {
            per_window,
            last: 1,
            to: 2,
        },
    )
}

#[allow(
    clippy::too_many_lines,
    reason = "the cases, one after another, as a table"
)]
fn cases() -> Vec<Case> {
    let c96 = contract(96, 96);
    vec![
        Case {
            name: "client moving to a worker of its data plane it had not reached, half the limit",
            contract: c96,
            conforming: true,
            shapes: |c, w| migrating(c.limit.get() / 2, w),
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "the same, participant held 100 ms across the move",
            contract: c96,
            conforming: true,
            shapes: |c, w| migrating(c.limit.get() / 2, w),
            faults: paused,
            lossy: false,
            beside: None,
        },
        Case {
            name: "client moving to a worker it had not reached, at the limit",
            contract: c96,
            conforming: true,
            shapes: |c, w| migrating(c.limit.get(), w),
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "the same at the limit, participant held 100 ms across the move",
            contract: c96,
            conforming: true,
            shapes: |c, w| migrating(c.limit.get(), w),
            faults: paused,
            lossy: false,
            beside: None,
        },
        Case {
            name: "client moving to another data plane, at the limit, participant held 100 ms across the move",
            contract: c96,
            conforming: true,
            shapes: |c, w| migrating_away(c.limit.get(), w),
            faults: paused,
            lossy: false,
            beside: None,
        },
        Case {
            name: "tiny limit: 1 a second, a bucket of 1, spread, at the limit",
            contract: contract(1, 1),
            conforming: true,
            shapes: spread_at,
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "small limit: 50 a second (half a request a cycle), a bucket of 5, spread, at the limit",
            contract: contract(50, 5),
            conforming: true,
            shapes: spread_at,
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "small limit: 50 a second, a bucket of 5, spread, 3× the limit",
            contract: contract(50, 5),
            conforming: false,
            shapes: spread_thrice,
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "steady overload, spread, 3× the limit",
            contract: c96,
            conforming: false,
            shapes: spread_thrice,
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "a bucket at once on every data plane, every second",
            contract: c96,
            conforming: false,
            shapes: bursts,
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "a bucket on every data plane over 30 ms, every second",
            contract: c96,
            conforming: false,
            shapes: |c, w| {
                (
                    Shape::SpreadBursts {
                        each: c.burst.get() / w as u64,
                        over: ms(30),
                    },
                    Shape::SpreadBursts {
                        each: c.burst.get(),
                        over: ms(30),
                    },
                )
            },
            faults: none,
            lossy: false,
            beside: None,
        },
        Case {
            name: "bursts, the key's owner lost half way",
            contract: c96,
            conforming: false,
            shapes: bursts,
            faults: |span, _| {
                let f = Fault::OwnerLost { at: span / 2 };
                (f, f)
            },
            lossy: false,
            beside: None,
        },
        Case {
            name: "bursts, the key's owner restarted half way",
            contract: c96,
            conforming: false,
            shapes: bursts,
            faults: |span, _| {
                let f = Fault::OwnerRestarted { at: span / 2 };
                (f, f)
            },
            lossy: false,
            beside: None,
        },
        Case {
            name: "spread at the limit, half the data planes cut off from the store for 4 s",
            contract: c96,
            conforming: true,
            shapes: spread_at,
            faults: |span, _| {
                let f = Fault::PlanesCutOff {
                    at: span / 3,
                    length: 4 * SECOND,
                    planes: PLANES / 2,
                };
                (f, f)
            },
            lossy: false,
            beside: None,
        },
        Case {
            name: "bursts over TCP, one packet in ten lost",
            contract: c96,
            conforming: false,
            shapes: bursts,
            faults: none,
            lossy: true,
            beside: None,
        },
        Case {
            name: "spread at the limit, a data plane restarted half way",
            contract: c96,
            conforming: true,
            shapes: spread_at,
            faults: |span, _| {
                let f = Fault::Restart {
                    at: span / 2,
                    plane: 1,
                };
                (f, f)
            },
            lossy: false,
            beside: None,
        },
        Case {
            name: "spread at the limit, a rolling restart, a data plane a second",
            contract: c96,
            conforming: true,
            shapes: spread_at,
            faults: |span, _| {
                let f = Fault::Rolling {
                    from: span / 4,
                    every: SECOND,
                };
                (f, f)
            },
            lossy: false,
            beside: None,
        },
        Case {
            name: "two routes, each under its own limit and one over the data plane (different owners), bursts",
            contract: c96,
            conforming: false,
            shapes: bursts,
            faults: none,
            lossy: false,
            beside: Some(Beside::Routes(contract(80, 80))),
        },
        Case {
            name: "two routes under their own limits and one shared, spread at the shared limit",
            contract: c96,
            conforming: true,
            shapes: spread_at,
            faults: none,
            lossy: false,
            beside: Some(Beside::Routes(contract(200, 200))),
        },
    ]
}

/// One packet in ten lost, sent again as TCP sends it after 200 ms.
fn tcp() -> Link {
    Link {
        loss_per_million: 100_000,
        retransmit: Some(ms(200)),
        ..reliable()
    }
}

fn pooled() {
    let seeds: u64 = arg(2).and_then(|arg| arg.parse().ok()).unwrap_or(200);
    let w: usize = arg(3).and_then(|arg| arg.parse().ok()).unwrap_or(8).max(1);
    let scheduled = arg(4).as_deref() != Some("every");
    let poll_quiet = arg(5).as_deref() == Some("poll");
    println!(
        "{PLANES} data planes on the store's three pods; runs of ten seconds, {seeds} seeds \
         each; messages take 1 ms, give or take 0.5 ms (TCP rows: one packet in ten lost \
         between the data planes and the pods, sent again after 200 ms); a 10 ms cycle; the \
         participant {}, quiet keys {}. \"{w} workers\": each data plane's workers deciding \
         from the entries they share; \"1 worker\": one a data plane.\n",
        if scheduled {
            "visiting only the keys that need it"
        } else {
            "visiting every key every cycle"
        },
        if poll_quiet { "polled" } else { "not polled" },
    );
    let mut table: Vec<(String, [Totals; 2])> = Vec::new();
    for case in cases() {
        let span = 10 * case.contract.window.get();
        let (many_shape, one_shape) = (case.shapes)(&case.contract, w);
        let (many_fault, one_fault) = (case.faults)(span, w);
        let planes_link = if case.lossy { tcp() } else { reliable() };
        let mut cell = [Totals::default(); 2];
        for (totals, (workers, shape, fault)) in cell
            .iter_mut()
            .zip([(w, many_shape, many_fault), (1, one_shape, one_fault)])
        {
            for seed in 0..seeds {
                let fleet = Fleet {
                    planes: PLANES,
                    workers,
                    contract: case.contract,
                    beside: case.beside,
                    workload: Workload {
                        shape,
                        span,
                        conforming: case.conforming,
                    },
                    seed,
                    scheduled,
                    poll_quiet,
                };
                totals.add(runs::run_over(&fleet, reliable(), planes_link, fault));
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
        println!("| {what} | {w} workers | 1 worker |");
        println!("|---|---|---|");
        for (name, [many, one]) in &table {
            println!("| {name} | {} | {} |", cell(many), cell(one));
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
            t.waited_for as f64 / t.waited.max(1) as f64 / ms(1) as f64
        )
    });
    print("Messages a second", &|t| {
        format!("{:.0}", t.messages as f64 / t.runs.max(1) as f64 / 10.0)
    });
    print(
        "Admitted never counted (of them forgiven), counted twice; not held by the key's owner \
         at the end (lost in a takeover, or forgotten with the key, quiet for its refill time \
         and full again); runs that never formed",
        &|t| {
            format!(
                "{} ({}), {}; {}; {}",
                t.lost, t.forgiven, t.twice, t.unheld, t.never_formed
            )
        },
    );
}

/// Totals over the seeds of one cell of `two-limits`.
#[derive(Default)]
struct Cell {
    runs: u64,
    sent: u64,
    refused: u64,
    admitted: u64,
    one_bucket: u64,
    over: Option<i64>,
    route_over: Option<i64>,
    counted_all: u64,
    admitted_all: u64,
    lost: u64,
    twice: u64,
    unheld: u64,
    never_formed: u64,
}

impl Cell {
    fn add(&mut self, run: Option<Run>, routes: Option<Contract>) {
        let Some(run) = run else {
            self.never_formed += 1;
            return;
        };
        self.runs += 1;
        self.sent += run.sent;
        self.refused += run.refused;
        self.admitted += run.admitted.first().copied().unwrap_or(0);
        self.one_bucket += run.one_bucket;
        self.over = self.over.max(run.worst_excess.first().copied());
        if routes.is_some() {
            // The routes' own keys follow the run's.
            for over in run.worst_excess.iter().skip(1) {
                self.route_over = self.route_over.max(Some(*over));
            }
        }
        for (admitted, counted) in run.admitted.iter().zip(&run.counted) {
            self.admitted_all += admitted;
            self.counted_all += counted;
            self.lost += admitted.saturating_sub(*counted);
            self.twice += counted.saturating_sub(*admitted);
        }
        for (admitted, held) in run.admitted.iter().zip(&run.held) {
            self.unheld += admitted.saturating_sub(*held);
        }
    }

    fn print(&self, routes: bool) {
        let refused = 100.0 * self.refused as f64 / self.sent.max(1) as f64;
        let ratio = self.admitted as f64 / self.one_bucket.max(1) as f64;
        let lost = 100.0 * self.lost as f64 / self.admitted_all.max(1) as f64;
        let twice = 100.0 * self.twice as f64 / self.admitted_all.max(1) as f64;
        let unheld = 100.0 * self.unheld as f64 / self.admitted_all.max(1) as f64;
        let over = self.over.unwrap_or(0);
        // Over a run, on average, how many more than one bucket holding the limit admits.
        let surplus = (self.admitted as f64 - self.one_bucket as f64) / self.runs.max(1) as f64;
        print!(" {refused:.1}% / {ratio:.3}× ({surplus:+.0}) / {over}");
        if routes {
            print!(" / {}", self.route_over.unwrap_or(0));
        }
        print!(" / {lost:.2}% / {twice:.2}% / {unheld:.2}%");
        if self.never_formed > 0 {
            print!(" ({} never formed)", self.never_formed);
        }
        print!(" |");
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the cases, then the table, as the example had them"
)]
fn two_limits() {
    let seeds: u64 = arg(2).and_then(|arg| arg.parse().ok()).unwrap_or(100);
    let fault_name = arg(3).unwrap_or_default();
    let planes: Vec<usize> = arg(4)
        .map(|arg| arg.split(',').filter_map(|n| n.parse().ok()).collect())
        .filter(|planes: &Vec<usize>| !planes.is_empty())
        .unwrap_or_else(|| vec![2, 8]);
    let per_mille: u32 = arg(5).and_then(|arg| arg.parse().ok()).unwrap_or(0);
    let poll_quiet = arg(6).as_deref() == Some("poll");
    let planes_link = Link {
        loss_per_million: per_mille * 1_000,
        retransmit: std::env::var("RETRANSMIT_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
            .map(ms),
        ..reliable()
    };
    let span = 10 * SECOND;
    let fault = |planes: usize| match fault_name.as_str() {
        "owner-lost" => Fault::OwnerLost { at: span / 2 },
        "owner-restart" => Fault::OwnerRestarted { at: span / 2 },
        "restart" => Fault::Restart {
            at: span / 2,
            plane: planes - 1,
        },
        "rolling" => Fault::Rolling {
            from: span * 3 / 10,
            every: span * 4 / 10 / planes as u64,
        },
        "partition" => Fault::PlanesCutOff {
            at: span * 3 / 10,
            length: span * 4 / 10,
            planes: planes / 2,
        },
        _ => Fault::None,
    };
    let scenarios = [
        (
            "20 a second, bucket 20, under 200 a second over the data plane",
            contract(20, 20),
            Beside::Global(contract(200, 200)),
        ),
        (
            "Two routes of 80 a second each under 100 a second over the data plane (both bind)",
            contract(100, 100),
            Beside::Routes(contract(80, 80)),
        ),
        (
            "Two routes of 20 a second each under 200 a second over the data plane (the routes bind)",
            contract(200, 200),
            Beside::Routes(contract(20, 20)),
        ),
    ];
    println!(
        "The store's three pods; runs of ten seconds, {seeds} seeds each; one worker a data \
         plane, keys scheduled, quiet keys {}; messages take 1 ms, give or take 0.5 \
         ms, {per_mille} in 1,000 lost between the data planes and the pods; fault: \
         {fault_name}. Each cell, for the run's limit: share of requests refused / admitted \
         against one bucket holding it, over the whole run (how many more a run, on average) \
         / the most over it in any span, worst seed / (two routes: the most over a route's \
         own limit) / share of admissions some owner never counted / share counted twice / \
         share the key's owner does not hold at the end (lost in a takeover, or forgotten \
         with the key, quiet for its refill time and full again), over every limit.\n",
        if poll_quiet { "polled" } else { "not polled" },
    );
    for (title, limit, beside) in scenarios {
        let routes = match beside {
            Beside::Routes(each) => Some(each),
            Beside::Global(_) => None,
        };
        let (l, b) = (limit.limit.get(), limit.burst.get());
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
                "a whole bucket on every data plane at once",
                Shape::Bursts { each: b },
                false,
            ),
            (
                "a whole bucket on every data plane over 30 ms",
                Shape::SpreadBursts {
                    each: b,
                    over: ms(30),
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
                "a quarter over it, steadily, all on one data plane",
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
        println!("**{title}**\n");
        print!("| Workload | Limits |");
        for &p in &planes {
            print!(" {p} data planes |");
        }
        println!();
        println!("|---|---|{}", "---|".repeat(planes.len()));
        for (name, shape, conforming) in workloads {
            for (rows, under) in [("alone", None), ("both", Some(beside))] {
                print!("| {name} | {rows} |");
                for &p in &planes {
                    let mut cell = Cell::default();
                    for seed in 0..seeds {
                        let fleet = Fleet {
                            planes: p,
                            workers: 1,
                            contract: limit,
                            beside: under,
                            workload: Workload {
                                shape,
                                span,
                                conforming,
                            },
                            seed,
                            scheduled: true,
                            poll_quiet,
                        };
                        let run = runs::run_over(&fleet, reliable(), planes_link, fault(p));
                        cell.add(run, under.and(routes));
                    }
                    cell.print(routes.is_some() && under.is_some());
                }
                println!();
            }
        }
        println!();
    }
}
