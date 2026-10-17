//! Ways for a pod's workers to share one rate-limit view (12 item 3), side by side and
//! narrowed down to that alone — no protocol, no sockets, no proxy:
//!
//! - `none`: no limit, the floor;
//! - `apart`: each worker its own state, held to its part of the contract, as EdgeRush
//!   decides today;
//! - `rooms`: a room a key that workers take from by compare-and-swap, folded into the
//!   process thread's single-threaded state every millisecond, as the shared spike does;
//! - `locked`, `locked-cached`: a lock a key, the whole decision under it, as Redis runs a
//!   script; keys in a sharded map read on every request, or held by each worker once met;
//! - `atomic`, `atomic-cached`: a key's whole state in one word moved by compare-and-swap,
//!   the map as above.
//!
//! Each worker does a request's worth of other work (`--work` µs), then decides one under
//! a key from its stream: one hot key, 100,000 quiet ones drawn evenly, or 100,000 drawn
//! by Zipf (a few take most). Every design but `none` and `apart` has a background thread:
//! the cycle every 10 ms that reports to the service, and for `rooms` the sync and the
//! answers to asks.
//!
//! ```text
//! sharing [--mode cost|accuracy|throttle] [--seconds S] [--threads 1,2,4,8,16]
//!         [--work 0,5,20] [--keys hot,quiet,zipf] [--designs none,apart,...]
//! ```
//!
//! `cost` (the default): ns a request over `none`'s, with room for everything. `accuracy`:
//! one hot key at 1,000,000 a second with a bucket of 1,000, what each admits against the
//! contract. `throttle`: the background thread stopped for 100 ms half way, as a pod over
//! its CPU limit is held, with a share a cycle the load fits in (`--share`): how long
//! decisions took — their tail, and how many waited past a millisecond — and how many were
//! refused. `cost` and `throttle` run each design once untimed first, so that every key has
//! been met.

#![forbid(unsafe_code)]

mod apart;
mod atomic;
mod locked;
mod model;
mod rooms;

use model::{Clock, Contract};
use std::collections::HashMap;
use std::hint::black_box;
use std::process::ExitCode;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// How a design's worker decides.
pub(crate) trait Decide {
    /// Admits or refuses a request under `key` at `now`.
    fn decide(&mut self, key: u64, now: u64) -> bool;

    /// How many times it asked another thread.
    fn asks(&self) -> u64 {
        0
    }
}

/// A design: its workers, and what runs beside them.
pub(crate) trait Design: Sync {
    type Worker<'a>: Decide
    where
        Self: 'a;

    fn worker(&self, index: usize) -> Self::Worker<'_>;

    /// The background thread, until `stop`; held still while `pause` is set.
    fn background(&self, clock: Clock, stop: &AtomicBool, pause: &AtomicBool);
}

/// Sleeps until `when` on `clock`, and on while `pause` is set.
pub(crate) fn sleep_until(clock: Clock, when: u64, pause: &AtomicBool) {
    loop {
        while pause.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        let now = clock.now();
        if now >= when {
            return;
        }
        std::thread::sleep(Duration::from_nanos((when - now).min(1_000_000)));
    }
}

/// Distinct keys of the quiet and Zipf streams.
const KEYS: u64 = 100_000;
/// A worker's stream of keys, gone round again.
const STREAM: usize = 1 << 20;

/// A worker's keys: `kind` drawn with `seed`.
fn stream(kind: &str, seed: u64) -> Result<Vec<u64>, String> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    match kind {
        "hot" => Ok(vec![0; STREAM]),
        "quiet" => Ok((0..STREAM).map(|_| next() % KEYS).collect()),
        "zipf" => {
            // Key k drawn in proportion to 1/(k+1).
            let mut total = 0.0;
            let cumulative: Vec<f64> = (1..=KEYS)
                .map(|k| {
                    total += 1.0 / k as f64;
                    total
                })
                .collect();
            Ok((0..STREAM)
                .map(|_| {
                    let draw = (next() >> 11) as f64 / (1u64 << 53) as f64 * total;
                    cumulative.partition_point(|&sum| sum < draw) as u64
                })
                .collect())
        }
        other => Err(format!("no keys {other}: hot, quiet or zipf")),
    }
}

/// Spins for `steps`: a request's other work.
fn work(steps: u64) {
    let mut x = 0u64;
    for i in 0..steps {
        x = black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(i));
    }
    black_box(x);
}

/// Steps of `work` a microsecond here.
fn calibrate() -> f64 {
    let steps = 50_000_000;
    let start = Instant::now();
    work(steps);
    steps as f64 / (start.elapsed().as_nanos() as f64 / 1_000.0)
}

/// What a run came to.
#[derive(Default)]
struct Outcome {
    requests: u64,
    admitted: u64,
    asks: u64,
    /// Nanoseconds a request, the workers' mean.
    ns: f64,
    /// Nanoseconds each decision took, when timed.
    took: Vec<u32>,
    seconds: f64,
}

/// How a design is run.
#[derive(Clone, Copy)]
struct Plan<'a> {
    threads: usize,
    /// Steps of work a request.
    steps: u64,
    streams: &'a [Vec<u64>],
    seconds: f64,
    /// Each decision timed.
    timed: bool,
    /// The background thread held still for 100 ms half way.
    throttle: bool,
    /// A run as long first, not counted, so that every key has been met.
    warm: bool,
}

/// `design` run as `plan` says, on `clock`.
fn run<D: Design>(design: &D, clock: Clock, plan: Plan<'_>) -> Outcome {
    let Plan {
        threads,
        steps,
        streams,
        seconds,
        timed,
        throttle,
        ..
    } = plan;
    let stop = AtomicBool::new(false);
    let pause = AtomicBool::new(false);
    let started = Barrier::new(threads + 1);
    std::thread::scope(|scope| {
        let background = scope.spawn(|| design.background(clock, &stop, &pause));
        let workers: Vec<_> = (0..threads)
            .map(|index| {
                let keys = &streams[index % streams.len()];
                let (started, stop) = (&started, &stop);
                scope.spawn(move || {
                    let mut worker = design.worker(index);
                    let mut outcome = Outcome::default();
                    started.wait();
                    let began = Instant::now();
                    while !stop.load(Ordering::Relaxed) {
                        work(steps);
                        let key = keys[outcome.requests as usize % keys.len()];
                        let admitted = if timed {
                            let at = Instant::now();
                            let admitted = worker.decide(key, clock.now());
                            outcome
                                .took
                                .push(u32::try_from(at.elapsed().as_nanos()).unwrap_or(u32::MAX));
                            admitted
                        } else {
                            worker.decide(key, clock.now())
                        };
                        outcome.admitted += u64::from(admitted);
                        outcome.requests += 1;
                    }
                    outcome.ns = began.elapsed().as_nanos() as f64 / outcome.requests.max(1) as f64;
                    outcome.asks = worker.asks();
                    outcome
                })
            })
            .collect();
        started.wait();
        let began = Instant::now();
        if throttle {
            std::thread::sleep(Duration::from_secs_f64(seconds / 2.0));
            pause.store(true, Ordering::Release);
            std::thread::sleep(Duration::from_millis(100));
            pause.store(false, Ordering::Release);
            std::thread::sleep(Duration::from_secs_f64(seconds / 2.0));
        } else {
            std::thread::sleep(Duration::from_secs_f64(seconds));
        }
        stop.store(true, Ordering::Release);
        let mut all = Outcome {
            seconds: began.elapsed().as_secs_f64(),
            ..Outcome::default()
        };
        for worker in workers {
            let Ok(outcome) = worker.join() else {
                continue;
            };
            all.requests += outcome.requests;
            all.admitted += outcome.admitted;
            all.asks += outcome.asks;
            all.ns += outcome.ns / threads as f64;
            all.took.extend(outcome.took);
        }
        let _ = background.join();
        all
    })
}

/// `design` run as `plan` says, after a warm-up if it asks for one: the run counted.
fn warmed<D: Design>(design: &D, plan: Plan<'_>) -> Outcome {
    let clock = Clock::start();
    if plan.warm {
        let warm = Plan {
            timed: false,
            throttle: false,
            ..plan
        };
        run(design, clock, warm);
    }
    run(design, clock, plan)
}

/// Runs the design `name` under `contract` as `plan` says; and what its cycles reported
/// to the service, where it reports.
fn design_run(
    name: &str,
    contract: Contract,
    plan: Plan<'_>,
) -> Result<(Outcome, Option<u64>), String> {
    let ways = plan.threads as u64;
    Ok(match name {
        "none" => (warmed(&apart::Nothing, plan), None),
        "apart" => (warmed(&apart::Apart::new(contract, ways), plan), None),
        "rooms" => {
            let design = rooms::Rooms::new(contract);
            let outcome = warmed(&design, plan);
            (outcome, Some(design.reported()))
        }
        "locked" | "locked-cached" => {
            let design = locked::Locked::new(contract, name == "locked-cached");
            let outcome = warmed(&design, plan);
            (outcome, Some(design.reported()))
        }
        "atomic" | "atomic-cached" => {
            let design = atomic::Atomics::new(contract, name == "atomic-cached");
            let outcome = warmed(&design, plan);
            (outcome, Some(design.reported()))
        }
        other => return Err(format!("no design {other}")),
    })
}

/// `--name value` pairs.
fn options(args: &[String]) -> Result<HashMap<String, String>, String> {
    let mut read = HashMap::new();
    for pair in args.chunks(2) {
        let [name, value] = pair else {
            return Err(format!("no value for {}", pair[0]));
        };
        let name = name
            .strip_prefix("--")
            .ok_or(format!("not a flag: {name}"))?;
        read.insert(name.to_owned(), value.clone());
    }
    Ok(read)
}

fn list<T: std::str::FromStr>(value: &str) -> Result<Vec<T>, String> {
    value
        .split(',')
        .map(|item| item.parse().map_err(|_| format!("cannot read {item}")))
        .collect()
}

fn percentile(sorted: &[u32], share: f64) -> f64 {
    let Some(last) = sorted.len().checked_sub(1) else {
        return f64::NAN;
    };
    f64::from(sorted[((share * sorted.len() as f64) as usize).min(last)])
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match go(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sharing: {error}");
            ExitCode::from(2)
        }
    }
}

const DESIGNS: &str = "none,apart,rooms,locked,locked-cached,atomic,atomic-cached";

fn go(args: &[String]) -> Result<(), String> {
    let options = options(args)?;
    let get = |name: &str, default: &str| options.get(name).cloned().unwrap_or(default.to_owned());
    let mode = get("mode", "cost");
    let seconds: f64 = get("seconds", "1")
        .parse()
        .map_err(|_| "--seconds: a number")?;
    let designs: Vec<String> = list(&get("designs", DESIGNS))?;
    let per_us = calibrate();
    println!("# {mode}, {seconds} s a run, {per_us:.0} steps of work a µs");
    match mode.as_str() {
        "cost" => {
            // Room for everything: what is measured is deciding.
            let contract = Contract::new(1_000_000_000, 1_000_000_000, u64::MAX);
            for keys in list::<String>(&get("keys", "hot,quiet,zipf"))? {
                for work_us in list::<f64>(&get("work", "0,5,20"))? {
                    for threads in list::<usize>(&get("threads", "1,2,4,8,16"))? {
                        let streams: Vec<Vec<u64>> = (0..threads as u64)
                            .map(|seed| stream(&keys, seed + 1))
                            .collect::<Result<_, _>>()?;
                        let steps = (work_us * per_us) as u64;
                        let mut floor = None;
                        for name in &designs {
                            let plan = Plan {
                                threads,
                                steps,
                                streams: &streams,
                                seconds,
                                timed: false,
                                throttle: false,
                                warm: true,
                            };
                            let (outcome, _) = design_run(name, contract, plan)?;
                            let over = floor.map_or(String::new(), |floor: f64| {
                                format!(" ({:+.0} ns)", outcome.ns - floor)
                            });
                            if name == "none" {
                                floor = Some(outcome.ns);
                            }
                            println!(
                                "{keys} keys, {work_us} µs, {threads} threads, {name}: {:.0} ns a \
                                 request{over}, {:.0}k a second, asks {}",
                                outcome.ns,
                                outcome.requests as f64 / outcome.seconds / 1_000.0,
                                outcome.asks
                            );
                        }
                    }
                }
            }
        }
        "accuracy" => {
            let (rate, burst) = (1_000_000, 1_000);
            let contract = Contract::new(rate, burst, u64::MAX);
            let work_us: f64 = get("work", "0").parse().map_err(|_| "--work: a number")?;
            for threads in list::<usize>(&get("threads", "1,2,4,8,16"))? {
                let streams = vec![stream("hot", 1)?];
                for name in designs.iter().filter(|name| *name != "none") {
                    let steps = (work_us * per_us) as u64;
                    // No warm-up: the bucket is counted from the start.
                    let plan = Plan {
                        threads,
                        steps,
                        streams: &streams,
                        seconds,
                        timed: false,
                        throttle: false,
                        warm: false,
                    };
                    let (outcome, reported) = design_run(name, contract, plan)?;
                    let allowed = burst as f64 + rate as f64 * outcome.seconds;
                    println!(
                        "hot key at {rate}/s, bucket {burst}, {threads} threads, {name}: admitted \
                         {} of {} offered, allowed {allowed:.0} ({:+.2}%), reported {}, asks {}",
                        outcome.admitted,
                        outcome.requests,
                        (outcome.admitted as f64 / allowed - 1.0) * 100.0,
                        reported.map_or("-".to_owned(), |reported| reported.to_string()),
                        outcome.asks
                    );
                }
            }
        }
        "throttle" => {
            // A share a cycle that the offered load fits in with room to spare (8 threads at
            // 5 µs, some 1.5 M a second, 15 k a cycle): a cycle that does not come shows.
            let share: u64 = get("share", "30000")
                .parse()
                .map_err(|_| "--share: a number")?;
            let contract = Contract::new(1_000_000_000, 1_000_000_000, share);
            let work_us: f64 = get("work", "5").parse().map_err(|_| "--work: a number")?;
            for keys in list::<String>(&get("keys", "hot,quiet"))? {
                for threads in list::<usize>(&get("threads", "8"))? {
                    let streams: Vec<Vec<u64>> = (0..threads as u64)
                        .map(|seed| stream(&keys, seed + 1))
                        .collect::<Result<_, _>>()?;
                    for name in &designs {
                        let steps = (work_us * per_us) as u64;
                        let plan = Plan {
                            threads,
                            steps,
                            streams: &streams,
                            seconds,
                            timed: true,
                            throttle: true,
                            warm: true,
                        };
                        let (mut outcome, _) = design_run(name, contract, plan)?;
                        outcome.took.sort_unstable();
                        let late = outcome
                            .took
                            .iter()
                            .filter(|took| **took > 1_000_000)
                            .count();
                        println!(
                            "{keys} keys, {threads} threads, background held 100 ms, {name}: \
                             decision p50 {:.0} ns, p99 {:.0}, p99.9 {:.0}, most {:.1} ms; {late} \
                             of {} past 1 ms; refused {}; asks {}",
                            percentile(&outcome.took, 0.5),
                            percentile(&outcome.took, 0.99),
                            percentile(&outcome.took, 0.999),
                            percentile(&outcome.took, 1.0) / 1e6,
                            outcome.took.len(),
                            outcome.requests - outcome.admitted,
                            outcome.asks
                        );
                    }
                }
            }
        }
        other => return Err(format!("no mode {other}: cost, accuracy or throttle")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `design` raced by `threads` workers on one key at one instant: what they admitted.
    fn at_once<D: Design>(design: &D, threads: usize) -> u64 {
        let clock = Clock::start();
        let stop = AtomicBool::new(false);
        let pause = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let background = scope.spawn(|| design.background(clock, &stop, &pause));
            let workers: Vec<_> = (0..threads)
                .map(|index| {
                    scope.spawn(move || {
                        let mut worker = design.worker(index);
                        (0..5_000).filter(|_| worker.decide(0, 0)).count() as u64
                    })
                })
                .collect();
            let admitted = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .sum();
            stop.store(true, Ordering::Release);
            background.join().unwrap();
            admitted
        })
    }

    /// One a second, a bucket of 1,000: four workers at once get the bucket and no more,
    /// shared or each its part; the refill over the test's milliseconds is a token at most.
    #[test]
    fn no_design_admits_past_the_bucket() {
        let contract = Contract::new(1, 1_000, u64::MAX);
        let admitted = [
            ("apart", at_once(&apart::Apart::new(contract, 4), 4)),
            ("rooms", at_once(&rooms::Rooms::new(contract), 4)),
            ("locked", at_once(&locked::Locked::new(contract, false), 4)),
            (
                "locked-cached",
                at_once(&locked::Locked::new(contract, true), 4),
            ),
            ("atomic", at_once(&atomic::Atomics::new(contract, false), 4)),
            (
                "atomic-cached",
                at_once(&atomic::Atomics::new(contract, true), 4),
            ),
        ];
        for (name, admitted) in admitted {
            assert!((999..=1_001).contains(&admitted), "{name}: {admitted}");
        }
    }

    /// A share of 100 a cycle holds the shared designs to 100 in a cycle, whatever the
    /// bucket has.
    #[test]
    fn the_cycles_share_bounds_what_is_admitted() {
        let contract = Contract::new(1, 1_000, 100);
        for (name, admitted) in [
            ("locked", at_once(&locked::Locked::new(contract, true), 4)),
            ("atomic", at_once(&atomic::Atomics::new(contract, true), 4)),
        ] {
            // A cycle may pass while the test runs: two shares at most.
            assert!((100..=200).contains(&admitted), "{name}: {admitted}");
        }
    }
}
