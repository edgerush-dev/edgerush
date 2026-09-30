//! What per-worker counts give up against counts shared by every worker (03 §6), measured
//! by simulation rather than argued: requests arrive at random, each at a worker, and the
//! worker's balancer sends it to one of a few pods that serve a few at a time and queue the
//! rest. One pod stalls for two seconds halfway through.
//!
//! Deterministic: the same seed gives the same numbers. `cargo test -p edgerush-proxy --lib
//! simulation -- --nocapture` prints them.

use super::{Candidates, RoundRobin, Share, Tried, p2c};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// Gateway workers.
const WORKERS: usize = 8;
/// Pods behind the upstream.
const PODS: usize = 4;
/// Requests a pod serves at once; the rest wait in its queue.
const SERVERS: usize = 4;
/// A request's mean service time, exponentially distributed, in microseconds.
const SERVICE_US: f64 = 10_000.0;
/// How long a run lasts, in microseconds.
const RUN_US: u64 = 60_000_000;
/// When the stalled pod stops and starts again: nothing it holds moves in between.
const STALL_US: (u64, u64) = (30_000_000, 32_000_000);
/// The pod that stalls.
const STALLED: usize = 0;

/// How the workers balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Balancer {
    Random,
    RoundRobin,
    /// Counts per worker, as built.
    P2cPerWorker,
    /// Counts shared by every worker, as Envoy and HAProxy keep them.
    P2cShared,
}

/// What a run found.
#[derive(Debug, Clone, Copy)]
struct Outcome {
    mean_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    /// Requests sent to the stalled pod while it was stalled.
    into_stall: usize,
    /// Requests sent anywhere while it was.
    during_stall: usize,
}

/// A small generator of the simulations' own: the same seed, the same run.
pub(super) struct Random(pub(super) u64);

impl Random {
    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mixed = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    /// Exponentially distributed, of mean `mean`.
    pub(super) fn exponential(&mut self, mean: f64) -> f64 {
        // Uniform over (0, 1]: never zero, whose logarithm has no end.
        let uniform = ((self.next() >> 11) + 1) as f64 / (1_u64 << 53) as f64;
        -uniform.ln() * mean
    }
}

/// Pods as one worker's balancer sees them: its own counts, or everyone's.
struct View<'a> {
    in_flight: &'a [u32],
}

impl Candidates for View<'_> {
    fn count(&self) -> usize {
        self.in_flight.len()
    }
    fn serves(&self, _: usize) -> bool {
        true
    }
    fn in_flight(&self, at: usize) -> u32 {
        self.in_flight[at]
    }
    fn share(&self, _: usize) -> Share {
        Share::FULL
    }
}

/// When a request that starts at `start` and needs `work` of service ends at the pod `pod`:
/// the stalled pod makes no progress while it is stalled.
fn finish(pod: usize, start: u64, work: u64) -> u64 {
    let (from, to) = STALL_US;
    if pod != STALLED || start + work <= from || start >= to {
        return start + work;
    }
    if start < from {
        to + (work - (from - start))
    } else {
        to + work
    }
}

/// One run at `load`, the fraction of the pods' capacity asked of them.
fn run(balancer: Balancer, load: f64, seed: u64) -> Outcome {
    let mut random = Random(seed);
    let rate_per_us = load * (PODS * SERVERS) as f64 / SERVICE_US;
    let mut per_worker = vec![vec![0_u32; PODS]; WORKERS];
    let mut shared = vec![0_u32; PODS];
    let mut round_robin: Vec<RoundRobin> = (0..WORKERS)
        .map(|_| RoundRobin::starting_at(random.next() as usize))
        .collect();
    let mut busy = [0_usize; PODS];
    // A pod's queue: when each request came, which worker sent it, and its work.
    let mut queues: Vec<VecDeque<(u64, usize, u64)>> = vec![VecDeque::new(); PODS];
    // Ends of service: when, at which pod, which worker sent it, when it came.
    let mut ends: BinaryHeap<Reverse<(u64, usize, usize, u64)>> = BinaryHeap::new();
    let mut latencies = Vec::new();
    let (mut into_stall, mut during_stall) = (0, 0);

    let mut now = random.exponential(1.0 / rate_per_us) as u64;
    while now < RUN_US {
        // Every service that ends before this arrival ends first.
        while let Some(&Reverse((end, pod, worker, came))) = ends.peek() {
            if end > now {
                break;
            }
            ends.pop();
            latencies.push(end - came);
            per_worker[worker][pod] -= 1;
            shared[pod] -= 1;
            match queues[pod].pop_front() {
                Some((came, worker, work)) => {
                    ends.push(Reverse((finish(pod, end, work), pod, worker, came)))
                }
                None => busy[pod] -= 1,
            }
        }
        let worker = (random.next() % WORKERS as u64) as usize;
        let mut draw = || random.next();
        let pod = match balancer {
            Balancer::Random => (draw() % PODS as u64) as usize,
            Balancer::RoundRobin => round_robin[worker]
                .pick(&View { in_flight: &shared }, &Tried::default(), &mut draw)
                .unwrap(),
            Balancer::P2cPerWorker => p2c(
                &View {
                    in_flight: &per_worker[worker],
                },
                &Tried::default(),
                &mut draw,
            )
            .unwrap(),
            Balancer::P2cShared => {
                p2c(&View { in_flight: &shared }, &Tried::default(), &mut draw).unwrap()
            }
        };
        if (STALL_US.0..STALL_US.1).contains(&now) {
            during_stall += 1;
            if pod == STALLED {
                into_stall += 1;
            }
        }
        per_worker[worker][pod] += 1;
        shared[pod] += 1;
        let work = random.exponential(SERVICE_US) as u64;
        if busy[pod] < SERVERS {
            busy[pod] += 1;
            ends.push(Reverse((finish(pod, now, work), pod, worker, now)));
        } else {
            queues[pod].push_back((now, worker, work));
        }
        now += random.exponential(1.0 / rate_per_us) as u64;
    }

    latencies.sort_unstable();
    let at = |fraction: f64| {
        latencies[((latencies.len() as f64 * fraction) as usize).min(latencies.len() - 1)] as f64
            / 1_000.0
    };
    Outcome {
        mean_ms: latencies.iter().sum::<u64>() as f64 / latencies.len() as f64 / 1_000.0,
        p99_ms: at(0.99),
        p999_ms: at(0.999),
        into_stall,
        during_stall,
    }
}

fn table(load: f64) -> Vec<(Balancer, Outcome)> {
    let balancers = [
        Balancer::Random,
        Balancer::RoundRobin,
        Balancer::P2cPerWorker,
        Balancer::P2cShared,
    ];
    println!(
        "\nload {:.0}% of capacity; {WORKERS} workers, {PODS} pods of {SERVERS}, {SERVICE_US} µs mean service; pod {STALLED} stalls 2 s",
        load * 100.0
    );
    println!(
        "{:<14} {:>9} {:>9} {:>10} {:>16}",
        "balancer", "mean ms", "p99 ms", "p99.9 ms", "into the stall"
    );
    balancers
        .iter()
        .map(|&balancer| {
            let outcome = run(balancer, load, 0x5EED);
            println!(
                "{:<14} {:>9.2} {:>9.2} {:>10.2} {:>9} of {:>5}",
                format!("{balancer:?}"),
                outcome.mean_ms,
                outcome.p99_ms,
                outcome.p999_ms,
                outcome.into_stall,
                outcome.during_stall
            );
            (balancer, outcome)
        })
        .collect()
}

fn of(outcomes: &[(Balancer, Outcome)], balancer: Balancer) -> Outcome {
    outcomes.iter().find(|(b, _)| *b == balancer).unwrap().1
}

#[test]
fn per_worker_counts_steer_away_from_a_stall_at_light_and_heavy_load() {
    for load in [0.2, 0.85] {
        let outcomes = table(load);
        let random = of(&outcomes, Balancer::Random);
        let per_worker = of(&outcomes, Balancer::P2cPerWorker);
        // Random sends a stalled pod its fair share, a quarter; per-worker P2C, once each
        // worker has a few requests stuck there, less than a quarter of that. Near capacity
        // the other pods' counts are high too, so the stalled one still wins some pairs early
        // in the stall — with shared counts as well.
        let fair = per_worker.during_stall / PODS;
        assert!(
            per_worker.into_stall * 4 < fair,
            "{per_worker:?} against {random:?}"
        );
        assert!(
            per_worker.p999_ms < random.p999_ms,
            "{per_worker:?} against {random:?}"
        );
    }
}

#[test]
fn per_worker_counts_stay_near_shared_ones() {
    for load in [0.2, 0.85] {
        let outcomes = table(load);
        let per_worker = of(&outcomes, Balancer::P2cPerWorker);
        let shared = of(&outcomes, Balancer::P2cShared);
        let random = of(&outcomes, Balancer::Random);
        // Shared counts see more and should do better; per-worker ones should still beat
        // random by far more than they trail shared ones.
        assert!(
            per_worker.p99_ms <= random.p99_ms,
            "{per_worker:?} against {random:?}"
        );
        assert!(
            per_worker.mean_ms <= shared.mean_ms * 1.25,
            "{per_worker:?} against {shared:?}"
        );
    }
}
