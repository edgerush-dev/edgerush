//! What the shared limiter costs in time (12 item 3, a spike), beside the instruction counts
//! of `benches/shared.rs`:
//!
//! - the process's sync with 1,000 to 1,000,000 rooms held, none or 100 of them taken from
//!   since the last: how its scan grows with the keys held;
//! - one hot key taken from by 1 to 16 threads at once, each doing a request's worth of
//!   other work between its takes, with the process syncing every millisecond and its data
//!   plane talking to a service pod every cycle, as the proxy's process thread does: what a
//!   take costs a thread over the same work counted on a counter of its own, as each worker
//!   apart does.
//!
//! `cargo run --release -p edgerush-ratelimit --example shared [seconds a run]`
//!
//! Microseconds a sync, the median of five; nanoseconds a take over the thread's own
//! counter, the median of three runs.

use edgerush_ratelimit::service::layer::Local;
use edgerush_ratelimit::service::shared::{Handed, Process, Worker};
use edgerush_ratelimit::service::{Contract, DataPlane, Keyed, ServicePod, View};
use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
const RUNS: usize = 5;

fn hasher() -> Keyed {
    Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210])
}

/// A billion a second, a bucket as big: room that nothing measured here runs out of.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(1_000_000_000),
        window: n(1_000 * MS),
        burst: n(1_000_000_000),
    }
}

fn key(i: usize) -> Box<[u8]> {
    format!("api/0\0digest00key-{i}")
        .into_bytes()
        .into_boxed_slice()
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

/// A process and its service pod with `keys` warm at `now`, and their rooms.
fn warm(keys: usize, now: u64) -> (Process<Keyed>, ServicePod, Vec<Handed>) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view.clone());
    let plane = DataPlane::with_hasher(1, CYCLE, view, hasher());
    let mut process = Process::new(plane, hasher());
    let names: Vec<Box<[u8]>> = (0..keys).map(key).collect();
    for (id, name) in (0..).zip(&names) {
        process.on_ask(0, id, &[(&name[..], contract())], now);
    }
    exchange(&mut process, &mut pod, now);
    process.take_answers(now);
    process.plane().on_cycle(now);
    exchange(&mut process, &mut pod, now);
    let mut rooms = Vec::with_capacity(keys);
    for (id, name) in (1u64 << 32..).zip(&names) {
        if let Some(answer) = process.on_ask(0, id, &[(&name[..], contract())], now) {
            rooms.extend(answer.rooms);
        }
    }
    (process, pod, rooms)
}

/// What the data plane has to send, answered by `pod`, until nothing is left.
fn exchange(process: &mut Process<Keyed>, pod: &mut ServicePod, now: u64) {
    for _ in 0..10 {
        let outgoing = process.plane().take_outgoing();
        if outgoing.is_empty() {
            return;
        }
        for (_, batch) in outgoing {
            let answers = pod.on_batch(batch, now);
            process.plane().on_answers(answers, now);
        }
    }
}

/// µs a sync with `held` rooms, `taken` of them taken from once since the last.
fn sync(held: usize, taken: usize) -> f64 {
    let (mut process, _pod, rooms) = warm(held, 0);
    let mut worker = Worker::with_hasher(hasher());
    worker.on_rooms(rooms);
    let names: Vec<Box<[u8]>> = (0..taken.min(held)).map(key).collect();
    process.on_sync(MS);
    let mut samples = Vec::with_capacity(RUNS);
    for run in 0..RUNS as u64 {
        let now = (run + 2) * MS;
        for name in &names {
            worker.decide(&[(&name[..], contract())], now);
        }
        let start = Instant::now();
        process.on_sync(now);
        samples.push(start.elapsed().as_nanos() as f64 / 1_000.0);
    }
    median(samples)
}

/// Spins for about `iterations` steps: a request's other work.
fn work(iterations: u64) {
    let mut x = 0u64;
    for i in 0..iterations {
        x = black_box(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(i));
    }
    black_box(x);
}

/// Steps of `work` a microsecond, as measured here.
fn calibrate() -> f64 {
    let steps = 50_000_000;
    let start = Instant::now();
    work(steps);
    steps as f64 / (start.elapsed().as_nanos() as f64 / 1_000.0)
}

/// `threads` threads for `seconds`, each a request after another of `steps` of work and a
/// decision: from one shared room with a process thread syncing it, if `shared`, or from a
/// counter of the thread's own. Nanoseconds a request, the threads' mean, and asks.
fn hot(threads: usize, steps: u64, seconds: f64, shared: bool) -> (f64, u64) {
    let (process, pod, rooms) = warm(1, 0);
    let stop = AtomicBool::new(false);
    // The workers, the process thread, and this one.
    let start = Barrier::new(threads + 2);
    let epoch = Instant::now();
    let now = || u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let name = key(0);
    let keys = [(&name[..], contract())];
    std::thread::scope(|scope| {
        // The process thread: a sync every millisecond, a cycle with the pod every ten.
        let syncing = scope.spawn(|| {
            let (mut process, mut pod) = (process, pod);
            start.wait();
            let mut cycle = 0;
            while !stop.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
                let at = now();
                if shared {
                    process.on_sync(at);
                }
                cycle += 1;
                if cycle % 10 == 0 {
                    process.plane().on_cycle(at);
                    exchange(&mut process, &mut pod, at);
                }
            }
        });
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let rooms = rooms.clone();
                let (start, stop, keys) = (&start, &stop, &keys);
                scope.spawn(move || {
                    let mut worker = Worker::with_hasher(hasher());
                    worker.on_rooms(rooms);
                    let mut own = u64::MAX;
                    let (mut requests, mut asks) = (0u64, 0u64);
                    start.wait();
                    let began = Instant::now();
                    while !stop.load(Ordering::Relaxed) {
                        work(steps);
                        if shared {
                            if worker.decide(keys, now()) != Local::Admit {
                                asks += 1;
                            }
                        } else {
                            own = black_box(own.wrapping_sub(u64::from(black_box(now()) > 0)));
                        }
                        requests += 1;
                    }
                    (began.elapsed().as_nanos() as f64 / requests as f64, asks)
                })
            })
            .collect();
        start.wait();
        std::thread::sleep(Duration::from_secs_f64(seconds));
        stop.store(true, Ordering::Release);
        let results: Vec<(f64, u64)> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap_or((f64::NAN, 0)))
            .collect();
        let _ = syncing.join();
        let ns = results.iter().map(|(ns, _)| ns).sum::<f64>() / threads as f64;
        (ns, results.iter().map(|(_, asks)| asks).sum())
    })
}

fn main() {
    let seconds: f64 = std::env::args()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .unwrap_or(2.0);
    for held in [1_000, 10_000, 100_000, 1_000_000] {
        println!(
            "sync with {held} rooms held, µs: none taken {:.1}, 100 taken {:.1}",
            sync(held, 0),
            sync(held, 100)
        );
    }
    let per_us = calibrate();
    println!("work: {per_us:.0} steps a µs");
    for work_us in [0.0, 5.0, 20.0] {
        let steps = (work_us * per_us) as u64;
        for threads in [1, 2, 4, 8, 12, 16] {
            let runs: Vec<((f64, u64), (f64, u64))> = (0..3)
                .map(|_| {
                    (
                        hot(threads, steps, seconds, true),
                        hot(threads, steps, seconds, false),
                    )
                })
                .collect();
            let shared = median(runs.iter().map(|(shared, _)| shared.0).collect());
            let own = median(runs.iter().map(|(_, own)| own.0).collect());
            let asks: u64 = runs.iter().map(|(shared, _)| shared.1).sum();
            println!(
                "hot key, {work_us} µs of work, {threads} threads: ns a request {shared:.0} shared, \
                 {own:.0} own; a take costs {:.0} ns more; asks {asks}",
                shared - own
            );
        }
    }
}
