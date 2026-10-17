//! What the layered limiter costs in time (12 item 3), beside the instruction counts of
//! `benches/layer.rs`: a worker's decision from its slices, and a data plane's own, holding
//! a thousand other keys; and a sync with 1, 100 and 10,000 keys in use, the process's side
//! and a worker's.
//!
//! `cargo run --release -p edgerush-ratelimit --example layer [millions of decisions]`
//!
//! Nanoseconds a decision and microseconds a sync, the median of five runs.

use edgerush_ratelimit::service::layer::{Local, Process, Slice, Used, Worker};
use edgerush_ratelimit::service::{Contract, DataPlane, Decision, Keyed, ServicePod, View};
use std::hint::black_box;
use std::num::NonZeroU64;
use std::time::Instant;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
/// A key as the proxy makes it: the rule's name, a zero, the definition's digest, and an
/// API key's value.
const KEY: &[u8] = b"api/0\0\x01\x23\x45\x67\x89\xab\xcd\xefc3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
const RUNS: usize = 5;
const SYNCS: u64 = 200;

fn hasher() -> Keyed {
    Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210])
}

/// So much room that nothing measured here runs out of it.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(1_000_000_000),
        window: n(1_000 * MS),
        burst: n(1_000_000_000),
    }
}

fn other(i: usize) -> Box<[u8]> {
    format!("api/0\0digest00key-{i}")
        .into_bytes()
        .into_boxed_slice()
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn slice(key: Box<[u8]>, allowance: u64) -> Slice {
    Slice {
        key,
        contract: contract(),
        allowance,
        refuse_until: None,
    }
}

/// A worker holding a thousand other keys and `KEY` with `decisions` of allowance: ns a
/// decision.
fn worker_decide(decisions: u64) -> f64 {
    let mut worker = Worker::with_hasher(hasher());
    let mut slices: Vec<Slice> = (0..1_000).map(|i| slice(other(i), 1)).collect();
    slices.push(slice(KEY.into(), decisions));
    worker.on_slices(slices);
    let keys = [(KEY, contract())];
    let mut admitted = 0;
    let start = Instant::now();
    for _ in 0..decisions {
        admitted += u64::from(worker.decide(black_box(&keys), black_box(MS)) == Local::Admit);
    }
    let ns = start.elapsed().as_nanos() as f64 / decisions as f64;
    assert_eq!(admitted, decisions, "every decision admits");
    ns
}

/// A data plane holding a thousand other keys and `KEY`, warm: ns a decision.
fn plane_decide(decisions: u64) -> f64 {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view.clone());
    let mut plane = DataPlane::with_hasher(1, CYCLE, view, hasher());
    for key in (0..1_000).map(other).chain([KEY.into()]) {
        plane.decide(&key, contract(), 0);
    }
    for (_, batch) in plane.take_outgoing() {
        let answers = pod.on_batch(batch, 0);
        plane.on_answers(answers, 0);
    }
    plane.take_decided();
    plane.on_cycle(MS);
    plane.take_outgoing();
    let mut admitted = 0;
    let start = Instant::now();
    for _ in 0..decisions {
        let decision = plane.decide(black_box(KEY), black_box(contract()), black_box(2 * MS));
        admitted += u64::from(decision == Decision::Admit);
    }
    let ns = start.elapsed().as_nanos() as f64 / decisions as f64;
    assert_eq!(admitted, decisions, "every decision admits");
    ns
}

/// A process of four workers holding `keys` warm keys, worker 0 syncing every millisecond
/// with each used once: µs a sync, the process's side and the worker's.
fn sync(keys: usize) -> (f64, f64) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view.clone());
    let plane = DataPlane::with_hasher(1, CYCLE, view, hasher());
    let mut process = Process::new(plane, 4, MS, hasher());
    let mut id = 0;
    for i in 0..keys {
        id += 1;
        process.on_ask(0, id, &[(&other(i)[..], contract())], 0);
    }
    for _ in 0..2 {
        for (_, batch) in process.plane().take_outgoing() {
            let answers = pod.on_batch(batch, 0);
            process.plane().on_answers(answers, 0);
        }
        process.take_answers(0);
        process.plane().on_cycle(0);
    }
    let mut worker = Worker::with_hasher(hasher());
    let names: Vec<Box<[u8]>> = (0..keys).map(other).collect();
    worker.on_slices(
        process.on_used(
            0,
            names
                .iter()
                .map(|key| Used {
                    key: key.clone(),
                    contract: contract(),
                    count: 0,
                })
                .collect(),
            MS,
        ),
    );
    let (mut at_process, mut at_worker) = (0, 0);
    for sync in 2..SYNCS + 2 {
        let now = sync * MS;
        for key in &names {
            worker.decide(&[(&key[..], contract())], now - MS / 2);
        }
        let start = Instant::now();
        let used = worker.take_used(now);
        at_worker += start.elapsed().as_nanos();
        let start = Instant::now();
        let slices = process.on_used(0, used, now);
        at_process += start.elapsed().as_nanos();
        let start = Instant::now();
        worker.on_slices(slices);
        at_worker += start.elapsed().as_nanos();
        // The service's side of the cycle, outside what is measured.
        if sync % 10 == 0 {
            process.plane().on_cycle(now);
            for (_, batch) in process.plane().take_outgoing() {
                let answers = pod.on_batch(batch, now);
                process.plane().on_answers(answers, now);
            }
        }
    }
    let per = |total: u128| total as f64 / SYNCS as f64 / 1_000.0;
    (per(at_process), per(at_worker))
}

fn main() {
    let millions: u64 = std::env::args()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .unwrap_or(20);
    let decisions = millions * 1_000_000;
    let worker = median((0..RUNS).map(|_| worker_decide(decisions)).collect());
    let plane = median((0..RUNS).map(|_| plane_decide(decisions)).collect());
    println!("decision, ns: worker from its slice {worker:.1}, data plane's own {plane:.1}");
    for keys in [1, 100, 10_000] {
        let runs: Vec<(f64, f64)> = (0..RUNS).map(|_| sync(keys)).collect();
        let process = median(runs.iter().map(|run| run.0).collect());
        let worker = median(runs.iter().map(|run| run.1).collect());
        println!("sync of {keys} keys, µs: process {process:.2}, worker {worker:.2}");
    }
}
