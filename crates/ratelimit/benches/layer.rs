//! Instruction counts for the layered limiter (12 item 3): a worker's decision from its
//! slices on the request path, against a data plane's own (`decide`); and a sync with 1,
//! 100 and 10,000 keys in use, the worker's side — what it used, then its new slices — and
//! the process's, which charges what was used and works out each key's slice.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench layer`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::layer::{Local, Process, Slice, Used, Worker};
use edgerush_ratelimit::service::{Contract, DataPlane, Keyed, ServicePod, View};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
/// A key as the proxy makes it: the rule's name, a zero, the definition's digest, and an
/// API key's value.
const KEY: &[u8] = b"api/0\0\x01\x23\x45\x67\x89\xab\xcd\xefc3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
const ROUTE: &[u8] =
    b"orders\0\x01\x23\x45\x67\x89\xab\xcd\xefc3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
const GLOBAL: &[u8] =
    b"global\0\x01\x23\x45\x67\x89\xab\xcd\xefc3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";

fn hasher() -> Keyed {
    Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210])
}

/// A million a second, a bucket as big: room that no sync measured here runs out of.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(1_000_000),
        window: n(1_000 * MS),
        burst: n(1_000_000),
    }
}

/// The `i`th of the other keys held.
fn other(i: usize) -> Box<[u8]> {
    format!("api/0\0digest00key-{i}")
        .into_bytes()
        .into_boxed_slice()
}

/// A worker, boxed so that handing it over copies a pointer, holding a slice of `KEY`,
/// `ROUTE`, `GLOBAL` and a thousand other keys; `KEY` refused until later if `refused`.
fn holding(refused: bool) -> Box<Worker<Keyed>> {
    let mut worker = Box::new(Worker::with_hasher(hasher()));
    let slice = |key: Box<[u8]>| Slice {
        key,
        contract: contract(),
        allowance: 100,
        refuse_until: None,
    };
    let mut slices: Vec<Slice> = (0..1_000).map(|i| slice(other(i))).collect();
    slices.extend([KEY, ROUTE, GLOBAL].map(|key| slice(key.into())));
    if refused {
        slices[1_000].allowance = 0;
        slices[1_000].refuse_until = Some(10 * MS);
    }
    worker.on_slices(slices);
    worker
}

#[library_benchmark]
#[bench::admitted(holding(false), 1)]
#[bench::refused(holding(true), 1)]
#[bench::two(holding(false), 2)]
#[bench::three(holding(false), 3)]
fn worker_decide(worker: Box<Worker<Keyed>>, limits: usize) -> (Local, Box<Worker<Keyed>>) {
    let mut worker = worker;
    let keys = [(KEY, contract()), (ROUTE, contract()), (GLOBAL, contract())];
    let local = worker.decide(black_box(&keys[..limits]), black_box(MS));
    // Handed back, so that its keys are freed outside what is measured.
    (local, worker)
}

/// A process of four workers holding `keys` warm keys, and what worker 0 used of each since
/// its last sync, one request a key.
fn process(keys: usize) -> (Box<Process<Keyed>>, Vec<Used>) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view.clone());
    let plane = DataPlane::with_hasher(1, CYCLE, view, hasher());
    let mut process = Box::new(Process::new(plane, 4, MS, hasher()));
    for i in 0..keys {
        let key = other(i);
        process.on_ask(0, i as u64, &[(&key[..], contract())], 0);
    }
    for _ in 0..2 {
        for (_, batch) in process.plane().take_outgoing() {
            let answers = pod.on_batch(batch, 0);
            process.plane().on_answers(answers, 0);
        }
        process.take_answers(0);
        process.plane().on_cycle(0);
    }
    let used = |count| -> Vec<Used> {
        (0..keys)
            .map(|i| Used {
                key: other(i),
                contract: contract(),
                count,
            })
            .collect()
    };
    // A first sync, so that what is measured is one in a run of them.
    process.on_used(0, used(0), MS);
    (process, used(1))
}

#[library_benchmark]
#[bench::one(process(1))]
#[bench::hundred(process(100))]
#[bench::ten_thousand(process(10_000))]
fn process_sync(setup: (Box<Process<Keyed>>, Vec<Used>)) -> (Vec<Slice>, Box<Process<Keyed>>) {
    let (mut process, used) = setup;
    let slices = process.on_used(black_box(0), black_box(used), black_box(2 * MS));
    (slices, process)
}

/// A worker holding a slice of `keys` keys, each used once since its last sync, and its
/// next slices.
fn worker(keys: usize) -> (Box<Worker<Keyed>>, Vec<Slice>) {
    let mut worker = Box::new(Worker::with_hasher(hasher()));
    let slices = || -> Vec<Slice> {
        (0..keys)
            .map(|i| Slice {
                key: other(i),
                contract: contract(),
                allowance: 100,
                refuse_until: None,
            })
            .collect()
    };
    worker.on_slices(slices());
    for i in 0..keys {
        let key = other(i);
        worker.decide(&[(&key[..], contract())], MS);
    }
    (worker, slices())
}

#[library_benchmark]
#[bench::one(worker(1))]
#[bench::hundred(worker(100))]
#[bench::ten_thousand(worker(10_000))]
fn worker_sync(setup: (Box<Worker<Keyed>>, Vec<Slice>)) -> (Vec<Used>, Box<Worker<Keyed>>) {
    let (mut worker, slices) = setup;
    let used = worker.take_used(black_box(2 * MS));
    worker.on_slices(black_box(slices));
    (used, worker)
}

library_benchmark_group!(name = layer; benchmarks = worker_decide, process_sync, worker_sync);
main!(library_benchmark_groups = layer);
