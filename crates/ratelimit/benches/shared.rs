//! Instruction counts for the shared limiter (12 item 3, a spike): a worker's decision
//! from the rooms it holds, every key's one room, against a data plane's own (`decide`)
//! and a layered worker's (`layer`); and the process's sync with 1, 100 and 10,000 rooms
//! taken from since the last, a thousand others held untouched beside them.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench shared`, see the
//! repository README. One thread: what other workers taking from the same room cost is
//! the contention example's (`examples/contention.rs`) and the proxy's saturation.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::layer::Local;
use edgerush_ratelimit::service::shared::{Handed, Process, Worker};
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

/// A million a second, a bucket as big: room that nothing measured here runs out of.
fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(1_000_000),
        window: n(1_000 * MS),
        burst: n(1_000_000),
    }
}

/// The `i`th of the other keys.
fn other(i: usize) -> Box<[u8]> {
    format!("api/0\0digest00key-{i}")
        .into_bytes()
        .into_boxed_slice()
}

/// A process holding `keys` warm, each asked about, answered and folded; and their rooms.
fn process(keys: &[Box<[u8]>]) -> (Box<Process<Keyed>>, Vec<Handed>) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, CYCLE, view.clone());
    let plane = DataPlane::with_hasher(1, CYCLE, view, hasher());
    let mut process = Box::new(Process::new(plane, hasher()));
    for (id, key) in (0..).zip(keys) {
        process.on_ask(0, id, &[(&key[..], contract())], 0);
    }
    for _ in 0..2 {
        for (_, batch) in process.plane().take_outgoing() {
            let answers = pod.on_batch(batch, 0);
            process.plane().on_answers(answers, 0);
        }
        process.take_answers(0);
        process.plane().on_cycle(0);
    }
    let mut rooms = Vec::with_capacity(keys.len());
    for (id, key) in (1_000_000..).zip(keys) {
        if let Some(answer) = process.on_ask(0, id, &[(&key[..], contract())], MS) {
            rooms.extend(answer.rooms);
        }
    }
    (process, rooms)
}

/// A worker, boxed so that handing it over copies a pointer, holding the rooms of `KEY`,
/// `ROUTE`, `GLOBAL` and a thousand other keys.
fn holding() -> (Box<Worker<Keyed>>, Box<Process<Keyed>>) {
    let mut keys: Vec<Box<[u8]>> = (0..1_000).map(other).collect();
    keys.extend([KEY, ROUTE, GLOBAL].map(Box::from));
    let (process, rooms) = process(&keys);
    let mut worker = Box::new(Worker::with_hasher(hasher()));
    worker.on_rooms(rooms);
    (worker, process)
}

#[library_benchmark]
#[bench::admitted(holding(), 1)]
#[bench::two(holding(), 2)]
#[bench::three(holding(), 3)]
fn worker_decide(
    setup: (Box<Worker<Keyed>>, Box<Process<Keyed>>),
    limits: usize,
) -> (Local, Box<Worker<Keyed>>, Box<Process<Keyed>>) {
    let (mut worker, process) = setup;
    let keys = [(KEY, contract()), (ROUTE, contract()), (GLOBAL, contract())];
    let local = worker.decide(black_box(&keys[..limits]), black_box(2 * MS));
    // Handed back, so that its rooms are freed outside what is measured.
    (local, worker, process)
}

/// A process holding a thousand rooms untouched and `taken` taken from once each.
fn taken(taken: usize) -> Box<Process<Keyed>> {
    let keys: Vec<Box<[u8]>> = (0..1_000 + taken).map(other).collect();
    let (mut process, rooms) = process(&keys);
    // A first sync, so that what is measured is one in a run of them.
    process.on_sync(2 * MS);
    let mut worker = Worker::with_hasher(hasher());
    worker.on_rooms(rooms);
    for key in &keys[1_000..] {
        worker.decide(&[(&key[..], contract())], 2 * MS);
    }
    process
}

#[library_benchmark]
#[bench::one(taken(1))]
#[bench::hundred(taken(100))]
#[bench::ten_thousand(taken(10_000))]
fn process_sync(process: Box<Process<Keyed>>) -> Box<Process<Keyed>> {
    let mut process = process;
    process.on_sync(black_box(3 * MS));
    process
}

library_benchmark_group!(name = shared; benchmarks = worker_decide, process_sync);
main!(library_benchmark_groups = shared);
