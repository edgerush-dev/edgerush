//! Instruction counts for local mode's decision on the request path (03 §2: at most 150 ns
//! for an existing key): a worker deciding on a key it holds a handle to, admitted and
//! refused, among a thousand other keys; under two and three limits; and meeting a key it
//! holds no handle to, in a registry of a hundred thousand: found, and made.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench local`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::local::{Decision, Handles, Local};
use edgerush_ratelimit::service::{Contract, Keyed};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
/// A key as the proxy makes it: the rule's name, a zero, and an API key's value.
const KEY: &[u8] = b"api/0\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
/// The same client's keys under a route's limit and one over the whole data plane.
const ROUTE: &[u8] = b"orders\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
const GLOBAL: &[u8] = b"global\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";

fn contract(burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(burst),
    }
}

fn hasher() -> Keyed {
    Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210])
}

/// The registry and a worker's handles, boxed, so that handing them to what is measured
/// copies no more than a pointer.
type Worker = Box<(Local<Keyed>, Handles<Keyed>)>;

/// A worker holding handles to a thousand keys and `KEY`, `ROUTE` and `GLOBAL`, each asked
/// about once.
fn held(burst: u64) -> (Worker, Contract) {
    let mut worker: Worker = Box::new((Local::new(hasher(), 4), Handles::new(hasher())));
    let contract = contract(burst);
    let keys = (0..1_000).map(|k| format!("api/0\0key-{k}").into_bytes());
    for key in keys.chain([KEY, ROUTE, GLOBAL].map(<[u8]>::to_vec)) {
        let (local, handles) = &mut *worker;
        local.decide(handles, &[(&key, contract)], 0);
    }
    (worker, contract)
}

#[library_benchmark]
#[bench::admitted(held(100))]
#[bench::refused_empty(held(1))]
fn decide(setup: (Worker, Contract)) -> (Decision, Worker) {
    let (mut worker, contract) = setup;
    let (local, handles) = &mut *worker;
    let decision = local.decide(handles, black_box(&[(KEY, contract)]), black_box(MS));
    // Handed back, so that its keys are freed outside what is measured.
    (decision, worker)
}

#[library_benchmark]
#[bench::two(held(100), 2)]
#[bench::three(held(100), 3)]
fn decide_all(setup: (Worker, Contract), limits: usize) -> (Decision, Worker) {
    let (mut worker, contract) = setup;
    let (local, handles) = &mut *worker;
    let keys = [(KEY, contract), (ROUTE, contract), (GLOBAL, contract)];
    let decision = local.decide(handles, black_box(&keys[..limits]), black_box(MS));
    (decision, worker)
}

/// A registry of a hundred thousand keys, `KEY` among them unless said, and a worker with
/// no handles.
fn registry(with_key: bool) -> (Worker, Contract) {
    let mut worker: Worker = Box::new((Local::new(hasher(), 4), Handles::new(hasher())));
    let contract = contract(100);
    {
        let (local, _) = &mut *worker;
        let mut other = Handles::new(hasher());
        let keys = (0..100_000).map(|k| format!("api/0\0key-{k}").into_bytes());
        for key in keys.chain(with_key.then(|| KEY.to_vec())) {
            local.decide(&mut other, &[(&key, contract)], 0);
        }
    }
    (worker, contract)
}

#[library_benchmark]
#[bench::found(registry(true))]
#[bench::made(registry(false))]
fn first_meeting(setup: (Worker, Contract)) -> (Decision, Worker) {
    let (mut worker, contract) = setup;
    let (local, handles) = &mut *worker;
    let decision = local.decide(handles, black_box(&[(KEY, contract)]), black_box(MS));
    (decision, worker)
}

library_benchmark_group!(name = local; benchmarks = decide, decide_all, first_meeting);
main!(library_benchmark_groups = local);
