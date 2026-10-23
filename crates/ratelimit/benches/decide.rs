//! Instruction counts for a data plane's decision on the request path (03 §2: at most
//! 150 ns for an existing key): a warm key, its copy bounded by a cycle, admitted; and
//! refused, its cycle's share spent and its bucket empty; and a request under two and three
//! limits, all warm, admitted. A thousand other keys held, so that the lookup is not a map
//! of one. And a worker meeting a key it holds no handle to, in its process's registry of a
//! hundred thousand: found, made by another worker; and made, cold keys decided locally. And
//! a worker deciding on a key it holds, as the proxy's do: the first request of a cycle, which
//! starts the key's count and notes when, one after it, and one refused, the bucket empty.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench decide`, see the
//! repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::pooled::{self, Entry, Levers, Outcome, Participant, Registry};
use edgerush_ratelimit::service::{Contract, DataPlane, Decision, Keyed, ServicePod, View};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::Arc;

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

/// A data plane, boxed, so that handing it to what is measured copies no more than a
/// pointer, hashing its keys as the proxy's does.
type Plane = Box<DataPlane<Keyed>>;

/// A data plane holding a thousand keys and `KEY`, `ROUTE` and `GLOBAL` warm: asked about,
/// answered, and bounded by a cycle after the answer.
fn warm(burst: u64) -> (Plane, Contract) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, 10 * MS, view.clone());
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    let mut plane: Plane = Box::new(DataPlane::with_hasher(1, 10 * MS, view, hasher));
    let contract = contract(burst);
    let keys = (0..1_000).map(|k| format!("api/0\0key-{k}").into_bytes());
    for key in keys.chain([KEY, ROUTE, GLOBAL].map(<[u8]>::to_vec)) {
        plane.decide(&key, contract, 0);
    }
    for (_, batch) in plane.take_outgoing() {
        let answers = pod.on_batch(batch, 0);
        plane.on_answers(answers, 0);
    }
    plane.take_decided();
    plane.on_cycle(MS);
    plane.take_outgoing();
    (plane, contract)
}

/// `KEY` warm, its share of the cycle spent.
fn spent() -> (Plane, Contract) {
    let (mut plane, contract) = warm(100);
    while plane.decide(KEY, contract, MS) == Decision::Admit {}
    (plane, contract)
}

#[library_benchmark]
#[bench::admitted(warm(100))]
#[bench::refused_over_its_share(spent())]
#[bench::refused_empty(warm(1))]
fn decide(setup: (Plane, Contract)) -> (Decision, Plane) {
    let (mut plane, contract) = setup;
    let decision = plane.decide(black_box(KEY), black_box(contract), black_box(2 * MS));
    // Handed back, so that its keys are freed outside what is measured.
    (decision, plane)
}

#[library_benchmark]
#[bench::two(warm(100), 2)]
#[bench::three(warm(100), 3)]
fn decide_all(setup: (Plane, Contract), limits: usize) -> (Decision, Plane) {
    let (mut plane, contract) = setup;
    let keys = [(KEY, contract), (ROUTE, contract), (GLOBAL, contract)];
    let decision = plane.decide_all(black_box(&keys[..limits]), black_box(2 * MS));
    (decision, plane)
}

/// A process's registry of entries, its keys hashed as the proxy's are.
type Shared = Arc<Registry<Keyed>>;

/// An entry found or made in a registry, with whether it was made.
type Made = Option<(Arc<Entry>, bool)>;

/// A process's registry of a hundred thousand keys, `KEY` among them.
fn registry() -> Shared {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    let participant =
        Participant::new(1, 10 * MS, view, hasher).expect("native 128-bit atomics on Linux");
    let registry = participant.registry();
    let contract = contract(100);
    let keys = (0..100_000).map(|k| format!("api/0\0key-{k}").into_bytes());
    for key in keys.chain([KEY.to_vec()]) {
        let _made = registry.find_or_make(&key, contract, 0, 0);
    }
    registry
}

#[library_benchmark]
#[bench::found(registry())]
fn find(registry: Shared) -> (Option<Arc<Entry>>, Shared) {
    let found = registry.find(black_box(KEY), black_box(contract(100)));
    (found, registry)
}

#[library_benchmark]
#[bench::made(registry())]
fn find_or_make(registry: Shared) -> (Made, Shared) {
    let made = registry.find_or_make(black_box(ROUTE), black_box(contract(100)), 0, MS);
    (made, registry)
}

/// A worker's handle to `KEY`'s entry under a bucket of `burst`, warm: admitted, reported,
/// answered and visited at the next cycle, which leaves its count at none; and `admitted`
/// requests decided on it since, at 21 ms.
fn held(burst: u64, admitted: usize) -> Arc<Entry> {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, 10 * MS, view.clone());
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    let mut participant =
        Participant::new(1, 10 * MS, view, hasher).expect("native 128-bit atomics on Linux");
    participant.set_levers(Levers {
        poll_quiet: false,
        ask_cold: false,
    });
    let contract = contract(burst);
    participant.decide_all(&[(KEY, contract)], 0);
    participant.on_cycle(10 * MS);
    for (_, batch) in participant.take_outgoing() {
        let answers = pod.on_batch(batch, 10 * MS);
        participant.on_answers(answers, 10 * MS);
    }
    participant.on_cycle(20 * MS);
    let entry = participant.entries(&[(KEY, contract)]).remove(0);
    for _ in 0..admitted {
        let _decided = pooled::decide(&[&entry], 21 * MS);
    }
    entry
}

#[library_benchmark]
#[bench::first_of_a_cycle(held(100, 0))]
#[bench::admitted(held(100, 1))]
#[bench::refused_empty(held(1, 1))]
fn worker(entry: Arc<Entry>) -> (Outcome, Arc<Entry>) {
    let outcome = pooled::decide(&[black_box(&*entry)], black_box(22 * MS));
    (outcome, entry)
}

library_benchmark_group!(name = data_plane; benchmarks = decide, decide_all);
library_benchmark_group!(name = registry_of_keys; benchmarks = find, find_or_make);
library_benchmark_group!(name = participant; benchmarks = worker);
main!(
    library_benchmark_groups = data_plane,
    registry_of_keys,
    participant
);
