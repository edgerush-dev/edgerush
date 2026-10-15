//! Instruction counts for a data plane's decision on the request path (03 §2: at most
//! 150 ns for an existing key): a warm key, its copy bounded by a cycle, admitted; and
//! refused, its cycle's share spent and its bucket empty; and a request under two and three
//! limits, all warm, admitted. A thousand other keys held, so that the lookup is not a map
//! of one.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench decide`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::{Contract, DataPlane, Decision, ServicePod, View};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hash::RandomState;
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

/// A data plane, boxed, so that handing it to what is measured copies no more than a
/// pointer.
type Plane = Box<DataPlane<RandomState>>;

/// A data plane holding a thousand keys and `KEY`, `ROUTE` and `GLOBAL` warm: asked about,
/// answered, and bounded by a cycle after the answer.
fn warm(burst: u64) -> (Plane, Contract) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, 10 * MS, view.clone());
    let mut plane: Plane = Box::new(DataPlane::new(1, 10 * MS, view));
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

library_benchmark_group!(name = data_plane; benchmarks = decide, decide_all);
main!(library_benchmark_groups = data_plane);
