//! Instruction counts for choosing a rule's backend, once per request.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-config`, see the repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{UpstreamId, WeightedBackends};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn backends(count: usize) -> WeightedBackends {
    WeightedBackends::new((0..count).map(|at| (UpstreamId(at), 10)))
}

// The backends are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::one_backend(backends(1), 0x9E37_79B9_7F4A_7C15)]
#[bench::canary_split(backends(2), 0x9E37_79B9_7F4A_7C15)]
#[bench::sixteen_backends(backends(16), 0x9E37_79B9_7F4A_7C15)]
#[bench::nowhere_to_go(backends(0), 0x9E37_79B9_7F4A_7C15)]
fn pick(backends: WeightedBackends, random: u64) -> (WeightedBackends, Option<UpstreamId>) {
    let picked = black_box(&backends).pick(black_box(random));
    (backends, picked)
}

library_benchmark_group!(name = weighted; benchmarks = pick);
main!(library_benchmark_groups = weighted);
