//! Instruction counts for the per-request host lookup, against an index the size of a
//! large cluster: 10 000 exact hosts, 100 wildcards and a claim on every host.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-router`, see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_router::{HostClaim, HostIndex, HostPattern, WildcardLabels};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn claim(pattern: Option<String>, value: u32) -> HostClaim<u32> {
    HostClaim {
        pattern: pattern.map(|text| {
            HostPattern::parse(&text, WildcardLabels::OneOrMore).expect("valid pattern")
        }),
        falls_through: true,
        value,
    }
}

fn large_index() -> HostIndex<u32> {
    let exact = (0..10_000).map(|n| Some(format!("host-{n}.example.com")));
    let wildcards = (0..100).map(|n| Some(format!("*.wild-{n}.example.com")));
    let patterns = exact.chain(wildcards).chain([None]);
    HostIndex::new(
        (0..)
            .zip(patterns)
            .map(|(value, pattern)| claim(pattern, value)),
    )
}

// Building the index happens in the argument expressions, outside the measured function;
// handing the index back keeps dropping it outside as well.
#[library_benchmark]
#[bench::exact_hit(large_index(), "host-5000.example.com")]
#[bench::exact_hit_upper_case(large_index(), "HOST-5000.EXAMPLE.COM")]
#[bench::wildcard_hit(large_index(), "a.wild-50.example.com")]
#[bench::wildcard_hit_three_labels_down(large_index(), "a.b.c.wild-50.example.com")]
#[bench::miss_after_six_suffixes(large_index(), "a.b.c.d.e.nowhere.test")]
fn lookup(index: HostIndex<u32>, host: &str) -> (HostIndex<u32>, usize) {
    let candidates = black_box(&index).lookup(black_box(host)).len();
    (index, candidates)
}

library_benchmark_group!(name = host_index; benchmarks = lookup);
main!(library_benchmark_groups = host_index);
