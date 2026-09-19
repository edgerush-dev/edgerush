//! Instruction counts for what a request pays for being counted: finding its thread's
//! shard, a few counters and one observation in a histogram.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-telemetry`, see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_telemetry::{Counter, Gauge, Histogram, Sharded};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroUsize;

/// Request durations in nanoseconds, from half a millisecond to ten seconds.
const BOUNDS: [u64; 14] = [
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
];

#[derive(Debug, Default)]
struct Group {
    requests: Counter,
    responses: [Counter; 5],
    active: Gauge,
    duration: Histogram<14>,
}

fn group() -> Sharded<Group> {
    Sharded::new(NonZeroUsize::new(8).expect("not zero"))
}

// The group is handed back so that dropping it is not measured.
#[library_benchmark]
#[bench::a_request(group(), 3_400_000)]
#[bench::a_slow_request(group(), 60_000_000_000)]
fn count_a_request(group: Sharded<Group>, nanoseconds: u64) -> Sharded<Group> {
    let shard = black_box(&group).local();
    shard.requests.inc();
    shard.active.inc();
    if let Some(class) = shard.responses.get(black_box(1)) {
        class.inc();
    }
    shard.duration.observe(&BOUNDS, black_box(nanoseconds));
    shard.active.dec();
    group
}

#[library_benchmark]
#[bench::one_counter(group())]
fn count_once(group: Sharded<Group>) -> Sharded<Group> {
    black_box(&group).local().requests.inc();
    group
}

library_benchmark_group!(name = counting; benchmarks = count_a_request, count_once);
main!(library_benchmark_groups = counting);
