//! Instruction counts for what a listener that generates IDs asks of every request: its ID
//! written as text, and as a header's value.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-filters`, see the repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_filters::request_id::{LENGTH, RANDOM_BYTES, text, value};
use http::header::HeaderValue;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// 2025-09-29 10:40:00 UTC.
const NOW: u64 = 1_759_142_400_000;
const RANDOM: [u8; RANDOM_BYTES] = [0x3c, 0x91, 0xe4, 0x07, 0x5a, 0xb2, 0x6f, 0x18, 0xd3, 0x40];

#[library_benchmark]
fn as_text() -> [u8; LENGTH] {
    text(black_box(NOW), black_box(RANDOM))
}

#[library_benchmark]
fn as_value() -> HeaderValue {
    value(black_box(NOW), black_box(RANDOM))
}

library_benchmark_group!(name = request_id; benchmarks = as_text, as_value);
main!(library_benchmark_groups = request_id);
