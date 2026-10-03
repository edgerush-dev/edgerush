//! Instruction counts for keeping a request's body for later
//! ([03 §6](../../../docs/03-data-plane.md)): the retry recording, which keeps it to send
//! again and sends it once more, and a mirror's copy that has fallen behind, which holds it
//! on its queue until it is read. Frames under 4 KiB are copied together into runs; larger
//! ones are kept as they are.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench kept`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use bytes::Bytes;
use edgerush_proxy::kept::Frames;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// A body of `count` frames of `size` bytes each.
fn frames(count: usize, size: usize) -> Frames {
    let frame = Bytes::from(vec![b'x'; size]);
    Frames::new(vec![frame; count])
}

// A small request's body, one frame; 64 KiB in frames of 1 KiB; 64 KiB in frames of 16 KiB,
// which are kept as they are; and 4 KiB in frames of a byte, as one-byte chunks arrive.
#[library_benchmark]
#[bench::small(frames(1, 200))]
#[bench::kib_frames(frames(64, 1024))]
#[bench::large_frames(frames(4, 16 * 1024))]
#[bench::byte_frames(frames(4096, 1))]
fn recorded(body: Frames) -> usize {
    edgerush_proxy::kept::recorded(black_box(body))
}

#[library_benchmark]
#[bench::small(frames(1, 200))]
#[bench::kib_frames(frames(64, 1024))]
#[bench::large_frames(frames(4, 16 * 1024))]
#[bench::byte_frames(frames(4096, 1))]
fn mirrored(body: Frames) -> usize {
    edgerush_proxy::kept::mirrored(black_box(body))
}

library_benchmark_group!(
    name = kept;
    benchmarks = recorded, mirrored
);
main!(library_benchmark_groups = kept);
