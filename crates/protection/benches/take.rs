//! Instruction counts for what a protected request pays at its pick: a slot taken in its
//! pod's count and given back — reading, as it is given back, whether anyone waits in the
//! backend's queue — and a full pod found by reading alone (25 §3); and a slot given back
//! while a request waits, which calls it (25 §5).
//!
//! Linux only (valgrind): `cargo bench -p edgerush-protection`, see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_protection::{InLine, PodCount, Queue, Slot};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU32;
use std::sync::Arc;

const CAP: NonZeroU32 = NonZeroU32::new(20).expect("not zero");

/// A pod with `held` requests in flight, and their slots.
fn holding(held: u32) -> (Arc<PodCount>, Vec<Slot>) {
    let pod = PodCount::new();
    let slots = (0..held)
        .map(|_| PodCount::take(&pod, CAP).expect("below the cap"))
        .collect();
    (pod, slots)
}

// What was set up is handed back so that dropping it is not measured.
#[library_benchmark]
#[bench::with_room(holding(5))]
fn take_and_give_back(held: (Arc<PodCount>, Vec<Slot>)) -> (Arc<PodCount>, Vec<Slot>) {
    let slot = PodCount::take(black_box(&held.0), black_box(CAP));
    drop(black_box(slot));
    held
}

#[library_benchmark]
#[bench::full(holding(20))]
fn refused_when_full(held: (Arc<PodCount>, Vec<Slot>)) -> (Arc<PodCount>, Vec<Slot>) {
    let slot = PodCount::take(black_box(&held.0), black_box(CAP));
    assert!(black_box(slot).is_none());
    held
}

/// A full pod with a request waiting in its backend's queue, and the pod's slots.
fn waited_on() -> (Arc<PodCount>, Vec<Slot>, InLine) {
    let queue = Queue::new();
    let pod = PodCount::of(&queue);
    let slots = (0..CAP.get())
        .map(|_| PodCount::take(&pod, CAP).expect("below the cap"))
        .collect();
    let in_line = Queue::join(&queue, CAP).expect("the line has room");
    (pod, slots, in_line)
}

#[library_benchmark]
#[bench::one_waiting(waited_on())]
fn give_back_to_the_line(
    held: (Arc<PodCount>, Vec<Slot>, InLine),
) -> (Arc<PodCount>, Vec<Slot>, InLine) {
    let (pod, mut slots, in_line) = held;
    drop(black_box(slots.pop()));
    (pod, slots, in_line)
}

library_benchmark_group!(
    name = pick;
    benchmarks = take_and_give_back, refused_when_full, give_back_to_the_line
);
main!(library_benchmark_groups = pick);
