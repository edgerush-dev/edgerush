//! Instruction counts for the worker's timers on the request path
//! ([03 §2](../../../docs/03-data-plane.md)): what a connection or an exchange asks of the
//! heap on every poll, with its deadline moving later as a busy one's does, and what the
//! worker's task does when the deadlines of many come at once.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench timers`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::timers::heap::{Heap, Key};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::time::{Duration, Instant};

/// Owners on one worker, as in the macro benchmark's 256 connections over two workers.
const OWNERS: usize = 128;

/// A worker's heap with every owner waiting on a deadline 30 seconds out, and the moment
/// they were set from.
fn waiting() -> (Heap<u64>, Vec<Key>, Instant) {
    let start = Instant::now();
    let mut heap = Heap::default();
    let keys: Vec<Key> = (0..OWNERS).map(|_| heap.add(0)).collect();
    for (at, key) in keys.iter().enumerate() {
        let due = start + Duration::from_secs(30) + Duration::from_micros(at as u64);
        heap.wait(*key, due, || due, |_| {});
    }
    (heap, keys, start)
}

/// The moment an owner waits for, as the heap asks for it. Made here, as [`woken`] and
/// [`counting`] are, not in the benchmarks: a closure written there would put the
/// benchmark's path into the name of the generic it is handed to, which iai-callgrind stops
/// counting in (10 §3).
fn due_at(due: Instant) -> impl FnOnce() -> Instant {
    move || due
}

/// What an owner keeps of being woken: a count.
fn woken(times: &mut u64) {
    *times += 1;
}

/// What the worker's task does with each owner it hands a deadline to: counts it into
/// `handed`.
fn counting(handed: &mut u64) -> impl FnMut(&mut u64) + '_ {
    move |woken| *handed += *woken + 1
}

// Every owner polled four times, its deadline a little later each time: what a busy
// worker's heap is asked, request after request, and never has to queue.
#[library_benchmark]
#[bench::busy(waiting())]
fn waits_moving_later((mut heap, keys, start): (Heap<u64>, Vec<Key>, Instant)) -> Heap<u64> {
    for round in 1..=4_u64 {
        for (at, key) in keys.iter().enumerate() {
            let due = start + Duration::from_secs(30 + round) + Duration::from_micros(at as u64);
            black_box(heap.wait(*key, due, due_at(due), woken));
        }
    }
    heap
}

// Every owner's deadline coming at once: the worker's task handing each over.
#[library_benchmark]
#[bench::all_due(waiting())]
fn expiring((mut heap, _keys, start): (Heap<u64>, Vec<Key>, Instant)) -> (Heap<u64>, u64) {
    let mut handed = 0;
    heap.expire(start + Duration::from_secs(31), counting(&mut handed));
    (heap, black_box(handed))
}

library_benchmark_group!(
    name = timers;
    benchmarks = waits_moving_later, expiring
);
main!(library_benchmark_groups = timers);
