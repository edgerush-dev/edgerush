//! Instruction counts for taking and giving back a worker's places, by upstream
//! ([03 §9](../../../docs/03-data-plane.md)), against the one count it took before: every
//! exchange takes a place and gives it back, so this is on every request's path.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench places`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::places::{Place, Places};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::cell::Cell;
use std::hint::black_box;
use std::rc::Rc;

/// Exchanges taken and given back, one after another.
const EXCHANGES: usize = 100;

/// Upstreams the config has, and the exchanges go to in turn.
const UPSTREAMS: usize = 8;

/// Whether the config has no upstream but the one asked for: it has eight.
const ALONE: bool = false;

/// A worker's places, with `held` of them held, spread over the upstreams.
fn holding(held: usize) -> (Rc<Places>, Vec<Place>) {
    // As many slots as the data plane's metrics have.
    let places = Places::new(1024, 4096);
    let taken = (0..held)
        .filter_map(|at| places.take(at % UPSTREAMS, ALONE).ok())
        .collect();
    (places, taken)
}

// The places are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::with_room(holding(0))]
#[bench::short_of_places(holding(900))]
fn take_and_give_back(worker: (Rc<Places>, Vec<Place>)) -> ((Rc<Places>, Vec<Place>), usize) {
    let mut taken = 0;
    for exchange in 0..EXCHANGES {
        let place = worker.0.take(black_box(exchange % UPSTREAMS), ALONE);
        taken += usize::from(black_box(&place).is_ok());
    }
    (worker, taken)
}

// The count alone, as every exchange took its place before.
#[library_benchmark]
#[bench::with_room(Rc::new(Cell::new(0)))]
fn one_count(held: Rc<Cell<usize>>) -> (Rc<Cell<usize>>, usize) {
    let mut taken = 0;
    for _ in 0..EXCHANGES {
        let in_hand = held.get();
        if in_hand < black_box(1024) {
            held.set(in_hand + 1);
            let place = Rc::clone(&held);
            taken += black_box(&place).get();
            place.set(place.get() - 1);
        }
    }
    (held, taken)
}

library_benchmark_group!(
    name = places;
    benchmarks = take_and_give_back, one_count
);
main!(library_benchmark_groups = places);
