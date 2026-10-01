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

use edgerush_proxy::places::Places;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::cell::Cell;
use std::hint::black_box;
use std::rc::Rc;

/// Exchanges taken and given back, one after another.
const EXCHANGES: usize = 100;

/// Upstreams the config has, and the exchanges go to in turn.
const UPSTREAMS: usize = 8;

// A worker with room: what nearly every exchange meets.
#[library_benchmark]
fn with_room() -> usize {
    let places = Places::new(1024);
    let mut taken = 0;
    for exchange in 0..EXCHANGES {
        if let Ok(place) = places.take(black_box(exchange % UPSTREAMS), UPSTREAMS) {
            taken += black_box(&place).places_held();
        }
    }
    taken
}

// A worker short of places, every exchange counted against its upstream's share.
#[library_benchmark]
fn short_of_places() -> usize {
    let places = Places::new(1024);
    let _held: Vec<_> = (0..900)
        .filter_map(|held| places.take(held % UPSTREAMS, UPSTREAMS).ok())
        .collect();
    let mut taken = 0;
    for exchange in 0..EXCHANGES {
        if let Ok(place) = places.take(black_box(exchange % UPSTREAMS), UPSTREAMS) {
            taken += black_box(&place).places_held();
        }
    }
    taken
}

// The count alone, as every exchange took its place before.
#[library_benchmark]
fn one_count() -> usize {
    let held = Rc::new(Cell::new(0_usize));
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
    taken
}

/// What a place is asked in the benchmarks, so that taking one is not optimised away.
trait Held {
    fn places_held(&self) -> usize;
}

impl Held for edgerush_proxy::places::Place {
    fn places_held(&self) -> usize {
        1
    }
}

library_benchmark_group!(
    name = places;
    benchmarks = with_room, short_of_places, one_count
);
main!(library_benchmark_groups = places);
