//! Instruction counts for picking an endpoint, once per exchange
//! ([03 §6](../../../docs/03-data-plane.md)): `p2c` and `round_robin`, in the common case —
//! every endpoint serving, none ramping, a first try — and in the cases that cost more: an
//! endpoint failing its checks drawn, a retry that keeps away from what it tried, and an
//! endpoint in slow start.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench balance`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::balance::{Candidates, RoundRobin, Share, Tried, p2c};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// Endpoints as the benchmark states them.
pub struct Endpoints {
    serves: Vec<bool>,
    in_flight: Vec<u32>,
    share: Vec<Share>,
}

impl Candidates for Endpoints {
    fn count(&self) -> usize {
        self.serves.len()
    }
    fn serves(&self, at: usize) -> bool {
        self.serves.get(at).copied().unwrap_or(false)
    }
    fn in_flight(&self, at: usize) -> u32 {
        self.in_flight.get(at).copied().unwrap_or(0)
    }
    fn share(&self, at: usize) -> Share {
        self.share.get(at).copied().unwrap_or(Share::FULL)
    }
}

/// `count` endpoints, all serving, a few in flight to each, none ramping.
fn serving(count: usize) -> Endpoints {
    Endpoints {
        serves: vec![true; count],
        in_flight: (0..count).map(|at| (at % 3) as u32).collect(),
        share: vec![Share::FULL; count],
    }
}

/// Sixteen, the first drawn failing its checks.
fn one_failing() -> Endpoints {
    let mut endpoints = serving(16);
    endpoints.serves[7] = false;
    endpoints
}

/// Sixteen, the first drawn in slow start at a tenth.
fn one_ramping() -> Endpoints {
    let mut endpoints = serving(16);
    endpoints.share[7] = Share::of(6_554);
    endpoints
}

/// Draws that land where each case means them to: `p2c`'s first on endpoint 7 of 16 (3 of
/// 4), its second on another; endpoint 7 in slow start refused once by `p2c` and kept by
/// `round_robin`.
fn draws() -> impl FnMut() -> u64 {
    let mut sequence = [7_u64 + 16 * 1_000, u64::MAX, 5, 2, 9, 11]
        .into_iter()
        .cycle();
    move || sequence.next().unwrap_or(0)
}

fn tried(positions: &[usize]) -> Tried {
    let mut tried = Tried::default();
    for &at in positions {
        tried.add(at);
    }
    tried
}

// The endpoints are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::one_endpoint(serving(1), Tried::default())]
#[bench::four(serving(4), Tried::default())]
#[bench::sixteen(serving(16), Tried::default())]
#[bench::sixteen_one_failing_drawn(one_failing(), Tried::default())]
#[bench::sixteen_one_ramping_drawn(one_ramping(), Tried::default())]
#[bench::sixteen_retry_two_tried(serving(16), tried(&[7, 3]))]
fn pick_p2c(endpoints: Endpoints, tried: Tried) -> (Endpoints, Option<usize>) {
    let mut random = draws();
    let picked = p2c(black_box(&endpoints), black_box(&tried), &mut random);
    (endpoints, picked)
}

#[library_benchmark]
#[bench::one_endpoint(serving(1), Tried::default())]
#[bench::four(serving(4), Tried::default())]
#[bench::sixteen(serving(16), Tried::default())]
#[bench::sixteen_one_failing_reached(one_failing(), Tried::default())]
#[bench::sixteen_one_ramping_reached(one_ramping(), Tried::default())]
#[bench::sixteen_retry_two_tried(serving(16), tried(&[7, 3]))]
fn pick_round_robin(endpoints: Endpoints, tried: Tried) -> (Endpoints, Option<usize>) {
    let mut random = draws();
    let mut round_robin = RoundRobin::starting_at(black_box(7));
    let picked = round_robin.pick(black_box(&endpoints), black_box(&tried), &mut random);
    (endpoints, picked)
}

library_benchmark_group!(name = balance; benchmarks = pick_p2c, pick_round_robin);

main!(library_benchmark_groups = balance);
