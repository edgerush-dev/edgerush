//! Instruction counts for picking an endpoint, once per exchange
//! ([03 §6](../../../docs/03-data-plane.md)): `p2c` and `round_robin`, in the common case —
//! an unprotected upstream, every endpoint serving, none ramping, a first try — and in the
//! cases that cost more: an
//! endpoint failing its checks drawn, a retry that keeps away from what it tried, an
//! endpoint in slow start, and a protected upstream's endpoints all with room, its full
//! endpoint drawn, or every one of them full
//! ([25 §4](../../../docs/25-backend-protection.md)); and with a zone kept to,
//! whose rule looks at each of its endpoints, beside an upstream whose endpoints are all its
//! zone's, which has none.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench balance`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::balance::{Candidates, RoundRobin, Share, Tried, p2c, zone};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// Endpoints as the benchmark states them.
pub struct Endpoints {
    serves: Vec<bool>,
    /// Whether each has room, on a protected upstream; on an unprotected one, none, and every
    /// endpoint has room without a look, as the data plane's own unprotected upstream does.
    room: Option<Vec<bool>>,
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
    fn has_room(&self, at: usize) -> bool {
        let Some(room) = &self.room else {
            return true;
        };
        room.get(at).copied().unwrap_or(false)
    }
    fn in_flight(&self, at: usize) -> u32 {
        self.in_flight.get(at).copied().unwrap_or(0)
    }
    fn share(&self, at: usize) -> Share {
        self.share.get(at).copied().unwrap_or(Share::FULL)
    }
}

/// `count` endpoints of an unprotected upstream, all serving, a few in flight to each, none
/// ramping.
fn serving(count: usize) -> Endpoints {
    Endpoints {
        serves: vec![true; count],
        room: None,
        in_flight: (0..count).map(|at| (at % 3) as u32).collect(),
        share: vec![Share::FULL; count],
    }
}

/// `count` endpoints of a protected upstream, as `serving`, each with room.
fn protected(count: usize) -> Endpoints {
    Endpoints {
        room: Some(vec![true; count]),
        ..serving(count)
    }
}

/// Sixteen, the first drawn failing its checks.
fn one_failing() -> Endpoints {
    let mut endpoints = serving(16);
    endpoints.serves[7] = false;
    endpoints
}

/// Sixteen, protected, the first drawn full.
fn one_full() -> Endpoints {
    let mut room = vec![true; 16];
    room[7] = false;
    Endpoints {
        room: Some(room),
        ..serving(16)
    }
}

/// Sixteen, protected, every one full: a backend refused.
fn all_full() -> Endpoints {
    Endpoints {
        room: Some(vec![false; 16]),
        ..serving(16)
    }
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
#[bench::sixteen_protected(protected(16), Tried::default())]
#[bench::sixteen_one_full_drawn(one_full(), Tried::default())]
#[bench::sixteen_all_full(all_full(), Tried::default())]
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
#[bench::sixteen_protected(protected(16), Tried::default())]
#[bench::sixteen_one_full_reached(one_full(), Tried::default())]
#[bench::sixteen_all_full(all_full(), Tried::default())]
fn pick_round_robin(endpoints: Endpoints, tried: Tried) -> (Endpoints, Option<usize>) {
    let mut random = draws();
    let mut round_robin = RoundRobin::starting_at(black_box(7));
    let picked = round_robin.pick(black_box(&endpoints), black_box(&tried), &mut random);
    (endpoints, picked)
}

// The first `zoned` of the endpoints are the zone's: the zone's rule, then the pick.
#[library_benchmark]
#[bench::sixteen_zone_of_eight(serving(16), 8, Tried::default())]
#[bench::sixteen_zone_of_eight_one_failing(one_failing(), 8, Tried::default())]
#[bench::sixteen_all_the_zones(serving(16), 16, Tried::default())]
fn pick_p2c_zoned(endpoints: Endpoints, zoned: usize, tried: Tried) -> (Endpoints, Option<usize>) {
    let mut random = draws();
    let (endpoints_seen, tried) = (black_box(&endpoints), black_box(&tried));
    let picked = match zone(endpoints_seen, black_box(zoned), tried) {
        Some(zone) => p2c(&zone, tried, &mut random),
        None => p2c(endpoints_seen, tried, &mut random),
    };
    (endpoints, picked)
}

library_benchmark_group!(
    name = balance;
    benchmarks = pick_p2c, pick_round_robin, pick_p2c_zoned
);

main!(library_benchmark_groups = balance);
