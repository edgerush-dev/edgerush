//! Instruction counts for a service pod's timer (06 §4) over a hundred thousand keys under
//! three limits, from data planes come and gone: a cycle on, when none is due, as almost
//! every timer runs; after a second's gap, when the keys of the shortest limit are due to
//! be forgotten; and past the pod's capacity, when a tenth of what it holds is evicted.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench timer`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::{Batch, Contract, Entry, Report, ServicePod, View};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
/// Keys the pod holds.
const HELD: u32 = 100_000;
/// Data planes the keys' hits come from, each a share of them; a key is one plane's.
const PLANES: u32 = 16;

/// A hundred a second with a bucket of ten, a tenth of a second's refill; a thousand a
/// minute; ten thousand an hour.
fn contract(k: u32) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    let (limit, window, burst) = match k % 3 {
        0 => (100, 1_000 * MS, 10),
        1 => (1_000, 60_000 * MS, 100),
        _ => (10_000, 3_600_000 * MS, 1_000),
    };
    Contract {
        limit: n(limit),
        window: n(window),
        burst: n(burst),
    }
}

/// A key as the proxy makes it: the rule's name, a zero, and the value, here a client's
/// address.
fn key(k: u32) -> Box<[u8]> {
    let address = Ipv4Addr::from_bits(0x0A00_0000 + k);
    format!("api/0\0{address}").into_bytes().into()
}

/// The only pod of its service, holding `HELD` keys, each hit once by one of `PLANES` data
/// planes, its timer run once since; and the time to run it at next, `after` later.
fn holding(after: u64) -> (Box<ServicePod>, u64) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: Box<ServicePod> = Box::new(ServicePod::new(0, CYCLE, view));
    for plane in 0..PLANES {
        let entries = (plane..HELD)
            .step_by(PLANES as usize)
            .map(|k| Entry {
                key: key(k),
                contract: contract(k),
                report: Report::Hits {
                    sequence: 1,
                    count: 1,
                    seen: 0,
                    age: 0,
                },
            })
            .collect();
        pod.on_batch(
            Batch {
                from: u64::from(plane),
                entries,
                made: 0,
            },
            0,
        );
    }
    pod.take_replicas();
    pod.on_timer(0);
    (pod, after)
}

/// As [`holding`], its capacity a tenth short of what its keys hold.
fn over_capacity() -> (Box<ServicePod>, u64) {
    let (mut pod, after) = holding(CYCLE);
    pod.set_capacity(pod.holding() - pod.holding() / 10);
    (pod, after)
}

#[library_benchmark]
#[bench::none_due(holding(CYCLE))]
#[bench::after_a_gap(holding(1_000 * MS))]
#[bench::past_capacity(over_capacity())]
fn on_timer(setup: (Box<ServicePod>, u64)) -> Box<ServicePod> {
    let (mut pod, now) = setup;
    pod.on_timer(black_box(now));
    // Handed back, so that its keys are freed outside what is measured.
    pod
}

library_benchmark_group!(name = service_pod_timer; benchmarks = on_timer);
main!(library_benchmark_groups = service_pod_timer);
