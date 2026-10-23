//! Instruction counts for a service pod taking in a data plane's batch (06 §4): hits for a
//! hundred keys, each key looked up among the ten thousand the pod holds and the data
//! plane's record found among those of the data planes admitting the key — one, as for
//! most keys, or sixteen, as for a key every pod of a deployment admits.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench batch`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::{
    Answers, Batch, Contract, Entry, PlaneId, Report, ServicePod, View,
};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
/// Keys the pod holds, and how many of them the batch carries hits for.
const HELD: u32 = 10_000;
const SENT: u32 = 100;

fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(100),
    }
}

/// A key as the proxy makes it: the rule's name, a zero, and the value, here a client's
/// address.
fn key(k: u32) -> Box<[u8]> {
    let address = Ipv4Addr::from_bits(0x0A00_0000 + k);
    format!("api/0\0{address}").into_bytes().into()
}

/// Data plane `from`'s batch number `sequence` for `keys`: one hit each.
fn batch(from: PlaneId, sequence: u64, keys: impl Iterator<Item = u32>) -> Batch {
    let entries = keys
        .map(|k| Entry {
            key: key(k),
            contract: contract(),
            report: Report::Hits {
                sequence,
                count: 1,
                asked: 0,
                refused: 0,
                seen: sequence - 1,
                age: 0,
            },
        })
        .collect();
    Batch {
        from,
        entries,
        asks: Vec::new(),
        oldest_ask: 0,
        made: 0,
    }
}

/// The only pod of its service, so that it owns every key, holding `HELD` keys from data
/// plane 0, the first `SENT` of them from `planes` data planes; and data plane 0's next
/// batch for those.
fn holding(planes: PlaneId) -> (Box<ServicePod>, Batch) {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: Box<ServicePod> = Box::new(ServicePod::new(0, 10 * MS, view));
    pod.on_batch(batch(0, 1, 0..HELD), 0);
    for plane in 1..planes {
        pod.on_batch(batch(plane, 1, 0..SENT), 0);
    }
    pod.take_replicas();
    (pod, batch(0, 2, 0..SENT))
}

#[library_benchmark]
#[bench::one_plane(holding(1))]
#[bench::sixteen_planes(holding(16))]
fn on_batch(setup: (Box<ServicePod>, Batch)) -> (Answers, Box<ServicePod>) {
    let (mut pod, batch) = setup;
    let answers = pod.on_batch(black_box(batch), black_box(MS));
    // Handed back, so that its keys are freed outside what is measured.
    (answers, pod)
}

library_benchmark_group!(name = service_pod; benchmarks = on_batch);
main!(library_benchmark_groups = service_pod);
