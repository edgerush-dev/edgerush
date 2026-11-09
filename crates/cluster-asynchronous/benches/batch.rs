//! Instruction counts for a pod of the asynchronous mode taking in a data plane's batch on the
//! store, as `edgerush-ratelimit`'s `batch` benchmark counts today's pod (`ServicePod`): hits
//! for a hundred keys, each among the ten thousand the pod holds, the data plane's record found
//! among one data plane's or sixteen's; and, a pod among three, a batch for a thousand keys, of
//! which it owns about a third, its store copying their changes to the other two at its next
//! tick — measured with the tick, as today's pod copies within the batch.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-cluster-asynchronous --bench batch`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_cluster::{Config, Message as StoreMessage, NodeId, Time, table};
use edgerush_cluster_asynchronous::{Answers, Batch, Entry, Message, Outbox, Pod};
use edgerush_ratelimit::service::{Contract, PlaneId, Report};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;

const MS: Time = 1_000_000;
const CYCLE: Time = 10 * MS;
/// Keys the pod holds, and how many of them the batch carries hits for.
const HELD: u64 = 10_000;
const SENT: u64 = 100;
/// Keys a batch carries hits for when its pod is one of three.
const SENT_AMONG_THREE: u64 = 1_000;

fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(100),
    }
}

/// Key `k` as a data plane hashes one: spread over the shards.
const fn key(k: u64) -> u64 {
    k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Data plane `from`'s batch number `sequence` for `keys`: one hit each.
fn batch(from: PlaneId, sequence: u64, keys: impl Iterator<Item = u64>) -> Batch {
    let entries = keys
        .map(|k| Entry {
            key: key(k),
            contract: contract(),
            report: Report::Hits {
                sequence,
                count: 1,
                seen: sequence - 1,
                age: 0,
            },
        })
        .collect();
    Batch {
        from,
        stamp: (0, 0),
        made: 0,
        entries,
    }
}

/// Pod 1 among `pods`, their first table taken and its lease held at `now`.
fn member(pods: &[NodeId], now: Time) -> Pod {
    let mut config = Config::defaults();
    config.shards = 256;
    let peers = pods.iter().copied().filter(|pod| *pod != 1);
    let mut pod = Pod::new(1, "pod-1:7000".to_owned(), config, peers, CYCLE);
    let members = pods
        .iter()
        .map(|id| (*id, format!("pod-{id}:7000")))
        .collect();
    let replicas = pods.len().saturating_sub(1);
    let first = table::first(&members, 256, replicas, 1);
    let _ = pod.receive(1, StoreMessage::TableReply { table: first }, now);
    lease(&mut pod, pods, now);
    pod
}

/// Its peers heard at `now`, its lease held.
fn lease(pod: &mut Pod, pods: &[NodeId], now: Time) {
    let hears: Vec<(NodeId, Time)> = pods.iter().map(|id| (*id, now)).collect();
    for peer in pods.iter().copied().filter(|peer| *peer != 1) {
        let heard = StoreMessage::Heartbeat {
            term: pod.store().term(),
            leader: Some(1),
            committed: pod.store().committed_stamp(),
            fresh: false,
            sent: now,
            wall: 0,
            hears: hears.clone(),
            leased: true,
        };
        let _ = pod.receive(peer, heard, now);
    }
    let _ = pod.tick(now);
}

/// The only pod of its service, so that it owns every key, holding `HELD` keys from data
/// plane 0, the first `SENT` of them from `planes` data planes; and data plane 0's next batch
/// for those.
fn holding(planes: PlaneId) -> (Box<Pod>, Batch) {
    let mut pod = Box::new(member(&[1], MS));
    let answers = pod.batch(batch(0, 1, 0..HELD), MS);
    assert_eq!(
        answers.entries.len() as u64,
        HELD,
        "the lone pod serves every key"
    );
    for plane in 1..planes {
        let _ = pod.batch(batch(plane, 1, 0..SENT), MS);
    }
    let _ = pod.tick(2 * MS);
    (pod, batch(0, 2, 0..SENT))
}

/// Pod 1 of three, owning about a third of the keys, its store copying each it changes to the
/// other two, holding those of `HELD` keys from data plane 0 that it owns; and its next batch
/// for the first `SENT_AMONG_THREE`.
fn among_three() -> (Box<Pod>, Batch) {
    let pods = [1, 2, 3];
    let mut pod = Box::new(member(&pods, MS));
    let answers = pod.batch(batch(0, 1, 0..HELD), MS);
    assert!(!answers.entries.is_empty(), "the pod serves its own shards");
    // Past a copying cycle, so that what the setup changed is copied and out of the way.
    lease(&mut pod, &pods, MS + CYCLE);
    let next = batch(0, 2, 0..SENT_AMONG_THREE);
    // What is measured, tried on a copy: the batch's changes do go out at the tick after.
    let mut tried = pod.clone();
    let _ = tried.batch(next.clone(), 3 * CYCLE);
    let copies = tried.tick(4 * CYCLE);
    let copied = copies.iter().any(|(_, message)| {
        matches!(message, Message::Store(StoreMessage::Replicate { entries, .. }) if !entries.is_empty())
    });
    assert!(copied, "the tick copies the batch's changes");
    (pod, next)
}

#[library_benchmark]
#[bench::one_plane(holding(1))]
#[bench::sixteen_planes(holding(16))]
#[bench::a_third_of_them(among_three())]
fn on_batch(setup: (Box<Pod>, Batch)) -> (Answers, Box<Pod>) {
    let (mut pod, batch) = setup;
    let answers = pod.batch(black_box(batch), black_box(3 * CYCLE));
    // Handed back, so that its keys are freed outside what is measured.
    (answers, pod)
}

// The batch and the tick after, which copies its changes to the two replicas.
#[library_benchmark]
#[bench::copied_among_three(among_three())]
fn on_batch_then_copied(setup: (Box<Pod>, Batch)) -> (Answers, Outbox, Box<Pod>) {
    let (mut pod, batch) = setup;
    let answers = pod.batch(black_box(batch), black_box(3 * CYCLE));
    let copies = pod.tick(black_box(4 * CYCLE));
    (answers, copies, pod)
}

library_benchmark_group!(name = asynchronous_pod; benchmarks = on_batch, on_batch_then_copied);
main!(library_benchmark_groups = asynchronous_pod);
