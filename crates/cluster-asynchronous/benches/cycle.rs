//! Instruction counts for a data plane's cycle on the store, as `edgerush-ratelimit`'s `cycle`
//! benchmark counts it by today's view: the proxy's participant routed by the table
//! ([`Routed`]) — keys scheduled, quiet ones not polled — its hits for a hundred keys of the ten
//! thousand it holds reported to their owner, the batch made with the keys' hashes, and the
//! owner's answers to it taken in, each key found by its hash.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-cluster-asynchronous --bench cycle`, see
//! the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_cluster::{Config, Message as StoreMessage, NodeId, Time, table};
use edgerush_cluster_asynchronous::{Answers, Pod, Routed};
use edgerush_ratelimit::service::pooled::{Levers, Participant};
use edgerush_ratelimit::service::{Contract, Keyed, Limit};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

const MS: Time = 1_000_000;
const CYCLE: Time = 10 * MS;
/// Keys the data plane holds, and how many of them admit a request in the cycle measured.
const HELD: u32 = 10_000;
const ADMITTING: u32 = 100;
/// The only pod of the service.
const POD: NodeId = 1;

/// A participant, boxed, so that handing it to what is measured copies no more than a
/// pointer, hashing its keys as the proxy's does.
type Plane = Box<Participant<Keyed, Routed>>;

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
fn key(k: u32) -> Vec<u8> {
    let address = Ipv4Addr::from_bits(0x0A00_0000 + k);
    format!("api/0\0{address}").into_bytes()
}

/// A request for each of `keys` at `now`, decided.
fn admit(plane: &mut Plane, keys: impl Iterator<Item = u32>, now: u64) {
    let contract = contract();
    for k in keys {
        plane.decide_all(&[(&key(k), Limit::Bucket(contract))], now);
    }
}

/// The only pod of its service, owning every key, its first table taken and its lease held
/// at `now`.
fn lone_pod(now: Time) -> Pod {
    let mut config = Config::defaults();
    config.shards = 256;
    let mut pod = Pod::new(POD, "pod-1:7000".to_owned(), config, Vec::new(), CYCLE);
    let members = [(POD, "pod-1:7000".to_owned())].into_iter().collect();
    let first = table::first(&members, 256, 0, 1);
    let _ = pod.receive(POD, StoreMessage::TableReply { table: first }, now);
    let _ = pod.tick(now);
    pod
}

/// The participant's batches taken in by `pod` at `now`, and the answers to them.
fn exchange(plane: &mut Plane, pod: &mut Pod, now: u64) -> Vec<Answers> {
    plane
        .take_outgoing()
        .into_iter()
        .map(|(_, batch)| pod.batch(batch, now))
        .collect()
}

/// A data plane holding `HELD` keys, each admitted, reported and answered once — more than a
/// piece takes, so over a few cycles — then visited at a quiet cycle, which leaves none of
/// them queued; the only pod of its service owning them all; and a request for each of the
/// first `ADMITTING` admitted since, at the time it returns.
fn admitted() -> (Plane, Box<Pod>, u64) {
    let mut pod = Box::new(lone_pod(MS));
    let table = pod.store().committed().cloned();
    assert!(table.is_some(), "the pod holds its table");
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    let mut plane: Plane = Box::new(
        Participant::new(1, CYCLE, Routed::new(table), hasher)
            .expect("native 128-bit atomics on Linux"),
    );
    plane.set_levers(Levers { poll_quiet: false });
    plane.set_scheduled(true);
    admit(&mut plane, 0..HELD, MS);
    let mut now = CYCLE;
    loop {
        plane.on_cycle(now);
        let answers = exchange(&mut plane, &mut pod, now);
        now += CYCLE;
        if answers.is_empty() {
            break;
        }
        for answers in answers {
            assert!(answers.unserved.is_empty(), "the lone pod serves every key");
            plane.on_reply(Routed::reply(POD, None, answers), now - CYCLE);
        }
    }
    admit(&mut plane, 0..ADMITTING, now);
    (plane, pod, now)
}

/// The cycle after `admitted`'s requests, its batch taken in by the pod: the data plane,
/// the answers it has yet to take in, and when.
fn answered() -> (Plane, Vec<Answers>, u64) {
    let (mut plane, mut pod, now) = admitted();
    let visits = plane.visits();
    plane.on_cycle(now);
    assert_eq!(
        plane.visits() - visits,
        u64::from(ADMITTING),
        "only the keys admitting"
    );
    let answers = exchange(&mut plane, &mut pod, now);
    let reported: usize = answers.iter().map(|answers| answers.entries.len()).sum();
    assert_eq!(reported, ADMITTING as usize, "each key admitting reported");
    (plane, answers, now)
}

#[library_benchmark]
#[bench::hundred_keys(admitted())]
fn on_cycle(setup: (Plane, Box<Pod>, u64)) -> (Plane, Box<Pod>) {
    let (mut plane, pod, now) = setup;
    plane.on_cycle(black_box(now));
    // The batch made in the store's terms, as the hub takes it to send.
    let batches = plane.take_outgoing();
    black_box(batches);
    // Handed back, so that its keys are freed outside what is measured.
    (plane, pod)
}

#[library_benchmark]
#[bench::hundred_keys(answered())]
fn on_reply(setup: (Plane, Vec<Answers>, u64)) -> Plane {
    let (mut plane, answers, now) = setup;
    for answers in answers {
        plane.on_reply(Routed::reply(POD, None, black_box(answers)), black_box(now));
    }
    plane
}

library_benchmark_group!(name = data_plane_cycle; benchmarks = on_cycle, on_reply);
main!(library_benchmark_groups = data_plane_cycle);
