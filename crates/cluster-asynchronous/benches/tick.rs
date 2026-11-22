//! Instruction counts for a pod of the asynchronous mode's tick over a hundred thousand keys
//! under three limits, from sixteen data planes: a cycle on, with nothing to copy and none due
//! to be forgotten, as almost every tick runs; a second on, the keys of the shortest limit due
//! to be forgotten, a third of them, of which a cycle forgets at most its share; and past the
//! pod's cap on what its keys hold, none due, a batch of keys new to it just in, of which a
//! cycle evicts as many and its share besides, down to nine tenths of the cap. A pod forgets
//! and evicts at each cycle, a bounded number a cycle, so the last two show that bound.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-cluster-asynchronous --bench tick`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_cluster::{Config, Key, Message as StoreMessage, Time, table};
use edgerush_cluster_asynchronous::{Batch, Entry, Outbox, Pod};
use edgerush_ratelimit::service::{Contract, PlaneId, Report};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;

const MS: Time = 1_000_000;
const CYCLE: Time = 10 * MS;
/// Keys the pod holds.
const HELD: u64 = 100_000;
/// Data planes the keys' hits come from, each a share of them; a key is one plane's.
const PLANES: u64 = 16;

/// A hundred a second with a bucket of ten, a tenth of a second's refill; a thousand a minute;
/// ten thousand an hour.
fn contract(k: u64) -> Contract {
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

/// The two longer of [`contract`]'s limits alone, none of whose keys is due within a minute.
fn longer(k: u64) -> Contract {
    contract(1 + k % 2)
}

/// Key `k` as a data plane hashes one: both halves spread, over the shards.
const fn key(k: u64) -> Key {
    let spread = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    // A second multiplier, as unrelated to the first as two runs of the keyed hash are: a
    // half drawn from the other (its complement, say) would fold every key into one shard.
    edgerush_cluster::key::joined(spread, k.wrapping_mul(0xC2B2_AE3D_27D4_EB4F))
}

/// The only pod of its service, so that it owns every key, under `config`: its first table
/// taken and its lease held at `now`.
fn alone(config: Config, now: Time) -> Pod {
    let mut pod = Pod::new(1, "pod-1:7000".to_owned(), config, [], CYCLE);
    let members = [(1, "pod-1:7000".to_owned())].into_iter().collect();
    let first = table::first(&members, config.shards, 0, 1);
    let _ = pod.receive(1, StoreMessage::TableReply { table: first }, now);
    let _ = pod.tick(now);
    pod
}

/// When the measured ticks begin: the pod past its first copy cycle, which copies what the
/// keys' first hits changed.
const SETTLED: Time = 2 * MS + CYCLE;

/// A pod holding `HELD` keys under `contract`, each hit once by one of `PLANES` data planes, at
/// 1 ms, under `config`; ticked at 2 ms and at [`SETTLED`], its first copy cycle done.
fn filled(config: Config, contract: fn(u64) -> Contract) -> Box<Pod> {
    let mut pod = Box::new(alone(config, MS));
    for plane in 0..PLANES {
        let entries = (plane..HELD)
            .step_by(usize::try_from(PLANES).unwrap_or(1))
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
        let _ = pod.batch(
            Batch {
                from: PlaneId::from(plane),
                stamp: (0, 0),
                made: 0,
                entries,
            },
            MS,
        );
    }
    let _ = pod.tick(2 * MS);
    let _ = pod.tick(SETTLED);
    pod
}

fn config() -> Config {
    let mut config = Config::defaults();
    config.shards = 256;
    config
}

/// How many keys `pod` holds after a tick at `now`, tried on a copy.
fn kept_after(pod: &Pod, now: Time) -> u64 {
    let mut tried = pod.clone();
    let _ = tried.tick(now);
    tried.store().key_count() as u64
}

/// A pod holding the keys, and the time to tick it at next, `after` after [`SETTLED`].
fn holding(after: Time) -> (Box<Pod>, Time) {
    let pod = filled(config(), contract);
    assert_eq!(
        pod.store().key_count() as u64,
        HELD,
        "the lone pod holds every key"
    );
    (pod, SETTLED + after)
}

/// A cycle on: nothing to copy, nothing due.
fn nothing_due() -> (Box<Pod>, Time) {
    let (pod, now) = holding(CYCLE);
    assert_eq!(kept_after(&pod, now), HELD, "nothing forgotten");
    (pod, now)
}

/// A second on: the shortest limit's keys, a third of them, due to be forgotten, more than a
/// cycle forgets.
fn a_second_on() -> (Box<Pod>, Time) {
    let (pod, now) = holding(1_000 * MS);
    let kept = kept_after(&pod, now);
    assert!(
        (HELD - HELD / 4..HELD).contains(&kept),
        "a cycle's share of the shortest limit's keys forgotten: {kept} kept"
    );
    (pod, now)
}

/// Keys new to the pod in [`over_the_cap`]'s batch.
const MADE: u64 = 10_000;

/// The keys under the two longer limits alone, none due to be forgotten, its cap on what they
/// hold a tenth short of what they hold, so that its first cycles evicted down to nine tenths
/// of it; then [`MADE`] keys new to it in one batch, taking it past its cap again, and a cycle
/// on: as many to evict as it made, and more than a cycle's share.
fn over_the_cap() -> (Box<Pod>, Time) {
    let full = filled(config(), longer).store().key_bytes();
    let mut capped = config();
    capped.max_key_bytes = Some(full - full / 10);
    let mut pod = filled(capped, longer);
    let settled = pod.store().key_count() as u64;
    assert!(
        settled < HELD - HELD / 10,
        "evicted to nine tenths: {settled} kept"
    );
    let entries = (HELD..HELD + MADE)
        .map(|k| Entry {
            key: key(k),
            contract: longer(k),
            report: Report::Hits {
                sequence: 1,
                count: 1,
                seen: 0,
                age: 0,
            },
        })
        .collect();
    let batch = Batch {
        from: PlaneId::from(0_u64),
        stamp: (0, 0),
        made: 0,
        entries,
    };
    let _ = pod.batch(batch, SETTLED + MS);
    let made = pod.store().key_count() as u64;
    assert_eq!(made, settled + MADE, "the batch's keys made");
    let now = SETTLED + CYCLE;
    let kept = kept_after(&pod, now);
    assert!(
        (settled - MADE / 10..made - MADE / 2).contains(&kept),
        "a cycle evicts the keys made and more, down to nine tenths: {kept} of {made} kept"
    );
    (pod, now)
}

#[library_benchmark]
#[bench::none_due(nothing_due())]
#[bench::after_a_gap(a_second_on())]
#[bench::past_capacity(over_the_cap())]
fn on_tick(setup: (Box<Pod>, Time)) -> (Outbox, Box<Pod>) {
    let (mut pod, now) = setup;
    let out = pod.tick(black_box(now));
    // Handed back, so that its keys are freed outside what is measured.
    (out, pod)
}

library_benchmark_group!(name = asynchronous_pod_tick; benchmarks = on_tick);
main!(library_benchmark_groups = asynchronous_pod_tick);
