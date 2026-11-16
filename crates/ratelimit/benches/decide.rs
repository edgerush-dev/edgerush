//! Instruction counts for a data plane's decision on the request path (03 §2: at most
//! 150 ns for an existing key), as the proxy's workers decide: a worker meeting a key it holds
//! no handle to, in its process's registry of a hundred thousand: found, made by another
//! worker; and made, cold keys decided locally. And a worker deciding on a key it holds: the
//! first request of a cycle, which starts the key's count and notes when, one after it, and
//! one refused, the bucket empty; and a request under two and three keys it holds, admitted.
//! The participant's one pod answers with the real owner (`owner::apply`).
//!
//! Linux only (valgrind): `cargo bench -p edgerush-ratelimit --bench decide`, see the
//! repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_ratelimit::service::pooled::{
    self, Entry, Levers, Outcome, Participant, Registry, Replied, Reply, Routing,
};
use edgerush_ratelimit::service::{Batch, Contract, Keyed, PodId, owner};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::Arc;

const MS: u64 = 1_000_000;
/// A key as the proxy makes it: the rule's name, a zero, and an API key's value.
const KEY: &[u8] = b"api/0\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
/// The same client's keys under a route's limit and one over the whole data plane.
const ROUTE: &[u8] = b"orders\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";
const GLOBAL: &[u8] = b"global\0c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9";

fn contract(burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    Contract {
        limit: n(100),
        window: n(1_000 * MS),
        burst: n(burst),
    }
}

/// The one pod of a service, owning every key.
#[derive(Clone, Copy, Debug)]
struct One;

impl Routing for One {
    type Pod = PodId;
    type Batch = Batch;

    fn owner(&self, _: &[u8], _: u64) -> Option<PodId> {
        Some(0)
    }

    fn through(&self, _: u64, _: PodId) -> Option<PodId> {
        None
    }

    fn polled(&self, _: u64) -> Option<PodId> {
        Some(0)
    }

    fn later(&self, _: &Self) -> bool {
        false
    }

    fn names(&self, pod: PodId) -> bool {
        pod == 0
    }

    fn batch(&self, _: PodId, batch: Batch, _: impl Fn(&[u8]) -> u64) -> Batch {
        batch
    }
}

/// A participant hashing its keys as the proxy's does, its one pod owning every key.
fn participant() -> Participant<Keyed, One> {
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    Participant::new(1, 10 * MS, One, hasher).expect("native 128-bit atomics on Linux")
}

/// `participant`'s batches for its one key answered at `now` by its owner, each report applied
/// to the key's `value` with the real owner ([`owner::apply`]).
fn answer(participant: &mut Participant<Keyed, One>, value: &mut owner::Value, now: u64) {
    for (_, batch) in participant.take_outgoing() {
        let entries = batch
            .entries
            .iter()
            .map(|entry| {
                let applied = owner::apply(
                    value,
                    batch.from,
                    entry.contract,
                    entry.report,
                    10 * MS,
                    now,
                );
                Replied {
                    key: participant.hash(&entry.key),
                    behind: applied.answered.behind,
                    applied: applied.answered.applied,
                    others: applied.answered.others,
                }
            })
            .collect();
        let reply = Reply {
            from: 0,
            via: None,
            life: 0,
            number: 0,
            made: batch.made,
            entries,
            routing: None,
        };
        participant.on_reply(reply, now);
    }
}

/// A process's registry of entries, its keys hashed as the proxy's are.
type Shared = Arc<Registry<Keyed>>;

/// An entry found or made in a registry, with whether it was made.
type Made = Option<(Arc<Entry>, bool)>;

/// A process's registry of a hundred thousand keys, `KEY` among them.
fn registry() -> Shared {
    let registry = participant().registry();
    let contract = contract(100);
    let keys = (0..100_000).map(|k| format!("api/0\0key-{k}").into_bytes());
    for key in keys.chain([KEY.to_vec()]) {
        let _made = registry.find_or_make(&key, contract, 0, 0);
    }
    registry
}

#[library_benchmark]
#[bench::found(registry())]
fn find(registry: Shared) -> (Option<Arc<Entry>>, Shared) {
    let found = registry.find(black_box(KEY), black_box(contract(100)));
    (found, registry)
}

#[library_benchmark]
#[bench::made(registry())]
fn find_or_make(registry: Shared) -> (Made, Shared) {
    let made = registry.find_or_make(black_box(ROUTE), black_box(contract(100)), 0, MS);
    (made, registry)
}

/// A worker's handle to `KEY`'s entry under a bucket of `burst`, warm: admitted, reported,
/// answered and visited at the next cycle, which leaves its count at none; and `admitted`
/// requests decided on it since, at 21 ms.
fn held(burst: u64, admitted: usize) -> Arc<Entry> {
    held_key(KEY, burst, admitted)
}

/// As [`held`], for `key`.
fn held_key(key: &[u8], burst: u64, admitted: usize) -> Arc<Entry> {
    let mut participant = participant();
    participant.set_levers(Levers { poll_quiet: false });
    let contract = contract(burst);
    participant.decide_all(&[(key, contract)], 0);
    participant.on_cycle(10 * MS);
    answer(&mut participant, &mut owner::Value::default(), 10 * MS);
    participant.on_cycle(20 * MS);
    let entry = participant.entries(&[(key, contract)]).remove(0);
    for _ in 0..admitted {
        let _decided = pooled::decide(&[&entry], 21 * MS);
    }
    entry
}

#[library_benchmark]
#[bench::first_of_a_cycle(held(100, 0))]
#[bench::admitted(held(100, 1))]
#[bench::refused_empty(held(1, 1))]
fn worker(entry: Arc<Entry>) -> (Outcome, Arc<Entry>) {
    let outcome = pooled::decide(&[black_box(&*entry)], black_box(22 * MS));
    (outcome, entry)
}

/// The first `limits` of `KEY`, `ROUTE` and `GLOBAL`, each held as [`held`] has it, one
/// request decided on each since.
fn held_all(limits: usize) -> Vec<Arc<Entry>> {
    [KEY, ROUTE, GLOBAL][..limits]
        .iter()
        .map(|key| held_key(key, 100, 1))
        .collect()
}

#[library_benchmark]
#[bench::two(held_all(2))]
#[bench::three(held_all(3))]
fn worker_all(held: Vec<Arc<Entry>>) -> (Outcome, Vec<Arc<Entry>>) {
    // The keys as the proxy hands the usual few over, with no list made for them.
    let now = black_box(22 * MS);
    let outcome = match held.as_slice() {
        [a, b] => pooled::decide(black_box(&[&**a, &**b]), now),
        [a, b, c] => pooled::decide(black_box(&[&**a, &**b, &**c]), now),
        more => pooled::decide(&more.iter().map(|entry| &**entry).collect::<Vec<_>>(), now),
    };
    (outcome, held)
}

library_benchmark_group!(name = registry_of_keys; benchmarks = find, find_or_make);
library_benchmark_group!(name = participants_worker; benchmarks = worker, worker_all);
main!(
    library_benchmark_groups = registry_of_keys,
    participants_worker
);
