//! Local mode's limiter against the references it keeps (`reference::TokenBucket`, and
//! `reference::FixedWindow` aligned to the clock), and its registry: keys made, shared,
//! forgotten and evicted.

#![cfg(not(loom))]

use super::*;
use crate::reference::{Alignment, FixedWindow, Model, TokenBucket};
use crate::service::FixedState;
use proptest::prelude::*;
use std::collections::HashMap;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

pub(super) fn contract(limit: u64, window: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(window),
        burst: n(burst),
    }
}

pub(super) fn local() -> Local<FixedState> {
    Local::new(FixedState::default())
}

/// Asserts that `decision` refuses at `position` with the wait `owed` nanoseconds of `c`'s
/// refill come to less the slack ([`SLACK`](crate::SLACK)), within a unit of the contract
/// either way: the slack is rounded up to a whole unit, erring towards admitting, and a time
/// off the units' grid is rounded when converted.
#[track_caller]
fn assert_refused(decision: Decision, position: usize, owed: u64, c: Contract) {
    let unit = c.window.get().div_ceil(c.limit.get() * UNITS);
    let due = owed - crate::SLACK;
    let Decision::Refuse { position: at, wait } = decision else {
        panic!("admitted, not refused at {position}");
    };
    assert_eq!(at, position);
    assert!(
        (due - unit..=due + unit).contains(&wait),
        "waits {wait}, due {due}"
    );
}

/// Decides one request under `key` alone.
fn one(
    local: &Local<FixedState>,
    handles: &mut Handles<FixedState>,
    key: &[u8],
    c: Contract,
    now: u64,
) -> bool {
    local.decide(handles, &[(key, c)], now) == Decision::Admit
}

#[test]
fn a_new_key_admits_its_bucket_then_refuses() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 5);
    let admitted = (0..8)
        .filter(|_| one(&local, &mut handles, b"k", c, 0))
        .count();
    assert_eq!(admitted, 5);
}

/// A key's bucket holds its burst and the slack's refill (`crate::SLACK`): at a request a
/// microsecond, a hundred more than a burst of one.
#[test]
fn a_key_holds_its_burst_and_the_slacks_refill() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(1_000_000, SECOND, 1);
    let admitted = (0..200)
        .filter(|_| one(&local, &mut handles, b"k", c, SECOND))
        .count();
    assert_eq!(admitted, 101);
}

#[test]
fn it_refills_at_its_rate() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert!(one(&local, &mut handles, b"k", c, 0));
    assert!(!one(&local, &mut handles, b"k", c, 99 * MS));
    assert!(one(&local, &mut handles, b"k", c, 100 * MS));
}

#[test]
fn a_refusal_says_how_long_until_room() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert!(one(&local, &mut handles, b"k", c, 0));
    // A client waiting as long finds room.
    let decision = local.decide(&mut handles, &[(b"k", c)], 30 * MS);
    assert_refused(decision, 0, 70 * MS, c);
    let Decision::Refuse { wait, .. } = decision else {
        unreachable!("asserted refused");
    };
    assert!(one(&local, &mut handles, b"k", c, 30 * MS + wait));
}

#[test]
fn workers_share_a_key() {
    let local = local();
    let (mut one_worker, mut other) = (
        Handles::new(FixedState::default()),
        Handles::new(FixedState::default()),
    );
    let c = contract(10, SECOND, 2);
    assert!(one(&local, &mut one_worker, b"k", c, 0));
    assert!(one(&local, &mut other, b"k", c, 0));
    assert!(!one(&local, &mut one_worker, b"k", c, 0));
    assert!(!one(&local, &mut other, b"k", c, 0));
    assert_eq!(local.key_count(), 1);
}

#[test]
fn a_changed_contract_starts_the_key_afresh() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (before, after) = (contract(10, SECOND, 1), contract(20, SECOND, 1));
    assert!(one(&local, &mut handles, b"k", before, 0));
    assert!(!one(&local, &mut handles, b"k", before, 0));
    assert!(one(&local, &mut handles, b"k", after, 0));
    assert_eq!(local.key_count(), 1);
}

#[test]
fn several_limits_charge_all_or_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (wide, narrow) = (contract(10, SECOND, 3), contract(10, SECOND, 1));
    let both: [(&[u8], Contract); 2] = [(b"wide", wide), (b"narrow", narrow)];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_refused(local.decide(&mut handles, &both, 0), 1, 100 * MS, narrow);
    // The refusal charged neither: the wide limit still has two.
    assert!(one(&local, &mut handles, b"wide", wide, 0));
    assert!(one(&local, &mut handles, b"wide", wide, 0));
    assert!(!one(&local, &mut handles, b"wide", wide, 0));
}

#[test]
fn several_refusing_say_the_longest_wait() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (slow, fast) = (contract(1, SECOND, 1), contract(10, SECOND, 1));
    let both: [(&[u8], Contract); 2] = [(b"fast", fast), (b"slow", slow)];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_refused(local.decide(&mut handles, &both, 0), 1, SECOND, slow);
}

#[test]
fn a_full_quiet_key_is_forgotten_and_one_in_use_kept() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert!(one(&local, &mut handles, b"quiet", c, 0));
    assert!(one(&local, &mut handles, b"busy", c, 0));
    assert!(one(&local, &mut handles, b"busy", c, SECOND));
    // At 1.05 s "quiet" is full and was last used a second ago; "busy" owes until 1.1 s.
    let swept = local.sweep(SECOND + 50 * MS, 500 * MS);
    assert_eq!(swept.forgotten, 1);
    assert_eq!(local.key_count(), 1);
    // A worker's handle to the key forgotten finds it closed and makes it again, full.
    assert!(one(&local, &mut handles, b"quiet", c, SECOND + 50 * MS));
    assert_eq!(local.key_count(), 2);
}

#[test]
fn a_full_key_used_lately_is_kept() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 5);
    assert!(one(&local, &mut handles, b"k", c, 0));
    // Full again at 100 ms, but used within the half second asked for.
    assert_eq!(local.sweep(400 * MS, 500 * MS).forgotten, 0);
    assert_eq!(local.sweep(600 * MS, 500 * MS).forgotten, 1);
}

#[test]
fn a_quiet_key_still_owing_is_kept() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    // Spent at once, full again only after five seconds.
    let c = contract(1, SECOND, 5);
    for _ in 0..5 {
        assert!(one(&local, &mut handles, b"k", c, 0));
    }
    assert_eq!(local.sweep(2 * SECOND, SECOND).forgotten, 0);
    // Two refilled by then; forgotten, it would admit five.
    let admitted = (0..5)
        .filter(|_| one(&local, &mut handles, b"k", c, 2 * SECOND))
        .count();
    assert_eq!(admitted, 2);
}

#[test]
fn a_refusal_names_each_limit_out_of_room_and_charges_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (roomy, tight) = (contract(10, SECOND, 5), contract(10, SECOND, 1));
    let three: [(&[u8], Contract); 3] = [(b"a", tight), (b"b", roomy), (b"c", tight)];
    assert_eq!(local.decide(&mut handles, &three, 0), Decision::Admit);
    let (decision, refusing) = local.decide_saying(&mut handles, &three, 0);
    assert_refused(decision, 0, 100 * MS, tight);
    assert_eq!(refusing, 0b101);
    // The roomy one still has its four.
    let left = (0..5)
        .filter(|_| one(&local, &mut handles, b"b", roomy, 0))
        .count();
    assert_eq!(left, 4);
}

#[test]
fn one_limit_refusing_is_named() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert_eq!(
        local.decide_saying(&mut handles, &[(b"k", c)], 0),
        (Decision::Admit, 0)
    );
    let (decision, refusing) = local.decide_saying(&mut handles, &[(b"k", c)], 0);
    assert_refused(decision, 0, 100 * MS, c);
    assert_eq!(refusing, 1);
}

#[test]
fn of_limits_refusing_as_long_the_first_is_named() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    let both: [(&[u8], Contract); 2] = [(b"one", c), (b"other", c)];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_refused(local.decide(&mut handles, &both, 0), 0, 100 * MS, c);
}

proptest! {
    /// However its interval divides into nanoseconds, a client waiting as long as it was
    /// told finds room: spent until refused, past its burst by the slack's refill at most.
    #[test]
    fn the_wait_said_is_never_short(
        limit in 1_u64..1_000,
        window_ms in 1_u64..2_000,
        burst in 1_u64..5,
        at in 0_u64..10 * SECOND,
    ) {
        let c = contract(limit, window_ms * MS, burst);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        for _ in 0..burst {
            prop_assert!(one(&local, &mut handles, b"k", c, at));
        }
        let slack = c.refilled(crate::SLACK) + 1;
        let decision = (0..=slack)
            .map(|_| local.decide(&mut handles, &[(b"k", c)], at))
            .find(|decision| *decision != Decision::Admit);
        let Some(Decision::Refuse { wait, .. }) = decision else {
            return Err(TestCaseError::fail("not refused past the burst and the slack"));
        };
        prop_assert!(one(&local, &mut handles, b"k", c, at + wait), "after {}", wait);
    }
}

/// A key counts its own bytes as it comes, and as it goes until the last handle to it does:
/// forgotten, it is freed only once the worker's handles let go of it. The tables holding
/// it, the registry's and the worker's handles', count as they are allocated, keep their
/// room as keys go, and are given back with the handles.
#[test]
fn bytes_are_counted_as_keys_come_and_go() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert_eq!(local.holding(), 0);
    assert!(one(&local, &mut handles, b"k1", c, 0));
    let tables = local.tables();
    assert!(tables > 0, "a shard's table and a part of the handles'");
    assert_eq!(tables, local.registry_tables() + handles.set.bytes());
    assert_eq!(local.holding(), tables + key_bytes(b"k1", c.into()));
    assert!(one(&local, &mut handles, b"k22", c, 0));
    let tables = local.tables();
    assert_eq!(
        local.holding(),
        tables + key_bytes(b"k1", c.into()) + key_bytes(b"k22", c.into())
    );
    local.sweep(10 * SECOND, 0);
    assert_eq!(local.key_count(), 0, "both forgotten");
    assert_eq!(
        local.holding(),
        tables + key_bytes(b"k1", c.into()) + key_bytes(b"k22", c.into()) + local.gone_room(),
        "and counted while the worker's handles hold them"
    );
    handles.prune();
    assert_eq!(local.evict(10 * SECOND), 0);
    assert_eq!(local.gone_room(), 0);
    assert_eq!(
        local.holding(),
        tables,
        "freed once let go of; the tables keep their room"
    );
    drop(handles);
    assert_eq!(
        local.holding(),
        local.registry_tables(),
        "the handles' given back"
    );
}

/// A cap whose nine tenths hold `local`'s tables, the room of its list of keys let go of, and
/// `keys` keys of `each` bytes, with half a key to spare: what eviction comes down to.
fn room_for(local: &Local<FixedState>, keys: u64, each: u64) -> u64 {
    (local.tables() + local.gone_room() + keys * each + each / 2) * 10 / 9
}

/// A name of some 2 KB, the `n`th: keys large beside the tables of a handful of them, so
/// that a cap's tenth is a few keys.
fn long_name(n: u64) -> String {
    format!("{n:03}{}", "k".repeat(2_000))
}

#[test]
fn past_its_cap_it_evicts_the_keys_full_soonest() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 10);
    // Keys spending 1 to 5 of their bucket: full again after 100 to 500 ms.
    for spent in 1..=5_u64 {
        let key = format!("k{spent}");
        for _ in 0..spent {
            assert!(one(&local, &mut handles, key.as_bytes(), c, 0));
        }
    }
    let each = key_bytes(b"k1", c.into());
    let tables = local.tables();
    // Nine tenths of the cap keeps two, those full latest.
    local.set_capacity(room_for(&local, 2, each));
    let swept = local.sweep(0, SECOND);
    assert_eq!(swept.evicted, 3);
    let kept: Vec<bool> = (1..=5)
        .map(|spent| local.contains(format!("k{spent}").as_bytes()))
        .collect();
    assert_eq!(kept, [false, false, false, true, true]);
    assert_eq!(local.evicted(), 3);
    assert_eq!(
        local.holding(),
        tables + 5 * each + local.gone_room(),
        "the three evicted counted while the worker's handles hold them"
    );
    handles.prune();
    assert_eq!(
        local.evict(0),
        0,
        "freed once let go of, and within the cap"
    );
    assert_eq!(local.holding(), tables + 2 * each);
}

/// The key of `name` in `local`'s registry.
fn registered(local: &Local<FixedState>, name: &[u8]) -> Arc<Key> {
    let hash = local.hasher.hash_one(name);
    let held = read(local.shard(hash)).get_hashed(hash, name).cloned();
    held.expect("registered")
}

/// A key evicted counts until the last handle to it goes, however many workers hold it, and
/// is freed at the next look once none does.
#[test]
fn an_evicted_key_counts_until_the_last_handle_to_it_goes() {
    let local = local();
    let mut first = Handles::new(FixedState::default());
    let mut second = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 10);
    assert!(one(&local, &mut first, b"k", c, 0));
    assert!(one(&local, &mut second, b"k", c, 0));
    let held = Arc::downgrade(&registered(&local, b"k"));
    let tables = local.tables();
    local.set_capacity(1);
    assert_eq!(local.evict(0), 1);
    assert!(!local.contains(b"k"));
    let counted = tables + key_bytes(b"k", c.into()) + local.gone_room();
    assert_eq!(local.holding(), counted, "held by both workers");
    first.prune();
    assert_eq!(local.evict(0), 0, "no key left to evict");
    assert_eq!(local.holding(), counted, "still held by the second");
    assert!(held.upgrade().is_some());
    second.prune();
    assert_eq!(local.evict(0), 0);
    assert!(held.upgrade().is_none(), "freed");
    assert_eq!(local.holding(), tables);
}

/// Past the cap with keys evicted earlier still held, eviction does not evict again for
/// them: they count in what the process holds until they are freed, but the cap is held by
/// the keys kept, so the looks while they wait evict nothing more — not a tenth more each
/// look (#602).
#[test]
fn keys_evicted_and_still_held_are_not_evicted_for_again() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(20, SECOND, 20);
    // Keys spending 1 to 12 of their bucket: full again after 50 to 600 ms.
    for spent in 1..=12_u64 {
        for _ in 0..spent {
            assert!(one(&local, &mut handles, long_name(spent).as_bytes(), c, 0));
        }
    }
    let each = key_bytes(long_name(1).as_bytes(), c.into());
    let tables = local.tables();
    let cap = room_for(&local, 8, each);
    local.set_capacity(cap);
    let evicted = local.evict(0);
    assert!(evicted > 0, "past the cap");
    let kept = 12 - evicted;
    assert!(
        local.holding() > cap,
        "the evicted counted while the worker's handles hold them"
    );
    for look in 1..=9 {
        assert_eq!(local.evict(look * SECOND / 10), 0, "look {look}");
    }
    assert_eq!(local.key_count() as u64, kept);
    handles.prune();
    assert_eq!(local.evict(SECOND), 0);
    assert_eq!(local.holding(), tables + kept * each);
}

/// Eviction leaves room for the keys made over the last `HANDLES_PRUNED`, what workers may
/// go on holding of keys let go of: keys made in a burst count at once, a process within its
/// cap by its keys alone evicting for them.
#[test]
fn eviction_leaves_room_for_the_keys_made_lately() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(20, SECOND, 20);
    for n in 0..6 {
        assert!(one(&local, &mut handles, long_name(n).as_bytes(), c, 0));
    }
    let each = key_bytes(long_name(0).as_bytes(), c.into());
    let cap = room_for(&local, 11, each);
    local.set_capacity(cap);
    assert_eq!(
        local.evict(0),
        0,
        "the first look, which knows of none made lately"
    );
    for n in 6..10 {
        assert!(one(&local, &mut handles, long_name(n).as_bytes(), c, 0));
    }
    assert!(local.holding() <= cap, "within the cap by the keys alone");
    let lately = 4 * each;
    let needed = local.holding() + lately - (cap - cap / 10);
    assert_eq!(
        local.evict(SECOND / 2),
        needed.div_ceil(each),
        "room left for the four"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(256)
    ))]

    /// Past the cap, eviction takes the keys full soonest, to within a 1,024th of the span of
    /// their times until full, down to nine tenths of the cap and no key past it; within the
    /// cap, none.
    #[test]
    fn eviction_takes_the_keys_full_soonest_to_within_a_step(
        spends in proptest::collection::vec(1..=20_u64, 1..40),
        share in 0.0..1.0_f64,
    ) {
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let c = contract(20, SECOND, 20);
        let names: Vec<String> = (0..spends.len() as u64).map(long_name).collect();
        for (name, spent) in names.iter().zip(&spends) {
            for _ in 0..*spent {
                prop_assert!(one(&local, &mut handles, name.as_bytes(), c, 0));
            }
        }
        let each = key_bytes(names[0].as_bytes(), c.into());
        let full: Vec<u64> = names
            .iter()
            .map(|name| registered(&local, name.as_bytes()).full_in(0))
            .collect();
        let room = (share * names.len() as f64) as u64;
        let cap = room_for(&local, room, each);
        let past = local.holding() > cap;
        local.set_capacity(cap);
        local.evict(0);
        let kept: Vec<bool> = names.iter().map(|name| local.contains(name.as_bytes())).collect();
        let expected = if past { room } else { names.len() as u64 };
        prop_assert_eq!(kept.iter().filter(|k| **k).count() as u64, expected);
        // A range's step over the span of when the keys will be full: 1,024 of them.
        let spread = full.iter().max().unwrap_or(&0) - full.iter().min().unwrap_or(&0);
        let step = spread / 1_024 + 1;
        let fulls = |want: bool| {
            full.iter()
                .zip(&kept)
                .filter(move |(_, k)| **k == want)
                .map(|(full, _)| *full)
        };
        if let (Some(last), Some(first)) = (fulls(false).max(), fulls(true).min()) {
            prop_assert!(last < first + step, "evicted up to {}, kept from {}", last, first);
        }
    }
}

/// With many keys, eviction finds where to stop from a sample of the shards, each key read
/// counted for the shards not read: it still takes the keys full soonest, and as many as the
/// cap needs to within a twentieth.
#[test]
fn eviction_from_a_sample_takes_the_keys_full_soonest_and_about_as_many() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(20, SECOND, 20);
    let keys = cutoff::SAMPLED_FROM as u64;
    let name = |n: u64| format!("key{n:08}");
    // Odd spends of 1 to 15, mixed over the keys: full again after 50 to 750 ms.
    let spent = |n: u64| (n.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 59) & 15 | 1;
    for n in 0..keys {
        for _ in 0..spent(n) {
            assert!(one(&local, &mut handles, name(n).as_bytes(), c, 0));
        }
    }
    assert!(cutoff::stride(local.key_count()) > 1, "read from a sample");
    let each = key_bytes(name(0).as_bytes(), c.into());
    let room = keys / 2;
    local.set_capacity(room_for(&local, room, each));
    let evicted = local.evict(0);
    let kept = keys - evicted;
    assert!(
        kept.abs_diff(room) <= room / 20,
        "kept {kept} keys, room for {room}"
    );
    let local = &local;
    let spends = |kept: bool| {
        (0..keys)
            .filter(move |n| local.contains(name(*n).as_bytes()) == kept)
            .map(spent)
    };
    // Full soonest: those that spent least, every one evicted before any that spent more.
    let (most_evicted, least_kept) = (spends(false).max(), spends(true).min());
    assert!(
        most_evicted <= least_kept,
        "evicted keys that spent up to {most_evicted:?}, kept from {least_kept:?}"
    );
}

/// Eviction alone, as the sweep's thread takes it between sweeps: past the cap it evicts the
/// keys full soonest, down to nine tenths, and forgets no key it is not evicting; within the
/// cap it evicts none, however many could be forgotten.
#[test]
fn eviction_alone_evicts_past_the_cap_and_forgets_nothing() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 10);
    for spent in 1..=5_u64 {
        let key = format!("k{spent}");
        for _ in 0..spent {
            assert!(one(&local, &mut handles, key.as_bytes(), c, 0));
        }
    }
    let each = key_bytes(b"k1", c.into());
    let tables = local.tables();
    // Every key full and quiet by then: a sweep would forget them all.
    let later = 10 * SECOND;
    local.set_capacity(tables + 5 * each);
    assert_eq!(local.evict(later), 0, "within the cap");
    assert_eq!(local.key_count(), 5);
    local.set_capacity(room_for(&local, 2, each));
    assert_eq!(local.evict(0), 3);
    let kept: Vec<bool> = (1..=5)
        .map(|spent| local.contains(format!("k{spent}").as_bytes()))
        .collect();
    assert_eq!(kept, [false, false, false, true, true]);
    assert_eq!(local.evicted(), 3);
    assert_eq!(
        local.holding(),
        tables + 5 * each + local.gone_room(),
        "the three evicted counted while the worker's handles hold them"
    );
    handles.prune();
    assert_eq!(
        local.evict(0),
        0,
        "freed once let go of, and within the cap"
    );
    assert_eq!(local.holding(), tables + 2 * each);
}

#[test]
fn a_key_quiet_for_longer_than_the_clock_wraps_is_full() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    // One a nanosecond: 2^16 units a nanosecond, so the 63-bit clock comes round every
    // 2^47 ns (39 hours). Then the word reads a request's interval ahead, refusing, unless
    // the key's last use says it is full.
    let c = contract(1, 1, 1);
    assert!(one(&local, &mut handles, b"k", c, 0));
    assert!(one(&local, &mut handles, b"k", c, 1 << 47));
}

#[test]
fn a_workers_handles_drop_keys_forgotten() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert!(one(&local, &mut handles, b"k", c, 0));
    assert_eq!(handles.len(), 1);
    local.sweep(10 * SECOND, 0);
    handles.prune();
    assert_eq!(handles.len(), 0);
}

/// A worker lets go of the keys forgotten a part of its handles at a time, so that no one
/// pass reads them all: one pass reads one part, and as many passes as there are parts read
/// every one.
#[test]
fn a_workers_handles_drop_keys_forgotten_a_part_at_a_time() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    let keys = 64 * 100;
    for k in 0..keys {
        assert!(one(&local, &mut handles, format!("k{k}").as_bytes(), c, 0));
    }
    local.sweep(10 * SECOND, 0);
    handles.prune_next();
    let left = handles.len();
    assert!(
        left < keys && left > keys - keys / 32,
        "one part, of about a 64th, let go: {left} of {keys} left"
    );
    for _ in 1..HANDLE_PARTS {
        handles.prune_next();
    }
    assert_eq!(handles.len(), 0);
}

/// Times at which a contract's units are whole: there the limiter's arithmetic is exact,
/// and its decisions are the reference bucket's, one for one.
fn exact_times(c: &Contract, steps: &[u64]) -> Vec<u64> {
    let (limit, window) = (c.limit.get(), c.window.get());
    let step = window / gcd(window, limit);
    let mut at = 0;
    steps
        .iter()
        .map(|gap| {
            at += gap * step;
            at
        })
        .collect()
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

proptest! {
    #[test]
    fn one_key_decides_as_the_reference_bucket(
        limit in 1_u64..1_000,
        window_ms in 1_u64..2_000,
        burst in 1_u64..50,
        steps in proptest::collection::vec(0_u64..4, 1..300),
    ) {
        let c = contract(limit, window_ms * MS, burst);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let mut reference = TokenBucket::new(c.limit, c.window, c.burst).with_slack(crate::SLACK);
        for now in exact_times(&c, &steps) {
            let expected = reference.decide(now);
            prop_assert_eq!(one(&local, &mut handles, b"k", c, now), expected, "at {}", now);
        }
    }

    #[test]
    fn at_any_times_it_errs_only_towards_admitting_by_less_than_one(
        limit in 1_u64..1_000,
        window_ms in 1_u64..2_000,
        burst in 1_u64..50,
        gaps in proptest::collection::vec(0_u64..50 * MS, 1..300),
    ) {
        let c = contract(limit, window_ms * MS, burst);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let mut admitted = Vec::new();
        let mut now = 0;
        for gap in gaps {
            now += gap;
            if one(&local, &mut handles, b"k", c, now) {
                admitted.push(now);
            }
        }
        // Never more than the contract allows in any span, but for one.
        let sim = (c.limit, c.window, c.burst);
        prop_assert!(most_over(sim, &admitted) <= 1);
    }

    #[test]
    fn a_client_at_its_rate_is_never_refused(
        limit in 1_u64..1_000,
        window_ms in 1_u64..2_000,
        burst in 1_u64..50,
        jitter in proptest::collection::vec(0_u64..1_000, 1..200),
    ) {
        let c = contract(limit, window_ms * MS, burst);
        let interval = (window_ms * MS).div_ceil(limit);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let mut now = 0;
        for extra in jitter {
            prop_assert!(one(&local, &mut handles, b"k", c, now), "at {}", now);
            now += interval + extra;
        }
    }
}

/// The most requests admitted at `times` over what `limit` a `window` with a bucket of
/// `burst` allows in any span: as the simulator measures it.
fn most_over((limit, window, burst): (NonZeroU64, NonZeroU64, NonZeroU64), times: &[u64]) -> i64 {
    let (window, limit, burst) = (
        i128::from(window.get()),
        i128::from(limit.get()),
        i128::from(burst.get()),
    );
    let mut lowest: Option<i128> = None;
    let mut widest = i128::MIN;
    for (k, &at) in times.iter().enumerate() {
        let g = i128::try_from(k).unwrap() * window - limit * i128::from(at);
        let low = lowest.map_or(g, |low| low.min(g));
        lowest = Some(low);
        widest = widest.max(g - low);
    }
    if widest == i128::MIN {
        return 0;
    }
    i64::try_from((widest + window - burst * window).div_euclid(window)).unwrap()
}

/// A key for the set a worker's handles and the registry's shards keep their keys in
/// (`key_set`, #111).
fn held_key(name: &[u8]) -> Arc<Key> {
    let terms = Arc::new(Terms::new(contract(10, SECOND, 10).into()));
    Arc::new(Key::new(name.into(), terms, 0, 0))
}

proptest! {
    /// Whatever is held, replaced, removed or kept, and however often the set grows on
    /// the way, in one part or in a worker's many, it finds each name's latest key, as a
    /// map of names does.
    #[test]
    fn holds_what_a_map_of_names_holds(
        ops in proptest::collection::vec((0_u8..4, 0_u16..400), 0..3_000),
    ) {
        holds_what_a_map_holds::<1>(&ops)?;
        holds_what_a_map_holds::<{ HANDLE_PARTS }>(&ops)?;
    }
}

/// `ops` on a set of `PARTS` parts and on a map of names alike, compared after each.
fn holds_what_a_map_holds<const PARTS: usize>(
    ops: &[(u8, u16)],
) -> Result<(), proptest::test_runner::TestCaseError> {
    let mut set = key_set::KeySet::<_, PARTS>::with_hasher(FixedState::default());
    let mut map: HashMap<Vec<u8>, Arc<Key>> = HashMap::new();
    for &(op, at) in ops {
        let name = format!("k{at}").into_bytes();
        match op {
            0 | 1 => {
                let made = held_key(&name);
                let hash = set.hash(&name);
                set.replace_hashed(hash, Arc::clone(&made));
                map.insert(name.clone(), made);
            }
            2 => {
                set.remove(&name);
                map.remove(&name);
            }
            _ => {
                // Keeps the names of an odd number.
                set.retain(|key| key.key.last().is_some_and(|last| last % 2 == 1));
                map.retain(|name, _| name.last().is_some_and(|last| last % 2 == 1));
            }
        }
        prop_assert_eq!(set.len(), map.len());
        prop_assert_eq!(set.is_empty(), map.is_empty());
        let found = set.get(&name).map(Arc::as_ptr);
        prop_assert_eq!(found, map.get(&name).map(Arc::as_ptr));
    }
    for (name, held) in &map {
        prop_assert!(set.get(name).is_some_and(|key| Arc::ptr_eq(key, held)));
    }
    prop_assert_eq!(set.iter().count(), map.len());
    Ok(())
}

/// A worker's handles spread over all their parts, each a fair share, so that a part that
/// doubles moves about a 64th of them.
#[test]
fn handles_spread_over_their_parts() {
    let mut set = key_set::KeySet::<_, { HANDLE_PARTS }>::with_hasher(FixedState::default());
    let keys = 64_000;
    for at in 0..keys {
        let name = format!("client-{at}").into_bytes();
        let hash = set.hash(&name);
        set.replace_hashed(hash, held_key(&name));
    }
    let lens = set.part_lens();
    assert_eq!(lens.len(), HANDLE_PARTS);
    assert_eq!(lens.iter().sum::<usize>(), keys);
    let share = keys / HANDLE_PARTS;
    assert!(
        lens.iter().all(|&len| len > share / 2 && len < share * 2),
        "{lens:?}"
    );
}

/// A request whose clock lags the key's last charge by up to the slack (`crate::SLACK`), as
/// one worker's can lag another's, is decided at its own clock and refused nothing within the
/// limit: as of that clock the bucket owes the lag's refill more, which the slack covers. Ten
/// of a burst of ten at 100 a second: one charged, then nine more half the slack behind (eight
/// without the slack).
#[test]
fn a_request_with_a_clock_lagging_by_the_slack_is_refused_nothing_within_the_limit() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(100, SECOND, 10);
    let at = 100 * MS;
    assert!(one(&local, &mut handles, b"k", c, at));
    let behind = at - crate::SLACK / 2;
    let admitted = (0..12)
        .filter(|_| one(&local, &mut handles, b"k", c, behind))
        .count();
    assert_eq!(admitted, 9);
}

/// The same through a request under several keys, which looks at each before charging any.
#[test]
fn a_request_under_several_keys_with_a_clock_lagging_by_the_slack_is_refused_nothing() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(100, SECOND, 10);
    let at = 100 * MS;
    assert!(one(&local, &mut handles, b"a", c, at));
    let both = [(&b"a"[..], c), (&b"b"[..], c)];
    let behind = at - crate::SLACK / 2;
    let admitted = (0..12)
        .filter(|_| local.decide(&mut handles, &both, behind) == Decision::Admit)
        .count();
    assert_eq!(admitted, 9);
}

/// A limit no request could reach refuses none with a clock lagging the last charge by a
/// little: the burst kept is capped (2^40), the rate is not, so at this rate the bucket and
/// the slack's refill, capped the same, cover some 200 ns of lag.
#[test]
fn an_enormous_limit_refuses_none_with_a_clock_a_little_behind() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(u64::MAX / 4, SECOND, u64::MAX / 4);
    assert!(one(&local, &mut handles, b"k", c, 1_000));
    assert!((0..100).all(|_| one(&local, &mut handles, b"k", c, 900)));
}

fn window(limit: u64, window: u64) -> Window {
    let n = |n| NonZeroU64::new(n).unwrap();
    Window {
        limit: n(limit),
        window: n(window),
    }
}

/// How far `key`'s windows are shifted in `local`: where its first window after 0 starts
/// is the window less this.
fn offset(local: &Local<FixedState>, key: &[u8], window: u64) -> u64 {
    local.hasher.hash_one(key) % window
}

/// Decides one request under `key`'s window alone.
fn one_in(
    local: &Local<FixedState>,
    handles: &mut Handles<FixedState>,
    key: &[u8],
    w: Window,
    now: u64,
) -> bool {
    local.decide(handles, &[(key, w)], now) == Decision::Admit
}

#[test]
fn a_window_admits_its_limit_in_each_of_its_windows() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let w = window(3, SECOND);
    let start = SECOND - offset(&local, b"k", SECOND);
    let admitted = |handles: &mut Handles<FixedState>, now| {
        (0..5)
            .filter(|_| one_in(&local, handles, b"k", w, now))
            .count()
    };
    assert_eq!(admitted(&mut handles, start), 3);
    assert_eq!(
        admitted(&mut handles, start + SECOND - 1),
        0,
        "the window's last moment"
    );
    assert_eq!(admitted(&mut handles, start + SECOND), 3, "the next window");
}

#[test]
fn a_window_refusal_waits_until_its_window_ends() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let w = window(1, SECOND);
    let start = SECOND - offset(&local, b"k", SECOND);
    assert!(one_in(&local, &mut handles, b"k", w, start));
    let Decision::Refuse { position: 0, wait } =
        local.decide(&mut handles, &[(b"k", w)], start + 300 * MS)
    else {
        panic!("refused");
    };
    assert_eq!(wait, 700 * MS);
    assert!(one_in(
        &local,
        &mut handles,
        b"k",
        w,
        start + 300 * MS + wait
    ));
}

/// Keys' windows are shifted by their hashes, so that their boundaries spread: two keys
/// whose offsets differ end their windows apart.
#[test]
fn keys_windows_end_at_their_own_offsets() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let w = window(1, SECOND);
    let (a, b) = (offset(&local, b"a", SECOND), offset(&local, b"b", SECOND));
    assert_ne!(a, b, "the test needs keys of different offsets");
    for (key, off) in [(&b"a"[..], a), (&b"b"[..], b)] {
        let end = 2 * SECOND - off;
        assert!(one_in(&local, &mut handles, key, w, end - 1));
        assert!(!one_in(&local, &mut handles, key, w, end - 1));
        assert!(one_in(&local, &mut handles, key, w, end));
    }
}

#[test]
fn a_quiet_window_is_forgotten_once_it_counts_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let w = window(5, SECOND);
    let start = SECOND - offset(&local, b"k", SECOND);
    assert!(one_in(&local, &mut handles, b"k", w, start));
    assert_eq!(
        local.sweep(start + SECOND - 1, 0).forgotten,
        0,
        "still counting"
    );
    assert_eq!(local.sweep(start + SECOND, 0).forgotten, 1);
}

/// A key unused for as many windows as its windows' numbers take to come round is a fresh
/// one, not the window it last counted in: its word's number is the same as the window's.
#[test]
fn a_window_unused_while_its_numbers_come_round_is_fresh() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let w = window(1, MS);
    let start = MS - offset(&local, b"k", MS);
    assert!(one_in(&local, &mut handles, b"k", w, start));
    let round = (NUMBER_MASK + 1) * MS;
    assert!(one_in(&local, &mut handles, b"k", w, start + round));
    // The same under two keys, which a decision looks at before charging either.
    let both = [(&b"a"[..], w), (&b"b"[..], w)];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_eq!(local.decide(&mut handles, &both, round), Decision::Admit);
}

#[test]
fn a_window_and_a_bucket_charge_all_or_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let both: [(&[u8], Limit); 2] = [
        (b"b", contract(1, SECOND, 1).into()),
        (b"w", window(2, 10 * SECOND).into()),
    ];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert!(matches!(
        local.decide(&mut handles, &both, 0),
        Decision::Refuse { position: 0, .. }
    ));
    // The refusal charged the window nothing: one of its two is left.
    let w = window(2, 10 * SECOND);
    assert!(one_in(&local, &mut handles, b"w", w, 0));
    assert!(!one_in(&local, &mut handles, b"w", w, 0));
}

#[test]
fn a_windows_count_saturates_in_its_bits() {
    assert_eq!(Span::count(Span::word(7, COUNT_MASK + 5), 7), COUNT_MASK);
    assert_eq!(Span::count(Span::word(7, 3), 8), 0, "another window's");
}

/// Eviction takes the keys that will count none soonest first: a window's time is to its
/// end while it counts, none once it counts none.
#[test]
fn a_window_is_counting_until_its_end() {
    let local = local();
    let w = window(5, SECOND);
    let start = SECOND - offset(&local, b"k", SECOND);
    let key = local.find_or_make(b"k", w.into(), start);
    assert_eq!(key.full_in(start), 0, "counted none");
    assert!(key.charge(start));
    assert_eq!(key.full_in(start + 400 * MS), 600 * MS);
    assert_eq!(key.full_in(start + SECOND), 0);
}

/// Times that go back as well as forward, as several workers' clocks do: each is a gap from
/// the one before, at least 0.
fn wandering(window: u64, gaps: &[i64]) -> Vec<u64> {
    let mut now: i64 = 0;
    let span = i64::try_from(window).unwrap();
    gaps.iter()
        .map(|gap| {
            now = (now + gap * span / 100).max(0);
            u64::try_from(now).unwrap()
        })
        .collect()
}

proptest! {
    /// A key's window decides as the reference's aligned to the clock, shifted by the key's
    /// offset, and a refusal waits until the reference would admit. A clock behind the key's
    /// last charge across a window's boundary is decided in the charge's window, as the
    /// reference has it, the word saying which; its wait runs from its own clock.
    #[test]
    fn one_window_decides_as_the_reference(
        limit in 1_u64..20,
        window_ms in 1_u64..2_000,
        gaps in proptest::collection::vec(-30_i64..150, 1..300),
    ) {
        let w = window(limit, window_ms * MS);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let off = offset(&local, b"k", w.window.get());
        let mut reference = FixedWindow::new(w.limit, w.window, Alignment::Clock);
        for now in wandering(w.window.get(), &gaps) {
            let shifted = now + off;
            let expected = reference.decide(shifted);
            match local.decide(&mut handles, &[(b"k", w)], now) {
                Decision::Admit => {
                    prop_assert!(expected, "admitted at {}", now);
                }
                Decision::Refuse { wait, .. } => {
                    prop_assert!(!expected, "refused at {}", now);
                    let retry = reference.retry_at(shifted).unwrap();
                    prop_assert_eq!(wait, retry - (now + off), "at {}", now);
                }
            }
        }
    }

    /// Two windows on two keys, decided together, as the references are: all or none.
    #[test]
    fn two_windows_decide_as_the_references_together(
        limits in (1_u64..10, 1_u64..10),
        windows_ms in (1_u64..500, 1_u64..500),
        gaps in proptest::collection::vec(-30_i64..150, 1..300),
    ) {
        let (a, b) = (window(limits.0, windows_ms.0 * MS), window(limits.1, windows_ms.1 * MS));
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let (off_a, off_b) = (offset(&local, b"a", a.window.get()), offset(&local, b"b", b.window.get()));
        let mut ref_a = FixedWindow::new(a.limit, a.window, Alignment::Clock);
        let mut ref_b = FixedWindow::new(b.limit, b.window, Alignment::Clock);
        let keys = [(&b"a"[..], a), (&b"b"[..], b)];
        for now in wandering(a.window.get().min(b.window.get()), &gaps) {
            let expected = ref_a.admits(now + off_a) && ref_b.admits(now + off_b);
            if expected {
                ref_a.charge(now + off_a);
                ref_b.charge(now + off_b);
            }
            let admitted = local.decide(&mut handles, &keys, now) == Decision::Admit;
            prop_assert_eq!(admitted, expected, "at {}", now);
        }
    }
}

/// The key `local` holds for `key`, if it holds one.
fn key_of(local: &Local<FixedState>, key: &[u8]) -> Option<Arc<Key>> {
    let hash = local.hasher.hash_one(key);
    read(local.shard(hash)).get_hashed(hash, key).cloned()
}

/// Every key under one limit points to one copy of the limit's arithmetic, rather than
/// keeping its own; a key under another limit, to that limit's.
#[test]
fn keys_under_one_limit_share_its_terms() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (c, other) = (contract(10, SECOND, 5), contract(20, SECOND, 5));
    assert!(one(&local, &mut handles, b"a", c, 0));
    assert!(one(&local, &mut handles, b"b", c, 0));
    assert!(one_in(&local, &mut handles, b"w", window(3, SECOND), 0));
    assert!(one(&local, &mut handles, b"c", other, 0));
    let terms = |key: &[u8]| Arc::clone(&key_of(&local, key).unwrap().terms);
    assert!(Arc::ptr_eq(&terms(b"a"), &terms(b"b")));
    assert!(!Arc::ptr_eq(&terms(b"a"), &terms(b"c")));
    assert!(!Arc::ptr_eq(&terms(b"a"), &terms(b"w")));
    assert_eq!(read(&local.terms).len(), 3);
}

/// A limit's terms are let go of once no key holds them, as after a reload that drops the
/// limit, and made again if a key under it comes back. A key forgotten still holds them
/// while a worker's handle does: they go at the sweep after the handles let go of it.
#[test]
fn the_sweep_lets_go_of_terms_no_key_holds() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let (gone, kept) = (contract(10, SECOND, 1), contract(20, SECOND, 1));
    assert!(one(&local, &mut handles, b"gone", gone, 0));
    assert!(one(&local, &mut handles, b"kept", kept, 0));
    // `gone` is full and quiet by then; `kept` was used a moment before.
    assert!(one(&local, &mut handles, b"kept", kept, 2 * SECOND));
    assert_eq!(local.sweep(2 * SECOND, SECOND).forgotten, 1);
    assert!(key_of(&local, b"gone").is_none());
    assert_eq!(read(&local.terms).len(), 2, "a handle still holds `gone`");
    handles.prune();
    local.sweep(2 * SECOND, SECOND);
    assert_eq!(read(&local.terms).len(), 1, "only `kept`'s terms are left");
    assert!(one(&local, &mut handles, b"gone", gone, 3 * SECOND));
    assert_eq!(read(&local.terms).len(), 2);
}

/// A key holds its name, a pointer to its limit's terms, its word, its last use and its
/// window's offset: no arithmetic of its own (#259).
#[test]
fn a_key_holds_no_arithmetic_of_its_own() {
    assert_eq!(size_of::<Key>(), 48);
}
