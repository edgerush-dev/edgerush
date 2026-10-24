//! Local mode's limiter against the reference bucket it keeps (`reference::TokenBucket`),
//! and its registry: keys made, shared, forgotten and evicted.

#![cfg(not(loom))]

use super::*;
use crate::reference::{Model, TokenBucket};
use crate::service::FixedState;
use proptest::prelude::*;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

fn contract(limit: u64, window: u64, burst: u64) -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap();
    Contract {
        limit: n(limit),
        window: n(window),
        burst: n(burst),
    }
}

fn local() -> Local<FixedState> {
    Local::new(FixedState::default(), 1)
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
    // Never short, so that a client waiting as long finds room; long by less than a unit.
    let Decision::Refuse { position: 0, wait } = local.decide(&mut handles, &[(b"k", c)], 30 * MS)
    else {
        panic!("refused");
    };
    assert!((70 * MS..70 * MS + 2_000).contains(&wait), "{wait}");
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
    assert_eq!(
        local.decide(&mut handles, &both, 0),
        Decision::Refuse {
            position: 1,
            wait: 100 * MS
        }
    );
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
    assert_eq!(
        local.decide(&mut handles, &both, 0),
        Decision::Refuse {
            position: 1,
            wait: SECOND
        }
    );
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
    assert_eq!(
        local.refused_by(&mut handles, &three, 0),
        [(0, 100 * MS), (2, 100 * MS)]
    );
    // The roomy one still has its four.
    let left = (0..5)
        .filter(|_| one(&local, &mut handles, b"b", roomy, 0))
        .count();
    assert_eq!(left, 4);
}

#[test]
fn of_limits_refusing_as_long_the_first_is_named() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    let both: [(&[u8], Contract); 2] = [(b"one", c), (b"other", c)];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_eq!(
        local.decide(&mut handles, &both, 0),
        Decision::Refuse {
            position: 0,
            wait: 100 * MS
        }
    );
}

proptest! {
    /// However its interval divides into nanoseconds, a client waiting as long as it was
    /// told finds room.
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
        let Decision::Refuse { wait, .. } = local.decide(&mut handles, &[(b"k", c)], at) else {
            return Err(TestCaseError::fail("refused once spent"));
        };
        prop_assert!(one(&local, &mut handles, b"k", c, at + wait), "after {}", wait);
    }
}

#[test]
fn bytes_are_counted_as_keys_come_and_go() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(10, SECOND, 1);
    assert_eq!(local.holding(), 0);
    assert!(one(&local, &mut handles, b"k1", c, 0));
    let one_key = local.holding();
    assert_eq!(one_key, key_bytes(b"k1", 1));
    assert!(one(&local, &mut handles, b"k22", c, 0));
    assert_eq!(local.holding(), one_key + key_bytes(b"k22", 1));
    local.sweep(10 * SECOND, 0);
    assert_eq!(local.holding(), 0);
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
    let each = key_bytes(b"k1", 1);
    // Room for three keys: nine tenths of it keeps two, those full latest.
    local.set_capacity(3 * each);
    let swept = local.sweep(0, SECOND);
    assert_eq!(swept.evicted, 3);
    let kept: Vec<bool> = (1..=5)
        .map(|spent| local.contains(format!("k{spent}").as_bytes()))
        .collect();
    assert_eq!(kept, [false, false, false, true, true]);
    assert_eq!(local.holding(), 2 * each);
    assert_eq!(local.evicted(), 3);
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
        let mut reference = TokenBucket::new(c.limit, c.window, c.burst);
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
