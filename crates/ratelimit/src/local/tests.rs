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
        local.decide_saying(&mut handles, &three, 0),
        (
            Decision::Refuse {
                position: 0,
                wait: 100 * MS
            },
            0b101
        )
    );
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
    assert_eq!(
        local.decide_saying(&mut handles, &[(b"k", c)], 0),
        (
            Decision::Refuse {
                position: 0,
                wait: 100 * MS
            },
            1
        )
    );
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

/// A request whose clock is behind the key's last charge, as one worker's can be behind
/// another's, is decided as of that charge (#108): at its own time it would see the charge as
/// still to come, and the bucket emptier than it is by the refill between.
#[test]
fn a_request_with_a_clock_behind_the_keys_is_decided_as_of_its_last_charge() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(100, SECOND, 10);
    assert!(one(&local, &mut handles, b"k", c, 100 * MS));
    let admitted = (0..12)
        .filter(|_| one(&local, &mut handles, b"k", c, 0))
        .count();
    assert_eq!(admitted, 9);
}

/// The same through a request under several keys, which looks at each before charging any.
#[test]
fn a_request_under_several_keys_with_a_clock_behind_is_decided_as_of_the_last_charge() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(100, SECOND, 10);
    assert!(one(&local, &mut handles, b"a", c, 100 * MS));
    let both = [(&b"a"[..], c), (&b"b"[..], c)];
    let admitted = (0..12)
        .filter(|_| local.decide(&mut handles, &both, 0) == Decision::Admit)
        .count();
    assert_eq!(admitted, 9);
}

/// A limit no request could reach refuses none, even with a clock behind: the burst kept is
/// capped, the rate is not, so a microsecond's refill is more than the whole bucket (#108).
#[test]
fn an_enormous_limit_refuses_none_with_a_clock_behind() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let c = contract(u64::MAX / 4, SECOND, u64::MAX / 4);
    assert!(one(&local, &mut handles, b"k", c, 1_000));
    assert!((0..100).all(|_| one(&local, &mut handles, b"k", c, 0)));
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
    /// offset, and a refusal waits until the reference would admit, a clock behind the key's
    /// last charge decided as of that charge, as the reference has it.
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
        let mut last = 0;
        for now in wandering(w.window.get(), &gaps) {
            let shifted = now + off;
            let expected = reference.decide(shifted);
            match local.decide(&mut handles, &[(b"k", w)], now) {
                Decision::Admit => {
                    prop_assert!(expected, "admitted at {}", now);
                    last = last.max(now);
                }
                Decision::Refuse { wait, .. } => {
                    prop_assert!(!expected, "refused at {}", now);
                    let retry = reference.retry_at(shifted).unwrap();
                    prop_assert_eq!(wait, retry - (now.max(last) + off), "at {}", now);
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
