//! Local mode's rolling window against the slot rule it keeps — the synchronous store's, a
//! request counted in its slot, the window the slots from the latest back — and in the
//! registry: several limits, forgetting, eviction, bytes.

#![cfg(not(loom))]

use super::super::tests::{contract, local};
use super::super::*;
use super::{MOST_SLOTS, NARROW_BELOW};
use crate::service::FixedState;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;

fn rolling(limit: u64, window: u64, slots: u64) -> Rolling {
    let n = |n| NonZeroU64::new(n).unwrap();
    Rolling {
        limit: n(limit),
        window: n(window),
        slots: n(slots),
    }
}

/// Decides one request under `key`'s rolling window alone: admitted, or refused with its wait.
fn one(
    local: &Local<FixedState>,
    handles: &mut Handles<FixedState>,
    key: &[u8],
    r: Rolling,
    now: u64,
) -> Result<(), u64> {
    match local.decide(handles, &[(key, r)], now) {
        Decision::Admit => Ok(()),
        Decision::Refuse { wait, .. } => Err(wait),
    }
}

/// The slot rule as plainly as it goes: each admission counted in its slot, the window the
/// latest slot and those before it, `slots` in all; a request admitted while they count
/// fewer than the limit, else refused until enough of the oldest have left.
struct Slotted {
    limit: u64,
    len: u64,
    slots: u64,
    counts: BTreeMap<u64, u64>,
}

impl Slotted {
    fn new(r: Rolling) -> Self {
        let slots = r.slots.get();
        Self {
            limit: r.limit.get(),
            len: (r.window.get() / slots).max(1),
            slots,
            counts: BTreeMap::new(),
        }
    }

    fn decide(&mut self, now: u64) -> Result<(), u64> {
        let slot = now / self.len;
        let oldest = slot.saturating_sub(self.slots - 1);
        self.counts.retain(|held, _| *held >= oldest);
        let total: u64 = self.counts.values().sum();
        if total < self.limit {
            *self.counts.entry(slot).or_insert(0) += 1;
            return Ok(());
        }
        let mut left = 0;
        for (x, count) in &self.counts {
            left += count;
            if total - left < self.limit {
                return Err((x + self.slots) * self.len - now);
            }
        }
        unreachable!("the counts add up to the total")
    }
}

#[test]
fn a_rolling_window_admits_its_limit_in_any_window() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(3, SECOND, 1_000);
    for at in [0, 400 * MS, 800 * MS] {
        assert_eq!(one(&local, &mut handles, b"k", r, at), Ok(()));
    }
    assert!(one(&local, &mut handles, b"k", r, 999 * MS).is_err());
    assert_eq!(
        one(&local, &mut handles, b"k", r, SECOND),
        Ok(()),
        "the first has left"
    );
    assert!(
        one(&local, &mut handles, b"k", r, 1_100 * MS).is_err(),
        "the second has not"
    );
    assert_eq!(one(&local, &mut handles, b"k", r, 1_400 * MS), Ok(()));
}

#[test]
fn a_rolling_refusal_waits_until_its_oldest_admission_leaves() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(2, SECOND, 1_000);
    assert_eq!(one(&local, &mut handles, b"k", r, 250 * MS), Ok(()));
    assert_eq!(one(&local, &mut handles, b"k", r, 600 * MS), Ok(()));
    let wait = one(&local, &mut handles, b"k", r, 700 * MS).unwrap_err();
    assert_eq!(wait, 550 * MS, "until the admission at 250 ms leaves");
    assert!(one(&local, &mut handles, b"k", r, 700 * MS + wait - 1).is_err());
    assert_eq!(one(&local, &mut handles, b"k", r, 700 * MS + wait), Ok(()));
}

/// With fewer slots, an admission counts from its slot's start, so its room comes back up
/// to a slot early: never late.
#[test]
fn fewer_slots_bring_room_back_up_to_a_slot_early() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(1, SECOND, 10);
    assert_eq!(one(&local, &mut handles, b"k", r, 950 * MS), Ok(()));
    assert_eq!(
        one(&local, &mut handles, b"k", r, 1_000 * MS),
        Err(900 * MS),
        "its slot, from 900 ms, leaves at 1.9 s"
    );
    assert!(one(&local, &mut handles, b"k", r, 1_900 * MS - 1).is_err());
    assert_eq!(one(&local, &mut handles, b"k", r, 1_900 * MS), Ok(()));
}

/// A key idle for as many windows as its entries' laps take to come round (4,096 for the
/// 4-byte entries) reads none of its old counts as new ones: its slots are emptied as it
/// takes them off. Read as new, the count at 3 ms would halve the wait.
#[test]
fn a_key_idle_while_its_laps_come_round_reads_no_old_count() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(1, 10 * MS, 10);
    assert_eq!(one(&local, &mut handles, b"k", r, 3 * MS), Ok(()));
    let round = 4_096 * 10 * MS;
    let at = round + 8 * MS;
    assert_eq!(one(&local, &mut handles, b"k", r, at), Ok(()));
    assert_eq!(
        one(&local, &mut handles, b"k", r, at),
        Err(10 * MS),
        "until the admission just made leaves"
    );
}

/// Under a clock behind the key's last decision by the slack, as one worker's is behind
/// another's, a request within the limit is still admitted.
#[test]
fn a_request_with_a_clock_lagging_by_the_slack_is_refused_nothing_within_the_limit() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(10, SECOND, 1_000);
    let at = 100 * MS;
    assert_eq!(one(&local, &mut handles, b"k", r, at), Ok(()));
    let behind = at - crate::SLACK / 2;
    let admitted = (0..12)
        .filter(|_| one(&local, &mut handles, b"k", r, behind).is_ok())
        .count();
    assert_eq!(admitted, 9);
}

#[test]
fn a_rolling_window_and_a_bucket_charge_all_or_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let both: [(&[u8], Limit); 2] = [
        (b"b", contract(1, SECOND, 1).into()),
        (b"r", rolling(2, 10 * SECOND, 1_000).into()),
    ];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert!(matches!(
        local.decide(&mut handles, &both, 0),
        Decision::Refuse { position: 0, .. }
    ));
    // The refusal charged the rolling window nothing: one of its two is left.
    let r = rolling(2, 10 * SECOND, 1_000);
    assert_eq!(one(&local, &mut handles, b"r", r, 0), Ok(()));
    assert!(one(&local, &mut handles, b"r", r, 0).is_err());
}

/// A rolling window refusing under several limits says its own wait, the longest.
#[test]
fn a_rolling_window_refusing_beside_a_bucket_says_its_wait() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(1, 10 * SECOND, 1_000);
    let both: [(&[u8], Limit); 2] = [
        (b"b", contract(1_000, SECOND, 1_000).into()),
        (b"r", r.into()),
    ];
    assert_eq!(local.decide(&mut handles, &both, 0), Decision::Admit);
    assert_eq!(
        local.decide(&mut handles, &both, 4 * SECOND),
        Decision::Refuse {
            position: 1,
            wait: 6 * SECOND
        }
    );
}

/// The clock some windows on from its origin, where a window's slots all lie after it.
const LATER: u64 = 10 * SECOND;

#[test]
fn a_quiet_rolling_window_is_forgotten_once_it_counts_none() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(5, SECOND, 1_000);
    assert_eq!(one(&local, &mut handles, b"k", r, LATER), Ok(()));
    assert_eq!(
        local.sweep(LATER + SECOND - 1, 0).forgotten,
        0,
        "still counting"
    );
    assert_eq!(local.sweep(LATER + SECOND, 0).forgotten, 1);
    assert_eq!(local.holding(), 0);
}

/// One counting none but used lately is kept until it has been quiet for the time asked,
/// from the end of the slot it was last used in.
#[test]
fn a_rolling_window_used_lately_is_kept() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(5, SECOND, 1_000);
    assert_eq!(one(&local, &mut handles, b"k", r, LATER), Ok(()));
    let used = LATER + MS;
    assert_eq!(
        local.sweep(used + SECOND - 1, SECOND).forgotten,
        0,
        "quiet for a moment less than asked"
    );
    assert_eq!(local.sweep(used + SECOND, SECOND).forgotten, 1);
}

/// Eviction takes the keys that will count none soonest first: a rolling window's time is
/// until its newest counted slot leaves.
#[test]
fn a_rolling_window_is_counting_until_its_newest_slot_leaves() {
    let local = local();
    let r = rolling(5, SECOND, 1_000);
    let key = local.find_or_make(b"k", r.into(), 0);
    assert_eq!(key.full_in(0), 0, "counted none");
    assert!(key.charge(100 * MS));
    assert!(key.charge(300 * MS));
    assert_eq!(
        key.full_in(400 * MS),
        900 * MS,
        "until the one at 300 ms leaves"
    );
    assert_eq!(key.full_in(1_300 * MS), 0);
}

/// A rolling key is counted with its own terms and its ring: a count a slot, 4 bytes below
/// 2^19, 8 from it.
#[test]
fn a_rolling_key_counts_its_ring_in_its_bytes() {
    let bucket = key_bytes(b"k", contract(1, SECOND, 1).into(), 1);
    let ring = |limit, slots| key_bytes(b"k", rolling(limit, SECOND, slots).into(), 1) - bucket;
    let own = (2 * size_of::<usize>() + size_of::<Terms>() + size_of::<Ring>()) as u64;
    assert_eq!(ring(100, 1_000), own + 4_000);
    assert_eq!(ring(100, 60), own + 240);
    assert_eq!(ring(NARROW_BELOW, 1_000), own + 8_000);
    assert_eq!(ring(NARROW_BELOW - 1, 1_000), own + 4_000);
    assert_eq!(ring(100, 5_000), own + 4 * MOST_SLOTS, "at most a thousand");
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(100, SECOND, 60);
    assert_eq!(one(&local, &mut handles, b"k", r, 0), Ok(()));
    assert_eq!(local.holding(), key_bytes(b"k", r.into(), 1));
    local.sweep(10 * SECOND, 0);
    assert_eq!(local.holding(), 0);
}

/// A ring counts in entries of 4 bytes below 2^19, of 8 from it, as its bytes say.
#[test]
fn entries_are_4_bytes_below_2_19() {
    for (limit, four) in [(NARROW_BELOW - 1, true), (NARROW_BELOW, false)] {
        let ring = Ring::new(rolling(limit, SECOND, 10));
        assert_eq!(
            matches!(ring.cells, super::Cells::Narrow(_)),
            four,
            "{limit}"
        );
    }
}

/// A key charged past its limit, as requests under several limits racing can leave it,
/// waits until enough of its counts have left for one more to fit: here two of three.
#[test]
fn a_key_charged_past_its_limit_waits_until_enough_have_left() {
    let local = local();
    let r = rolling(2, SECOND, 1_000);
    let key = local.find_or_make(b"k", r.into(), 0);
    for at in [100 * MS, 200 * MS, 300 * MS] {
        assert!(key.charge(at));
    }
    let Look::Full { wait } = key.look(400 * MS) else {
        panic!("refused");
    };
    assert_eq!(wait, 800 * MS, "until the one at 200 ms leaves");
}

/// Each rolling key keeps its ring in terms of its own, kept from the registry's map of
/// limits' terms: two keys under one limit count apart.
#[test]
fn rolling_keys_keep_terms_of_their_own() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    let r = rolling(1, SECOND, 1_000);
    assert_eq!(one(&local, &mut handles, b"a", r, 0), Ok(()));
    assert_eq!(one(&local, &mut handles, b"b", r, 0), Ok(()));
    let terms = |key: &[u8]| {
        let hash = local.hasher.hash_one(key);
        let held = read(local.shard(hash)).get_hashed(hash, key).cloned();
        Arc::clone(&held.unwrap().terms)
    };
    assert!(!Arc::ptr_eq(&terms(b"a"), &terms(b"b")));
    assert!(read(&local.terms).is_empty());
}

/// A key held to other slots, or to another limit, is a new key: counted afresh.
#[test]
fn a_rolling_window_of_other_slots_starts_the_key_afresh() {
    let local = local();
    let mut handles = Handles::new(FixedState::default());
    assert_eq!(
        one(&local, &mut handles, b"k", rolling(1, SECOND, 1_000), 0),
        Ok(())
    );
    assert!(one(&local, &mut handles, b"k", rolling(1, SECOND, 1_000), 0).is_err());
    assert_eq!(
        one(&local, &mut handles, b"k", rolling(1, SECOND, 60), 0),
        Ok(())
    );
    assert_eq!(
        local.holding(),
        key_bytes(b"k", rolling(1, SECOND, 60).into(), 1)
    );
}

/// Gaps between requests, mostly within a window, now and then an idle stretch of up to
/// three windows, or one as long as the 4-byte entries' laps take to come round: in
/// hundredths of a window.
fn gaps() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(
        prop_oneof![
            8 => 0_u64..30,
            2 => 0_u64..300,
            1 => 409_500_u64..409_800,
        ],
        1..300,
    )
}

proptest! {
    /// One key decides as the slot rule does, admissions and waits alike, for any limit and
    /// slots, under both widths of entry, over idle stretches long enough for the entries'
    /// laps to come round.
    #[test]
    fn one_rolling_window_decides_as_the_slot_rule(
        limit in prop_oneof![1_u64..20, Just(NARROW_BELOW)],
        slots in prop_oneof![Just(1_u64), 2_u64..12, Just(60), Just(MOST_SLOTS)],
        window_ms in 1_u64..2_000,
        gaps in gaps(),
    ) {
        let r = rolling(limit, window_ms * MS, slots);
        let local = local();
        let mut handles = Handles::new(FixedState::default());
        let mut slotted = Slotted::new(r);
        let mut now = 0;
        for gap in gaps {
            now += gap * r.window.get() / 100;
            let expected = slotted.decide(now);
            prop_assert_eq!(one(&local, &mut handles, b"k", r, now), expected, "at {}", now);
            let key = local.find_or_make(b"k", r.into(), now);
            let Held::Rolling(ring) = &key.terms.held else {
                panic!("a rolling key");
            };
            let (total, counted) = ring.standing(&key.state, now);
            prop_assert_eq!(total, counted, "the total is what the slots count, at {}", now);
        }
    }
}
