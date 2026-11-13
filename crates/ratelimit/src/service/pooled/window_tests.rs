//! Tests of the pooled participant under a fixed window (06 §4): each window an entry of its
//! own, the key's record carried from one to the next, answers rebasing only their own
//! window's copy, and the cycle's share of what the copy has left.

#![cfg(not(loom))]

use super::*;
use crate::service::{FixedState, FixedWindow};
use proptest::prelude::*;
use std::collections::HashSet;
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
const MINUTE: u64 = 60_000 * MS;
const KEY: &[u8] = b"api/0\0client";

/// `limit` a minute, a fixed window.
fn window(limit: u64) -> Limit {
    let n = |n| NonZeroU64::new(n).unwrap();
    Limit::FixedWindow(FixedWindow {
        limit: n(limit),
        length: n(MINUTE),
    })
}

fn participant() -> Participant<FixedState> {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    Participant::new(7, CYCLE, view, FixedState::default()).unwrap()
}

/// `participant`'s entry of `KEY`.
fn entry(participant: &Participant<FixedState>) -> Arc<Entry> {
    Arc::clone(&participant.keys.get(KEY).unwrap().entry)
}

/// `count` requests on `KEY` under `limit` at `now`: how many were admitted.
fn requests(participant: &mut Participant<FixedState>, limit: Limit, count: u64, now: u64) -> u64 {
    (0..count)
        .filter(|_| participant.decide_all(&[(KEY, limit)], now) == Decision::Admit)
        .count() as u64
}

/// The hits the participant sends, each report's number, count and window.
fn sent(participant: &mut Participant<FixedState>) -> Vec<(u64, u64, u64)> {
    participant
        .take_outgoing()
        .into_iter()
        .flat_map(|(_, batch)| batch.entries)
        .filter_map(|entry| match entry.report {
            Report::Hits {
                sequence,
                count,
                window,
                ..
            } => Some((sequence, count, window)),
            Report::Poll => None,
        })
        .collect()
}

/// The owner's answer about `KEY` reaches `participant` at `now`.
fn answer(
    participant: &mut Participant<FixedState>,
    standing: Standing,
    applied: u64,
    others: u64,
    now: u64,
) {
    let reply = Reply {
        from: 0,
        via: None,
        life: 0,
        number: 0,
        made: 0,
        entries: vec![Replied {
            key: participant.hash(KEY),
            standing,
            applied,
            others,
        }],
        routing: None,
    };
    participant.on_reply(reply, now);
}

proptest! {
    /// Every moment is in one window: the window ends after it and began a window's length
    /// before its end, the moment before its end is in it, and its end is in the next.
    #[test]
    fn every_moment_is_in_one_window(hash in any::<u64>(), length in 1u64..1 << 50, now in 0u64..1 << 60) {
        let (number, ends) = word::window_at(hash, length, now);
        prop_assert!(number >= 1);
        prop_assert!(now < ends && ends - now <= length);
        prop_assert_eq!(word::window_at(hash, length, ends - 1), (number, ends));
        prop_assert_eq!(word::window_at(hash, length, ends), (number + 1, ends + length));
    }
}

/// Alone on a key, a data plane admits the window's limit and refuses the next request until
/// the window ends; the next window admits again, in an entry of its own.
#[test]
fn a_window_admits_its_limit_and_refuses_until_it_ends() {
    let limit = window(5);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 6, MS), 5);
    let first = entry(&participant);
    let ends = first.ends;
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], 2 * MS),
        Some((0, ends - 2 * MS)),
        "until the window ends"
    );
    assert_eq!(requests(&mut participant, limit, 1, ends - 1), 0);
    assert_eq!(
        requests(&mut participant, limit, 6, ends),
        5,
        "the next window"
    );
    let next = entry(&participant);
    assert!(!Arc::ptr_eq(&first, &next));
    assert_eq!(next.window, first.window + 1);
    assert!(first.closed());
}

/// The next window's entry takes the key's record on: what the old window admitted and had
/// not sent goes as a report of its window, numbered on and due at once, ahead of the new
/// window's; the reports still out stay out; the others admitting carry over.
#[test]
fn the_next_windows_entry_takes_the_keys_record_on() {
    let limit = window(100);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 3, MS), 3);
    let old = entry(&participant);
    let number = old.window;
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [(1, 3, number)]);
    answer(
        &mut participant,
        Standing::Window { number, count: 3 },
        0,
        2,
        CYCLE + MS,
    );
    // One more just before the window ends, not yet drained; then the next window.
    let ends = old.ends;
    assert_eq!(requests(&mut participant, limit, 1, ends - 1), 1);
    assert_eq!(requests(&mut participant, limit, 2, ends), 2);
    let next = entry(&participant);
    assert_eq!(next.window, number + 1);
    assert_eq!(
        next.others.load(Ordering::Acquire),
        2,
        "the others carried over"
    );
    participant.on_cycle(ends + CYCLE);
    let reports = sent(&mut participant);
    assert!(reports.contains(&(1, 3, number)), "still out: {reports:?}");
    assert!(
        reports.contains(&(2, 1, number)),
        "the old window's: {reports:?}"
    );
    assert!(
        reports.contains(&(3, 2, number + 1)),
        "the new window's: {reports:?}"
    );
}

/// An answer rebases the copy of its own window alone: the owner's count and what the owner
/// has yet to apply. Another window's answer leaves the count as it is, its others taken.
#[test]
fn an_answer_rebases_only_its_own_windows_count() {
    let limit = window(10);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 2, MS), 2);
    let number = entry(&participant).window;
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [(1, 2, number)]);
    // The owner has applied it, others' hits with it: 7 in all.
    answer(
        &mut participant,
        Standing::Window { number, count: 7 },
        1,
        1,
        CYCLE + MS,
    );
    assert_eq!(entry(&participant).snapshot().arrival, 7);
    for other in [number - 1, number + 1] {
        answer(
            &mut participant,
            Standing::Window {
                number: other,
                count: 0,
            },
            1,
            4,
            CYCLE + 2 * MS,
        );
        assert_eq!(entry(&participant).snapshot().arrival, 7, "window {other}");
        assert_eq!(entry(&participant).others.load(Ordering::Acquire), 4);
    }
    // Not yet applied by the owner, counted on top of its count.
    assert_eq!(requests(&mut participant, limit, 1, CYCLE + 3 * MS), 1);
    participant.on_cycle(2 * CYCLE);
    answer(
        &mut participant,
        Standing::Window { number, count: 7 },
        1,
        1,
        2 * CYCLE + MS,
    );
    assert_eq!(entry(&participant).snapshot().arrival, 8);
    // A bucket's answer is not about a window's copy.
    answer(
        &mut participant,
        Standing::Bucket { behind: 0 },
        1,
        9,
        2 * CYCLE + 2 * MS,
    );
    assert_eq!(entry(&participant).others.load(Ordering::Acquire), 1);
}

/// A cycle's share of a window: alone, what the copy has left; among others, its share of it,
/// at least one while the copy shows room and none once it shows none; the cycle after a
/// share keeps what it left; at most what the word holds.
#[test]
fn a_cycles_share_of_a_window_is_one_while_room_is_left() {
    let advance = |others| Advance {
        cycle: CYCLE,
        refill: 0,
        refill_units: 0,
        others,
        record: 0,
    };
    let fields = |count, left, shared| Fields {
        arrival: count,
        left,
        cycle: 0,
        shared,
        closed: false,
        unreported: 0,
        set: true,
    };
    let next =
        |limit, fields, others| word::window_advanced(limit, fields, advance(others), CYCLE).left;
    assert_eq!(
        next(1_000, fields(400, 0, false), 0),
        600,
        "alone: all that is left"
    );
    assert_eq!(
        next(1_000, fields(400, 0, false), 3),
        150,
        "a quarter of it"
    );
    assert_eq!(
        next(1_000, fields(997, 0, false), 3),
        1,
        "one while room is left"
    );
    assert_eq!(
        next(1_000, fields(1_000, 0, false), 3),
        0,
        "none with none left"
    );
    assert_eq!(next(1_000, fields(1_200, 0, false), 3), 0, "none past it");
    assert_eq!(
        next(1_000, fields(400, 5, true), 3),
        5,
        "after a share, what it left"
    );
    assert_eq!(
        next(10_000_000, fields(0, 0, false), 0),
        word::MOST_LEFT,
        "at most what the word holds"
    );
}

/// A worker meeting a window's end makes the next window's entry, the old one closed and its
/// others carried; the participant adopting it takes the key's record on, its reports'
/// numbers kept.
#[test]
fn a_worker_meeting_a_windows_end_makes_the_next() {
    let limit = window(100);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 1, MS), 1);
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant).len(), 1);
    let old = entry(&participant);
    old.others.store(3, Ordering::Release);
    let registry = participant.registry();
    assert!(
        registry.find(KEY, limit, old.ends).is_none(),
        "its window over"
    );
    let (next, made) = registry.find_or_make(KEY, limit, CYCLE, old.ends).unwrap();
    assert!(made);
    assert!(old.closed());
    assert_eq!(next.window, old.window + 1);
    assert_eq!(next.others.load(Ordering::Acquire), 3);
    let (again, made) = registry.find_or_make(KEY, limit, CYCLE, old.ends).unwrap();
    assert!(!made && Arc::ptr_eq(&again, &next), "one entry a window");
    participant.last_now = old.ends;
    participant.adopt(Arc::clone(&next));
    let tracked = participant.keys.get(KEY).unwrap();
    assert!(Arc::ptr_eq(&tracked.entry, &next));
    assert_eq!(tracked.sent, 1, "numbered on");
    assert_eq!(tracked.unacknowledged.as_slice().len(), 1, "still out");
    // Its old entry, adopted late, changes nothing.
    participant.adopt(old);
    assert!(Arc::ptr_eq(
        &participant.keys.get(KEY).unwrap().entry,
        &next
    ));
}

/// A worker on a window's entry decides nothing once the window is over: the next window's
/// entry decides.
#[test]
fn an_entry_decides_nothing_once_its_window_is_over() {
    let limit = window(100);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 1, MS), 1);
    let old = entry(&participant);
    assert_eq!(decide(&[&old], old.ends - 1).local, Local::Admit);
    assert_eq!(decide(&[&old], old.ends).local, Local::Ask);
    assert_eq!(old.wait(old.ends), None);
}

/// Keys' windows end at moments of their own, shifted by their hashes, so that their counts
/// do not all start again at once.
#[test]
fn keys_windows_end_at_moments_of_their_own() {
    let ends: HashSet<u64> = (0..100_u64)
        .map(|k| word::window_at(k.wrapping_mul(0x9E37_79B9_7F4A_7C15), MINUTE, 0).1)
        .collect();
    assert!(ends.len() > 95, "{} moments", ends.len());
}

/// What the owner has yet to apply, counted on top of its answer, is of the answer's window
/// alone: the window before's report still out is no part of the new window's count.
#[test]
fn only_the_answers_windows_hits_are_counted_on_top_of_it() {
    let limit = window(100);
    let mut participant = participant();
    assert_eq!(requests(&mut participant, limit, 2, MS), 2);
    let old = entry(&participant);
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [(1, 2, old.window)]);
    assert_eq!(requests(&mut participant, limit, 3, old.ends), 3);
    let number = entry(&participant).window;
    participant.on_cycle(old.ends + CYCLE);
    assert!(sent(&mut participant).contains(&(2, 3, number)));
    answer(
        &mut participant,
        Standing::Window { number, count: 10 },
        0,
        0,
        old.ends + CYCLE + MS,
    );
    assert_eq!(
        entry(&participant).snapshot().arrival,
        13,
        "10 and its own 3"
    );
}

/// Visiting only the keys that need it, a key whose report is still out as its window ends is
/// visited again in its next window's entry: what that window admits is reported, and the
/// report still out is sent again. A key left marked as queued under its old entry would be
/// visited never again, its hits never reported (the cluster simulator, packets lost).
#[test]
fn a_key_queued_as_its_window_ends_is_visited_in_the_next() {
    let limit = window(100);
    let mut participant = participant();
    participant.set_scheduled(true);
    assert_eq!(requests(&mut participant, limit, 2, MS), 2);
    let old = entry(&participant);
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [(1, 2, old.window)]);
    // Unanswered, the key stays queued for the cycle after; the window ends meanwhile.
    assert_eq!(requests(&mut participant, limit, 3, old.ends), 3);
    let next = entry(&participant).window;
    participant.on_cycle(old.ends + CYCLE);
    let reports = sent(&mut participant);
    assert!(reports.contains(&(2, 3, next)), "{reports:?}");
    participant.on_cycle(old.ends + 5 * CYCLE);
    let reports = sent(&mut participant);
    assert!(
        reports.contains(&(1, 2, old.window)),
        "sent again: {reports:?}"
    );
}
