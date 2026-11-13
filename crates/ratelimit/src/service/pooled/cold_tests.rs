//! Tests of a cold key's share (06 §4): until its owner first answers about it, a key's copy
//! shows the data plane's cold share of its limit, a bucket's or a window's, and a refusal
//! meanwhile waits a cycle; under a fixed window, every window's start is cold. The whole,
//! when not set, leaves every copy as it was.

#![cfg(not(loom))]

use super::*;
use crate::service::{Contract, FixedState, FixedWindow};
use std::num::NonZeroU64;

const MS: u64 = 1_000_000;
const CYCLE: u64 = 10 * MS;
const MINUTE: u64 = 60_000 * MS;
const HOUR: u64 = 60 * MINUTE;
const KEY: &[u8] = b"api/0\0client";

fn n(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

/// `limit` a minute, a fixed window.
fn window(limit: u64) -> Limit {
    Limit::FixedWindow(FixedWindow {
        limit: n(limit),
        length: n(MINUTE),
    })
}

/// `limit` an hour, a bucket of `burst`: slow to refill, so that a refusal's wait tells a
/// cycle from the bucket's refill.
fn bucket(limit: u64, burst: u64) -> Limit {
    Limit::Bucket(Contract {
        limit: n(limit),
        window: n(HOUR),
        burst: n(burst),
    })
}

/// A participant whose copies start at `share` percent of their limits.
fn participant(share: Option<u64>) -> Participant<FixedState> {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut participant = Participant::new(7, CYCLE, view, FixedState::default()).unwrap();
    if let Some(share) = share {
        participant.set_cold_share(share);
    }
    participant
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

/// The hits the participant sends: each report's count.
fn sent(participant: &mut Participant<FixedState>) -> Vec<u64> {
    participant
        .take_outgoing()
        .into_iter()
        .flat_map(|(_, batch)| batch.entries)
        .filter_map(|entry| match entry.report {
            Report::Hits { count, .. } => Some(count),
            Report::Poll => None,
        })
        .collect()
}

/// The owner's answer about `KEY`, having applied `applied` of its reports, reaches
/// `participant` at `now`.
fn answer(participant: &mut Participant<FixedState>, standing: Standing, applied: u64, now: u64) {
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
            others: 0,
        }],
        routing: None,
    };
    participant.on_reply(reply, now);
}

/// Cold, a window's copy shows its share of the limit, rounded up: refused past it, told to
/// come back in a cycle, when the owner will have answered. Its answer gives the window's
/// count, and from the next cycle, whose allowance is shared from it, the copy decides from
/// it: room the owner says is left is admitted, and a window the owner says is spent waits
/// for its end.
#[test]
fn a_cold_window_admits_its_share_and_waits_a_cycle() {
    let limit = window(10);
    let mut participant = participant(Some(75));
    assert_eq!(
        requests(&mut participant, limit, 10, MS),
        8,
        "75% of 10, up"
    );
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], 2 * MS),
        Some((0, CYCLE)),
        "a cycle, not the window's end"
    );
    let number = entry(&participant).window;
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [8]);
    // Alone on the key: the owner counts these 8.
    answer(
        &mut participant,
        Standing::Window { number, count: 8 },
        1,
        CYCLE + MS,
    );
    assert_eq!(
        requests(&mut participant, limit, 1, CYCLE + 2 * MS),
        0,
        "the cycle's allowance spent"
    );
    assert_eq!(requests(&mut participant, limit, 5, 2 * CYCLE + MS), 2);
    let ends = entry(&participant).ends;
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], 2 * CYCLE + 2 * MS),
        Some((0, ends - 2 * CYCLE - 2 * MS)),
        "answered, the window's end"
    );
}

/// A data plane meeting a key whose window others have spent admits at most its share
/// before the owner's first answer, and from that answer on refuses until the window ends.
#[test]
fn a_window_spent_elsewhere_is_refused_to_its_end_from_the_first_answer() {
    let limit = window(10);
    let mut participant = participant(Some(50));
    let start = 30_000 * MS;
    assert_eq!(requests(&mut participant, limit, 10, start), 5);
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], start),
        Some((0, CYCLE))
    );
    let number = entry(&participant).window;
    participant.on_cycle(start + CYCLE);
    assert_eq!(sent(&mut participant), [5]);
    // The 10 others admitted, and these 5.
    answer(
        &mut participant,
        Standing::Window { number, count: 15 },
        1,
        start + CYCLE + MS,
    );
    let now = start + CYCLE + 2 * MS;
    assert_eq!(requests(&mut participant, limit, 3, now), 0);
    let ends = entry(&participant).ends;
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], now),
        Some((0, ends - now))
    );
}

/// Every window's start is cold: the next window's entry shows the share again until the
/// owner answers about that window, and an answer about the window before does not end it.
#[test]
fn every_windows_start_is_cold() {
    let limit = window(10);
    let mut participant = participant(Some(50));
    assert_eq!(requests(&mut participant, limit, 1, MS), 1);
    let first = entry(&participant);
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [1]);
    answer(
        &mut participant,
        Standing::Window {
            number: first.window,
            count: 1,
        },
        1,
        CYCLE + MS,
    );
    assert_eq!(requests(&mut participant, limit, 10, 2 * CYCLE + MS), 9);
    let ends = first.ends;
    assert_eq!(requests(&mut participant, limit, 10, ends), 5, "cold again");
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], ends),
        Some((0, CYCLE))
    );
    // A late answer about the window before rebases nothing and leaves the new one cold.
    answer(
        &mut participant,
        Standing::Window {
            number: first.window,
            count: 10,
        },
        1,
        ends + MS,
    );
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], ends + MS),
        Some((0, CYCLE))
    );
}

/// Cold, a bucket's copy shows its share of the bucket and refuses past it with a cycle's
/// wait rather than the bucket's refill, minutes at an hour's rate; from the owner's answer
/// on, the copy is the owner's, admitting from the next cycle, and its refusals wait for the
/// refill.
#[test]
fn a_cold_bucket_admits_its_share_and_waits_a_cycle() {
    let limit = bucket(10, 10);
    let mut participant = participant(Some(50));
    assert_eq!(requests(&mut participant, limit, 10, MS), 5);
    assert_eq!(
        participant.retry_after(&[(KEY, limit)], 2 * MS),
        Some((0, CYCLE))
    );
    participant.on_cycle(CYCLE);
    assert_eq!(sent(&mut participant), [5]);
    // The owner's bucket, these 5 taken: five intervals of the hour's ten behind full.
    let behind = 5 * u128::from(HOUR);
    answer(&mut participant, Standing::Bucket { behind }, 1, CYCLE + MS);
    assert_eq!(requests(&mut participant, limit, 10, 2 * CYCLE + MS), 5);
    let (_, wait) = participant
        .retry_after(&[(KEY, limit)], 2 * CYCLE + 2 * MS)
        .unwrap();
    assert!(wait > MINUTE, "the refill, {wait} ns");
}

/// The share is rounded up, so that a copy always shows one request at least.
#[test]
fn a_share_under_one_request_is_one() {
    for limit in [window(3), bucket(10, 3)] {
        let mut one = participant(Some(1));
        assert_eq!(requests(&mut one, limit, 3, MS), 1, "{limit:?}");
    }
}

/// The whole, set or left out, is the copy as it always was: a cold key admits its whole
/// limit, and a refusal waits for the window's end or the bucket's refill.
#[test]
fn the_whole_share_leaves_a_cold_copy_as_it_was() {
    for share in [None, Some(100)] {
        let limit = window(10);
        let mut windowed = participant(share);
        assert_eq!(requests(&mut windowed, limit, 11, MS), 10, "{share:?}");
        let ends = entry(&windowed).ends;
        assert_eq!(
            windowed.retry_after(&[(KEY, limit)], 2 * MS),
            Some((0, ends - 2 * MS)),
            "{share:?}"
        );
        let limit = bucket(10, 10);
        let mut bucketed = participant(share);
        assert_eq!(requests(&mut bucketed, limit, 11, MS), 10, "{share:?}");
        let (_, wait) = bucketed.retry_after(&[(KEY, limit)], 2 * MS).unwrap();
        assert!(wait > MINUTE, "{share:?}: {wait} ns");
    }
}
