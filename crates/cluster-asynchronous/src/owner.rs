//! The owner applying one entry of a data plane's batch to its key, on `Node::update`: today's
//! owner's (`ServicePod::on_batch`), by the wall clock the store gives it.

use crate::value::{Admitted, Tally, Value};
use edgerush_cluster::{Changed, Kept, Time};
use edgerush_ratelimit::reference::Model;
use edgerush_ratelimit::service::{ACTIVE_CYCLES, Limit, PlaneId, Report, Standing};

/// What the owner answers a data plane about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answered {
    /// Where the key stands as the owner holds it: its bucket, or its latest window.
    pub standing: Standing,
    /// The highest of the data plane's batches applied, every one before it too: a copy is
    /// never ahead of its owner.
    pub applied: u64,
    /// How many other data planes the owner has seen admit the key lately.
    pub others: u64,
}

/// Data plane `from`'s report on a key it holds under `limit` reaches the key's owner at
/// `now`, by the wall clock; its data planes send every `cycle` nanoseconds. The answer to
/// give, and what changed for the store to copy: hits are a change of the whole; a poll only
/// keeps the key from being forgotten, and is copied to nobody.
///
/// A limit other than the key's starts the key afresh. Hits not yet applied are applied in
/// order, once each; one ahead of a batch still missing waits for it. A bucket's are charged
/// as of when the earliest was admitted, owing what does not fit while it would be paid back
/// within a refill time; a fixed window's are counted in the window they were admitted in,
/// those of a window earlier than the latest counted for nothing (06 §4). Records of data
/// planes silent for ten minutes, or a refill time (a window) if longer, go.
pub fn apply(
    kept: &mut Kept<Value>,
    from: PlaneId,
    limit: Limit,
    report: Report,
    cycle: u64,
    now: Time,
) -> (Answered, Changed) {
    let lately = ACTIVE_CYCLES * cycle.max(1);
    let value = &mut kept.value;
    if value.limit != Some(limit) {
        *value = Value::new(limit);
    }
    let latest = value
        .planes
        .iter()
        .map(|(_, record)| record.last_admitted)
        .max()
        .unwrap_or_default();
    let record = value.planes.entry(from);
    let mut changed = Changed::Nothing;
    if let Report::Hits {
        sequence,
        count,
        seen,
        age,
        window,
    } = report
    {
        changed = Changed::Whole;
        // Taken over from a copy that had not caught up: batches the data plane saw applied,
        // and will not send again, are skipped.
        if seen > record.applied {
            record.skip_to(seen);
        }
        // A batch not applied yet is an admission now. One sent again, its answer lost, keeps
        // its data plane counted with those admitting, as of the latest admission and no
        // later: made the latest itself, long after a burst, it would leave the others that
        // admitted with it uncounted, and each would take too large a share of the next burst.
        if sequence > record.applied {
            record.last_admitted = Admitted::at(now);
        } else {
            record.last_admitted = record.last_admitted.max(latest);
        }
        let tally = Tally {
            count,
            at: now.saturating_sub(age),
            window,
        };
        match limit {
            // Charged as of when the earliest was admitted, the batch's trip here counted in
            // with its age, however long ago: charged any later, a batch of more hits than a
            // bucket holds would be charged late, refusing a client within its rate.
            Limit::Bucket(_) => {
                let mut ready: Option<Tally> = None;
                record.take(sequence, tally, |next| {
                    ready = Some(ready.map_or(next, |held| held.add(next)));
                });
                let ready = ready.unwrap_or_default();
                value.charge(ready.count, ready.at, now);
            }
            // Each batch in its own window, in order: batches waiting may straddle a boundary.
            Limit::FixedWindow(_) => {
                let (latest, counted) = (&mut value.window, &mut value.counted);
                record.take(sequence, tally, |next| {
                    *counted = counted.saturating_add(next.count);
                    latest.add(next.window, next.count);
                });
            }
        }
    }
    value.retire();
    // Forgotten once quiet for a refill time and a bucket full again, as today's owner forgets
    // it; a window once quiet for its length, every data plane past the window it held.
    let quiet = now.saturating_add(limit.refill_time()).saturating_add(1);
    kept.forget_at = match limit {
        Limit::Bucket(contract) => quiet.max(value.bucket_under(contract).forgettable_at()),
        Limit::FixedWindow(_) => quiet,
    };
    let answered = Answered {
        standing: kept.value.standing(limit, now),
        applied: kept.value.applied(from).unwrap_or(0),
        others: kept.value.others(from, lately),
    };
    (answered, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Record;
    use edgerush_ratelimit::service::{Contract, FixedWindow, MOST_UNACKNOWLEDGED, RECORD_KEPT};
    use std::num::NonZeroU64;

    const MS: u64 = 1_000_000;
    const CYCLE: u64 = 10 * MS;

    /// 100 a second, a bucket of ten: a refill time of 100 ms.
    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(1_000 * MS),
            burst: n(10),
        }
    }

    /// The usual contract's bucket.
    fn bucket() -> Limit {
        Limit::Bucket(contract())
    }

    fn hits(sequence: u64, count: u64, seen: u64) -> Report {
        Report::Hits {
            sequence,
            count,
            seen,
            age: 0,
            window: 0,
        }
    }

    /// `from`'s report under the usual contract, at `now`.
    fn send(kept: &mut Kept<Value>, from: PlaneId, report: Report, now: Time) -> Answered {
        apply(kept, from, bucket(), report, CYCLE, now).0
    }

    /// The bucket's distance from full an answer says.
    fn behind(answer: Answered) -> u128 {
        let Standing::Bucket { behind } = answer.standing else {
            panic!("a bucket's answer: {answer:?}");
        };
        behind
    }

    fn record(kept: &Kept<Value>, plane: PlaneId) -> &Record {
        kept.value.planes.get(plane).unwrap()
    }

    fn retry_at(kept: &Kept<Value>, now: Time) -> Option<Time> {
        kept.value.bucket().unwrap().retry_at(now)
    }

    /// The data planes the owner keeps records of.
    fn recorded(kept: &Kept<Value>) -> Vec<PlaneId> {
        kept.value.planes().collect()
    }

    /// The owner owes hits past the bucket up to a refill time, and forgives the rest:
    /// twenty-five in a bucket of ten leave it owing ten.
    #[test]
    fn the_owner_owes_excess_up_to_a_refill_time() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 25, 0), 0);
        assert_eq!(kept.value.counted(), 25);
        assert_eq!(retry_at(&kept, 0), Some(110 * MS));
    }

    /// A report of any number of hits is charged in one step, owed and forgiven as twenty-five
    /// are.
    #[test]
    fn a_report_of_any_number_of_hits_is_charged_at_once() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, u64::MAX, 0), 0);
        assert_eq!(kept.value.counted(), u64::MAX);
        assert_eq!(retry_at(&kept, 0), Some(110 * MS));
        send(&mut kept, 1, hits(2, u64::MAX, 0), 0);
        assert_eq!(kept.value.counted(), u64::MAX, "as many as it can hold");
    }

    /// Reports kept waiting are added to the one they waited for without overflowing.
    #[test]
    fn reports_that_waited_are_taken_in_with_no_overflow() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(2, u64::MAX, 0), 0);
        send(&mut kept, 1, hits(1, 1, 0), 0);
        assert_eq!(kept.value.counted(), u64::MAX);
        assert_eq!(record(&kept, 1).applied, 2);
        assert_eq!(retry_at(&kept, 0), Some(110 * MS));
    }

    proptest::proptest! {
        /// Hits charged at once are charged as one by one: each owed while the bucket would
        /// admit again within a refill time, the rest forgiven.
        #[test]
        fn hits_charged_at_once_are_charged_as_one_by_one(
            reports in proptest::collection::vec((0u64..300, 0u64..40), 1..20),
            limit in 1u64..8,
            window in 1u64..2_000,
            burst in 1u64..12,
        ) {
            let n = |n| NonZeroU64::new(n).unwrap();
            let contract = Contract { limit: n(limit), window: n(window), burst: n(burst) };
            let most = contract.refill_time();
            let mut value = Value::new(Limit::Bucket(contract));
            let mut one_by_one = contract.bucket();
            let mut now = 0;
            for (gap, count) in reports {
                now += gap;
                value.charge(count, now, now);
                for _ in 0..count {
                    if one_by_one.retry_at(now).is_none_or(|retry| retry - now <= most) {
                        one_by_one.charge(now);
                    }
                }
                proptest::prop_assert_eq!(value.bucket(), Some(one_by_one.clone()));
            }
        }
    }

    /// The owner counts the others admitting as of the latest admission it knows of, not as
    /// of now: data planes that burst together and fell quiet count together.
    #[test]
    fn the_owner_counts_those_admitting_as_of_its_latest_traffic() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 1, 0), 0);
        assert_eq!(send(&mut kept, 2, hits(1, 1, 0), 0).others, 1);
        let later = 5_000 * MS;
        assert_eq!(
            send(&mut kept, 3, hits(1, 1, 0), later).others,
            0,
            "the two before, long ago"
        );
        assert_eq!(
            send(&mut kept, 1, hits(2, 1, 0), later + CYCLE).others,
            1,
            "the third, a cycle before"
        );
        assert_eq!(
            send(&mut kept, 2, Report::Poll, later + 100 * CYCLE).others,
            2,
            "nothing since: the two last admitting"
        );
    }

    /// A report sent again, its batch applied already, keeps its data plane counted with
    /// those admitting as of the latest admission, and no later; one that brings a batch not
    /// applied yet is an admission then, however late it comes.
    #[test]
    fn a_report_sent_again_admits_no_later_than_the_latest() {
        let mut kept = Kept::default();
        for plane in 1..=3 {
            send(&mut kept, plane, hits(1, 1, 0), 0);
        }
        // Its answer lost, the first data plane's batch is sent again, and comes late.
        let answer = send(&mut kept, 1, hits(1, 1, 0), 400 * MS);
        assert_eq!(answer.others, 2, "the three still together");
        let answer = send(&mut kept, 1, hits(2, 1, 1), 400 * MS);
        assert_eq!(answer.others, 0, "a new batch, the others long before");

        // While another admits, one whose batch comes again is counted with it.
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 1, 0), 0);
        send(&mut kept, 2, hits(1, 1, 0), 0);
        send(&mut kept, 2, hits(2, 1, 1), 250 * MS);
        send(&mut kept, 1, hits(1, 1, 0), 250 * MS);
        let answer = send(&mut kept, 2, hits(3, 1, 2), 500 * MS);
        assert_eq!(answer.others, 1, "the first, sent again lately");
    }

    /// A new owner whose copy missed batches skips to what the data plane saw applied: the
    /// copy holds the first batch, the old owner applied the second, lost with its copy, and
    /// the third is applied.
    #[test]
    fn a_new_owner_skips_the_batches_its_copy_missed() {
        let mut copy = Kept::default();
        send(&mut copy, 1, hits(1, 1, 0), 0);
        let answer = send(&mut copy, 1, hits(3, 2, 2), 2 * CYCLE);
        assert_eq!(answer.applied, 3);
        assert_eq!(copy.value.counted(), 1 + 2);
        assert!(record(&copy, 1).waiting.is_none());
    }

    /// A contract changed starts the key afresh: what was counted under the old one goes.
    #[test]
    fn a_changed_contract_starts_the_key_afresh() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 5, 0), 0);
        let mut bigger = contract();
        bigger.burst = NonZeroU64::new(20).unwrap();
        apply(&mut kept, 1, Limit::Bucket(bigger), hits(2, 1, 1), CYCLE, 0);
        assert_eq!(kept.value.limit(), Some(Limit::Bucket(bigger)));
        assert_eq!(kept.value.counted(), 1, "the owner starts afresh");
    }

    /// A key is forgotten once quiet for a refill time and full again; a poll keeps it a
    /// refill time more, as a change of nothing to copy, and hits as a change of the whole.
    #[test]
    fn a_key_is_forgotten_a_refill_time_after_its_last_report() {
        let mut kept = Kept::default();
        let (_, changed) = apply(&mut kept, 1, bucket(), hits(1, 1, 0), CYCLE, 0);
        assert_eq!(changed, Changed::Whole);
        assert_eq!(kept.forget_at, 100 * MS + 1, "a refill time");
        let (_, changed) = apply(&mut kept, 1, bucket(), Report::Poll, CYCLE, 50 * MS);
        assert_eq!(changed, Changed::Nothing);
        assert_eq!(kept.forget_at, 150 * MS + 1);
        // Owing past its refill time, it is kept until full again.
        apply(&mut kept, 1, bucket(), hits(2, 25, 1), CYCLE, 60 * MS);
        assert_eq!(
            kept.forget_at,
            kept.value.bucket().unwrap().forgettable_at()
        );
        assert!(kept.forget_at > 160 * MS + 1);
    }

    /// A batch the owner has applied, arriving again, is answered and nothing more: it is not
    /// kept waiting.
    #[test]
    fn a_batch_applied_arriving_again_is_not_kept() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 2, 0), 0);
        let answer = send(&mut kept, 1, hits(1, 2, 0), CYCLE);
        assert_eq!(answer.applied, 1);
        assert_eq!(kept.value.counted(), 2);
        assert!(record(&kept, 1).waiting.is_none());
    }

    /// Batches arriving in order hold no room for batches kept waiting; one arriving ahead of
    /// a batch still missing is kept, and the room given back once the missing one comes.
    #[test]
    fn only_batches_ahead_of_a_missing_one_hold_room() {
        let room = |kept: &Kept<Value>| {
            record(kept, 1)
                .waiting
                .as_ref()
                .map_or(0, |waiting| waiting.capacity())
        };
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 1, 0), 0);
        send(&mut kept, 1, hits(2, 1, 1), CYCLE);
        assert_eq!(room(&kept), 0, "in order");
        send(&mut kept, 1, hits(4, 1, 2), 2 * CYCLE);
        assert_eq!(kept.value.counted(), 2, "the fourth waits for the third");
        assert!(room(&kept) > 0);
        send(&mut kept, 1, hits(3, 1, 2), 3 * CYCLE);
        assert_eq!(kept.value.counted(), 4);
        assert_eq!(room(&kept), 0, "given back");
    }

    /// The owner keeps at most [`MOST_UNACKNOWLEDGED`] of a data plane's batches waiting for a
    /// missing one: one more is answered unapplied, and counted when the data plane sends it
    /// again.
    #[test]
    fn the_owner_keeps_a_bounded_number_of_batches_waiting() {
        let waiting = |kept: &Kept<Value>| record(kept, 1).waiting_len();
        let mut kept = Kept::default();
        let most = MOST_UNACKNOWLEDGED as u64;
        let last = most + 8;
        for sequence in 2..=last {
            assert_eq!(send(&mut kept, 1, hits(sequence, 1, 0), 0).applied, 0);
        }
        assert_eq!(waiting(&kept), MOST_UNACKNOWLEDGED);
        send(&mut kept, 1, hits(2, 1, 0), CYCLE);
        assert_eq!(
            waiting(&kept),
            MOST_UNACKNOWLEDGED,
            "one waiting, sent again"
        );
        send(&mut kept, 1, hits(1, 1, 0), CYCLE);
        assert_eq!(kept.value.counted(), 1 + most);
        for sequence in most + 2..=last {
            send(&mut kept, 1, hits(sequence, 1, 0), 2 * CYCLE);
        }
        assert_eq!(kept.value.counted(), last, "each counted once");
        assert_eq!(waiting(&kept), 0);
    }

    /// A batch waiting, sent again emptied as older than a refill time, keeps the emptied
    /// tally, whichever comes first; with as many hits, the one charged earlier.
    #[test]
    fn a_batch_waiting_sent_again_keeps_the_smaller_tally() {
        let waited = |kept: &Kept<Value>| record(kept, 1).waiting.as_ref().unwrap()[0].1;
        for (first, again, at) in [(5, 0, 200 * MS), (0, 5, 0)] {
            let mut kept = Kept::default();
            send(&mut kept, 1, hits(2, first, 0), 0);
            send(&mut kept, 1, hits(2, again, 0), 200 * MS);
            assert_eq!(
                waited(&kept),
                Tally {
                    count: 0,
                    at,
                    window: 0
                }
            );
        }
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(2, 5, 0), 0);
        send(&mut kept, 1, hits(2, 5, 0), CYCLE);
        assert_eq!(
            waited(&kept),
            Tally {
                count: 5,
                at: 0,
                window: 0
            }
        );
    }

    /// A batch left waiting at or below the highest applied, as a merge with a copy that had
    /// applied further can leave one, holds up none after it.
    #[test]
    fn a_batch_waiting_below_the_highest_applied_holds_up_none() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 1, 0), 0);
        let held = kept.value.planes.entry(1);
        held.applied = 4;
        held.waiting = Some(Box::new(vec![(
            5,
            Tally {
                count: 1,
                at: 0,
                window: 0,
            },
        )]));
        send(&mut kept, 1, hits(7, 1, 4), CYCLE);
        // The fifth sent again is applied, then the sixth: the seventh follows on.
        send(&mut kept, 1, hits(5, 1, 4), 2 * CYCLE);
        assert_eq!(record(&kept, 1).applied, 5);
        let answer = send(&mut kept, 1, hits(6, 1, 5), 3 * CYCLE);
        assert_eq!(answer.applied, 7);
        assert_eq!(record(&kept, 1).waiting_len(), 0);
    }

    /// A data plane's record of a key goes once it has admitted nothing on it for
    /// [`RECORD_KEPT`] before the latest admission, though another keeps the key in use, and
    /// the room it took with it; one that has only polled goes at once.
    #[test]
    fn a_record_silent_for_ten_minutes_is_retired() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 1, 0), 0);
        send(&mut kept, 2, hits(1, 1, 0), 0);
        send(&mut kept, 2, hits(2, 1, 1), RECORD_KEPT);
        send(&mut kept, 3, Report::Poll, RECORD_KEPT);
        assert_eq!(recorded(&kept), [1, 2]);
        send(&mut kept, 2, hits(3, 1, 2), RECORD_KEPT + CYCLE);
        assert_eq!(recorded(&kept), [2]);
        assert_eq!(kept.value.planes.room(), 1);
    }

    /// A key whose refill time is longer than [`RECORD_KEPT`] keeps a silent data plane's
    /// record for its refill time.
    #[test]
    fn a_long_refill_keeps_a_silent_record_for_its_refill_time() {
        let n = |n| NonZeroU64::new(n).unwrap();
        let hourly = Contract {
            limit: n(1),
            window: n(1_000 * MS),
            burst: n(3_600),
        };
        let refill_time = hourly.refill_time();
        assert!(refill_time > RECORD_KEPT);
        let mut kept = Kept::default();
        let send = |kept: &mut Kept<Value>, from, sequence, now| {
            apply(
                kept,
                from,
                Limit::Bucket(hourly),
                hits(sequence, 1, sequence - 1),
                CYCLE,
                now,
            );
        };
        send(&mut kept, 1, 1, 0);
        send(&mut kept, 2, 1, 0);
        send(&mut kept, 2, 2, refill_time);
        assert_eq!(recorded(&kept), [1, 2]);
        send(&mut kept, 2, 3, refill_time + CYCLE);
        assert_eq!(recorded(&kept), [2]);
    }

    /// A data plane back after its record went starts a fresh one from the batches it saw
    /// applied: none is counted twice.
    #[test]
    fn a_data_plane_back_after_its_record_went_is_counted_once() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 2, 0), 0);
        send(&mut kept, 2, hits(1, 1, 0), RECORD_KEPT + CYCLE);
        assert_eq!(recorded(&kept), [2]);
        let answer = send(&mut kept, 1, hits(2, 3, 1), RECORD_KEPT + 2 * CYCLE);
        assert_eq!(answer.applied, 2);
        assert_eq!(kept.value.counted(), 6);
    }

    /// Hits are charged as of when they were admitted, not on arrival: one admitted 30 ms
    /// before, at 100 a second, has refilled by the time it arrives.
    #[test]
    fn a_hit_is_charged_as_of_when_it_was_admitted() {
        let mut kept = Kept::default();
        let late = Report::Hits {
            sequence: 1,
            count: 1,
            seen: 0,
            age: 30 * MS,
            window: 0,
        };
        let answer = send(&mut kept, 100, late, 1_000 * MS);
        let bucket = kept.value.bucket().unwrap();
        assert_eq!(bucket.remaining(1_000 * MS), 10, "refilled since");
        assert_eq!(behind(answer), 0);
    }

    /// The owner charges back to the earliest hit however long ago, past the key's refill
    /// time too: 15 hits said to be 150 ms old have all refilled as they arrive.
    #[test]
    fn hits_are_charged_back_to_the_earliest_past_a_refill_time() {
        let mut kept = Kept::default();
        let old = Report::Hits {
            sequence: 1,
            count: 15,
            seen: 0,
            age: 150 * MS,
            window: 0,
        };
        send(&mut kept, 100, old, 1_000 * MS);
        assert_eq!(kept.value.bucket().unwrap().remaining(1_000 * MS), 10);
    }

    /// Ten a minute, a fixed window.
    fn window() -> Limit {
        let n = |n| NonZeroU64::new(n).unwrap();
        Limit::FixedWindow(FixedWindow {
            limit: n(10),
            length: n(60_000 * MS),
        })
    }

    /// Hits admitted in window `number`.
    fn in_window(sequence: u64, count: u64, seen: u64, number: u64) -> Report {
        Report::Hits {
            sequence,
            count,
            seen,
            age: 0,
            window: number,
        }
    }

    /// `from`'s report under the fixed window, at `now`.
    fn send_window(kept: &mut Kept<Value>, from: PlaneId, report: Report, now: Time) -> Answered {
        apply(kept, from, window(), report, CYCLE, now).0
    }

    /// The window and count an answer says.
    fn standing(answer: Answered) -> (u64, u64) {
        let Standing::Window { number, count } = answer.standing else {
            panic!("a window's answer: {answer:?}");
        };
        (number, count)
    }

    /// A fixed window's hits are counted in the window they were admitted in, every data
    /// plane's together, and the answer says that window and its count.
    #[test]
    fn a_windows_hits_are_counted_in_their_window() {
        let mut kept = Kept::default();
        let answer = send_window(&mut kept, 1, in_window(1, 3, 0, 5), 0);
        assert_eq!(standing(answer), (5, 3));
        let answer = send_window(&mut kept, 2, in_window(1, 2, 0, 5), CYCLE);
        assert_eq!(standing(answer), (5, 5));
        assert_eq!(answer.others, 1);
        assert_eq!(kept.value.window(), (5, 5));
        assert_eq!(kept.value.bucket(), None, "no bucket under a window");
    }

    /// Hits of a later window start it; hits of an earlier one, that window over, count for
    /// nothing in it, though they are applied, once.
    #[test]
    fn a_later_window_replaces_and_an_earlier_counts_for_nothing() {
        let mut kept = Kept::default();
        send_window(&mut kept, 1, in_window(1, 4, 0, 5), 0);
        let answer = send_window(&mut kept, 2, in_window(1, 1, 0, 6), CYCLE);
        assert_eq!(standing(answer), (6, 1), "the later window");
        let answer = send_window(&mut kept, 1, in_window(2, 3, 1, 5), 2 * CYCLE);
        assert_eq!(standing(answer), (6, 1), "the earlier window over");
        assert_eq!(answer.applied, 2, "applied all the same");
        assert_eq!(kept.value.counted(), 8);
    }

    /// Batches kept waiting for a missing one are each counted in their own window, in order,
    /// though they straddle a boundary: none is counted in another's window.
    #[test]
    fn batches_waiting_across_a_boundary_are_each_counted_in_their_window() {
        let mut kept = Kept::default();
        send_window(&mut kept, 1, in_window(2, 2, 0, 6), 0);
        send_window(&mut kept, 1, in_window(3, 1, 0, 6), 0);
        assert_eq!(kept.value.window(), (0, 0), "waiting for the first");
        let answer = send_window(&mut kept, 1, in_window(1, 3, 0, 5), CYCLE);
        assert_eq!(answer.applied, 3);
        assert_eq!(
            standing(answer),
            (6, 3),
            "the first in its own window, now over"
        );
        assert_eq!(kept.value.counted(), 6);
    }

    /// A batch sent again, its answer lost, is counted once.
    #[test]
    fn a_windows_batch_sent_again_is_counted_once() {
        let mut kept = Kept::default();
        send_window(&mut kept, 1, in_window(1, 3, 0, 5), 0);
        let answer = send_window(&mut kept, 1, in_window(1, 3, 0, 5), CYCLE);
        assert_eq!(standing(answer), (5, 3));
    }

    /// No data plane numbers a window 0: hits said to be in it count for nothing.
    #[test]
    fn hits_in_window_zero_count_for_nothing() {
        let mut kept = Kept::default();
        let answer = send_window(&mut kept, 1, in_window(1, 3, 0, 0), 0);
        assert_eq!(standing(answer), (0, 0));
        assert_eq!(answer.applied, 1);
    }

    /// The model changed starts the key afresh, a bucket's charges gone with it, and back.
    #[test]
    fn a_changed_model_starts_the_key_afresh() {
        let mut kept = Kept::default();
        send(&mut kept, 1, hits(1, 5, 0), 0);
        let answer = send_window(&mut kept, 1, in_window(2, 2, 1, 5), CYCLE);
        assert_eq!(kept.value.limit(), Some(window()));
        assert_eq!(kept.value.counted(), 2, "afresh");
        assert_eq!(standing(answer), (5, 2));
        let answer = send(&mut kept, 1, hits(3, 1, 2), 2 * CYCLE);
        assert_eq!(kept.value.counted(), 1, "afresh again");
        assert_eq!(kept.value.window(), (0, 0));
        assert!(behind(answer) > 0);
    }

    /// A window's key is forgotten once quiet for the window's length, a poll keeping it as
    /// long again.
    #[test]
    fn a_windows_key_is_forgotten_a_window_after_its_last_report() {
        let length = 60_000 * MS;
        let mut kept = Kept::default();
        let (_, changed) = apply(&mut kept, 1, window(), in_window(1, 30, 0, 5), CYCLE, 0);
        assert_eq!(changed, Changed::Whole);
        assert_eq!(kept.forget_at, length + 1, "owing nothing past its window");
        let (_, changed) = apply(&mut kept, 1, window(), Report::Poll, CYCLE, 50 * MS);
        assert_eq!(changed, Changed::Nothing);
        assert_eq!(kept.forget_at, 50 * MS + length + 1);
    }

    /// A data plane's batch as the property below sends it: its number, hits and window.
    #[derive(Clone, Debug)]
    struct Sent {
        plane: PlaneId,
        sequence: u64,
        count: u64,
        number: u64,
    }

    /// Up to four data planes' batches, each plane's numbered from 1 and in windows that never
    /// go back, each sent once or twice, in an order of the seed's.
    fn delivered() -> impl proptest::strategy::Strategy<Value = (Vec<Sent>, Vec<Sent>)> {
        use proptest::prelude::*;
        let plane = proptest::collection::vec((0u64..20, 0u64..3, any::<bool>()), 1..8);
        (proptest::collection::vec(plane, 1..4), any::<u64>()).prop_map(|(planes, mut seed)| {
            let mut sent = Vec::new();
            for (id, batches) in planes.into_iter().enumerate() {
                let mut number = 1;
                for (sequence, (count, step, twice)) in batches.into_iter().enumerate() {
                    number += step;
                    let batch = Sent {
                        plane: id as u64 + 1,
                        sequence: sequence as u64 + 1,
                        count,
                        number,
                    };
                    if twice {
                        sent.push(batch.clone());
                    }
                    sent.push(batch);
                }
            }
            let all = sent.clone();
            for at in (1..sent.len()).rev() {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let other = usize::try_from(seed >> 33).unwrap() % (at + 1);
                sent.swap(at, other);
            }
            (all, sent)
        })
    }

    proptest::proptest! {
        /// However every data plane's batches arrive — out of order, some twice — the owner
        /// ends holding the latest window any was admitted in and every hit admitted in it,
        /// each batch counted once.
        #[test]
        fn the_owner_holds_the_latest_window_and_every_hit_in_it((all, arriving) in delivered()) {
            let mut kept = Kept::default();
            for batch in &arriving {
                let report = in_window(batch.sequence, batch.count, 0, batch.number);
                send_window(&mut kept, batch.plane, report, 0);
            }
            let mut once: Vec<&Sent> = all.iter().collect();
            once.dedup_by(|a, b| a.plane == b.plane && a.sequence == b.sequence);
            let latest = once.iter().map(|batch| batch.number).max().unwrap_or(0);
            let in_latest: u64 = once
                .iter()
                .filter(|batch| batch.number == latest)
                .map(|batch| batch.count)
                .sum();
            proptest::prop_assert_eq!(kept.value.window(), (latest, in_latest));
            let counted: u64 = once.iter().map(|batch| batch.count).sum();
            proptest::prop_assert_eq!(kept.value.counted(), counted);
        }
    }
}
