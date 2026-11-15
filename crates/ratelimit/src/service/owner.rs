//! The asynchronous mode's owner: applying one entry of a data plane's batch to its key's
//! [`Value`], by the wall clock. The store runs it on each key's owner, copying what changed to
//! the key's replicas (`edgerush-cluster-asynchronous`); nothing here knows of the store.

mod value;

pub use value::{Parts, PlaneParts, Value};

use crate::reference::Model;
use crate::service::{ACTIVE_CYCLES, Contract, PlaneId, Report};
use value::{Admitted, Tally};

/// What the owner answers a data plane about one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answered {
    /// The bucket as the owner holds it: how far from full, as `TokenBucket::behind` says.
    pub behind: u128,
    /// The highest of the data plane's batches applied, every one before it too: a copy is
    /// never ahead of its owner.
    pub applied: u64,
    /// How many other data planes the owner has seen admit the key lately.
    pub others: u64,
}

/// What applying a report did: the answer to give, and what the store is to do with the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    /// The answer to the data plane.
    pub answered: Answered,
    /// Whether the value changed, to be copied whole: hits change it; a poll only keeps the key
    /// from being forgotten, and is copied to nobody.
    pub changed: bool,
    /// From when the key may be forgotten, by the wall clock: once quiet for a refill time and
    /// full again.
    pub forget_at: u64,
}

/// Data plane `from`'s report on a key it holds under `contract` reaches the key's owner at
/// `now`, by the wall clock; its data planes send every `cycle` nanoseconds.
///
/// A contract other than the key's starts the key afresh. Hits not yet applied are applied in
/// order, once each, as of when the earliest was admitted, owing what does not fit while it
/// would be paid back within a refill time; one ahead of a batch still missing waits for it.
/// Records of data planes silent for ten minutes, or a refill time if longer, go.
pub fn apply(
    value: &mut Value,
    from: PlaneId,
    contract: Contract,
    report: Report,
    cycle: u64,
    now: u64,
) -> Applied {
    let lately = ACTIVE_CYCLES * cycle.max(1);
    if value.contract != Some(contract) {
        *value = Value::new(contract);
    }
    let latest = value
        .planes
        .iter()
        .map(|(_, record)| record.last_admitted)
        .max()
        .unwrap_or_default();
    let record = value.planes.entry(from);
    let mut changed = false;
    if let Report::Hits {
        sequence,
        count,
        seen,
        age,
    } = report
    {
        changed = true;
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
        // Charged as of when the earliest was admitted, the batch's trip here counted in with
        // its age, however long ago: charged any later, a batch of more hits than a bucket
        // holds would be charged late, refusing a client within its rate.
        let ready = record.take(
            sequence,
            Tally {
                count,
                at: now.saturating_sub(age),
            },
        );
        value.charge(ready.count, ready.at, now);
    }
    value.retire();
    let bucket = value.bucket_under(contract);
    let forget_at = now
        .saturating_add(contract.refill_time())
        .saturating_add(1)
        .max(bucket.forgettable_at());
    let answered = Answered {
        behind: bucket.behind(now),
        applied: value.applied(from).unwrap_or(0),
        others: value.others(from, lately),
    };
    Applied {
        answered,
        changed,
        forget_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{MOST_UNACKNOWLEDGED, RECORD_KEPT};
    use std::num::NonZeroU64;
    use value::Record;

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

    fn hits(sequence: u64, count: u64, seen: u64) -> Report {
        Report::Hits {
            sequence,
            count,
            seen,
            age: 0,
        }
    }

    /// `from`'s report under the usual contract, at `now`.
    fn send(value: &mut Value, from: PlaneId, report: Report, now: u64) -> Answered {
        apply(value, from, contract(), report, CYCLE, now).answered
    }

    fn record(value: &Value, plane: PlaneId) -> &Record {
        value.planes.get(plane).unwrap()
    }

    fn retry_at(value: &Value, now: u64) -> Option<u64> {
        value.bucket().unwrap().retry_at(now)
    }

    /// The data planes the owner keeps records of.
    fn recorded(value: &Value) -> Vec<PlaneId> {
        value.planes().collect()
    }

    /// The owner owes hits past the bucket up to a refill time, and forgives the rest:
    /// twenty-five in a bucket of ten leave it owing ten.
    #[test]
    fn the_owner_owes_excess_up_to_a_refill_time() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 25, 0), 0);
        assert_eq!(value.counted(), 25);
        assert_eq!(retry_at(&value, 0), Some(110 * MS));
    }

    /// A report of any number of hits is charged in one step, owed and forgiven as twenty-five
    /// are.
    #[test]
    fn a_report_of_any_number_of_hits_is_charged_at_once() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, u64::MAX, 0), 0);
        assert_eq!(value.counted(), u64::MAX);
        assert_eq!(retry_at(&value, 0), Some(110 * MS));
        send(&mut value, 1, hits(2, u64::MAX, 0), 0);
        assert_eq!(value.counted(), u64::MAX, "as many as it can hold");
    }

    /// Reports value waiting are added to the one they waited for without overflowing.
    #[test]
    fn reports_that_waited_are_taken_in_with_no_overflow() {
        let mut value = Value::default();
        send(&mut value, 1, hits(2, u64::MAX, 0), 0);
        send(&mut value, 1, hits(1, 1, 0), 0);
        assert_eq!(value.counted(), u64::MAX);
        assert_eq!(record(&value, 1).applied, 2);
        assert_eq!(retry_at(&value, 0), Some(110 * MS));
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
            let mut value = Value::new(contract);
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
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 1, 0), 0);
        assert_eq!(send(&mut value, 2, hits(1, 1, 0), 0).others, 1);
        let later = 5_000 * MS;
        assert_eq!(
            send(&mut value, 3, hits(1, 1, 0), later).others,
            0,
            "the two before, long ago"
        );
        assert_eq!(
            send(&mut value, 1, hits(2, 1, 0), later + CYCLE).others,
            1,
            "the third, a cycle before"
        );
        assert_eq!(
            send(&mut value, 2, Report::Poll, later + 100 * CYCLE).others,
            2,
            "nothing since: the two last admitting"
        );
    }

    /// A report sent again, its batch applied already, keeps its data plane counted with
    /// those admitting as of the latest admission, and no later; one that brings a batch not
    /// applied yet is an admission then, however late it comes.
    #[test]
    fn a_report_sent_again_admits_no_later_than_the_latest() {
        let mut value = Value::default();
        for plane in 1..=3 {
            send(&mut value, plane, hits(1, 1, 0), 0);
        }
        // Its answer lost, the first data plane's batch is sent again, and comes late.
        let answer = send(&mut value, 1, hits(1, 1, 0), 400 * MS);
        assert_eq!(answer.others, 2, "the three still together");
        let answer = send(&mut value, 1, hits(2, 1, 1), 400 * MS);
        assert_eq!(answer.others, 0, "a new batch, the others long before");

        // While another admits, one whose batch comes again is counted with it.
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 1, 0), 0);
        send(&mut value, 2, hits(1, 1, 0), 0);
        send(&mut value, 2, hits(2, 1, 1), 250 * MS);
        send(&mut value, 1, hits(1, 1, 0), 250 * MS);
        let answer = send(&mut value, 2, hits(3, 1, 2), 500 * MS);
        assert_eq!(answer.others, 1, "the first, sent again lately");
    }

    /// A new owner whose copy missed batches skips to what the data plane saw applied: the
    /// copy holds the first batch, the old owner applied the second, lost with its copy, and
    /// the third is applied.
    #[test]
    fn a_new_owner_skips_the_batches_its_copy_missed() {
        let mut copy = Value::default();
        send(&mut copy, 1, hits(1, 1, 0), 0);
        let answer = send(&mut copy, 1, hits(3, 2, 2), 2 * CYCLE);
        assert_eq!(answer.applied, 3);
        assert_eq!(copy.counted(), 1 + 2);
        assert!(record(&copy, 1).waiting.is_none());
    }

    /// A contract changed starts the key afresh: what was counted under the old one goes.
    #[test]
    fn a_changed_contract_starts_the_key_afresh() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 5, 0), 0);
        let mut bigger = contract();
        bigger.burst = NonZeroU64::new(20).unwrap();
        apply(&mut value, 1, bigger, hits(2, 1, 1), CYCLE, 0);
        assert_eq!(value.contract(), Some(bigger));
        assert_eq!(value.counted(), 1, "the owner starts afresh");
    }

    /// A key is forgotten once quiet for a refill time and full again; a poll keeps it a
    /// refill time more, changing nothing to copy, and hits change the whole.
    #[test]
    fn a_key_is_forgotten_a_refill_time_after_its_last_report() {
        let mut value = Value::default();
        let applied = apply(&mut value, 1, contract(), hits(1, 1, 0), CYCLE, 0);
        assert!(applied.changed);
        assert_eq!(applied.forget_at, 100 * MS + 1, "a refill time");
        let applied = apply(&mut value, 1, contract(), Report::Poll, CYCLE, 50 * MS);
        assert!(!applied.changed);
        assert_eq!(applied.forget_at, 150 * MS + 1);
        // Owing past its refill time, it is value until full again.
        let applied = apply(&mut value, 1, contract(), hits(2, 25, 1), CYCLE, 60 * MS);
        assert_eq!(applied.forget_at, value.bucket().unwrap().forgettable_at());
        assert!(applied.forget_at > 160 * MS + 1);
    }

    /// A batch the owner has applied, arriving again, is answered and nothing more: it is not
    /// value waiting.
    #[test]
    fn a_batch_applied_arriving_again_is_not_kept() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 2, 0), 0);
        let answer = send(&mut value, 1, hits(1, 2, 0), CYCLE);
        assert_eq!(answer.applied, 1);
        assert_eq!(value.counted(), 2);
        assert!(record(&value, 1).waiting.is_none());
    }

    /// Batches arriving in order hold no room for batches value waiting; one arriving ahead of
    /// a batch still missing is value, and the room given back once the missing one comes.
    #[test]
    fn only_batches_ahead_of_a_missing_one_hold_room() {
        let room = |value: &Value| {
            record(value, 1)
                .waiting
                .as_ref()
                .map_or(0, |waiting| waiting.capacity())
        };
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 1, 0), 0);
        send(&mut value, 1, hits(2, 1, 1), CYCLE);
        assert_eq!(room(&value), 0, "in order");
        send(&mut value, 1, hits(4, 1, 2), 2 * CYCLE);
        assert_eq!(value.counted(), 2, "the fourth waits for the third");
        assert!(room(&value) > 0);
        send(&mut value, 1, hits(3, 1, 2), 3 * CYCLE);
        assert_eq!(value.counted(), 4);
        assert_eq!(room(&value), 0, "given back");
    }

    /// The owner keeps at most [`MOST_UNACKNOWLEDGED`] of a data plane's batches waiting for a
    /// missing one: one more is answered unapplied, and counted when the data plane sends it
    /// again.
    #[test]
    fn the_owner_keeps_a_bounded_number_of_batches_waiting() {
        let waiting = |value: &Value| record(value, 1).waiting_len();
        let mut value = Value::default();
        let most = MOST_UNACKNOWLEDGED as u64;
        let last = most + 8;
        for sequence in 2..=last {
            assert_eq!(send(&mut value, 1, hits(sequence, 1, 0), 0).applied, 0);
        }
        assert_eq!(waiting(&value), MOST_UNACKNOWLEDGED);
        send(&mut value, 1, hits(2, 1, 0), CYCLE);
        assert_eq!(
            waiting(&value),
            MOST_UNACKNOWLEDGED,
            "one waiting, sent again"
        );
        send(&mut value, 1, hits(1, 1, 0), CYCLE);
        assert_eq!(value.counted(), 1 + most);
        for sequence in most + 2..=last {
            send(&mut value, 1, hits(sequence, 1, 0), 2 * CYCLE);
        }
        assert_eq!(value.counted(), last, "each counted once");
        assert_eq!(waiting(&value), 0);
    }

    /// A batch waiting, sent again emptied as older than a refill time, keeps the emptied
    /// tally, whichever comes first; with as many hits, the one charged earlier.
    #[test]
    fn a_batch_waiting_sent_again_keeps_the_smaller_tally() {
        let waited = |value: &Value| record(value, 1).waiting.as_ref().unwrap()[0].1;
        for (first, again, at) in [(5, 0, 200 * MS), (0, 5, 0)] {
            let mut value = Value::default();
            send(&mut value, 1, hits(2, first, 0), 0);
            send(&mut value, 1, hits(2, again, 0), 200 * MS);
            assert_eq!(waited(&value), Tally { count: 0, at });
        }
        let mut value = Value::default();
        send(&mut value, 1, hits(2, 5, 0), 0);
        send(&mut value, 1, hits(2, 5, 0), CYCLE);
        assert_eq!(waited(&value), Tally { count: 5, at: 0 });
    }

    /// A batch left waiting at or below the highest applied, as a merge with a copy that had
    /// applied further can leave one, holds up none after it.
    #[test]
    fn a_batch_waiting_below_the_highest_applied_holds_up_none() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 1, 0), 0);
        let held = value.planes.entry(1);
        held.applied = 4;
        held.waiting = Some(Box::new(vec![(5, Tally { count: 1, at: 0 })]));
        send(&mut value, 1, hits(7, 1, 4), CYCLE);
        // The fifth sent again is applied, then the sixth: the seventh follows on.
        send(&mut value, 1, hits(5, 1, 4), 2 * CYCLE);
        assert_eq!(record(&value, 1).applied, 5);
        let answer = send(&mut value, 1, hits(6, 1, 5), 3 * CYCLE);
        assert_eq!(answer.applied, 7);
        assert_eq!(record(&value, 1).waiting_len(), 0);
    }

    /// A data plane's record of a key goes once it has admitted nothing on it for
    /// [`RECORD_KEPT`] before the latest admission, though another keeps the key in use, and
    /// the room it took with it; one that has only polled goes at once.
    #[test]
    fn a_record_silent_for_ten_minutes_is_retired() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 1, 0), 0);
        send(&mut value, 2, hits(1, 1, 0), 0);
        send(&mut value, 2, hits(2, 1, 1), RECORD_KEPT);
        send(&mut value, 3, Report::Poll, RECORD_KEPT);
        assert_eq!(recorded(&value), [1, 2]);
        send(&mut value, 2, hits(3, 1, 2), RECORD_KEPT + CYCLE);
        assert_eq!(recorded(&value), [2]);
        assert_eq!(value.planes.room(), 1);
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
        let mut value = Value::default();
        let send = |value: &mut Value, from, sequence, now| {
            apply(
                value,
                from,
                hourly,
                hits(sequence, 1, sequence - 1),
                CYCLE,
                now,
            );
        };
        send(&mut value, 1, 1, 0);
        send(&mut value, 2, 1, 0);
        send(&mut value, 2, 2, refill_time);
        assert_eq!(recorded(&value), [1, 2]);
        send(&mut value, 2, 3, refill_time + CYCLE);
        assert_eq!(recorded(&value), [2]);
    }

    /// A data plane back after its record went starts a fresh one from the batches it saw
    /// applied: none is counted twice.
    #[test]
    fn a_data_plane_back_after_its_record_went_is_counted_once() {
        let mut value = Value::default();
        send(&mut value, 1, hits(1, 2, 0), 0);
        send(&mut value, 2, hits(1, 1, 0), RECORD_KEPT + CYCLE);
        assert_eq!(recorded(&value), [2]);
        let answer = send(&mut value, 1, hits(2, 3, 1), RECORD_KEPT + 2 * CYCLE);
        assert_eq!(answer.applied, 2);
        assert_eq!(value.counted(), 6);
    }

    /// Hits are charged as of when they were admitted, not on arrival: one admitted 30 ms
    /// before, at 100 a second, has refilled by the time it arrives.
    #[test]
    fn a_hit_is_charged_as_of_when_it_was_admitted() {
        let mut value = Value::default();
        let late = Report::Hits {
            sequence: 1,
            count: 1,
            seen: 0,
            age: 30 * MS,
        };
        let answer = send(&mut value, 100, late, 1_000 * MS);
        let bucket = value.bucket().unwrap();
        assert_eq!(bucket.remaining(1_000 * MS), 10, "refilled since");
        assert_eq!(answer.behind, 0);
    }

    /// The owner charges back to the earliest hit however long ago, past the key's refill
    /// time too: 15 hits said to be 150 ms old have all refilled as they arrive.
    #[test]
    fn hits_are_charged_back_to_the_earliest_past_a_refill_time() {
        let mut value = Value::default();
        let old = Report::Hits {
            sequence: 1,
            count: 15,
            seen: 0,
            age: 150 * MS,
        };
        send(&mut value, 100, old, 1_000 * MS);
        assert_eq!(value.bucket().unwrap().remaining(1_000 * MS), 10);
    }
}
