//! The owner applying one entry of a data plane's batch to its key, on `Node::update`: the
//! owner's applying (`edgerush_ratelimit::service::owner::apply`), by the wall clock the store
//! gives it, told the store as what changed and when the key may be forgotten.

use crate::value::Value;
use edgerush_cluster::{Changed, Kept, Time};
use edgerush_ratelimit::service::owner;
use edgerush_ratelimit::service::{Contract, PlaneId, Report};

pub use owner::Answered;

/// Data plane `from`'s report on a key it holds under `contract` reaches the key's owner at
/// `now`, by the wall clock; its data planes send every `cycle` nanoseconds. The answer to
/// give, and what changed for the store to copy: hits are a change of the whole; a poll only
/// keeps the key from being forgotten, and is copied to nobody.
pub fn apply(
    kept: &mut Kept<Value>,
    from: PlaneId,
    contract: Contract,
    report: Report,
    cycle: u64,
    now: Time,
) -> (Answered, Changed) {
    let applied = owner::apply(&mut kept.value, from, contract, report, cycle, now);
    kept.forget_at = applied.forget_at;
    let changed = if applied.changed {
        Changed::Whole
    } else {
        Changed::Nothing
    };
    (applied.answered, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_ratelimit::reference::Model;
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

    fn hits(sequence: u64, count: u64, seen: u64) -> Report {
        Report::Hits {
            sequence,
            count,
            seen,
            age: 0,
        }
    }

    /// The store is told a key is forgotten once quiet for a refill time and full again; a
    /// poll keeps it a refill time more, as a change of nothing to copy, and hits as a change
    /// of the whole.
    #[test]
    fn a_key_is_forgotten_a_refill_time_after_its_last_report() {
        let mut kept = Kept::default();
        let (_, changed) = apply(&mut kept, 1, contract(), hits(1, 1, 0), CYCLE, 0);
        assert_eq!(changed, Changed::Whole);
        assert_eq!(kept.forget_at, 100 * MS + 1, "a refill time");
        let (_, changed) = apply(&mut kept, 1, contract(), Report::Poll, CYCLE, 50 * MS);
        assert_eq!(changed, Changed::Nothing);
        assert_eq!(kept.forget_at, 150 * MS + 1);
        // Owing past its refill time, it is kept until full again.
        apply(&mut kept, 1, contract(), hits(2, 25, 1), CYCLE, 60 * MS);
        assert_eq!(
            kept.forget_at,
            kept.value.bucket().unwrap().forgettable_at()
        );
        assert!(kept.forget_at > 160 * MS + 1);
    }
}
