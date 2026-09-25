//! How many retries an upstream may be sent: a share of its requests, and a few a second
//! whatever the share ([03 §6](../../../docs/03-data-plane.md)).
//!
//! tower's `TpsBudget`, which linkerd bounds retries with, at its defaults: retries up to
//! a fifth of the requests of the last ten seconds, and ten a second besides so that a
//! quiet upstream can still be retried. Each request deposits; each retry withdraws five
//! times as much; the ten seconds are kept in ten slots of a second each, which expire as
//! time goes by. Nothing here reads a clock: the time is passed in.

use std::time::Duration;
use tokio::time::Instant;

/// Slots the window is kept in.
const SLOTS: usize = 10;

/// The window: what was deposited and withdrawn longer ago than this is forgotten.
const WINDOW: Duration = Duration::from_secs(10);

/// Retries allowed a second whatever the share.
const AT_LEAST_A_SECOND: isize = 10;

/// What one retry withdraws: one over the share of requests that may be retried, a fifth.
const WITHDRAWN: isize = 5;

/// One upstream's retry budget, as one worker keeps it.
#[derive(Debug)]
pub(crate) struct Budget {
    slots: [isize; SLOTS],
    /// The slot being written.
    at: usize,
    /// When it began.
    since: Instant,
}

impl Budget {
    /// A budget that nothing has been deposited in yet, at `now`.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            slots: [0; SLOTS],
            at: 0,
            since: now,
        }
    }

    /// A request went, at `now`.
    pub(crate) fn deposit(&mut self, now: Instant) {
        self.expire(now);
        self.slots[self.at] += 1;
    }

    /// Takes one retry out of the budget, at `now`, if it has one.
    pub(crate) fn withdraw(&mut self, now: Instant) -> bool {
        self.expire(now);
        let reserve = AT_LEAST_A_SECOND * WINDOW.as_secs() as isize * WITHDRAWN;
        let balance: isize = self.slots.iter().sum::<isize>() + reserve;
        if balance < WITHDRAWN {
            return false;
        }
        self.slots[self.at] -= WITHDRAWN;
        true
    }

    /// Forgets the slots that have fallen out of the window.
    fn expire(&mut self, now: Instant) {
        let slot = WINDOW / SLOTS as u32;
        let mut passed = 0;
        while now.saturating_duration_since(self.since) >= slot && passed < SLOTS {
            self.at = (self.at + 1) % SLOTS;
            self.slots[self.at] = 0;
            self.since += slot;
            passed += 1;
        }
        // Idle for longer than the window: all of it is gone, and the clock catches up.
        if passed == SLOTS {
            self.since = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retries_allowed(budget: &mut Budget, now: Instant) -> usize {
        let mut allowed = 0;
        while budget.withdraw(now) {
            allowed += 1;
            assert!(allowed < 100_000, "a budget without an end");
        }
        allowed
    }

    #[test]
    fn a_quiet_upstream_may_be_retried_ten_times_a_second_over_the_window() {
        let now = Instant::now();
        let mut budget = Budget::new(now);
        assert_eq!(retries_allowed(&mut budget, now), 100);
        // What was withdrawn is forgotten with its slot, and the reserve is there again.
        let later = now + WINDOW;
        assert_eq!(retries_allowed(&mut budget, later), 100);
    }

    #[test]
    fn a_busy_upstream_may_be_retried_a_fifth_of_its_requests_more() {
        let now = Instant::now();
        let mut budget = Budget::new(now);
        for _ in 0..1000 {
            budget.deposit(now);
        }
        assert_eq!(retries_allowed(&mut budget, now), 100 + 200);
    }

    #[test]
    fn requests_older_than_the_window_count_for_nothing() {
        let now = Instant::now();
        let mut budget = Budget::new(now);
        for _ in 0..1000 {
            budget.deposit(now);
        }
        // Half the window on, the deposits are still in it.
        let mut half = Budget::new(now);
        for _ in 0..1000 {
            half.deposit(now);
        }
        assert_eq!(retries_allowed(&mut half, now + WINDOW / 2), 300);
        // A whole window on, they are not.
        assert_eq!(
            retries_allowed(&mut budget, now + WINDOW + Duration::from_millis(1)),
            100
        );
    }

    #[test]
    fn a_long_idle_budget_starts_afresh() {
        let now = Instant::now();
        let mut budget = Budget::new(now);
        assert_eq!(retries_allowed(&mut budget, now), 100);
        let much_later = now + WINDOW * 100;
        assert_eq!(retries_allowed(&mut budget, much_later), 100);
        // And the window then runs from that moment, not the one long ago: what was just
        // withdrawn stays spent for the window, and only then is forgotten.
        assert_eq!(retries_allowed(&mut budget, much_later + WINDOW / 2), 0);
        assert_eq!(retries_allowed(&mut budget, much_later + WINDOW), 100);
    }
}
