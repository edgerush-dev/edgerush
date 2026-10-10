//! EdgeRush: a cooperative turn deadline, independent of protocol timers and scheduling.

use std::task::{Context, Poll};
use std::time::Instant;

/// An absolute deadline shared by all work in one owner task poll.
///
/// Copying the budget does not renew it. Checkpoints cannot interrupt a frame encode,
/// decode or underlying I/O poll already in progress. Added by EdgeRush's vendored copy.
#[derive(Clone, Copy, Debug)]
pub struct TurnBudget {
    deadline: Instant,
    now: fn() -> Instant,
}

impl TurnBudget {
    /// Uses the system monotonic clock. Protocol timeouts remain independent.
    pub fn new(deadline: Instant) -> Self {
        Self::with_clock(deadline, Instant::now)
    }

    /// Uses an injected monotonic clock in the same time domain as `deadline`.
    /// The clock must not block or reenter the connection; checkpoints also run while
    /// stream state is locked. This permits deterministic tests without sleeping.
    pub fn with_clock(deadline: Instant, now: fn() -> Instant) -> Self {
        Self { deadline, now }
    }

    /// Whether the deadline has been reached, including equality.
    pub fn expired(&self) -> bool {
        (self.now)() >= self.deadline
    }
}

// Each codec half retains the same absolute budget and its own exhaustion latch. No
// allocation or synchronization is needed; both halves belong to the connection task.
#[derive(Debug, Default)]
pub(crate) struct Turn {
    budget: Option<TurnBudget>,
    exhausted: bool,
}

impl Turn {
    pub(crate) fn set(&mut self, budget: Option<TurnBudget>) {
        self.budget = budget;
        self.exhausted = false;
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn expired(&mut self) -> bool {
        self.exhausted = self.exhausted || self.budget.is_some_and(|budget| budget.expired());
        self.exhausted
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.expired() {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
