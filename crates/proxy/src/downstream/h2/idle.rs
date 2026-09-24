//! An inactivity clock for one direction of one stream
//! ([14 §8](../../../../../docs/14-downstream-server.md)).
//!
//! It runs only while that direction is waited on and nothing comes, and progress stops it:
//! a stream held back by the other end's backpressure is not charged for the wait. Its timer
//! is made the first time the stream waits and reset after that, so a stream that never waits
//! — a small request, an answer that always finds room — never makes one.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

/// One direction's inactivity clock.
#[derive(Debug)]
pub(crate) struct Idle {
    bound: Duration,
    timer: Option<Pin<Box<Sleep>>>,
    /// Waited on, with nothing since the wait began.
    waiting: bool,
}

impl Idle {
    /// A clock that runs out after `bound` of waiting with nothing.
    pub(crate) fn new(bound: Duration) -> Self {
        Self {
            bound,
            timer: None,
            waiting: false,
        }
    }

    /// The direction was waited on and had nothing: ready once it has waited its bound
    /// since the wait began, and otherwise watching for that moment.
    pub(crate) fn waiting(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.waiting {
            self.waiting = true;
            let due = Instant::now() + self.bound;
            match &mut self.timer {
                Some(timer) => timer.as_mut().reset(due),
                None => self.timer = Some(Box::pin(tokio::time::sleep_until(due))),
            }
        }
        match &mut self.timer {
            Some(timer) => timer.as_mut().poll(cx),
            // Set just above whenever it was not.
            None => Poll::Pending,
        }
    }

    /// The direction moved: the clock stops until it is waited on again.
    pub(crate) fn moved(&mut self) {
        self.waiting = false;
    }
}
