//! A timer that is moved only to go off sooner.
//!
//! Moving a Tokio timer sooner, making one and dropping one all take the runtime's timer
//! driver's lock; letting one run on to a later time takes nothing. What keeps deadlines —
//! a client connection, an exchange with an upstream — sees its next deadline move with
//! every step of every request, mostly later and now and then sooner, so the timer is left
//! where it is while it is no later than the deadline, and checked against the deadline
//! when it goes off: a timer that went off before the deadline, which moved on since it was
//! set, is set again for the deadline itself. A deadline still falls due at its own time,
//! never the timer's.
//!
//! Set sooner, it goes no further ahead than the shortest time any deadline is ever set
//! for: a deadline set later, from a later now, then falls due after it and moves nothing.
//! That is how a busy connection sets its timer once rather than once a request, at the
//! price of a quiet one being woken once more than it would be.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep};

/// One timer, for whatever deadline is next.
///
/// The default one is set for the deadline itself whenever it has to be moved sooner: for
/// what lives no longer than the deadlines it keeps, and so has no later ones to be ready
/// for.
#[derive(Debug, Default)]
pub(crate) struct Alarm {
    /// Made the first time it is set, and kept: made and dropped, it takes the driver's
    /// lock both times.
    timer: Option<Pin<Box<Sleep>>>,
    /// When it is set for: never later than the deadline it was last asked about.
    armed: Option<Instant>,
    /// The shortest time a deadline is ever set for, which is as far ahead as the timer is
    /// set when it has to be moved sooner.
    ahead: Option<Duration>,
}

impl Alarm {
    /// An alarm for deadlines that are set at least `ahead` from when they are set. One
    /// set sooner than that is kept all the same, and moves the timer to keep it.
    pub(crate) fn new(ahead: Duration) -> Self {
        Self {
            timer: None,
            armed: None,
            ahead: Some(ahead),
        }
    }

    /// Waits for `due`, the deadline that is next. Ready once it has come; until then the
    /// task is woken when the timer goes off, which may be before `due`.
    ///
    /// Asked about a later deadline than before, the timer stays where it is; about a
    /// sooner one, it is moved. Asking about a deadline and then about a later one while
    /// the sooner still stands loses the sooner: `due` is always the soonest there is.
    pub(crate) fn poll_until(&mut self, context: &mut Context<'_>, due: Instant) -> Poll<()> {
        if self.armed.is_none_or(|armed| armed > due) {
            let at = match self.ahead {
                Some(ahead) => due.min(Instant::now() + ahead),
                None => due,
            };
            self.arm(at);
        }
        loop {
            let Some(timer) = &mut self.timer else {
                // Set just above.
                return Poll::Pending;
            };
            match timer.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) if self.armed >= Some(due) => return Poll::Ready(()),
                // Early: the deadline moved on after the timer was set.
                Poll::Ready(()) => self.arm(due),
            }
        }
    }

    /// Whether `due` has come by the last time the timer went off.
    pub(crate) fn reached(&self, due: Instant) -> bool {
        self.armed >= Some(due) && self.timer.as_ref().is_some_and(|timer| timer.is_elapsed())
    }

    fn arm(&mut self, at: Instant) {
        match &mut self.timer {
            Some(timer) => timer.as_mut().reset(at),
            None => self.timer = Some(Box::pin(tokio::time::sleep_until(at))),
        }
        self.armed = Some(at);
        #[cfg(test)]
        SET.with(|set| set.set(set.get() + 1));
    }
}

#[cfg(test)]
thread_local! {
    static SET: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread's alarms have set their timers.
#[cfg(test)]
pub(crate) fn times_set() -> usize {
    SET.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    const AHEAD: Duration = Duration::from_secs(5);

    fn secs(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    /// Waits on `alarm` for the deadline `due` gives each time it is asked, and says
    /// when it came.
    async fn waited(alarm: &mut Alarm, mut due: impl FnMut() -> Instant) -> Instant {
        poll_fn(|context| alarm.poll_until(context, due())).await;
        Instant::now()
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_comes_at_its_time() {
        let start = Instant::now();
        let mut alarm = Alarm::new(AHEAD);
        let due = start + secs(3);
        assert!(!alarm.reached(due));
        assert_eq!(waited(&mut alarm, || due).await, due);
        assert!(alarm.reached(due));
        assert!(!alarm.reached(due + Duration::from_millis(1)));
    }

    /// Set no further ahead than the shortest a deadline is set for, and then for the
    /// deadline itself: one early wake, and the deadline kept.
    #[tokio::test(start_paused = true)]
    async fn a_far_deadline_is_reached_by_way_of_one_early_wake() {
        let start = Instant::now();
        let before = times_set();
        let mut alarm = Alarm::new(AHEAD);
        let due = start + secs(60);
        assert_eq!(waited(&mut alarm, || due).await, due);
        assert_eq!(times_set() - before, 2);
    }

    /// A deadline that keeps moving later, as one does with every request, moves the timer
    /// only when the timer goes off early, and is kept at wherever it got to.
    #[tokio::test(start_paused = true)]
    async fn a_deadline_moving_later_leaves_the_timer_where_it_is() {
        let start = Instant::now();
        let before = times_set();
        let mut alarm = Alarm::new(AHEAD);
        let mut due = start + AHEAD;
        let mut polled = 0;
        let came = waited(&mut alarm, || {
            polled += 1;
            if polled <= 100 {
                due += Duration::from_millis(10);
            }
            due
        });
        let checking = async {
            // Polled again and again while the deadline moves: nothing is woken by it, so
            // the polling is done here, as progress on a socket would do it.
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
        };
        let (came, ()) = tokio::join!(came, checking);
        assert_eq!(came, start + AHEAD + secs(1));
        assert!(
            times_set() - before <= 2,
            "set {} times",
            times_set() - before
        );
    }

    /// The default alarm goes to the deadline at once.
    #[tokio::test(start_paused = true)]
    async fn a_default_alarm_is_set_for_the_deadline_itself() {
        let start = Instant::now();
        let before = times_set();
        let mut alarm = Alarm::default();
        let due = start + secs(60);
        assert_eq!(waited(&mut alarm, || due).await, due);
        assert_eq!(times_set() - before, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_coming_sooner_moves_the_timer() {
        let start = Instant::now();
        let mut alarm = Alarm::new(AHEAD);
        let mut polled = 0;
        let came = waited(&mut alarm, || {
            polled += 1;
            if polled == 1 {
                start + secs(4)
            } else {
                start + secs(2)
            }
        });
        let nudging = async {
            tokio::time::sleep(secs(1)).await;
        };
        let (came, ()) = tokio::join!(came, nudging);
        assert_eq!(came, start + secs(2));
    }
}
