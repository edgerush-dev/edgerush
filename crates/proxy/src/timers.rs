//! The worker's timers ([03 §2](../../../docs/03-data-plane.md)).
//!
//! Every deadline a worker keeps is a slot in its [`Timers`]: a [`heap`] of deadlines under
//! one Tokio sleep for the soonest, which a task of the worker's waits on
//! ([`Timers::run`]). What keeps a deadline — a client connection, an exchange with an
//! upstream — does so through an [`Alarm`], and nothing it does with it touches Tokio's
//! timer: a Tokio timer made, moved sooner or dropped takes the runtime's timer lock, and
//! doing that for every request is what this is instead of.
//!
//! When the sleep goes off, the owners whose deadlines have come are woken, and told the
//! time it went off: an owner reads no clock to learn that its deadline came. Nothing here
//! is shared with another worker, so nothing takes a lock.

// Public only for the fuzz targets, which drive exchanges and so give them timers, and
// for the benchmark of the heap.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

pub mod heap;

use heap::{Heap, Key, Waited};
use std::cell::RefCell;
use std::convert::Infallible;
#[cfg(any(test, feature = "fuzzing"))]
use std::future::Future;
use std::future::poll_fn;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::time::Instant;

/// One worker's timers.
#[derive(Debug, Default)]
pub struct Timers {
    inner: RefCell<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Each slot holds the waker of the task that waits on it, while one does.
    heap: Heap<Option<Waker>>,
    /// When the sleep is set for, while it is set for anything.
    armed: Option<std::time::Instant>,
    /// The task that waits on the sleep, to be woken when a deadline is queued sooner
    /// than the sleep is set for.
    driver: Option<Waker>,
    /// The wakers of the owners whose deadlines came, kept for their room.
    woken: Vec<Waker>,
}

impl Timers {
    /// A worker's timers, which [`Timers::run`] must be waiting on for any deadline to come.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// Waits on the sleep, for as long as the worker runs, and wakes each owner whose
    /// deadline comes. Spawned into the worker's `LocalSet` beside its listeners.
    pub(crate) async fn run(self: Rc<Self>) -> Infallible {
        let mut sleep = pin!(tokio::time::sleep_until(Instant::now()));
        poll_fn(|context| {
            loop {
                let mut inner = self.inner.borrow_mut();
                if !inner
                    .driver
                    .as_ref()
                    .is_some_and(|driver| driver.will_wake(context.waker()))
                {
                    inner.driver = Some(context.waker().clone());
                }
                let Some(next) = inner.heap.soonest() else {
                    inner.armed = None;
                    return Poll::Pending;
                };
                // Set only to go off sooner: when it goes off before the next entry, which
                // came after it, nothing is due and it is set again, for that entry.
                if inner.armed.is_none_or(|armed| next < armed) {
                    sleep.as_mut().reset(Instant::from_std(next));
                    inner.armed = Some(next);
                }
                let Some(armed) = inner.armed else {
                    return Poll::Pending;
                };
                drop(inner);
                if sleep.as_mut().poll(context).is_pending() {
                    return Poll::Pending;
                }
                let mut inner = self.inner.borrow_mut();
                inner.armed = None;
                let mut woken = std::mem::take(&mut inner.woken);
                inner.heap.expire(armed, |waker| woken.extend(waker.take()));
                drop(inner);
                // Woken with nothing borrowed, whatever waking does.
                for waker in woken.drain(..) {
                    waker.wake();
                }
                self.inner.borrow_mut().woken = woken;
            }
        })
        .await
    }

    /// Runs `future` with the timers waited on beside it, for what has no worker to do that.
    #[cfg(any(test, feature = "fuzzing"))]
    pub async fn driving<F: Future>(self: &Rc<Self>, future: F) -> F::Output {
        let mut run = pin!(Rc::clone(self).run());
        let mut future = pin!(future);
        poll_fn(|context| {
            if let Poll::Ready(never) = run.as_mut().poll(context) {
                match never {}
            }
            future.as_mut().poll(context)
        })
        .await
    }
}

/// One owner's deadline, in its worker's timers.
#[derive(Debug)]
pub(crate) struct Alarm {
    timers: Rc<Timers>,
    key: Key,
    /// The shortest time the owner sets a deadline for, which is as far ahead as one is
    /// queued: then the deadlines it sets after it fall due later and are not queued.
    /// Without one, a deadline is queued for itself.
    ahead: Option<Duration>,
}

impl Alarm {
    /// An owner's deadline in `timers`, for one that sets them at least `ahead` from when
    /// it sets them. One set sooner than that is kept all the same, and queued again.
    pub(crate) fn new(timers: &Rc<Timers>, ahead: Option<Duration>) -> Self {
        let key = timers.inner.borrow_mut().heap.add(None);
        Self {
            timers: Rc::clone(timers),
            key,
            ahead,
        }
    }

    /// Says how far ahead the owner's deadlines are set, once it knows.
    pub(crate) fn set_ahead(&mut self, ahead: Duration) {
        self.ahead = Some(ahead);
    }

    /// Waits for `due`, the owner's deadline that is next: ready once it has come. Until
    /// then the task is woken when it comes, and may be woken before.
    ///
    /// Asking about a later deadline than before leaves what is queued where it is; about
    /// a sooner one, queues it. `due` is always the soonest the owner has.
    pub(crate) fn poll_until(&mut self, context: &mut Context<'_>, due: Instant) -> Poll<()> {
        let due = due.into_std();
        let ahead = self.ahead;
        let waker = context.waker();
        let mut inner = self.timers.inner.borrow_mut();
        let waited = inner.heap.wait(
            self.key,
            due,
            || match ahead {
                Some(ahead) => (Instant::now() + ahead).into_std(),
                None => due,
            },
            |waiting| {
                if !waiting.as_ref().is_some_and(|kept| kept.will_wake(waker)) {
                    *waiting = Some(waker.clone());
                }
            },
        );
        let at = match waited {
            Waited::Came => return Poll::Ready(()),
            Waited::Waiting => return Poll::Pending,
            Waited::Queued(at) => at,
        };
        let driver = inner
            .armed
            .is_none_or(|armed| at < armed)
            .then(|| inner.driver.clone())
            .flatten();
        drop(inner);
        #[cfg(test)]
        QUEUED.with(|count| count.set(count.get() + 1));
        // Queued sooner than the sleep is set for, which only the driver can move.
        if let Some(driver) = driver {
            driver.wake();
        }
        Poll::Pending
    }

    /// Whether `due` had come the last time the owner's deadline was found to have come.
    pub(crate) fn reached(&self, due: Instant) -> bool {
        self.timers
            .inner
            .borrow()
            .heap
            .reached(self.key, due.into_std())
    }
}

impl Drop for Alarm {
    fn drop(&mut self) {
        self.timers.inner.borrow_mut().heap.remove(self.key);
    }
}

#[cfg(test)]
thread_local! {
    static QUEUED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread's alarms have queued a deadline.
#[cfg(test)]
pub(crate) fn times_queued() -> usize {
    QUEUED.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let timers = Timers::new();
        let start = Instant::now();
        let mut alarm = Alarm::new(&timers, Some(AHEAD));
        let due = start + secs(3);
        assert!(!alarm.reached(due));
        assert_eq!(timers.driving(waited(&mut alarm, || due)).await, due);
        assert!(alarm.reached(due));
        assert!(!alarm.reached(due + Duration::from_millis(1)));
    }

    /// Queued no further ahead than the shortest a deadline is set for, and then for the
    /// deadline itself: one early wake of the worker's task, and the deadline kept.
    #[tokio::test(start_paused = true)]
    async fn a_far_deadline_is_reached_by_way_of_one_early_wake() {
        let timers = Timers::new();
        let start = Instant::now();
        let before = times_queued();
        let mut alarm = Alarm::new(&timers, Some(AHEAD));
        let due = start + secs(60);
        assert_eq!(timers.driving(waited(&mut alarm, || due)).await, due);
        assert_eq!(times_queued() - before, 1);
    }

    /// A deadline that keeps moving later, as one does with every request, is queued once
    /// and kept at wherever it got to.
    #[tokio::test(start_paused = true)]
    async fn a_deadline_moving_later_is_queued_once() {
        let timers = Timers::new();
        let start = Instant::now();
        let before = times_queued();
        let mut alarm = Alarm::new(&timers, Some(AHEAD));
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
            // Polled again and again while the deadline moves, as progress on a socket
            // would poll it.
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
        };
        let (came, ()) = timers.driving(async { tokio::join!(came, checking) }).await;
        assert_eq!(came, start + AHEAD + secs(1));
        assert_eq!(times_queued() - before, 1);
    }

    /// A deadline that comes sooner than the worker's sleep is set for moves the sleep.
    #[tokio::test(start_paused = true)]
    async fn a_deadline_coming_sooner_moves_the_sleep() {
        let timers = Timers::new();
        let start = Instant::now();
        let mut far = Alarm::new(&timers, None);
        let mut near = Alarm::new(&timers, None);
        let waiting = async {
            let far_came = waited(&mut far, || start + secs(40));
            let near_came = async {
                // Asked after the far one has set the sleep.
                tokio::task::yield_now().await;
                waited(&mut near, || start + secs(2)).await
            };
            tokio::join!(far_came, near_came)
        };
        let (far_came, near_came) = timers.driving(waiting).await;
        assert_eq!(near_came, start + secs(2));
        assert_eq!(far_came, start + secs(40));
    }

    /// An owner that goes takes its deadline with it: nobody is woken for it, and the
    /// rest still come.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_alarm_takes_its_deadline_with_it() {
        let timers = Timers::new();
        let start = Instant::now();
        let mut kept = Alarm::new(&timers, None);
        let mut gone = Alarm::new(&timers, None);
        let came = timers
            .driving(async {
                let early = start + secs(1);
                let polled = poll_fn(|context| Poll::Ready(gone.poll_until(context, early))).await;
                assert!(polled.is_pending());
                drop(gone);
                waited(&mut kept, || start + secs(3)).await
            })
            .await;
        assert_eq!(came, start + secs(3));
    }
}
