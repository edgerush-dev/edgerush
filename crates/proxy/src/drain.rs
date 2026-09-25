//! Whether a worker is draining, and what wakes the connections waiting to hear it
//! ([03 §10](../../docs/03-data-plane.md)).
//!
//! Worker-local: the process-wide flag is on [`crate::Proxy`], and each worker's sweep
//! brings it here, so no connection watches anything another thread writes. For the same
//! reason what waits is kept here too, without a lock: a keep-alive connection asks again
//! every time it goes back to waiting, once a request, and a waiter asking again with the
//! waker it gave before costs a comparison.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// One worker's drain.
#[derive(Debug, Default)]
pub(crate) struct Drain {
    on: Cell<bool>,
    waiting: RefCell<Waiting>,
}

/// The wakers of what waits, each in a slot of its own, and the slots let go of, to be
/// used again rather than grow.
#[derive(Debug, Default)]
struct Waiting {
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
}

impl Drain {
    /// Starts draining; what waits to hear it is woken, once.
    pub(crate) fn start(&self) {
        if self.on.replace(true) {
            return;
        }
        // Taken out first: a waker may run what it wakes at once, and that may ask again.
        let wakers: Vec<Waker> = self
            .waiting
            .borrow_mut()
            .slots
            .iter_mut()
            .filter_map(Option::take)
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }

    /// Whether draining has started.
    pub(crate) fn is_on(&self) -> bool {
        self.on.get()
    }

    /// Something to poll until draining starts. Made once for a connection, before it
    /// first waits.
    pub(crate) fn notified(&self) -> Heard<'_> {
        Heard {
            drain: self,
            slot: None,
        }
    }

    /// Ready once draining has started, watching for it through `heard` until then.
    pub(crate) fn poll_on(&self, heard: Pin<&mut Heard<'_>>, cx: &mut Context<'_>) -> Poll<()> {
        heard.poll(cx)
    }

    /// Until draining starts.
    pub(crate) async fn started(&self) {
        self.notified().await;
    }
}

/// What a connection polls to hear that its worker drains.
#[derive(Debug)]
pub(crate) struct Heard<'a> {
    drain: &'a Drain,
    /// Its slot among the waiting, once it has waited.
    slot: Option<usize>,
}

impl Future for Heard<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.drain.is_on() {
            return Poll::Ready(());
        }
        let mut waiting = this.drain.waiting.borrow_mut();
        let Waiting { slots, free } = &mut *waiting;
        match this.slot.and_then(|at| slots.get_mut(at)) {
            Some(Some(kept)) if kept.will_wake(cx.waker()) => {}
            Some(held) => *held = Some(cx.waker().clone()),
            None => {
                let at = match free.pop() {
                    Some(at) => at,
                    None => {
                        slots.push(None);
                        slots.len() - 1
                    }
                };
                if let Some(held) = slots.get_mut(at) {
                    *held = Some(cx.waker().clone());
                }
                this.slot = Some(at);
            }
        }
        Poll::Pending
    }
}

impl Drop for Heard<'_> {
    fn drop(&mut self) {
        if let Some(at) = self.slot {
            let mut waiting = self.drain.waiting.borrow_mut();
            if let Some(held) = waiting.slots.get_mut(at) {
                *held = None;
            }
            waiting.free.push(at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    /// What waits is woken when draining starts, and what asks afterwards is told at once.
    #[tokio::test]
    async fn waiters_are_woken_and_latecomers_told() {
        let drain = std::rc::Rc::new(Drain::default());
        let waiting = {
            let drain = std::rc::Rc::clone(&drain);
            async move { drain.started().await }
        };
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let waiting = tokio::task::spawn_local(waiting);
                tokio::task::yield_now().await;
                assert!(!waiting.is_finished());
                drain.start();
                tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                    .await
                    .expect("not woken")
                    .unwrap();
                assert!(drain.is_on());
                tokio::time::timeout(std::time::Duration::from_secs(1), drain.started())
                    .await
                    .expect("a latecomer waited");
            })
            .await;
    }

    /// Counts its wakes.
    struct Counted(AtomicUsize);

    impl Wake for Counted {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Asking again keeps one slot, and takes the newer waker when it changes; a waiter let
    /// go of gives its slot back and is not woken.
    #[test]
    fn a_waiter_keeps_one_slot_and_gives_it_back() {
        let drain = Drain::default();
        let first = Arc::new(Counted(AtomicUsize::new(0)));
        let second = Arc::new(Counted(AtomicUsize::new(0)));
        let gone = Arc::new(Counted(AtomicUsize::new(0)));
        let (first_waker, second_waker, gone_waker) = (
            Waker::from(Arc::clone(&first)),
            Waker::from(Arc::clone(&second)),
            Waker::from(Arc::clone(&gone)),
        );

        let mut heard = std::pin::pin!(drain.notified());
        for _ in 0..3 {
            let mut cx = Context::from_waker(&first_waker);
            assert!(drain.poll_on(heard.as_mut(), &mut cx).is_pending());
        }
        assert_eq!(
            drain.waiting.borrow().slots.len(),
            1,
            "asking again took more room"
        );
        let mut cx = Context::from_waker(&second_waker);
        assert!(drain.poll_on(heard.as_mut(), &mut cx).is_pending());

        {
            let mut left = std::pin::pin!(drain.notified());
            let mut cx = Context::from_waker(&gone_waker);
            assert!(drain.poll_on(left.as_mut(), &mut cx).is_pending());
            assert_eq!(drain.waiting.borrow().slots.len(), 2);
        }
        // Its slot is used again.
        {
            let mut again = std::pin::pin!(drain.notified());
            let mut cx = Context::from_waker(&gone_waker);
            assert!(drain.poll_on(again.as_mut(), &mut cx).is_pending());
            assert_eq!(
                drain.waiting.borrow().slots.len(),
                2,
                "a slot let go of was not reused"
            );
        }

        drain.start();
        assert_eq!(
            first.0.load(Ordering::SeqCst),
            0,
            "woke a waker replaced since"
        );
        assert_eq!(second.0.load(Ordering::SeqCst), 1);
        assert_eq!(gone.0.load(Ordering::SeqCst), 0, "woke a waiter let go of");
        let mut cx = Context::from_waker(&first_waker);
        assert!(drain.poll_on(heard.as_mut(), &mut cx).is_ready());
    }
}
