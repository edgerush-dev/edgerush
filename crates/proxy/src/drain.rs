//! Whether a worker is draining, and what wakes the connections waiting to hear it
//! ([03 §10](../../docs/03-data-plane.md)).
//!
//! Worker-local: the process-wide flag is on [`crate::Proxy`], and each worker's sweep
//! brings it here, so no connection watches anything another thread writes.

use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// One worker's drain.
#[derive(Debug, Default)]
pub(crate) struct Drain {
    on: Cell<bool>,
    notify: Notify,
}

impl Drain {
    /// Starts draining; what waits to hear it is woken, once.
    pub(crate) fn start(&self) {
        if !self.on.replace(true) {
            self.notify.notify_waiters();
        }
    }

    /// Whether draining has started.
    pub(crate) fn is_on(&self) -> bool {
        self.on.get()
    }

    /// Something to poll until draining starts. Made once for a connection, before it
    /// first waits.
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Ready once draining has started, watching for it through `notified` until then.
    pub(crate) fn poll_on(
        &self,
        notified: Pin<&mut Notified<'_>>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if self.is_on() {
            return Poll::Ready(());
        }
        notified.poll(cx)
    }

    /// Until draining starts.
    pub(crate) async fn started(&self) {
        let notified = self.notified();
        if !self.is_on() {
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
