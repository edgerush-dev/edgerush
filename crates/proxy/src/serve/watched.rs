//! An HTTP/1 try's TCP connect, carried on when the try lets go of it, so that an endpoint
//! whose connects hang is still set aside ([03 §6](../../docs/03-data-plane.md)).
//!
//! A try's clock includes its connect. Cut first — by its own clock, the request's, or its
//! client going — a connect inside the try's future went with it, and an endpoint whose
//! connects hang was never set aside: every try that drew it waited out its clock. So a
//! try connects through [`Connecting`], which, let go of before its connect ends and before
//! the try's own connect bound, hands the connect as it stands — the same future, never
//! begun again — to its worker's [`Watcher`], with the try's place and count at the endpoint
//! and the deadline the try gave it. The watcher sees it to that deadline: refused or out
//! of time sets the endpoint aside, and a connection made is closed.
//!
//! At most one connect is carried on for each destination on a worker, whether queued or
//! being watched: one is all setting it aside takes, and that is every worker's. Another let
//! go of meanwhile ends with its place, as before. So a destination whose connects hang is
//! set aside within the connect bound, while not every connect let go of is seen to its end:
//! an endpoint that fails now and then may not be. Each one carried on holds its place, so
//! the worker's places bound them all.
//!
//! A try that connects pays for none of this but a box for its connect, which is what lets
//! the connect move: the watcher's task, its timers and its queue are for the connects let
//! go of.

use super::Admitted;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::dial::{self, Unconnected};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};

/// A TCP connect, boxed so that it can move once begun.
type Connect = Pin<Box<dyn Future<Output = Result<TcpStream, Unconnected>>>>;

/// A worker's connects carried on for the tries that let go of them.
#[derive(Default)]
pub(super) struct Watcher {
    /// The destinations, by key, with a connect queued or watched.
    watched: RefCell<HashSet<u64>>,
    /// Handed over and not yet taken up by the task.
    queue: RefCell<Vec<Carried>>,
    /// The task that sees them through has been started.
    started: Cell<bool>,
    /// That task is there to take them: not yet ended with its worker.
    open: Cell<bool>,
    /// What wakes it.
    waker: RefCell<Option<Waker>>,
}

impl std::fmt::Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watcher")
            .field("watched", &self.watched.borrow().len())
            .field("queued", &self.queue.borrow().len())
            .finish_non_exhaustive()
    }
}

impl Watcher {
    /// How many destinations have a connect queued or watched.
    #[cfg(test)]
    pub(super) fn watched(&self) -> usize {
        self.watched.borrow().len()
    }

    /// How many connects wait to be taken up.
    #[cfg(test)]
    pub(super) fn queued(&self) -> usize {
        self.queue.borrow().len()
    }

    /// Starts the task that sees connects through, once in the worker's life, from a try
    /// that is connecting: a place a task can be started from, which a try being dropped
    /// is not.
    fn start(self: &Rc<Self>) {
        if self.started.replace(true) {
            return;
        }
        self.open.set(true);
        let _detached = tokio::task::spawn_local(watch(Rc::clone(self)));
    }

    /// Takes over `connect`, let go of by a try: queued with what it holds, unless its
    /// destination already has one carried on, or the task has ended with its worker, and
    /// then dropped here, and all it holds with it. Never waits.
    fn adopt(
        self: &Rc<Self>,
        connect: Connect,
        identity: &Arc<ReuseIdentity>,
        deadline: Instant,
        admitted: Option<Admitted>,
    ) {
        if !self.open.get() {
            return;
        }
        let Some(registered) = Registered::of(self, identity.key()) else {
            return;
        };
        self.queue.borrow_mut().push(Carried {
            connect,
            identity: Arc::clone(identity),
            due: Box::pin(tokio::time::sleep_until(deadline)),
            _admitted: admitted,
            _registered: registered,
        });
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

/// A destination with a connect queued or watched, until this is dropped, with the connect.
struct Registered {
    watcher: Rc<Watcher>,
    key: u64,
}

impl Registered {
    /// The destination `key` registered, unless it already was.
    fn of(watcher: &Rc<Watcher>, key: u64) -> Option<Self> {
        watcher.watched.borrow_mut().insert(key).then(|| Self {
            watcher: Rc::clone(watcher),
            key,
        })
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.watcher.watched.borrow_mut().remove(&self.key);
    }
}

/// A connect carried on, with everything it holds: let go of when this is dropped.
struct Carried {
    connect: Connect,
    identity: Arc<ReuseIdentity>,
    /// The deadline its try gave it, its own timer only now that nobody else waits.
    due: Pin<Box<Sleep>>,
    _admitted: Option<Admitted>,
    _registered: Registered,
}

impl Carried {
    /// Whether it has ended, and if so what it came to, settled: refused or out of time
    /// sets its endpoint aside; a connection made is closed.
    fn poll_ended(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let ended = match self.connect.as_mut().poll(cx) {
            Poll::Ready(connected) => connected,
            Poll::Pending => match self.due.as_mut().poll(cx) {
                Poll::Ready(()) => Err(Unconnected::Endpoint(std::io::ErrorKind::TimedOut.into())),
                Poll::Pending => return Poll::Pending,
            },
        };
        if let Err(unconnected) = ended
            && let Some(why) = unconnected.aside()
        {
            self.identity.set_aside(why);
        }
        Poll::Ready(())
    }
}

/// Marks the watcher closed when its task ends, with its worker, and lets go of what was
/// still queued.
struct Closing(Rc<Watcher>);

impl Drop for Closing {
    fn drop(&mut self) {
        self.0.open.set(false);
        let queued = std::mem::take(&mut *self.0.queue.borrow_mut());
        drop(queued);
    }
}

/// Sees every connect handed over through to its end, each polled on every turn so that
/// none waits on another. Ends only with its worker.
async fn watch(watcher: Rc<Watcher>) {
    let _closing = Closing(Rc::clone(&watcher));
    let mut watching: Vec<Carried> = Vec::new();
    poll_fn(|cx| {
        watching.append(&mut watcher.queue.borrow_mut());
        watching.retain_mut(|carried| carried.poll_ended(cx).is_pending());
        *watcher.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::<()>::Pending
    })
    .await;
}

/// A try's TCP connect to `identity`, made in the try's own future, and handed to
/// `watcher` with the try's place if the try lets go of it before it ends and before
/// `deadline` ([module docs](self)). The try's own bound on it is the try's to keep.
pub(super) struct Connecting<'a> {
    connect: Option<Connect>,
    identity: &'a Arc<ReuseIdentity>,
    deadline: Instant,
    admitted: &'a mut Option<Admitted>,
    watcher: &'a Rc<Watcher>,
}

impl<'a> Connecting<'a> {
    /// The connect begun, by `deadline`, for a try whose place is in `admitted`.
    pub(super) fn new(
        identity: &'a Arc<ReuseIdentity>,
        deadline: Instant,
        admitted: &'a mut Option<Admitted>,
        watcher: &'a Rc<Watcher>,
    ) -> Self {
        watcher.start();
        Self {
            connect: Some(Box::pin(dial::connect(identity.address()))),
            identity,
            deadline,
            admitted,
            watcher,
        }
    }
}

impl Future for Connecting<'_> {
    type Output = Result<TcpStream, Unconnected>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(connect) = this.connect.as_mut() else {
            return Poll::Pending;
        };
        let connected = std::task::ready!(connect.as_mut().poll(cx));
        this.connect = None;
        Poll::Ready(connected)
    }
}

impl Drop for Connecting<'_> {
    fn drop(&mut self) {
        let Some(connect) = self.connect.take() else {
            return;
        };
        // Let go of at its bound, the try's own timeout has it: nothing to carry on.
        if Instant::now() >= self.deadline {
            return;
        }
        self.watcher
            .adopt(connect, self.identity, self.deadline, self.admitted.take());
    }
}
