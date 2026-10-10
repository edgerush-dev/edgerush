//! A connection's streams, run by the connection's own task rather than a task each
//! ([15 §3](../../../../../docs/15-http2-and-grpc.md)).
//!
//! A worker shares its turns by task, so a stream that is a task of its own gets a turn of its
//! own: a connection with a hundred streams open takes a hundred turns for every one an HTTP/1
//! connection beside it gets. Here a connection's streams wait in its queue, and its task polls
//! those that are ready, so many a turn and no more, then gives the turn back. Every stream
//! still runs as soon as it can — one waiting on its upstream is simply not ready — so nothing
//! is put in line behind another's exchange.
//!
//! A stream is held as what it was handed over as until it is first polled: its future,
//! kilobytes, is made only then, so streams waiting for their turn hold no more than their
//! requests.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Wake, Waker};

/// What a stream is in the queue.
enum State<W, F> {
    /// Handed over, its future not yet made.
    Waiting(W),
    /// Made, and running.
    Running(Pin<Box<F>>),
    /// Between the two, as its future is made.
    Taken,
}

/// One stream, and the waker that puts it back in line.
struct Entry<W, F> {
    state: State<W, F>,
    waker: Waker,
    told: Arc<Told>,
}

/// The streams of one connection: those handed over and not yet made, `W`, and those running,
/// `F`.
pub(crate) struct Queue<W, F> {
    entries: Vec<Option<Entry<W, F>>>,
    /// Places in `entries` a stream has left, to be taken again.
    free: Vec<usize>,
    ready: Arc<Ready>,
}

/// The streams of a connection woken since they were last polled, and the connection's task.
/// Shared with wakers, which may be woken from another thread: the queue itself never leaves
/// the worker.
#[derive(Default)]
struct Ready {
    list: Mutex<VecDeque<usize>>,
    driver: Mutex<Option<Waker>>,
}

impl Ready {
    fn push(&self, index: usize) {
        self.list
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(index);
        let driver = self
            .driver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(driver) = driver {
            driver.wake();
        }
    }

    fn pop(&self) -> Option<usize> {
        self.list
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.list
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }
}

/// A stream's waker: it puts the stream in line once, however often it is woken before the
/// stream is polled again.
struct Told {
    index: usize,
    queued: AtomicBool,
    ready: Arc<Ready>,
}

impl Wake for Told {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            self.ready.push(self.index);
        }
    }
}

impl<W, F: Future<Output = ()>> Queue<W, F> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            ready: Arc::new(Ready::default()),
        }
    }

    /// Takes in a stream as it was handed over, in line to be made and polled.
    pub(crate) fn push(&mut self, waiting: W) {
        let index = self.free.pop().unwrap_or(self.entries.len());
        let told = Arc::new(Told {
            index,
            queued: AtomicBool::new(true),
            ready: Arc::clone(&self.ready),
        });
        let entry = Entry {
            state: State::Waiting(waiting),
            waker: Waker::from(Arc::clone(&told)),
            told,
        };
        if index == self.entries.len() {
            self.entries.push(Some(entry));
        } else if let Some(place) = self.entries.get_mut(index) {
            *place = Some(entry);
        }
        self.ready.push(index);
    }

    /// Examines at most `most` ready entries and returns how many it examined, including
    /// stale wakes and streams refused by `make`. A live stream is made if needed and polled;
    /// `None` from `make` leaves the queue. A full allowance may leave entries for the next
    /// turn; the caller must arrange that turn. `driver` is woken when a stream joins the line.
    pub(crate) fn run(
        &mut self,
        driver: &Waker,
        most: usize,
        mut make: impl FnMut(W) -> Option<F>,
    ) -> usize {
        {
            let mut held = self
                .ready
                .driver
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !held.as_ref().is_some_and(|held| held.will_wake(driver)) {
                *held = Some(driver.clone());
            }
        }
        let mut examined = 0;
        while examined < most {
            let Some(index) = self.ready.pop() else {
                break;
            };
            // Refusals and stale wakes cost work even when no future is polled.
            examined += 1;
            // A waker outlives its stream: its place may be empty, or another's since.
            let Some(Some(entry)) = self.entries.get_mut(index) else {
                continue;
            };
            entry.told.queued.store(false, Ordering::Release);
            if let State::Waiting(_) = entry.state {
                let State::Waiting(waiting) = std::mem::replace(&mut entry.state, State::Taken)
                else {
                    continue;
                };
                match make(waiting) {
                    Some(future) => entry.state = State::Running(Box::pin(future)),
                    None => {
                        self.leave(index);
                        continue;
                    }
                }
            }
            let State::Running(future) = &mut entry.state else {
                continue;
            };
            let mut cx = Context::from_waker(&entry.waker);
            if future.as_mut().poll(&mut cx).is_ready() {
                self.leave(index);
            }
        }
        examined
    }

    fn leave(&mut self, index: usize) {
        if let Some(place) = self.entries.get_mut(index)
            && place.take().is_some()
        {
            self.free.push(index);
        }
    }

    /// What is left in it: the streams running, and those never made.
    #[cfg(test)]
    fn into_left(self) -> (Vec<Pin<Box<F>>>, Vec<W>) {
        let mut running = Vec::new();
        let mut waiting = Vec::new();
        for entry in self.entries.into_iter().flatten() {
            match entry.state {
                State::Running(future) => running.push(future),
                State::Waiting(w) => waiting.push(w),
                State::Taken => {}
            }
        }
        (running, waiting)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::task::Poll;

    /// A stream that is ready again as soon as it is polled, `left` times, saying each poll:
    /// bounded, so that a queue that never stops polling ends the test rather than running on.
    struct Busy {
        name: usize,
        left: usize,
        polled: Rc<RefCell<Vec<usize>>>,
    }

    impl Future for Busy {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.polled.borrow_mut().push(self.name);
            if self.left == 0 {
                return Poll::Ready(());
            }
            self.left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn busy(polled: &Rc<RefCell<Vec<usize>>>, left: usize) -> impl FnMut(usize) -> Option<Busy> {
        let polled = Rc::clone(polled);
        move |name| {
            Some(Busy {
                name,
                left,
                polled: Rc::clone(&polled),
            })
        }
    }

    /// A turn polls no more than it is allowed; the streams left wait in line for the next
    /// turn, and every one gets its turn in the order it came.
    #[test]
    fn streams_past_a_turns_allowance_wait_for_the_next_turn() {
        let polled = Rc::new(RefCell::new(Vec::new()));
        let mut queue = Queue::new();
        for name in 0..10 {
            queue.push(name);
        }
        let mut make = busy(&polled, 20);
        assert_eq!(queue.run(Waker::noop(), 4, &mut make), 4);
        assert!(!queue.ready.is_empty());
        assert_eq!(*polled.borrow(), [0, 1, 2, 3]);
        assert_eq!(queue.run(Waker::noop(), 4, &mut make), 4);
        assert!(!queue.ready.is_empty());
        assert_eq!(queue.run(Waker::noop(), 4, &mut make), 4);
        // Those polled first, ready again at once, come round again only after the rest.
        assert_eq!(*polled.borrow(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1]);
    }

    /// A stream's future is made on its first turn, not as it comes: those still waiting hold
    /// what they came as.
    #[test]
    fn a_stream_is_made_on_its_first_turn() {
        let polled = Rc::new(RefCell::new(Vec::new()));
        let made = Rc::new(Cell::new(0));
        let mut queue = Queue::new();
        for name in 0..10 {
            queue.push(name);
        }
        let mut inner = busy(&polled, 20);
        let counting = Rc::clone(&made);
        let mut make = move |name| {
            counting.set(counting.get() + 1);
            inner(name)
        };
        queue.run(Waker::noop(), 3, &mut make);
        assert_eq!(made.get(), 3);
        let (running, waiting) = queue.into_left();
        assert_eq!((running.len(), waiting), (3, (3..10).collect::<Vec<_>>()));
    }

    /// A stream woken many times before its turn is in line once.
    #[test]
    fn a_stream_woken_twice_is_in_line_once() {
        let polled = Rc::new(RefCell::new(Vec::new()));
        let wakers: Rc<RefCell<Vec<Waker>>> = Rc::default();
        let mut queue = Queue::new();
        queue.push(0);
        let (keeping, polling) = (Rc::clone(&wakers), Rc::clone(&polled));
        let mut make = move |name| {
            let (keeping, polling) = (Rc::clone(&keeping), Rc::clone(&polling));
            Some(std::future::poll_fn(move |cx| {
                polling.borrow_mut().push(name);
                keeping.borrow_mut().push(cx.waker().clone());
                Poll::<()>::Pending
            }))
        };
        queue.run(Waker::noop(), 10, &mut make);
        let waker = wakers.borrow()[0].clone();
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_eq!(queue.run(Waker::noop(), 10, &mut make), 1);
        assert!(queue.ready.is_empty());
        assert_eq!(*polled.borrow(), [0, 0]);
    }

    /// Waking a stream wakes the connection's task, to give it its turn.
    #[test]
    fn a_stream_woken_wakes_its_connection() {
        struct Counted(std::sync::atomic::AtomicUsize);
        impl Wake for Counted {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counted = Arc::new(Counted(std::sync::atomic::AtomicUsize::new(0)));
        let driver = Waker::from(Arc::clone(&counted));
        let wakers: Rc<RefCell<Vec<Waker>>> = Rc::default();
        let keeping = Rc::clone(&wakers);
        let mut queue = Queue::new();
        queue.push(0);
        let mut make = move |_| {
            let keeping = Rc::clone(&keeping);
            Some(std::future::poll_fn(move |cx| {
                keeping.borrow_mut().push(cx.waker().clone());
                Poll::<()>::Pending
            }))
        };
        queue.run(&driver, 10, &mut make);
        let before = counted.0.load(Ordering::SeqCst);
        wakers.borrow()[0].wake_by_ref();
        assert_eq!(counted.0.load(Ordering::SeqCst), before + 1);
    }

    /// A stream that has finished leaves, and its place is taken by the next; a waker it left
    /// behind puts the newcomer in line for no reason, which costs a poll and nothing more.
    #[test]
    fn a_finished_streams_place_is_taken_again() {
        let polled = Rc::new(RefCell::new(Vec::new()));
        let wakers: Rc<RefCell<Vec<Waker>>> = Rc::default();
        let (keeping, polling) = (Rc::clone(&wakers), Rc::clone(&polled));
        let mut make = move |name: usize| {
            let (keeping, polling) = (Rc::clone(&keeping), Rc::clone(&polling));
            let mut first = true;
            Some(std::future::poll_fn(move |cx| {
                polling.borrow_mut().push(name);
                keeping.borrow_mut().push(cx.waker().clone());
                // The first stream finishes on its first poll; the others wait.
                if name == 0 && first {
                    first = false;
                    return Poll::Ready(());
                }
                Poll::Pending
            }))
        };
        let mut queue = Queue::new();
        queue.push(0);
        queue.run(Waker::noop(), 10, &mut make);
        queue.push(1);
        queue.run(Waker::noop(), 10, &mut make);
        assert_eq!(queue.entries.len(), 1, "the place was not taken again");
        // The finished stream's waker, woken late.
        wakers.borrow()[0].wake_by_ref();
        assert_eq!(queue.run(Waker::noop(), 10, &mut make), 1);
        assert_eq!(*polled.borrow(), [0, 1, 1]);
    }

    /// A stream answered without a future — refused for want of storage — leaves at once,
    /// and its place is free for the next.
    #[test]
    fn a_stream_made_into_nothing_leaves() {
        let mut queue: Queue<usize, std::future::Pending<()>> = Queue::new();
        queue.push(0);
        queue.push(1);
        assert_eq!(queue.run(Waker::noop(), 10, |_| None), 2);
        assert_eq!(queue.free.len(), 2);
        let (running, waiting) = queue.into_left();
        assert!(running.is_empty() && waiting.is_empty());
    }

    /// Refusing requests at construction still spends the turn: the remaining requests
    /// keep their places until the next turn.
    #[test]
    fn refused_entries_spend_the_turns_allowance() {
        let mut queue: Queue<usize, std::future::Pending<()>> = Queue::new();
        for name in 0..5 {
            queue.push(name);
        }
        let mut refused = Vec::new();
        let examined = queue.run(Waker::noop(), 2, |name| {
            refused.push(name);
            None
        });
        assert_eq!(refused, [0, 1], "refusals ran past the allowance");
        assert_eq!(examined, 2);
        assert_eq!(queue.free.len(), 2);
        let examined = queue.run(Waker::noop(), 2, |name| {
            refused.push(name);
            None
        });
        assert_eq!(refused, [0, 1, 2, 3]);
        assert_eq!(examined, 2);
        assert_eq!(queue.run(Waker::noop(), 2, |_| None), 1);
        assert_eq!(queue.free.len(), 5);
        assert!(queue.ready.is_empty());
    }

    /// Late wakes of completed streams also spend the turn, even though no future is
    /// constructed or polled for their empty places.
    #[test]
    fn stale_entries_spend_the_turns_allowance() {
        let polled = Rc::new(RefCell::new(Vec::new()));
        let mut queue = Queue::new();
        for name in 0..5 {
            queue.push(name);
        }
        let wakers: Vec<_> = queue
            .entries
            .iter()
            .flatten()
            .map(|e| e.waker.clone())
            .collect();
        let mut make = busy(&polled, 0);
        assert_eq!(queue.run(Waker::noop(), 5, &mut make), 5);
        assert_eq!(queue.free.len(), 5);
        for waker in wakers {
            waker.wake();
        }
        let examined = queue.run(Waker::noop(), 2, &mut make);
        assert!(
            !queue.ready.is_empty(),
            "stale wakes ran past the allowance"
        );
        assert_eq!(examined, 2);
        assert_eq!(queue.run(Waker::noop(), 2, &mut make), 2);
        assert!(!queue.ready.is_empty());
        assert_eq!(queue.run(Waker::noop(), 2, &mut make), 1);
        assert!(queue.ready.is_empty());
        assert_eq!(*polled.borrow(), [0, 1, 2, 3, 4]);
    }
}
