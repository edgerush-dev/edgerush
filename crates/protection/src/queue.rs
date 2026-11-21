//! A full backend's queue (25 §5): the requests that found every pod of a backend full,
//! waiting their turn, every worker's in one line.
//!
//! **A release only reads whether anyone waits.** A [`Slot`](crate::Slot) given back reads
//! [`Queue::waiting`]; only when someone does is the line locked, and its oldest called. A
//! call is a wake, not a slot: the request called picks again, on its own worker, any pod
//! with room by its own bound — so that whatever its worker's balancer, slow start and
//! health make of the pods holds for it as for any request. One that finds every pod full
//! again — a retry or a mirror's copy, which never queue, or a request just arriving, took
//! the room first — goes back to its place in line, which its ticket keeps.
//!
//! **No call is lost.** A request that joins at the front of the line looks at the pods
//! once more, after it is counted: either a release reads it counted, and calls it, or it
//! sees the slot that release gave back. A request called that leaves without answering the
//! call — gone, out of time, or holding a slot it took before the call came — passes the
//! call to the next in line.
//!
//! **No clock.** How long a request may wait is its caller's to bound: one out of time is
//! dropped, which takes it out of line.

use crate::sync::{Arc, AtomicUsize, Mutex, Ordering, fence, lock};
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::task::{Context, Poll, Waker};

/// One backend's line of requests waiting for a slot in any of its pods, in a data plane,
/// shared by its workers (25 §5).
#[derive(Debug, Default)]
pub struct Queue {
    /// Requests waiting: in line, or called and yet to take a slot or go back in line.
    waiting: AtomicUsize,
    line: Mutex<Line>,
}

#[derive(Debug, Default)]
struct Line {
    /// The ticket the next request to join is given.
    next: u64,
    /// In line, in the order of their tickets: oldest first.
    places: VecDeque<Arc<Place>>,
}

#[derive(Debug)]
struct Place {
    /// Its turn: earlier tickets go first, wherever it is in line.
    ticket: u64,
    /// Locked after the line's lock where both are.
    state: Mutex<State>,
}

#[derive(Debug)]
enum State {
    /// Waiting to be called, and what wakes it when it is.
    InLine(Option<Waker>),
    /// Out of line, called to pick again.
    Called,
    /// Gone for good: took a slot, or left.
    Left,
}

/// A request's place in a backend's line, from [`Queue::join`] until it takes a slot or is
/// dropped: dropped, it leaves the line (25 §5).
#[derive(Debug)]
pub struct InLine {
    waiting: Waiting,
    /// At the front of the line, and so to look at the pods once more before it waits: a
    /// release that came before it was counted called nobody for it.
    front: bool,
}

impl InLine {
    /// What `take` gives — a slot in a pod of its backend, taken as the caller's balancer
    /// takes one — once the line calls this request. Each time it is called it takes
    /// again, and goes back to its place if every pod is full again. Dropped, it leaves the
    /// line: a caller bounds the wait by dropping it once out of time.
    pub async fn wait<T>(self, mut take: impl FnMut() -> Option<T>) -> T {
        let Self {
            mut waiting,
            mut front,
        } = self;
        loop {
            if front && let Some(taken) = take() {
                waiting.took();
                return taken;
            }
            std::future::poll_fn(|cx| waiting.poll_called(cx)).await;
            if let Some(taken) = take() {
                waiting.took();
                return taken;
            }
            front = waiting.rejoin();
        }
    }
}

impl Queue {
    /// An empty line.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// How many requests wait: in line, or called and yet to pick. A read, for a request
    /// that looks before it joins — any wait, and it does not take a slot ahead of them —
    /// and for a release, which calls one only if any does.
    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }

    /// A place at the back of `queue`'s line, for a request that found every pod of its
    /// backend full, if fewer than `size` wait.
    #[must_use]
    pub fn join(queue: &Arc<Self>, size: NonZeroU32) -> Option<InLine> {
        let mut line = lock(&queue.line);
        // A size past what a `usize` holds is never reached.
        let size = usize::try_from(size.get()).unwrap_or(usize::MAX);
        if queue.waiting.load(Ordering::Relaxed) >= size {
            return None;
        }
        let place = Arc::new(Place {
            ticket: line.next,
            state: Mutex::new(State::InLine(None)),
        });
        line.next += 1;
        line.places.push_back(Arc::clone(&place));
        let front = line.places.len() == 1;
        // Counted before it looks again, and fenced: a release that gives its slot back
        // after this reads it, and one before is seen by the look (`Self::release` says
        // why).
        queue.waiting.fetch_add(1, Ordering::SeqCst);
        drop(line);
        if front {
            fence(Ordering::SeqCst);
        }
        Some(InLine {
            waiting: Waiting {
                queue: Arc::clone(queue),
                place,
                answering: false,
            },
            front,
        })
    }

    /// For a slot just given back, in a count whose decrement was `SeqCst`: the oldest in
    /// line called, if anyone waits.
    pub(crate) fn release(&self) {
        // Why no call is lost, between this release — the count's decrement (A), then this
        // read of `waiting` (L), both `SeqCst` — and a request joining at the front of the
        // line — its increment of `waiting` (J), then a `SeqCst` fence (F), then its look at
        // the counts, relaxed (`Self::join`). C++20, which Rust's atomics follow, puts every
        // `SeqCst` operation in one total order S, and a load after a `SeqCst` fence sees
        // every `SeqCst` write before that fence in S. If L does not see J, L comes before
        // J in S, so A, L, J, F in that order: the look after F sees A, the room. If L sees
        // J, this release calls the request. Either way the request has the room or a call.
        //
        // Loom takes a `SeqCst` access for an acquire and release one, and so would find a
        // call lost here that the memory model rules out (its README, "Unsupported
        // features"): under loom a fence, which it does model in full, stands in for L's
        // ordering. Its models check that stand-in; what ships rests on the argument above,
        // the same instructions on x86, and is to be run at length on ARM (docs/10 §4).
        #[cfg(loom)]
        fence(Ordering::SeqCst);
        if self.waiting.load(Ordering::SeqCst) > 0 {
            let called = call(&mut lock(&self.line));
            if let Some(waker) = called {
                waker.wake();
            }
        }
    }

    /// Every request in line called, for a new config that may have made room without a
    /// slot given back: a cap raised, a pod added. Those that find none go back to their
    /// places.
    pub fn call_all(&self) {
        if self.waiting.load(Ordering::SeqCst) == 0 {
            return;
        }
        let wakers: Vec<Waker> = {
            let mut line = lock(&self.line);
            line.places
                .drain(..)
                .filter_map(|place| called(&place))
                .collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// The oldest in `line` called: out of line, and what wakes it.
fn call(line: &mut Line) -> Option<Waker> {
    let place = line.places.pop_front()?;
    called(&place)
}

/// `place`, taken out of line, called: what wakes it.
fn called(place: &Place) -> Option<Waker> {
    match std::mem::replace(&mut *lock(&place.state), State::Called) {
        State::InLine(waker) => waker,
        // Only what is in line is taken out of it, and only to be called.
        State::Called | State::Left => None,
    }
}

/// What a request in line holds of its place.
#[derive(Debug)]
struct Waiting {
    queue: Arc<Queue>,
    place: Arc<Place>,
    /// Whether it has seen the call it is answering: picking again because it was called.
    answering: bool,
}

impl Waiting {
    /// Ready once it is called, its waker kept until then.
    fn poll_called(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = lock(&self.place.state);
        match &mut *state {
            State::InLine(waker) => {
                match waker {
                    Some(waker) => waker.clone_from(cx.waker()),
                    None => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
            State::Called | State::Left => {
                self.answering = true;
                Poll::Ready(())
            }
        }
    }

    /// Back in its place in line, for a request called that found every pod full: whether
    /// that place is the front, from which it is to look at the pods once more, as at
    /// [`Queue::join`].
    fn rejoin(&mut self) -> bool {
        let mut line = lock(&self.queue.line);
        let mut state = lock(&self.place.state);
        if !matches!(*state, State::Called) {
            return false;
        }
        *state = State::InLine(None);
        drop(state);
        self.answering = false;
        let ticket = self.place.ticket;
        let at = line.places.partition_point(|place| place.ticket < ticket);
        line.places.insert(at, Arc::clone(&self.place));
        drop(line);
        if at == 0 {
            fence(Ordering::SeqCst);
        }
        at == 0
    }

    /// Out of the queue, holding a slot it took.
    fn took(mut self) {
        self.leave();
    }

    /// Out of line and uncounted. A call it was given and did not answer goes to the next
    /// in line: it may be for a slot nobody else is waking anyone for.
    fn leave(&mut self) {
        let mut line = lock(&self.queue.line);
        let was = std::mem::replace(&mut *lock(&self.place.state), State::Left);
        let passed = match was {
            State::InLine(_) => {
                let ticket = self.place.ticket;
                if let Ok(at) = line
                    .places
                    .binary_search_by_key(&ticket, |place| place.ticket)
                {
                    line.places.remove(at);
                }
                self.queue.waiting.fetch_sub(1, Ordering::SeqCst);
                None
            }
            State::Called => {
                self.queue.waiting.fetch_sub(1, Ordering::SeqCst);
                if self.answering {
                    None
                } else {
                    call(&mut line)
                }
            }
            State::Left => None,
        };
        drop(line);
        if let Some(waker) = passed {
            waker.wake();
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        // Gone without a slot: a call it was answering is unanswered too.
        self.answering = false;
        self.leave();
    }
}

// Two attributes, not `all(test, not(loom))`: clippy takes only a plain `cfg(test)` for a
// test module.
#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::{PodCount, Slot};
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::Wake;
    use std::time::{Duration, Instant};

    const SIZE: NonZeroU32 = NonZeroU32::new(10).unwrap();

    fn cap(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    #[derive(Default)]
    struct Woken(AtomicBool);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A request in a queue, its future polled by hand.
    struct Request<'a> {
        future: Pin<Box<dyn Future<Output = Option<Slot>> + 'a>>,
        woken: Arc<Woken>,
    }

    impl Request<'_> {
        /// Whether it was woken since it was last asked.
        fn woken(&self) -> bool {
            self.woken.0.swap(false, Ordering::SeqCst)
        }

        fn poll(&mut self) -> Poll<Option<Slot>> {
            let waker = Waker::from(Arc::clone(&self.woken));
            self.future.as_mut().poll(&mut Context::from_waker(&waker))
        }

        /// Polled once, and still waiting.
        fn waits(mut self) -> Self {
            assert!(self.poll().is_pending());
            self
        }
    }

    /// A request for a slot in `pod`, at a cap of what `bound` holds, waiting in `queue`.
    fn request<'a>(
        queue: &'a Arc<Queue>,
        pod: &'a Arc<PodCount>,
        bound: &'a Cell<u32>,
    ) -> Request<'a> {
        Request {
            future: Box::pin(async move {
                let in_line = Queue::join(queue, SIZE)?;
                Some(in_line.wait(|| PodCount::take(pod, cap(bound.get()))).await)
            }),
            woken: Arc::default(),
        }
    }

    /// A pod of `queue`'s backend, full at a cap of `held`, and its slots.
    fn full(queue: &Arc<Queue>, held: u32) -> (Arc<PodCount>, Vec<Slot>) {
        let pod = PodCount::of(queue);
        let slots = (0..held)
            .map(|_| PodCount::take(&pod, cap(held)).unwrap())
            .collect();
        (pod, slots)
    }

    #[test]
    fn a_full_line_refuses_at_once() {
        let queue = Queue::new();
        let (pod, _held) = full(&queue, 1);
        let bound = Cell::new(1);
        let _first = request(&queue, &pod, &bound).waits();
        assert!(Queue::join(&queue, cap(1)).is_none());
        assert_eq!(queue.waiting(), 1, "nothing queued");
    }

    #[test]
    fn a_slot_given_back_calls_the_oldest_alone() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let mut first = request(&queue, &pod, &bound).waits();
        let mut second = request(&queue, &pod, &bound).waits();
        assert_eq!(queue.waiting(), 2);
        drop(held.pop());
        assert!(first.woken());
        assert!(!second.woken());
        let taken = first.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
        assert_eq!(queue.waiting(), 1);
        assert!(second.poll().is_pending());
    }

    /// First come, first served: room a request joining behind others finds is theirs to
    /// be called for.
    #[test]
    fn a_request_behind_others_does_not_take_room_ahead_of_them() {
        let queue = Queue::new();
        let (pod, _held) = full(&queue, 1);
        let bound = Cell::new(1);
        let mut first = request(&queue, &pod, &bound).waits();
        // Room made without a slot given back, as a raised cap makes it.
        bound.set(2);
        let mut second = request(&queue, &pod, &bound).waits();
        assert_eq!(pod.in_flight(), 1, "the second did not take it");
        queue.call_all();
        let taken = first.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
        assert!(second.poll().is_pending());
    }

    #[test]
    fn a_request_called_that_leaves_passes_the_call_on() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let first = request(&queue, &pod, &bound).waits();
        let mut second = request(&queue, &pod, &bound).waits();
        drop(held.pop());
        assert!(!second.woken());
        // Gone, or out of time, before it took the slot it was called for.
        drop(first);
        assert!(second.woken());
        let taken = second.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
        assert_eq!(queue.waiting(), 0);
    }

    #[test]
    fn a_request_called_that_takes_its_slot_wakes_nobody_else() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let mut first = request(&queue, &pod, &bound).waits();
        let second = request(&queue, &pod, &bound).waits();
        drop(held.pop());
        let taken = first.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))));
        assert!(!second.woken());
    }

    #[test]
    fn a_request_called_that_finds_every_pod_full_again_keeps_its_place() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let mut first = request(&queue, &pod, &bound).waits();
        let second = request(&queue, &pod, &bound).waits();
        drop(held.pop());
        // A retry, which never queues, takes the slot before the request called does.
        let retry = PodCount::take(&pod, cap(1)).unwrap();
        assert!(first.poll().is_pending(), "back in line");
        assert_eq!(queue.waiting(), 2);
        drop(retry);
        assert!(first.woken(), "still first");
        assert!(!second.woken());
        let taken = first.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
    }

    #[test]
    fn a_request_gone_from_line_is_counted_no_more() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let first = request(&queue, &pod, &bound).waits();
        let mut second = request(&queue, &pod, &bound).waits();
        drop(first);
        assert_eq!(queue.waiting(), 1);
        drop(held.pop());
        assert!(second.woken(), "the next in line, now the oldest");
        let taken = second.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
        assert_eq!(queue.waiting(), 0);
    }

    #[test]
    fn everyone_called_at_once_goes_back_in_the_order_they_came() {
        let queue = Queue::new();
        let (pod, mut held) = full(&queue, 1);
        let bound = Cell::new(1);
        let mut first = request(&queue, &pod, &bound).waits();
        let mut second = request(&queue, &pod, &bound).waits();
        let mut third = request(&queue, &pod, &bound).waits();
        bound.set(2);
        queue.call_all();
        assert!(first.woken() && second.woken() && third.woken());
        let taken = first.poll();
        assert!(matches!(taken, Poll::Ready(Some(_))), "and holds it");
        // The third back before the second: the second's ticket still puts it first.
        assert!(third.poll().is_pending());
        assert!(second.poll().is_pending());
        drop(held.pop());
        assert!(second.woken());
        assert!(!third.woken());
    }

    /// `future` run to its end on this thread, parked between polls; fails if it has not
    /// ended `within`, as a request whose call was lost would not.
    fn block_on<F: Future>(future: F, within: Duration) -> F::Output {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        let until = Instant::now() + within;
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            let left = until.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "a request waited with nothing to call it");
            std::thread::park_timeout(left);
        }
    }

    /// Workers taking slots and waiting in line for them at once: however they interleave,
    /// never more than the cap in flight, nobody left waiting, and the line empty at the
    /// end. Loom tries every interleaving of a few (`loom_tests`); this runs many.
    #[test]
    fn racing_workers_and_their_line_lose_no_call() {
        const WORKERS: usize = 8;
        const ROUNDS: usize = 500;
        let queue = Queue::new();
        let pod = PodCount::of(&queue);
        let most = std::sync::atomic::AtomicU32::new(0);
        let start = std::sync::Barrier::new(WORKERS);
        std::thread::scope(|scope| {
            for _ in 0..WORKERS {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..ROUNDS {
                        let slot = match PodCount::take(&pod, cap(2)) {
                            Some(slot) => slot,
                            None => block_on(
                                Queue::join(&queue, SIZE)
                                    .unwrap()
                                    .wait(|| PodCount::take(&pod, cap(2))),
                                Duration::from_secs(10),
                            ),
                        };
                        most.fetch_max(pod.in_flight(), Ordering::Relaxed);
                        drop(slot);
                    }
                });
            }
        });
        assert!(most.load(Ordering::Relaxed) <= 2);
        assert_eq!(pod.in_flight(), 0);
        assert_eq!(queue.waiting(), 0);
    }
}
