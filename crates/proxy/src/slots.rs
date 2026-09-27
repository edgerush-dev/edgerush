//! Where a request's future lives while it runs: a slot the worker lends and takes back,
//! rather than room inside the connection's own task
//! ([14 §3](../../docs/14-downstream-server.md)).
//!
//! A task's future is stored inline in the task, and so is everything it awaits: a
//! connection whose driver awaited the request's future inline would be as large as that
//! future's largest state for as long as it lived, waiting for a first byte or between
//! requests included. So the driver runs each request in a slot instead, and holds the slot
//! only while the request does. A slot is boxed room for one future, pinned once and never
//! moved: a request's future is set into it, dropped where it stands when the request is
//! done, and the empty slot goes back to the worker for the next request, of any of its
//! connections, to use. What a request costs is then the set and the drop, never an
//! allocation, and what an idle connection costs is none of it.
//!
//! Free slots are kept as the worker's blocks are ([`crate::upstream::h1::blocks`]): up to
//! [`PARKED`] after a burst, trimmed to [`KEPT`] by the worker's once-a-second sweep. They
//! are not charged to the worker's storage: how many are in use is bounded by the exchanges
//! the worker admits, and how many are free by [`PARKED`].
//!
//! Nothing here does I/O or reads a clock.

#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use std::any::Any;
use std::cell::{OnceCell, RefCell};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// How many free slots a worker keeps after a burst. Beyond this a slot given back is
/// dropped: as for blocks, a burst should not leave its memory parked for ever.
pub const PARKED: usize = 64;

/// How many free slots a quiet worker keeps: what [`Slots::sweep`] trims down to.
pub const KEPT: usize = 4;

/// Room for one future of type `F`, pinned where it was made. Empty between requests.
type Room<F> = Pin<Box<Option<F>>>;

/// A worker's free slots for futures of type `F`.
pub struct Slots<F> {
    free: RefCell<Vec<Room<F>>>,
}

impl<F> Default for Slots<F> {
    fn default() -> Self {
        Self {
            free: RefCell::new(Vec::new()),
        }
    }
}

impl<F> fmt::Debug for Slots<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slots")
            .field("free", &self.free.try_borrow().map(|free| free.len()).ok())
            .finish()
    }
}

impl<F: Future> Slots<F> {
    /// Runs the future `make` makes in a free slot, or in a new one if none is free.
    ///
    /// Made here, once the slot is in hand, rather than by the caller and handed in: a
    /// future is as large as its largest state, and every move of it copies all of that, so
    /// it is made as late as it can be, to be moved as few times as it can be.
    #[inline]
    pub fn start(&self, make: impl FnOnce() -> F) -> Slot<'_, F> {
        let free = self
            .free
            .try_borrow_mut()
            .ok()
            .and_then(|mut free| free.pop());
        let mut room = free.unwrap_or_else(|| Box::pin(None));
        room.as_mut().set(Some(make()));
        Slot {
            room: Some(room),
            slots: self,
        }
    }
}

impl<F> Slots<F> {
    /// How many slots are free.
    #[cfg(test)]
    pub(crate) fn free(&self) -> usize {
        self.free.try_borrow().map_or(0, |free| free.len())
    }

    /// Drops free slots down to what a quiet worker keeps, for the worker's sweep. Under
    /// load the slots dropped are made again within the second, which is a few allocations
    /// a second rather than one a request.
    pub fn sweep(&self) {
        if let Ok(mut free) = self.free.try_borrow_mut() {
            free.truncate(KEPT);
        }
    }

    /// Takes an emptied slot back, unless enough are free already.
    fn give(&self, room: Room<F>) {
        if let Ok(mut free) = self.free.try_borrow_mut()
            && free.len() < PARKED
        {
            free.push(room);
        }
    }
}

/// A future running in a slot: polled as the future itself, and dropped with it. Dropping
/// it drops the future there and then, as a future held inline would be, and gives the
/// slot back.
pub struct Slot<'a, F> {
    /// Always there until the drop; an `Option` only so that the drop can give it back.
    room: Option<Room<F>>,
    slots: &'a Slots<F>,
}

impl<F: Future> Future for Slot<'_, F> {
    type Output = F::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        match self
            .get_mut()
            .room
            .as_mut()
            .and_then(|room| room.as_mut().as_pin_mut())
        {
            Some(future) => future.poll(context),
            // Not reached: the room holds the future from `start` to the drop.
            None => Poll::Pending,
        }
    }
}

impl<F> Drop for Slot<'_, F> {
    fn drop(&mut self) {
        if let Some(mut room) = self.room.take() {
            // The future goes first, and with it whatever it held, before anything else
            // is touched: its drop may be what lets an exchange's resources go.
            room.as_mut().set(None);
            self.slots.give(room);
        }
    }
}

/// What the worker's sweep needs of a worker's slots, whatever they are slots for.
trait Swept: Any {
    fn sweep(&self);
}

impl<F: 'static> Swept for Slots<F> {
    fn sweep(&self) {
        Slots::sweep(self);
    }
}

/// A worker's slots for its requests' futures. Their type is known where a connection is
/// served but has no name the worker could hold it by — it is an `async fn`'s — so they are
/// held here without it, made for the first type asked for.
#[derive(Default)]
pub(crate) struct WorkerSlots {
    slots: OnceCell<Rc<dyn Swept>>,
}

impl fmt::Debug for WorkerSlots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerSlots")
            .field("made", &self.slots.get().is_some())
            .finish()
    }
}

impl WorkerSlots {
    /// The worker's slots for futures of type `F`. A worker serves one kind of request
    /// future; one of another kind, which no worker asks for, is given slots of its own
    /// rather than none.
    pub(crate) fn of<F: 'static>(&self) -> Rc<Slots<F>> {
        let kept = self
            .slots
            .get_or_init(|| Rc::new(Slots::<F>::default()) as Rc<dyn Swept>);
        let kept: Rc<dyn Swept> = Rc::clone(kept);
        let any: Rc<dyn Any> = kept;
        any.downcast::<Slots<F>>().unwrap_or_default()
    }

    /// Trims the free slots, for the worker's once-a-second sweep.
    pub(crate) fn sweep(&self) {
        if let Some(slots) = self.slots.get() {
            slots.sweep();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::task::Waker;

    /// A future that is ready at once with `value`, and says when it is dropped.
    struct Marked {
        value: u32,
        dropped: Rc<Cell<bool>>,
    }

    impl Future for Marked {
        type Output = u32;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u32> {
            Poll::Ready(self.value)
        }
    }

    impl Drop for Marked {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    fn marked(value: u32) -> (Marked, Rc<Cell<bool>>) {
        let dropped = Rc::new(Cell::new(false));
        let future = Marked {
            value,
            dropped: Rc::clone(&dropped),
        };
        (future, dropped)
    }

    fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Where a slot's room is, which is what says whether a slot was used again.
    fn address<F>(slot: &Slot<'_, F>) -> *const Option<F> {
        slot.room
            .as_ref()
            .map_or(std::ptr::null(), |room| std::ptr::from_ref(&**room))
    }

    #[test]
    fn a_future_in_a_slot_is_polled_as_itself() {
        let slots = Slots::default();
        let (future, _) = marked(7);
        let mut slot = slots.start(|| future);
        assert_eq!(poll(&mut slot), Poll::Ready(7));
    }

    /// Dropped with its slot, at that moment, as it would be held inline; and the slot is
    /// free again.
    #[test]
    fn a_future_goes_when_its_slot_does_and_the_slot_is_free_again() {
        let slots = Slots::default();
        let (future, dropped) = marked(1);
        let slot = slots.start(|| future);
        assert!(!dropped.get());
        assert_eq!(slots.free(), 0);
        drop(slot);
        assert!(dropped.get(), "the future outlived its slot");
        assert_eq!(slots.free(), 1);
    }

    /// The next future takes the room the last one left, rather than new room.
    #[test]
    fn a_free_slot_is_used_again() {
        let slots = Slots::default();
        let first = slots.start(|| marked(1).0);
        let room = address(&first);
        drop(first);
        let second = slots.start(|| marked(2).0);
        assert_eq!(address(&second), room);
        assert_eq!(slots.free(), 0);
    }

    /// A future dropped before it is done, as a request is when its client goes, still gives
    /// its slot back.
    #[test]
    fn a_future_never_finished_gives_its_slot_back() {
        let slots = Slots::default();
        let never = slots.start(std::future::pending::<()>);
        drop(never);
        assert_eq!(slots.free(), 1);
    }

    /// A burst leaves at most [`PARKED`] free, and a sweep brings that down to [`KEPT`].
    #[test]
    fn free_slots_are_bounded_and_swept() {
        let slots = Slots::default();
        let burst: Vec<_> = (0..PARKED + 10)
            .map(|value| slots.start(|| marked(u32::try_from(value).unwrap()).0))
            .collect();
        drop(burst);
        assert_eq!(slots.free(), PARKED);
        slots.sweep();
        assert_eq!(slots.free(), KEPT);
    }

    /// One kind of future: the worker's slots, the same every time they are asked for.
    #[test]
    fn a_worker_keeps_one_set_of_slots_for_its_kind_of_future() {
        let worker = WorkerSlots::default();
        let first = worker.of::<Marked>();
        let again = worker.of::<Marked>();
        assert!(Rc::ptr_eq(&first, &again));
        drop(first.start(|| marked(1).0));
        assert_eq!(again.free(), 1);
        worker.sweep();
        assert_eq!(again.free(), 1, "a sweep keeps what a quiet worker keeps");
    }

    /// Another kind, which no worker asks for, gets slots of its own that the worker does
    /// not keep: never someone else's.
    #[test]
    fn a_future_of_another_kind_gets_slots_of_its_own() {
        let worker = WorkerSlots::default();
        let kept = worker.of::<Marked>();
        let other = worker.of::<std::future::Pending<()>>();
        drop(other.start(std::future::pending));
        assert_eq!(other.free(), 1);
        assert_eq!(kept.free(), 0);
        let again = worker.of::<std::future::Pending<()>>();
        assert!(!Rc::ptr_eq(&other, &again));
    }
}
