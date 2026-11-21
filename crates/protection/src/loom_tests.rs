//! Every interleaving of workers racing on one pod's count, under loom
//! (`RUSTFLAGS="--cfg loom" cargo test --release -p edgerush-protection --lib loom`): the
//! last slot taken by one of them alone, and a slot given back while another takes; and of
//! a backend's queue, whose calls none is lost however slots given back race the requests
//! joining, leaving and going back in line.

#![cfg(loom)]

use super::*;
use loom::thread;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

fn cap(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

#[test]
fn loom_two_workers_racing_for_the_last_slot_take_it_once() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(2)).unwrap();
        let other = Arc::clone(&pod);
        // Each keeps what it took until both have tried.
        let racer = thread::spawn(move || PodCount::take(&other, cap(2)));
        let mine = PodCount::take(&pod, cap(2));
        let theirs = racer.join().unwrap();
        assert!(
            mine.is_some() != theirs.is_some(),
            "exactly one of them takes the last slot"
        );
        assert_eq!(pod.in_flight(), 2);
        drop((held, mine, theirs));
        assert_eq!(pod.in_flight(), 0);
    });
}

#[test]
fn loom_three_workers_never_pass_a_cap_of_one() {
    loom::model(|| {
        let pod = PodCount::new();
        let racers: Vec<_> = (0..2)
            .map(|_| {
                let pod = Arc::clone(&pod);
                thread::spawn(move || {
                    let slot = PodCount::take(&pod, cap(1));
                    if slot.is_some() {
                        assert_eq!(pod.in_flight(), 1);
                    }
                    slot.is_some()
                })
            })
            .collect();
        let mine = PodCount::take(&pod, cap(1));
        if mine.is_some() {
            assert_eq!(pod.in_flight(), 1);
        }
        drop(mine);
        for racer in racers {
            racer.join().unwrap();
        }
        assert_eq!(pod.in_flight(), 0, "every slot came back");
    });
}

#[test]
fn loom_a_slot_given_back_while_another_worker_takes_is_never_lost() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(1)).unwrap();
        let other = Arc::clone(&pod);
        let taker = thread::spawn(move || PodCount::take(&other, cap(1)));
        drop(held);
        let taken = taker.join().unwrap();
        // Taken after the give-back or refused before it: the count says which.
        assert_eq!(pod.in_flight(), u32::from(taken.is_some()));
        drop(taken);
        assert_eq!(pod.in_flight(), 0);
    });
}

const SIZE: NonZeroU32 = NonZeroU32::new(4).unwrap();

/// `future` run to its end on this thread, parked while it waits: loom fails the model if
/// every thread is parked, as a request whose call was lost would leave its own. Loom's
/// own `block_on` is behind a feature that adds a crate; this is all it takes.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(std::sync::Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// A request waiting in `pod`'s backend's queue for a slot at a cap of one, made to be
/// run on any thread.
fn request(pod: &Arc<PodCount>) -> Pin<Box<dyn Future<Output = Option<Slot>> + Send>> {
    let pod = Arc::clone(pod);
    Box::pin(async move {
        let queue = Arc::clone(&pod.queue);
        let in_line = Queue::join(&queue, SIZE)?;
        Some(in_line.wait(|| PodCount::take(&pod, cap(1))).await)
    })
}

/// Polled once, with nothing to wake: in line, or done.
fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// The race a release reads whether anyone waits for: a slot given back as the one request
/// for it joins. Either the release reads it waiting and calls it, or it sees the room;
/// loom fails the model if the request is left blocked with nothing to wake it.
#[test]
fn loom_a_slot_given_back_as_a_request_joins_is_never_lost() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(1)).unwrap();
        let releaser = thread::spawn(move || drop(held));
        let taken = block_on(request(&pod));
        assert!(taken.is_some());
        releaser.join().unwrap();
        drop(taken);
        assert_eq!(pod.in_flight(), 0);
        assert_eq!(pod.queue.waiting(), 0);
    });
}

/// A call given to a request that leaves as it comes goes to the next in line.
#[test]
fn loom_a_call_to_a_request_that_leaves_goes_to_the_next() {
    loom::model(|| {
        let pod = PodCount::new();
        let held = PodCount::take(&pod, cap(1)).unwrap();
        let mut first = request(&pod);
        let mut second = request(&pod);
        assert!(poll_once(first.as_mut()).is_pending());
        assert!(poll_once(second.as_mut()).is_pending());
        let releaser = thread::spawn(move || drop(held));
        // Gone, the client away or out of time, whether called by then or not.
        let leaver = thread::spawn(move || drop(first));
        let taken = block_on(second);
        assert!(taken.is_some());
        releaser.join().unwrap();
        leaver.join().unwrap();
        drop(taken);
        assert_eq!(pod.queue.waiting(), 0);
    });
}

/// Two slots given back at once to two requests in line: each request gets one, whether a
/// call finds the other's slot taken and sends it back in line or not. Three threads' every
/// interleaving takes minutes; up to four preemptions in a run, a second.
#[test]
fn loom_two_slots_given_back_at_once_reach_both_requests_in_line() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(4);
    builder.check(|| {
        let pod = PodCount::new();
        // Two slots, at a cap of one: a cap lowered with two in flight.
        let held = [
            PodCount::take(&pod, cap(2)).unwrap(),
            PodCount::take(&pod, cap(2)).unwrap(),
        ];
        let mut first = request(&pod);
        let mut second = request(&pod);
        assert!(poll_once(first.as_mut()).is_pending());
        assert!(poll_once(second.as_mut()).is_pending());
        let [one, two] = held;
        let releaser = thread::spawn(move || drop(one));
        let other = thread::spawn(move || drop(two));
        let taken = block_on(first);
        assert!(taken.is_some());
        drop(taken);
        let taken = block_on(second);
        assert!(taken.is_some());
        releaser.join().unwrap();
        other.join().unwrap();
        drop(taken);
        assert_eq!(pod.in_flight(), 0);
        assert_eq!(pod.queue.waiting(), 0);
    });
}
