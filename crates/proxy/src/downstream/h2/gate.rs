//! Holds back the wakes a connection's task gives itself while it runs
//! ([15 §3](../../../../../docs/15-http2-and-grpc.md)).
//!
//! A worker runs its tasks in a `LocalSet`, which polls up to 61 of them before the runtime
//! looks for I/O again. A connection that ends its turn with work left would be polled again
//! in that same round, and an HTTP/1 request whose bytes have arrived waits until the round
//! ends: 61 turns, milliseconds. A task that ends its turn so says so (`defer`), and behind
//! the gate it is polled again only once the runtime has looked for I/O, as
//! `tokio::task::yield_now` does — HAProxy's rule: a connection runs once a loop. The wakes
//! it gets as it runs — from h2, or from its own streams — are only noted: one that ends its
//! turn with nothing left is polled again at once, as it would be without the gate, rather
//! than made to wait for I/O for what it has already seen to; otherwise a lone connection
//! would go to the I/O poll after every answer. A wake from outside its turn — its socket, a
//! timer, an upstream's answer — goes through.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Wake, Waker};

/// Not polling.
const IDLE: u8 = 0;
/// Polling, and not woken since it began.
const POLLING: u8 = 1;
/// Polling, and woken since it began.
const WOKEN: u8 = 2;

thread_local! {
    /// Set by the task polled behind a gate when its turn ends with work left.
    static DEFER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Said by a gated task that ends its turn with work left, as it returns Pending: it is
/// polled again after the runtime's next look for I/O. It must still wake itself, for when it
/// runs without a gate.
pub(crate) fn defer() {
    DEFER.set(true);
}

/// The gate a task's waker goes through.
struct Gate {
    state: AtomicU8,
    /// The task's own waker, as of its last poll.
    task: Mutex<Option<Waker>>,
}

impl Wake for Gate {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Noted while the task runs; passed on otherwise. A wake as the poll ends either
        // lands before the end sees it, or finds the gate idle and goes through.
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                POLLING => match self.state.compare_exchange(
                    POLLING,
                    WOKEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(now) => state = now,
                },
                IDLE => break,
                _ => return,
            }
        }
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(task) = task {
            task.wake();
        }
    }
}

/// Runs `future` behind a gate: what wakes it while it is polled has it polled again only
/// after the runtime has looked for I/O.
pub(crate) async fn gated<F: Future>(future: F) -> F::Output {
    let gate = Arc::new(Gate {
        state: AtomicU8::new(IDLE),
        task: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&gate));
    let mut future = pin!(future);
    std::future::poll_fn(|cx| {
        {
            let mut task = gate.task.lock().unwrap_or_else(PoisonError::into_inner);
            if !task.as_ref().is_some_and(|task| task.will_wake(cx.waker())) {
                *task = Some(cx.waker().clone());
            }
        }
        DEFER.set(false);
        gate.state.store(POLLING, Ordering::Release);
        let polled = future.as_mut().poll(&mut Context::from_waker(&waker));
        let woken = gate.state.swap(IDLE, Ordering::AcqRel) == WOKEN;
        let deferring = DEFER.replace(false);
        if polled.is_pending() {
            if deferring {
                // Its first poll hands the task's waker to the runtime, to be woken after its
                // next look for I/O; then it is done with.
                let _deferred = pin!(tokio::task::yield_now()).poll(cx);
            } else if woken {
                cx.waker().wake_by_ref();
            }
        }
        polled
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::task::Poll;

    /// A future that is never done and ends each poll with work left, as a connection always
    /// busy does — or, `deferring` false, only wakes itself as it runs; it counts its polls.
    fn busy(polls: Rc<Cell<usize>>, deferring: bool) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| {
            polls.set(polls.get() + 1);
            if deferring {
                defer();
            }
            cx.waker().wake_by_ref();
            Poll::<()>::Pending
        })
    }

    /// How often `busy` tasks were polled before a task waiting on a socket with bytes
    /// already in it got its turn.
    fn polls_before_io(gate: bool, deferring: bool) -> usize {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let polls = Rc::new(Cell::new(0));
            let mut busy_tasks = Vec::new();
            for _ in 0..4 {
                let polls = Rc::clone(&polls);
                busy_tasks.push(tokio::task::spawn_local(async move {
                    if gate {
                        gated(busy(polls, deferring)).await;
                    } else {
                        busy(polls, deferring).await;
                    }
                }));
            }
            // The busy tasks have each had a turn, and are woken.
            tokio::task::yield_now().await;
            let started = polls.get();
            tokio::io::AsyncWriteExt::write_all(&mut client, b"x")
                .await
                .unwrap();
            let reader = tokio::task::spawn_local(async move {
                server.readable().await.unwrap();
            });
            // Give the kernel the byte before anything is polled again.
            std::thread::sleep(std::time::Duration::from_millis(20));
            reader.await.unwrap();
            let between = polls.get() - started;
            for task in busy_tasks {
                task.abort();
            }
            between
        })
    }

    #[test]
    fn a_task_that_ends_its_turn_with_work_left_waits_for_the_runtime_to_look_for_io() {
        // Four busy tasks: behind the gate each is polled about once a look for I/O.
        let gated = polls_before_io(true, true);
        assert!(
            gated <= 12,
            "{gated} polls of gated tasks before the socket's"
        );
        // Without it they take the whole of a `LocalSet`'s round, 61 polls, first.
        let ungated = polls_before_io(false, true);
        assert!(
            ungated >= 50,
            "{ungated} polls of ungated tasks before the socket's"
        );
    }

    /// A wake noted as the task ran, its turn not ended with work left, has it polled again at
    /// once, as with no gate: it is not made to wait for I/O.
    #[test]
    fn a_task_only_woken_as_it_ran_is_polled_again_at_once() {
        let gated = polls_before_io(true, false);
        assert!(
            gated >= 50,
            "{gated} polls of gated tasks before the socket's"
        );
    }

    #[test]
    fn a_wake_from_outside_the_turn_goes_through() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (send, receive) = tokio::sync::oneshot::channel();
            let waiting = tokio::spawn(gated(receive));
            std::thread::spawn(move || send.send(7));
            let received = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
                .await
                .expect("a wake from another thread never reached the task");
            assert_eq!(received.unwrap().unwrap(), 7);
        });
    }

    #[test]
    fn a_wake_noted_in_the_turn_has_the_task_polled_again() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counting = Arc::clone(&polls);
            let done = gated(std::future::poll_fn(move |cx| {
                if counting.fetch_add(1, Ordering::Relaxed) < 3 {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }));
            tokio::time::timeout(std::time::Duration::from_secs(5), done)
                .await
                .unwrap();
            assert_eq!(polls.load(Ordering::Relaxed), 4);
        });
    }
}
