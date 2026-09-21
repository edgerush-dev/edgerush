//! A scripted upstream, and a bounded run to drive it under.
//!
//! One input becomes a script: bytes for the upstream to say, points for it to stall at,
//! a close, a failure, a cancellation. Both clients are then driven from that one script
//! over a socket that is nothing but memory, so that what an upstream did is a value a
//! test or a fuzz target chose rather than a race with a real peer
//! ([13 §8](../../../../docs/13-http1-upstream.md)).
//!
//! **A stall says what releases it.** Returning `Pending` describes nothing: it says that
//! there is no progress now, not when there will be. So a wait names the thing it is
//! waiting for — bytes of the request, the clock, or nothing ever — and the run goes on
//! when that thing happens. What holds the whole of it up is the budget: operations and
//! time, both bounded, so a script that stalls produces an outcome that says so instead
//! of hanging whoever ran it.
//!
//! Nothing here judges what a client did with the bytes. The tape records what was
//! written and how much was handed over; what that ought to have come to belongs to the
//! oracles, and reads are not consumption — a client may read past the end of a message,
//! which is its own business and not a difference from anything.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

/// One thing the upstream does. A script is these, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Says these bytes. They are one delivery: a client asking for more than there is
    /// here is given this much and asks again, which is how a script splits an answer
    /// across reads at a boundary it chose.
    Say(Vec<u8>),
    /// Takes no more of the request until something gives it room again.
    Block,
    /// Takes up to this many more bytes of the request.
    Take(usize),
    /// Neither says nor takes anything until the thing named happens.
    Wait(Wait),
    /// Closes its end. What it has already said is still read before the end is.
    Close,
    /// The connection fails. What was said before it is still read first: what a client
    /// can see has to be exactly what the script said, and a reset that swallowed some of
    /// it would leave the oracle guessing how far the client had got.
    Fail,
    /// The client goes away: whatever is driving the exchange is dropped here.
    Cancel,
}

/// What a stall is waiting for. There is no stall that waits for nothing in particular.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    /// Until the client has written this many bytes in all — an upstream that will not
    /// answer until it has been told enough.
    Written(usize),
    /// Until this much has passed on the clock, counted from when the script reached
    /// this step rather than from when the run began.
    Time(Duration),
    /// For ever. The peer has stopped talking and will not start again, which is what a
    /// deadline is for; a client that has none waits here until the budget is gone.
    Forever,
}

/// What the upstream does, and how much of the request it will take before it has to be
/// asked for room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Script {
    steps: Vec<Step>,
    room: usize,
}

/// Room to write that a script does not mention: enough that a request goes out without
/// a script having to say so, since most scripts are about the answer.
const ROOM: usize = 64 * 1024;

impl Script {
    /// These steps, with the usual room for a request to go out in.
    #[must_use]
    pub fn new(steps: Vec<Step>) -> Self {
        Self { steps, room: ROOM }
    }

    /// The same, where how much the upstream will take to begin with is the point.
    #[must_use]
    pub fn with_room(steps: Vec<Step>, room: usize) -> Self {
        Self { steps, room }
    }

    /// The steps, in order.
    #[must_use]
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// Everything the upstream says, all together: the bytes an oracle reads. Anything
    /// after a close or a failure is left out, because it never reaches a client.
    #[must_use]
    pub fn said(&self) -> Vec<u8> {
        let mut said = Vec::new();
        for step in &self.steps {
            match step {
                Step::Say(bytes) => said.extend_from_slice(bytes),
                Step::Close | Step::Fail => break,
                _ => {}
            }
        }
        said
    }

    /// Whether the upstream's last word is a failure rather than a close or a silence.
    /// The bytes are the same either way; what differs is whether the stream ended or
    /// broke, which is not something a framer can tell from the bytes.
    #[must_use]
    pub fn fails(&self) -> bool {
        self.steps
            .iter()
            .find(|step| matches!(step, Step::Close | Step::Fail))
            .is_some_and(|step| *step == Step::Fail)
    }

    /// Reads one input as a script, within `budget`.
    ///
    /// Every byte means something and nothing is rejected, so that a fuzzer's corpus is
    /// all usable and a shrunk input is still a script. A byte chooses the step, the byte
    /// after it is that step's operand, and an input that stops in the middle of a step
    /// stops the script: there is nothing to be gained by guessing at what was cut off.
    #[must_use]
    pub fn decode(bytes: &[u8], budget: &Budget) -> Self {
        let mut steps = Vec::new();
        let mut said = 0;
        let mut rest = bytes;
        while steps.len() < budget.steps {
            let Some((tag, after)) = rest.split_first() else {
                break;
            };
            let Some((raw, after)) = after.split_first() else {
                break;
            };
            let operand = usize::from(*raw);
            rest = after;
            let step = match tag % 11 {
                // Saying something is what most of an input ought to be, so most of the
                // tags are that.
                0..=4 => {
                    let room = budget.said.saturating_sub(said);
                    let length = operand.min(rest.len()).min(room);
                    let (say, after) = rest.split_at(length);
                    rest = after;
                    said += length;
                    Step::Say(say.to_vec())
                }
                5 => Step::Block,
                6 => Step::Take(operand),
                7 => Step::Wait(Wait::Written(operand)),
                // Milliseconds, so that a whole operand's worth of them is still well
                // inside any deadline this project has.
                8 => Step::Wait(Wait::Time(Duration::from_millis(u64::from(*raw)))),
                9 => Step::Close,
                // The two that end a run outright share one tag, chosen by the operand:
                // a corpus full of them would say nothing about framing.
                _ if operand % 2 == 0 => Step::Fail,
                _ => Step::Cancel,
            };
            steps.push(step);
        }
        Self::new(steps)
    }
}

/// What a run may spend, and what an input may come to. Every one of these is a bound
/// that stops a scripted peer holding a fuzzer up for ever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// The most steps a script may have.
    pub steps: usize,
    /// The most an upstream may say across all of them.
    pub said: usize,
    /// The most reads and writes a run may attempt. A client with nothing to do that
    /// keeps asking anyway is going round without getting anywhere, and this notices.
    pub ops: usize,
    /// How far the clock may go. Longer than any deadline the code under test has, so
    /// that a deadline is what ends a stalled run and this is only the backstop for a
    /// client that has none.
    pub time: Duration,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            steps: 32,
            said: 8 * 1024,
            ops: 4096,
            time: Duration::from_secs(600),
        }
    }
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    /// The work finished, and this is what it came to.
    Done(T),
    /// The script said the client goes away here, so the work was dropped where it
    /// stood. What it would have returned is not a thing that exists.
    Cancelled,
    /// The budget ran out. This is the run saying so rather than hanging; it is not a
    /// verdict on either peer, and a comparison has nothing to compare.
    Spent(Spent),
}

/// Which bound a run reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spent {
    /// Reads and writes ran out: something was going round without getting anywhere.
    Ops,
    /// The clock reached the end of the budget with the work still waiting.
    Time,
}

/// What a run recorded. Facts about what happened, with no opinion about any of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tape {
    /// Every byte the client wrote, in order.
    pub written: Vec<u8>,
    /// How many bytes were handed to the client.
    ///
    /// Reads, not consumption. A client may read past the end of a message and hold what
    /// it has not used yet — hyper's does — so this is never compared with where a
    /// message ends ([13 §8](../../../../docs/13-http1-upstream.md)).
    pub delivered: usize,
    /// Said and never read, because the run ended first.
    pub unread: usize,
    /// How many steps the script got through.
    pub reached: usize,
    /// Reads and writes attempted.
    pub ops: usize,
    /// Whether the client shut its writing half.
    pub shut: bool,
    /// How far the clock moved. Simulated: a run of a minute takes no time at all.
    pub elapsed: Duration,
}

/// The run spent every operation it was given.
///
/// A failure rather than a stall, because a client that is spinning is not waiting for
/// anything and would never notice a `Pending`. The driver reports it as the budget it
/// is, not as a connection that failed.
#[derive(Debug)]
struct Overrun;

impl std::fmt::Display for Overrun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the run's operations ran out")
    }
}

impl std::error::Error for Overrun {}

/// The script, and everything the run has recorded of it. Shared, because the socket is
/// handed to the client and what it did with it is read back afterwards.
#[derive(Debug)]
struct Shared {
    steps: VecDeque<Step>,
    /// Said and not yet read.
    pending: Vec<u8>,
    /// How much more of the request the upstream will take.
    credit: usize,
    written: Vec<u8>,
    delivered: usize,
    reached: usize,
    ops: usize,
    most_ops: usize,
    overrun: bool,
    /// The upstream will say nothing more.
    ended: bool,
    /// The connection failed.
    failed: bool,
    /// The script said the client goes away.
    cancel: bool,
    shut: bool,
    /// The wait being served, while the script is at a [`Wait::Time`].
    sleep: Option<Pin<Box<Sleep>>>,
    reader: Option<Waker>,
    writer: Option<Waker>,
}

impl Shared {
    fn new(script: Script, most_ops: usize) -> Self {
        let room = script.room;
        Self {
            steps: script.steps.into(),
            pending: Vec::new(),
            credit: room,
            written: Vec::new(),
            delivered: 0,
            reached: 0,
            ops: 0,
            most_ops,
            overrun: false,
            ended: false,
            failed: false,
            cancel: false,
            shut: false,
            sleep: None,
            reader: None,
            writer: None,
        }
    }

    /// Counts one read or write, and refuses it once the budget is gone.
    fn op(&mut self) -> io::Result<()> {
        self.ops += 1;
        if self.ops > self.most_ops {
            self.overrun = true;
            return Err(io::Error::other(Overrun));
        }
        Ok(())
    }

    /// Done with this step; on to the next.
    fn next(&mut self) {
        self.steps.pop_front();
        self.reached += 1;
    }

    /// Runs the script as far as it will go.
    ///
    /// It stops at a wait whose thing has not happened, at a second thing to say while
    /// the first is still unread, or at the end of the connection. Steps about room to
    /// write are not held up by an unread delivery: an upstream deciding how much of a
    /// request it will take has nothing to do with whether the client has got round to
    /// reading yet, and tying the two together would invent deadlocks that no peer
    /// would ever cause.
    fn advance(&mut self, cx: &mut Context<'_>) {
        while !self.ended && !self.failed && !self.cancel {
            match self.steps.front() {
                None => break,
                Some(Step::Say(_)) => {
                    if !self.pending.is_empty() {
                        break;
                    }
                    if let Some(Step::Say(bytes)) = self.steps.pop_front() {
                        self.pending = bytes;
                        self.reached += 1;
                        wake(&mut self.reader);
                    }
                }
                Some(Step::Block) => {
                    self.credit = 0;
                    self.next();
                }
                Some(&Step::Take(more)) => {
                    self.credit = self.credit.saturating_add(more);
                    self.next();
                    wake(&mut self.writer);
                }
                Some(&Step::Wait(Wait::Written(bytes))) => {
                    if self.written.len() < bytes {
                        break;
                    }
                    self.next();
                }
                Some(&Step::Wait(Wait::Time(delay))) => {
                    // Made when the script gets here, so the wait is counted from then.
                    let ready = {
                        let sleep = self
                            .sleep
                            .get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
                        sleep.as_mut().poll(cx).is_ready()
                    };
                    if !ready {
                        break;
                    }
                    self.sleep = None;
                    self.next();
                }
                Some(&Step::Wait(Wait::Forever)) => break,
                Some(Step::Close) => {
                    self.ended = true;
                    self.next();
                    wake(&mut self.reader);
                }
                Some(Step::Fail) => {
                    self.failed = true;
                    self.next();
                    wake(&mut self.reader);
                    wake(&mut self.writer);
                }
                Some(Step::Cancel) => {
                    self.cancel = true;
                    self.next();
                }
            }
        }
    }
}

/// Wakes whoever was waiting, and forgets them: a waker taken when it is used and
/// registered again on the next `Pending` cannot wake a task that has moved on.
fn wake(waker: &mut Option<Waker>) {
    if let Some(waker) = waker.take() {
        waker.wake();
    }
}

/// The connection to the scripted upstream: a socket that is memory and a script.
#[derive(Debug)]
pub struct Scripted {
    shared: Arc<Mutex<Shared>>,
}

impl Scripted {
    fn shared(&self) -> MutexGuard<'_, Shared> {
        // A run is one thread and the lock is never held across an await, so there is
        // nothing to poison it; and if something did, what it recorded is still the
        // truth about what happened before.
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The failure that a [`Step::Fail`] is.
fn reset() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        "the upstream's connection failed",
    )
}

impl AsyncRead for Scripted {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut shared = self.shared();
        if let Err(error) = shared.op() {
            return Poll::Ready(Err(error));
        }
        shared.advance(cx);
        // Nowhere to put anything: not the end of the connection, and not a read.
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // Before the failure, not after it: everything the script said is read, and only
        // then does the connection break under the client.
        if !shared.pending.is_empty() {
            let taken = buf.remaining().min(shared.pending.len());
            buf.put_slice(&shared.pending[..taken]);
            shared.pending.drain(..taken);
            shared.delivered += taken;
            return Poll::Ready(Ok(()));
        }
        if shared.failed {
            return Poll::Ready(Err(reset()));
        }
        if shared.ended {
            // Nothing read and no error: the end.
            return Poll::Ready(Ok(()));
        }
        shared.reader = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for Scripted {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut shared = self.shared();
        if let Err(error) = shared.op() {
            return Poll::Ready(Err(error));
        }
        shared.advance(cx);
        if shared.failed {
            return Poll::Ready(Err(reset()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if shared.credit == 0 {
            shared.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let taken = buf.len().min(shared.credit);
        shared.written.extend_from_slice(&buf[..taken]);
        shared.credit -= taken;
        // The upstream may have been waiting for exactly these bytes.
        shared.advance(cx);
        Poll::Ready(Ok(taken))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Nothing is held on the way out: a write that returned has arrived.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared().shut = true;
        Poll::Ready(Ok(()))
    }
}

/// The work, until the script says the client goes away.
///
/// Cancellation is looked at after the work has been polled, because the step that asks
/// for it is reached from inside that poll: the run stops on the same turn the script
/// said to stop, with the work dropped where it stood.
struct Until<T> {
    work: Pin<Box<dyn Future<Output = T>>>,
    shared: Arc<Mutex<Shared>>,
}

impl<T> Future for Until<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(value) = this.work.as_mut().poll(cx) {
            return Poll::Ready(Some(value));
        }
        let cancel = this
            .shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancel;
        if cancel {
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

/// Runs `work` against `script`, on one thread with the clock stopped.
///
/// The clock moves only when there is nothing else to do, so what happens is in the
/// script's order and not the machine's: the same script gives the same run every time,
/// and a minute-long deadline costs nothing to wait for. Both bounds in `budget` end a
/// run that is getting nowhere, so a script that stalls gives an [`Outcome`] like any
/// other rather than hanging.
///
/// # Panics
///
/// If a runtime cannot be built, which is the harness itself being broken.
#[expect(
    clippy::expect_used,
    reason = "a runtime that will not build is the harness itself being broken, and a run \
              that cannot happen has nothing to report"
)]
pub fn run<T, F, Fut>(script: Script, budget: Budget, work: F) -> (Outcome<T>, Tape)
where
    F: FnOnce(Scripted) -> Fut,
    Fut: Future<Output = T> + 'static,
{
    let shared = Arc::new(Mutex::new(Shared::new(script, budget.ops)));
    let socket = Scripted {
        shared: Arc::clone(&shared),
    };
    let until = Until {
        work: Box::pin(work(socket)),
        shared: Arc::clone(&shared),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime with a clock");

    let (outcome, elapsed) = runtime.block_on(async move {
        let began = tokio::time::Instant::now();
        let outcome = match tokio::time::timeout(budget.time, until).await {
            Ok(Some(value)) => Outcome::Done(value),
            Ok(None) => Outcome::Cancelled,
            Err(_) => Outcome::Spent(Spent::Time),
        };
        (outcome, began.elapsed())
    });

    let shared = shared.lock().unwrap_or_else(PoisonError::into_inner);
    let tape = Tape {
        written: shared.written.clone(),
        delivered: shared.delivered,
        unread: shared.pending.len(),
        reached: shared.reached,
        ops: shared.ops,
        shut: shared.shut,
        elapsed,
    };
    // An overrun reached the client as an error, because an error is the only thing a
    // client that is not waiting for anything will look at. It is the budget all the same.
    let outcome = if shared.overrun {
        Outcome::Spent(Spent::Ops)
    } else {
        outcome
    };
    (outcome, tape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Reads everything the upstream says, until the end or an error. The error is kept
    /// as its kind, so that an outcome can simply be compared.
    async fn read_all(mut socket: Scripted) -> Result<Vec<u8>, io::ErrorKind> {
        let mut said = Vec::new();
        let mut buffer = [0; 64];
        loop {
            let read = socket
                .read(&mut buffer)
                .await
                .map_err(|error| error.kind())?;
            if read == 0 {
                return Ok(said);
            }
            said.extend_from_slice(&buffer[..read]);
        }
    }

    #[test]
    fn what_is_said_arrives_in_the_pieces_the_script_says_it_in() {
        let script = Script::new(vec![
            Step::Say(b"one".to_vec()),
            Step::Say(b"two".to_vec()),
            Step::Close,
        ]);
        let (outcome, tape) = run(script, Budget::default(), |mut socket| async move {
            let mut buffer = [0; 64];
            let first = socket
                .read(&mut buffer)
                .await
                .map_err(|error| error.kind())?;
            let second = socket
                .read(&mut buffer)
                .await
                .map_err(|error| error.kind())?;
            Ok::<_, io::ErrorKind>((first, second))
        });
        // One read gets one delivery, however much room it offered: a script that says
        // two things is two arrivals, which is how a message is split across reads.
        assert_eq!(outcome, Outcome::Done(Ok((3, 3))));
        assert_eq!(tape.delivered, 6);
    }

    #[test]
    fn a_write_waits_until_the_upstream_makes_room_for_it() {
        let budget = Budget::default();
        let script = Script::with_room(
            vec![
                Step::Wait(Wait::Time(Duration::from_secs(2))),
                Step::Take(4),
                Step::Close,
            ],
            0,
        );
        let (outcome, tape) = run(script, budget, |mut socket| async move {
            socket
                .write(b"abcdefgh")
                .await
                .map_err(|error| error.kind())
        });
        // The write could not go until the step that made room for it, and then went
        // only as far as that room reached.
        assert_eq!(outcome, Outcome::Done(Ok(4)));
        assert_eq!(tape.written, b"abcd");
        assert_eq!(tape.elapsed, Duration::from_secs(2));
    }

    #[test]
    fn a_wait_on_the_request_releases_when_the_bytes_have_gone() {
        let script = Script::new(vec![
            Step::Wait(Wait::Written(4)),
            Step::Say(b"ok".to_vec()),
            Step::Close,
        ]);
        let (outcome, tape) = run(script, Budget::default(), |mut socket| async move {
            // Three bytes are not the four the upstream is waiting for, so the answer
            // comes only once the fourth has gone.
            socket
                .write_all(b"abc")
                .await
                .map_err(|error| error.kind())?;
            let mut buffer = [0; 8];
            let early =
                tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buffer)).await;
            assert!(early.is_err(), "the answer came before it was earned");
            socket.write_all(b"d").await.map_err(|error| error.kind())?;
            let mut said = Vec::new();
            socket
                .read_to_end(&mut said)
                .await
                .map_err(|error| error.kind())?;
            Ok::<_, io::ErrorKind>(said)
        });
        assert_eq!(outcome, Outcome::Done(Ok(b"ok".to_vec())));
        assert_eq!(tape.written, b"abcd");
    }

    #[test]
    fn a_wait_on_the_clock_is_counted_from_where_the_script_reached_it() {
        let script = Script::new(vec![
            Step::Wait(Wait::Time(Duration::from_secs(3))),
            Step::Say(b"late".to_vec()),
            Step::Wait(Wait::Time(Duration::from_secs(3))),
            Step::Close,
        ]);
        let (outcome, tape) = run(script, Budget::default(), read_all);
        assert_eq!(outcome, Outcome::Done(Ok(b"late".to_vec())));
        // Six seconds, not three: the second wait began when the script got to it. Waits
        // armed when the run began would both have been over at three.
        assert_eq!(tape.elapsed, Duration::from_secs(6));
    }

    #[test]
    fn a_peer_that_stops_talking_ends_on_the_budget_and_does_not_hang() {
        let budget = Budget {
            time: Duration::from_secs(30),
            ..Budget::default()
        };
        let script = Script::new(vec![
            Step::Say(b"partial".to_vec()),
            Step::Wait(Wait::Forever),
        ]);
        let (outcome, tape) = run(script, budget, read_all);
        assert_eq!(outcome, Outcome::Spent(Spent::Time));
        // The clock went to the end of the budget and stopped there. A client with a
        // deadline of its own would have given up before this; one without waits here.
        assert_eq!(tape.elapsed, budget.time);
        assert_eq!(tape.delivered, 7);
    }

    #[test]
    fn a_close_comes_after_everything_that_was_said_before_it() {
        let script = Script::new(vec![Step::Say(b"all of it".to_vec()), Step::Close]);
        let (outcome, tape) = run(script, Budget::default(), read_all);
        assert_eq!(outcome, Outcome::Done(Ok(b"all of it".to_vec())));
        assert_eq!(tape.unread, 0);
    }

    #[test]
    fn a_failure_comes_after_everything_that_was_said_before_it() {
        let script = Script::new(vec![Step::Say(b"said".to_vec()), Step::Fail]);
        let (outcome, tape) = run(script, Budget::default(), read_all);
        // The bytes, and then the reset: a failure is a connection that broke rather
        // than an orderly end, but what was said before it still arrives. A reset that
        // swallowed some of it would make what a client can see depend on how far it had
        // got, and the oracle would be guessing at that.
        assert_eq!(
            outcome,
            Outcome::Done(Err(io::ErrorKind::ConnectionReset)),
            "{tape:?}"
        );
        assert_eq!(tape.delivered, 4);
        assert_eq!(tape.unread, 0);
    }

    #[test]
    fn a_cancel_drops_the_work_where_the_script_says_it() {
        let script = Script::new(vec![
            Step::Say(b"first".to_vec()),
            Step::Cancel,
            Step::Say(b"never".to_vec()),
        ]);
        let (outcome, tape) = run(script, Budget::default(), read_all);
        assert_eq!(outcome, Outcome::Cancelled);
        // What came before the cancellation was read; the step after it was never
        // reached, so there was nothing more to read and nothing left unread either.
        assert_eq!(tape.delivered, 5);
        assert_eq!(tape.reached, 2);
        assert_eq!(tape.unread, 0);
    }

    #[test]
    fn a_client_that_goes_round_without_getting_anywhere_runs_out_of_operations() {
        let budget = Budget {
            ops: 64,
            ..Budget::default()
        };
        let script = Script::new(vec![Step::Wait(Wait::Forever)]);
        let (outcome, tape) = run(script, budget, |mut socket| async move {
            // Asking again and again for something that is not coming, and never
            // waiting for it: the run has to be what stops this.
            loop {
                let mut buffer = [0; 8];
                let asked =
                    tokio::time::timeout(Duration::from_millis(1), socket.read(&mut buffer)).await;
                if matches!(asked, Ok(Err(_))) {
                    return;
                }
            }
        });
        // The work saw an error and returned, so it finished; what is reported is the
        // budget all the same, because an outcome that called this a connection failure
        // would send a comparison looking for a fault in the peer.
        assert_eq!(outcome, Outcome::Spent(Spent::Ops));
        // Every one of them, and the one that was refused.
        assert_eq!(tape.ops, budget.ops + 1);
    }

    /// What a two-task run came to: how much the writer wrote, and what the reader read.
    type Halves = Result<(usize, Vec<u8>), io::ErrorKind>;

    /// What a task came to, with a task that did not finish counted as a failure of the
    /// run rather than of either peer.
    fn joined<T>(task: Result<io::Result<T>, tokio::task::JoinError>) -> Result<T, io::ErrorKind> {
        match task {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error.kind()),
            Err(_) => Err(io::ErrorKind::Other),
        }
    }

    /// Reading and writing in tasks of their own, so that a step has to wake whichever
    /// one is waiting on it. With both in a single task any wake at all would serve and
    /// these wakes would look load-bearing without being it. Which task the clock wakes
    /// is whichever polled it last, so both orders are worth running.
    fn in_two_tasks(writer_first: bool) -> (Outcome<Halves>, Tape) {
        let script = Script::with_room(
            vec![
                Step::Wait(Wait::Time(Duration::from_secs(1))),
                Step::Take(4),
                Step::Say(b"go".to_vec()),
                // Nothing after it: an end would wake the reader by itself, and the
                // delivery's own wake would look load-bearing without being it.
                Step::Wait(Wait::Forever),
            ],
            0,
        );
        run(script, Budget::default(), move |socket| async move {
            let (mut reading, mut writing) = tokio::io::split(socket);
            let read = async move {
                let mut said = [0; 8];
                let taken = reading.read(&mut said).await?;
                Ok(said[..taken].to_vec())
            };
            let write = async move { writing.write(b"abcd").await };
            let (wrote, said) = if writer_first {
                let writer = tokio::spawn(write);
                let reader = tokio::spawn(read);
                (writer.await, reader.await)
            } else {
                let reader = tokio::spawn(read);
                let writer = tokio::spawn(write);
                (writer.await, reader.await)
            };
            Ok((joined(wrote)?, joined(said)?))
        })
    }

    #[test]
    fn both_halves_are_woken_by_the_step_they_were_waiting_on() {
        for writer_first in [false, true] {
            let (outcome, tape) = in_two_tasks(writer_first);
            // Neither half could have got there by itself: the writer had no room until
            // the clock's step gave it some, and the reader had nothing to read until
            // the write had gone.
            assert_eq!(
                outcome,
                Outcome::Done(Ok((4, b"go".to_vec()))),
                "the writer went first: {writer_first}"
            );
            assert_eq!(tape.written, b"abcd");
            assert_eq!(tape.elapsed, Duration::from_secs(1));
        }
    }

    #[test]
    fn a_read_with_nowhere_to_put_anything_is_neither_an_end_nor_a_wait() {
        let script = Script::new(vec![Step::Wait(Wait::Forever)]);
        let (outcome, tape) = run(script, Budget::default(), |mut socket| async move {
            socket.read(&mut []).await.map_err(|error| error.kind())
        });
        // Nothing was asked for and nothing came of it. Waiting would be a hang, since
        // nothing that happens later would give this read anywhere to put a byte.
        assert_eq!(outcome, Outcome::Done(Ok(0)));
        assert_eq!(tape.ops, 1, "the socket was never asked");
    }

    #[test]
    fn what_was_delivered_is_counted_apart_from_what_was_used() {
        let script = Script::new(vec![
            Step::Say(b"more than is wanted".to_vec()),
            Step::Close,
        ]);
        let (outcome, tape) = run(script, Budget::default(), |mut socket| async move {
            let mut buffer = [0; 4];
            socket
                .read_exact(&mut buffer)
                .await
                .map(|_| buffer)
                .map_err(|error| error.kind())
        });
        assert_eq!(outcome, Outcome::Done(Ok(*b"more")));
        // Four bytes were handed over and fifteen were not. Nothing here says what the
        // client made of the four: reads are not consumption.
        assert_eq!(tape.delivered, 4);
        assert_eq!(tape.unread, 15);
    }

    #[test]
    fn a_half_close_by_the_client_is_recorded() {
        let script = Script::new(vec![Step::Close]);
        let (_outcome, tape) = run(script, Budget::default(), |mut socket| async move {
            socket.shutdown().await.map_err(|error| error.kind())
        });
        assert!(tape.shut, "the client's half-close was not seen");
    }

    #[test]
    fn what_an_oracle_reads_is_what_was_said_before_the_end() {
        let script = Script::new(vec![
            Step::Say(b"one".to_vec()),
            Step::Wait(Wait::Written(1)),
            Step::Say(b"two".to_vec()),
            Step::Close,
            Step::Say(b"after the end".to_vec()),
        ]);
        // Waits and room are not bytes, and nothing after the connection ended ever
        // reaches a client, so neither belongs in what an oracle is given.
        assert_eq!(script.said(), b"onetwo");
        assert!(!script.fails());
        assert!(Script::new(vec![Step::Say(b"x".to_vec()), Step::Fail]).fails());
        // A script that never ends its connection has not failed it either.
        assert!(!Script::new(vec![Step::Wait(Wait::Forever)]).fails());
    }

    /// The thing the harness is for, driven over the scripted socket: a script fits the
    /// real client, not only the toy readers and writers above. What it does with the
    /// bytes belongs to the oracles and to the comparison that comes after this.
    #[test]
    fn an_exchange_runs_against_a_script() {
        use crate::upstream::h1::H1Limits;
        use crate::upstream::h1::codec::Sending;
        use crate::upstream::h1::exchange::Exchange;
        use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
        use http_body_util::Empty;
        use hyper::body::Bytes;

        let script = Script::new(vec![
            // Nothing is said until the request has begun to arrive, which is what an
            // upstream does.
            Step::Wait(Wait::Written(20)),
            Step::Say(b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()),
            Step::Close,
        ]);
        let uri: Uri = "/a?b=1".parse().expect("a path is a uri");
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("host"),
            HeaderValue::from_static("up.test"),
        );

        let (outcome, tape) = run(script, Budget::default(), move |socket| async move {
            let limits = H1Limits::default();
            let (answer, rest) = Exchange::new(socket)
                .send(
                    &Method::GET,
                    &uri,
                    &headers,
                    Sending::None,
                    Empty::<Bytes>::new(),
                    &limits,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>((answer.head.status.as_u16(), rest.upload_finished()))
        });

        assert_eq!(outcome, Outcome::Done(Ok((204, true))), "{tape:?}");
        let head = String::from_utf8(tape.written).expect("the head is text");
        assert!(head.starts_with("GET /a?b=1 HTTP/1.1\r\n"), "{head}");
        assert!(head.contains("host: up.test\r\n"), "{head}");
    }

    #[test]
    fn any_input_at_all_is_a_script_inside_its_budget() {
        let budget = Budget {
            steps: 6,
            said: 12,
            ..Budget::default()
        };
        // Every tag, with operands that would run past the end if they were trusted.
        for seed in 0u8..=255 {
            let bytes: Vec<u8> = (0..40).map(|index| seed.wrapping_mul(index + 1)).collect();
            let script = Script::decode(&bytes, &budget);
            assert!(script.steps().len() <= budget.steps, "{seed}");
            assert!(script.said().len() <= budget.said, "{seed}");
        }
    }

    #[test]
    fn a_decoded_script_says_what_the_input_said() {
        // Tag 0 is "say", and the byte after it is how much: three bytes, then two.
        let script = Script::decode(b"\x00\x03abc\x00\x02de", &Budget::default());
        assert_eq!(
            script.steps(),
            [Step::Say(b"abc".to_vec()), Step::Say(b"de".to_vec())]
        );
        // An input that stops in the middle of a step stops the script there.
        assert_eq!(Script::decode(b"\x00", &Budget::default()).steps(), []);
        // And a step whose operand asks for more than is left takes what there is.
        assert_eq!(
            Script::decode(b"\x00\xff\xaa", &Budget::default()).steps(),
            [Step::Say(vec![0xaa])]
        );
    }
}
