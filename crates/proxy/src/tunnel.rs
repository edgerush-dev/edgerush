//! Two connections carried to each other byte for byte, both ways at once: an L4
//! listener's connection and its backend ([17 §4](../../../docs/17-tcp-and-tls-passthrough.md)),
//! or a WebSocket's client and backend once the upgrade is made
//! ([19 §5](../../../docs/19-websocket.md)).
//!
//! Each way reads into a block of the worker's and writes what it read on before it reads
//! again, so a side that stops taking bytes stops the other from sending more than a block
//! ahead. **A way holds a block only while bytes are on their way**: it takes one to read
//! into, reads into it again once everything in it has been written on, and gives it back
//! once the tunnel has carried nothing for a second. An idle tunnel holds no buffer at all,
//! which is what lets a worker hold as many quiet WebSockets as it has connections for.
//! The blocks come from the worker's free list, so taking and giving one is a push and a
//! pop.
//!
//! A side's end is passed on as a half-close (`shutdown(Write)`), and the other way goes on
//! until it ends too: a protocol that says "that is all I have" and then waits for the
//! answer is carried as it is. The tunnel is closed once both ways have ended, when either
//! side fails, when it has carried nothing for its idle bound, when it is drained — its
//! worker drains, or its client's connection, or a reload takes its route away — and its
//! bound is up, or when the worker has no storage for a block to read into.

use crate::connections::Held;
use crate::drain::Drain;
use crate::h2_stream::H2Stream;
use crate::random::{random, unguessable};
use crate::timers::{Alarm, Timers};
use crate::upstream::balancing::InFlight;
use crate::upstream::h1::blocks::{Block, Blocks};
use crate::upstream::secure::Socket;
use crate::websocket::frames::{Frames, GOING_AWAY, going_away_masked};
use std::cell::RefCell;
use std::future::poll_fn;
use std::mem::MaybeUninit;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

/// How a tunnel ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carried {
    /// Both ways ended, each end passed on.
    Closed,
    /// Nothing either way for the idle bound.
    Idle,
    /// It was drained, and the drain's bound came.
    Drained,
    /// A side failed.
    Failed,
    /// The worker could not pay for a block to read into.
    Exhausted,
}

/// How a tunnel ended, and what it carried each way: the bytes passed on, not those of
/// the gateway's own Close frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tunneled {
    pub(crate) how: Carried,
    /// From the client to the backend.
    pub(crate) up: u64,
    /// From the backend to the client.
    pub(crate) down: u64,
}

impl From<Carried> for crate::metrics::Tunnel {
    fn from(carried: Carried) -> Self {
        match carried {
            Carried::Closed => Self::Closed,
            Carried::Idle => Self::Idle,
            Carried::Drained => Self::Drained,
            Carried::Failed => Self::Failed,
            Carried::Exhausted => Self::Exhausted,
        }
    }
}

/// What a tunnel is held to, and how it ends when it is drained.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    /// How long it may carry nothing either way.
    pub(crate) idle: Duration,
    /// How long it may go on once it is drained.
    pub(crate) drain_within: Duration,
    /// Whether it carries a WebSocket, whose frames are followed both ways so that a drain
    /// can close it with a Close frame each way (19 §6).
    pub(crate) websocket: bool,
}

/// How long a draining WebSocket's close is left for the Close replies: its moment falls in
/// the drain bound less this (19 §6).
const REPLIES: Duration = Duration::from_secs(5);

/// How long a tunnel that carried nothing either way keeps the blocks it last read into
/// before it gives them back. Not at once: a tunnel carrying a request and its answer at a
/// time would take and give a block each way for every one, which cost passthrough some
/// 0.7k instructions a request; our HTTP/2 server keeps its buffers for as long before it
/// gives them back, for the same reason (15 §3).
const RELEASE_AFTER: Duration = Duration::from_secs(1);

/// The room on the stack a way reads into when it holds no block: enough for most of what
/// a quiet tunnel wakes to, a WebSocket message or a ping, whole.
const PROBE: usize = 1024;

/// A WebSocket's backend, once its upgrade is made (19 §2, §4): the connection its 101 came
/// on, or the HTTP/2 stream its extended CONNECT was answered on; what was read past the
/// 101; what the tunnel is held to; and the worker's parts the tunnel needs. The request
/// core leaves it with the request's interim channel, and the server that wrote the answer
/// — a 101, or an extended CONNECT's 200 — carries it to its client, from a task of its own
/// if the server runs one a stream.
pub(crate) struct Switched {
    /// The backend's side.
    pub(crate) backend: Backend,
    /// What the backend sent after its 101, which goes to the client first.
    pub(crate) leftover: Option<Block>,
    /// The rule's idle bound, and the drain bound.
    pub(crate) bounds: Bounds,
    pub(crate) blocks: Rc<RefCell<Blocks>>,
    pub(crate) timers: Rc<Timers>,
    /// The drain of the listener, route and upstream it was routed by, which a reload that
    /// takes any of them away starts (03 §10).
    pub(crate) route: Rc<Drain>,
    /// Told how the tunnel ended, which counts it as its listener's.
    pub(crate) ended: Box<dyn FnOnce(Tunneled)>,
    /// The handshake's count at its endpoint, held until the tunnel closes: a WebSocket is
    /// load on its backend for as long as it is open (03 §6).
    pub(crate) counted: Option<InFlight>,
    /// For a client over HTTP/2 or HTTP/3, the tunnel's count among its worker's
    /// connections, held until it closes (03 §9).
    pub(crate) held: Option<Held>,
}

impl std::fmt::Debug for Switched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Switched")
            .field("backend", &self.backend)
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}

impl Switched {
    /// Carries `client` to the backend until the tunnel ends, what the client sent after its
    /// handshake (`early`) going first, and counts how it ended. `drain` is the client
    /// connection's, which the tunnel drains with: the server that carries it hands it over.
    pub(crate) async fn carry<C>(
        self,
        client: &mut C,
        early: Option<Block>,
        drain: &Drain,
    ) -> Tunneled
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let Self {
            mut backend,
            leftover,
            bounds,
            blocks,
            timers,
            route,
            ended,
            counted,
            held,
        } = self;
        let carried = carry(
            client,
            &mut backend,
            early,
            leftover,
            &blocks,
            bounds,
            &timers,
            [drain, &route],
        )
        .await;
        // Load on its backend no longer, and a connection of its worker's no longer.
        drop(counted);
        drop(held);
        ended(carried);
        carried
    }
}

/// A WebSocket's backend side: a connection spoken to in HTTP/1.1, or a stream of one in
/// HTTP/2.
#[derive(Debug)]
pub(crate) enum Backend {
    Socket(Socket),
    H2(H2Stream),
}

impl AsyncRead for Backend {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Socket(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::H2(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Backend {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Socket(socket) => Pin::new(socket).poll_write(cx, buf),
            Self::H2(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Socket(socket) => Pin::new(socket).poll_flush(cx),
            Self::H2(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Socket(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::H2(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// One way through the tunnel.
struct Way {
    /// What has been read and not yet written on; none while nothing is on its way.
    block: Option<Block>,
    /// The side it reads from has ended.
    ended: bool,
    /// And that end has been passed on.
    shut: bool,
    /// For a WebSocket, where the frames of what this way reads begin.
    frames: Option<Frames>,
    /// A Close of the gateway's that this way owes its far side.
    close: Close,
    /// Whether its far side is the backend, whose Close from the gateway is masked.
    to_backend: bool,
    /// The bytes it has passed on.
    carried: u64,
}

/// Where a way is with a Close frame of the gateway's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Close {
    /// None is owed.
    None,
    /// One is owed at the next frame boundary.
    Wanted,
    /// It is being written: its bytes, how long it is and how much has gone.
    Writing([u8; 8], u8, u8),
    /// It has gone: what this way reads from now on is followed, not passed on.
    Sent,
}

impl Way {
    fn new(block: Option<Block>, websocket: bool, to_backend: bool) -> Self {
        let frames = websocket.then(|| {
            let mut frames = Frames::new();
            if let Some(block) = &block {
                frames.read(block.data());
            }
            frames
        });
        Self {
            block,
            ended: false,
            shut: false,
            frames,
            close: Close::None,
            to_backend,
            carried: 0,
        }
    }

    /// Whether this way is where it may take a Close frame: nothing of its own in hand, and
    /// what it has passed on ending with a frame.
    fn at_boundary(&self) -> bool {
        self.block.as_ref().is_none_or(Block::is_empty)
            && self.frames.as_ref().is_some_and(Frames::at_boundary)
    }
}

/// Why a way stopped short of its end.
enum Stopped {
    /// A side failed.
    Failed,
    /// No block could be had to read into.
    Exhausted,
}

/// Where a WebSocket is with a drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Draining {
    /// Not draining.
    No,
    /// Its Close frames go at this moment.
    At(Instant),
    /// They are owed, or have gone, each way; done once both replies are in.
    Closing,
    /// Left to end by itself or at the bound: already closing, or not one to close.
    Left,
}

/// Carries `client` to `backend` and back until the tunnel ends. `up` may already hold
/// bytes the client sent and `down` bytes the backend sent, which go first. Every block is
/// back with `blocks` when it returns. It is drained once either of `drains` starts: its
/// client connection's, and its route's.
///
/// A WebSocket (`bounds.websocket`) is closed on drain as 19 §6 has it: at a moment drawn
/// in the drain bound less five seconds, each side is sent a Close 1001 at its next frame
/// boundary — the backend's masked, as a client's must be — and nothing more of the other's;
/// once both have answered with their own Close, the tunnel ends. One already closing is
/// left to finish, and one whose frames could not be followed is closed bare.
#[expect(
    clippy::too_many_arguments,
    reason = "each is a different thing the tunnel needs, and a struct to hold them would \
              be indirection for a lint rather than for a reader"
)]
pub(crate) async fn carry<C, B>(
    client: &mut C,
    backend: &mut B,
    up: Option<Block>,
    down: Option<Block>,
    blocks: &RefCell<Blocks>,
    bounds: Bounds,
    timers: &Rc<Timers>,
    drains: [&Drain; 2],
) -> Tunneled
where
    C: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut up = Way::new(up, bounds.websocket, true);
    let mut down = Way::new(down, bounds.websocket, false);
    let mut alarm = Alarm::new(timers, None);
    let [drain, also] = drains;
    let mut heard = pin!(drain.notified());
    let mut heard_too = pin!(also.notified());
    let mut last = Instant::now();
    let mut drain_by = None;
    let mut draining = Draining::No;
    let mut releasing = false;
    // Until when a read that finds nothing keeps its block: a second after bytes last moved,
    // and not at all before any have.
    let mut keep_until: Option<Instant> = None;
    let how = poll_fn(|cx| {
        loop {
            let mut moved = false;
            // Past its release time, a read that finds nothing gives its block back.
            // A read that finds nothing keeps its block only while the tunnel is busy: bytes
            // moved within the release time, which the alarm ends. No clock is read for it.
            if std::mem::take(&mut releasing) {
                keep_until = None;
            }
            let quiet = keep_until.is_none();
            for (way, from_client) in [(&mut up, true), (&mut down, false)] {
                if way.shut {
                    continue;
                }
                let pumped = if from_client {
                    pump(client, backend, way, blocks, quiet, &mut moved, cx)
                } else {
                    pump(backend, client, way, blocks, quiet, &mut moved, cx)
                };
                match pumped {
                    Poll::Ready(Err(Stopped::Failed)) => return Poll::Ready(Carried::Failed),
                    Poll::Ready(Err(Stopped::Exhausted)) => {
                        return Poll::Ready(Carried::Exhausted);
                    }
                    Poll::Ready(Ok(())) | Poll::Pending => {}
                }
            }
            if up.shut && down.shut {
                return Poll::Ready(if draining == Draining::Closing {
                    Carried::Drained
                } else {
                    Carried::Closed
                });
            }
            // Both Closes gone, and both answered: the WebSocket is closed as RFC 6455 has
            // it, and its connections go with it.
            if draining == Draining::Closing
                && up.close == Close::Sent
                && down.close == Close::Sent
                && up.frames.as_ref().is_some_and(Frames::closing)
                && down.frames.as_ref().is_some_and(Frames::closing)
            {
                return Poll::Ready(Carried::Drained);
            }
            if moved {
                last = Instant::now();
                keep_until = Some(last + RELEASE_AFTER);
            }
            if drain_by.is_none()
                && (drain.poll_on(heard.as_mut(), cx).is_ready()
                    || also.poll_on(heard_too.as_mut(), cx).is_ready())
            {
                let now = Instant::now();
                drain_by = Some(now + bounds.drain_within);
                if bounds.websocket {
                    draining = Draining::At(now + spread(bounds.drain_within));
                }
            }
            if let Draining::At(moment) = draining
                && Instant::now() >= moment
            {
                draining = close_at_boundaries(&mut up, &mut down);
                match draining {
                    // Not one that can be closed with a frame: closed bare, now.
                    Draining::No => return Poll::Ready(Carried::Drained),
                    // Its Closes may go at once: round again to write them.
                    Draining::Closing => continue,
                    Draining::At(_) | Draining::Left => {}
                }
            }
            // Blocks kept while nothing moves go back at the release time, when the ways'
            // reads, finding nothing, give them back.
            let holding = [&up, &down]
                .iter()
                .any(|way| way.block.as_ref().is_some_and(Block::is_empty));
            let release_by = keep_until.filter(|_| holding);
            let idle_by = last + bounds.idle;
            let mut due = drain_by.map_or(idle_by, |drain_by: Instant| drain_by.min(idle_by));
            if let Draining::At(moment) = draining {
                due = due.min(moment);
            }
            if let Some(release_by) = release_by {
                due = due.min(release_by);
            }
            if alarm.poll_until(cx, due).is_ready() {
                if release_by.is_some_and(|release_by| release_by <= due) {
                    // The release time came, as the timers tell it, which may be a little
                    // before the clock does: round again, the ways' reads now giving back.
                    releasing = true;
                    continue;
                }
                if let Draining::At(moment) = draining
                    && moment <= due
                {
                    // The moment came, as the timers tell it, which may be a little before
                    // the clock does: it is taken as come, and acted on.
                    draining = Draining::At(Instant::now());
                    continue;
                }
                let drained = drain_by.is_some_and(|drain_by| drain_by <= idle_by);
                return Poll::Ready(if drained {
                    Carried::Drained
                } else {
                    Carried::Idle
                });
            }
            return Poll::Pending;
        }
    })
    .await;
    // A tunnel ended in order says its end on each side not yet told: over TLS with a
    // closure alert (RFC 8446 §6.1), over TCP a FIN, on an HTTP/2 or HTTP/3 stream its end.
    // One try, never waited on, as HAProxy and Envoy close: a side that cannot take it now
    // has stopped reading, and would not see it. One that failed is told nothing.
    if matches!(how, Carried::Idle | Carried::Drained) {
        poll_fn(|cx| {
            if !up.shut {
                let _told = Pin::new(&mut *backend).poll_shutdown(cx);
            }
            if !down.shut {
                let _told = Pin::new(&mut *client).poll_shutdown(cx);
            }
            Poll::Ready(())
        })
        .await;
    }
    let carried = Tunneled {
        how,
        up: up.carried,
        down: down.carried,
    };
    let mut blocks = blocks.borrow_mut();
    for block in [up.block, down.block].into_iter().flatten() {
        blocks.give(block);
    }
    carried
}

/// When, within a drain bound of `drain_within`, a WebSocket is closed: drawn evenly in all
/// but the last five seconds of it, so that its clients come back to the other pods spread
/// out rather than all at once, and have those seconds to answer (19 §6).
fn spread(drain_within: Duration) -> Duration {
    let window = drain_within.saturating_sub(REPLIES);
    let millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(random() % millis)
}

/// Owes each side a Close at its next frame boundary, unless the WebSocket is closing
/// already (a Close has gone by either way), which is left to finish, or cannot be followed
/// either way, which is closed bare: `Draining::No` says so.
fn close_at_boundaries(up: &mut Way, down: &mut Way) -> Draining {
    let (Some(upward), Some(downward)) = (up.frames.as_ref(), down.frames.as_ref()) else {
        return Draining::No;
    };
    if upward.lost() || downward.lost() {
        return Draining::No;
    }
    if upward.closing() || downward.closing() {
        return Draining::Left;
    }
    up.close = Close::Wanted;
    down.close = Close::Wanted;
    Draining::Closing
}

/// The Close frame a way owes: to the backend masked, as a client's frames are, with a
/// mask no one can guess (RFC 6455 §5.3); to the client as it is.
fn close_frame(to_backend: bool) -> ([u8; 8], u8) {
    if to_backend {
        let mask = unguessable::<4>().unwrap_or([0x5a; 4]);
        (going_away_masked(mask), 8)
    } else {
        let mut frame = [0; 8];
        frame[..4].copy_from_slice(&GOING_AWAY);
        (frame, 4)
    }
}

/// Moves what `from` sends on to `to`, as far as both allow now. Ready once `from` has
/// ended and `to` has been told; `moved` is set if any byte went. `quiet`: the tunnel has
/// carried nothing for its release time, and a read that finds nothing gives its block
/// back rather than keep it for the next.
///
/// The way's block is read into and written from where it lies: this runs a few times for
/// every exchange a tunnel carries, and moving the block in and out of the way each time
/// cost passthrough a thousand instructions a request. What is rare — no block to be had, a
/// Close to write, an end to pass on, frames to follow — is done apart.
fn pump<F, T>(
    from: &mut F,
    to: &mut T,
    way: &mut Way,
    blocks: &RefCell<Blocks>,
    quiet: bool,
    moved: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<Result<(), Stopped>>
where
    F: AsyncRead + Unpin,
    T: AsyncWrite + Unpin,
{
    // Whether this way carried anything in this turn, which makes it busy whatever the
    // tunnel was before.
    let mut carried = false;
    loop {
        // What has been read goes on before anything more is read. The block it came in is
        // then read into again, and goes back if the read finds nothing: a way that is busy
        // keeps one block, and a quiet one none.
        if let Some(block) = way.block.as_mut() {
            while !block.is_empty() {
                match Pin::new(&mut *to).poll_write(cx, block.data()) {
                    Poll::Ready(Ok(0) | Err(_)) => return Poll::Ready(Err(Stopped::Failed)),
                    Poll::Ready(Ok(written)) => {
                        block.consume(written);
                        way.carried += u64::try_from(written).unwrap_or(u64::MAX);
                        *moved = true;
                        carried = true;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
        if way.close != Close::None || way.ended {
            match closing(to, way, blocks, moved, cx) {
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(stopped)) => return Poll::Ready(Err(stopped)),
                Poll::Pending => return Poll::Pending,
            }
            if way.shut {
                return Poll::Ready(Ok(()));
            }
        }
        // One whose memory went with a frame cut from it — a client's head — has no room
        // left, and is traded for one that has.
        if way
            .block
            .as_mut()
            .is_some_and(|block| block.room().is_empty())
        {
            give_back(way, blocks);
            match blocks.borrow_mut().take() {
                Ok(fresh) => way.block = Some(fresh),
                Err(_) => return Poll::Ready(Err(Stopped::Exhausted)),
            }
        }
        // Into the block the way holds, or one taken to read into. A worker with none to
        // give reads into a little room on the stack instead.
        let block = match &mut way.block {
            Some(block) => block,
            held @ None => {
                let taken = blocks.borrow_mut().take();
                match taken {
                    Ok(block) => held.insert(block),
                    Err(_) => match probe(from, way, blocks, cx) {
                        Poll::Ready(Ok(read)) => {
                            carried |= read;
                            *moved |= read;
                            continue;
                        }
                        Poll::Ready(Err(stopped)) => return Poll::Ready(Err(stopped)),
                        Poll::Pending => return Poll::Pending,
                    },
                }
            }
        };
        let mut read = ReadBuf::new(block.room());
        let polled = Pin::new(&mut *from).poll_read(cx, &mut read);
        let count = read.filled().len();
        match polled {
            Poll::Ready(Ok(())) if count > 0 => {
                carried = true;
                *moved = true;
                if way.frames.is_none() {
                    block.arrived(count);
                } else {
                    followed(way, count);
                }
            }
            // Its block goes back as the end is passed on.
            Poll::Ready(Ok(())) => way.ended = true,
            Poll::Ready(Err(_)) => {
                give_back(way, blocks);
                return Poll::Ready(Err(Stopped::Failed));
            }
            // Kept for the next read, until the tunnel has been quiet a while.
            Poll::Pending => {
                if !carried && quiet {
                    give_back(way, blocks);
                }
                return Poll::Pending;
            }
        }
    }
}

/// Gives the way's block back, if it holds one.
fn give_back(way: &mut Way, blocks: &RefCell<Blocks>) {
    if let Some(block) = way.block.take() {
        blocks.borrow_mut().give(block);
    }
}

/// What is rare on a way, once what it read has gone on: a Close of the gateway's to write,
/// once what has gone on ends with a frame, and an end to pass on. The way's block, empty,
/// goes back first. Ready with `true` if the Close moved on and the way should go round
/// again, with `false` if the way reads on — or has shut, and is done.
#[cold]
fn closing<T>(
    to: &mut T,
    way: &mut Way,
    blocks: &RefCell<Blocks>,
    moved: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<Result<bool, Stopped>>
where
    T: AsyncWrite + Unpin,
{
    give_back(way, blocks);
    if way.close == Close::Wanted && way.at_boundary() {
        let (frame, length) = close_frame(way.to_backend);
        way.close = Close::Writing(frame, length, 0);
    }
    if let Close::Writing(frame, length, written) = way.close {
        let rest = &frame[usize::from(written)..usize::from(length)];
        return match Pin::new(&mut *to).poll_write(cx, rest) {
            Poll::Ready(Ok(0) | Err(_)) => Poll::Ready(Err(Stopped::Failed)),
            Poll::Ready(Ok(more)) => {
                // Never more than the eight bytes asked for.
                let written = written.saturating_add(u8::try_from(more).unwrap_or(length));
                way.close = if written >= length {
                    Close::Sent
                } else {
                    Close::Writing(frame, length, written)
                };
                *moved = true;
                Poll::Ready(Ok(true))
            }
            Poll::Pending => Poll::Pending,
        };
    }
    if way.ended {
        return match Pin::new(&mut *to).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                way.shut = true;
                Poll::Ready(Ok(false))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(Stopped::Failed)),
            Poll::Pending => Poll::Pending,
        };
    }
    Poll::Ready(Ok(false))
}

/// A read with no block to read into, as a worker short of storage has none to give: into
/// a little room on the stack. A read that finds nothing — what most wakes of a quiet
/// tunnel come to — needs no block, and a worker short of storage does not end its quiet
/// tunnels merely for waking them; what did arrive needs one. Ready with whether anything
/// was carried.
#[cold]
#[inline(never)]
fn probe<F>(
    from: &mut F,
    way: &mut Way,
    blocks: &RefCell<Blocks>,
    cx: &mut Context<'_>,
) -> Poll<Result<bool, Stopped>>
where
    F: AsyncRead + Unpin,
{
    let mut probe = [MaybeUninit::<u8>::uninit(); PROBE];
    let mut read = ReadBuf::uninit(&mut probe);
    match Pin::new(&mut *from).poll_read(cx, &mut read) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Err(_)) => return Poll::Ready(Err(Stopped::Failed)),
        Poll::Ready(Ok(())) => {}
    }
    let arrived = read.filled();
    if arrived.is_empty() {
        way.ended = true;
        return Poll::Ready(Ok(false));
    }
    let Ok(mut block) = blocks.borrow_mut().take() else {
        return Poll::Ready(Err(Stopped::Exhausted));
    };
    let (kept, boundary) = follow(&mut way.frames, way.close, arrived);
    // A block has room for far more than the probe holds.
    if let Some(room) = block.room().get_mut(..kept) {
        room.copy_from_slice(&arrived[..kept]);
        block.arrived(kept);
    }
    way.block = Some(block);
    kept_in(way, boundary);
    Poll::Ready(Ok(true))
}

/// A WebSocket's read of `count` bytes, just made into the way's block, followed: as much of
/// it as goes on is kept (19 §6).
#[cold]
fn followed(way: &mut Way, count: usize) {
    let Some(block) = way.block.as_mut() else {
        return;
    };
    let (kept, boundary) = follow(&mut way.frames, way.close, &block.room()[..count]);
    block.arrived(kept);
    kept_in(way, boundary);
}

/// How much of `arrived`, just read, goes on: all of it, but for a WebSocket owed a Close
/// only as far as the frame boundary the Close goes at — that boundary said too — and after
/// its Close none. Whatever does not go on is still followed, for the Close that answers
/// the gateway's.
fn follow(frames: &mut Option<Frames>, close: Close, arrived: &[u8]) -> (usize, Option<usize>) {
    let count = arrived.len();
    match (frames, close) {
        (None, _) => (count, None),
        (Some(frames), Close::Sent) => {
            frames.read(arrived);
            (0, None)
        }
        (Some(frames), Close::Wanted) => match frames.until_boundary(arrived) {
            Some(at) => {
                frames.read(&arrived[at..]);
                (at, Some(at))
            }
            None => (count, None),
        },
        (Some(frames), Close::None | Close::Writing(..)) => {
            frames.read(arrived);
            (count, None)
        }
    }
}

/// Where what was kept of a read held the boundary a Close is owed at, has the Close written
/// right after it, as the loop comes round. A read that kept nothing is one owed or past a
/// Close, whose block goes back as the Close is dealt with.
fn kept_in(way: &mut Way, boundary: Option<usize>) {
    if boundary.is_some() {
        let (frame, length) = close_frame(way.to_backend);
        way.close = Close::Writing(frame, length, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use crate::upstream::h1::blocks::{SMALL, Sizes};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
    use tokio::time::Instant;

    const BOUNDS: Bounds = Bounds {
        idle: Duration::from_secs(60),
        drain_within: Duration::from_secs(60),
        websocket: false,
    };

    /// A test that waits for what never comes should fail, not hang.
    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("timed out")
    }

    /// The same for a test on a stopped clock that waits through a drain, whose bound is
    /// 25 seconds of it: a minute of the clock, which costs no time at all.
    async fn within_drain<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(60), future)
            .await
            .expect("timed out")
    }

    /// A worker's blocks, paid for within `limit`.
    fn blocks_within(limit: usize) -> RefCell<Blocks> {
        RefCell::new(Blocks::new(Sizes::default(), Storage::new(limit)))
    }

    /// Both ends of a tunnel's two connections: the tunnel's side of each, and the test's.
    fn ends() -> ((DuplexStream, DuplexStream), (DuplexStream, DuplexStream)) {
        let (client, client_side) = duplex(64 * 1024);
        let (backend, backend_side) = duplex(64 * 1024);
        ((client, backend), (client_side, backend_side))
    }

    async fn moved(from: &mut DuplexStream, to: &mut DuplexStream, bytes: &[u8]) {
        from.write_all(bytes).await.unwrap();
        let mut read = vec![0; bytes.len()];
        to.read_exact(&mut read).await.unwrap();
        assert_eq!(read, bytes);
    }

    /// A tunnel gives its blocks back once it has been quiet for a second (19 §5): two
    /// tunnels on a worker that can pay for one tunnel's blocks carry bytes both ways in
    /// turn, each quiet for longer than that while the other carries. One that kept its
    /// blocks while quiet would leave the other none.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_tunnel_holds_no_block() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(2 * SMALL);
        let ((mut one, mut one_backend), (mut one_client, mut one_far)) = ends();
        let ((mut two, mut two_backend), (mut two_client, mut two_far)) = ends();
        let quiet = RELEASE_AFTER + Duration::from_millis(100);
        let carried = within_drain(timers.driving(async {
            let first = carry(
                &mut one,
                &mut one_backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let second = carry(
                &mut two,
                &mut two_backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let talking = async {
                for round in 0..3_u8 {
                    let said = [round; 1000];
                    moved(&mut one_client, &mut one_far, &said).await;
                    moved(&mut one_far, &mut one_client, &said).await;
                    tokio::time::sleep(quiet).await;
                    moved(&mut two_far, &mut two_client, &said).await;
                    moved(&mut two_client, &mut two_far, &said).await;
                    tokio::time::sleep(quiet).await;
                }
                for end in [&mut one_client, &mut one_far, &mut two_client, &mut two_far] {
                    end.shutdown().await.unwrap();
                }
            };
            let ((), first, second) = tokio::join!(talking, first, second);
            (first, second)
        }))
        .await;
        assert_eq!(
            (carried.0.how, carried.1.how),
            (Carried::Closed, Carried::Closed)
        );
        // And every block is back.
        assert_eq!(blocks.borrow().parked(), 2);
    }

    /// A busy tunnel keeps a block each way between reads rather than give it back and
    /// take one again for every read; it gives them back a second after the last byte.
    #[tokio::test(start_paused = true)]
    async fn a_busy_tunnel_keeps_its_blocks_until_it_is_quiet() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        let carried = within_drain(timers.driving(async {
            let carrying = carry(
                &mut client,
                &mut backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let talking = async {
                moved(&mut client_side, &mut backend_side, b"ping").await;
                moved(&mut backend_side, &mut client_side, b"pong").await;
                assert_eq!(blocks.borrow().parked(), 0, "given back while busy");
                tokio::time::sleep(RELEASE_AFTER / 2).await;
                assert_eq!(blocks.borrow().parked(), 0, "given back too soon");
                tokio::time::sleep(RELEASE_AFTER).await;
                assert_eq!(blocks.borrow().parked(), 2, "kept while quiet");
                client_side.shutdown().await.unwrap();
                backend_side.shutdown().await.unwrap();
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried.0.how, Carried::Closed);
    }

    /// Bytes already read go first, each way, before anything more is read.
    #[tokio::test]
    async fn what_was_read_before_the_tunnel_goes_first() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let with = |bytes: &[u8]| {
            let mut block = blocks.borrow_mut().take().unwrap();
            block.room()[..bytes.len()].copy_from_slice(bytes);
            block.arrived(bytes.len());
            Some(block)
        };
        let (up, down) = (with(b"early"), with(b"hello"));
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        let carried = within(timers.driving(async {
            let carrying = carry(
                &mut client,
                &mut backend,
                up,
                down,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let talking = async {
                client_side.write_all(b"-late").await.unwrap();
                client_side.shutdown().await.unwrap();
                backend_side.shutdown().await.unwrap();
                let (mut up, mut down) = (Vec::new(), Vec::new());
                backend_side.read_to_end(&mut up).await.unwrap();
                client_side.read_to_end(&mut down).await.unwrap();
                (up, down)
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried.0.how, Carried::Closed);
        assert_eq!(carried.1, (b"early-late".to_vec(), b"hello".to_vec()));
    }

    /// A tunnel that has carried nothing holds no block, however often it is woken: a read
    /// that finds nothing gives back the block it took.
    #[tokio::test]
    async fn a_tunnel_that_has_carried_nothing_holds_no_block() {
        let timers = Timers::new();
        let drain = Drain::default();
        // Room for one block: none is left to lend if the tunnel kept the one it read into.
        let blocks = blocks_within(SMALL);
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        let carried = within(timers.driving(async {
            let carrying = carry(
                &mut client,
                &mut backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let watching = async {
                for _ in 0..3 {
                    tokio::task::yield_now().await;
                }
                let lent = blocks.borrow_mut().take();
                let free = lent.is_ok();
                if let Ok(block) = lent {
                    blocks.borrow_mut().give(block);
                }
                client_side.shutdown().await.unwrap();
                backend_side.shutdown().await.unwrap();
                free
            };
            tokio::join!(carrying, watching)
        }))
        .await;
        assert_eq!(carried.0.how, Carried::Closed);
        // What was taken to read into went back when the reads found nothing.
        assert!(
            carried.1,
            "a block was kept by a tunnel that carried nothing"
        );
    }

    /// A read that finds nothing needs no block: a worker with no storage left at all
    /// still carries a tunnel with nothing to carry to its end, and does not end it as
    /// exhausted for being woken.
    #[tokio::test]
    async fn a_tunnel_with_nothing_to_carry_needs_no_block() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(0);
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        let carried = within(timers.driving(async {
            let carrying = carry(
                &mut client,
                &mut backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let ending = async {
                tokio::task::yield_now().await;
                client_side.shutdown().await.unwrap();
                backend_side.shutdown().await.unwrap();
            };
            tokio::join!(carrying, ending).0
        }))
        .await;
        assert_eq!(carried.how, Carried::Closed);
    }

    /// A worker that cannot pay for a block to read into ends the tunnel, saying so, rather
    /// than reading into nothing, which would be taken for a close.
    #[tokio::test]
    async fn a_tunnel_the_worker_cannot_pay_for_ends_exhausted() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(0);
        let ((mut client, mut backend), (mut client_side, _backend_side)) = ends();
        client_side.write_all(b"hello").await.unwrap();
        let carried = within(timers.driving(carry(
            &mut client,
            &mut backend,
            None,
            None,
            &blocks,
            BOUNDS,
            &timers,
            [&drain, &drain],
        )))
        .await;
        assert_eq!(carried.how, Carried::Exhausted);
    }

    /// A way whose side has ended gives its block back as the end is passed on, while the
    /// other way goes on: a tunnel half closed for as long as its other side talks holds
    /// nothing for the half that is done.
    #[tokio::test(start_paused = true)]
    async fn a_way_that_has_ended_holds_no_block() {
        let timers = Timers::new();
        let drain = Drain::default();
        // One block's worth: the worker's only block, parked when no way holds it.
        let blocks = blocks_within(SMALL);
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        let carried = within_drain(timers.driving(async {
            let tunnel = carry(
                &mut client,
                &mut backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let talking = async {
                client_side.write_all(b"x").await.unwrap();
                client_side.shutdown().await.unwrap();
                assert_eq!(exactly(&mut backend_side, 1).await, b"x");
                let mut end = [0; 1];
                assert_eq!(backend_side.read(&mut end).await.unwrap(), 0);
                // Quiet past the release time, the backend's way has given its block back
                // too; the client's, which ended, never keeps one.
                tokio::time::sleep(RELEASE_AFTER + Duration::from_millis(100)).await;
                assert_eq!(blocks.borrow().parked(), 1, "a block is still held");
                backend_side.shutdown().await.unwrap();
            };
            tokio::join!(tunnel, talking).0
        }))
        .await;
        assert_eq!(carried.how, Carried::Closed);
    }

    /// A block handed over with no room left — its memory gone with a frame cut from it,
    /// as a client's head is — is traded for one with room before the way reads again,
    /// rather than read into and taken for the end of the client's bytes.
    #[tokio::test]
    async fn a_block_with_no_room_is_traded_for_one_with_room() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(crate::storage::LIMIT);
        let mut early = blocks.borrow_mut().take().unwrap();
        let size = early.room().len();
        early.room().fill(b'h');
        early.arrived(size);
        let _head = early.cut_frame(0..size, size);
        assert!(early.room().is_empty());
        let ((mut client, mut backend), (mut client_side, mut backend_side)) = ends();
        // Waiting when the tunnel starts, so that its first read finds them: a read into no
        // room at all is answered at once, as a socket's is, with nothing.
        client_side.write_all(b"more").await.unwrap();
        let carried = within(timers.driving(async {
            let tunnel = carry(
                &mut client,
                &mut backend,
                Some(early),
                None,
                &blocks,
                BOUNDS,
                &timers,
                [&drain, &drain],
            );
            let talking = async {
                assert_eq!(exactly(&mut backend_side, 4).await, b"more");
                for end in [&mut client_side, &mut backend_side] {
                    end.shutdown().await.unwrap();
                }
            };
            tokio::join!(tunnel, talking).0
        }))
        .await;
        assert_eq!(carried.how, Carried::Closed);
    }

    const WEBSOCKET: Bounds = Bounds {
        idle: Duration::from_secs(3600),
        drain_within: Duration::from_secs(25),
        websocket: true,
    };

    /// The tunnel, and then both of its connections closed, as a server drops them.
    async fn carried_and_closed(
        mut client: DuplexStream,
        mut backend: DuplexStream,
        blocks: &RefCell<Blocks>,
        timers: &Rc<Timers>,
        drains: [&Drain; 2],
    ) -> Carried {
        carry(
            &mut client,
            &mut backend,
            None,
            None,
            blocks,
            WEBSOCKET,
            timers,
            drains,
        )
        .await
        .how
    }

    /// A text frame as a client sends it, masked.
    fn from_client(text: &[u8]) -> Vec<u8> {
        let mask = [1, 2, 3, 4];
        let mut frame = vec![0x81, 0x80 | u8::try_from(text.len()).unwrap()];
        frame.extend_from_slice(&mask);
        frame.extend(
            text.iter()
                .zip(mask.iter().cycle())
                .map(|(byte, key)| byte ^ key),
        );
        frame
    }

    /// A text frame as a server sends it.
    fn from_server(text: &[u8]) -> Vec<u8> {
        let mut frame = vec![0x81, u8::try_from(text.len()).unwrap()];
        frame.extend_from_slice(text);
        frame
    }

    async fn exactly(from: &mut DuplexStream, count: usize) -> Vec<u8> {
        let mut read = vec![0; count];
        from.read_exact(&mut read).await.unwrap();
        read
    }

    /// The status code of a Close frame read from `from`, unmasked if it is masked, and
    /// whether it was.
    async fn close_read(from: &mut DuplexStream) -> (u16, bool) {
        let head = exactly(from, 2).await;
        assert_eq!(head[0], 0x88, "not a Close: {head:?}");
        let masked = head[1] & 0x80 != 0;
        assert_eq!(head[1] & 0x7f, 2, "{head:?}");
        let mask = if masked {
            exactly(from, 4).await
        } else {
            vec![0; 4]
        };
        let code = exactly(from, 2).await;
        (
            u16::from_be_bytes([code[0] ^ mask[0], code[1] ^ mask[1]]),
            masked,
        )
    }

    /// A draining worker closes a WebSocket as RFC 6455 does: each side is sent a Close
    /// 1001 — the backend's masked — at a moment in the drain bound less five seconds, the
    /// frames under way before it going first; each answers with its own, which goes no
    /// further; and the tunnel ends then, well inside the bound (19 §6).
    #[tokio::test(start_paused = true)]
    async fn a_draining_websocket_is_closed_with_going_away_both_ways() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let ((client, backend), (mut client_side, mut backend_side)) = ends();
        let began = Instant::now();
        let (carried, ended) = within_drain(timers.driving(async {
            let carrying = carried_and_closed(client, backend, &blocks, &timers, [&drain, &drain]);
            let talking = async {
                moved(&mut client_side, &mut backend_side, &from_client(b"hi")).await;
                moved(&mut backend_side, &mut client_side, &from_server(b"hello")).await;
                drain.start();
                assert_eq!(close_read(&mut client_side).await, (1001, false));
                assert_eq!(close_read(&mut backend_side).await, (1001, true));
                // Each answers; neither answer is passed on.
                client_side
                    .write_all(&going_away_masked([7, 7, 7, 7]))
                    .await
                    .unwrap();
                backend_side.write_all(&GOING_AWAY).await.unwrap();
                let (mut to_client, mut to_backend) = (Vec::new(), Vec::new());
                client_side.read_to_end(&mut to_client).await.unwrap();
                backend_side.read_to_end(&mut to_backend).await.unwrap();
                assert_eq!(to_client, b"", "the backend's answer went on");
                assert_eq!(to_backend, b"", "the client's answer went on");
                began.elapsed()
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried, Carried::Drained);
        assert!(ended < Duration::from_secs(21), "{ended:?}");
    }

    /// Either drain a tunnel hears closes it the same way: its route's, a reload having
    /// taken the route away, while its client's connection drains not (03 §10).
    #[tokio::test(start_paused = true)]
    async fn a_websocket_whose_route_drains_is_closed_with_going_away() {
        let timers = Timers::new();
        let (connection, route) = (Drain::default(), Drain::default());
        let blocks = blocks_within(4 * SMALL);
        let ((client, backend), (mut client_side, mut backend_side)) = ends();
        let (carried, ()) = within_drain(timers.driving(async {
            let carrying =
                carried_and_closed(client, backend, &blocks, &timers, [&connection, &route]);
            let talking = async {
                moved(&mut client_side, &mut backend_side, &from_client(b"hi")).await;
                route.start();
                assert_eq!(close_read(&mut client_side).await, (1001, false));
                assert_eq!(close_read(&mut backend_side).await, (1001, true));
                client_side
                    .write_all(&going_away_masked([7, 7, 7, 7]))
                    .await
                    .unwrap();
                backend_side.write_all(&GOING_AWAY).await.unwrap();
                let mut to_client = Vec::new();
                client_side.read_to_end(&mut to_client).await.unwrap();
                assert_eq!(to_client, b"");
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried, Carried::Drained);
        assert!(!connection.is_on());
    }

    /// A Close goes only where a frame ends: one the backend is part way through sending
    /// when the moment comes is finished first, and what comes after it goes nowhere.
    #[tokio::test(start_paused = true)]
    async fn a_close_waits_for_the_frame_under_way() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let ((client, backend), (mut client_side, mut backend_side)) = ends();
        let (carried, ()) = within_drain(timers.driving(async {
            let carrying = carried_and_closed(client, backend, &blocks, &timers, [&drain, &drain]);
            let talking = async {
                let frame = from_server(b"a long message");
                moved(&mut backend_side, &mut client_side, &frame[..6]).await;
                drain.start();
                // Past every moment the drain can draw: the Close is owed, and waits.
                tokio::time::sleep(Duration::from_secs(21)).await;
                let (_, masked) = close_read(&mut backend_side).await;
                assert!(masked);
                let mut rest = frame[6..].to_vec();
                rest.extend(from_server(b"never sent"));
                backend_side.write_all(&rest).await.unwrap();
                assert_eq!(
                    exactly(&mut client_side, frame.len() - 6).await,
                    &frame[6..]
                );
                assert_eq!(close_read(&mut client_side).await, (1001, false));
                client_side
                    .write_all(&going_away_masked([1, 1, 1, 1]))
                    .await
                    .unwrap();
                backend_side.write_all(&GOING_AWAY).await.unwrap();
                let mut to_client = Vec::new();
                client_side.read_to_end(&mut to_client).await.unwrap();
                assert_eq!(to_client, b"", "a frame after the Close went on");
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried, Carried::Drained);
    }

    /// A WebSocket one side is already closing is sent no second Close: it is left to
    /// finish, and closed at the drain bound if it has not.
    #[tokio::test(start_paused = true)]
    async fn a_websocket_already_closing_is_left_to_finish() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let ((client, backend), (mut client_side, mut backend_side)) = ends();
        let began = Instant::now();
        let (carried, ()) = within_drain(timers.driving(async {
            let carrying = carried_and_closed(client, backend, &blocks, &timers, [&drain, &drain]);
            let talking = async {
                let close = going_away_masked([3, 3, 3, 3]);
                moved(&mut client_side, &mut backend_side, &close).await;
                drain.start();
                let mut to_backend = Vec::new();
                backend_side.read_to_end(&mut to_backend).await.unwrap();
                assert_eq!(to_backend, b"", "a second Close");
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried, Carried::Drained);
        assert!(
            began.elapsed() >= Duration::from_secs(25),
            "{:?}",
            began.elapsed()
        );
    }

    /// A stream whose frames could not be followed is closed bare at its moment: no Close
    /// frame goes into what might be the middle of one.
    #[tokio::test(start_paused = true)]
    async fn a_websocket_that_cannot_be_followed_is_closed_bare() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(4 * SMALL);
        let ((client, backend), (mut client_side, mut backend_side)) = ends();
        let mut unfollowable = vec![0x82, 127];
        unfollowable.extend_from_slice(&(1_u64 << 63).to_be_bytes());
        let began = Instant::now();
        let (carried, ()) = within_drain(timers.driving(async {
            let carrying = carried_and_closed(client, backend, &blocks, &timers, [&drain, &drain]);
            let talking = async {
                moved(&mut backend_side, &mut client_side, &unfollowable).await;
                drain.start();
                let mut to_client = Vec::new();
                client_side.read_to_end(&mut to_client).await.unwrap();
                assert_eq!(to_client, b"", "a Close went into the stream");
            };
            tokio::join!(carrying, talking)
        }))
        .await;
        assert_eq!(carried, Carried::Drained);
        // At its moment, not left for the bound.
        assert!(
            began.elapsed() < Duration::from_secs(21),
            "{:?}",
            began.elapsed()
        );
    }

    /// A drain's closes are spread over all but its last five seconds.
    #[test]
    fn moments_are_spread_over_the_drain() {
        let moments: Vec<Duration> = (0..1000).map(|_| spread(Duration::from_secs(25))).collect();
        assert!(
            moments
                .iter()
                .all(|moment| *moment < Duration::from_secs(20))
        );
        assert!(
            moments
                .iter()
                .any(|moment| *moment < Duration::from_secs(5))
        );
        assert!(
            moments
                .iter()
                .any(|moment| *moment > Duration::from_secs(15))
        );
        assert_eq!(spread(Duration::from_secs(5)), Duration::ZERO);
    }
}
