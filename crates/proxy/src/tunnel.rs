//! Two connections carried to each other byte for byte, both ways at once: an L4
//! listener's connection and its backend ([17 §4](../../../docs/17-tcp-and-tls-passthrough.md)),
//! or a WebSocket's client and backend once the upgrade is made
//! ([19 §5](../../../docs/19-websocket.md)).
//!
//! Each way reads into a block of the worker's and writes what it read on before it reads
//! again, so a side that stops taking bytes stops the other from sending more than a block
//! ahead. **A way holds a block only while bytes are on their way**: it takes one to read
//! into, and gives it back as soon as everything in it has been written on, or as soon as
//! a read finds nothing. An idle tunnel holds no buffer at all, which is what lets a worker
//! hold as many quiet WebSockets as it has connections for. The blocks come from the
//! worker's free list, so taking and giving one is a push and a pop.
//!
//! A side's end is passed on as a half-close (`shutdown(Write)`), and the other way goes on
//! until it ends too: a protocol that says "that is all I have" and then waits for the
//! answer is carried as it is. The tunnel is closed once both ways have ended, when either
//! side fails, when it has carried nothing for its idle bound, when the worker drains and
//! its bound is up, or when the worker has no storage for a block to read into.

use crate::drain::Drain;
use crate::timers::{Alarm, Timers};
use crate::upstream::h1::blocks::{Block, Blocks};
use crate::upstream::secure::Socket;
use std::cell::RefCell;
use std::future::poll_fn;
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
    /// The worker drained, and the drain's bound came.
    Drained,
    /// A side failed.
    Failed,
    /// The worker could not pay for a block to read into.
    Exhausted,
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

/// What a tunnel is held to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    /// How long it may carry nothing either way.
    pub(crate) idle: Duration,
    /// How long it may go on once the worker drains.
    pub(crate) drain_within: Duration,
}

/// A WebSocket's backend, once its upgrade is made (19 §2): the connection its 101 came on,
/// what was read past the 101, and what the tunnel is held to. The request core hands it to
/// the server that wrote the 101, which carries it to its client.
#[derive(Debug)]
pub(crate) struct Switched {
    /// The backend's connection.
    pub(crate) backend: Socket,
    /// What the backend sent after its 101, which goes to the client first.
    pub(crate) leftover: Option<Block>,
    /// The rule's idle bound, and the worker's drain bound.
    pub(crate) bounds: Bounds,
}

/// One way through the tunnel.
struct Way {
    /// What has been read and not yet written on; none while nothing is on its way.
    block: Option<Block>,
    /// The side it reads from has ended.
    ended: bool,
    /// And that end has been passed on.
    shut: bool,
}

impl Way {
    fn new(block: Option<Block>) -> Self {
        Self {
            block,
            ended: false,
            shut: false,
        }
    }
}

/// Why a way stopped short of its end.
enum Stopped {
    /// A side failed.
    Failed,
    /// No block could be had to read into.
    Exhausted,
}

/// Carries `client` to `backend` and back until the tunnel ends. `up` may already hold
/// bytes the client sent and `down` bytes the backend sent, which go first. Every block is
/// back with `blocks` when it returns.
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
    drain: &Drain,
) -> Carried
where
    C: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut up = Way::new(up);
    let mut down = Way::new(down);
    let mut alarm = Alarm::new(timers, None);
    let mut heard = pin!(drain.notified());
    let mut last = Instant::now();
    let mut drain_by = None;
    let carried = poll_fn(|cx| {
        let mut moved = false;
        for (way, from_client) in [(&mut up, true), (&mut down, false)] {
            if way.shut {
                continue;
            }
            let pumped = if from_client {
                pump(client, backend, way, blocks, &mut moved, cx)
            } else {
                pump(backend, client, way, blocks, &mut moved, cx)
            };
            match pumped {
                Poll::Ready(Err(Stopped::Failed)) => return Poll::Ready(Carried::Failed),
                Poll::Ready(Err(Stopped::Exhausted)) => return Poll::Ready(Carried::Exhausted),
                Poll::Ready(Ok(())) | Poll::Pending => {}
            }
        }
        if up.shut && down.shut {
            return Poll::Ready(Carried::Closed);
        }
        if moved {
            last = Instant::now();
        }
        if drain_by.is_none() && drain.poll_on(heard.as_mut(), cx).is_ready() {
            drain_by = Some(Instant::now() + bounds.drain_within);
        }
        let idle_by = last + bounds.idle;
        let due = drain_by.map_or(idle_by, |drain_by: Instant| drain_by.min(idle_by));
        if alarm.poll_until(cx, due).is_ready() {
            let drained = drain_by.is_some_and(|drain_by| drain_by <= idle_by);
            return Poll::Ready(if drained {
                Carried::Drained
            } else {
                Carried::Idle
            });
        }
        Poll::Pending
    })
    .await;
    let mut blocks = blocks.borrow_mut();
    for block in [up.block, down.block].into_iter().flatten() {
        blocks.give(block);
    }
    carried
}

/// Moves what `from` sends on to `to`, as far as both allow now. Ready once `from` has
/// ended and `to` has been told; `moved` is set if any byte went.
fn pump<F, T>(
    from: &mut F,
    to: &mut T,
    way: &mut Way,
    blocks: &RefCell<Blocks>,
    moved: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<Result<(), Stopped>>
where
    F: AsyncRead + Unpin,
    T: AsyncWrite + Unpin,
{
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
                        *moved = true;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
        let spare = way.block.take();
        if way.ended {
            if let Some(block) = spare {
                blocks.borrow_mut().give(block);
            }
            return match Pin::new(&mut *to).poll_shutdown(cx) {
                Poll::Ready(Ok(())) => {
                    way.shut = true;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(_)) => Poll::Ready(Err(Stopped::Failed)),
                Poll::Pending => Poll::Pending,
            };
        }
        // One whose memory went with a frame cut from it — a client's head — has no room
        // left, and is traded for one that has.
        let lent = match spare {
            Some(mut block) => {
                if block.room().is_empty() {
                    let mut blocks = blocks.borrow_mut();
                    blocks.give(block);
                    blocks.take()
                } else {
                    Ok(block)
                }
            }
            None => blocks.borrow_mut().take(),
        };
        let Ok(mut block) = lent else {
            return Poll::Ready(Err(Stopped::Exhausted));
        };
        let mut read = ReadBuf::new(block.room());
        let polled = Pin::new(&mut *from).poll_read(cx, &mut read);
        let count = read.filled().len();
        match polled {
            Poll::Ready(Ok(())) if count > 0 => {
                block.arrived(count);
                way.block = Some(block);
                *moved = true;
            }
            Poll::Ready(Ok(())) => {
                blocks.borrow_mut().give(block);
                way.ended = true;
            }
            Poll::Ready(Err(_)) => {
                blocks.borrow_mut().give(block);
                return Poll::Ready(Err(Stopped::Failed));
            }
            Poll::Pending => {
                blocks.borrow_mut().give(block);
                return Poll::Pending;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use crate::upstream::h1::blocks::{SMALL, Sizes};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

    const BOUNDS: Bounds = Bounds {
        idle: Duration::from_secs(60),
        drain_within: Duration::from_secs(60),
    };

    /// A test that waits for what never comes should fail, not hang.
    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future)
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

    /// A tunnel holds a block only while bytes are on their way (19 §5): two tunnels on a
    /// worker that can pay for one block between them carry bytes each way in turn, each
    /// quiet while the other carries. One that kept a block while quiet would leave the
    /// other none.
    #[tokio::test]
    async fn a_quiet_tunnel_holds_no_block() {
        let timers = Timers::new();
        let drain = Drain::default();
        let blocks = blocks_within(SMALL);
        let ((mut one, mut one_backend), (mut one_client, mut one_far)) = ends();
        let ((mut two, mut two_backend), (mut two_client, mut two_far)) = ends();
        let carried = within(timers.driving(async {
            let first = carry(
                &mut one,
                &mut one_backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                &drain,
            );
            let second = carry(
                &mut two,
                &mut two_backend,
                None,
                None,
                &blocks,
                BOUNDS,
                &timers,
                &drain,
            );
            let talking = async {
                for round in 0..3_u8 {
                    let said = [round; 1000];
                    moved(&mut one_client, &mut one_far, &said).await;
                    moved(&mut two_far, &mut two_client, &said).await;
                    moved(&mut one_far, &mut one_client, &said).await;
                    moved(&mut two_client, &mut two_far, &said).await;
                }
                for end in [&mut one_client, &mut one_far, &mut two_client, &mut two_far] {
                    end.shutdown().await.unwrap();
                }
            };
            let ((), first, second) = tokio::join!(talking, first, second);
            (first, second)
        }))
        .await;
        assert_eq!(carried, (Carried::Closed, Carried::Closed));
        // And every block is back.
        assert_eq!(blocks.borrow().parked(), 1);
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
                &drain,
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
        assert_eq!(carried.0, Carried::Closed);
        assert_eq!(carried.1, (b"early-late".to_vec(), b"hello".to_vec()));
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
            &drain,
        )))
        .await;
        assert_eq!(carried, Carried::Exhausted);
    }
}
