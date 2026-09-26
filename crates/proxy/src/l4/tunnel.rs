//! A connection carried to its backend byte for byte, both ways at once
//! ([17 §4](../../../../docs/17-tcp-and-tls-passthrough.md)).
//!
//! Each way reads into a block of the worker's and writes what it read on before it reads
//! again, so a side that stops taking bytes stops the other from sending more than a block
//! ahead. A side's end is passed on as a half-close (`shutdown(Write)`), and the other way
//! goes on until it ends too: a protocol that says "that is all I have" and then waits for
//! the answer is carried as it is. The tunnel is closed once both ways have ended, when
//! either side fails, when it has carried nothing for its idle bound, or when the worker
//! drains and its bound is up.

use crate::drain::Drain;
use crate::timers::{Alarm, Timers};
use crate::upstream::h1::blocks::Block;
use std::future::poll_fn;
use std::io;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
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
}

/// What a tunnel is held to.
#[derive(Clone, Copy)]
pub(crate) struct Bounds {
    /// How long it may carry nothing either way.
    pub(crate) idle: Duration,
    /// How long it may go on once the worker drains.
    pub(crate) drain_within: Duration,
}

/// One way through the tunnel.
struct Way<'a> {
    block: &'a mut Block,
    /// The side it reads from has ended.
    ended: bool,
    /// And that end has been passed on.
    shut: bool,
}

impl Way<'_> {
    fn done(&self) -> bool {
        self.shut
    }
}

/// Carries `client` to `backend` and back until the tunnel ends. `up` may already hold
/// bytes the client sent, which go first; `down` is empty.
pub(crate) async fn carry(
    client: &mut TcpStream,
    backend: &mut TcpStream,
    up: &mut Block,
    down: &mut Block,
    bounds: Bounds,
    timers: &Rc<Timers>,
    drain: &Drain,
) -> Carried {
    let mut up = Way {
        block: up,
        ended: false,
        shut: false,
    };
    let mut down = Way {
        block: down,
        ended: false,
        shut: false,
    };
    let mut alarm = Alarm::new(timers, None);
    let mut heard = pin!(drain.notified());
    let mut last = Instant::now();
    let mut drain_by = None;
    poll_fn(|cx| {
        let mut moved = false;
        if !up.done() {
            match pump(client, backend, &mut up, &mut moved, cx) {
                Poll::Ready(Err(_)) => return Poll::Ready(Carried::Failed),
                Poll::Ready(Ok(())) | Poll::Pending => {}
            }
        }
        if !down.done() {
            match pump(backend, client, &mut down, &mut moved, cx) {
                Poll::Ready(Err(_)) => return Poll::Ready(Carried::Failed),
                Poll::Ready(Ok(())) | Poll::Pending => {}
            }
        }
        if up.done() && down.done() {
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
    .await
}

/// Moves what `from` sends on to `to`, as far as both allow now. Ready once `from` has
/// ended and `to` has been told; `moved` is set if any byte went.
fn pump(
    from: &mut TcpStream,
    to: &mut TcpStream,
    way: &mut Way<'_>,
    moved: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    loop {
        // What has been read goes on before anything more is read.
        while !way.block.is_empty() {
            match Pin::new(&mut *to).poll_write(cx, way.block.data()) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(written)) => {
                    way.block.consume(written);
                    *moved = true;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        if way.ended {
            if !way.shut {
                match Pin::new(&mut *to).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => way.shut = true,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            return Poll::Ready(Ok(()));
        }
        let mut read = ReadBuf::new(way.block.room());
        match Pin::new(&mut *from).poll_read(cx, &mut read) {
            Poll::Ready(Ok(())) => {
                let count = read.filled().len();
                if count == 0 {
                    way.ended = true;
                } else {
                    way.block.arrived(count);
                    *moved = true;
                }
            }
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
    }
}
