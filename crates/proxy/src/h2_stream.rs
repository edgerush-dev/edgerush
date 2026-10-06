//! An HTTP/2 stream read and written as bytes, for a tunnel to carry
//! ([19 §3, §4](../../../docs/19-websocket.md)): a WebSocket's stream once its extended
//! CONNECT has been answered, the client's to our server or ours to an HTTP/2 backend
//! (RFC 8441 §5: "the HTTP/2 stream ... as if it were the TCP connection").
//!
//! Reading takes the stream's DATA frames in turn and gives each frame's flow-control
//! credit back when the next is asked for, as a request body's reader does, so what the
//! stream holds in h2 and in hand stays within its window. Writing hands h2 no more than it
//! has granted room for, a frame at a time, each piece paid for from the worker's storage
//! until h2 lets it go, as an answer's writer does. The stream's end is a half-close each
//! way: END_STREAM received reads as the end of the bytes, and shutting the writing half
//! sends it. A stream dropped before its end is reset by h2 (CANCEL), which is how a
//! tunnel that fails tells the far side; one the gateway ends idle or drained has its
//! writing half ended first.

use crate::downstream::h2::writer::Outgoing;
use crate::storage::Storage;
use crate::upstream::h2::client::Place;
use bytes::{Buf, Bytes};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The most handed to h2 at once: one frame of the size every peer accepts (RFC 9113
/// §4.2), as for an answer.
const PIECE: usize = 16_384;

/// One HTTP/2 stream, both ways.
pub(crate) struct H2Stream {
    send: ::h2::SendStream<Outgoing>,
    recv: ::h2::RecvStream,
    /// What is left of the DATA frame last received.
    reading: Bytes,
    /// Credit for the frame last received, given back when the next is asked for.
    owed: usize,
    storage: Rc<Storage>,
    /// The writing half has been ended.
    finished: bool,
    /// For a stream to a backend, its place on the connection, held for the stream's life.
    _place: Option<Place>,
}

impl std::fmt::Debug for H2Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H2Stream")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl H2Stream {
    /// The stream whose halves are `send` and `recv`, its writes paid for from `storage`;
    /// `place` for one to a backend.
    pub(crate) fn new(
        send: ::h2::SendStream<Outgoing>,
        recv: ::h2::RecvStream,
        storage: Rc<Storage>,
        place: Option<Place>,
    ) -> Self {
        Self {
            send,
            recv,
            reading: Bytes::new(),
            owed: 0,
            storage,
            finished: false,
            _place: place,
        }
    }
}

fn failed(error: ::h2::Error) -> io::Error {
    io::Error::other(error)
}

impl AsyncRead for H2Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // An empty DATA frame carries nothing, and must not read as the end.
        while this.reading.is_empty() {
            if this.owed > 0 {
                // Fails only for a stream h2 has already let go of, whose credit is moot.
                let _gone = this
                    .recv
                    .flow_control()
                    .release_capacity(std::mem::take(&mut this.owed));
            }
            match this.recv.poll_data(cx) {
                Poll::Ready(Some(Ok(data))) => {
                    this.owed = data.len();
                    this.reading = data;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(failed(error))),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
        let count = this.reading.len().min(buf.remaining());
        buf.put_slice(&this.reading[..count]);
        this.reading.advance(count);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for H2Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            let granted = this.send.capacity();
            if granted > 0 {
                let count = granted.min(data.len()).min(PIECE);
                let charge = this
                    .storage
                    .reserve(count)
                    .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
                let piece = Outgoing::charged(Bytes::copy_from_slice(&data[..count]), charge);
                this.send.send_data(piece, false).map_err(failed)?;
                return Poll::Ready(Ok(count));
            }
            this.send.reserve_capacity(data.len().min(PIECE));
            match this.send.poll_capacity(cx) {
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(failed(error))),
                Poll::Ready(None) => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // h2 writes what it holds as the connection's task runs; nothing waits here.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.finished {
            this.finished = true;
            this.send
                .send_data(Outgoing::empty(), true)
                .map_err(failed)?;
        }
        Poll::Ready(Ok(()))
    }
}
