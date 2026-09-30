//! An HTTP/3 request stream read and written as bytes, for a tunnel to carry: a WebSocket's
//! once its extended CONNECT has been answered (RFC 9220, [19 §3](../../../../../docs/19-websocket.md)).
//!
//! Reading goes through the stream's request body, which takes pieces out of quiche only as
//! they are asked for — which is what gives the client its credit back — and its trailers,
//! if any, are read and dropped. Writing goes through the stream's responder, as much as
//! quiche takes at a time. The client's FIN reads as the end of the bytes, and shutting the
//! writing half sends ours. A stream let go of before both ends is reset with
//! `H3_REQUEST_CANCELLED` as it goes, as RFC 9220 has an aborted WebSocket end.

use crate::downstream::h3::body::IncomingH3;
use crate::downstream::h3::writer::Responder;
use bytes::{Buf, Bytes};
use http_body::Body;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// One HTTP/3 request stream, both ways.
pub(crate) struct H3Stream {
    incoming: IncomingH3,
    responder: Responder,
    /// What is left of the piece last read.
    reading: Bytes,
    /// Our side has been ended.
    finished: bool,
}

impl H3Stream {
    /// The stream `incoming` reads and `responder` writes, its final head already sent.
    pub(crate) fn new(mut incoming: IncomingH3, responder: Responder) -> Self {
        incoming.drop_trailers();
        Self {
            incoming,
            responder,
            reading: Bytes::new(),
            finished: false,
        }
    }
}

impl AsyncRead for H3Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.reading.is_empty() {
            match Pin::new(&mut this.incoming).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        this.reading = data;
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(io::Error::other(error))),
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

impl AsyncWrite for H3Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.get_mut()
            .responder
            .poll_data(cx, data, false)
            .map_err(io::Error::other)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The driver sends what quiche holds as it turns; nothing waits here.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(Ok(()));
        }
        match this.responder.poll_data(cx, &[], true) {
            Poll::Ready(Ok(_)) => {
                this.finished = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(io::Error::other(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}
