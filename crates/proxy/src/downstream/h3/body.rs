//! A request's body as quiche received it, read as the core asks for it
//! ([16 §4, §5](../../../../../docs/16-http3.md)).
//!
//! A piece is read out of quiche only when the next frame is asked for, and reading is what
//! gives the client its flow-control credit back, so an upload that nobody reads holds at
//! most its stream window. Data and trailers stay separate frames; a request whose HEADERS
//! ended the stream has ended before it is read.
//!
//! The body is held to the length its `Content-Length` declares: one that brings more, or
//! ends with less, is malformed (RFC 9114 §4.1.2) and fails here, before the difference can
//! reach an upstream that reads lengths. So do malformed trailers. A malformed body has its
//! stream reset with `H3_MESSAGE_ERROR`, as a malformed head has.

use crate::downstream::h2::idle::Idle;
use crate::downstream::h3::code;
use crate::downstream::h3::conn::{Conn, Slot, State};
use crate::downstream::h3::head::Refused;
use crate::interim::Interim;
use crate::request_body::RequestBodyError;
use bytes::{Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

/// The most read out of quiche at once: what a DATA frame of the usual size carries.
const PIECE: usize = 16 * 1024;

/// What went wrong with a stream, as a cause of a [`RequestBodyError`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamError {
    /// The client reset the stream.
    #[error("the client reset the stream with {0:#x}")]
    Reset(u64),
    /// The connection ended under the stream.
    #[error("the connection closed")]
    Closed,
    /// The body did not come to the length its `Content-Length` declared.
    #[error("the body's length is not the one declared")]
    Length,
    /// The trailers were malformed or too large.
    #[error("trailers refused: {0:?}")]
    Trailers(Refused),
    /// quiche refused.
    #[error("quiche: {0}")]
    H3(#[from] quiche::h3::Error),
}

/// A request body quiche is receiving.
pub(crate) struct IncomingH3 {
    conn: Rc<Conn>,
    stream: u64,
    /// The length `Content-Length` declared, if it declared one.
    declared: Option<u64>,
    received: u64,
    /// Every byte of data has been handed over; what is left is trailers, if any.
    data_done: bool,
    /// Everything has been handed over.
    ended: bool,
    idle: Idle,
    interim: Option<Interim>,
}

impl std::fmt::Debug for IncomingH3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingH3")
            .field("stream", &self.stream)
            .field("received", &self.received)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl IncomingH3 {
    /// The body of stream `stream` of `conn`, whose head declared `declared` and ended the
    /// stream if `ended`; its reader waits `idle` at the most with nothing coming.
    pub(crate) fn new(
        conn: Rc<Conn>,
        stream: u64,
        declared: Option<u64>,
        ended: bool,
        idle: Duration,
    ) -> Self {
        Self {
            conn,
            stream,
            declared,
            received: 0,
            data_done: ended,
            ended,
            idle: Idle::new(idle),
            interim: None,
        }
    }

    /// The same, telling `interim` what the continue decision needs to know.
    #[must_use]
    pub(crate) fn heard_by(mut self, interim: Interim) -> Self {
        self.interim = Some(interim);
        self
    }

    /// What the stream has, read with the connection in hand.
    fn read(
        &mut self,
        state: &mut State,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let State {
            quic,
            h3,
            streams,
            closed,
            ..
        } = state;
        let Some(slot) = streams.get_mut(&self.stream) else {
            return Poll::Ready(Some(Err(other(StreamError::Closed))));
        };
        if let Some(code) = slot.reset {
            return Poll::Ready(Some(Err(other(StreamError::Reset(code)))));
        }
        if !self.data_done {
            let Some(h3) = h3.as_mut() else {
                return Poll::Ready(Some(Err(other(StreamError::Closed))));
            };
            let mut piece = BytesMut::with_capacity(PIECE);
            match h3.recv_body_buf(quic, self.stream, &mut piece) {
                Ok(read) if read > 0 => {
                    self.received += read as u64;
                    if self
                        .declared
                        .is_some_and(|declared| self.received > declared)
                    {
                        return Poll::Ready(Some(Err(self.malformed(quic, StreamError::Length))));
                    }
                    self.idle.moved();
                    if let Some(interim) = &self.interim {
                        interim.client_sent_body();
                    }
                    // Credit may be owed to the client now.
                    self.conn.stir();
                    return Poll::Ready(Some(Ok(Frame::data(piece.freeze()))));
                }
                Ok(_) | Err(quiche::h3::Error::Done) => {}
                Err(error) => return Poll::Ready(Some(Err(other(StreamError::H3(error))))),
            }
            // Trailers and the stream's end are handed on only once the data is read.
            if slot.trailers.is_none() && !slot.finished {
                return self.wait(slot, *closed, cx, true);
            }
            self.data_done = true;
        }
        if self
            .declared
            .is_some_and(|declared| declared != self.received)
        {
            return Poll::Ready(Some(Err(self.malformed(quic, StreamError::Length))));
        }
        match slot.trailers.take() {
            Some(Ok(trailers)) => return Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Some(Err(refused @ Refused::Malformed(_))) => {
                return Poll::Ready(Some(Err(
                    self.malformed(quic, StreamError::Trailers(refused))
                )));
            }
            Some(Err(refused)) => {
                return Poll::Ready(Some(Err(invalid(StreamError::Trailers(refused)))));
            }
            None => {}
        }
        if slot.finished {
            self.ended = true;
            return Poll::Ready(None);
        }
        self.wait(slot, *closed, cx, false)
    }

    /// Refuses a malformed body (RFC 9114 §4.1.2): the stream is reset both ways with
    /// `H3_MESSAGE_ERROR`, as for a malformed head, and the read fails.
    fn malformed(&self, quic: &mut quiche::Connection, error: StreamError) -> RequestBodyError {
        // Fails only for a side already done.
        let _reset =
            quic.stream_shutdown(self.stream, quiche::Shutdown::Write, code::MESSAGE_ERROR);
        let _stopped =
            quic.stream_shutdown(self.stream, quiche::Shutdown::Read, code::MESSAGE_ERROR);
        self.conn.stir();
        invalid(error)
    }

    /// Waits for the driver to hand on more, or for the idle bound.
    fn wait(
        &mut self,
        slot: &mut Slot,
        closed: bool,
        cx: &mut Context<'_>,
        wanting_data: bool,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        if closed {
            return Poll::Ready(Some(Err(RequestBodyError::Incomplete(Box::new(
                StreamError::Closed,
            )))));
        }
        slot.reader = Some(cx.waker().clone());
        if wanting_data && let Some(interim) = &self.interim {
            interim.body_wanted();
        }
        // The driver looks again: what quiche holds past the data it has let us read.
        self.conn.stir();
        match self.idle.waiting(cx) {
            Poll::Ready(()) => Poll::Ready(Some(Err(RequestBodyError::TimedOut))),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn other(error: StreamError) -> RequestBodyError {
    RequestBodyError::Other(Box::new(error))
}

fn invalid(error: StreamError) -> RequestBodyError {
    RequestBodyError::Invalid(Box::new(error))
}

impl Body for IncomingH3 {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let conn = Rc::clone(&this.conn);
        conn.with(|state| this.read(state, cx))
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }

    fn size_hint(&self) -> SizeHint {
        // A length is never promised here: it is the head's to say, and trailers may follow.
        if self.ended {
            SizeHint::with_exact(0)
        } else {
            SizeHint::default()
        }
    }
}
