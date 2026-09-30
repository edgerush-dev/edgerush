//! An answer sent on an HTTP/3 stream, as quiche can take it
//! ([16 §4, §5](../../../../../docs/16-http3.md)).
//!
//! quiche takes an answer's bytes only as far as the stream's flow control and the
//! connection's send capacity allow, and reports how much it took; what it did not take is
//! offered again once the driver has heard that the stream has room. So what quiche holds of
//! an answer is bounded by what the client grants, and nothing waits outside it.
//!
//! Interim heads go before the final head only, as for HTTP/2; quiche would send one after
//! it. A stream the gateway gives up is reset with `H3_REQUEST_CANCELLED`, one whose answer
//! failed part way with `H3_INTERNAL_ERROR`, so that a cut answer is never taken for a whole
//! one.

use crate::downstream::h2::idle::Idle;
use crate::downstream::h3::code;
use crate::downstream::h3::conn::{Conn, State};
use crate::downstream::h3::head;
use crate::request_body::RequestBodyError;
use bytes::Bytes;
use http::{HeaderMap, Response, StatusCode};
use http_body::Body;
use std::error::Error as StdError;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

/// Why an answer could not be sent whole.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SendError {
    /// The client asked the stream to stop, or reset it.
    #[error("the client stopped the stream: {0:#x}")]
    Reset(u64),
    /// The connection ended under the stream.
    #[error("the connection closed before the answer was sent")]
    Closed,
    /// quiche refused what it was given.
    #[error("quiche refused the answer: {0}")]
    H3(quiche::h3::Error),
    /// The answer's own body failed part way.
    #[error("the answer's body failed")]
    Body(#[source] Box<dyn StdError + Send + Sync>),
    /// An interim head after the final head, or a second final head.
    #[error("the final head has already been sent")]
    AfterFinal,
    /// The client gave no room for the answer for longer than its idle bound.
    #[error("the client stopped taking the answer")]
    TimedOut,
}

impl From<quiche::h3::Error> for SendError {
    fn from(error: quiche::h3::Error) -> Self {
        match error {
            quiche::h3::Error::TransportError(quiche::Error::StreamStopped(code)) => {
                Self::Reset(code)
            }
            error => Self::H3(error),
        }
    }
}

/// What a write came to: done, or waiting for room.
enum Wrote<T> {
    Done(T),
    Blocked,
}

/// A stream's heads, then its body.
pub(crate) struct Responder {
    conn: Rc<Conn>,
    stream: u64,
    /// A head has gone, so the next goes as an additional one.
    headed: bool,
    final_sent: bool,
    idle: Idle,
}

impl Responder {
    /// The answer to stream `stream` of `conn`, which waits for room no longer than `idle`
    /// at a time.
    pub(crate) fn new(conn: Rc<Conn>, stream: u64, idle: Duration) -> Self {
        Self {
            conn,
            stream,
            headed: false,
            final_sent: false,
            idle: Idle::new(idle),
        }
    }

    /// Ready once the client has reset the stream or stopped the answer, watching for it
    /// until then.
    pub(crate) fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.conn.with(|state| {
            let Some(slot) = state.streams.get_mut(&self.stream) else {
                return Poll::Ready(());
            };
            if slot.reset.is_some() || slot.stopped.is_some() || state.closed {
                return Poll::Ready(());
            }
            // Woken by the driver when the client resets or stops, as a waiting writer is.
            slot.writer = Some(cx.waker().clone());
            Poll::Pending
        })
    }

    /// Whether the client gave the stream up, resetting it or stopping the answer, rather
    /// than the connection going under it.
    pub(crate) fn given_up(&self) -> bool {
        self.conn.with(|state| {
            state
                .streams
                .get(&self.stream)
                .is_some_and(|slot| slot.reset.is_some() || slot.stopped.is_some())
        })
    }

    /// Sends an interim head, before the final one only. One the stream has no room for
    /// is not sent: an interim head says nothing the final one will not.
    pub(crate) fn interim(&mut self, head: &Response<()>) -> Result<(), SendError> {
        if self.final_sent {
            return Err(SendError::AfterFinal);
        }
        let sent = self.head(head.status(), head.headers(), false)?;
        if matches!(sent, Wrote::Done(())) {
            self.conn.stir();
        }
        Ok(())
    }

    /// Sends the final head, ending the stream with it if there is no body to follow, as
    /// soon as the stream has room for it.
    pub(crate) async fn final_head(
        &mut self,
        head: &http::response::Parts,
        end_stream: bool,
    ) -> Result<(), SendError> {
        if self.final_sent {
            return Err(SendError::AfterFinal);
        }
        poll_fn(
            |cx| match self.head(head.status, &head.headers, end_stream) {
                Ok(Wrote::Done(())) => Poll::Ready(Ok(())),
                Ok(Wrote::Blocked) => self.wait(cx),
                Err(error) => Poll::Ready(Err(error)),
            },
        )
        .await?;
        self.final_sent = true;
        self.conn.stir();
        Ok(())
    }

    /// Sends `body` after the final head, to the body's end. On failure the stream is
    /// reset, unless the client stopped it first.
    pub(crate) async fn send_body<B>(&mut self, body: B) -> Result<(), SendError>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let sent = self.sending(body).await;
        let reset = match &sent {
            Ok(()) | Err(SendError::Reset(_) | SendError::Closed) => None,
            // The client's own upload stopping, or the client not taking the answer, is
            // the client's doing.
            Err(SendError::Body(error)) if client_stopped(&**error) => {
                Some(code::REQUEST_CANCELLED)
            }
            Err(SendError::TimedOut) => Some(code::REQUEST_CANCELLED),
            Err(_) => Some(code::INTERNAL_ERROR),
        };
        if let Some(code) = reset {
            self.reset(code);
        }
        sent
    }

    /// Waits for room no longer than `idle` at a time from here on: a tunnel's own idle
    /// bound, in place of an answer's ([19 §5](../../../../../docs/19-websocket.md)).
    pub(crate) fn idle_for(&mut self, idle: Duration) {
        self.idle = Idle::new(idle);
    }

    /// Sends what of `data` the stream takes now, after the final head, and says how much;
    /// with `end`, the stream's end once all of it has gone (`data` empty: the end alone).
    /// Waits for room when the stream takes nothing.
    pub(crate) fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
        data: &[u8],
        end: bool,
    ) -> Poll<Result<usize, SendError>> {
        let written = self.conn.with(|state| {
            let (quic, h3) = streams_of(state)?;
            match h3.send_body(quic, self.stream, data, end) {
                Ok(written) => Ok(Some(written)),
                Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => Ok(None),
                Err(error) => Err(SendError::from(error)),
            }
        });
        match written {
            Err(error) => Poll::Ready(Err(error)),
            Ok(Some(written)) if written > 0 || data.is_empty() => {
                self.idle.moved();
                self.conn.stir();
                Poll::Ready(Ok(written))
            }
            Ok(_) => self.wait(cx),
        }
    }

    /// Resets the stream's sending side with `code`, and stops reading it.
    pub(crate) fn reset(&self, code: u64) {
        self.conn.with(|state| {
            // Fails only for a side already done.
            let _reset = state
                .quic
                .stream_shutdown(self.stream, quiche::Shutdown::Write, code);
            let _stopped = state
                .quic
                .stream_shutdown(self.stream, quiche::Shutdown::Read, code);
        });
        self.conn.stir();
    }

    async fn sending<B>(&mut self, body: B) -> Result<(), SendError>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let mut body = std::pin::pin!(body);
        loop {
            let frame = poll_fn(|cx| {
                if let Poll::Ready(()) = self.poll_reset(cx) {
                    return Poll::Ready(Err(self.stopped()));
                }
                body.as_mut().poll_frame(cx).map(Ok)
            })
            .await?;
            let frame = match frame {
                None => return self.data(&[], true).await,
                Some(Err(error)) => return Err(SendError::Body(error.into())),
                Some(Ok(frame)) => frame,
            };
            match frame.into_data() {
                Ok(data) => {
                    let end = body.is_end_stream();
                    self.data(&data, end).await?;
                    if end {
                        return Ok(());
                    }
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        return self.trailers(&trailers).await;
                    }
                }
            }
        }
    }

    /// Sends `data`, the stream's last if `end`, as the stream takes it.
    async fn data(&mut self, data: &[u8], end: bool) -> Result<(), SendError> {
        let mut sent = 0;
        poll_fn(|cx| {
            loop {
                let written = self.conn.with(|state| {
                    let (quic, h3) = streams_of(state)?;
                    match h3.send_body(quic, self.stream, &data[sent..], end) {
                        Ok(written) => Ok(Some(written)),
                        Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => Ok(None),
                        Err(error) => Err(SendError::from(error)),
                    }
                });
                match written {
                    Err(error) => return Poll::Ready(Err(error)),
                    Ok(Some(written)) => {
                        sent += written;
                        self.idle.moved();
                        self.conn.stir();
                        // All of it, and its end if it ends the stream: quiche sets FIN
                        // only on a call that takes the last byte.
                        if sent == data.len() {
                            return Poll::Ready(Ok(()));
                        }
                        if written == 0 {
                            return self.wait(cx);
                        }
                    }
                    Ok(None) => return self.wait(cx),
                }
            }
        })
        .await
    }

    /// Sends `trailers`, which end the stream.
    async fn trailers(&mut self, trailers: &HeaderMap) -> Result<(), SendError> {
        let fields = head::trailer_fields(trailers);
        poll_fn(|cx| {
            let sent = self.conn.with(|state| {
                let (quic, h3) = streams_of(state)?;
                match h3.send_additional_headers(quic, self.stream, &fields, true, true) {
                    Ok(()) => Ok(Wrote::Done(())),
                    Err(quiche::h3::Error::StreamBlocked) => Ok(Wrote::Blocked),
                    Err(error) => Err(SendError::from(error)),
                }
            });
            match sent {
                Ok(Wrote::Done(())) => {
                    self.conn.stir();
                    Poll::Ready(Ok(()))
                }
                Ok(Wrote::Blocked) => self.wait(cx),
                Err(error) => Poll::Ready(Err(error)),
            }
        })
        .await
    }

    /// Sends a head: the first as the stream's response head, any later one after it.
    fn head(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        end: bool,
    ) -> Result<Wrote<()>, SendError> {
        let fields = head::answer(&status, headers);
        let headed = self.headed;
        let sent = self.conn.with(|state| {
            let (quic, h3) = streams_of(state)?;
            let sent = if headed {
                h3.send_additional_headers(quic, self.stream, &fields, false, end)
            } else {
                h3.send_response(quic, self.stream, &fields, end)
            };
            match sent {
                Ok(()) => Ok(Wrote::Done(())),
                Err(quiche::h3::Error::StreamBlocked) => Ok(Wrote::Blocked),
                Err(error) => Err(SendError::from(error)),
            }
        })?;
        if matches!(sent, Wrote::Done(())) {
            self.headed = true;
        }
        Ok(sent)
    }

    /// Waits for the driver to say the stream has room, for the idle bound at the most, and
    /// for a reset meanwhile.
    fn wait<T>(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, SendError>> {
        if let Poll::Ready(()) = self.poll_reset(cx) {
            return Poll::Ready(Err(self.stopped()));
        }
        match self.idle.waiting(cx) {
            Poll::Ready(()) => Poll::Ready(Err(SendError::TimedOut)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Why the stream cannot be written to any more.
    fn stopped(&self) -> SendError {
        self.conn.with(|state| {
            match state
                .streams
                .get(&self.stream)
                .and_then(|slot| slot.reset.or(slot.stopped))
            {
                Some(code) => SendError::Reset(code),
                None => SendError::Closed,
            }
        })
    }
}

/// The connection's transport and HTTP/3 layer, both at once, as quiche's calls take them.
fn streams_of(
    state: &mut State,
) -> Result<(&mut quiche::Connection, &mut quiche::h3::Connection), SendError> {
    if state.closed {
        return Err(SendError::Closed);
    }
    let State { quic, h3, .. } = state;
    let h3 = h3.as_mut().ok_or(SendError::Closed)?;
    Ok((quic, h3))
}

/// Whether `error` comes of the client's upload stopping for longer than its idle bound.
fn client_stopped(error: &(dyn StdError + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if matches!(
            error.downcast_ref::<RequestBodyError>(),
            Some(RequestBodyError::TimedOut)
        ) {
            return true;
        }
        cause = error.source();
    }
    false
}
