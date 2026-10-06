//! A request's body, whichever of EdgeRush's servers read it off the client's connection.
//!
//! The request core and the upstream exchange take this, and nothing a server defines:
//! what a server hands over is wrapped where it hands it over, and what it reports going
//! wrong is sorted into [`RequestBodyError`] there too. There are three: HTTP/1's, whose
//! body cannot leave the worker and so makes this one that cannot either
//! ([14 §2](../../../docs/14-downstream-server.md)), HTTP/2's over h2
//! ([15](../../../docs/15-http2-and-grpc.md)), and HTTP/3's over quiche
//! ([16](../../../docs/16-http3.md)). Each is tested where it is made.

use crate::downstream::h1::connection::IncomingBody;
use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h3::body::IncomingH3;
use crate::mirror;
use crate::retry::replay::{Replayed, Tee};
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::error::Error as StdError;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// A request's body, as the server that read it hands it over.
///
/// Named, and not a box or a trait object, for the reason the answer's body is: every
/// request would pay for that, and what is behind it could no longer say for itself whether
/// it has ended and how much of it is left. Those two are what decide how a body is sent on,
/// before a byte of it has been read, so each case passes them on as its server gives them.
/// Data and trailers stay separate frames.
#[derive(Debug)]
pub(crate) enum RequestBody {
    /// Read by EdgeRush's own HTTP/1 server, from the connection's own input.
    Ours(IncomingBody),
    /// Received by EdgeRush's own HTTP/2 server, over h2.
    H2(IncomingH2),
    /// Received by EdgeRush's own HTTP/3 server, over quiche.
    H3(IncomingH3),
    /// Either of those, kept as it goes so that it can be sent again: only for a request
    /// whose rule may retry it.
    Recorded(Box<Tee>),
    /// What was kept, sent again.
    Replayed(Replayed),
    /// Any of those, copied to mirrors as it goes: only for a request a rule mirrors.
    Mirrored(Box<mirror::Tee>),
    /// A mirror's copy.
    Copy(mirror::Copy),
    /// Any of those, its bytes counted as they come: only for a request whose listener
    /// logs ([21 §4](../../docs/21-access-logs.md)).
    Counted(Box<Counted>),
    /// None at all, whatever the client's stream goes on to carry: an extended CONNECT's,
    /// whose stream is a WebSocket's once it is answered
    /// ([19 §3](../../../docs/19-websocket.md)).
    None,
}

/// What a server reported going wrong, kept as the cause of a [`RequestBodyError`] without
/// its type in the signature.
type Cause = Box<dyn StdError + Send + Sync>;

/// Why a request's body could not be read to its end.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestBodyError {
    /// The client stopped before the body's end: the connection closed short of the length
    /// it gave, or before the chunk that ends the body.
    #[error("the client stopped before the end of the request body")]
    Incomplete(#[source] Cause),
    /// The body broke its own framing: a chunk size or chunk line that cannot be read.
    #[error("the request body's framing could not be read")]
    Invalid(#[source] Cause),
    /// Anything else the server reported, an HTTP/2 stream the client reset among them.
    #[error("the request body could not be read")]
    Other(#[source] Cause),
    /// The client sent nothing more for longer than the body's idle bound while it was
    /// waited on.
    #[error("the client stopped sending the request body")]
    TimedOut,
}

impl RequestBodyError {
    /// Sorts an error of h2's: a connection that ended under the stream cut its body short;
    /// anything else, a reset above all, is the stream failing.
    pub(crate) fn from_h2(error: ::h2::Error) -> Self {
        if error.is_io() {
            Self::Incomplete(Box::new(error))
        } else {
            Self::Other(Box::new(error))
        }
    }
}

impl RequestBody {
    /// Has the body end where its trailers would come: a request's trailers go no further
    /// than the gateway (03 §11). They are still read, to the end of the message. For the
    /// body as its server handed it over; one kept or copied is made from it after.
    pub(crate) fn drop_trailers(&mut self) {
        match self {
            Self::Ours(body) => body.drop_trailers(),
            Self::H2(body) => body.drop_trailers(),
            Self::H3(body) => body.drop_trailers(),
            Self::Recorded(_)
            | Self::Replayed(_)
            | Self::Mirrored(_)
            | Self::Copy(_)
            | Self::None => {}
            Self::Counted(counted) => counted.body.drop_trailers(),
        }
    }

    /// What keeping `data`, a frame this body handed on, holds that nothing pays for once
    /// the next frame is asked for: what a recording or a mirror that keeps it is to pay
    /// (14 §8). Nothing for HTTP/1's, cut from blocks paid for until the last piece of them
    /// goes; its length for HTTP/2's, h2's memory, paid for by its credit until it is given
    /// back; the piece it was read into for HTTP/3's, which nothing pays for once read out of
    /// quiche. Nothing for what a recording hands on, which it pays for itself while it
    /// keeps it; for a mirror's copy, what its queue charged.
    pub(crate) fn unpaid(&self, data: &Bytes) -> usize {
        match self {
            Self::Ours(_) | Self::Replayed(_) | Self::None => 0,
            Self::Copy(body) => body.unpaid(data),
            Self::H2(_) => data.len(),
            Self::H3(_) => IncomingH3::PIECE,
            Self::Recorded(body) => body.unpaid(data),
            Self::Mirrored(body) => body.unpaid(data),
            Self::Counted(counted) => counted.body.unpaid(data),
        }
    }

    /// `body`, its bytes counted for `counts` as they come; one with nothing to come, as it
    /// is.
    pub(crate) fn counted(body: Self, counts: Rc<dyn Counts>) -> Self {
        if body.is_end_stream() {
            return body;
        }
        Self::Counted(Box::new(Counted { body, counts }))
    }
}

/// What a request body's bytes are counted for.
pub(crate) trait Counts {
    /// `bytes` more of the body have come.
    fn received(&self, bytes: usize);
}

/// A request body, its bytes counted as they come.
pub(crate) struct Counted {
    body: RequestBody,
    counts: Rc<dyn Counts>,
}

impl std::fmt::Debug for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counted")
            .field("body", &self.body)
            .finish_non_exhaustive()
    }
}

/// `polled`, ended at a trailers frame the body does not hand over.
fn trailers_if(
    wanted: bool,
    polled: Poll<Option<Result<Frame<Bytes>, RequestBodyError>>>,
) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
    match polled {
        Poll::Ready(Some(Ok(frame))) if !wanted && frame.is_trailers() => Poll::Ready(None),
        polled => polled,
    }
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        match self.get_mut() {
            Self::Ours(body) => trailers_if(body.wants_trailers(), Pin::new(body).poll_frame(cx)),
            Self::H2(body) => trailers_if(body.wants_trailers(), Pin::new(body).poll_frame(cx)),
            Self::H3(body) => trailers_if(body.wants_trailers(), Pin::new(body).poll_frame(cx)),
            Self::Recorded(body) => Pin::new(&mut **body).poll_frame(cx),
            Self::Replayed(body) => Pin::new(body).poll_frame(cx),
            Self::Mirrored(body) => Pin::new(&mut **body).poll_frame(cx),
            Self::Copy(body) => Pin::new(body).poll_frame(cx),
            Self::Counted(counted) => {
                let polled = Pin::new(&mut counted.body).poll_frame(cx);
                if let Poll::Ready(Some(Ok(frame))) = &polled
                    && let Some(data) = frame.data_ref()
                {
                    counted.counts.received(data.len());
                }
                polled
            }
            Self::None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Ours(body) => body.is_end_stream(),
            Self::H2(body) => body.is_end_stream(),
            Self::H3(body) => body.is_end_stream(),
            Self::Recorded(body) => body.is_end_stream(),
            Self::Replayed(body) => body.is_end_stream(),
            Self::Mirrored(body) => body.is_end_stream(),
            Self::Copy(body) => body.is_end_stream(),
            Self::Counted(counted) => counted.body.is_end_stream(),
            Self::None => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Ours(body) => body.size_hint(),
            Self::H2(body) => body.size_hint(),
            Self::H3(body) => body.size_hint(),
            Self::Recorded(body) => body.size_hint(),
            Self::Replayed(body) => body.size_hint(),
            Self::Mirrored(body) => body.size_hint(),
            Self::Copy(body) => body.size_hint(),
            Self::Counted(counted) => counted.body.size_hint(),
            Self::None => SizeHint::with_exact(0),
        }
    }
}
