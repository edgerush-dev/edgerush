//! A request's body, whichever of EdgeRush's servers read it off the client's connection.
//!
//! The request core and the upstream exchange take this, and nothing a server defines:
//! what a server hands over is wrapped where it hands it over, and what it reports going
//! wrong is sorted into [`RequestBodyError`] there too. There are two: HTTP/1's, whose body
//! cannot leave the worker and so makes this one that cannot either
//! ([14 §2](../../../docs/14-downstream-server.md)), and HTTP/2's over h2
//! ([15](../../../docs/15-http2-and-grpc.md)). Each is tested where it is made.

use crate::downstream::h1::connection::IncomingBody;
use crate::downstream::h2::body::IncomingH2;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::error::Error as StdError;
use std::pin::Pin;
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

impl Body for RequestBody {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        match self.get_mut() {
            Self::Ours(body) => Pin::new(body).poll_frame(cx),
            Self::H2(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Ours(body) => body.is_end_stream(),
            Self::H2(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Ours(body) => body.size_hint(),
            Self::H2(body) => body.size_hint(),
        }
    }
}
