//! One request sent to an HTTP/2 upstream, and its answer read back
//! ([15 §5](../../../../docs/15-http2-and-grpc.md)).
//!
//! The stream is opened on a place the pool reserved. The request's body goes up while
//! the answer is waited for, not before it: an upstream may answer early, and one that
//! reads the body only as it answers would otherwise never be sent the rest. What is left
//! of the body when the answer's head arrives goes on with the answer's body, which sends
//! it as the answer is read — the same arrangement as the HTTP/1 client's.
//!
//! What h2 does on both sides is shared with the downstream server: the body is sent by
//! the same writer, which asks for room before it stages anything, cuts it into frames and
//! charges them to the worker's storage; the answer is read by the same reader, which
//! returns a frame's flow-control credit when the next is asked for.

use super::client::{Client, Place, PlaceError};
use super::head::{self, HeadError};
use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h2::writer::{SendError, send_body};
use crate::request_body::{RequestBody, RequestBodyError};
use crate::storage::Storage;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use bytes::Bytes;
use http::response::Parts;
use http::{Method, Uri};
use http_body::{Body, Frame, SizeHint};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// Why a request got no answer from an HTTP/2 upstream.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ExchangeError {
    /// No place on a connection.
    #[error(transparent)]
    Place(#[from] PlaceError),
    /// The request cannot be put into HTTP/2.
    #[error(transparent)]
    Head(#[from] HeadError),
    /// h2 refused to send it, or the upstream reset its stream or the connection.
    #[error("the HTTP/2 exchange failed: {0}")]
    H2(#[from] ::h2::Error),
    /// No final head within the time allowed after the request's head was sent.
    #[error("the upstream took longer than {limit:?} to answer")]
    TooSlow {
        /// The time allowed.
        limit: Duration,
    },
    /// The request's own body failed while it was being sent, before any answer.
    #[error("the request's body failed")]
    RequestBody(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// What is left of a request's body to send, sending itself.
type Upload = Pin<Box<dyn Future<Output = Result<(), SendError>>>>;

/// How long things may take.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timing {
    /// From the request's head sent to the answer's final head.
    pub(crate) final_head: Duration,
    /// How long a body, in either direction, may be waited on with nothing moving.
    pub(crate) idle: Duration,
}

/// Sends a request for `target` with `fields` to `destination` and waits for the answer's
/// head, sending the body meanwhile as `sending` says.
///
/// # Errors
///
/// An [`ExchangeError`] for whatever stopped the request before its answer's head.
#[expect(
    clippy::too_many_arguments,
    reason = "each is a different thing the exchange needs, as for the HTTP/1 client"
)]
pub(crate) async fn exchange<F: OutgoingFields + ?Sized>(
    client: &Rc<Client>,
    destination: &Arc<ReuseIdentity>,
    method: &Method,
    target: &Uri,
    fields: &F,
    sending: Sending,
    body: RequestBody,
    storage: &Rc<Storage>,
    timing: Timing,
) -> Result<(Parts, Answer), ExchangeError> {
    let head = head::request(method, target, fields, sending)?;
    let mut place = client.place(destination).await?;
    let sender = place.sender();
    // A handle that has opened nothing is ready unless the connection can take no new
    // streams at all; the place is what says there is room on it.
    poll_fn(|cx| sender.poll_ready(cx)).await?;
    let end_stream = matches!(sending, Sending::None) || body.is_end_stream();
    let (response, stream) = sender.send_request(head, end_stream)?;
    let mut upload: Option<Upload> = (!end_stream).then(|| {
        let storage = Rc::clone(storage);
        let upload: Upload = Box::pin(async move {
            let mut stream = stream;
            send_body(&mut stream, body, &storage, timing.idle).await
        });
        upload
    });

    let mut response = std::pin::pin!(response);
    let answered = tokio::time::timeout(
        timing.final_head,
        poll_fn(|cx| {
            if let Some(sending) = upload.as_mut()
                && let Poll::Ready(sent) = sending.as_mut().poll(cx)
            {
                upload = None;
                // The body failed on its way in: the stream has been reset, and the
                // answer is the client's doing, not the upstream's.
                if let Err(SendError::Body(cause)) = sent {
                    return Poll::Ready(Err(ExchangeError::RequestBody(cause)));
                }
            }
            response.as_mut().poll(cx).map_err(ExchangeError::H2)
        }),
    )
    .await
    .map_err(|_| ExchangeError::TooSlow {
        limit: timing.final_head,
    })??;

    let (parts, received) = answered.into_parts();
    let answer = Answer {
        body: IncomingH2::new(received, timing.idle),
        upload,
        _place: place,
    };
    Ok((parts, answer))
}

/// The body of an HTTP/2 upstream's answer, with what is left of the request's body going
/// up as it is read. It holds the stream's place on its connection until it is dropped.
pub(crate) struct Answer {
    body: IncomingH2,
    upload: Option<Upload>,
    _place: Place,
}

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Answer")
            .field("uploading", &self.upload.is_some())
            .finish_non_exhaustive()
    }
}

impl Body for Answer {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let this = self.get_mut();
        if let Some(sending) = this.upload.as_mut()
            && sending.as_mut().poll(cx).is_ready()
        {
            // Whichever way it went: a body that failed has reset the stream, which the
            // answer's body then reports.
            this.upload = None;
        }
        Pin::new(&mut this.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
