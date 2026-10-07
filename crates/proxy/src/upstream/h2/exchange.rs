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

use super::client::{Client, Place, PlaceError, Shed};
use super::head::{self, HeadError};
use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h2::writer::{Outgoing, SendError, send_body};
use crate::h2_stream::H2Stream;
use crate::interim::{Channel, Interim};
use crate::request_body::{RequestBody, RequestBodyError};
use crate::retry::replay::{Recorded, Tee};
use crate::storage::Storage;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use crate::upstream::h1::exchange::expects_continue;
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
    /// The upstream gave the request's body no room for the idle bound while it was being
    /// sent, before any answer. The client's own body stopping is [`Self::RequestBody`].
    #[error("the upstream gave the request's body no room for {limit:?}")]
    Idle {
        /// The time a body may wait for room.
        limit: Duration,
    },
    /// The request's own body failed while it was being sent, before any answer.
    #[error("the request's body failed")]
    RequestBody(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The worker could not pay, before any answer: for a piece of the request's body, or
    /// for what h2 held of the connection's answers, the connection being closed for it.
    /// Its own shortage, not the upstream's failing.
    #[error("the worker could not pay for the exchange's storage")]
    Exhausted,
    /// More interim heads, or more of them, than an exchange takes before its final one.
    #[error("the upstream sent more interim answers than {heads} heads or {bytes} bytes")]
    Interim {
        /// The most heads taken.
        heads: usize,
        /// The most bytes taken, by RFC 9113's measure of a header list.
        bytes: usize,
    },
}

/// What is left of a request's body to send, sending itself.
type Upload = Pin<Box<dyn Future<Output = Result<(), SendError>>>>;

/// What an exchange will not go beyond (the HTTP/1 client's own, [13 §7]).
///
/// [13 §7]: ../../../../docs/13-http1-upstream.md
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    /// From the request's head sent to the answer's final head; none where the rule's own
    /// timeouts bound the wait instead ([03 §6](../../../../docs/03-data-plane.md)).
    pub(crate) final_head: Option<Duration>,
    /// How long a body, in either direction, may be waited on with nothing moving.
    pub(crate) idle: Duration,
    /// How long a body held back for `100 Continue` waits for it.
    pub(crate) continue_wait: Duration,
    /// Interim heads taken before the final one.
    pub(crate) interim_heads: usize,
    /// What those heads may come to, by RFC 9113's measure of a header list.
    pub(crate) interim_bytes: usize,
}

/// Sends a request for `target` with `fields` to `destination` and waits for the answer's
/// head, sending the body meanwhile as `sending` says. Interim answers go to `interim`, as
/// the HTTP/1 client's do, and the continue decision is made the same way: a request that
/// said `Expect: 100-continue` has its body held back until the upstream says `100`, or
/// the wait runs out, and never sent if the final answer comes first.
///
/// A request the upstream shows it never processed — its stream refused, or above the
/// last one a GOAWAY accepted, the only two signs of it ([15 §3]) — is sent once more if
/// all of it can be: its body is kept as it goes, up to what [`replay`] keeps, and a body
/// none of which went is given back whole; one that had partly gone and grew past what
/// is kept, or had not ended, is not sent again. A continue wait the first stream began
/// runs on for the second. gRPC retries the same ones, the same once ("transparent"
/// retries). `retrying` is told when it does.
///
/// [`replay`]: crate::retry::replay
///
/// [15 §3]: ../../../../docs/15-http2-and-grpc.md
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
    interim: Option<Interim>,
    bounds: Bounds,
    deadline: Option<tokio::time::Instant>,
    mut retrying: impl FnMut(),
) -> Result<(Parts, Answer), ExchangeError> {
    let first = head::request(method, target, fields, sending, destination.scheme())?;
    let mut channel = interim.map_or_else(Channel::unheard, Channel::Listened);
    let nothing_to_send = matches!(sending, Sending::None | Sending::Length(0));
    channel.begin(expects_continue(fields), nothing_to_send);
    let end_stream = matches!(sending, Sending::None) || body.is_end_stream();
    let (body, kept) = kept(body, end_stream, storage);
    let mut body = Some(body);
    let mut head = Some(first);
    let mut again = false;
    // Begun by the first stream whose head goes, and run on by a stream sent again: the
    // continue decision is the request's, made once.
    let mut continue_wait = None;
    loop {
        // A head to send again is made again, from what the first was made from: kept for
        // a retry only when one happens, so no request pays for a copy it does not use.
        let request = match head.take() {
            Some(head) => head,
            None => head::request(method, target, fields, sending, destination.scheme())?,
        };
        let attempt = Attempt {
            client,
            destination,
            end_stream,
            storage,
            bounds,
            deadline,
        };
        match attempt
            .run(request, &mut body, &mut channel, &mut continue_wait)
            .await
        {
            Err(ExchangeError::H2(error)) if !again && unprocessed(&error) => {
                if let Some(kept) = &kept {
                    let Some(whole) = whole_again(kept, body.take()) else {
                        return Err(ExchangeError::H2(error));
                    };
                    body = Some(whole);
                }
                retrying();
                again = true;
            }
            outcome => return outcome,
        }
    }
}

/// `body` as it is to be sent, and what is kept of it to send again: nothing where there is
/// nothing to keep, a request with no body being sent again from its head alone; the
/// recording that already keeps it, where a rule's retry does, so that a body is copied and
/// paid for once ([03 §6](../../../../docs/03-data-plane.md)); and otherwise a recording
/// made here.
fn kept(
    body: RequestBody,
    end_stream: bool,
    storage: &Rc<Storage>,
) -> (RequestBody, Option<Recorded>) {
    if end_stream {
        return (body, None);
    }
    if let RequestBody::Recorded(tee) = &body {
        let kept = tee.recorded();
        return (body, Some(kept));
    }
    let (tee, kept) = Tee::new(body, storage);
    (RequestBody::Recorded(Box::new(tee)), Some(kept))
}

/// The whole of a body to send again, from what `kept` kept of it, if there is one: all of
/// it, where it went and was kept; or the body itself, where none of it went. `body` is
/// what is still here of it: a body no stream took is let go of as one a stream took and
/// left untouched is, and given back the same way.
fn whole_again(kept: &Recorded, body: Option<RequestBody>) -> Option<RequestBody> {
    drop(body);
    kept.replay()
        .map(RequestBody::Replayed)
        .or_else(|| kept.given_back())
}

/// What an error h2 gives a stream says of its exchange: the worker's own shortage where its
/// connection was closed because the worker could not pay for it, whatever h2 then told the
/// stream ([14 §8](../../../../docs/14-downstream-server.md)); otherwise h2's error as it is.
fn failed(shed: Shed) -> impl Fn(::h2::Error) -> ExchangeError {
    move |error| {
        if shed.is_shed() {
            ExchangeError::Exhausted
        } else {
            ExchangeError::H2(error)
        }
    }
}

/// Whether the upstream says it never processed the stream that failed with `error`: it
/// refused it, or a GOAWAY left it above the last it accepted. Nothing else says so — a
/// stream it reset any other way, or a connection lost under it, may have been acted on.
fn unprocessed(error: &::h2::Error) -> bool {
    error.is_remote()
        && (error.is_go_away() || error.reason() == Some(::h2::Reason::REFUSED_STREAM))
}

/// One try at an exchange.
struct Attempt<'a> {
    client: &'a Rc<Client>,
    destination: &'a Arc<ReuseIdentity>,
    end_stream: bool,
    storage: &'a Rc<Storage>,
    bounds: Bounds,
    deadline: Option<tokio::time::Instant>,
}

impl Attempt<'_> {
    async fn run(
        self,
        mut request: http::Request<()>,
        body: &mut Option<RequestBody>,
        channel: &mut Channel,
        continue_wait: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    ) -> Result<(Parts, Answer), ExchangeError> {
        let Self {
            client,
            destination,
            end_stream,
            storage,
            bounds,
            deadline,
        } = self;
        let mut place = client.place(destination).await?;
        let failed = failed(place.shed());
        // A gRPC call's deadline goes up as the time it has left now, waiting for a place
        // and all: the upstream sees the deadline the client set, not a fresh one (15 §6).
        if let Some(deadline) = deadline {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            request
                .headers_mut()
                .insert("grpc-timeout", crate::grpc::timeout::format(left));
        }
        let sender = place.sender();
        // A handle that has opened nothing is ready unless the connection can take no new
        // streams at all; the place is what says there is room on it.
        poll_fn(|cx| sender.poll_ready(cx)).await.map_err(&failed)?;
        let (mut response, stream) = sender.send_request(request, end_stream).map_err(&failed)?;
        let mut upload: Option<Upload> = if end_stream {
            None
        } else {
            body.take().map(|body| {
                let storage = Rc::clone(storage);
                let upload: Upload = Box::pin(async move {
                    let mut stream = stream;
                    send_body(&mut stream, body, &storage, bounds.idle).await
                });
                upload
            })
        };

        // The head is out as far as this hop can tell once h2 has it. The channel says so
        // once a request: a stream sent again runs on with the wait the first began.
        if channel.head_sent() {
            *continue_wait = Some(Box::pin(tokio::time::sleep(bounds.continue_wait)));
        }
        let mut may_send = channel.may_poll_upload();
        let (mut heads, mut bytes) = (0, 0);
        let waiting = poll_fn(|cx| {
            // Interim answers first, as h2 has them asked for, in the order they came.
            while let Poll::Ready(Some(interim)) = response.poll_informational(cx) {
                let (interim, ()) = interim.map_err(&failed)?.into_parts();
                heads += 1;
                bytes += list_size(&interim.headers);
                if heads > bounds.interim_heads || bytes > bounds.interim_bytes {
                    return Poll::Ready(Err(ExchangeError::Interim {
                        heads: bounds.interim_heads,
                        bytes: bounds.interim_bytes,
                    }));
                }
                channel.upstream_interim(interim.status, interim.headers);
                may_send = channel.may_poll_upload();
            }
            if let Some(wait) = continue_wait.as_mut()
                && wait.as_mut().poll(cx).is_ready()
            {
                *continue_wait = None;
                channel.wait_expired();
                may_send = channel.may_poll_upload();
            }
            if may_send
                && let Some(sending) = upload.as_mut()
                && let Poll::Ready(sent) = sending.as_mut().poll(cx)
            {
                upload = None;
                // The body failed on its way in: the stream has been reset, and the
                // answer is the client's doing, not the upstream's.
                match sent {
                    Err(SendError::Body(cause)) => {
                        return Poll::Ready(Err(ExchangeError::RequestBody(cause)));
                    }
                    // The upstream's window stayed shut: the sender has reset the
                    // stream, and what the answer would say of that is not the cause.
                    Err(SendError::TimedOut) => {
                        return Poll::Ready(Err(ExchangeError::Idle { limit: bounds.idle }));
                    }
                    // The worker had nothing to pay for a piece with: the sender has reset
                    // the stream, and the answer would blame the upstream for it.
                    Err(SendError::Exhausted(_)) => {
                        return Poll::Ready(Err(ExchangeError::Exhausted));
                    }
                    _ => {}
                }
            }
            Pin::new(&mut response).poll(cx).map_err(&failed)
        });
        let answered = match bounds.final_head {
            Some(limit) => tokio::time::timeout(limit, waiting)
                .await
                .map_err(|_| ExchangeError::TooSlow { limit })??,
            None => waiting.await?,
        };
        channel.final_head();
        // Held back for a `100` that never came, and not wanted now the final answer has:
        // never sent. It goes, resetting the stream, when the answer does.
        let abandoned = channel.abandoned();

        let (parts, received) = answered.into_parts();
        let answer = Answer {
            body: IncomingH2::new(received, bounds.idle),
            upload,
            abandoned,
            place,
        };
        Ok((parts, answer))
    }
}

/// What an extended CONNECT for a WebSocket came to ([19 §4](../../../../docs/19-websocket.md)).
pub(crate) enum Connected {
    /// The backend said yes (any 2xx: RFC 9110 §9.3.6): the answer's head, and its stream,
    /// which is the WebSocket's from here on.
    Switched(Parts, H2Stream),
    /// It said something else: an ordinary answer.
    Refused(Parts, Answer),
    /// Its connection does not announce extended CONNECT (RFC 8441 §3): it is not sent one.
    NotOffered,
}

/// Asks `destination` for a WebSocket at `target` with an extended CONNECT (RFC 8441), its
/// fields `fields`, once a connection's SETTINGS say it takes them; `bounds.final_head`
/// bounds the whole of it, the wait for the SETTINGS included. The stream is not ended: its
/// DATA frames are the WebSocket's.
///
/// # Errors
///
/// An [`ExchangeError`] for whatever stopped the request before its answer's head.
pub(crate) async fn connect<F: OutgoingFields + ?Sized>(
    client: &Rc<Client>,
    destination: &Arc<ReuseIdentity>,
    target: &Uri,
    fields: &F,
    storage: &Rc<Storage>,
    bounds: Bounds,
) -> Result<Connected, ExchangeError> {
    let mut request = head::request(
        &Method::CONNECT,
        target,
        fields,
        Sending::None,
        destination.scheme(),
    )?;
    request
        .extensions_mut()
        .insert(::h2::ext::Protocol::from_static("websocket"));
    let connecting = async {
        let mut place = client.place(destination).await?;
        let failed = failed(place.shed());
        // A connection that ended unsettled said nothing either way, and is not offered
        // another try beside it: its readiness fails, and says how.
        let settled = place.settled().await;
        if settled && !place.sender().is_extended_connect_protocol_enabled() {
            return Ok(Connected::NotOffered);
        }
        let sender = place.sender();
        poll_fn(|cx| sender.poll_ready(cx)).await.map_err(&failed)?;
        let (response, mut send) = sender.send_request(request, false).map_err(&failed)?;
        let (parts, recv) = response.await.map_err(&failed)?.into_parts();
        if parts.status.is_success() {
            let stream = H2Stream::new(send, recv, Rc::clone(storage), Some(place));
            return Ok(Connected::Switched(parts, stream));
        }
        // Nothing more goes up: its end is sent, and its answer read as any other's.
        let _ended = send.send_data(Outgoing::empty(), true);
        let answer = Answer {
            body: IncomingH2::new(recv, bounds.idle),
            upload: None,
            abandoned: false,
            place,
        };
        Ok(Connected::Refused(parts, answer))
    };
    match bounds.final_head {
        Some(limit) => tokio::time::timeout(limit, connecting)
            .await
            .map_err(|_| ExchangeError::TooSlow { limit })?,
        None => connecting.await,
    }
}

/// Takes out of `trailers` the names that may not travel as trailers
/// ([13 §4](../../../../../docs/13-http1-upstream.md)). HTTP/2 has no `Connection` to nominate
/// more. Allocates only for a section that holds one.
fn deny(trailers: &mut http::HeaderMap) {
    let denied: Vec<_> = trailers
        .keys()
        .filter(|name| crate::h1::is_denied(name, &[]))
        .cloned()
        .collect();
    for name in denied {
        trailers.remove(name);
    }
}

/// A header list's size as RFC 9113 §6.5.2 measures it: each field's name and value, and
/// 32 for each field.
fn list_size(headers: &http::HeaderMap) -> usize {
    headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + 32)
        .sum()
}

/// The body of an HTTP/2 upstream's answer, with what is left of the request's body going
/// up as it is read. It holds the stream's place on its connection until it is dropped.
///
/// Its trailers are held to the names that may travel as trailers, as an HTTP/1 upstream's
/// are where they are read ([13 §4](../../../../../docs/13-http1-upstream.md)), so that the
/// writer of every kind of client is handed only those.
pub(crate) struct Answer {
    body: IncomingH2,
    upload: Option<Upload>,
    /// The upload was never wanted: it is not sent, only dropped with the answer.
    abandoned: bool,
    place: Place,
}

impl Answer {
    /// Whether its connection was closed because the worker could not pay for what h2 held
    /// of its answers: what then cuts this one is the worker's shortage, not the upstream.
    pub(crate) fn is_shed(&self) -> bool {
        self.place.shed().is_shed()
    }
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
        if !this.abandoned
            && let Some(sending) = this.upload.as_mut()
            && sending.as_mut().poll(cx).is_ready()
        {
            // Whichever way it went: a body that failed has reset the stream, which the
            // answer's body then reports.
            this.upload = None;
        }
        match Pin::new(&mut this.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_trailers() {
                // A section left with nothing in it is no section, as an HTTP/1 upstream's
                // empty one is none.
                Ok(mut trailers) => {
                    deny(&mut trailers);
                    Poll::Ready((!trailers.is_empty()).then(|| Ok(Frame::trailers(trailers))))
                }
                Err(data) => Poll::Ready(Some(Ok(data))),
            },
            polled => polled,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::replay::Replayed;
    use http_body_util::BodyExt;

    /// A body a rule's retry already keeps as it goes is kept once: the same recording
    /// serves a stream sent again, and nothing of it is copied, or paid for, a second time.
    #[tokio::test]
    async fn a_body_kept_for_a_retry_is_kept_once() {
        let storage = Storage::with_provision(1 << 20, 0);
        let frames = (0..1024)
            .map(|_| Frame::data(Bytes::from_static(b"x")))
            .collect();
        let (tee, _for_the_retry) = Tee::new(RequestBody::Replayed(Replayed::of(frames)), &storage);
        let (mut body, again) = kept(RequestBody::Recorded(Box::new(tee)), false, &storage);
        while let Some(frame) = body.frame().await {
            frame.unwrap();
        }
        assert_eq!(
            storage.used(),
            1024,
            "a run of 1,024 bytes kept more than once"
        );
        assert!(again.and_then(|kept| kept.replay()).is_some());
    }

    /// A body nothing keeps yet is kept as it goes from here; one with nothing in it is
    /// not kept at all, its head being all there is to send again.
    #[tokio::test]
    async fn a_body_nothing_keeps_is_kept_from_here() {
        let storage = Storage::with_provision(1 << 20, 0);
        let frames = vec![Frame::data(Bytes::from_static(b"hi"))];
        let (mut body, again) = kept(RequestBody::Replayed(Replayed::of(frames)), false, &storage);
        assert!(matches!(body, RequestBody::Recorded(_)));
        while let Some(frame) = body.frame().await {
            frame.unwrap();
        }
        assert!(again.and_then(|kept| kept.replay()).is_some());
        let (_body, again) = kept(RequestBody::None, true, &storage);
        assert!(again.is_none());
    }

    /// What is sent again of a body: all of it, from what was kept, once it went whole; the
    /// body itself where none of it went, whether a stream took it or none did; nothing
    /// where only part of it went.
    #[tokio::test]
    async fn a_body_is_sent_again_whole_or_not_at_all() {
        let storage = Storage::with_provision(1 << 20, 0);
        let two = || {
            RequestBody::Replayed(Replayed::of(vec![
                Frame::data(Bytes::from_static(b"a")),
                Frame::data(Bytes::from_static(b"b")),
            ]))
        };
        async fn all_of(body: Option<RequestBody>) -> Vec<u8> {
            body.unwrap().collect().await.unwrap().to_bytes().to_vec()
        }

        let (body, again) = kept(two(), false, &storage);
        let recorded = again.unwrap();
        assert_eq!(all_of(whole_again(&recorded, Some(body))).await, b"ab");

        let (body, again) = kept(two(), false, &storage);
        let recorded = again.unwrap();
        drop(body);
        assert_eq!(all_of(whole_again(&recorded, None)).await, b"ab");

        let (body, again) = kept(two(), false, &storage);
        let recorded = again.unwrap();
        let _went = body.collect().await.unwrap();
        assert_eq!(all_of(whole_again(&recorded, None)).await, b"ab");

        let (mut body, again) = kept(two(), false, &storage);
        let recorded = again.unwrap();
        let _one = body.frame().await.unwrap().unwrap();
        drop(body);
        assert!(whole_again(&recorded, None).is_none());
    }
}
