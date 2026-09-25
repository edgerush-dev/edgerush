//! A gRPC call's answer, ended by exactly one status ([15 §6](../../../docs/15-http2-and-grpc.md)).
//!
//! Once an answer's head has gone to the client, the only way left to tell it the call
//! failed is a status in the trailers. So a gRPC answer whose upstream fails part way, or
//! whose deadline passes before it ends, is ended here with trailers of the gateway's own
//! rather than cut off (linkerd does the same; Envoy resets the stream); and the upstream's
//! own status, once through, is never followed by a second. Only an answer that is itself
//! gRPC's — `200`, `application/grpc` — is given one: any other is passed on as it came,
//! for the client to read by its HTTP status.

use super::status::{Code, is_grpc, message};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use http_body::{Body, Frame, SizeHint};
use std::error::Error as StdError;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{Instant, Sleep};

/// Whether an answer with this status and these fields is gRPC's.
pub(crate) fn is_grpc_answer(status: StatusCode, head: &HeaderMap) -> bool {
    status == StatusCode::OK
        && head
            .get(http::header::CONTENT_TYPE)
            .is_some_and(|content_type| is_grpc(content_type.as_bytes()))
}

/// A gRPC answer's body.
pub(crate) struct Answered<B> {
    inner: B,
    /// Ends the call as `DEADLINE_EXCEEDED` if it has not ended by then.
    deadline: Option<Pin<Box<Sleep>>>,
    /// A status has gone, the upstream's or ours: nothing more is sent.
    ended: bool,
    /// The answer's head carried the status itself (a trailers-only answer).
    status_in_head: bool,
}

impl<B> std::fmt::Debug for Answered<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Answered")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl<B> Answered<B> {
    /// The body of a gRPC answer whose head is `head`, for a call that must end by
    /// `deadline`.
    pub(crate) fn new(inner: B, head: &HeaderMap, deadline: Option<Instant>) -> Self {
        Self {
            inner,
            deadline: deadline.map(|at| Box::pin(tokio::time::sleep_until(at))),
            ended: false,
            status_in_head: head.contains_key("grpc-status"),
        }
    }

    /// Ends the call with `code`, told as `why`.
    fn end<E>(&mut self, code: Code, why: &str) -> Poll<Option<Result<Frame<Bytes>, E>>> {
        self.ended = true;
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", code.value());
        trailers.insert("grpc-message", message(why));
        Poll::Ready(Some(Ok(Frame::trailers(trailers))))
    }
}

/// The status for an upstream that failed with `error` part way through an answer: for a
/// stream it reset or a connection it told to go, the status gRPC gives that reason (its
/// HTTP/2 mapping); for anything else — a connection lost — `UNAVAILABLE`.
fn failed(error: &(dyn StdError + 'static)) -> Code {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if let Some(reason) = error
            .downcast_ref::<::h2::Error>()
            .and_then(::h2::Error::reason)
        {
            return match reason {
                ::h2::Reason::REFUSED_STREAM => Code::Unavailable,
                ::h2::Reason::CANCEL => Code::Cancelled,
                ::h2::Reason::ENHANCE_YOUR_CALM => Code::ResourceExhausted,
                ::h2::Reason::INADEQUATE_SECURITY => Code::PermissionDenied,
                _ => Code::Internal,
            };
        }
        cause = error.source();
    }
    Code::Unavailable
}

impl<B> Body for Answered<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: StdError + 'static,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if let Some(deadline) = this.deadline.as_mut()
            && deadline.as_mut().poll(cx).is_ready()
        {
            // The upstream's stream goes with the answer's body, and is cancelled then.
            return this.end(Code::DeadlineExceeded, "the call's deadline passed");
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => {
                if frame.trailers_ref().is_some() {
                    this.ended = true;
                }
                Poll::Ready(Some(Ok(frame)))
            }
            // Failed part way: the client is told so in the trailers, while it can be.
            Poll::Ready(Some(Err(error))) => {
                this.end(failed(&error), "the upstream failed during the call")
            }
            Poll::Ready(None) => {
                this.ended = true;
                if this.status_in_head {
                    return Poll::Ready(None);
                }
                // An answer that ended with no status at all: not a success.
                this.end(
                    Code::Internal,
                    "the upstream ended the call without a status",
                )
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended || (self.status_in_head && self.inner.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        // Trailers may be added: no length is promised.
        SizeHint::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::collections::VecDeque;
    use std::time::Duration;

    #[derive(Debug, thiserror::Error)]
    enum Broke {
        #[error("the stream was reset")]
        Reset(#[source] ::h2::Error),
        #[error("the connection was lost")]
        Lost,
    }

    /// A body that yields what it is given, one frame at a time, then ends or pends.
    struct Scripted {
        frames: VecDeque<Result<Frame<Bytes>, Broke>>,
        then_pend: bool,
    }

    impl Body for Scripted {
        type Data = Bytes;
        type Error = Broke;
        fn is_end_stream(&self) -> bool {
            self.frames.is_empty() && !self.then_pend
        }
        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Broke>>> {
            let this = self.get_mut();
            match this.frames.pop_front() {
                Some(frame) => Poll::Ready(Some(frame)),
                None if this.then_pend => Poll::Pending,
                None => Poll::Ready(None),
            }
        }
    }

    fn scripted(frames: Vec<Result<Frame<Bytes>, Broke>>, then_pend: bool) -> Scripted {
        Scripted {
            frames: frames.into(),
            then_pend,
        }
    }

    fn status(code: &str) -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", code.parse().unwrap());
        trailers
    }

    fn data() -> Result<Frame<Bytes>, Broke> {
        Ok(Frame::data(Bytes::from_static(b"m")))
    }

    /// What the body gave: its data, and the status its trailers said.
    async fn read(body: Answered<Scripted>) -> (Vec<u8>, Option<String>) {
        let mut body = body;
        let mut data = Vec::new();
        let mut code = None;
        while let Some(frame) = body.frame().await {
            match frame.unwrap().into_data() {
                Ok(chunk) => data.extend_from_slice(&chunk),
                Err(frame) => {
                    let trailers = frame.into_trailers().unwrap();
                    assert!(code.is_none(), "a second status");
                    code = Some(trailers["grpc-status"].to_str().unwrap().to_owned());
                }
            }
        }
        (data, code)
    }

    #[test]
    fn only_an_answer_of_grpcs_own_is_one() {
        let mut head = HeaderMap::new();
        head.insert("content-type", "application/grpc".parse().unwrap());
        assert!(is_grpc_answer(StatusCode::OK, &head));
        assert!(!is_grpc_answer(StatusCode::SERVICE_UNAVAILABLE, &head));
        head.insert("content-type", "text/html".parse().unwrap());
        assert!(!is_grpc_answer(StatusCode::OK, &head));
        assert!(!is_grpc_answer(StatusCode::OK, &HeaderMap::new()));
    }

    #[tokio::test]
    async fn the_upstreams_status_goes_through_alone() {
        let inner = scripted(vec![data(), Ok(Frame::trailers(status("0")))], false);
        let answered = Answered::new(inner, &HeaderMap::new(), None);
        assert_eq!(read(answered).await, (b"m".to_vec(), Some("0".to_owned())));
    }

    #[tokio::test]
    async fn a_trailers_only_answer_ends_with_its_head() {
        let answered = Answered::new(scripted(vec![], false), &status("5"), None);
        assert!(answered.is_end_stream());
        assert_eq!(read(answered).await, (vec![], None));
    }

    #[tokio::test]
    async fn an_upstream_that_fails_part_way_is_told_by_what_failed() {
        for (broke, code) in [
            (Broke::Lost, "14"),
            (Broke::Reset(::h2::Reason::CANCEL.into()), "1"),
            (Broke::Reset(::h2::Reason::REFUSED_STREAM.into()), "14"),
            (Broke::Reset(::h2::Reason::ENHANCE_YOUR_CALM.into()), "8"),
            (Broke::Reset(::h2::Reason::PROTOCOL_ERROR.into()), "13"),
        ] {
            let inner = scripted(vec![data(), Err(broke)], false);
            let answered = Answered::new(inner, &HeaderMap::new(), None);
            assert_eq!(read(answered).await, (b"m".to_vec(), Some(code.to_owned())));
        }
    }

    #[tokio::test]
    async fn an_answer_without_a_status_is_not_a_success() {
        let inner = scripted(vec![data()], false);
        let answered = Answered::new(inner, &HeaderMap::new(), None);
        assert_eq!(read(answered).await, (b"m".to_vec(), Some("13".to_owned())));
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_past_its_deadline_is_ended_as_exceeded() {
        let inner = scripted(vec![data()], true);
        let deadline = Instant::now() + Duration::from_secs(2);
        let answered = Answered::new(inner, &HeaderMap::new(), Some(deadline));
        assert_eq!(read(answered).await, (b"m".to_vec(), Some("4".to_owned())));
        assert!(Instant::now() >= deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn after_its_status_nothing_more_is_said() {
        let inner = scripted(vec![Ok(Frame::trailers(status("0")))], true);
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut answered = Answered::new(inner, &HeaderMap::new(), Some(deadline));
        let _status = answered.frame().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(answered.frame().await.is_none());
    }
}
