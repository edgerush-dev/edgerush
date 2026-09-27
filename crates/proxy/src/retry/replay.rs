//! A request's body kept as it goes, so that it can be sent again
//! ([03 §6](../../../docs/03-data-plane.md)).
//!
//! As linkerd's `ReplayBody` does: each frame is kept as it is sent on, the kept one the
//! same bytes as the sent one — a count on a shared buffer, not a copy — up to
//! [`MOST`] in all. A body that grows past it is let go of, and the request is not sent
//! again; so is one that has not ended by the time its answer asks for a retry, since
//! what it has not sent yet cannot be sent twice. Trailers are kept with the data.

use crate::request_body::{RequestBody, RequestBodyError};
use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// The most of a body kept to send again: linkerd's and Pingora's bound.
pub(crate) const MOST: usize = 64 * 1024;

#[derive(Debug, Clone)]
enum Kept {
    Data(Bytes),
    Trailers(HeaderMap),
}

#[derive(Debug, Default)]
struct Recording {
    kept: Vec<Kept>,
    size: usize,
    /// It grew past [`MOST`]: nothing is kept, and it is not to be sent again.
    capped: bool,
    ended: bool,
}

/// A body on its way the first time, kept as it goes.
#[derive(Debug)]
pub(crate) struct Tee {
    inner: RequestBody,
    recording: Rc<RefCell<Recording>>,
}

/// What a [`Tee`] kept, to send again from.
#[derive(Debug, Clone)]
pub(crate) struct Recorded(Rc<RefCell<Recording>>);

impl Tee {
    /// `inner`, kept as it goes, and where to find what was kept. A body that has ended
    /// before it starts — none at all — is kept whole already.
    pub(crate) fn new(inner: RequestBody) -> (Self, Recorded) {
        let recording = Rc::new(RefCell::new(Recording {
            ended: inner.is_end_stream(),
            ..Recording::default()
        }));
        (
            Self {
                inner,
                recording: Rc::clone(&recording),
            },
            Recorded(recording),
        )
    }
}

impl Recorded {
    /// The body again, if all of it was kept.
    pub(crate) fn replay(&self) -> Option<Replayed> {
        let recording = self.0.borrow();
        (recording.ended && !recording.capped).then(|| Replayed {
            frames: recording.kept.iter().cloned().collect(),
        })
    }
}

impl Body for Tee {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        let mut recording = this.recording.borrow_mut();
        match &polled {
            Poll::Ready(Some(Ok(frame))) if !recording.capped => {
                if let Some(data) = frame.data_ref() {
                    recording.size += data.len();
                    if recording.size > MOST {
                        recording.capped = true;
                        recording.kept = Vec::new();
                    } else {
                        recording.kept.push(Kept::Data(data.clone()));
                    }
                } else if let Some(trailers) = frame.trailers_ref() {
                    recording.kept.push(Kept::Trailers(trailers.clone()));
                }
            }
            Poll::Ready(None) => recording.ended = true,
            _ => {}
        }
        // A body may say it has ended with its last frame, and whatever sends it may stop
        // there without asking for the end, as the HTTP/2 writer does: it is whole with that
        // frame.
        if matches!(polled, Poll::Ready(Some(Ok(_)))) && this.inner.is_end_stream() {
            recording.ended = true;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// A kept body, sent again.
#[derive(Debug)]
pub(crate) struct Replayed {
    frames: VecDeque<Kept>,
}

impl Replayed {
    /// A body of these frames, as though kept: for tests.
    #[cfg(test)]
    pub(crate) fn of(frames: Vec<Frame<Bytes>>) -> Self {
        Self {
            frames: frames
                .into_iter()
                .map(|frame| match frame.into_data() {
                    Ok(data) => Kept::Data(data),
                    Err(frame) => Kept::Trailers(frame.into_trailers().unwrap_or_default()),
                })
                .collect(),
        }
    }
}

impl Body for Replayed {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        Poll::Ready(self.get_mut().frames.pop_front().map(|kept| {
            Ok(match kept {
                Kept::Data(data) => Frame::data(data),
                Kept::Trailers(trailers) => Frame::trailers(trailers),
            })
        }))
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        if self.frames.is_empty() {
            return SizeHint::with_exact(0);
        }
        // Trailers may follow: the length is the head's to say, as for the body kept.
        SizeHint::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    fn data(bytes: &'static [u8]) -> Frame<Bytes> {
        Frame::data(Bytes::from_static(bytes))
    }

    fn trailers() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-sum", "7".parse().unwrap());
        trailers
    }

    /// What a body gives, as data and trailers.
    async fn read(
        body: impl Body<Data = Bytes, Error = RequestBodyError> + Unpin,
    ) -> (Vec<u8>, Option<HeaderMap>) {
        let mut body = body;
        let (mut all, mut last) = (Vec::new(), None);
        while let Some(frame) = body.frame().await {
            match frame.unwrap().into_data() {
                Ok(chunk) => all.extend_from_slice(&chunk),
                Err(frame) => last = frame.into_trailers().ok(),
            }
        }
        (all, last)
    }

    #[tokio::test]
    async fn a_body_kept_whole_is_sent_again_the_same_trailers_and_all() {
        let inner = Replayed::of(vec![data(b"ab"), data(b"cd"), Frame::trailers(trailers())]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner));
        assert!(
            recorded.replay().is_none(),
            "sent again before it had all gone"
        );
        assert_eq!(read(tee).await, (b"abcd".to_vec(), Some(trailers())));
        let again = recorded.replay().expect("kept whole");
        assert_eq!(read(again).await, (b"abcd".to_vec(), Some(trailers())));
        // As often as asked.
        assert!(recorded.replay().is_some());
    }

    #[tokio::test]
    async fn a_body_past_the_bound_is_not_kept_and_goes_on_as_it_came() {
        let big = Bytes::from(vec![b'x'; MOST]);
        let inner = Replayed::of(vec![data(b"a"), Frame::data(big)]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner));
        let (sent, _) = read(tee).await;
        assert_eq!(sent.len(), MOST + 1, "the body itself was cut short");
        assert!(recorded.replay().is_none());
        // Exactly at the bound is kept.
        let inner = Replayed::of(vec![Frame::data(Bytes::from(vec![b'x'; MOST]))]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner));
        let _sent = read(tee).await;
        assert!(recorded.replay().is_some());
    }

    /// A body may say it has ended with its last frame, and whatever sends it may stop
    /// there without asking for the end, as the HTTP/2 writer does: it is kept whole all
    /// the same, and can be sent again.
    #[tokio::test]
    async fn a_body_that_says_it_has_ended_is_kept_whole() {
        let bodies = [
            (vec![data(b"ab"), data(b"cd")], (b"abcd".to_vec(), None)),
            (
                vec![data(b"ab"), Frame::trailers(trailers())],
                (b"ab".to_vec(), Some(trailers())),
            ),
        ];
        for (frames, whole) in bodies {
            let (mut tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(frames)));
            while !tee.is_end_stream() {
                let _sent = tee.frame().await;
            }
            drop(tee);
            let again = recorded.replay().expect("kept whole");
            assert_eq!(read(again).await, whole);
        }
    }

    #[tokio::test]
    async fn no_body_at_all_is_kept_from_the_start() {
        let (_tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(vec![])));
        let again = recorded.replay().expect("nothing to wait for");
        assert!(again.is_end_stream());
    }
}
