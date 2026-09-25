//! A request's body copied to its mirrors as it goes, never holding it up
//! ([03 §6](../../../docs/03-data-plane.md)).
//!
//! Each frame the request's upstream takes is put on each mirror's queue too, the copy the
//! same bytes — a count on a shared buffer — and the mirror takes it from there at its own
//! pace. A mirror more than [`MOST_BEHIND`] behind is given up on, its copy failing, rather
//! than let it slow the request or hold more: nginx reads the whole body before its mirrors
//! start, and Envoy lets a backed-up mirror push back on the client, and neither is what a
//! copy is for. A copy goes no faster than the request's own upstream reads, and when the
//! request is given up on before its body ends, so are its copies.

use crate::request_body::{RequestBody, RequestBodyError};
use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// The most a mirror may fall behind: the bytes on its queue it has not taken yet.
pub(crate) const MOST_BEHIND: usize = 64 * 1024;

/// Why a copy stopped short.
#[derive(Debug, thiserror::Error)]
#[error("the mirror was given up on: it fell behind, or the request it copies went")]
struct GivenUp;

#[derive(Debug)]
enum Copied {
    Data(Bytes),
    Trailers(HeaderMap),
}

#[derive(Debug, Default)]
struct Queue {
    frames: VecDeque<Copied>,
    /// Bytes on it not taken yet.
    behind: usize,
    ended: bool,
    given_up: bool,
    /// It was given up on for falling behind, and not because the request went.
    fell_behind: bool,
    waiting: Option<Waker>,
}

impl Queue {
    fn give_up(&mut self) {
        self.given_up = true;
        self.frames.clear();
        self.behind = 0;
        if let Some(waker) = self.waiting.take() {
            waker.wake();
        }
    }

    fn put(&mut self, copied: Copied) {
        self.frames.push_back(copied);
        if let Some(waker) = self.waiting.take() {
            waker.wake();
        }
    }
}

/// A request's body on its way to its upstream, copied to its mirrors as it goes.
#[derive(Debug)]
pub(crate) struct Tee {
    inner: RequestBody,
    queues: Vec<Rc<RefCell<Queue>>>,
}

/// One mirror's copy of a request's body.
#[derive(Debug)]
pub(crate) struct Copy(Rc<RefCell<Queue>>);

/// What became of one mirror's copy, to be asked once the mirror is done.
#[derive(Debug)]
pub(crate) struct Kept(Rc<RefCell<Queue>>);

impl Kept {
    /// Whether the mirror fell too far behind and was given up on.
    pub(crate) fn fell_behind(&self) -> bool {
        self.0.borrow().fell_behind
    }
}

impl Tee {
    /// `inner`, copied to `mirrors` of them as it goes.
    pub(crate) fn new(inner: RequestBody, mirrors: usize) -> (Self, Vec<(Copy, Kept)>) {
        let ended = inner.is_end_stream();
        let queues: Vec<_> = (0..mirrors)
            .map(|_| {
                Rc::new(RefCell::new(Queue {
                    ended,
                    ..Queue::default()
                }))
            })
            .collect();
        let copies = queues
            .iter()
            .map(|queue| (Copy(Rc::clone(queue)), Kept(Rc::clone(queue))))
            .collect();
        (Self { inner, queues }, copies)
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
        for queue in &this.queues {
            let mut queue = queue.borrow_mut();
            if queue.given_up {
                continue;
            }
            match &polled {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        if queue.behind + data.len() > MOST_BEHIND {
                            queue.fell_behind = true;
                            queue.give_up();
                        } else {
                            queue.behind += data.len();
                            queue.put(Copied::Data(data.clone()));
                        }
                    } else if let Some(trailers) = frame.trailers_ref() {
                        queue.put(Copied::Trailers(trailers.clone()));
                    }
                }
                Poll::Ready(None) => {
                    queue.ended = true;
                    if let Some(waker) = queue.waiting.take() {
                        waker.wake();
                    }
                }
                Poll::Ready(Some(Err(_))) => queue.give_up(),
                Poll::Pending => {}
            }
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

impl Drop for Tee {
    /// A request given up on before its body ended leaves its copies short: they fail
    /// rather than wait for what will not come.
    fn drop(&mut self) {
        for queue in &self.queues {
            let mut queue = queue.borrow_mut();
            if !queue.ended {
                queue.give_up();
            }
        }
    }
}

impl Body for Copy {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let mut queue = self.0.borrow_mut();
        if queue.given_up {
            return Poll::Ready(Some(Err(RequestBodyError::Other(Box::new(GivenUp)))));
        }
        match queue.frames.pop_front() {
            Some(Copied::Data(data)) => {
                queue.behind -= data.len();
                Poll::Ready(Some(Ok(Frame::data(data))))
            }
            Some(Copied::Trailers(trailers)) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            None if queue.ended => Poll::Ready(None),
            None => {
                queue.waiting = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        let queue = self.0.borrow();
        queue.ended && queue.frames.is_empty() && !queue.given_up
    }

    fn size_hint(&self) -> SizeHint {
        if self.is_end_stream() {
            return SizeHint::with_exact(0);
        }
        // The length is the head's to say, as it is for the request copied.
        SizeHint::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::replay::Replayed;
    use http_body_util::BodyExt;

    fn data(bytes: &'static [u8]) -> Frame<Bytes> {
        Frame::data(Bytes::from_static(bytes))
    }

    fn trailers() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-sum", "7".parse().unwrap());
        trailers
    }

    fn body(frames: Vec<Frame<Bytes>>) -> RequestBody {
        RequestBody::Replayed(Replayed::of(frames))
    }

    /// Everything a body gives, or the error it stopped with; a body that waits on
    /// for more than a few seconds is a failure, not a test that never ends.
    async fn read(
        body: impl Body<Data = Bytes, Error = RequestBodyError> + Unpin,
    ) -> Result<(Vec<u8>, Option<HeaderMap>), RequestBodyError> {
        let mut body = body;
        let (mut all, mut last) = (Vec::new(), None);
        let bound = std::time::Duration::from_secs(5);
        while let Some(frame) = tokio::time::timeout(bound, body.frame())
            .await
            .expect("a body left waiting")
        {
            match frame?.into_data() {
                Ok(chunk) => all.extend_from_slice(&chunk),
                Err(frame) => last = frame.into_trailers().ok(),
            }
        }
        Ok((all, last))
    }

    #[tokio::test]
    async fn every_mirror_gets_the_body_as_it_went_trailers_and_all() {
        let (tee, mut copies) = Tee::new(
            body(vec![data(b"ab"), data(b"cd"), Frame::trailers(trailers())]),
            2,
        );
        let whole = (b"abcd".to_vec(), Some(trailers()));
        assert_eq!(read(tee).await.unwrap(), whole);
        let (second, kept) = copies.pop().unwrap();
        let (first, _) = copies.pop().unwrap();
        assert_eq!(read(first).await.unwrap(), whole);
        assert_eq!(read(second).await.unwrap(), whole);
        assert!(!kept.fell_behind());
    }

    #[tokio::test]
    async fn a_copy_waits_for_the_request_and_never_the_other_way() {
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1);
        let (mut copy, _kept) = copies.pop().unwrap();
        // Nothing has gone yet: the copy has nothing to give.
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut copy).poll_frame(&mut cx).is_pending());
        // The request's body goes all the way without the copy being read at all.
        assert_eq!(read(&mut tee).await.unwrap().0, b"abcd");
        assert_eq!(read(copy).await.unwrap().0, b"abcd");
    }

    #[tokio::test]
    async fn a_mirror_too_far_behind_is_given_up_on_and_the_request_goes_on() {
        let big = Bytes::from(vec![b'x'; MOST_BEHIND]);
        let (tee, mut copies) = Tee::new(body(vec![data(b"a"), Frame::data(big)]), 2);
        let (keeping_up, _) = copies.pop().unwrap();
        let (behind, kept) = copies.pop().unwrap();
        // One mirror takes its first frame as it comes; the other takes nothing.
        let mut tee = tee;
        let mut keeping_up = keeping_up;
        let first = tee.frame().await.unwrap().unwrap();
        assert_eq!(first.into_data().unwrap(), "a");
        let taken = keeping_up.frame().await.unwrap().unwrap();
        assert_eq!(taken.into_data().unwrap(), "a");
        // The request goes on whole.
        assert_eq!(read(&mut tee).await.unwrap().0.len(), MOST_BEHIND);
        drop(tee);
        assert!(
            read(behind).await.is_err(),
            "a mirror past its bound was kept"
        );
        assert!(kept.fell_behind());
        // The one that kept up got it all: exactly at the bound is within it.
        assert_eq!(read(keeping_up).await.unwrap().0.len(), MOST_BEHIND);
    }

    #[tokio::test]
    async fn a_request_given_up_on_leaves_its_copies_failing_not_waiting() {
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1);
        let (copy, kept) = copies.pop().unwrap();
        let _first = tee.frame().await;
        drop(tee);
        assert!(read(copy).await.is_err());
        assert!(!kept.fell_behind(), "not the mirror's doing");
    }

    #[tokio::test]
    async fn a_request_without_a_body_is_copied_as_one() {
        let (_tee, mut copies) = Tee::new(body(vec![]), 1);
        let (copy, _) = copies.pop().unwrap();
        assert!(copy.is_end_stream());
        assert_eq!(read(copy).await.unwrap().0, b"");
    }
}
