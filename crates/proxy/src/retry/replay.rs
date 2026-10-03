//! A request's body kept as it goes, so that it can be sent again
//! ([03 §6](../../../docs/03-data-plane.md)).
//!
//! As linkerd's `ReplayBody` does: each frame is kept as it is sent on, the kept one the
//! same bytes as the sent one — a count on a shared buffer, not a copy — up to
//! [`MOST`] in all; small frames are copied together into runs as they are kept
//! ([`crate::runs`]), since a list of frames holds an entry for each however small it is,
//! and a client chooses how small. A body that grows past [`MOST`] is let go of, and the
//! request is not sent again; so is one that has not ended by the time its answer asks for a
//! retry, since what it has not sent yet cannot be sent twice. Trailers are kept with the
//! data. Once nothing can send it again — no handle to what was kept is left, as when its
//! answer's head has come — a body is kept no more, and what was kept goes.
//!
//! A body none of which went is whole as it is: a try that lets go of it untouched — one
//! that could not connect never reads it — gives it back, for the next try to send.

use crate::request_body::{RequestBody, RequestBodyError};
use crate::runs;
use crate::storage::{Charge, Exhausted, Storage};
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

#[derive(Debug)]
struct Recording {
    kept: Vec<Kept>,
    /// A small frame after everything in `kept`, kept as it is while no other small frame
    /// follows it: a body of one small frame is kept without a copy.
    lone: Option<Bytes>,
    /// Small frames being copied together, after everything in `kept`: kept itself once
    /// anything else comes, or the body ends.
    run: Vec<u8>,
    /// The worker's account, which pays for the runs: a copy is paid for by nothing it was
    /// copied from (14 §8).
    storage: Rc<Storage>,
    /// What the runs are charged, until the recording goes or keeps nothing.
    charge: Option<Charge>,
    size: usize,
    /// It grew past [`MOST`], or past what the worker could pay for, or nothing can send it
    /// again: nothing is kept, and it is not to be sent again.
    capped: bool,
    ended: bool,
    /// The body itself, given back by a try that let go of it with none of it gone.
    untouched: Option<RequestBody>,
}

impl Recording {
    /// Keeps a frame's data: a larger one as it is, and a small one as it is too when it is
    /// the first small one in a row; one that follows it is copied onto a run, the first
    /// with it.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if the worker cannot pay for the run.
    fn keep(&mut self, data: &Bytes) -> Result<(), Exhausted> {
        if data.len() >= runs::COPIED_BELOW {
            self.close_run();
            self.kept.push(Kept::Data(data.clone()));
            return Ok(());
        }
        if self.run.is_empty() {
            match self.lone.take() {
                None => {
                    self.lone = Some(data.clone());
                    return Ok(());
                }
                Some(first) => self.copy(&first)?,
            }
        }
        self.copy(data)
    }

    /// Copies `data` onto the run, paying for its room first, and keeps each run that fills.
    fn copy(&mut self, data: &[u8]) -> Result<(), Exhausted> {
        let mut rest = data;
        loop {
            let Self {
                run,
                storage,
                charge,
                ..
            } = self;
            rest = runs::fill(run, rest, |more| match charge {
                Some(charge) => charge.grow(more),
                None => storage.reserve(more).map(|paid| *charge = Some(paid)),
            })?;
            if rest.is_empty() {
                return Ok(());
            }
            self.close_run();
        }
    }

    /// Keeps nothing from now on, and lets go of what was kept and its charge.
    fn cap(&mut self) {
        self.capped = true;
        self.kept = Vec::new();
        self.lone = None;
        self.run = Vec::new();
        self.charge = None;
    }

    /// Keeps the small frames after everything in `kept`, as they stand: the lone one, or
    /// the run.
    fn close_run(&mut self) {
        if let Some(lone) = self.lone.take() {
            self.kept.push(Kept::Data(lone));
        }
        if !self.run.is_empty() {
            let run = std::mem::take(&mut self.run);
            self.kept.push(Kept::Data(Bytes::from(run)));
        }
    }

    /// The body has ended: all of it is kept.
    fn end(&mut self) {
        self.close_run();
        self.ended = true;
    }
}

/// A body on its way, kept as it goes.
#[derive(Debug)]
pub(crate) struct Tee {
    inner: RequestBody,
    recording: Rc<RefCell<Recording>>,
    /// Some of it went: a frame, or an error in its place.
    touched: bool,
}

/// What a [`Tee`] kept, to send again from.
#[derive(Debug, Clone)]
pub(crate) struct Recorded(Rc<RefCell<Recording>>);

impl Tee {
    /// `inner`, kept as it goes, the small frames it copies paid for in `storage`, and where
    /// to find what was kept. A body that has ended before it starts — none at all — is kept
    /// whole already.
    pub(crate) fn new(inner: RequestBody, storage: &Rc<Storage>) -> (Self, Recorded) {
        let recording = Rc::new(RefCell::new(Recording {
            kept: Vec::new(),
            lone: None,
            run: Vec::new(),
            storage: Rc::clone(storage),
            charge: None,
            size: 0,
            capped: false,
            ended: inner.is_end_stream(),
            untouched: None,
        }));
        (
            Self {
                inner,
                recording: Rc::clone(&recording),
                touched: false,
            },
            Recorded(recording),
        )
    }
}

impl Tee {
    /// What it keeps, for one more to send again from: a body kept once serves all who may
    /// send it again — a rule's retry, and an HTTP/2 upstream's resend of a refused stream.
    pub(crate) fn recorded(&self) -> Recorded {
        Recorded(Rc::clone(&self.recording))
    }
}

impl Drop for Tee {
    /// A body none of which went is given back whole, for the next try to send.
    fn drop(&mut self) {
        if self.touched {
            return;
        }
        // Not `borrow_mut`, which panics: nothing holds a borrow of the recording while a
        // body is dropped, so this is not refused, and were it ever, the body would only go
        // unsent rather than take the worker with it.
        if let Ok(mut recording) = self.recording.try_borrow_mut()
            && !recording.ended
        {
            recording.untouched = Some(std::mem::replace(&mut self.inner, RequestBody::None));
        }
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

    /// The body itself, if a try let go of it with none of it gone: once, and kept as it
    /// goes this time.
    pub(crate) fn given_back(&self) -> Option<RequestBody> {
        let inner = self.0.borrow_mut().untouched.take()?;
        Some(RequestBody::Recorded(Box::new(Tee {
            inner,
            recording: Rc::clone(&self.0),
            touched: false,
        })))
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
        this.touched |= matches!(polled, Poll::Ready(Some(_)));
        let mut recording = this.recording.borrow_mut();
        // Nothing can send it again once no handle to the recording is left — as when its
        // answer's head has come — so it keeps nothing more, and lets go of what it kept.
        if !recording.capped && Rc::strong_count(&this.recording) == 1 {
            recording.cap();
        }
        match &polled {
            Poll::Ready(Some(Ok(frame))) if !recording.capped => {
                if let Some(data) = frame.data_ref() {
                    recording.size += data.len();
                    if recording.size > MOST || recording.keep(data).is_err() {
                        recording.cap();
                    }
                } else if let Some(trailers) = frame.trailers_ref() {
                    recording.close_run();
                    recording.kept.push(Kept::Trailers(trailers.clone()));
                }
            }
            Poll::Ready(None) => recording.end(),
            _ => {}
        }
        // A body may say it has ended with its last frame, and whatever sends it may stop
        // there without asking for the end, as the HTTP/2 writer does: it is whole with that
        // frame.
        if matches!(polled, Poll::Ready(Some(Ok(_)))) && this.inner.is_end_stream() {
            recording.end();
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
    /// A body of these frames, as though kept: for tests and the benchmarks.
    #[cfg(any(test, feature = "fuzzing"))]
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

    /// A worker's account with room for anything a test keeps.
    fn ample() -> Rc<Storage> {
        Storage::new(crate::storage::LIMIT)
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
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
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
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        let (sent, _) = read(tee).await;
        assert_eq!(sent.len(), MOST + 1, "the body itself was cut short");
        assert!(recorded.replay().is_none());
        // Exactly at the bound is kept.
        let inner = Replayed::of(vec![Frame::data(Bytes::from(vec![b'x'; MOST]))]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        let _sent = read(tee).await;
        assert!(recorded.replay().is_some());
    }

    /// Small frames are kept copied together and large ones as they are, the same buffer as
    /// the one sent; sent again, the body is the same bytes in the same order, trailers and
    /// all.
    #[tokio::test]
    async fn a_body_of_small_and_large_frames_is_sent_again_the_same() {
        let large = Bytes::from(vec![b'L'; 5_000]);
        let inner = Replayed::of(vec![
            data(b"ab"),
            data(b"c"),
            Frame::data(large.clone()),
            data(b"de"),
            Frame::trailers(trailers()),
        ]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        let mut whole = b"abc".to_vec();
        whole.extend_from_slice(&large);
        whole.extend_from_slice(b"de");
        assert_eq!(read(tee).await, (whole.clone(), Some(trailers())));
        let shared = recorded
            .0
            .borrow()
            .kept
            .iter()
            .any(|kept| matches!(kept, Kept::Data(data) if data.as_ptr() == large.as_ptr()));
        assert!(shared, "the large frame was copied");
        let again = recorded.replay().expect("kept whole");
        assert_eq!(read(again).await, (whole, Some(trailers())));
    }

    /// A body of one small frame is kept as it is, the same buffer as the one sent: only a
    /// small frame that follows another is copied.
    #[tokio::test]
    async fn a_lone_small_frame_is_kept_as_it_is() {
        let small = Bytes::from_static(b"small");
        let inner = Replayed::of(vec![Frame::data(small.clone())]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        let _sent = read(tee).await;
        let recording = recorded.0.borrow();
        assert!(
            matches!(recording.kept.as_slice(), [Kept::Data(data)] if data.as_ptr() == small.as_ptr()),
            "{:?}",
            recording.kept
        );
    }

    /// The runs a recording copies are paid for in the worker's storage for as long as it
    /// keeps them, and a recording the worker cannot pay for is not kept: the body goes on
    /// whole and is not sent again.
    #[tokio::test]
    async fn a_recordings_runs_are_paid_for_or_it_is_not_kept() {
        let bytes = || (0..1024).map(|_| data(b"x")).collect();
        let storage = Storage::with_provision(1 << 20, 0);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(bytes())), &storage);
        assert_eq!(read(tee).await.0.len(), 1024);
        assert_eq!(storage.used(), 1024, "a run of 1,024 bytes");
        assert!(recorded.replay().is_some());
        drop(recorded);
        assert_eq!(storage.used(), 0);

        let storage = Storage::with_provision(512, 0);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(bytes())), &storage);
        assert_eq!(
            read(tee).await.0.len(),
            1024,
            "the body itself was cut short"
        );
        assert!(
            recorded.replay().is_none(),
            "kept what the worker could not pay for"
        );
        assert_eq!(storage.used(), 0);
    }

    /// A recording nothing can send again any more — its handle gone, as once its answer's
    /// head has come — keeps nothing more, and lets go of what it kept and its charge, while
    /// the body goes on whole.
    #[tokio::test]
    async fn a_recording_nothing_can_send_again_lets_go_of_what_it_kept() {
        let storage = Storage::with_provision(1 << 20, 0);
        let frames = (0..1024).map(|_| data(b"x")).collect();
        let (mut tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(frames)), &storage);
        for _ in 0..512 {
            let _sent = tee.frame().await;
        }
        assert!(storage.used() > 0, "the run was not charged");
        drop(recorded);
        let _sent = tee.frame().await;
        assert_eq!(storage.used(), 0, "kept on for nothing");
        assert_eq!(read(tee).await.0.len(), 511, "the body did not go on whole");
    }

    /// What a recording holds is bounded however the body is cut: a body within `MOST` in
    /// one-byte frames, as an HTTP/1 body of one-byte chunks arrives, is kept in no more than
    /// about `MOST`, its list of frames included, or not kept at all.
    #[tokio::test]
    async fn a_body_in_small_frames_is_kept_within_the_bound_or_not_at_all() {
        let frames = (0..MOST).map(|_| data(b"x")).collect();
        let (tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(frames)), &ample());
        let (sent, _) = read(tee).await;
        assert_eq!(sent.len(), MOST);
        let recording = recorded.0.borrow();
        let held = recording.size + recording.kept.capacity() * std::mem::size_of::<Kept>();
        assert!(
            recording.capped || held <= 2 * MOST,
            "{} frames kept, holding {held} bytes for a body of {MOST}",
            recording.kept.len()
        );
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
            let (mut tee, recorded) =
                Tee::new(RequestBody::Replayed(Replayed::of(frames)), &ample());
            while !tee.is_end_stream() {
                let _sent = tee.frame().await;
            }
            drop(tee);
            let again = recorded.replay().expect("kept whole");
            assert_eq!(read(again).await, whole);
        }
    }

    /// A try that lets go of a body none of which went — one that could not connect never
    /// reads it — gives it back whole, and it is kept as it goes the second time.
    #[tokio::test]
    async fn a_body_none_of_which_went_is_given_back_whole() {
        let inner = Replayed::of(vec![data(b"ab"), data(b"cd"), Frame::trailers(trailers())]);
        let (tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        drop(tee);
        assert!(recorded.replay().is_none(), "kept what never went");
        let again = recorded.given_back().expect("given back");
        assert!(recorded.given_back().is_none(), "given back twice");
        assert_eq!(read(again).await, (b"abcd".to_vec(), Some(trailers())));
        let third = recorded.replay().expect("kept the second time");
        assert_eq!(read(third).await, (b"abcd".to_vec(), Some(trailers())));
    }

    /// One some of which went is not: what went cannot be had again from the body.
    #[tokio::test]
    async fn a_body_some_of_which_went_is_not_given_back() {
        let inner = Replayed::of(vec![data(b"ab"), data(b"cd")]);
        let (mut tee, recorded) = Tee::new(RequestBody::Replayed(inner), &ample());
        let _sent = tee.frame().await;
        drop(tee);
        assert!(recorded.given_back().is_none());
        assert!(recorded.replay().is_none());
    }

    #[tokio::test]
    async fn no_body_at_all_is_kept_from_the_start() {
        let (_tee, recorded) = Tee::new(RequestBody::Replayed(Replayed::of(vec![])), &ample());
        let again = recorded.replay().expect("nothing to wait for");
        assert!(again.is_end_stream());
    }
}
