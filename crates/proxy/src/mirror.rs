//! A request's body copied to its mirrors as it goes, never holding it up
//! ([03 §6](../../../docs/03-data-plane.md)).
//!
//! Each frame the request's upstream takes is put on each mirror's queue too, the copy the
//! same bytes — a count on a shared buffer — and the mirror takes it from there at its own
//! pace. A small frame that finds another the mirror has not taken yet at the queue's end is
//! copied together with it onto a run ([`crate::runs`]): a mirror that keeps up is handed
//! each frame itself, and one that falls behind holds a few runs rather than an entry for
//! every frame, however small a client made them. A mirror more than [`MOST_BEHIND`] behind
//! is given up on, its copy failing, rather
//! than let it slow the request or hold more: nginx reads the whole body before its mirrors
//! start, and Envoy lets a backed-up mirror push back on the client, and neither is what a
//! copy is for. A copy goes no faster than the request's own upstream reads, and when the
//! request is given up on before its body ends, so are its copies. Whoever sends a copy
//! is told at once that it was given up on ([`Kept::given_up`]), not only when it next
//! reads it.

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
use std::task::{Context, Poll, Waker};

/// The most a mirror may fall behind: the bytes on its queue it has not taken yet.
pub(crate) const MOST_BEHIND: usize = 64 * 1024;

/// Why a copy stopped short.
#[derive(Debug, thiserror::Error)]
#[error("the mirror was given up on: it fell behind, or the request it copies went")]
struct GivenUp;

#[derive(Debug)]
enum Copied {
    /// A frame as it is, and what the worker is charged for it until the mirror takes it,
    /// where its source pays for it no longer ([`RequestBody::unpaid`]).
    Data(Bytes, Option<Charge>),
    /// Small frames copied together, and what the worker is charged for them until the
    /// mirror takes them; the last one on the queue may take more.
    Run(Vec<u8>, Option<Charge>),
    Trailers(HeaderMap),
}

#[derive(Debug)]
struct Queue {
    frames: VecDeque<Copied>,
    /// The worker's account, which pays for the runs, since a copy is paid for by nothing it
    /// was copied from (14 §8), and for the frames whose source pays for them no longer.
    storage: Rc<Storage>,
    /// What the frame the mirror took last is charged, until it asks for the next: as h2's
    /// credit pays for a frame its body has handed on.
    in_hand: Option<Charge>,
    /// Bytes on it not taken yet.
    behind: usize,
    ended: bool,
    given_up: bool,
    /// It was given up on for falling behind, and not because the request went.
    fell_behind: bool,
    /// The copy's reader, waiting for a frame.
    waiting: Option<Waker>,
    /// Whoever waits to be told the copy was given up on.
    watching: Option<Waker>,
}

impl Queue {
    fn give_up(&mut self) {
        self.given_up = true;
        self.frames.clear();
        self.behind = 0;
        for waker in [self.waiting.take(), self.watching.take()]
            .into_iter()
            .flatten()
        {
            waker.wake();
        }
    }

    fn put(&mut self, copied: Copied) {
        self.frames.push_back(copied);
        self.wake_reader();
    }

    /// Puts a frame's data on the queue: as it is, charged `unpaid`, unless it is small and
    /// finds a small frame or a run not taken yet at the queue's end, when it is copied onto
    /// the run — the frame it found with it.
    ///
    /// # Errors
    ///
    /// [`Exhausted`] if the worker cannot pay for the frame or the run.
    fn put_data(&mut self, data: &Bytes, unpaid: usize) -> Result<(), Exhausted> {
        let small = |len: usize| len < runs::COPIED_BELOW;
        let joins = small(data.len())
            && match self.frames.back() {
                Some(Copied::Run(..)) => true,
                Some(Copied::Data(last, _)) => small(last.len()),
                _ => false,
            };
        if !joins {
            let charge = match unpaid {
                0 => None,
                unpaid => Some(self.storage.reserve(unpaid)?),
            };
            self.put(Copied::Data(data.clone(), charge));
            return Ok(());
        }
        // Copied, the frame it found goes, and its charge with it.
        if matches!(self.frames.back(), Some(Copied::Data(..)))
            && let Some(Copied::Data(last, _)) = self.frames.pop_back()
        {
            self.frames.push_back(Copied::Run(Vec::new(), None));
            self.copy(&last)?;
        }
        self.copy(data)?;
        self.wake_reader();
        Ok(())
    }

    /// Copies `data` onto the run at the queue's end, paying for its room first, and starts
    /// runs as each fills.
    fn copy(&mut self, data: &[u8]) -> Result<(), Exhausted> {
        let mut rest = data;
        while !rest.is_empty() {
            let storage = &self.storage;
            match self.frames.back_mut() {
                Some(Copied::Run(run, charge)) if run.len() < runs::RUN => {
                    rest = runs::fill(run, rest, |more| match charge {
                        Some(charge) => charge.grow(more),
                        None => storage.reserve(more).map(|paid| *charge = Some(paid)),
                    })?;
                }
                _ => self.frames.push_back(Copied::Run(Vec::new(), None)),
            }
        }
        Ok(())
    }

    fn wake_reader(&mut self) {
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

    /// Ready once the copy is given up on, for falling behind or because the request went,
    /// whether or not anything is reading it — its reader may be waiting on something else,
    /// such as room its upstream will never give. Never, for a copy that goes whole.
    pub(crate) fn given_up(&self) -> impl Future<Output = ()> + '_ {
        std::future::poll_fn(|cx| {
            let mut queue = self.0.borrow_mut();
            if queue.given_up {
                return Poll::Ready(());
            }
            queue.watching = Some(cx.waker().clone());
            Poll::Pending
        })
    }
}

impl Tee {
    /// `inner`, copied to `mirrors` of them as it goes.
    pub(crate) fn new(
        inner: RequestBody,
        mirrors: usize,
        storage: &Rc<Storage>,
    ) -> (Self, Vec<(Copy, Kept)>) {
        let ended = inner.is_end_stream();
        let queues: Vec<_> = (0..mirrors)
            .map(|_| {
                Rc::new(RefCell::new(Queue {
                    frames: VecDeque::new(),
                    storage: Rc::clone(storage),
                    in_hand: None,
                    behind: 0,
                    ended,
                    given_up: false,
                    fell_behind: false,
                    waiting: None,
                    watching: None,
                }))
            })
            .collect();
        let copies = queues
            .iter()
            .map(|queue| (Copy(Rc::clone(queue)), Kept(Rc::clone(queue))))
            .collect();
        (Self { inner, queues }, copies)
    }

    /// What keeping `data`, a frame it handed on, holds that nothing pays for: what its
    /// body says ([`RequestBody::unpaid`]).
    pub(crate) fn unpaid(&self, data: &Bytes) -> usize {
        self.inner.unpaid(data)
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
        // A body may say it has ended with its last frame, and whatever sends it may stop
        // there without asking for the end, as the HTTP/2 writer does: its copies end with
        // that frame, or the request's own end would leave them given up on.
        let ended = match &polled {
            Poll::Ready(None) => true,
            Poll::Ready(Some(Ok(_))) => this.inner.is_end_stream(),
            Poll::Ready(Some(Err(_))) | Poll::Pending => false,
        };
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
                        } else if queue.put_data(data, this.inner.unpaid(data)).is_ok() {
                            queue.behind += data.len();
                        } else {
                            // Behind further than the worker can pay to hold.
                            queue.fell_behind = true;
                            queue.give_up();
                        }
                    } else if let Some(trailers) = frame.trailers_ref() {
                        queue.put(Copied::Trailers(trailers.clone()));
                    }
                }
                Poll::Ready(Some(Err(_))) => queue.give_up(),
                Poll::Ready(None) | Poll::Pending => {}
            }
            if ended && !queue.given_up {
                queue.ended = true;
                if let Some(waker) = queue.waiting.take() {
                    waker.wake();
                }
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

impl Copy {
    /// What keeping `data`, the frame it handed on last, holds that nothing pays for once
    /// the next is asked for: what it was charged on the queue, which its source, or a run's
    /// copying, made it cost. Nothing for one its source still pays for.
    pub(crate) fn unpaid(&self, _data: &Bytes) -> usize {
        self.0.borrow().in_hand.as_ref().map_or(0, Charge::bytes)
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
        // Asked for the next, the mirror is done with the frame it took last.
        queue.in_hand = None;
        if queue.given_up {
            return Poll::Ready(Some(Err(RequestBodyError::Other(Box::new(GivenUp)))));
        }
        match queue.frames.pop_front() {
            Some(Copied::Data(data, charge)) => {
                queue.behind -= data.len();
                queue.in_hand = charge;
                Poll::Ready(Some(Ok(Frame::data(data))))
            }
            Some(Copied::Run(run, charge)) => {
                queue.behind -= run.len();
                queue.in_hand = charge;
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(run)))))
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

    /// A worker's account with room for anything a test holds.
    fn ample() -> Rc<Storage> {
        Storage::new(crate::storage::LIMIT)
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
            &ample(),
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
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1, &ample());
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
        let (tee, mut copies) = Tee::new(body(vec![data(b"a"), Frame::data(big)]), 2, &ample());
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

    /// A mirror behind on a body of tiny frames holds them copied together, a few runs
    /// rather than an entry a frame, and gets every byte in order; one that keeps up is
    /// handed each frame as it comes, not held back to fill a run.
    #[tokio::test]
    async fn a_mirror_behind_on_small_frames_holds_them_in_runs() {
        let frames = (0..MOST_BEHIND).map(|_| data(b"x")).collect();
        let (tee, mut copies) = Tee::new(body(frames), 2, &ample());
        let (keeping_up, _) = copies.pop().unwrap();
        let (behind, kept) = copies.pop().unwrap();
        let (mut tee, mut keeping_up) = (tee, keeping_up);
        let _first = tee.frame().await.unwrap().unwrap();
        let taken = keeping_up.frame().await.unwrap().unwrap();
        assert_eq!(taken.into_data().unwrap(), "x");
        assert_eq!(read(&mut tee).await.unwrap().0.len(), MOST_BEHIND - 1);
        let held = behind.0.borrow().frames.len();
        assert!(
            held <= MOST_BEHIND / runs::RUN + 1,
            "{held} entries held for {MOST_BEHIND} one-byte frames"
        );
        assert!(!kept.fell_behind());
        assert_eq!(read(behind).await.unwrap().0, vec![b'x'; MOST_BEHIND]);
        assert_eq!(read(keeping_up).await.unwrap().0.len(), MOST_BEHIND - 1);
    }

    /// A mirror that keeps up is handed each frame itself, the same buffer as the one sent,
    /// small or not: nothing is copied for it.
    #[tokio::test]
    async fn a_mirror_keeping_up_is_handed_the_frames_themselves() {
        let sent = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let frames = sent.iter().cloned().map(Frame::data).collect();
        let (mut tee, mut copies) = Tee::new(body(frames), 1, &ample());
        let (mut copy, _kept) = copies.pop().unwrap();
        for frame in &sent {
            let _went = tee.frame().await.unwrap().unwrap();
            let taken = copy.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(
                taken.as_ptr(),
                frame.as_ptr(),
                "copied for a mirror keeping up"
            );
        }
    }

    /// A copy waiting for its next frame is woken by a small one, which joins a run rather
    /// than coming as a frame of its own.
    #[tokio::test]
    async fn a_copy_waiting_is_woken_by_a_small_frame() {
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"a"), data(b"b")]), 1, &ample());
        let (mut copy, _kept) = copies.pop().unwrap();
        let woken = std::sync::Arc::new(Woken::default());
        let waker = Waker::from(std::sync::Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut copy).poll_frame(&mut cx).is_pending());
        let _first = tee.frame().await.unwrap().unwrap();
        assert!(woken.was(), "a waiting copy was not told of a small frame");
    }

    /// A frame put on a mirror's queue as it is is paid for at what its source pays for no
    /// longer (14 §8), until the mirror has taken it and asked for the next; a small one
    /// copied onto a run with the next lets go of its charge for the run's.
    #[tokio::test]
    async fn frames_a_mirror_holds_are_paid_for_what_their_source_pays_no_longer() {
        let storage = Storage::with_provision(1 << 20, 0);
        let (tee, mut copies) = Tee::new(body(vec![data(b"x")]), 1, &storage);
        let (mut copy, _kept) = copies.pop().unwrap();
        let piece = 16 * 1024;
        // As the request's tee puts them, counting what the mirror has yet to take.
        let put = |data: Bytes, unpaid: usize| {
            let mut queue = tee.queues[0].borrow_mut();
            queue.put_data(&data, unpaid).unwrap();
            queue.behind += data.len();
        };
        put(Bytes::from(vec![b'L'; 5_000]), piece);
        put(Bytes::from_static(b"a"), piece);
        assert_eq!(storage.used(), 2 * piece);
        put(Bytes::from_static(b"b"), piece);
        assert_eq!(storage.used(), piece + 2, "copied, a run of two bytes");
        let first = copy.frame().await.unwrap().unwrap();
        assert_eq!(first.data_ref().map(Bytes::len), Some(5_000));
        assert_eq!(storage.used(), piece + 2, "taken, and in hand");
        let first = first.into_data().unwrap();
        assert_eq!(copy.unpaid(&first), piece, "for whoever keeps it after");
        let run = copy.frame().await.unwrap().unwrap();
        assert_eq!(run.data_ref().map(|run| &run[..]), Some(&b"ab"[..]));
        assert_eq!(storage.used(), 2, "the next asked for: the last let go of");
        assert_eq!(copy.unpaid(&run.into_data().unwrap()), 2, "the run's room");
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut copy).poll_frame(&mut cx).is_pending());
        assert_eq!(storage.used(), 0);
        put(Bytes::from(vec![b'L'; 5_000]), 0);
        assert_eq!(storage.used(), 0, "one its source pays for");
        let paid = copy.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(copy.unpaid(&paid), 0);
    }

    /// The runs a mirror that has fallen behind holds are paid for in the worker's storage
    /// until it takes them, and a mirror the worker cannot pay to hold behind is given up on
    /// as one too far behind: the request goes on.
    #[tokio::test]
    async fn a_mirrors_runs_are_paid_for_or_it_is_given_up() {
        let bytes = || (0..1024).map(|_| data(b"x")).collect();
        let storage = Storage::with_provision(1 << 20, 0);
        let (mut tee, mut copies) = Tee::new(body(bytes()), 1, &storage);
        let (copy, kept) = copies.pop().unwrap();
        assert_eq!(read(&mut tee).await.unwrap().0.len(), 1024);
        assert_eq!(storage.used(), 1024, "a run of 1,024 bytes");
        assert_eq!(read(copy).await.unwrap().0.len(), 1024);
        assert_eq!(storage.used(), 0, "the run taken, its charge went with it");
        assert!(!kept.fell_behind());

        let storage = Storage::with_provision(512, 0);
        let (mut tee, mut copies) = Tee::new(body(bytes()), 1, &storage);
        let (copy, kept) = copies.pop().unwrap();
        assert_eq!(
            read(&mut tee).await.unwrap().0.len(),
            1024,
            "the request held up"
        );
        assert!(kept.fell_behind());
        assert!(read(copy).await.is_err());
        assert_eq!(storage.used(), 0);
    }

    #[tokio::test]
    async fn a_request_given_up_on_leaves_its_copies_failing_not_waiting() {
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1, &ample());
        let (copy, kept) = copies.pop().unwrap();
        let _first = tee.frame().await;
        drop(tee);
        assert!(read(copy).await.is_err());
        assert!(!kept.fell_behind(), "not the mirror's doing");
    }

    /// A waker that remembers being woken.
    #[derive(Default)]
    struct Woken(std::sync::atomic::AtomicBool);

    impl std::task::Wake for Woken {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Woken {
        fn was(&self) -> bool {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Whoever holds what became of a copy is told the moment it is given up on, for
    /// falling behind or because the request went, though nothing is reading the copy: its
    /// reader may be waiting on something else — room its upstream will never give — and
    /// would wait there until its own bound. A copy that goes whole is never given up on.
    #[tokio::test]
    async fn a_copy_given_up_on_is_known_to_be_at_once_read_or_not() {
        let woken = std::sync::Arc::new(Woken::default());
        let waker = Waker::from(std::sync::Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        // Too far behind: its reader took the first frame and turned to something else.
        let big = Bytes::from(vec![b'x'; MOST_BEHIND + 1]);
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"a"), Frame::data(big)]), 1, &ample());
        let (mut copy, kept) = copies.pop().unwrap();
        let _first = tee.frame().await;
        let _taken = copy.frame().await;
        let mut told = std::pin::pin!(kept.given_up());
        assert!(told.as_mut().poll(&mut cx).is_pending());
        assert_eq!(read(&mut tee).await.unwrap().0.len(), MOST_BEHIND + 1);
        assert!(woken.was(), "a copy too far behind was given up on untold");
        assert!(told.as_mut().poll(&mut cx).is_ready());
        assert!(kept.fell_behind());

        // The request went before its body ended.
        let woken = std::sync::Arc::new(Woken::default());
        let waker = Waker::from(std::sync::Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let (mut tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1, &ample());
        let (_copy, kept) = copies.pop().unwrap();
        let _first = tee.frame().await;
        let mut told = std::pin::pin!(kept.given_up());
        assert!(told.as_mut().poll(&mut cx).is_pending());
        drop(tee);
        assert!(
            woken.was(),
            "a copy of a request that went was given up on untold"
        );
        assert!(told.as_mut().poll(&mut cx).is_ready());
        assert!(!kept.fell_behind());

        // Whole, and not read at all.
        let (tee, mut copies) = Tee::new(body(vec![data(b"ab"), data(b"cd")]), 1, &ample());
        let (_copy, kept) = copies.pop().unwrap();
        assert_eq!(read(tee).await.unwrap().0, b"abcd");
        let mut cx = Context::from_waker(Waker::noop());
        assert!(std::pin::pin!(kept.given_up()).poll(&mut cx).is_pending());
    }

    /// A body may say it has ended with its last frame, and whatever sends it may stop
    /// there without asking for the end, as the HTTP/2 writer does: its copies end whole,
    /// not given up on as if the request went.
    #[tokio::test]
    async fn a_body_that_says_it_has_ended_ends_its_copies_whole() {
        let bodies = [
            (vec![data(b"ab"), data(b"cd")], (b"abcd".to_vec(), None)),
            (
                vec![data(b"ab"), Frame::trailers(trailers())],
                (b"ab".to_vec(), Some(trailers())),
            ),
        ];
        for (frames, whole) in bodies {
            let (mut tee, mut copies) = Tee::new(body(frames), 1, &ample());
            let (copy, kept) = copies.pop().unwrap();
            while !tee.is_end_stream() {
                let _sent = tee.frame().await;
            }
            drop(tee);
            assert_eq!(read(copy).await.unwrap(), whole);
            assert!(!kept.fell_behind());
        }
    }

    #[tokio::test]
    async fn a_request_without_a_body_is_copied_as_one() {
        let (_tee, mut copies) = Tee::new(body(vec![]), 1, &ample());
        let (copy, _) = copies.pop().unwrap();
        assert!(copy.is_end_stream());
        assert_eq!(read(copy).await.unwrap().0, b"");
    }
}
