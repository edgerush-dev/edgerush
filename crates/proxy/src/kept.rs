//! What keeps a request's body for later — the retry recording and a mirror's copy — driven
//! end to end from frames the caller gives, for the benchmarks (`benches/kept.rs`), so that
//! the body and its keepers stay private to the crate.

use crate::mirror;
use crate::request_body::RequestBody;
use crate::retry::replay::{Replayed, Tee};
use bytes::Bytes;
use http_body::{Body, Frame};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// A request's body made of the data frames given, every one of them ready at once.
#[derive(Debug)]
pub struct Frames(RequestBody);

impl Frames {
    /// A body of `data`, a frame each.
    #[must_use]
    pub fn new(data: Vec<Bytes>) -> Self {
        let frames = data.into_iter().map(Frame::data).collect();
        Self(RequestBody::Replayed(Replayed::of(frames)))
    }
}

/// Sends `body` on as a request whose rule may retry it, kept as it goes, then sends what
/// was kept once more; the frames given in all.
#[must_use]
pub fn recorded(body: Frames) -> usize {
    let (mut tee, recorded) = Tee::new(body.0);
    let sent = drain(&mut tee);
    drop(tee);
    let again = recorded
        .replay()
        .map_or(0, |mut replayed| drain(&mut replayed));
    sent + again
}

/// Sends `body` on with one mirror's copy beside it, the copy read only once the body has
/// gone, as a mirror that has fallen behind reads it; the frames given in all.
#[must_use]
pub fn mirrored(body: Frames) -> usize {
    let (mut tee, mut copies) = mirror::Tee::new(body.0, 1);
    let sent = drain(&mut tee);
    let copied = copies.pop().map_or(0, |(mut copy, _kept)| drain(&mut copy));
    sent + copied
}

/// Takes every frame `body` has ready; how many there were.
fn drain(body: &mut (impl Body<Data = Bytes> + Unpin)) -> usize {
    let mut cx = Context::from_waker(Waker::noop());
    let mut frames = 0;
    while let Poll::Ready(Some(Ok(_))) = Pin::new(&mut *body).poll_frame(&mut cx) {
        frames += 1;
    }
    frames
}
