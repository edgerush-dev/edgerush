//! An answer sent on an HTTP/2 stream, within the capacity h2 grants
//! ([15 §3, §5](../../../../../docs/15-http2-and-grpc.md)).
//!
//! h2's `send_data` takes any amount and holds what the client's window does not allow, so
//! nothing is handed to it before it has granted the room: capacity is asked for, awaited,
//! and a frame is cut to what was granted. What h2 holds is then at most its send buffer a
//! stream, and every piece handed over carries its storage [`Charge`], which goes when h2
//! drops the piece — once it is written, or the stream is gone. So the ledger counts what
//! waits in h2 to the byte, for as long as it waits.
//!
//! The last DATA frame carries END_STREAM, and trailers end the stream themselves: no empty
//! frame is added to say what the last one could have said. A client that resets the stream
//! ends the sending at once, while it waits for the body or for room, and the body is dropped
//! with it. A body that fails resets the stream, so a cut answer is never taken for a whole
//! one.
//!
//! Interim heads go only before the final head: h2 would send one after it, on a stream it
//! has closed (15 §3), and [`Responder`] refuses it instead.

use crate::storage::{Charge, Exhausted, Storage};
use bytes::{Buf, Bytes};
use http::Response;
use http_body::Body;
use std::error::Error as StdError;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

/// The most handed to h2 at once: one frame of the size every client accepts (RFC 9113
/// §4.2).
const PIECE: usize = 16_384;

/// A piece of an answer handed to h2, with what pays for it until h2 lets it go.
#[derive(Debug)]
pub(crate) struct Outgoing {
    bytes: Bytes,
    _charge: Option<Charge>,
}

impl Outgoing {
    fn charged(bytes: Bytes, charge: Charge) -> Self {
        Self {
            bytes,
            _charge: Some(charge),
        }
    }

    fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            _charge: None,
        }
    }
}

impl Buf for Outgoing {
    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.bytes.chunk()
    }

    fn advance(&mut self, count: usize) {
        self.bytes.advance(count);
    }
}

/// Why an answer could not be sent whole.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SendError {
    /// The client reset the stream.
    #[error("the client reset the stream: {0:?}")]
    Reset(::h2::Reason),
    /// The stream can take nothing more, the connection having gone with it.
    #[error("the stream was closed before the answer was sent")]
    Closed,
    /// h2 refused what it was given.
    #[error("h2 refused the answer: {0}")]
    H2(#[from] ::h2::Error),
    /// The answer's own body failed part way.
    #[error("the answer's body failed")]
    Body(#[source] Box<dyn StdError + Send + Sync>),
    /// The worker could not pay for a piece.
    #[error(transparent)]
    Exhausted(#[from] Exhausted),
    /// An interim head after the final head, or a second final head.
    #[error("the final head has already been sent")]
    AfterFinal,
}

/// A stream's heads: interim ones, then one final head.
#[derive(Debug)]
pub(crate) struct Responder {
    respond: ::h2::server::SendResponse<Outgoing>,
    final_sent: bool,
}

impl Responder {
    /// The heads of the stream `respond` answers.
    pub(crate) fn new(respond: ::h2::server::SendResponse<Outgoing>) -> Self {
        Self {
            respond,
            final_sent: false,
        }
    }

    /// Ready once the client has reset the stream, watching for it until then.
    pub(crate) fn poll_reset(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.respond.poll_reset(cx).map(|_| ())
    }

    /// Sends an interim head, before the final one only.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "interim heads reach HTTP/2 clients in 15 step 2's third slice"
        )
    )]
    pub(crate) fn interim(&mut self, head: Response<()>) -> Result<(), SendError> {
        if self.final_sent {
            return Err(SendError::AfterFinal);
        }
        Ok(self.respond.send_informational(head)?)
    }

    /// Sends the final head, ending the stream with it if there is no body to follow.
    pub(crate) fn final_head(
        &mut self,
        head: Response<()>,
        end_stream: bool,
    ) -> Result<::h2::SendStream<Outgoing>, SendError> {
        if self.final_sent {
            return Err(SendError::AfterFinal);
        }
        self.final_sent = true;
        Ok(self.respond.send_response(head, end_stream)?)
    }
}

/// Sends `body` on `stream`, whose final head has gone without ending it, to the body's
/// end. On failure the stream is reset unless the client reset it first.
pub(crate) async fn send_body<B>(
    stream: &mut ::h2::SendStream<Outgoing>,
    body: B,
    storage: &Rc<Storage>,
) -> Result<(), SendError>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let sent = sending(stream, body, storage).await;
    if let Err(SendError::Body(_) | SendError::Exhausted(_) | SendError::H2(_)) = &sent {
        stream.send_reset(::h2::Reason::INTERNAL_ERROR);
    }
    sent
}

async fn sending<B>(
    stream: &mut ::h2::SendStream<Outgoing>,
    body: B,
    storage: &Rc<Storage>,
) -> Result<(), SendError>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let mut body = std::pin::pin!(body);
    loop {
        let frame = poll_fn(|cx| {
            ready!(unless_reset(stream, cx))?;
            body.as_mut().poll_frame(cx).map(Ok::<_, SendError>)
        })
        .await?;
        let frame = match frame {
            None => {
                stream.send_data(Outgoing::empty(), true)?;
                return Ok(());
            }
            Some(Err(error)) => return Err(SendError::Body(error.into())),
            Some(Ok(frame)) => frame,
        };
        match frame.into_data() {
            Ok(data) => {
                let end = body.is_end_stream();
                send_data(stream, data, end, storage).await?;
                if end {
                    return Ok(());
                }
            }
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    stream.send_trailers(trailers)?;
                    return Ok(());
                }
            }
        }
    }
}

/// Hands `data` to h2 a granted piece at a time, the last piece ending the stream if `end`.
async fn send_data(
    stream: &mut ::h2::SendStream<Outgoing>,
    mut data: Bytes,
    end: bool,
    storage: &Rc<Storage>,
) -> Result<(), SendError> {
    if data.is_empty() {
        if end {
            stream.send_data(Outgoing::empty(), true)?;
        }
        return Ok(());
    }
    while !data.is_empty() {
        stream.reserve_capacity(data.len());
        let granted = poll_fn(|cx| granted(stream, cx)).await?;
        // No larger than a frame: h2 keeps a piece until its last byte is written, while it
        // grants room again as each frame of it goes, so a larger piece would be paid for
        // beside the next.
        let piece = data.split_to(granted.min(data.len()).min(PIECE));
        let charge = storage.reserve(piece.len())?;
        stream.send_data(Outgoing::charged(piece, charge), end && data.is_empty())?;
    }
    Ok(())
}

/// Room h2 has granted, waiting for some if there is none, and for a reset meanwhile.
fn granted(
    stream: &mut ::h2::SendStream<Outgoing>,
    cx: &mut Context<'_>,
) -> Poll<Result<usize, SendError>> {
    ready!(unless_reset(stream, cx))?;
    loop {
        let capacity = stream.capacity();
        if capacity > 0 {
            return Poll::Ready(Ok(capacity));
        }
        match ready!(stream.poll_capacity(cx)) {
            Some(Ok(_)) => {}
            Some(Err(error)) => return Poll::Ready(Err(error.into())),
            None => return Poll::Ready(Err(SendError::Closed)),
        }
    }
}

/// Ready with an error if the client has reset the stream, and ready with nothing to say
/// otherwise; the reset is watched for from here on either way.
fn unless_reset(
    stream: &mut ::h2::SendStream<Outgoing>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), SendError>> {
    match stream.poll_reset(cx) {
        Poll::Ready(Ok(reason)) => Poll::Ready(Err(SendError::Reset(reason))),
        Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
        Poll::Pending => Poll::Ready(Ok(())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downstream::h2::body::IncomingH2;
    use crate::downstream::h2::testing::{locally, pair, post, serving, wire, within};
    use crate::h2_peer::{self, Frame as Wire, Peer, code, flag, kind, setting};
    use http::HeaderMap;
    use http_body::{Frame, SizeHint};
    use http_body_util::BodyExt;
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use tokio::io::DuplexStream;
    use tokio::sync::mpsc;

    type Accepted = (
        http::Request<::h2::RecvStream>,
        ::h2::server::SendResponse<Outgoing>,
    );

    /// A body a test hands out frame by frame; it says it has ended once it has nothing
    /// left, and may fail where the script says. Its drop is seen.
    struct Scripted {
        frames: VecDeque<Result<Frame<Bytes>, &'static str>>,
        dropped: Rc<Cell<bool>>,
    }

    impl Scripted {
        fn new(frames: Vec<Result<Frame<Bytes>, &'static str>>) -> (Self, Rc<Cell<bool>>) {
            let dropped = Rc::new(Cell::new(false));
            (
                Self {
                    frames: frames.into(),
                    dropped: Rc::clone(&dropped),
                },
                dropped,
            )
        }
    }

    impl Drop for Scripted {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    impl Body for Scripted {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
            Poll::Ready(self.get_mut().frames.pop_front())
        }

        fn is_end_stream(&self) -> bool {
            self.frames.is_empty()
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    fn data(bytes: &'static [u8]) -> Result<Frame<Bytes>, &'static str> {
        Ok(Frame::data(Bytes::from_static(bytes)))
    }

    fn sum() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-sum", "7".parse().unwrap());
        trailers
    }

    /// An h2 server whose accepted streams come out of the receiver, driven in the
    /// background, and the scripted client on the other end of `io`, which has asked for
    /// `settings` and acknowledged the server's.
    async fn scripted_client(
        io: (DuplexStream, DuplexStream),
        settings: &[(u16, u32)],
    ) -> (Peer<DuplexStream>, mpsc::UnboundedReceiver<Accepted>) {
        let (near, far) = io;
        let mut peer = Peer::open_as_client(far, settings).await;
        let mut builder = ::h2::server::Builder::new();
        builder.max_send_buffer_size(65_536);
        let mut server = within(builder.handshake::<_, Outgoing>(near))
            .await
            .unwrap();
        peer.until(|f| f.kind == kind::SETTINGS && !f.has(flag::ACK))
            .await;
        peer.send(&h2_peer::settings_ack()).await;
        let (tx, accepted) = mpsc::unbounded_channel();
        tokio::task::spawn_local(async move {
            while let Some(Ok(stream)) = server.accept().await {
                let _ = tx.send(stream);
            }
        });
        (peer, accepted)
    }

    /// The server's side of stream 1, which the scripted client opens with a GET.
    async fn stream_one(
        peer: &mut Peer<DuplexStream>,
        accepted: &mut mpsc::UnboundedReceiver<Accepted>,
    ) -> Responder {
        peer.send(&h2_peer::headers(1, h2_peer::request("GET", "/"), true))
            .await;
        let (_request, respond) = within(accepted.recv()).await.unwrap();
        Responder::new(respond)
    }

    /// What reached the client on stream 1: each frame's kind and whether it ended the
    /// stream, or the reset's code.
    fn on_stream_one(frames: &[Wire]) -> Vec<(u8, bool, Option<u32>)> {
        frames
            .iter()
            .filter(|f| f.stream == 1)
            .map(|f| {
                let reset = (f.kind == kind::RST_STREAM).then(|| f.reset());
                (f.kind, f.has(flag::END_STREAM), reset)
            })
            .collect()
    }

    #[test]
    fn an_answer_with_no_body_is_its_head_alone() {
        locally(async {
            let (mut peer, mut accepted) = scripted_client(wire(), &[]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            responder.final_head(Response::new(()), true).unwrap();
            assert_eq!(
                on_stream_one(&peer.settled().await),
                vec![(kind::HEADERS, true, None)]
            );
        });
    }

    /// The last DATA frame ends the stream; no empty frame follows to say so.
    #[test]
    fn the_last_data_frame_ends_the_stream() {
        locally(async {
            let storage = Storage::new(1 << 20);
            let (mut peer, mut accepted) = scripted_client(wire(), &[]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let mut stream = responder.final_head(Response::new(()), false).unwrap();
            let (body, _) = Scripted::new(vec![data(b"hello"), data(b"world")]);
            within(send_body(&mut stream, body, &storage))
                .await
                .unwrap();
            assert_eq!(
                on_stream_one(&peer.settled().await),
                vec![
                    (kind::HEADERS, false, None),
                    (kind::DATA, false, None),
                    (kind::DATA, true, None)
                ]
            );
            assert_eq!(storage.used(), 0, "a charge outlived its piece");
        });
    }

    #[test]
    fn trailers_end_the_answer() {
        locally(async {
            let storage = Storage::new(1 << 20);
            let (mut peer, mut accepted) = scripted_client(wire(), &[]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let mut stream = responder.final_head(Response::new(()), false).unwrap();
            let (body, _) = Scripted::new(vec![data(b"hello"), Ok(Frame::trailers(sum()))]);
            within(send_body(&mut stream, body, &storage))
                .await
                .unwrap();
            assert_eq!(
                on_stream_one(&peer.settled().await),
                vec![
                    (kind::HEADERS, false, None),
                    (kind::DATA, false, None),
                    (kind::HEADERS, true, None)
                ]
            );
        });
    }

    /// A body that fails part way resets the stream: the client never sees an end.
    #[test]
    fn a_body_that_fails_resets_the_stream() {
        locally(async {
            let storage = Storage::new(1 << 20);
            let (mut peer, mut accepted) = scripted_client(wire(), &[]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let mut stream = responder.final_head(Response::new(()), false).unwrap();
            let (body, _) = Scripted::new(vec![data(b"hel"), Err("the upstream went away")]);
            let sent = within(send_body(&mut stream, body, &storage)).await;
            assert!(matches!(sent, Err(SendError::Body(_))), "{sent:?}");
            // Whatever of it had gone before the reset, the stream never ends cleanly: h2
            // drops a reset stream's frames still queued.
            let seen = on_stream_one(&peer.settled().await);
            assert!(seen.iter().all(|(_, end, _)| !end), "{seen:?}");
            assert_eq!(
                seen.last(),
                Some(&(kind::RST_STREAM, false, Some(code::INTERNAL_ERROR)))
            );
        });
    }

    /// Nothing is handed to h2 past the room the client granted; what h2 holds is paid for
    /// while it holds it, and paid back once written. What it holds is the stream's send
    /// buffer and, once a frame has left the stream's queue for the connection's, that frame
    /// as it is written: the send buffer and one frame.
    #[test]
    fn only_granted_room_is_handed_over_and_what_waits_in_h2_is_paid_for() {
        locally(async {
            let storage = Storage::new(1 << 20);
            // A connection that takes 4 KiB at a time, so that h2 cannot write out what it
            // is given until the client reads.
            let (mut peer, mut accepted) = scripted_client(
                tokio::io::duplex(4096),
                &[(setting::INITIAL_WINDOW_SIZE, 16_384)],
            )
            .await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let mut stream = responder.final_head(Response::new(()), false).unwrap();
            // Frames larger than the window, so that a frame handed over whole would be more
            // than was granted.
            let frames = (0..4).map(|_| Ok(Frame::data(Bytes::from(vec![7u8; 65_536]))));
            let (body, dropped) = Scripted::new(frames.collect());
            let paid = Rc::clone(&storage);
            let sending =
                tokio::task::spawn_local(async move { send_body(&mut stream, body, &paid).await });

            // The window is 16 KiB: that much is handed over, and it waits in h2, paid for,
            // behind a connection that will not take it.
            for _ in 0..1000 {
                if storage.used() > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            // Given every chance to take more, it has not.
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            assert_eq!(storage.used(), 16_384, "what h2 holds, to the byte");

            // The client reads, and grants room for the rest.
            let mut received = 0;
            peer.send(&h2_peer::window_update(0, 1 << 20)).await;
            peer.send(&h2_peer::window_update(1, 1 << 20)).await;
            while received < 4 * 65_536 {
                let frame = peer.next().await;
                if frame.kind == kind::DATA && frame.stream == 1 {
                    received += frame.payload.len();
                    let bound = 65_536 + 16_384;
                    assert!(
                        storage.used() <= bound,
                        "past the bound: {}",
                        storage.used()
                    );
                }
            }
            within(sending).await.unwrap().unwrap();
            assert!(dropped.get());
            assert_eq!(storage.used(), 0);
        });
    }

    /// A client that resets the stream while the answer waits for room ends the sending at
    /// once, and the body goes with it: whatever it held upstream is let go.
    #[test]
    fn a_reset_while_waiting_for_room_ends_the_sending_and_drops_the_body() {
        locally(async {
            let storage = Storage::new(1 << 20);
            let (mut peer, mut accepted) =
                scripted_client(wire(), &[(setting::INITIAL_WINDOW_SIZE, 0)]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let mut stream = responder.final_head(Response::new(()), false).unwrap();
            let (body, dropped) = Scripted::new(vec![data(b"never sent")]);
            let paid = Rc::clone(&storage);
            let sending =
                tokio::task::spawn_local(async move { send_body(&mut stream, body, &paid).await });
            peer.settled().await;
            assert!(!sending.is_finished(), "sent into a window of nothing");
            peer.send(&h2_peer::rst_stream(1, code::CANCEL)).await;
            let sent = within(sending).await.unwrap();
            assert!(
                matches!(sent, Err(SendError::Reset(reason)) if reason == ::h2::Reason::CANCEL),
                "{sent:?}"
            );
            assert!(dropped.get(), "the body outlived the stream");
            assert_eq!(storage.used(), 0);
        });
    }

    /// An interim head goes before the final one and is refused after it, where h2 would
    /// put a head on a closed stream.
    #[test]
    fn an_interim_head_goes_before_the_final_one_and_never_after() {
        locally(async {
            let (mut peer, mut accepted) = scripted_client(wire(), &[]).await;
            let mut responder = stream_one(&mut peer, &mut accepted).await;
            let hint = Response::builder().status(103).body(()).unwrap();
            responder.interim(hint).unwrap();
            responder.final_head(Response::new(()), true).unwrap();
            let late = Response::builder().status(103).body(()).unwrap();
            assert!(matches!(
                responder.interim(late),
                Err(SendError::AfterFinal)
            ));
            assert!(matches!(
                responder.final_head(Response::new(()), true),
                Err(SendError::AfterFinal)
            ));
            assert_eq!(
                on_stream_one(&peer.settled().await),
                vec![(kind::HEADERS, false, None), (kind::HEADERS, true, None)]
            );
        });
    }

    /// Reads what the client receives on `response` to its end, returning credit as it goes.
    async fn received(response: ::h2::client::ResponseFuture) -> usize {
        let mut body = within(response).await.unwrap().into_body();
        let mut length = 0;
        while let Some(data) = within(body.data()).await {
            let data = data.unwrap();
            length += data.len();
            body.flow_control().release_capacity(data.len()).unwrap();
        }
        length
    }

    /// Both halves of a stream move on their own: an answer streams out while its upload is
    /// held back unread, and an upload is read to its end while the client takes none of the
    /// answer.
    #[test]
    fn both_halves_progress_while_the_other_is_held_back() {
        locally(async {
            let storage = Storage::new(1 << 20);
            let mut builder = ::h2::server::Builder::new();
            builder
                .initial_window_size(65_535)
                .initial_connection_window_size(1 << 20)
                .max_send_buffer_size(65_536);
            let (mut send, server) = pair(&builder).await;
            let mut accepted = serving(server);
            let quarter_mib = || Bytes::from(vec![1u8; 256 * 1024]);
            let answer = || {
                let frames = (0..16).map(|_| Ok(Frame::data(Bytes::from(vec![2u8; 16_384]))));
                Scripted::new(frames.collect()).0
            };

            // The upload is never read, and the answer arrives whole all the same.
            let (response, mut upload) = send.send_request(post(), false).unwrap();
            upload.send_data(quarter_mib(), true).unwrap();
            let (request, respond) = within(accepted.recv()).await.unwrap();
            let held_back = IncomingH2::new(request.into_body());
            let mut stream = Responder::new(respond)
                .final_head(Response::new(()), false)
                .unwrap();
            let paid = Rc::clone(&storage);
            let answering =
                tokio::task::spawn_local(
                    async move { send_body(&mut stream, answer(), &paid).await },
                );
            assert_eq!(received(response).await, 256 * 1024);
            within(answering).await.unwrap().unwrap();

            // The answer is never read, and the upload arrives whole all the same.
            let (_unread, mut upload) = send.send_request(post(), false).unwrap();
            upload.send_data(quarter_mib(), true).unwrap();
            let (request, respond) = within(accepted.recv()).await.unwrap();
            let mut stream = Responder::new(respond)
                .final_head(Response::new(()), false)
                .unwrap();
            let paid = Rc::clone(&storage);
            let answering =
                tokio::task::spawn_local(
                    async move { send_body(&mut stream, answer(), &paid).await },
                );
            let uploaded = within(BodyExt::collect(IncomingH2::new(request.into_body())))
                .await
                .unwrap();
            assert_eq!(uploaded.to_bytes().len(), 256 * 1024);
            assert!(
                !answering.is_finished(),
                "the client took an answer it never read"
            );
            drop(held_back);
        });
    }
}
