//! A request's body as h2 received it, handed on frame by frame
//! ([15 §3, §5](../../../../../docs/15-http2-and-grpc.md)).
//!
//! **Credit goes back one frame late.** A DATA frame's flow-control credit is returned when
//! the next frame is asked for, not when the frame is handed over. Whoever reads this body
//! asks for the next frame only once the last has gone on — the upstream exchange holds one
//! frame and writes it out whole first — so what an upload holds, in h2 and in hand
//! together, stays within its window. Returning credit on hand-over, as hyper does, measured
//! the same (within 1.5%, 15 §3) and would let a frame in hand sit outside it.
//!
//! Data and trailers stay separate frames, and the body says truthfully whether it has
//! ended: a request whose HEADERS ended the stream has ended before it is read.
//!
//! A body waited on that brings nothing for its idle bound fails with
//! [`RequestBodyError::TimedOut`], which the core answers 408 before its answer has begun.

use crate::downstream::h2::idle::Idle;
use crate::interim::Interim;
use crate::request_body::RequestBodyError;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A request body h2 is receiving.
#[derive(Debug)]
pub(crate) struct IncomingH2 {
    stream: ::h2::RecvStream,
    /// Credit for the frame last handed over, returned when the next is asked for.
    owed: usize,
    /// Every DATA frame has been handed over; what is left is trailers, if any.
    data_done: bool,
    idle: Idle,
    /// Told when the body is wanted and nothing is here, and when the client sent some
    /// without waiting: what decides a `100` of our own (14 §5).
    interim: Option<Interim>,
    /// Whether its trailers are handed over; the request core wants none (03 §11), and
    /// a server's own tests all of them.
    trailers_wanted: bool,
}

impl IncomingH2 {
    /// Has its trailers read to their end and not handed over.
    pub(crate) fn drop_trailers(&mut self) {
        self.trailers_wanted = false;
    }

    /// Whether its trailers are handed over.
    pub(crate) fn wants_trailers(&self) -> bool {
        self.trailers_wanted
    }

    /// The body of a request h2 accepted, which may keep its reader waiting for `idle`
    /// at the most.
    pub(crate) fn new(stream: ::h2::RecvStream, idle: Duration) -> Self {
        Self {
            stream,
            owed: 0,
            data_done: false,
            idle: Idle::new(idle),
            interim: None,
            trailers_wanted: true,
        }
    }

    /// The same, telling `interim` what the continue decision needs to know.
    #[must_use]
    pub(crate) fn heard_by(mut self, interim: Interim) -> Self {
        self.interim = Some(interim);
        self
    }

    /// Pending, unless the wait has run out.
    fn waited(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        match self.idle.waiting(cx) {
            Poll::Ready(()) => Poll::Ready(Some(Err(RequestBodyError::TimedOut))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Body for IncomingH2 {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let this = self.get_mut();
        if this.owed > 0 {
            // Fails only for a stream h2 has already let go of, whose credit is moot.
            let _gone = this
                .stream
                .flow_control()
                .release_capacity(std::mem::take(&mut this.owed));
        }
        if !this.data_done {
            let Poll::Ready(data) = this.stream.poll_data(cx) else {
                if let Some(interim) = &this.interim {
                    interim.body_wanted();
                }
                return this.waited(cx);
            };
            this.idle.moved();
            if let Some(interim) = &this.interim {
                interim.client_sent_body();
            }
            match data {
                Some(Ok(data)) => {
                    this.owed = data.len();
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Some(Err(error)) => {
                    return Poll::Ready(Some(Err(RequestBodyError::from_h2(error))));
                }
                None => this.data_done = true,
            }
        }
        let Poll::Ready(trailers) = this.stream.poll_trailers(cx) else {
            return this.waited(cx);
        };
        this.idle.moved();
        Poll::Ready(match trailers {
            Ok(Some(trailers)) => Some(Ok(Frame::trailers(trailers))),
            Ok(None) => None,
            Err(error) => Some(Err(RequestBodyError::from_h2(error))),
        })
    }

    fn is_end_stream(&self) -> bool {
        self.stream.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        // A length is never promised: trailers may follow any frame, and a declared length
        // is the head's to say, not the body's.
        if self.is_end_stream() {
            SizeHint::with_exact(0)
        } else {
            SizeHint::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downstream::h2::testing::{LONG, locally, locally_paused, pair, post, wire, within};
    use crate::downstream::h2::writer::Outgoing;
    use crate::h2_peer::{self, Peer, kind};
    use http::HeaderMap;
    use http_body_util::BodyExt;

    /// What reading a body gave, frame by frame.
    #[derive(Debug, PartialEq)]
    enum Read {
        Data(&'static str),
        Trailers(HeaderMap),
        Failed(&'static str),
    }

    async fn read(mut body: IncomingH2) -> Vec<Read> {
        let mut seen = Vec::new();
        while let Some(frame) = within(body.frame()).await {
            match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(data) => {
                        seen.push(Read::Data(String::from_utf8(data.to_vec()).unwrap().leak()))
                    }
                    Err(frame) => seen.push(Read::Trailers(frame.into_trailers().unwrap())),
                },
                Err(error) => {
                    seen.push(Read::Failed(match error {
                        RequestBodyError::Incomplete(_) => "incomplete",
                        RequestBodyError::Invalid(_) => "invalid",
                        RequestBodyError::Other(_) => "other",
                        RequestBodyError::TimedOut => "timed out",
                    }));
                    break;
                }
            }
        }
        seen
    }

    fn sum() -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-sum", "7".parse().unwrap());
        trailers
    }

    /// The server's side of one request the h2 client sends, its body wrapped as the data
    /// plane will wrap it.
    async fn received(
        send: &mut ::h2::client::SendRequest<Bytes>,
        server: &mut ::h2::server::Connection<tokio::io::DuplexStream, Outgoing>,
        end_stream: bool,
    ) -> (::h2::SendStream<Bytes>, IncomingH2) {
        let (_response, sending) = send.send_request(post(), end_stream).unwrap();
        let (request, _respond) = within(server.accept()).await.unwrap().unwrap();
        (sending, IncomingH2::new(request.into_body(), LONG))
    }

    #[test]
    fn data_comes_in_order_and_the_body_ends_with_the_stream() {
        locally(async {
            let (mut send, mut server) = pair(&::h2::server::Builder::new()).await;
            let (mut sending, body) = received(&mut send, &mut server, false).await;
            sending
                .send_data(Bytes::from_static(b"hello"), false)
                .unwrap();
            sending
                .send_data(Bytes::from_static(b"world"), true)
                .unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            assert!(!body.is_end_stream(), "ended before it was read");
            assert_eq!(
                read(body).await,
                vec![Read::Data("hello"), Read::Data("world")]
            );
        });
    }

    /// No body is one that has ended before anything is read, which is what has it sent on
    /// with no framing at all.
    #[test]
    fn a_request_ended_by_its_headers_has_ended_before_it_is_read() {
        locally(async {
            let (mut send, mut server) = pair(&::h2::server::Builder::new()).await;
            let (_sending, body) = received(&mut send, &mut server, true).await;
            assert!(body.is_end_stream());
            assert_eq!(body.size_hint().exact(), Some(0));
            assert_eq!(read(body).await, vec![]);
        });
    }

    #[test]
    fn trailers_come_apart_from_the_data_and_may_come_alone() {
        locally(async {
            let (mut send, mut server) = pair(&::h2::server::Builder::new()).await;
            let (mut with_data, first) = received(&mut send, &mut server, false).await;
            with_data
                .send_data(Bytes::from_static(b"hello"), false)
                .unwrap();
            with_data.send_trailers(sum()).unwrap();
            let (mut alone, second) = received(&mut send, &mut server, false).await;
            alone.send_trailers(sum()).unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            assert_eq!(
                read(first).await,
                vec![Read::Data("hello"), Read::Trailers(sum())]
            );
            assert_eq!(read(second).await, vec![Read::Trailers(sum())]);
        });
    }

    /// A client that resets its stream, or loses its connection, fails the body: an end it
    /// never said is never taken for one.
    #[test]
    fn a_reset_or_a_lost_connection_fails_the_body() {
        locally(async {
            let (mut send, mut server) = pair(&::h2::server::Builder::new()).await;
            let (mut sending, mut body) = received(&mut send, &mut server, false).await;
            sending
                .send_data(Bytes::from_static(b"hel"), false)
                .unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            // Taken before the reset: a reset drops what the client still had queued.
            let first = within(body.frame()).await.unwrap().unwrap();
            assert_eq!(first.into_data().unwrap(), Bytes::from_static(b"hel"));
            sending.send_reset(::h2::Reason::CANCEL);
            assert_eq!(read(body).await, vec![Read::Failed("other")]);

            let (near, far) = wire();
            let mut peer = Peer::open_as_client(far, &[]).await;
            let mut server = within(::h2::server::Builder::new().handshake::<_, Outgoing>(near))
                .await
                .unwrap();
            peer.send(&h2_peer::headers(1, h2_peer::request("POST", "/"), false))
                .await;
            peer.send(&h2_peer::data(1, b"hel", false)).await;
            let (request, _respond) = within(server.accept()).await.unwrap().unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            drop(peer);
            let body = IncomingH2::new(request.into_body(), LONG);
            assert_eq!(
                read(body).await,
                vec![Read::Data("hel"), Read::Failed("incomplete")]
            );
        });
    }

    /// 15 §3: a frame's credit goes back when the next frame is asked for, and not when the
    /// frame is handed over, so a frame in hand still counts against the window.
    #[test]
    fn credit_for_a_frame_goes_back_when_the_next_one_is_asked_for() {
        locally(async {
            let (near, far) = wire();
            let mut peer = Peer::open_as_client(far, &[]).await;
            let mut server = within(::h2::server::Builder::new().handshake::<_, Outgoing>(near))
                .await
                .unwrap();
            peer.until(|f| f.kind == kind::SETTINGS).await;
            peer.send(&h2_peer::settings_ack()).await;
            peer.send(&h2_peer::headers(1, h2_peer::request("POST", "/"), false))
                .await;
            for _ in 0..3 {
                peer.send(&h2_peer::data(1, &[0u8; 16_384], false)).await;
            }
            let (request, _respond) = within(server.accept()).await.unwrap().unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            let mut body = IncomingH2::new(request.into_body(), LONG);
            let updates = |frames: Vec<h2_peer::Frame>| -> Vec<(u32, u32)> {
                frames
                    .into_iter()
                    .filter(|f| f.kind == kind::WINDOW_UPDATE && f.stream == 1)
                    .map(|f| (f.stream, f.increment()))
                    .collect()
            };

            let first = within(body.frame()).await.unwrap().unwrap();
            assert_eq!(first.data_ref().map(Bytes::len), Some(16_384));
            assert_eq!(
                updates(peer.settled().await),
                vec![],
                "credit for a frame in hand"
            );
            let _second = within(body.frame()).await.unwrap().unwrap();
            assert_eq!(updates(peer.settled().await), vec![(1, 16_384)]);
            drop(body);
        });
    }

    /// The body's clock runs only while it is asked for and nothing comes, and each frame
    /// that comes starts it afresh: a client that sends within its bound is never cut off,
    /// a reader that stops asking does not charge the client for the time, and a client
    /// that stops is cut off one bound after its last frame.
    #[test]
    fn the_upload_clock_runs_only_while_the_body_is_waited_on_with_nothing() {
        locally_paused(async {
            let idle = std::time::Duration::from_secs(5);
            let (near, far) = wire();
            let mut peer = Peer::open_as_client(far, &[]).await;
            let mut server = ::h2::server::Builder::new()
                .handshake::<_, Outgoing>(near)
                .await
                .unwrap();
            peer.send(&h2_peer::headers(1, h2_peer::request("POST", "/"), false))
                .await;
            let (request, _respond) = server.accept().await.unwrap().unwrap();
            tokio::task::spawn_local(async move { while server.accept().await.is_some() {} });
            let mut body = IncomingH2::new(request.into_body(), idle);
            let started = tokio::time::Instant::now();

            // A frame every four seconds, inside the bound each time.
            let sending = tokio::task::spawn_local(async move {
                for _ in 0..4 {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    peer.send(&h2_peer::data(1, b"x", false)).await;
                }
                peer
            });
            for _ in 0..4 {
                let frame = within(body.frame()).await.unwrap().unwrap();
                assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"x"));
            }
            let _peer = sending.await.unwrap();
            assert_eq!(started.elapsed(), std::time::Duration::from_secs(16));

            // Not asked for twenty seconds: the reader's time, not the client's.
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            let asked = tokio::time::Instant::now();
            // Bounded, so that a clock that never runs out fails here rather than hangs.
            let ended = within(body.frame()).await.unwrap();
            assert!(
                matches!(ended, Err(RequestBodyError::TimedOut)),
                "{ended:?}"
            );
            assert_eq!(
                asked.elapsed(),
                idle,
                "cut off by the bound after it was asked"
            );
        });
    }
}
