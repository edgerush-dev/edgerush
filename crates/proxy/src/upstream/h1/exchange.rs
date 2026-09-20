//! One request and its answer, over one connection.
//!
//! This is where the codec meets a socket. It sends the request and reads the answer at
//! the same time, in one loop over both directions: **a write that cannot proceed never
//! stops the reading**. An upstream that means to refuse an upload says so before it has
//! taken all of it, and an exchange that was busy writing and not listening would wait for
//! room that the upstream is never going to make while the upstream waits for a proxy that
//! is never going to listen. Both would then wait until a deadline, and the client would
//! wait with them ([13 §5](../../../docs/13-http1-upstream.md)).
//!
//! There is one exchange on a connection at a time; nothing here pipelines, and nothing
//! here sends a request twice.

use super::H1Limits;
use super::codec::{
    Asked, BodyReader, BodyWriter, CodecError, Delivery, Framing, Head, HeadReader, Piece,
    ResponseHead, Sending, Trailers, delivery, write_head,
};
use http::{HeaderMap, Method, Uri};
use hyper::body::{Body, Bytes, Frame, SizeHint};
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Sleep, timeout};

/// How much of a request waits in memory to be written. One frame at a time is staged;
/// the body is not asked for more until what it gave has gone.
const STAGING: usize = 16 * 1024;

/// How much is read from the socket at once.
const READING: usize = 16 * 1024;

/// Why an exchange could not be carried through.
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    /// What the upstream sent could not be read.
    #[error("the upstream's answer could not be read: {0}")]
    Codec(#[from] CodecError),
    /// The connection itself failed.
    #[error("the connection to the upstream failed: {0}")]
    Io(#[from] io::Error),
    /// The request's own body could not be read, which is the client's end failing, not
    /// the upstream's.
    #[error("the request body could not be read: {0}")]
    RequestBody(Box<dyn StdError + Send + Sync>),
    /// The upstream went away without answering. The request may have reached it, so
    /// there is nothing to do but say so: it is never sent a second time.
    #[error("the upstream closed the connection without answering")]
    Closed,
    /// More interim answers than an exchange will wait through.
    #[error("the upstream sent more than {limit} interim answers")]
    TooManyInterim {
        /// How many interim answers one exchange may carry.
        limit: usize,
    },
    /// Interim answers coming to more than an exchange will hold.
    #[error("the upstream's interim answers came to more than {limit} bytes")]
    InterimTooLong {
        /// What they may come to together.
        limit: usize,
    },
    /// No final head within the time an exchange has for one, however busy it was.
    #[error("the upstream did not answer within {after:?}")]
    TooSlow {
        /// The time an exchange has to reach a final head.
        after: Duration,
    },
    /// Neither direction moved for long enough that neither is going to.
    #[error("nothing moved on the connection for {after:?}")]
    Idle {
        /// How long nothing may happen.
        after: Duration,
    },
}

/// An upstream's answer, once its head has been read.
#[derive(Debug)]
pub struct Answer {
    /// The final head.
    pub head: ResponseHead,
    /// What delimits the body after it, and whether the connection may be kept.
    pub delivery: Delivery,
    /// How many interim heads came before it. They are consumed and not passed on; the
    /// engine that serves the client has no way to send one ([13 §5]).
    pub interim: usize,
    /// True where the answer arrived before the request had finished going out. What was
    /// left of the request was abandoned, so the connection is out of step and must not be
    /// kept, whatever else says otherwise.
    pub cut_short_the_request: bool,
}

/// One exchange on one connection.
///
/// Dropping it before the answer is complete drops the connection: there is no way to
/// leave it in a state anybody could take over, and pretending otherwise is how a socket
/// with half an answer still on it reaches the next request.
#[derive(Debug)]
pub struct Exchange<S> {
    socket: S,
    /// Read from the socket and not yet used. What is left when the head has been read is
    /// the beginning of the body.
    incoming: Vec<u8>,
    /// Waiting to be written, and how much of it has gone.
    outgoing: Vec<u8>,
    written: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Exchange<S> {
    /// An exchange on `socket`, which nothing has been said on yet.
    pub fn new(socket: S) -> Self {
        Self {
            socket,
            incoming: Vec::new(),
            outgoing: Vec::new(),
            written: 0,
        }
    }

    /// Sends the request and reads back the answer's head, both at once.
    ///
    /// Returns as soon as a final head is whole, whether or not the request had finished
    /// going out. What is left of the answer is its body, which is read from the socket
    /// and the bytes already in hand.
    ///
    /// # Errors
    ///
    /// Anything the upstream said that cannot be read, a connection that failed or closed
    /// without answering, or a request body that could not be read.
    pub async fn send<B>(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        sending: Sending,
        body: B,
        limits: &H1Limits,
    ) -> Result<Answer, ExchangeError>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let by = limits.final_head;
        match timeout(
            by,
            self.exchange(method, uri, headers, sending, body, limits),
        )
        .await
        {
            Ok(answer) => answer,
            Err(_) => Err(ExchangeError::TooSlow { after: by }),
        }
    }

    /// The exchange itself, with the clock kept outside it.
    async fn exchange<B>(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        sending: Sending,
        body: B,
        limits: &H1Limits,
    ) -> Result<Answer, ExchangeError>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        write_head(&mut self.outgoing, method, uri, headers, sending, limits)?;
        let mut writer = BodyWriter::new(sending);
        let mut body = std::pin::pin!(body);
        let mut trailers: Option<HeaderMap> = None;

        let mut reader = HeadReader::default();
        let mut interim = 0;
        let mut interim_bytes = 0;

        loop {
            // What is already in hand comes first. Going back to the socket before
            // looking at it is how an upstream that answers and closes at once has its
            // answer thrown away for a close that had already been overtaken.
            let Head::Read { head, consumed } = reader.read(&self.incoming, limits)? else {
                let round = poll_fn(|cx| self.round(cx, &mut body, &mut writer, &mut trailers));
                match timeout(limits.idle, round).await {
                    Err(_) => return Err(ExchangeError::Idle { after: limits.idle }),
                    Ok(Err(error)) => return Err(error),
                    Ok(Ok(Moved::Read | Moved::Wrote)) => continue,
                    Ok(Ok(Moved::Closed)) => return Err(ExchangeError::Closed),
                }
            };
            if head.status.is_informational() {
                // Consumed and not passed on. The exchange goes on to the final head.
                interim += 1;
                interim_bytes += consumed;
                if interim > limits.interim_heads {
                    return Err(ExchangeError::TooManyInterim {
                        limit: limits.interim_heads,
                    });
                }
                if interim_bytes > limits.interim_bytes {
                    return Err(ExchangeError::InterimTooLong {
                        limit: limits.interim_bytes,
                    });
                }
                self.incoming.drain(..consumed);
                reader = HeadReader::default();
                continue;
            }
            self.incoming.drain(..consumed);
            let delivery = delivery(&head, Asked::from(method))?;
            return Ok(Answer {
                head,
                delivery,
                interim,
                // The answer came first, so what is left of the request will never go.
                cut_short_the_request: !writer.is_done() || self.written < self.outgoing.len(),
            });
        }
    }

    /// One round of whatever can be done: writing what is waiting, taking another frame of
    /// the request, and reading whatever has arrived. Pending only when none of the three
    /// can move, so a blocked write never holds the reading up.
    fn round<B>(
        &mut self,
        cx: &mut Context<'_>,
        body: &mut Pin<&mut B>,
        writer: &mut BodyWriter,
        trailers: &mut Option<HeaderMap>,
    ) -> Poll<Result<Moved, ExchangeError>>
    where
        B: Body<Data = Bytes>,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let mut wrote = false;

        // Take another frame of the request, unless what has been taken is still waiting.
        if !writer.is_done() && self.outgoing.len() - self.written < STAGING {
            match body.as_mut().poll_frame(cx) {
                Poll::Pending => {}
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(ExchangeError::RequestBody(error.into())));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    match frame.into_data() {
                        Ok(data) => writer.data(&mut self.outgoing, &data)?,
                        // Trailers come last and are written with the body's end.
                        Err(frame) => {
                            if let Ok(fields) = frame.into_trailers() {
                                *trailers = Some(fields);
                            }
                        }
                    }
                    wrote = true;
                }
                Poll::Ready(None) => {
                    writer.finish(&mut self.outgoing, trailers.as_ref(), &[])?;
                    wrote = true;
                }
            }
        }
        // Write whatever is waiting.
        while self.written < self.outgoing.len() {
            match Pin::new(&mut self.socket).poll_write(cx, &self.outgoing[self.written..]) {
                Poll::Pending => break,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(gone)) => {
                    self.written += gone;
                    wrote = true;
                }
            }
        }
        if self.written == self.outgoing.len() && self.written > 0 {
            self.outgoing.clear();
            self.written = 0;
        }

        // And read, whatever the writing did. This is the part that must not be skipped.
        let was = self.incoming.len();
        self.incoming.resize(was + READING, 0);
        let mut read = ReadBuf::new(&mut self.incoming[was..]);
        let outcome = Pin::new(&mut self.socket).poll_read(cx, &mut read);
        let filled = read.filled().len();
        self.incoming.truncate(was + filled);
        match outcome {
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) if filled == 0 => Poll::Ready(Ok(Moved::Closed)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(Moved::Read)),
            Poll::Pending if wrote => Poll::Ready(Ok(Moved::Wrote)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// The bytes that came after the head, and the socket they came on. What is left is
    /// the start of the answer's body.
    pub fn into_body_parts(self) -> (S, Vec<u8>) {
        (self.socket, self.incoming)
    }
}

/// The body of an upstream's answer, read as the client asks for it.
///
/// It holds the socket, what was already read from it, and where the reading of the body
/// had got to. Nothing is read until somebody asks for a frame, so a client that is slow
/// to take the answer is an upstream that is slow to be read: the backpressure goes all
/// the way through rather than piling up here.
///
/// Whether the body finished — every framing check passed, every byte accounted for — is
/// [`H1Body::is_complete`]. It is the first of the things a connection must satisfy before
/// it could ever be used again ([13 §6](../../../docs/13-http1-upstream.md)); the rest
/// come with the pool, and until then this connection is closed when the body is dropped.
#[derive(Debug)]
pub struct H1Body<S> {
    /// Gone once the body has failed: there is nothing to be done with a socket whose
    /// message stopped making sense.
    socket: Option<S>,
    /// Read from the socket and not yet handed on.
    buffered: Vec<u8>,
    reader: BodyReader,
    limits: H1Limits,
    /// The upstream closed its end. Told to the reader, which alone knows whether that is
    /// the end of this body or the loss of it.
    ended: bool,
    /// Every check passed and every byte accounted for.
    complete: bool,
    /// Came with the end and has not been handed on yet.
    trailers: Option<HeaderMap>,
    /// How many trailer fields were dropped on the way, for a counter to add up.
    discarded: usize,
    /// Armed only while a read is outstanding, and thrown away the moment anything
    /// arrives. A client that has not asked for the next frame is not an upstream being
    /// slow, so while nobody is waiting on the socket nothing is counted against it.
    waiting: Option<Pin<Box<Sleep>>>,
}

impl<S> H1Body<S> {
    /// A body of `framing`, on `socket`, with `buffered` already read from it.
    pub fn new(socket: S, buffered: Vec<u8>, framing: Framing, limits: H1Limits) -> Self {
        let reader = BodyReader::new(framing);
        // A body that was never going to carry anything is finished before it starts. Said
        // now and not at the first poll, because nothing need ever poll an empty body.
        let complete = reader.is_done();
        Self {
            socket: Some(socket),
            buffered,
            reader,
            limits,
            ended: false,
            complete,
            trailers: None,
            discarded: 0,
            waiting: None,
        }
    }

    /// Whether the whole body arrived and every check on it passed.
    pub fn is_complete(&self) -> bool {
        self.complete && self.trailers.is_none()
    }

    /// How many trailer fields were dropped as fields that may not travel on.
    pub fn discarded_trailers(&self) -> usize {
        self.discarded
    }

    /// What is left of the connection, for whoever may reuse it. `None` once the body has
    /// failed, and never to be taken while the body is unfinished.
    pub fn into_connection(self) -> Option<(S, Vec<u8>)> {
        self.socket.map(|socket| (socket, self.buffered))
    }

    /// Takes the end apart: what the reader said, kept for the frame after this one.
    fn ended_with(&mut self, trailers: Option<Trailers>) {
        self.complete = true;
        if let Some(trailers) = trailers {
            self.discarded = trailers.discarded;
            if !trailers.fields.is_empty() {
                self.trailers = Some(trailers.fields);
            }
        }
    }
}

impl<S: AsyncRead + Unpin> Body for H1Body<S> {
    type Data = Bytes;
    type Error = ExchangeError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ExchangeError>>> {
        let this = self.get_mut();
        loop {
            if this.complete {
                // The trailers are a frame of their own, never folded into the head.
                return match this.trailers.take() {
                    Some(fields) => Poll::Ready(Some(Ok(Frame::trailers(fields)))),
                    None => Poll::Ready(None),
                };
            }
            let Some(socket) = this.socket.as_mut() else {
                return Poll::Ready(None);
            };

            match this.reader.read(&this.buffered, this.ended, &this.limits) {
                Err(error) => {
                    // A body that stopped making sense is not an end; saying so would be
                    // handing the client half an answer as though it were the whole one.
                    this.socket = None;
                    return Poll::Ready(Some(Err(error.into())));
                }
                Ok(Piece::End { trailers, consumed }) => {
                    this.buffered.drain(..consumed);
                    this.ended_with(trailers);
                    continue;
                }
                Ok(Piece::Data { data, consumed }) => {
                    let frame =
                        (!data.is_empty()).then(|| Bytes::copy_from_slice(&this.buffered[data]));
                    this.buffered.drain(..consumed);
                    let Some(frame) = frame else {
                        // Framing bytes and nothing else; keep going.
                        continue;
                    };
                    // Whether that was the last of it is worth knowing now: a client
                    // told how long a body is need never poll it again, and a body whose
                    // end was never checked is one whose connection cannot be trusted.
                    // Asked only where the answer is already certain — reading to find
                    // out would move the reader past bytes still sitting in the buffer.
                    if this.reader.is_spent()
                        && let Ok(Piece::End { trailers, consumed }) =
                            this.reader.read(&this.buffered, this.ended, &this.limits)
                    {
                        this.buffered.drain(..consumed);
                        this.ended_with(trailers);
                    }
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Ok(Piece::More) => {}
            }

            // Only now, and only because somebody asked for a frame.
            let was = this.buffered.len();
            this.buffered.resize(was + READING, 0);
            let mut read = ReadBuf::new(&mut this.buffered[was..]);
            let outcome = Pin::new(socket).poll_read(cx, &mut read);
            let filled = read.filled().len();
            this.buffered.truncate(was + filled);
            match outcome {
                Poll::Pending => {
                    let idle = this.limits.idle;
                    let waiting = this
                        .waiting
                        .get_or_insert_with(|| Box::pin(tokio::time::sleep(idle)));
                    if waiting.as_mut().poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    // Long enough with a read outstanding and nothing to show for it.
                    this.socket = None;
                    return Poll::Ready(Some(Err(ExchangeError::Idle { after: idle })));
                }
                Poll::Ready(Err(error)) => {
                    this.socket = None;
                    return Poll::Ready(Some(Err(error.into())));
                }
                Poll::Ready(Ok(())) => {
                    // Something happened, so the waiting starts again from here.
                    this.waiting = None;
                    if filled == 0 {
                        // The reader is told; it alone knows whether a close ends this
                        // body or loses it.
                        this.ended = true;
                    }
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.complete && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        if self.complete && self.trailers.is_none() {
            return SizeHint::with_exact(0);
        }
        SizeHint::default()
    }
}

/// What a round of an exchange managed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Moved {
    /// Bytes arrived, which may have finished a head.
    Read,
    /// Something went out, and nothing came in.
    Wrote,
    /// The upstream closed its end.
    Closed,
}

impl From<&Method> for Asked {
    fn from(method: &Method) -> Self {
        if method == Method::HEAD {
            Self::Head
        } else {
            Self::Anything
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{Empty, Full};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// A body that gives a frame and then fails, as a client's does when it goes away
    /// part way through sending one.
    struct Failing(bool);

    impl Body for Failing {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, &'static str>>> {
            if self.0 {
                return Poll::Ready(Some(Err("the client went away")));
            }
            self.0 = true;
            let frame = hyper::body::Frame::data(Bytes::from_static(b"ab"));
            Poll::Ready(Some(Ok(frame)))
        }
    }

    /// The other end of the connection: what an upstream would be.
    struct Peer(DuplexStream);

    impl Peer {
        /// Reads until it has seen `mark`, and returns everything up to and including it.
        async fn until(&mut self, mark: &[u8]) -> Vec<u8> {
            let mut seen = Vec::new();
            let mut byte = [0; 1];
            while !seen.ends_with(mark) {
                let read = self.0.read(&mut byte).await.unwrap();
                assert!(read == 1, "the connection ended before {mark:?}");
                seen.push(byte[0]);
            }
            seen
        }

        async fn say(&mut self, bytes: &str) {
            self.0.write_all(bytes.as_bytes()).await.unwrap();
        }
    }

    /// An exchange and the other end of its connection, with `room` bytes of it in flight
    /// before a write has to wait.
    fn connected(room: usize) -> (Exchange<DuplexStream>, Peer) {
        let (ours, theirs) = tokio::io::duplex(room);
        (Exchange::new(ours), Peer(theirs))
    }

    fn headers(fields: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in fields {
            headers.append(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    /// Everything of the answer's body that was already in hand when the head was read.
    fn left_over(exchange: Exchange<DuplexStream>) -> Vec<u8> {
        exchange.into_body_parts().1
    }

    #[tokio::test]
    async fn a_request_goes_out_and_its_answer_comes_back() {
        let (mut exchange, mut peer) = connected(4096);
        let limits = H1Limits::default();
        let sent = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 204 No Content\r\n\r\n").await;
            (head, peer)
        });

        let answer = exchange
            .send(
                &Method::GET,
                &"/a?b=1".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 204);
        assert_eq!(answer.interim, 0);
        assert!(!answer.cut_short_the_request);
        let (head, _peer) = sent.await.unwrap();
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("GET /a?b=1 HTTP/1.1\r\n"), "{head}");
        assert!(head.contains("host: up.test\r\n"), "{head}");
    }

    #[tokio::test]
    async fn a_counted_body_goes_out_with_the_head() {
        let (mut exchange, mut peer) = connected(4096);
        let sent = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            let mut body = vec![0; 5];
            peer.0.read_exact(&mut body).await.unwrap();
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            (head, body)
        });

        let answer = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::Length(5),
                Full::new(Bytes::from_static(b"hello")),
                &H1Limits::default(),
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        assert!(!answer.cut_short_the_request);
        let (head, body) = sent.await.unwrap();
        assert!(
            String::from_utf8(head)
                .unwrap()
                .contains("content-length: 5\r\n")
        );
        assert_eq!(body, b"hello");
        // The two bytes of the answer's body were read along with its head.
        assert_eq!(left_over(exchange), b"ok");
    }

    #[tokio::test]
    async fn a_body_of_unknown_length_goes_out_chunked() {
        let (mut exchange, mut peer) = connected(4096);
        let sent = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            let body = peer.until(b"0\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            (head, body)
        });

        exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::Chunked,
                Full::new(Bytes::from_static(b"hello")),
                &H1Limits::default(),
            )
            .await
            .unwrap();

        let (head, body) = sent.await.unwrap();
        assert!(
            String::from_utf8(head)
                .unwrap()
                .contains("transfer-encoding: chunked\r\n")
        );
        assert_eq!(body, b"5\r\nhello\r\n0\r\n\r\n");
    }

    /// **The reason this reads and writes at once.** An upstream that will not take the
    /// body but does answer must still be heard. With the connection too small to hold
    /// the upload, an exchange that wrote first and listened afterwards would wait for
    /// room the upstream is never going to make, while the upstream waits for nothing at
    /// all — and the client would wait for both of them.
    #[tokio::test]
    async fn an_answer_arrives_though_the_upload_cannot_go() {
        // Room for the head and little else; the body has nowhere to go.
        let (mut exchange, mut peer) = connected(64);
        let upstream = tokio::spawn(async move {
            // It reads the head and then stops reading altogether.
            peer.until(b"\r\n\r\n").await;
            peer.say(
                "HTTP/1.1 413 Payload Too Large\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await;
            peer
        });

        let huge = Bytes::from(vec![b'x'; 512 * 1024]);
        let answer = tokio::time::timeout(
            Duration::from_secs(10),
            exchange.send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::Length(huge.len() as u64),
                Full::new(huge),
                &H1Limits::default(),
            ),
        )
        .await
        .expect("the answer came while the upload was stuck")
        .unwrap();

        assert_eq!(answer.head.status, 413);
        // The request never finished going out, so this connection is out of step.
        assert!(answer.cut_short_the_request);
        assert!(!answer.delivery.persistent);
        drop(upstream);
    }

    #[tokio::test]
    async fn interim_answers_are_consumed_and_counted() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
            peer.say("HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n")
                .await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            peer
        });

        let answer = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &H1Limits::default(),
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        assert_eq!(answer.interim, 2);
        // Nothing of the interim heads is passed on.
        assert!(!answer.head.headers.contains_key("link"));
    }

    #[tokio::test]
    async fn an_upstream_that_never_stops_being_interim_is_given_up_on() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            for _ in 0..100 {
                peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
            }
            peer
        });

        let limits = H1Limits {
            interim_heads: 4,
            ..H1Limits::default()
        };
        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(failed, ExchangeError::TooManyInterim { limit: 4 }),
            "{failed}"
        );
    }

    #[tokio::test]
    async fn an_upstream_that_closes_without_answering_is_not_asked_again() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            // It takes the request, says nothing, and goes.
            peer.until(b"\r\n\r\n").await;
        });

        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &H1Limits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(failed, ExchangeError::Closed), "{failed}");
    }

    #[tokio::test]
    async fn a_request_body_that_fails_stops_the_exchange() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer
        });

        let body = Failing(false);

        let failed = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::Chunked,
                body,
                &H1Limits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(failed, ExchangeError::RequestBody(_)), "{failed}");
    }

    /// Reads a whole body, as a client taking the answer would.
    async fn collected<S: AsyncRead + Unpin>(
        body: &mut H1Body<S>,
    ) -> Result<(Vec<u8>, Option<HeaderMap>), ExchangeError> {
        let mut data = Vec::new();
        let mut trailers = None;
        let mut body = Pin::new(body);
        while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            let frame = frame?;
            match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => {
                    if let Ok(fields) = frame.into_trailers() {
                        trailers = Some(fields);
                    }
                }
            }
        }
        Ok((data, trailers))
    }

    /// A body on a connection whose other end is the test's to write on.
    fn body_on(framing: Framing, buffered: &[u8]) -> (H1Body<DuplexStream>, Peer) {
        let (ours, theirs) = tokio::io::duplex(4096);
        let body = H1Body::new(ours, buffered.to_vec(), framing, H1Limits::default());
        (body, Peer(theirs))
    }

    /// A body that carries nothing is over before anybody asks, because nothing need ever
    /// ask an empty body anything.
    #[tokio::test]
    async fn a_body_of_nothing_is_finished_before_it_is_polled() {
        let (body, _peer) = body_on(Framing::None, b"");
        assert!(body.is_complete());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    }

    #[tokio::test]
    async fn a_counted_body_comes_from_what_was_already_read() {
        let (mut body, _peer) = body_on(Framing::Length(5), b"hello");
        let (data, trailers) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(trailers, None);
        assert!(body.is_complete());
    }

    /// And the rest of it from the socket, a piece at a time, only when asked.
    #[tokio::test]
    async fn a_counted_body_is_read_on_as_it_is_wanted() {
        let (mut body, mut peer) = body_on(Framing::Length(8), b"hel");
        tokio::spawn(async move {
            peer.say("lo").await;
            tokio::task::yield_now().await;
            peer.say(" th").await;
            peer
        });
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello th");
        assert!(body.is_complete());
    }

    #[tokio::test]
    async fn a_chunked_body_comes_back_with_its_trailers() {
        let (mut body, _peer) = body_on(
            Framing::Chunked,
            b"5\r\nhello\r\n0\r\ngrpc-status: 0\r\ncontent-length: 9\r\n\r\n",
        );
        let (data, trailers) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");
        let trailers = trailers.unwrap();
        assert_eq!(trailers["grpc-status"], "0");
        // The one that may not travel was dropped, and counted.
        assert!(!trailers.contains_key("content-length"));
        assert_eq!(body.discarded_trailers(), 1);
        assert!(body.is_complete());
    }

    #[tokio::test]
    async fn a_body_the_close_ends_is_what_came_before_it() {
        let (mut body, peer) = body_on(Framing::UntilClose, b"some");
        tokio::spawn(async move {
            let mut peer = peer;
            peer.say(" bytes").await;
            drop(peer);
        });
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"some bytes");
        assert!(body.is_complete());
    }

    /// A close part way through a body that said how long it would be is a lost answer,
    /// and it is told as one: handing on what arrived would be passing off half an answer
    /// as the whole of it.
    #[tokio::test]
    async fn a_body_cut_short_is_an_error_and_not_a_clean_end() {
        let (mut body, peer) = body_on(Framing::Length(8), b"hel");
        drop(peer);
        let failed = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(failed, ExchangeError::Codec(CodecError::Truncated)),
            "{failed}"
        );
        assert!(!body.is_complete());
        // And the connection is gone: there is nothing to be done with it.
        assert!(body.into_connection().is_none());
    }

    #[tokio::test]
    async fn a_body_that_stops_making_sense_is_an_error() {
        let (mut body, _peer) = body_on(Framing::Chunked, b"zz\r\nhello\r\n");
        let failed = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(failed, ExchangeError::Codec(CodecError::Chunk)),
            "{failed}"
        );
        assert!(!body.is_complete());
    }

    /// A client told how long a body is need never ask again, so the last frame of one
    /// must already have been checked to its end — otherwise a connection whose framing
    /// was never finished could look finished.
    #[tokio::test]
    async fn a_counted_body_is_finished_by_its_last_frame() {
        let (mut body, _peer) = body_on(Framing::Length(5), b"hello");
        let mut pinned = Pin::new(&mut body);
        let frame = poll_fn(|cx| pinned.as_mut().poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"hello"));
        // Without a second poll.
        assert!(body.is_complete());
        assert!(body.is_end_stream());
    }

    /// Nothing is read until a frame is wanted: what an upstream sends sits in its own
    /// socket until the client is ready for it.
    #[tokio::test]
    async fn nothing_is_read_before_a_frame_is_asked_for() {
        let (mut body, mut peer) = body_on(Framing::Length(4), b"");
        peer.say("abcd").await;
        // The bytes are there for the taking and have not been taken.
        assert!(!body.is_complete());

        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"abcd");
    }

    /// An upstream that takes the request, says nothing, and stays. Time is the test's to
    /// move, so nothing here really waits a minute.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_never_answers_is_given_up_on() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            // It holds the connection open and says nothing at all.
            std::future::pending::<()>().await;
            drop(peer);
        });

        let limits = H1Limits::default();
        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        // The idle bound is the shorter of the two, so it is the one that speaks.
        assert!(
            matches!(failed, ExchangeError::Idle { after } if after == limits.idle),
            "{failed}"
        );
    }

    /// An upstream that keeps something happening never goes idle, and is still given up
    /// on: the time an exchange has for a final head is counted from its start and is not
    /// extended by an upstream that stays busy.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_dribbles_does_not_buy_itself_more_time() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            // A byte of a head that never ends, often enough never to be idle.
            loop {
                peer.say("x").await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let limits = H1Limits::default();
        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(failed, ExchangeError::TooSlow { after } if after == limits.final_head),
            "{failed}"
        );
    }

    /// Nor do interim answers, which is the same rule said of the other way an upstream
    /// can look busy without getting anywhere.
    #[tokio::test(start_paused = true)]
    async fn interim_answers_do_not_buy_more_time_either() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            loop {
                peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        // Room for more interim answers than the clock will allow through.
        let limits = H1Limits {
            interim_heads: 1024,
            ..H1Limits::default()
        };
        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(failed, ExchangeError::TooSlow { after } if after == limits.final_head),
            "{failed}"
        );
    }

    /// An upstream that answers in good time is not hurried by any of this.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_answers_in_time_is_left_alone() {
        let (mut exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            tokio::time::sleep(Duration::from_secs(20)).await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            peer
        });

        let answer = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                Sending::None,
                Empty::<Bytes>::new(),
                &H1Limits::default(),
            )
            .await
            .unwrap();
        assert_eq!(answer.head.status, 200);
    }

    /// An upstream that stops part way through a body it promised, while the client is
    /// waiting for the rest, is given up on rather than waited for forever.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_stops_mid_body_is_given_up_on() {
        let (mut body, _peer) = body_on(Framing::Length(8), b"hel");
        let failed = collected(&mut body).await.unwrap_err();
        assert!(matches!(failed, ExchangeError::Idle { .. }), "{failed}");
        assert!(!body.is_complete());
    }

    /// **Only while somebody is waiting.** A client that takes its time between frames is
    /// not an upstream being slow, and the upstream is not punished for it: no read is
    /// outstanding, so nothing is counted. Here the clock moves far past the bound
    /// between one frame and the next, and the body comes through all the same.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_is_slow_to_ask_does_not_time_the_upstream_out() {
        let (mut body, mut peer) = body_on(Framing::Length(8), b"hel");
        peer.say("lo th").await;

        let mut pinned = Pin::new(&mut body);
        let first = poll_fn(|cx| pinned.as_mut().poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.into_data().unwrap(), Bytes::from_static(b"hel"));

        // The client goes away and thinks about it for twice as long as the bound.
        tokio::time::sleep(H1Limits::default().idle * 2).await;

        let (rest, _) = collected(&mut body).await.unwrap();
        assert_eq!(rest, b"lo th");
        assert!(body.is_complete());
    }

    /// And an upstream that is slow but not stopped keeps its connection: the bound is
    /// about nothing arriving, not about taking a while.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_is_merely_slow_is_not_given_up_on() {
        let (mut body, peer) = body_on(Framing::Length(8), b"");
        tokio::spawn(async move {
            let mut peer = peer;
            for piece in ["he", "ll", "o ", "th"] {
                tokio::time::sleep(H1Limits::default().idle / 2).await;
                peer.say(piece).await;
            }
            peer
        });

        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello th");
        assert!(body.is_complete());
    }
}
