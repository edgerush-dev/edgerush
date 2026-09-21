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
use super::pool::Lease;
use http::{HeaderMap, HeaderName, Method, StatusCode, Uri};
use hyper::body::{Body, Bytes, Frame, SizeHint};
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep, timeout, timeout_at};

/// How much of a request waits in memory to be written. One frame at a time is staged;
/// the body is not asked for more until what it gave has gone.
const STAGING: usize = 16 * 1024;

/// How much is read from the socket at once.
const READING: usize = 16 * 1024;

/// How many turns of staging one push may take before it gives the socket a chance.
const ROUNDS: usize = 8;

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
    /// The upstream had said something before it was asked anything.
    #[error("the upstream said something before it was asked")]
    Unsolicited,
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
    /// What the exchange was waiting for did not happen for long enough that it is not
    /// going to.
    #[error("{waiting} for {after:?}")]
    Idle {
        /// How long the thing being waited for may not happen.
        after: Duration,
        /// Which of the three it was, so that a counter can say where exchanges die
        /// without a label a backend could invent.
        waiting: Stalled,
    },
}

/// The request, as far as it has been sent.
///
/// It outlives the final head: an upstream may answer before it has taken all of a body,
/// and stopping there would leave an echo server waiting for bytes that were never going
/// to come while the client waited for an answer that was never going to finish.
#[derive(Debug)]
pub struct Upload<B> {
    body: B,
    writer: BodyWriter,
    trailers: Option<HeaderMap>,
    /// The one frame being forwarded, and how much of it has been staged. One at a time
    /// and never a queue of them: a client that is faster than the upstream would
    /// otherwise be held in this buffer rather than slowed down by it.
    pending: Option<(Bytes, usize)>,
    /// What the request's own `Connection` nominated, which does not travel on as a
    /// trailer either. Kept from before the head was stripped.
    nominated: Vec<HeaderName>,
    /// Nothing more of the request will be sent, whether because it was all sent or
    /// because there is no longer anywhere for it to go.
    stopped: bool,
}

impl<B> Upload<B> {
    /// A request to be sent as `sending` says, whose `Connection` named these fields.
    pub fn new(body: B, sending: Sending, nominated: Vec<HeaderName>) -> Self {
        Self {
            body,
            writer: BodyWriter::new(sending),
            trailers: None,
            pending: None,
            nominated,
            stopped: false,
        }
    }
}

impl<S, B> Rest<S, B> {
    /// Whether every byte of the request went out: encoded to the last byte **and**
    /// written to the socket.
    ///
    /// An encoder that has finished is not a request that has arrived. A connection with
    /// bytes still queued is one whose upstream is part way through reading a request,
    /// and writing the next one into it would join the two together.
    pub fn upload_finished(&self) -> bool {
        self.upload.finished() && self.exchange.nothing_queued()
    }
}

impl<B> Upload<B> {
    /// Whether the request has been encoded to its last byte. Not the same as its
    /// having gone: what is queued is [`Exchange::nothing_queued`]'s business.
    fn finished(&self) -> bool {
        self.writer.is_done() && self.pending.is_none()
    }

    /// Gives up on the rest of the request. What has not gone will never go, so the
    /// connection is out of step and is not to be used again.
    fn abandon(&mut self) {
        self.stopped = true;
    }
}

/// What is left of an exchange once its final head has been read: the connection, what was
/// read past the head, and a request that may still be going out.
#[derive(Debug)]
pub struct Rest<S, B> {
    exchange: Exchange<S>,
    upload: Upload<B>,
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
    /// What this answer's own `Connection` named as its own. Read here, because by the
    /// time the hop-by-hop fields have been taken off there is nothing left to read.
    pub nominated: Vec<HeaderName>,
    /// Whether this answer is one that says to stop sending the request: a refusal that
    /// also closes the connection.
    ///
    /// A head arriving early says nothing by itself — an echo answers at once — and
    /// neither does a status alone, nor a `Connection: close` on an answer that is going
    /// well ([13 §5](../../../docs/13-http1-upstream.md)).
    pub stop_uploading: bool,
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

impl<S> Exchange<S> {
    /// Whether everything encoded has left for the socket.
    fn nothing_queued(&self) -> bool {
        self.written >= self.outgoing.len()
    }
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
    /// `nominated` is what this request's own `Connection` named, read from the head
    /// before routing took the hop-by-hop fields off it. Those names are hop-by-hop for
    /// this hop, so they do not travel on among the request's trailers either; by the
    /// time a head reaches here there is nothing left to read them from, which is why
    /// they are handed over rather than worked out
    /// ([13 §4](../../../docs/13-http1-upstream.md)).
    ///
    /// # Errors
    ///
    /// Anything the upstream said that cannot be read, a connection that failed or closed
    /// without answering, or a request body that could not be read.
    #[expect(
        clippy::too_many_arguments,
        reason = "each of them is a different thing an exchange needs, and a struct \n                  to hold them would be indirection for a lint rather than for a reader"
    )]
    pub async fn send<B>(
        mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        nominated: &[HeaderName],
        sending: Sending,
        body: B,
        limits: &H1Limits,
    ) -> Result<(Answer, Rest<S, B>), ExchangeError>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        // Nothing may be waiting here before a byte of the request has gone. HTTP/1.1
        // pairs an answer with the request that was outstanding when it arrived
        // ([RFC 9112 §9.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-9.2)), so
        // bytes already on the socket answer no request of this end's, and sending the
        // request now would pair them with a question nobody asked.
        //
        // An immediate look, and nothing more: it cannot catch bytes that race the first
        // write, and it says nothing about bytes arriving later while an upload is still
        // going out — an upstream answering early is doing exactly that and is allowed
        // to ([13 §5](../../../docs/13-http1-upstream.md)).
        if !nothing_to_say(&mut self.socket) {
            return Err(ExchangeError::Unsolicited);
        }
        let by = limits.final_head;
        let mut upload = Upload::new(body, sending, nominated.to_vec());
        let asked = timeout(
            by,
            self.exchange(method, uri, headers, sending, &mut upload, limits),
        )
        .await;
        let answer = match asked {
            Ok(answer) => answer?,
            Err(_) => return Err(ExchangeError::TooSlow { after: by }),
        };
        if answer.stop_uploading {
            upload.abandon();
        }
        Ok((
            answer,
            Rest {
                exchange: self,
                upload,
            },
        ))
    }

    /// The exchange itself, with the clock kept outside it.
    async fn exchange<B>(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        sending: Sending,
        upload: &mut Upload<B>,
        limits: &H1Limits,
    ) -> Result<Answer, ExchangeError>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        write_head(&mut self.outgoing, method, uri, headers, sending, limits)?;
        // A request that asks to be told before it sends its body has its head go out
        // alone; what follows waits for the upstream to answer, or for the wait to end.
        let mut may_send = !expects_continue(headers);
        let withheld = !may_send;
        let ask_by = Instant::now() + limits.continue_wait;

        let mut reader = HeadReader::default();
        let mut interim = 0;
        let mut interim_bytes = 0;
        let mut clocks = Clocks::default();

        loop {
            // What is already in hand comes first. Going back to the socket before
            // looking at it is how an upstream that answers and closes at once has its
            // answer thrown away for a close that had already been overtaken.
            let Head::Read { head, consumed } = reader.read(&self.incoming, limits)? else {
                let round =
                    poll_fn(|cx| self.round(cx, upload, may_send, &mut clocks, limits.idle));
                // The clocks inside the round are the exchange's own. This one is the wait
                // for permission, which is not an exchange gone quiet but a question gone
                // unanswered, and is counted from when the head went.
                let outcome = if may_send {
                    round.await
                } else {
                    match timeout_at(ask_by, round).await {
                        Ok(outcome) => outcome,
                        Err(_) => {
                            // Long enough. An upstream that will not say whether it wants
                            // the body is one that will be sent it. What was being waited
                            // for was this, so the next wait is a new one.
                            may_send = true;
                            clocks = Clocks::default();
                            continue;
                        }
                    }
                };
                match outcome {
                    Err(error) => return Err(error),
                    Ok(Moved::Read | Moved::Wrote) => continue,
                    Ok(Moved::Closed) => return Err(ExchangeError::Closed),
                }
            };
            // Checked before it is believed, interim or final alike. An interim head
            // that claims a body is a sender describing bytes that nobody will read as
            // one here and something else may read as one next; a 101 is a protocol this
            // does not speak. Neither may be waved through for being on the way to
            // something else ([13 §4](../../../docs/13-http1-upstream.md)).
            let delivery = delivery(&head, Asked::from(method))?;

            if head.status.is_informational() {
                // Only a 100 says to send the body. Another interim answer says something
                // else entirely, and saying something else is not saying yes.
                if head.status == StatusCode::CONTINUE {
                    may_send = true;
                }
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
            // A refusal that also closes the connection says to stop; an answer that came
            // early says nothing at all, because an echo answers early by nature. A
            // request still being withheld for a 100 is never started now.
            let refused = head.status.is_client_error() || head.status.is_server_error();
            // A request still being withheld for a 100 is never started by an answer:
            // the upstream answered instead of asking, so it is not waiting for a body.
            let never_asked_for = withheld && !may_send;
            let stop_uploading = never_asked_for || (refused && !delivery.persistent);
            return Ok(Answer {
                nominated: crate::hop_by_hop::nominated(&head.headers),
                head,
                delivery,
                interim,
                stop_uploading,
            });
        }
    }

    /// Pushes the request along as far as it will go without waiting: another frame of it
    /// if there is room to stage one, and whatever is staged out onto the socket.
    ///
    /// Says what moved and what it is now waiting for. Never waits on the request: a
    /// body that has nothing ready is not a reason to stop reading, which is the whole
    /// point of doing both.
    fn push<B>(
        &mut self,
        cx: &mut Context<'_>,
        upload: &mut Upload<B>,
        may_send: bool,
    ) -> Result<Pushed, ExchangeError>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let mut pushed = Pushed::default();
        // Nothing more will be sent, so there is nothing to take, nothing to write and
        // nothing to wait for.
        if upload.stopped {
            return Ok(pushed);
        }

        // Bounded work per turn: a body that keeps handing over frames must not be able
        // to hold this loop for as long as it cares to.
        for _ in 0..ROUNDS {
            let staged = self.outgoing.len();
            if staged >= STAGING {
                break;
            }
            // What is in hand goes first, a bounded slice at a time. The frame itself is
            // not copied in whole: that would be the client's pace, not the upstream's.
            if let Some((frame, at)) = upload.pending.as_mut() {
                let take = (STAGING - staged).min(frame.len() - *at);
                upload
                    .writer
                    .data(&mut self.outgoing, &frame[*at..*at + take])?;
                *at += take;
                if *at == frame.len() {
                    upload.pending = None;
                }
                continue;
            }
            // Held back means not polled at all: asking a client for bytes it was told
            // to keep is how both ends come to be waiting for each other.
            if !may_send || upload.writer.is_done() {
                break;
            }
            match Pin::new(&mut upload.body).poll_frame(cx) {
                Poll::Pending => {
                    // Asked, with room for what it gives, and it had nothing. From here
                    // the wait is the client's.
                    pushed.wants_client = true;
                    break;
                }
                Poll::Ready(Some(Err(error))) => {
                    return Err(ExchangeError::RequestBody(error.into()));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    match frame.into_data() {
                        // A frame of nothing is not progress and is not counted as any:
                        // a body that hands over nothing for ever would otherwise keep
                        // this loop turning for ever with it.
                        Ok(data) if data.is_empty() => {}
                        Ok(data) => {
                            upload.pending = Some((data, 0));
                            pushed.took = true;
                        }
                        // Trailers come last, and go out with the body's end.
                        Err(frame) => {
                            if let Ok(fields) = frame.into_trailers() {
                                upload.trailers = Some(fields);
                                pushed.took = true;
                            }
                        }
                    }
                }
                Poll::Ready(None) => {
                    upload.writer.finish(
                        &mut self.outgoing,
                        upload.trailers.as_ref(),
                        &upload.nominated,
                    )?;
                    // The end of the request is the client's last word and its best:
                    // there is nothing more to wait on it for.
                    pushed.took = true;
                    break;
                }
            }
        }

        // And out onto the socket, as far as it will take.
        while self.written < self.outgoing.len() {
            match Pin::new(&mut self.socket).poll_write(cx, &self.outgoing[self.written..]) {
                Poll::Pending => break,
                Poll::Ready(Err(error)) => return Err(error.into()),
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(gone)) => {
                    self.written += gone;
                    pushed.wrote = true;
                }
            }
        }
        // What has gone is let go of, whether or not all of it went. Keeping a sent
        // prefix in front of the next frame is how a buffer comes to hold the whole of
        // an upload rather than the part of it that is waiting.
        if self.written > 0 {
            self.outgoing.drain(..self.written);
            self.written = 0;
        }
        // Anything still staged is the upstream's to take, and until it does the wait is
        // the upstream's. A client asked for more while the socket is backed up is not a
        // client that is being slow, so its clock does not run while this one does.
        pushed.wants_upstream = !self.outgoing.is_empty();
        if pushed.wants_upstream {
            pushed.wants_client = false;
        }
        Ok(pushed)
    }

    /// Whether the request is behind the exchange: encoded to its last byte and gone, or
    /// given up on. It is what makes the answer the only thing left to wait for.
    fn upload_is_behind<B>(&self, upload: &Upload<B>) -> bool {
        upload.stopped || (upload.finished() && self.nothing_queued())
    }

    /// One round of whatever can be done: writing what is waiting, taking another frame of
    /// the request, and reading whatever has arrived. Pending only when none of the three
    /// can move and no clock that was running has run out, so a blocked write never holds
    /// the reading up and no one wait is counted against another.
    fn round<B>(
        &mut self,
        cx: &mut Context<'_>,
        upload: &mut Upload<B>,
        may_send: bool,
        clocks: &mut Clocks,
        idle: Duration,
    ) -> Poll<Result<Moved, ExchangeError>>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let pushed = match self.push(cx, upload, may_send) {
            Ok(pushed) => pushed,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if pushed.took {
            clocks.client_moved();
        }
        if pushed.wrote {
            clocks.upstream_moved();
        }

        // And read, whatever the writing did. This is the part that must not be skipped.
        let was = self.incoming.len();
        self.incoming.resize(was + READING, 0);
        let mut read = ReadBuf::new(&mut self.incoming[was..]);
        let outcome = Pin::new(&mut self.socket).poll_read(cx, &mut read);
        let filled = read.filled().len();
        self.incoming.truncate(was + filled);
        match outcome {
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) if filled == 0 => return Poll::Ready(Ok(Moved::Closed)),
            Poll::Ready(Ok(())) => {
                clocks.answer_moved();
                return Poll::Ready(Ok(Moved::Read));
            }
            Poll::Pending if pushed.wrote || pushed.took => {
                return Poll::Ready(Ok(Moved::Wrote));
            }
            Poll::Pending => {}
        }

        // Nothing moved, so what is left is to say what is being waited for and see
        // whether that wait has gone on too long.
        let waiting = self.waiting_on(upload, pushed);
        if let Some(stalled) = clocks.expired(cx, waiting, idle) {
            return Poll::Ready(Err(ExchangeError::Idle {
                after: idle,
                waiting: stalled,
            }));
        }
        Poll::Pending
    }

    /// Which clocks are running, given what the last push did.
    ///
    /// The answer's clock does not run while the request is still going out: an upstream
    /// that has not been given the whole request has every reason to say nothing, and a
    /// backend that answers a head and then reads a long upload before it says more is
    /// doing nothing wrong. It starts fresh when the request gets behind the exchange.
    fn waiting_on<B>(&self, upload: &Upload<B>, pushed: Pushed) -> Waiting {
        let behind = self.upload_is_behind(upload);
        Waiting {
            client: pushed.wants_client && !behind,
            upstream: pushed.wants_upstream,
            // The thing waited for when nothing else is: an exchange that is waiting must
            // be waiting for something, or it waits for ever.
            answer: behind || !(pushed.wants_client || pushed.wants_upstream),
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
pub struct H1Body<S, B> {
    /// The connection, what was read past the head, and the request still going out.
    /// Gone once the body has failed: there is nothing to be done with a connection whose
    /// message stopped making sense.
    rest: Option<Rest<S, B>>,
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
    /// Armed only while a poll is outstanding, and thrown away the moment anything moves
    /// in either direction. A client that has not asked for the next frame is not an
    /// upstream being slow, so while nobody is waiting nothing is counted against it.
    clocks: Clocks,
    /// What the answer's head said about the connection carrying another exchange. Not by
    /// itself enough to keep it: the request has to have finished going out too.
    persistent: bool,
    /// Where this connection came from, and where it may go back to. Absent for a body on
    /// a connection that was never leased from anywhere.
    returner: Option<Lease<S>>,
}

impl<S, B> H1Body<S, B> {
    /// A body of `framing` on what is left of an exchange.
    ///
    /// `persistent` is what the answer's head said about keeping the connection. Whether
    /// the request finished is not asked yet: an upstream may answer before it has taken
    /// all of one, and the rest of it goes out while this body is read.
    pub fn new(
        rest: Rest<S, B>,
        framing: Framing,
        persistent: bool,
        nominated: Vec<HeaderName>,
        limits: H1Limits,
    ) -> Self {
        let reader = BodyReader::nominating(framing, nominated);
        // A body that was never going to carry anything is finished before it starts. Said
        // now and not at the first poll, because nothing need ever poll an empty body.
        let complete = reader.is_done();
        Self {
            rest: Some(rest),
            reader,
            limits,
            ended: false,
            complete,
            trailers: None,
            discarded: 0,
            clocks: Clocks::default(),
            persistent,
            returner: None,
        }
    }

    /// The same, on a connection that came out of a pool and may go back to it.
    #[must_use]
    pub fn returning_to(mut self, lease: Lease<S>) -> Self {
        self.returner = Some(lease);
        self
    }

    /// Whether the whole body arrived and every check on it passed.
    pub fn is_complete(&self) -> bool {
        self.complete && self.trailers.is_none()
    }

    /// How many trailer fields were dropped as fields that may not travel on.
    pub fn discarded_trailers(&self) -> usize {
        self.discarded
    }

    /// The connection, if it has earned its way back.
    ///
    /// Every one of these holds, and none of them is inferred from another
    /// ([13 §6](../../../docs/13-http1-upstream.md)):
    ///
    /// - the answer's head allowed the connection to carry another exchange;
    /// - the request went out whole — which may have happened after the head arrived, and
    ///   is no worse for that;
    /// - exactly one answer was read through to its end, chunk terminator and trailers
    ///   included, and everything of it has been handed on;
    /// - nothing went wrong along the way — an error leaves no connection here to give;
    /// - nothing is left over in hand, and nothing is readable on the socket now, and the
    ///   upstream has not closed.
    ///
    /// **It is not a promise that the peer will behave.** TCP carries no mark that tells
    /// a delayed answer to the last request from an answer to the next, so nothing here
    /// can show that an upstream will not send something unasked-for a moment from now.
    /// What it shows is that the upstream has not done so *yet* and that this end is in a
    /// state it can account for. Keeping each destination's connections to itself limits
    /// who could be affected by a peer that misbehaves; it cannot stop one.
    pub fn take_if_reusable(&mut self) -> Option<Kept<S>>
    where
        S: AsyncRead + Unpin,
    {
        // What the head allowed, and one answer read to its end with everything passed on.
        if !self.persistent || !self.is_complete() {
            return None;
        }
        let rest = self.rest.as_ref()?;
        // A request that never finished leaves the connection out of step, whenever the
        // answer happened to arrive.
        if rest.upload.stopped || !rest.upload_finished() {
            return None;
        }
        // Bytes in hand after a message that is over are a peer saying something nobody
        // asked for, and the upstream having closed is the end of the connection anyway.
        if !rest.exchange.incoming.is_empty() || self.ended {
            return None;
        }
        let rest = self.rest.take()?;
        let mut socket = rest.exchange.socket;
        if !nothing_to_say(&mut socket) {
            return None;
        }
        Some(Kept {
            socket,
            returner: self.returner.take(),
        })
    }

    /// Puts the connection back if it has earned its way, now that the body is over.
    ///
    /// Done when the body ends rather than when whoever holds it lets go: a body that is
    /// finished with has nothing more to say, and a connection that could be carrying the
    /// next request should not wait on a client to drop an object.
    pub fn settle(&mut self)
    where
        S: AsyncRead + Unpin,
    {
        let limits = self.limits;
        if let Some(kept) = self.take_if_reusable() {
            kept.put_back(&limits);
        }
    }

    /// What is left of the connection whatever state it is in, for a caller that means to
    /// close it. Never a way back into a pool: that is [`H1Body::take_if_reusable`] alone.
    pub fn into_connection(self) -> Option<(S, Vec<u8>)> {
        self.rest
            .map(|rest| (rest.exchange.socket, rest.exchange.incoming))
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
        // The answer is over. Anything of the request that has not gone is not going now:
        // nobody is left to read it, and the connection cannot be handed on.
        if let Some(rest) = self.rest.as_mut()
            && !rest.upload_finished()
        {
            rest.upload.abandon();
        }
    }
}

impl<S, B> Body for H1Body<S, B>
where
    S: AsyncRead + AsyncWrite + Unpin,
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
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
            let Some(rest) = this.rest.as_mut() else {
                return Poll::Ready(None);
            };

            // The rest of the request goes out while the answer is read. An upstream that
            // answers early may still be reading, and one that has stopped reading is not
            // a reason to stop delivering what it has already said.
            let pushed = match rest.exchange.push(cx, &mut rest.upload, true) {
                Ok(pushed) => pushed,
                // A write the upstream will not take is an upstream that has stopped
                // reading, which §5 says to go on reading the answer through: the answer
                // it already gave stands, and what is let go of is the request. The
                // connection cannot be kept either way.
                Err(ExchangeError::Io(_)) => {
                    rest.upload.abandon();
                    Pushed::default()
                }
                // The client's own body failing, or framing that does not add up, is not
                // that. Nothing here can say the request went, so nothing here may hand
                // on an answer that finished cleanly: a client told that would believe
                // the upstream had the whole of what it sent
                // ([13 §7](../../../docs/13-http1-upstream.md)).
                Err(error) => {
                    this.rest = None;
                    return Poll::Ready(Some(Err(error)));
                }
            };
            if pushed.took {
                this.clocks.client_moved();
            }
            if pushed.wrote {
                this.clocks.upstream_moved();
            }
            let mut moved = pushed.took || pushed.wrote;

            match this
                .reader
                .read(&rest.exchange.incoming, this.ended, &this.limits)
            {
                Err(error) => {
                    // A body that stopped making sense is not an end; saying so would be
                    // handing the client half an answer as though it were the whole one.
                    this.rest = None;
                    return Poll::Ready(Some(Err(error.into())));
                }
                Ok(Piece::End { trailers, consumed }) => {
                    rest.exchange.incoming.drain(..consumed);
                    this.ended_with(trailers);
                    continue;
                }
                Ok(Piece::Data { data, consumed }) => {
                    let frame = (!data.is_empty())
                        .then(|| Bytes::copy_from_slice(&rest.exchange.incoming[data]));
                    rest.exchange.incoming.drain(..consumed);
                    let Some(frame) = frame else {
                        // Framing bytes and nothing else; keep going.
                        continue;
                    };
                    // Whether that was the last of it is worth knowing now: a client
                    // told how long a body is need never poll it again, and a body whose
                    // end was never checked is one whose connection cannot be trusted.
                    // Asked only where the answer is already certain — reading to find
                    // out would move the reader past bytes still sitting in the buffer.
                    if this.reader.is_spent() {
                        let settled =
                            this.reader
                                .read(&rest.exchange.incoming, this.ended, &this.limits);
                        if let Ok(Piece::End { trailers, consumed }) = settled {
                            rest.exchange.incoming.drain(..consumed);
                            this.ended_with(trailers);
                        }
                    }
                    this.clocks.answer_moved();
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Ok(Piece::More) => {}
            }

            // Only now, and only because somebody asked for a frame.
            let was = rest.exchange.incoming.len();
            rest.exchange.incoming.resize(was + READING, 0);
            let mut read = ReadBuf::new(&mut rest.exchange.incoming[was..]);
            let outcome = Pin::new(&mut rest.exchange.socket).poll_read(cx, &mut read);
            let filled = read.filled().len();
            rest.exchange.incoming.truncate(was + filled);
            match outcome {
                Poll::Pending => {
                    if moved {
                        // Something went out even though nothing came in; go round again
                        // rather than sleep on a socket that may now be writable.
                        moved = false;
                        let _went_out = moved;
                        continue;
                    }
                    let idle = this.limits.idle;
                    let waiting = rest.exchange.waiting_on(&rest.upload, pushed);
                    let Some(stalled) = this.clocks.expired(cx, waiting, idle) else {
                        return Poll::Pending;
                    };
                    // Long enough waiting for one thing, with a poll outstanding and
                    // nothing to show for it. The connection goes with the exchange: an
                    // upstream part way through a message is not one to lend out again.
                    this.rest = None;
                    return Poll::Ready(Some(Err(ExchangeError::Idle {
                        after: idle,
                        waiting: stalled,
                    })));
                }
                Poll::Ready(Err(error)) => {
                    this.rest = None;
                    return Poll::Ready(Some(Err(error.into())));
                }
                Poll::Ready(Ok(())) => {
                    // The upstream said something, so its clock starts again from here.
                    this.clocks.answer_moved();
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

/// A connection that finished an exchange with nothing owing, which is the only kind that
/// may be kept. Made by [`H1Body::take_if_reusable`] and nowhere else, so that keeping one
/// cannot be arranged by anybody who has merely got hold of a socket.
#[derive(Debug)]
pub struct Kept<S> {
    socket: S,
    returner: Option<Lease<S>>,
}

impl<S> Kept<S> {
    /// Puts the connection back where it came from. A connection that came from nowhere
    /// goes nowhere: it is closed here, which is the only other thing to do with one.
    pub fn put_back(self, limits: &H1Limits) {
        match self.returner {
            Some(lease) => lease.keep(self.socket, limits),
            None => drop(self.socket),
        }
    }

    /// The connection itself, for a caller that means to do something else with it.
    pub fn into_socket(self) -> S {
        self.socket
    }
}

/// Whether the upstream is saying nothing at this moment: no bytes waiting to be read,
/// and no close.
///
/// Asked when a connection is put back, and again when one is taken out: what was quiet
/// then may not be quiet now, and a socket that has been sitting in a pool has had every
/// chance to hear something nobody asked for.
///
/// Asked with a waker that wakes nobody, because this is a question about now and not a
/// wait for an answer. Anything readable here is a peer out of step with us, and the
/// connection is dropped rather than read — what is found is never taken for the start of
/// whatever the next request would have got.
pub fn nothing_to_say<S: AsyncRead + Unpin>(socket: &mut S) -> bool {
    let mut byte = [0; 1];
    let mut read = ReadBuf::new(&mut byte);
    let nobody = Waker::noop();
    // Pending is nothing to be read, which is what a connection between exchanges looks
    // like. Ready is a close, or something nobody asked for; either way it does not stay.
    Pin::new(socket)
        .poll_read(&mut Context::from_waker(nobody), &mut read)
        .is_pending()
}

/// Whether a request asked to be told before it sends its body.
///
/// Only `100-continue` is waited on. An expectation this does not know is passed on as it
/// came and waited on by nobody, which is what the engine's own client does with one.
fn expects_continue(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::EXPECT)
        .iter()
        .flat_map(crate::hop_by_hop::options)
        .any(|option| option.eq_ignore_ascii_case(b"100-continue"))
}

/// What a round of an exchange managed to do.
/// Which way an exchange stopped moving.
///
/// An exchange has three things it can be waiting for and they fail for different
/// reasons: the client that is sending the request, the upstream that is taking it, and
/// the upstream that is answering. Only one of them is ever at fault, and saying which is
/// what keeps a slow client from being read as a slow backend
/// ([13 §7](../../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stalled {
    /// The client stopped handing over the request while there was room to forward it.
    Client,
    /// The upstream stopped taking the request that was waiting to go to it.
    Upstream,
    /// The upstream stopped answering, with the request behind it already sent.
    Answer,
}

impl std::fmt::Display for Stalled {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let said = match self {
            Self::Client => "the client sent nothing more of the request",
            Self::Upstream => "the upstream took nothing more of the request",
            Self::Answer => "the upstream said nothing more of the answer",
        };
        out.write_str(said)
    }
}

/// Which of an exchange's three clocks are running.
///
/// At most one of the request's two: an upstream that will not take what is staged is not
/// a client that is being slow, and the client is not asked to answer for it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Waiting {
    client: bool,
    upstream: bool,
    answer: bool,
}

/// The clocks themselves, one per thing that can be waited for.
///
/// A clock exists only while its own thing is being waited for: it is made when that
/// starts and dropped when it stops, so time spent waiting for something else is not
/// counted against it. Progress in one direction never touches another's
/// ([13 §7](../../../docs/13-http1-upstream.md)).
#[derive(Debug, Default)]
struct Clocks {
    client: Option<Pin<Box<Sleep>>>,
    upstream: Option<Pin<Box<Sleep>>>,
    answer: Option<Pin<Box<Sleep>>>,
}

impl Clocks {
    /// Sets the clocks to `on`, and says which has run out.
    ///
    /// One that is not running is dropped rather than paused, which is what makes the
    /// next wait a fresh one. A clock that is already running keeps running: the same
    /// wait going on is not a new wait.
    fn expired(&mut self, cx: &mut Context<'_>, on: Waiting, idle: Duration) -> Option<Stalled> {
        let each = [
            (&mut self.upstream, on.upstream, Stalled::Upstream),
            (&mut self.client, on.client, Stalled::Client),
            (&mut self.answer, on.answer, Stalled::Answer),
        ];
        let mut ran_out = None;
        for (clock, running, which) in each {
            if !running {
                *clock = None;
                continue;
            }
            let ticking = clock.get_or_insert_with(|| Box::pin(tokio::time::sleep(idle)));
            if ticking.as_mut().poll(cx).is_ready() && ran_out.is_none() {
                ran_out = Some(which);
            }
        }
        ran_out
    }

    /// Something of the request went out, so the upstream is taking it.
    fn upstream_moved(&mut self) {
        self.upstream = None;
    }

    /// The client handed something over, so it has not stopped.
    fn client_moved(&mut self) {
        self.client = None;
    }

    /// The upstream said something, so it has not stopped either.
    fn answer_moved(&mut self) {
        self.answer = None;
    }
}

/// What one push of the request did, and what it left the exchange waiting for.
#[derive(Debug, Clone, Copy, Default)]
struct Pushed {
    /// The client handed over data, trailers or its end.
    took: bool,
    /// Bytes of the request went out onto the socket.
    wrote: bool,
    /// There was room to forward and the client had nothing ready.
    wants_client: bool,
    /// Bytes are encoded and the socket would not take them.
    wants_upstream: bool,
}

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

/// What this end is holding for one exchange.
///
/// **Capacity, not length.** A buffer that grew and was drained is still holding what it
/// grew to, and a bound measured on lengths would be a bound on nothing.
///
/// **What it does not measure.** Only the buffers this code owns, and of the request frame
/// in hand only the slice that is visible here. A `Bytes` is a view into an allocation
/// somebody else made, and a short view can hold a long allocation open, so the frame is
/// counted at what can be seen and no claim is made about what stands behind it. Neither
/// hyper's own buffers, its parsed maps, nor the process's memory are in here; those
/// belong to the measurement in [10 §3](../../../docs/10-testing.md), not to this
/// ([13 §7](../../../docs/13-http1-upstream.md)).
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Held {
    /// Room the outgoing buffer has taken, encoded request and framing alike.
    staged: usize,
    /// Room the incoming buffer has taken, which is where the answer arrives.
    buffered: usize,
    /// The frame of the request in hand, whole, however much of it has gone.
    frame: usize,
    /// How many frames are in hand. One is the most there is ever meant to be.
    frames: usize,
}

#[cfg(test)]
impl Held {
    fn total(self) -> usize {
        self.staged + self.buffered + self.frame
    }
}

#[cfg(test)]
impl<S, B> H1Body<S, B> {
    /// What this end is holding right now.
    fn held(&self) -> Held {
        let Some(rest) = self.rest.as_ref() else {
            return Held::default();
        };
        let (frame, frames) = match rest.upload.pending.as_ref() {
            Some((bytes, _gone)) => (bytes.len(), 1),
            None => (0, 0),
        };
        Held {
            staged: rest.exchange.outgoing.capacity(),
            buffered: rest.exchange.incoming.capacity(),
            frame,
            frames,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{Empty, Full};
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// A body that gives a frame and then fails, as a client's does when it goes away
    /// part way through sending one.
    #[derive(Debug)]
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
    fn left_over<B>(rest: Rest<DuplexStream, B>) -> Vec<u8> {
        rest.exchange.incoming
    }

    #[tokio::test]
    async fn a_request_goes_out_and_its_answer_comes_back() {
        let (exchange, mut peer) = connected(4096);
        let limits = H1Limits::default();
        let sent = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 204 No Content\r\n\r\n").await;
            (head, peer)
        });

        let (answer, rest) = exchange
            .send(
                &Method::GET,
                &"/a?b=1".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 204);
        assert_eq!(answer.interim, 0);
        assert!(rest.upload_finished(), "the request did not all go");
        let (head, _peer) = sent.await.unwrap();
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("GET /a?b=1 HTTP/1.1\r\n"), "{head}");
        assert!(head.contains("host: up.test\r\n"), "{head}");
    }

    #[tokio::test]
    async fn an_upstream_that_speaks_before_it_is_asked_is_refused() {
        let (exchange, mut peer) = connected(4096);
        // On the socket before a byte of the request has gone: said here, and not
        // left to which end happens to be polled first.
        peer.say("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            .await;

        let limits = H1Limits::default();
        let error = exchange
            .send(
                &Method::GET,
                &"/a".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();

        // An answer pairs with the request outstanding when it arrives, so bytes
        // waiting here answer nothing this end sent.
        assert!(matches!(error, ExchangeError::Unsolicited), "{error}");
        // And the request never went out: the look comes before the write, which is
        // what makes it a check on the connection rather than a race with one.
        let mut sent = Vec::new();
        peer.0.read_to_end(&mut sent).await.unwrap();
        assert!(sent.is_empty(), "the request went out anyway: {sent:?}");
    }

    #[tokio::test]
    async fn a_counted_body_goes_out_with_the_head() {
        let (exchange, mut peer) = connected(4096);
        let sent = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            let mut body = vec![0; 5];
            peer.0.read_exact(&mut body).await.unwrap();
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            (head, body)
        });

        let (answer, rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::Length(5),
                Full::new(Bytes::from_static(b"hello")),
                &H1Limits::default(),
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        assert!(rest.upload_finished(), "the request did not all go");
        let (head, body) = sent.await.unwrap();
        assert!(
            String::from_utf8(head)
                .unwrap()
                .contains("content-length: 5\r\n")
        );
        assert_eq!(body, b"hello");
        // The two bytes of the answer's body were read along with its head.
        assert_eq!(left_over(rest), b"ok");
    }

    #[tokio::test]
    async fn a_body_of_unknown_length_goes_out_chunked() {
        let (exchange, mut peer) = connected(4096);
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
                &[],
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
        let (exchange, mut peer) = connected(64);
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
        let (answer, rest) = tokio::time::timeout(
            Duration::from_secs(10),
            exchange.send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::Length(huge.len() as u64),
                Full::new(huge),
                &H1Limits::default(),
            ),
        )
        .await
        .expect("the answer came while the upload was stuck")
        .unwrap();

        assert_eq!(answer.head.status, 413);
        // The upload is still unfinished; it is not abandoned for the head alone, and
        // the answer that closes the connection is what ends it.
        assert!(!rest.upload_finished());
        assert!(!answer.delivery.persistent);
        drop(upstream);
    }

    #[tokio::test]
    async fn interim_answers_are_consumed_and_counted() {
        let (exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
            peer.say("HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n")
                .await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            peer
        });

        let (answer, _rest) = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
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
        let (exchange, mut peer) = connected(4096);
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
                &[],
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
        let (exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            // It takes the request, says nothing, and goes.
            peer.until(b"\r\n\r\n").await;
        });

        let failed = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
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
        let (exchange, mut peer) = connected(4096);
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
                &[],
                Sending::Chunked,
                body,
                &H1Limits::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(failed, ExchangeError::RequestBody(_)), "{failed}");
    }

    /// Reads a whole body, as a client taking the answer would. Whatever is behind it
    /// is still going out meanwhile, which for most of these tests is nothing.
    async fn collected<B>(
        body: &mut H1Body<DuplexStream, B>,
    ) -> Result<(Vec<u8>, Option<HeaderMap>), ExchangeError>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
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

    /// A body on a connection whose other end is the test's to write on, on a head that
    /// allowed the connection to be kept and a request that all went out.
    fn body_on(framing: Framing, buffered: &[u8]) -> (SpentBody, Peer) {
        keepable_body_on(framing, buffered, true)
    }

    /// The same, saying whether the head allowed it to be kept at all.
    fn keepable_body_on(framing: Framing, buffered: &[u8], persistent: bool) -> (SpentBody, Peer) {
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut exchange = Exchange::new(ours);
        exchange.incoming = buffered.to_vec();
        // A request with nothing in it, already all sent.
        let mut upload = Upload::new(Empty::<Bytes>::new(), Sending::None, Vec::new());
        let mut nothing = Vec::new();
        upload.writer.finish(&mut nothing, None, &[]).unwrap();
        let rest = Rest { exchange, upload };
        let body = H1Body::new(rest, framing, persistent, Vec::new(), H1Limits::default());
        (body, Peer(theirs))
    }

    /// A body whose request is behind it, which is every body these tests make.
    type SpentBody = H1Body<DuplexStream, Empty<Bytes>>;

    /// A request body that fails after the answer's head has arrived is not the
    /// abandonment §5 allows. That is for an upstream that has stopped reading — the
    /// answer stands and the request is let go of. This is the client's own body failing,
    /// or the framing of it not adding up, and an answer that finished cleanly would tell
    /// a client its request went when it did not
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    #[tokio::test]
    async fn a_request_body_that_fails_after_the_head_fails_the_answer() {
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut exchange = Exchange::new(ours);
        // The whole answer is already in hand, so nothing about the upstream is at fault.
        exchange.incoming = b"ok".to_vec();
        let upload = Upload::new(Failing(true), Sending::Chunked, Vec::new());
        let rest = Rest { exchange, upload };
        let mut body = H1Body::new(
            rest,
            Framing::Length(2),
            true,
            Vec::new(),
            H1Limits::default(),
        );
        let _peer = Peer(theirs);

        let error = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(error, ExchangeError::RequestBody(_)),
            "the client's body failed and the answer was called finished: {error}"
        );
        // And the connection goes with it: a request that was cut off leaves the upstream
        // waiting for bytes that are never coming.
        assert!(body.take_if_reusable().is_none());
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

    /// And an upstream still taking the request in is an upstream that is still there.
    /// The bound is for hearing nothing at all from it; a backend draining a long upload
    /// before it has anything to say is saying plenty, only in the other direction.
    ///
    /// Here it reads on for three times the bound and answers at the end of it.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_still_taking_the_request_in_is_not_given_up_on() {
        let idle = H1Limits::default().idle;
        // Room for far less than the request, so that the upload only goes as fast as the
        // upstream reads it and every byte of progress is the upstream's doing.
        let (ours, theirs) = tokio::io::duplex(1024);
        let upload = Upload::new(
            Full::new(Bytes::from(vec![b'x'; 64 * 1024])),
            Sending::Length(64 * 1024),
            Vec::new(),
        );
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload,
        };
        let mut body = H1Body::new(
            rest,
            Framing::Length(2),
            true,
            Vec::new(),
            H1Limits::default(),
        );

        tokio::spawn(async move {
            let mut peer = theirs;
            let mut sink = [0; 1024];
            // Never at the bound itself, so that what is being tested is the rearming and
            // not which of two deadlines a tie goes to.
            for _ in 0..4 {
                tokio::time::sleep(idle * 3 / 4).await;
                let taken = peer.read(&mut sink).await.unwrap();
                assert!(taken > 0, "the upload stopped before the test did");
            }
            peer.write_all(b"ok").await.unwrap();
            peer
        });

        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"ok");
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
        let (exchange, mut peer) = connected(4096);
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
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        // The idle bound is the shorter of the two, so it is the one that speaks, and
        // what it was waiting for was the answer: the request was sent long ago.
        assert!(
            matches!(
                failed,
                ExchangeError::Idle { after, waiting: Stalled::Answer } if after == limits.idle
            ),
            "{failed}"
        );
    }

    /// An upstream that keeps something happening never goes idle, and is still given up
    /// on: the time an exchange has for a final head is counted from its start and is not
    /// extended by an upstream that stays busy.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_dribbles_does_not_buy_itself_more_time() {
        let (exchange, mut peer) = connected(4096);
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
                &[],
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
        let (exchange, mut peer) = connected(4096);
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
                &[],
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
        let (exchange, mut peer) = connected(4096);
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            tokio::time::sleep(Duration::from_secs(20)).await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            peer
        });

        let (answer, _rest) = exchange
            .send(
                &Method::GET,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
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

    /// A body that hands over one frame and then nothing, as a client does when it stops
    /// uploading part way through and does not close.
    #[derive(Debug)]
    struct Stops(Option<Bytes>);

    impl Body for Stops {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            match self.0.take() {
                Some(data) => Poll::Ready(Some(Ok(Frame::data(data)))),
                // Never woken: the client has gone quiet, not gone away.
                None => Poll::Pending,
            }
        }
    }

    /// An answer whose request has stopped coming, with the answer itself still arriving.
    ///
    /// **An answer cannot vouch for a request.** The two clocks are separate, so bytes
    /// coming back say nothing about a client that has stopped sending, and the exchange
    /// is given up on for the reason it really stopped
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    #[tokio::test(start_paused = true)]
    async fn an_answer_still_arriving_does_not_vouch_for_a_client_that_stopped() {
        let idle = H1Limits::default().idle;
        let (ours, theirs) = tokio::io::duplex(4096);
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload: Upload::new(
                Stops(Some(Bytes::from_static(b"half"))),
                Sending::Chunked,
                Vec::new(),
            ),
        };
        let mut body = H1Body::new(
            rest,
            Framing::Chunked,
            true,
            Vec::new(),
            H1Limits::default(),
        );

        let (mut reading, mut writing) = tokio::io::split(theirs);
        // Taking in what arrived, so that a socket backed up is not what ends this. It
        // reads in a task of its own because an upstream that waited here for a client
        // that has stopped would stop answering too, and an answer that stops is exactly
        // what this test must not rely on.
        let _taking = tokio::spawn(async move {
            let mut sink = [0; 256];
            for _ in 0..64 {
                match reading.read(&mut sink).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        let _answering = tokio::spawn(async move {
            // Answering all the while, for eight times the bound: one shared clock would
            // be reset by every one of these and the exchange would never end at all.
            for _ in 0..64 {
                tokio::time::sleep(idle / 8).await;
                if writing.write_all(b"1\r\na\r\n").await.is_err() {
                    break;
                }
            }
        });

        let began = tokio::time::Instant::now();
        let failed = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(
                failed,
                ExchangeError::Idle {
                    waiting: Stalled::Client,
                    ..
                }
            ),
            "{failed}"
        );
        // A bound after the client went quiet, not a bound after anything else did.
        assert!(began.elapsed() < idle * 2, "{:?}", began.elapsed());
    }

    /// And the other way round: an upstream that answers but stops taking the request is
    /// given up on for that, not excused by the answer it is still sending.
    #[tokio::test(start_paused = true)]
    async fn an_answer_still_arriving_does_not_excuse_an_upstream_that_stopped_reading() {
        let idle = H1Limits::default().idle;
        // Room for a little of the request and no more, so the rest stays staged.
        let (ours, theirs) = tokio::io::duplex(64);
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload: Upload::new(
                Full::new(Bytes::from(vec![b'x'; 64 * 1024])),
                Sending::Length(64 * 1024),
                Vec::new(),
            ),
        };
        let mut body = H1Body::new(
            rest,
            Framing::Chunked,
            true,
            Vec::new(),
            H1Limits::default(),
        );

        let _answering = tokio::spawn(async move {
            let mut peer = Peer(theirs);
            // Talking, never listening, for eight times the bound: only a clock of its
            // own for the request going out can end this.
            for _ in 0..64 {
                tokio::time::sleep(idle / 8).await;
                peer.say("1\r\na\r\n").await;
            }
            peer
        });

        let began = tokio::time::Instant::now();
        let failed = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(
                failed,
                ExchangeError::Idle {
                    waiting: Stalled::Upstream,
                    ..
                }
            ),
            "{failed}"
        );
        // A bound after the socket stopped taking bytes, not after anything else.
        assert!(began.elapsed() < idle * 2, "{:?}", began.elapsed());
    }

    /// An upstream that will not take the request is not a client that is being slow.
    /// While the socket is backed up the client is not asked for anything, so nothing it
    /// does or fails to do is counted: the blame follows the block.
    ///
    /// The client here hands over one frame and then never speaks again, while the
    /// upstream takes a little of it every half-bound. Only the upstream's clock runs,
    /// and every one of those sips rearms it, so the exchange outlives the bound many
    /// times over. A client's clock running alongside would have gone off at the first
    /// one and blamed a client that was never asked for anything
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    #[tokio::test(start_paused = true)]
    async fn a_blocked_upstream_is_not_counted_against_the_client() {
        let idle = H1Limits::default().idle;
        // Room for a little of the request and no more, so the rest stays staged and the
        // client is asked for nothing while it waits.
        let (ours, theirs) = tokio::io::duplex(64);
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload: Upload::new(
                // More than the sips below will ever take, so the socket stays backed up
                // throughout and the client is never asked for a second frame.
                Stops(Some(Bytes::from(vec![b'x'; 8 * 1024]))),
                Sending::Chunked,
                Vec::new(),
            ),
        };
        let mut body = H1Body::new(
            rest,
            Framing::Chunked,
            true,
            Vec::new(),
            H1Limits::default(),
        );

        let _sipping = tokio::spawn(async move {
            let mut peer = Peer(theirs);
            let mut sink = [0; 64];
            // A sip every half-bound for eight times the bound, and never a word said.
            // Each rearms the upstream's clock; none of them is the client doing anything.
            for _ in 0..16 {
                tokio::time::sleep(idle / 2).await;
                if peer.0.read(&mut sink).await.is_err() {
                    break;
                }
            }
            peer
        });

        let began = tokio::time::Instant::now();
        let failed = collected(&mut body).await.unwrap_err();
        assert!(
            matches!(
                failed,
                ExchangeError::Idle {
                    waiting: Stalled::Upstream,
                    ..
                }
            ),
            "{failed}"
        );
        // Long past the bound a client that had said nothing all this time would have
        // broken, had anybody been counting its silence.
        assert!(began.elapsed() > idle * 8, "{:?}", began.elapsed());
    }

    // ---- what one exchange holds ----

    /// A request body of `left` frames of `size` bytes, handed over as fast as asked for.
    #[derive(Debug)]
    struct Frames {
        left: usize,
        size: usize,
    }

    impl Body for Frames {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            if self.left == 0 {
                return Poll::Ready(None);
            }
            self.left -= 1;
            let size = self.size;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'x'; size])))))
        }
    }

    /// Sends `frames` frames of `size` upstream and reads `chunks` chunks of `size` back,
    /// and says the most this end was holding at any point along the way.
    async fn holding(frames: usize, chunks: usize, size: usize) -> Held {
        let total = frames * size;
        let (ours, theirs) = tokio::io::duplex(4096);
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload: Upload::new(
                Frames { left: frames, size },
                Sending::Length(total as u64),
                Vec::new(),
            ),
        };
        let mut body = H1Body::new(
            rest,
            Framing::Chunked,
            true,
            Vec::new(),
            H1Limits::default(),
        );

        let _peer = tokio::spawn(async move {
            let mut peer = Peer(theirs);
            let mut sink = vec![0; 4096];
            let mut taken = 0;
            // Bounded: room for the whole request and a little over, never an open loop.
            for _ in 0..(total / 1024 + 64) {
                if taken >= total {
                    break;
                }
                match peer.0.read(&mut sink).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => taken += read,
                }
            }
            for _ in 0..chunks {
                peer.say(&format!("{size:x}\r\n")).await;
                peer.0.write_all(&vec![b'y'; size]).await.unwrap();
                peer.say("\r\n").await;
            }
            peer.say("0\r\n\r\n").await;
            peer
        });

        let mut most = Held::default();
        let mut watch = |held: Held| {
            most.staged = most.staged.max(held.staged);
            most.buffered = most.buffered.max(held.buffered);
            most.frame = most.frame.max(held.frame);
            most.frames = most.frames.max(held.frames);
        };
        watch(body.held());
        loop {
            let polled = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            watch(body.held());
            match polled {
                None => break,
                Some(frame) => {
                    frame.unwrap();
                }
            }
        }
        assert!(body.is_complete());
        most
    }

    /// **What is held does not move with what is moved.** Sixteen kibibytes through the
    /// exchange or sixteen mebibytes, at frames from 4 KiB to 256 KiB: the same two
    /// buffers, the same size, every time.
    ///
    /// The bound is exactly the buffers this code owns -- one for staging the request and
    /// one for reading the answer -- and it is asserted as an equality rather than a
    /// ceiling, because a ceiling would not notice a buffer that had begun to creep.
    #[tokio::test]
    async fn what_is_held_does_not_move_with_what_is_moved() {
        let smallest = holding(4, 4, 4 * 1024).await;
        for (frames, size) in [
            (16, 4 * 1024),
            (64, 4 * 1024),
            (16, 16 * 1024),
            (64, 16 * 1024),
            (16, 64 * 1024),
            (64, 64 * 1024),
            (64, 256 * 1024),
        ] {
            let most = holding(frames, frames, size).await;
            assert_eq!(most, smallest, "{frames} frames of {size}");
        }
        // And what that unmoving amount is: the two buffers this code owns, and nothing
        // else. Said in terms of the bounds themselves, so that raising one is a decision
        // taken here rather than a number that drifted.
        assert_eq!(smallest.total(), STAGING + READING, "{smallest:?}");
    }

    /// And exchanges beside one another are still an exchange each: nothing here is
    /// shared, so nothing here adds up differently for being one of several.
    #[tokio::test]
    async fn exchanges_beside_one_another_hold_what_one_holds() {
        let alone = holding(4, 4, 4 * 1024).await;
        let (first, second, third, fourth) = tokio::join!(
            holding(64, 64, 64 * 1024),
            holding(64, 64, 64 * 1024),
            holding(16, 16, 256 * 1024),
            holding(4, 4, 4 * 1024),
        );
        for (which, most) in [first, second, third, fourth].into_iter().enumerate() {
            assert_eq!(most, alone, "exchange {which}");
        }
    }

    /// **One frame in hand, never a queue of them.** A client with sixty-four frames to
    /// give and an upstream that will take almost none of them is a client that is not
    /// asked for the next one: what is held is the frame being forwarded and the two
    /// buffers, however much more is offered
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    ///
    /// The frame counts at its own size, which is the client's choice and not this end's.
    /// What this end promises is that there is one.
    #[tokio::test]
    async fn only_one_frame_of_the_request_is_ever_in_hand() {
        let size = 64 * 1024;
        // Room for a little of the request and no more, so the frames have to wait.
        let (ours, _theirs) = tokio::io::duplex(64);
        let rest = Rest {
            exchange: Exchange::new(ours),
            upload: Upload::new(
                Frames { left: 64, size },
                Sending::Length((64 * size) as u64),
                Vec::new(),
            ),
        };
        let mut body = H1Body::new(
            rest,
            Framing::Chunked,
            true,
            Vec::new(),
            H1Limits::default(),
        );

        let mut context = Context::from_waker(Waker::noop());
        let mut ever = false;
        for _ in 0..16 {
            let _polled = Pin::new(&mut body).poll_frame(&mut context);
            let held = body.held();
            assert!(held.frames <= 1, "frames queued up: {held:?}");
            assert!(held.staged <= STAGING, "staging grew: {held:?}");
            ever |= held.frames == 1;
        }
        assert!(ever, "no frame was ever in hand, so nothing was measured");

        let held = body.held();
        assert_eq!(held.frame, size, "{held:?}");
        assert_eq!(held.total(), STAGING + READING + size, "{held:?}");
    }

    /// A body that says whether anybody has asked it for anything. Holding a body back
    /// means not asking it, not asking and discarding, so what is watched is the asking.
    struct Watched {
        asked: Arc<AtomicBool>,
        data: Option<Bytes>,
    }

    impl Watched {
        fn new(data: &'static [u8]) -> (Self, Arc<AtomicBool>) {
            let asked = Arc::new(AtomicBool::new(false));
            let body = Self {
                asked: Arc::clone(&asked),
                data: Some(Bytes::from_static(data)),
            };
            (body, asked)
        }
    }

    impl Body for Watched {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
            self.asked.store(true, Ordering::SeqCst);
            Poll::Ready(
                self.data
                    .take()
                    .map(|data| Ok(hyper::body::Frame::data(data))),
            )
        }
    }

    fn expecting() -> HeaderMap {
        headers(&[("host", "up.test"), ("expect", "100-continue")])
    }

    /// The head goes out alone and the body waits to be asked for. Only when the upstream
    /// says 100 is the client's body touched at all.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_was_told_to_wait_waits_for_the_upstreams_word() {
        let (exchange, mut peer) = connected(4096);
        let (body, asked) = Watched::new(b"hello");
        let watching = Arc::clone(&asked);
        let peering = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            // Long enough that a body which was going to be sent would have been.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let asked_too_soon = watching.load(Ordering::SeqCst);
            peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
            let body = peer.until(b"0\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            (head, asked_too_soon, body)
        });

        // A wait long enough that only the 100 can end it.
        let limits = H1Limits {
            continue_wait: Duration::from_secs(60),
            ..H1Limits::default()
        };
        let (answer, _rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &expecting(),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        let (head, asked_too_soon, body) = peering.await.unwrap();
        assert!(
            String::from_utf8(head)
                .unwrap()
                .contains("expect: 100-continue\r\n"),
            "the expectation goes to the upstream, which is who can answer it"
        );
        assert!(
            !asked_too_soon,
            "the body was asked for before it was wanted"
        );
        assert!(asked.load(Ordering::SeqCst), "and then never asked for");
        assert_eq!(body, b"5\r\nhello\r\n0\r\n\r\n");
    }

    /// An upstream that will not say either way does not hold the body forever: after the
    /// wait it is sent anyway, because the client is waiting on both of them meanwhile.
    #[tokio::test(start_paused = true)]
    async fn a_body_held_back_goes_anyway_once_the_wait_is_up() {
        let (exchange, mut peer) = connected(4096);
        let (body, asked) = Watched::new(b"hello");
        let peering = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            // It says nothing at all about the expectation.
            let body = peer.until(b"0\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            body
        });

        let (answer, _rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &expecting(),
                &[],
                Sending::Chunked,
                body,
                &H1Limits::default(),
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        assert!(asked.load(Ordering::SeqCst));
        assert_eq!(peering.await.unwrap(), b"5\r\nhello\r\n0\r\n\r\n");
    }

    /// Another interim answer says something else, and saying something else is not
    /// saying yes: the body keeps waiting.
    #[tokio::test(start_paused = true)]
    async fn an_interim_answer_that_is_not_a_100_does_not_release_the_body() {
        let (exchange, mut peer) = connected(4096);
        let (body, asked) = Watched::new(b"hello");
        let watching = Arc::clone(&asked);
        let peering = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 103 Early Hints\r\nlink: </a>\r\n\r\n")
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            let asked_on_a_103 = watching.load(Ordering::SeqCst);
            peer.say("HTTP/1.1 100 Continue\r\n\r\n").await;
            let body = peer.until(b"0\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            (asked_on_a_103, body)
        });

        let limits = H1Limits {
            continue_wait: Duration::from_secs(60),
            ..H1Limits::default()
        };
        let (answer, _rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &expecting(),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 200);
        let (asked_on_a_103, body) = peering.await.unwrap();
        assert!(
            !asked_on_a_103,
            "a 103 released a body it does not speak for"
        );
        assert_eq!(body, b"5\r\nhello\r\n0\r\n\r\n");
    }

    /// An upstream that refuses outright is answered at once, and the body it refused is
    /// never asked for: this is what asking first was for.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_refuses_is_never_sent_the_body() {
        let (exchange, mut peer) = connected(4096);
        let (body, asked) = Watched::new(b"hello");
        tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say(
                "HTTP/1.1 417 Expectation Failed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await;
            peer
        });

        let limits = H1Limits {
            continue_wait: Duration::from_secs(60),
            ..H1Limits::default()
        };
        let (answer, _rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &expecting(),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap();

        assert_eq!(answer.head.status, 417);
        assert!(
            !asked.load(Ordering::SeqCst),
            "the refused body was sent anyway"
        );
        assert!(
            answer.stop_uploading,
            "a refusal that closes did not stop the upload"
        );
        assert!(!answer.delivery.persistent);
    }

    /// An expectation nobody here knows is passed on and waited on by no one, which is
    /// what the engine's own client does with one.
    #[tokio::test(start_paused = true)]
    async fn an_expectation_that_is_not_a_continue_holds_nothing_back() {
        let (exchange, mut peer) = connected(4096);
        let (body, asked) = Watched::new(b"hello");
        let peering = tokio::spawn(async move {
            let head = peer.until(b"\r\n\r\n").await;
            let body = peer.until(b"0\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            (head, body)
        });

        let limits = H1Limits {
            // Long enough that anything which waited would be caught waiting.
            continue_wait: Duration::from_secs(600),
            ..H1Limits::default()
        };
        exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &headers(&[("host", "up.test"), ("expect", "the-moon-on-a-stick")]),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap();

        assert!(asked.load(Ordering::SeqCst));
        let (head, body) = peering.await.unwrap();
        assert!(
            String::from_utf8(head)
                .unwrap()
                .contains("expect: the-moon-on-a-stick\r\n")
        );
        assert_eq!(body, b"5\r\nhello\r\n0\r\n\r\n");
    }

    /// A body read cleanly to its end, on a head that allowed it, gives its connection
    /// back. Every test below takes one condition away from this and gets nothing.
    #[tokio::test(start_paused = true)]
    async fn a_finished_exchange_gives_its_connection_back() {
        let (mut body, _peer) = body_on(Framing::Length(5), b"hello");
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");
        assert!(body.take_if_reusable().is_some());
    }

    /// What the head said comes first. A connection the answer said to close is not kept,
    /// however cleanly the body read.
    #[tokio::test(start_paused = true)]
    async fn a_connection_the_answer_closed_is_not_kept() {
        let (mut body, _peer) = keepable_body_on(Framing::Length(5), b"hello", false);
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");
        assert!(body.take_if_reusable().is_none());
    }

    /// A body nobody finished reading is a connection in the middle of a message.
    ///
    /// Nothing is buffered on purpose. Bytes in hand would refuse this connection for
    /// being bytes in hand, and the test would pass without the answer's end ever having
    /// been the reason — which is what bytes waiting to be read cost a test here.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_was_never_read_to_its_end_is_not_kept() {
        let (mut body, _peer) = body_on(Framing::Length(5), b"");
        // Not a frame taken, and none waiting to be.
        assert!(!body.is_complete());
        assert!(body.take_if_reusable().is_none());
    }

    /// A chunked body has no way of knowing it is over until it has read the chunk that
    /// says so, so one left after its data is one whose end was never checked — and an
    /// end that was never checked is a connection nobody can account for.
    #[tokio::test(start_paused = true)]
    async fn a_chunked_body_stopped_before_its_end_is_not_kept() {
        let (mut body, _peer) = body_on(
            Framing::Chunked,
            b"5\r\nhello\r\n0\r\ngrpc-status: 0\r\n\r\n",
        );
        let mut pinned = Pin::new(&mut body);
        let frame = poll_fn(|cx| pinned.as_mut().poll_frame(cx)).await.unwrap();
        assert_eq!(
            frame.unwrap().into_data().unwrap(),
            Bytes::from_static(b"hello")
        );
        // The zero chunk and the trailers are still on the wire.
        assert!(body.take_if_reusable().is_none());
    }

    /// A body that stopped making sense took its connection with it: there is nothing
    /// left here to give back.
    #[tokio::test(start_paused = true)]
    async fn a_body_that_failed_has_no_connection_to_give() {
        let (mut body, _peer) = body_on(Framing::Chunked, b"zz\r\nhello\r\n");
        assert!(collected(&mut body).await.is_err());
        assert!(body.take_if_reusable().is_none());
    }

    /// **Bytes after the end are a peer that is out of step.** Whatever they are, they
    /// are not the beginning of whatever the next request would have been given, and the
    /// connection goes rather than being read to find out.
    #[tokio::test(start_paused = true)]
    async fn bytes_arriving_after_the_answer_stop_it_being_kept() {
        let (mut body, mut peer) = body_on(Framing::Length(5), b"hello");
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");

        // Said after the answer was whole, and asked for by nobody.
        peer.say("HTTP/1.1 200 OK\r\n\r\n").await;
        tokio::task::yield_now().await;
        assert!(body.take_if_reusable().is_none());
    }

    /// The same for a close: an upstream that has gone is not a connection to keep.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_closed_after_answering_is_not_kept() {
        let (mut body, peer) = body_on(Framing::Length(5), b"hello");
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");

        drop(peer);
        tokio::task::yield_now().await;
        assert!(body.take_if_reusable().is_none());
    }

    /// A body the close delimits has had its connection ended by definition; nothing that
    /// arrived that way can be kept, and the head says so before the body is read.
    #[tokio::test(start_paused = true)]
    async fn a_body_the_close_delimited_is_never_kept() {
        let (mut body, peer) = keepable_body_on(Framing::UntilClose, b"some", false);
        drop(peer);
        let (data, _) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"some");
        assert!(body.take_if_reusable().is_none());
    }
}

#[cfg(test)]
mod lifecycle {
    use super::*;
    #[derive(Debug)]
    struct Frames;
    impl Body for Frames {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(&[b'x'; 4096])))))
        }
    }
    #[tokio::test]
    async fn staging_does_not_keep_what_it_has_already_sent() {
        use tokio::io::AsyncReadExt;
        let (socket, mut peer) = tokio::io::duplex(1024);
        let mut exchange = Exchange::new(socket);
        let mut upload = Upload::new(Frames, Sending::Chunked, Vec::new());
        let mut drain = [0; 1024];
        for _ in 0..100 {
            tokio::task::yield_now().await;
            exchange
                .push(&mut Context::from_waker(Waker::noop()), &mut upload, true)
                .unwrap();
            peer.read_exact(&mut drain).await.unwrap();
        }
        assert!(
            exchange.outgoing.len() <= STAGING + 4096,
            "retained {} bytes for {} unsent",
            exchange.outgoing.len(),
            exchange.outgoing.len() - exchange.written
        );
    }
    #[tokio::test]
    async fn a_request_still_queued_is_not_a_request_that_went() {
        let (socket, _peer) = tokio::io::duplex(64);
        let mut exchange = Exchange::new(socket);
        exchange.outgoing.extend_from_slice(b"0\r\n\r\n");
        let mut upload = Upload::new(
            http_body_util::Empty::<Bytes>::new(),
            Sending::Length(0),
            Vec::new(),
        );
        upload.writer.finish(&mut Vec::new(), None, &[]).unwrap();
        let mut body = H1Body::new(
            Rest { exchange, upload },
            Framing::None,
            true,
            Vec::new(),
            H1Limits::default(),
        );
        assert!(
            body.take_if_reusable().is_none(),
            "returned socket with request bytes still unsent"
        );
    }
}
