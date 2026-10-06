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
use super::blocks::{Block, Blocks};
use super::codec::{
    Asked, BodyReader, BodyWriter, CodecError, Delivery, Framing, Head, HeadReader, OutgoingFields,
    Piece, ResponseHead, Sending, Trailers, delivery, head_len, write_head,
};
use super::pool::{Close, Lease};
use crate::interim::{Channel, Interim};
use crate::storage::{Charge, Exhausted};
use crate::timers::{Alarm, Timers};
use bytes::Bytes;
use edgerush_router::Fields;
use http::{HeaderMap, HeaderName, Method, StatusCode, Uri};
use http_body::{Body, Frame, SizeHint};
use std::cell::RefCell;
use std::error::Error as StdError;
use std::future::poll_fn;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

/// How much of a request waits in memory to be written. One frame at a time is staged;
/// the body is not asked for more until what it gave has gone.
const STAGING: usize = 16 * 1024;

/// Maximum write batches per push, and body polls within each batch.
const ROUNDS: usize = 8;

/// Why an exchange could not be carried through.
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    /// What the upstream sent could not be read.
    #[error("the upstream's answer could not be read: {0}")]
    Codec(#[from] CodecError),
    /// No connection could be opened: refused, reset or out of time while connecting, no
    /// local port free to it, its TLS handshake failed, or no socket to connect with, the
    /// worker's own shortage. Nothing of the request was sent
    /// ([03 §6](../../../docs/03-data-plane.md)).
    #[error("no connection to the upstream could be opened: {0}")]
    Unconnected(#[source] crate::upstream::dial::Unconnected),
    /// The connection itself failed.
    #[error("the connection to the upstream failed: {0}")]
    Io(#[from] io::Error),
    /// The worker could not pay for storage the exchange needed. Never taken for an
    /// upstream that stopped reading: the exchange is cancelled, not carried on with half a
    /// request sent ([14 §8](../../../docs/14-downstream-server.md)).
    #[error("the worker could not pay for what the exchange needed: {0}")]
    Exhausted(#[from] Exhausted),
    /// The request's own body could not be read, which is the client's end failing, not
    /// the upstream's.
    #[error("the request body could not be read: {0}")]
    RequestBody(#[source] Box<dyn StdError + Send + Sync>),
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
    /// How many interim heads came before it. Each was told to the exchange's side channel,
    /// which passes it on where EdgeRush's own server listens and consumes it otherwise
    /// ([14 §5](../../../docs/14-downstream-server.md)).
    #[cfg_attr(
        not(any(test, feature = "fuzzing")),
        expect(dead_code, reason = "counted for the tests, which check how many came")
    )]
    pub interim: usize,
    /// What this answer's own `Connection` named as its own. Read here, because by the
    /// time the hop-by-hop fields have been taken off there is nothing left to read.
    pub nominated: Vec<HeaderName>,
    /// Whether this answer is one that says to stop sending the request: a refusal that
    /// says `Connection: close`, on its own head or on an interim one before it. A
    /// connection that merely will not persist has not said the body is unwanted.
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
    ///
    /// Lent from the worker's blocks while there is anything in it and given back the
    /// moment there is not: an exchange waiting on a quiet upstream holds none, and a
    /// connection goes back to its pool holding none either.
    incoming: Option<Block>,
    blocks: Rc<RefCell<Blocks>>,
    /// Waiting to be written, and how much of it has gone.
    ///
    /// Lent from the worker's blocks with its room already made, and given back once
    /// everything in it has gone; empty and unallocated in between.
    outgoing: Vec<u8>,
    /// The charge for `outgoing`'s capacity, while it has any.
    outgoing_charge: Option<Charge>,
    /// The continue decision, and where the interim answers go: the downstream server's,
    /// when it listens for them, and one nobody listens on otherwise
    /// ([14 §5](../../../docs/14-downstream-server.md)).
    interim: Channel,
    written: usize,
    /// A shared slice of the upload frame, between its encoded prefix and suffix.
    payload: Bytes,
    chunk_tail: usize,
    /// Head bytes still queued, separately from body bytes staged behind them. Response
    /// deadlines start at the write that takes the last head byte, even if it also
    /// takes body bytes; neither encoding the head nor starting its write is enough.
    head_left: usize,
    head_sent: Option<Instant>,
    /// The one timer for every deadline of the exchange, and then of its answer's body:
    /// made once an exchange rather than once a wait, and moved rather than made again.
    alarm: Alarm,
    /// Whether what bounds the wait for the answer's head is the rule's own timeouts,
    /// kept by whoever waits on the exchange, in place of the head deadline and the
    /// answer's idle clock before the head ([03 §6](../../../../docs/03-data-plane.md)).
    head_bounded_elsewhere: bool,
    /// Whether the request is a WebSocket handshake, whose 101 is its final answer
    /// ([19 §2](../../../../docs/19-websocket.md)).
    upgrading: bool,
}

impl<S> Exchange<S> {
    /// Tells `interim` what the upstream says in the meantime, and takes the continue
    /// decision from it: for a request whose server passes interim answers on.
    pub(crate) fn heard_by(mut self, interim: Interim) -> Self {
        self.interim = Channel::Listened(interim);
        self
    }

    /// Whether everything encoded has left for the socket.
    fn nothing_queued(&self) -> bool {
        self.written >= self.outgoing.len() && self.payload.is_empty() && self.chunk_tail == 0
    }

    /// What has been read and not yet used.
    fn unread(&self) -> &[u8] {
        self.incoming.as_ref().map_or(&[][..], Block::data)
    }

    /// Says the first `count` of [`Exchange::unread`] have been used.
    fn used(&mut self, count: usize) {
        if let Some(block) = self.incoming.as_mut() {
            block.consume(count);
        }
        self.give_back_if_empty();
    }

    /// Cuts `data` of [`Exchange::unread`] out as a frame, sharing the block's memory
    /// rather than copying it, and says the first `through` bytes have been used.
    fn take_frame(&mut self, data: Range<usize>, through: usize) -> Bytes {
        let frame = self
            .incoming
            .as_mut()
            .map_or_else(Bytes::new, |block| block.take_frame(data, through));
        self.give_back_if_empty();
        frame
    }

    /// Gives the block back if nothing in it is waiting to be used, which is the moment
    /// holding it stops being worth anything.
    fn give_back_if_empty(&mut self) {
        if let Some(block) = self.incoming.take_if(|block| block.is_empty()) {
            self.blocks.borrow_mut().give(block);
        }
    }

    /// Makes sure there is a staging buffer with its room made, lent from the worker's
    /// blocks if none is held.
    fn lend_staging(&mut self) -> Result<(), Exhausted> {
        if self.outgoing.capacity() < STAGING {
            let (mut lent, charge) = self.blocks.borrow_mut().take_staging(STAGING)?;
            lent.extend_from_slice(&self.outgoing);
            self.outgoing = lent;
            self.outgoing_charge = Some(charge);
        }
        Ok(())
    }

    /// Makes room in the staging buffer for `more` bytes, paying for the larger buffer
    /// before it is made, while the smaller one is still paid for
    /// ([14 §8](../../../docs/14-downstream-server.md)). A buffer with the room already
    /// changes nothing.
    fn stage_room(&mut self, more: usize) -> Result<(), Exhausted> {
        let wanted = self.outgoing.len().saturating_add(more);
        if wanted <= self.outgoing.capacity() {
            return Ok(());
        }
        let charge = self.blocks.borrow().storage().reserve(wanted)?;
        // Exactly, so that what is paid for is what is held.
        self.outgoing.reserve_exact(more);
        self.outgoing_charge = Some(charge);
        #[cfg(test)]
        assert!(
            self.staging_paid_for(),
            "staging grew without being paid for"
        );
        Ok(())
    }

    /// Whether the staging buffer's whole capacity, and no more, is paid for.
    #[cfg(test)]
    fn staging_paid_for(&self) -> bool {
        self.outgoing_charge.as_ref().map_or(0, Charge::bytes) == self.outgoing.capacity()
    }

    /// Gives the staging buffer back if everything staged in it has gone, rather than
    /// hold it empty until there is more.
    fn give_back_staging_if_empty(&mut self) {
        if self.outgoing.is_empty() && self.outgoing.capacity() > 0 {
            let spent = std::mem::take(&mut self.outgoing);
            if let Some(charge) = self.outgoing_charge.take() {
                self.blocks.borrow_mut().give_staging(spent, charge);
            }
        }
    }
}

impl<S: AsyncRead + Unpin> Exchange<S> {
    /// Reads what the socket has, into a block borrowed for it if none is held.
    ///
    /// Says how the read went and how many bytes it brought, which are what both of its
    /// callers decide on. A block with no room is refilled first — its memory taken back
    /// from frames that have gone, or grown for a head that does not fit — because a read
    /// into no room comes back with nothing, and nothing is what a close looks like.
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> (Poll<Result<(), ExchangeError>>, usize) {
        // A block the worker cannot pay for fails the read rather than waits for memory to
        // come free (14 §8); what was held goes with it.
        let lent = match self.incoming.take() {
            Some(mut block) => {
                if block.room().is_empty() {
                    self.blocks.borrow_mut().refill(block)
                } else {
                    Ok(block)
                }
            }
            None => self.blocks.borrow_mut().take(),
        };
        let mut block = match lent {
            Ok(block) => block,
            Err(exhausted) => return (Poll::Ready(Err(exhausted.into())), 0),
        };
        if block.room().is_empty() {
            // Not reached while the blocks are sized by `Sizes::within`: a grown block has
            // room for anything the codec assembles whole, and the codec refuses anything
            // bigger first. Failed all the same rather than read, because a read of nothing
            // would be taken for the upstream closing.
            self.incoming = Some(block);
            let outgrown = io::Error::other("an answer outgrew the most a block holds");
            return (Poll::Ready(Err(outgrown.into())), 0);
        }
        let mut read = ReadBuf::new(block.room());
        let outcome = Pin::new(&mut self.socket).poll_read(cx, &mut read);
        let filled = read.filled().len();
        block.arrived(filled);
        self.incoming = Some(block);
        self.give_back_if_empty();
        (outcome.map_err(ExchangeError::from), filled)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Exchange<S> {
    /// An exchange on `socket`, which nothing has been said on yet, reading into blocks
    /// lent from `blocks`.
    pub fn new(socket: S, blocks: Rc<RefCell<Blocks>>, timers: Rc<Timers>) -> Self {
        Self {
            socket,
            incoming: None,
            blocks,
            outgoing: Vec::new(),
            outgoing_charge: None,
            interim: Channel::unheard(),
            written: 0,
            payload: Bytes::new(),
            chunk_tail: 0,
            head_left: 0,
            head_sent: None,
            alarm: Alarm::new(&timers, None),
            head_bounded_elsewhere: false,
            upgrading: false,
        }
    }

    /// Sends a WebSocket handshake, whose 101 is taken as its final answer rather than
    /// refused ([19 §2](../../../../docs/19-websocket.md)).
    pub(crate) fn upgrading(mut self) -> Self {
        self.upgrading = true;
        self
    }

    /// Leaves the wait for the answer's head to the rule's own timeouts, which the caller
    /// keeps: neither the head deadline nor the answer's idle clock runs before the head.
    /// The clocks for the upload, and the answer's once its head has come, run as ever:
    /// they find data that stopped flowing, which no timeout of a rule's is for.
    pub(crate) fn head_bounded_elsewhere(mut self) -> Self {
        self.head_bounded_elsewhere = true;
        self
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
    pub async fn send<F, B>(
        mut self,
        method: &Method,
        uri: &Uri,
        headers: &F,
        nominated: &[HeaderName],
        sending: Sending,
        body: B,
        limits: &H1Limits,
    ) -> Result<(Answer, Rest<S, B>), ExchangeError>
    where
        F: OutgoingFields + ?Sized,
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
        let mut upload = Upload::new(body, sending, nominated.to_vec());
        let mut answer = self
            .exchange(method, uri, headers, sending, &mut upload, limits)
            .await?;
        if answer.stop_uploading {
            upload.abandon();
        }
        // A request-side close forbids reuse even if the peer ignores it. This is
        // separate from upload refusal: closing after an authenticated request must
        // not stop its body from reaching the backend or replace the backend's answer.
        let request_closes = headers
            .values(&http::header::CONNECTION)
            .flat_map(crate::hop_by_hop::options_of)
            .any(|option| option.eq_ignore_ascii_case(b"close"));
        answer.delivery.persistent &= !request_closes;
        Ok((
            answer,
            Rest {
                exchange: self,
                upload,
            },
        ))
    }

    /// The exchange itself, with deadlines anchored to transmission of the request head.
    async fn exchange<F, B>(
        &mut self,
        method: &Method,
        uri: &Uri,
        headers: &F,
        sending: Sending,
        upload: &mut Upload<B>,
        limits: &H1Limits,
    ) -> Result<Answer, ExchangeError>
    where
        F: OutgoingFields + ?Sized,
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        // Set no further ahead than the answer's clock and the final head's deadline, so
        // that the clocks starting as the exchange goes on fall due after it and leave it
        // where it is. A wait for a `100` is shorter, and rare: it moves the timer.
        self.alarm.set_ahead(limits.idle.min(limits.final_head));
        self.lend_staging()?;
        // Paid for before it is written. A head over the bound is refused below, before a
        // byte of it is, so no room is made for one.
        let head = head_len(method, uri, headers, sending);
        if head <= limits.head {
            self.stage_room(head)?;
        }
        write_head(
            &mut self.outgoing,
            method,
            uri,
            headers,
            sending,
            head,
            limits,
        )?;
        self.head_left = self.outgoing.len();
        // A request that asks to be told before it sends its body has its head go out
        // alone; what follows waits for the upstream to answer, or for the wait to end.
        //
        // A body framed as nothing has nothing to hold back: waiting would ask the
        // upstream's leave to send no bytes, and an answer given instead of a 100 would
        // then abandon an upload that was never there, and the connection with it. One of
        // unknown length is another matter: nobody knows it is empty until it is asked.
        let nothing_to_send = matches!(sending, Sending::None | Sending::Length(0));
        self.interim
            .begin(expects_continue(headers), nothing_to_send);
        let mut may_send = self.interim.may_poll_upload();
        // Whether the wait for a `100` was started, which the coordinator says once.
        let mut continuing = false;

        let mut reader = HeadReader::default();
        let mut interim = 0;
        let mut interim_bytes = 0;
        // A close said on an interim head holds for the rest of the exchange: RFC 9112
        // §9.6 has a client that receives one cease sending requests on the connection,
        // and the final head saying nothing does not take it back.
        let mut close_said = false;
        let mut clocks = Clocks::default();

        loop {
            // What is already in hand comes first. Going back to the socket before
            // looking at it is how an upstream that answers and closes at once has its
            // answer thrown away for a close that had already been overtaken.
            let Head::Read {
                head: lines,
                consumed,
            } = reader.read(self.unread(), limits)?
            else {
                let outcome = poll_fn(|cx| {
                    // The write-idle clock covers the head while it is queued. These
                    // two waits begin only once the complete head has reached the socket.
                    // They are looked at even when I/O is ready: a busy peer cannot extend
                    // an absolute deadline by supplying progress or interim heads.
                    if self.head_sent.is_some() && !may_send && !continuing {
                        continuing = self.interim.head_sent();
                    }
                    let mut waits = self.waits(continuing && !may_send, limits);
                    if let Some(soonest) = waits.soonest(clocks.soonest())
                        && self.alarm.poll_until(cx, soonest).is_ready()
                    {
                        if waits.final_head.is_some_and(|due| self.alarm.reached(due)) {
                            return Poll::Ready(Err(ExchangeError::TooSlow {
                                after: limits.final_head,
                            }));
                        }
                        if waits.continuing.is_some_and(|due| self.alarm.reached(due)) {
                            self.interim.wait_expired();
                            may_send = self.interim.may_poll_upload();
                            clocks = Clocks::default();
                            waits = self.waits(continuing && !may_send, limits);
                        }
                    }
                    self.round(
                        cx,
                        upload,
                        may_send,
                        &mut clocks,
                        waits.soonest(None),
                        limits,
                    )
                })
                .await;
                match outcome {
                    Err(error) => return Err(error),
                    Ok(Moved::Read | Moved::Wrote) => continue,
                    Ok(Moved::Closed) => return Err(ExchangeError::Closed),
                }
            };
            // Copied out of the block, which is then free to be read into again: a head
            // lives only until it is written, and a cut one would hold the block's memory
            // until it was reclaimed and set to zeros again, which costs more than the copy
            // ([14 §8](../../../docs/14-downstream-server.md)).
            let head = lines.of(Bytes::copy_from_slice(
                self.unread().get(..consumed).unwrap_or_default(),
            ));
            // Checked before it is believed, interim or final alike. An interim head
            // that claims a body is a sender describing bytes that nobody will read as
            // one here and something else may read as one next; a 101 is a protocol this
            // does not speak. Neither may be waved through for being on the way to
            // something else ([13 §4](../../../docs/13-http1-upstream.md)).
            let asked = if self.upgrading {
                Asked::Upgrade
            } else {
                Asked::from(method)
            };
            let mut delivery = delivery(&head, asked)?;
            // A handshake's 101 is its final answer: the connection is the new protocol's
            // from the byte after it.
            let switching = self.upgrading && head.status == StatusCode::SWITCHING_PROTOCOLS;

            if head.status.is_informational() && !switching {
                // A 1.1 interim head is persistent unless it says close; 1.0 ones are
                // refused before this. Read before its hop-by-hop fields come off.
                close_said |= !delivery.persistent;
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
                self.used(consumed);
                // Only a 100 says to send the body; another interim answer says something
                // else entirely. Which are passed on, and which the gateway keeps, is the
                // coordinator's to say.
                self.interim.upstream_interim(head.status, head.to_map());
                may_send = self.interim.may_poll_upload();
                reader = HeadReader::default();
                continue;
            }
            self.used(consumed);
            close_said |= head
                .values(&http::header::CONNECTION)
                .flat_map(crate::hop_by_hop::options_of)
                .any(|option| option.eq_ignore_ascii_case(b"close"));
            delivery.persistent &= !close_said;
            // A refusal that says close is what says to stop (RFC 9112 §9.5). A connection
            // that merely will not persist — HTTP/1.0, a body the close delimits — has not
            // said the body is unwanted, and an answer that came early says nothing at all,
            // because an echo answers early by nature.
            let refused = head.status.is_client_error() || head.status.is_server_error();
            // A request still being withheld for a 100 is never started by an answer:
            // the upstream answered instead of asking, so it is not waiting for a body.
            self.interim.final_head();
            let never_asked_for = self.interim.abandoned();
            let stop_uploading = never_asked_for || (refused && close_said);
            return Ok(Answer {
                nominated: crate::hop_by_hop::nominated(&head),
                head,
                delivery,
                interim,
                stop_uploading,
            });
        }
    }

    /// The exchange's absolute deadlines as they stand: none until the whole head has gone,
    /// and the wait for a `100` only while `continuing`.
    fn waits(&self, continuing: bool, limits: &H1Limits) -> Waits {
        let Some(sent) = self.head_sent else {
            return Waits::default();
        };
        Waits {
            final_head: (!self.head_bounded_elsewhere).then_some(sent + limits.final_head),
            continuing: continuing.then_some(sent + limits.continue_wait),
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
        limits: &H1Limits,
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
        // The staging buffer is lent with its bound already made, never grown to it a
        // piece at a time: grown, it lands wherever the doubling takes it — past the
        // bound, by however much the last step overshot — and what is measured is what a
        // buffer holds, not what is in it ([13 §7](../../../docs/13-http1-upstream.md)).
        // The final batch may observe EOF and write terminal framing, but cannot start
        // another payload. Otherwise a bodyless answer arriving exactly at the work
        // budget could hide request completion and unnecessarily discard the socket.
        for batch in 0..=ROUNDS {
            self.lend_staging()?;

            // Bounded work per turn: a body that keeps handing over frames must not be able
            // to hold this loop for as long as it cares to.
            for _ in 0..ROUNDS {
                if !self.payload.is_empty() || self.chunk_tail != 0 {
                    break;
                }
                let staged = self.outgoing.len();
                if staged >= STAGING {
                    break;
                }
                // What is in hand goes first: the rest of the frame, in one write. It is
                // shared with the frame rather than copied into staging, so the staging
                // bound says nothing about how much of it one write may carry, and every
                // write it is cut into is another call and another push of segments into
                // the kernel — on an 8 MiB upload, most of what it cost
                // ([13 §7](../../../docs/13-http1-upstream.md)).
                if let Some((frame, at)) = upload.pending.as_mut() {
                    if *at == frame.len() {
                        upload.pending = None;
                        continue;
                    }
                    if batch == ROUNDS {
                        break;
                    }
                    // Staging holds only the framing: a chunk's size line, and room behind
                    // it for the line ending and for the chunk that ends the body.
                    if STAGING - staged < upload.writer.framing_room() {
                        break;
                    }
                    let take = frame.len() - *at;
                    let chunked = upload.writer.data_prefix(&mut self.outgoing, take)?;
                    self.payload = frame.slice(*at..*at + take);
                    self.chunk_tail = if chunked { 2 } else { 0 };
                    *at += take;
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
                        self.stage_room(upload.writer.finish_room(upload.trailers.as_ref()))?;
                        upload.writer.finish(
                            &mut self.outgoing,
                            upload.trailers.as_ref(),
                            &upload.nominated,
                            limits,
                        )?;
                        // The end of the request is the client's last word and its best:
                        // there is nothing more to wait on it for.
                        pushed.took = true;
                        break;
                    }
                }
            }

            // And out onto the socket, as far as it will take.
            while !self.nothing_queued() {
                // Vectored writes keep the head/chunk prefix, shared payload and suffix in
                // order without assembling a second copy of the payload. The default
                // AsyncWrite implementation also works, taking only the first slice.
                let result = if self.payload.is_empty() && self.chunk_tail == 0 {
                    // Head-only requests retain their single-buffer write path.
                    Pin::new(&mut self.socket).poll_write(cx, &self.outgoing[self.written..])
                } else {
                    let slices = [
                        io::IoSlice::new(&self.outgoing[self.written..]),
                        io::IoSlice::new(&self.payload),
                        io::IoSlice::new(&b"\r\n"[2 - self.chunk_tail..]),
                    ];
                    Pin::new(&mut self.socket).poll_write_vectored(cx, &slices)
                };
                match result {
                    Poll::Pending => break,
                    Poll::Ready(Err(error)) => return Err(error.into()),
                    Poll::Ready(Ok(0)) => {
                        return Err(io::Error::from(io::ErrorKind::WriteZero).into());
                    }
                    Poll::Ready(Ok(gone)) => {
                        if self.head_left > 0 {
                            self.head_left = self.head_left.saturating_sub(gone);
                            if self.head_left == 0 {
                                self.head_sent = Some(Instant::now());
                            }
                        }
                        let prefix = gone.min(self.outgoing.len() - self.written);
                        self.written += prefix;
                        let payload = (gone - prefix).min(self.payload.len());
                        self.payload = self.payload.slice(payload..);
                        self.chunk_tail -= gone - prefix - payload;
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
            self.give_back_staging_if_empty();
            // Anything still staged is the upstream's to take, and until it does the wait is
            // the upstream's. A client asked for more while the socket is backed up is not a
            // client that is being slow, so its clock does not run while this one does.
            pushed.wants_upstream = !self.nothing_queued();
            if pushed.wants_upstream {
                pushed.wants_client = false;
            }
            if pushed.wants_upstream || pushed.wants_client || upload.writer.is_done() {
                break;
            }
        }
        // What the staging buffer holds is what is paid for, to the byte.
        #[cfg(test)]
        assert!(
            self.staging_paid_for(),
            "staging grew without being paid for"
        );
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
        waits: Option<Instant>,
        limits: &H1Limits,
    ) -> Poll<Result<Moved, ExchangeError>>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        let idle = limits.idle;
        let pushed = match self.push(cx, upload, may_send, limits) {
            Ok(pushed) => pushed,
            // A write the upstream will not take is an upstream that has stopped reading,
            // not one that has stopped talking: what it already said may be a refusal
            // sitting in the socket, which §5 says to deliver. The upload goes, and the
            // connection with it; the read below finds the answer, or the close or the
            // failure that there really was.
            Err(ExchangeError::Io(_)) => {
                upload.abandon();
                Pushed::default()
            }
            Err(error) => return Poll::Ready(Err(error)),
        };
        if pushed.took {
            clocks.client_moved();
        }
        if pushed.wrote {
            clocks.upstream_moved();
        }

        // And read, whatever the writing did. This is the part that must not be skipped.
        let (outcome, filled) = self.poll_fill(cx);
        match outcome {
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
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
        // whether that wait has gone on too long. Before the head, which this is, the
        // answer is waited for as long as the rule allows, where the rule says.
        let mut waiting = self.waiting_on(upload, pushed);
        waiting.answer &= !self.head_bounded_elsewhere;
        if let Some(stalled) = clocks.expired(cx, waiting, idle, &mut self.alarm, waits) {
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
    /// The worker's, shared rather than copied: a copy would take the body past the size
    /// the allocator serves from its fast cache, and it is boxed once a request.
    limits: Rc<H1Limits>,
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
        limits: impl Into<Rc<H1Limits>>,
    ) -> Self {
        let reader = BodyReader::nominating(framing, nominated);
        // A body that was never going to carry anything is finished before it starts. Said
        // now and not at the first poll, because nothing need ever poll an empty body.
        let complete = reader.is_done();
        Self {
            rest: Some(rest),
            reader,
            limits: limits.into(),
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
    #[cfg(any(test, feature = "fuzzing"))]
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
        S: AsyncRead + Unpin + Close,
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
        if !rest.exchange.unread().is_empty() || self.ended {
            return None;
        }
        let rest = self.rest.take()?;
        let mut socket = rest.exchange.socket;
        if !nothing_to_say(&mut socket) {
            // Out of step with us, but at the end of an exchange that finished: closed in
            // good order all the same.
            socket.close();
            return None;
        }
        Some(Kept {
            socket,
            returner: self.returner.take(),
        })
    }

    /// Keeps the connection from going back to its pool whatever the answer says: for a
    /// request that may have bound it to its client.
    pub(crate) fn not_kept(&mut self) {
        self.persistent = false;
    }

    /// Puts the connection back if it has earned its way, now that the body is over, and
    /// otherwise closes it in good order if the exchange finished with nothing owing — the
    /// upstream said it would not carry another, or closed (13 §6).
    ///
    /// Done when the body ends rather than when whoever holds it lets go: a body that is
    /// finished with has nothing more to say, and a connection that could be carrying the
    /// next request should not wait on a client to drop an object.
    pub fn settle(&mut self)
    where
        S: AsyncRead + Unpin + Close,
    {
        let limits = Rc::clone(&self.limits);
        if let Some(kept) = self.take_if_reusable() {
            kept.put_back(&limits);
            return;
        }
        if self.is_complete()
            && let Some(rest) = self
                .rest
                .take_if(|rest| !rest.upload.stopped && rest.upload_finished())
        {
            rest.exchange.socket.close();
        }
    }

    /// The connection a WebSocket handshake's 101 switched, and what was read past the 101
    /// — the first of what the backend sends in its new protocol — for a tunnel to carry
    /// ([19 §2](../../../../docs/19-websocket.md)). None if the handshake had not all gone
    /// out: RFC 9110 §7.8 has a client send its whole request before the new protocol
    /// begins, and a connection with some of it still to go is out of step. Never a way
    /// back into a pool.
    pub(crate) fn into_switched(mut self) -> Option<(S, Option<Block>)> {
        let rest = self.rest.take()?;
        if !rest.upload_finished() {
            return None;
        }
        let mut exchange = rest.exchange;
        // Everything staged has gone, so the staging buffer goes back.
        exchange.outgoing.clear();
        exchange.give_back_staging_if_empty();
        let leftover = exchange.incoming.take();
        Some((exchange.socket, leftover))
    }

    /// What is left of the connection whatever state it is in, for a caller that means to
    /// close it. Never a way back into a pool: that is [`H1Body::take_if_reusable`] alone.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn into_connection(self) -> Option<(S, Vec<u8>)> {
        self.rest.map(|rest| {
            let unread = rest.exchange.unread().to_vec();
            (rest.exchange.socket, unread)
        })
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
            let pushed = match rest.exchange.push(cx, &mut rest.upload, true, &this.limits) {
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
                .read(rest.exchange.unread(), this.ended, &this.limits)
            {
                Err(error) => {
                    // A body that stopped making sense is not an end; saying so would be
                    // handing the client half an answer as though it were the whole one.
                    this.rest = None;
                    return Poll::Ready(Some(Err(error.into())));
                }
                Ok(Piece::End { trailers, consumed }) => {
                    rest.exchange.used(consumed);
                    this.ended_with(trailers);
                    continue;
                }
                Ok(Piece::Data { data, consumed }) => {
                    if data.is_empty() {
                        // Framing bytes and nothing else; keep going.
                        rest.exchange.used(consumed);
                        continue;
                    }
                    // Cut from the block rather than copied out of it: a copy is an
                    // allocation a piece, and the allocator handing that memory back to
                    // the kernel and faulting it in again was a tenth of what a large
                    // answer cost ([13 §7](../../../docs/13-http1-upstream.md)).
                    let frame = rest.exchange.take_frame(data, consumed);
                    // What the request is waiting for, read while the request is still in
                    // hand. Its clocks run on whether the request is moving and never on
                    // whether the answer is: an upstream with plenty to say must not be
                    // able to cover for a client that has stopped. Without this the only
                    // place they are looked at is a socket read that had nothing, and an
                    // answer that always has a frame ready never reaches one
                    // ([13 §7](../../../docs/13-http1-upstream.md)).
                    let waiting = Waiting {
                        answer: false,
                        ..rest.exchange.waiting_on(&rest.upload, pushed)
                    };
                    let idle = this.limits.idle;
                    let alarm = &mut rest.exchange.alarm;
                    if let Some(stalled) = this.clocks.expired(cx, waiting, idle, alarm, None) {
                        this.rest = None;
                        return Poll::Ready(Some(Err(ExchangeError::Idle {
                            after: idle,
                            waiting: stalled,
                        })));
                    }
                    // Whether that was the last of it is worth knowing now: a client
                    // told how long a body is need never poll it again, and a body whose
                    // end was never checked is one whose connection cannot be trusted.
                    // Asked only where the answer is already certain — reading to find
                    // out would move the reader past bytes still sitting in the buffer.
                    if this.reader.is_spent() {
                        let settled =
                            this.reader
                                .read(rest.exchange.unread(), this.ended, &this.limits);
                        if let Ok(Piece::End { trailers, consumed }) = settled {
                            rest.exchange.used(consumed);
                            this.ended_with(trailers);
                        }
                    }
                    this.clocks.answer_moved();
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Ok(Piece::More) => {}
            }

            // Only now, and only because somebody asked for a frame.
            let (outcome, filled) = rest.exchange.poll_fill(cx);
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
                    let alarm = &mut rest.exchange.alarm;
                    let Some(stalled) = this.clocks.expired(cx, waiting, idle, alarm, None) else {
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
                    return Poll::Ready(Some(Err(error)));
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
        // A counted body says what is left of it, which is how a server passing it on
        // knows to give it a length rather than chunks.
        match self.reader.remaining() {
            Some(left) if self.rest.is_some() => SizeHint::with_exact(left),
            _ => SizeHint::default(),
        }
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

impl<S: Close> Kept<S> {
    /// Puts the connection back where it came from. A connection that came from nowhere
    /// goes nowhere: it is closed here, which is the only other thing to do with one.
    pub fn put_back(self, limits: &H1Limits) {
        match self.returner {
            Some(lease) => lease.keep(self.socket, limits),
            None => self.socket.close(),
        }
    }
}

impl<S> Kept<S> {
    /// The connection itself, for a caller that means to do something else with it.
    #[cfg(any(test, feature = "fuzzing"))]
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
pub(crate) fn expects_continue<F: Fields + ?Sized>(headers: &F) -> bool {
    headers
        .values(&http::header::EXPECT)
        .flat_map(crate::hop_by_hop::options_of)
        .any(|option| option.eq_ignore_ascii_case(b"100-continue"))
}

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

/// The exchange's absolute deadlines, from the moment its head went: the final answer's
/// head, and the end of the wait for a `100` while the body is held back for one.
#[derive(Debug, Clone, Copy, Default)]
struct Waits {
    final_head: Option<Instant>,
    continuing: Option<Instant>,
}

impl Waits {
    /// The soonest of these and `also`.
    fn soonest(self, also: Option<Instant>) -> Option<Instant> {
        [self.final_head, self.continuing, also]
            .into_iter()
            .flatten()
            .min()
    }
}

/// When each wait runs out, one clock per thing that can be waited for.
///
/// A clock runs only while its own thing is being waited for: it starts when that starts
/// and stops when it stops, so time spent waiting for something else is not counted
/// against it. Progress in one direction never touches another's
/// ([13 §7](../../../docs/13-http1-upstream.md)). They are deadlines rather than timers:
/// the exchange's one [`Alarm`] keeps them, with its other deadlines.
#[derive(Debug, Default)]
struct Clocks {
    client: Option<Instant>,
    upstream: Option<Instant>,
    answer: Option<Instant>,
}

impl Clocks {
    /// Sets the clocks to `on`, and says which has run out.
    ///
    /// One that is not running is stopped rather than paused, which is what makes the
    /// next wait a fresh one. A clock that is already running keeps running: the same
    /// wait going on is not a new wait. `waits` is the soonest of the exchange's other
    /// deadlines, which the alarm keeps too; when that is what came, the task is woken
    /// to find it where it is looked for.
    fn expired(
        &mut self,
        cx: &mut Context<'_>,
        on: Waiting,
        idle: Duration,
        alarm: &mut Alarm,
        waits: Option<Instant>,
    ) -> Option<Stalled> {
        let mut now = None;
        let each = [
            (&mut self.upstream, on.upstream),
            (&mut self.client, on.client),
            (&mut self.answer, on.answer),
        ];
        for (clock, running) in each {
            if !running {
                *clock = None;
            } else if clock.is_none() {
                *clock = Some(*now.get_or_insert_with(Instant::now) + idle);
            }
        }
        let soonest = [self.soonest(), waits].into_iter().flatten().min()?;
        if alarm.poll_until(cx, soonest).is_pending() {
            return None;
        }
        let ran_out = [
            (self.upstream, Stalled::Upstream),
            (self.client, Stalled::Client),
            (self.answer, Stalled::Answer),
        ]
        .into_iter()
        .find_map(|(due, which)| due.filter(|&due| alarm.reached(due)).map(|_| which));
        if ran_out.is_none() {
            cx.waker().wake_by_ref();
        }
        ran_out
    }

    /// When the first of the running clocks runs out.
    fn soonest(&self) -> Option<Instant> {
        [self.upstream, self.client, self.answer]
            .into_iter()
            .flatten()
            .min()
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

/// Blocks of their own, for a test that is not a worker.
#[cfg(test)]
fn test_blocks() -> Rc<RefCell<Blocks>> {
    Rc::new(RefCell::new(Blocks::new(
        super::blocks::Sizes::default(),
        crate::storage::Storage::new(crate::storage::LIMIT),
    )))
}

#[cfg(test)]
impl<S> Exchange<S> {
    /// Puts `bytes` where a read would have, for a test that starts part way through.
    fn holding(&mut self, bytes: &[u8]) {
        let mut blocks = self.blocks.borrow_mut();
        let mut block = blocks.take().unwrap();
        if bytes.len() > block.capacity() {
            block = blocks.grow(block).unwrap();
        }
        block.room()[..bytes.len()].copy_from_slice(bytes);
        block.arrived(bytes.len());
        drop(blocks);
        self.incoming = Some(block);
    }
}

#[cfg(test)]
impl Held {
    fn total(self) -> usize {
        self.staged + self.buffered + self.frame
    }

    /// The staging buffer and the read block: what this code holds, without the frame
    /// whose size is the client's choice.
    fn buffers(self) -> (usize, usize) {
        (self.staged, self.buffered)
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
            buffered: rest.exchange.incoming.as_ref().map_or(0, Block::capacity),
            frame,
            frames,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::h1::blocks::{SMALL, Sizes};
    use http::HeaderValue;
    use http::StatusCode;
    use http_body_util::{Empty, Full};
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// Closed as it is dropped: the tests' upstream has no closure alert to be sent.
    impl Close for DuplexStream {
        fn close(self) {}
    }

    /// A body that never gives anything and never ends: a client that has stopped
    /// sending without saying so.
    #[derive(Debug)]
    struct Silent;

    impl Body for Silent {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
            Poll::Pending
        }
    }

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
        ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
            if self.0 {
                return Poll::Ready(Some(Err("the client went away")));
            }
            self.0 = true;
            let frame = Frame::data(Bytes::from_static(b"ab"));
            Poll::Ready(Some(Ok(frame)))
        }
    }

    thread_local! {
        /// The test's timers: each test runs on a thread of its own.
        static TIMERS: Rc<Timers> = Timers::new();
    }

    /// The timers of the test running, which a test whose deadlines must come waits on
    /// with [`Timers::driving`].
    fn timers() -> Rc<Timers> {
        TIMERS.with(Rc::clone)
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
        (Exchange::new(ours, test_blocks(), timers()), Peer(theirs))
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
        rest.exchange.unread().to_vec()
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

    /// An exchange sets one timer for all its waits, from the head going out to the last
    /// of its answer's body: the final head's deadline, the answer's clock, then the
    /// body's, which each start as the one before is done with. Setting a timer is what
    /// costs, and it was once each of them. The clock is stopped, so the waits take none.
    #[tokio::test(start_paused = true)]
    async fn an_exchange_sets_one_timer_for_all_its_waits() {
        use http_body_util::BodyExt;
        let (exchange, mut peer) = connected(4096);
        let limits = H1Limits::default();
        let before = crate::timers::times_queued();
        let answering = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nhello")
                .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            peer.say("there").await;
            peer
        });

        let (answer, rest) = exchange
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap();
        let framing = answer.delivery.framing;
        let body = H1Body::new(rest, framing, true, Vec::new(), limits);
        let read = body.collect().await.unwrap().to_bytes();
        assert_eq!(&read[..], b"hellothere");
        let _peer = answering.await.unwrap();
        assert_eq!(crate::timers::times_queued() - before, 1);
    }

    /// **A finished exchange gives back what it was lent.** The block it read the answer
    /// into and the buffer it staged the request in are both the worker's; either one
    /// going with the exchange instead would be memory made again for the next request,
    /// which is the whole of what lending them saves, and nothing else would notice.
    #[tokio::test]
    async fn a_finished_exchange_gives_back_what_it_was_lent() {
        let blocks = test_blocks();
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut peer = Peer(theirs);
        let limits = H1Limits::default();
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello")
                .await;
            peer
        });

        let (answer, rest) = Exchange::new(ours, Rc::clone(&blocks), timers())
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap();
        let _peer = answered.await.unwrap();
        let mut body = H1Body::new(
            rest,
            answer.delivery.framing,
            answer.delivery.persistent,
            answer.nominated,
            limits,
        );
        let (data, _trailers) = collected(&mut body).await.unwrap();
        assert_eq!(data, b"hello");

        // Both lent while they were wanted and back now: one block and one staging
        // buffer, however many times either was given back empty and lent again in
        // between, because neither is taken while one is held.
        assert_eq!(
            blocks.borrow().parked(),
            2,
            "the block or the staging buffer went with the exchange"
        );
        assert!(
            body.take_if_reusable().is_some(),
            "a clean exchange was not kept"
        );
    }

    /// **A head bigger than a block is read whole.** A block starts at the read bound and
    /// a head may be four times that ([13 §7](../../../docs/13-http1-upstream.md)): the
    /// block it outgrows is grown, and the head is none the wiser.
    #[tokio::test]
    async fn a_head_bigger_than_a_block_is_read_whole() {
        let (exchange, mut peer) = connected(4096);
        let limits = H1Limits::default();
        // One field long enough to fill the first block by itself, and well inside the
        // head bound.
        let length = SMALL + SMALL / 4;
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            let long = "a".repeat(length);
            peer.say(&format!(
                "HTTP/1.1 200 OK\r\nx-long: {long}\r\ncontent-length: 0\r\n\r\n"
            ))
            .await;
            peer
        });

        let (answer, _rest) = exchange
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap();
        let _peer = answered.await.unwrap();

        assert_eq!(answer.head.status, 200);
        assert_eq!(answer.head.to_map()["x-long"].len(), length);
    }

    /// **An answer's head holds no block.** It is copied out of the block it was read
    /// into, so once the exchange and its blocks have gone nothing of the worker's is left
    /// to count while the answer is still held: a head lives only until it is written, and
    /// holding the block for it would cost setting the block to zeros again (14 §8).
    #[tokio::test]
    async fn an_answers_head_holds_no_block() {
        let blocks = test_blocks();
        let storage = Rc::clone(blocks.borrow().storage());
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut peer = Peer(theirs);
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\nx-a: 1\r\ncontent-length: 0\r\n\r\n")
                .await;
            peer
        });

        let (answer, rest) = Exchange::new(ours, Rc::clone(&blocks), timers())
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &H1Limits::default(),
            )
            .await
            .unwrap();
        let _peer = answered.await.unwrap();
        drop(rest);
        blocks.borrow_mut().trim(0);
        storage.sweep();

        assert_eq!(storage.outlived(), 0, "the head holds its block's memory");
        assert_eq!(storage.used(), 0, "the head holds its block's memory");
        let head = answer.head.into_answer();
        assert_eq!(
            head.fields()
                .values_of(&HeaderName::from_static("x-a"))
                .next(),
            Some(&b"1"[..])
        );
    }

    /// **And an answer no block can hold is a failure, not a close.** Not reached with
    /// blocks sized from the limits, which is what makes it worth a test of its own: a
    /// read into no room at all comes back with nothing, and nothing is what a close looks
    /// like. Blocks made too small for the limits show which of the two it is taken for.
    #[tokio::test]
    async fn an_answer_no_block_can_hold_is_a_failure_and_not_a_close() {
        let tiny = Sizes {
            small: 8,
            large: 16,
            parked: 1,
            kept: 1,
            cut: 4,
        };
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut peer = Peer(theirs);
        let limits = H1Limits::default();
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            // Longer than the largest block, far short of the head bound, and not over:
            // the upstream is still there and has more to say.
            peer.say("HTTP/1.1 200 OK\r\nx-long: aaaaaaaaaaaaaaaa")
                .await;
            peer
        });

        let error = Exchange::new(
            ours,
            Rc::new(RefCell::new(Blocks::new(
                tiny,
                crate::storage::Storage::new(crate::storage::LIMIT),
            ))),
            timers(),
        )
        .send(
            &Method::GET,
            &"/".parse().unwrap(),
            &headers(&[("host", "up.test")]),
            &[],
            Sending::None,
            Empty::<Bytes>::new(),
            &limits,
        )
        .await
        .unwrap_err();
        let _peer = answered.await.unwrap();

        assert!(matches!(error, ExchangeError::Io(_)), "{error}");
    }

    /// An exchange failed because the worker could not pay for what it needed.
    fn refused_for_storage(error: &ExchangeError) -> bool {
        matches!(error, ExchangeError::Exhausted(_))
    }

    /// Blocks paying against an account of `limit` bytes.
    fn blocks_within(limit: usize) -> Rc<RefCell<Blocks>> {
        Rc::new(RefCell::new(Blocks::new(
            Sizes::default(),
            crate::storage::Storage::new(limit),
        )))
    }

    /// **And a worker that cannot pay for a block to read an answer into fails the
    /// exchange**, at once and saying why, rather than reading into nothing — which would
    /// be taken for a close — or waiting for memory to come free (14 §8). It can pay for
    /// the staging the request goes out in, and no more, so the request does go out: the
    /// upstream sees the head before the read that fails.
    #[tokio::test]
    async fn an_answer_the_worker_cannot_pay_to_read_is_a_failure() {
        let (ours, theirs) = tokio::io::duplex(4096);
        let mut peer = Peer(theirs);
        let limits = H1Limits::default();
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            // The exchange may have failed and gone already, as it should.
            let _gone = peer
                .0
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            peer
        });

        let error = Exchange::new(ours, blocks_within(STAGING), timers())
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test")]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        let _peer = answered.await.unwrap();
        assert!(refused_for_storage(&error), "{error}");
    }

    /// A head too long for the staging buffer grows it, and the growth is paid for before
    /// it is made: a worker that cannot pay fails the exchange before a byte of the head has
    /// gone (14 §8).
    #[tokio::test]
    async fn a_head_the_worker_cannot_pay_to_stage_is_refused_before_it_is_sent() {
        let (ours, mut theirs) = tokio::io::duplex(1 << 17);
        let limits = H1Limits::default();
        let long = "a".repeat(40 * 1024);
        let error = Exchange::new(ours, blocks_within(48 * 1024), timers())
            .send(
                &Method::GET,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test"), ("x-long", &long)]),
                &[],
                Sending::None,
                Empty::<Bytes>::new(),
                &limits,
            )
            .await
            .unwrap_err();
        assert!(refused_for_storage(&error), "{error}");
        let mut sent = Vec::new();
        theirs.read_to_end(&mut sent).await.unwrap();
        assert!(sent.is_empty(), "{} bytes went out", sent.len());
    }

    /// A chunked body of nothing but trailers ends while its head is still staged, unsent,
    /// so the room made for the trailers is on top of what is already there: the growth
    /// is paid for at the buffer's whole new size, and the exchange goes through (14 §8).
    #[tokio::test]
    async fn trailers_behind_a_head_still_staged_are_paid_for_in_full() {
        let (exchange, mut peer) = connected(1 << 17);
        let limits = H1Limits {
            trailers: 64 * 1024,
            ..H1Limits::default()
        };
        let answered = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            let rest = peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 204 No Content\r\n\r\n").await;
            rest.len()
        });
        let mut trailers = HeaderMap::new();
        trailers.insert(
            HeaderName::from_static("x-long"),
            HeaderValue::from_str(&"a".repeat(20 * 1024)).unwrap(),
        );
        let body = Frames {
            left: 0,
            size: 0,
            trailers: Some(trailers),
        };
        let (head, _rest) = exchange
            .send(
                &Method::POST,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test"), ("te", "trailers")]),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap();
        assert_eq!(head.head.status, StatusCode::NO_CONTENT);
        assert!(
            answered.await.unwrap() > 20 * 1024,
            "the trailers never went"
        );
    }

    /// The same for the end of a chunked body, whose trailers may need more room than the
    /// staging buffer has: that room is paid for before the section is written (14 §8).
    #[tokio::test]
    async fn trailers_the_worker_cannot_pay_to_stage_fail_the_exchange() {
        let (ours, mut theirs) = tokio::io::duplex(1 << 17);
        let draining = tokio::spawn(async move {
            let mut sent = Vec::new();
            let _ended = theirs.read_to_end(&mut sent).await;
            sent
        });
        let limits = H1Limits {
            trailers: 64 * 1024,
            ..H1Limits::default()
        };
        let mut trailers = HeaderMap::new();
        trailers.insert(
            HeaderName::from_static("x-long"),
            HeaderValue::from_str(&"a".repeat(40 * 1024)).unwrap(),
        );
        let body = Frames {
            left: 1,
            size: 10,
            trailers: Some(trailers),
        };
        let error = Exchange::new(ours, blocks_within(48 * 1024), timers())
            .send(
                &Method::POST,
                &"/".parse().unwrap(),
                &headers(&[("host", "up.test"), ("te", "trailers")]),
                &[],
                Sending::Chunked,
                body,
                &limits,
            )
            .await
            .unwrap_err();
        assert!(refused_for_storage(&error), "{error}");
        let sent = draining.await.unwrap();
        assert!(
            !sent.windows(6).any(|at| at == b"x-long"),
            "the trailers went out"
        );
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
        assert!(!answer.head.to_map().contains_key("link"));
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
        let mut exchange = Exchange::new(ours, test_blocks(), timers());
        exchange.holding(buffered);
        // A request with nothing in it, already all sent.
        let mut upload = Upload::new(Empty::<Bytes>::new(), Sending::None, Vec::new());
        let mut nothing = Vec::new();
        upload
            .writer
            .finish(&mut nothing, None, &[], &H1Limits::default())
            .unwrap();
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
        let mut exchange = Exchange::new(ours, test_blocks(), timers());
        // The whole answer is already in hand, so nothing about the upstream is at fault.
        exchange.holding(b"ok");
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

    /// An upstream with plenty to say must not be able to hide a request that has
    /// stopped. The clocks for the request run on whether the request is moving, and
    /// an answer with a frame ready is not that: a client that went quiet mid-upload
    /// would otherwise hold the exchange for as long as the upstream kept talking
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    #[tokio::test(start_paused = true)]
    async fn an_answer_that_keeps_coming_does_not_hide_a_stalled_request() {
        timers()
            .driving(async {
                let limits = H1Limits::default();
                let (ours, theirs) = tokio::io::duplex(4096);
                let mut exchange = Exchange::new(ours, test_blocks(), timers());
                // Chunk after chunk, all of it already in hand, so every ask has a frame
                // ready without the socket being touched.
                exchange.holding("4\r\nabcd\r\n".repeat(64).as_bytes());
                // And a client that has handed over nothing, and will not.
                let upload = Upload::new(Silent, Sending::Chunked, Vec::new());
                let rest = Rest { exchange, upload };
                let mut body = H1Body::new(rest, Framing::Chunked, true, Vec::new(), limits);
                let _peer = Peer(theirs);

                let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
                assert!(matches!(frame, Some(Ok(_))), "{frame:?}");
                // Time passes with the request still going nowhere.
                tokio::time::advance(limits.idle + Duration::from_secs(1)).await;

                let error = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                    .await
                    .expect("the answer ended rather than the request being noticed")
                    .unwrap_err();
                assert!(
                    matches!(
                        error,
                        ExchangeError::Idle {
                            waiting: Stalled::Client,
                            ..
                        }
                    ),
                    "the request stopped and the answer went on covering for it: {error}"
                );
            })
            .await;
    }

    /// What is staged stays inside its bound, the framing it adds included. Payload
    /// written to the brim and a size line put after it is how a bound is quietly
    /// gone past ([13 §7](../../../docs/13-http1-upstream.md)).
    #[tokio::test]
    async fn staging_keeps_room_for_the_framing_it_adds() {
        // A socket that takes almost nothing, so what is staged stays staged.
        let (ours, _theirs) = tokio::io::duplex(1);
        let mut exchange = Exchange::new(ours, test_blocks(), timers());
        let mut upload = Upload::new(
            Full::new(Bytes::from(vec![b'x'; STAGING * 2])),
            Sending::Chunked,
            Vec::new(),
        );
        let _pushed = exchange
            .push(
                &mut Context::from_waker(Waker::noop()),
                &mut upload,
                true,
                &H1Limits::default(),
            )
            .unwrap();
        assert!(
            exchange.outgoing.len() <= STAGING,
            "{} bytes staged where the bound is {STAGING}",
            exchange.outgoing.len()
        );
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
            exchange: Exchange::new(ours, test_blocks(), timers()),
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

    #[tokio::test(start_paused = true)]
    async fn final_head_deadline_starts_after_the_request_head_is_written() {
        timers()
            .driving(async {
                let (exchange, mut peer) = connected(1);
                let limits = H1Limits {
                    final_head: Duration::from_secs(5),
                    idle: Duration::from_secs(30),
                    ..H1Limits::default()
                };
                let holding = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    peer.until(b"\r\n\r\n").await;
                    std::future::pending::<()>().await;
                    drop(peer);
                });
                let began = Instant::now();
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
                holding.abort();
                assert!(matches!(failed, ExchangeError::TooSlow { .. }), "{failed}");
                assert_eq!(began.elapsed(), Duration::from_secs(15));
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_blocked_request_head_uses_upload_write_idle() {
        timers()
            .driving(async {
                let (exchange, _peer) = connected(1);
                let limits = H1Limits {
                    final_head: Duration::from_secs(5),
                    idle: Duration::from_secs(10),
                    ..H1Limits::default()
                };
                let began = Instant::now();
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
                    matches!(
                        failed,
                        ExchangeError::Idle {
                            waiting: Stalled::Upstream,
                            ..
                        }
                    ),
                    "{failed}"
                );
                assert_eq!(began.elapsed(), limits.idle);
            })
            .await;
    }

    /// An upstream that takes the request, says nothing, and stays. Time is the test's to
    /// move, so nothing here really waits a minute.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_never_answers_is_given_up_on() {
        timers()
            .driving(async {
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
            })
            .await;
    }

    /// An upstream that keeps something happening never goes idle, and is still given up
    /// on: the time for a final head is counted from transmission of the request head
    /// and is not extended by an upstream that stays busy.
    #[tokio::test(start_paused = true)]
    async fn an_upstream_that_dribbles_does_not_buy_itself_more_time() {
        timers()
            .driving(async {
            let (exchange, mut peer) = connected(4096);
            tokio::spawn(async move {
                peer.until(b"\r\n\r\n").await;
                // A byte of a head that never ends, often enough never to be idle. It starts
                // as a head would, so that the start is not what refuses it.
                peer.say("HTTP/1.1 200 OK\r\nx-a: ").await;
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
            })
            .await;
    }

    /// Nor do interim answers, which is the same rule said of the other way an upstream
    /// can look busy without getting anywhere.
    #[tokio::test(start_paused = true)]
    async fn interim_answers_do_not_buy_more_time_either() {
        timers()
            .driving(async {
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
            })
            .await;
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
        timers()
            .driving(async {
                let (mut body, _peer) = body_on(Framing::Length(8), b"hel");
                let failed = collected(&mut body).await.unwrap_err();
                assert!(matches!(failed, ExchangeError::Idle { .. }), "{failed}");
                assert!(!body.is_complete());
            })
            .await;
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
        timers()
            .driving(async {
                let idle = H1Limits::default().idle;
                let (ours, theirs) = tokio::io::duplex(4096);
                let rest = Rest {
                    exchange: Exchange::new(ours, test_blocks(), timers()),
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
            })
            .await;
    }

    /// And the other way round: an upstream that answers but stops taking the request is
    /// given up on for that, not excused by the answer it is still sending.
    #[tokio::test(start_paused = true)]
    async fn an_answer_still_arriving_does_not_excuse_an_upstream_that_stopped_reading() {
        timers()
            .driving(async {
                let idle = H1Limits::default().idle;
                // Room for a little of the request and no more, so the rest stays staged.
                let (ours, theirs) = tokio::io::duplex(64);
                let rest = Rest {
                    exchange: Exchange::new(ours, test_blocks(), timers()),
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
            })
            .await;
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
        timers()
            .driving(async {
                let idle = H1Limits::default().idle;
                // Room for a little of the request and no more, so the rest stays staged and the
                // client is asked for nothing while it waits.
                let (ours, theirs) = tokio::io::duplex(64);
                let rest = Rest {
                    exchange: Exchange::new(ours, test_blocks(), timers()),
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
            })
            .await;
    }

    // ---- what one exchange holds ----

    /// A request body of `left` frames of `size` bytes, handed over as fast as asked for.
    #[derive(Debug)]
    struct Frames {
        left: usize,
        size: usize,
        /// Sent after the last of them, where a body carries any.
        trailers: Option<HeaderMap>,
    }

    impl Body for Frames {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            if self.left == 0 {
                return Poll::Ready(
                    self.trailers
                        .take()
                        .map(|fields| Ok(Frame::trailers(fields))),
                );
            }
            self.left -= 1;
            let size = self.size;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'x'; size])))))
        }
    }

    /// Sends `frames` frames of `size` upstream and reads `chunks` chunks of `size` back,
    /// and says the most this end was holding at any point along the way.
    async fn holding(frames: usize, chunks: usize, size: usize) -> Held {
        holding_sent(frames, chunks, size, Sent::Counted).await
    }

    /// How a request's body is framed on its way out, which is what the staging buffer
    /// has to hold besides the bytes themselves.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Sent {
        /// A length known in advance: the bytes and nothing else.
        Counted,
        /// In chunks: a size line before every frame, and the chunk that ends them.
        Chunked,
        /// The same, with fields after the last chunk.
        WithTrailers,
    }

    async fn holding_sent(frames: usize, chunks: usize, size: usize, sent: Sent) -> Held {
        let total = frames * size;
        let trailers = (sent == Sent::WithTrailers).then(|| {
            let mut fields = HeaderMap::new();
            fields.insert(
                HeaderName::from_static("x-done"),
                HeaderValue::from_static("yes"),
            );
            fields
        });
        let sending = match sent {
            Sent::Counted => Sending::Length(total as u64),
            Sent::Chunked | Sent::WithTrailers => Sending::Chunked,
        };
        let (ours, theirs) = tokio::io::duplex(4096);
        let rest = Rest {
            exchange: Exchange::new(ours, test_blocks(), timers()),
            upload: Upload::new(
                Frames {
                    left: frames,
                    size,
                    trailers,
                },
                sending,
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
            // After every poll, including the ones with nothing to show for it. A buffer
            // is held while something is in it and given back when not, so the moments it
            // is held are the ones spent waiting -- which are the polls that come back
            // with nothing, and the only ones that would see the upload at work.
            let polled = poll_fn(|cx| {
                let polled = Pin::new(&mut body).poll_frame(cx);
                watch(body.held());
                polled
            })
            .await;
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
    /// The bound is exactly the buffers this code holds at its most -- one for staging the
    /// request and one block, lent, for reading the answer -- and it is asserted as an
    /// equality rather than a ceiling, because a ceiling would not notice a buffer that
    /// had begun to creep.
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
            assert_eq!(
                most.buffers(),
                smallest.buffers(),
                "{frames} frames of {size}: {most:?}"
            );
            // The frame in hand is the client's size, and there is one at most.
            assert!(most.frames <= 1, "frames queued up: {most:?}");
        }
        // Payload is shared with the held frame, so no staging allocation survives a
        // push in this fixture; the response still holds one read block.
        assert_eq!(smallest.buffers(), (0, SMALL), "{smallest:?}");
    }

    /// **And a chunked request holds no more than a counted one.** A chunk carries a size
    /// line of its own and the chunk that ends the body, and room is kept back for both,
    /// so the staging buffer is the same buffer whichever way the request is framed.
    ///
    /// A request that carries trailers is the one exception, and it is bounded rather
    /// than free: the section is staged in one piece, and what it may come to is the
    /// trailer bound of [13 §7](../../../docs/13-http1-upstream.md).
    #[tokio::test]
    async fn a_chunked_request_holds_what_a_counted_one_holds() {
        let counted = holding_sent(16, 16, 4 * 1024, Sent::Counted).await;
        let chunked = holding_sent(16, 16, 4 * 1024, Sent::Chunked).await;
        assert_eq!(
            chunked.buffers(),
            counted.buffers(),
            "chunked held more than counted: {chunked:?}"
        );
        assert_eq!(chunked.buffers(), (0, SMALL), "{chunked:?}");

        // With trailers on the end, what is staged may reach the section's own bound
        // besides -- and no further.
        let trailing = holding_sent(16, 16, 4 * 1024, Sent::WithTrailers).await;
        assert!(
            trailing.staged + trailing.buffered <= STAGING + SMALL + H1Limits::default().trailers,
            "{trailing:?} is past the staging and trailer bounds together"
        );
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
            assert_eq!(
                most.buffers(),
                alone.buffers(),
                "exchange {which}: {most:?}"
            );
        }
    }

    /// **One frame in hand, never a queue of them.** A client with sixty-four frames to
    /// give and an upstream that will take almost none of them is a client that is not
    /// asked for the next one: what is held is the frame being forwarded and the staging
    /// buffer, however much more is offered
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    ///
    /// And no block to read into, because the upstream has said nothing: a block is lent
    /// while there is something in it and given back the moment there is not, so an
    /// exchange waiting on a silent upstream is not holding a buffer against the chance.
    ///
    /// The frame counts at its own size, which is the client's choice and not this end's.
    /// What this end promises is that there is one.
    #[tokio::test]
    async fn only_one_frame_of_the_request_is_ever_in_hand() {
        let size = 64 * 1024;
        // Room for a little of the request and no more, so the frames have to wait.
        let (ours, _theirs) = tokio::io::duplex(64);
        let rest = Rest {
            exchange: Exchange::new(ours, test_blocks(), timers()),
            upload: Upload::new(
                Frames {
                    left: 64,
                    size,
                    trailers: None,
                },
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
        assert_eq!(
            held.buffered, 0,
            "a block held for an answer nobody sent: {held:?}"
        );
        assert_eq!(held.total(), size, "{held:?}");
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
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.asked.store(true, Ordering::SeqCst);
            Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
        }
    }

    fn expecting() -> HeaderMap {
        headers(&[("host", "up.test"), ("expect", "100-continue")])
    }

    #[tokio::test(start_paused = true)]
    async fn continue_wait_starts_after_the_request_head_is_written() {
        timers()
            .driving(async {
                let (exchange, mut peer) = connected(1);
                let (body, asked) = Watched::new(b"hello");
                let peering = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    let asked_before_head = asked.load(Ordering::SeqCst);
                    peer.until(b"\r\n\r\n").await;
                    let sent = Instant::now();
                    let body = peer.until(b"0\r\n\r\n").await;
                    let waited = sent.elapsed();
                    peer.say("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .await;
                    (asked_before_head, waited, body)
                });
                let limits = H1Limits::default();
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
                let (asked_before_head, waited, body) = peering.await.unwrap();
                assert!(
                    !asked_before_head,
                    "the continue timer ran during the head write"
                );
                assert_eq!(waited, limits.continue_wait);
                assert_eq!(body, b"5\r\nhello\r\n0\r\n\r\n");
                assert_eq!(answer.head.status, 200);
            })
            .await;
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
        timers()
            .driving(async {
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
            })
            .await;
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

    /// Only a refusal that says `close` stops an upload: RFC 9112 §9.5 asks a client to
    /// stop where the server "does not wish to receive the message body and is closing
    /// the connection". A refusal on a connection that merely will not persist — HTTP/1.0,
    /// or a body the close delimits — has said neither, and a close said on this exchange's
    /// interim head is said all the same.
    #[tokio::test(start_paused = true)]
    async fn only_a_refusal_that_says_close_stops_the_upload() {
        for (answer, stops) in [
            (
                "HTTP/1.1 413 Too Large\r\nconnection: close\r\ncontent-length: 0\r\n\r\n",
                true,
            ),
            (
                "HTTP/1.1 100 Continue\r\nconnection: close\r\n\r\n\
                 HTTP/1.1 413 Too Large\r\ncontent-length: 0\r\n\r\n",
                true,
            ),
            ("HTTP/1.1 413 Too Large\r\ncontent-length: 0\r\n\r\n", false),
            ("HTTP/1.0 413 Too Large\r\ncontent-length: 0\r\n\r\n", false),
            ("HTTP/1.1 413 Too Large\r\n\r\n", false),
            (
                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 0\r\n\r\n",
                false,
            ),
        ] {
            let (exchange, mut peer) = connected(4096);
            tokio::spawn(async move {
                peer.until(b"\r\n\r\n").await;
                peer.say(answer).await;
                peer
            });
            let (body, _asked) = Watched::new(b"hello");
            let (got, _rest) = exchange
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
                .unwrap();
            assert_eq!(got.stop_uploading, stops, "{answer:?}");
        }
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

    /// Reads an answer's body to its end, and says whether the connection was kept.
    async fn finished<B>(answer: Answer, rest: Rest<DuplexStream, B>) -> (Vec<u8>, bool)
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut body = H1Body::new(
            rest,
            answer.delivery.framing,
            answer.delivery.persistent,
            answer.nominated,
            H1Limits::default(),
        );
        let (data, _) = collected(&mut body).await.unwrap();
        (data, body.take_if_reusable().is_some())
    }

    /// Asking to be told before sending a body that has nothing in it holds nothing back,
    /// so an answer that never said 100 abandons nothing, and the connection is as good as
    /// any other (from Pingora, where the expectation has no bearing on keeping one).
    #[tokio::test(start_paused = true)]
    async fn an_expectation_with_nothing_to_hold_back_costs_nothing() {
        let (exchange, mut peer) = connected(4096);
        let _peering = tokio::spawn(async move {
            peer.until(b"\r\n\r\n").await;
            peer.say("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await;
            peer
        });
        let (answer, rest) = exchange
            .send(
                &Method::POST,
                &"/x".parse().unwrap(),
                &expecting(),
                &[],
                Sending::Length(0),
                Empty::<Bytes>::new(),
                &H1Limits::default(),
            )
            .await
            .unwrap();
        let (data, kept) = finished(answer, rest).await;
        assert_eq!(data, b"ok");
        assert!(
            kept,
            "a connection was given up for a body that had nothing in it"
        );
    }

    /// A refusal while a body is held back, that does not close: the body — here a whole
    /// second request, the shape a smuggling attempt takes — never reaches the upstream,
    /// and the connection is not lent to anyone else, because what it carried is not
    /// finished (Envoy's `NonWebsocketUpgradeWithPrePayloadDoesNotPoisonConnection`).
    #[tokio::test(start_paused = true)]
    async fn a_body_held_back_from_a_refusal_never_arrives_and_the_connection_goes() {
        const SMUGGLED: &[u8] = b"GET /smuggled HTTP/1.1\r\nhost: up.test\r\n\r\n";
        for refusal in [
            "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n",
            "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
        ] {
            let (exchange, mut peer) = connected(4096);
            let (body, asked) = Watched::new(SMUGGLED);
            let peering = tokio::spawn(async move {
                peer.until(b"\r\n\r\n").await;
                peer.say(refusal).await;
                // Everything else the connection ever carries, until it closes.
                let mut rest = Vec::new();
                peer.0.read_to_end(&mut rest).await.unwrap();
                rest
            });
            let limits = H1Limits {
                continue_wait: Duration::from_secs(60),
                ..H1Limits::default()
            };
            let (answer, rest) = exchange
                .send(
                    &Method::POST,
                    &"/x".parse().unwrap(),
                    &expecting(),
                    &[],
                    Sending::Length(SMUGGLED.len() as u64),
                    body,
                    &limits,
                )
                .await
                .unwrap();
            assert!(answer.delivery.persistent, "{refusal}");
            let (_, kept) = finished(answer, rest).await;
            assert!(!kept, "{refusal}: a connection owed a body was lent on");
            assert!(
                !asked.load(Ordering::SeqCst),
                "{refusal}: the body was asked for"
            );
            let after = peering.await.unwrap();
            assert!(
                after.is_empty(),
                "{refusal}: {:?}",
                String::from_utf8_lossy(&after)
            );
        }
    }

    /// A body that gives a frame, waits a while, and then fails: long enough for what it
    /// gave to have gone out before it does.
    #[derive(Debug)]
    struct FailingLater {
        gave: bool,
        waiting: Pin<Box<tokio::time::Sleep>>,
    }

    impl FailingLater {
        fn new() -> Self {
            Self {
                gave: false,
                waiting: Box::pin(tokio::time::sleep(Duration::from_secs(1))),
            }
        }
    }

    impl Body for FailingLater {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
            if !self.gave {
                self.gave = true;
                let frame = Frame::data(Bytes::from_static(b"ab"));
                return Poll::Ready(Some(Ok(frame)));
            }
            match self.waiting.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => Poll::Ready(Some(Err("the client went away"))),
            }
        }
    }

    /// A request body that fails part way leaves the upstream with a request that is
    /// visibly unfinished: no zero chunk to end it, no padding to make up a length
    /// (hyper's client tests).
    #[tokio::test(start_paused = true)]
    async fn a_request_body_that_fails_is_never_made_to_look_finished() {
        for (sending, expected) in [
            (Sending::Chunked, &b"2\r\nab\r\n"[..]),
            (Sending::Length(10), &b"ab"[..]),
        ] {
            let (exchange, mut peer) = connected(4096);
            // Everything after the head, until the connection closes.
            let peering = tokio::spawn(async move {
                peer.until(b"\r\n\r\n").await;
                let mut rest = Vec::new();
                peer.0.read_to_end(&mut rest).await.unwrap();
                rest
            });
            let failed = exchange
                .send(
                    &Method::POST,
                    &"/x".parse().unwrap(),
                    &headers(&[("host", "up.test")]),
                    &[],
                    sending,
                    FailingLater::new(),
                    &H1Limits::default(),
                )
                .await
                .unwrap_err();
            assert!(matches!(failed, ExchangeError::RequestBody(_)), "{failed}");
            // What the client gave, and nothing that would end it.
            assert_eq!(peering.await.unwrap(), expected, "{sending:?}");
        }
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

    /// A body of `left` frames of a hundred bytes each, and then its end.
    #[derive(Debug)]
    struct Several {
        left: usize,
    }

    impl Body for Several {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            if self.left == 0 {
                return Poll::Ready(None);
            }
            self.left -= 1;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(&[b'x'; 100])))))
        }
    }

    /// One push writes at most `ROUNDS` frames, each in a batch of its own, and the batch
    /// after them may still see the body end and write what ends it — but may not start
    /// another frame. Otherwise a bodyless answer arriving just as the budget ran out could
    /// hide that the request had finished, and cost the connection for nothing.
    #[tokio::test]
    async fn the_last_write_batch_observes_eof_without_starting_another_payload() {
        for chunked in [false, true] {
            for extra in [0, 1] {
                let frames = ROUNDS + extra;
                let sending = if chunked {
                    Sending::Chunked
                } else {
                    Sending::Length(100 * frames as u64)
                };
                let (socket, _peer) = tokio::io::duplex(64 * 1024);
                let mut exchange = Exchange::new(socket, test_blocks(), Timers::new());
                let mut upload = Upload::new(Several { left: frames }, sending, Vec::new());
                exchange
                    .push(
                        &mut Context::from_waker(Waker::noop()),
                        &mut upload,
                        true,
                        &H1Limits::default(),
                    )
                    .unwrap();
                assert!(exchange.nothing_queued(), "{chunked} {extra}");
                assert_eq!(upload.finished(), extra == 0, "{chunked} {extra}");
                if extra != 0 {
                    // The frame the last batch took is held, not started.
                    assert_eq!(upload.pending.as_ref().unwrap().1, 0, "{chunked}");
                }
            }
        }
    }

    #[tokio::test]
    async fn blocked_uploads_share_the_original_frame_until_the_wire_is_complete() {
        use http_body_util::Full;
        use tokio::io::AsyncReadExt;

        for sending in [Sending::Length(100), Sending::Chunked] {
            // Split the prefix, payload and suffix at every byte boundary, including
            // the scalar fallback for transports without vectored writes.
            let (socket, mut peer) = tokio::io::duplex(1);
            let mut exchange = Exchange::new(socket, test_blocks(), Timers::new());
            let original = Bytes::from(vec![b'x'; 100]);
            let mut upload = Upload::new(Full::new(original.clone()), sending, Vec::new());
            let mut wire = Vec::new();
            let mut shared = false;
            for _ in 0..200 {
                tokio::task::yield_now().await;
                let pushed = exchange
                    .push(
                        &mut Context::from_waker(Waker::noop()),
                        &mut upload,
                        true,
                        &H1Limits::default(),
                    )
                    .unwrap();
                if !exchange.payload.is_empty() {
                    let offset = original.len() - exchange.payload.len();
                    assert_eq!(exchange.payload.as_ptr(), original[offset..].as_ptr());
                    assert!(!exchange.nothing_queued());
                    shared = true;
                }
                if pushed.wrote {
                    wire.push(peer.read_u8().await.unwrap());
                }
                if upload.finished() && exchange.nothing_queued() {
                    break;
                }
            }
            assert!(shared);
            assert!(upload.finished() && exchange.nothing_queued());
            let expected = match sending {
                Sending::Chunked => [b"64\r\n".as_slice(), &original, b"\r\n0\r\n\r\n"].concat(),
                _ => original.to_vec(),
            };
            assert_eq!(wire, expected);
        }
    }
    /// A socket that records how much each write took, which is what a write costs the
    /// kernel by: one call and one push of segments, however large.
    struct Recording {
        inner: tokio::io::DuplexStream,
        writes: Rc<RefCell<Vec<usize>>>,
    }

    impl AsyncRead for Recording {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Recording {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let written = Pin::new(&mut self.inner).poll_write(cx, buf);
            if let Poll::Ready(Ok(gone)) = written {
                self.writes.borrow_mut().push(gone);
            }
            written
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            // All of the slices in one call, as a socket's writev does; a duplex on its
            // own would take only the first.
            let whole: Vec<u8> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
            self.poll_write(cx, &whole)
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A frame the client handed over goes to an upstream that will take it in one write,
    /// not in pieces the size of the staging buffer. The payload is not staged, so the
    /// staging bound says nothing about how much of it one write may carry; every extra
    /// write is another call and another push of segments into the kernel, which is what
    /// an upload of 8 MiB spent most of its time on (23 times the writes of the engine's
    /// client, measured on Linux).
    #[tokio::test]
    async fn a_frame_goes_upstream_in_one_write() {
        use http_body_util::Full;
        const SIZE: usize = 256 * 1024;
        for sending in [Sending::Length(SIZE as u64), Sending::Chunked] {
            let (inner, _peer) = tokio::io::duplex(4 * SIZE);
            let writes = Rc::new(RefCell::new(Vec::new()));
            let socket = Recording {
                inner,
                writes: Rc::clone(&writes),
            };
            let mut exchange = Exchange::new(socket, test_blocks(), Timers::new());
            let mut upload = Upload::new(
                Full::new(Bytes::from(vec![b'x'; SIZE])),
                sending,
                Vec::new(),
            );
            exchange
                .push(
                    &mut Context::from_waker(Waker::noop()),
                    &mut upload,
                    true,
                    &H1Limits::default(),
                )
                .unwrap();
            assert!(
                upload.finished() && exchange.nothing_queued(),
                "{sending:?}"
            );
            let writes = writes.borrow();
            // The frame and its framing in one write; the chunk that ends the body may
            // follow in another.
            assert!(
                writes.iter().any(|&gone| gone >= SIZE),
                "{sending:?}: the frame went out as {} writes: {writes:?}",
                writes.len()
            );
            assert!(writes.len() <= 2, "{sending:?}: {writes:?}");
        }
    }

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
        let mut exchange = Exchange::new(socket, test_blocks(), Timers::new());
        let mut upload = Upload::new(Frames, Sending::Chunked, Vec::new());
        let mut drain = [0; 1024];
        for _ in 0..100 {
            tokio::task::yield_now().await;
            exchange
                .push(
                    &mut Context::from_waker(Waker::noop()),
                    &mut upload,
                    true,
                    &H1Limits::default(),
                )
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
        let mut exchange = Exchange::new(socket, test_blocks(), Timers::new());
        exchange.outgoing.extend_from_slice(b"0\r\n\r\n");
        let mut upload = Upload::new(
            http_body_util::Empty::<Bytes>::new(),
            Sending::Length(0),
            Vec::new(),
        );
        upload
            .writer
            .finish(&mut Vec::new(), None, &[], &H1Limits::default())
            .unwrap();
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
