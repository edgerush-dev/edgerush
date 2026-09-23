//! One client connection, served over HTTP/1 by one task
//! ([14 §2](../../../../docs/14-downstream-server.md)).
//!
//! The task owns the socket in both directions. It reads a head, hands the request core a
//! request whose body is a handle onto this connection's own input, polls the answer
//! directly rather than from a task of its own, and writes the answer with the pure writer.
//! While it waits on the core it keeps both directions moving: it reads for the body when
//! the body asks, and writes whatever is queued. Nothing here spawns, and nothing here is
//! shared with another connection.
//!
//! Requests are served one after another. Bytes of the next request that arrive with this
//! one stay in the input, bounded, and are read as the next head once this answer is done;
//! nothing of them is looked at before then. Which deadline runs is the pure
//! [`Deadlines`]'s to say, and one timer follows its answer.
//!
//! An `Expect: 100-continue` is met as the engine's server meets it: with a `100` the
//! first time the body is asked for and nothing of it has arrived. The coordinator that
//! also relays an upstream's interim heads replaces this in a later step (14 §5, §9).

use super::codec::{Head, HeadReader, RequestError, RequestHead, arrival};
use super::date::HttpDate;
use super::deadlines::{Bounds, Clock, Deadlines};
use super::writer::{Asked, BodyFramer, Content, Delimited, write_head};
use crate::h1::{BodyReader, CodecError, Framing, Piece};
use crate::request_body::{RequestBody, RequestBodyError};
use crate::upstream::h1::H1Limits;
use bytes::{Buf, Bytes, BytesMut};
use http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use http_body::{Body, Frame, SizeHint};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::time::{Instant, Sleep};

/// How much is read from the socket at a time.
const READ: usize = 4096;

/// How much of an answer is queued for the socket before its body is asked for more: the
/// body staging of 14 §8.
const STAGING: usize = 16 * 1024;

/// What a connection is held to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    /// The bounds on what a client sends, shared with the upstream side.
    pub limits: H1Limits,
    /// The connection's deadlines.
    pub bounds: Bounds,
}

/// How serving a connection ended, for the caller and for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ended {
    /// The client closed between requests.
    Closed,
    /// An answer was written that the connection does not outlive.
    Answered,
    /// A request was refused with this status before it reached the core.
    Refused(StatusCode),
    /// This deadline ran out.
    TimedOut(Clock),
    /// The client went, or the socket failed, before an answer was done.
    Gone,
    /// The answer's body failed after its head had gone, and the client was left with a
    /// message it can tell is unfinished.
    Cut,
}

/// What the connection has read and not yet handed on, shared with the body of the
/// request being served.
#[derive(Debug)]
struct Inbound {
    input: BytesMut,
    /// The client has closed its sending half.
    ended: bool,
    /// Reading the socket failed.
    failed: bool,
    /// The body of the request being served, until its end. `None` once it has ended, or
    /// for a request without one.
    reader: Option<BodyReader>,
    /// The body asked for more than is here.
    wanted: bool,
    /// Whoever polled the body, to be woken when more arrives. Usually this task; an
    /// engine client's connection task when that client carries the upload.
    waker: Option<Waker>,
    /// This connection's own task, which does the reading and writing the body asks for.
    /// Woken by a body polled from another task, which would otherwise ask and never be
    /// heard.
    driver: Option<Waker>,
    /// A `100 Continue` is owed the moment the body is asked for with nothing of it here.
    continue_owed: bool,
    /// ...and it has been asked for: the connection writes it.
    continue_due: bool,
    limits: H1Limits,
}

impl Inbound {
    /// Notes the connection's own task, for a body polled elsewhere to wake.
    fn heard_by(&mut self, context: &Context<'_>) {
        if !self
            .driver
            .as_ref()
            .is_some_and(|driver| driver.will_wake(context.waker()))
        {
            self.driver = Some(context.waker().clone());
        }
    }

    fn wake(&mut self) {
        self.wanted = false;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// The body of a request this connection read, as the request core and the upstream
/// exchange see it: a handle onto the connection's input, read by the connection's own
/// body reader. Worker-local, and never `Send`.
#[derive(Debug)]
pub(crate) struct IncomingBody {
    inbound: Rc<RefCell<Inbound>>,
    /// For a counted body, how much of it is still to come.
    left: Option<u64>,
    done: bool,
}

impl IncomingBody {
    fn failed(error: CodecError) -> RequestBodyError {
        let incomplete = matches!(error, CodecError::Truncated | CodecError::BodyShort);
        let cause = Box::new(error);
        if incomplete {
            RequestBodyError::Incomplete(cause)
        } else {
            RequestBodyError::Invalid(cause)
        }
    }
}

impl Body for IncomingBody {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let mut inbound = this.inbound.borrow_mut();
        let inbound = &mut *inbound;
        let Some(reader) = inbound.reader.as_mut() else {
            this.done = true;
            return Poll::Ready(None);
        };
        loop {
            match reader.read(&inbound.input, inbound.ended, &inbound.limits) {
                Ok(Piece::More) => {
                    if inbound.failed {
                        this.done = true;
                        let lost = io::Error::from(io::ErrorKind::ConnectionReset);
                        return Poll::Ready(Some(Err(RequestBodyError::Other(Box::new(lost)))));
                    }
                    if std::mem::take(&mut inbound.continue_owed) {
                        inbound.continue_due = true;
                    }
                    inbound.wanted = true;
                    inbound.waker = Some(context.waker().clone());
                    // Never this task waking itself, which would only poll it again to
                    // find the same thing.
                    if let Some(driver) = &inbound.driver
                        && !driver.will_wake(context.waker())
                    {
                        driver.wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Ok(Piece::Data { data, consumed }) => {
                    let taken = inbound.input.split_to(consumed).freeze();
                    if data.is_empty() {
                        continue;
                    }
                    // Some of it is here already: nobody is waiting to be told to send it.
                    inbound.continue_owed = false;
                    let frame = taken.slice(data);
                    if let Some(left) = &mut this.left {
                        *left = left.saturating_sub(u64::try_from(frame.len()).unwrap_or(0));
                    }
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Ok(Piece::End { trailers, consumed }) => {
                    inbound.input.advance(consumed);
                    inbound.reader = None;
                    inbound.continue_owed = false;
                    this.done = true;
                    return Poll::Ready(
                        trailers
                            .filter(|trailers| !trailers.fields.is_empty())
                            .map(|trailers| Ok(Frame::trailers(trailers.fields))),
                    );
                }
                Err(error) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(Self::failed(error))));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.inbound.borrow().reader.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        match self.left {
            Some(left) => SizeHint::with_exact(left),
            None => SizeHint::default(),
        }
    }
}

/// Why a round of pumping stopped the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    TimedOut(Clock),
    Gone,
}

/// The connection's socket, what it has queued to write, and its deadlines.
struct Connection<S> {
    socket: S,
    inbound: Rc<RefCell<Inbound>>,
    queued: VecDeque<Bytes>,
    queued_bytes: usize,
    deadlines: Deadlines,
    /// When the one timer is set for, so that it is set again only when that changes.
    armed: Option<Instant>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    fn queue(&mut self, bytes: Bytes) {
        if !bytes.is_empty() {
            self.queued_bytes += bytes.len();
            self.queued.push_back(bytes);
        }
    }

    /// Writes what is queued, as far as the socket takes it.
    fn poll_write_queued(&mut self, context: &mut Context<'_>) -> Result<bool, Stop> {
        let mut moved = false;
        while let Some(front) = self.queued.front_mut() {
            match Pin::new(&mut self.socket).poll_write(context, front) {
                Poll::Ready(Ok(0) | Err(_)) => return Err(Stop::Gone),
                Poll::Ready(Ok(written)) => {
                    front.advance(written);
                    self.queued_bytes -= written;
                    if front.is_empty() {
                        self.queued.pop_front();
                    }
                    moved = true;
                    self.deadlines.write_moved(now());
                }
                Poll::Pending => break,
            }
        }
        self.deadlines
            .write_waited_on(now(), !self.queued.is_empty());
        Ok(moved)
    }

    /// Reads from the socket into the input if `wanted` says to and there is room, and
    /// says whether anything arrived or the client closed.
    fn poll_read(&mut self, context: &mut Context<'_>, room: usize) -> bool {
        let mut inbound = self.inbound.borrow_mut();
        if inbound.ended || inbound.failed || inbound.input.len() >= room {
            return false;
        }
        inbound.input.reserve(READ);
        let before = inbound.input.len();
        let polled = {
            let reading = std::pin::pin!(self.socket.read_buf(&mut inbound.input));
            reading.poll(context)
        };
        match polled {
            Poll::Pending => false,
            Poll::Ready(Ok(0)) => {
                inbound.ended = true;
                inbound.wake();
                true
            }
            Poll::Ready(Ok(_)) => {
                debug_assert!(inbound.input.len() > before);
                drop(inbound);
                self.deadlines.bytes_arrived(now());
                self.inbound.borrow_mut().wake();
                true
            }
            Poll::Ready(Err(_)) => {
                inbound.failed = true;
                inbound.wake();
                true
            }
        }
    }

    /// Checks the one deadline that is next, setting the timer again if it has moved.
    fn poll_deadline(
        &mut self,
        context: &mut Context<'_>,
        mut timer: Pin<&mut Sleep>,
    ) -> Result<(), Stop> {
        let Some((clock, due)) = self.deadlines.next() else {
            return Ok(());
        };
        let due = Instant::from_std(due);
        if self.armed != Some(due) {
            timer.as_mut().reset(due);
            self.armed = Some(due);
        }
        match timer.poll(context) {
            Poll::Ready(()) => Err(Stop::TimedOut(clock)),
            Poll::Pending => Ok(()),
        }
    }

    /// Writes what is queued until nothing is, keeping the deadlines.
    async fn flush(&mut self, mut timer: Pin<&mut Sleep>) -> Result<(), Stop> {
        poll_fn(|context| {
            loop {
                let moved = self.poll_write_queued(context)?;
                if self.queued.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                if !moved {
                    self.poll_deadline(context, timer.as_mut())?;
                    return Poll::Pending;
                }
            }
        })
        .await
    }
}

/// The time the deadlines are told of. Tokio's, so that a test that stops the clock stops
/// these too.
fn now() -> std::time::Instant {
    Instant::now().into_std()
}

/// Whether a request asked to be told before it sends its body.
fn expects_continue(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::EXPECT)
        .iter()
        .flat_map(crate::hop_by_hop::options)
        .any(|option| option.eq_ignore_ascii_case(b"100-continue"))
}

/// Whether a request said it can take trailers.
fn takes_trailers(headers: &HeaderMap) -> bool {
    headers
        .get_all(http::header::TE)
        .iter()
        .flat_map(crate::hop_by_hop::options)
        .any(|option| option.eq_ignore_ascii_case(b"trailers"))
}

/// Serves `socket` until the connection ends, handing each request to `respond`, and says
/// how it ended. The caller closes the socket, lingering where bytes may still be arriving.
pub(crate) async fn serve<S, R, F, B>(
    socket: S,
    settings: Settings,
    date: impl Fn() -> HttpDate,
    mut respond: R,
) -> Ended
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: FnMut(Request<RequestBody>) -> F,
    F: Future<Output = Response<B>>,
    B: Body<Data = Bytes> + Unpin,
{
    let limits = settings.limits;
    let mut connection = Connection {
        socket,
        inbound: Rc::new(RefCell::new(Inbound {
            input: BytesMut::new(),
            ended: false,
            failed: false,
            reader: None,
            wanted: false,
            waker: None,
            continue_owed: false,
            continue_due: false,
            driver: None,
            limits,
        })),
        queued: VecDeque::new(),
        queued_bytes: 0,
        deadlines: Deadlines::accepted(now(), settings.bounds),
        armed: None,
    };
    let mut timer = std::pin::pin!(tokio::time::sleep_until(Instant::now()));

    loop {
        // The head, from what is already here and then from the socket.
        let mut reader = HeadReader::default();
        let read = poll_fn(|context| {
            loop {
                let found = {
                    let inbound = connection.inbound.borrow();
                    reader.read(&inbound.input, &limits)
                };
                match found {
                    Err(error) => return Poll::Ready(Err(Err(error))),
                    Ok(Head::Read { head, consumed }) => {
                        return Poll::Ready(Ok((head, consumed)));
                    }
                    Ok(Head::More) => {}
                }
                let (ended, failed, empty) = {
                    let inbound = connection.inbound.borrow();
                    (inbound.ended, inbound.failed, inbound.input.is_empty())
                };
                if ended || failed {
                    // Between requests a close is the client's to make; part way through a
                    // head it is a request that never came.
                    return Poll::Ready(Err(Ok(if empty && !failed {
                        Ended::Closed
                    } else {
                        Ended::Gone
                    })));
                }
                // The head's own bound, plus a read's worth, is all it may hold.
                if !connection.poll_read(context, limits.head + READ) {
                    if let Err(Stop::TimedOut(clock)) =
                        connection.poll_deadline(context, timer.as_mut())
                    {
                        return Poll::Ready(Err(Ok(Ended::TimedOut(clock))));
                    }
                    return Poll::Pending;
                }
            }
        })
        .await;
        let (head, consumed) = match read {
            Ok(read) => read,
            Err(Ok(ended)) => return ended,
            Err(Err(error)) => return refuse(&mut connection, timer.as_mut(), error, &date).await,
        };
        connection.deadlines.head_read();
        connection.inbound.borrow_mut().input.advance(consumed);
        let arrived = match arrival(&head) {
            Ok(arrived) => arrived,
            Err(error) => return refuse(&mut connection, timer.as_mut(), error, &date).await,
        };

        let RequestHead {
            method,
            target,
            version,
            headers,
            content_length: _,
        } = head;
        let asked = Asked {
            head: method == Method::HEAD,
            version,
            trailers: takes_trailers(&headers),
        };
        let (reader, left) = match arrived.framing {
            Framing::None | Framing::Length(0) => (None, None),
            Framing::Length(length) => (Some(BodyReader::new(arrived.framing)), Some(length)),
            framing => (Some(BodyReader::new(framing)), None),
        };
        {
            let mut inbound = connection.inbound.borrow_mut();
            inbound.continue_owed =
                reader.is_some() && version == Version::HTTP_11 && expects_continue(&headers);
            inbound.continue_due = false;
            inbound.reader = reader;
        }
        let body = IncomingBody {
            inbound: Rc::clone(&connection.inbound),
            left,
            done: false,
        };
        let mut request = Request::new(RequestBody::Ours(body));
        *request.method_mut() = method;
        *request.uri_mut() = target;
        *request.version_mut() = version;
        *request.headers_mut() = headers;

        // The answer, with both directions kept moving while it is worked out.
        let answer = {
            let mut responding = std::pin::pin!(respond(request));
            poll_fn(|context| {
                connection.inbound.borrow_mut().heard_by(context);
                loop {
                    if let Poll::Ready(response) = responding.as_mut().poll(context) {
                        return Poll::Ready(Ok(response));
                    }
                    let mut moved = false;
                    {
                        let mut inbound = connection.inbound.borrow_mut();
                        if std::mem::take(&mut inbound.continue_due) {
                            drop(inbound);
                            connection.queue(Bytes::from_static(b"HTTP/1.1 100 Continue\r\n\r\n"));
                            moved = true;
                        }
                    }
                    moved |= connection.poll_write_queued(context)?;
                    let (wanted, body_done) = {
                        let inbound = connection.inbound.borrow();
                        (inbound.wanted, inbound.reader.is_none())
                    };
                    connection
                        .deadlines
                        .body_waited_on(now(), wanted && !body_done);
                    // Read for the body when it asks; once it is whole, read on only to
                    // hear a client that goes, keeping what arrives for the next request.
                    if (wanted || body_done) && connection.poll_read(context, limits.head) {
                        moved = true;
                        connection.deadlines.body_moved(now());
                        let inbound = connection.inbound.borrow();
                        if inbound.ended && inbound.reader.is_none() {
                            // The engine's server gives up on a request whose client
                            // closes, and so does this: there is nobody to answer.
                            return Poll::Ready(Err(Stop::Gone));
                        }
                    }
                    if !moved {
                        connection.poll_deadline(context, timer.as_mut())?;
                        return Poll::Pending;
                    }
                }
            })
            .await
        };
        let response = match answer {
            Ok(response) => response,
            Err(Stop::Gone) => return Ended::Gone,
            Err(Stop::TimedOut(clock)) => return Ended::TimedOut(clock),
        };
        // A `100` not yet written is never written now: the answer says what it would have.
        connection.inbound.borrow_mut().continue_due = false;

        let (parts, mut body) = response.into_parts();
        let content = if body.is_end_stream() {
            Content::Empty
        } else {
            match body.size_hint().exact() {
                Some(length) => Content::Length(length),
                None => Content::Unknown,
            }
        };
        // The connection is kept only if the request was read to its end by the time its
        // answer began; an upload still arriving is closed, lingering, rather than read
        // as the next request.
        let persistent = arrived.persistent && {
            let inbound = connection.inbound.borrow();
            inbound.reader.is_none() && !inbound.ended && !inbound.failed
        };
        let mut head = Vec::with_capacity(512);
        let written = match write_head(
            &mut head,
            parts.status,
            &parts.headers,
            content,
            asked,
            persistent,
            &date(),
        ) {
            Ok(written) => written,
            // Only an interim status is refused, and the core returns none.
            Err(_) => return Ended::Gone,
        };
        connection.queue(Bytes::from(head));

        let mut framer = BodyFramer::new(written.delimited);
        let mut body_left = written.delimited != Delimited::Nothing;
        let sent = poll_fn(|context| {
            loop {
                connection.inbound.borrow_mut().heard_by(context);
                let mut moved = connection.poll_write_queued(context)?;
                while body_left && connection.queued_bytes < STAGING {
                    let frame = match Pin::new(&mut body).poll_frame(context) {
                        Poll::Pending => break,
                        Poll::Ready(frame) => frame,
                    };
                    moved = true;
                    let mut framing = Vec::new();
                    match frame {
                        Some(Ok(frame)) => match frame.into_data() {
                            Ok(data) => {
                                let closing = framer
                                    .data_prefix(&mut framing, data.len())
                                    .map_err(|_| Stop::Gone)?;
                                connection.queue(Bytes::from(framing));
                                connection.queue(data);
                                if closing {
                                    connection.queue(Bytes::from_static(b"\r\n"));
                                }
                            }
                            Err(frame) => {
                                let trailers = frame.into_trailers().ok();
                                framer
                                    .finish(&mut framing, trailers.as_ref())
                                    .map_err(|_| Stop::Gone)?;
                                connection.queue(Bytes::from(framing));
                                body_left = false;
                            }
                        },
                        None => {
                            framer.finish(&mut framing, None).map_err(|_| Stop::Gone)?;
                            connection.queue(Bytes::from(framing));
                            body_left = false;
                        }
                        // After the head nothing can be taken back: the client is left
                        // with a message it can tell is unfinished.
                        Some(Err(_)) => return Poll::Ready(Ok(false)),
                    }
                }
                // An upload the answer did not wait for is still read for it.
                let wanted = connection.inbound.borrow().wanted;
                if wanted && connection.poll_read(context, limits.head) {
                    moved = true;
                }
                if !body_left && connection.queued.is_empty() {
                    return Poll::Ready(Ok(true));
                }
                if !moved {
                    connection.poll_deadline(context, timer.as_mut())?;
                    return Poll::Pending;
                }
            }
        })
        .await;
        drop(body);
        match sent {
            Ok(true) => {}
            Ok(false) => return Ended::Cut,
            Err(Stop::Gone) => return Ended::Gone,
            Err(Stop::TimedOut(clock)) => return Ended::TimedOut(clock),
        }
        // A kept connection's request had ended when its answer began (`persistent`), and
        // nothing is read while the answer is written unless that request's body asks.
        if written.closes {
            return Ended::Answered;
        }
        let read_ahead = !connection.inbound.borrow().input.is_empty();
        connection.deadlines.answered(now(), read_ahead);
    }
}

/// Answers a request refused before it reached the core, and ends the connection.
async fn refuse<S: AsyncRead + AsyncWrite + Unpin>(
    connection: &mut Connection<S>,
    timer: Pin<&mut Sleep>,
    error: RequestError,
    date: &impl Fn() -> HttpDate,
) -> Ended {
    let status = error.status();
    let mut head = Vec::with_capacity(128);
    let asked = Asked {
        head: false,
        version: Version::HTTP_11,
        trailers: false,
    };
    if write_head(
        &mut head,
        status,
        &HeaderMap::new(),
        Content::Empty,
        asked,
        false,
        &date(),
    )
    .is_err()
    {
        return Ended::Gone;
    }
    connection.queue(Bytes::from(head));
    match connection.flush(timer).await {
        Ok(()) => Ended::Refused(status),
        Err(Stop::Gone) => Ended::Gone,
        Err(Stop::TimedOut(clock)) => Ended::TimedOut(clock),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use std::cell::Cell;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// An answer's body of a length nobody knows until it ends.
    struct Unknown(VecDeque<Bytes>);

    impl Body for Unknown {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Ready(
                self.get_mut()
                    .0
                    .pop_front()
                    .map(|data| Ok(Frame::data(data))),
            )
        }
    }

    /// The answers a test's core can give.
    enum Answer {
        Full(Full<Bytes>),
        Unknown(Unknown),
    }

    impl Body for Answer {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            match self.get_mut() {
                Self::Full(full) => Pin::new(full).poll_frame(context),
                Self::Unknown(unknown) => Pin::new(unknown).poll_frame(context),
            }
        }

        fn is_end_stream(&self) -> bool {
            match self {
                Self::Full(full) => full.is_end_stream(),
                Self::Unknown(_) => false,
            }
        }

        fn size_hint(&self) -> SizeHint {
            match self {
                Self::Full(full) => full.size_hint(),
                Self::Unknown(_) => SizeHint::default(),
            }
        }
    }

    fn settings() -> Settings {
        Settings {
            limits: H1Limits::default(),
            bounds: Bounds::default(),
        }
    }

    fn date() -> HttpDate {
        HttpDate::from_unix(0)
    }

    /// What the core was asked, in order.
    type Asked = Rc<RefCell<Vec<String>>>;

    /// A core that reads each request's body to its end, and answers with what it read,
    /// prefixed by the method and target it was asked.
    fn echoing(
        asked: &Asked,
    ) -> impl FnMut(Request<RequestBody>) -> Pin<Box<dyn Future<Output = Response<Answer>>>> {
        let asked = Rc::clone(asked);
        move |request| {
            let asked = Rc::clone(&asked);
            Box::pin(async move {
                let (head, body) = request.into_parts();
                let read = body.collect().await;
                let said = match read {
                    Ok(collected) => {
                        let trailers = collected
                            .trailers()
                            .map(|fields| format!(" +{}", fields.len()))
                            .unwrap_or_default();
                        let bytes = collected.to_bytes();
                        format!(
                            "{} {} {}{trailers}",
                            head.method,
                            head.uri,
                            String::from_utf8_lossy(&bytes)
                        )
                    }
                    Err(error) => format!("{} {} failed: {error}", head.method, head.uri),
                };
                asked.borrow_mut().push(said.clone());
                Response::new(Answer::Full(Full::new(Bytes::from(said))))
            })
        }
    }

    /// Serves `sent` in pieces of `split`, the client closing its side after it when
    /// `close` says so, and returns what the client received and how serving ended.
    async fn served<R, F>(sent: &[u8], split: usize, close: bool, respond: R) -> (String, Ended)
    where
        R: FnMut(Request<RequestBody>) -> F,
        F: Future<Output = Response<Answer>>,
    {
        let (mut client, server) = tokio::io::duplex(1 << 20);
        let sending = sent.to_vec();
        let writing = async move {
            for piece in sending.chunks(split.max(1)) {
                client.write_all(piece).await.unwrap();
                tokio::task::yield_now().await;
            }
            if close {
                client.shutdown().await.unwrap();
            }
            let mut received = Vec::new();
            client.read_to_end(&mut received).await.unwrap();
            String::from_utf8_lossy(&received).into_owned()
        };
        let serving = serve(server, settings(), date, respond);
        let (received, ended) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(writing, serving)
        })
        .await
        .expect("serving never finished");
        (received, ended)
    }

    const GET: &[u8] = b"GET /one HTTP/1.1\r\nhost: a\r\n\r\n";

    /// Two requests sent together are answered in turn, however they arrive, and the core
    /// is asked the second only after the first is answered.
    #[tokio::test]
    async fn pipelined_requests_are_answered_in_turn() {
        let mut sent = GET.to_vec();
        sent.extend_from_slice(b"POST /two HTTP/1.1\r\nhost: a\r\ncontent-length: 3\r\n\r\nabc");
        for split in [1, 7, sent.len()] {
            let asked = Asked::default();
            let (received, ended) = served(&sent, split, true, echoing(&asked)).await;
            assert_eq!(
                *asked.borrow(),
                ["GET /one ", "POST /two abc"],
                "in pieces of {split}"
            );
            let first = received.find("GET /one").expect(&received);
            let second = received.find("POST /two abc").expect(&received);
            assert!(first < second, "{received}");
            assert_eq!(
                received.matches("HTTP/1.1 200 OK\r\n").count(),
                2,
                "{received}"
            );
            assert_eq!(ended, Ended::Closed, "in pieces of {split}");
        }
    }

    /// A chunked body with trailers reaches the core whole, however it is split.
    #[tokio::test]
    async fn a_chunked_body_and_its_trailers_reach_the_core() {
        let sent = b"POST /up HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\nx-sum: 1\r\n\r\n";
        for split in 1..=sent.len() {
            let asked = Asked::default();
            let (_, ended) = served(sent, split, true, echoing(&asked)).await;
            assert_eq!(
                *asked.borrow(),
                ["POST /up abcde +1"],
                "in pieces of {split}"
            );
            assert_eq!(ended, Ended::Closed);
        }
    }

    /// A request that cannot be read is answered with its status, which the client is
    /// told closes the connection, and never reaches the core.
    #[tokio::test]
    async fn a_request_refused_is_answered_and_closed() {
        let long_line = format!("GET /{} HTTP/1.1\r\nhost: a\r\n\r\n", "a".repeat(9 * 1024));
        let cases: [(&[u8], u16); 6] = [
            (b"GET / HTTP/1.1\nhost: a\n\n", 400),
            (b"GET / HTTP/1.2\r\nhost: a\r\n\r\n", 505),
            (long_line.as_bytes(), 414),
            (b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 1\r\ntransfer-encoding: chunked\r\n\r\n", 400),
            (b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: gzip, chunked\r\n\r\n", 501),
            (b"POST / HTTP/1.0\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\n", 400),
        ];
        for (sent, status) in cases {
            let asked = Asked::default();
            let (received, ended) = served(sent, sent.len(), false, echoing(&asked)).await;
            assert!(
                received.starts_with(&format!("HTTP/1.1 {status} ")),
                "{status}: {received}"
            );
            assert!(received.contains("connection: close\r\n"), "{received}");
            assert_eq!(ended, Ended::Refused(StatusCode::from_u16(status).unwrap()));
            assert!(asked.borrow().is_empty());
        }
    }

    /// A head the client stops part way through, then closes, is not a request.
    #[tokio::test]
    async fn a_close_part_way_through_a_head_is_nothing_answered() {
        let asked = Asked::default();
        let (received, ended) = served(b"GET / HT", 8, true, echoing(&asked)).await;
        assert_eq!(received, "");
        assert_eq!(ended, Ended::Gone);
        assert!(asked.borrow().is_empty());
    }

    /// HTTP/1.0 keeps a connection only when it asks to, and is told so; an answer of a
    /// length nobody knows goes to it delimited by the close, as it cannot read chunks.
    #[tokio::test]
    async fn http10_keeps_a_connection_only_when_asked_and_reads_no_chunks() {
        let asked = Asked::default();
        let (received, ended) =
            served(b"GET /a HTTP/1.0\r\n\r\n", 64, false, echoing(&asked)).await;
        assert!(!received.contains("connection:"), "{received}");
        assert_eq!(ended, Ended::Answered);

        let sent = b"GET /a HTTP/1.0\r\nconnection: keep-alive\r\n\r\nGET /b HTTP/1.0\r\n\r\n";
        let (received, ended) = served(sent, 64, false, echoing(&asked)).await;
        assert!(
            received.contains("connection: keep-alive\r\n"),
            "{received}"
        );
        assert!(received.contains("GET /b"), "{received}");
        assert_eq!(ended, Ended::Answered);

        let unknown = |_: Request<RequestBody>| async {
            let parts = VecDeque::from([Bytes::from_static(b"some"), Bytes::from_static(b"thing")]);
            Response::new(Answer::Unknown(Unknown(parts)))
        };
        let sent = b"GET /a HTTP/1.0\r\nconnection: keep-alive\r\n\r\n";
        let (received, ended) = served(sent, 64, false, unknown).await;
        assert!(!received.contains("transfer-encoding"), "{received}");
        assert!(received.ends_with("\r\n\r\nsomething"), "{received}");
        assert_eq!(ended, Ended::Answered);
    }

    /// An answer to HEAD says what the body would have been and sends none of it, even
    /// where the core hands it one, and the connection goes on to the next request.
    #[tokio::test]
    async fn an_answer_to_head_sends_no_body() {
        let answering = |_: Request<RequestBody>| async {
            let mut response = Response::new(Answer::Full(Full::new(Bytes::from_static(b"hello"))));
            response.headers_mut().insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_static("5"),
            );
            response
        };
        let sent = b"HEAD / HTTP/1.1\r\nhost: a\r\n\r\nGET / HTTP/1.1\r\nhost: a\r\n\r\n";
        let (received, ended) = served(sent, sent.len(), true, answering).await;
        let answers: Vec<&str> = received.split_inclusive("\r\n\r\n").collect();
        assert_eq!(answers.len(), 3, "{received}");
        assert!(answers[0].contains("content-length: 5\r\n"), "{received}");
        assert!(answers[1].starts_with("HTTP/1.1 200 OK\r\n"), "{received}");
        assert_eq!(answers[2], "hello", "{received}");
        assert_eq!(ended, Ended::Closed);
    }

    /// An answer that does not wait for the upload closes the connection once it is
    /// written: what is still arriving is not read as the next request.
    #[tokio::test]
    async fn an_answer_before_the_upload_is_read_closes_the_connection() {
        let refusing = |_: Request<RequestBody>| async {
            let mut response = Response::new(Answer::Full(Full::new(Bytes::from_static(b"no"))));
            *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
            response
        };
        let sent = b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 100\r\n\r\nGET /smuggled HTTP/1.1\r\n\r\n";
        let (received, ended) = served(sent, sent.len(), false, refusing).await;
        assert!(received.starts_with("HTTP/1.1 413 "), "{received}");
        assert!(received.contains("connection: close\r\n"), "{received}");
        assert_eq!(received.matches("HTTP/1.1").count(), 1, "{received}");
        assert_eq!(ended, Ended::Answered);
    }

    /// A `100` is sent once, when the body is first asked for and nothing of it has come,
    /// and never to HTTP/1.0 or when the body is already here. The clock is stopped, so a
    /// client's wait costs no time.
    #[tokio::test(start_paused = true)]
    async fn a_continue_is_sent_only_when_the_body_is_waited_for() {
        let head =
            b"POST / HTTP/1.1\r\nhost: a\r\nexpect: 100-continue\r\ncontent-length: 3\r\n\r\n";
        // The client waits for the 100 before sending its body.
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let asked = Asked::default();
        let serving = serve(server, settings(), date, echoing(&asked));
        let talking = async move {
            client.write_all(head).await.unwrap();
            let mut seen = Vec::new();
            while !seen.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                client.read_exact(&mut byte).await.unwrap();
                seen.push(byte[0]);
            }
            assert_eq!(seen, b"HTTP/1.1 100 Continue\r\n\r\n");
            client.write_all(b"abc").await.unwrap();
            client.shutdown().await.unwrap();
            let mut rest = Vec::new();
            client.read_to_end(&mut rest).await.unwrap();
            String::from_utf8(rest).unwrap()
        };
        let (_, rest) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(serving, talking)
        })
        .await
        .unwrap();
        assert!(rest.starts_with("HTTP/1.1 200 OK"), "{rest}");
        assert!(!rest.contains("100 Continue"), "{rest}");

        let mut sent = head.to_vec();
        sent.extend_from_slice(b"abc");
        let (received, _) = served(&sent, sent.len(), true, echoing(&asked)).await;
        assert!(
            !received.contains("100 Continue"),
            "sent with the body here: {received}"
        );

        // An HTTP/1.0 client that waits hears nothing until it gives up waiting and sends.
        let old = b"POST / HTTP/1.0\r\nexpect: 100-continue\r\ncontent-length: 3\r\n\r\n";
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let serving = serve(server, settings(), date, echoing(&asked));
        let talking = async move {
            client.write_all(old).await.unwrap();
            let mut early = [0; 64];
            let heard = tokio::time::timeout(Duration::from_secs(1), client.read(&mut early)).await;
            if let Ok(read) = heard {
                let read = read.unwrap();
                panic!(
                    "to HTTP/1.0, before its body: {:?}",
                    String::from_utf8_lossy(&early[..read])
                );
            }
            client.write_all(b"abc").await.unwrap();
            client.shutdown().await.unwrap();
            let mut rest = Vec::new();
            client.read_to_end(&mut rest).await.unwrap();
            String::from_utf8(rest).unwrap()
        };
        let (_, rest) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(serving, talking)
        })
        .await
        .unwrap();
        assert!(rest.contains(" 200 OK\r\n"), "{rest}");
        assert!(rest.ends_with("POST / abc"), "{rest}");
    }

    /// Each deadline closes the connection at its time: the first head, the wait between
    /// requests, and a body that stops arriving. The clock is stopped, so the times are
    /// exact.
    #[tokio::test(start_paused = true)]
    async fn each_deadline_ends_the_connection_at_its_time() {
        let bounds = Bounds::default();
        let cases: [(&[u8], Clock, Duration); 4] = [
            (b"", Clock::FirstRequest, bounds.first_request),
            (b"GET / HT", Clock::FirstRequest, bounds.first_request),
            (GET, Clock::KeepAlive, bounds.keep_alive),
            (
                b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 9\r\n\r\nabc",
                Clock::BodyIdle,
                bounds.idle,
            ),
        ];
        for (sent, clock, after) in cases {
            let (mut client, server) = tokio::io::duplex(1 << 16);
            client.write_all(sent).await.unwrap();
            let asked = Asked::default();
            let started = Instant::now();
            // Bounded, so that a deadline never set fails here rather than waiting forever.
            let ended = tokio::time::timeout(
                after + Duration::from_secs(1),
                serve(server, settings(), date, echoing(&asked)),
            )
            .await
            .unwrap_or_else(|_| panic!("{clock:?} never ran out"));
            assert_eq!(
                ended,
                Ended::TimedOut(clock),
                "{:?}",
                String::from_utf8_lossy(sent)
            );
            let took = started.elapsed();
            assert!(
                took >= after && took < after + Duration::from_millis(10),
                "{clock:?} after {took:?}"
            );
            drop(client);
        }
    }

    /// A trickled head gets no more time for trickling: the first request's deadline is
    /// absolute from accept.
    #[tokio::test(start_paused = true)]
    async fn a_trickled_head_buys_no_time() {
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let asked = Asked::default();
        let trickling = async move {
            for byte in b"GET / HTTP/1.1\r\nhost: aaaaaaaa" {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if client.write_all(&[*byte]).await.is_err() {
                    break;
                }
            }
            client
        };
        let started = Instant::now();
        let (ended, _client) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(serve(server, settings(), date, echoing(&asked)), trickling)
        })
        .await
        .expect("the first request's deadline never ran out");
        assert_eq!(ended, Ended::TimedOut(Clock::FirstRequest));
        assert!(started.elapsed() < Bounds::default().first_request + Duration::from_secs(2));
    }

    /// An answer the client is not reading is taken from the core only as far as there is
    /// room to queue it: a slow client holds back the upstream, not the worker's memory.
    #[tokio::test]
    async fn an_answer_nobody_reads_is_not_taken_without_bound() {
        /// Always ready, and far longer than the staging, but not endless: a driver that
        /// takes without bound takes all of it and fails the assertion, rather than never
        /// yielding to the timeout and taking memory until the process dies.
        struct Plenty(Rc<Cell<usize>>);
        impl Body for Plenty {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
                let this = self.get_mut();
                if this.0.get() >= 1 << 20 {
                    return Poll::Ready(None);
                }
                this.0.set(this.0.get() + 1024);
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'x'; 1024])))))
            }
        }
        let handed = Rc::new(Cell::new(0));
        let counting = Rc::clone(&handed);
        let (mut client, server) = tokio::io::duplex(8 * 1024);
        client.write_all(GET).await.unwrap();
        let answering = move |_: Request<RequestBody>| {
            let counting = Rc::clone(&counting);
            async move { Response::new(Plenty(counting)) }
        };
        let serving = serve(server, settings(), date, answering);
        // Long enough for an unbounded driver to take far more than it may.
        let _still = tokio::time::timeout(Duration::from_millis(200), serving).await;
        assert!(
            handed.get() <= STAGING + 8 * 1024 + 2 * 1024,
            "{} bytes taken for a client reading none",
            handed.get()
        );
        drop(client);
    }

    /// A client that goes while its request is being answered takes the answer with it:
    /// the core's work is dropped, not finished for nobody.
    #[tokio::test]
    async fn a_client_that_goes_while_waiting_drops_the_work() {
        let dropped = Rc::new(Cell::new(false));
        let noticing = Rc::clone(&dropped);
        let waiting = move |_: Request<RequestBody>| {
            struct Noticed(Rc<Cell<bool>>);
            impl Drop for Noticed {
                fn drop(&mut self) {
                    self.0.set(true);
                }
            }
            let noticed = Noticed(Rc::clone(&noticing));
            async move {
                let _held = noticed;
                std::future::pending::<Response<Answer>>().await
            }
        };
        let (received, ended) = served(GET, GET.len(), true, waiting).await;
        assert_eq!(received, "");
        assert_eq!(ended, Ended::Gone);
        assert!(dropped.get());
    }
}
