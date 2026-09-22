//! Both clients, one script, and the oracles that judge each of them.
//!
//! A script ([`super::script`]) becomes an upstream; the same request goes out by
//! EdgeRush's own path and by the engine's client; and what each of them came to is held
//! against the two oracles — the reference framer for what the message means
//! ([`super::reference`]) and the lifecycle model for whether the connection survived it
//! ([`super::lifecycle`]). Neither client is the other's oracle: they are compared with
//! the specification, and with each other only where both are inside the subset both
//! undertake to support ([13 §8](../../../../docs/13-http1-upstream.md)).
//!
//! **What is compared is meaning, not serialisation.** Field names arrive lowered and the
//! fields are compared as a bag, because a header map does not keep the order fields
//! arrived in and neither client promises one; repeats keep their multiplicity, because
//! two of a field is not the same message as one of it. A body is its bytes, whatever
//! reads or chunks they came in. Which complaint a refusal made is never compared: that
//! is a fact about a client and not about a message.
//!
//! **Reads are not consumption.** What the scripted socket handed over is recorded on the
//! tape, and nothing here compares it with where the reference says the message ended.
//! hyper's client reads ahead, which is its own business; whether a boundary was read
//! correctly shows in the exchange that follows, not in a byte count.

use super::blocks::{Blocks, SMALL, Sizes};
use super::exchange::{Exchange, H1Body, Kept};
use super::script::{self, Budget, Script, Scripted, Tape};
use super::{H1Limits, lifecycle, reference};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use hyper::body::{Body as HttpBody, Bytes, Frame, SizeHint};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

/// Where the request goes. Nothing here is about routing, so it never varies.
const TARGET: &str = "/a?b=1";

/// How long a body that never ends waits between saying so. Longer than any deadline in
/// [`H1Limits`], so that a client with one gives up first and a client without one meets
/// the run's budget — but a timer all the same, because a `Pending` with no wake behind
/// it is a hang rather than a wait.
const FOREVER: Duration = Duration::from_secs(3600);

/// The request to send. A small matrix, kept small on purpose — but not upload-blind:
/// whether a connection may be used again turns on whether the request all went, so a
/// harness that only ever sent bodyless requests would leave the lifecycle model with
/// nothing to be wrong about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Asking {
    /// `GET`, with no body.
    Nothing,
    /// `HEAD`, with no body: the answer has a body's head and no body.
    Head,
    /// `POST` of these bytes as one frame, with a length.
    Counted(Vec<u8>),
    /// `POST` of these frames, of unknown length, so framed in chunks.
    Chunked(Vec<Vec<u8>>),
    /// The same, with each frame arriving only after the clock has moved: the answer is
    /// read while the upload is still going.
    Slow(Vec<Vec<u8>>, Duration),
    /// Frames and then trailers.
    Trailing(Vec<Vec<u8>>, Vec<(&'static str, &'static str)>),
    /// Frames and then nothing, ever. Whatever the answer does, the request never
    /// finished.
    Endless(Vec<Vec<u8>>),
}

impl Asking {
    /// The method to send it with.
    #[must_use]
    pub fn method(&self) -> Method {
        match self {
            Self::Nothing => Method::GET,
            Self::Head => Method::HEAD,
            _ => Method::POST,
        }
    }

    /// What the answer's framing turns on.
    #[must_use]
    pub fn asked(&self) -> reference::Asked {
        match self {
            Self::Head => reference::Asked::Head,
            _ => reference::Asked::Anything,
        }
    }

    /// The fields to send. A length where there is one, so that both clients frame the
    /// request the same way for the same reason.
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("host"),
            HeaderValue::from_static("up.test"),
        );
        if let Self::Counted(bytes) = self {
            let length = bytes.len().to_string();
            if let Ok(value) = HeaderValue::from_str(&length) {
                headers.insert(HeaderName::from_static("content-length"), value);
            }
        }
        headers
    }

    /// How EdgeRush's own path is told to frame it. The engine's client works this out
    /// for itself from the body and the fields above, which is the same decision by a
    /// different route.
    fn sending(&self) -> super::codec::Sending {
        match self {
            Self::Nothing | Self::Head => super::codec::Sending::None,
            Self::Counted(bytes) => {
                super::codec::Sending::Length(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            }
            _ => super::codec::Sending::Chunked,
        }
    }

    /// A body to hand to a client. Built twice, once for each path, so that both are
    /// given the same thing rather than two pieces of code that agree.
    fn upload(&self) -> Upload {
        let frames = |frames: &Vec<Vec<u8>>| -> VecDeque<Frame<Bytes>> {
            frames
                .iter()
                .map(|frame| Frame::data(Bytes::copy_from_slice(frame)))
                .collect()
        };
        match self {
            Self::Nothing | Self::Head => Upload::new(VecDeque::new(), None, false, Some(0)),
            Self::Counted(bytes) => {
                let mut one = VecDeque::new();
                if !bytes.is_empty() {
                    one.push_back(Frame::data(Bytes::copy_from_slice(bytes)));
                }
                Upload::new(one, None, false, u64::try_from(bytes.len()).ok())
            }
            Self::Chunked(parts) => Upload::new(frames(parts), None, false, None),
            Self::Slow(parts, delay) => Upload::new(frames(parts), Some(*delay), false, None),
            Self::Trailing(parts, trailers) => {
                let mut queue = frames(parts);
                let mut fields = HeaderMap::new();
                for (name, value) in trailers {
                    if let (Ok(name), Ok(value)) = (
                        HeaderName::from_bytes(name.as_bytes()),
                        HeaderValue::from_str(value),
                    ) {
                        fields.append(name, value);
                    }
                }
                queue.push_back(Frame::trailers(fields));
                Upload::new(queue, None, false, None)
            }
            Self::Endless(parts) => Upload::new(frames(parts), None, true, None),
        }
    }

    /// Whether every byte of this request reached the socket, read off the tape rather
    /// than asked of the client: what the client believes about its own upload is the
    /// thing being checked.
    ///
    /// A request has all gone when the framing it chose has been terminated — the last
    /// byte of a counted body, or the zero chunk and the empty line after a chunked one.
    fn all_went(&self, written: &[u8]) -> bool {
        let head_ended = |written: &[u8]| written.ends_with(b"\r\n\r\n");
        match self {
            Self::Nothing | Self::Head => head_ended(written),
            Self::Counted(bytes) if bytes.is_empty() => head_ended(written),
            Self::Counted(bytes) => written.ends_with(bytes),
            Self::Chunked(frames) | Self::Slow(frames, _) | Self::Trailing(frames, _) => {
                // A body with no bytes in it at all is one a client may frame as
                // absent, which the engine's client does and ours does not: with
                // nothing to send, the head having gone is the whole request going.
                if frames.iter().all(Vec::is_empty) {
                    return head_ended(written);
                }
                // Otherwise the zero chunk, and then the end of the trailer section,
                // which is the same empty line whether trailers were sent or not.
                written.windows(5).any(|window| window == b"\r\n0\r\n") && head_ended(written)
            }
            Self::Endless(_) => false,
        }
    }
}

/// A request body that gives its frames on the harness's terms.
struct Upload {
    frames: VecDeque<Frame<Bytes>>,
    /// How long to wait before each frame.
    delay: Option<Duration>,
    /// The wait being served.
    sleep: Option<Pin<Box<Sleep>>>,
    /// Never ends: once the frames are gone it waits, and goes on waiting.
    endless: bool,
    length: Option<u64>,
}

impl Upload {
    fn new(
        frames: VecDeque<Frame<Bytes>>,
        delay: Option<Duration>,
        endless: bool,
        length: Option<u64>,
    ) -> Self {
        Self {
            frames,
            delay,
            sleep: None,
            endless,
            length,
        }
    }
}

impl HttpBody for Upload {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        if !this.frames.is_empty() {
            if let Some(delay) = this.delay {
                let sleep = this
                    .sleep
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.sleep = None;
            }
            return Poll::Ready(this.frames.pop_front().map(Ok));
        }
        if !this.endless {
            return Poll::Ready(None);
        }
        // A body that never ends still has to be woken by something, or the run would be
        // a hang rather than a wait that a deadline or a budget ends.
        let sleep = this
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(FOREVER)));
        let _waiting = sleep.as_mut().poll(cx);
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_empty() && !self.endless
    }

    fn size_hint(&self) -> SizeHint {
        match self.length {
            Some(length) => SizeHint::with_exact(length),
            None => SizeHint::default(),
        }
    }
}

/// What a client came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Got {
    /// An answer, read through to its end.
    Answer(Seen),
    /// The client would not carry the exchange through. The text is for a report and is
    /// never compared: which complaint a refusal makes is a fact about a client, not
    /// about a message.
    Refused(String),
    /// The run's budget ended it, which is the harness saying so rather than hanging.
    Spent(script::Spent),
    /// The script said the client goes away.
    Cancelled,
}

/// An answer as a client presented it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    /// Its status.
    pub status: u16,
    /// Its fields, names lowered. Compared as a bag, sorted by [`Seen::bag`].
    pub fields: Vec<(String, String)>,
    /// Every data frame's bytes, in order and run together.
    pub body: Vec<u8>,
    /// Its trailers, as this client presented them.
    pub trailers: Vec<(String, String)>,
    /// Whether this client kept the connection for another exchange.
    pub kept: bool,
}

impl Seen {
    /// The fields in a fixed order, for comparing without claiming that either client
    /// keeps the order they arrived in.
    #[must_use]
    pub fn bag(fields: &[(String, String)]) -> Vec<(String, String)> {
        let mut bag = fields.to_vec();
        bag.sort();
        bag
    }
}

/// What the oracles say about a script, a request, and what the tape recorded.
#[derive(Debug, Clone)]
pub struct Expected {
    /// What the message means, and where it ends.
    pub reading: reference::Reading,
    /// Whether the connection should have survived it.
    pub reuse: Result<(), lifecycle::Refused>,
    /// The facts the reuse verdict was reached from.
    pub trace: lifecycle::Trace,
}

/// How a path's result stands against the oracles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The path did what the oracles say the message means.
    Agrees,
    /// It did not, and this is where.
    Disagrees(Vec<String>),
    /// The message is outside the subset both clients undertake to support, or outside
    /// this project's own bounds. All that is required there is a classified outcome,
    /// and there is one; the reasons are named so that a report can say which.
    Outside(Vec<String>),
}

/// One path's run: what it came to, what the socket recorded, what the oracles say, and
/// how the two stand together.
#[derive(Debug)]
pub struct Checked {
    /// What the client came to.
    pub got: Got,
    /// What the scripted socket recorded.
    pub tape: Tape,
    /// What the oracles say.
    pub expected: Expected,
    /// How the two stand together.
    pub verdict: Verdict,
}

/// Which client to drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Path {
    /// EdgeRush's own.
    Ours,
    /// The engine's, through `hyper::client::conn::http1` and not the pooled client: a
    /// pool would answer a different question, about which connection a request may use.
    Theirs,
}

/// Drives `path` against `script` with `ask`, and holds what it did against the oracles.
#[must_use]
pub fn check(
    path: Path,
    script: &Script,
    ask: &Asking,
    budget: Budget,
    limits: H1Limits,
) -> Checked {
    let (got, _again, tape) = drive(path, script, ask, budget, limits, false);
    let expected = expected(script, ask, &tape, &got);
    // Whether the request ever reached the socket at all. Nothing that arrived on a
    // connection the request never went out on is an answer to anything.
    let started = !tape.written.is_empty();
    let verdict = judge(path, &got, &expected, &limits, started);
    Checked {
        got,
        tape,
        expected,
        verdict,
    }
}

/// What the oracles make of the script, the request, what the socket recorded, and how
/// the run ended.
///
/// `got` is consulted for one fact only: whether the exchange was cancelled. That is the
/// run's own doing rather than a client's opinion — a `Cancel` step the script reached
/// after the exchange had already finished cancelled nothing at all, and an oracle that
/// read it off the steps would blame a connection kept by an exchange that went through.
#[must_use]
pub fn expected(script: &Script, ask: &Asking, tape: &Tape, got: &Got) -> Expected {
    // What the upstream said over the steps the run reached, which is not the whole
    // script: a `Say` behind a wait that never released never reached the wire, and an
    // oracle handed those bytes would hold a client to a message nobody sent.
    let spoken = script.within(tape.reached);
    // Only a clean close ends a body the close delimits. A connection that failed
    // did not end such a message, it cut one off, and a client that presented it as
    // whole would be handing on a truncation.
    let closed = spoken.ended && !spoken.failed;
    let reading = reference::read(&spoken.bytes, ask.asked(), closed);
    let answer = match &reading {
        reference::Reading::Read(answer) => Some(answer),
        _ => None,
    };
    let trace = lifecycle::Trace {
        request_finished: ask.all_went(&tape.written),
        // The model keeps this apart from an unfinished request; the harness cannot tell
        // the two apart from outside, and reports the unfinished request either way.
        upload_abandoned: false,
        response_finished: answer.is_some(),
        persistent: answer.is_some_and(|answer| answer.persistent),
        close_delimited: answer.is_some_and(|answer| answer.framing == reference::Framing::ToClose),
        tunnel: answer.is_some_and(|answer| answer.notable.contains(&reference::Notable::Upgrade)),
        // Nothing here ever asks for the connection to close.
        asked_to_close: false,
        failed: spoken.failed || answer.is_none(),
        cancelled: matches!(got, Got::Cancelled),
        surplus: answer.is_some_and(|answer| answer.boundary < spoken.bytes.len()),
        peer_closed: spoken.ended,
    };
    Expected {
        reading,
        reuse: lifecycle::reusable(&trace),
        trace,
    }
}

/// Where the answer goes past a bound that is this project's own rather than the
/// specification's, named so that a report can say which.
fn past_bounds(measured: &reference::Measured, limits: &H1Limits) -> Vec<String> {
    let mut past = Vec::new();
    let mut over = |what: &str, seen: usize, bound: usize| {
        if seen > bound {
            past.push(format!("{what}: {seen} past the bound of {bound}"));
        }
    };
    over("the head", measured.head, limits.head);
    over("its fields", measured.fields, limits.fields);
    over(
        "interim heads",
        measured.interim_heads,
        limits.interim_heads,
    );
    over(
        "interim bytes",
        measured.interim_bytes,
        limits.interim_bytes,
    );
    // Two for the terminator, which this bound may or may not count: a message within
    // two bytes of it is one nothing here will insist either way about.
    over(
        "a chunk-size line",
        measured.chunk_line + 2,
        limits.chunk_line,
    );
    over("the trailer section", measured.trailers, limits.trailers);
    over("its fields", measured.trailer_fields, limits.trailer_fields);
    past
}

/// Holds what a client did against what the oracles say.
///
/// Three questions, and they are not the same question. Whether the bytes are a message
/// at all is the specification's. Whether this path was entitled to refuse one is partly
/// the specification's — where it names a choice — and partly this project's, where a
/// bound of its own says no; a bound belongs to the path that set it, so hyper's client
/// is never held to one. Whether the connection survived is the lifecycle model's, and it
/// is asked of every answer a path presents, inside the shared subset or not: reading a
/// message nobody has to accept is no licence to keep a connection that cannot carry
/// another exchange.
fn judge(path: Path, got: &Got, expected: &Expected, limits: &H1Limits, started: bool) -> Verdict {
    let answer = match &expected.reading {
        reference::Reading::Read(answer) => answer,
        // Not a message. Nothing may present one; a refusal is the only thing that is
        // not a disagreement, and a budget or a cancellation is the run's own end.
        _ => {
            return match got {
                Got::Answer(seen) => Verdict::Disagrees(vec![format!(
                    "presented an answer of {} where the bytes are not a message",
                    seen.status
                )]),
                Got::Refused(_) => Verdict::Agrees,
                Got::Spent(spent) => {
                    Verdict::Outside(vec![format!("the run's budget ran out: {spent:?}")])
                }
                Got::Cancelled => {
                    Verdict::Outside(vec!["the script took the client away".to_owned()])
                }
            };
        }
    };

    // Bounds are the path's own, so only the path that set them answers for them.
    let bounds = match path {
        Path::Ours => past_bounds(&answer.measured, limits),
        Path::Theirs => Vec::new(),
    };
    // Choices the specification names and leaves open belong to neither path.
    let choices: Vec<String> = answer
        .notable
        .iter()
        .map(|notable| format!("{notable:?}"))
        .collect();

    let seen = match got {
        Got::Answer(seen) => seen,
        Got::Refused(why) => {
            return if !started {
                // No byte of the request went out, so there is no exchange for these
                // bytes to be an answer to: a refusal is the only outcome there could be,
                // whatever they would otherwise have been read as.
                Verdict::Outside(vec!["the request never went out".to_owned()])
            } else if bounds.is_empty() && choices.is_empty() {
                Verdict::Disagrees(vec![format!(
                    "refused a message the specification reads: {why}"
                )])
            } else {
                // Entitled to refuse it: a bound of its own, or a choice the
                // specification left to it.
                Verdict::Outside([bounds, choices].concat())
            };
        }
        // A budget or a cancellation is the run ending. There is nothing to compare,
        // which is not agreement and is not a fault either.
        Got::Spent(spent) => {
            return Verdict::Outside(vec![format!("the run's budget ran out: {spent:?}")]);
        }
        Got::Cancelled => {
            return Verdict::Outside(vec!["the script took the client away".to_owned()]);
        }
    };

    let mut faults = Vec::new();
    // A bound is a promise not to read past it.
    for past in &bounds {
        faults.push(format!("read past a bound of its own — {past}"));
    }
    // An HTTP/1.0 connection is one 13 §4 has ours never pool, whatever it says about
    // keeping alive. That is a policy of the path's, like a bound: ours answers for it,
    // and giving the connection up is keeping to it rather than a fault.
    let never_pooled = path == Path::Ours && answer.version == reference::Version::Ten;
    match expected.reuse {
        Ok(()) if !seen.kept && !never_pooled => faults.push(format!(
            "gave up a connection the trace says survived: {:?}",
            expected.trace
        )),
        Err(refused) if seen.kept => {
            faults.push(format!("kept a connection that {refused:?}"));
        }
        _ => {}
    }
    // What the message means is only compared where both clients undertake to read it.
    if bounds.is_empty() && choices.is_empty() {
        if seen.status != answer.status {
            faults.push(format!(
                "the status is {} and the message says {}",
                seen.status, answer.status
            ));
        }
        if seen.body != answer.body {
            faults.push(format!(
                "the body is {} bytes and the message says {}",
                seen.body.len(),
                answer.body.len()
            ));
        }
        let wanted = Seen::bag(&answer.fields);
        let got_fields = Seen::bag(&seen.fields);
        if got_fields != wanted {
            faults.push(format!("the fields are {got_fields:?} and not {wanted:?}"));
        }
    }

    if !faults.is_empty() {
        Verdict::Disagrees(faults)
    } else if !choices.is_empty() {
        Verdict::Outside(choices)
    } else {
        Verdict::Agrees
    }
}

/// Two exchanges on one connection, where the first kept it.
///
/// The only place a boundary that was read wrongly shows. A client that took a byte too
/// few leaves it sitting in front of the next answer; one that took a byte too many has
/// eaten the next answer's first byte. Neither shows in the first answer, and neither can
/// be found by counting what a client read, because reading ahead of a boundary is that
/// client's own business ([13 §8](../../../../docs/13-http1-upstream.md)).
#[derive(Debug)]
pub struct Twice {
    /// What the first exchange came to.
    pub first: Got,
    /// What a second exchange on the connection came to, where the first kept it. `None`
    /// where it did not, which is no fault by itself: whether it should have is what
    /// [`Expected::reuse`] answers.
    pub second: Option<Got>,
    /// What the socket recorded across both of them.
    pub tape: Tape,
}

/// Drives `path` twice on one connection, the second exchange only if the first left a
/// connection to carry it.
#[must_use]
pub fn twice(path: Path, script: &Script, ask: &Asking, budget: Budget, limits: H1Limits) -> Twice {
    let (first, second, tape) = drive(path, script, ask, budget, limits, true);
    Twice {
        first,
        second,
        tape,
    }
}

/// Blocks that have all been used before, and hold what an upstream might have said.
///
/// A block is lent without being cleared, so what the last exchange read is still in it,
/// out of reach only because the cursors say so. Fresh blocks would hide a cursor that is
/// out by one: what it let through would be zeros, which the parser throws away as
/// nonsense. Blocks full of a plausible answer make the same mistake an answer that is
/// wrong, which is what both oracles are there to see.
fn used_blocks(limits: &H1Limits) -> Rc<RefCell<Blocks>> {
    const STALE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\nSTALE!";
    let mut blocks = Blocks::new(Sizes::within(limits, SMALL));
    // As many of each size as one exchange could want at once, and then some.
    let mut lent: Vec<_> = (0..4).map(|_| blocks.take()).collect();
    for _ in 0..4 {
        let small = blocks.take();
        lent.push(blocks.grow(small));
    }
    for mut block in lent {
        let room = block.room();
        for (at, byte) in room.iter_mut().enumerate() {
            *byte = STALE[at % STALE.len()];
        }
        let filled = room.len();
        block.arrived(filled);
        block.consume(filled);
        blocks.give(block);
    }
    Rc::new(RefCell::new(blocks))
}

/// One exchange by EdgeRush's own path, and the connection back if it kept it.
async fn one_ours(socket: Scripted, ask: &Asking, limits: H1Limits) -> (Got, Option<Scripted>) {
    let uri: Uri = TARGET.parse().unwrap_or_default();
    let sent = Exchange::new(socket, used_blocks(&limits))
        .send(
            &ask.method(),
            &uri,
            &ask.headers(),
            &[],
            ask.sending(),
            ask.upload(),
            &limits,
        )
        .await;
    let (answer, rest) = match sent {
        Ok(pair) => pair,
        Err(error) => return (Got::Refused(error.to_string()), None),
    };
    let mut body = H1Body::new(
        rest,
        answer.delivery.framing,
        answer.delivery.persistent,
        answer.nominated,
        limits,
    );
    let mut data = Vec::new();
    let mut trailers = Vec::new();
    loop {
        match poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            None => break,
            Some(Err(error)) => return (Got::Refused(error.to_string()), None),
            Some(Ok(frame)) => match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => {
                    if let Ok(fields) = frame.into_trailers() {
                        trailers.extend(fields_of(&fields));
                    }
                }
            },
        }
    }
    let kept = body.take_if_reusable();
    let seen = Seen {
        status: answer.head.status.as_u16(),
        fields: fields_of(&answer.head.headers),
        body: data,
        trailers,
        kept: kept.is_some(),
    };
    (Got::Answer(seen), kept.map(Kept::into_socket))
}

/// EdgeRush's own path, over the scripted socket, once or twice.
fn ours(
    script: Script,
    ask: &Asking,
    budget: Budget,
    limits: H1Limits,
    twice: bool,
) -> (Got, Option<Got>, Tape) {
    let ask = ask.clone();
    let (outcome, tape) = script::run(script, budget, move |socket| async move {
        let (first, kept) = one_ours(socket, &ask, limits).await;
        let second = match kept {
            Some(socket) if twice => Some(one_ours(socket, &ask, limits).await.0),
            _ => None,
        };
        (first, second)
    });
    let (first, second) = settled(outcome);
    (first, second, tape)
}

/// One exchange by the engine's client on a sender that is already connected.
async fn one_theirs(
    sender: &mut hyper::client::conn::http1::SendRequest<Upload>,
    ask: &Asking,
) -> Got {
    let uri: Uri = TARGET.parse().unwrap_or_default();
    let mut request = http::Request::new(ask.upload());
    *request.method_mut() = ask.method();
    *request.uri_mut() = uri;
    *request.headers_mut() = ask.headers();
    let response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => return Got::Refused(error.to_string()),
    };
    let (head, mut body) = response.into_parts();
    let mut data = Vec::new();
    let mut trailers = Vec::new();
    loop {
        match poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            None => break,
            Some(Err(error)) => return Got::Refused(error.to_string()),
            Some(Ok(frame)) => match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => {
                    if let Ok(fields) = frame.into_trailers() {
                        trailers.extend(fields_of(&fields));
                    }
                }
            },
        }
    }
    // Whether this client would carry another exchange on the connection. Asked of the
    // sender rather than read out of hyper's own buffers: what it did with the bytes it
    // read ahead is its business, and the question here is only whether it would use the
    // connection again.
    let kept = sender.ready().await.is_ok();
    Got::Answer(Seen {
        status: head.status.as_u16(),
        fields: fields_of(&head.headers),
        body: data,
        trailers,
        kept,
    })
}

/// The engine's client, over the same scripted socket, once or twice.
fn theirs(script: Script, ask: &Asking, budget: Budget, twice: bool) -> (Got, Option<Got>, Tape) {
    let ask = ask.clone();
    let (outcome, tape) = script::run(script, budget, move |socket| async move {
        let io = hyper_util::rt::TokioIo::new(socket);
        let (mut sender, connection) = match hyper::client::conn::http1::handshake(io).await {
            Ok(pair) => pair,
            Err(error) => return (Got::Refused(error.to_string()), None),
        };
        // The connection is what drives the socket; without it nothing moves.
        let _driving = tokio::spawn(connection);

        let first = one_theirs(&mut sender, &ask).await;
        let kept = matches!(&first, Got::Answer(seen) if seen.kept);
        let second = match twice && kept {
            true => Some(one_theirs(&mut sender, &ask).await),
            false => None,
        };
        (first, second)
    });
    let (first, second) = settled(outcome);
    (first, second, tape)
}

/// Drives `path` once, or twice on the connection the first exchange kept.
fn drive(
    path: Path,
    script: &Script,
    ask: &Asking,
    budget: Budget,
    limits: H1Limits,
    twice: bool,
) -> (Got, Option<Got>, Tape) {
    match path {
        Path::Ours => ours(script.clone(), ask, budget, limits, twice),
        Path::Theirs => theirs(script.clone(), ask, budget, twice),
    }
}

/// The run's own ends, which are not either client's doing.
fn settled(outcome: script::Outcome<(Got, Option<Got>)>) -> (Got, Option<Got>) {
    match outcome {
        script::Outcome::Done(got) => got,
        script::Outcome::Cancelled => (Got::Cancelled, None),
        script::Outcome::Spent(spent) => (Got::Spent(spent), None),
    }
}

/// A header map as pairs, names lowered and repeats kept.
fn fields_of(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_ascii_lowercase(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use script::{Step, Wait};

    /// How much of a request an upstream waits for before it answers. Any answer at
    /// all, however well formed, is unsolicited until the request has begun to arrive,
    /// and a script that speaks first asks a different question from this one.
    const BEGUN: usize = 20;

    /// The upstream waits for the request to begin, says this, and then keeps the
    /// connection, saying nothing more: a peer that has answered and is waiting for
    /// whatever comes next.
    fn says(bytes: &str) -> Script {
        Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(bytes.as_bytes().to_vec()),
            Step::Wait(Wait::Forever),
        ])
    }

    /// The same, and then the upstream closes its end.
    fn says_and_closes(bytes: &str) -> Script {
        Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(bytes.as_bytes().to_vec()),
            Step::Close,
        ])
    }

    fn checked(path: Path, script: &Script, ask: &Asking) -> Checked {
        check(path, script, ask, Budget::default(), H1Limits::default())
    }

    fn seen(checked: &Checked) -> &Seen {
        match &checked.got {
            Got::Answer(seen) => seen,
            other => panic!("{other:?} is not an answer: {:?}", checked.verdict),
        }
    }

    use proptest::prelude::*;

    /// Small bounds, so that a generated message can reach them at all. The rest stay as
    /// they are: a bound nothing here can reach is one these properties have nothing to
    /// say about.
    fn small() -> H1Limits {
        H1Limits {
            head: 256,
            fields: 8,
            chunk_line: 32,
            trailers: 128,
            trailer_fields: 4,
            interim_heads: 2,
            interim_bytes: 256,
            ..H1Limits::default()
        }
    }

    /// What a generated answer's body is.
    #[derive(Debug, Clone)]
    enum Made {
        /// None at all, and nothing said about one.
        Nothing,
        /// This much, counted.
        Counted(Vec<u8>),
        /// These frames, in chunks, with these trailers after them.
        Chunked(Vec<Vec<u8>>, Vec<(String, String)>),
        /// This much, ending only with the connection.
        ToClose(Vec<u8>),
    }

    /// A field name this generator will make up, and a value for it.
    ///
    /// Every name that means something to the framing or to the trailer policy is left
    /// out on purpose: what those do is asserted by name elsewhere, and a generator that
    /// stumbled on one would be comparing two clients' policies rather than their
    /// reading of a message.
    fn made_field() -> impl Strategy<Value = (String, String)> {
        (
            prop::sample::select(vec!["x-a", "x-b", "x-c"]),
            prop::sample::select(vec!["1", "two", "a b", ""]),
        )
            .prop_map(|(name, value)| (name.to_owned(), value.to_owned()))
    }

    /// `closing` says whether a body that only the close delimits is one of the choices.
    /// On a connection that has to carry a second answer it is not, and it is left out
    /// of the strategy rather than filtered out of it: a choice that is always rejected
    /// is a generator that makes no progress.
    fn made_body(closing: bool) -> BoxedStrategy<Made> {
        let keeping = prop_oneof![
            Just(Made::Nothing),
            prop::collection::vec(any::<u8>(), 0..6).prop_map(Made::Counted),
            (
                prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4), 0..3),
                prop::collection::vec(made_field(), 0..2),
            )
                .prop_map(|(frames, trailers)| Made::Chunked(frames, trailers)),
        ];
        if !closing {
            return keeping.boxed();
        }
        prop_oneof![
            keeping,
            prop::collection::vec(any::<u8>(), 0..6).prop_map(Made::ToClose),
        ]
        .boxed()
    }

    /// A whole answer: its status paired with a body that status may carry, the fields it
    /// carries, and the interim answers that come before it.
    fn made_answer(
        closing: bool,
    ) -> impl Strategy<Value = (u16, Vec<(String, String)>, Made, Vec<u16>)> {
        made_body(closing).prop_flat_map(|body| {
            let statuses = match body {
                // Nothing said about a body and nothing there: only the statuses that
                // have no body have that shape. On any other status a head with no
                // framing in it is a body the close delimits, which is `ToClose`.
                Made::Nothing => vec![204u16, 304],
                _ => vec![200u16, 201, 202, 205, 404, 500],
            };
            (
                prop::sample::select(statuses),
                prop::collection::vec(made_field(), 0..3),
                Just(body),
                prop::collection::vec(prop::sample::select(vec![100u16, 103]), 0..2),
            )
        })
    }

    /// The answer as bytes on the wire.
    fn render(status: u16, fields: &[(String, String)], body: &Made, interim: &[u16]) -> Vec<u8> {
        let mut out = Vec::new();
        for code in interim {
            out.extend(format!("HTTP/1.1 {code} Interim\r\nx-i: 1\r\n\r\n").as_bytes());
        }
        out.extend(format!("HTTP/1.1 {status} Made\r\n").as_bytes());
        for (name, value) in fields {
            out.extend(format!("{name}: {value}\r\n").as_bytes());
        }
        match body {
            Made::Nothing | Made::ToClose(_) => {}
            Made::Counted(bytes) => {
                out.extend(format!("content-length: {}\r\n", bytes.len()).as_bytes());
            }
            Made::Chunked(..) => out.extend(b"transfer-encoding: chunked\r\n"),
        }
        out.extend(b"\r\n");
        match body {
            Made::Nothing => {}
            Made::Counted(bytes) | Made::ToClose(bytes) => out.extend(bytes),
            Made::Chunked(frames, trailers) => {
                for frame in frames {
                    // A chunk of nothing is the chunk that ends the body, so a frame
                    // with nothing in it is not one.
                    if frame.is_empty() {
                        continue;
                    }
                    out.extend(format!("{:x}\r\n", frame.len()).as_bytes());
                    out.extend(frame);
                    out.extend(b"\r\n");
                }
                out.extend(b"0\r\n");
                for (name, value) in trailers {
                    out.extend(format!("{name}: {value}\r\n").as_bytes());
                }
                out.extend(b"\r\n");
            }
        }
        out
    }

    /// The bytes in the pieces the generator chose, so that the same answer is delivered
    /// in different reads from one case to the next. Where a message is split is the
    /// seam a reader that keeps state between arrivals can be read two ways at.
    fn cut_into(bytes: &[u8], cuts: &[u8]) -> Vec<Vec<u8>> {
        let mut offsets: Vec<usize> = cuts
            .iter()
            .map(|cut| usize::from(*cut) * bytes.len() / 256)
            .filter(|offset| *offset > 0)
            .collect();
        offsets.sort_unstable();
        offsets.dedup();
        offsets.push(bytes.len());
        let mut pieces = Vec::new();
        let mut at = 0;
        for offset in offsets {
            pieces.push(bytes[at..offset].to_vec());
            at = offset;
        }
        pieces
    }

    /// The script that delivers `bytes`, in pieces, once the request has begun.
    fn delivering(bytes: &[u8], cuts: &[u8], close: bool) -> Script {
        let mut steps = vec![Step::Wait(Wait::Written(BEGUN))];
        for piece in cut_into(bytes, cuts) {
            steps.push(Step::Say(piece));
        }
        steps.push(if close {
            Step::Close
        } else {
            Step::Wait(Wait::Forever)
        });
        Script::new(steps)
    }

    /// A request whose body finishes, so that reuse turns on the answer and not on the
    /// upload. What an unfinished upload costs is asserted by name elsewhere.
    fn finishing_ask() -> impl Strategy<Value = Asking> {
        prop_oneof![
            Just(Asking::Nothing),
            Just(Asking::Head),
            prop::collection::vec(any::<u8>(), 0..4).prop_map(Asking::Counted),
            prop::collection::vec(prop::collection::vec(any::<u8>(), 0..3), 0..2)
                .prop_map(Asking::Chunked),
        ]
    }

    proptest::proptest! {
        // Sixty-four cases: every one of them builds a runtime and drives two clients,
        // so this is where the ordinary suite's runtime would go if it went anywhere.
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// **The strict arm.** A well-formed answer inside the subset both clients
        /// undertake to support, delivered in pieces: both paths agree with the oracles,
        /// and both read the same message out of it.
        #[test]
        fn a_shared_answer_is_read_the_same_way_by_both(
            (status, fields, body, interim) in made_answer(true),
            ask in finishing_ask(),
            cuts in prop::collection::vec(any::<u8>(), 0..3),
        ) {
            let bytes = render(status, &fields, &body, &interim);
            // A body that only the close ends needs the close to end it.
            let close = matches!(body, Made::ToClose(_));
            let script = delivering(&bytes, &cuts, close);

            let ours = check(Path::Ours, &script, &ask, Budget::default(), small());
            let theirs = check(Path::Theirs, &script, &ask, Budget::default(), small());
            prop_assert_eq!(&ours.verdict, &Verdict::Agrees, "ours: {:?}", ours);
            prop_assert_eq!(&theirs.verdict, &Verdict::Agrees, "theirs: {:?}", theirs);

            // And the same message by both, which is the arm's own claim: the oracles
            // could both be satisfied by two clients reading two different things only
            // if the oracle were the weaker of the two, and it is not.
            let (Got::Answer(one), Got::Answer(two)) = (&ours.got, &theirs.got) else {
                return Err(TestCaseError::fail(format!(
                    "a shared answer was not read by both: {:?} and {:?}",
                    ours.got, theirs.got
                )));
            };
            prop_assert_eq!(one.status, two.status);
            prop_assert_eq!(&one.body, &two.body);
            prop_assert_eq!(Seen::bag(&one.fields), Seen::bag(&two.fields));
            prop_assert_eq!(Seen::bag(&one.trailers), Seen::bag(&two.trailers));
            prop_assert_eq!(one.kept, two.kept);
        }

        /// **A boundary shows in the next exchange.** Two answers on one connection: both
        /// paths read the first, keep the connection, and get the second — which they
        /// could not if either had taken a byte too many or too few from the first.
        #[test]
        fn a_second_answer_is_found_where_the_first_one_ended(
            (status, fields, body, interim) in made_answer(false),
            (next_status, next_fields, next_body, next_interim) in made_answer(false),
            cuts in prop::collection::vec(any::<u8>(), 0..3),
        ) {
            let first = render(status, &fields, &body, &interim);
            let second = render(next_status, &next_fields, &next_body, &next_interim);
            let mut steps = vec![Step::Wait(Wait::Written(BEGUN))];
            for piece in cut_into(&first, &cuts) {
                steps.push(Step::Say(piece));
            }
            // Not before the second request has had time to go out: two answers said
            // together would be one of them arriving unasked-for.
            steps.push(Step::Wait(Wait::Time(Duration::from_secs(1))));
            steps.push(Step::Say(second.clone()));
            steps.push(Step::Wait(Wait::Forever));
            let script = Script::new(steps);

            // What the oracle reads: the first message, and the second from where the
            // first one ended. Every step of this script is reached, so what it says and
            // what it hoped to say are the same thing.
            let said = script.within(usize::MAX).bytes;
            let reference::Reading::Read(one) =
                reference::read(&said, reference::Asked::Anything, false)
            else {
                return Ok(());
            };
            prop_assume!(one.shared() && one.boundary == first.len());
            let reference::Reading::Read(two) =
                reference::read(&said[one.boundary..], reference::Asked::Anything, false)
            else {
                return Ok(());
            };
            prop_assume!(two.shared());
            // Only where the first answer leaves a connection to carry the second.
            prop_assume!(one.persistent && one.framing != reference::Framing::ToClose);

            for path in [Path::Ours, Path::Theirs] {
                let ran = twice(
                    path,
                    &script,
                    &Asking::Nothing,
                    Budget::default(),
                    small(),
                );
                let Got::Answer(got_one) = &ran.first else {
                    return Err(TestCaseError::fail(format!(
                        "{path:?} did not read the first answer: {:?}",
                        ran.first
                    )));
                };
                prop_assert_eq!(got_one.status, one.status, "{:?}", path);
                prop_assert_eq!(&got_one.body, &one.body, "{:?}", path);
                prop_assert!(got_one.kept, "{:?} would not carry a second exchange", path);
                let Some(Got::Answer(got_two)) = &ran.second else {
                    return Err(TestCaseError::fail(format!(
                        "{path:?} had no second exchange: {:?}",
                        ran.second
                    )));
                };
                prop_assert_eq!(got_two.status, two.status, "{:?}", path);
                prop_assert_eq!(&got_two.body, &two.body, "{:?}", path);
            }
        }

        /// **The hostile arm.** Any bytes at all, as a script: ours may refuse whatever
        /// it likes, and may meet a bound or a deadline, but it may never present a
        /// message the specification does not read there, never read past a bound of its
        /// own, and never keep a connection the trace says is finished.
        ///
        /// Nothing is asserted of hyper's client here beyond its not panicking: reading
        /// something the grammar does not have is a difference of the first kind, not a
        /// promise this project can make on its behalf.
        #[test]
        fn nothing_hostile_gets_a_message_past_ours(bytes: Vec<u8>) {
            let budget = Budget {
                steps: 8,
                said: 512,
                ops: 512,
                time: Duration::from_secs(120),
            };
            let script = Script::decode(&bytes, &budget);
            for ask in [Asking::Nothing, Asking::Counted(b"ab".to_vec())] {
                let ours = check(Path::Ours, &script, &ask, budget, small());
                prop_assert!(
                    !matches!(ours.verdict, Verdict::Disagrees(_)),
                    "{:?} against {:?}",
                    ours,
                    script.steps()
                );
                let theirs = check(Path::Theirs, &script, &ask, budget, small());
                // Every outcome is classified; a run that hangs never gets here at all.
                prop_assert!(
                    matches!(
                        theirs.got,
                        Got::Answer(_) | Got::Refused(_) | Got::Spent(_) | Got::Cancelled
                    ),
                    "{:?}",
                    theirs
                );
            }
        }
    }

    #[test]
    fn a_second_exchange_on_one_connection_gets_the_second_answer() {
        let first = "HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\none";
        let second = "HTTP/1.1 201 Created\r\ncontent-length: 3\r\n\r\ntwo";
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(first.as_bytes().to_vec()),
            // Not until the second request has had time to go out: two answers said
            // together would be one of them arriving unasked-for, which is the surplus
            // case and costs the connection.
            Step::Wait(Wait::Time(Duration::from_secs(1))),
            Step::Say(second.as_bytes().to_vec()),
            Step::Wait(Wait::Forever),
        ]);

        // The oracle finds the second message where the first one ended, which is what
        // the boundary is for.
        let said = script.within(usize::MAX).bytes;
        let reference::Reading::Read(one) =
            reference::read(&said, reference::Asked::Anything, false)
        else {
            panic!("the first answer is not a message");
        };
        let reference::Reading::Read(two) =
            reference::read(&said[one.boundary..], reference::Asked::Anything, false)
        else {
            panic!("the second answer is not a message");
        };
        assert_eq!((one.status, two.status), (200, 201));

        for path in [Path::Ours, Path::Theirs] {
            let ran = twice(
                path,
                &script,
                &Asking::Nothing,
                Budget::default(),
                H1Limits::default(),
            );
            let Got::Answer(got_one) = &ran.first else {
                panic!("{path:?} did not read the first answer: {:?}", ran.first);
            };
            assert_eq!(got_one.status, one.status, "{path:?}");
            assert_eq!(got_one.body, one.body, "{path:?}");
            assert!(got_one.kept, "{path:?} would not carry a second exchange");
            let Some(Got::Answer(got_two)) = &ran.second else {
                panic!("{path:?} had no second exchange: {:?}", ran.second);
            };
            // The second answer, whole and its own: a client that had taken a byte too
            // many or too few from the first would be reading something else here.
            assert_eq!(got_two.status, two.status, "{path:?}");
            assert_eq!(got_two.body, two.body, "{path:?}");
            assert!(got_two.kept, "{path:?} gave up a good connection");
        }
    }

    #[test]
    fn an_answer_behind_a_wait_that_never_released_was_never_said() {
        // The upstream waits for more of the request than a request of this shape
        // has in it, so it never speaks. What the oracle is given has to be what was
        // said and not what the script hoped to say: handed those bytes it would hold
        // ours to a message nobody sent, and then blame it for meeting the deadline
        // it was left with instead. Found by the fuzz target.
        let script = Script::new(vec![
            Step::Wait(Wait::Written(150)),
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n".to_vec()),
        ]);
        let checked = checked(Path::Ours, &script, &Asking::Head);
        assert_eq!(checked.tape.delivered, 0, "{checked:?}");
        assert_eq!(checked.expected.reading, reference::Reading::Unfinished);
        assert!(matches!(checked.got, Got::Refused(_)), "{:?}", checked.got);
        assert_eq!(checked.verdict, Verdict::Agrees, "{checked:?}");
    }

    #[test]
    fn a_body_the_close_delimits_is_cut_off_by_a_failure_rather_than_ended_by_it() {
        // No length and no coding, so only the close ends this body — and what comes
        // is not a close but a connection that failed. The bytes that arrived are all
        // there were, and there is no saying whether they were all there was going to
        // be, so nothing may present them as a whole message. Found by the fuzz
        // target, which had the oracle calling it one.
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(b"HTTP/1.1 200 OK\r\n\r\nas much as arrived".to_vec()),
            Step::Fail,
        ]);
        let cut_off = checked(Path::Ours, &script, &Asking::Nothing);
        assert_eq!(cut_off.expected.reading, reference::Reading::Unfinished);
        assert!(matches!(cut_off.got, Got::Refused(_)), "{:?}", cut_off.got);
        assert_eq!(cut_off.verdict, Verdict::Agrees, "{cut_off:?}");

        // The same bytes, ended by a close instead, are a whole message.
        let closed = says_and_closes("HTTP/1.1 200 OK\r\n\r\nas much as arrived");
        let ended = checked(Path::Ours, &closed, &Asking::Nothing);
        assert_eq!(ended.verdict, Verdict::Agrees, "{ended:?}");
        assert_eq!(seen(&ended).body, b"as much as arrived");
    }

    #[test]
    fn a_status_line_that_stops_after_the_code_is_read_by_both() {
        // RFC 9112 section 4 requires a sender to send the space before the reason
        // phrase even when the phrase is absent. A recipient is given no rule and the
        // code is not in doubt without it, so refusing it and reading it are both
        // allowed; both clients read it. Found by the fuzz target, which had the
        // oracle calling it no message at all.
        let script = says_and_closes("HTTP/1.1 200\r\n\r\n");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert!(
                matches!(checked.verdict, Verdict::Outside(_)),
                "{path:?} {checked:?}"
            );
            assert_eq!(seen(&checked).status, 200, "{path:?}");
        }
    }

    #[test]
    fn a_cancellation_the_exchange_finished_before_cancelled_nothing() {
        // The script says to take the client away, and gets there only once the
        // answer has been read — by which time the exchange is over and the
        // connection kept. A step being reached is not the same as its having had an
        // effect. Found by the fuzz target, which had the oracle blaming a connection
        // that an exchange which went through had every right to keep.
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec()),
            Step::Cancel,
        ]);
        let checked = checked(Path::Ours, &script, &Asking::Nothing);
        assert!(matches!(checked.got, Got::Answer(_)), "{:?}", checked.got);
        assert!(!checked.expected.trace.cancelled, "{checked:?}");
        assert_eq!(checked.expected.reuse, Ok(()));
        assert_eq!(checked.verdict, Verdict::Agrees, "{checked:?}");
        assert!(seen(&checked).kept);
    }

    #[test]
    fn a_connection_that_failed_before_the_request_went_out_answers_nothing() {
        // The upstream says an answer and the connection fails in the same breath,
        // before a byte of the request has gone. There is no exchange for those bytes
        // to be an answer to, so a refusal is the only outcome there could be —
        // whatever they would otherwise have been read as. Found by the fuzz target.
        let script = Script::new(vec![
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec()),
            Step::Fail,
        ]);
        let checked = checked(Path::Ours, &script, &Asking::Head);
        assert!(checked.tape.written.is_empty(), "{:?}", checked.tape);
        assert!(matches!(checked.got, Got::Refused(_)), "{:?}", checked.got);
        assert!(
            matches!(checked.verdict, Verdict::Outside(_)),
            "{checked:?}"
        );
    }

    #[test]
    fn empty_connection_lists_are_shared_by_both_paths() {
        for fields in [
            "connection:\r\n",
            "connection: \t\r\n",
            "connection: , ,\t\r\n",
            "connection: ,\r\nconnection: keep-alive,,\r\n",
            "connection: ,\r\nconnection: , CLOSE,\r\n",
        ] {
            let script = says(&format!(
                "HTTP/1.1 200 OK\r\n{fields}content-length: 2\r\n\r\nok"
            ));
            for path in [Path::Ours, Path::Theirs] {
                let checked = checked(path, &script, &Asking::Nothing);
                assert_eq!(checked.verdict, Verdict::Agrees, "{fields:?}: {checked:?}");
                assert_eq!(seen(&checked).body, b"ok", "{path:?}: {fields:?}");
            }
        }
    }

    #[test]
    fn a_connection_field_that_is_not_a_list_of_tokens_is_refused_by_one_path() {
        // RFC 9110 section 7.6.1 gives `Connection = #connection-option` with
        // `connection-option = token`, and section 5.5 gives a recipient no rule for
        // a value that fails its field's grammar. What the peer meant by this one
        // cannot be worked out, so 13 section 4 validates it and refuses; hyper's
        // client reads on. Found by the fuzz target.
        let script = says("HTTP/1.1 200 OK\r\nconnection: 00 =K\r\ncontent-length: 2\r\n\r\nok");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        assert!(matches!(ours.verdict, Verdict::Outside(_)), "{ours:?}");
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert!(matches!(theirs.verdict, Verdict::Outside(_)), "{theirs:?}");
        assert_eq!(seen(&theirs).body, b"ok");
    }

    #[test]
    fn a_status_outside_the_range_http_has_is_refused_by_ours_and_read_by_hyper() {
        // RFC 9110 section 15: "Values outside the range 100..599 are invalid.
        // Implementations often use three-digit integer values outside of that range
        // (i.e., 600..999) for internal communication of non-HTTP status (e.g.,
        // library errors). A client that receives a response with an invalid status
        // code SHOULD process the response as if it had a 5xx (Server Error) status
        // code." Ours answers 502 and lets the connection go, which is one way of
        // doing that; hyper's client reads the status as it stands, which is that
        // library tolerance the paragraph describes.
        for code in [600, 999] {
            let script = says(&format!(
                "HTTP/1.1 {code} Out Of Range\r\ncontent-length: 2\r\n\r\nok"
            ));
            let ours = checked(Path::Ours, &script, &Asking::Nothing);
            let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
            assert!(
                matches!(ours.got, Got::Refused(_)),
                "{code}: {:?}",
                ours.got
            );
            assert!(
                matches!(ours.verdict, Verdict::Outside(_)),
                "{code}: {ours:?}"
            );
            assert_eq!(seen(&theirs).status, code, "{code}");
            assert!(
                matches!(theirs.verdict, Verdict::Outside(_)),
                "{code}: {theirs:?}"
            );
        }

        // The last status HTTP has, which is nothing out of the ordinary.
        let last = says("HTTP/1.1 599 The Last\r\ncontent-length: 2\r\n\r\nok");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &last, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(seen(&checked).status, 599, "{path:?}");
        }

        // Below the range there is nothing either client will take: the engine's own
        // status type does not hold one.
        let low = says("HTTP/1.1 059 Below\r\ncontent-length: 2\r\n\r\nok");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &low, &Asking::Nothing);
            assert!(
                matches!(checked.got, Got::Refused(_)),
                "{path:?} {checked:?}"
            );
            assert!(
                matches!(checked.verdict, Verdict::Outside(_)),
                "{path:?} {checked:?}"
            );
        }
    }

    #[test]
    fn both_paths_read_a_plain_answer_the_way_the_specification_does() {
        let script = says("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            let seen = seen(&checked);
            assert_eq!((seen.status, seen.body.as_slice()), (200, &b"ok"[..]));
            // The peer is still there and the message accounted for every byte, so both
            // clients keep the connection.
            assert!(seen.kept, "{path:?} gave up a good connection");
        }
    }

    /// How long a script waits before answering, where the point is that the upstream
    /// has read the whole request first. Longer than any upload below takes to arrive,
    /// and well inside every deadline in [`H1Limits`].
    const AFTER_THE_REQUEST: Duration = Duration::from_secs(10);

    /// An upstream that reads the whole request and only then answers.
    fn answers_after_reading(bytes: &str) -> Script {
        Script::new(vec![
            Step::Wait(Wait::Time(AFTER_THE_REQUEST)),
            Step::Say(bytes.as_bytes().to_vec()),
            Step::Wait(Wait::Forever),
        ])
    }

    #[test]
    fn a_request_with_a_body_reaches_the_upstream_by_both_paths() {
        let script = answers_after_reading("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
        for ask in [
            Asking::Counted(b"hello".to_vec()),
            Asking::Chunked(vec![b"he".to_vec(), b"llo".to_vec()]),
            Asking::Slow(
                vec![b"he".to_vec(), b"llo".to_vec()],
                Duration::from_secs(2),
            ),
            Asking::Trailing(vec![b"he".to_vec(), b"llo".to_vec()], vec![("x-a", "1")]),
        ] {
            for path in [Path::Ours, Path::Theirs] {
                let checked = checked(path, &script, &ask);
                assert_eq!(
                    checked.verdict,
                    Verdict::Agrees,
                    "{path:?} {ask:?} {checked:?}"
                );
                let written = String::from_utf8_lossy(&checked.tape.written).into_owned();
                // Each frame reached the socket. Not the whole body in one piece: a
                // chunked body has framing between its frames, and where those
                // boundaries fall is not something either client promises.
                for part in ["he", "llo"] {
                    assert!(written.contains(part), "{path:?} {ask:?}: {written}");
                }
                assert!(
                    checked.expected.trace.request_finished,
                    "{path:?} {ask:?}: {written}"
                );
                // The request all went, so the connection is still good.
                assert!(seen(&checked).kept, "{path:?} {ask:?} gave up a connection");
            }
        }
    }

    #[test]
    fn a_requests_trailers_reach_the_upstream_by_one_path_and_not_the_other() {
        let script = answers_after_reading("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
        let ask = Asking::Trailing(vec![b"hi".to_vec()], vec![("x-a", "1")]);
        let ours = checked(Path::Ours, &script, &ask);
        let theirs = checked(Path::Theirs, &script, &ask);
        let ours_wire = String::from_utf8_lossy(&ours.tape.written).into_owned();
        let theirs_wire = String::from_utf8_lossy(&theirs.tape.written).into_owned();
        // One of 13 section 5's differences, in the request direction and at the
        // client layer: RFC 9110 section 6.5 lets a recipient retain or discard
        // trailers, hyper's client puts none of them on the wire, and ours forwards
        // them so that end-to-end fields survive the hop.
        assert!(ours_wire.contains("x-a: 1"), "{ours_wire}");
        assert!(!theirs_wire.contains("x-a"), "{theirs_wire}");
        // Both sent a whole request all the same, and both connections survive it.
        assert!(seen(&ours).kept && seen(&theirs).kept);
    }

    #[test]
    fn an_answer_that_finishes_first_is_finished_with_by_one_path_and_not_the_other() {
        // A bodyless answer, so the message is over at its head while frames of the
        // request are still waiting on the clock.
        let script = says("HTTP/1.1 204 No Content\r\n\r\n");
        let ask = Asking::Slow(
            vec![b"first".to_vec(), b"second".to_vec()],
            Duration::from_secs(5),
        );
        let ours = checked(Path::Ours, &script, &ask);
        let theirs = checked(Path::Theirs, &script, &ask);

        // One of the choices 13 section 5 lists as left open. Ours stops
        // uploading a body the answer has finished without, which is 13 section 5's
        // rule, and the connection goes with it. hyper's client sends the rest of the
        // request and keeps the connection. Neither is a violation: RFC 9112 section 9.5
        // asks a client to stop only where the answer says the server does not want the
        // body and is closing, which a 204 without `Connection: close` does not say.
        assert!(!ours.expected.trace.request_finished);
        assert_eq!(
            ours.expected.reuse,
            Err(lifecycle::Refused::UnfinishedRequest)
        );
        assert!(!seen(&ours).kept, "ours kept a connection mid-request");
        assert!(theirs.expected.trace.request_finished);
        assert_eq!(theirs.expected.reuse, Ok(()));
        assert!(seen(&theirs).kept, "hyper gave up a whole exchange");

        // Each path is what the oracles say it should be, given what it did: the
        // difference is in what they chose to do, not in either of them being wrong
        // about the connection afterwards.
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
        assert_eq!(theirs.verdict, Verdict::Agrees, "{theirs:?}");
    }

    #[test]
    fn an_upstream_that_speaks_before_the_request_is_refused_by_both() {
        // Every other script here waits for the request to begin. This one does not:
        // the answer is on the socket before a byte of the request has gone, and the
        // script puts it there rather than leaving it to which client polls first.
        let script = Script::new(vec![
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok".to_vec()),
            Step::Wait(Wait::Forever),
        ]);
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            // HTTP/1.1 pairs an answer with the request that was outstanding when it
            // arrived (RFC 9112 section 9.2), so these bytes answer nothing this end
            // sent, and pairing them with the request about to go out would be
            // answering a question nobody asked.
            assert!(
                matches!(checked.got, Got::Refused(_)),
                "{path:?} {checked:?}"
            );
            assert!(
                checked.tape.written.is_empty(),
                "{path:?} sent the request anyway"
            );
            assert!(
                matches!(checked.verdict, Verdict::Outside(_)),
                "{path:?} {checked:?}"
            );
        }
    }

    #[test]
    fn a_request_that_never_finishes_costs_the_connection() {
        let script = says("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
        let ask = Asking::Endless(vec![b"ab".to_vec()]);
        let ours = checked(Path::Ours, &script, &ask);
        assert_eq!(
            ours.expected.reuse,
            Err(lifecycle::Refused::UnfinishedRequest)
        );
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
        // The answer arrived while the request was still going out, which is allowed and
        // is the point; what it costs is the connection.
        assert_eq!(seen(&ours).body, b"ok");
        assert!(!seen(&ours).kept, "ours kept a connection mid-request");
    }

    #[test]
    fn a_length_with_a_coding_is_outside_what_both_clients_support() {
        let script = says(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ncontent-length: 99\r\n\r\n\
             2\r\nhi\r\n0\r\n\r\n",
        );
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        // Neither is wrong: RFC 9112 section 6.3 rule 3 says such a message ought to be
        // handled as an error, and that a forwarder which forwards it must first remove
        // the length. Ours takes the error; hyper's removes the length. Both are
        // classified outcomes and neither is compared with the other.
        assert!(
            matches!(ours.verdict, Verdict::Outside(_)),
            "{:?}",
            ours.verdict
        );
        assert!(
            matches!(theirs.verdict, Verdict::Outside(_)),
            "{:?}",
            theirs.verdict
        );
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert_eq!(seen(&theirs).body, b"hi");
    }

    #[test]
    fn bad_whitespace_in_a_chunk_extension_belongs_to_the_grammar() {
        // RFC 9112 section 7.1.1 is
        // `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`,
        // so the whitespace around the marks is part of the message. Refusing it cost a
        // connection for a message that was not wrong, which is what this found.
        let script = says(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2 ; a = \"b\"\r\nhi\r\n0\r\n\r\n",
        );
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(seen(&checked).body, b"hi");
            assert!(seen(&checked).kept, "{path:?} gave up a good connection");
        }

        // Whitespace with no mark after it is not in the grammar, and ours refuses it
        // still: what was fixed is the grammar, not a licence to skip whitespace
        // wherever it turns up.
        let trailing =
            says("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;a \r\nhi\r\n0\r\n\r\n");
        let ours = checked(Path::Ours, &trailing, &Asking::Nothing);
        assert!(matches!(
            ours.expected.reading,
            reference::Reading::Invalid(reference::Invalid::ChunkExtension)
        ));
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
    }

    #[test]
    fn an_extension_with_no_name_is_read_by_one_client_and_not_the_other() {
        let script =
            says("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;=v\r\nhi\r\n0\r\n\r\n");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        // One of 13 section 5's differences, of the one kind where a difference is a
        // fault: RFC 9112 section 7.1.1 wants a name before any `=`, so this is not an
        // extension to be ignored but bytes that do not parse. Ours refuses; hyper's
        // client does not look, and presents a message the grammar does not have.
        assert!(matches!(
            ours.expected.reading,
            reference::Reading::Invalid(reference::Invalid::ChunkExtension)
        ));
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert!(
            matches!(theirs.verdict, Verdict::Disagrees(_)),
            "{:?}",
            theirs.verdict
        );
    }

    #[test]
    fn a_peer_that_stops_talking_meets_a_deadline_on_one_path_and_the_budget_on_the_other() {
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-len".to_vec()),
            Step::Wait(Wait::Forever),
        ]);
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        // A limit of this project's own, which is the third of the four kinds: ours has
        // a deadline for reaching a final head and hyper's client has none, so one ends
        // the exchange and the other waits for as long as it is allowed to.
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        // The idle clock is what notices a peer that stops in the middle of a head: the
        // absolute head deadline is the backstop behind it, for a peer that trickles
        // rather than one that stops.
        assert_eq!(ours.tape.elapsed, H1Limits::default().idle);
        assert!(ours.tape.elapsed < H1Limits::default().final_head);
        assert_eq!(theirs.got, Got::Spent(script::Spent::Time));
        assert_eq!(theirs.tape.elapsed, Budget::default().time);
        // Neither is a fault: the bytes are not a message either way.
        assert_eq!(ours.verdict, Verdict::Agrees);
        assert!(matches!(theirs.verdict, Verdict::Outside(_)));
    }

    /// The upstream answers, waits for the next request to have gone out, and answers
    /// that too.
    fn answers_twice(first: &str, second: &str) -> Script {
        Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(first.as_bytes().to_vec()),
            Step::Wait(Wait::Time(Duration::from_secs(1))),
            Step::Say(second.as_bytes().to_vec()),
            Step::Wait(Wait::Forever),
        ])
    }

    /// A second answer with no body, so that it answers a `HEAD` as well as a `GET`.
    const NEXT: &str = "HTTP/1.1 201 Created\r\ncontent-length: 0\r\n\r\n";

    /// Both paths read a first answer, keep the connection, and read `NEXT` on it.
    fn both_carry_a_second_exchange(script: &Script, ask: &Asking) -> [Seen; 2] {
        [Path::Ours, Path::Theirs].map(|path| {
            let ran = twice(path, script, ask, Budget::default(), H1Limits::default());
            let Got::Answer(one) = ran.first else {
                panic!(
                    "{path:?} {ask:?} did not read the first answer: {:?}",
                    ran.first
                );
            };
            assert!(
                one.kept,
                "{path:?} {ask:?} would not carry a second exchange"
            );
            let Some(Got::Answer(two)) = &ran.second else {
                panic!("{path:?} {ask:?} had no second exchange: {:?}", ran.second);
            };
            assert_eq!(two.status, 201, "{path:?} {ask:?}");
            one
        })
    }

    /// A head that describes a body it does not send leaves nothing to wait for, and the
    /// connection for the next exchange. Every project surveyed tests this one.
    #[test]
    fn a_body_described_and_not_sent_leaves_the_connection_for_the_next_answer() {
        for (first, ask) in [
            (
                "HTTP/1.1 200 OK\r\ncontent-length: 26\r\n\r\n",
                Asking::Head,
            ),
            (
                "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
                Asking::Head,
            ),
            (
                "HTTP/1.1 304 Not Modified\r\ncontent-length: 100\r\n\r\n",
                Asking::Nothing,
            ),
            (
                "HTTP/1.1 304 Not Modified\r\ntransfer-encoding: chunked\r\n\r\n",
                Asking::Nothing,
            ),
            ("HTTP/1.1 304 Not Modified\r\n\r\n", Asking::Nothing),
            ("HTTP/1.1 204 No Content\r\n\r\n", Asking::Nothing),
            // After interim answers too, which Pingora once got wrong for HEAD.
            (
                "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n",
                Asking::Head,
            ),
            (
                "HTTP/1.1 103 Early Hints\r\nlink: </s.css>\r\n\r\n\
                 HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n",
                Asking::Head,
            ),
        ] {
            for one in both_carry_a_second_exchange(&answers_twice(first, NEXT), &ask) {
                assert!(one.body.is_empty(), "{first:?}: {:?}", one.body);
            }
        }
    }

    /// A body sent where none may be, or bytes after the end of one, is not delivered as
    /// anything, and the connection it arrived on is not trusted again (nginx's
    /// `proxy_extra_data.t`, HAProxy's `http_bodyless_response.vtc`, hyper's client tests).
    #[test]
    fn a_body_sent_where_none_may_be_is_not_delivered_and_costs_the_connection() {
        for (answer, ask, body) in [
            (
                "HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\nskipped data",
                Asking::Head,
                "",
            ),
            (
                "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n",
                Asking::Head,
                "",
            ),
            (
                "HTTP/1.1 304 Not Modified\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n",
                Asking::Nothing,
                "",
            ),
            (
                "HTTP/1.1 304 Not Modified\r\ncontent-length: 8\r\n\r\nSEE-THIS",
                Asking::Nothing,
                "",
            ),
            (
                "HTTP/1.1 204 No Content\r\n\r\nNOT-THIS",
                Asking::Nothing,
                "",
            ),
            (
                "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\nNOT-THIS",
                Asking::Nothing,
                "",
            ),
            // A stray CRLF after a counted body, which real servers send.
            (
                "HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello\r\n",
                Asking::Nothing,
                "hello",
            ),
        ] {
            let ours = checked(Path::Ours, &says(answer), &ask);
            assert!(
                !matches!(ours.verdict, Verdict::Disagrees(_)),
                "{answer:?} {ours:?}"
            );
            let seen = seen(&ours);
            assert_eq!(seen.body, body.as_bytes(), "{answer:?}");
            assert!(
                !seen.kept,
                "{answer:?} left a connection that was not finished"
            );
        }
    }

    /// The end of a chunked body arriving in two pieces: nothing is finished until the
    /// empty line is, and the connection is good afterwards (nginx's `proxy_keepalive.t`).
    #[test]
    fn a_last_chunk_whose_empty_line_comes_later_still_leaves_a_good_connection() {
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(
                b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
                  1a\r\nabcdefghijklmnopqrstuvwxyz\r\n0\r\n"
                    .to_vec(),
            ),
            Step::Wait(Wait::Time(Duration::from_millis(50))),
            Step::Say(b"\r\n".to_vec()),
            Step::Wait(Wait::Time(Duration::from_secs(1))),
            Step::Say(NEXT.as_bytes().to_vec()),
            Step::Wait(Wait::Forever),
        ]);
        for one in both_carry_a_second_exchange(&script, &Asking::Nothing) {
            assert_eq!(one.body, b"abcdefghijklmnopqrstuvwxyz");
        }
    }

    /// An interim answer and the final one, cut at every byte between them: a reader
    /// that starts afresh after an interim head must keep what it already has of the next
    /// (Pingora's client tests; a stall it fixed).
    #[test]
    fn an_interim_answer_and_the_final_one_cut_anywhere_are_the_same_answer() {
        let bytes = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
        for cut in 1..bytes.len() {
            let script = Script::new(vec![
                Step::Wait(Wait::Written(BEGUN)),
                Step::Say(bytes[..cut].to_vec()),
                Step::Say(bytes[cut..].to_vec()),
                Step::Wait(Wait::Forever),
            ]);
            for path in [Path::Ours, Path::Theirs] {
                let checked = checked(path, &script, &Asking::Nothing);
                assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} at {cut}");
                assert_eq!(seen(&checked).body, b"ok", "{path:?} at {cut}");
            }
        }
    }

    /// A `Connection: close` on an interim head holds for the whole exchange. RFC 9112 §9.6
    /// says a client that receives one "MUST cease sending requests on that connection",
    /// and a final head that says nothing about it does not take it back. One of 13 §5's
    /// differences.
    #[test]
    fn a_close_said_on_an_interim_answer_is_not_forgotten_by_the_final_one() {
        for interim in ["connection: close\r\n", "connection: x-a, Close\r\n"] {
            let script = says(&format!(
                "HTTP/1.1 100 Continue\r\n{interim}\r\n\
                 HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"
            ));
            let ours = checked(Path::Ours, &script, &Asking::Nothing);
            assert_eq!(ours.verdict, Verdict::Agrees, "{interim:?}: {ours:?}");
            assert_eq!(seen(&ours).body, b"ok", "{interim:?}");
            assert!(!seen(&ours).kept, "{interim:?}: ours kept it");
            // hyper's client forgets it and keeps the connection, which the oracle calls
            // the defect it is. Listed in 13 §5; this says so if hyper ever changes.
            let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
            assert_eq!(seen(&theirs).body, b"ok", "{interim:?}");
            assert!(seen(&theirs).kept, "{interim:?}: hyper let it go");
            assert!(
                matches!(theirs.verdict, Verdict::Disagrees(_)),
                "{interim:?}: {theirs:?}"
            );
        }
    }

    /// What follows an interim head is a head, and one that is not is a failure to read
    /// an answer rather than another interim one (Pingora's client tests).
    #[test]
    fn what_follows_an_interim_answer_has_to_be_an_answer() {
        let script = says("HTTP/1.1 100 Continue\r\n\r\nHTP/1.1 200 OK\r\n\r\n");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
    }

    /// The first read fills the reading buffer exactly, and the rest comes after a pause:
    /// a full buffer with nothing more in it yet is not the end of anything (nginx's
    /// `proxy_available.t`, ticket #2367).
    #[test]
    fn a_first_read_that_fills_the_buffer_exactly_is_not_the_end() {
        const FILLED: usize = 16 * 1024;
        let budget = Budget {
            said: 4 * FILLED,
            ..Budget::default()
        };
        // Delimited by the close, and counted. The count is five digits either way, so
        // the head is the same length whatever it says.
        const COUNTED: &str = "HTTP/1.1 200 OK\r\ncontent-length: 00000\r\n\r\n";
        for close in [true, false] {
            let head = if close {
                "HTTP/1.1 200 OK\r\n\r\n".to_owned()
            } else {
                let body = FILLED - COUNTED.len() + b"AND-THIS".len();
                format!("HTTP/1.1 200 OK\r\ncontent-length: {body:05}\r\n\r\n")
            };
            let filler = vec![b'f'; FILLED - head.len()];
            let mut first = head.into_bytes();
            first.extend(&filler);
            assert_eq!(first.len(), FILLED);
            let script = Script::new(vec![
                Step::Wait(Wait::Written(BEGUN)),
                Step::Say(first),
                Step::Wait(Wait::Time(Duration::from_millis(1100))),
                Step::Say(b"AND-THIS".to_vec()),
                if close {
                    Step::Close
                } else {
                    Step::Wait(Wait::Forever)
                },
            ]);
            for path in [Path::Ours, Path::Theirs] {
                let checked = check(path, &script, &Asking::Nothing, budget, H1Limits::default());
                assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {close}");
                let body = &seen(&checked).body;
                assert_eq!(body.len(), filler.len() + 8, "{path:?} {close}");
                assert!(body.ends_with(b"AND-THIS"), "{path:?} {close}");
            }
        }
    }

    /// A length, and then the close before a byte of the body: no answer at all, not an
    /// empty one (nginx's `proxy_extra_data.t`).
    #[test]
    fn a_close_before_any_of_a_counted_body_is_no_answer() {
        let script = says_and_closes("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert!(
                matches!(checked.got, Got::Refused(_)),
                "{path:?} {:?}",
                checked.got
            );
        }
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
    }

    /// A body the close delimits is whatever it holds, even when that looks like framing
    /// (from Envoy's codec tests).
    #[test]
    fn a_body_the_close_delimits_is_read_as_bytes_however_it_looks() {
        let script = says_and_closes(
            "HTTP/1.1 200 OK\r\n\r\ntransfer-encoding: chunked\r\n\r\nb\r\nhello world\r\n0\r\n\r\n",
        );
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(
                seen(&checked).body,
                b"transfer-encoding: chunked\r\n\r\nb\r\nhello world\r\n0\r\n\r\n"
            );
        }
    }

    /// Bytes after a chunked body's end in the same read are not part of it, trailers or
    /// no trailers (nginx's `proxy_chunked_extra.t`).
    #[test]
    fn bytes_after_the_end_of_a_chunked_body_are_not_part_of_it() {
        for tail in ["0\r\n\r\n75\r\nzzz\r\n0\r\n\r\n", "0\r\nx-a: 1\r\n\r\nJUNK"] {
            let answer = format!(
                "HTTP/1.1 200 OK\r\nconnection: close\r\ntransfer-encoding: chunked\r\n\r\n2\r\nyy\r\n{tail}"
            );
            let ours = checked(Path::Ours, &says(&answer), &Asking::Nothing);
            assert!(!matches!(ours.verdict, Verdict::Disagrees(_)), "{ours:?}");
            assert_eq!(seen(&ours).body, b"yy", "{tail:?}");
            assert!(!seen(&ours).kept, "{tail:?}");
        }
    }

    /// An HTTP/1.0 answer is read by its own framing and never kept, and a length on it
    /// is not a reason to wait for the close (Pingora's client tests).
    #[test]
    fn an_http_1_0_answer_is_read_and_its_connection_let_go() {
        for script in [
            says("HTTP/1.0 200 OK\r\ncontent-length: 3\r\n\r\nabc"),
            says_and_closes("HTTP/1.0 200 OK\r\n\r\nabc"),
        ] {
            let ours = checked(Path::Ours, &script, &Asking::Nothing);
            // HTTP/1.0 is outside the subset both clients undertake to share, so a
            // classified outcome is what is owed, and it has to be a right one.
            assert_eq!(
                ours.verdict,
                Verdict::Outside(vec!["Http10".to_owned()]),
                "{ours:?}"
            );
            assert_eq!(seen(&ours).body, b"abc");
            assert!(!seen(&ours).kept, "{ours:?}");
            assert!(ours.tape.elapsed < H1Limits::default().idle, "{ours:?}");
        }
    }

    /// HTTP/1.0 has no interim answers. Ours refuses one written in it, by 13 §4; hyper's
    /// client consumes it and reads the final answer. One of 13 §5's differences.
    #[test]
    fn an_interim_answer_in_http_1_0_is_refused_by_ours_and_consumed_by_hyper() {
        let script =
            says("HTTP/1.0 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert_eq!(
            ours.verdict,
            Verdict::Outside(vec!["Http10".to_owned()]),
            "{ours:?}"
        );
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        assert_eq!(seen(&theirs).body, b"ok", "{theirs:?}");
        assert!(matches!(theirs.verdict, Verdict::Outside(_)), "{theirs:?}");
    }

    /// An HTTP/1.0 answer that asks to be kept alive is not kept, because 13 §4 pools no
    /// connection that speaks 1.0 — a policy of this project's, which the verdict leaves
    /// room for. Called a disagreement, it would fail the hostile arm on any input that
    /// reached this shape.
    #[test]
    fn an_http_1_0_answer_that_asks_to_be_kept_is_let_go_without_blame() {
        let script =
            says("HTTP/1.0 200 OK\r\nconnection: keep-alive\r\ncontent-length: 3\r\n\r\nabc");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        assert_eq!(seen(&ours).body, b"abc");
        assert!(!seen(&ours).kept, "{ours:?}");
        assert_eq!(
            ours.verdict,
            Verdict::Outside(vec!["Http10".to_owned()]),
            "{ours:?}"
        );
    }

    /// An answer that says it closes is delivered at its length without waiting for the
    /// close, and not kept (nginx's `proxy_noclose.t`, Envoy's pool tests).
    #[test]
    fn an_answer_that_says_close_is_delivered_without_waiting_for_it() {
        let script = says(
            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 12\r\n\r\n0123456789\r\n",
        );
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(seen(&checked).body, b"0123456789\r\n", "{path:?}");
            assert!(!seen(&checked).kept, "{path:?}");
            assert!(checked.tape.elapsed < H1Limits::default().idle, "{path:?}");
        }
    }

    /// Three answers in one write while the upload is still going: the first is the
    /// answer, the rest are surplus, and the connection goes (Envoy's integration tests).
    #[test]
    fn answers_piled_up_behind_an_unfinished_upload_cost_the_connection() {
        let one = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n";
        let script = says(&one.repeat(3));
        let ours = checked(Path::Ours, &script, &Asking::Endless(vec![b"ab".to_vec()]));
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
        assert_eq!(seen(&ours).status, 200);
        assert!(seen(&ours).body.is_empty());
        assert!(!seen(&ours).kept);
    }

    /// An upstream that closes its sending side while the client's body is waiting on
    /// nothing: the exchange ends promptly as a close without an answer, rather than
    /// going round (hyper #4085) or waiting out a deadline.
    #[test]
    fn an_upstream_that_stops_sending_mid_upload_ends_the_exchange_promptly() {
        let script = Script::new(vec![Step::Wait(Wait::Written(BEGUN)), Step::Close]);
        let ours = checked(Path::Ours, &script, &Asking::Endless(vec![b"ab".to_vec()]));
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert!(
            ours.tape.elapsed < H1Limits::default().idle,
            "waited {:?}",
            ours.tape.elapsed
        );
    }

    /// A refusal that arrives, and then the connection fails while the upload is blocked,
    /// after the head was read: the answer already given stands (13 §5; nginx's lingering
    /// close tests).
    #[test]
    fn a_refusal_read_before_the_upload_failed_is_still_delivered() {
        let script = Script::with_room(
            vec![
                Step::Wait(Wait::Written(BEGUN)),
                Step::Say(b"HTTP/1.1 413 Payload Too Large\r\ncontent-length: 7\r\n\r\n".to_vec()),
                Step::Wait(Wait::Time(Duration::from_secs(1))),
                Step::Say(b"refused".to_vec()),
                Step::Fail,
            ],
            64,
        );
        let ours = checked(Path::Ours, &script, &Asking::Counted(vec![b'x'; 4096]));
        let seen = seen(&ours);
        assert_eq!((seen.status, seen.body.as_slice()), (413, &b"refused"[..]));
        assert!(!seen.kept);
    }

    /// The same, with the failure arriving before the head has been read: the answer is
    /// on the socket all the same, and a write that fails is no reason not to read it
    /// (Pingora: "flush already received data if upstream write errors").
    #[test]
    fn a_refusal_already_sent_when_the_upload_fails_is_still_delivered() {
        let script = Script::with_room(
            vec![
                Step::Wait(Wait::Written(BEGUN)),
                Step::Say(
                    b"HTTP/1.1 413 Payload Too Large\r\nconnection: close\r\ncontent-length: 7\r\n\r\nrefused"
                        .to_vec(),
                ),
                Step::Fail,
            ],
            64,
        );
        let ours = checked(Path::Ours, &script, &Asking::Counted(vec![b'x'; 4096]));
        let seen = seen(&ours);
        assert_eq!((seen.status, seen.body.as_slice()), (413, &b"refused"[..]));
        assert!(!seen.kept);
    }

    /// A head that arrives in two pieces with a stretch of upload between them is one
    /// head (Pingora's client tests).
    #[test]
    fn a_head_split_by_a_stretch_of_upload_is_one_head() {
        const TAKEN: usize = 16 * 1024;
        let script = Script::with_room(
            vec![
                Step::Wait(Wait::Written(BEGUN)),
                Step::Say(b"HTTP/1.1 200 OK\r\nconte".to_vec()),
                Step::Take(TAKEN),
                Step::Wait(Wait::Written(1024 + TAKEN)),
                Step::Say(b"nt-length: 2\r\n\r\nok".to_vec()),
                Step::Wait(Wait::Forever),
            ],
            1024,
        );
        let ours = checked(Path::Ours, &script, &Asking::Counted(vec![b'x'; 64 * 1024]));
        assert!(!matches!(ours.verdict, Verdict::Disagrees(_)), "{ours:?}");
        assert_eq!(ours.tape.written.len(), 1024 + TAKEN);
        let seen = seen(&ours);
        assert_eq!((seen.status, seen.body.as_slice()), (200, &b"ok"[..]));
        // The upload did not finish, so the connection cannot be kept.
        assert!(!seen.kept);
    }

    /// A `Transfer-Encoding` that names nothing, with a length: hyper's client reads to
    /// the close as RFC 9112 §6.3 says, and ours refuses it rather than frame it by the
    /// length, which would end the body somewhere else.
    #[test]
    fn a_coding_that_names_nothing_does_not_leave_the_length_in_charge() {
        let script = says_and_closes(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: \r\ncontent-length: 5\r\n\r\nhello world",
        );
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        assert_eq!(seen(&theirs).body, b"hello world");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
    }

    /// Framing hyper's client reads and ours refuses, by §4's rules. Each row is one of
    /// 13 §5's differences, measured here so that none is lost.
    #[test]
    fn framing_hyper_reads_and_ours_refuses() {
        let zeros = format!(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n{}5\r\nhello\r\n0\r\n\r\n",
            "0".repeat(5000)
        );
        for (script, ask, body) in [
            (
                says(
                    "HTTP/1.1 200 OK\r\ntransfer-encoding: gzip, chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
                ),
                Asking::Nothing,
                "hello",
            ),
            (
                says_and_closes("HTTP/1.1 200 OK\r\ntransfer-encoding: yolo\r\n\r\nhello"),
                Asking::Nothing,
                "hello",
            ),
            (
                says("HTTP/1.1 200 OK\r\ncontent-length: 5,5\r\n\r\nhello"),
                Asking::Nothing,
                "hello",
            ),
            (
                says("HTTP/1.1 200 OK\r\ntransfer-encoding: gzip\r\n\r\n"),
                Asking::Head,
                "",
            ),
            (
                says(
                    "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5 \r\nhello\r\n0\r\n\r\n",
                ),
                Asking::Nothing,
                "hello",
            ),
            (says(&zeros), Asking::Nothing, "hello"),
        ] {
            let theirs = checked(Path::Theirs, &script, &ask);
            assert_eq!(seen(&theirs).body, body.as_bytes(), "{:?}", script.steps());
            let ours = checked(Path::Ours, &script, &ask);
            assert!(matches!(ours.got, Got::Refused(_)), "{:?}", script.steps());
            assert!(!matches!(ours.verdict, Verdict::Disagrees(_)), "{ours:?}");
        }
    }

    /// And the other way about: hyper's client bounds the extension bytes of a whole body
    /// at 16 KiB, where ours bounds each line and not their sum.
    #[test]
    fn many_long_extensions_are_refused_by_hyper_and_read_by_ours() {
        let chunk = format!("1;{}\r\nA\r\n", "x".repeat(4000));
        let answer = format!(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n{}0\r\n\r\n",
            chunk.repeat(5)
        );
        let budget = Budget {
            said: 64 * 1024,
            ..Budget::default()
        };
        let script = says(&answer);
        let ours = check(
            Path::Ours,
            &script,
            &Asking::Nothing,
            budget,
            H1Limits::default(),
        );
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
        assert_eq!(seen(&ours).body, b"AAAAA");
        let theirs = check(
            Path::Theirs,
            &script,
            &Asking::Nothing,
            budget,
            H1Limits::default(),
        );
        assert!(matches!(theirs.got, Got::Refused(_)), "{:?}", theirs.got);
    }

    #[test]
    fn a_cancelled_exchange_is_the_run_ending_and_not_a_disagreement() {
        let script = Script::new(vec![
            Step::Wait(Wait::Written(BEGUN)),
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n".to_vec()),
            Step::Cancel,
        ]);
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.got, Got::Cancelled, "{path:?}");
            assert!(
                matches!(checked.verdict, Verdict::Outside(_)),
                "{path:?} {:?}",
                checked.verdict
            );
        }
    }
}
