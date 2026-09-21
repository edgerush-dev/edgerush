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

use super::exchange::{Exchange, H1Body};
use super::script::{self, Budget, Script, Tape};
use super::{H1Limits, lifecycle, reference};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use hyper::body::{Body as HttpBody, Bytes, Frame, SizeHint};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::Pin;
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
    fn asked(&self) -> reference::Asked {
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
            // The zero chunk, and then the end of the trailer section — which is the
            // same empty line whether any trailers were sent or not.
            Self::Chunked(_) | Self::Slow(..) | Self::Trailing(..) => {
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
    let (got, tape) = match path {
        Path::Ours => ours(script.clone(), ask, budget, limits),
        Path::Theirs => theirs(script.clone(), ask, budget),
    };
    let expected = expected(script, ask, &tape);
    let verdict = judge(&got, &expected, &limits);
    Checked {
        got,
        tape,
        expected,
        verdict,
    }
}

/// What the oracles make of the script, the request, and what the socket recorded.
#[must_use]
pub fn expected(script: &Script, ask: &Asking, tape: &Tape) -> Expected {
    let said = script.said();
    let ended = script.ends();
    let reading = reference::read(&said, ask.asked(), ended);
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
        failed: script.fails() || answer.is_none(),
        cancelled: script.cancels(),
        surplus: answer.is_some_and(|answer| answer.boundary < said.len()),
        peer_closed: ended,
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
fn judge(got: &Got, expected: &Expected, limits: &H1Limits) -> Verdict {
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

    // Outside the shared subset, or outside this project's bounds: a classified outcome
    // is all that is asked for, and every arm of `Got` is one.
    let mut outside: Vec<String> = answer
        .notable
        .iter()
        .map(|notable| format!("{notable:?}"))
        .collect();
    outside.extend(past_bounds(&answer.measured, limits));
    if !outside.is_empty() {
        return Verdict::Outside(outside);
    }

    let seen = match got {
        Got::Answer(seen) => seen,
        Got::Refused(why) => {
            return Verdict::Disagrees(vec![format!(
                "refused a message the specification reads: {why}"
            )]);
        }
        // A budget or a cancellation is the run ending. There is nothing to compare,
        // which is not the same as agreement and is not a fault either.
        Got::Spent(spent) => {
            return Verdict::Outside(vec![format!("the run's budget ran out: {spent:?}")]);
        }
        Got::Cancelled => {
            return Verdict::Outside(vec!["the script took the client away".to_owned()]);
        }
    };

    let mut faults = Vec::new();
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
    match expected.reuse {
        Ok(()) if !seen.kept => faults.push(format!(
            "gave up a connection the trace says survived: {:?}",
            expected.trace
        )),
        Err(refused) if seen.kept => {
            faults.push(format!("kept a connection that {refused:?}"));
        }
        _ => {}
    }
    if faults.is_empty() {
        Verdict::Agrees
    } else {
        Verdict::Disagrees(faults)
    }
}

/// EdgeRush's own path, over the scripted socket.
fn ours(script: Script, ask: &Asking, budget: Budget, limits: H1Limits) -> (Got, Tape) {
    let method = ask.method();
    let headers = ask.headers();
    let sending = ask.sending();
    let upload = ask.upload();
    let uri: Uri = TARGET.parse().unwrap_or_default();

    let (outcome, tape) = script::run(script, budget, move |socket| async move {
        let sent = Exchange::new(socket)
            .send(&method, &uri, &headers, sending, upload, &limits)
            .await;
        let (answer, rest) = match sent {
            Ok(pair) => pair,
            Err(error) => return Got::Refused(error.to_string()),
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
        let kept = body.take_if_reusable().is_some();
        Got::Answer(Seen {
            status: answer.head.status.as_u16(),
            fields: fields_of(&answer.head.headers),
            body: data,
            trailers,
            kept,
        })
    });
    (settled(outcome), tape)
}

/// The engine's client, over the same scripted socket.
fn theirs(script: Script, ask: &Asking, budget: Budget) -> (Got, Tape) {
    let method = ask.method();
    let headers = ask.headers();
    let upload = ask.upload();
    let uri: Uri = TARGET.parse().unwrap_or_default();

    let (outcome, tape) = script::run(script, budget, move |socket| async move {
        let io = hyper_util::rt::TokioIo::new(socket);
        let (mut sender, connection) = match hyper::client::conn::http1::handshake(io).await {
            Ok(pair) => pair,
            Err(error) => return Got::Refused(error.to_string()),
        };
        // The connection is what drives the socket; without it nothing moves.
        let _driving = tokio::spawn(connection);

        let mut request = http::Request::new(upload);
        *request.method_mut() = method;
        *request.uri_mut() = uri;
        *request.headers_mut() = headers;
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
        // Whether this client would carry another exchange on the connection. Asked of
        // the sender rather than read out of hyper's own buffers: what it did with the
        // bytes it read ahead is its business, and the question here is only whether it
        // would use the connection again.
        let kept = sender.ready().await.is_ok();
        Got::Answer(Seen {
            status: head.status.as_u16(),
            fields: fields_of(&head.headers),
            body: data,
            trailers,
            kept,
        })
    });
    (settled(outcome), tape)
}

/// The run's own ends, which are not either client's doing.
fn settled(outcome: script::Outcome<Got>) -> Got {
    match outcome {
        script::Outcome::Done(got) => got,
        script::Outcome::Cancelled => Got::Cancelled,
        script::Outcome::Spent(spent) => Got::Spent(spent),
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
        // The second of the eleven differences, in the request direction and at the
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

        // **A difference that is not one of the eleven in 13 section 5.** Ours stops
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
    fn an_upstream_that_speaks_before_the_request_is_refused_by_one_path_and_not_the_other() {
        // Every other script here waits for the request to begin. This one does not: the
        // answer is on the socket before a byte of the request has gone.
        let script = Script::new(vec![
            Step::Say(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok".to_vec()),
            Step::Wait(Wait::Forever),
        ]);
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);

        // **A second difference that is not one of the eleven.** hyper's client reads
        // before it writes, finds a message where no request is in flight, and ends the
        // connection without sending anything at all. Ours pairs the waiting bytes with
        // the request it then sends.
        assert!(matches!(theirs.got, Got::Refused(_)), "{:?}", theirs.got);
        assert!(
            theirs.tape.written.is_empty(),
            "hyper sent the request anyway"
        );
        assert_eq!(seen(&ours).body, b"ok");

        // HTTP has no rule about this — a peer that answers a request it has not been
        // sent is not a case the specification describes — so it is not a violation by
        // either client. What it is is a check ours does not have on a fresh connection:
        // on a pooled one, anything readable at checkout discards the socket (13 section
        // 6), and this is the same peer behaviour one exchange earlier.
        assert_eq!(ours.verdict, Verdict::Agrees, "{ours:?}");
    }

    #[test]
    fn surplus_bytes_in_front_of_the_connection_cost_it() {
        let script = says("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nokand more besides");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(
                checked.expected.reuse,
                Err(lifecycle::Refused::Surplus),
                "the oracle should have refused the connection"
            );
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            // The answer is still the answer: the surplus is not part of it.
            assert_eq!(seen(&checked).body, b"ok");
            assert!(
                !seen(&checked).kept,
                "{path:?} kept a connection with bytes on it"
            );
        }
    }

    #[test]
    fn an_answer_delimited_by_the_close_is_read_to_the_close() {
        let script = says_and_closes("HTTP/1.1 200 OK\r\n\r\nas much as there is");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(
                checked.expected.reuse,
                Err(lifecycle::Refused::ClosedDelimited)
            );
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(seen(&checked).body, b"as much as there is");
            assert!(!seen(&checked).kept, "{path:?} kept a closed connection");
        }
    }

    #[test]
    fn an_answer_that_asks_for_closure_costs_the_connection_even_on_an_open_socket() {
        // The upstream says to close and then does not: only what the answer's head
        // said forbids the reuse, which is the one condition a peer that closed would
        // have hidden.
        let script = says("HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 2\r\n\r\nok");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(
                checked.expected.reuse,
                Err(lifecycle::Refused::NotPersistent)
            );
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert_eq!(seen(&checked).body, b"ok");
            assert!(
                !seen(&checked).kept,
                "{path:?} kept a connection the answer asked to close"
            );
        }
    }

    #[test]
    fn a_chunked_answer_carries_its_trailers_by_both_paths() {
        let script = says(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
             5\r\nhello\r\n0\r\nx-a: 1\r\n\r\n",
        );
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Nothing);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            let seen = seen(&checked);
            assert_eq!(seen.body, b"hello");
            assert_eq!(
                seen.trailers,
                [("x-a".to_owned(), "1".to_owned())],
                "{path:?}"
            );
            assert!(seen.kept, "{path:?} gave up a good connection");
        }
    }

    #[test]
    fn an_answer_to_head_has_no_body_by_either_path() {
        let script = says("HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\n");
        for path in [Path::Ours, Path::Theirs] {
            let checked = checked(path, &script, &Asking::Head);
            assert_eq!(checked.verdict, Verdict::Agrees, "{path:?} {checked:?}");
            assert!(seen(&checked).body.is_empty(), "{path:?} read a body");
            // The five bytes the head promised are not there and were never coming; the
            // connection is still good.
            assert!(seen(&checked).kept, "{path:?} gave up a good connection");
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
    fn bad_whitespace_in_a_chunk_extension_is_refused_by_ours_and_the_grammar_allows_it() {
        let script = says(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2 ; a = \"b\"\r\nhi\r\n0\r\n\r\n",
        );
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);

        // **A third difference that is not one of the eleven, and this one is a defect
        // in ours.** RFC 9112 section 7.1.1 gives
        // `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )`,
        // so the whitespace around the `;` and the `=` is part of the grammar. Ours
        // requires the `;` immediately and refuses the exchange; hyper's client does not
        // look at extensions at all and reads the message.
        //
        // This test records what happens today rather than what should: the harness
        // reporting ours as disagreeing with the specification is the finding, and it
        // becomes the regression test for the fix by having its verdict flipped.
        assert!(matches!(ours.expected.reading, reference::Reading::Read(_)));
        assert!(
            matches!(ours.verdict, Verdict::Disagrees(_)),
            "the grammar's whitespace is accepted now, so this finding is fixed: {:?}",
            ours.verdict
        );
        assert!(matches!(ours.got, Got::Refused(_)), "{:?}", ours.got);
        assert_eq!(theirs.verdict, Verdict::Agrees, "{theirs:?}");
        assert_eq!(seen(&theirs).body, b"hi");
    }

    #[test]
    fn an_extension_with_no_name_is_read_by_one_client_and_not_the_other() {
        let script =
            says("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n2;=v\r\nhi\r\n0\r\n\r\n");
        let ours = checked(Path::Ours, &script, &Asking::Nothing);
        let theirs = checked(Path::Theirs, &script, &Asking::Nothing);
        // The first of the eleven differences, and the one kind where a difference is a
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
