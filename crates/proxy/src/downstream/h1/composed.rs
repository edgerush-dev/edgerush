//! The driver composed: the real connection driver ([`super::connection::serve`]) over a
//! socket that is nothing but memory, on a stopped clock, in front of a scripted core
//! ([14 §9](../../../../docs/14-downstream-server.md), step 5, piece 2).
//!
//! One run is a client's script — bytes to send and when, answers to wait for, a sending
//! half to close, reading to stop — and a core's script, one act for each request it is
//! handed: how long it takes, how much of the body it reads, and what it answers. What the
//! run records is facts, for the oracles to judge: the lifecycle events the harness saw
//! ([`super::lifecycle::Event`]), what the core was handed, how serving ended, and what
//! storage was still held once it had.
//!
//! **Answers are read from the client's side, by a reader that is not the driver's.** The
//! upstream side's reference reader ([`crate::upstream::h1::reference`]), written from
//! RFC 9112, reads what the client received; the core tags every answer with the request
//! it was for, so an answer that went to the wrong request says so.
//!
//! Every run is bounded by the clock, which is stopped: a run that would never end ends
//! when the time it was given is gone, and says that it did.

use super::connection::{Answered, Budget, Ended, Settings, serve};
use super::date::HttpDate;
use super::deadlines::Bounds;
use super::lifecycle::{self, Event, Trace, Violation};
use super::reference as request_reference;
use crate::interim::Interim;
use crate::raw::RawHead;
use crate::request_body::RequestBody;
use crate::storage::{LIMIT, Storage};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, Sizes};
use crate::upstream::h1::reference::{self, Asked, Reading};
use bytes::Bytes;
use http::{HeaderValue, Response, StatusCode};
use http_body::{Body, Frame, SizeHint};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

/// The field the core tags each answer with: the position of the request it answers.
pub const TAG: &str = "x-composed-request";

/// One thing the client does. A client's script is these, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStep {
    /// Writes these bytes, in one write.
    Send(Vec<u8>),
    /// Waits this long.
    Pause(Duration),
    /// Waits until it has read this many final answers in all, or the connection ends.
    Answers(usize),
    /// Waits until an informational answer has arrived, as a client that asked for a
    /// `100` does before it sends its body.
    Interim,
    /// Closes its sending half.
    CloseWrite,
    /// Stops reading, so that what the server writes backs up.
    StopReading,
    /// Reads again.
    ResumeReading,
}

/// How much of a request's body the core reads before it answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Take {
    /// All of it, trailers included.
    All,
    /// At most this many bytes of its data.
    Upto(usize),
    /// None of it.
    Nothing,
}

/// How the core's answer ends, or fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// Cleanly.
    Whole,
    /// Its body fails before any of it is ready, while its head is still unsent.
    FailsFirst,
    /// Its body fails once its data has been given, after its head has had a chance to go.
    FailsLater,
}

/// What the core does with one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Act {
    /// How long it works on the request before it answers.
    pub delay: Duration,
    /// How much of the body it reads first.
    pub take: Take,
    /// The status it answers with.
    pub status: u16,
    /// The answer's body.
    pub body: Vec<u8>,
    /// Whether its length is said in advance, or left to chunks.
    pub known_length: bool,
    /// How the answer ends.
    pub ending: Ending,
}

impl Default for Act {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            take: Take::All,
            status: 200,
            body: b"ok".to_vec(),
            known_length: true,
            ending: Ending::Whole,
        }
    }
}

/// What the core does with each request, in the order they come; past the end of the
/// list, the default act.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Core {
    /// One for each request, in order.
    pub acts: Vec<Act>,
}

/// What the core was handed of one request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Handed {
    /// Its method.
    pub method: String,
    /// Its target, as the driver made it.
    pub target: String,
    /// Its fields, lowered in name; the values of a name in the order they came.
    pub fields: Vec<(String, String)>,
    /// What of its body the core read.
    pub body: Vec<u8>,
    /// Its trailers, lowered in name, where the core read to the end.
    pub trailers: Vec<(String, String)>,
    /// Whether the core read the body to its end.
    pub body_whole: bool,
    /// Whether reading the body failed.
    pub body_failed: bool,
}

/// What a run came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    /// What the harness saw happen, in order.
    pub events: Vec<Event>,
    /// What the core was handed, one for each request, in the order it was handed them.
    pub handed: Vec<Handed>,
    /// How serving ended, or `None` if the time the run was given ran out first.
    pub(crate) ended: Option<Ended>,
    /// Application storage still held once serving had ended (14 §8).
    pub storage_left: usize,
    /// Everything the client received.
    pub received: Vec<u8>,
    /// Everything the client sent.
    pub sent: Vec<u8>,
}

impl Run {
    /// Whether the run was given up on: the time it was given ran out before serving
    /// ended, which no deadline of the driver's allows.
    #[must_use]
    pub fn given_up(&self) -> bool {
        self.ended.is_none()
    }
}

/// How long a run may take on the stopped clock before it is given up on: past every
/// deadline the driver has, so that a run that ends does so by the driver's doing.
pub const RUN_TIME: Duration = Duration::from_secs(600);

/// Runs `client` against the driver in front of `core`, on a stopped clock.
///
/// # Panics
///
/// If a runtime cannot be built, which is not known to happen.
#[must_use]
#[expect(
    clippy::expect_used,
    reason = "a harness for tests and fuzzing: a runtime that cannot be built is the harness failing"
)]
pub fn run(client: &[ClientStep], core: &Core) -> Run {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime for the harness");
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(composed(client.to_vec(), core.clone())))
}

/// What both ends of the run share: what the client sent, what the driver wrote, how far
/// the answers in it have been read, and what the harness saw.
#[derive(Debug, Default)]
struct Wire {
    events: Vec<Event>,
    /// Everything the client wrote.
    sent: Vec<u8>,
    /// Everything the driver wrote.
    written: Vec<u8>,
    /// Where the next answer starts in what was written.
    at: usize,
    /// Final answers read.
    finals: usize,
    /// Informational answers seen.
    interim: usize,
    /// Whether serving has ended.
    done: bool,
}

type Shared = Rc<RefCell<Wire>>;

/// The run itself, on the harness's runtime.
async fn composed(client: Vec<ClientStep>, core: Core) -> Run {
    let wire: Shared = Rc::new(RefCell::new(Wire::default()));
    let handed = Rc::new(RefCell::new(Vec::new()));
    let storage = Storage::new(LIMIT);
    let blocks = Rc::new(RefCell::new(Blocks::new(
        Sizes::default(),
        Rc::clone(&storage),
    )));
    let settings = Settings {
        limits: H1Limits::default(),
        bounds: Bounds::default(),
        budget: Budget::default(),
    };
    let (near, far) = tokio::io::duplex(64 * 1024);
    let far = Recorded {
        inner: far,
        wire: Rc::clone(&wire),
    };

    let respond = {
        let (wire, handed) = (Rc::clone(&wire), Rc::clone(&handed));
        let count = Rc::new(RefCell::new(0_usize));
        move |head: RawHead, body: RequestBody, _interim: Interim| {
            let index = {
                let mut count = count.borrow_mut();
                let index = *count;
                *count += 1;
                index
            };
            wire.borrow_mut().events.push(Event::Dispatched(index));
            let act = core.acts.get(index).cloned().unwrap_or_default();
            respond(index, head, body, act, Rc::clone(&wire), Rc::clone(&handed))
        }
    };
    let serving = {
        let wire = Rc::clone(&wire);
        async move {
            let ended = serve(
                far,
                settings,
                Rc::clone(&blocks),
                || HttpDate::from_unix(0),
                respond,
            )
            .await;
            // The socket is gone with the driver: what it wrote is all there is.
            let mut wire = wire.borrow_mut();
            take_answers(&mut wire, true);
            wire.events.push(Event::Closed);
            wire.done = true;
            drop(blocks);
            ended
        }
    };
    let talking = client_side(near, client, Rc::clone(&wire));

    let outcome = tokio::time::timeout(RUN_TIME, both(serving, talking)).await;
    let (ended, received) = match outcome {
        Ok((ended, received)) => (Some(ended), received),
        Err(_) => (None, Vec::new()),
    };
    let events = wire.borrow().events.clone();
    let sent = wire.borrow().sent.clone();
    let handed = handed.borrow().clone();
    Run {
        events,
        handed,
        ended,
        storage_left: storage.used(),
        received,
        sent,
    }
}

/// Both futures to their ends, polled together: what `tokio::join!` does, without the
/// macros the library does not build tokio with.
async fn both<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
    let (mut first, mut second) = (std::pin::pin!(first), std::pin::pin!(second));
    let (mut one, mut two) = (None, None);
    poll_fn(|context| {
        if one.is_none()
            && let Poll::Ready(output) = first.as_mut().poll(context)
        {
            one = Some(output);
        }
        if two.is_none()
            && let Poll::Ready(output) = second.as_mut().poll(context)
        {
            two = Some(output);
        }
        match (one.take(), two.take()) {
            (Some(one), Some(two)) => Poll::Ready((one, two)),
            (left, right) => {
                one = left;
                two = right;
                Poll::Pending
            }
        }
    })
    .await
}

/// The driver's end of the socket, which reads every answer as it is written: an answer
/// is recorded the moment the driver hands its last byte over, before the driver can do
/// anything after it, so the trace's order is the driver's own.
struct Recorded {
    inner: DuplexStream,
    wire: Shared,
}

impl Recorded {
    fn wrote(&self, bufs: &[io::IoSlice<'_>], mut count: usize) {
        let mut wire = self.wire.borrow_mut();
        for buf in bufs {
            let taken = count.min(buf.len());
            wire.written.extend_from_slice(&buf[..taken]);
            count -= taken;
            if count == 0 {
                break;
            }
        }
        take_answers(&mut wire, false);
    }
}

impl AsyncRead for Recorded {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buf)
    }
}

impl AsyncWrite for Recorded {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.inner).poll_write(context, buf);
        if let Poll::Ready(Ok(count)) = written {
            this.wrote(&[io::IoSlice::new(buf)], count);
        }
        written
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.inner).poll_write_vectored(context, bufs);
        if let Poll::Ready(Ok(count)) = written {
            this.wrote(bufs, count);
        }
        written
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

/// The core's work on one request: what it was handed recorded, the act carried out.
async fn respond(
    index: usize,
    head: RawHead,
    mut body: RequestBody,
    act: Act,
    wire: Shared,
    handed: Rc<RefCell<Vec<Handed>>>,
) -> Answered<Scripted> {
    let parts = head.into_parts();
    let mut record = Handed {
        method: parts.method.as_str().to_owned(),
        target: parts.uri.to_string(),
        fields: parts
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        ..Handed::default()
    };
    tokio::time::sleep(act.delay).await;
    let limit = match act.take {
        Take::All => usize::MAX,
        Take::Upto(most) => most,
        Take::Nothing => 0,
    };
    while record.body.len() < limit || act.take == Take::All {
        match poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await {
            None => {
                record.body_whole = true;
                break;
            }
            Some(Err(_)) => {
                record.body_failed = true;
                break;
            }
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => record.body.extend_from_slice(&data),
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        record.trailers = trailers
                            .iter()
                            .map(|(name, value)| {
                                (
                                    name.as_str().to_owned(),
                                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                                )
                            })
                            .collect();
                    }
                }
            },
        }
    }
    if !record.body_whole && !record.body_failed && !body.is_end_stream() {
        wire.borrow_mut().events.push(Event::UploadAbandoned(index));
    }
    handed.borrow_mut().push(record);

    let status = StatusCode::from_u16(act.status).unwrap_or(StatusCode::OK);
    let mut response = Response::new(Scripted {
        data: (!act.body.is_empty()).then(|| Bytes::from(act.body.clone())),
        length: act.known_length.then_some(act.body.len() as u64),
        ending: act.ending,
        waited: false,
    });
    *response.status_mut() = status;
    if let Ok(tag) = HeaderValue::from_str(&index.to_string()) {
        response.headers_mut().insert(TAG, tag);
    }
    Answered::Map(response)
}

/// The core's answer body, as its act says.
#[derive(Debug)]
pub struct Scripted {
    data: Option<Bytes>,
    length: Option<u64>,
    ending: Ending,
    /// Whether it has made its head wait once, so that the head can go before it fails.
    waited: bool,
}

impl Body for Scripted {
    type Data = Bytes;
    type Error = &'static str;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        match this.ending {
            Ending::FailsFirst => return Poll::Ready(Some(Err("failed before anything"))),
            Ending::FailsLater if !this.waited => {
                this.waited = true;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            _ => {}
        }
        if let Some(data) = this.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        match this.ending {
            Ending::FailsLater => Poll::Ready(Some(Err("failed part way"))),
            _ => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.ending == Ending::Whole
    }

    fn size_hint(&self) -> SizeHint {
        self.length
            .map_or_else(SizeHint::default, SizeHint::with_exact)
    }
}

/// How often a client that is not reading looks again at whether what it waits for has
/// happened. The clock is stopped, so this costs no time, only turns.
const LOOK_AGAIN: Duration = Duration::from_millis(100);

/// The client: its script carried out. The answers are read at the driver's end of the
/// socket ([`Recorded`]); this end only takes what arrives, or stops taking it.
async fn client_side(mut socket: DuplexStream, steps: Vec<ClientStep>, wire: Shared) -> Vec<u8> {
    let mut received = Vec::new();
    let mut reading = true;
    for step in steps {
        match step {
            ClientStep::Send(bytes) => {
                wire.borrow_mut().sent.extend_from_slice(&bytes);
                if socket.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            ClientStep::Pause(time) => {
                if reading {
                    let _still = tokio::time::timeout(
                        time,
                        drain(&mut socket, &mut received, &wire, |_| false),
                    )
                    .await;
                } else {
                    tokio::time::sleep(time).await;
                }
            }
            ClientStep::Answers(count) => {
                wait(&mut socket, &mut received, &wire, reading, |wire| {
                    wire.finals >= count
                })
                .await;
            }
            ClientStep::Interim => {
                wait(&mut socket, &mut received, &wire, reading, |wire| {
                    wire.interim > 0
                })
                .await;
            }
            ClientStep::CloseWrite => {
                let _shut = socket.shutdown().await;
            }
            ClientStep::StopReading => reading = false,
            ClientStep::ResumeReading => reading = true,
        }
    }
    // To the end of the connection, reading or not.
    wait(&mut socket, &mut received, &wire, reading, |_| false).await;
    received
}

/// Waits until `until` holds or serving has ended, taking what arrives meanwhile if the
/// client is reading.
async fn wait(
    socket: &mut DuplexStream,
    received: &mut Vec<u8>,
    wire: &Shared,
    reading: bool,
    until: impl Fn(&Wire) -> bool,
) {
    if reading {
        drain(socket, received, wire, until).await;
        return;
    }
    loop {
        {
            let wire = wire.borrow();
            if until(&wire) || wire.done {
                return;
            }
        }
        tokio::time::sleep(LOOK_AGAIN).await;
    }
}

/// Takes what arrives until `until` holds or the connection ends.
async fn drain(
    socket: &mut DuplexStream,
    received: &mut Vec<u8>,
    wire: &Shared,
    until: impl Fn(&Wire) -> bool,
) {
    loop {
        if until(&wire.borrow()) {
            return;
        }
        let mut bytes = [0; 4096];
        match socket.read(&mut bytes).await {
            Ok(0) | Err(_) => return,
            Ok(read) => received.extend_from_slice(&bytes[..read]),
        }
    }
}

/// Records every answer that has become whole in what the driver wrote; with `closed`,
/// the one left cut short too.
fn take_answers(wire: &mut Wire, closed: bool) {
    loop {
        let rest = &wire.written[wire.at..];
        if rest.is_empty() {
            return;
        }
        let asked = asked(&wire.sent, wire.finals);
        match reference::read(rest, asked, closed) {
            Reading::Read(answer) => {
                wire.interim += answer.interim.len();
                wire.finals += 1;
                wire.at += answer.boundary;
                wire.events.push(Event::Answered {
                    request: tagged(&answer.fields),
                    status: answer.status,
                    complete: true,
                });
            }
            Reading::Unfinished | Reading::Invalid(_) => {
                let interim = interim_heads(rest);
                wire.interim = wire.interim.max(interim);
                if closed && let Some((status, request)) = head_of(rest) {
                    wire.events.push(Event::Answered {
                        request,
                        status,
                        complete: false,
                    });
                }
                return;
            }
        }
    }
}

/// Whether the answer after `answered` others is to a `HEAD`, as far as the client's own
/// requests say.
fn asked(sent: &[u8], answered: usize) -> Asked {
    let mut at = 0;
    for _ in 0..answered {
        match super::reference::read(&sent[at..]) {
            super::reference::Reading::Read(request) => match request.body {
                super::reference::BodyReading::Whole(body) => at += body.end,
                _ => return Asked::Anything,
            },
            _ => return Asked::Anything,
        }
    }
    match super::reference::read(&sent[at..]) {
        super::reference::Reading::Read(request) if request.method == "HEAD" => Asked::Head,
        _ => Asked::Anything,
    }
}

/// The request an answer's tag names.
fn tagged(fields: &[(String, String)]) -> Option<usize> {
    fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(TAG))
        .and_then(|(_, value)| value.trim().parse().ok())
}

/// The status and tag of the final head at the start of `bytes`, informational heads
/// before it passed over, where a whole head is there.
fn head_of(bytes: &[u8]) -> Option<(u16, Option<usize>)> {
    let mut rest = bytes;
    loop {
        let end = rest.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
        let head = String::from_utf8_lossy(&rest[..end]).into_owned();
        let status: u16 = head.get(9..12)?.parse().ok()?;
        if (100..200).contains(&status) {
            rest = &rest[end..];
            continue;
        }
        let fields: Vec<(String, String)> = head
            .split("\r\n")
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
            .collect();
        return Some((status, tagged(&fields)));
    }
}

/// How many whole informational heads start `bytes`.
fn interim_heads(bytes: &[u8]) -> usize {
    let mut rest = bytes;
    let mut count = 0;
    while let Some(end) = rest.windows(4).position(|window| window == b"\r\n\r\n") {
        let is_interim = rest.get(9) == Some(&b'1');
        if !is_interim {
            break;
        }
        count += 1;
        rest = &rest[end + 4..];
    }
    count
}

/// What a run got wrong, by one oracle or the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// The connection's life broke a rule of the lifecycle oracle.
    Lifecycle(Violation),
    /// The core was handed something the client did not send: the request at this
    /// position, and what of it differed.
    Meaning {
        /// The request, by position among those the core was handed.
        request: usize,
        /// What differed.
        what: &'static str,
    },
    /// The core was handed a request the client did not send whole, or at all.
    HandedNothingSent(usize),
    /// Storage was still held once serving had ended.
    StorageHeld(usize),
    /// The run was given up on: no deadline of the driver's ended it.
    GivenUp,
}

/// Holds a run against both oracles: its life against the lifecycle model, and what the
/// core was handed against what the reference reader says the client sent.
///
/// # Errors
///
/// The first thing the run got wrong.
pub fn judge(run: &Run) -> Result<(), Finding> {
    if run.given_up() {
        return Err(Finding::GivenUp);
    }
    let limits = H1Limits::default();
    let trace = Trace {
        events: run.events.clone(),
        permits_left: 0,
    };
    lifecycle::judge(&run.sent, &trace, &limits).map_err(Finding::Lifecycle)?;

    let mut at = 0;
    for (index, handed) in run.handed.iter().enumerate() {
        let request = match request_reference::read(run.sent.get(at..).unwrap_or_default()) {
            request_reference::Reading::Read(request) => request,
            _ => return Err(Finding::HandedNothingSent(index)),
        };
        let meaning = |what| Finding::Meaning {
            request: index,
            what,
        };
        if handed.method != request.method {
            return Err(meaning("method"));
        }
        if handed.target != request.target {
            return Err(meaning("target"));
        }
        if by_name(&handed.fields) != by_name(&request.fields) {
            return Err(meaning("fields"));
        }
        match &request.body {
            request_reference::BodyReading::Whole(body) => {
                if handed.body_whole && handed.body != body.data {
                    return Err(meaning("body"));
                }
                if !body.data.starts_with(&handed.body) {
                    return Err(meaning("part of the body"));
                }
                // Filtered, but never made up: every trailer handed on was sent.
                let sent = by_name(&body.trailers);
                for (name, value) in by_name(&handed.trailers) {
                    if sent.get(&name) != Some(&value) {
                        return Err(meaning("trailers"));
                    }
                }
                at += body.end;
            }
            // Not all of it came, so only what did can be held to it.
            request_reference::BodyReading::Unfinished
            | request_reference::BodyReading::Invalid(_) => {
                if handed.body_whole {
                    return Err(meaning("a body that did not end was handed on whole"));
                }
                at = run.sent.len();
            }
        }
    }
    if run.storage_left > 0 {
        return Err(Finding::StorageHeld(run.storage_left));
    }
    Ok(())
}

/// Fields as a map from name to its values in order, names lowered and values trimmed,
/// so that two readers that group repeated names differently still compare equal.
fn by_name(fields: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in fields {
        map.entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.trim_matches([' ', '\t']).to_owned());
    }
    map
}

#[cfg(test)]
mod tests {
    use super::super::deadlines::Clock;
    use super::*;
    use ClientStep::{Answers, CloseWrite, Interim, Pause, ResumeReading, Send, StopReading};

    fn get(path: &str) -> Vec<u8> {
        format!("GET {path} HTTP/1.1\r\nhost: a\r\n\r\n").into_bytes()
    }

    fn answered(request: usize, status: u16, complete: bool) -> Event {
        Event::Answered {
            request: Some(request),
            status,
            complete,
        }
    }

    #[test]
    fn pipelined_requests_are_handed_on_in_order_and_answered_in_order() {
        let sent = [get("/0"), get("/1")].concat();
        let run = run(&[Send(sent), Answers(2), CloseWrite], &Core::default());
        assert_eq!(
            run.events,
            [
                Event::Dispatched(0),
                answered(0, 200, true),
                Event::Dispatched(1),
                answered(1, 200, true),
                Event::Closed,
            ]
        );
        let targets: Vec<&str> = run.handed.iter().map(|h| h.target.as_str()).collect();
        assert_eq!(targets, ["/0", "/1"]);
        assert_eq!(run.storage_left, 0);
    }

    #[test]
    fn a_body_the_core_leaves_unread_ends_the_connection() {
        let sent = b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 3\r\n\r\n".to_vec();
        let core = Core {
            acts: vec![Act {
                take: Take::Nothing,
                ..Act::default()
            }],
        };
        let run = run(&[Send(sent), Answers(1)], &core);
        assert_eq!(run.events[0], Event::Dispatched(0));
        assert!(
            run.events.contains(&Event::UploadAbandoned(0)),
            "{:?}",
            run.events
        );
        assert!(
            run.events.contains(&answered(0, 200, true)),
            "{:?}",
            run.events
        );
        assert_eq!(run.events.last(), Some(&Event::Closed), "{:?}", run.events);
        assert!(run.ended.is_some());
    }

    #[test]
    fn a_body_and_its_trailers_reach_the_core() {
        let sent = b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\nte: trailers\r\n\r\n3\r\nabc\r\n0\r\nx-t: 1\r\n\r\n".to_vec();
        let run = run(&[Send(sent), Answers(1), CloseWrite], &Core::default());
        let handed = &run.handed[0];
        assert_eq!(handed.body, b"abc");
        assert!(handed.body_whole);
        assert_eq!(handed.trailers, [("x-t".to_owned(), "1".to_owned())]);
    }

    #[test]
    fn a_refused_request_reaches_no_core_and_is_answered_by_the_driver() {
        let run = run(
            &[Send(b"GE T / HTTP/1.1\r\n\r\n".to_vec())],
            &Core::default(),
        );
        assert!(run.handed.is_empty());
        assert_eq!(
            run.events,
            [
                Event::Answered {
                    request: None,
                    status: 400,
                    complete: true
                },
                Event::Closed
            ]
        );
    }

    #[test]
    fn an_answer_that_fails_before_its_head_goes_is_replaced_and_one_after_is_cut() {
        for (ending, event) in [
            (
                Ending::FailsFirst,
                Event::Answered {
                    request: None,
                    status: 502,
                    complete: true,
                },
            ),
            (Ending::FailsLater, answered(0, 200, false)),
        ] {
            let core = Core {
                acts: vec![Act {
                    known_length: false,
                    ending,
                    ..Act::default()
                }],
            };
            let run = run(&[Send(get("/")), Answers(1)], &core);
            assert!(run.events.contains(&event), "{ending:?}: {:?}", run.events);
            assert_eq!(run.events.last(), Some(&Event::Closed));
        }
    }

    #[test]
    fn a_run_that_nobody_ends_ends_at_the_driver_s_deadline() {
        // A head never finished: the driver's deadline, not the run's, ends it.
        let run = run(&[Send(b"GET / HT".to_vec())], &Core::default());
        assert!(!run.given_up(), "the run was given up on");
        assert_eq!(run.events, [Event::Closed]);
    }

    #[test]
    fn a_client_that_waits_for_its_100_is_sent_one_and_then_sends_its_body() {
        let head =
            b"POST / HTTP/1.1\r\nhost: a\r\nexpect: 100-continue\r\ncontent-length: 3\r\n\r\n";
        let run = run(
            &[
                Send(head.to_vec()),
                Interim,
                Send(b"abc".to_vec()),
                Answers(1),
                CloseWrite,
            ],
            &Core::default(),
        );
        assert!(run.received.starts_with(b"HTTP/1.1 100 Continue\r\n\r\n"));
        assert!(
            run.events.contains(&answered(0, 200, true)),
            "{:?}",
            run.events
        );
        assert_eq!(run.handed[0].body, b"abc");
    }

    /// A client that stops reading holds the answer back; one that reads again before the
    /// write deadline gets all of it, and one that does not has its connection closed at
    /// the deadline, the answer cut short. The clock is stopped, so the waits cost nothing.
    #[test]
    fn a_client_that_stops_reading_is_waited_for_until_the_write_deadline() {
        let core = Core {
            acts: vec![Act {
                body: vec![b'x'; 256 * 1024],
                ..Act::default()
            }],
        };
        let idle = Bounds::default().idle;
        let whole = run(
            &[
                StopReading,
                Send(get("/")),
                Pause(idle / 2),
                ResumeReading,
                Answers(1),
                CloseWrite,
            ],
            &core,
        );
        assert!(
            whole.events.contains(&answered(0, 200, true)),
            "{:?}",
            whole.events
        );

        let cut = run(
            &[StopReading, Send(get("/")), Pause(idle * 2), ResumeReading],
            &core,
        );
        assert_eq!(cut.ended, Some(Ended::TimedOut(Clock::WriteIdle)));
        assert!(
            cut.events.contains(&answered(0, 200, false)),
            "{:?}",
            cut.events
        );
        assert_eq!(cut.storage_left, 0);
    }

    #[test]
    fn a_body_the_core_reads_only_part_of_ends_the_connection() {
        let sent = b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 10\r\n\r\n0123456789".to_vec();
        let core = Core {
            acts: vec![Act {
                take: Take::Upto(3),
                ..Act::default()
            }],
        };
        let run = run(&[Send(sent), Answers(1)], &core);
        assert!(run.handed[0].body.len() >= 3);
        assert!(!run.handed[0].body_whole);
        assert!(
            run.events.contains(&Event::UploadAbandoned(0)),
            "{:?}",
            run.events
        );
        assert_eq!(run.events.last(), Some(&Event::Closed));
    }

    /// The examples above, each held against both oracles too.
    #[test]
    fn the_examples_pass_both_oracles() {
        let post = b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 3\r\n\r\nabc".to_vec();
        for (client, core) in [
            (
                vec![
                    Send([get("/0"), get("/1")].concat()),
                    Answers(2),
                    CloseWrite,
                ],
                Core::default(),
            ),
            (
                vec![Send(post.clone()), Answers(1), CloseWrite],
                Core::default(),
            ),
            (
                vec![Send(post), Answers(1)],
                Core {
                    acts: vec![Act {
                        take: Take::Nothing,
                        ..Act::default()
                    }],
                },
            ),
            (
                vec![Send(b"GE T / HTTP/1.1\r\n\r\n".to_vec())],
                Core::default(),
            ),
            (vec![Send(b"GET / HT".to_vec())], Core::default()),
        ] {
            let run = run(&client, &core);
            assert_eq!(judge(&run), Ok(()), "{client:?}: {:?}", run.events);
        }
    }

    mod generated {
        use super::*;
        use proptest::prelude::*;
        use proptest::sample::Index;

        /// How a request's body is framed.
        #[derive(Debug, Clone)]
        enum Framed {
            None,
            Length(Vec<u8>),
            Chunked(Vec<Vec<u8>>, Vec<(String, String)>),
        }

        fn text(max: usize) -> impl Strategy<Value = String> {
            proptest::string::string_regex(&format!("[a-z0-9]{{0,{max}}}")).unwrap()
        }

        fn framed() -> impl Strategy<Value = Framed> {
            prop_oneof![
                3 => Just(Framed::None),
                2 => prop::collection::vec(any::<u8>(), 0..40).prop_map(Framed::Length),
                2 => (
                    prop::collection::vec(prop::collection::vec(any::<u8>(), 1..12), 0..4),
                    prop::collection::vec(
                        (prop::sample::select(vec!["x-t", "x-u", "content-length", "host"]), text(6)),
                        0..3
                    ),
                )
                    .prop_map(|(chunks, trailers)| {
                        Framed::Chunked(
                            chunks,
                            trailers
                                .into_iter()
                                .map(|(name, value)| (name.to_owned(), value))
                                .collect(),
                        )
                    }),
            ]
        }

        /// One request, from parts that are each valid on their own; together they can
        /// still be one this project refuses, which is for the oracles to know.
        fn request() -> impl Strategy<Value = Vec<u8>> {
            (
                prop::sample::select(vec!["GET", "POST", "HEAD", "PUT", "OPTIONS"]),
                prop::sample::select(vec!["/", "/a?b=c", "/%61/b", "http://h.test/x", "/a/../b"]),
                // Most requests keep the connection, so that pipelines are long enough to
                // be about pipelining.
                prop::bool::weighted(0.2),
                prop::bool::weighted(0.9),
                prop::collection::vec(
                    (prop::sample::select(vec!["x-a", "x-b", "accept"]), text(8)),
                    0..3,
                ),
                prop::bool::weighted(0.15),
                prop::bool::weighted(0.2),
                framed(),
            )
                .prop_map(
                    |(method, target, old, host, fields, close, expect, framed)| {
                        let version = if old { "1.0" } else { "1.1" };
                        let mut head = format!("{method} {target} HTTP/{version}\r\n");
                        if host {
                            head.push_str("host: h.test\r\n");
                        }
                        for (name, value) in fields {
                            head.push_str(&format!("{name}: {value}\r\n"));
                        }
                        if close {
                            head.push_str("connection: close\r\n");
                        }
                        if expect {
                            head.push_str("expect: 100-continue\r\n");
                        }
                        let mut bytes = match &framed {
                            Framed::None => head.into_bytes(),
                            Framed::Length(body) => {
                                format!("{head}content-length: {}\r\n", body.len()).into_bytes()
                            }
                            Framed::Chunked(..) => {
                                format!("{head}te: trailers\r\ntransfer-encoding: chunked\r\n")
                                    .into_bytes()
                            }
                        };
                        bytes.extend_from_slice(b"\r\n");
                        match framed {
                            Framed::None => {}
                            Framed::Length(body) => bytes.extend_from_slice(&body),
                            Framed::Chunked(chunks, trailers) => {
                                for chunk in chunks {
                                    bytes.extend_from_slice(
                                        format!("{:x}\r\n", chunk.len()).as_bytes(),
                                    );
                                    bytes.extend_from_slice(&chunk);
                                    bytes.extend_from_slice(b"\r\n");
                                }
                                bytes.extend_from_slice(b"0\r\n");
                                for (name, value) in trailers {
                                    bytes.extend_from_slice(
                                        format!("{name}: {value}\r\n").as_bytes(),
                                    );
                                }
                                bytes.extend_from_slice(b"\r\n");
                            }
                        }
                        bytes
                    },
                )
        }

        /// Bytes that are not a request, or not one this project reads, to follow the
        /// requests now and then.
        fn junk() -> impl Strategy<Value = Vec<u8>> {
            prop::sample::select(vec![
                &b""[..],
                b"GE T / HTTP/1.1\r\n\r\n",
                b"\r\n\r\n",
                b"GET / HTTP/1.1\nhost: a\n\n",
                b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 1\r\ncontent-length: 2\r\n\r\nab",
                b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\nzz\r\n",
                b"GET /x#y HTTP/1.1\r\nhost: a\r\n\r\n",
                b"GET / HT",
            ])
            .prop_map(<[u8]>::to_vec)
        }

        fn act() -> impl Strategy<Value = Act> {
            (
                prop::sample::select(vec![0_u64, 100, 5_000]),
                prop_oneof![
                    6 => Just(Take::All),
                    1 => (0_usize..20).prop_map(Take::Upto),
                    1 => Just(Take::Nothing),
                ],
                prop::sample::select(vec![200_u16, 404, 500]),
                prop::collection::vec(any::<u8>(), 0..50),
                any::<bool>(),
                prop_oneof![
                    8 => Just(Ending::Whole),
                    1 => Just(Ending::FailsFirst),
                    1 => Just(Ending::FailsLater),
                ],
            )
                .prop_map(|(delay, take, status, body, known_length, ending)| Act {
                    delay: Duration::from_millis(delay),
                    take,
                    status,
                    body,
                    known_length,
                    ending,
                })
        }

        /// A client's script: requests, maybe junk after them, cut at chosen bytes and
        /// sent with pauses between, with an early stop in reading now and then, and an
        /// end that waits for answers, closes its sending half, or does neither.
        fn client() -> impl Strategy<Value = Vec<ClientStep>> {
            (
                prop::collection::vec(request(), 1..6),
                prop_oneof![3 => Just(Vec::new()), 1 => junk()],
                prop::collection::vec(any::<Index>(), 0..4),
                prop::sample::select(vec![0_u64, 50, 2_000]),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
            )
                .prop_map(
                    |(requests, junk, cuts, pause, stop_reading, wait, close)| {
                        let count = requests.len();
                        let mut bytes = requests.concat();
                        bytes.extend_from_slice(&junk);
                        let mut cuts: Vec<usize> =
                            cuts.iter().map(|cut| cut.index(bytes.len() + 1)).collect();
                        cuts.sort_unstable();
                        cuts.dedup();
                        let mut steps = Vec::new();
                        if stop_reading {
                            steps.push(StopReading);
                        }
                        let mut from = 0;
                        for cut in cuts.into_iter().chain([bytes.len()]) {
                            if cut > from {
                                steps.push(Send(bytes[from..cut].to_vec()));
                                steps.push(Pause(Duration::from_millis(pause)));
                                from = cut;
                            }
                        }
                        if stop_reading {
                            steps.push(ResumeReading);
                        }
                        if wait {
                            steps.push(Answers(count));
                        }
                        if close {
                            steps.push(CloseWrite);
                        }
                        steps
                    },
                )
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 256,
                ..ProptestConfig::default()
            })]

            /// However a client sends its requests and however the core answers them, the
            /// connection lives as the lifecycle oracle says it must, the core is handed
            /// what the client sent, and nothing is left held.
            #[test]
            fn every_run_passes_both_oracles(client in client(), acts in prop::collection::vec(act(), 0..5)) {
                let core = Core { acts };
                let run = run(&client, &core);
                prop_assert_eq!(judge(&run), Ok(()), "{:?}\n{:?}", run.events, String::from_utf8_lossy(&run.sent));
            }
        }
    }
}
