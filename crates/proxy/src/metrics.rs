//! What the data plane counts, and how a scrape reads it.
//!
//! Series belong to the data plane, not to a config snapshot, so a reload never resets a
//! counter. A listener's series are found by the position of its socket. An upstream's are
//! found by a **slot** that its name is given when a config that has it first arrives: a
//! request carries the slot, a plain number, across the wait for its upstream, and
//! counting is a load and an add — no lock, no reference count, no name. Slots are never
//! handed back, and there are [`UPSTREAM_SLOTS`] of them; upstreams beyond that are
//! counted together in one series, as docs/08 wants of anything that a config can make
//! arbitrarily many of.

use crate::downstream::h1::codec::RequestError;
use crate::forwarding::Cut;
use crate::grpc::status::{Code, NAMES};
use crate::request::Rejection;
use edgerush_telemetry::{Counter, Exposition, Gauge, Histogram, Kind, Sharded};
use http::StatusCode;
use std::cell::Cell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

/// How many upstream names get series of their own over the life of a data plane.
pub(crate) const UPSTREAM_SLOTS: usize = 4096;

/// The slot of the series that upstreams beyond [`UPSTREAM_SLOTS`] share, and the name it
/// is shown under.
const OVERFLOW_SLOT: usize = 0;
const OVERFLOW_NAME: &str = "_overflow";

/// Upper bounds of the time to the response head, in nanoseconds: half a millisecond to
/// ten seconds.
const HEAD_TIME_BOUNDS: [u64; 14] = [
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
];

const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];

/// Why an access-log record was dropped ([21 §4](../../docs/21-access-logs.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogsDropped {
    /// A worker had no batch free to write it into: the logger was behind.
    Behind,
    /// The logger could not write it.
    Unwritten,
}

impl LogsDropped {
    /// The name this reason is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Behind => "behind",
            Self::Unwritten => "unwritten",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    pub(crate) const ALL: [Self; 2] = [Self::Behind, Self::Unwritten];
}

/// Why an exchange by EdgeRush's own path ended without an answer.
///
/// A fixed list, and what a counter is labelled with is a name from it: an upstream
/// cannot invent a new series by failing in a new way, and no error text or address ever
/// reaches a label ([13 §7](../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// What the upstream said could not be read.
    Codec,
    /// The connection itself failed.
    Io,
    /// The request's own body could not be read.
    RequestBody,
    /// The upstream went away without answering.
    Closed,
    /// The upstream said something before it was asked anything.
    Unsolicited,
    /// More interim answers, or longer ones, than an exchange will wait through.
    Interim,
    /// No final head within the time an exchange has for one.
    TooSlow,
    /// Whatever was being waited for stopped happening for long enough.
    Idle,
    /// The worker could not pay for storage the exchange needed.
    Exhausted,
}

impl Stopped {
    /// The name this reason is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Codec => "codec",
            Self::Io => "io",
            Self::RequestBody => "request_body",
            Self::Closed => "closed",
            Self::Unsolicited => "unsolicited",
            Self::Interim => "interim",
            Self::TooSlow => "too_slow",
            Self::Idle => "idle",
            Self::Exhausted => "exhausted",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    const ALL: [Self; 9] = [
        Self::Codec,
        Self::Io,
        Self::RequestBody,
        Self::Closed,
        Self::Unsolicited,
        Self::Interim,
        Self::TooSlow,
        Self::Idle,
        Self::Exhausted,
    ];
}

/// What an HTTP/3 listener did with a datagram other than hand it to its connection
/// ([16 §3, §4](../../docs/16-http3.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Quic {
    /// Handed to the worker whose connection it is for.
    Forwarded,
    /// Dropped: that worker's inbox was full.
    InboxFull,
    /// Answered with a Retry, handshakes under way being past the threshold.
    Retry,
    /// Answered with a version negotiation.
    Negotiation,
    /// An Initial dropped: its worker had no room for another connection, or the listener
    /// held its share of the room (03 §9).
    NoRoom,
}

impl Quic {
    /// The name this is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::InboxFull => "inbox_full",
            Self::Retry => "retry",
            Self::Negotiation => "version_negotiation",
            Self::NoRoom => "no_room",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    const ALL: [Self; 5] = [
        Self::Forwarded,
        Self::InboxFull,
        Self::Retry,
        Self::Negotiation,
        Self::NoRoom,
    ];
}

/// Why a listener stopped accepting, its next connections left in the kernel's backlog
/// ([03 §9](../../docs/03-data-plane.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptPause {
    /// It holds its fair share of the workers' room for connections, which is short.
    Share,
    /// The worker holds as many connections as it may.
    WorkerCap,
}

impl AcceptPause {
    const ALL: [Self; 2] = [Self::Share, Self::WorkerCap];

    /// The name this is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Share => "share",
            Self::WorkerCap => "worker_cap",
        }
    }
}

/// How a `tcp` or `tls` listener's connection ended
/// ([17 §4](../../docs/17-tcp-and-tls-passthrough.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tunnel {
    /// Carried until both ends had finished.
    Closed,
    /// Closed at its idle bound, having carried nothing either way for that long.
    Idle,
    /// Closed at the drain's bound.
    Drained,
    /// One end failed part way.
    Failed,
    /// Not routed: its ClientHello was refused, asked for no name, or for one no route has.
    Refused,
    /// Its ClientHello did not come within the first-request deadline.
    TooSlow,
    /// Its route had no backend with an endpoint.
    NoBackend,
    /// The backend's endpoint could not be reached within the connect bound.
    ConnectFailed,
    /// The worker was short: of storage for its buffers, or of a socket to connect to the
    /// backend with.
    Exhausted,
}

impl Tunnel {
    /// The name this is counted under.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Idle => "idle",
            Self::Drained => "drained",
            Self::Failed => "failed",
            Self::Refused => "refused",
            Self::TooSlow => "too_slow",
            Self::NoBackend => "no_backend",
            Self::ConnectFailed => "connect_failed",
            Self::Exhausted => "exhausted",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    const ALL: [Self; 9] = [
        Self::Closed,
        Self::Idle,
        Self::Drained,
        Self::Failed,
        Self::Refused,
        Self::TooSlow,
        Self::NoBackend,
        Self::ConnectFailed,
        Self::Exhausted,
    ];
}

/// What came of the PROXY protocol header a listener with senders reads at the start of
/// every connection ([20 §6](../../docs/20-proxy-protocol.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProxyHeader {
    /// A sender's, whose addresses are used.
    Accepted,
    /// A sender's that leaves the connection's own ends in use: `LOCAL`, `UNKNOWN`, or a
    /// family or transport not taken.
    Local,
    /// A valid header from a peer that is not among the senders, dropped.
    Untrusted,
    /// A header that is not valid.
    Malformed,
    /// First bytes that are no header.
    Missing,
    /// The client closed before a whole header: one that never sends a byte included.
    Closed,
    /// No whole header within the first-request deadline.
    TooSlow,
}

impl ProxyHeader {
    /// The name this is counted under.
    fn name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Local => "local",
            Self::Untrusted => "untrusted",
            Self::Malformed => "malformed",
            Self::Missing => "missing",
            Self::Closed => "closed",
            Self::TooSlow => "too_slow",
        }
    }

    /// Every one of them, for a scrape that shows a series whether it has happened or not.
    const ALL: [Self; 7] = [
        Self::Accepted,
        Self::Local,
        Self::Untrusted,
        Self::Malformed,
        Self::Missing,
        Self::Closed,
        Self::TooSlow,
    ];
}

/// What became of a connection to an upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Socket {
    /// Opened for this exchange, there being none to take.
    Opened,
    /// Taken from what the worker was keeping.
    Reused,
    /// Dropped rather than used or kept: it had something to say at checkout, or its age
    /// or idleness had run out.
    Discarded,
}

impl Socket {
    fn name(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Reused => "reused",
            Self::Discarded => "discarded",
        }
    }

    const ALL: [Self; 3] = [Self::Opened, Self::Reused, Self::Discarded];
}

/// Why the data plane answered a request itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    BadHost,
    BadPath,
    BadConnection,
    BadTarget,
    NoRoute,
    NoBackend,
    NoEndpoints,
    /// No connection to the endpoint could be opened: refused, reset or out of time while
    /// connecting, or a TLS handshake that failed. Nothing of the request was sent
    /// ([03 §6](../../docs/03-data-plane.md)).
    Unreachable,
    /// The upstream failed once connected: an answer that could not be read, or none.
    UpstreamFailed,
    /// A try ran out of time before its answer's head: the upstream's own clocks for a head,
    /// or for what it was sent or would say, not a gRPC call's deadline
    /// ([13 §7](../../docs/13-http1-upstream.md)). The upstream's failing, and counted so.
    UpstreamTimedOut,
    /// This worker already has as many exchanges in hand as it will take.
    TooBusy,
    /// This worker is short of places for exchanges, and the request's upstream already
    /// holds its fair share of them ([03 §9](../../docs/03-data-plane.md)).
    OverShare,
    /// This worker could not pay for the storage an exchange needed
    /// ([14 §8](../../docs/14-downstream-server.md)): its own failing, not the upstream's.
    Exhausted,
    /// A WebSocket for an HTTP/2 or HTTP/3 client, which counts as a connection, found its
    /// worker at its cap or its listener at its share
    /// ([03 §9](../../docs/03-data-plane.md)).
    NoRoom,
    /// The request's head could not take its changes, or they took it past the bound on a
    /// head ([14 §6](../../docs/14-downstream-server.md)).
    Edits,
    /// The request's body could not be read: the client's fault, found once the request
    /// had gone upstream, and not the upstream failing.
    BadBody,
    /// The request's body stopped arriving for longer than its idle bound, while it was
    /// being waited on ([14 §8](../../docs/14-downstream-server.md)).
    BodyTimedOut,
    /// As many requests wait for a place on an HTTP/2 upstream's connections as may
    /// ([15 §4](../../docs/15-http2-and-grpc.md)).
    QueueFull,
    /// The request waited as long as it may for a place on one.
    QueueTimedOut,
    /// Connection-bound credentials (NTLM, Negotiate) for an HTTP/2 upstream, where they
    /// would authenticate every client's streams ([15 §5](../../docs/15-http2-and-grpc.md)).
    ConnectionAuth,
    /// The request's deadline passed before its answer began: its rule's `request` timeout
    /// or a gRPC call's own ([03 §6](../../docs/03-data-plane.md),
    /// [15 §6](../../docs/15-http2-and-grpc.md)).
    DeadlineExceeded,
    /// The request's rule redirects it ([18](../../docs/18-redirects-and-rewrites.md)).
    Redirected,
    /// An extended CONNECT for a protocol other than WebSocket
    /// ([19 §4](../../docs/19-websocket.md)).
    UnknownProtocol,
}

impl Answer {
    const ALL: [Self; 23] = [
        Self::BadHost,
        Self::BadPath,
        Self::BadConnection,
        Self::BadTarget,
        Self::NoRoute,
        Self::NoBackend,
        Self::NoEndpoints,
        Self::Unreachable,
        Self::UpstreamFailed,
        Self::UpstreamTimedOut,
        Self::TooBusy,
        Self::OverShare,
        Self::Exhausted,
        Self::NoRoom,
        Self::Edits,
        Self::BadBody,
        Self::BodyTimedOut,
        Self::QueueFull,
        Self::QueueTimedOut,
        Self::ConnectionAuth,
        Self::DeadlineExceeded,
        Self::Redirected,
        Self::UnknownProtocol,
    ];

    /// The status that is answered with.
    pub(crate) fn status(self) -> StatusCode {
        match self {
            Self::BadHost
            | Self::BadPath
            | Self::BadConnection
            | Self::BadTarget
            | Self::BadBody => StatusCode::BAD_REQUEST,
            Self::NoRoute => StatusCode::NOT_FOUND,
            Self::NoBackend | Self::Edits => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NoEndpoints
            | Self::TooBusy
            | Self::OverShare
            | Self::Exhausted
            | Self::NoRoom
            | Self::QueueFull
            | Self::QueueTimedOut => StatusCode::SERVICE_UNAVAILABLE,
            Self::ConnectionAuth | Self::UnknownProtocol => StatusCode::NOT_IMPLEMENTED,
            Self::DeadlineExceeded | Self::UpstreamTimedOut => StatusCode::GATEWAY_TIMEOUT,
            Self::Unreachable | Self::UpstreamFailed => StatusCode::BAD_GATEWAY,
            Self::BodyTimedOut => StatusCode::REQUEST_TIMEOUT,
            // A redirect's own status, one of five, takes its place.
            Self::Redirected => StatusCode::FOUND,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::BadHost => "bad_host",
            Self::BadPath => "bad_path",
            Self::BadConnection => "bad_connection",
            Self::BadTarget => "bad_target",
            Self::NoRoute => "no_route",
            Self::NoBackend => "no_backend",
            Self::NoEndpoints => "no_endpoints",
            Self::Unreachable => "upstream_unreachable",
            Self::UpstreamFailed => "upstream_failed",
            Self::UpstreamTimedOut => "upstream_timed_out",
            Self::TooBusy => "too_busy",
            Self::OverShare => "over_share",
            Self::Exhausted => "exhausted",
            Self::NoRoom => "no_room",
            Self::Edits => "edits",
            Self::BadBody => "bad_body",
            Self::BodyTimedOut => "body_timed_out",
            Self::QueueFull => "upstream_queue_full",
            Self::QueueTimedOut => "upstream_queue_timeout",
            Self::ConnectionAuth => "connection_auth",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Redirected => "redirected",
            Self::UnknownProtocol => "unknown_protocol",
        }
    }

    /// The status a gRPC call is answered with instead, and what it is told
    /// ([15 §6](../../docs/15-http2-and-grpc.md)). Chosen for the cause, not read back
    /// from the HTTP status, as linkerd chooses: what may be tried again is `UNAVAILABLE`,
    /// a deadline is `DEADLINE_EXCEEDED`, a method nobody serves is `UNIMPLEMENTED` — what
    /// a server says of an unknown method — and a request the gateway cannot make sense of
    /// is `INTERNAL`, as gRPC calls a protocol error.
    pub(crate) fn grpc(self) -> (Code, &'static str) {
        match self {
            Self::BadHost => (Code::Internal, "the call names no host the gateway can use"),
            Self::BadPath => (Code::Internal, "the call's path cannot be routed"),
            Self::BadConnection => (Code::Internal, "the call's head is not well formed"),
            Self::BadTarget => (Code::Internal, "the call's target cannot be sent on"),
            Self::Edits => (Code::Internal, "the call's head could not take its changes"),
            Self::BadBody => (Code::Internal, "the call's messages could not be read"),
            Self::NoRoute => (Code::Unimplemented, "no route serves this method"),
            Self::ConnectionAuth => (
                Code::Unimplemented,
                "connection-bound credentials are not sent over HTTP/2",
            ),
            Self::NoBackend | Self::NoEndpoints => {
                (Code::Unavailable, "no upstream can serve the call")
            }
            Self::Unreachable => (Code::Unavailable, "the upstream could not be reached"),
            Self::UpstreamFailed => (Code::Unavailable, "the upstream failed to answer"),
            Self::TooBusy | Self::NoRoom | Self::QueueFull | Self::QueueTimedOut => (
                Code::Unavailable,
                "the gateway is too busy to take the call",
            ),
            Self::OverShare => (
                Code::Unavailable,
                "the upstream holds its share of the gateway's places",
            ),
            Self::Exhausted => (Code::ResourceExhausted, "the gateway ran out of room"),
            Self::BodyTimedOut => (
                Code::DeadlineExceeded,
                "the call's messages stopped arriving",
            ),
            Self::DeadlineExceeded => (Code::DeadlineExceeded, "the call's deadline passed"),
            Self::UpstreamTimedOut => (
                Code::DeadlineExceeded,
                "the upstream did not answer in time",
            ),
            Self::Redirected => (
                Code::Unimplemented,
                "the route answers with a redirect, which a call cannot follow",
            ),
            Self::UnknownProtocol => (Code::Unimplemented, "no protocol but WebSocket is carried"),
        }
    }
}

impl From<Rejection> for Answer {
    fn from(rejection: Rejection) -> Self {
        match rejection {
            Rejection::Host(_) => Self::BadHost,
            Rejection::Path(_) => Self::BadPath,
            Rejection::Connection(_) => Self::BadConnection,
            Rejection::Target => Self::BadTarget,
            Rejection::NoRoute => Self::NoRoute,
            Rejection::NoBackend => Self::NoBackend,
            Rejection::Edits => Self::Edits,
            Rejection::Protocol => Self::UnknownProtocol,
        }
    }
}

/// What is counted per listener: everything a request counts on its way, in one group.
#[derive(Debug, Default)]
pub(crate) struct ListenerCounters {
    pub(crate) accepted: Counter,
    pub(crate) active: Gauge,
    pub(crate) accept_errors: Counter,
    paused: [Counter; AcceptPause::ALL.len()],
    responses: [Counter; 5],
    answers: [Counter; Answer::ALL.len()],
    /// Connections the gateway closed with requests under way, by why.
    closed: [Counter; Cut::ALL.len()],
    /// Heads refused before the core had a request, by the reasons of
    /// [`RequestError::NAMES`] that are not among [`Answer`]'s.
    refusals: [Counter; RequestError::NAMES.len()],
    head_time: Histogram<14>,
    grpc: [Counter; NAMES.len()],
    quic: [Counter; Quic::ALL.len()],
    tunnels: [Counter; Tunnel::ALL.len()],
    proxy_headers: [Counter; ProxyHeader::ALL.len()],
}

impl ListenerCounters {
    /// A response is on its way to the client, `nanoseconds` after its request came in.
    pub(crate) fn responded(&self, status: StatusCode, nanoseconds: u64) {
        if let Some(class) = self.responses.get(class_of(status)) {
            class.inc();
        }
        self.head_time.observe(&HEAD_TIME_BOUNDS, nanoseconds);
    }

    /// A gRPC call ended with `code`, gRPC's number for it.
    pub(crate) fn called(&self, code: usize) {
        if let Some(counter) = self.grpc.get(code) {
            counter.inc();
        }
    }

    /// The listener's HTTP/3 side did `event` with a datagram.
    pub(crate) fn quic(&self, event: Quic) {
        if let Some(counter) = self.quic.get(event as usize) {
            counter.inc();
        }
    }

    /// The listener stopped accepting, for `why`.
    pub(crate) fn paused(&self, why: AcceptPause) {
        if let Some(counter) = self.paused.get(why as usize) {
            counter.inc();
        }
    }

    /// A tunnel of the listener's ended so.
    pub(crate) fn tunnel(&self, ended: Tunnel) {
        if let Some(counter) = self.tunnels.get(ended as usize) {
            counter.inc();
        }
    }

    /// A connection's PROXY header came to `outcome`.
    pub(crate) fn proxy_header(&self, outcome: ProxyHeader) {
        if let Some(counter) = self.proxy_headers.get(outcome as usize) {
            counter.inc();
        }
    }

    /// A head was refused, `why` being one of [`RequestError::NAMES`], and answered
    /// `status`: one of the gateway's own answers, counted among the responses by class and
    /// among those answers by why — under the core's reason where one has the same name. Not
    /// timed: there was no request.
    pub(crate) fn refused(&self, status: StatusCode, why: &str) {
        if let Some(class) = self.responses.get(class_of(status)) {
            class.inc();
        }
        if let Some(answer) = Answer::ALL.iter().find(|answer| answer.label() == why) {
            self.answered(*answer);
        } else if let Some(position) = RequestError::NAMES.iter().position(|name| *name == why)
            && let Some(counter) = self.refusals.get(position)
        {
            counter.inc();
        }
    }

    /// The gateway closed one of the listener's connections, cutting off what was under way
    /// on it, for `why`.
    pub(crate) fn closed(&self, why: Cut) {
        let position = Cut::ALL.iter().position(|other| *other == why);
        if let Some(counter) = position.and_then(|position| self.closed.get(position)) {
            counter.inc();
        }
    }

    /// The response is one of the data plane's own.
    pub(crate) fn answered(&self, answer: Answer) {
        let position = Answer::ALL.iter().position(|other| *other == answer);
        if let Some(counter) = position.and_then(|position| self.answers.get(position)) {
            counter.inc();
        }
    }
}

/// What is counted per upstream.
#[derive(Debug, Default)]
pub(crate) struct UpstreamCounters {
    pub(crate) requests: Counter,
    responses: [Counter; 5],
    pub(crate) failures: Counter,
    /// Answers whose head was handed on and whose body then failed. Counted apart from
    /// `failures`, which is answers that never arrived: by the time one of these happens
    /// the status has been counted and the client has been told
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    pub(crate) body_failures: Counter,
    /// Requests sent again: because the upstream showed it never processed them (15 §6),
    /// or because a rule's retry said to (03 §6).
    pub(crate) retries: Counter,
    /// Retries a rule's retry wanted that the budget did not allow.
    pub(crate) retries_over_budget: Counter,
    /// Retries a rule's retry wanted for a body not kept whole.
    pub(crate) retries_unkept: Counter,
    /// Retries a rule's retry wanted that would have waited past the request's deadline, found
    /// no place on the worker, or no endpoint to go to.
    pub(crate) retries_deadline: Counter,
    pub(crate) retries_busy: Counter,
    pub(crate) retries_nowhere: Counter,
    /// Copies a mirror to this upstream did not get, by why: no place for them on the
    /// worker, fallen too far behind, no endpoint to send them to, credentials bound to
    /// the client's connection, or a WebSocket handshake, of which a mirror could never be
    /// sent more than the handshake.
    pub(crate) mirrors_busy: Counter,
    pub(crate) mirrors_behind: Counter,
    pub(crate) mirrors_nowhere: Counter,
    pub(crate) mirrors_credentials: Counter,
    pub(crate) mirrors_upgrade: Counter,
    /// Endpoints set aside because a try could not connect to them (03 §6), counted by the
    /// health checker as it finds them.
    pub(crate) set_asides: Counter,
    /// The same, for want of a local port to them.
    pub(crate) set_asides_no_port: Counter,
}

impl UpstreamCounters {
    pub(crate) fn responded(&self, status: StatusCode) {
        if let Some(class) = self.responses.get(class_of(status)) {
            class.inc();
        }
    }
}

/// The position of a status in [`CLASSES`]. `http` has no status below 100 or above 999;
/// what is above 599 counts as 5xx.
fn class_of(status: StatusCode) -> usize {
    usize::from(status.as_u16() / 100).clamp(1, 5) - 1
}

/// All the series of a data plane.
#[derive(Debug)]
pub(crate) struct Metrics {
    shards: NonZeroUsize,
    /// By the position of the listener's socket.
    listeners: Vec<Sharded<ListenerCounters>>,
    /// By slot; a slot is filled when it is given out and stays filled.
    upstreams: Box<[OnceLock<Sharded<UpstreamCounters>>]>,
    /// Which slot an upstream's name has. Only reloads and scrapes come here.
    slots: Mutex<HashMap<String, usize>>,
    pub(crate) reloads: Counter,
    /// Seconds since the Unix epoch; zero before the first reload.
    pub(crate) last_reload: AtomicU64,
    /// Exchanges of EdgeRush's own that ended without an answer, by reason.
    stopped: Sharded<[Counter; Stopped::ALL.len()]>,
    /// What became of the connections a worker used, by which of the three it was.
    connections: Sharded<[Counter; 3]>,
    /// What each worker has in hand, sampled by the worker itself as it sweeps.
    workers: Sharded<WorkerGauges>,
    /// Access-log records dropped, by why: counted by the workers and by the logger.
    pub(crate) logs_dropped: crate::access_log::Dropped,
}

/// What one worker holds at the moment it last looked. Sampled rather than kept up to
/// date on the request path: a gauge is for how much there is now, and a sweep already
/// walks everything this asks about.
#[derive(Debug, Default)]
pub(crate) struct WorkerGauges {
    /// Exchanges in hand, from before a connection is looked for until the answer's body
    /// has been let go of.
    exchanges: Gauge,
    /// Connections the worker is keeping for an upstream to be asked again.
    idle: Gauge,
    /// Bytes of application storage the worker holds (14 §8): its ledger's count.
    storage: Gauge,
}

impl WorkerGauges {
    /// What this worker holds now, `said` being what it last said. A gauge is given the
    /// difference, never set: a thread that is not a worker can take a shard's number
    /// before a worker does and leave two workers in one shard (08 §1).
    pub(crate) fn holding(&self, said: &Said, exchanges: usize, idle: usize, storage: usize) {
        said.exchanges.move_to(&self.exchanges, exchanges);
        said.idle.move_to(&self.idle, idle);
        said.storage.move_to(&self.storage, storage);
    }
}

/// What one worker last said it holds, kept by the worker.
#[derive(Debug, Default)]
pub(crate) struct Said {
    exchanges: SaidOne,
    idle: SaidOne,
    storage: SaidOne,
}

#[derive(Debug, Default)]
struct SaidOne(Cell<i64>);

impl SaidOne {
    /// Moves `gauge` by the difference between `now` and what was said before, and
    /// remembers `now`.
    fn move_to(&self, gauge: &Gauge, now: usize) {
        let now = i64::try_from(now).unwrap_or(i64::MAX);
        gauge.add(now.wrapping_sub(self.0.replace(now)));
    }
}

impl Metrics {
    pub(crate) fn new(shards: NonZeroUsize, listeners: usize) -> Self {
        Self {
            shards,
            listeners: (0..listeners).map(|_| Sharded::new(shards)).collect(),
            upstreams: (0..UPSTREAM_SLOTS).map(|_| OnceLock::new()).collect(),
            slots: Mutex::default(),
            reloads: Counter::default(),
            last_reload: AtomicU64::new(0),
            stopped: Sharded::new(shards),
            connections: Sharded::new(shards),
            workers: Sharded::new(shards),
            logs_dropped: Arc::new(Sharded::new(shards)),
        }
    }

    /// Counts an exchange of our own that ended without an answer.
    pub(crate) fn stopped(&self, why: Stopped) {
        if let Some(counter) = self.stopped.local().get(why as usize) {
            counter.inc();
        }
    }

    /// Counts what became of a connection.
    pub(crate) fn socket(&self, what: Socket) {
        if let Some(counter) = self.connections.local().get(what as usize) {
            counter.inc();
        }
    }

    /// This worker's gauges, for it to say what it is holding.
    pub(crate) fn worker(&self) -> &WorkerGauges {
        self.workers.local()
    }

    /// This thread's shard of a listener's counters.
    pub(crate) fn listener(&self, socket: usize) -> Option<&ListenerCounters> {
        self.listeners.get(socket).map(Sharded::local)
    }

    /// The connections open on every listener, over TCP and QUIC alike, every worker's.
    pub(crate) fn open_connections(&self) -> usize {
        let open: i64 = self
            .listeners
            .iter()
            .map(|series| series.sum(|shard| shard.active.get()))
            .sum();
        usize::try_from(open).unwrap_or(0)
    }

    /// This thread's shard of the counters in an upstream's slot.
    pub(crate) fn upstream(&self, slot: usize) -> Option<&UpstreamCounters> {
        self.upstreams.get(slot)?.get().map(Sharded::local)
    }

    /// The slot of the upstream of that name, which it keeps for good. For reloads: it
    /// takes a lock and may allocate.
    pub(crate) fn upstream_slot(&self, name: &str) -> usize {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = match slots.get(name) {
            Some(slot) => *slot,
            None => {
                // Slot zero is the one that is shared, so names start at one.
                let next = slots.len() + 1;
                if next >= self.upstreams.len() {
                    OVERFLOW_SLOT
                } else {
                    slots.insert(name.to_owned(), next);
                    next
                }
            }
        };
        if let Some(series) = self.upstreams.get(slot) {
            series.get_or_init(|| Sharded::new(self.shards));
        }
        slot
    }

    /// The scrape: the listeners by their names, and the upstreams of the current config
    /// by theirs, each with the slot it has; and, for the upstreams of the current config,
    /// how many of their endpoints pass their checks and how many are set aside.
    pub(crate) fn render(
        &self,
        listeners: &[String],
        upstreams: &[(&str, usize)],
        endpoints: &[(&str, usize, usize)],
    ) -> String {
        let mut scrape = Exposition::new();
        let listeners = || listeners.iter().zip(&self.listeners);

        let name = "edgerush_listener_connections_accepted_total";
        scrape.family(name, Kind::Counter, "Connections accepted.");
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.accepted.get()));
        }
        let name = "edgerush_listener_connections_active";
        scrape.family(name, Kind::Gauge, "Connections that are open.");
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.active.get()));
        }
        let name = "edgerush_listener_accept_errors_total";
        scrape.family(
            name,
            Kind::Counter,
            "Connections that could not be accepted.",
        );
        for (listener, series) in listeners() {
            let labels = [("listener", listener.as_str())];
            scrape.sample(name, &labels, series.sum(|shard| shard.accept_errors.get()));
        }
        let name = "edgerush_listener_accept_paused_total";
        let help = "Times the listener stopped accepting, its next connections left in the \
                    kernel's backlog: for holding its fair share of the workers' room, or \
                    for a worker at its cap.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for why in AcceptPause::ALL {
                let labels = [("listener", listener.as_str()), ("reason", why.name())];
                let count = |shard: &ListenerCounters| {
                    shard.paused.get(why as usize).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_responses_total";
        let help = "Responses sent, the upstreams' and the gateway's own, by status class.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, class) in CLASSES.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("class", class)];
                let count = |shard: &ListenerCounters| {
                    shard.responses.get(position).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_local_answers_total";
        let help = "Responses that are the gateway's own, by the reason for them.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, answer) in Answer::ALL.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("reason", answer.label())];
                let count =
                    |shard: &ListenerCounters| shard.answers.get(position).map_or(0, Counter::get);
                scrape.sample(name, &labels, series.sum(count));
            }
            for (position, why) in RequestError::NAMES.iter().enumerate() {
                // Counted under the core's reason of the same name.
                if Answer::ALL.iter().any(|answer| answer.label() == *why) {
                    continue;
                }
                let labels = [("listener", listener.as_str()), ("reason", *why)];
                let count =
                    |shard: &ListenerCounters| shard.refusals.get(position).map_or(0, Counter::get);
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_connections_closed_total";
        let help = "HTTP/2 and HTTP/3 connections the gateway closed with what was under way \
                    on them, by why: drained at the drain's bound, for the worker's storage \
                    running out (exhausted), or for their clients' resets.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, why) in Cut::ALL.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("reason", why.label())];
                let count =
                    |shard: &ListenerCounters| shard.closed.get(position).map_or(0, Counter::get);
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_grpc_calls_total";
        let help = "gRPC calls ended, by the status they ended with: the upstream's, or \
                    the gateway's own.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for (position, status) in NAMES.iter().enumerate() {
                let labels = [("listener", listener.as_str()), ("status", status)];
                let count =
                    |shard: &ListenerCounters| shard.grpc.get(position).map_or(0, Counter::get);
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_quic_datagrams_total";
        let help = "HTTP/3 datagrams not simply handed to their connection: forwarded to the \
                    worker that owns it, dropped for a full inbox, answered with a Retry or a \
                    version negotiation, or an Initial dropped for no room among its worker's \
                    connections.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for event in Quic::ALL {
                let labels = [("listener", listener.as_str()), ("event", event.name())];
                let count = |shard: &ListenerCounters| {
                    shard.quic.get(event as usize).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_tunnels_total";
        let help = "Connections of tcp and tls listeners, by how they ended: carried to \
                    their close, closed idle or at the drain's bound, failed part way, or \
                    never carried.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for ended in Tunnel::ALL {
                let labels = [("listener", listener.as_str()), ("outcome", ended.name())];
                let count = |shard: &ListenerCounters| {
                    shard.tunnels.get(ended as usize).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_proxy_headers_total";
        let help = "PROXY protocol headers read at the start of connections, by what came of \
                    them: a sender's used, or left the connection's own ends in use; a \
                    stranger's dropped; or none taken, the connection closed.";
        scrape.family(name, Kind::Counter, help);
        for (listener, series) in listeners() {
            for outcome in ProxyHeader::ALL {
                let labels = [("listener", listener.as_str()), ("outcome", outcome.name())];
                let count = |shard: &ListenerCounters| {
                    shard
                        .proxy_headers
                        .get(outcome as usize)
                        .map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_listener_time_to_response_head_seconds";
        let help = "From a request's head coming in to its response's head going out; \
                    bodies stream beyond it.";
        scrape.family(name, Kind::Histogram, help);
        for (listener, series) in listeners() {
            // Added up bucket by bucket over the shards.
            let mut counts = [0_u64; HEAD_TIME_BOUNDS.len() + 1];
            for shard in series.shards() {
                for (total, count) in counts.iter_mut().zip(shard.head_time.counts()) {
                    *total = total.wrapping_add(count);
                }
            }
            scrape.histogram(
                name,
                &[("listener", listener.as_str())],
                HEAD_TIME_BOUNDS.map(seconds),
                counts,
                seconds(series.sum(|shard| shard.head_time.sum())),
            );
        }

        // Upstreams that share a series are shown once, under a name of its own.
        let mut shown: Vec<(&str, usize)> = Vec::new();
        for (upstream, slot) in upstreams {
            let upstream = if *slot == OVERFLOW_SLOT {
                OVERFLOW_NAME
            } else {
                upstream
            };
            if !shown.contains(&(upstream, *slot)) {
                shown.push((upstream, *slot));
            }
        }
        let upstreams = || {
            shown
                .iter()
                .filter_map(|(upstream, slot)| Some((*upstream, self.upstreams.get(*slot)?.get()?)))
        };
        let name = "edgerush_upstream_requests_total";
        scrape.family(name, Kind::Counter, "Requests sent to the upstream.");
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.requests.get()));
        }
        let name = "edgerush_upstream_responses_total";
        scrape.family(
            name,
            Kind::Counter,
            "Responses of the upstream, by status class.",
        );
        for (upstream, series) in upstreams() {
            for (position, class) in CLASSES.iter().enumerate() {
                let labels = [("upstream", upstream), ("class", class)];
                let count = |shard: &UpstreamCounters| {
                    shard.responses.get(position).map_or(0, Counter::get)
                };
                scrape.sample(name, &labels, series.sum(count));
            }
        }
        let name = "edgerush_upstream_failures_total";
        let help = "Requests the upstream did not answer: not reached, or not in HTTP.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.failures.get()));
        }

        let name = "edgerush_upstream_body_failures_total";
        let help = "Answers whose head was handed on and whose body then failed.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.body_failures.get()));
        }

        let name = "edgerush_upstream_retries_total";
        let help = "Requests sent again: never processed by the upstream, or a rule's retry.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            let labels = [("upstream", upstream)];
            scrape.sample(name, &labels, series.sum(|shard| shard.retries.get()));
        }

        let name = "edgerush_upstream_retries_refused_total";
        let help = "Retries a rule's retry wanted and did not get, by why.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            for (reason, count) in [
                (
                    "budget",
                    series.sum(|shard| shard.retries_over_budget.get()),
                ),
                ("body", series.sum(|shard| shard.retries_unkept.get())),
                ("deadline", series.sum(|shard| shard.retries_deadline.get())),
                ("busy", series.sum(|shard| shard.retries_busy.get())),
                ("nowhere", series.sum(|shard| shard.retries_nowhere.get())),
            ] {
                scrape.sample(name, &[("upstream", upstream), ("reason", reason)], count);
            }
        }

        let name = "edgerush_upstream_mirrors_given_up_total";
        let help = "Copies of requests a mirror to the upstream did not send or finish, by why.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            for (reason, count) in [
                ("busy", series.sum(|shard| shard.mirrors_busy.get())),
                ("behind", series.sum(|shard| shard.mirrors_behind.get())),
                ("nowhere", series.sum(|shard| shard.mirrors_nowhere.get())),
                (
                    "credentials",
                    series.sum(|shard| shard.mirrors_credentials.get()),
                ),
                ("upgrade", series.sum(|shard| shard.mirrors_upgrade.get())),
            ] {
                scrape.sample(name, &[("upstream", upstream), ("reason", reason)], count);
            }
        }

        let name = "edgerush_upstream_set_asides_total";
        let help = "Endpoints set aside because a try could not connect to them, by why: \
                    the connect failed, or no local port was free to them.";
        scrape.family(name, Kind::Counter, help);
        for (upstream, series) in upstreams() {
            for (reason, count) in [
                ("connect", series.sum(|shard| shard.set_asides.get())),
                (
                    "no_port",
                    series.sum(|shard| shard.set_asides_no_port.get()),
                ),
            ] {
                scrape.sample(name, &[("upstream", upstream), ("reason", reason)], count);
            }
        }

        let name = "edgerush_upstream_healthy_endpoints";
        let help = "Endpoints the health checks, if any, say serve.";
        scrape.family(name, Kind::Gauge, help);
        for (upstream, serving, _) in endpoints {
            scrape.sample(name, &[("upstream", upstream)], *serving as u64);
        }

        let name = "edgerush_upstream_set_aside_endpoints";
        let help = "Endpoints set aside until a connect probe gets through.";
        scrape.family(name, Kind::Gauge, help);
        for (upstream, _, aside) in endpoints {
            scrape.sample(name, &[("upstream", upstream)], *aside as u64);
        }

        let name = "edgerush_upstream_exchanges_stopped_total";
        let help = "Exchanges by EdgeRush's own path that ended without an answer.";
        scrape.family(name, Kind::Counter, help);
        for why in Stopped::ALL {
            let labels = [("reason", why.name())];
            let count = |shard: &[Counter; Stopped::ALL.len()]| shard[why as usize].get();
            scrape.sample(name, &labels, self.stopped.sum(count));
        }

        let name = "edgerush_upstream_connections_total";
        let help = "Connections to upstreams, by what became of each.";
        scrape.family(name, Kind::Counter, help);
        for what in Socket::ALL {
            let labels = [("state", what.name())];
            let count = |shard: &[Counter; 3]| shard[what as usize].get();
            scrape.sample(name, &labels, self.connections.sum(count));
        }

        let name = "edgerush_upstream_exchanges_active";
        let help = "Exchanges a worker has in hand, as its last sweep found them.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(
            name,
            &[],
            self.workers
                .sum(|shard| shard.exchanges.get().max(0).cast_unsigned()),
        );
        let name = "edgerush_upstream_connections_idle";
        let help = "Connections a worker is keeping, as its last sweep found them.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(
            name,
            &[],
            self.workers
                .sum(|shard| shard.idle.get().max(0).cast_unsigned()),
        );
        let name = "edgerush_worker_storage_bytes";
        let help =
            "Bytes of application storage the workers hold, as their last sweeps found them.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(
            name,
            &[],
            self.workers
                .sum(|shard| shard.storage.get().max(0).cast_unsigned()),
        );

        let name = "edgerush_access_log_dropped_total";
        let help = "Access-log records dropped, by why.";
        scrape.family(name, Kind::Counter, help);
        for why in LogsDropped::ALL {
            let labels = [("reason", why.name())];
            let count = |shard: &[Counter; LogsDropped::ALL.len()]| shard[why as usize].get();
            scrape.sample(name, &labels, self.logs_dropped.sum(count));
        }

        let name = "edgerush_config_reloads_total";
        scrape.family(name, Kind::Counter, "Configs taken over while running.");
        scrape.sample(name, &[], self.reloads.get());
        let name = "edgerush_config_last_reload_timestamp_seconds";
        let help = "When the last config was taken over; zero if none has been.";
        scrape.family(name, Kind::Gauge, help);
        scrape.sample(name, &[], self.last_reload.load(Ordering::Relaxed));
        scrape.finish()
    }
}

/// Nanoseconds as seconds, for writing out. What is lost beyond 2⁵³ does not show.
fn seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Metrics {
        Metrics::new(NonZeroUsize::new(4).unwrap(), 2)
    }

    #[test]
    fn a_status_is_counted_in_its_class() {
        for (status, class) in [
            (100, 0),
            (200, 1),
            (204, 1),
            (301, 2),
            (404, 3),
            (599, 4),
            (999, 4),
        ] {
            assert_eq!(
                class_of(StatusCode::from_u16(status).unwrap()),
                class,
                "{status}"
            );
        }
    }

    #[test]
    fn every_rejection_of_the_core_is_an_answer_with_the_same_status() {
        use crate::{ConnectionError, HostError};
        use edgerush_router::NormaliseError;
        for rejection in [
            Rejection::Host(HostError::Missing),
            Rejection::Path(NormaliseError::Backslash),
            Rejection::Connection(ConnectionError::Malformed),
            Rejection::Target,
            Rejection::NoRoute,
            Rejection::NoBackend,
            Rejection::Edits,
        ] {
            assert_eq!(
                Answer::from(rejection).status(),
                rejection.status(),
                "{rejection:?}"
            );
        }
        assert_eq!(Answer::NoEndpoints.status(), 503);
        assert_eq!(Answer::UpstreamFailed.status(), 502);
    }

    /// A try that ran out of time is a gateway timeout, and a call's deadline exceeded: not
    /// the `UNAVAILABLE` of an upstream that could not be reached, which is what would be
    /// sent again.
    #[test]
    fn a_try_that_ran_out_of_time_is_a_504() {
        assert_eq!(Answer::UpstreamTimedOut.status(), 504);
        assert_eq!(Answer::UpstreamTimedOut.label(), "upstream_timed_out");
        assert_eq!(Answer::UpstreamTimedOut.grpc().0, Code::DeadlineExceeded);
    }

    #[test]
    fn answers_have_labels_of_their_own() {
        let mut labels: Vec<&str> = Answer::ALL.iter().map(|answer| answer.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), Answer::ALL.len());
    }

    #[test]
    fn an_upstream_keeps_the_slot_its_name_was_given() {
        let metrics = metrics();
        let cart = metrics.upstream_slot("cart");
        let pages = metrics.upstream_slot("pages");
        assert_ne!(cart, pages);
        assert_ne!(cart, OVERFLOW_SLOT);
        assert_eq!(metrics.upstream_slot("cart"), cart);
        assert!(metrics.upstream(cart).is_some());
        assert!(metrics.upstream(UPSTREAM_SLOTS - 1).is_none());
        assert!(metrics.upstream(UPSTREAM_SLOTS).is_none());
    }

    #[test]
    fn upstreams_beyond_the_slots_share_one_series() {
        let metrics = metrics();
        let slots: Vec<usize> = (0..UPSTREAM_SLOTS + 10)
            .map(|upstream| metrics.upstream_slot(&format!("upstream-{upstream}")))
            .collect();
        let own = slots.iter().filter(|slot| **slot != OVERFLOW_SLOT).count();
        assert_eq!(own, UPSTREAM_SLOTS - 1);
        assert_eq!(slots.last(), Some(&OVERFLOW_SLOT));
        // The names that came first still have their own.
        assert_eq!(metrics.upstream_slot("upstream-0"), slots[0]);

        metrics.upstream(OVERFLOW_SLOT).unwrap().requests.inc();
        let scrape = metrics.render(
            &[],
            &[("late-a", OVERFLOW_SLOT), ("late-b", OVERFLOW_SLOT)],
            &[],
        );
        let line = "edgerush_upstream_requests_total{upstream=\"_overflow\"} 1\n";
        assert_eq!(scrape.matches(line).count(), 1, "{scrape}");
        assert!(!scrape.contains("late-a"), "{scrape}");
    }

    #[test]
    fn a_scrape_shows_what_was_counted() {
        let metrics = metrics();
        let listeners = ["admin".to_owned(), "web".to_owned()];
        let cart = metrics.upstream_slot("cart");

        let web = metrics.listener(1).unwrap();
        web.accepted.inc();
        web.active.inc();
        web.responded(StatusCode::OK, 700_000);
        web.responded(StatusCode::NOT_FOUND, 90_000);
        web.answered(Answer::NoRoute);
        web.quic(Quic::Forwarded);
        web.quic(Quic::Forwarded);
        web.quic(Quic::Retry);
        let upstream = metrics.upstream(cart).unwrap();
        upstream.requests.inc();
        upstream.responded(StatusCode::OK);
        metrics.reloads.inc();

        let scrape = metrics.render(&listeners, &[("cart", cart)], &[]);
        for line in [
            "# TYPE edgerush_listener_responses_total counter\n",
            "edgerush_listener_connections_accepted_total{listener=\"web\"} 1\n",
            "edgerush_listener_connections_accepted_total{listener=\"admin\"} 0\n",
            "edgerush_listener_connections_active{listener=\"web\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"4xx\"} 1\n",
            "edgerush_listener_responses_total{listener=\"web\",class=\"5xx\"} 0\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"no_route\"} 1\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"bad_host\"} 0\n",
            "edgerush_listener_quic_datagrams_total{listener=\"web\",event=\"forwarded\"} 2\n",
            "edgerush_listener_quic_datagrams_total{listener=\"web\",event=\"inbox_full\"} 0\n",
            "edgerush_listener_quic_datagrams_total{listener=\"web\",event=\"retry\"} 1\n",
            "edgerush_listener_quic_datagrams_total{listener=\"admin\",event=\"version_negotiation\"} 0\n",
            "edgerush_listener_quic_datagrams_total{listener=\"web\",event=\"no_room\"} 0\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"0.0005\"} 1\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"0.001\"} 2\n",
            "edgerush_listener_time_to_response_head_seconds_bucket{listener=\"web\",le=\"+Inf\"} 2\n",
            "edgerush_listener_time_to_response_head_seconds_sum{listener=\"web\"} 0.00079\n",
            "edgerush_listener_time_to_response_head_seconds_count{listener=\"web\"} 2\n",
            "edgerush_upstream_requests_total{upstream=\"cart\"} 1\n",
            "edgerush_upstream_responses_total{upstream=\"cart\",class=\"2xx\"} 1\n",
            "edgerush_upstream_failures_total{upstream=\"cart\"} 0\n",
            "edgerush_config_reloads_total 1\n",
            "edgerush_config_last_reload_timestamp_seconds 0\n",
        ] {
            assert!(scrape.contains(line), "{line} is not in\n{scrape}");
        }
    }

    #[test]
    fn what_is_counted_on_other_threads_shows_too() {
        let metrics = std::sync::Arc::new(metrics());
        let threads: Vec<_> = (0..6)
            .map(|_| {
                let metrics = std::sync::Arc::clone(&metrics);
                std::thread::spawn(move || {
                    metrics
                        .listener(0)
                        .unwrap()
                        .responded(StatusCode::OK, 1_000);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let scrape = metrics.render(&["web".to_owned()], &[], &[]);
        let line = "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 6\n";
        assert!(scrape.contains(line), "{scrape}");
    }

    /// What every worker holds adds up on a scrape even when two workers count in one
    /// shard, which a data plane gets when a thread that is not a worker (the health
    /// checker counting a set-aside, the logger an unwritten record) counts before every
    /// worker has. Each worker sweeps twice, saying less the second time.
    #[test]
    fn what_workers_hold_adds_up_even_when_two_share_a_shard() {
        use std::collections::HashSet;
        let metrics = Arc::new(Metrics::new(NonZeroUsize::new(2).unwrap(), 1));
        // A worker on a thread of its own: says what it holds, and in which shard.
        let sweeps = |exchanges: usize, storage: usize| {
            let metrics = Arc::clone(&metrics);
            std::thread::spawn(move || {
                let gauges = metrics.worker();
                let said = Said::default();
                gauges.holding(&said, exchanges + 2, 0, storage * 2);
                gauges.holding(&said, exchanges, 0, storage);
                std::ptr::from_ref(gauges) as usize
            })
            .join()
            .unwrap()
        };
        // Workers until two have counted in one shard, which two of any three do here.
        let (mut shards, mut exchanges, mut storage) = (Vec::new(), 0, 0);
        for (held, bytes) in [(3, 1_000), (5, 2_000), (7, 4_000)] {
            shards.push(sweeps(held, bytes));
            exchanges += held;
            storage += bytes;
            if shards.iter().collect::<HashSet<_>>().len() < shards.len() {
                break;
            }
        }
        assert!(shards.iter().collect::<HashSet<_>>().len() < shards.len());
        let scrape = metrics.render(&["web".to_owned()], &[], &[]);
        let sample = |name: &str| {
            scrape
                .lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
                .unwrap_or("absent")
                .to_owned()
        };
        assert_eq!(
            (
                sample("edgerush_upstream_exchanges_active"),
                sample("edgerush_worker_storage_bytes")
            ),
            (exchanges.to_string(), storage.to_string()),
            "{} workers, shards {shards:?}",
            shards.len()
        );
    }

    /// A refused head is counted among the gateway's own answers by why; a reason the core's
    /// answers have too is one series, not two, which a scrape would refuse.
    #[test]
    fn a_refused_head_is_one_of_the_gateway_s_own_answers() {
        let metrics = metrics();
        let counters = metrics.listener(0).unwrap();
        counters.refused(StatusCode::BAD_REQUEST, "repeated_length");
        counters.refused(StatusCode::BAD_REQUEST, "bad_connection");
        counters.answered(Answer::BadConnection);
        counters.refused(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, "head_too_long");
        let scrape = metrics.render(&["web".to_owned(), "api".to_owned()], &[], &[]);
        for line in [
            "edgerush_listener_responses_total{listener=\"web\",class=\"4xx\"} 3\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"repeated_length\"} 1\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"bad_connection\"} 2\n",
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"head_too_long\"} 1\n",
            "edgerush_listener_local_answers_total{listener=\"api\",reason=\"head_too_long\"} 0\n",
        ] {
            assert!(scrape.contains(line), "{line}");
        }
        let series: Vec<&str> = scrape
            .lines()
            .filter(|line| line.starts_with("edgerush_listener_local_answers_total{"))
            .filter_map(|line| line.rsplit_once(' ').map(|(series, _)| series))
            .collect();
        let distinct: std::collections::HashSet<_> = series.iter().collect();
        assert_eq!(distinct.len(), series.len(), "a series twice");
    }

    #[test]
    fn a_listener_that_stops_accepting_is_counted_by_why() {
        let metrics = metrics();
        let web = metrics.listener(0).unwrap();
        web.paused(AcceptPause::Share);
        web.paused(AcceptPause::Share);
        web.paused(AcceptPause::WorkerCap);
        let scrape = metrics.render(&["web".to_owned()], &[], &[]);
        for line in [
            "edgerush_listener_accept_paused_total{listener=\"web\",reason=\"share\"} 2
",
            "edgerush_listener_accept_paused_total{listener=\"web\",reason=\"worker_cap\"} 1
",
        ] {
            assert!(scrape.contains(line), "{scrape}");
        }
    }

    #[test]
    fn proxy_headers_are_counted_by_what_came_of_them() {
        let metrics = metrics();
        let web = metrics.listener(0).unwrap();
        web.proxy_header(ProxyHeader::Accepted);
        web.proxy_header(ProxyHeader::Accepted);
        web.proxy_header(ProxyHeader::Missing);
        let scrape = metrics.render(&["web".to_owned()], &[], &[]);
        for (outcome, count) in [
            ("accepted", 2),
            ("local", 0),
            ("untrusted", 0),
            ("malformed", 0),
            ("missing", 1),
            ("closed", 0),
            ("too_slow", 0),
        ] {
            let line = format!(
                "edgerush_listener_proxy_headers_total{{listener=\"web\",outcome=\"{outcome}\"}} {count}\n"
            );
            assert!(scrape.contains(&line), "{scrape}");
        }
    }

    #[test]
    fn an_upstream_that_the_config_no_longer_has_is_not_shown_and_not_forgotten() {
        let metrics = metrics();
        let cart = metrics.upstream_slot("cart");
        metrics.upstream(cart).unwrap().requests.add(5);
        assert!(!metrics.render(&[], &[], &[]).contains("cart"));
        let again = metrics.upstream_slot("cart");
        let scrape = metrics.render(&[], &[("cart", again)], &[]);
        assert!(scrape.contains("edgerush_upstream_requests_total{upstream=\"cart\"} 5\n"));
    }
}
