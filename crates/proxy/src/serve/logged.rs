//! A request's access-log record while the request is served
//! ([21 §4](../../docs/21-access-logs.md)): what is known at its head, kept here, and what
//! comes later filled in by whoever learns it — routing, each try, the answer and its body.
//! Made only for a request of a listener that logs.
//!
//! It is written when the last of those that hold it lets go: the answer's body, once the
//! server is done with it, or the request itself, given up with no answer. Whoever that is,
//! nothing has to remember to write it, and a request cannot end without its record.

use super::{Body, BodyError, Snapshot, Worker};
use crate::access_log::Sink;
use crate::downstream::h1::connection::{self as h1, Answered};
use crate::forwarding::{Client, FORWARDED_FOR};
use crate::head::Forwarded;
use crate::request_body::Counts;
use edgerush_filters::forwarding::client_address;
use edgerush_router::Fields;
use edgerush_telemetry::access_log::{Kind, Protocol, Record};
use http::{HeaderValue, Version};
use http_body::Body as HttpBody;
use std::cell::{Cell, RefCell};
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Where one of the record's strings is in [`Logging::texts`].
#[derive(Debug, Clone, Copy)]
struct Span {
    from: usize,
    to: usize,
}

/// A request's record while it is served.
pub(super) struct Logging {
    worker: Rc<Worker>,
    sink: Sink,
    time_ms: u64,
    came_in: Instant,
    client: IpAddr,
    peer: Option<SocketAddr>,
    protocol: Option<Protocol>,
    /// The record's strings one after another, copied while they are in hand: those of the
    /// head when it arrives, the route's and upstream's names when it is routed.
    texts: RefCell<String>,
    id: Cell<Option<Span>>,
    listener: Span,
    method: Span,
    host: Option<Span>,
    path: Option<Span>,
    route: Cell<Option<Span>>,
    rule: Cell<Option<usize>>,
    upstream: Cell<Option<Span>>,
    endpoint: Cell<Option<SocketAddr>>,
    tries: Cell<u32>,
    upstream_us: Cell<Option<u64>>,
    status: Cell<Option<u16>>,
    reason: Cell<Option<&'static str>>,
    received: Cell<u64>,
    sent: Cell<u64>,
    /// Whether the answer's body was gone through to its end, or had nothing to go through.
    whole: Cell<bool>,
}

impl Logging {
    /// A record for a request that came in on `listener` now, if the listener logs: what
    /// its head says, kept before routing changes any of it.
    pub(super) fn start<H: Forwarded>(
        worker: &Rc<Worker>,
        snapshot: &Snapshot,
        listener: usize,
        client: &Client,
        head: &H,
    ) -> Option<Rc<Self>> {
        let came_in = Instant::now();
        let sink = (*snapshot.logs.get(listener)?)?;
        let compiled = snapshot.listener(listener)?;
        let mut texts = String::with_capacity(160);
        let mut keep = |text: &str| {
            let from = texts.len();
            texts.push_str(text);
            Span {
                from,
                to: texts.len(),
            }
        };
        let listener = keep(&compiled.name);
        let method = keep(head.method().as_str());
        let host = head
            .uri()
            .authority()
            .map(http::uri::Authority::as_str)
            .or_else(|| head.host_field().ok())
            .map(&mut keep);
        let path = head
            .uri()
            .path_and_query()
            .map(http::uri::PathAndQuery::as_str)
            .map(&mut keep);
        // Who the client is, as forwarding works it out: whom a trusted proxy names
        // (03 §11).
        let trusted = &compiled.forwarding.trusted_proxies;
        let address = if trusted.trusts(client.address()) {
            client_address(
                client.address(),
                head.fields().values(&FORWARDED_FOR),
                trusted,
            )
        } else {
            client.address()
        };
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH);
        let time_ms = since_epoch.map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
        Some(Rc::new(Self {
            worker: Rc::clone(worker),
            sink,
            time_ms,
            came_in,
            client: address,
            peer: client.peer(),
            protocol: protocol(head.version()),
            texts: RefCell::new(texts),
            id: Cell::new(None),
            listener,
            method,
            host,
            path,
            route: Cell::new(None),
            rule: Cell::new(None),
            upstream: Cell::new(None),
            endpoint: Cell::new(None),
            tries: Cell::new(0),
            upstream_us: Cell::new(None),
            status: Cell::new(None),
            reason: Cell::new(None),
            received: Cell::new(0),
            sent: Cell::new(0),
            whole: Cell::new(false),
        }))
    }

    /// Notes the request's ID, once it has one.
    pub(super) fn identified(&self, id: Option<&HeaderValue>) {
        let (Some(id), Ok(mut texts)) = (
            id.and_then(|id| id.to_str().ok()),
            self.texts.try_borrow_mut(),
        ) else {
            return;
        };
        let from = texts.len();
        texts.push_str(id);
        self.id.set(Some(Span {
            from,
            to: texts.len(),
        }));
    }

    /// Notes where the request was routed: its route's name, the rule's position in it,
    /// and the upstream's name, if it has one.
    pub(super) fn routed(&self, route: &str, rule: usize, upstream: Option<&str>) {
        let Ok(mut texts) = self.texts.try_borrow_mut() else {
            return;
        };
        let mut keep = |text: &str| {
            let from = texts.len();
            texts.push_str(text);
            Span {
                from,
                to: texts.len(),
            }
        };
        self.route.set(Some(keep(route)));
        self.upstream.set(upstream.map(&mut keep));
        self.rule.set(Some(rule));
    }

    /// Notes a try, to `endpoint`.
    pub(super) fn tried(&self, endpoint: SocketAddr) {
        self.tries.set(self.tries.get() + 1);
        self.endpoint.set(Some(endpoint));
    }

    /// Notes that the upstream's answer head has arrived, for the last try that got one.
    pub(super) fn upstream_answered(&self) {
        self.upstream_us.set(Some(microseconds(self.came_in)));
    }

    /// Notes the answer: its status, and why the gateway gave it, if the answer is its own.
    pub(super) fn answered(&self, answered: &Answered<Body>) {
        self.status.set(Some(answered.status().as_u16()));
        if let Answered::Map(response) = answered
            && let Some(h1::Local(why)) = response.extensions().get::<h1::Local>()
        {
            self.reason.set(Some(why.label()));
        }
    }

    fn sent(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.sent.set(self.sent.get().saturating_add(bytes));
    }

    fn failed(&self, error: &BodyError) {
        let why = match error {
            BodyError::DeadlinePassed => "deadline_exceeded",
            BodyError::Ours(_) | BodyError::H2(_) => "upstream_failed",
        };
        self.reason.set(Some(why));
    }
}

impl Counts for Logging {
    fn received(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.received.set(self.received.get().saturating_add(bytes));
    }
}

impl Drop for Logging {
    fn drop(&mut self) {
        let texts = self.texts.get_mut();
        let text = |span: Span| texts.get(span.from..span.to);
        // A client that got no answer, or not all of one, left before it could: unless
        // something else is known to have ended it.
        let reason = self
            .reason
            .get()
            .or_else(|| (!self.whole.get()).then_some("client_closed"));
        let record = Record {
            time_ms: self.time_ms,
            kind: Kind::Request,
            id: self.id.get().and_then(text),
            listener: text(self.listener).unwrap_or_default(),
            client: Some(self.client),
            peer: self.peer,
            protocol: self.protocol,
            method: text(self.method),
            host: self.host.and_then(text),
            path: self.path.and_then(text),
            status: self.status.get(),
            reason,
            route: self.route.get().and_then(text),
            rule: self.rule.get(),
            upstream: self.upstream.get().and_then(text),
            endpoint: self.endpoint.get(),
            tries: Some(self.tries.get()).filter(|tries| *tries > 0),
            grpc_status: None,
            bytes_in: Some(self.received.get()),
            bytes_out: Some(self.sent.get()),
            duration_us: Some(microseconds(self.came_in)),
            upstream_us: self.upstream_us.get(),
        };
        let worker = &self.worker;
        worker
            .batches
            .record(&worker.proxy.logs, self.sink, |out| record.write(out));
    }
}

/// An answer's body, with the record it ends: what goes of it is counted, and how it ended
/// noted, as the server takes it.
pub(super) struct Logged {
    pub(super) body: Body,
    logging: Rc<Logging>,
}

impl Logged {
    /// What a server takes of the body, counted.
    pub(super) fn poll_frame(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<bytes::Bytes>, BodyError>>> {
        let polled = std::pin::Pin::new(&mut self.body).poll_frame(context);
        match &polled {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.logging.sent(data.len());
                }
            }
            std::task::Poll::Ready(Some(Err(error))) => self.logging.failed(error),
            std::task::Poll::Ready(None) => self.logging.whole.set(true),
            std::task::Poll::Pending => {}
        }
        // A body of known length is over at its last frame, and its server need not ask
        // again.
        if self.body.is_end_stream() {
            self.logging.whole.set(true);
        }
        polled
    }
}

impl Drop for Logged {
    fn drop(&mut self) {
        // An answer with nothing to send, whose server never asked it for anything.
        if self.body.is_end_stream() {
            self.logging.whole.set(true);
        }
    }
}

/// `answered`, its body counted for `logging`.
pub(super) fn logged(answered: Answered<Body>, logging: Rc<Logging>) -> Answered<Body> {
    let wrap = |body: Body| Body::Logged(Box::new(Logged { body, logging }));
    match answered {
        Answered::Raw(answer, body) => Answered::Raw(answer, wrap(body)),
        Answered::Map(response) => Answered::Map(response.map(wrap)),
    }
}

fn protocol(version: Version) -> Option<Protocol> {
    match version {
        Version::HTTP_10 => Some(Protocol::Http10),
        Version::HTTP_11 => Some(Protocol::Http11),
        Version::HTTP_2 => Some(Protocol::Http2),
        Version::HTTP_3 => Some(Protocol::Http3),
        _ => None,
    }
}

fn microseconds(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_micros()).unwrap_or(u64::MAX)
}
