//! A request's access-log record while the request is served
//! ([21 §4](../../docs/21-access-logs.md)): what is known at its head, kept here, and what
//! comes later filled in by whoever learns it — routing, each try, the answer and its body.
//! Made only for a request of a listener that logs.
//!
//! It is written when the last of those that hold it lets go: the answer's body, once the
//! server is done with it, or the request itself, given up with no answer. Whoever that is,
//! nothing has to remember to write it, and a request cannot end without its record. A
//! WebSocket's handshake has two: the first once its answer has gone, and the second when
//! its tunnel, which holds it too, ends.
//!
//! A `tcp` or `tls` listener's connection has a record of its own kind, [`Passing`], which
//! the one task that carries it fills in and writes.

use super::{Body, BodyError, Snapshot, Worker};
use crate::access_log::Sink;
use crate::downstream::h1::connection::{self as h1, Answered};
use crate::forwarding::{Client, Cut, FORWARDED_FOR};
use crate::head::Forwarded;
use crate::metrics::Tunnel;
use crate::request_body::Counts;
use crate::tunnel::Tunneled;
use edgerush_filters::forwarding::client_address;
use edgerush_router::Fields;
use edgerush_telemetry::access_log::{Kind, Protocol, Record};
use http::{HeaderValue, StatusCode, Version};
use http_body::Body as HttpBody;
use std::cell::{Cell, RefCell};
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
    /// The connection's client, which says whether the gateway cut the connection.
    connection: Rc<Client>,
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
    grpc_status: Cell<Option<u32>>,
    received: Cell<u64>,
    sent: Cell<u64>,
    /// Whether the answer's body was gone through to its end, or had nothing to go through.
    whole: Cell<bool>,
    /// Whether the core switched a backend for it, a WebSocket's handshake.
    switched: Cell<bool>,
    /// When a switched handshake's first record was written, its answer gone: the start of
    /// its tunnel's life.
    opened: Cell<Option<Instant>>,
    /// How its tunnel ended, and what it carried.
    tunneled: Cell<Option<Tunneled>>,
}

impl Logging {
    /// A record for a request that came in on `listener` now, if the listener logs: what
    /// its head says, kept before routing changes any of it.
    pub(super) fn start<H: Forwarded>(
        worker: &Rc<Worker>,
        snapshot: &Snapshot,
        listener: usize,
        client: &Rc<Client>,
        head: &H,
    ) -> Option<Rc<Self>> {
        let came_in = Instant::now();
        let sink = (*snapshot.logs.get(listener)?)?;
        let compiled = snapshot.listener(listener)?;
        let mut texts = String::with_capacity(160);
        let listener = keep(&mut texts, &compiled.name);
        let method = keep(&mut texts, head.method().as_str());
        let host = head
            .uri()
            .authority()
            .map(http::uri::Authority::as_str)
            .or_else(|| head.host_field().ok())
            .map(|host| keep(&mut texts, host));
        let path = head
            .uri()
            .path_and_query()
            .map(http::uri::PathAndQuery::as_str)
            .map(|path| keep(&mut texts, path));
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
        Some(Rc::new(Self {
            worker: Rc::clone(worker),
            sink,
            time_ms: unix_millis(Duration::ZERO),
            came_in,
            connection: Rc::clone(client),
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
            grpc_status: Cell::new(None),
            received: Cell::new(0),
            sent: Cell::new(0),
            whole: Cell::new(false),
            switched: Cell::new(false),
            opened: Cell::new(None),
            tunneled: Cell::new(None),
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
        self.route.set(Some(keep(&mut texts, route)));
        self.upstream
            .set(upstream.map(|upstream| keep(&mut texts, upstream)));
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

    /// Notes the answer: its status; why the gateway gave it, if the answer is its own; and
    /// a gRPC status its head carries, which a call the gateway answers itself has, and a
    /// trailers-only answer.
    pub(super) fn answered(&self, answered: &Answered<Body>) {
        self.status.set(Some(answered.status().as_u16()));
        if let Answered::Map(response) = answered {
            if let Some(h1::Local(why)) = response.extensions().get::<h1::Local>() {
                self.reason.set(Some(why.label()));
            }
            let status = response.headers().get("grpc-status");
            if let Some(code) = status.and_then(|code| code.to_str().ok()?.parse().ok()) {
                self.grpc_status.set(Some(code));
            }
        }
    }

    /// Notes that the server answered `status` in place of the answer noted: its body
    /// failed before any of its head went (14 §4).
    pub(super) fn replaced(&self, status: StatusCode) {
        self.status.set(Some(status.as_u16()));
    }

    /// Notes the status a gRPC call ended with, wherever it came.
    pub(super) fn called(&self, code: usize) {
        self.grpc_status
            .set(Some(u32::try_from(code).unwrap_or(u32::MAX)));
    }

    /// Notes that the core switched a backend for the request, a WebSocket's handshake,
    /// whose tunnel goes on once its answer has gone.
    pub(super) fn switched(&self) {
        self.switched.set(true);
    }

    /// Notes how the request's tunnel ended, and what it carried.
    pub(super) fn tunneled(&self, tunneled: Tunneled) {
        self.tunneled.set(Some(tunneled));
    }

    fn sent(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.sent.set(self.sent.get().saturating_add(bytes));
    }

    fn failed(&self, error: &BodyError) {
        self.reason.set(Some(error.answer().label()));
    }

    /// Writes the record as it stands now, as one of `kind`: the request's, a WebSocket's
    /// first, or its second, which has its tunnel's outcome, bytes and life in place of the
    /// handshake's.
    fn write(&self, kind: Kind) {
        let Ok(texts) = self.texts.try_borrow() else {
            return;
        };
        let text = |span: Span| texts.get(span.from..span.to);
        // A client that got no answer, or not all of one, left before it could: unless
        // something else is known to have ended it, the gateway closing its connection
        // among them.
        let reason = self.reason.get().or_else(|| {
            (!self.whole.get()).then(|| {
                self.connection
                    .was_cut()
                    .map_or("client_closed", Cut::label)
            })
        });
        let mut record = Record {
            time_ms: self.time_ms,
            kind,
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
            grpc_status: self.grpc_status.get(),
            bytes_in: Some(self.received.get()),
            bytes_out: Some(self.sent.get()),
            duration_us: Some(microseconds(self.came_in)),
            upstream_us: self.upstream_us.get(),
        };
        if kind == Kind::WebSocketClose {
            // A tunnel its server never carried: its client was gone before the handshake's
            // answer could reach it.
            let tunneled = self.tunneled.get();
            record.reason = Some(tunneled.map_or("client_closed", |tunneled| {
                Tunnel::from(tunneled.how).name()
            }));
            record.bytes_in = Some(tunneled.map_or(0, |tunneled| tunneled.up));
            record.bytes_out = Some(tunneled.map_or(0, |tunneled| tunneled.down));
            record.duration_us = self.opened.get().map(microseconds);
        }
        let worker = &self.worker;
        worker
            .batches
            .record(&worker.proxy.logs, self.sink, |out| record.write(out));
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
        let kind = if self.opened.get().is_some() {
            Kind::WebSocketClose
        } else {
            Kind::Request
        };
        self.write(kind);
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
        // A WebSocket's handshake, its answer gone: its first record now, so that a long
        // one is seen before it ends, and its second when its tunnel lets go (08 §2).
        let logging = &self.logging;
        if logging.switched.get() && logging.opened.get().is_none() {
            logging.write(Kind::WebSocketOpen);
            logging.opened.set(Some(Instant::now()));
        }
    }
}

/// Writes the record of a head refused before the core had a request, from `client` on
/// `listener`, if the listener logs: what is known of it, which is who sent it, the
/// `protocol` if that is known, the `status` it was answered and `why` (08 §2). It has no
/// method, host or path, nothing having been read that could be believed.
pub(super) fn refused(
    worker: &Worker,
    listener: usize,
    client: &Client,
    protocol: Option<Protocol>,
    status: http::StatusCode,
    why: &'static str,
) {
    if !worker.proxy.logs.on() {
        return;
    }
    let snapshot = worker.proxy.current.load();
    let (Some(Some(sink)), Some(compiled)) =
        (snapshot.logs.get(listener), snapshot.listener(listener))
    else {
        return;
    };
    let record = Record {
        time_ms: unix_millis(Duration::ZERO),
        kind: Kind::Request,
        listener: &compiled.name,
        client: Some(client.address()),
        peer: client.peer(),
        protocol,
        status: Some(status.as_u16()),
        reason: Some(why),
        ..Record::default()
    };
    worker
        .batches
        .record(&worker.proxy.logs, *sink, |out| record.write(out));
}

/// A `tcp` or `tls` listener's connection's record while it is carried: who it came
/// from, the name its ClientHello asks for, its route, upstream and endpoint as each is
/// found, and what its tunnel carried. Written with how it ended, which is the one place
/// a connection's task ends.
pub(super) struct Passing {
    sink: Sink,
    time_ms: u64,
    accepted: Instant,
    client: Option<IpAddr>,
    peer: Option<SocketAddr>,
    texts: String,
    listener: Span,
    host: Option<Span>,
    route: Option<Span>,
    upstream: Option<Span>,
    endpoint: Option<SocketAddr>,
    /// What its tunnel carried, less the PROXY header the gateway put ahead of the
    /// client's bytes.
    up: u64,
    down: u64,
}

impl Passing {
    /// A record for a connection `accepted` on `listener`, if the listener logs: from
    /// `client` as a PROXY header named it, or else `peer`, the address that connected.
    pub(super) fn start(
        snapshot: &Snapshot,
        listener: usize,
        client: Option<IpAddr>,
        peer: Option<SocketAddr>,
        accepted: Instant,
    ) -> Option<Self> {
        let sink = (*snapshot.logs.get(listener)?)?;
        let compiled = snapshot.listener(listener)?;
        let mut texts = String::with_capacity(96);
        let listener = keep(&mut texts, &compiled.name);
        Some(Self {
            sink,
            time_ms: unix_millis(accepted.elapsed()),
            accepted,
            client: client.or_else(|| peer.map(|peer| peer.ip())),
            peer,
            texts,
            listener,
            host: None,
            route: None,
            upstream: None,
            endpoint: None,
            up: 0,
            down: 0,
        })
    }

    /// Notes the name its ClientHello asks for.
    pub(super) fn named(&mut self, name: &str) {
        self.host = Some(keep(&mut self.texts, name));
    }

    /// Notes its route's name and its upstream's.
    pub(super) fn routed(&mut self, route: &str, upstream: Option<&str>) {
        self.route = Some(keep(&mut self.texts, route));
        self.upstream = upstream.map(|upstream| keep(&mut self.texts, upstream));
    }

    /// Notes the endpoint it goes to.
    pub(super) fn connecting(&mut self, endpoint: SocketAddr) {
        self.endpoint = Some(endpoint);
    }

    /// Notes what its tunnel carried, of which the first `told` bytes up were a PROXY
    /// header of the gateway's.
    pub(super) fn carried(&mut self, tunneled: Tunneled, told: usize) {
        let told = u64::try_from(told).unwrap_or(u64::MAX);
        self.up = tunneled.up.saturating_sub(told);
        self.down = tunneled.down;
    }

    /// Writes the record, of a connection that ended `how`.
    pub(super) fn ended(self, worker: &Worker, how: Tunnel) {
        let text = |span: Span| self.texts.get(span.from..span.to);
        let record = Record {
            time_ms: self.time_ms,
            kind: Kind::Connection,
            listener: text(self.listener).unwrap_or_default(),
            client: self.client,
            peer: self.peer,
            host: self.host.and_then(text),
            reason: Some(how.name()),
            route: self.route.and_then(text),
            upstream: self.upstream.and_then(text),
            endpoint: self.endpoint,
            bytes_in: Some(self.up),
            bytes_out: Some(self.down),
            duration_us: Some(microseconds(self.accepted)),
            ..Record::default()
        };
        worker
            .batches
            .record(&worker.proxy.logs, self.sink, |out| record.write(out));
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

/// `text` kept at the end of `texts`, and where.
fn keep(texts: &mut String, text: &str) -> Span {
    let from = texts.len();
    texts.push_str(text);
    Span {
        from,
        to: texts.len(),
    }
}

/// When it was `ago`, in milliseconds since the Unix epoch.
fn unix_millis(ago: Duration) -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH);
    let at = since_epoch.map_or(Duration::ZERO, |since| since.saturating_sub(ago));
    u64::try_from(at.as_millis()).unwrap_or(u64::MAX)
}
