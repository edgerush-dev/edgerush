//! Serving: connections come in, requests go through the request core, and what it
//! forwards goes to an endpoint of the chosen upstream and comes back. This is where
//! EdgeRush's own servers, of HTTP/1, HTTP/2 and HTTP/3, meet the core.
//!
//! Bodies stream in both directions and are never held here. Upstream connections are
//! HTTP/1.1, by EdgeRush's own client and pool.
//!
//! The config is published whole and at once ([`Proxy::reload`]): a request reads the
//! current snapshot without waiting for anybody, works with that one snapshot until it has
//! been directed, and from then on holds on to its rule at most. Sockets and upstream
//! connections belong to the data plane, not to a snapshot, and outlive every reload.
//!
//! A connection is served on the worker that took it, and stays there: everything it
//! spawns goes into that worker's `LocalSet`, so nothing a request touches need be
//! `Send`. That is what lets a worker own things a thread cannot share —
//! the pool of upstream connections to come, above all.

use crate::downstream::detect::{Protocol, detect};
use crate::downstream::h1::connection::{self as h1, Answered};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h1::deadlines::Bounds;
use crate::downstream::h2;
pub use crate::downstream::h3::listener::Forwarding;
use crate::downstream::h3::{self, listener as h3_listener};
use crate::drain::Drain;
use crate::forwarding::Client;
use crate::gathered::Gathered;
use crate::grpc::answer::{Answered as GrpcAnswered, Count, is_grpc_answer};
use crate::grpc::call::Call;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::l4::hello::{self, Hello};
use crate::linger::{self, Lent, linger};
use crate::map_head::MapHead;
use crate::metrics::{Answer, Metrics, Socket, Stopped, Tunnel};
use crate::mirror;
use crate::random::{random, unguessable};
use crate::raw::{RawAnswer, RawHead};
use crate::request::{Decision, Opening, decide};
use crate::request_body::{RequestBody, RequestBodyError};
use crate::retry::budget::Budget;
use crate::retry::replay::Tee;
use crate::slots::{Slots, WorkerSlots};
use crate::storage::Storage;
use crate::timers::{Alarm, Timers};
use crate::tls::{self, Tls, TlsError};
use crate::tunnel::{Backend, Bounds as TunnelBounds, Carried, Switched, carry};
use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Block, Blocks, SMALL, Sizes};
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use crate::upstream::h1::exchange::{Exchange, ExchangeError, H1Body, Stalled, nothing_to_say};
use crate::upstream::h1::pool::{Lease, Pool};
use crate::upstream::h2::client::{Client as H2Client, PlaceError, Settings as H2Settings};
use crate::upstream::h2::exchange::{self as h2_exchange, Bounds as H2Bounds, Connected};
use crate::upstream::h2::pool::Limits as H2Limits;
use crate::upstream::secure::{Secure, Socket as UpstreamSocket};
use crate::websocket::{self, Key};
use arc_swap::ArcSwap;
use bytes::Bytes;
use edgerush_config::{
    Compiled, CompiledListener, CompiledRetry, CompiledRule, L4, RequestId, Timeout,
    UpstreamProtocol,
};
use edgerush_filters::{HeaderModifier, request_id};
use edgerush_router::Fields;
use http::uri::{Authority, Scheme};
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::Instant;

/// How long accepting pauses after an error that is not about one connection, instead of
/// failing again at once, over and over.
pub(crate) const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

/// How long a connection has from being accepted to finishing its first request head,
/// whatever it spends the time on: saying nothing, the engine working out which HTTP it
/// speaks, or the head itself ([14 §8](../../docs/14-downstream-server.md)).
pub(crate) const FIRST_REQUEST: Duration = Duration::from_secs(10);

/// How long a connection has, once an answer is done, to finish its next request head:
/// the engine's head timeout, which also runs while nothing is said. The engine has one
/// clock for both, so this cannot yet be two deadlines as 14 §8 proposes.
pub(crate) const NEXT_REQUEST: Duration = Duration::from_secs(30);

/// How long a request body, or the room to write an answer, may be waited on with nothing
/// coming ([14 §8](../../docs/14-downstream-server.md)).
pub(crate) const IDLE: Duration = Duration::from_secs(30);

/// How long a draining worker's connections have to finish: inside Kubernetes' default
/// termination grace of 30 seconds, with room to exit (03 §10).
pub(crate) const DRAIN: Duration = Duration::from_secs(25);

/// The deadlines a worker holds its client connections to: [`FIRST_REQUEST`],
/// [`NEXT_REQUEST`], [`IDLE`] and [`DRAIN`], short in tests so that they can run on real
/// sockets and real time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadlines {
    first_request: Duration,
    next_request: Duration,
    idle: Duration,
    drain: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            first_request: FIRST_REQUEST,
            next_request: NEXT_REQUEST,
            idle: IDLE,
            drain: DRAIN,
        }
    }
}

/// What is answered: the upstream's body as it arrives, or nothing at all.
///
/// Named, and not a box or a trait object, because every request would pay for that and
/// because the engine can see through this to what is really left to send — a body whose
/// length is known keeps it, and an answer of ours is end-of-stream from the start.
///
/// An upstream's answer carries its exchange's place with it, whichever client read it:
/// the exchange is over when the body is.
enum Body {
    /// The upstream's answer, as EdgeRush's own path reads it. In a box because it is
    /// much larger than an empty answer, and every answer would otherwise carry room for
    /// it.
    Ours(Box<H1Body<UpstreamSocket, RequestBody>>, Admitted, Watch),
    /// An HTTP/2 upstream's answer, read by EdgeRush's own HTTP/2 client (15 step 6).
    H2(Box<h2_exchange::Answer>, Admitted, Watch),
    /// A gRPC call's answer, ended by one status whatever becomes of it (15 §6).
    Grpc(Box<GrpcAnswered<Body, Called>>),
    /// An answer held to its request's deadline (03 §6).
    Timed(Box<Timed>),
    /// An answer of the data plane's own. It has no body, and never will have one.
    Empty,
}

/// An answer held to its request's deadline: cut off, as a body that failed, if the
/// deadline passes before its end. The client has its head, so this is all that is left to
/// tell it with: HTTP/1 closes the connection, HTTP/2 and HTTP/3 reset the stream.
struct Timed {
    body: Body,
    deadline: Instant,
    alarm: Alarm,
}

/// Where a gRPC call's status is counted: its listener's.
struct Called {
    proxy: Arc<Proxy>,
    listener: usize,
}

impl Count for Called {
    fn ended(&self, code: usize) {
        if let Some(counters) = self.proxy.metrics.listener(self.listener) {
            counters.called(code);
        }
    }
}

/// Waits for `opening` to connect, and gives up after `limit`: a destination that never
/// answers a connect must not hold a request, or the place it was admitted to, for as long
/// as the operating system cares to retry ([13 §7](../../docs/13-http1-upstream.md)).
/// Given the connect rather than an address, so that one which never completes can be
/// put to it.
async fn connect_within<S>(
    limit: Duration,
    opening: impl Future<Output = io::Result<S>>,
) -> Result<S, ExchangeError> {
    match tokio::time::timeout(limit, opening).await {
        Ok(socket) => Ok(socket?),
        Err(_) => Err(ExchangeError::Io(io::ErrorKind::TimedOut.into())),
    }
}

/// Which of the named reasons an exchange stopped for.
///
/// A fixed list on purpose: an upstream that fails in a new way must not be able to make
/// a new series, and no error text reaches a label
/// ([13 §7](../../docs/13-http1-upstream.md)).
fn why_stopped(error: &ExchangeError) -> Stopped {
    match error {
        ExchangeError::Codec(_) => Stopped::Codec,
        ExchangeError::Io(_) => Stopped::Io,
        ExchangeError::RequestBody(_) => Stopped::RequestBody,
        ExchangeError::Closed => Stopped::Closed,
        ExchangeError::Unsolicited => Stopped::Unsolicited,
        ExchangeError::TooManyInterim { .. } | ExchangeError::InterimTooLong { .. } => {
            Stopped::Interim
        }
        ExchangeError::TooSlow { .. } => Stopped::TooSlow,
        ExchangeError::Idle { .. } => Stopped::Idle,
        ExchangeError::Exhausted(_) => Stopped::Exhausted,
    }
}

/// Where a body says that it failed.
///
/// Carried by the body because that is where a failure after the head happens: by then
/// the status has been counted and the client has been told, so nothing else is left to
/// notice ([13 §7](../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone)]
struct Watch {
    proxy: Arc<Proxy>,
    upstream: usize,
}

impl Watch {
    fn body_failed(&self) {
        if let Some(counters) = self.proxy.metrics.upstream(self.upstream) {
            counters.body_failures.inc();
        }
    }
}

/// Why an answer's body stopped. One kind for whichever way it was being read, so that
/// what carries it does not change when a second way arrives.
#[derive(Debug, thiserror::Error)]
enum BodyError {
    #[error("the upstream's answer could not be read: {0}")]
    Ours(#[from] ExchangeError),
    #[error("the HTTP/2 upstream's answer could not be read")]
    H2(#[source] RequestBodyError),
    #[error("the request's deadline passed before its answer's end")]
    DeadlinePassed,
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        match self.get_mut() {
            Self::Ours(ours, _place, watch) => {
                let frame = Pin::new(&mut *ours).poll_frame(context);
                // The moment the answer is known to be over, which for a body of known
                // length is its last frame and not some later poll: a client told how
                // long a body is has no reason to ask again, and a connection waiting on
                // a poll that never comes is a connection nobody gets to use.
                if ours.is_end_stream() {
                    ours.settle();
                }
                frame.map(|frame| {
                    frame.map(|frame| {
                        frame.map_err(|error| {
                            watch.body_failed();
                            BodyError::Ours(error)
                        })
                    })
                })
            }
            Self::H2(answer, _place, watch) => {
                Pin::new(&mut **answer).poll_frame(context).map(|frame| {
                    frame.map(|frame| {
                        frame.map_err(|error| {
                            watch.body_failed();
                            BodyError::H2(error)
                        })
                    })
                })
            }
            Self::Grpc(answered) => Pin::new(&mut **answered).poll_frame(context),
            Self::Timed(timed) => {
                // Looked at first, whatever the body has ready: an answer that keeps coming
                // must not keep its deadline from being seen.
                if timed.alarm.poll_until(context, timed.deadline).is_ready() {
                    // Let go of now, and the upstream's connection or stream with it.
                    timed.body = Self::Empty;
                    return Poll::Ready(Some(Err(BodyError::DeadlinePassed)));
                }
                Pin::new(&mut timed.body).poll_frame(context)
            }
            Self::Empty => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Ours(ours, ..) => ours.is_end_stream(),
            Self::H2(answer, ..) => answer.is_end_stream(),
            Self::Grpc(answered) => answered.is_end_stream(),
            Self::Timed(timed) => timed.body.is_end_stream(),
            Self::Empty => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Ours(ours, ..) => ours.size_hint(),
            Self::H2(answer, ..) => answer.size_hint(),
            Self::Grpc(answered) => answered.size_hint(),
            Self::Timed(timed) => timed.body.size_hint(),
            Self::Empty => SizeHint::with_exact(0),
        }
    }
}

/// A data plane: one for the whole process, whatever its workers. It holds what must be
/// one — the config every worker serves, and the counters they all add to — and takes a
/// new config while they run.
///
/// What a worker keeps to itself is in [`Worker`].
#[derive(Debug)]
pub struct Proxy {
    /// The names of the listeners of the config the data plane was made with. A socket is
    /// served as the listener at its position here, whatever configs come later.
    listeners: Vec<String>,
    current: ArcSwap<Snapshot>,
    /// Outside the snapshot, so that a reload resets no counter.
    metrics: Metrics,
    /// Outside it for a different reason: a key must not come round again when a config
    /// does, so what hands them out lives as long as the process.
    keys: Keys,
    /// Set once, to drain: every worker's sweep brings it to the worker (03 §10).
    draining: AtomicBool,
    /// The keys every worker issues and reads QUIC connection IDs and Retry tokens with.
    quic: h3_listener::Secrets,
}

/// One worker's share of the data plane: the connections it holds to the upstreams, which
/// are its own and no other worker's, and a handle on what they all share.
///
/// It is made on the worker's own thread, in its `LocalSet`, and never leaves it: it is
/// counted with an [`Rc`] and holds what cannot be sent anywhere. This is the place the
/// pool of upstream connections will go.
#[derive(Debug)]
pub struct Worker {
    proxy: Arc<Proxy>,
    /// The connections this worker keeps to its upstreams. One for the life of the worker:
    /// a reload does not throw warm connections away. Those to an endpoint that is no
    /// longer used grow idle and are closed.
    pool: Rc<RefCell<Pool<UpstreamSocket>>>,
    /// What its exchanges read into, lent and taken back rather than made each time
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    blocks: Rc<RefCell<Blocks>>,
    /// Every deadline the worker keeps ([03 §2](../../docs/03-data-plane.md)).
    timers: Rc<Timers>,
    /// How many exchanges this worker has in hand. Its own, like everything else here:
    /// no worker waits on another to find out whether it may take a request.
    in_flight: Rc<Cell<usize>>,
    limits: H1Limits,
    deadlines: Deadlines,
    /// The time an answer is dated with, which the worker's sweep keeps current so that
    /// no answer reads a clock for it (14 §4).
    date: Cell<HttpDate>,
    /// This worker's drain, which its sweep starts once the data plane's has.
    drain: Rc<Drain>,
    /// Its HTTP/2 connections to upstreams, many requests at once on each (15 §4).
    h2: Rc<H2Client>,
    /// Its retry budgets, by upstream slot: a worker's own, as its connections are.
    budgets: RefCell<HashMap<usize, Budget>>,
    /// Connections accepted since everything else last had a turn.
    accepted: Cell<usize>,
    /// Its limits again, for the bodies of answers to share rather than copy.
    body_limits: Rc<H1Limits>,
    /// Itself, for the tasks that send a mirror's copies to hold on to.
    me: Weak<Worker>,
    /// Its number among the data plane's workers, which the QUIC connection IDs it issues
    /// carry (16 §3).
    position: u16,
    /// Where its HTTP/1 connections run their requests' futures, lent for as long as a
    /// request runs, so that no connection holds room for one while it waits (14 §3).
    slots: WorkerSlots,
    /// What its HTTP/1 connections are held to, which each borrows rather than copies.
    h1: h1::Settings,
}

/// Serves `socket`, a connection of `ours` that speaks HTTP/1, with our own server (14).
/// `asking` is set when the first request is handed over.
async fn serve_h1<S>(ours: Rc<Connection>, client: Rc<Client>, asking: Rc<Cell<bool>>, socket: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let worker = Rc::clone(&ours.worker);
    let listener = ours.listener;
    // The connection is kept by this for as long as it is served; each request's future
    // owns only a handle on the worker. It is that future itself, not one wrapped around
    // it, so that it is not moved into another on every request.
    let respond = move |head: RawHead, body, interim| {
        asking.set(true);
        Rc::clone(&ours.worker).handle_head(listener, Rc::clone(&client), head, body, Some(interim))
    };
    let slots = slots_for(&worker.slots, &respond);
    let _ended = h1::serve(
        socket,
        &worker.h1,
        Rc::clone(&worker.blocks),
        Rc::clone(&worker.timers),
        || worker.date.get(),
        &worker.drain,
        respond,
        &slots,
    )
    .await;
}

/// The worker's slots for the futures `respond` makes: named by the closure, as the futures'
/// own type cannot be.
fn slots_for<R, F>(slots: &WorkerSlots, _respond: &R) -> Rc<Slots<F>>
where
    R: FnMut(RawHead, RequestBody, Interim) -> F,
    F: Future + 'static,
{
    slots.of()
}

/// Serves `socket`, a connection of `ours` secured with `tls`, as whichever of HTTP/1 and
/// HTTP/2 the handshake agrees on (ALPN).
async fn serve_tls(
    ours: Rc<Connection>,
    client: Rc<Client>,
    asking: Rc<Cell<bool>>,
    deadlines: Deadlines,
    tls: &Tls,
    socket: Lent,
) {
    // A handshake that fails has nobody to tell but the client, whom BoringSSL has sent its
    // alert.
    let Ok(secured) = tokio_boring::accept(tls.acceptor(), socket).await else {
        return;
    };
    if secured.ssl().selected_alpn_protocol() == Some(tls::H2) {
        serve_h2(ours, client, asking, deadlines, secured).await;
    } else {
        // An answer's pieces sealed as one record, not one each.
        serve_h1(ours, client, asking, Gathered::new(secured)).await;
    }
}

/// Serves `socket`, a connection of `ours` that speaks HTTP/2, over h2 (15 step 2).
/// `asking` is set when the first request is handed over.
async fn serve_h2<S>(
    ours: Rc<Connection>,
    client: Rc<Client>,
    asking: Rc<Cell<bool>>,
    deadlines: Deadlines,
    socket: S,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let worker = Rc::clone(&ours.worker);
    let listener = ours.listener;
    let storage = Rc::clone(worker.blocks.borrow().storage());
    let dating = Rc::clone(&worker);
    let date = Rc::new(move || dating.date.get());
    let respond = Rc::new(move |request: Request<RequestBody>, interim| {
        asking.set(true);
        Rc::clone(&ours.worker).handle(listener, Rc::clone(&client), request, Some(interim))
    });
    let settings = h2::connection::Settings {
        keep_alive: deadlines.next_request,
        closing: Bounds::default().next_head.min(deadlines.next_request),
        idle: deadlines.idle,
        drain_within: deadlines.drain,
        ..h2::connection::Settings::default()
    };
    h2::connection::serve(socket, settings, storage, date, &worker.drain, respond).await;
}

/// One exchange's place among those a worker has in hand, given back when it is dropped.
///
/// Taken before a connection is looked for and held until the answer's body is let go of,
/// so that what is counted is work in hand rather than requests begun. Dropping is the
/// only way to give it back, which is what makes every way out of an exchange — answered,
/// failed, or a client that stopped reading — release it without being told to
/// ([13 §7](../../docs/13-http1-upstream.md)).
#[derive(Debug)]
struct Admitted(Rc<Cell<usize>>);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// A compiled config and what the data plane works out from it, once, when it arrives.
#[derive(Debug)]
struct Snapshot {
    config: Compiled,
    /// By position in [`Proxy::listeners`]: where this config has the listener of that
    /// name, if it has one. No request looks a listener up by its name.
    listeners: Vec<Option<usize>>,
    /// By position of the upstream, then of the endpoint: where to connect, in the form a
    /// request target takes, made once so that no request formats an address.
    endpoints: Vec<Vec<Authority>>,
    /// By position of the upstream: the slot of its counters.
    upstream_slots: Vec<usize>,
    /// By position of the upstream, then of the endpoint: what a kept connection to it is
    /// filed under. Worked out against the config this one replaces, because that is the
    /// only moment both are in hand ([13 §3](../../docs/13-http1-upstream.md)).
    destinations: Destinations,
    /// By position of the upstream: what its connections are secured with, if anything.
    /// Kept from the config before for as long as the TLS is the same.
    secure: Vec<Option<Arc<Secure>>>,
    /// By position in [`Proxy::listeners`]: what a connection to it is accepted with, if
    /// it is to speak TLS. Kept from the config before for as long as the certificates
    /// are the same, and with it the keys of the session tickets it has issued.
    tls: Vec<Option<Arc<Tls>>>,
    /// By position in [`Proxy::listeners`]: the `Alt-Svc` its answers carry, if it serves
    /// HTTP/3 (16 §5), made once so that no answer formats it.
    alt_svc: Vec<Option<HeaderModifier>>,
}

impl Snapshot {
    /// The compiled listener whose socket is at `position`, if the config still has it.
    fn listener(&self, position: usize) -> Option<&CompiledListener> {
        let at = self.listeners.get(position).copied().flatten()?;
        self.config.listeners.get(at)
    }

    fn new(
        config: Compiled,
        listeners: &[String],
        metrics: &Metrics,
        previous: Option<&Snapshot>,
        keys: &Keys,
    ) -> Result<Self, ProxyError> {
        let endpoints = config
            .upstreams
            .iter()
            .map(|upstream| upstream.endpoints.iter().map(authority).collect())
            .collect::<Result<_, _>>()?;
        let listeners: Vec<Option<usize>> = listeners
            .iter()
            .map(|name| config.listeners.iter().position(|l| l.name == *name))
            .collect();
        let tls = listeners
            .iter()
            .enumerate()
            .map(|(position, at)| {
                let Some(listener) = at.and_then(|at| config.listeners.get(at)) else {
                    return Ok(None);
                };
                let Some(source) = &listener.tls else {
                    return Ok(None);
                };
                let before = previous
                    .and_then(|previous| previous.tls.get(position))
                    .and_then(Option::as_ref);
                if let Some(kept) = before.filter(|kept| kept.is_for(source)) {
                    return Ok(Some(Arc::clone(kept)));
                }
                // New certificates behind the front the listener had, if it can keep it.
                before
                    .map_or_else(|| Tls::new(source), |before| Tls::after(before, source))
                    .map(|tls| Some(Arc::new(tls)))
                    .map_err(|error| ProxyError::Tls {
                        listener: listener.name.clone(),
                        error,
                    })
            })
            .collect::<Result<_, _>>()?;
        let alt_svc = listeners
            .iter()
            .map(|at| alt_svc(at.and_then(|at| config.listeners.get(at))?))
            .collect();
        let upstream_slots = config
            .upstreams
            .iter()
            .map(|upstream| metrics.upstream_slot(&upstream.name))
            .collect();
        let secure: Vec<Option<Arc<Secure>>> = config
            .upstreams
            .iter()
            .map(|upstream| {
                let Some(source) = &upstream.tls else {
                    return Ok(None);
                };
                let kept = previous
                    .into_iter()
                    .flat_map(|previous| previous.secure.iter().flatten())
                    .find(|kept| kept.is_for(source, upstream.protocol));
                match kept {
                    Some(kept) => Ok(Some(Arc::clone(kept))),
                    None => Secure::new(source, upstream.protocol)
                        .map(|secure| Some(Arc::new(secure)))
                        .map_err(|error| ProxyError::UpstreamTls {
                            upstream: upstream.name.clone(),
                            error,
                        }),
                }
            })
            .collect::<Result<_, _>>()?;
        let nothing_yet = Destinations::default();
        let previous_destinations =
            previous.map_or(&nothing_yet, |previous| &previous.destinations);
        let destinations = Destinations::reconcile(&config, previous_destinations, keys, &secure);
        Ok(Self {
            config,
            listeners,
            endpoints,
            upstream_slots,
            destinations,
            tls,
            alt_svc,
            secure,
        })
    }
}

/// The `Alt-Svc` a listener's answers carry: HTTP/3 on the listener's port, for as long as
/// its config says (RFC 7838 §3). None for a listener without HTTP/3, or on a port the
/// operating system is to choose, which the config does not know.
fn alt_svc(listener: &edgerush_config::CompiledListener) -> Option<HeaderModifier> {
    let http3 = listener.http3?;
    let port = listener.address.port();
    if port == 0 {
        return None;
    }
    let value = format!("h3=\":{port}\"; ma={}", http3.alt_svc_max_age);
    HeaderModifier::new([("alt-svc", value.as_str())], [], []).ok()
}

impl Proxy {
    /// A data plane that runs `config` on `workers` of them. Nothing is listened on or
    /// connected to yet, and no worker exists until [`Worker::new`] makes one.
    ///
    /// The worker count is told, not guessed: it is what the counters are sharded by, and
    /// a process left to work it out for itself reads the machine rather than what it was
    /// given ([03 §2] in the docs).
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] for an endpoint address that cannot be part of a request
    /// target (one with an IPv6 zone).
    pub fn new(config: Compiled, workers: NonZeroUsize) -> Result<Self, ProxyError> {
        let listeners: Vec<String> = config
            .listeners
            .iter()
            .map(|listener| listener.name.clone())
            .collect();
        // A shard for every worker, so that no two write to one line of cache.
        let metrics = Metrics::new(workers, listeners.len());
        let keys = Keys::default();
        let snapshot = Snapshot::new(config, &listeners, &metrics, None, &keys)?;
        let quic =
            h3_listener::Secrets::new().map_err(|error| ProxyError::Random(error.to_string()))?;
        Ok(Self {
            listeners,
            current: ArcSwap::from_pointee(snapshot),
            metrics,
            keys,
            draining: AtomicBool::new(false),
            quic,
        })
    }

    /// The names of the listeners that can be served: those of the config the data plane
    /// was made with, in its order.
    #[must_use]
    pub fn listeners(&self) -> &[String] {
        &self.listeners
    }

    /// Drains the data plane: every worker, at its next sweep, stops accepting and lets its
    /// connections go as they finish (03 §10). It cannot be undone.
    pub fn drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    /// Runs `config` from now on. It is published whole and at once: every request is
    /// served by one config or by the other, none waits and none is dropped. Requests that
    /// are under way finish by the rule they began with; sockets stay open and upstream
    /// connections stay warm.
    ///
    /// A listener keeps its socket by its name. While a config does not have it, nothing
    /// that comes in on its socket has a route; listeners that only a later config has are
    /// not served, as no socket is theirs.
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] as [`Proxy::new`] does; the data plane then runs on as it
    /// was.
    pub fn reload(&self, config: Compiled) -> Result<(), ProxyError> {
        // Against the config on its way out, so that a destination which has not changed
        // keeps what its connections are filed under and one that has gone is retired.
        let previous = self.current.load();
        let snapshot = Snapshot::new(
            config,
            &self.listeners,
            &self.metrics,
            Some(&previous),
            &self.keys,
        )?;
        drop(previous);
        // Only now that all of it is accepted: a config refused for one listener must not
        // have changed the certificates of another.
        for tls in snapshot.tls.iter().flatten() {
            tls.install();
        }
        self.current.store(Arc::new(snapshot));
        self.metrics.reloads.inc();
        let now = SystemTime::now().duration_since(UNIX_EPOCH);
        let now = now.map_or(0, |since_epoch| since_epoch.as_secs());
        self.metrics.last_reload.store(now, Ordering::Relaxed);
        Ok(())
    }

    /// What has been counted, in the Prometheus text format: per listener and per upstream
    /// of the current config. Counters are kept outside the config, so a reload resets
    /// none of them. For scrapes: it adds up the shards of every series and allocates.
    #[must_use]
    pub fn metrics(&self) -> String {
        let snapshot = self.current.load();
        let upstreams: Vec<(&str, usize)> = snapshot
            .config
            .upstreams
            .iter()
            .zip(&snapshot.upstream_slots)
            .map(|(upstream, slot)| (upstream.name.as_str(), *slot))
            .collect();
        // Read where the health checker writes it, at the moment of the scrape.
        let healthy: Vec<(&str, usize)> = snapshot
            .config
            .upstreams
            .iter()
            .enumerate()
            .map(|(position, upstream)| {
                let serving = snapshot
                    .destinations
                    .of(position)
                    .iter()
                    .filter(|destination| destination.is_healthy())
                    .count();
                (upstream.name.as_str(), serving)
            })
            .collect();
        self.metrics.render(&self.listeners, &upstreams, &healthy)
    }

    /// Probes the endpoints of every upstream that asks for health checks, for as long as
    /// the data plane runs, and keeps those that fail them out of load balancing
    /// ([03 §6](../../docs/03-data-plane.md)). For a thread and runtime of its own, in a
    /// `LocalSet`: the probes must still run when the workers are saturated.
    pub async fn check_health(self: Arc<Self>) {
        crate::health::check(self).await;
    }

    /// The destinations of the running config whose endpoints are probed.
    pub(crate) fn checked(&self) -> impl Iterator<Item = Arc<ReuseIdentity>> + use<> {
        let snapshot = self.current.load();
        let checked: Vec<Arc<ReuseIdentity>> = snapshot
            .destinations
            .all()
            .filter(|destination| destination.health_check().is_some())
            .map(Arc::clone)
            .collect();
        checked.into_iter()
    }

    /// Counts a failure to accept on the socket of the listener at position `listener`,
    /// and says how long to wait before accepting again: not at all after the failure of
    /// the one connection that was next in line, a moment after one that is not about a
    /// connection — out of file descriptors, say — and would only happen again at once.
    /// For whoever accepts by themselves and serves with [`Worker::serve_connection`].
    pub fn accept_failed(&self, listener: usize, error: &io::Error) -> Option<Duration> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.accept_errors.inc();
        }
        (!is_about_one_connection(error)).then_some(ACCEPT_PAUSE)
    }
}

impl Worker {
    /// A worker of `proxy`, with upstream connections of its own. Made on the thread that
    /// serves with it, inside that thread's `LocalSet`, and never moved off it.
    #[must_use]
    pub fn new(proxy: Arc<Proxy>) -> Rc<Self> {
        Self::with_limits(proxy, H1Limits::default())
    }

    /// The same, on bounds of the caller's choosing. The stage has no configuration for
    /// these; what it has is one value per worker
    /// ([13 §7](../../docs/13-http1-upstream.md)), which a benchmark may override and
    /// then say that it did.
    #[must_use]
    pub fn with_limits(proxy: Arc<Proxy>, limits: H1Limits) -> Rc<Self> {
        Self::with_deadlines(proxy, limits, Deadlines::default())
    }

    /// The same, as the `position`th of the data plane's workers: what the QUIC
    /// connection IDs it issues say, so that another worker can hand it a datagram of its
    /// connections'.
    #[must_use]
    pub fn at(proxy: Arc<Proxy>, limits: H1Limits, position: u16) -> Rc<Self> {
        Self::made(proxy, limits, Deadlines::default(), position)
    }

    /// The same, holding client connections to `deadlines`.
    fn with_deadlines(proxy: Arc<Proxy>, limits: H1Limits, deadlines: Deadlines) -> Rc<Self> {
        Self::made(proxy, limits, deadlines, 0)
    }

    fn made(proxy: Arc<Proxy>, limits: H1Limits, deadlines: Deadlines, position: u16) -> Rc<Self> {
        let body_limits = Rc::new(limits);
        let h1 = h1::Settings {
            limits: Rc::clone(&body_limits),
            bounds: Bounds {
                first_request: deadlines.first_request,
                // 14 §8's ten seconds for a head once it has begun, never longer than the
                // wait for it to begin.
                next_head: Bounds::default().next_head.min(deadlines.next_request),
                keep_alive: deadlines.next_request,
                idle: deadlines.idle,
                ..Bounds::default()
            },
            budget: h1::Budget::default(),
        };
        Rc::new_cyclic(|me| Self {
            proxy,
            pool: Rc::new(RefCell::new(Pool::default())),
            blocks: Rc::new(RefCell::new(Blocks::new(
                Sizes::within(&limits, SMALL),
                Storage::new(limits.storage),
            ))),
            timers: Timers::new(),
            in_flight: Rc::new(Cell::new(0)),
            limits,
            deadlines,
            date: Cell::new(HttpDate::from_unix(unix_now())),
            drain: Rc::new(Drain::default()),
            h2: H2Client::new(h2_settings(&limits)),
            budgets: RefCell::new(HashMap::new()),
            accepted: Cell::new(0),
            body_limits,
            me: Weak::clone(me),
            position,
            slots: WorkerSlots::default(),
            h1,
        })
    }

    /// Starts this worker's drain now: it stops accepting, HTTP/1 connections close once
    /// idle and say so on the answer in hand, and HTTP/2 connections are told to go
    /// (03 §10). A worker's sweep does this by itself once [`Proxy::drain`] has been called.
    pub fn drain(&self) {
        self.drain.start();
    }

    /// Until this worker drains.
    pub async fn draining(&self) {
        self.drain.started().await;
    }

    /// The next connection on `socket`, or `None` once this worker drains: a draining
    /// worker takes nothing new (03 §10).
    pub async fn accept(&self, socket: &TcpListener) -> Option<io::Result<TcpStream>> {
        // After a batch, everything else the worker has ready goes first: accepting all a
        // backlog holds would put a flood of new connections ahead of the requests of the
        // ones it has (03 §3).
        if self.accepted.get() >= self.limits.accept_batch {
            self.accepted.set(0);
            tokio::task::yield_now().await;
        }
        self.accepted.set(self.accepted.get() + 1);
        let mut draining = pin!(self.drain.notified());
        std::future::poll_fn(|cx| {
            if self.drain.poll_on(draining.as_mut(), cx).is_ready() {
                return Poll::Ready(None);
            }
            socket
                .poll_accept(cx)
                .map(|accepted| Some(accepted.map(|(stream, _)| stream)))
        })
        .await
    }

    /// Looks over the connections this worker is keeping, for as long as it runs.
    ///
    /// One sweep for the worker rather than a timer for every connection, and it is what
    /// clears out a destination that a reload took away and that nothing will ask for
    /// again ([13 §3](../../docs/13-http1-upstream.md)). It waits on the worker's timers
    /// too, beside the sweep. Spawned into the worker's `LocalSet` beside its listeners; a
    /// worker without it keeps what it should drop, and none of its deadlines ever comes.
    pub async fn maintain(self: Rc<Self>) {
        let mut timing = pin!(Rc::clone(&self.timers).run());
        let mut sweeping = pin!(self.sweep());
        poll_fn(|context| {
            if let Poll::Ready(never) = timing.as_mut().poll(context) {
                match never {}
            }
            sweeping.as_mut().poll(context)
        })
        .await;
    }

    /// The sweep itself, for as long as the worker runs.
    async fn sweep(self: Rc<Self>) {
        let every = self.limits.sweep;
        loop {
            tokio::time::sleep(every).await;
            self.date.set(HttpDate::from_unix(unix_now()));
            if self.proxy.draining.load(Ordering::Acquire) {
                self.drain.start();
            }
            // Borrowed for the sweep and let go of before anything is waited on again.
            let swept = self.pool.borrow_mut().sweep(&self.limits);
            // And what a burst left parked of the blocks, down to what a quiet worker keeps.
            self.blocks.borrow_mut().sweep();
            self.slots.sweep();
            self.h2.sweep();
            let metrics = &self.proxy.metrics;
            for _discarded in 0..swept {
                metrics.socket(Socket::Discarded);
            }
            // What this worker holds at the moment it last looked. A sweep already walks
            // everything these ask about, so nothing is counted on the request path for
            // them ([13 §7](../../docs/13-http1-upstream.md)).
            let storage = self.blocks.borrow().storage().used();
            metrics
                .worker()
                .holding(self.in_flight.get(), self.idle_connections(), storage);
        }
    }

    /// How many connections this worker is keeping. For tests and, later, a gauge.
    #[must_use]
    pub fn idle_connections(&self) -> usize {
        self.pool.borrow().idle()
    }

    /// How many HTTP/2 connections to upstreams this worker has, open or being opened.
    /// For tests and, later, a gauge.
    #[must_use]
    pub fn h2_connections(&self) -> usize {
        self.h2.connections()
    }

    /// Takes a place among the exchanges this worker has in hand, if one is going.
    ///
    /// Nothing waits here. A request arriving at a worker that is already full is
    /// answered, because holding it would cost the very memory the bound is for.
    fn admit(&self) -> Option<Admitted> {
        let in_hand = self.in_flight.get();
        if in_hand >= self.limits.exchanges {
            return None;
        }
        self.in_flight.set(in_hand + 1);
        Some(Admitted(Rc::clone(&self.in_flight)))
    }

    /// Sends a request by EdgeRush's own path and returns the answer's head and body.
    ///
    /// A connection comes out of the pool where there is one to use, and is opened where
    /// there is not; either way the answer's body carries the way back, and the
    /// connection returns only if the body earns it ([13 §6](../../docs/13-http1-upstream.md)).
    ///
    /// # Errors
    ///
    /// Anything the upstream said that cannot be read, or a connection that could not be
    /// opened, failed or closed without answering.
    #[expect(
        clippy::too_many_arguments,
        reason = "each of them is a different thing an exchange needs, and a struct \n                  to hold them would be indirection for a lint rather than for a reader"
    )]
    async fn through_h1<F, B>(
        &self,
        identity: &Arc<ReuseIdentity>,
        method: &Method,
        uri: &Uri,
        headers: &F,
        nominated: &[HeaderName],
        sending: Sending,
        body: B,
        interim: Option<Interim>,
        head_by_rule: bool,
        upgrading: bool,
    ) -> Result<(RawAnswer, Box<H1Body<UpstreamSocket, B>>), ExchangeError>
    where
        F: OutgoingFields + ?Sized,
        B: HttpBody<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Bound in its own statement, so the pool is not still borrowed when the connect
        // below is waited on.
        let mut kept = None;
        loop {
            let found = self.pool.borrow_mut().take(identity, &self.limits);
            let Some((mut socket, opened)) = found else {
                break;
            };
            // Quiet when it was put back is not quiet now. Anything readable is an
            // upstream that has said something nobody asked for, and an answer to the
            // last request must never be handed to the next one; the connection goes
            // and another is tried.
            if nothing_to_say(&mut socket) {
                kept = Some((socket, opened));
                break;
            }
            // Anything readable is an upstream saying something nobody asked for, and the
            // socket goes rather than being lent again.
            self.proxy.metrics.socket(Socket::Discarded);
        }
        let (socket, opened) = match kept {
            Some(reused) => {
                self.proxy.metrics.socket(Socket::Reused);
                reused
            }
            None => {
                self.proxy.metrics.socket(Socket::Opened);
                let secure = identity.secure().cloned();
                // One bound for the connection and its handshake together.
                let opening = async {
                    let socket = TcpStream::connect(identity.address()).await?;
                    // Worth having, not worth refusing an upstream over.
                    let _unset = socket.set_nodelay(true);
                    match secure {
                        None => Ok(UpstreamSocket::Plain(socket)),
                        Some(secure) => secure
                            .connect(socket)
                            .await
                            .map(|secured| UpstreamSocket::Secured(Gathered::new(secured))),
                    }
                };
                let socket = connect_within(self.limits.connect, opening).await?;
                (socket, Instant::now())
            }
        };

        let mut exchange = Exchange::new(socket, Rc::clone(&self.blocks), Rc::clone(&self.timers));
        if let Some(interim) = interim {
            exchange = exchange.heard_by(interim);
        }
        if head_by_rule {
            exchange = exchange.head_bounded_elsewhere();
        }
        if upgrading {
            exchange = exchange.upgrading();
        }
        let (answer, rest) = exchange
            .send(method, uri, headers, nominated, sending, body, &self.limits)
            .await?;

        // The request may still be going out; what is left of it goes with the body,
        // which drives it while the client reads the answer. Boxed where it is made, so
        // that only a pointer to it is passed back up to where it is served.
        let lease = Lease::in_use(Arc::clone(identity), opened, &self.pool);
        let head = answer.head;
        let persistent =
            answer.delivery.persistent && !crate::upstream::auth::challenges(head.status, &head);
        let body = Box::new(
            H1Body::new(
                rest,
                answer.delivery.framing,
                persistent,
                answer.nominated,
                Rc::clone(&self.body_limits),
            )
            .returning_to(lease),
        );
        Ok((head.into_answer(), body))
    }

    /// What this worker serves: the data plane the whole process shares.
    #[must_use]
    pub fn proxy(&self) -> &Arc<Proxy> {
        &self.proxy
    }

    /// Serves the connections that come in on `socket` as those of the listener at
    /// position `listener` of [`Proxy::listeners`], HTTP/1.1 and HTTP/2 alike. Never
    /// returns; dropping the future stops accepting, and connections already accepted
    /// carry on.
    ///
    /// It accepts whatever comes, with no bound on how many connections it holds: it is
    /// for tests and single-worker harnesses. The data plane's workers accept through
    /// their own loop, which stops at each worker's cap
    /// ([14 §8](../../docs/14-downstream-server.md)).
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where the connections it accepts are served;
    /// without one there is nowhere to put them and the first connection panics.
    pub async fn serve(self: Rc<Self>, listener: usize, socket: TcpListener) {
        // Draining: nothing new is taken, and the socket goes with this.
        while let Some(accepted) = self.accept(&socket).await {
            match accepted {
                Ok(stream) => {
                    let connection = Rc::clone(&self).serve_connection(listener, stream);
                    let _detached = tokio::task::spawn_local(connection);
                }
                Err(error) => {
                    if let Some(pause) = self.proxy.accept_failed(listener, &error) {
                        tokio::time::sleep(pause).await;
                    }
                }
            }
        }
    }

    /// Serves one connection, to its end, as one of the listener at position `listener`
    /// of [`Proxy::listeners`]. It may have been accepted anywhere — by another thread,
    /// which then hands it over as a socket of the standard library — as long as `stream`
    /// was made on the runtime that runs this.
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where the engine's own futures go. An
    /// HTTP/2 connection
    /// panics without one, as the engine spawns a future for every stream.
    pub async fn serve_connection(self: Rc<Self>, listener: usize, stream: TcpStream) {
        // Worth having, not worth refusing a connection over.
        let _unset = stream.set_nodelay(true);
        // Gone only if the client already is.
        let peer = stream.peer_addr();
        let deadlines = self.deadlines;
        let (tls, passthrough) = {
            let snapshot = self.proxy.current.load();
            // The TLS of the config the connection came in under, which it keeps to its end.
            let tls = snapshot.tls.get(listener).cloned().flatten();
            // A `tcp` or `tls` listener's connections are carried, not served (17).
            let passthrough = snapshot
                .listener(listener)
                .and_then(|compiled| compiled.l4.as_ref())
                .map(|l4| matches!(l4, L4::Tls(_)));
            (tls, passthrough)
        };
        if let Some(by_name) = passthrough {
            return self.pass_through(listener, stream, by_name).await;
        }
        let connection = Rc::new(Connection::open(self, listener));
        let Ok(peer) = peer else {
            return;
        };
        // Who the upstream is told the client is, written once for all its requests.
        let client = Rc::new(Client::new(peer.ip()));
        // Set when the engine hands over the first request, which is the end of the one
        // stretch its own deadlines do not cover.
        let asked = Rc::new(Cell::new(false));
        let (ours, ours_asking) = (Rc::clone(&connection), Rc::clone(&asked));
        // Lent rather than given, so that it comes back once the engine is done with it.
        let (lent, back) = Lent::new(stream);
        // Each served by our own server: HTTP/1 by the one of 14, HTTP/2 over h2 (15 step 2).
        // A task is as large as its future's largest state, from accept to close, whatever
        // the connection turns out to be (14 §3). So only plain HTTP/1, which waits between
        // requests holding little, is served inline; HTTP/2 and TLS, which hold much more
        // anyway, are boxed once when the connection is found to be one of them.
        let serving = async move {
            match tls {
                // Told apart by our own detector.
                None => match detect(lent).await {
                    Ok(Some((Protocol::Http1, replay))) => {
                        serve_h1(ours, client, ours_asking, replay).await;
                    }
                    Ok(Some((Protocol::Http2, replay))) => {
                        Box::pin(serve_h2(ours, client, ours_asking, deadlines, replay)).await;
                    }
                    // Closed having said nothing, or failed before saying enough.
                    Ok(None) | Err(_) => {}
                },
                Some(tls) => {
                    Box::pin(serve_tls(ours, client, ours_asking, deadlines, &tls, lent)).await;
                }
            }
        };
        // From accept to the first request, whichever server takes the connection: the
        // detector and the engine's HTTP/2 server wait for bytes with no deadline of their
        // own, so a connection that never says anything, or stops part way through the
        // HTTP/2 preface, is bounded here.
        let cut_off = {
            let mut serving = std::pin::pin!(serving);
            let mut first = std::pin::pin!(tokio::time::sleep(deadlines.first_request));
            std::future::poll_fn(|cx| {
                // An error here is the end of one connection: the peer went away or spoke
                // nonsense. There is nobody to tell.
                if serving.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(false);
                }
                if !asked.get() && first.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(true);
                }
                Poll::Pending
            })
            .await
        };
        if let Some(stream) = back.take() {
            if cut_off {
                // It has been sent nothing, so there is no answer a reset could take with
                // it, and lingering would only hold the connection longer.
                drop(stream);
            } else {
                // Dropped with bytes the client sent still unread — the rest of an upload
                // the engine never read — the connection would be reset, and the reset can
                // take the answer the client has not read yet with it. So it lingers
                // instead.
                linger(stream, linger::QUIET, linger::MOST).await;
            }
        }
    }

    /// Carries a connection of a `tcp` or `tls` listener to a backend of its route, byte for
    /// byte, and counts how it ended ([17 §4](../../docs/17-tcp-and-tls-passthrough.md)).
    /// `by_name`: the route is the one whose hostnames cover the name the ClientHello asks
    /// for, rather than the listener's one route.
    async fn pass_through(self: Rc<Self>, listener: usize, mut client: TcpStream, by_name: bool) {
        let connection = Connection::open(Rc::clone(&self), listener);
        let ended = self.carry_through(listener, &mut client, by_name).await;
        if let Some(counters) = self.proxy.metrics.listener(listener) {
            counters.tunnel(ended);
        }
        drop(connection);
    }

    /// The tunnel. A `tls` listener reads the ClientHello into a block of the worker's,
    /// which then carries the client's bytes on, so that what was read goes to the backend
    /// first and unchanged; the tunnel gives it back once it has.
    async fn carry_through(
        &self,
        listener: usize,
        client: &mut TcpStream,
        by_name: bool,
    ) -> Tunnel {
        let (name, hello) = if by_name {
            let Ok(mut block) = self.blocks.borrow_mut().take() else {
                return Tunnel::Exhausted;
            };
            match self.read_hello(client, &mut block).await {
                Ok(name) => (Some(name), Some(block)),
                Err(ended) => {
                    self.blocks.borrow_mut().give(block);
                    return ended;
                }
            }
        } else {
            (None, None)
        };
        let routed = self.pass_route(listener, name.as_deref());
        let (address, idle) = match routed {
            Ok(routed) => routed,
            Err(ended) => {
                if let Some(block) = hello {
                    self.blocks.borrow_mut().give(block);
                }
                return ended;
            }
        };
        self.proxy.metrics.socket(Socket::Opened);
        let Ok(mut backend) =
            connect_within(self.limits.connect, TcpStream::connect(address)).await
        else {
            if let Some(block) = hello {
                self.blocks.borrow_mut().give(block);
            }
            return Tunnel::ConnectFailed;
        };
        let _unset = backend.set_nodelay(true);
        let bounds = TunnelBounds {
            idle,
            drain_within: self.deadlines.drain,
            websocket: false,
        };
        carry(
            client,
            &mut backend,
            hello,
            None,
            &self.blocks,
            bounds,
            &self.timers,
            &self.drain,
        )
        .await
        .into()
    }

    /// The route of a connection of a `tcp` or `tls` listener, from the config in force now,
    /// and a backend's endpoint: its address, and the listener's idle bound.
    fn pass_route(
        &self,
        listener: usize,
        name: Option<&str>,
    ) -> Result<(SocketAddr, Duration), Tunnel> {
        let snapshot = self.proxy.current.load();
        let Some(compiled) = snapshot.listener(listener) else {
            return Err(Tunnel::Refused);
        };
        let route = match (&compiled.l4, name) {
            (Some(L4::Tcp(route)), _) => Some(route),
            (Some(L4::Tls(routes)), Some(name)) => routes.route(name),
            _ => None,
        };
        let Some(route) = route else {
            return Err(Tunnel::Refused);
        };
        // A backend with no endpoint refuses its share of connections, as TLSRoute has
        // it for a backend that cannot be used.
        let Some(upstream) = route.backends.pick(random()) else {
            return Err(Tunnel::NoBackend);
        };
        let destinations = snapshot.destinations.of(upstream.0);
        let healthy = |at: usize| destinations.get(at).is_some_and(|d| d.is_healthy());
        let Some(identity) = pick_healthy(destinations.len(), random(), healthy)
            .and_then(|at| snapshot.destinations.at(upstream.0, at))
        else {
            return Err(Tunnel::NoBackend);
        };
        Ok((identity.address(), compiled.tunnel_idle))
    }

    /// Reads a TLS client's ClientHello into `into`, within the first-request deadline
    /// and [`hello::LIMIT`], for the host name it asks for (17 §3).
    async fn read_hello(&self, client: &mut TcpStream, into: &mut Block) -> Result<String, Tunnel> {
        let mut alarm = Alarm::new(&self.timers, None);
        let due = Instant::now() + self.deadlines.first_request;
        std::future::poll_fn(|cx| {
            loop {
                match hello::read(into.data()) {
                    Hello::Whole(Some(name)) => return Poll::Ready(Ok(name)),
                    // Asking for no name, it asks for no route.
                    Hello::Whole(None) | Hello::Refused(_) => {
                        return Poll::Ready(Err(Tunnel::Refused));
                    }
                    Hello::More => {}
                }
                let mut read = ReadBuf::new(into.room());
                if read.remaining() == 0 {
                    return Poll::Ready(Err(Tunnel::Refused));
                }
                match Pin::new(&mut *client).poll_read(cx, &mut read) {
                    Poll::Ready(Ok(())) => {
                        let count = read.filled().len();
                        // Gone before saying enough to be routed.
                        if count == 0 {
                            return Poll::Ready(Err(Tunnel::Refused));
                        }
                        into.arrived(count);
                    }
                    Poll::Ready(Err(_)) => return Poll::Ready(Err(Tunnel::Failed)),
                    Poll::Pending => {
                        if alarm.poll_until(cx, due).is_ready() {
                            return Poll::Ready(Err(Tunnel::TooSlow));
                        }
                        return Poll::Pending;
                    }
                }
            }
        })
        .await
    }

    /// Serves HTTP/3 on `socket`, the UDP socket of the listener at position `listener` of
    /// [`Proxy::listeners`], until the worker drains and its last connection has gone
    /// ([16](../../docs/16-http3.md)). The listener's TLS, and whether it has every client
    /// prove its address first, are those of the config in force when a connection comes.
    /// `forwarding` is this worker's share of the listener's
    /// [`Forwarding`] group: where a datagram for another worker's connection is handed,
    /// and where this worker's are handed to it.
    ///
    /// # Errors
    ///
    /// A socket with no address of its own, or BoringSSL failing to set up what the
    /// connection IDs are made with.
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where every connection and request is a task.
    pub async fn serve_h3(
        self: Rc<Self>,
        listener: usize,
        socket: UdpSocket,
        forwarding: Forwarding,
    ) -> io::Result<()> {
        let deadlines = self.deadlines;
        let settings = h3::Settings {
            first_request: deadlines.first_request,
            keep_alive: deadlines.next_request,
            stream_idle: deadlines.idle,
            drain_within: deadlines.drain,
            ..h3::Settings::default()
        };
        let counting = Arc::clone(&self.proxy);
        let count = Box::new(move |event| {
            if let Some(counters) = counting.metrics.listener(listener) {
                counters.quic(event);
            }
        });
        let shared = h3_listener::Shared::new(
            socket,
            settings,
            &self.proxy.quic,
            self.position,
            Rc::clone(&self.timers),
            Rc::clone(&self.drain),
            Rc::clone(self.blocks.borrow().storage()),
            count,
        )
        .map_err(io::Error::other)?;
        let reading = Rc::clone(&self);
        let in_force = move || {
            let snapshot = reading.proxy.current.load();
            let tls = snapshot.tls.get(listener).cloned().flatten()?;
            let force_retry = snapshot
                .listener(listener)
                .and_then(|listener| listener.http3)
                .is_some_and(|http3| http3.force_retry);
            Some(h3_listener::InForce { tls, force_retry })
        };
        let answering = Rc::clone(&self);
        let respond = Rc::new(
            move |request: Request<RequestBody>, interim, client: Rc<Client>| {
                Rc::clone(&answering).handle(listener, client, request, Some(interim))
            },
        );
        let dating = Rc::clone(&self);
        let date = Rc::new(move || dating.date.get());
        let opening = Rc::clone(&self);
        let opened = move || Connection::open(Rc::clone(&opening), listener);
        h3_listener::serve(Rc::new(shared), in_force, respond, date, opened, forwarding).await;
        Ok(())
    }

    /// Answers a request that came in on `listener`. `interim` is where the server that
    /// read it wants the upstream's interim answers, if it passes them on
    /// ([14 §5](../../docs/14-downstream-server.md)).
    ///
    /// Not itself `async`: a future of its own would hold the request as well as the
    /// future it hands it to, and every stream's task is made by copying it.
    fn handle(
        self: Rc<Self>,
        listener: usize,
        client: Rc<Client>,
        request: Request<RequestBody>,
        interim: Option<Interim>,
    ) -> impl Future<Output = Answered<Body>> {
        let (head, body) = request.into_parts();
        // What the core adds is kept beside the map, in room the worker lends (14 §6).
        let head = MapHead::lent(head, &self.blocks);
        self.handle_head(listener, client, head, body, interim)
    }

    /// The same for a request's head of whatever kind: a map, or the raw head our own
    /// server reads ([14 §6](../../docs/14-downstream-server.md)).
    async fn handle_head<H: Forwarded>(
        self: Rc<Self>,
        listener: usize,
        client: Rc<Client>,
        head: H,
        body: RequestBody,
        interim: Option<Interim>,
    ) -> Answered<Body> {
        let came_in = Instant::now();
        // First, so that every answer the core gives, its own included, carries it.
        let id = self.proxy.request_id(listener);
        let mut answered = self
            .respond_to(listener, &client, head, body, interim, id.as_ref())
            .await;
        self.proxy.advertise(listener, &mut answered);
        if let Some(id) = id {
            tell_id(&mut answered, id);
        }
        if let Some(counters) = self.proxy.metrics.listener(listener) {
            let took = u64::try_from(came_in.elapsed().as_nanos()).unwrap_or(u64::MAX);
            counters.responded(answered.status(), took);
        }
        answered
    }

    async fn respond_to<H: Forwarded>(
        &self,
        listener: usize,
        client: &Client,
        mut head: H,
        mut body: RequestBody,
        interim: Option<Interim>,
        id: Option<&HeaderValue>,
    ) -> Answered<Body> {
        // A request's trailers go no further than the gateway (03 §11): its body ends
        // where they would come, for the upstream, a retry and a mirror alike.
        body.drop_trailers();
        // How the body is to be sent on, worked out from what arrived and before `direct`
        // takes the hop-by-hop fields off it — and before the body itself is touched,
        // because the path is chosen while there is still nothing to undo.
        let sending = sending_for(&head, &body);
        // A gRPC call is answered as one, the gateway's own answers included; read from the
        // head as the client sent it, before any filter touches it (15 §6).
        let call = Call::of(head.version(), head.outgoing(), Instant::now);
        let deadline = call.and_then(|call| call.deadline());
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return self
                .proxy
                .answer_to(listener, Answer::DeadlineExceeded, call)
                .into();
        }
        // What this request's own `Connection` named, read before routing takes the
        // hop-by-hop fields off the head. Afterwards there is nothing left to read them
        // from and everything it named looks like an ordinary field, so a trailer of that
        // name would travel on ([13 §4](../../docs/13-http1-upstream.md)).
        let nominated = crate::hop_by_hop::nominated(head.outgoing());
        let mut directed = match self.proxy.direct(listener, client, &mut head, id) {
            Ok(Directing::Upstream(directed)) => directed,
            Ok(Directing::Redirect(redirect)) => {
                return self.proxy.redirect(listener, redirect, call).into();
            }
            Err(answer) => return self.proxy.answer_to(listener, answer, call).into(),
        };
        let timing = Timing::of(call, directed.rule.as_deref());

        // Credentials can bind the upstream socket to this client, even when the
        // response is successful. Decide after rule filters and before the client
        // dispatches. On HTTP/2 there is no connection of one client's to bind them to,
        // and they are not sent at all (15 §5).
        let multiplexed = directed.endpoint.protocol() == UpstreamProtocol::Http2;
        if crate::upstream::auth::carries_credentials(head.outgoing()) {
            if multiplexed {
                return self
                    .proxy
                    .answer_to(listener, Answer::ConnectionAuth, call)
                    .into();
            }
            if let Err(rejection) = head.close_connection() {
                return self
                    .proxy
                    .answer_to(listener, rejection.into(), call)
                    .into();
            }
        }

        // What asks the backend to switch is the gateway's own, its key included: in place
        // of the client's `Upgrade`, `Connection` (a `close` for credentials among it: a
        // refused handshake's connection is kept from the pool instead) and key (19 §2).
        if let Some(handshake) = &directed.websocket {
            let asked = match &handshake.toward {
                // An extended CONNECT to an HTTP/1.1 backend is RFC 6455's GET (19 §3).
                Toward::Upgrade(ours) => {
                    if head.method() != Method::GET {
                        head.set_method(Method::GET);
                    }
                    head.set_field(http::header::UPGRADE, websocket::WEBSOCKET)
                        .and_then(|()| {
                            head.set_field(http::header::CONNECTION, websocket::UPGRADE_OPTION)
                        })
                        .and_then(|()| {
                            head.set_field(http::header::SEC_WEBSOCKET_KEY, ours.value())
                        })
                }
                // An extended CONNECT has no key (RFC 8441 §5); its method is the
                // exchange's to write.
                Toward::Connect => head.remove_where(|name| {
                    name.eq_ignore_ascii_case(http::header::SEC_WEBSOCKET_KEY.as_str().as_bytes())
                }),
            };
            if let Err(rejection) = asked {
                return self
                    .proxy
                    .answer_to(listener, rejection.into(), call)
                    .into();
            }
        }

        // Before either client looks for a connection or opens one: a place is what
        // entitles a request to a connection, so it is taken before one is sought. The same
        // bound whichever client carries the request, so that the two are compared doing
        // the same work ([14 §2](../../docs/14-downstream-server.md)).
        let Some(admitted) = self.admit() else {
            return self.proxy.answer_to(listener, Answer::TooBusy, call).into();
        };
        let body = if directed.mirrors.is_empty() {
            body
        } else {
            let mirrors = std::mem::take(&mut directed.mirrors);
            self.mirror(mirrors, &head, &nominated, sending, body)
        };
        let retry = directed.rule.as_ref().and_then(|rule| rule.retry());
        let outcome = match retry {
            // Only a rule that asks pays for keeping the body and the loop around it.
            // Boxed: a future is as big as its biggest state, and every request would
            // otherwise carry room for the retry loop's.
            Some(retry) => {
                Box::pin(self.with_retries(
                    &directed, &mut head, &nominated, sending, body, admitted, interim, timing,
                    retry,
                ))
                .await
            }
            None => {
                let endpoint = Arc::clone(&directed.endpoint);
                self.attempt(
                    &directed, &endpoint, &head, &nominated, sending, body, admitted, interim,
                    timing,
                )
                .await
            }
        };
        match outcome {
            // A gRPC call's answer that is gRPC's own ends with one status, whatever becomes
            // of it; any other answer goes on as it came, for the client to read (15 §6).
            // Only the answer that goes to the client: one a retry set aside is not counted.
            Ok(Answered::Map(response))
                if call.is_some() && is_grpc_answer(response.status(), response.headers()) =>
            {
                let (parts, body) = response.into_parts();
                let called = Called {
                    proxy: Arc::clone(&self.proxy),
                    listener,
                };
                let answered = GrpcAnswered::counted(body, &parts.headers, timing.deadline, called);
                Answered::Map(Response::from_parts(parts, Body::Grpc(Box::new(answered))))
            }
            // Any other answer is cut off where its deadline passes, as one that failed.
            Ok(answered) => match timing.deadline {
                Some(deadline) => timed(answered, deadline, &self.timers),
                None => answered,
            },
            Err(answer) => self.proxy.answer_to(listener, answer, call).into(),
        }
    }

    /// One try at an answer from `endpoint`, by the client its protocol calls for: the
    /// upstream's answer, edited, or the reason there is none, which the caller answers
    /// with (or tries again for).
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn attempt<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        let timing = timing.for_try();
        if endpoint.protocol() == UpstreamProtocol::Http2 {
            if let Some(handshake) = directed.websocket.as_deref() {
                return self
                    .connect_by_h2(
                        directed, endpoint, head, handshake, admitted, interim, timing,
                    )
                    .await;
            }
            return self
                .respond_by_h2(
                    directed,
                    endpoint,
                    head,
                    head.method(),
                    sending,
                    body,
                    admitted,
                    interim,
                    timing,
                )
                .await;
        }
        let exchanged = pin!(self.by_ours(
            directed,
            endpoint,
            head,
            nominated,
            sending,
            body,
            admitted,
            interim,
            timing.head_by_rule,
        ));
        let answered = by_deadline(&self.timers, timing.deadline, exchanged)
            .await
            .unwrap_or(Err(timing.lapsed()));

        let upstream = self.proxy.metrics.upstream(directed.upstream_slot);
        let (mut answer, body) = match answered {
            Ok(answered) => answered,
            // The worker's own storage running out, or a client's body that cannot be read,
            // is not the upstream failing, and is not counted as though it were
            // ([14 §8](../../docs/14-downstream-server.md)).
            Err(
                answer @ (Answer::Exhausted
                | Answer::BadBody
                | Answer::BodyTimedOut
                | Answer::DeadlineExceeded),
            ) => return Err(answer),
            Err(answer) => {
                if let Some(upstream) = upstream {
                    upstream.failures.inc();
                }
                return Err(answer);
            }
        };
        if let Some(upstream) = upstream {
            upstream.responded(answer.status());
        }
        // A name the answer's own `Connection` gave does not travel on, and is not declared
        // onwards either; then what is about the upstream's connection comes off, and the
        // rule's changes are made (14 §6).
        let nominated = crate::hop_by_hop::nominated(&answer);
        let changes = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref());
        // Read before the hop-by-hop fields come off: RFC 9110 §15.5.22 has a 426 name the
        // protocol it wants, and WebSocket is one the gateway can switch to (19 §2).
        let offers = answer.status() == StatusCode::UPGRADE_REQUIRED
            && directed.upgradable
            && websocket::offered(&answer);
        let edited = answer.filter_declaration(&nominated).and_then(|()| {
            answer.strip();
            changes.map_or(Ok(()), |changes| answer.apply(changes))
        });
        // A 101 says it switched to WebSocket, with the Accept of the client's own key; the
        // backend's was of the gateway's.
        let handshake = directed.websocket.as_deref();
        let upgraded = match handshake.and_then(|handshake| handshake.client.as_ref()) {
            Some(client) if answer.status() == StatusCode::SWITCHING_PROTOCOLS => answer
                .set_field(http::header::UPGRADE, websocket::WEBSOCKET)
                .and_then(|()| {
                    answer.set_field(http::header::SEC_WEBSOCKET_ACCEPT, client.accept_value())
                }),
            _ if offers => answer.set_field(http::header::UPGRADE, websocket::WEBSOCKET),
            _ => Ok(()),
        };
        if edited.is_err() || upgraded.is_err() {
            return Err(Answer::Edits);
        }
        // An extended CONNECT's client is told of the switch with a 200 (RFC 8441 §5), and
        // must never be told 2xx of anything else: to a CONNECT that opens the tunnel.
        if handshake.is_some_and(|handshake| handshake.client.is_none()) {
            return connected_answer(answer.status(), answer.into_parts(), body);
        }
        // Written in this hop's version and not the upstream's: "Intermediaries that
        // process HTTP messages ... MUST send their own HTTP-version in forwarded messages"
        // (RFC 9110 §6.2). Our writer says HTTP/1.1, and so does a map made for HTTP/2.
        Ok(Answered::Raw(answer, body))
    }

    /// Tries, and tries again while the rule's retry says to, the budget allows and the
    /// body was kept whole (03 §6). Each try goes to an endpoint drawn afresh; each waits
    /// its backoff first; none goes past the request's deadline. What decides is the
    /// answer's head alone — its status, or a gRPC status a trailers-only head carries —
    /// so nothing of an answer has gone to the client when a request is sent again.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn with_retries<H: Forwarded>(
        &self,
        directed: &Directed,
        head: &mut H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
        retry: &CompiledRetry,
    ) -> Result<Answered<Body>, Answer> {
        let deadline = timing.deadline;
        let (tee, recorded) = Tee::new(body);
        let mut body = RequestBody::Recorded(Box::new(tee));
        let mut admitted = admitted;
        let mut interim = interim;
        let mut endpoint = Arc::clone(&directed.endpoint);
        self.budget(directed.upstream_slot, |budget| {
            budget.deposit(Instant::now());
        });
        let upstream = || self.proxy.metrics.upstream(directed.upstream_slot);
        let mut tried = 0;
        loop {
            let outcome = self
                .attempt(
                    directed,
                    &endpoint,
                    head,
                    nominated,
                    sending,
                    body,
                    admitted,
                    interim.take(),
                    timing,
                )
                .await;
            if tried >= retry.attempts || !wants_again(retry, &outcome) {
                return outcome;
            }
            let Some(replayed) = recorded.replay() else {
                if let Some(upstream) = upstream() {
                    upstream.retries_unkept.inc();
                }
                return outcome;
            };
            if !self.budget(directed.upstream_slot, |budget| {
                budget.withdraw(Instant::now())
            }) {
                if let Some(upstream) = upstream() {
                    upstream.retries_over_budget.inc();
                }
                return outcome;
            }
            let wait = backoff(retry, tried);
            if deadline.is_some_and(|deadline| Instant::now() + wait >= deadline) {
                return outcome;
            }
            // A place for the next try before this one's is given back with its answer:
            // a worker at its bound keeps the answer it has rather than lose it.
            let Some(next) = self.admit() else {
                return outcome;
            };
            let Some((target, drawn)) = directed.draw(head.uri()) else {
                return outcome;
            };
            drop(outcome);
            tokio::time::sleep(wait).await;
            if let Some(upstream) = upstream() {
                upstream.retries.inc();
            }
            head.set_uri(target);
            endpoint = drawn;
            body = RequestBody::Replayed(replayed);
            admitted = next;
            tried += 1;
        }
    }

    /// Sends a copy of the request to each of `mirrors`, each its own exchange on a task
    /// of its own that nothing waits for, and gives back the body the request is to be
    /// sent with, which copies itself to them as it goes (03 §6). Each copy takes a place
    /// as any exchange does; a worker without one to give sends none. Its answer is read
    /// and thrown away.
    fn mirror<H: Forwarded>(
        &self,
        mirrors: Vec<Mirrored>,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
    ) -> RequestBody {
        let given_up = |slot: usize,
                        counter: fn(
            &crate::metrics::UpstreamCounters,
        ) -> &edgerush_telemetry::Counter| {
            if let Some(upstream) = self.proxy.metrics.upstream(slot) {
                counter(upstream).inc();
            }
        };
        // Credentials bound to the client's connection are not the mirror's to use, and
        // without them the copy would not be the request.
        use crate::upstream::auth::carries_credentials;
        let upstream_carries = carries_credentials(head.outgoing());
        let mirrors: Vec<Mirrored> = mirrors
            .into_iter()
            .filter(|mirror| {
                let bound = mirror
                    .fields
                    .as_ref()
                    .map_or(upstream_carries, carries_credentials);
                if bound {
                    given_up(mirror.upstream_slot, |counters| {
                        &counters.mirrors_credentials
                    });
                }
                !bound
            })
            .collect();
        if mirrors.is_empty() {
            return body;
        }
        let Some(worker) = self.me.upgrade() else {
            return body;
        };
        let placed: Vec<_> = mirrors
            .into_iter()
            .filter_map(|mirror| match self.admit() {
                Some(admitted) => Some((mirror, admitted)),
                None => {
                    given_up(mirror.upstream_slot, |counters| &counters.mirrors_busy);
                    None
                }
            })
            .collect();
        if placed.is_empty() {
            return body;
        }
        // The request as it goes upstream, for the copies of it; made only if one is.
        let mut going = None;
        let (tee, copies) = mirror::Tee::new(body, placed.len());
        for ((mut mirror, admitted), (copy, kept)) in placed.into_iter().zip(copies) {
            let headers = match mirror.fields.take() {
                // A copy made before the changes after it, and so before what the rest of
                // the way does.
                Some(own) => own,
                None => going
                    .get_or_insert_with(|| {
                        let mut headers = HeaderMap::new();
                        head.outgoing().each_field(|name, value| {
                            if let (Ok(name), Ok(value)) =
                                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
                            {
                                headers.append(name, value);
                            }
                        });
                        headers
                    })
                    .clone(),
            };
            let (mut parts, ()) = Request::new(()).into_parts();
            parts.method = head.method().clone();
            parts.uri = mirror.target;
            parts.headers = headers;
            // Nobody is there to be told to go on: a copy is sent without asking.
            parts.headers.remove(http::header::EXPECT);
            let nominated = nominated.to_vec();
            let worker = Rc::clone(&worker);
            let _copying = tokio::task::spawn_local(async move {
                let directed = Directed {
                    rule: None,
                    upstream_slot: mirror.upstream_slot,
                    endpoint: Arc::clone(&mirror.endpoint),
                    others: None,
                    mirrors: Vec::new(),
                    websocket: None,
                    upgradable: false,
                };
                // A copy given up on is let go of at once, its exchange and place with it:
                // what sends it may be waiting for room its upstream will never give, and
                // would learn of it only at its idle bound.
                {
                    let mut copying = pin!(async {
                        let answered = worker
                            .attempt(
                                &directed,
                                &mirror.endpoint,
                                &parts,
                                &nominated,
                                sending,
                                RequestBody::Copy(copy),
                                admitted,
                                None,
                                Timing::FIXED,
                            )
                            .await;
                        // Read to its end, so that its connection can carry another request.
                        let mut body = match answered {
                            Ok(Answered::Raw(_, body)) => body,
                            Ok(Answered::Map(response)) => response.into_body(),
                            Err(_) => Body::Empty,
                        };
                        while let Some(Ok(_)) =
                            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
                        {
                        }
                    });
                    let mut given_up = pin!(kept.given_up());
                    std::future::poll_fn(|cx| {
                        if copying.as_mut().poll(cx).is_ready()
                            || given_up.as_mut().poll(cx).is_ready()
                        {
                            return Poll::Ready(());
                        }
                        Poll::Pending
                    })
                    .await;
                }
                if kept.fell_behind()
                    && let Some(upstream) = worker.proxy.metrics.upstream(mirror.upstream_slot)
                {
                    upstream.mirrors_behind.inc();
                }
            });
        }
        RequestBody::Mirrored(Box::new(tee))
    }

    /// This worker's retry budget for the upstream in `slot`, for `act` to use.
    fn budget<T>(&self, slot: usize, act: impl FnOnce(&mut Budget) -> T) -> T {
        let mut budgets = self.budgets.borrow_mut();
        let budget = budgets
            .entry(slot)
            .or_insert_with(|| Budget::new(Instant::now()));
        act(budget)
    }

    /// The answer of an HTTP/2 upstream, edited as an HTTP/1 upstream's would be: what its
    /// `Connection` named and what is about its connection comes off — h2 lets none of
    /// the latter through, and this does not rest on it — and the rule's changes are made.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn respond_by_h2<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        method: &Method,
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        let storage = Rc::clone(self.blocks.borrow().storage());
        let bounds = self.h2_bounds(timing);
        let exchanging = pin!(h2_exchange::exchange(
            &self.h2,
            endpoint,
            method,
            head.uri(),
            head.outgoing(),
            sending,
            body,
            &storage,
            interim,
            bounds,
            timing.told,
            || {
                if let Some(upstream) = self.proxy.metrics.upstream(directed.upstream_slot) {
                    upstream.retries.inc();
                }
            },
        ));
        let upstream = self.proxy.metrics.upstream(directed.upstream_slot);
        let Some(exchanged) = by_deadline(&self.timers, timing.deadline, exchanging).await else {
            let lapsed = timing.lapsed();
            if lapsed == Answer::UpstreamTimedOut
                && let Some(upstream) = upstream
            {
                upstream.failures.inc();
            }
            return Err(lapsed);
        };
        let (parts, answer) = exchanged.map_err(|error| h2_failed(error, upstream))?;
        if let Some(upstream) = upstream {
            upstream.responded(parts.status);
        }
        let watch = Watch {
            proxy: Arc::clone(&self.proxy),
            upstream: directed.upstream_slot,
        };
        let mut response = Response::from_parts(parts, Body::H2(Box::new(answer), admitted, watch));
        let headers = response.headers_mut();
        let nominated = crate::hop_by_hop::nominated(&*headers);
        crate::h1::filter_declaration(headers, &nominated);
        crate::hop_by_hop::strip_response(headers);
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(headers);
        }
        Ok(Answered::Map(response))
    }

    /// By EdgeRush's own path, the one there is.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn by_ours<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        head_by_rule: bool,
    ) -> Result<(RawAnswer, Body), Answer> {
        // The server that read a handshake takes its backend from here once the 101 is
        // answered.
        let listening = directed.websocket.as_ref().and_then(|_| interim.clone());
        let answer = match self
            .through_h1(
                endpoint,
                head.method(),
                head.uri(),
                head.outgoing(),
                nominated,
                sending,
                body,
                interim,
                head_by_rule,
                directed.websocket.is_some(),
            )
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                self.proxy.metrics.stopped(why_stopped(&error));
                return Err(match error {
                    ExchangeError::Exhausted(_) => Answer::Exhausted,
                    ExchangeError::RequestBody(cause)
                        if matches!(
                            cause.downcast_ref::<RequestBodyError>(),
                            Some(RequestBodyError::TimedOut)
                        ) =>
                    {
                        Answer::BodyTimedOut
                    }
                    ExchangeError::RequestBody(_) => Answer::BadBody,
                    // The client's upload stopping, seen by the exchange's clock for it
                    // rather than the body's own: the same client, as slow either way.
                    ExchangeError::Idle {
                        waiting: Stalled::Client,
                        ..
                    } => Answer::BodyTimedOut,
                    // Only what the upstream was to do before its head. The client's own
                    // upload stopping is not the upstream out of time.
                    ExchangeError::TooSlow { .. }
                    | ExchangeError::Idle {
                        waiting: Stalled::Upstream | Stalled::Answer,
                        ..
                    } => Answer::UpstreamTimedOut,
                    _ => Answer::UpstreamFailed,
                });
            }
        };
        let (read, mut body) = answer;
        if let Some(handshake) = &directed.websocket {
            if read.status() == StatusCode::SWITCHING_PROTOCOLS {
                return self.switch(read, *body, handshake, listening);
            }
            // A refused handshake's connection could carry another request, but not one
            // whose credentials may have bound it to this client (13 §6).
            if crate::upstream::auth::carries_credentials(head.outgoing()) {
                body.not_kept();
            }
        }
        // Nothing need ever poll an empty body, so its connection would otherwise sit
        // until the body object was dropped.
        if body.is_end_stream() {
            body.settle();
        }
        // The place goes with the body, which is what is still being worked on. Every
        // other way out of here has dropped it already.
        let watch = Watch {
            proxy: Arc::clone(&self.proxy),
            upstream: directed.upstream_slot,
        };
        Ok((read, Body::Ours(body, admitted, watch)))
    }

    /// A handshake's 101 (19 §2). Only one that says it switched to the WebSocket asked for,
    /// with the Accept of the gateway's own key, is taken: its connection goes to the server
    /// that read the handshake, left with its interim channel, and the 101 goes on with
    /// nothing after it. Any other is the backend failing, and its connection is closed.
    fn switch(
        &self,
        read: RawAnswer,
        body: H1Body<UpstreamSocket, RequestBody>,
        handshake: &Handshake,
        listening: Option<Interim>,
    ) -> Result<(RawAnswer, Body), Answer> {
        let Toward::Upgrade(ours) = &handshake.toward else {
            return Err(Answer::UpstreamFailed);
        };
        let switched = listening.filter(|_| websocket::switched(&read, ours));
        let Some((interim, (backend, leftover))) = switched.zip(body.into_switched()) else {
            self.proxy.metrics.stopped(Stopped::Codec);
            return Err(Answer::UpstreamFailed);
        };
        interim.switch(self.switched(Backend::Socket(backend), leftover, handshake));
        Ok((read, Body::Empty))
    }

    /// A switched WebSocket's backend, with what its tunnel needs of this worker, and its
    /// end counted among its listener's tunnels.
    fn switched(
        &self,
        backend: Backend,
        leftover: Option<Block>,
        handshake: &Handshake,
    ) -> Switched {
        let proxy = Arc::clone(&self.proxy);
        let listener = handshake.listener;
        Switched {
            backend,
            leftover,
            bounds: TunnelBounds {
                idle: handshake.idle,
                drain_within: self.deadlines.drain,
                websocket: true,
            },
            blocks: Rc::clone(&self.blocks),
            timers: Rc::clone(&self.timers),
            drain: Rc::clone(&self.drain),
            ended: Box::new(move |carried: Carried| {
                if let Some(counters) = proxy.metrics.listener(listener) {
                    counters.tunnel(carried.into());
                }
            }),
        }
    }

    /// A WebSocket handshake to an HTTP/2 backend: an extended CONNECT if the connection
    /// announces them, and otherwise the plain request it came as, a GET (19 §4).
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn connect_by_h2<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        handshake: &Handshake,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        let storage = Rc::clone(self.blocks.borrow().storage());
        let bounds = self.h2_bounds(timing);
        let connecting = pin!(h2_exchange::connect(
            &self.h2,
            endpoint,
            head.uri(),
            head.outgoing(),
            &storage,
            bounds,
        ));
        let upstream = self.proxy.metrics.upstream(directed.upstream_slot);
        let Some(connected) = by_deadline(&self.timers, timing.deadline, connecting).await else {
            let lapsed = timing.lapsed();
            if lapsed == Answer::UpstreamTimedOut
                && let Some(upstream) = upstream
            {
                upstream.failures.inc();
            }
            return Err(lapsed);
        };
        let (mut parts, body) = match connected.map_err(|error| h2_failed(error, upstream))? {
            Connected::NotOffered => {
                let answered = self
                    .respond_by_h2(
                        directed,
                        endpoint,
                        head,
                        &Method::GET,
                        Sending::None,
                        RequestBody::None,
                        admitted,
                        interim,
                        timing,
                    )
                    .await?;
                return match (&handshake.client, answered) {
                    (None, Answered::Map(response)) => {
                        let (parts, body) = response.into_parts();
                        connected_answer(parts.status, parts, body)
                    }
                    (_, answered) => Ok(answered),
                };
            }
            Connected::Switched(parts, stream) => {
                let Some(interim) = interim else {
                    return Err(Answer::UpstreamFailed);
                };
                interim.switch(self.switched(Backend::H2(stream), None, handshake));
                (parts, Body::Empty)
            }
            Connected::Refused(parts, answer) => {
                let watch = Watch {
                    proxy: Arc::clone(&self.proxy),
                    upstream: directed.upstream_slot,
                };
                (parts, Body::H2(Box::new(answer), admitted, watch))
            }
        };
        if let Some(upstream) = upstream {
            upstream.responded(parts.status);
        }
        let switched = parts.status.is_success();
        let headers = &mut parts.headers;
        let nominated = crate::hop_by_hop::nominated(&*headers);
        crate::h1::filter_declaration(headers, &nominated);
        crate::hop_by_hop::strip_response(headers);
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(headers);
        }
        // To an HTTP/1.1 client, the switch is its 101, with the Accept of its own key.
        if switched && let Some(client) = &handshake.client {
            parts.status = StatusCode::SWITCHING_PROTOCOLS;
            parts
                .headers
                .insert(http::header::UPGRADE, websocket::WEBSOCKET);
            parts
                .headers
                .insert(http::header::SEC_WEBSOCKET_ACCEPT, client.accept_value());
        } else if switched {
            parts.status = StatusCode::OK;
        }
        Ok(Answered::Map(Response::from_parts(parts, body)))
    }

    /// What an HTTP/2 exchange is held to, for a try held to `timing`.
    fn h2_bounds(&self, timing: Timing) -> H2Bounds {
        H2Bounds {
            final_head: (!timing.head_by_rule).then_some(self.limits.final_head),
            idle: self.limits.idle,
            continue_wait: self.limits.continue_wait,
            interim_heads: self.limits.interim_heads,
            interim_bytes: self.limits.interim_bytes,
        }
    }
}

/// What an HTTP/2 exchange that failed is answered with, counted against `upstream` where
/// it was the upstream's failing: waiting for a place, or the client's own body, is not.
fn h2_failed(
    error: h2_exchange::ExchangeError,
    upstream: Option<&crate::metrics::UpstreamCounters>,
) -> Answer {
    let answer = match error {
        h2_exchange::ExchangeError::Place(PlaceError::Full) => Answer::QueueFull,
        h2_exchange::ExchangeError::Place(PlaceError::TimedOut) => Answer::QueueTimedOut,
        h2_exchange::ExchangeError::RequestBody(cause)
            if matches!(
                cause.downcast_ref::<RequestBodyError>(),
                Some(RequestBodyError::TimedOut)
            ) =>
        {
            Answer::BodyTimedOut
        }
        h2_exchange::ExchangeError::RequestBody(_) => Answer::BadBody,
        h2_exchange::ExchangeError::TooSlow { .. } | h2_exchange::ExchangeError::Idle { .. } => {
            Answer::UpstreamTimedOut
        }
        _ => Answer::UpstreamFailed,
    };
    if matches!(answer, Answer::UpstreamFailed | Answer::UpstreamTimedOut)
        && let Some(upstream) = upstream
    {
        upstream.failures.inc();
    }
    answer
}

/// An extended CONNECT's answer, from its backend's (19 §3, §4): a switch, 101 from an
/// HTTP/1.1 backend, is told as 200 (RFC 8441 §5), with no Accept, which has no key to be of;
/// any other 2xx is answered 502 — to a CONNECT every 2xx opens the tunnel (RFC 9110
/// §9.3.6), and a page the backend served in place of the switch would be read as
/// WebSocket frames — and the backend's answer dropped, its connection with it; anything
/// else goes as it came.
fn connected_answer(
    status: StatusCode,
    mut parts: http::response::Parts,
    body: Body,
) -> Result<Answered<Body>, Answer> {
    if status == StatusCode::SWITCHING_PROTOCOLS {
        parts.status = StatusCode::OK;
        parts.headers.remove(http::header::SEC_WEBSOCKET_ACCEPT);
        parts.headers.remove(http::header::UPGRADE);
        return Ok(Answered::Map(Response::from_parts(parts, body)));
    }
    if status.is_success() {
        return Err(Answer::UpstreamFailed);
    }
    Ok(Answered::Map(Response::from_parts(parts, body)))
}

impl Proxy {
    /// An answer of the data plane's own for a request that may be a gRPC `call`: for one
    /// that is, `200` and the status gRPC gives the cause, with nothing after the head — a
    /// trailers-only answer, which is how gRPC answers a call it fails before any message
    /// (15 §6). Counted by its reason either way.
    /// Says on `answered` that the listener serves HTTP/3 as well, if it does. Every answer
    /// carries it, HTTP/3's own too, which keeps what a client remembers fresh.
    fn advertise(&self, listener: usize, answered: &mut Answered<Body>) {
        let snapshot = self.current.load();
        let Some(alt_svc) = snapshot.alt_svc.get(listener).and_then(Option::as_ref) else {
            return;
        };
        match answered {
            // An overlay holds as many fields as a rule adds and more; this is one.
            Answered::Raw(answer, _) => {
                let _added = answer.apply(alt_svc);
            }
            Answered::Map(response) => alt_svc.apply(response.headers_mut()),
        }
    }

    /// An ID for a request that came in on `listener`, if it gives its requests one (08 §3 in
    /// the docs). Worked out once, so that a config that changes while the request is
    /// served cannot have it told one ID and not the other.
    fn request_id(&self, listener: usize) -> Option<HeaderValue> {
        let snapshot = self.current.load();
        if snapshot.listener(listener)?.request_id != RequestId::Generate {
            return None;
        }
        Some(request_id::value(unix_millis(), unguessable()?))
    }

    fn answer_to(&self, listener: usize, answer: Answer, call: Option<Call>) -> Response<Body> {
        let mut response = self.answer(listener, answer);
        if call.is_some() {
            let (code, why) = answer.grpc();
            if let Some(counters) = self.metrics.listener(listener) {
                counters.called(code as usize);
            }
            *response.status_mut() = StatusCode::OK;
            let headers = response.headers_mut();
            headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/grpc"),
            );
            headers.insert("grpc-status", code.value());
            headers.insert("grpc-message", crate::grpc::status::message(why));
        }
        response
    }

    /// An answer of the data plane's own, counted by its reason.
    fn answer(&self, listener: usize, answer: Answer) -> Response<Body> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.answered(answer);
        }
        let mut response = Response::new(Body::Empty);
        *response.status_mut() = answer.status();
        response.extensions_mut().insert(h1::Local);
        response
    }

    /// A redirect's answer: its status, its `Location` and no body, with its rule's changes to
    /// the answer's headers; counted among the data plane's own. A gRPC call is answered as
    /// one, as it is with the data plane's other answers
    /// ([15 §6](../../docs/15-http2-and-grpc.md)): no gRPC client follows a redirect.
    fn redirect(&self, listener: usize, redirect: Redirect, call: Option<Call>) -> Response<Body> {
        if call.is_some() {
            return self.answer_to(listener, Answer::Redirected, call);
        }
        let mut response = self.answer(listener, Answer::Redirected);
        *response.status_mut() = redirect.status;
        response
            .headers_mut()
            .insert(http::header::LOCATION, redirect.location);
        if let Some(changes) = redirect
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(response.headers_mut());
        }
        response
    }

    /// Makes the head of a request that came in on a listener's socket the head of the
    /// request to send, target included, or says what to answer instead. All of it is done
    /// on one snapshot, which is let go of before anything is waited for; what is kept for
    /// the response is the rule, and only if it has something to do to the response, and
    /// the slot of the upstream's counters.
    fn direct<H: Forwarded>(
        &self,
        listener: usize,
        client: &Client,
        head: &mut H,
        id: Option<&HeaderValue>,
    ) -> Result<Directing, Answer> {
        let snapshot = self.current.load();
        let came_on = listener;
        let listener = snapshot
            .listeners
            .get(listener)
            .copied()
            .flatten()
            .and_then(|position| snapshot.config.listeners.get(position))
            .ok_or(Answer::NoRoute)?;
        let forward = match decide(&snapshot.config, listener, head, client, &mut random, id)? {
            Decision::Forward(forward) => forward,
            Decision::Redirect(redirected) => {
                return Ok(Directing::Redirect(Redirect {
                    rule: redirected
                        .rule
                        .response_headers
                        .is_some()
                        .then(|| Arc::clone(redirected.rule)),
                    status: redirected.status,
                    location: redirected.location,
                }));
            }
        };
        // An upstream the snapshot does not have is not known to happen.
        let upstream = forward.upstream.0;
        let endpoints = snapshot.endpoints.get(upstream).ok_or(Answer::NoBackend)?;
        let upstream_slot = *snapshot
            .upstream_slots
            .get(upstream)
            .ok_or(Answer::NoBackend)?;
        let destinations = snapshot.destinations.of(upstream);
        let at = pick_healthy(destinations.len(), random(), |at| {
            destinations
                .get(at)
                .is_some_and(|destination| destination.is_healthy())
        })
        .ok_or(Answer::NoEndpoints)?;
        let endpoint = endpoints.get(at).ok_or(Answer::NoEndpoints)?;
        let identity = snapshot
            .destinations
            .at(upstream, at)
            .ok_or(Answer::NoEndpoints)?;

        let target = at_endpoint(head.uri(), endpoint).ok_or(Answer::BadTarget)?;
        head.set_uri(target);
        let upgradable = head.version() == Version::HTTP_11;
        head.onward();
        // A handshake goes on as HTTP/1.1's upgrade to a backend spoken to in HTTP/1.1, and
        // as an extended CONNECT to one spoken to in HTTP/2 (19 §2, §4).
        let websocket = forward.websocket.as_ref().and_then(|opening| {
            let client = match opening {
                Opening::Upgrade(key) => Some(Key::read(key.as_bytes())?),
                Opening::Connect => None,
            };
            let toward = match identity.protocol() {
                UpstreamProtocol::Http1 => Toward::Upgrade(Key::of(unguessable()?)),
                UpstreamProtocol::Http2 => Toward::Connect,
            };
            Some(Box::new(Handshake {
                client,
                toward,
                idle: forward
                    .rule
                    .timeouts()
                    .and_then(|timeouts| timeouts.tunnel_idle)
                    .unwrap_or(TUNNEL_IDLE),
                listener: came_on,
            }))
        });
        if let Some(counters) = self.metrics.upstream(upstream_slot) {
            counters.requests.inc();
        }
        let mut mirrors = Vec::new();
        // The mirrors that take this request were drawn with it, and a copy made for each
        // placed before a change (18 §5).
        for mirroring in forward.mirrors {
            let Some(mirror) = forward.rule.mirrors.get(mirroring.mirror) else {
                continue;
            };
            let (target, fields) = match mirroring.own {
                Some(copied) => (copied.target, Some(copied.fields)),
                None => (head.uri().clone(), None),
            };
            let upstream = mirror.upstream.0;
            let Some(&slot) = snapshot.upstream_slots.get(upstream) else {
                continue;
            };
            // A mirror could only ever be sent a WebSocket's handshake, never its messages.
            if forward.websocket.is_some() {
                if let Some(counters) = self.metrics.upstream(slot) {
                    counters.mirrors_upgrade.inc();
                }
                continue;
            }
            let destinations = snapshot.destinations.of(upstream);
            let found = pick_healthy(destinations.len(), random(), |at| {
                destinations
                    .get(at)
                    .is_some_and(|destination| destination.is_healthy())
            })
            .and_then(|at| {
                let authority = snapshot.endpoints.get(upstream)?.get(at)?;
                Some((at_endpoint(&target, authority)?, destinations.get(at)?))
            });
            let counters = self.metrics.upstream(slot);
            let Some((target, destination)) = found else {
                if let Some(counters) = counters {
                    counters.mirrors_nowhere.inc();
                }
                continue;
            };
            if let Some(counters) = counters {
                counters.requests.inc();
            }
            mirrors.push(Mirrored {
                upstream_slot: slot,
                endpoint: Arc::clone(destination),
                target,
                fields,
            });
        }
        let kept = forward.rule.response_headers.is_some()
            || forward.rule.retry().is_some()
            || forward.rule.timeouts().is_some();
        // Only a request that may be sent again keeps where else it could go.
        let others = forward.rule.retry().map(|_| {
            endpoints
                .iter()
                .cloned()
                .zip(destinations.iter().map(Arc::clone))
                .collect()
        });
        Ok(Directing::Upstream(Directed {
            rule: kept.then(|| Arc::clone(forward.rule)),
            upstream_slot,
            endpoint: Arc::clone(identity),
            others,
            mirrors,
            websocket,
            upgradable,
        }))
    }
}

/// What becomes of a request, decided on one snapshot.
enum Directing {
    /// It goes upstream.
    Upstream(Directed),
    /// It is answered with a redirect.
    Redirect(Redirect),
}

/// What a redirected request keeps of the snapshot it was decided on.
struct Redirect {
    /// The rule, only if it changes the answer's headers.
    rule: Option<Arc<CompiledRule>>,
    status: StatusCode,
    location: http::HeaderValue,
}

/// What a request keeps of the snapshot it was directed on.
struct Directed {
    rule: Option<Arc<CompiledRule>>,
    upstream_slot: usize,
    /// The endpoint this request was directed to, taken from the same snapshot as the
    /// route so that no reload can come between the two.
    endpoint: Arc<ReuseIdentity>,
    /// Every endpoint of the upstream, for a request its rule may send again: each try
    /// draws afresh.
    others: Option<Vec<(Authority, Arc<ReuseIdentity>)>>,
    /// Where the copies of it go, for a request its rule mirrors.
    mirrors: Vec<Mirrored>,
    /// For a WebSocket handshake carried to an HTTP/1.1 backend (19 §2). Boxed: rare, and
    /// every request's future holds this across its waits.
    websocket: Option<Box<Handshake>>,
    /// Whether its client spoke HTTP/1.1, which alone has `Upgrade`: a 426 that offers
    /// WebSocket says so to it, and to no other (19 §2).
    upgradable: bool,
}

/// A WebSocket handshake as the gateway carries it (19 §2 to §4).
struct Handshake {
    /// The key an HTTP/1.1 client sent, whose Accept its 101 carries; none for an extended
    /// CONNECT, which has none and is answered 200.
    client: Option<Key>,
    /// How it goes to the backend.
    toward: Toward,
    /// How long the tunnel may carry nothing: the rule's, or an hour.
    idle: Duration,
    /// The listener it came in on, whose tunnels it is counted among.
    listener: usize,
}

/// How a WebSocket handshake goes to its backend.
enum Toward {
    /// To one spoken to in HTTP/1.1: a GET that asks to upgrade, with a key of the gateway's
    /// own, whose Accept its 101 must carry — one the client never saw, so that a 101 the
    /// backend hands back for it cannot pass.
    Upgrade(Key),
    /// To one spoken to in HTTP/2: an extended CONNECT, if its connection announces them.
    Connect,
}

/// A WebSocket's idle bound where its rule states none (19 §5).
const TUNNEL_IDLE: Duration = Duration::from_secs(3600);

/// Where one copy of a request goes: drawn with the request, from the same snapshot.
struct Mirrored {
    upstream_slot: usize,
    endpoint: Arc<ReuseIdentity>,
    target: Uri,
    /// The fields as they stood at the mirror's place, before a change after it; none for
    /// a copy of the request as it goes upstream.
    fields: Option<HeaderMap>,
}

impl Directed {
    /// An endpoint drawn afresh for another try, and the target at it.
    fn draw(&self, target: &Uri) -> Option<(Uri, Arc<ReuseIdentity>)> {
        let others = self.others.as_ref()?;
        let at = pick_healthy(others.len(), random(), |at| {
            others
                .get(at)
                .is_some_and(|(_, destination)| destination.is_healthy())
        })?;
        let (authority, destination) = others.get(at)?;
        Some((at_endpoint(target, authority)?, Arc::clone(destination)))
    }
}

/// `exchange`'s outcome, or nothing if `deadline` comes first, kept in the worker's
/// `timers`. The exchange is pinned by the caller, where it is, and not moved into a
/// future of its own here: a future moved into another keeps its room in both, and these
/// are the biggest part of a request's.
async fn by_deadline<F: Future>(
    timers: &Rc<Timers>,
    deadline: Option<Instant>,
    mut exchange: Pin<&mut F>,
) -> Option<F::Output> {
    let Some(deadline) = deadline else {
        return Some(exchange.await);
    };
    // Boxed: only a request with a deadline has one, and the rest should not carry room
    // for it.
    let mut alarm = Box::new(Alarm::new(timers, None));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(outcome) = exchange.as_mut().poll(cx) {
            return Poll::Ready(Some(outcome));
        }
        alarm.poll_until(cx, deadline).map(|()| None)
    })
    .await
}

/// What a request is held to in time ([03 §6](../../docs/03-data-plane.md)), worked out
/// once its rule is known.
#[derive(Debug, Clone, Copy)]
struct Timing {
    /// When it is out of time: the earlier of its rule's `request` timeout, counted from
    /// when it is routed, a moment after its head came, and a gRPC call's own deadline.
    deadline: Option<Instant>,
    /// The deadline an upstream is told of as `grpc-timeout`. Only a call that said one
    /// has one told on, as the time left of the earlier of the two: a rule's timeout is
    /// not the client's to have sent.
    told: Option<Instant>,
    /// Whether the rule's own timeouts bound the wait for an answer's head, in place of
    /// the fixed clocks for it.
    head_by_rule: bool,
    /// How long each try may take to its answer's head: the rule's `backend_request`.
    per_try: Option<Duration>,
    /// Whether `deadline` is a try's rather than the request's: set for a try whose own
    /// clock runs out first.
    try_first: bool,
}

impl Timing {
    /// Held to the fixed clocks alone, as a mirror's copy is: nothing waits on it, and
    /// nobody's deadline is its.
    const FIXED: Self = Self {
        deadline: None,
        told: None,
        head_by_rule: false,
        per_try: None,
        try_first: false,
    };

    /// A request's, from its gRPC call if it is one and its rule if it has one. No clock
    /// is read for a rule that states no timeout.
    fn of(call: Option<Call>, rule: Option<&CompiledRule>) -> Self {
        let called = call.and_then(|call| call.deadline());
        let timeouts = rule.and_then(CompiledRule::timeouts);
        let request = timeouts.and_then(|timeouts| timeouts.request);
        let backend_request = timeouts.and_then(|timeouts| timeouts.backend_request);
        let ruled = match request {
            Some(Timeout::After(after)) => Some(Instant::now() + after),
            Some(Timeout::Off) | None => None,
        };
        let deadline = match (called, ruled) {
            (Some(called), Some(ruled)) => Some(called.min(ruled)),
            (called, ruled) => called.or(ruled),
        };
        Self {
            deadline,
            told: called.and(deadline),
            head_by_rule: request.is_some() || backend_request.is_some(),
            per_try: match backend_request {
                Some(Timeout::After(after)) => Some(after),
                Some(Timeout::Off) | None => None,
            },
            try_first: false,
        }
    }

    /// A try's, starting now: held to the earlier of the request's deadline and its own
    /// clock, which runs from its start — connecting and waiting for a place included — to
    /// its answer's head. An upstream is told the earlier too: it is all it will be waited
    /// for.
    fn for_try(self) -> Self {
        let Some(per_try) = self.per_try else {
            return self;
        };
        let ends = Instant::now() + per_try;
        if self.deadline.is_some_and(|deadline| deadline <= ends) {
            return self;
        }
        Self {
            deadline: Some(ends),
            told: self.told.map(|told| told.min(ends)),
            try_first: true,
            ..self
        }
    }

    /// What a try is answered with when its deadline passes before its answer's head: the
    /// upstream out of time if the try's own clock ran out, the request's deadline passed
    /// if that did.
    fn lapsed(self) -> Answer {
        if self.try_first {
            Answer::UpstreamTimedOut
        } else {
            Answer::DeadlineExceeded
        }
    }
}

/// `answered`, its body cut off, as a body that failed, if it is still coming at
/// `deadline`. An answer with nothing to come is left as it is: it has nothing to cut.
fn timed(answered: Answered<Body>, deadline: Instant, timers: &Rc<Timers>) -> Answered<Body> {
    let wrap = |body: Body| {
        if body.is_end_stream() {
            return body;
        }
        Body::Timed(Box::new(Timed {
            body,
            deadline,
            alarm: Alarm::new(timers, None),
        }))
    };
    match answered {
        Answered::Raw(answer, body) => Answered::Raw(answer, wrap(body)),
        Answered::Map(response) => Answered::Map(response.map(wrap)),
    }
}

/// Whether an outcome is one the rule's retry sends a request again for: an answer whose
/// status it names, a gRPC call whose trailers-only head carries a status it names, an
/// upstream that could not be reached or answered nothing, as a `502`, or a try that ran
/// out of time before its head, if the rule says `on_timeout` (and not as a `502`: a rule
/// that sends a request again for an upstream it cannot reach may not want to for one that
/// is slow).
fn wants_again(retry: &CompiledRetry, outcome: &Result<Answered<Body>, Answer>) -> bool {
    match outcome {
        Ok(Answered::Raw(answer, _)) => retry.on_status(answer.status().as_u16()),
        Ok(Answered::Map(response)) => {
            retry.on_status(response.status().as_u16())
                || response.headers().get("grpc-status").is_some_and(|status| {
                    retry.on_grpc(crate::grpc::status::code_of(status.as_bytes()))
                })
        }
        Err(Answer::UpstreamFailed) => retry.on_status(502),
        Err(Answer::UpstreamTimedOut) => retry.on_timeout,
        Err(_) => false,
    }
}

/// The wait before try `tried + 1`: the base doubled for each try before, no more than the
/// most, and as much again at random — linkerd's backoff, with its jitter of one.
fn backoff(retry: &CompiledRetry, tried: u32) -> Duration {
    let doubled = retry
        .backoff_base
        .saturating_mul(1 << tried.min(16))
        .min(retry.backoff_max);
    let jitter = (random() >> 11) as f64 / (1_u64 << 53) as f64;
    doubled + doubled.mul_f64(jitter)
}

/// How a request's body is to be sent on.
///
/// Read from the request as it arrived rather than from the body's own account of itself:
/// a body that says how long it is may still end with trailers, and over HTTP/2 it always
/// may. What is certain is what the client framed it as
/// ([13 §4](../../docs/13-http1-upstream.md)).
fn sending_for<H: Forwarded>(head: &H, body: &RequestBody) -> Sending {
    // The engine says outright when there is no body, and that is the one thing a length
    // alone would not settle. A client that said its body is a length of nothing goes on
    // saying so: RFC 9110 §8.6 has a sender state a length for a method whose content
    // means something, and a server may answer 411 without one. One that said nothing,
    // as an HTTP/2 request ended by its headers does, is sent no framing either.
    if body.is_end_stream() {
        return match request_length(head.outgoing()) {
            Some(0) => Sending::Length(0),
            _ => Sending::None,
        };
    }
    if matches!(head.version(), Version::HTTP_2 | Version::HTTP_3) {
        // Framed as frames, with trailers allowed after any of them. There is no length
        // here that would still be true by the end.
        return Sending::Chunked;
    }
    if crate::hop_by_hop::is_chunked_request(head.outgoing()) {
        return Sending::Chunked;
    }
    match request_length(head.outgoing()) {
        Some(length) => Sending::Length(length),
        // No length and no coding, over HTTP/1.1, is no body at all.
        None => Sending::None,
    }
}

/// A request's `Content-Length`, where it has exactly one that is a plain number. Hyper
/// has already refused what it will refuse; anything left that does not read as a length
/// is treated as no length, and the body is framed by this end instead.
fn request_length<F: Fields + ?Sized>(headers: &F) -> Option<u64> {
    let mut lengths = headers.values(&http::header::CONTENT_LENGTH);
    let only = lengths.next()?;
    if lengths.next().is_some() {
        return None;
    }
    // What `HeaderValue::to_str` takes, visible ASCII and tabs, before it is read as text.
    if !only
        .iter()
        .all(|&byte| (32..127).contains(&byte) || byte == b'\t')
    {
        return None;
    }
    std::str::from_utf8(only).ok()?.trim().parse().ok()
}

/// A connection that came in on a listener's socket: what its requests share, and what
/// counts it as open until the last of them is done, wherever that happens.
struct Connection {
    worker: Rc<Worker>,
    listener: usize,
}

impl Connection {
    fn open(worker: Rc<Worker>, listener: usize) -> Self {
        if let Some(counters) = worker.proxy.metrics.listener(listener) {
            counters.accepted.inc();
            counters.active.inc();
        }
        Self { worker, listener }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(counters) = self.worker.proxy.metrics.listener(self.listener) {
            counters.active.dec();
        }
    }
}

/// Why a data plane cannot run a config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProxyError {
    /// An endpoint address that cannot be written into a request target.
    #[error("endpoint {0} cannot be part of a request target")]
    Endpoint(SocketAddr),
    /// An upstream's TLS that cannot be set up: an authority that is not a certificate.
    #[error("upstream `{upstream}`: {error}")]
    UpstreamTls {
        /// The upstream's name.
        upstream: String,
        /// What is wrong.
        error: TlsError,
    },
    /// A listener's certificates that cannot be served.
    #[error("listener `{listener}`: {error}")]
    Tls {
        /// The listener's name.
        listener: String,
        /// What is wrong with them.
        error: TlsError,
    },
    /// BoringSSL could not give the keys the data plane makes at start.
    #[error("no random keys to be had: {0}")]
    Random(String),
}

fn authority(endpoint: &SocketAddr) -> Result<Authority, ProxyError> {
    Authority::try_from(endpoint.to_string()).map_err(|_| ProxyError::Endpoint(*endpoint))
}

/// One of the endpoints, each as likely as any other; `None` if there are none.
fn pick_at(endpoints: usize, random: u64) -> Option<usize> {
    let count = u64::try_from(endpoints).ok()?;
    usize::try_from(random.checked_rem(count)?).ok()
}

/// One of the endpoints that `healthy` says serve, each as likely as any other — unless
/// fewer than half of them do, when their health is set aside and any may be picked:
/// Envoy's panic threshold, at its default of half. Probes that fail most of an upstream
/// are more likely wrong themselves, or about to put the rest under a load that fails them
/// too, than a reason to answer every request 503.
///
/// Costs one look when the endpoint first drawn serves, which is the usual case.
fn pick_healthy(endpoints: usize, random: u64, healthy: impl Fn(usize) -> bool) -> Option<usize> {
    let first = pick_at(endpoints, random)?;
    if healthy(first) {
        return Some(first);
    }
    let serving = (0..endpoints).filter(|at| healthy(*at)).count();
    if serving * 2 < endpoints {
        return Some(first);
    }
    // Another draw, among those that serve.
    let nth = pick_at(serving, random / endpoints as u64)?;
    (0..endpoints).filter(|at| healthy(*at)).nth(nth)
}

/// The same path and query, at the endpoint: the form in which the client is told where to
/// connect. What it sends is the origin form, and the `Host` field is left as it is.
fn at_endpoint(target: &Uri, endpoint: &Authority) -> Option<Uri> {
    let mut parts = target.clone().into_parts();
    parts.scheme = Some(Scheme::HTTP);
    parts.authority = Some(endpoint.clone());
    Uri::from_parts(parts).ok()
}

/// What a worker's HTTP/2 client is held to, from the worker's bounds (15 §3, §4).
fn h2_settings(limits: &H1Limits) -> H2Settings {
    H2Settings {
        pool: H2Limits {
            streams: limits.h2_streams,
            connections: limits.h2_connections,
            connections_total: limits.idle_total,
            waiting: limits.h2_waiting,
            // One connection opened at a time for a destination; demand queued meanwhile
            // is served by it, or has the next opened (15 §4).
            dialing: 1,
            // Well short of the 2^30 streams a connection's identifiers allow.
            requests: 1 << 29,
            age: limits.max_age,
            idle: limits.idle_timeout,
        },
        connect: limits.connect,
        stream_window: 1 << 20,
        connection_window: 16 << 20,
        header_list: 64 * 1024,
        send_buffer: 400 * 1024,
        closing: Duration::from_secs(10),
    }
}

/// Seconds since the Unix epoch, for dating answers.
pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Milliseconds since the Unix epoch, for a request's ID: read for each, since the date the
/// worker keeps is of whole seconds.
fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Tells the client the request's ID on `answered`, in place of any the upstream gave.
fn tell_id<B>(answered: &mut Answered<B>, id: HeaderValue) {
    match answered {
        // An overlay holds as many fields as a rule adds and more; this is one.
        Answered::Raw(answer, _) => {
            let _set = answer.set_field(request_id::HEADER, id);
        }
        Answered::Map(response) => {
            response.headers_mut().insert(request_id::HEADER, id);
        }
    }
}

/// Whether a failure to accept is the failure of the one connection that was next in line,
/// so that the one after it can be accepted at once.
pub(crate) fn is_about_one_connection(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::rt::Executor;
    use hyper_util::rt::TokioIo;

    /// Where the futures hyper's HTTP/2 client spawns go: this worker's `LocalSet`.
    #[derive(Debug, Clone, Copy, Default)]
    struct OnThisWorker;

    impl<F: Future<Output = ()> + 'static> Executor<F> for OnThisWorker {
        fn execute(&self, future: F) {
            let _detached = tokio::task::spawn_local(future);
        }
    }
    use edgerush_config::{Config, compile};
    use http::StatusCode;
    use http_body_util::{BodyExt, Empty};
    use std::num::NonZeroUsize;
    use std::rc::Rc;
    use std::time::Duration;

    /// What a future `make` returns takes, without one being made.
    fn size_of_made<A, F>(_make: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    /// A connection's task is as large as its future, from accept to close, whatever the
    /// connection turns out to be (14 §3). HTTP/2 and TLS hold far more than plain HTTP/1
    /// does waiting between requests, and are boxed once a connection is found to be one of
    /// them: the future is no larger than plain HTTP/1 needs, and what serving any
    /// connection holds besides — its first request's deadline, the detector, its
    /// deadlines and handles on it — which is well under what either would add.
    #[test]
    fn a_connection_is_no_larger_than_plain_http1_needs() {
        use crate::downstream::detect::Replay;
        type Plain = (Rc<Connection>, Rc<Client>, Rc<Cell<bool>>, Replay<Lent>);
        type Secured = (
            Rc<Connection>,
            Rc<Client>,
            Rc<Cell<bool>>,
            Deadlines,
            &'static Tls,
            Lent,
        );
        type Http2 = (
            Rc<Connection>,
            Rc<Client>,
            Rc<Cell<bool>>,
            Deadlines,
            Replay<Lent>,
        );
        let connection = size_of_made(|(worker, stream): (Rc<Worker>, TcpStream)| {
            worker.serve_connection(0, stream)
        });
        let plain = size_of_made(|(ours, client, asking, socket): Plain| {
            serve_h1(ours, client, asking, socket)
        });
        let tls = size_of_made(|(ours, client, asking, deadlines, tls, socket): Secured| {
            serve_tls(ours, client, asking, deadlines, tls, socket)
        });
        let h2 = size_of_made(|(ours, client, asking, deadlines, socket): Http2| {
            serve_h2(ours, client, asking, deadlines, socket)
        });
        assert!(
            connection <= plain + 768,
            "{connection} bytes; plain HTTP/1 {plain}, TLS {tls}, HTTP/2 {h2}"
        );
    }

    /// A connect that never completes is given up on at the limit and not before, and one
    /// that completes inside it is kept (linkerd2-proxy tests its connect timeout the same
    /// way, with a connector that never finishes).
    #[tokio::test(start_paused = true)]
    async fn a_connect_that_never_completes_is_given_up_on_at_its_limit() {
        let limit = Duration::from_secs(5);
        let started = tokio::time::Instant::now();
        let failed = connect_within(limit, std::future::pending::<io::Result<()>>())
            .await
            .unwrap_err();
        assert!(
            matches!(&failed, ExchangeError::Io(error) if error.kind() == io::ErrorKind::TimedOut),
            "{failed}"
        );
        assert_eq!(started.elapsed(), limit);

        let slow = async {
            tokio::time::sleep(limit - Duration::from_millis(1)).await;
            Ok::<_, io::Error>("connected")
        };
        assert_eq!(connect_within(limit, slow).await.unwrap(), "connected");

        let refused = async { Err::<(), _>(io::Error::from(io::ErrorKind::ConnectionRefused)) };
        let failed = connect_within(limit, refused).await.unwrap_err();
        assert!(
            matches!(&failed, ExchangeError::Io(error) if error.kind() == io::ErrorKind::ConnectionRefused),
            "{failed}"
        );
    }

    /// The worker's sweep lets go of the blocks a burst left parked, so that a worker that
    /// has gone quiet holds only what a quiet worker keeps ([13 §7](../../docs/13-http1-upstream.md)).
    #[tokio::test(start_paused = true)]
    async fn a_sweep_lets_go_of_what_a_burst_left_parked() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let limits = H1Limits::default();
                let worker =
                    Worker::with_limits(sending_to("127.0.0.1:9".parse().unwrap()), limits);
                {
                    let mut blocks = worker.blocks.borrow_mut();
                    let burst: Vec<_> = (0..20).map(|_| blocks.take().unwrap()).collect();
                    for block in burst {
                        blocks.give(block);
                    }
                    assert_eq!(blocks.parked(), 20);
                }
                let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
                tokio::time::sleep(limits.sweep + Duration::from_millis(1)).await;
                let blocks = worker.blocks.borrow();
                assert_eq!(
                    blocks.parked(),
                    blocks.sizes().kept,
                    "the burst is still parked"
                );
            })
            .await;
    }

    /// Deadlines short enough that the tests of them run on real sockets and real time: a
    /// stopped clock jumps ahead whenever every task waits on a socket, which a loopback
    /// round trip does, and that would fire a deadline nobody reached.
    const SHORT: Deadlines = Deadlines {
        first_request: Duration::from_millis(300),
        next_request: Duration::from_millis(700),
        idle: Duration::from_millis(500),
        drain: Duration::from_millis(900),
    };

    /// How late a deadline may be seen to fire on a loaded machine.
    const SLACK: Duration = Duration::from_millis(400);

    /// How early a deadline may be seen to fire. A server's clock starts at what the client
    /// sees only afterwards — the accept, before the client's first write; the end of the
    /// answer, before the client has read it and written again — so a deadline kept to the
    /// microsecond looks that much early from the client's side. Far less than any wrong
    /// deadline would be.
    const EARLY: Duration = Duration::from_millis(50);

    /// Serves a worker for `upstream` on a listener of its own, and says where.
    /// Serves `socket` with `worker`, whose timers are waited on beside it as its
    /// maintenance would wait on them.
    fn serving(worker: &Rc<Worker>, socket: TcpListener) -> tokio::task::JoinHandle<()> {
        let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
        tokio::task::spawn_local(Rc::clone(worker).serve(0, socket))
    }

    async fn serving_worker(upstream: SocketAddr) -> SocketAddr {
        let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        front
    }

    /// Reads from `stream` until the proxy closes it, and says how long that took. Bounded
    /// well past every deadline under test, so that a deadline missing fails rather than
    /// hangs.
    async fn closed_after(stream: &mut TcpStream) -> Duration {
        use tokio::io::AsyncReadExt;
        let started = tokio::time::Instant::now();
        let mut rest = Vec::new();
        let _ended = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
            .await
            .expect("the connection was never closed");
        started.elapsed()
    }

    /// Reads one answer of the counting upstream's (`ok`, and its head) off `stream`.
    async fn answered(stream: &mut TcpStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut seen = Vec::new();
        let mut byte = [0; 1];
        while !seen.ends_with(b"\r\n\r\nok") {
            let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
                .await
                .expect("no answer")
                .unwrap();
            assert_ne!(
                read,
                0,
                "closed before answering: {}",
                String::from_utf8_lossy(&seen)
            );
            seen.push(byte[0]);
        }
        String::from_utf8(seen).unwrap()
    }

    const ASKED: &[u8] = b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n";

    /// A connection that has not finished its first request head is closed at its first
    /// request deadline from being accepted, whether it never said anything, stalled part
    /// way through the HTTP/2 preface — where protocol detection, the engine's or ours,
    /// waits with no deadline of its own — or is trickling a head. Without this each of
    /// them holds its connection for ever.
    #[tokio::test]
    async fn a_connection_without_a_first_request_is_closed_at_its_deadline() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                for said in [
                    &b""[..],
                    b"PRI * HT",
                    // All of the preface but its last byte.
                    &crate::downstream::detect::PREFACE[..23],
                    b"GET / HTTP/1.1\r\nhost: exa",
                ] {
                    let mut stream = TcpStream::connect(front).await.unwrap();
                    stream.write_all(said).await.unwrap();
                    let took = closed_after(&mut stream).await;
                    assert!(
                        took + EARLY >= SHORT.first_request && took < SHORT.first_request + SLACK,
                        "{:?}: closed after {took:?}",
                        String::from_utf8_lossy(said)
                    );
                }
            })
            .await;
    }

    /// A worker started as the data plane starts one, serving and maintained, keeps its
    /// deadlines: its maintenance is what waits on its timers, and without it a client
    /// that has been answered and asks nothing more would be held for ever.
    #[tokio::test]
    async fn a_maintained_worker_keeps_its_deadlines() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream.write_all(ASKED).await.unwrap();
                answered(&mut stream).await;
                let took = closed_after(&mut stream).await;
                assert!(
                    took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                    "closed after {took:?}"
                );
            })
            .await;
    }

    /// After an answer, a connection waiting for its next request head — idle, or with
    /// part of one — is closed at its next request deadline.
    #[tokio::test]
    async fn a_connection_waiting_for_its_next_request_is_closed_at_its_deadline() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                for then in [&b""[..], b"GET / HT"] {
                    let mut stream = TcpStream::connect(front).await.unwrap();
                    stream.write_all(ASKED).await.unwrap();
                    answered(&mut stream).await;
                    stream.write_all(then).await.unwrap();
                    let took = closed_after(&mut stream).await;
                    assert!(
                        took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                        "{:?}: closed after {took:?}",
                        String::from_utf8_lossy(then)
                    );
                }
            })
            .await;
    }

    /// The deadlines cut off nobody who is keeping to them: a first head sent just inside
    /// its deadline is answered, and so is another request well inside the next.
    #[tokio::test]
    async fn a_connection_that_keeps_to_the_deadlines_is_served() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut stream = TcpStream::connect(front).await.unwrap();
                tokio::time::sleep(SHORT.first_request - Duration::from_millis(150)).await;
                stream.write_all(ASKED).await.unwrap();
                assert!(answered(&mut stream).await.starts_with("HTTP/1.1 200"));
                tokio::time::sleep(SHORT.next_request - Duration::from_millis(250)).await;
                stream.write_all(ASKED).await.unwrap();
                assert!(answered(&mut stream).await.starts_with("HTTP/1.1 200"));
            })
            .await;
    }

    /// An HTTP/2 client of the worker at `front`, past its handshake, over a real socket.
    async fn h2_client(front: SocketAddr) -> crate::h2_peer::Peer<TcpStream> {
        use crate::h2_peer::{self, Peer, flag, kind};
        let stream = TcpStream::connect(front).await.unwrap();
        let mut peer = Peer::open_as_client(stream, &[]).await;
        peer.until(|f| f.kind == kind::SETTINGS && !f.has(flag::ACK))
            .await;
        peer.send(&h2_peer::settings_ack()).await;
        peer
    }

    /// A GET of `path` on stream `id`, ending the stream.
    async fn h2_get(peer: &mut crate::h2_peer::Peer<TcpStream>, id: u32, path: &str) {
        use crate::h2_peer::{self};
        let block = h2_peer::block(&[
            (":method", "GET"),
            (":scheme", "http"),
            (":authority", "example.test"),
            (":path", path),
        ]);
        peer.send(&h2_peer::headers(id, block, true)).await;
    }

    /// An HTTP/2 connection with no stream open is told to go at its keep-alive deadline —
    /// a graceful GOAWAY, naming no last stream yet — and, as this client never answers the
    /// PING that comes with it, closed at its closing bound after that. hyper's HTTP/2
    /// server, before ours, held such a connection for ever.
    #[tokio::test]
    async fn an_idle_http2_connection_is_told_to_go_at_its_keep_alive_deadline() {
        use crate::h2_peer::{code, flag, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut peer = h2_client(front).await;
                h2_get(&mut peer, 1, "/").await;
                peer.until(|f| f.stream == 1 && f.has(flag::END_STREAM))
                    .await;
                let answered = tokio::time::Instant::now();

                let (goaway, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
                let took = answered.elapsed();
                assert!(
                    took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                    "told to go after {took:?}"
                );
                assert_eq!(goaway.goaway(), (0x7fff_ffff, code::NO_ERROR));

                let told = tokio::time::Instant::now();
                let _rest = peer.rest().await;
                let took = told.elapsed();
                let closing = Bounds::default().next_head.min(SHORT.next_request);
                assert!(took < closing + SLACK, "closed {took:?} after GOAWAY");
            })
            .await;
    }

    /// A stream still waiting for its answer keeps its connection: the keep-alive clock
    /// runs only while no stream is open, and starts when the last one ends.
    #[tokio::test]
    async fn an_http2_connection_with_a_stream_open_is_not_idle() {
        use crate::h2_peer::{flag, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, held) = scripted_upstream().await;
                let front = serving_worker(upstream).await;
                let mut peer = h2_client(front).await;
                // Held unanswered by the upstream.
                h2_get(&mut peer, 1, "/held").await;
                let quiet = peer.drain_for(SHORT.next_request * 2).await;
                assert!(
                    quiet.iter().all(|f| f.kind != kind::GOAWAY),
                    "told to go with a stream open: {quiet:?}"
                );

                // The upstream goes away; the stream is answered for it, and ends.
                held.borrow_mut().clear();
                peer.until(|f| f.stream == 1 && f.has(flag::END_STREAM))
                    .await;
                let ended = tokio::time::Instant::now();
                peer.until(|f| f.kind == kind::GOAWAY).await;
                let took = ended.elapsed();
                assert!(
                    took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                    "told to go {took:?} after the last stream ended"
                );
            })
            .await;
    }

    /// A stream the client resets ends with nothing written, and the keep-alive clock still
    /// starts then: the stream's end wakes the connection, where no frame would.
    #[tokio::test]
    async fn the_keep_alive_clock_starts_when_the_client_resets_its_last_stream() {
        use crate::h2_peer::{self, code, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _held) = scripted_upstream().await;
                let front = serving_worker(upstream).await;
                let mut peer = h2_client(front).await;
                h2_get(&mut peer, 1, "/held").await;
                peer.settled().await;
                peer.send(&h2_peer::rst_stream(1, code::CANCEL)).await;
                let reset = tokio::time::Instant::now();
                peer.until(|f| f.kind == kind::GOAWAY).await;
                let took = reset.elapsed();
                assert!(
                    took + EARLY >= SHORT.next_request && took < SHORT.next_request + SLACK,
                    "told to go {took:?} after the reset"
                );
            })
            .await;
    }

    /// An HTTP/2 client of h2's own, over a real socket to `front`, built as `builder` says.
    async fn h2_library_client(
        front: SocketAddr,
        builder: &::h2::client::Builder,
    ) -> ::h2::client::SendRequest<Bytes> {
        let stream = TcpStream::connect(front).await.unwrap();
        let (send, connection) = builder.handshake(stream).await.unwrap();
        let _driving = tokio::task::spawn_local(async move {
            let _ended = connection.await;
        });
        send
    }

    /// An upload that stops while it is waited on is answered 408 at its idle deadline, on
    /// its own stream: the connection and its other streams carry on (14 §8).
    #[tokio::test]
    async fn a_stalled_http2_upload_is_answered_408_at_its_idle_deadline() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _held) = scripted_upstream().await;
                let front = serving_worker(upstream).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://example.test/held")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, mut upload) = send.send_request(request, false).unwrap();
                upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
                let stalled = tokio::time::Instant::now();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .expect("never answered")
                    .unwrap();
                let took = stalled.elapsed();
                assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
                assert!(
                    took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                    "answered after {took:?}"
                );
            })
            .await;
    }

    /// An upload that stops after its answer has begun cannot be answered 408 any more: the
    /// stream is cancelled at the idle deadline, CANCEL and not INTERNAL_ERROR, since it
    /// was the client that stopped.
    #[tokio::test]
    async fn an_http2_upload_stalled_under_its_answer_is_cancelled_at_its_idle_deadline() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _held) = scripted_upstream().await;
                let front = serving_worker(upstream).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://example.test/endless")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, mut upload) = send.send_request(request, false).unwrap();
                upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
                let stalled = tokio::time::Instant::now();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .expect("no head")
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let mut body = response.into_body();
                let error = loop {
                    match tokio::time::timeout(Duration::from_secs(10), body.data())
                        .await
                        .expect("never cancelled")
                    {
                        Some(Ok(data)) => {
                            let _ = body.flow_control().release_capacity(data.len());
                        }
                        Some(Err(error)) => break error,
                        None => panic!("the endless answer ended"),
                    }
                };
                let took = stalled.elapsed();
                assert_eq!(error.reason(), Some(::h2::Reason::CANCEL), "{error:?}");
                assert!(
                    took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                    "cancelled after {took:?}"
                );
            })
            .await;
    }

    /// A client that gives no room for its answer has the stream cancelled at the idle
    /// deadline, and the upstream's answer is let go of with it.
    #[tokio::test]
    async fn an_http2_answer_given_no_room_is_cancelled_at_its_idle_deadline() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut builder = ::h2::client::Builder::new();
                builder.initial_window_size(0);
                let mut send = h2_library_client(front, &builder).await;
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, _) = send.send_request(request, true).unwrap();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .expect("no head")
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let headed = tokio::time::Instant::now();
                let mut body = response.into_body();
                let ended = tokio::time::timeout(Duration::from_secs(10), body.data())
                    .await
                    .expect("never cancelled");
                let took = headed.elapsed();
                let error = match ended {
                    Some(Err(error)) => error,
                    other => panic!("not cancelled: {other:?}"),
                };
                assert_eq!(error.reason(), Some(::h2::Reason::CANCEL), "{error:?}");
                assert!(
                    took + EARLY >= SHORT.idle && took < SHORT.idle + SLACK,
                    "cancelled after {took:?}"
                );
            })
            .await;
    }

    /// An upstream that reads a request's head — and its chunked body to the end, if
    /// `whole` — then says `said` and holds the connection.
    async fn saying_upstream(said: &'static [u8], whole: bool) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = backend.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            loop {
                let (mut stream, _) = backend.accept().await.unwrap();
                let _answering = tokio::task::spawn_local(async move {
                    let mut seen = Vec::new();
                    let mut byte = [0; 1];
                    let end: &[u8] = if whole { b"0\r\n\r\n" } else { b"\r\n\r\n" };
                    while !seen.ends_with(end) {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => seen.push(byte[0]),
                        }
                    }
                    let _ = stream.write_all(said).await;
                    let mut rest = Vec::new();
                    let _ = stream.read_to_end(&mut rest).await;
                });
            }
        });
        address
    }

    /// An upstream that reads a request's head, says `first`, and says `then` only once
    /// `gate` opens: what it said first cannot have waited for what it says after.
    async fn gated_upstream(
        first: &'static [u8],
        gate: tokio::sync::oneshot::Receiver<()>,
        then: &'static [u8],
    ) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = backend.local_addr().unwrap();
        let _answering = tokio::task::spawn_local(async move {
            let (mut stream, _) = backend.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut byte = [0; 1];
            while !seen.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => seen.push(byte[0]),
                }
            }
            let _ = stream.write_all(first).await;
            let _ = gate.await;
            let _ = stream.write_all(then).await;
            let mut rest = Vec::new();
            let _ = stream.read_to_end(&mut rest).await;
        });
        address
    }

    /// The interim heads a response future hands over, until the final head is next.
    async fn interim_heads(
        response: &mut ::h2::client::ResponseFuture,
    ) -> Vec<(StatusCode, http::HeaderMap)> {
        let mut heads = Vec::new();
        while let Some(head) = tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| response.poll_informational(cx)),
        )
        .await
        .expect("no interim head nor final one")
        {
            let head = head.unwrap();
            heads.push((head.status(), head.headers().clone()));
        }
        heads
    }

    /// An upstream's 103 reaches an HTTP/2 client before its final answer, with its fields:
    /// what hyper's HTTP/2 server could not send (14 §5, 15 §1).
    #[tokio::test]
    async fn an_upstream_103_reaches_an_http2_client_before_its_answer() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (open, gate) = tokio::sync::oneshot::channel();
                let upstream = gated_upstream(
                    b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\n",
                    gate,
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
                )
                .await;
                let front = serving_worker(upstream).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (mut response, _) = send.send_request(request, true).unwrap();
                // Heard while the upstream is still working on its answer: the final head is
                // not written until the hint has arrived.
                let hint = tokio::time::timeout(
                    Duration::from_secs(10),
                    std::future::poll_fn(|cx| response.poll_informational(cx)),
                )
                .await
                .expect("the hint waited for the answer")
                .expect("the final head came first")
                .unwrap();
                assert_eq!(hint.status(), StatusCode::EARLY_HINTS);
                assert_eq!(hint.headers()["link"], "</a.css>; rel=preload");
                open.send(()).unwrap();
                assert!(interim_heads(&mut response).await.is_empty());
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
            })
            .await;
    }

    /// A worker serving HTTP/3 for `upstream` on a UDP socket of its own, beside its TCP
    /// listener's configuration: an `https` listener with HTTP/3, for `a.test`.
    async fn serving_h3_worker(upstream: SocketAddr) -> SocketAddr {
        let http3 = edgerush_config::Http3 {
            alt_svc_max_age: 60,
            force_retry: false,
        };
        serving_h3(&h3_config(upstream, http3)).await.0
    }

    /// A config for `upstream` whose `web` listener is `https`, for `a.test`, with `http3`.
    fn h3_config(upstream: SocketAddr, http3: edgerush_config::Http3) -> edgerush_config::Config {
        let mut config = everything_config(upstream);
        let web = config.listeners.get_mut("web").unwrap();
        web.protocol = edgerush_config::Protocol::Https;
        web.tls = Some(edgerush_config::Tls {
            certificates: vec![crate::tls::testing::certificate(&["a.test"])],
            client_validation: None,
        });
        web.http3 = Some(http3);
        config
    }

    /// A worker serving `config`'s `web` listener over HTTP/3 on a UDP socket of its own,
    /// and the proxy, for a test to give a new config.
    async fn serving_h3(config: &edgerush_config::Config) -> (SocketAddr, Arc<Proxy>) {
        let proxy = Arc::new(Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap());
        let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
        let alone = Forwarding::group(1).remove(0);
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone));
        (front, proxy)
    }

    /// A redirect is answered over HTTP/3 as over TCP, and a scheme it does not state is the
    /// listener's, `https` (18 §3).
    #[tokio::test]
    async fn a_redirect_is_answered_over_http3_with_the_listeners_scheme() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::{Answer, Client};
                let field = |answer: &Answer, name: &str| {
                    answer.heads.last().and_then(|head| {
                        head.iter()
                            .find(|(field, _)| field == name)
                            .map(|(_, value)| value.clone())
                    })
                };
                let (upstream, opened) = counting_upstream().await;
                let http3 = edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                };
                let mut config = h3_config(upstream, http3);
                let redirects = [
                    "matches: [{ path: { prefix: /old } }]\n\
                     redirect: { status: 301, path: { replace_prefix: /new }, query: keep }",
                    "matches: [{ path: { prefix: /away } }]\n\
                     redirect: { status: 302, host: www.example.org, query: drop }",
                ];
                for (at, rule) in redirects.into_iter().enumerate() {
                    config.routes[0]
                        .rules
                        .insert(at, serde_saphyr::from_str(rule).unwrap());
                }
                let (front, _) = serving_h3(&config).await;
                let mut client = Client::connect(front, "a.test").await;

                let moved = client.get("a.test", "/old/a?x=1").await;
                assert_eq!(moved.final_status(), Some("301"));
                assert_eq!(field(&moved, "location").as_deref(), Some("/new/a?x=1"));
                assert!(moved.body.is_empty());
                let away = client.get("a.test", "/away/b?y").await;
                assert_eq!(away.final_status(), Some("302"));
                assert_eq!(
                    field(&away, "location").as_deref(),
                    Some("https://www.example.org/away/b")
                );
                assert_eq!(opened.load(Ordering::SeqCst), 0);
            })
            .await;
    }

    /// A listener whose config forces Retry has every client prove its address first,
    /// however few handshakes are under way, and is served after; a config that stops
    /// forcing it applies to the next client, with no new socket (16 §3).
    #[tokio::test]
    async fn an_http3_listener_forces_retry_as_the_config_in_force_says() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::Client;
                // A long header whose type is Retry (RFC 9000 §17.2.5).
                let retry = |datagram: &[u8]| datagram[0] & 0xf0 == 0xf0;
                let (upstream, _) = counting_upstream().await;
                let mut http3 = edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: true,
                };
                let (front, proxy) = serving_h3(&h3_config(upstream, http3)).await;
                let mut client = Client::connect(front, "a.test").await;
                assert!(retry(&client.received[0]), "no Retry first");
                assert_eq!(client.get("a.test", "/").await.final_status(), Some("200"));

                http3.force_retry = false;
                proxy
                    .reload(compile(&h3_config(upstream, http3)).unwrap())
                    .unwrap();
                let mut client = Client::connect(front, "a.test").await;
                assert!(!client.received.iter().any(|datagram| retry(datagram)));
                assert_eq!(client.get("a.test", "/").await.final_status(), Some("200"));
            })
            .await;
    }

    /// An HTTP/3 request goes through the request core to an HTTP/1 upstream, and its
    /// answer comes back, as any request's does.
    #[tokio::test]
    async fn an_http3_request_is_answered_by_the_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::Client;
                let (upstream, _) = counting_upstream().await;
                let front = serving_h3_worker(upstream).await;
                let mut client = Client::connect(front, "a.test").await;
                for path in ["/", "/again"] {
                    let answer = client.get("a.test", path).await;
                    assert_eq!(answer.final_status(), Some("200"), "{path}");
                    assert_eq!(answer.body, b"ok");
                }
            })
            .await;
    }

    /// An upstream's 103 reaches an HTTP/3 client before its final answer, as over HTTP/2.
    #[tokio::test]
    async fn an_upstream_103_reaches_an_http3_client_before_its_answer() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::Client;
                let (open, gate) = tokio::sync::oneshot::channel();
                let upstream = gated_upstream(
                    b"HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload\r\n\r\n",
                    gate,
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
                )
                .await;
                let front = serving_h3_worker(upstream).await;
                let mut client = Client::connect(front, "a.test").await;
                let id = client.request(&crate::downstream::h3::testing::get("a.test", "/"), true);
                // Heard while the upstream is still working on its answer.
                client
                    .until(|client| client.answers.get(&id).is_some_and(|a| !a.heads.is_empty()))
                    .await;
                let hint = client.answers[&id].clone();
                assert_eq!(hint.status(0), Some("103"));
                assert!(
                    hint.heads[0]
                        .iter()
                        .any(|(name, value)| name == "link" && value == "</a.css>; rel=preload")
                );
                open.send(()).unwrap();
                let answer = client.answer(id).await;
                assert_eq!(answer.final_status(), Some("200"));
                assert_eq!(answer.body, b"ok");
            })
            .await;
    }

    /// An HTTP/2 client that asks to be told before it sends its body is told, with a
    /// `100`, and answered once it has sent it.
    #[tokio::test]
    async fn an_http2_client_expecting_continue_is_told_to_send_its_body() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    saying_upstream(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok", true).await;
                let front = serving_worker(upstream).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://example.test/")
                    .version(Version::HTTP_2)
                    .header("expect", "100-continue")
                    .body(())
                    .unwrap();
                let (mut response, mut upload) = send.send_request(request, false).unwrap();
                // Nothing sent until told to: the first thing heard is the 100.
                let told = tokio::time::timeout(
                    Duration::from_secs(10),
                    std::future::poll_fn(|cx| response.poll_informational(cx)),
                )
                .await
                .expect("never told to send")
                .expect("the final head came first")
                .unwrap();
                assert_eq!(told.status(), StatusCode::CONTINUE);
                upload.send_data(Bytes::from_static(b"abc"), true).unwrap();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
            })
            .await;
    }

    /// Where the upstream is not asked to say yes first — a filter takes `Expect` off — the
    /// client is told to send its body as soon as the body is wanted, not after the
    /// continue wait (14 §5).
    #[tokio::test]
    async fn an_http2_client_expecting_continue_is_told_at_once_when_the_upstream_is_not_asked() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    saying_upstream(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok", true).await;
                let yaml = format!(
                    r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        filters:
          - type: request_header_modifier
            remove: [expect]
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
                );
                let config: Config = serde_saphyr::from_str(&yaml).unwrap();
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://example.test/")
                    .version(Version::HTTP_2)
                    .header("expect", "100-continue")
                    .body(())
                    .unwrap();
                let asked = tokio::time::Instant::now();
                let (mut response, mut upload) = send.send_request(request, false).unwrap();
                let told = tokio::time::timeout(
                    Duration::from_secs(10),
                    std::future::poll_fn(|cx| response.poll_informational(cx)),
                )
                .await
                .expect("never told to send")
                .expect("the final head came first")
                .unwrap();
                assert_eq!(told.status(), StatusCode::CONTINUE);
                let took = asked.elapsed();
                assert!(
                    took < H1Limits::default().continue_wait / 2,
                    "told only after {took:?}: by the wait, not the wanted body"
                );
                upload.send_data(Bytes::from_static(b"abc"), true).unwrap();
                let response = tokio::time::timeout(Duration::from_secs(10), response)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
            })
            .await;
    }

    /// The same as [`serving_worker`], with the worker, for a test that looks inside it.
    async fn serving_worker_and(upstream: SocketAddr) -> (SocketAddr, Rc<Worker>) {
        let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// A rapid reset (CVE-2023-44487): streams opened and reset at once, each let settle so
    /// that h2's own bound on resets waiting to be accepted is not what stops it. Past 500
    /// streams, half or more reset before their answer, the connection is told to calm
    /// down and closed (15 §3).
    #[tokio::test]
    async fn a_rapid_reset_is_cut_off_by_its_share_of_early_resets() {
        use crate::h2_peer::{self, code, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _held) = scripted_upstream().await;
                let front = serving_worker(upstream).await;
                let mut peer = h2_client(front).await;
                let mut cut_off = None;
                for n in 0..600u32 {
                    let id = n * 2 + 1;
                    h2_get(&mut peer, id, "/held").await;
                    peer.send_if_open(&h2_peer::rst_stream(id, code::CANCEL))
                        .await;
                    let frames = peer.settled().await;
                    if let Some(goaway) = frames.iter().find(|f| f.kind == kind::GOAWAY) {
                        cut_off = Some((n + 1, goaway.goaway().1));
                        break;
                    }
                }
                let (after, code) = cut_off.expect("never cut off");
                assert_eq!(code, code::ENHANCE_YOUR_CALM);
                assert!(
                    (500..=510).contains(&after),
                    "cut off after {after} streams"
                );
            })
            .await;
    }

    /// A client that cancels now and then — one stream in ten — is nowhere near the rule,
    /// and keeps its connection.
    #[tokio::test]
    async fn a_client_that_cancels_now_and_then_keeps_its_connection() {
        use crate::h2_peer::{self, code, flag, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut peer = h2_client(front).await;
                for n in 0..600u32 {
                    let id = n * 2 + 1;
                    h2_get(&mut peer, id, "/").await;
                    if n % 10 == 0 {
                        peer.send_if_open(&h2_peer::rst_stream(id, code::CANCEL))
                            .await;
                        let frames = peer.settled().await;
                        assert!(frames.iter().all(|f| f.kind != kind::GOAWAY), "at {n}");
                    } else {
                        let (_, before) = peer
                            .until(|f| f.stream == id && f.has(flag::END_STREAM))
                            .await;
                        assert!(before.iter().all(|f| f.kind != kind::GOAWAY), "at {n}");
                    }
                }
            })
            .await;
    }

    /// Streams the client resets while their upstream is still working let go of what they
    /// held: the worker's count of exchanges in hand goes back to nothing.
    #[tokio::test]
    async fn resetting_http2_streams_leaks_no_admission() {
        use crate::h2_peer::{self, code};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, held) = scripted_upstream().await;
                let (front, worker) = serving_worker_and(upstream).await;
                let mut peer = h2_client(front).await;
                for n in 0..20u32 {
                    h2_get(&mut peer, n * 2 + 1, "/held").await;
                }
                until(|| held.borrow().len() == 20).await;
                assert_eq!(worker.in_flight.get(), 20);
                for n in 0..20u32 {
                    peer.send(&h2_peer::rst_stream(n * 2 + 1, code::CANCEL))
                        .await;
                }
                until(|| worker.in_flight.get() == 0).await;
            })
            .await;
    }

    /// The HTTP/2 server holds header lists to 64 KiB, as HTTP/1 holds heads: one under it is
    /// served, one over it answered 431 by h2 before the core sees it, and the connection
    /// carries on (15 §3).
    #[tokio::test]
    async fn an_http2_header_list_past_64_kib_is_answered_431() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let mut statuses = Vec::new();
                for size in [60_000, 70_000] {
                    let request = Request::get("http://example.test/")
                        .version(Version::HTTP_2)
                        .header("x-large", "a".repeat(size))
                        .body(())
                        .unwrap();
                    let (response, _) = send.send_request(request, true).unwrap();
                    let response = tokio::time::timeout(Duration::from_secs(10), response)
                        .await
                        .unwrap()
                        .unwrap();
                    statuses.push(response.status());
                }
                assert_eq!(
                    statuses,
                    vec![StatusCode::OK, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE]
                );
            })
            .await;
    }

    /// A draining worker closes an HTTP/1 connection waiting idle for its next request at
    /// once, and takes no new connection (03 §10).
    #[tokio::test]
    async fn a_draining_worker_closes_idle_http1_connections_and_accepts_nothing() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let (front, worker) = serving_worker_and(upstream).await;
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream.write_all(ASKED).await.unwrap();
                answered(&mut stream).await;
                worker.drain();
                let took = closed_after(&mut stream).await;
                assert!(took < SLACK, "closed {took:?} after the drain began");

                // Nothing new is taken: the listening socket went with the drain, so a
                // connection is refused.
                tokio::task::yield_now().await;
                let late = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(front))
                    .await
                    .expect("connecting hung");
                assert!(late.is_err(), "a draining worker still listens");
            })
            .await;
    }

    /// An HTTP/1 request under way when the worker drains is answered, saying the
    /// connection closes, and then it does.
    #[tokio::test]
    async fn an_http1_answer_in_hand_while_draining_says_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, held) = scripted_upstream().await;
                let (front, worker) = serving_worker_and(upstream).await;
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream
                    .write_all(b"GET /held HTTP/1.1\r\nhost: example.test\r\n\r\n")
                    .await
                    .unwrap();
                until(|| held.borrow().len() == 1).await;
                worker.drain();
                // The upstream goes; the proxy answers for it.
                held.borrow_mut().clear();
                let mut answer = Vec::new();
                tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut answer))
                    .await
                    .expect("never closed")
                    .unwrap();
                let answer = String::from_utf8_lossy(&answer).to_lowercase();
                assert!(answer.starts_with("http/1.1 502"), "{answer}");
                assert!(answer.contains("connection: close\r\n"), "{answer}");
            })
            .await;
    }

    /// An HTTP/2 connection with a stream under way when the worker drains is told to go,
    /// gracefully; the stream is still answered, and then the connection closes.
    #[tokio::test]
    async fn a_draining_http2_connection_finishes_its_streams_then_closes() {
        use crate::h2_peer::{Frame, code, flag, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, held) = scripted_upstream().await;
                let (front, worker) = serving_worker_and(upstream).await;
                let mut peer = h2_client(front).await;
                h2_get(&mut peer, 1, "/held").await;
                until(|| held.borrow().len() == 1).await;
                worker.drain();
                let (announced, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
                assert_eq!(announced.goaway(), (0x7fff_ffff, code::NO_ERROR));
                let (ping, _) = peer
                    .until(|f| f.kind == kind::PING && !f.has(flag::ACK))
                    .await;
                peer.send(&Frame::new(kind::PING, flag::ACK, 0, ping.payload.clone()))
                    .await;
                let (last, _) = peer.until(|f| f.kind == kind::GOAWAY).await;
                assert_eq!(last.goaway(), (1, code::NO_ERROR));

                held.borrow_mut().clear();
                let rest = peer.rest().await;
                assert!(
                    rest.iter()
                        .any(|f| f.stream == 1 && f.has(flag::END_STREAM)),
                    "stream 1 was not answered: {rest:?}"
                );
            })
            .await;
    }

    /// A stream that outlasts the drain's time is not waited for: the connection is closed
    /// at the drain's bound.
    #[tokio::test]
    async fn a_draining_http2_connection_is_closed_at_the_drain_bound() {
        use crate::h2_peer::{Frame, flag, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _held) = scripted_upstream().await;
                let (front, worker) = serving_worker_and(upstream).await;
                let mut peer = h2_client(front).await;
                h2_get(&mut peer, 1, "/held").await;
                peer.settled().await;
                worker.drain();
                let began = tokio::time::Instant::now();
                let (ping, _) = peer
                    .until(|f| f.kind == kind::PING && !f.has(flag::ACK))
                    .await;
                peer.send(&Frame::new(kind::PING, flag::ACK, 0, ping.payload.clone()))
                    .await;
                let (_, rest) = peer
                    .until(|f| f.kind == kind::GOAWAY && f.goaway().0 == 1)
                    .await;
                assert!(rest.iter().all(|f| f.kind != kind::RST_STREAM));
                // Closed at the bound, not after waiting out the time a closing connection
                // is given to flush.
                let _closed = peer.rest().await;
                let took = began.elapsed();
                assert!(
                    took + EARLY >= SHORT.drain && took < SHORT.drain + SLACK,
                    "closed {took:?} after the drain began"
                );
            })
            .await;
    }

    /// The data plane's drain reaches a worker at its next sweep.
    #[tokio::test]
    async fn the_data_planes_drain_reaches_a_worker_at_its_sweep() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                // A sweep far shorter than the time an idle connection is kept, so that it is
                // the drain that closes it.
                let limits = H1Limits {
                    sweep: Duration::from_millis(50),
                    ..H1Limits::default()
                };
                let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
                let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream.write_all(ASKED).await.unwrap();
                answered(&mut stream).await;
                worker.proxy.drain();
                let took = closed_after(&mut stream).await;
                assert!(
                    took < worker.limits.sweep + SLACK && took < SHORT.next_request,
                    "closed {took:?} after the data plane began to drain"
                );
            })
            .await;
    }

    /// Serves a worker whose listener speaks TLS with `certificates`, sending everything
    /// to `upstream`, and says where.
    async fn serving_secured_worker(
        upstream: SocketAddr,
        certificates: Vec<edgerush_config::Certificate>,
    ) -> (SocketAddr, Rc<Worker>) {
        let config = everything_secured_to(upstream, certificates);
        let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// A TLS client of `front`, asking for `name` and offering `protocols` (ALPN, in the
    /// wire form), set up beyond that by `configure`. It takes whatever certificate it is
    /// given: which one that is, is for the test to look at.
    async fn tls_client(
        front: SocketAddr,
        name: &str,
        protocols: Option<&[u8]>,
        configure: impl FnOnce(&mut boring::ssl::SslConnectorBuilder),
    ) -> Result<tokio_boring::SslStream<TcpStream>, String> {
        use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        if let Some(protocols) = protocols {
            builder.set_alpn_protos(protocols).unwrap();
        }
        configure(&mut builder);
        let config = builder.build().configure().unwrap().verify_hostname(false);
        let stream = TcpStream::connect(front).await.unwrap();
        within(tokio_boring::connect(config, name, stream))
            .await
            .map_err(|error| format!("{error:?}"))
    }

    /// Bounded, so that what nobody finishes fails the test rather than hangs it.
    async fn within<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("did not finish")
    }

    /// Sends one HTTP/1.1 request on `stream` and reads the answer to its end.
    async fn h1_over<S>(mut stream: S) -> String
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let _ended = within(stream.read_to_end(&mut answer)).await;
        String::from_utf8_lossy(&answer).into_owned()
    }

    /// A TCP stream that keeps every byte it reads: what arrived on the wire, records and
    /// all, under the TLS the client puts over it.
    #[derive(Debug)]
    struct Tapped {
        stream: TcpStream,
        read: Rc<RefCell<Vec<u8>>>,
    }

    impl AsyncRead for Tapped {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let before = buf.filled().len();
            let polled = Pin::new(&mut this.stream).poll_read(context, buf);
            this.read
                .borrow_mut()
                .extend_from_slice(&buf.filled()[before..]);
            polled
        }
    }

    impl AsyncWrite for Tapped {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.get_mut().stream).poll_write(context, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().stream).poll_flush(context)
        }

        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
        }
    }

    /// An answer over TLS is one record, its head and body sealed together, as it is one
    /// write in plain TCP: sealed a piece at a time it was two records and two sends,
    /// which cost more than the rest of TLS. Counted on the second answer of a connection:
    /// the first comes with the session tickets.
    #[tokio::test]
    async fn an_answer_over_tls_is_one_record() {
        use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
                let (front, _worker) = serving_secured_worker(upstream, certificates).await;
                let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
                builder.set_verify(SslVerifyMode::NONE);
                let config = builder.build().configure().unwrap().verify_hostname(false);
                let read = Rc::new(RefCell::new(Vec::new()));
                let tapped = Tapped {
                    stream: TcpStream::connect(front).await.unwrap(),
                    read: Rc::clone(&read),
                };
                let mut client = within(tokio_boring::connect(config, "example.test", tapped))
                    .await
                    .unwrap();
                let mut answers = Vec::new();
                let mut arrived = 0;
                for _ in 0..2 {
                    arrived = read.borrow().len();
                    client
                        .write_all(b"GET / HTTP/1.1\r\nhost: example.test\r\n\r\n")
                        .await
                        .unwrap();
                    let mut answer = Vec::new();
                    while !(answer.ends_with(b"ok") && answer.windows(4).any(|w| w == b"\r\n\r\n"))
                    {
                        let mut some = [0; 512];
                        let got = within(client.read(&mut some)).await.unwrap();
                        assert_ne!(got, 0, "{:?}", String::from_utf8_lossy(&answer));
                        answer.extend_from_slice(&some[..got]);
                    }
                    answers.push(String::from_utf8_lossy(&answer).into_owned());
                }
                assert!(
                    answers[1].starts_with("HTTP/1.1 200 OK\r\n"),
                    "{}",
                    answers[1]
                );
                // The records the second answer came in: a type, a version and a length each.
                let wire = read.borrow()[arrived..].to_vec();
                let mut records = 0;
                let mut at = 0;
                while let Some(header) = wire.get(at..at + 5) {
                    records += 1;
                    at += 5 + usize::from(u16::from_be_bytes([header[3], header[4]]));
                }
                assert_eq!(at, wire.len(), "records cut short");
                assert_eq!(records, 1, "{} bytes in {records} records", wire.len());
            })
            .await;
    }

    /// An `https` listener speaks HTTP/2 to a client that agreed on it in the handshake,
    /// and HTTP/1.1 to one that agreed on that or on nothing (RFC 9113 §3.2).
    #[tokio::test]
    async fn an_https_listener_speaks_what_the_handshake_agreed_on() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
                let (front, _worker) = serving_secured_worker(upstream, certificates).await;

                let offered: &[u8] = b"\x08http/1.1";
                let h1 = tls_client(front, "example.test", Some(offered), |_| {})
                    .await
                    .unwrap();
                assert_eq!(h1.ssl().selected_alpn_protocol(), Some(&b"http/1.1"[..]));
                let answer = h1_over(h1).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

                let none = tls_client(front, "example.test", None, |_| {})
                    .await
                    .unwrap();
                assert_eq!(none.ssl().selected_alpn_protocol(), None);
                let answer = h1_over(none).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");

                // Offered both, it is given HTTP/2, whatever the client's order.
                let both: &[u8] = b"\x08http/1.1\x02h2";
                let h2 = tls_client(front, "example.test", Some(both), |_| {})
                    .await
                    .unwrap();
                assert_eq!(h2.ssl().selected_alpn_protocol(), Some(&b"h2"[..]));
                let (mut send, connection) = within(::h2::client::handshake(h2)).await.unwrap();
                let _driving = tokio::task::spawn_local(async move {
                    let _ended = connection.await;
                });
                let request = Request::get("https://example.test/").body(()).unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
            })
            .await;
    }

    /// A listener that validates clients serves only one that shows a certificate its
    /// authorities vouch for — whichever of its certificates the client asked for.
    #[tokio::test]
    async fn a_listener_that_validates_clients_serves_only_those_it_trusts() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::tls::testing::certificate;
                let (upstream, _) = counting_upstream().await;
                let trusted = certificate(&["client"]);
                let stranger = certificate(&["client"]);
                let mut config = everything_config(upstream);
                let web = config.listeners.get_mut("web").unwrap();
                web.protocol = edgerush_config::Protocol::Https;
                web.tls = Some(edgerush_config::Tls {
                    certificates: vec![certificate(&["a.test"]), certificate(&["b.test"])],
                    client_validation: Some(edgerush_config::ClientValidation {
                        authorities: vec![trusted.chain.clone()],
                    }),
                });
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                // What a client asking for `name`, showing `shown`, is answered: nothing at
                // all if the handshake, or the first read after it, failed.
                let answered = |name: &'static str, shown: Option<edgerush_config::Certificate>| async move {
                    let connected = tls_client(front, name, None, |builder| {
                        if let Some(shown) = &shown {
                            let chain = boring::x509::X509::from_pem(shown.chain.as_bytes()).unwrap();
                            let key = boring::pkey::PKey::private_key_from_pem(shown.key.as_bytes()).unwrap();
                            builder.set_certificate(&chain).unwrap();
                            builder.set_private_key(&key).unwrap();
                        }
                    })
                    .await;
                    match connected {
                        Ok(stream) => h1_over_or_nothing(stream).await,
                        Err(_) => String::new(),
                    }
                };
                for name in ["a.test", "b.test"] {
                    let answer = answered(name, Some(trusted.clone())).await;
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{name}: {answer}");
                    let answer = answered(name, None).await;
                    assert_eq!(answer, "", "{name}: served a client with no certificate");
                    let answer = answered(name, Some(stranger.clone())).await;
                    assert_eq!(answer, "", "{name}: served a client nobody vouches for");
                }
            })
            .await;
    }

    /// A worker serving `yaml`'s one listener, a passthrough one, with the short deadlines.
    async fn passing(yaml: &str) -> (SocketAddr, Rc<Worker>) {
        let config: Config = serde_saphyr::from_str(yaml).unwrap();
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// A `tcp` listener `db` whose one route goes to `backend`, with `extra` in the
    /// listener's settings.
    fn tcp_to(backend: SocketAddr, extra: &str) -> String {
        format!(
            "listeners: {{ db: {{ address: \"127.0.0.1:0\", protocol: tcp{extra} }} }}\n\
             routes: []\n\
             tcp_routes: [{{ name: db, listeners: [db], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
             upstreams: {{ up: {{ endpoints: [\"{backend}\"] }} }}\n"
        )
    }

    /// Whether the listener `listener` has counted one tunnel as ended by `outcome`, soon.
    async fn tunnel_ended(worker: &Worker, listener: &str, outcome: &str) {
        let line = format!(
            "edgerush_listener_tunnels_total{{listener=\"{listener}\",outcome=\"{outcome}\"}} 1\n"
        );
        until(|| worker.proxy().metrics().contains(&line)).await;
    }

    /// `reading`, which fails the test rather than hang it if the tunnel never passes an
    /// end on.
    async fn bounded<T>(reading: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), reading)
            .await
            .expect("the tunnel never ended")
    }

    /// A backend that reads what each connection sends until its end, answers with how
    /// many bytes came and their sum, and closes.
    async fn tallying_backend() -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                tokio::task::spawn_local(async move {
                    let mut came = Vec::new();
                    stream.read_to_end(&mut came).await.unwrap();
                    let sum: u64 = came.iter().map(|&byte| u64::from(byte)).sum();
                    let answer = format!("{} {sum}", came.len());
                    stream.write_all(answer.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                });
            }
        });
        address
    }

    /// A backend that accepts and then says and reads nothing, for as long as the
    /// connection lasts.
    async fn quiet_backend() -> SocketAddr {
        use tokio::io::AsyncReadExt;
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                tokio::task::spawn_local(async move {
                    let mut rest = Vec::new();
                    let _ended = stream.read_to_end(&mut rest).await;
                });
            }
        });
        address
    }

    /// A `tcp` listener's connection is carried to its route's backend and back, byte for
    /// byte: the client's end is passed on, so that the backend knows it has everything
    /// and answers, and the backend's end is passed back (17 §4).
    #[tokio::test]
    async fn a_tcp_tunnel_carries_bytes_both_ways_and_each_end() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let backend = tallying_backend().await;
                let (front, worker) = passing(&tcp_to(backend, "")).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                // Several blocks' worth, so that each way fills and empties many times.
                let sent: Vec<u8> = (0..300_000_u32).map(|at| (at % 253) as u8).collect();
                client.write_all(&sent).await.unwrap();
                client.shutdown().await.unwrap();
                let mut answer = String::new();
                bounded(client.read_to_string(&mut answer)).await.unwrap();
                let sum: u64 = sent.iter().map(|&byte| u64::from(byte)).sum();
                assert_eq!(answer, format!("{} {sum}", sent.len()));
                tunnel_ended(&worker, "db", "closed").await;
            })
            .await;
    }

    /// A backend that finishes first has its end passed back, and what the client sends
    /// after it still goes on, until the client finishes too.
    #[tokio::test]
    async fn a_backend_that_finishes_first_still_hears_the_client() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let backend = socket.local_addr().unwrap();
                let heard = Rc::new(RefCell::new(Vec::new()));
                let hearing = Rc::clone(&heard);
                tokio::task::spawn_local(async move {
                    let (mut stream, _) = socket.accept().await.unwrap();
                    stream.write_all(b"hello").await.unwrap();
                    stream.shutdown().await.unwrap();
                    let mut came = Vec::new();
                    stream.read_to_end(&mut came).await.unwrap();
                    *hearing.borrow_mut() = came;
                });
                let (front, worker) = passing(&tcp_to(backend, "")).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                let mut said = Vec::new();
                bounded(client.read_to_end(&mut said)).await.unwrap();
                assert_eq!(said, b"hello");
                client.write_all(b"and goodbye").await.unwrap();
                client.shutdown().await.unwrap();
                until(|| heard.borrow().as_slice() == b"and goodbye").await;
                tunnel_ended(&worker, "db", "closed").await;
            })
            .await;
    }

    /// A tunnel that carries nothing either way for its listener's idle bound is closed.
    #[tokio::test]
    async fn an_idle_tunnel_is_closed_at_its_bound() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let backend = quiet_backend().await;
                let (front, worker) = passing(&tcp_to(backend, ", tunnel_idle_seconds: 1")).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                let took = closed_after(&mut client).await;
                let bound = Duration::from_secs(1);
                assert!(took + EARLY >= bound, "closed after {took:?}");
                assert!(took < bound + SLACK, "closed after {took:?}");
                tunnel_ended(&worker, "db", "idle").await;
            })
            .await;
    }

    /// A backend that cannot be reached, or an upstream with no endpoint, closes the
    /// client's connection, counted by why.
    #[tokio::test]
    async fn a_tunnel_with_no_backend_to_reach_is_closed() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Bound, then let go: nothing listens there.
                let gone = TcpListener::bind("127.0.0.1:0")
                    .await
                    .unwrap()
                    .local_addr()
                    .unwrap();
                let (front, worker) = passing(&tcp_to(gone, "")).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                // Refused at once on Linux; Windows tries again for two seconds or so. The
                // connect bound is what holds either way.
                let bound = H1Limits::default().connect;
                assert!(closed_after(&mut client).await < bound + SLACK);
                tunnel_ended(&worker, "db", "connect_failed").await;

                let yaml = tcp_to(gone, "").replace(&format!("[\"{gone}\"]"), "[]");
                let (front, worker) = passing(&yaml).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                assert!(closed_after(&mut client).await < SLACK);
                tunnel_ended(&worker, "db", "no_backend").await;
            })
            .await;
    }

    /// A TLS backend that, once its handshake is done, says `says` and closes.
    async fn tls_backend(says: &'static str) -> SocketAddr {
        use boring::pkey::PKey;
        use boring::ssl::{SslAcceptor, SslMethod};
        use boring::x509::X509;
        use tokio::io::AsyncWriteExt;
        let certificate = crate::tls::testing::certificate(&["backend.test"]);
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        acceptor
            .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
            .unwrap();
        acceptor
            .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
            .unwrap();
        let acceptor = Rc::new(acceptor.build());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        tokio::task::spawn_local(async move {
            while let Ok((stream, _)) = socket.accept().await {
                let acceptor = Rc::clone(&acceptor);
                tokio::task::spawn_local(async move {
                    if let Ok(mut secured) = tokio_boring::accept(&acceptor, stream).await {
                        let _said = secured.write_all(says.as_bytes()).await;
                        let _closed = secured.shutdown().await;
                    }
                });
            }
        });
        address
    }

    /// What a TLS client asking for `name` (or none) through `front` is told once its
    /// handshake is done, or `None` if there was no handshake.
    async fn told_over_tls(front: SocketAddr, name: Option<&str>) -> Option<String> {
        use boring::ssl::{SslConnector, SslMethod, SslVerifyMode};
        use tokio::io::AsyncReadExt;
        let stream = TcpStream::connect(front).await.unwrap();
        let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
        connector.set_verify(SslVerifyMode::NONE);
        let mut config = connector.build().configure().unwrap();
        config.set_use_server_name_indication(name.is_some());
        config.set_verify_hostname(false);
        let mut secured = bounded(tokio_boring::connect(
            config,
            name.unwrap_or("none.test"),
            stream,
        ))
        .await
        .ok()?;
        let mut told = String::new();
        let _ended = bounded(secured.read_to_string(&mut told)).await;
        Some(told)
    }

    /// A `tls` listener's connection goes to the backend whose route's hostnames cover the
    /// name its ClientHello asks for, the most specific first, and the handshake is the
    /// backend's own: the ClientHello went on unchanged. A name no route has, or none at
    /// all, is refused.
    #[tokio::test]
    async fn a_tls_tunnel_goes_where_the_name_asked_for_routes_it() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let exact = tls_backend("exact").await;
                let wildcard = tls_backend("wildcard").await;
                let yaml = format!(
                    "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls }} }}\n\
                     routes: []\n\
                     tls_routes:\n\
                     \x20 - {{ name: exact, listeners: [sni], hostnames: [{{ name: api.example.test, falls_through: true }}], backends: [{{ upstream: exact, weight: 1 }}] }}\n\
                     \x20 - {{ name: rest, listeners: [sni], hostnames: [{{ name: \"*.example.test\", wildcard: any_labels, falls_through: true }}], backends: [{{ upstream: wildcard, weight: 1 }}] }}\n\
                     upstreams: {{ exact: {{ endpoints: [\"{exact}\"] }}, wildcard: {{ endpoints: [\"{wildcard}\"] }} }}\n"
                );
                let (front, worker) = passing(&yaml).await;
                assert_eq!(
                    told_over_tls(front, Some("api.example.test")).await.as_deref(),
                    Some("exact")
                );
                assert_eq!(
                    told_over_tls(front, Some("WWW.Example.test")).await.as_deref(),
                    Some("wildcard")
                );
                tunnel_ended(&worker, "sni", "closed").await;
                assert_eq!(told_over_tls(front, Some("elsewhere.test")).await, None);
                tunnel_ended(&worker, "sni", "refused").await;
                assert_eq!(told_over_tls(front, None).await, None);
                let line = "edgerush_listener_tunnels_total{listener=\"sni\",outcome=\"refused\"} 2\n";
                until(|| worker.proxy().metrics().contains(line)).await;
            })
            .await;
    }

    /// A TLS client that has not sent its ClientHello within the first-request deadline is
    /// let go of.
    #[tokio::test]
    async fn a_client_hello_that_never_comes_is_given_up_on() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let backend = quiet_backend().await;
                let yaml = format!(
                    "listeners: {{ sni: {{ address: \"127.0.0.1:0\", protocol: tls }} }}\n\
                     routes: []\n\
                     tls_routes: [{{ name: a, listeners: [sni], hostnames: [{{ name: a.test, falls_through: true }}], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
                     upstreams: {{ up: {{ endpoints: [\"{backend}\"] }} }}\n"
                );
                let (front, worker) = passing(&yaml).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                // The start of a handshake record, and then nothing.
                client.write_all(&[22, 3, 1, 1, 0]).await.unwrap();
                let took = closed_after(&mut client).await;
                assert!(took + EARLY >= SHORT.first_request, "closed after {took:?}");
                assert!(took < SHORT.first_request + SLACK, "closed after {took:?}");
                tunnel_ended(&worker, "sni", "too_slow").await;
            })
            .await;
    }

    /// A tunnel carrying on when the worker drains is left the drain's bound, and then
    /// closed; no new connection is taken meanwhile.
    #[tokio::test]
    async fn a_draining_worker_closes_tunnels_at_its_bound() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let backend = quiet_backend().await;
                let (front, worker) = passing(&tcp_to(backend, "")).await;
                let mut client = TcpStream::connect(front).await.unwrap();
                until(|| {
                    worker
                        .proxy()
                        .metrics()
                        .contains("edgerush_listener_connections_active{listener=\"db\"} 1\n")
                })
                .await;
                worker.drain();
                let took = closed_after(&mut client).await;
                assert!(took + EARLY >= SHORT.drain, "closed after {took:?}");
                assert!(took < SHORT.drain + SLACK, "closed after {took:?}");
                tunnel_ended(&worker, "db", "drained").await;
            })
            .await;
    }

    /// An HTTPS listener that serves HTTP/3 says so on its TCP answers, with the port its
    /// config gives and for as long as it says (RFC 7838); one that does not, says nothing.
    #[tokio::test]
    async fn an_http3_listener_says_so_on_its_tcp_answers() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                for http3 in [true, false] {
                    let mut config = everything_config(upstream);
                    let web = config.listeners.get_mut("web").unwrap();
                    // What Alt-Svc names: the port the config gives, whatever socket the
                    // test serves on.
                    web.address = "127.0.0.1:8443".parse().unwrap();
                    web.protocol = edgerush_config::Protocol::Https;
                    web.tls = Some(edgerush_config::Tls {
                        certificates: vec![crate::tls::testing::certificate(&["a.test"])],
                        client_validation: None,
                    });
                    web.http3 = http3.then_some(edgerush_config::Http3 {
                        alt_svc_max_age: 60,
                        force_retry: false,
                    });
                    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                    let worker =
                        Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let front = socket.local_addr().unwrap();
                    let _serving = serving(&worker, socket);
                    let stream = tls_client(front, "a.test", None, |_| {}).await.unwrap();
                    let answer = h1_over_or_nothing(stream).await.to_ascii_lowercase();
                    assert!(answer.starts_with("http/1.1 200 ok\r\n"), "{answer}");
                    assert_eq!(
                        answer.contains("\r\nalt-svc: h3=\":8443\"; ma=60\r\n"),
                        http3,
                        "{answer}"
                    );
                }
            })
            .await;
    }

    /// What a request over `stream` is answered, or nothing if the stream fails: under
    /// TLS 1.3 a client's certificate is judged after the client has finished its side of
    /// the handshake, so a refusal can arrive as the first thing read.
    async fn h1_over_or_nothing<S>(mut stream: S) -> String
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let request = b"GET / HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n";
        if stream.write_all(request).await.is_err() {
            return String::new();
        }
        let mut answer = Vec::new();
        let _ended = within(stream.read_to_end(&mut answer)).await;
        String::from_utf8_lossy(&answer).into_owned()
    }

    /// A worker takes no more than its batch of connections before whatever else it has
    /// ready runs: a backlog full of new connections does not go ahead of the rest.
    #[tokio::test]
    async fn a_worker_accepts_a_batch_then_lets_the_rest_run() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                for (batch, other_ran) in [(1, true), (2, false)] {
                    let proxy = served("127.0.0.1:9".parse().unwrap());
                    let limits = H1Limits {
                        accept_batch: batch,
                        ..H1Limits::default()
                    };
                    let worker = Worker::with_deadlines(proxy, limits, SHORT);
                    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let at = socket.local_addr().unwrap();
                    let _first = TcpStream::connect(at).await.unwrap();
                    let _second = TcpStream::connect(at).await.unwrap();
                    // A connect can return before the listener has the connection in its
                    // queue: on Linux the handshake's last step reaches the listening side
                    // just after. Nothing can be asked about the queue without taking from
                    // it, so the kernel is given the moment it needs.
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    within(worker.accept(&socket)).await.unwrap().unwrap();
                    let ran = Rc::new(Cell::new(false));
                    let running = Rc::clone(&ran);
                    let _other = tokio::task::spawn_local(async move { running.set(true) });
                    within(worker.accept(&socket)).await.unwrap().unwrap();
                    assert_eq!(ran.get(), other_ran, "batch of {batch}");
                }
            })
            .await;
    }

    /// A reload with new certificates has new handshakes given them at once, behind the
    /// front the listener already had.
    #[tokio::test]
    async fn new_certificates_are_served_from_the_reload_on() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::tls::testing::certificate;
                let (upstream, _) = counting_upstream().await;
                let old = certificate(&["example.test"]);
                let (front, worker) = serving_secured_worker(upstream, vec![old.clone()]).await;
                let shown = || async {
                    let stream = tls_client(front, "example.test", None, |_| {})
                        .await
                        .unwrap();
                    stream.ssl().peer_certificate().unwrap().to_pem().unwrap()
                };
                assert_eq!(shown().await, old.chain.as_bytes());

                let new = certificate(&["example.test"]);
                worker
                    .proxy()
                    .reload(everything_secured_to(upstream, vec![new.clone()]))
                    .unwrap();
                assert_eq!(shown().await, new.chain.as_bytes());
            })
            .await;
    }

    /// The certificate a client is given is the one whose names cover the name it asked
    /// for, and the first when none does.
    #[tokio::test]
    async fn the_certificate_is_chosen_by_the_name_asked_for() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let certificates = vec![
                    crate::tls::testing::certificate(&["first.test"]),
                    crate::tls::testing::certificate(&["b.test"]),
                    crate::tls::testing::certificate(&["*.c.test"]),
                ];
                let leaves: Vec<Vec<u8>> = certificates
                    .iter()
                    .map(|certificate| {
                        boring::x509::X509::from_pem(certificate.chain.as_bytes())
                            .unwrap()
                            .to_der()
                            .unwrap()
                    })
                    .collect();
                let (front, _worker) = serving_secured_worker(upstream, certificates).await;
                for (name, expected) in [
                    ("b.test", 1),
                    ("B.Test", 1),
                    ("x.c.test", 2),
                    ("first.test", 0),
                    ("nobody.test", 0),
                ] {
                    let client = tls_client(front, name, None, |_| {}).await.unwrap();
                    let given = client.ssl().peer_certificate().unwrap().to_der().unwrap();
                    assert_eq!(given, leaves[expected], "asked for {name}");
                }
            })
            .await;
    }

    /// TLS 1.2 is the oldest spoken, and a client that can exchange keys post-quantum
    /// does.
    #[tokio::test]
    async fn tls_is_1_2_or_later_and_keys_are_exchanged_post_quantum_when_they_can_be() {
        use boring::ssl::SslVersion;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
                let (front, _worker) = serving_secured_worker(upstream, certificates).await;

                // A client of BoringSSL's will not go below 1.2 unless told to.
                let old = tls_client(front, "example.test", None, |builder| {
                    builder
                        .set_min_proto_version(Some(SslVersion::TLS1))
                        .unwrap();
                    builder
                        .set_max_proto_version(Some(SslVersion::TLS1_1))
                        .unwrap();
                })
                .await;
                assert!(old.is_err(), "TLS 1.1 was accepted");

                let twelve = tls_client(front, "example.test", None, |builder| {
                    builder
                        .set_max_proto_version(Some(SslVersion::TLS1_2))
                        .unwrap();
                })
                .await
                .unwrap();
                assert_eq!(twelve.ssl().version_str(), "TLSv1.2");

                let hybrid = tls_client(front, "example.test", None, |builder| {
                    builder.set_curves_list("X25519MLKEM768:X25519").unwrap();
                })
                .await
                .unwrap();
                assert_eq!(hybrid.ssl().version_str(), "TLSv1.3");
                assert_eq!(hybrid.ssl().curve_name(), Some("X25519MLKEM768"));
            })
            .await;
    }

    /// A handshake is part of the first request's time: one that never finishes is cut
    /// off at its deadline, and a client that speaks plain HTTP to a TLS listener is
    /// answered with nothing a client could read as HTTP.
    #[tokio::test]
    async fn a_handshake_is_bounded_by_the_first_request_deadline() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let certificates = vec![crate::tls::testing::certificate(&["example.test"])];
                let (front, _worker) = serving_secured_worker(upstream, certificates).await;

                let mut silent = TcpStream::connect(front).await.unwrap();
                let took = closed_after(&mut silent).await;
                assert!(
                    took + EARLY >= SHORT.first_request && took < SHORT.first_request + SLACK,
                    "closed {took:?} after connecting"
                );

                let mut plain = TcpStream::connect(front).await.unwrap();
                plain.write_all(ASKED).await.unwrap();
                let mut answer = Vec::new();
                let _ended = within(plain.read_to_end(&mut answer)).await;
                assert!(!answer.starts_with(b"HTTP/"), "{answer:?}");
            })
            .await;
    }

    /// A config whose certificates are those of the config before keeps what it served
    /// them with, and so the session tickets that were issued with it; other certificates
    /// are served afresh; and certificates that cannot be used are refused, with the
    /// listener named, and change nothing.
    #[test]
    fn a_reload_keeps_the_tls_of_certificates_that_have_not_changed() {
        let upstream: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let same = vec![crate::tls::testing::certificate(&["example.test"])];
        let config = everything_secured_to(upstream, same.clone());
        let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
        let tls = |proxy: &Proxy| Arc::clone(proxy.current.load().tls[0].as_ref().unwrap());
        let before = tls(&proxy);

        proxy.reload(everything_secured_to(upstream, same)).unwrap();
        assert!(Arc::ptr_eq(&before, &tls(&proxy)));

        // New certificates are served behind the same front, whose keys seal the tickets:
        // clients keep their resumption.
        let other = vec![crate::tls::testing::certificate(&["example.test"])];
        proxy
            .reload(everything_secured_to(upstream, other))
            .unwrap();
        let after = tls(&proxy);
        assert!(!Arc::ptr_eq(&before, &after));
        assert!(after.shares_keys_with(&before));

        // Validating clients otherwise is a new front.
        let mut validating = everything_config(upstream);
        let web = validating.listeners.get_mut("web").unwrap();
        web.protocol = edgerush_config::Protocol::Https;
        web.tls = Some(edgerush_config::Tls {
            certificates: vec![crate::tls::testing::certificate(&["example.test"])],
            client_validation: Some(edgerush_config::ClientValidation {
                authorities: vec![crate::tls::testing::certificate(&["ca"]).chain],
            }),
        });
        proxy.reload(compile(&validating).unwrap()).unwrap();
        assert!(!tls(&proxy).shares_keys_with(&after));
        proxy
            .reload(everything_secured_to(
                upstream,
                vec![crate::tls::testing::certificate(&["example.test"])],
            ))
            .unwrap();
        let after = tls(&proxy);

        let unusable = vec![edgerush_config::Certificate {
            chain: "not a certificate".to_owned(),
            key: "not a key".to_owned(),
        }];
        let refused = proxy.reload(everything_secured_to(upstream, unusable));
        assert!(
            matches!(&refused, Err(ProxyError::Tls { listener, .. }) if listener == "web"),
            "{refused:?}"
        );
        assert!(Arc::ptr_eq(&after, &tls(&proxy)));
    }

    /// What an HTTP/2 upstream of the tests saw.
    #[derive(Debug, Default)]
    struct SeenUp {
        /// Connections accepted.
        connections: Cell<usize>,
        /// Streams open now.
        open: Cell<usize>,
        /// Every request's head, and how many bytes of body came with it.
        requests: RefCell<Vec<(Request<()>, usize)>>,
    }

    /// How an HTTP/2 upstream of the tests behaves.
    #[derive(Clone, Copy)]
    struct UpstreamH2 {
        /// What it announces as its limit on streams.
        streams: u32,
        /// Answer only once `gate` lets it: for requests to be held open.
        gated: bool,
        /// Send the answer's head before reading the body, then read it, then end the
        /// answer: as a server of a bidirectional stream does.
        early: bool,
        /// Tell the client to go away after this many requests on a connection.
        away_after: Option<usize>,
    }

    impl Default for UpstreamH2 {
        fn default() -> Self {
            Self {
                streams: 100,
                gated: false,
                early: false,
                away_after: None,
            }
        }
    }

    /// An upstream that speaks HTTP/2 by prior knowledge, answering each request `200`
    /// with `ok` and the length of the body it read in `x-read`. With `gated`, each
    /// answer waits for a permit of `gate`.
    async fn h2_upstream(how: UpstreamH2) -> (SocketAddr, Rc<SeenUp>, Rc<tokio::sync::Semaphore>) {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let seen = Rc::new(SeenUp::default());
        let gate = Rc::new(tokio::sync::Semaphore::new(0));
        let (seeing, gating) = (Rc::clone(&seen), Rc::clone(&gate));
        let _accepting = tokio::task::spawn_local(async move {
            loop {
                let Ok((stream, _)) = socket.accept().await else {
                    return;
                };
                seeing.connections.set(seeing.connections.get() + 1);
                let (seeing, gating) = (Rc::clone(&seeing), Rc::clone(&gating));
                let _serving = tokio::task::spawn_local(async move {
                    let mut builder = ::h2::server::Builder::new();
                    builder.max_concurrent_streams(how.streams);
                    let Ok(mut connection) = builder.handshake::<_, Bytes>(stream).await else {
                        return;
                    };
                    let mut served = 0;
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        served += 1;
                        if how.away_after == Some(served) {
                            connection.graceful_shutdown();
                        }
                        let (seeing, gating) = (Rc::clone(&seeing), Rc::clone(&gating));
                        let _answering = tokio::task::spawn_local(async move {
                            seeing.open.set(seeing.open.get() + 1);
                            let (head, mut body) = request.into_parts();
                            let at = seeing.requests.borrow().len();
                            seeing
                                .requests
                                .borrow_mut()
                                .push((Request::from_parts(head, ()), 0));
                            let mut read = 0;
                            // Read in full by `read_all`.
                            if !how.early {
                                read = read_all(&mut body).await;
                            }
                            if how.gated {
                                let _permit = gating.acquire().await.unwrap();
                                _permit.forget();
                            }
                            let answer = Response::builder()
                                .status(200)
                                .header("x-read", read.to_string())
                                .body(())
                                .unwrap();
                            if let Ok(mut sending) = respond.send_response(answer, false) {
                                if how.early {
                                    read = read_all(&mut body).await;
                                }
                                let _ = sending.send_data(Bytes::from_static(b"ok"), true);
                            }
                            seeing.requests.borrow_mut()[at].1 = read;
                            seeing.open.set(seeing.open.get() - 1);
                        });
                    }
                });
            }
        });
        (address, seen, gate)
    }

    /// Reads a body h2 received to its end, giving the credit back, and says how much.
    async fn read_all(body: &mut ::h2::RecvStream) -> usize {
        let mut read = 0;
        while let Some(Ok(data)) = body.data().await {
            read += data.len();
            let _ = body.flow_control().release_capacity(data.len());
        }
        read
    }

    /// A worker whose one upstream `up`, at `upstream`, is spoken to in HTTP/2.
    async fn serving_worker_to_h2(
        upstream: SocketAddr,
        limits: H1Limits,
    ) -> (SocketAddr, Rc<Worker>) {
        let mut config = everything_config(upstream);
        config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// Sends one HTTP/1.1 request of `request` bytes, and reads the answer to its end.
    async fn h1_answer(front: SocketAddr, request: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = TcpStream::connect(front).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut answer = Vec::new();
        let _ended = within(stream.read_to_end(&mut answer)).await;
        String::from_utf8_lossy(&answer).into_owned()
    }

    const CLOSING_GET: &[u8] =
        b"GET /a/b?c=d HTTP/1.1\r\nhost: shop.example.com\r\nconnection: close\r\naccept: */*\r\n\r\n";

    /// An upstream configured for HTTP/2 is spoken to in HTTP/2, whatever the client
    /// spoke: the host in `:authority`, nothing about a connection, and the answer back.
    #[tokio::test]
    async fn an_http2_upstream_is_spoken_to_in_http2() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert!(answer.contains("\r\n\r\n2\r\nok\r\n0\r\n\r\n"), "{answer}");
                {
                    let requests = seen.requests.borrow();
                    let (request, _) = &requests[0];
                    assert_eq!(request.version(), Version::HTTP_2);
                    assert_eq!(request.method(), Method::GET);
                    assert_eq!(request.uri().authority().unwrap(), "shop.example.com");
                    assert_eq!(request.uri().path_and_query().unwrap(), "/a/b?c=d");
                    assert!(request.headers().get("connection").is_none());
                    assert!(request.headers().get("host").is_none());
                    assert_eq!(request.headers()["accept"], "*/*");
                }

                // And from a client that spoke HTTP/2 itself.
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://shop.example.com/x").body(()).unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                assert_eq!(seen.requests.borrow().len(), 2);
                assert_eq!(seen.connections.get(), 1, "the connection was not shared");
            })
            .await;
    }

    /// A worker serving `config`, on a socket of its own.
    async fn serving_config(config: &Config) -> (SocketAddr, Rc<Worker>) {
        let proxy = Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// An HTTP/1 upstream answering its first request `first` and the rest `200`, one
    /// request to a connection, and every head it was sent, in lower case.
    fn recording_upstream(first: &'static str) -> (SocketAddr, Rc<RefCell<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let address = socket.local_addr().unwrap();
        let socket = TcpListener::from_std(socket).unwrap();
        let heads = Rc::new(RefCell::new(Vec::<String>::new()));
        let seen = Rc::clone(&heads);
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let mut read = Vec::new();
                let mut chunk = [0; 4096];
                while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => read.extend_from_slice(&chunk[..n]),
                    }
                }
                let status = if seen.borrow().is_empty() {
                    first
                } else {
                    "200 OK"
                };
                seen.borrow_mut()
                    .push(String::from_utf8_lossy(&read).to_lowercase());
                let answer =
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = stream.write_all(answer.as_bytes()).await;
            }
        });
        (address, heads)
    }

    /// A mirror is sent the request as it stands at its place in the rule's filters: one
    /// before the rewrite, the path and host routed on; one after it, the rewritten ones,
    /// as the upstream is (18 §5).
    #[tokio::test]
    async fn a_mirror_is_sent_the_request_as_it_stands_at_its_place() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (primary, sent) = recording_upstream("200 OK");
                let (before, copied_before) = recording_upstream("200 OK");
                let (after, copied_after) = recording_upstream("200 OK");
                let mut config = everything_config(primary);
                let mut copy = config.upstreams["up"].clone();
                copy.endpoints = vec![before];
                config.upstreams.insert("before".to_owned(), copy.clone());
                copy.endpoints = vec![after];
                config.upstreams.insert("after".to_owned(), copy);
                let filters = [
                    "{ type: request_mirror, upstream: before, fraction: { numerator: 1, denominator: 1 } }",
                    "{ type: url_rewrite, host: one.example.org, path: { replace_prefix: /api } }",
                    "{ type: request_mirror, upstream: after, fraction: { numerator: 1, denominator: 1 } }",
                ];
                for filter in filters {
                    config.routes[0].rules[0]
                        .filters
                        .push(serde_saphyr::from_str(filter).unwrap());
                }
                let (front, _worker) = serving_config(&config).await;

                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                until(|| copied_before.borrow().len() == 1 && copied_after.borrow().len() == 1)
                    .await;
                let rewritten = |head: &str| {
                    head.starts_with("get /api/a/b?c=d http/1.1\r\n")
                        && head.contains("\r\nhost: one.example.org\r\n")
                };
                assert!(rewritten(&sent.borrow()[0]), "{:?}", sent.borrow());
                assert!(rewritten(&copied_after.borrow()[0]), "{:?}", copied_after.borrow());
                let routed_on = &copied_before.borrow()[0];
                assert!(
                    routed_on.starts_with("get /a/b?c=d http/1.1\r\n"),
                    "{routed_on}"
                );
                assert!(
                    routed_on.contains("\r\nhost: shop.example.com\r\n"),
                    "{routed_on}"
                );
                // The rest of the request is the request's.
                assert!(routed_on.contains("\r\naccept: */*\r\n"), "{routed_on}");
            })
            .await;
    }

    /// `everything_config`, its `web` listener trusting `trusted` and taking off the control
    /// plane's default trusted-only headers from anyone else.
    fn forwarding_config(upstream: SocketAddr, trusted: &[&str]) -> Config {
        let mut config = everything_config(upstream);
        config.listeners.get_mut("web").unwrap().forwarding = Some(edgerush_config::Forwarding {
            trusted_proxies: trusted.iter().map(|range| (*range).to_owned()).collect(),
            trusted_only_headers: ["Forwarded", "X-Real-IP", "X-Forwarded-*"]
                .map(str::to_owned)
                .to_vec(),
        });
        config
    }

    /// Over HTTP/1.1 and HTTP/2 alike, the upstream is told who the client is — the peer of
    /// the connection, which no proxy in front vouched for — and not what the client said
    /// of it; the scheme and host it asked for; and that it came through the gateway.
    #[tokio::test]
    async fn the_upstream_is_told_who_the_client_is_whatever_it_spoke() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, heads) = recording_upstream("200 OK");
                let (front, _worker) = serving_config(&forwarding_config(upstream, &[])).await;

                let answer = h1_answer(
                    front,
                    b"GET /a HTTP/1.1\r\nhost: shop.example.com:8080\r\nconnection: close\r\n\
                      x-forwarded-for: 10.9.9.9\r\nx-forwarded-proto: https\r\n\
                      forwarded: for=10.9.9.9\r\nx-real-ip: 10.9.9.9\r\nvia: 1.0 cdn\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                let head = heads.borrow()[0].clone();
                for line in [
                    "\r\nx-forwarded-for: 127.0.0.1\r\n",
                    "\r\nx-forwarded-proto: http\r\n",
                    "\r\nx-forwarded-host: shop.example.com:8080\r\n",
                    "\r\nvia: 1.0 cdn\r\n",
                    "\r\nvia: 1.1 edgerush\r\n",
                ] {
                    assert!(head.contains(line), "{line:?} in {head}");
                }
                for gone in ["10.9.9.9", "forwarded:", "x-real-ip", "https"] {
                    assert!(!head.contains(gone), "{gone:?} in {head}");
                }

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://shop.example.com/b")
                    .header("x-forwarded-for", "10.9.9.9")
                    .body(())
                    .unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
                let head = heads.borrow()[1].clone();
                for line in [
                    "\r\nx-forwarded-for: 127.0.0.1\r\n",
                    "\r\nx-forwarded-proto: http\r\n",
                    "\r\nx-forwarded-host: shop.example.com\r\n",
                    "\r\nvia: 2 edgerush\r\n",
                ] {
                    assert!(head.contains(line), "{line:?} in {head}");
                }
                assert!(!head.contains("10.9.9.9"), "{head}");
            })
            .await;
    }

    /// A proxy in front that the listener trusts names the client, and says what scheme and
    /// host the client asked for.
    #[tokio::test]
    async fn a_trusted_proxy_in_front_names_the_client() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, heads) = recording_upstream("200 OK");
                let config = forwarding_config(upstream, &["127.0.0.0/8"]);
                let (front, _worker) = serving_config(&config).await;

                let answer = h1_answer(
                    front,
                    b"GET /a HTTP/1.1\r\nhost: internal:8080\r\nconnection: close\r\n\
                      x-forwarded-for: 1.2.3.4, 198.51.100.9\r\nx-forwarded-proto: https\r\n\
                      x-forwarded-host: shop.example.com\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                let head = heads.borrow()[0].clone();
                for line in [
                    "\r\nx-forwarded-for: 198.51.100.9\r\n",
                    "\r\nx-forwarded-proto: https\r\n",
                    "\r\nx-forwarded-host: shop.example.com\r\n",
                ] {
                    assert!(head.contains(line), "{line:?} in {head}");
                }
                assert!(!head.contains("1.2.3.4"), "{head}");
            })
            .await;
    }

    /// Over HTTP/3 the client is the peer of the path its requests came on, and the scheme
    /// `https`.
    #[tokio::test]
    async fn the_upstream_is_told_who_an_http3_client_is() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::{Client, get};
                let (upstream, heads) = recording_upstream("200 OK");
                let http3 = edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                };
                let mut config = h3_config(upstream, http3);
                config.listeners.get_mut("web").unwrap().forwarding =
                    forwarding_config(upstream, &[]).listeners["web"]
                        .forwarding
                        .clone();
                let (front, _) = serving_h3(&config).await;
                let mut client = Client::connect(front, "a.test").await;

                let mut fields = get("a.test", "/c");
                fields.push(("x-forwarded-for", "10.9.9.9"));
                let id = client.request(&fields, true);
                let answer = client.answer(id).await;
                assert_eq!(answer.final_status(), Some("200"));
                let head = heads.borrow()[0].clone();
                for line in [
                    "\r\nx-forwarded-for: 127.0.0.1\r\n",
                    "\r\nx-forwarded-proto: https\r\n",
                    "\r\nx-forwarded-host: a.test\r\n",
                    "\r\nvia: 3 edgerush\r\n",
                ] {
                    assert!(head.contains(line), "{line:?} in {head}");
                }
                assert!(!head.contains("10.9.9.9"), "{head}");
            })
            .await;
    }

    /// Every `x-request-id` in `text`, a head or answer in lower case, as the value it holds.
    fn request_ids(text: &str) -> Vec<String> {
        text.split("\r\n")
            .filter_map(|line| line.strip_prefix("x-request-id: "))
            .map(str::to_owned)
            .collect()
    }

    /// Whether `id` is a request ID as the gateway makes them: a UUIDv7 in lower case.
    fn is_uuid7(id: &str) -> bool {
        let bytes = id.as_bytes();
        bytes.len() == 36
            && bytes.iter().enumerate().all(|(at, &byte)| match at {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
            })
            && bytes[14] == b'7'
            && b"89ab".contains(&bytes[19])
    }

    /// A listener that generates IDs gives every request its own, in place of whatever the
    /// client sent, and tells the client the same one, in place of whatever the upstream
    /// answered with (08 §3); over HTTP/1.1 and HTTP/2 alike.
    #[tokio::test]
    async fn a_request_and_its_client_are_told_one_id_whatever_it_spoke() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
                let (front, _worker) = serving_config(&everything_config(upstream)).await;

                let answer = h1_answer(
                    front,
                    b"GET /a HTTP/1.1\r\nhost: shop.example.com\r\nconnection: close\r\n\
                      x-request-id: mine\r\nX-Request-ID: again\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                let told = request_ids(&answer);
                assert_eq!(told.len(), 1, "{answer}");
                assert!(is_uuid7(&told[0]), "{answer}");
                let head = heads.borrow()[0].clone();
                assert_eq!(request_ids(&head), told, "{head}");

                // `theirs` is the second upstream answer's only as the first one's.
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://shop.example.com/b")
                    .header("x-request-id", "mine")
                    .body(())
                    .unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                let values: Vec<_> = answer.headers().get_all("x-request-id").iter().collect();
                assert_eq!(values.len(), 1, "{:?}", answer.headers());
                let id = values[0].to_str().unwrap();
                assert!(is_uuid7(id), "{id}");
                assert_ne!(id, told[0]);
                let head = heads.borrow()[1].clone();
                assert_eq!(request_ids(&head), [id], "{head}");
            })
            .await;
    }

    /// The gateway's own answers carry an ID too, each its own: a 404 for no rule, a 400 for
    /// a host that is none, a 502 for an upstream that cannot be reached, a redirect, and a
    /// gRPC call answered with a status (08 §3).
    #[tokio::test]
    async fn the_gateways_own_answers_are_told_their_ids() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (_held, refusing) = refusing();
                let mut config = everything_config(refusing);
                config.routes[0].rules = [
                    "matches: [{ path: { prefix: /old } }]\n\
                     redirect: { status: 301, path: { replace_prefix: /new }, query: keep }",
                    "matches: [{ path: { prefix: /up } }]\n\
                     forward: { backends: [{ upstream: up, weight: 1 }] }",
                ]
                .into_iter()
                .map(|rule| serde_saphyr::from_str(rule).unwrap())
                .collect();
                let (front, _worker) = serving_config(&config).await;

                let mut ids = Vec::new();
                for (request, status) in [
                    (
                        &b"GET /none HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n"[..],
                        "404",
                    ),
                    (
                        b"GET /up HTTP/1.1\r\nhost: a!test\r\nconnection: close\r\n\r\n",
                        "400",
                    ),
                    (
                        b"GET /up HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                        "502",
                    ),
                    (
                        b"GET /old HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                        "301",
                    ),
                ] {
                    let answer = h1_answer(front, request).await;
                    assert!(
                        answer.starts_with(&format!("HTTP/1.1 {status} ")),
                        "{answer}"
                    );
                    let told = request_ids(&answer);
                    assert_eq!(told.len(), 1, "{answer}");
                    assert!(is_uuid7(&told[0]), "{answer}");
                    ids.extend(told);
                }

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let (answer, _) = send.send_request(grpc_call("/none", None), true).unwrap();
                let answer = within(answer).await.unwrap();
                assert!(answer.headers().contains_key("grpc-status"));
                let id = answer.headers()["x-request-id"]
                    .to_str()
                    .unwrap()
                    .to_owned();
                assert!(is_uuid7(&id), "{id}");
                ids.push(id);

                ids.sort();
                ids.dedup();
                assert_eq!(ids.len(), 5, "{ids:?}");
            })
            .await;
    }

    /// A listener that passes IDs through leaves `X-Request-ID` as it is both ways: the
    /// client's reaches the upstream, the upstream's reaches the client, and the gateway's
    /// own answers carry none.
    #[tokio::test]
    async fn a_listener_that_passes_ids_leaves_them_as_they_are() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
                let mut config = everything_config(upstream);
                config.listeners.get_mut("web").unwrap().request_id =
                    Some(edgerush_config::RequestId::Pass);
                config.routes[0].rules[0].matches[0] =
                    serde_saphyr::from_str("path: { prefix: /up }").unwrap();
                let (front, _worker) = serving_config(&config).await;

                let answer = h1_answer(
                    front,
                    b"GET /up HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\
                      x-request-id: mine\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert_eq!(request_ids(&answer), ["theirs"], "{answer}");
                let head = heads.borrow()[0].clone();
                assert_eq!(request_ids(&head), ["mine"], "{head}");

                let answer = h1_answer(
                    front,
                    b"GET /none HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\
                      x-request-id: mine\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 404 "), "{answer}");
                assert!(request_ids(&answer).is_empty(), "{answer}");
            })
            .await;
    }

    /// Over HTTP/3 as over TCP: the request and its client are told one ID, the gateway's.
    #[tokio::test]
    async fn an_http3_request_and_its_client_are_told_one_id() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::downstream::h3::testing::{Client, get};
                let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
                let http3 = edgerush_config::Http3 {
                    alt_svc_max_age: 60,
                    force_retry: false,
                };
                let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
                let mut client = Client::connect(front, "a.test").await;

                let mut fields = get("a.test", "/c");
                fields.push(("x-request-id", "mine"));
                let stream = client.request(&fields, true);
                let answer = client.answer(stream).await;
                assert_eq!(answer.final_status(), Some("200"));
                let told: Vec<String> = answer
                    .heads
                    .last()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| name == "x-request-id")
                    .map(|(_, value)| value.clone())
                    .collect();
                assert_eq!(told.len(), 1, "{:?}", answer.heads);
                assert!(is_uuid7(&told[0]), "{told:?}");
                let head = heads.borrow()[0].clone();
                assert_eq!(request_ids(&head), told, "{head}");
            })
            .await;
    }

    /// `everything_config`, its one rule rewriting to `one.example.org` and under `/api`.
    fn rewriting_config(upstream: SocketAddr) -> Config {
        let mut config = everything_config(upstream);
        let rewrite =
            "{ type: url_rewrite, host: one.example.org, path: { replace_prefix: /api } }";
        config.routes[0].rules[0]
            .filters
            .push(serde_saphyr::from_str(rewrite).unwrap());
        config
    }

    /// An HTTP/2 upstream is sent the rewritten path, and the rewritten host as its
    /// `:authority` (18 §4).
    #[tokio::test]
    async fn an_http2_upstream_is_sent_the_rewritten_path_and_host() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
                let mut config = rewriting_config(upstream);
                config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
                let (front, _worker) = serving_config(&config).await;

                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                let requests = seen.requests.borrow();
                let (request, _) = &requests[0];
                assert_eq!(request.uri().authority().unwrap(), "one.example.org");
                assert_eq!(request.uri().path_and_query().unwrap(), "/api/a/b?c=d");
                assert!(request.headers().get("host").is_none());
            })
            .await;
    }

    /// An HTTP/1.1 upstream is sent the rewritten path and `Host`, and so on every try of a
    /// request sent again: the rewrite is made once, before the first (18 §4).
    #[tokio::test]
    async fn every_try_is_sent_the_rewritten_path_and_host() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, heads) = recording_upstream("503 Service Unavailable");
                let mut config = rewriting_config(upstream);
                config.routes[0].rules[0]
                    .forward
                    .as_mut()
                    .expect("the rule forwards")
                    .retry = Some(retrying(1, &[503], &[], 1));
                let (front, _worker) = serving_config(&config).await;

                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                let heads = heads.borrow();
                assert_eq!(heads.len(), 2, "{heads:?}");
                for head in heads.iter() {
                    assert!(head.starts_with("get /api/a/b?c=d http/1.1\r\n"), "{head}");
                    assert!(head.contains("\r\nhost: one.example.org\r\n"), "{head}");
                    // The host the client asked for is where the upstream is told of it, and
                    // nowhere else.
                    assert!(
                        head.contains("\r\nx-forwarded-host: shop.example.com\r\n"),
                        "{head}"
                    );
                    assert_eq!(head.matches("shop.example.com").count(), 1, "{head}");
                }
            })
            .await;
    }

    /// Requests share a connection up to its stream cap; past it another is opened, and no
    /// more than needed.
    #[tokio::test]
    async fn requests_share_http2_connections_up_to_their_stream_cap() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let how = UpstreamH2 {
                    gated: true,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, gate) = h2_upstream(how).await;
                let limits = H1Limits {
                    h2_streams: 2,
                    ..H1Limits::default()
                };
                let (front, worker) = serving_worker_to_h2(upstream, limits).await;
                // A burst on a cold destination: on each new connection only the first
                // stream goes before the upstream's SETTINGS are heard, and what the
                // connection will take once they are is counted on meanwhile.
                let held: Vec<_> = (0..5)
                    .map(|_| tokio::task::spawn_local(h1_answer(front, CLOSING_GET)))
                    .collect();
                until(|| seen.open.get() == 5).await;
                assert_eq!(seen.connections.get(), 3);
                assert_eq!(worker.h2_connections(), 3);
                // One more for the request after them.
                gate.add_permits(6);
                for answer in held {
                    let answer = answer.await.unwrap();
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                }
                // All three are kept for what comes next.
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert_eq!(seen.connections.get(), 3);
            })
            .await;
    }

    /// An upstream's own limit on streams, below ours, is what fills a connection: past it
    /// the next request goes on another, never into h2's queue behind the limit.
    #[tokio::test]
    async fn an_upstreams_stream_limit_below_ours_is_honoured() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let how = UpstreamH2 {
                    streams: 1,
                    gated: true,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, gate) = h2_upstream(how).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                // The first opens a connection and learns the limit from its SETTINGS.
                let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                until(|| seen.open.get() == 1).await;
                let rest: Vec<_> = (0..2)
                    .map(|_| tokio::task::spawn_local(h1_answer(front, CLOSING_GET)))
                    .collect();
                until(|| seen.open.get() == 3).await;
                assert_eq!(seen.connections.get(), 3);
                gate.add_permits(3);
                for answer in std::iter::once(first).chain(rest) {
                    assert!(answer.await.unwrap().starts_with("HTTP/1.1 200 OK\r\n"));
                }
            })
            .await;
    }

    /// A request's body goes up while its answer is waited for, and on after an answer that
    /// came before the upstream had read it.
    #[tokio::test]
    async fn an_upload_goes_up_to_an_http2_upstream_before_and_after_its_answer() {
        let body = vec![b'x'; 200 * 1024];
        let mut request =
            b"POST /upload HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 204800\r\n\r\n"
                .to_vec();
        request.extend_from_slice(&body);
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                for early in [false, true] {
                    let how = UpstreamH2 {
                        early,
                        ..UpstreamH2::default()
                    };
                    let (upstream, seen, _gate) = h2_upstream(how).await;
                    let (front, _worker) =
                        serving_worker_to_h2(upstream, H1Limits::default()).await;
                    let answer = h1_answer(front, &request).await;
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                    if !early {
                        assert!(answer.contains("x-read: 204800\r\n"), "{answer}");
                    }
                    until(|| {
                        seen.requests
                            .borrow()
                            .first()
                            .is_some_and(|(_, read)| *read == 204_800)
                    })
                    .await;
                    let requests = seen.requests.borrow();
                    assert_eq!(requests[0].0.headers()["content-length"], "204800");
                }
            })
            .await;
    }

    /// An HTTP/2 upstream nobody listens at is answered 502, promptly.
    #[tokio::test]
    async fn an_unreachable_http2_upstream_is_answered_502() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let nobody = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let upstream = nobody.local_addr().unwrap();
                drop(nobody);
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                until(|| worker.h2_connections() == 0).await;
            })
            .await;
    }

    /// Connection-bound credentials are not sent to an HTTP/2 upstream, where they would
    /// authenticate every client's streams (15 §5).
    #[tokio::test]
    async fn connection_bound_credentials_are_not_sent_to_an_http2_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let asked = b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nauthorization: NTLM TlRMTVNTUAABAAAA\r\n\r\n";
                let answer = h1_answer(front, asked).await;
                assert!(answer.starts_with("HTTP/1.1 501 "), "{answer}");
                assert!(seen.requests.borrow().is_empty());
                assert_eq!(seen.connections.get(), 0);
                let scrape = worker.proxy().metrics();
                assert!(scrape.contains("reason=\"connection_auth\"} 1"), "{scrape}");
            })
            .await;
    }

    /// Requests past the queue's bound are refused at once; one that waits too long for a
    /// place is refused when its time is up; the two are told apart.
    #[tokio::test]
    async fn waiting_for_a_place_is_bounded_in_number_and_in_time() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let how = UpstreamH2 {
                    gated: true,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, gate) = h2_upstream(how).await;
                let limits = H1Limits {
                    h2_streams: 1,
                    h2_connections: 1,
                    h2_waiting: 1,
                    connect: Duration::from_millis(500),
                    ..H1Limits::default()
                };
                let (front, worker) = serving_worker_to_h2(upstream, limits).await;
                let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                until(|| seen.open.get() == 1).await;
                let waiting = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                // Let the second join the queue before the third arrives.
                tokio::time::sleep(Duration::from_millis(100)).await;
                let refused = h1_answer(front, CLOSING_GET).await;
                assert!(refused.starts_with("HTTP/1.1 503 "), "{refused}");
                let timed_out = waiting.await.unwrap();
                assert!(timed_out.starts_with("HTTP/1.1 503 "), "{timed_out}");
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("reason=\"upstream_queue_full\"} 1"),
                    "{scrape}"
                );
                assert!(
                    scrape.contains("reason=\"upstream_queue_timeout\"} 1"),
                    "{scrape}"
                );
                // The one that gave up gave its place in the queue up with it: another may
                // wait there, and is served once the first is answered.
                let next = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                tokio::time::sleep(Duration::from_millis(100)).await;
                gate.add_permits(2);
                assert!(first.await.unwrap().starts_with("HTTP/1.1 200 OK\r\n"));
                let next = next.await.unwrap();
                assert!(next.starts_with("HTTP/1.1 200 OK\r\n"), "{next}");
            })
            .await;
    }

    /// An upstream that says GOAWAY finishes what it has, and what comes next goes on a new
    /// connection.
    #[tokio::test]
    async fn after_goaway_requests_go_on_a_new_http2_connection() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let how = UpstreamH2 {
                    away_after: Some(2),
                    ..UpstreamH2::default()
                };
                let (upstream, seen, _gate) = h2_upstream(how).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                for _ in 0..5 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                }
                assert_eq!(seen.connections.get(), 3);

                // A request that comes while a connection told to go still has a stream
                // on it goes on a new one, rather than on the one going.
                let how = UpstreamH2 {
                    away_after: Some(1),
                    gated: true,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, gate) = h2_upstream(how).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let first = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                until(|| seen.open.get() == 1).await;
                // Long enough for the GOAWAY to be heard.
                tokio::time::sleep(Duration::from_millis(100)).await;
                let second = tokio::task::spawn_local(h1_answer(front, CLOSING_GET));
                until(|| seen.open.get() == 2).await;
                assert_eq!(seen.connections.get(), 2);
                gate.add_permits(2);
                for answer in [first, second] {
                    let answer = answer.await.unwrap();
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                }
            })
            .await;
    }

    /// A request whose body stalls on its way to an HTTP/2 upstream, before any answer, is
    /// answered 408 as the client's doing, not 502 as the upstream's.
    #[tokio::test]
    async fn a_stalled_upload_to_an_http2_upstream_is_answered_408() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _seen, _gate) = h2_upstream(UpstreamH2::default()).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://example.test/")
                    .version(Version::HTTP_2)
                    .body(())
                    .unwrap();
                let (response, mut upload) = send.send_request(request, false).unwrap();
                upload.send_data(Bytes::from_static(b"abc"), false).unwrap();
                let response = within(response).await.unwrap();
                assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
            })
            .await;
    }

    /// How a scripted HTTP/2 upstream answers each request.
    type Script = Rc<
        dyn Fn(
            Request<::h2::RecvStream>,
            ::h2::server::SendResponse<Bytes>,
        ) -> Pin<Box<dyn Future<Output = ()>>>,
    >;

    /// An upstream that speaks HTTP/2 by prior knowledge and answers every request as
    /// `script` does.
    async fn scripted_h2_upstream(script: Script) -> SocketAddr {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((stream, _)) = socket.accept().await {
                // As a real server's: with Nagle's algorithm on, the first answer's frames
                // wait for the ACK of the handshake's, which Linux delays 40 ms and more,
                // longer than a script that times a reset after its head allows.
                stream.set_nodelay(true).unwrap();
                let script = Rc::clone(&script);
                let _serving = tokio::task::spawn_local(async move {
                    let Ok(mut connection) = ::h2::server::handshake(stream).await else {
                        return;
                    };
                    while let Some(Ok((request, respond))) = connection.accept().await {
                        let _answering = tokio::task::spawn_local(script(request, respond));
                    }
                });
            }
        });
        address
    }

    fn ok_head() -> Response<()> {
        Response::builder().status(200).body(()).unwrap()
    }

    /// Interim answers from an HTTP/2 upstream reach the client in order, before the final
    /// one; past the exchange's bound on them the upstream is given up on.
    #[tokio::test]
    async fn interim_answers_from_an_http2_upstream_reach_the_client() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let script: Script = Rc::new(|request, mut respond| {
                    Box::pin(async move {
                        let many: usize = request
                            .uri()
                            .path()
                            .trim_start_matches('/')
                            .parse()
                            .unwrap_or(1);
                        for _ in 0..many {
                            let hint = Response::builder()
                                .status(103)
                                .header("link", "</style.css>; rel=preload")
                                .body(())
                                .unwrap();
                            if respond.send_informational(hint).is_err() {
                                return;
                            }
                        }
                        let _ = respond.send_response(ok_head(), true);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://a.test/2").body(()).unwrap();
                let (mut answer, _) = send.send_request(request, true).unwrap();
                let mut interim = Vec::new();
                while let Some(head) =
                    within(std::future::poll_fn(|cx| answer.poll_informational(cx))).await
                {
                    let head = head.unwrap();
                    interim.push((head.status().as_u16(), head.headers()["link"].clone()));
                }
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                assert_eq!(interim.len(), 2);
                assert!(interim.iter().all(|(status, _)| *status == 103));

                let answer = h1_answer(
                    front,
                    b"GET /1 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                )
                .await;
                assert!(answer.starts_with("HTTP/1.1 103 "), "{answer}");
                assert!(answer.contains("\r\n\r\nHTTP/1.1 200 OK\r\n"), "{answer}");

                // Seventeen are one past the sixteen an exchange takes.
                let answer = h1_answer(
                    front,
                    b"GET /17 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                )
                .await;
                assert!(answer.contains("HTTP/1.1 502 "), "{answer}");
            })
            .await;
    }

    /// A request that said `Expect: 100-continue` has its body held back until the upstream
    /// says `100`; one whose upstream answers first never has it sent.
    #[tokio::test]
    async fn a_body_waits_for_an_http2_upstreams_100_continue() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let read = Rc::new(Cell::new(None::<usize>));
                let reading = Rc::clone(&read);
                let script: Script = Rc::new(move |request, mut respond| {
                    let reading = Rc::clone(&reading);
                    Box::pin(async move {
                        let refuse = request.uri().path() == "/refuse";
                        let mut body = request.into_body();
                        if refuse {
                            let no = Response::builder().status(417).body(()).unwrap();
                            let _ = respond.send_response(no, true);
                            // Whatever arrives after the answer.
                            reading.set(Some(read_all(&mut body).await));
                            return;
                        }
                        let go = Response::builder().status(100).body(()).unwrap();
                        let _ = respond.send_informational(go);
                        reading.set(Some(read_all(&mut body).await));
                        let _ = respond.send_response(ok_head(), true);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let limits = H1Limits {
                    continue_wait: Duration::from_secs(60),
                    ..H1Limits::default()
                };
                let (front, _worker) = serving_worker_to_h2(upstream, limits).await;

                let mut stream = TcpStream::connect(front).await.unwrap();
                stream
                    .write_all(b"POST /go HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\n")
                    .await
                    .unwrap();
                let mut first = vec![0; 64];
                let got = within(stream.read(&mut first)).await.unwrap();
                let first = String::from_utf8_lossy(&first[..got]).into_owned();
                assert!(first.starts_with("HTTP/1.1 100 "), "{first}");
                stream.write_all(b"hello").await.unwrap();
                let mut rest = Vec::new();
                let _ = within(stream.read_to_end(&mut rest)).await;
                let rest = String::from_utf8_lossy(&rest);
                assert!(rest.contains("HTTP/1.1 200 OK\r\n"), "{rest}");
                until(|| read.get() == Some(5)).await;

                // A client that sends its body without waiting to be told: the gateway still
                // holds it for the upstream, which answers without asking for it.
                read.set(None);
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream
                    .write_all(b"POST /refuse HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\nhello")
                    .await
                    .unwrap();
                let mut answer = vec![0; 64];
                let got = within(stream.read(&mut answer)).await.unwrap();
                let answer = String::from_utf8_lossy(&answer[..got]).into_owned();
                assert!(answer.starts_with("HTTP/1.1 417 "), "{answer}");
                drop(stream);
                until(|| read.get().is_some()).await;
                assert_eq!(read.get(), Some(0), "the body went up after the answer");
            })
            .await;
    }

    /// An HTTP/2 upstream's trailers reach an HTTP/2 client, as gRPC's status does; the
    /// client's own go no further than the gateway (03 §11), and its body ends where they
    /// would have been.
    #[tokio::test]
    async fn an_answers_trailers_travel_and_a_requests_do_not_through_an_http2_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // What the upstream heard after the body: `Some(None)` for no trailers.
                let heard = Rc::new(RefCell::new(None::<Option<http::HeaderMap>>));
                let hearing = Rc::clone(&heard);
                let script: Script = Rc::new(move |request, mut respond| {
                    let hearing = Rc::clone(&hearing);
                    Box::pin(async move {
                        let mut body = request.into_body();
                        let _ = read_all(&mut body).await;
                        let trailers = std::future::poll_fn(|cx| body.poll_trailers(cx)).await;
                        *hearing.borrow_mut() = Some(trailers.ok().flatten());
                        let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                            return;
                        };
                        let _ = sending.send_data(Bytes::from_static(b"reply"), false);
                        let mut status = http::HeaderMap::new();
                        status.insert("grpc-status", "0".parse().unwrap());
                        let _ = sending.send_trailers(status);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::post("http://a.test/rpc")
                    .header("te", "trailers")
                    .body(())
                    .unwrap();
                let (answer, mut upload) = send.send_request(request, false).unwrap();
                upload
                    .send_data(Bytes::from_static(b"call"), false)
                    .unwrap();
                let mut sent = http::HeaderMap::new();
                sent.insert("x-checksum", "abc".parse().unwrap());
                upload.send_trailers(sent).unwrap();

                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                let mut body = answer.into_body();
                let mut data = Vec::new();
                while let Some(chunk) = within(body.data()).await {
                    let chunk = chunk.unwrap();
                    let _ = body.flow_control().release_capacity(chunk.len());
                    data.extend_from_slice(&chunk);
                }
                assert_eq!(data, b"reply");
                let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
                    .await
                    .unwrap()
                    .expect("no trailers");
                assert_eq!(trailers["grpc-status"], "0");
                until(|| heard.borrow().is_some()).await;
                assert_eq!(
                    heard.borrow().as_ref(),
                    Some(&None),
                    "request trailers went up"
                );
            })
            .await;
    }

    /// An HTTP/2 upstream that resets a stream part way through its answer has the client's
    /// stream reset: with the same reason where it means the same thing on this hop, with
    /// INTERNAL_ERROR where it was about the upstream's hop.
    #[tokio::test]
    async fn an_http2_upstreams_reset_is_passed_on_where_it_means_the_same() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let script: Script = Rc::new(|request, mut respond| {
                    Box::pin(async move {
                        let reason = match request.uri().path() {
                            "/cancel" => ::h2::Reason::CANCEL,
                            _ => ::h2::Reason::PROTOCOL_ERROR,
                        };
                        let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                            return;
                        };
                        let _ = sending.send_data(Bytes::from_static(b"part"), false);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        sending.send_reset(reason);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for (path, expected) in [
                    ("/cancel", ::h2::Reason::CANCEL),
                    ("/protocol", ::h2::Reason::INTERNAL_ERROR),
                ] {
                    let request = Request::get(format!("http://a.test{path}"))
                        .body(())
                        .unwrap();
                    let (answer, _) = send.send_request(request, true).unwrap();
                    let answer = within(answer).await.unwrap();
                    let mut body = answer.into_body();
                    let failed = loop {
                        match within(body.data()).await {
                            Some(Ok(chunk)) => {
                                let _ = body.flow_control().release_capacity(chunk.len());
                            }
                            Some(Err(error)) => break error,
                            None => panic!("{path}: the answer ended cleanly"),
                        }
                    };
                    assert_eq!(failed.reason(), Some(expected), "{path}");
                }
            })
            .await;
    }

    /// One client that stops reading holds only its own stream's window: another stream on
    /// the same upstream connection is answered in full meanwhile.
    #[tokio::test]
    async fn a_slow_reader_does_not_hold_up_another_on_the_same_http2_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                const BIG: usize = 4 << 20;
                let script: Script = Rc::new(|_request, mut respond| {
                    Box::pin(async move {
                        let Ok(mut sending) = respond.send_response(ok_head(), false) else {
                            return;
                        };
                        let mut left = BIG;
                        while left > 0 {
                            let piece = left.min(64 * 1024);
                            sending.reserve_capacity(piece);
                            let Some(Ok(room)) =
                                std::future::poll_fn(|cx| sending.poll_capacity(cx)).await
                            else {
                                return;
                            };
                            let give = room.min(piece);
                            if sending
                                .send_data(Bytes::from(vec![b'z'; give]), left == give)
                                .is_err()
                            {
                                return;
                            }
                            left -= give;
                        }
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let limits = H1Limits {
                    h2_connections: 1,
                    ..H1Limits::default()
                };
                let (front, _worker) = serving_worker_to_h2(upstream, limits).await;

                let mut slow = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://a.test/slow").body(()).unwrap();
                let (stalled, _) = slow.send_request(request, true).unwrap();
                // Its head arrives; its body is never read.
                let _stalled = within(stalled).await.unwrap();

                let mut fast = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://a.test/fast").body(()).unwrap();
                let (answer, _) = fast.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                let mut body = answer.into_body();
                let mut read = 0;
                while let Some(chunk) = within(body.data()).await {
                    let chunk = chunk.unwrap();
                    read += chunk.len();
                    let _ = body.flow_control().release_capacity(chunk.len());
                }
                assert_eq!(read, BIG);
            })
            .await;
    }

    /// How a TLS upstream of the tests settles what it speaks.
    #[derive(Clone, Copy, PartialEq)]
    enum Agrees {
        /// `h2` if offered, else `http/1.1`; and speaks what it agreed on.
        Either,
        /// Agrees on nothing, and speaks HTTP/2 regardless.
        NothingButSpeaksH2,
    }

    /// An upstream that speaks TLS with `certificate`, settling what it speaks as `agrees`
    /// says. Every request is answered `200` with `ok`.
    async fn tls_upstream(
        certificate: &edgerush_config::Certificate,
        agrees: Agrees,
    ) -> SocketAddr {
        tls_upstream_asking(certificate, agrees, None).await
    }

    /// The same, requiring a client certificate `clients` vouches for, if given.
    async fn tls_upstream_asking(
        certificate: &edgerush_config::Certificate,
        agrees: Agrees,
        clients: Option<&edgerush_config::Certificate>,
    ) -> SocketAddr {
        use boring::pkey::PKey;
        use boring::ssl::{AlpnError, SslAcceptor, SslMethod, SslVerifyMode, select_next_proto};
        use boring::x509::X509;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        if let Some(clients) = clients {
            let mut trusted = boring::x509::store::X509StoreBuilder::new().unwrap();
            trusted
                .add_cert(X509::from_pem(clients.chain.as_bytes()).unwrap())
                .unwrap();
            builder.set_verify_cert_store(trusted.build()).unwrap();
            builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        }
        builder
            .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
            .unwrap();
        builder
            .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
            .unwrap();
        builder.set_alpn_select_callback(move |_, client| match agrees {
            Agrees::Either => {
                select_next_proto(b"\x02h2\x08http/1.1", client).ok_or(AlpnError::NOACK)
            }
            Agrees::NothingButSpeaksH2 => Err(AlpnError::NOACK),
        });
        let acceptor = Rc::new(builder.build());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((stream, _)) = socket.accept().await {
                let acceptor = Rc::clone(&acceptor);
                let _serving = tokio::task::spawn_local(async move {
                    let Ok(mut secured) = tokio_boring::accept(&acceptor, stream).await else {
                        return;
                    };
                    if agrees == Agrees::NothingButSpeaksH2
                        || secured.ssl().selected_alpn_protocol() == Some(&b"h2"[..])
                    {
                        let Ok(mut connection) = ::h2::server::handshake(secured).await else {
                            return;
                        };
                        while let Some(Ok((_request, mut respond))) = connection.accept().await {
                            if let Ok(mut sending) = respond.send_response(ok_head(), false) {
                                let _ = sending.send_data(Bytes::from_static(b"ok"), true);
                            }
                        }
                        return;
                    }
                    let mut seen = Vec::new();
                    let mut byte = [0; 1];
                    loop {
                        match secured.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => seen.push(byte[0]),
                        }
                        if seen.ends_with(b"\r\n\r\n") {
                            seen.clear();
                            let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                            if secured.write_all(answer).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        address
    }

    /// A worker whose one upstream `up`, at `upstream`, is reached over TLS as `tls` says
    /// and spoken to in `protocol`.
    async fn serving_worker_to_tls(
        upstream: SocketAddr,
        protocol: UpstreamProtocol,
        tls: edgerush_config::UpstreamTls,
    ) -> SocketAddr {
        let mut config = everything_config(upstream);
        let up = config.upstreams.get_mut("up").unwrap();
        up.protocol = protocol;
        up.tls = Some(tls);
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        front
    }

    fn trusting(
        server_name: &str,
        authority: &edgerush_config::Certificate,
    ) -> edgerush_config::UpstreamTls {
        edgerush_config::UpstreamTls {
            server_name: server_name.to_owned(),
            authorities: vec![authority.chain.clone()],
            client_certificate: None,
        }
    }

    /// An upstream that asks who the data plane is is shown the client certificate its
    /// TLS names, in either protocol; without one, it will not speak.
    #[tokio::test]
    async fn an_upstream_that_asks_is_shown_the_client_certificate() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::tls::testing::certificate;
                let server = certificate(&["backend.test"]);
                let ours = certificate(&["gateway"]);
                for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                    let upstream = tls_upstream_asking(&server, Agrees::Either, Some(&ours)).await;
                    let mut tls = trusting("backend.test", &server);
                    tls.client_certificate = Some(ours.clone());
                    let front = serving_worker_to_tls(upstream, protocol, tls.clone()).await;
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );

                    tls.client_certificate = None;
                    let front = serving_worker_to_tls(upstream, protocol, tls).await;
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 502 "),
                        "{protocol:?}: {answer}"
                    );
                }
            })
            .await;
    }

    /// An upstream reached over TLS is spoken to in HTTP/1.1 or in HTTP/2, as configured,
    /// once its certificate is found to be the named server's and vouched for by a trusted
    /// authority.
    #[tokio::test]
    async fn an_upstream_is_reached_over_tls_in_either_protocol() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let certificate = crate::tls::testing::certificate(&["backend.test"]);
                for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                    let upstream = tls_upstream(&certificate, Agrees::Either).await;
                    let front = serving_worker_to_tls(
                        upstream,
                        protocol,
                        trusting("backend.test", &certificate),
                    )
                    .await;
                    for _ in 0..2 {
                        let answer = h1_answer(front, CLOSING_GET).await;
                        assert!(
                            answer.starts_with("HTTP/1.1 200 OK\r\n"),
                            "{protocol:?}: {answer}"
                        );
                        assert!(answer.contains("ok"), "{answer}");
                    }
                }
            })
            .await;
    }

    /// An endpoint whose certificate no trusted authority vouches for, or that is not the
    /// named server's, is not spoken to: 502.
    #[tokio::test]
    async fn an_upstream_that_is_not_who_it_should_be_is_not_spoken_to() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let certificate = crate::tls::testing::certificate(&["backend.test"]);
                let stranger = crate::tls::testing::certificate(&["backend.test"]);
                for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                    let upstream = tls_upstream(&certificate, Agrees::Either).await;
                    for tls in [
                        trusting("backend.test", &stranger),
                        trusting("other.test", &certificate),
                    ] {
                        let front = serving_worker_to_tls(upstream, protocol, tls).await;
                        let answer = h1_answer(front, CLOSING_GET).await;
                        assert!(
                            answer.starts_with("HTTP/1.1 502 "),
                            "{protocol:?}: {answer}"
                        );
                    }
                }
            })
            .await;
    }

    /// An HTTP/2 upstream that does not agree on `h2` in the handshake is a failed
    /// connection, never one spoken to in HTTP/1.1 instead.
    #[tokio::test]
    async fn an_http2_upstream_that_will_not_agree_on_h2_is_not_spoken_to_in_http1() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let certificate = crate::tls::testing::certificate(&["backend.test"]);
                let upstream = tls_upstream(&certificate, Agrees::NothingButSpeaksH2).await;
                let front = serving_worker_to_tls(
                    upstream,
                    UpstreamProtocol::Http2,
                    trusting("backend.test", &certificate),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
            })
            .await;
    }

    /// A config whose upstream TLS is the config before's keeps what connections were
    /// secured with, and its destinations with it; TLS that changed is a new destination,
    /// whose connections are not the old one's; and an authority that is not a certificate
    /// is refused, with the upstream named.
    #[test]
    fn a_reload_keeps_the_tls_to_an_upstream_that_has_not_changed() {
        let certificate = crate::tls::testing::certificate(&["backend.test"]);
        let secured = |tls: edgerush_config::UpstreamTls| {
            let mut config = everything_config("127.0.0.1:9".parse().unwrap());
            config.upstreams.get_mut("up").unwrap().tls = Some(tls);
            compile(&config).unwrap()
        };
        let proxy = Proxy::new(
            secured(trusting("backend.test", &certificate)),
            NonZeroUsize::MIN,
        )
        .unwrap();
        let identity =
            |proxy: &Proxy| Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
        let before = identity(&proxy);
        let connector = Arc::clone(before.secure().unwrap());

        proxy
            .reload(secured(trusting("backend.test", &certificate)))
            .unwrap();
        assert!(Arc::ptr_eq(&before, &identity(&proxy)));
        let kept = Arc::clone(proxy.current.load().secure[0].as_ref().unwrap());
        assert!(
            Arc::ptr_eq(&connector, &kept),
            "an unchanged TLS was built again"
        );

        proxy
            .reload(secured(trusting("other.test", &certificate)))
            .unwrap();
        let after = identity(&proxy);
        assert_ne!(after.key(), before.key());
        assert!(before.is_retired());

        let unusable = edgerush_config::UpstreamTls {
            server_name: "backend.test".to_owned(),
            authorities: vec![
                "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n".to_owned(),
            ],
            client_certificate: None,
        };
        let refused = proxy.reload(secured(unusable));
        assert!(
            matches!(&refused, Err(ProxyError::UpstreamTls { upstream, .. }) if upstream == "up"),
            "{refused:?}"
        );
    }

    /// A gRPC call, as a gRPC client makes one: POST, `application/grpc`, `te: trailers`,
    /// and `grpc-timeout` if `timeout` says one.
    fn grpc_call(path: &str, timeout: Option<&str>) -> Request<()> {
        let mut call = Request::post(format!("http://a.test{path}"))
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(timeout) = timeout {
            call = call.header("grpc-timeout", timeout);
        }
        call.body(()).unwrap()
    }

    /// What a gRPC call came back with: the HTTP status, and the gRPC status and whether
    /// it came in the head (a trailers-only answer) or in trailers.
    async fn grpc_outcome(answer: ::h2::client::ResponseFuture) -> (StatusCode, String, bool) {
        let answer = within(answer).await.unwrap();
        let status = answer.status();
        if let Some(code) = answer.headers().get("grpc-status") {
            assert!(
                answer.body().is_end_stream(),
                "a status in the head, and more after it"
            );
            return (status, code.to_str().unwrap().to_owned(), true);
        }
        let mut body = answer.into_body();
        while let Some(chunk) = within(body.data()).await {
            let chunk = chunk.expect("the stream was reset, not ended with a status");
            let _ = body.flow_control().release_capacity(chunk.len());
        }
        let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx)))
            .await
            .expect("the stream was reset, not ended with a status")
            .expect("no status at all");
        (
            status,
            trailers["grpc-status"].to_str().unwrap().to_owned(),
            false,
        )
    }

    fn grpc_head() -> Response<()> {
        Response::builder()
            .status(200)
            .header("content-type", "application/grpc")
            .body(())
            .unwrap()
    }

    /// A gRPC call the gateway answers itself is answered as gRPC answers a call it fails
    /// before any message: `200`, and the status the cause calls for, in the head. The same
    /// request that is not a gRPC call is answered in HTTP.
    #[tokio::test]
    async fn a_grpc_call_the_gateway_answers_itself_is_told_a_grpc_status() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut config = everything_config("127.0.0.1:9".parse().unwrap());
                let up = config.upstreams.get_mut("up").unwrap();
                up.endpoints.clear();
                up.protocol = UpstreamProtocol::Http2;
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", None), true)
                    .unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                assert_eq!(answer.headers()["content-type"], "application/grpc");
                assert_eq!(answer.headers()["grpc-status"], "14");
                assert!(answer.headers().contains_key("grpc-message"));
                assert!(answer.body().is_end_stream());

                let plain = Request::post("http://a.test/pkg.Svc/Do").body(()).unwrap();
                let (answer, _) = send.send_request(plain, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert!(answer.headers().get("grpc-status").is_none());
                // The call is counted by its status; the request that was not a call is not.
                let scrape = worker.proxy().metrics();
                let line =
                    "edgerush_listener_grpc_calls_total{listener=\"web\",status=\"UNAVAILABLE\"} 1
";
                assert!(scrape.contains(line), "{scrape}");

                // Connection-bound credentials, which are not sent over HTTP/2.
                let (upstream, _seen, _gate) = h2_upstream(UpstreamH2::default()).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let mut call = grpc_call("/pkg.Svc/Do", None);
                call.headers_mut()
                    .insert("authorization", "Negotiate abc".parse().unwrap());
                let (answer, _) = send.send_request(call, true).unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "12".to_owned(), true)
                );
            })
            .await;
    }

    /// A gRPC call's deadline bounds how long its answer is waited for, and goes up as the
    /// time it has left; one already past is answered at once and never sent.
    #[tokio::test]
    async fn a_grpc_calls_deadline_bounds_it_and_goes_up_as_the_time_left() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let heard = Rc::new(RefCell::new(Vec::<String>::new()));
                let hearing = Rc::clone(&heard);
                let script: Script = Rc::new(move |request, _respond| {
                    let hearing = Rc::clone(&hearing);
                    Box::pin(async move {
                        let timeout = request
                            .headers()
                            .get("grpc-timeout")
                            .map(|value| value.to_str().unwrap().to_owned())
                            .unwrap_or_default();
                        hearing.borrow_mut().push(timeout);
                        // Never answers; keeps the stream open until it is reset.
                        let _respond = _respond;
                        std::future::pending::<()>().await;
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;

                let asked = tokio::time::Instant::now();
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Slow", Some("300m")), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), true)
                );
                let took = asked.elapsed();
                assert!(
                    took + EARLY >= Duration::from_millis(300)
                        && took < Duration::from_millis(300) + SLACK,
                    "answered after {took:?}"
                );
                let sent = crate::grpc::timeout::parse(heard.borrow()[0].as_bytes())
                    .expect("no grpc-timeout went up");
                // What was left when the stream opened, not what the client said.
                assert!(
                    sent < Duration::from_millis(300) && sent > Duration::from_millis(200),
                    "sent {sent:?}"
                );

                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Late", Some("0n")), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), true)
                );
                assert_eq!(heard.borrow().len(), 1, "a call past its deadline went up");
            })
            .await;
    }

    /// Once a gRPC answer has begun, whatever ends it early is told to the client as a
    /// status in its trailers: the upstream cutting it off, by what gRPC makes of the
    /// reason, or its deadline passing. The upstream's own status goes through once.
    #[tokio::test]
    async fn a_grpc_answer_always_ends_with_exactly_one_status() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let script: Script = Rc::new(|request, mut respond| {
                    Box::pin(async move {
                        let path = request.uri().path().to_owned();
                        if path == "/pkg.Svc/Plain" {
                            let unavailable = Response::builder()
                                .status(503)
                                .header("content-type", "text/plain")
                                .body(())
                                .unwrap();
                            let _ = respond.send_response(unavailable, true);
                            return;
                        }
                        let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                            return;
                        };
                        let _ = sending.send_data(Bytes::from_static(b"\0\0\0\0\x01m"), false);
                        match path.as_str() {
                            "/pkg.Svc/Cancelled" => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                sending.send_reset(::h2::Reason::CANCEL);
                            }
                            "/pkg.Svc/Hangs" => std::future::pending::<()>().await,
                            _ => {
                                let mut status = http::HeaderMap::new();
                                status.insert("grpc-status", "0".parse().unwrap());
                                let _ = sending.send_trailers(status);
                            }
                        }
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for (path, timeout, expected) in [
                    ("/pkg.Svc/Ok", None, (StatusCode::OK, "0", false)),
                    ("/pkg.Svc/Cancelled", None, (StatusCode::OK, "1", false)),
                    ("/pkg.Svc/Hangs", Some("300m"), (StatusCode::OK, "4", false)),
                ] {
                    let (answer, _) = send.send_request(grpc_call(path, timeout), true).unwrap();
                    let (status, code, in_head) = grpc_outcome(answer).await;
                    assert_eq!((status, code.as_str(), in_head), expected, "{path}");
                }
                // An answer that is not gRPC's goes on as it came, for the client to read.
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Plain", None), true)
                    .unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert!(answer.headers().get("grpc-status").is_none());
                let mut body = answer.into_body();
                while let Some(chunk) = within(body.data()).await {
                    let _ = body.flow_control().release_capacity(chunk.unwrap().len());
                }
                let trailers = within(std::future::poll_fn(|cx| body.poll_trailers(cx))).await;
                assert!(trailers.unwrap().is_none(), "a status was added to it");

                // Each call counted once, by how it ended; the one that was not answered as
                // gRPC is not counted as a call's status at all.
                let scrape = worker.proxy().metrics();
                for (status, count) in [
                    ("OK", 1),
                    ("CANCELLED", 1),
                    ("DEADLINE_EXCEEDED", 1),
                    ("UNKNOWN", 0),
                    ("INTERNAL", 0),
                ] {
                    let line = format!(
                        "edgerush_listener_grpc_calls_total{{listener=\"web\",status=\"{status}\"}} {count}
"
                    );
                    assert!(scrape.contains(&line), "{line}{scrape}");
                }
            })
            .await;
    }

    /// A request the upstream refuses outright — RST_STREAM(REFUSED_STREAM), which says it
    /// was never processed — is sent once more when all of it can be sent again: its body
    /// too, if it had one no bigger than what is kept. One refused twice is not sent a
    /// third time.
    #[tokio::test]
    async fn a_request_an_http2_upstream_refused_unprocessed_is_sent_once_more() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let asked = Rc::new(Cell::new(0_usize));
                let asking = Rc::clone(&asked);
                let bodies = Rc::new(RefCell::new(Vec::new()));
                let taking = Rc::clone(&bodies);
                let script: Script = Rc::new(move |request, mut respond| {
                    let asking = Rc::clone(&asking);
                    let taking = Rc::clone(&taking);
                    Box::pin(async move {
                        asking.set(asking.get() + 1);
                        // `/twice` is refused every time; the rest only the first time.
                        let refuse =
                            request.uri().path() == "/twice" || asking.get() % 2 == 1;
                        if refuse {
                            respond.send_reset(::h2::Reason::REFUSED_STREAM);
                            return;
                        }
                        let mut body = request.into_body();
                        let mut all = Vec::new();
                        while let Some(Ok(chunk)) = body.data().await {
                            let _ = body.flow_control().release_capacity(chunk.len());
                            all.extend_from_slice(&chunk);
                        }
                        taking.borrow_mut().push(all);
                        let _ = respond.send_response(ok_head(), true);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;

                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert_eq!(asked.get(), 2);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                    "{scrape}"
                );

                // A body, kept as it went the first time, goes again with it.
                asked.set(0);
                bodies.borrow_mut().clear();
                let with_body = b"POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";
                let answer = h1_answer(front, with_body).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert_eq!(asked.get(), 2);
                assert_eq!(*bodies.borrow(), vec![b"hi".to_vec()]);

                // One bigger than what is kept is not.
                asked.set(0);
                let size = crate::retry::replay::MOST + 1;
                let mut big = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
                )
                .into_bytes();
                big.resize(big.len() + size, b'x');
                let answer = h1_answer(front, &big).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                assert_eq!(asked.get(), 1);

                // Refused again: not a third time.
                asked.set(0);
                let twice = b"GET /twice HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n";
                let answer = h1_answer(front, twice).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                assert_eq!(asked.get(), 2);
            })
            .await;
    }

    /// A body that says it has ended with its last frame, as an HTTP/2 client's does, is
    /// sent to an HTTP/2 upstream without its end ever being asked for: refused unprocessed
    /// once all of it has gone, it is sent once more all the same. The upstream reads the
    /// whole body before it refuses, so that none of it is still to go when it does.
    #[tokio::test]
    async fn a_body_that_ends_with_its_last_frame_is_sent_once_more_when_refused() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let bodies = Rc::new(RefCell::new(Vec::new()));
                let taking = Rc::clone(&bodies);
                let script: Script = Rc::new(move |request, mut respond| {
                    let taking = Rc::clone(&taking);
                    Box::pin(async move {
                        let mut body = request.into_body();
                        let mut all = Vec::new();
                        while let Some(Ok(chunk)) = body.data().await {
                            let _ = body.flow_control().release_capacity(chunk.len());
                            all.extend_from_slice(&chunk);
                        }
                        let first = taking.borrow().is_empty();
                        taking.borrow_mut().push(all);
                        if first {
                            respond.send_reset(::h2::Reason::REFUSED_STREAM);
                        } else {
                            let _ = respond.send_response(ok_head(), true);
                        }
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::builder()
                    .method(Method::POST)
                    .uri("http://a.test/")
                    .body(())
                    .unwrap();
                let (answer, mut upload) = send.send_request(request, false).unwrap();
                upload.send_data(Bytes::from_static(b"hi"), true).unwrap();
                assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
                assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(), b"hi".to_vec()]);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                    "{scrape}"
                );
            })
            .await;
    }

    /// A request left above the last stream an upstream's GOAWAY accepted was never
    /// processed either, and goes once more — on another connection, the first going.
    #[tokio::test]
    async fn a_request_left_above_an_http2_goaway_is_sent_once_more() {
        use crate::h2_peer::{self, Peer, code, kind};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let upstream = socket.local_addr().unwrap();
                let connections = Rc::new(Cell::new(0_usize));
                let counting = Rc::clone(&connections);
                let _accepting = tokio::task::spawn_local(async move {
                    while let Ok((stream, _)) = socket.accept().await {
                        counting.set(counting.get() + 1);
                        let first = counting.get() == 1;
                        let _serving = tokio::task::spawn_local(async move {
                            let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                            let (asked, _) = peer.until(|frame| frame.kind == kind::HEADERS).await;
                            if first {
                                // Nothing accepted: the stream is above the last one.
                                peer.send(&h2_peer::goaway(0, code::NO_ERROR)).await;
                                let _ = peer.rest().await;
                                return;
                            }
                            let answer =
                                h2_peer::headers(asked.stream, h2_peer::response(200), true);
                            peer.send(&answer).await;
                            let _ = peer.rest().await;
                        });
                    }
                });
                let (front, worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                assert_eq!(connections.get(), 2);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                    "{scrape}"
                );
            })
            .await;
    }

    /// A retry for `statuses` and `grpc` statuses, `attempts` times, waiting `backoff_ms`
    /// at first and at most.
    fn retrying(
        attempts: u32,
        statuses: &[u16],
        grpc: &[&str],
        backoff_ms: u64,
    ) -> edgerush_config::Retry {
        edgerush_config::Retry {
            attempts,
            http_statuses: statuses.to_vec(),
            grpc_statuses: grpc.iter().map(|&name| name.to_owned()).collect(),
            on_timeout: false,
            backoff_base_ms: backoff_ms,
            backoff_max_ms: backoff_ms,
        }
    }

    /// A worker whose one rule retries as `retry` says, to `up` at `upstream` in `protocol`.
    async fn serving_retrying_worker(
        upstream: SocketAddr,
        protocol: UpstreamProtocol,
        retry: edgerush_config::Retry,
    ) -> (SocketAddr, Rc<Worker>) {
        serving_worker_with(upstream, protocol, Some(retry), H1Limits::default()).await
    }

    /// A worker for `up` at `upstream` in `protocol`, whose one rule retries as `retry`
    /// says if it says anything, with `limits` as its bounds.
    async fn serving_worker_with(
        upstream: SocketAddr,
        protocol: UpstreamProtocol,
        retry: Option<edgerush_config::Retry>,
        limits: H1Limits,
    ) -> (SocketAddr, Rc<Worker>) {
        serving_worker_forwarding(upstream, protocol, limits, |forward| forward.retry = retry).await
    }

    /// A worker for `up` at `upstream` in `protocol`, whose one rule's forwarding is as
    /// `change` leaves it, with `limits` as its bounds.
    async fn serving_worker_forwarding(
        upstream: SocketAddr,
        protocol: UpstreamProtocol,
        limits: H1Limits,
        change: impl FnOnce(&mut edgerush_config::Forward),
    ) -> (SocketAddr, Rc<Worker>) {
        let mut config = everything_config(upstream);
        config.upstreams.get_mut("up").unwrap().protocol = protocol;
        change(
            config.routes[0].rules[0]
                .forward
                .as_mut()
                .expect("the rule forwards"),
        );
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// An HTTP/1 upstream answering its requests with `statuses` in turn, the last of them
    /// from then on, one request to a connection; and the bodies it was sent.
    async fn statuses_upstream(statuses: Vec<u16>) -> (SocketAddr, Rc<RefCell<Vec<Vec<u8>>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let bodies = Rc::new(RefCell::new(Vec::new()));
        let seen = Rc::clone(&bodies);
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let mut read = Vec::new();
                let mut chunk = [0; 16 * 1024];
                let head_end = loop {
                    if let Some(at) = read.windows(4).position(|four| four == b"\r\n\r\n") {
                        break at + 4;
                    }
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => read.extend_from_slice(&chunk[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&read[..head_end]).to_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .map_or(0, |length| length.trim().parse().unwrap());
                while read.len() < head_end + length {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => read.extend_from_slice(&chunk[..n]),
                    }
                }
                let turn = seen.borrow().len();
                seen.borrow_mut().push(read[head_end..].to_vec());
                let status = statuses[turn.min(statuses.len() - 1)];
                let answer = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok"
                );
                let _ = stream.write_all(answer.as_bytes()).await;
            }
        });
        (address, bodies)
    }

    const POSTING_HI: &[u8] =
        b"POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";

    /// A request whose answer's status the rule names is sent again, body and all, to the
    /// answer after it; one whose status it does not name, or with no tries left, is not.
    #[tokio::test]
    async fn a_rule_sends_a_request_again_for_a_status_it_names() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let retry = retrying(2, &[503], &[], 1);
                let (upstream, bodies) = statuses_upstream(vec![503, 200]).await;
                let (front, worker) =
                    serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry.clone()).await;
                let answer = h1_answer(front, POSTING_HI).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(*bodies.borrow(), vec![b"hi".to_vec(), b"hi".to_vec()]);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                    "{scrape}"
                );

                // A status it does not name goes to the client as it came.
                let (upstream, bodies) = statuses_upstream(vec![500, 200]).await;
                let (front, _worker) =
                    serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry.clone()).await;
                let answer = h1_answer(front, POSTING_HI).await;
                assert!(answer.starts_with("HTTP/1.1 500 "), "{answer}");
                assert_eq!(bodies.borrow().len(), 1);

                // Out of tries: the last answer is the client's.
                let (upstream, bodies) = statuses_upstream(vec![503]).await;
                let (front, _worker) =
                    serving_retrying_worker(upstream, UpstreamProtocol::Http1, retry).await;
                let answer = h1_answer(front, POSTING_HI).await;
                assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
                assert_eq!(bodies.borrow().len(), 3);
            })
            .await;
    }

    /// An upstream that could not be reached counts as a `502` for a rule that names it.
    #[tokio::test]
    async fn an_unreachable_upstream_is_retried_as_a_502() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (_refusing, upstream) = refusing();
                let (front, worker) = serving_retrying_worker(
                    upstream,
                    UpstreamProtocol::Http1,
                    retrying(2, &[502], &[], 1),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 2\n"),
                    "{scrape}"
                );
            })
            .await;
    }

    /// An HTTP/1 upstream that leaves the first `silent` requests it is sent unanswered,
    /// each holding its connection until the proxy lets go of it, and answers every one
    /// after them `200`; and how many requests it was sent. `reading` is whether the silent
    /// ones are read at all, or their bytes left in the socket. Bounded: nothing is held for
    /// longer than ten seconds, so that a test whose deadline is missing fails rather than
    /// hangs.
    async fn upstream_silent_at_first(
        silent: usize,
        reading: bool,
    ) -> (SocketAddr, Rc<Cell<usize>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let asked = Rc::new(Cell::new(0_usize));
        let counting = Rc::clone(&asked);
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let counting = Rc::clone(&counting);
                let _serving = tokio::task::spawn_local(async move {
                    let held = Duration::from_secs(10);
                    let mut read = Vec::new();
                    let mut chunk = [0; 16 * 1024];
                    // A head is waited for even by an upstream that then reads no more.
                    while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                        match tokio::time::timeout(held, stream.read(&mut chunk)).await {
                            Ok(Ok(n)) if n > 0 => read.extend_from_slice(&chunk[..n]),
                            _ => return,
                        }
                    }
                    let turn = counting.get();
                    counting.set(turn + 1);
                    if turn < silent {
                        if reading {
                            // Until the proxy lets go of the connection.
                            let _held = tokio::time::timeout(held, async {
                                while matches!(stream.read(&mut chunk).await, Ok(n) if n > 0) {}
                            })
                            .await;
                        } else {
                            tokio::time::sleep(held).await;
                        }
                        return;
                    }
                    let answer =
                        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
                    let _ = stream.write_all(answer.as_bytes()).await;
                });
            }
        });
        (address, asked)
    }

    /// Bounds short enough for a test to wait out, whichever hop's clock it means to run
    /// out: nothing else is short.
    fn quick(limits: impl FnOnce(&mut H1Limits)) -> H1Limits {
        let mut quick = H1Limits::default();
        limits(&mut quick);
        quick
    }

    /// An upstream that never answers, with its head deadline run out, is answered `504`
    /// rather than `502`, and counted as the upstream failing and as what it was.
    #[tokio::test]
    async fn an_http1_upstream_that_never_answers_is_answered_504() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, asked) = upstream_silent_at_first(usize::MAX, true).await;
                let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
                let (front, worker) =
                    serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                assert_eq!(asked.get(), 1);
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// An answer that stops coming before its head, for the idle bound, is the same: `504`.
    #[tokio::test]
    async fn an_http1_upstream_that_goes_quiet_before_its_head_is_answered_504() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
                let limits = quick(|limits| limits.idle = Duration::from_millis(300));
                let (front, worker) =
                    serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// An upstream that stops taking a request's body, while the request is still going out
    /// and nothing has been answered, is `504` too: the write-idle clock, and not the
    /// client's, ran out.
    #[tokio::test]
    async fn an_http1_upstream_that_stops_taking_the_upload_is_answered_504() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _asked) = upstream_silent_at_first(usize::MAX, false).await;
                let limits = quick(|limits| limits.idle = Duration::from_millis(300));
                let (front, _worker) =
                    serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
                // More than the two sockets' buffers hold, so that the write blocks.
                let size = 64 * 1024 * 1024;
                let head = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
                );
                let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
                let _uploading = tokio::task::spawn_local(async move {
                    let _sent = write.write_all(head.as_bytes()).await;
                    let chunk = vec![b'x'; 64 * 1024];
                    for _ in 0..size / chunk.len() {
                        if write.write_all(&chunk).await.is_err() {
                            return;
                        }
                    }
                });
                let mut answer = Vec::new();
                let _ended = within(read.read_to_end(&mut answer)).await;
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            })
            .await;
    }

    /// A client whose upload stops while its request is going upstream is the client
    /// being slow, whichever clock sees it first: `408`, and not counted as the upstream
    /// failing. Here the exchange's own clock for the request's body runs out before the
    /// client connection's does.
    #[tokio::test]
    async fn an_upload_the_client_stops_is_the_clients_whichever_clock_sees_it() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
                // Shorter than the client connection's idle bound (`SHORT`, 500 ms).
                let limits = quick(|limits| limits.idle = Duration::from_millis(200));
                let (front, worker) =
                    serving_worker_with(upstream, UpstreamProtocol::Http1, None, limits).await;
                let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
                write
                    .write_all(b"POST / HTTP/1.1\r\nhost: a.test\r\ncontent-length: 1000\r\n\r\nten bytes!")
                    .await
                    .unwrap();
                let mut answer = Vec::new();
                let _ended = within(read.read_to_end(&mut answer)).await;
                drop(write);
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 0\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// The same for HTTP/2 upstream: no head within its deadline is `504`.
    #[tokio::test]
    async fn an_http2_upstream_that_never_answers_is_answered_504() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream = scripted_h2_upstream(never_answering()).await;
                let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
                let (front, worker) = serving_worker_to_h2(upstream, limits).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A gRPC call whose upstream never answers ends with `DEADLINE_EXCEEDED`, in its head:
    /// nothing had been said. Not `UNAVAILABLE`, which is what would be sent again.
    #[tokio::test]
    async fn a_grpc_call_to_an_upstream_that_never_answers_is_deadline_exceeded() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream = scripted_h2_upstream(never_answering()).await;
                let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
                let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", None), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), true)
                );
            })
            .await;
    }

    /// An HTTP/2 upstream that gives a request's body no room for the idle bound, with no
    /// answer yet, is `504`: it is the upstream not taking the upload, not the client.
    #[tokio::test]
    async fn an_http2_upstream_that_stops_taking_the_upload_is_answered_504() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Never reads the body, so that the room it gave runs out.
                let upstream = scripted_h2_upstream(never_answering()).await;
                let limits = quick(|limits| limits.idle = Duration::from_millis(300));
                let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
                // More than a stream's initial window of 64 KiB.
                let size = 512 * 1024;
                let head = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
                );
                let (mut read, mut write) = TcpStream::connect(front).await.unwrap().into_split();
                let _uploading = tokio::task::spawn_local(async move {
                    let _sent = write.write_all(head.as_bytes()).await;
                    let _sent = write.write_all(&vec![b'x'; size]).await;
                });
                let mut answer = Vec::new();
                let _ended = within(read.read_to_end(&mut answer)).await;
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
            })
            .await;
    }

    /// An HTTP/2 upstream that holds every stream and says nothing for a while, then lets
    /// go: bounded, so that a deadline that never runs fails the test rather than hanging it.
    fn never_answering() -> Script {
        Rc::new(|_request, respond| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(respond);
            })
        })
    }

    /// A try that ran out of time before its head is sent again when the rule says
    /// `on_timeout`, and goes to the next; that one answers.
    #[tokio::test]
    async fn a_try_that_ran_out_of_time_is_sent_again_when_the_rule_says_so() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, asked) = upstream_silent_at_first(1, true).await;
                let retry = edgerush_config::Retry {
                    on_timeout: true,
                    ..retrying(1, &[], &[], 1)
                };
                let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
                let (front, worker) =
                    serving_worker_with(upstream, UpstreamProtocol::Http1, Some(retry), limits)
                        .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(asked.get(), 2);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 1\n"),
                    "{scrape}"
                );
            })
            .await;
    }

    /// Without `on_timeout`, a `502` in the rule's statuses does not cover a try that ran
    /// out of time: it is answered `504`, and nothing is sent again.
    #[tokio::test]
    async fn a_502_does_not_stand_for_a_try_that_ran_out_of_time() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, asked) = upstream_silent_at_first(1, true).await;
                let limits = quick(|limits| limits.final_head = Duration::from_millis(300));
                let (front, worker) = serving_worker_with(
                    upstream,
                    UpstreamProtocol::Http1,
                    Some(retrying(2, &[502], &[], 1)),
                    limits,
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                assert_eq!(asked.get(), 1);
                let scrape = worker.proxy().metrics();
                assert!(
                    scrape.contains("edgerush_upstream_retries_total{upstream=\"up\"} 0\n"),
                    "{scrape}"
                );
            })
            .await;
    }

    /// What `wants_again` says of an upstream that ran out of time or could not be reached:
    /// the first is `on_timeout`'s, the second `502`'s, and neither is the other's.
    #[test]
    fn a_timed_out_try_is_wanted_again_only_under_on_timeout() {
        let retry = |statuses: Vec<u16>, on_timeout| CompiledRetry {
            attempts: 1,
            http_statuses: statuses,
            grpc_statuses: 0,
            on_timeout,
            backoff_base: Duration::from_millis(1),
            backoff_max: Duration::from_millis(1),
        };
        let timed_out = Err(Answer::UpstreamTimedOut);
        let unreached = Err(Answer::UpstreamFailed);
        assert!(wants_again(&retry(vec![], true), &timed_out));
        assert!(!wants_again(&retry(vec![], true), &unreached));
        assert!(!wants_again(&retry(vec![502, 504], false), &timed_out));
        assert!(wants_again(&retry(vec![502], false), &unreached));
    }

    /// A rule's `request` timeout of `ms` milliseconds, `0` for none.
    fn request_timeout(ms: u64) -> edgerush_config::Timeouts {
        edgerush_config::Timeouts {
            request_ms: Some(ms),
            backend_request_ms: None,
            tunnel_idle_ms: None,
        }
    }

    /// A rule's `backend_request` timeout of `ms` milliseconds, and `request` if it says one.
    fn try_timeout(ms: u64, request: Option<u64>) -> edgerush_config::Timeouts {
        edgerush_config::Timeouts {
            request_ms: request,
            backend_request_ms: Some(ms),
            tunnel_idle_ms: None,
        }
    }

    /// A worker for `up` at `upstream` in `protocol`, whose one rule states `timeouts`,
    /// with `limits` as its bounds.
    async fn serving_worker_timed(
        upstream: SocketAddr,
        protocol: UpstreamProtocol,
        timeouts: edgerush_config::Timeouts,
        limits: H1Limits,
    ) -> (SocketAddr, Rc<Worker>) {
        serving_worker_forwarding(upstream, protocol, limits, |forward| {
            forward.timeouts = Some(timeouts);
        })
        .await
    }

    /// An HTTP/1 upstream that answers each request `delay` after its head has come: a
    /// head saying `length` bytes, then those bytes `each` at a time, `gap` apart, one
    /// request to a connection. Bounded: it stops at a connection that takes no more, and
    /// the slowest answer a test asks of it is a few seconds long.
    async fn upstream_answering(
        delay: Duration,
        length: usize,
        each: usize,
        gap: Duration,
    ) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let _serving = tokio::task::spawn_local(async move {
                    let mut read = Vec::new();
                    let mut chunk = [0; 16 * 1024];
                    while !read.windows(4).any(|four| four == b"\r\n\r\n") {
                        match tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
                            .await
                        {
                            Ok(Ok(n)) if n > 0 => read.extend_from_slice(&chunk[..n]),
                            _ => return,
                        }
                    }
                    tokio::time::sleep(delay).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {length}\r\nconnection: close\r\n\r\n"
                    );
                    if stream.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    let mut left = length;
                    while left > 0 {
                        let now = each.min(left);
                        if stream.write_all(&vec![b'x'; now]).await.is_err() {
                            return;
                        }
                        left -= now;
                        tokio::time::sleep(gap).await;
                    }
                });
            }
        });
        address
    }

    /// Bounds whose clocks for an answer's head run out after 200 ms: what a rule's stated
    /// timeouts are to take the place of.
    fn head_clocks_short() -> H1Limits {
        quick(|limits| {
            limits.final_head = Duration::from_millis(200);
            limits.idle = Duration::from_millis(200);
        })
    }

    /// A rule's `request` timeout ends a request whose answer's head has not come: `504`,
    /// counted as the deadline it is and not as the upstream failing, at the timeout and
    /// not at the fixed head deadline's 60 seconds.
    #[tokio::test]
    async fn a_request_timeout_answers_504_when_no_head_has_come() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
                let (front, worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    request_timeout(300),
                    H1Limits::default(),
                )
                .await;
                let started = Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                let took = started.elapsed();
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                assert!(
                    took >= Duration::from_millis(300) - EARLY
                        && took < Duration::from_millis(300) + SLACK,
                    "{took:?}"
                );
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"deadline_exceeded\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 0\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A stated `request` timeout takes the place of the fixed clocks for an answer's head:
    /// an upstream that thinks for longer than they allow, and less than the rule does, is
    /// answered. Unstated, the same upstream is out of time (the tests of `on_timeout`).
    #[tokio::test]
    async fn a_request_timeout_takes_the_place_of_the_fixed_head_clocks() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    request_timeout(3000),
                    head_clocks_short(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            })
            .await;
    }

    /// Gateway API's `0s`: no timeout at all, and none of the fixed clocks for the head in
    /// its place.
    #[tokio::test]
    async fn a_request_timeout_of_zero_waits_for_the_head_as_long_as_it_takes() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    request_timeout(0),
                    head_clocks_short(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            })
            .await;
    }

    /// An answer still coming when the `request` timeout passes is cut off: its head has
    /// gone, so the connection closes short of the length it said. It was moving all the
    /// while, so no idle clock is what ended it.
    #[tokio::test]
    async fn an_answer_still_coming_at_the_request_timeout_is_cut_off() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::ZERO, 1000, 10, Duration::from_millis(50)).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    request_timeout(400),
                    H1Limits::default(),
                )
                .await;
                let started = Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                let took = started.elapsed();
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                let (_, body) = answer.split_once("\r\n\r\n").unwrap();
                assert!(!body.is_empty() && body.len() < 1000, "{}", body.len());
                assert!(took < Duration::from_millis(400) + SLACK, "{took:?}");
            })
            .await;
    }

    /// Over HTTP/2 the same is the stream reset, `INTERNAL_ERROR` as for any answer that
    /// failed after its head: the connection and its other streams carry on.
    #[tokio::test]
    async fn an_http2_answer_still_coming_at_the_request_timeout_is_reset() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::ZERO, 1000, 10, Duration::from_millis(50)).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    request_timeout(400),
                    H1Limits::default(),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://a.test/").body(()).unwrap();
                let (answer, _) = send.send_request(request, true).unwrap();
                let answer = within(answer).await.unwrap();
                assert_eq!(answer.status(), StatusCode::OK);
                let mut body = answer.into_body();
                let ended = within(async {
                    loop {
                        match body.data().await {
                            Some(Ok(chunk)) => {
                                let _ = body.flow_control().release_capacity(chunk.len());
                            }
                            Some(Err(error)) => return Some(error),
                            None => return None,
                        }
                    }
                })
                .await
                .expect("the answer ended as though whole");
                assert_eq!(
                    ended.reason(),
                    Some(::h2::Reason::INTERNAL_ERROR),
                    "{ended:?}"
                );
            })
            .await;
    }

    /// An HTTP/2 upstream is held to the rule's `request` timeout the same way: `504` when
    /// no head has come by then, and none of the fixed clocks for the head in its place.
    #[tokio::test]
    async fn a_request_timeout_holds_for_an_http2_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream = scripted_h2_upstream(never_answering()).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    request_timeout(300),
                    H1Limits::default(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");

                let thinking: Script = Rc::new(|_request, mut respond| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(600)).await;
                        let _sent = respond.send_response(ok_head(), true);
                    })
                });
                let upstream = scripted_h2_upstream(thinking).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    request_timeout(3000),
                    head_clocks_short(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            })
            .await;
    }

    /// A try that has no head within the rule's `backend_request` timeout is out of time:
    /// `504`, counted as the upstream failing, as a try that ran out the fixed clocks is.
    #[tokio::test]
    async fn a_try_past_its_backend_request_timeout_is_answered_504() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _asked) = upstream_silent_at_first(usize::MAX, true).await;
                let (front, worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    try_timeout(300, None),
                    H1Limits::default(),
                )
                .await;
                let started = Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                let took = started.elapsed();
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                assert!(
                    took >= Duration::from_millis(300) - EARLY
                        && took < Duration::from_millis(300) + SLACK,
                    "{took:?}"
                );
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A stated `backend_request` timeout takes the place of the fixed clocks for an
    /// answer's head, as a `request` timeout does.
    #[tokio::test]
    async fn a_backend_request_timeout_takes_the_place_of_the_fixed_head_clocks() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::from_millis(600), 2, 2, Duration::ZERO).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    try_timeout(3000, None),
                    head_clocks_short(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
            })
            .await;
    }

    /// Gateway API's conformance case: a retry of two attempts that names no status, each
    /// try given 300 ms. Two slow tries and then a quick one are answered by the third;
    /// three slow ones are out of time, and nothing is tried a fourth time.
    #[tokio::test]
    async fn tries_past_their_backend_request_timeout_are_sent_again_under_on_timeout() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                for (slow, status) in [(2, "200"), (3, "504")] {
                    let (upstream, asked) = upstream_silent_at_first(slow, true).await;
                    let retry = edgerush_config::Retry {
                        on_timeout: true,
                        ..retrying(2, &[], &[], 1)
                    };
                    let (front, _worker) = serving_worker_forwarding(
                        upstream,
                        UpstreamProtocol::Http1,
                        H1Limits::default(),
                        |forward| {
                            forward.timeouts = Some(try_timeout(300, None));
                            forward.retry = Some(retry);
                        },
                    )
                    .await;
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with(&format!("HTTP/1.1 {status} ")),
                        "{slow}: {answer}"
                    );
                    assert_eq!(asked.get(), 3, "{slow}");
                }
            })
            .await;
    }

    /// The `request` timeout takes in every try: when it passes, the try in hand is given
    /// up and no other is started, however many the retry had left. What ends it is the
    /// request's deadline, not the upstream's.
    #[tokio::test]
    async fn the_request_timeout_ends_the_tries_however_many_are_left() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, asked) = upstream_silent_at_first(usize::MAX, true).await;
                let retry = edgerush_config::Retry {
                    on_timeout: true,
                    ..retrying(5, &[], &[], 1)
                };
                let (front, worker) = serving_worker_forwarding(
                    upstream,
                    UpstreamProtocol::Http1,
                    H1Limits::default(),
                    |forward| {
                        forward.timeouts = Some(try_timeout(300, Some(500)));
                        forward.retry = Some(retry);
                    },
                )
                .await;
                let started = Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                let took = started.elapsed();
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                assert!(took < Duration::from_millis(500) + SLACK, "{took:?}");
                assert_eq!(asked.get(), 2);
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"deadline_exceeded\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A try's clock stops at its answer's head: an answer that takes longer than the
    /// `backend_request` timeout to stream arrives whole.
    #[tokio::test]
    async fn a_backend_request_timeout_stops_at_the_head() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream =
                    upstream_answering(Duration::ZERO, 1000, 100, Duration::from_millis(100)).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http1,
                    try_timeout(300, None),
                    H1Limits::default(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                let (_, body) = answer.split_once("\r\n\r\n").unwrap();
                assert_eq!(body.len(), 1000);
            })
            .await;
    }

    /// An HTTP/2 upstream is held to a `backend_request` timeout the same way, counted as
    /// the upstream failing.
    #[tokio::test]
    async fn a_backend_request_timeout_holds_for_an_http2_upstream() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let upstream = scripted_h2_upstream(never_answering()).await;
                let (front, worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    try_timeout(300, None),
                    H1Limits::default(),
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 504 "), "{answer}");
                let scrape = worker.proxy().metrics();
                let line = "edgerush_listener_local_answers_total{listener=\"web\",reason=\"upstream_timed_out\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                let line = "edgerush_upstream_failures_total{upstream=\"up\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A gRPC call's upstream is told the time its try has, when that is less than the
    /// call has left: it is all the upstream will be waited for. A call that said no
    /// `grpc-timeout` is told nothing.
    #[tokio::test]
    async fn a_grpc_call_s_upstream_is_told_the_time_its_try_has() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, noted) = grpc_timeouts_noted().await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    try_timeout(300, None),
                    H1Limits::default(),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for timeout in [None, Some("10S")] {
                    let (answer, _) = send
                        .send_request(grpc_call("/pkg.Svc/Do", timeout), true)
                        .unwrap();
                    assert_eq!(
                        grpc_outcome(answer).await,
                        (StatusCode::OK, "4".to_owned(), true)
                    );
                }
                let noted = noted.borrow();
                assert_eq!(noted.len(), 2);
                assert_eq!(noted[0], None);
                let sent = noted[1].as_deref().unwrap();
                let left = crate::grpc::timeout::parse(sent.as_bytes()).unwrap();
                assert!(left <= Duration::from_millis(300), "{sent}");
            })
            .await;
    }

    /// An HTTP/2 upstream that notes the `grpc-timeout` of each request it is sent, and
    /// answers none of them.
    async fn grpc_timeouts_noted() -> (SocketAddr, Rc<RefCell<Vec<Option<String>>>>) {
        let noted = Rc::new(RefCell::new(Vec::new()));
        let noting = Rc::clone(&noted);
        let script: Script = Rc::new(move |request, respond| {
            noting.borrow_mut().push(
                request
                    .headers()
                    .get("grpc-timeout")
                    .map(|timeout| timeout.to_str().unwrap().to_owned()),
            );
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(respond);
            })
        });
        (scripted_h2_upstream(script).await, noted)
    }

    /// A gRPC call is held to the earlier of its own deadline and its rule's: ended with
    /// `DEADLINE_EXCEEDED` when the rule's comes first. The upstream is told the time left
    /// of that deadline when the call said a `grpc-timeout`, and nothing when it did not:
    /// a rule's timeout is not the client's to have sent.
    #[tokio::test]
    async fn a_grpc_call_is_held_to_the_earlier_of_its_deadline_and_its_rules() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, noted) = grpc_timeouts_noted().await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    request_timeout(300),
                    H1Limits::default(),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                for timeout in [None, Some("10S")] {
                    let (answer, _) = send
                        .send_request(grpc_call("/pkg.Svc/Do", timeout), true)
                        .unwrap();
                    assert_eq!(
                        grpc_outcome(answer).await,
                        (StatusCode::OK, "4".to_owned(), true)
                    );
                }
                let noted = noted.borrow();
                assert_eq!(noted.len(), 2);
                assert_eq!(noted[0], None);
                let sent = noted[1].as_deref().unwrap();
                let left = crate::grpc::timeout::parse(sent.as_bytes()).unwrap();
                assert!(left <= Duration::from_millis(300), "{sent}");
            })
            .await;
    }

    /// A gRPC call whose answer is still coming at its rule's `request` timeout ends as
    /// one past its deadline does: trailers with `DEADLINE_EXCEEDED`.
    #[tokio::test]
    async fn a_grpc_answer_still_coming_at_the_request_timeout_ends_exceeded() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let script: Script = Rc::new(|_request, mut respond| {
                    Box::pin(async move {
                        let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                            return;
                        };
                        for _ in 0..60 {
                            let message = Bytes::from_static(b"\0\0\0\0\x01x");
                            if sending.send_data(message, false).is_err() {
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, _worker) = serving_worker_timed(
                    upstream,
                    UpstreamProtocol::Http2,
                    request_timeout(400),
                    H1Limits::default(),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", None), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "4".to_owned(), false)
                );
            })
            .await;
    }

    /// A body past what is kept goes on as it came, and is not sent again.
    #[tokio::test]
    async fn a_body_too_big_to_keep_is_not_sent_again() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, bodies) = statuses_upstream(vec![503, 200]).await;
                let (front, worker) = serving_retrying_worker(
                    upstream,
                    UpstreamProtocol::Http1,
                    retrying(2, &[503], &[], 1),
                )
                .await;
                let size = crate::retry::replay::MOST + 1;
                let mut request = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
                )
                .into_bytes();
                request.resize(request.len() + size, b'x');
                let answer = h1_answer(front, &request).await;
                assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
                assert_eq!(bodies.borrow().len(), 1);
                assert_eq!(bodies.borrow()[0].len(), size);
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_retries_refused_total{upstream=\"up\",reason=\"body\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// Retries stop when the upstream's budget is spent: its reserve of 100 and a fifth of
    /// its requests, each retry costing five. Request `n` finds `505 - 4n` left, so the
    /// 126th of an upstream that always fails is the first not sent again.
    #[tokio::test]
    async fn retries_stop_when_the_budget_is_spent() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, bodies) = statuses_upstream(vec![503]).await;
                let (front, worker) = serving_retrying_worker(
                    upstream,
                    UpstreamProtocol::Http1,
                    retrying(1, &[503], &[], 1),
                )
                .await;
                for _ in 0..126 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
                }
                assert_eq!(bodies.borrow().len(), 125 * 2 + 1);
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_retries_refused_total{upstream=\"up\",reason=\"budget\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
            })
            .await;
    }

    /// A gRPC call whose trailers-only answer carries a status the rule names is sent
    /// again, and only the answer the client gets is counted. A status in trailers after
    /// a head is not retried — the head has gone to the client — and nor is a call whose
    /// deadline comes before its backoff would end.
    #[tokio::test]
    async fn a_grpc_call_is_sent_again_for_a_status_in_its_head() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let asked = Rc::new(Cell::new(0_usize));
                let asking = Rc::clone(&asked);
                let script: Script = Rc::new(move |request, mut respond| {
                    let asking = Rc::clone(&asking);
                    Box::pin(async move {
                        asking.set(asking.get() + 1);
                        let mut unavailable = http::HeaderMap::new();
                        unavailable.insert("grpc-status", "14".parse().unwrap());
                        if request.uri().path() == "/pkg.Svc/Late" {
                            let mut stream = respond.send_response(grpc_head(), false).unwrap();
                            let _ = stream.send_trailers(unavailable);
                            return;
                        }
                        let first = asking.get() % 2 == 1;
                        let mut head = grpc_head();
                        let code = if first { "14" } else { "0" };
                        head.headers_mut()
                            .insert("grpc-status", code.parse().unwrap());
                        let _ = respond.send_response(head, true);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let (front, worker) = serving_retrying_worker(
                    upstream,
                    UpstreamProtocol::Http2,
                    retrying(2, &[], &["UNAVAILABLE"], 1),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", None), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "0".to_owned(), true)
                );
                assert_eq!(asked.get(), 2);
                let scrape = worker.proxy().metrics();
                assert!(
                    !scrape.contains("status=\"UNAVAILABLE\"} 1"),
                    "a call set aside was counted: {scrape}"
                );
                let line = "edgerush_listener_grpc_calls_total{listener=\"web\",status=\"OK\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");

                asked.set(0);
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Late", None), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "14".to_owned(), false)
                );
                assert_eq!(asked.get(), 1);

                // A backoff past the deadline: the answer there is, at once.
                let (front, _worker) = serving_retrying_worker(
                    upstream,
                    UpstreamProtocol::Http2,
                    retrying(2, &[], &["UNAVAILABLE"], 60_000),
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                asked.set(0);
                let (answer, _) = send
                    .send_request(grpc_call("/pkg.Svc/Do", Some("5S")), true)
                    .unwrap();
                assert_eq!(
                    grpc_outcome(answer).await,
                    (StatusCode::OK, "14".to_owned(), true)
                );
                assert_eq!(asked.get(), 1);
            })
            .await;
    }

    /// A worker whose one rule goes to `up` at `primary`, speaking what it says, and mirrors
    /// every request to each of `mirrors`: a name, where it is (nowhere, if not given) and
    /// what it speaks.
    async fn serving_mirroring_worker(
        (primary, speaking): (SocketAddr, UpstreamProtocol),
        mirrors: &[(&str, Option<SocketAddr>, UpstreamProtocol)],
    ) -> (SocketAddr, Rc<Worker>) {
        let mut config = everything_config(primary);
        config.upstreams.get_mut("up").unwrap().protocol = speaking;
        for &(name, at, protocol) in mirrors {
            let mut upstream = config.upstreams["up"].clone();
            upstream.endpoints = at.into_iter().collect();
            upstream.protocol = protocol;
            config.upstreams.insert(name.to_owned(), upstream);
            config.routes[0].rules[0]
                .filters
                .push(edgerush_config::Filter::RequestMirror(
                    edgerush_config::Mirror {
                        upstream: name.to_owned(),
                        fraction: edgerush_config::Fraction {
                            numerator: 1,
                            denominator: 1,
                        },
                    },
                ));
        }
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    /// An HTTP/2 upstream that takes every request and reads none of its body: a mirror
    /// that stops keeping up once its stream window is spent. What it was asked for, by
    /// path.
    async fn unread_h2_upstream() -> (SocketAddr, Rc<RefCell<Vec<String>>>) {
        let asked = Rc::new(RefCell::new(Vec::new()));
        let asking = Rc::clone(&asked);
        let script: Script = Rc::new(move |request, _respond| {
            asking.borrow_mut().push(request.uri().path().to_owned());
            Box::pin(async move {
                // Held, body unread, until the connection goes.
                let _held = request;
                std::future::pending::<()>().await;
            })
        });
        (scripted_h2_upstream(script).await, asked)
    }

    /// Every mirror gets a copy of the request, body and all, in the protocol it speaks,
    /// and its answer goes nowhere: the client gets the request's own.
    #[tokio::test]
    async fn a_mirror_gets_a_copy_and_the_client_the_answer_of_the_request() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (primary, sent) = statuses_upstream(vec![200]).await;
                let (shadow, copied) = statuses_upstream(vec![500]).await;
                let seen = Rc::new(RefCell::new(Vec::new()));
                let seeing = Rc::clone(&seen);
                let script: Script = Rc::new(move |request, mut respond| {
                    let seeing = Rc::clone(&seeing);
                    Box::pin(async move {
                        let path = request.uri().path().to_owned();
                        let mut body = request.into_body();
                        let mut all = Vec::new();
                        while let Some(Ok(chunk)) = body.data().await {
                            let _ = body.flow_control().release_capacity(chunk.len());
                            all.extend_from_slice(&chunk);
                        }
                        seeing.borrow_mut().push((path, all));
                        let answer = Response::builder().status(503).body(()).unwrap();
                        let _ = respond.send_response(answer, true);
                    })
                });
                let shadow_h2 = scripted_h2_upstream(script).await;
                let (front, worker) = serving_mirroring_worker(
                    (primary, UpstreamProtocol::Http1),
                    &[
                        ("shadow", Some(shadow), UpstreamProtocol::Http1),
                        ("shadow-h2", Some(shadow_h2), UpstreamProtocol::Http2),
                    ],
                )
                .await;
                let request =
                    b"POST /copied?q=1 HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi";
                let answer = h1_answer(front, request).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(*sent.borrow(), vec![b"hi".to_vec()]);
                until(|| copied.borrow().len() == 1 && seen.borrow().len() == 1).await;
                assert_eq!(*copied.borrow(), vec![b"hi".to_vec()]);
                assert_eq!(
                    *seen.borrow(),
                    vec![("/copied".to_owned(), b"hi".to_vec())]
                );
                // Each mirror is an upstream like any other, counted as one.
                let scrape = worker.proxy().metrics();
                for name in ["shadow", "shadow-h2"] {
                    let line = format!("edgerush_upstream_requests_total{{upstream=\"{name}\"}} 1\n");
                    assert!(scrape.contains(&line), "{scrape}");
                }
            })
            .await;
    }

    /// A body that says it has ended with its last frame, as an HTTP/2 client's does, is
    /// sent to an HTTP/2 upstream without its end ever being asked for: its copy goes whole
    /// all the same, ended as the request was, not cut off as if the request went.
    #[tokio::test]
    async fn a_mirror_gets_the_whole_of_a_body_that_ends_with_its_last_frame() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // What each upstream was sent, and how it ended.
                let seen = Rc::new(RefCell::new(Vec::new()));
                let reading = |name: &'static str, status: u16| -> Script {
                    let seeing = Rc::clone(&seen);
                    Rc::new(move |request, mut respond| {
                        let seeing = Rc::clone(&seeing);
                        Box::pin(async move {
                            let mut body = request.into_body();
                            let mut all = Vec::new();
                            let ended = loop {
                                match body.data().await {
                                    Some(Ok(chunk)) => {
                                        let _ = body.flow_control().release_capacity(chunk.len());
                                        all.extend_from_slice(&chunk);
                                    }
                                    Some(Err(error)) => break Err(error.to_string()),
                                    None => break Ok(()),
                                }
                            };
                            seeing.borrow_mut().push((name, all, ended));
                            let answer = Response::builder().status(status).body(()).unwrap();
                            let _ = respond.send_response(answer, true);
                        })
                    })
                };
                let primary = scripted_h2_upstream(reading("primary", 200)).await;
                let shadow = scripted_h2_upstream(reading("shadow", 503)).await;
                let (front, _worker) = serving_mirroring_worker(
                    (primary, UpstreamProtocol::Http2),
                    &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
                )
                .await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::builder()
                    .method(Method::POST)
                    .uri("http://a.test/")
                    .body(())
                    .unwrap();
                let (answer, mut upload) = send.send_request(request, false).unwrap();
                upload.send_data(Bytes::from_static(b"hi"), true).unwrap();
                assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
                until(|| seen.borrow().len() == 2).await;
                let mut seen = seen.borrow().clone();
                seen.sort();
                assert_eq!(
                    seen,
                    vec![
                        ("primary", b"hi".to_vec(), Ok(())),
                        ("shadow", b"hi".to_vec(), Ok(())),
                    ]
                );
            })
            .await;
    }

    /// A mirror that stops reading is given up on once it is too far behind; the request
    /// goes on at its own upstream's pace, whole. The rest of the body waits for the copy's
    /// head to reach the mirror: sent whole, it can be past the bound before the copy has
    /// begun, and a copy given up on then is never sent at all.
    #[tokio::test]
    async fn a_mirror_that_stops_reading_never_holds_the_request_up() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (primary, sent) = statuses_upstream(vec![200]).await;
                let (shadow, asked) = unread_h2_upstream().await;
                let (front, worker) = serving_mirroring_worker(
                    (primary, UpstreamProtocol::Http1),
                    &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
                )
                .await;
                let size = 1 << 20;
                let mut client = TcpStream::connect(front).await.unwrap();
                let head = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\nx"
                );
                client.write_all(head.as_bytes()).await.unwrap();
                until(|| asked.borrow().len() == 1).await;
                client.write_all(&vec![b'x'; size - 1]).await.unwrap();
                let mut answer = Vec::new();
                let _ended = within(client.read_to_end(&mut answer)).await;
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(sent.borrow()[0].len(), size);
                let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"behind\"} 1\n";
                until(|| worker.proxy().metrics().contains(line)).await;
            })
            .await;
    }

    /// A mirror given up on while it waits for room its upstream will never give — its
    /// window spent, the request's body going on without it — lets go of its exchange at
    /// once: its stream is reset and it is counted then, not held with its place until its
    /// idle bound, 30 s, runs out. Whether a copy is waiting there when it is given up on is
    /// a race in a request sent whole; this one's body is sent in two, the mirror's window
    /// spent in between.
    #[tokio::test]
    async fn a_mirror_given_up_on_while_it_waits_for_room_lets_go_at_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (primary, sent) = statuses_upstream(vec![200]).await;
                // Takes what comes and never gives the room back: h2's first window's
                // worth, and no more.
                let window = 65_535;
                let taken = Rc::new(std::cell::Cell::new(0));
                let let_go = Rc::new(std::cell::Cell::new(false));
                let (taking, letting_go) = (Rc::clone(&taken), Rc::clone(&let_go));
                let script: Script = Rc::new(move |request, respond| {
                    let (taking, letting_go) = (Rc::clone(&taking), Rc::clone(&letting_go));
                    Box::pin(async move {
                        let _unanswered = respond;
                        let mut body = request.into_body();
                        while let Some(Ok(chunk)) = body.data().await {
                            taking.set(taking.get() + chunk.len());
                        }
                        letting_go.set(true);
                    })
                });
                let shadow = scripted_h2_upstream(script).await;
                let (front, worker) = serving_mirroring_worker(
                    (primary, UpstreamProtocol::Http1),
                    &[("shadow", Some(shadow), UpstreamProtocol::Http2)],
                )
                .await;
                let size = 1 << 20;
                let mut client = TcpStream::connect(front).await.unwrap();
                let head = format!(
                    "POST / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\ncontent-length: {size}\r\n\r\n"
                );
                client.write_all(head.as_bytes()).await.unwrap();
                // A piece at a time, each well within its bound and taken before the next,
                // until its window is spent with a byte of the last still in hand: it waits
                // for room. Sent whole, the body is past its bound before its copy begins.
                let piece = 16 * 1024;
                let mut given = 0;
                while given <= window {
                    client.write_all(&vec![b'x'; piece]).await.unwrap();
                    given += piece;
                    until(|| taken.get() == given.min(window)).await;
                }
                // The rest takes it past its bound while it waits.
                client.write_all(&vec![b'x'; size - given]).await.unwrap();
                let mut answer = Vec::new();
                let _ended = within(client.read_to_end(&mut answer)).await;
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(sent.borrow()[0].len(), size);
                let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"behind\"} 1\n";
                until(|| let_go.get() && worker.proxy().metrics().contains(line)).await;
            })
            .await;
    }

    /// A copy that cannot go is counted by why, and the request goes as it would have:
    /// a mirror with no endpoint, and a request with credentials bound to its client's
    /// connection.
    #[tokio::test]
    async fn a_copy_that_cannot_go_is_counted_and_the_request_goes_anyway() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (primary, sent) = statuses_upstream(vec![200]).await;
                let (shadow, copied) = statuses_upstream(vec![200]).await;
                let (front, worker) = serving_mirroring_worker(
                    (primary, UpstreamProtocol::Http1),
                    &[
                        ("nowhere", None, UpstreamProtocol::Http1),
                        ("shadow", Some(shadow), UpstreamProtocol::Http1),
                    ],
                )
                .await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                until(|| copied.borrow().len() == 1).await;
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"nowhere\",reason=\"nowhere\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");

                let bound = b"GET / HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\nauthorization: Negotiate abc\r\n\r\n";
                let answer = h1_answer(front, bound).await;
                assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
                assert_eq!(sent.borrow().len(), 2);
                let scrape = worker.proxy().metrics();
                let line = "edgerush_upstream_mirrors_given_up_total{upstream=\"shadow\",reason=\"credentials\"} 1\n";
                assert!(scrape.contains(line), "{scrape}");
                assert_eq!(copied.borrow().len(), 1, "credentials went to a mirror");
            })
            .await;
    }

    /// A worker whose one upstream `up`, at `upstream`, is spoken to in HTTP/2 with
    /// `keepalive`.
    async fn serving_worker_with_keepalive(
        upstream: SocketAddr,
        keepalive: edgerush_config::Keepalive,
    ) -> (SocketAddr, Rc<Worker>) {
        let mut config = everything_config(upstream);
        let up = config.upstreams.get_mut("up").unwrap();
        up.protocol = UpstreamProtocol::Http2;
        up.keepalive = Some(keepalive);
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    fn every_second(without_calls: bool) -> edgerush_config::Keepalive {
        edgerush_config::Keepalive {
            interval_seconds: 1,
            timeout_seconds: 1,
            without_calls,
            backend_allows_short_intervals: true,
        }
    }

    /// What a raw HTTP/2 upstream of the keepalive tests does with PINGs.
    #[derive(Clone, Copy, PartialEq)]
    enum Pinged {
        /// Answers every one.
        Answers,
        /// Answers the first (the settling PING), then none.
        GoesQuiet,
        /// Answers the first, and says GOAWAY(ENHANCE_YOUR_CALM) at the next.
        CalmsDown,
        /// Answers none, the settling PING included.
        Deaf,
    }

    /// A raw HTTP/2 upstream that answers each request `200` after `hold`, does with
    /// PINGs as `pinged` says on its first connection (and answers them on the rest), and
    /// tells, in order, when each PING arrived and on which connection.
    async fn pinged_upstream(
        hold: Duration,
        pinged: Pinged,
    ) -> (SocketAddr, Rc<RefCell<Vec<(usize, tokio::time::Instant)>>>) {
        use crate::h2_peer::{self, Frame, Peer, code, flag, kind};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let pings = Rc::new(RefCell::new(Vec::new()));
        let recording = Rc::clone(&pings);
        let _accepting = tokio::task::spawn_local(async move {
            let mut connections = 0;
            while let Ok((stream, _)) = socket.accept().await {
                connections += 1;
                let (connection, recording) = (connections, Rc::clone(&recording));
                let pinged = if connection == 1 {
                    pinged
                } else {
                    Pinged::Answers
                };
                let _serving = tokio::task::spawn_local(async move {
                    let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                    let mut due: Vec<(u32, tokio::time::Instant)> = Vec::new();
                    let mut seen = 0;
                    loop {
                        let next_answer = due.first().map(|(_, at)| *at);
                        let frame = match next_answer {
                            Some(at) => tokio::select! {
                                frame = peer.try_next() => frame,
                                () = tokio::time::sleep_until(at) => {
                                    let (stream, _) = due.remove(0);
                                    let answer = h2_peer::headers(stream, h2_peer::response(200), true);
                                    peer.send(&answer).await;
                                    continue;
                                }
                            },
                            None => peer.try_next().await,
                        };
                        let Some(frame) = frame else { return };
                        if frame.kind == kind::HEADERS {
                            due.push((frame.stream, tokio::time::Instant::now() + hold));
                        } else if frame.kind == kind::PING && !frame.has(flag::ACK) {
                            seen += 1;
                            recording
                                .borrow_mut()
                                .push((connection, tokio::time::Instant::now()));
                            let answers = match pinged {
                                Pinged::Answers => true,
                                Pinged::GoesQuiet | Pinged::CalmsDown => seen == 1,
                                Pinged::Deaf => false,
                            };
                            if answers {
                                let ack =
                                    Frame::new(kind::PING, flag::ACK, 0, frame.payload.clone());
                                peer.send(&ack).await;
                            } else if pinged == Pinged::CalmsDown {
                                peer.send(&h2_peer::goaway(0, code::ENHANCE_YOUR_CALM))
                                    .await;
                                return;
                            }
                        }
                    }
                });
            }
        });
        (address, pings)
    }

    /// PINGs go while a call is open, one a second, and stop when the connection is idle.
    #[tokio::test]
    async fn keepalive_pings_while_a_call_is_open_and_not_when_idle() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, pings) =
                    pinged_upstream(Duration::from_millis(2500), Pinged::Answers).await;
                let (front, _worker) =
                    serving_worker_with_keepalive(upstream, every_second(false)).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                // The settling PING, then one a second while the call was open.
                let while_open = pings.borrow().len();
                assert!(
                    while_open >= 3,
                    "{while_open} PINGs while the call was open"
                );
                tokio::time::sleep(Duration::from_millis(2500)).await;
                assert_eq!(pings.borrow().len(), while_open, "PINGs with no call open");
            })
            .await;
    }

    /// A PING nobody answers in time takes its connection for dead: what is on it fails,
    /// and the next request goes on a new one.
    #[tokio::test]
    async fn a_ping_nobody_answers_takes_its_connection_for_dead() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, pings) =
                    pinged_upstream(Duration::from_secs(8), Pinged::GoesQuiet).await;
                let (front, worker) =
                    serving_worker_with_keepalive(upstream, every_second(false)).await;
                let asked = tokio::time::Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                let took = asked.elapsed();
                assert!(
                    took >= Duration::from_secs(2) && took < Duration::from_secs(4),
                    "given up on after {took:?}"
                );
                until(|| worker.h2_connections() == 0).await;
                assert!(pings.borrow().len() >= 2);
            })
            .await;
    }

    /// A connection that never answers even its first PING is as dead as one that stops:
    /// it is given up on at the keepalive's timeout.
    #[tokio::test]
    async fn a_connection_that_never_answers_a_ping_is_given_up_on() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _pings) =
                    pinged_upstream(Duration::from_secs(8), Pinged::Deaf).await;
                let (front, _worker) =
                    serving_worker_with_keepalive(upstream, every_second(false)).await;
                let asked = tokio::time::Instant::now();
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
                let took = asked.elapsed();
                assert!(took < Duration::from_secs(3), "given up on after {took:?}");
            })
            .await;
    }

    /// Told to calm down, the client waits twice as long between PINGs on its next
    /// connection to the same upstream: gRPC's backoff, so there is no storm of them.
    #[tokio::test]
    async fn told_to_calm_down_the_next_connection_pings_half_as_often() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, pings) = pinged_upstream(Duration::ZERO, Pinged::CalmsDown).await;
                let (front, worker) =
                    serving_worker_with_keepalive(upstream, every_second(true)).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                // Its first keepalive PING draws the GOAWAY, and the connection goes.
                until(|| pings.borrow().len() >= 2).await;
                until(|| worker.h2_connections() == 0).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                until(|| pings.borrow().iter().filter(|(on, _)| *on == 2).count() >= 2).await;
                let second: Vec<tokio::time::Instant> = pings
                    .borrow()
                    .iter()
                    .filter(|(on, _)| *on == 2)
                    .map(|(_, at)| *at)
                    .collect();
                let gap = second[1] - second[0];
                assert!(
                    gap >= Duration::from_millis(1800) && gap < Duration::from_millis(2800),
                    "{gap:?} between the second connection's PINGs"
                );
            })
            .await;
    }

    /// An HTTP/1 upstream that answers `/healthz` 200 while `serving` says so and 503
    /// otherwise, and every other request 200 with its name in `x-upstream`.
    async fn checked_upstream(name: &'static str, serving: Rc<Cell<bool>>) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let serving = Rc::clone(&serving);
                let _answering = tokio::task::spawn_local(async move {
                    let mut seen = Vec::new();
                    let mut byte = [0; 1];
                    loop {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => seen.push(byte[0]),
                        }
                        if !seen.ends_with(b"\r\n\r\n") {
                            continue;
                        }
                        let probe = seen.starts_with(b"GET /healthz ");
                        seen.clear();
                        let answer = if probe && !serving.get() {
                            "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n"
                                .to_owned()
                        } else {
                            format!(
                                "HTTP/1.1 200 OK\r\nx-upstream: {name}\r\ncontent-length: 0\r\n\r\n"
                            )
                        };
                        if stream.write_all(answer.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        address
    }

    /// A worker whose one upstream `up` has `endpoints`, checked as `check` says, with the
    /// health checker running.
    async fn serving_checked_worker(
        endpoints: Vec<SocketAddr>,
        protocol: UpstreamProtocol,
        check: edgerush_config::HealthCheck,
    ) -> (SocketAddr, Rc<Worker>) {
        let mut config = everything_config(endpoints[0]);
        let up = config.upstreams.get_mut("up").unwrap();
        up.endpoints = endpoints;
        up.protocol = protocol;
        up.health_check = Some(check);
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
        let _checking = tokio::task::spawn_local(Arc::clone(&proxy).check_health());
        let worker = Worker::with_deadlines(proxy, H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        (front, worker)
    }

    fn every_second_by(probe: edgerush_config::Probe) -> edgerush_config::HealthCheck {
        edgerush_config::HealthCheck {
            interval_seconds: 1,
            timeout_seconds: 1,
            healthy_threshold: 1,
            unhealthy_threshold: 1,
            probe,
        }
    }

    fn healthz() -> edgerush_config::Probe {
        edgerush_config::Probe::Http {
            path: "/healthz".to_owned(),
        }
    }

    /// How many endpoints of `up` the scrape says serve.
    fn serving_now(worker: &Worker) -> String {
        let scrape = worker.proxy().metrics();
        scrape
            .lines()
            .find(|line| line.starts_with("edgerush_upstream_healthy_endpoints{upstream=\"up\"}"))
            .unwrap_or_default()
            .rsplit(' ')
            .next()
            .unwrap_or_default()
            .to_owned()
    }

    /// Until the scrape says `count` endpoints serve, for up to ten seconds.
    async fn until_serving(worker: &Worker, count: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while serving_now(worker) != count {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("never {count} serving; now {}", serving_now(worker)));
    }

    /// An endpoint that fails its checks gets no requests while it does, and gets them
    /// again once it passes.
    #[tokio::test]
    async fn an_endpoint_failing_its_checks_is_kept_out_until_it_passes() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let steady = checked_upstream("steady", Rc::new(Cell::new(true))).await;
                let shaky_serving = Rc::new(Cell::new(false));
                let shaky = checked_upstream("shaky", Rc::clone(&shaky_serving)).await;
                let (front, worker) = serving_checked_worker(
                    vec![steady, shaky],
                    UpstreamProtocol::Http1,
                    every_second_by(healthz()),
                )
                .await;
                until_serving(&worker, "1").await;
                for _ in 0..20 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(answer.contains("x-upstream: steady\r\n"), "{answer}");
                }
                shaky_serving.set(true);
                until_serving(&worker, "2").await;
                let mut answered_by_shaky = false;
                for _ in 0..40 {
                    answered_by_shaky |= h1_answer(front, CLOSING_GET)
                        .await
                        .contains("x-upstream: shaky\r\n");
                }
                assert!(answered_by_shaky, "a healthy endpoint got nothing");
            })
            .await;
    }

    /// When fewer than half the endpoints pass, their health is set aside: requests go on
    /// to all of them rather than being answered 503.
    #[tokio::test]
    async fn with_most_endpoints_failing_their_health_is_set_aside() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let one = checked_upstream("one", Rc::new(Cell::new(false))).await;
                let two = checked_upstream("two", Rc::new(Cell::new(false))).await;
                let three = checked_upstream("three", Rc::new(Cell::new(true))).await;
                let (front, worker) = serving_checked_worker(
                    vec![one, two, three],
                    UpstreamProtocol::Http1,
                    every_second_by(healthz()),
                )
                .await;
                until_serving(&worker, "1").await;
                let mut unhealthy_answered = false;
                for _ in 0..40 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
                    unhealthy_answered |= !answer.contains("x-upstream: three\r\n");
                }
                assert!(unhealthy_answered, "health was not set aside below half");
            })
            .await;
    }

    /// A gRPC health check passes an endpoint whose health service says `SERVING`, and
    /// fails one that says anything else.
    #[tokio::test]
    async fn a_grpc_health_check_passes_only_serving() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let state = Rc::new(Cell::new(1_u8));
                let saying = Rc::clone(&state);
                let script: Script = Rc::new(move |request, mut respond| {
                    let saying = Rc::clone(&saying);
                    Box::pin(async move {
                        assert_eq!(request.uri().path(), "/grpc.health.v1.Health/Check");
                        let mut body = request.into_body();
                        let _ = read_all(&mut body).await;
                        let Ok(mut sending) = respond.send_response(grpc_head(), false) else {
                            return;
                        };
                        let message = [0, 0, 0, 0, 2, 0x08, saying.get()];
                        let _ = sending.send_data(Bytes::copy_from_slice(&message), false);
                        let mut status = http::HeaderMap::new();
                        status.insert("grpc-status", "0".parse().unwrap());
                        let _ = sending.send_trailers(status);
                    })
                });
                let upstream = scripted_h2_upstream(script).await;
                let probe = edgerush_config::Probe::Grpc {
                    service: String::new(),
                };
                let (_front, worker) = serving_checked_worker(
                    vec![upstream],
                    UpstreamProtocol::Http2,
                    every_second_by(probe),
                )
                .await;
                // NOT_SERVING takes it out, SERVING brings it back.
                state.set(2);
                until_serving(&worker, "0").await;
                state.set(1);
                until_serving(&worker, "1").await;
            })
            .await;
    }

    /// A client that gives its request up gives its stream's place back: with room for one
    /// stream on one connection, the next request is served rather than kept waiting.
    #[tokio::test]
    async fn a_request_given_up_gives_its_http2_place_back() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let how = UpstreamH2 {
                    gated: true,
                    ..UpstreamH2::default()
                };
                let (upstream, seen, gate) = h2_upstream(how).await;
                let limits = H1Limits {
                    h2_streams: 1,
                    h2_connections: 1,
                    ..H1Limits::default()
                };
                let (front, _worker) = serving_worker_to_h2(upstream, limits).await;
                let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
                let request = Request::get("http://a.test/").body(()).unwrap();
                let (answer, _sending) = send.send_request(request, true).unwrap();
                until(|| seen.open.get() == 1).await;
                // Dropping what waits for the answer resets the stream.
                drop(answer);
                drop(_sending);
                tokio::time::sleep(Duration::from_millis(100)).await;
                // One permit for the given-up stream, which finds it reset; one for this.
                gate.add_permits(2);
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            })
            .await;
    }

    /// A TLS ClientHello sent to a plaintext listener is not the HTTP/2 preface, so it
    /// goes to HTTP/1, which refuses it as a request line that is not one and closes:
    /// nothing hangs waiting for a preface that will never come.
    #[tokio::test]
    async fn a_tls_hello_on_a_plaintext_listener_is_refused_as_http1() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                let mut stream = TcpStream::connect(front).await.unwrap();
                stream
                    .write_all(b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03")
                    .await
                    .unwrap();
                let mut answer = Vec::new();
                let _ended =
                    tokio::time::timeout(SHORT.first_request / 2, stream.read_to_end(&mut answer))
                        .await
                        .unwrap_or_else(|_| panic!("still open"));
                let answer = String::from_utf8_lossy(&answer);
                assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
            })
            .await;
    }

    /// A worker takes on only so many exchanges at once, whichever client carries them,
    /// and answers the rest rather than opening another connection for them. The place is
    /// given back by every way out of an exchange, a failed one included, so a worker that
    /// has been full is not full for ever ([13 §7](../../docs/13-http1-upstream.md)).
    #[test]
    fn a_worker_full_of_exchanges_answers_rather_than_take_another() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let (upstream, held) = scripted_upstream().await;
            let limits = H1Limits {
                exchanges: 2,
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(served(upstream), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            // Two that will not come back, waited for where the worker has committed
            // to them rather than where they were sent.
            for _ in 0..2 {
                let _parked = tokio::task::spawn_local(async move {
                    let _never = status_of(front, "/silent").await;
                });
            }
            until(|| held.borrow().len() == 2).await;

            // Bounded, because a worker that does not refuse it holds it for as long
            // as the upstream says nothing, which is for ever.
            let refused = tokio::time::timeout(Duration::from_secs(5), status_of(front, "/ok"))
                .await
                .unwrap_or_else(|_| panic!("took on a third exchange"));
            assert_eq!(refused, StatusCode::SERVICE_UNAVAILABLE);

            // The upstream lets both go without answering, so both exchanges fail; a
            // failure gives its place back like any other ending, and the worker
            // takes requests again.
            held.borrow_mut().clear();
            until(|| worker.in_flight.get() == 0).await;
            assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        }));
    }

    /// Every way out of an exchange gives its place back, whichever client carried it: an
    /// upstream that could not be reached, an answer with no body, one read to its end, one
    /// the client stopped reading, one that failed part way, and a client that went before
    /// anything came back. A place that one of them kept would be kept for ever, and a
    /// worker would fill up with exchanges nobody has in hand.
    #[test]
    fn every_way_out_of_an_exchange_gives_its_place_back() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let serving = |upstream| {
                let worker = Worker::with_limits(served(upstream), H1Limits::default());
                async move {
                    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let front = socket.local_addr().unwrap();
                    let _serving = serving(&worker, socket);
                    (worker, front)
                }
            };
            let given_back = |worker: &Rc<Worker>, case: &str| {
                let worker = Rc::clone(worker);
                let case = case.to_owned();
                async move {
                    tokio::time::timeout(Duration::from_secs(10), async {
                        while worker.in_flight.get() != 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap_or_else(|_| panic!("{case}: the place was kept"));
                }
            };

            // Nothing listening where the upstream should be.
            let (_held, nowhere) = refusing();
            let (worker, front) = serving(nowhere).await;
            assert_eq!(status_of(front, "/ok").await, StatusCode::BAD_GATEWAY);
            given_back(&worker, "unreachable").await;

            let (upstream, held) = scripted_upstream().await;
            let (worker, front) = serving(upstream).await;

            assert_eq!(status_of(front, "/nothing").await, StatusCode::NO_CONTENT);
            given_back(&worker, "no body").await;

            assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
            given_back(&worker, "a whole body").await;

            // Taken as far as the answer's first bytes, then no further.
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/endless")).await.unwrap();
            let mut some = [0; 1024];
            let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut some))
                .await
                .expect("no answer");
            assert!(read.unwrap() > 0, "closed before answering");
            assert_eq!(worker.in_flight.get(), 1, "not in hand while answering");
            drop(client);
            given_back(&worker, "a client that stopped reading").await;

            // The head says ten bytes, and five come before the upstream goes.
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/short")).await.unwrap();
            let mut rest = Vec::new();
            let _ended =
                tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut rest))
                    .await
                    .expect("the failed answer was never ended");
            given_back(&worker, "a body that failed").await;

            // Asked, and gone before the upstream says anything.
            let mut client = TcpStream::connect(front).await.unwrap();
            client.write_all(&asking("/silent")).await.unwrap();
            until(|| held.borrow().len() == 1).await;
            assert_eq!(worker.in_flight.get(), 1, "not in hand while waiting");
            drop(client);
            given_back(&worker, "a client that went").await;
        }));
    }

    /// A request for `path` as a client would write it, asking for the connection to be
    /// closed after the answer so that reading to the end reads the one answer.
    fn asking(path: &str) -> Vec<u8> {
        format!("GET {path} HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n")
            .into_bytes()
    }

    /// An upstream that answers by the last part of the path it is asked for, one request
    /// after another on a connection: `nothing` with a 204, `ok` with a short body,
    /// `endless` with a body that never ends, `short` with a body that stops part way, and
    /// anything else not at all, its connection held open in what is returned.
    async fn scripted_upstream() -> (SocketAddr, Rc<RefCell<Vec<TcpStream>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = backend.local_addr().unwrap();
        let held = Rc::new(RefCell::new(Vec::new()));
        let holding = Rc::clone(&held);
        let _accepting = tokio::task::spawn_local(async move {
            loop {
                let (mut stream, _) = backend.accept().await.unwrap();
                let holding = Rc::clone(&holding);
                let _answering = tokio::task::spawn_local(async move {
                    let mut seen = Vec::new();
                    let mut byte = [0; 1];
                    loop {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => seen.push(byte[0]),
                        }
                        if !seen.ends_with(b"\r\n\r\n") {
                            continue;
                        }
                        let asked = String::from_utf8_lossy(&seen).into_owned();
                        seen.clear();
                        let target = asked.split(' ').nth(1).unwrap_or_default();
                        let answer: &[u8] = match target.rsplit('/').next() {
                            Some("nothing") => b"HTTP/1.1 204 No Content\r\n\r\n",
                            Some("ok") => b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
                            Some("endless") => {
                                let head =
                                    b"HTTP/1.1 200 OK\r\ncontent-length: 1000000000000\r\n\r\n";
                                let mut said = stream.write_all(head).await;
                                while said.is_ok() {
                                    said = stream.write_all(&[b'x'; 16 * 1024]).await;
                                }
                                return;
                            }
                            Some("short") => {
                                let head = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nshort";
                                let _said = stream.write_all(head).await;
                                // Long enough for the head to be on its way to the client
                                // before the body stops.
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                return;
                            }
                            _ => {
                                holding.borrow_mut().push(stream);
                                return;
                            }
                        };
                        if stream.write_all(answer).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (address, held)
    }

    /// A data plane for `upstream`.
    fn served(upstream: SocketAddr) -> Arc<Proxy> {
        Arc::new(Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap())
    }

    /// How many connections a worker opens for `requests` sent one after another, when it
    /// may keep `idle_per_destination` of them.
    async fn connections_for(keeping: usize, requests: usize) -> usize {
        let (upstream, opened) = counting_upstream().await;
        let limits = H1Limits {
            idle_per_destination: keeping,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(sending_to(upstream), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = serving(&worker, socket);
        for _ in 0..requests {
            assert_eq!(status_over_http1(front).await, StatusCode::OK);
        }
        opened.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// How many idle connections a worker keeps to one destination is the data plane's
    /// bound and not one client's, so it holds whichever client carries the request.
    /// Left to itself the engine's pool keeps as many as it likes, which is a different
    /// proxy from the one [13 §7](../../docs/13-http1-upstream.md) describes.
    #[test]
    fn the_bound_on_idle_connections_holds_for_every_client() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            // Keeping none: every request after the first opens its own connection.
            assert_eq!(connections_for(0, 3).await, 3, "keeping none");
            // Keeping one: the first connection carries all three.
            assert_eq!(connections_for(1, 3).await, 1, "keeping one");
        }));
    }

    /// The worker's sweep reaches every pool it keeps: a connection left idle past its
    /// time is closed by the sweep, whichever client's it is, with nothing asking for it
    /// again ([13 §3](../../docs/13-http1-upstream.md)).
    #[test]
    fn the_sweep_closes_what_every_pool_left_idle_too_long() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let (upstream, _opened) = counting_upstream().await;
            let limits = H1Limits {
                idle_timeout: Duration::from_millis(100),
                sweep: Duration::from_millis(50),
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(sending_to(upstream), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
            assert_eq!(status_over_http1(front).await, StatusCode::OK);
            until(|| worker.idle_connections() == 1).await;

            let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            tokio::time::timeout(Duration::from_secs(5), async {
                while worker.idle_connections() != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("an idle connection outlived the sweep"));
        }));
    }

    /// What became of every connection is counted, and so is a worker's own holding, for
    /// every client over this worker's pool. A benchmark that cannot tell a reused
    /// connection from a fresh one is measuring the wrong thing
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    #[test]
    fn what_became_of_a_connection_is_counted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let (upstream, _opened) = counting_upstream().await;
            let proxy = sending_to(upstream);
            let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            for _ in 0..3 {
                assert_eq!(status_over_http1(front).await, StatusCode::OK);
            }
            // One connection opened for the first request, and taken again for the rest.
            let scrape = proxy.metrics.render(&["web".to_owned()], &[], &[]);
            assert!(
                scrape.contains(
                    "edgerush_upstream_connections_total{state=\"opened\"} 1
"
                ),
                "{scrape}"
            );
            assert!(
                scrape.contains(
                    "edgerush_upstream_connections_total{state=\"reused\"} 2
"
                ),
                "{scrape}"
            );
            // And what the worker holds, which it says as it sweeps.
            let storage = worker.blocks.borrow().storage().used();
            proxy.metrics.worker().holding(
                worker.in_flight.get(),
                worker.idle_connections(),
                storage,
            );
            let scrape = proxy.metrics.render(&["web".to_owned()], &[], &[]);
            assert!(
                scrape.contains(
                    "edgerush_upstream_connections_idle 1
"
                ),
                "{scrape}"
            );
            assert!(storage > 0, "a connection kept holds a block");
            assert!(
                scrape.contains(&format!("\nedgerush_worker_storage_bytes {storage}\n")),
                "{scrape}"
            );
        }));
    }

    /// An exchange the worker could not pay for is counted as that, and not as the
    /// connection failing, which is what it would pass for among the I/O failures
    /// (14 §8).
    #[test]
    fn an_exchange_the_worker_could_not_pay_for_is_counted_as_that() {
        let exhausted = crate::storage::Storage::new(0).reserve(1).unwrap_err();
        assert_eq!(
            why_stopped(&ExchangeError::Exhausted(exhausted)),
            Stopped::Exhausted
        );
    }

    /// And the client is answered 503, counted under an answer reason of its own and not
    /// against the upstream, which did not fail: the worker did (14 §8). Asked over HTTP/2,
    /// whose server keeps storage of its own, so that the request is read and it is the
    /// exchange that has nothing to pay with; our own HTTP/1 server could not read the head
    /// on nothing, and would close.
    #[test]
    fn an_exchange_the_worker_cannot_pay_for_is_answered_503() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = listener.local_addr().unwrap();
            // Answers at once, so that a worker that could pay would be seen to.
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        let mut asked = [0; 1024];
                        let _read = stream.read(&mut asked).await;
                        let _said = stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await;
                    });
                }
            });

            let proxy = served(upstream);
            let limits = H1Limits {
                storage: 0,
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::clone(&proxy), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            assert_eq!(
                status_over_http2(front).await,
                StatusCode::SERVICE_UNAVAILABLE
            );
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains(
                    "edgerush_listener_local_answers_total{listener=\"web\",reason=\"exhausted\"} 1\n"
                ),
                "{scrape}"
            );
            assert!(
                scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        }));
    }

    /// A request whose body cannot be read is the client's fault, found after the request
    /// went upstream: it is answered 400 and counted as that, not as the upstream failing,
    /// which is what a 502 would have said.
    #[test]
    fn a_body_that_cannot_be_read_is_answered_400_and_not_held_against_the_upstream() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = listener.local_addr().unwrap();
            // Takes what it is sent and never answers: only the client can end this.
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        let mut taken = [0; 1024];
                        while matches!(stream.read(&mut taken).await, Ok(read) if read > 0) {}
                    });
                }
            });

            let proxy = served(upstream);
            let worker = Worker::new(Arc::clone(&proxy));
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut client = TcpStream::connect(front).await.unwrap();
            client
                .write_all(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
                .await
                .unwrap();
            let mut answer = Vec::new();
            let _read = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                client.read_to_end(&mut answer),
            )
            .await
            .unwrap();
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");

            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains(
                    "edgerush_listener_local_answers_total{listener=\"web\",reason=\"bad_body\"} 1\n"
                ),
                "{scrape}"
            );
            assert!(
                scrape.contains("edgerush_upstream_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        }));
    }

    /// **Many incomplete large heads, pinned frames and slow consumers, all at once**
    /// (14 §8), against a worker whose storage is a fraction of what they ask for and far
    /// below its exchange cap: at every moment it is looked at, it holds no more than its
    /// limit and provision; the limit, not the cap, is what stops it, since it comes within
    /// a grown block of it; and once they have all gone it holds no more than a quiet worker
    /// keeps, and serves again.
    #[test]
    fn a_worker_under_every_kind_of_load_at_once_stays_within_its_storage() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        const LIMIT: usize = 1 << 20;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            // An upstream that reads no more of an upload than its head, answers `/big`
            // with more than any client here will read, and `/ok` at once.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        let mut head = Vec::new();
                        let mut byte = [0; 1];
                        while !head.ends_with(b"\r\n\r\n") {
                            if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                                return;
                            }
                            head.push(byte[0]);
                        }
                        if head.starts_with(b"POST /upload") {
                            std::future::pending::<()>().await;
                        } else if head.starts_with(b"GET /big") {
                            // Said to close: a client's socket buffers can take the whole
                            // answer, and the connection then goes back to the pool, where
                            // a later request would wait on it for ever — this upstream
                            // answers one request a connection.
                            let length = 1 << 20;
                            let said = format!(
                                "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: {length}\r\n\r\n"
                            );
                            let _said = stream.write_all(said.as_bytes()).await;
                            let _said = stream.write_all(&vec![b'x'; length]).await;
                            std::future::pending::<()>().await;
                        } else {
                            let _said = stream
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                                .await;
                        }
                    });
                }
            });

            let proxy = served(upstream);
            // An upload stalled on its upstream is not read, so its client going unseen until
            // the exchange's own wait runs out: shortened here, so that the test sees the end
            // of what an upload pins without waiting the usual thirty seconds for it.
            let limits = H1Limits {
                storage: LIMIT,
                idle: Duration::from_secs(3),
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::clone(&proxy), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
            let _maintaining = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            let storage = Rc::clone(worker.blocks.borrow().storage());

            // Each client holds its connection until it is aborted.
            let client = |sent: Vec<u8>| {
                tokio::task::spawn_local(async move {
                    let mut client = TcpStream::connect(front).await.unwrap();
                    let _sent = client.write_all(&sent).await;
                    std::future::pending::<()>().await;
                })
            };
            let mut clients = Vec::new();
            // Uploads whose frames the upstream never takes, pinning the blocks they were
            // cut from once the sockets' own buffers are full: longer than any of those,
            // written until the worker stops reading them.
            for _ in 0..10 {
                clients.push(tokio::task::spawn_local(async move {
                    let mut client = TcpStream::connect(front).await.unwrap();
                    let head = b"POST /upload HTTP/1.1\r\nhost: example.test\r\n\
                        content-length: 1073741824\r\n\r\n";
                    let _sent = client.write_all(head).await;
                    let piece = vec![b'u'; 64 * 1024];
                    while client.write_all(&piece).await.is_ok() {}
                    std::future::pending::<()>().await;
                }));
            }
            // Clients that ask for a large answer and never read it.
            for _ in 0..10 {
                clients.push(client(
                    b"GET /big HTTP/1.1\r\nhost: example.test\r\n\r\n".to_vec(),
                ));
            }
            // The uploads and the slow consumers are under way, their frames pinned and
            // their answers queued, before the heads arrive to compete with them.
            tokio::time::sleep(Duration::from_millis(500)).await;
            // Heads that never end, each longer than a small block holds. Those the worker
            // cannot pay to read are closed, and say so.
            let turned_away = Rc::new(Cell::new(0));
            for _ in 0..40 {
                let mut head = b"GET / HTTP/1.1\r\nhost: example.test\r\nx-pad: ".to_vec();
                head.extend(std::iter::repeat_n(b'a', 50 * 1024));
                let turned_away = Rc::clone(&turned_away);
                clients.push(tokio::task::spawn_local(async move {
                    let mut client = TcpStream::connect(front).await.unwrap();
                    let _sent = client.write_all(&head).await;
                    let mut nothing = [0; 64];
                    if matches!(client.read(&mut nothing).await, Ok(0) | Err(_)) {
                        turned_away.set(turned_away.get() + 1);
                    }
                    std::future::pending::<()>().await;
                }));
            }

            let ceiling = LIMIT + crate::storage::PROVISION;
            let (mut most, mut outlived) = (0, 0);
            for _ in 0..250 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let used = storage.used();
                assert!(used <= ceiling, "{used} held, past {ceiling}");
                most = most.max(used);
                outlived = outlived.max(storage.outlived());
            }
            let sizes = worker.blocks.borrow().sizes();
            assert!(
                most + sizes.large > LIMIT,
                "the most held was {most}: the limit was never what stopped it"
            );
            assert!(turned_away.get() > 0, "no head was turned away");
            // Memory held by frames alone, its blocks let go of by a sweep while the frames
            // wait on the upstream, was among what was counted.
            assert!(outlived > 0, "no pinned memory was ever held to account");
            assert!(worker.in_flight.get() < limits.exchanges);

            for client in &clients {
                client.abort();
            }
            // Closes are noticed, stalled uploads given up on at their exchanges' wait, and a
            // sweep or two finds the last pinned frames gone and trims what is parked.
            tokio::time::sleep(limits.idle + limits.sweep * 3).await;
            let staging = 16 * 1024;
            let kept = sizes.kept * (sizes.small + sizes.large + staging);
            let used = storage.used();
            assert!(
                used <= kept,
                "{used} held once it was all over, past {kept}"
            );
            assert_eq!(storage.outlived(), 0, "pinned memory still charged");
            assert_eq!(status_of(front, "/ok").await, StatusCode::OK);
        }));
    }

    /// A local answer of the request core reaches the client from a worker that has run
    /// out, as our own server writes it from the provision (14 §8): here the head that asks
    /// takes the whole limit, and names no host.
    #[test]
    fn a_worker_that_has_run_out_still_gives_its_own_answers() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let (_held, nowhere) = refusing();
            let proxy = served(nowhere);
            let limits = H1Limits {
                storage: crate::upstream::h1::blocks::SMALL,
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::clone(&proxy), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            let mut client = tokio::net::TcpStream::connect(front).await.unwrap();
            client
                .write_all(b"GET / HTTP/1.1\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut received = Vec::new();
            tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut received))
                .await
                .unwrap()
                .unwrap();
            assert!(
                received.starts_with(b"HTTP/1.1 400 "),
                "{}",
                String::from_utf8_lossy(&received)
            );
        }));
    }

    /// An exchange that ends without an answer says which of the named reasons it
    /// was. The names are a fixed list: an upstream that fails in a new way does not
    /// get to make a new series, and no error text reaches a label.
    #[test]
    fn why_an_exchange_stopped_is_counted_by_a_name_of_ours() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            // An upstream that waits to be asked and then says something that is not
            // an answer.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut asked = [0; 1024];
                        let _read = stream.read(&mut asked).await;
                        let _said = stream.write_all(b"nonsense\r\n\r\n").await;
                    });
                }
            });

            let proxy = sending_to(upstream);
            let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            assert_eq!(status_over_http1(front).await, StatusCode::BAD_GATEWAY);
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy
                .metrics
                .render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains("edgerush_upstream_exchanges_stopped_total{reason=\"codec\"} 1\n"),
                "{scrape}"
            );
            // And the answer never arrived, so nothing counted it as a body that
            // failed part way: the two are different things.
            assert!(
                scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 0\n"),
                "{scrape}"
            );
        }));
    }

    /// A body that fails after its head has gone is counted where nothing else
    /// would notice it: the status was counted as a success and the client was told
    /// so, and only the body knew otherwise
    /// ([13 §7](../../docs/13-http1-upstream.md)). Both paths count it.
    #[test]
    fn a_body_that_fails_after_its_head_is_counted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async move {
            // A head that promises ten bytes, five bytes, and then the end.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut asked = [0; 1024];
                        let _read = stream.read(&mut asked).await;
                        let _said = stream
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nshort")
                            .await;
                        // Long enough for the head to reach the client before
                        // the body stops. Which of the two a client sees when a
                        // body fails is the downstream server's buffering rather
                        // than anything decided here, and this test is about the
                        // counter rather than about that.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    });
                }
            });

            let proxy = sending_to(upstream);
            let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = serving(&worker, socket);

            // The head arrives and says the answer succeeded; the body does not.
            let stream = TcpStream::connect(front).await.unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .unwrap();
            let _driving = tokio::task::spawn_local(async move {
                let _closed = connection.await;
            });
            let request = Request::builder()
                .uri("/")
                .header("host", "example.test")
                .body(Empty::<Bytes>::new())
                .unwrap();
            let answer = sender.send_request(request).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            assert!(
                answer.into_body().collect().await.is_err(),
                "a truncated body was made to look whole"
            );

            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy
                .metrics
                .render(&["web".to_owned()], &[("up", up)], &[]);
            assert!(
                scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 1\n"),
                "{scrape}"
            );
            // And the answer was counted a success, which is why the body needed
            // a counter of its own.
            assert!(
                scrape.contains(
                    "edgerush_upstream_responses_total{upstream=\"up\",class=\"2xx\"} 1\n"
                ),
                "{scrape}"
            );
        }));
    }

    /// A proxy of one listener that sends everything to `upstream`, by EdgeRush's own
    /// path because that is the path with a bound on it.
    fn sending_to(upstream: SocketAddr) -> Arc<Proxy> {
        Arc::new(Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap())
    }

    /// A config of one listener that sends everything to `upstream`.
    fn everything_to(upstream: SocketAddr) -> Compiled {
        compile(&everything_config(upstream)).unwrap()
    }

    /// The same, with `web` speaking TLS and presenting `certificates`.
    fn everything_secured_to(
        upstream: SocketAddr,
        certificates: Vec<edgerush_config::Certificate>,
    ) -> Compiled {
        let mut config = everything_config(upstream);
        let web = config.listeners.get_mut("web").unwrap();
        web.protocol = edgerush_config::Protocol::Https;
        web.tls = Some(edgerush_config::Tls {
            certificates,
            client_validation: None,
        });
        compile(&config).unwrap()
    }

    fn everything_config(upstream: SocketAddr) -> Config {
        let yaml = format!(
            r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
        );
        serde_saphyr::from_str(&yaml).unwrap()
    }

    /// Waits for something the test is about to depend on, and fails rather than hangs
    /// if it never happens.
    async fn until(mut settled: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !settled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waited for something that never happened");
    }

    /// What one HTTP/1.1 request to `address` is answered with.
    async fn status_over_http1(address: SocketAddr) -> StatusCode {
        status_of(address, "/").await
    }

    /// What one HTTP/1.1 request for `path` to `address` is answered with.
    /// An address that refuses every connection for as long as the socket returned with it
    /// is held: bound, so that nothing else can be given its port, and never listening.
    ///
    /// **Not a port read from a socket that was then let go of.** That hands the port back
    /// for the operating system to give to whatever asks next, and a test running beside
    /// this one was given it and answered 200 where a 502 was expected.
    fn refusing() -> (tokio::net::TcpSocket, SocketAddr) {
        let held = tokio::net::TcpSocket::new_v4().unwrap();
        held.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = held.local_addr().unwrap();
        (held, address)
    }

    async fn status_of(address: SocketAddr, path: &str) -> StatusCode {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _detached = tokio::task::spawn_local(async move {
            let _closed = connection.await;
        });
        // Routed like any other request, so it needs a host to be routed by.
        let request = Request::builder()
            .uri(path)
            .header("host", "example.test")
            .body(Empty::<Bytes>::new())
            .unwrap();
        sender.send_request(request).await.unwrap().status()
    }

    /// The status one HTTP/2 request to `address` is answered with.
    async fn status_over_http2(address: SocketAddr) -> StatusCode {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(OnThisWorker, TokioIo::new(stream))
                .await
                .unwrap();
        let _detached = tokio::task::spawn_local(async move {
            let _closed = connection.await;
        });
        let mut request = Request::new(Empty::<Bytes>::new());
        *request.version_mut() = Version::HTTP_2;
        *request.uri_mut() = format!("http://{address}/").parse().unwrap();
        sender.send_request(request).await.unwrap().status()
    }

    /// A config with the named upstreams, each at the address given.
    fn upstreams(named: &[(&str, &str)]) -> Compiled {
        let mut yaml = String::from("listeners: {}\nroutes: []\nupstreams:\n");
        for (name, address) in named {
            yaml += &format!("  {name}: {{ endpoints: [\"{address}\"] }}\n");
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    /// What a destination of the running config is filed under.
    fn filed_under(proxy: &Proxy, upstream: usize) -> u64 {
        proxy
            .current
            .load()
            .destinations
            .at(upstream, 0)
            .expect("a destination")
            .key()
    }

    /// Reconciling happens where a config is published, so a real reload keeps what has
    /// not changed and retires what has gone — not only the reconciler asked on its own.
    #[test]
    fn a_reload_keeps_what_has_not_changed_and_retires_what_has() {
        let proxy = Proxy::new(
            upstreams(&[("web", "127.0.0.1:1"), ("zed", "127.0.0.1:2")]),
            NonZeroUsize::MIN,
        )
        .unwrap();
        let web = filed_under(&proxy, 0);
        let zed = Arc::clone(proxy.current.load().destinations.at(1, 0).unwrap());

        // `aaa` sorts first, so every upstream after it moves along one, and `zed` goes.
        proxy
            .reload(upstreams(&[("aaa", "127.0.0.1:3"), ("web", "127.0.0.1:1")]))
            .unwrap();

        assert_eq!(filed_under(&proxy, 1), web, "web changed hands on a reload");
        assert_ne!(filed_under(&proxy, 0), web, "the newcomer took web's place");
        assert!(zed.is_retired(), "an upstream that is gone was left live");
    }

    /// An upstream that answers every request on whatever connection it arrives on, and
    /// counts how many connections it was given. Its answers are bytes, so that what is
    /// read back is what really went over the wire.
    async fn counting_upstream() -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = Arc::clone(&opened);
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = socket.accept().await.unwrap();
                counting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    // One request after another on the one connection.
                    let mut seen = Vec::new();
                    let mut byte = [0; 1];
                    loop {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => seen.push(byte[0]),
                        }
                        if seen.ends_with(b"\r\n\r\n") {
                            seen.clear();
                            let answer = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                            if stream.write_all(answer.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        (address, opened)
    }

    /// Reads a body to its end and says whether its connection went back.
    async fn drain(
        mut body: H1Body<UpstreamSocket, http_body_util::Empty<Bytes>>,
        limits: &H1Limits,
    ) -> (Vec<u8>, bool) {
        use http_body_util::BodyExt;

        let mut data = Vec::new();
        while let Some(frame) = body.frame().await {
            if let Ok(bytes) = frame.unwrap().into_data() {
                data.extend_from_slice(bytes.as_ref());
            }
        }
        match body.take_if_reusable() {
            Some(kept) => {
                kept.put_back(limits);
                (data, true)
            }
            None => (data, false),
        }
    }

    /// **The whole path.** A request goes out on a connection opened for it, the answer
    /// is read to its end, the connection goes back, and the next request is given the
    /// same one — which the upstream can see, because it was only ever accepted once.
    #[tokio::test]
    async fn a_connection_that_finished_carries_the_next_request_too() {
        let (upstream, opened) = counting_upstream().await;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let config = upstreams(&[("web", &upstream.to_string())]);
                let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN).unwrap());
                let worker = Worker::new(Arc::clone(&proxy));
                let identity = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());

                for round in 0..3 {
                    let (head, body) = worker
                        .through_h1(
                            &identity,
                            &Method::GET,
                            &"/x".parse().unwrap(),
                            &http::HeaderMap::new(),
                            &[],
                            Sending::None,
                            http_body_util::Empty::<Bytes>::new(),
                            None,
                            false,
                            false,
                        )
                        .await
                        .unwrap();
                    assert_eq!(head.status(), 200, "round {round}");

                    let (data, went_back) = drain(*body, &worker.limits).await;
                    assert_eq!(data, b"ok", "round {round}");
                    assert!(went_back, "round {round} did not put its connection back");
                    assert_eq!(worker.idle_connections(), 1, "round {round}");
                }

                assert_eq!(
                    opened.load(std::sync::atomic::Ordering::SeqCst),
                    1,
                    "the upstream was given a new connection for a later request"
                );
            })
            .await;
    }

    fn authorities(addresses: &[&str]) -> Vec<Authority> {
        addresses
            .iter()
            .map(|address| authority(&address.parse().unwrap()).unwrap())
            .collect()
    }

    /// A config with nothing but the named listeners.
    fn config_with(listeners: &[&str]) -> Compiled {
        let mut yaml = String::from("routes: []\nupstreams: {}\nlisteners:\n");
        for (at, name) in listeners.iter().enumerate() {
            yaml += &format!(
                "  {name}: {{ address: \"127.0.0.1:{at}\", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
            );
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    proptest::proptest! {
        /// An endpoint is picked from those that serve while at least half do, and from
        /// all of them when fewer do; never from nowhere.
        #[test]
        fn a_pick_follows_health_down_to_the_panic_threshold(
            health in proptest::collection::vec(proptest::bool::ANY, 0..12),
            random in proptest::num::u64::ANY,
        ) {
            let picked = pick_healthy(health.len(), random, |at| health[at]);
            let serving = health.iter().filter(|healthy| **healthy).count();
            match picked {
                None => proptest::prop_assert!(health.is_empty()),
                Some(at) => {
                    proptest::prop_assert!(at < health.len());
                    if serving * 2 >= health.len() {
                        proptest::prop_assert!(health[at], "an unhealthy pick above the threshold");
                    }
                }
            }
        }
    }

    /// Every endpoint that serves can be picked, whichever the first draw lands on.
    #[test]
    fn every_serving_endpoint_can_be_picked() {
        let health = [false, true, false, true, true];
        let mut seen = std::collections::BTreeSet::new();
        for random in 0..100 {
            seen.insert(pick_healthy(health.len(), random, |at| health[at]).unwrap());
        }
        assert_eq!(seen, std::collections::BTreeSet::from([1, 3, 4]));
    }

    #[test]
    fn a_snapshot_knows_where_it_has_the_listeners_that_have_sockets() {
        let sockets = ["admin".to_owned(), "web".to_owned()];
        let metrics = Metrics::new(NonZeroUsize::MIN, sockets.len());
        let listeners = |names: &[&str]| {
            Snapshot::new(
                config_with(names),
                &sockets,
                &metrics,
                None,
                &Keys::default(),
            )
            .unwrap()
            .listeners
        };

        assert_eq!(listeners(&["admin", "web"]), [Some(0), Some(1)]);
        // Listeners are held in the order of their names, so one more moves the others.
        assert_eq!(
            listeners(&["aaa", "admin", "metrics", "web"]),
            [Some(1), Some(3)]
        );
        assert_eq!(listeners(&["web"]), [None, Some(0)]);
        assert_eq!(listeners(&["other"]), [None, None]);
    }

    #[test]
    fn a_data_plane_serves_the_listeners_it_was_made_with() {
        let proxy = Proxy::new(config_with(&["web", "admin"]), NonZeroUsize::MIN).unwrap();
        assert_eq!(proxy.listeners(), ["admin", "web"]);
        proxy.reload(config_with(&["later"])).unwrap();
        assert_eq!(proxy.listeners(), ["admin", "web"]);
    }

    #[test]
    fn an_endpoint_is_written_as_a_target_would_have_it() {
        assert_eq!(authorities(&["127.0.0.1:80"]), ["127.0.0.1:80"]);
        assert_eq!(authorities(&["[2001:db8::7]:8080"]), ["[2001:db8::7]:8080"]);
    }

    #[test]
    fn every_endpoint_gets_its_turn() {
        let picked: Vec<usize> = (0..4).map(|random| pick_at(3, random).unwrap()).collect();
        assert_eq!(picked, [0, 1, 2, 0]);
        assert_eq!(pick_at(3, u64::MAX).unwrap(), 0);
    }

    #[test]
    fn no_endpoints_is_nowhere_to_connect() {
        assert_eq!(pick_at(0, 7), None);
    }

    #[test]
    fn the_target_keeps_its_path_and_query_at_the_endpoint() {
        let endpoint = &authorities(&["127.0.0.1:9002"])[0];
        for target in [
            "/cart/items?page=3",
            "http://shop.example.com/cart/items?page=3",
        ] {
            let target: Uri = target.parse().unwrap();
            assert_eq!(
                at_endpoint(&target, endpoint).unwrap(),
                "http://127.0.0.1:9002/cart/items?page=3"
            );
        }
    }

    #[test]
    fn an_answer_of_our_own_is_a_status_and_nothing_else_and_is_counted() {
        let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN).unwrap();
        let answer = proxy.answer(0, Answer::NoRoute);
        assert_eq!(answer.status(), 404);
        assert!(answer.headers().is_empty());
        let counted =
            "edgerush_listener_local_answers_total{listener=\"web\",reason=\"no_route\"} 1
";
        assert!(proxy.metrics().contains(counted));
    }

    #[test]
    fn a_reload_is_counted_and_resets_nothing() {
        let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN).unwrap();
        let _counted = proxy.answer(0, Answer::NoRoute);
        assert!(proxy.metrics().contains(
            "edgerush_config_reloads_total 0
"
        ));
        assert!(proxy.metrics().contains(
            "edgerush_config_last_reload_timestamp_seconds 0
"
        ));

        proxy.reload(config_with(&["web"])).unwrap();
        let scrape = proxy.metrics();
        assert!(
            scrape.contains(
                "edgerush_config_reloads_total 1
"
            ),
            "{scrape}"
        );
        assert!(
            !scrape.contains(
                "edgerush_config_last_reload_timestamp_seconds 0
"
            ),
            "{scrape}"
        );
        assert!(
            scrape.contains(
                "reason=\"no_route\"} 1
"
            ),
            "{scrape}"
        );
    }

    /// A backend over TLS as `certificate` says that speaks WebSocket as far as the
    /// handshake goes: a 101 with the Accept of the key it was sent and `hello` after it,
    /// then everything it reads sent back until its peer closes.
    async fn tls_websocket_backend(certificate: &edgerush_config::Certificate) -> SocketAddr {
        use boring::pkey::PKey;
        use boring::ssl::{SslAcceptor, SslMethod};
        use boring::x509::X509;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        builder
            .set_certificate(&X509::from_pem(certificate.chain.as_bytes()).unwrap())
            .unwrap();
        builder
            .set_private_key(&PKey::private_key_from_pem(certificate.key.as_bytes()).unwrap())
            .unwrap();
        let acceptor = Rc::new(builder.build());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((stream, _)) = socket.accept().await {
                let acceptor = Rc::clone(&acceptor);
                let _serving = tokio::task::spawn_local(async move {
                    let Ok(mut secured) = tokio_boring::accept(&acceptor, stream).await else {
                        return;
                    };
                    let mut head = Vec::new();
                    let mut byte = [0; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match secured.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8(head).unwrap();
                    let key = head
                        .split("\r\n")
                        .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                        .and_then(|key| Key::read(key.as_bytes()))
                        .unwrap();
                    let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                    let switched = format!(
                        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\nhello"
                    );
                    if secured.write_all(switched.as_bytes()).await.is_err() {
                        return;
                    }
                    let mut bytes = [0; 1024];
                    loop {
                        match secured.read(&mut bytes).await {
                            Ok(0) | Err(_) => break,
                            Ok(read) => {
                                if secured.write_all(&bytes[..read]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    let _ = secured.write_all(b"bye").await;
                    let _ = secured.shutdown().await;
                });
            }
        });
        address
    }

    /// A WebSocket is carried as it is in plaintext when both of its connections are TLS
    /// ones: the client's to an `https` listener and the gateway's to a backend it verifies
    /// (19 §5).
    #[tokio::test]
    async fn a_websocket_is_carried_over_tls_on_both_sides() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                use crate::tls::testing::certificate;
                let server = certificate(&["backend.test"]);
                let upstream = tls_websocket_backend(&server).await;
                let mut config = everything_config(upstream);
                config.upstreams.get_mut("up").unwrap().tls =
                    Some(trusting("backend.test", &server));
                let web = config.listeners.get_mut("web").unwrap();
                web.protocol = edgerush_config::Protocol::Https;
                web.tls = Some(edgerush_config::Tls {
                    certificates: vec![certificate(&["a.test"])],
                    client_validation: None,
                });
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                let mut client = tls_client(front, "a.test", Some(b"\x08http/1.1"), |_| {})
                    .await
                    .unwrap();
                client
                    .write_all(
                        b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                          connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                          sec-websocket-version: 13\r\n\r\nearly",
                    )
                    .await
                    .unwrap();
                let mut said = Vec::new();
                let mut bytes = [0; 1024];
                while !said.ends_with(b"helloearly") {
                    let read = within(client.read(&mut bytes)).await.unwrap();
                    assert_ne!(read, 0, "{}", String::from_utf8_lossy(&said));
                    said.extend_from_slice(&bytes[..read]);
                }
                let said = String::from_utf8(said).unwrap();
                assert!(
                    said.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
                    "{said}"
                );
                assert!(
                    said.contains("\r\nsec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"),
                    "{said}"
                );
                client.write_all(b"ping").await.unwrap();
                let mut echoed = [0; 4];
                within(client.read_exact(&mut echoed)).await.unwrap();
                assert_eq!(&echoed, b"ping");
                client.shutdown().await.unwrap();
                let mut rest = Vec::new();
                let _ended = within(client.read_to_end(&mut rest)).await;
                assert_eq!(rest, b"bye");
                tunnel_ended(&worker, "web", "closed").await;
            })
            .await;
    }

    /// A plaintext backend that speaks WebSocket as far as the handshake and the close go:
    /// a 101 with the Accept of the key it was sent, then it reads until a Close frame
    /// comes, answers it with a Close of its own, and says everything it read once the
    /// gateway has closed its connection.
    async fn closing_websocket_backend(
        saw: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    ) -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let _accepting = tokio::task::spawn_local(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let saw = saw.clone();
                let _serving = tokio::task::spawn_local(async move {
                    let mut head = Vec::new();
                    let mut byte = [0; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8(head).unwrap();
                    let key = head
                        .split("\r\n")
                        .find_map(|line| line.strip_prefix("sec-websocket-key: "))
                        .and_then(|key| Key::read(key.as_bytes()))
                        .unwrap();
                    let accept = String::from_utf8(key.accept().to_vec()).unwrap();
                    let switched = format!(
                        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                         connection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
                    );
                    stream.write_all(switched.as_bytes()).await.unwrap();
                    let mut read = Vec::new();
                    let mut bytes = [0; 1024];
                    let mut answered = false;
                    loop {
                        match stream.read(&mut bytes).await {
                            Ok(0) | Err(_) => break,
                            Ok(count) => read.extend_from_slice(&bytes[..count]),
                        }
                        if !answered && read.first() == Some(&0x88) {
                            answered = true;
                            // 1000, "Normal Closure", as a server answers a Close.
                            let _ = stream.write_all(&[0x88, 0x02, 0x03, 0xe8]).await;
                        }
                    }
                    let _ = saw.send(read);
                });
            }
        });
        address
    }

    /// A draining worker closes an open WebSocket with a Close 1001 each way, takes both
    /// answers, and counts it drained (19 §6). With the short deadlines' bound of under
    /// five seconds, its moment is at once.
    #[tokio::test]
    async fn a_draining_worker_closes_websockets_with_going_away() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (saw, mut seen) = tokio::sync::mpsc::unbounded_channel();
                let upstream = closing_websocket_backend(saw).await;
                let proxy = Proxy::new(
                    compile(&everything_config(upstream)).unwrap(),
                    NonZeroUsize::MIN,
                )
                .unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                let mut client = TcpStream::connect(front).await.unwrap();
                client
                    .write_all(
                        b"GET /chat HTTP/1.1\r\nhost: a.test\r\nupgrade: websocket\r\n\
                          connection: upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                          sec-websocket-version: 13\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut head = Vec::new();
                let mut byte = [0; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    within(client.read_exact(&mut byte)).await.unwrap();
                    head.push(byte[0]);
                }
                assert!(head.starts_with(b"HTTP/1.1 101 "));

                worker.drain();
                let mut close = [0; 4];
                within(client.read_exact(&mut close)).await.unwrap();
                assert_eq!(close, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
                // The client answers, masked, as a client must.
                client
                    .write_all(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9])
                    .await
                    .unwrap();
                let mut rest = Vec::new();
                let _closed = within(client.read_to_end(&mut rest)).await;
                assert_eq!(rest, b"", "the backend's answer went on");
                let heard = within(seen.recv()).await.unwrap();
                assert_eq!(heard.len(), 8, "{heard:?}");
                assert_eq!(&heard[..2], &[0x88, 0x82], "{heard:?}");
                let code = [heard[6] ^ heard[2], heard[7] ^ heard[3]];
                assert_eq!(u16::from_be_bytes(code), 1001);
                tunnel_ended(&worker, "web", "drained").await;
            })
            .await;
    }

    /// The same over HTTP/2: the Close frames go inside the stream's DATA frames, the
    /// connection's graceful GOAWAY lets the stream finish, and the tunnel ends drained
    /// once both answers are in (19 §6).
    #[tokio::test]
    async fn a_draining_worker_closes_http2_websockets_with_going_away() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (saw, mut seen) = tokio::sync::mpsc::unbounded_channel();
                let upstream = closing_websocket_backend(saw).await;
                let proxy = Proxy::new(
                    compile(&everything_config(upstream)).unwrap(),
                    NonZeroUsize::MIN,
                )
                .unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = serving(&worker, socket);

                let stream = TcpStream::connect(front).await.unwrap();
                let (send, connection) = ::h2::client::handshake(stream).await.unwrap();
                let _driving = tokio::task::spawn_local(async move {
                    let _ended = connection.await;
                });
                let mut send = within(send.ready()).await.unwrap();
                until(|| send.is_extended_connect_protocol_enabled()).await;
                let mut request = Request::builder()
                    .method(Method::CONNECT)
                    .uri("http://a.test/chat")
                    .header("sec-websocket-version", "13")
                    .body(())
                    .unwrap();
                request
                    .extensions_mut()
                    .insert(::h2::ext::Protocol::from_static("websocket"));
                let (response, mut stream) = send.send_request(request, false).unwrap();
                let response = within(response).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let mut body = response.into_body();

                worker.drain();
                let mut heard = Vec::new();
                while heard.len() < 4 {
                    let data = within(body.data()).await.unwrap().unwrap();
                    let _released = body.flow_control().release_capacity(data.len());
                    heard.extend_from_slice(&data);
                }
                assert_eq!(heard, [0x88, 0x02, 0x03, 0xe9], "not a Close 1001");
                stream
                    .send_data(
                        Bytes::from_static(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe9]),
                        false,
                    )
                    .unwrap();
                // Nothing more comes, and the stream ends.
                let mut rest = Vec::new();
                while let Some(data) = within(body.data()).await {
                    match data {
                        Ok(data) => rest.extend_from_slice(&data),
                        Err(_) => break,
                    }
                }
                assert_eq!(rest, b"", "the backend's answer went on");
                let backend_heard = within(seen.recv()).await.unwrap();
                assert_eq!(&backend_heard[..2], &[0x88, 0x82], "{backend_heard:?}");
                tunnel_ended(&worker, "web", "drained").await;
            })
            .await;
    }
}
