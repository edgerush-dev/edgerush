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

mod connection;
mod exchange;
mod passthrough;
mod plane;
mod respond;
mod tries;
mod worker;

use crate::balance::Tried;
use crate::connections::{Held, Loads};
use crate::downstream::h1::connection::{self as h1, Answered};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h1::deadlines::Bounds;
use crate::downstream::h2;
use crate::downstream::h3::listener as h3_listener;
pub use crate::downstream::h3::listener::Forwarding;
use crate::drain::Drain;
use crate::forwarding::Client;
use crate::gathered::Gathered;
use crate::grpc::answer::{Answered as GrpcAnswered, Count};
use crate::grpc::call::Call;
use crate::interim::Interim;
use crate::linger::Lent;
use crate::metrics::{Answer, Metrics, Stopped};
use crate::places::{Place, Places};
use crate::raw::RawHead;
use crate::received::Received;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::retry::budget::Budget;
use crate::routed::Routed;
use crate::slots::{Slots, WorkerSlots};
use crate::timers::{Alarm, Timers};
use crate::tls::{self, Tls, TlsError};
use crate::upstream::balancing::{self, Balancing, InFlight};
use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::Blocks;
use crate::upstream::h1::exchange::{ExchangeError, H1Body};
use crate::upstream::h1::pool::Pool;
use crate::upstream::h2::client::{Client as H2Client, Settings as H2Settings};
use crate::upstream::h2::exchange as h2_exchange;
use crate::upstream::h2::pool::Limits as H2Limits;
use crate::upstream::secure::{Secure, Socket as UpstreamSocket};
use crate::websocket::Key;
use arc_swap::ArcSwap;
use bytes::Bytes;
use edgerush_config::{Compiled, CompiledListener, CompiledRetry, CompiledRule, Timeout};
use edgerush_filters::HeaderModifier;
use http::uri::{Authority, Scheme};
use http::{HeaderMap, Request, StatusCode, Uri};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
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
        Ok(socket) => socket.map_err(ExchangeError::Unconnected),
        Err(_) => Err(ExchangeError::Unconnected(io::ErrorKind::TimedOut.into())),
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
        ExchangeError::Unconnected(_) | ExchangeError::Io(_) => Stopped::Io,
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
    /// The places for exchanges this worker has, and which upstreams hold them
    /// ([03 §9](../../docs/03-data-plane.md)). Its own, like everything else here: no worker
    /// waits on another to find out whether it may take a request.
    places: Rc<Places>,
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
    /// What it has in flight to each endpoint, and whose turn it is (03 §6).
    balancing: RefCell<Balancing>,
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
    /// Every worker's connections, counted, where the process counts them: a WebSocket it
    /// carries for an HTTP/2 or HTTP/3 client counts among them as long as it is open
    /// ([03 §9](../../docs/03-data-plane.md)). None for a worker that is not one of a
    /// process's, which counts nothing.
    connections: Option<Arc<Loads>>,
    /// Its HTTP/2 connections, server's and client's, each charged what h2 holds of what
    /// its peer sent; the one charged most is closed when the storage runs out (15 §3).
    received: Rc<Received>,
    /// Its HTTP/3 connections still in their handshake, on all its HTTP/3 listeners: past
    /// a threshold of them a client proves its address first (16 §6).
    handshakes: Rc<Cell<usize>>,
    /// By position in [`Proxy::listeners`]: the client validation its connections here are
    /// accepted under, and the drain they hear.
    validations: RefCell<Vec<Validation>>,
    /// Which snapshot `validations` and `routes` were last brought up to.
    validated: Cell<u64>,
    /// By key ([`Routed`]): the drain of the tunnels routed by a listener, route and
    /// upstream, which a reload that takes any of the three away starts (03 §10).
    routes: RefCell<HashMap<u64, Rc<Drain>>>,
}

/// The client validation a listener's connections were accepted under on a worker, and the
/// drain they hear: the worker's drain starts it, and so does a reload that replaces the
/// validation, which drains the connections accepted under the one before (03 §3, §10). A
/// certificate rotation keeps it.
#[derive(Debug)]
struct Validation {
    /// The listener's TLS when they were accepted, for its front, which a reload keeps
    /// exactly while the listener validates clients alike; none for a listener without TLS.
    tls: Option<Arc<Tls>>,
    drain: Rc<Drain>,
}

impl Validation {
    /// Whether a connection accepted with `tls` is accepted under this validation.
    fn holds_for(&self, tls: Option<&Arc<Tls>>) -> bool {
        match (&self.tls, tls) {
            (None, None) => true,
            (Some(kept), Some(tls)) => kept.same_front(tls),
            _ => false,
        }
    }
}

/// Serves `socket`, a connection of `ours` that speaks HTTP/1, with our own server (14).
/// `asking` is set when the first request is handed over.
async fn serve_h1<S>(ours: Rc<Connection>, client: Rc<Client>, asking: Rc<Cell<bool>>, socket: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connection = Rc::clone(&ours);
    let worker = &connection.worker;
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
        &connection.drain,
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
    let drain = Rc::clone(&ours.drain);
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
    h2::connection::serve(
        socket,
        settings,
        storage,
        &worker.received,
        date,
        drain,
        respond,
    )
    .await;
}

/// One exchange's place among those a worker has in hand, given back when it is dropped.
///
/// Taken before a connection is looked for and held until the answer's body is let go of,
/// so that what is counted is work in hand rather than requests begun. Dropping is the
/// only way to give it back, which is what makes every way out of an exchange — answered,
/// failed, or a client that stopped reading — release it without being told to
/// ([13 §7](../../docs/13-http1-upstream.md)).
#[derive(Debug)]
struct Admitted {
    /// The place itself, which goes back to the worker, and to its upstream's count, when
    /// this is dropped.
    _place: Place,
    /// The exchange's count at its endpoint, which goes where the place goes: with the
    /// answer's body to its end (03 §6).
    counted: Option<InFlight>,
}

impl Admitted {
    /// The place, holding the exchange's count at the endpoint it was sent to as well.
    fn counting(mut self, counted: Option<InFlight>) -> Self {
        self.counted = counted;
        self
    }

    /// The count, for a tunnel to hold once the place is let go of.
    fn count(&mut self) -> Option<InFlight> {
        self.counted.take()
    }
}

/// A compiled config and what the data plane works out from it, once, when it arrives.
#[derive(Debug)]
struct Snapshot {
    config: Compiled,
    /// Which config this is, counting from the first: what tells a worker its balancing
    /// state is for an earlier one.
    generation: u64,
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
    /// What its tunnels are routed by, keyed so that a worker sees at a reload which went
    /// (03 §10). Worked out against the config this one replaces, as `destinations` are.
    routed: Routed,
}

impl Snapshot {
    /// The compiled listener whose socket is at `position`, if the config still has it.
    fn listener(&self, position: usize) -> Option<&CompiledListener> {
        let at = self.listeners.get(position).copied().flatten()?;
        self.config.listeners().get(at)
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
    /// Its count at that endpoint, until the place its exchange is given takes it.
    counted: Option<InFlight>,
    /// Every endpoint of the upstream, for a request its rule may send again. Boxed: only
    /// such a request has it.
    others: Option<Box<Others>>,
    /// Where the copies of it go, for a request its rule mirrors.
    mirrors: Vec<Mirrored>,
    /// For a WebSocket handshake carried to an HTTP/1.1 backend (19 §2). Boxed: rare, and
    /// every request's future holds this across its waits.
    websocket: Option<Box<Handshake>>,
    /// Whether its client spoke HTTP/1.1, which alone has `Upgrade`: a 426 that offers
    /// WebSocket says so to it, and to no other (19 §2).
    upgradable: bool,
    /// Whether the snapshot has no upstream but this one, which may then take every place
    /// a worker has ([03 §9](../../docs/03-data-plane.md)). Beside the other flag, where
    /// it costs a request's future nothing.
    alone: bool,
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
    /// The channel of the server that read it, where a switched backend is left for it to
    /// carry. Held here and not taken as a try's informational channel is, which only the
    /// first try has: any try may be the one that switches.
    server: Option<Interim>,
    /// For an extended CONNECT, its count among the worker's connections, taken before
    /// anything goes upstream and handed to the tunnel by the try that switches; let go of
    /// with the handshake if none does (03 §9).
    held: Cell<Option<Held>>,
    /// The key of the listener, route and upstream it was routed by, whose drain its tunnel
    /// hears (03 §10).
    route: Option<u64>,
}

impl Handshake {
    /// Whether it came as an extended CONNECT, on a stream of an HTTP/2 or HTTP/3 client's
    /// connection, which may carry many: the ones with no key of the client's.
    fn on_a_stream(&self) -> bool {
        self.client.is_none()
    }
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

/// Where else a request its rule may send again can go: every endpoint of its upstream, of
/// the snapshot it was directed on, and the worker's balancing of them (03 §6).
struct Others {
    /// By position of the endpoint: where to connect, in the form a target takes.
    authorities: Vec<Authority>,
    /// By position of the endpoint.
    destinations: Vec<Arc<ReuseIdentity>>,
    balance: Rc<balancing::Upstream>,
    /// Where the first try went.
    first: usize,
}

/// Where one copy of a request goes: drawn with the request, from the same snapshot.
struct Mirrored {
    upstream_slot: usize,
    endpoint: Arc<ReuseIdentity>,
    /// Its count at that endpoint, until the place its exchange is given takes it: a copy
    /// is load on the mirror's backend like any exchange (03 §6).
    counted: Option<InFlight>,
    target: Uri,
    /// The fields as they stood at the mirror's place, before a change after it; none for
    /// a copy of the request as it goes upstream.
    fields: Option<HeaderMap>,
}

impl Directed {
    /// The endpoint for another try, which keeps away from those in `tried`, as the
    /// upstream's balancer picks it: the target at it, its destination, its position and its
    /// count.
    fn draw(
        &self,
        target: &Uri,
        tried: &Tried,
    ) -> Option<(Uri, Arc<ReuseIdentity>, usize, InFlight)> {
        let others = self.others.as_deref()?;
        let (at, counted) = others.balance.pick(&others.destinations, tried)?;
        let authority = others.authorities.get(at)?;
        let destination = others.destinations.get(at)?;
        Some((
            at_endpoint(target, authority)?,
            Arc::clone(destination),
            at,
            counted,
        ))
    }
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
        // Nothing of the request reached the endpoint: any retry at all sends it on
        // (GEP-1731).
        Err(Answer::Unreachable) => true,
        Err(Answer::UpstreamFailed) => retry.on_status(502),
        Err(Answer::UpstreamTimedOut) => retry.on_timeout,
        Err(_) => false,
    }
}

/// A connection's two ends as anyone is told of them: whoever connected and where, or what a
/// sender's PROXY header says of the client it came for, both from the one place, never one
/// end of each ([20 §3](../../docs/20-proxy-protocol.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ends {
    /// The client.
    client: SocketAddr,
    /// Where the client connected.
    local: SocketAddr,
}

/// A connection that came in on a listener's socket: what its requests share, and what
/// counts it as open until the last of them is done, wherever that happens.
struct Connection {
    worker: Rc<Worker>,
    listener: usize,
    /// What it drains with, and a WebSocket it carries too: the worker's drain, and a reload
    /// that replaces the client validation it was accepted under (03 §3).
    drain: Rc<Drain>,
}

impl Connection {
    fn open(worker: Rc<Worker>, listener: usize, drain: Rc<Drain>) -> Self {
        if let Some(counters) = worker.proxy.metrics.listener(listener) {
            counters.accepted.inc();
            counters.active.inc();
        }
        Self {
            worker,
            listener,
            drain,
        }
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

/// The worker's balancing state for the upstream at `upstream` of `snapshot`, made for it
/// first if it was made for an earlier config.
fn balance_of(
    balancing: &RefCell<Balancing>,
    snapshot: &Snapshot,
    upstream: usize,
) -> Option<Rc<balancing::Upstream>> {
    let mut balancing = balancing.borrow_mut();
    balancing.refresh(
        snapshot.generation,
        &snapshot.config,
        &snapshot.destinations,
    );
    balancing.upstream(upstream).cloned()
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
mod tests;
