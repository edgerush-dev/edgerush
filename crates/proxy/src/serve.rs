//! Serving: connections come in, requests go through the request core, and what it
//! forwards goes to an endpoint of the chosen upstream and comes back. This is the adapter
//! between the HTTP engine (hyper) and the core, and the only place that knows both.
//!
//! Bodies stream in both directions and are never held here. Upstream connections are
//! HTTP/1.1, by EdgeRush's own client and pool unless the engine's is asked for
//! ([`Upstream`]).
//!
//! The config is published whole and at once ([`Proxy::reload`]): a request reads the
//! current snapshot without waiting for anybody, works with that one snapshot until it has
//! been directed, and from then on holds on to its rule at most. Sockets and upstream
//! connections belong to the data plane, not to a snapshot, and outlive every reload.
//!
//! A connection is served on the worker that took it, and stays there: everything it
//! spawns goes into that worker's `LocalSet` ([`OnThisWorker`]), so nothing a request
//! touches need be `Send`. That is what lets a worker own things a thread cannot share —
//! the pool of upstream connections to come, above all.

use crate::hop_by_hop::strip_response;
use crate::linger::{self, Lent, linger};
use crate::metrics::{Answer, Metrics, Socket, Stopped};
use crate::random::random;
use crate::request::decide;
use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, SMALL, Sizes};
use crate::upstream::h1::codec::{ResponseHead, Sending, filter_declaration, filter_trailers};
use crate::upstream::h1::exchange::{Exchange, ExchangeError, H1Body, nothing_to_say};
use crate::upstream::h1::pool::{Lease, Pool};
use arc_swap::ArcSwap;
use edgerush_config::{Compiled, CompiledRule};
use http::request::Parts;
use http::response;
use http::uri::{Authority, Scheme};
use http::{HeaderMap, HeaderName, Method, Request, Response, Uri, Version};
use hyper::body::{Body as HttpBody, Bytes, Frame, Incoming, SizeHint};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
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

/// The deadlines a worker holds its client connections to: [`FIRST_REQUEST`] and
/// [`NEXT_REQUEST`], short in tests so that they can run on real sockets and real time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadlines {
    first_request: Duration,
    next_request: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            first_request: FIRST_REQUEST,
            next_request: NEXT_REQUEST,
        }
    }
}

/// Where the futures the engine spawns of its own accord go: the worker that is serving
/// the connection they belong to, never a thread pool. A worker is a single-threaded
/// runtime and a `LocalSet`, so a request and everything it holds stay on one core and
/// need not be `Send`.
#[derive(Debug, Clone, Copy, Default)]
struct OnThisWorker;

impl<F: Future<Output = ()> + 'static> Executor<F> for OnThisWorker {
    fn execute(&self, future: F) {
        // The engine only spawns while serving a connection, and a connection is only
        // ever served inside a worker's LocalSet, which is the one this goes into.
        let _detached = tokio::task::spawn_local(future);
    }
}

/// What is answered: the upstream's body as it arrives, or nothing at all.
///
/// Named, and not a box or a trait object, because every request would pay for that and
/// because the engine can see through this to what is really left to send — a body whose
/// length is known keeps it, and an answer of ours is end-of-stream from the start.
/// A second kind, read by EdgeRush's own upstream path, goes beside [`Self::Upstream`]
/// when there is one ([13 §1](../../../docs/13-http1-upstream.md)).
enum Body {
    /// The upstream's answer, as the engine's client reads it, and what that answer's
    /// own `Connection` named. The names are kept because the trailers have not arrived
    /// yet and the head they were read from will be gone by the time they do.
    Upstream(Incoming, Vec<HeaderName>, Watch),
    /// The upstream's answer, as EdgeRush's own path reads it. In a box because it is
    /// much the larger of the two, and every answer would otherwise carry room for it.
    /// It carries its exchange's place with it: the exchange is over when this is.
    Ours(Box<H1Body<TcpStream, Incoming>>, Admitted, Watch),
    /// An answer of the data plane's own. It has no body, and never will have one.
    Empty,
}

/// A frame on its way to the client, with what may not travel on taken out of it.
///
/// Only a trailer section is touched, and only by name: a field the answer's own
/// `Connection` named is hop-by-hop for that hop, and forwarding it is a thing an
/// intermediary may not do whichever client read it
/// ([13 §4](../../docs/13-http1-upstream.md)).
fn filtered(frame: Frame<Bytes>, nominated: &[HeaderName]) -> Frame<Bytes> {
    match frame.into_trailers() {
        Ok(mut fields) => {
            let _discarded = filter_trailers(&mut fields, nominated);
            Frame::trailers(fields)
        }
        Err(frame) => frame,
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
    Upstream(#[from] hyper::Error),
    #[error("the upstream's answer could not be read: {0}")]
    Ours(#[from] ExchangeError),
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyError>>> {
        match self.get_mut() {
            Self::Upstream(incoming, nominated, watch) => {
                Pin::new(incoming).poll_frame(context).map(|frame| {
                    frame.map(|frame| {
                        frame
                            .map(|frame| filtered(frame, nominated))
                            .map_err(|error| {
                                watch.body_failed();
                                BodyError::Upstream(error)
                            })
                    })
                })
            }
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
            Self::Empty => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Upstream(incoming, ..) => incoming.is_end_stream(),
            Self::Ours(ours, ..) => ours.is_end_stream(),
            Self::Empty => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Upstream(incoming, ..) => incoming.size_hint(),
            Self::Ours(ours, ..) => ours.size_hint(),
            Self::Empty => SizeHint::with_exact(0),
        }
    }
}

/// Which way a request reaches its upstream.
///
/// One choice for the process, made on the command line and never changed while it runs
/// — and never changed part way through a request. A path that failed is not a reason to
/// try the other: by then the request may already have reached the upstream, and sending
/// it again would be sending it twice ([13 §1](../../docs/13-http1-upstream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Upstream {
    /// The engine's client, kept to compare EdgeRush's own against.
    Hyper,
    /// EdgeRush's own, measured against the engine's and chosen over it
    /// ([13 §8](../../docs/13-http1-upstream.md)).
    #[default]
    Ours,
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
    /// Which way its workers reach an upstream.
    upstream: Upstream,
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
    /// One for the life of the worker: a reload does not throw warm connections away.
    /// Those to an endpoint that is no longer used grow idle and are closed.
    client: Client<HttpConnector, Incoming>,
    /// The connections this worker keeps by EdgeRush's own path.
    pool: Rc<RefCell<Pool<TcpStream>>>,
    /// What its exchanges read into, lent and taken back rather than made each time
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    blocks: Rc<RefCell<Blocks>>,
    /// How many exchanges this worker has in hand. Its own, like everything else here:
    /// no worker waits on another to find out whether it may take a request.
    in_flight: Rc<Cell<usize>>,
    limits: H1Limits,
    deadlines: Deadlines,
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
}

impl Snapshot {
    fn new(
        config: Compiled,
        listeners: &[String],
        metrics: &Metrics,
        previous: &Destinations,
        keys: &Keys,
    ) -> Result<Self, ProxyError> {
        let endpoints = config
            .upstreams
            .iter()
            .map(|upstream| upstream.endpoints.iter().map(authority).collect())
            .collect::<Result<_, _>>()?;
        let listeners = listeners
            .iter()
            .map(|name| config.listeners.iter().position(|l| l.name == *name))
            .collect();
        let upstream_slots = config
            .upstreams
            .iter()
            .map(|upstream| metrics.upstream_slot(&upstream.name))
            .collect();
        let destinations = Destinations::reconcile(&config, previous, keys);
        Ok(Self {
            config,
            listeners,
            endpoints,
            upstream_slots,
            destinations,
        })
    }
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
    pub fn new(
        config: Compiled,
        workers: NonZeroUsize,
        upstream: Upstream,
    ) -> Result<Self, ProxyError> {
        let listeners: Vec<String> = config
            .listeners
            .iter()
            .map(|listener| listener.name.clone())
            .collect();
        // A shard for every worker, so that no two write to one line of cache.
        let metrics = Metrics::new(workers, listeners.len());
        let keys = Keys::default();
        let nothing_yet = Destinations::default();
        let snapshot = Snapshot::new(config, &listeners, &metrics, &nothing_yet, &keys)?;
        Ok(Self {
            listeners,
            current: ArcSwap::from_pointee(snapshot),
            metrics,
            keys,
            upstream,
        })
    }

    /// The names of the listeners that can be served: those of the config the data plane
    /// was made with, in its order.
    #[must_use]
    pub fn listeners(&self) -> &[String] {
        &self.listeners
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
            &previous.destinations,
            &self.keys,
        )?;
        drop(previous);
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
        self.metrics.render(&self.listeners, &upstreams)
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

    /// The same, holding client connections to `deadlines`.
    fn with_deadlines(proxy: Arc<Proxy>, limits: H1Limits, deadlines: Deadlines) -> Rc<Self> {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            // How many idle connections a worker keeps to one destination is the data
            // plane's bound and not one client's, so the engine's pool is held to it too
            // ([13 §7](../../docs/13-http1-upstream.md)). Left alone it keeps as many as
            // it likes, which is a different proxy from the one that document describes.
            .pool_max_idle_per_host(limits.idle_per_destination)
            .build(connector);
        Rc::new(Self {
            proxy,
            client,
            pool: Rc::new(RefCell::new(Pool::default())),
            blocks: Rc::new(RefCell::new(Blocks::new(Sizes::within(&limits, SMALL)))),
            in_flight: Rc::new(Cell::new(0)),
            limits,
            deadlines,
        })
    }

    /// Looks over the connections this worker is keeping, for as long as it runs.
    ///
    /// One sweep for the worker rather than a timer for every connection, and it is what
    /// clears out a destination that a reload took away and that nothing will ask for
    /// again ([13 §3](../../docs/13-http1-upstream.md)). Spawned into the worker's
    /// `LocalSet` beside its listeners; a worker without it keeps what it should drop.
    pub async fn maintain(self: Rc<Self>) {
        let every = self.limits.sweep;
        loop {
            tokio::time::sleep(every).await;
            // Borrowed for the sweep and let go of before anything is waited on again.
            let swept = self.pool.borrow_mut().sweep(&self.limits);
            // And what a burst left parked of the blocks, down to what a quiet worker keeps.
            self.blocks.borrow_mut().sweep();
            let metrics = &self.proxy.metrics;
            for _discarded in 0..swept {
                metrics.socket(Socket::Discarded);
            }
            // What this worker holds at the moment it last looked. A sweep already walks
            // everything these ask about, so nothing is counted on the request path for
            // them ([13 §7](../../docs/13-http1-upstream.md)).
            metrics
                .worker()
                .holding(self.in_flight.get(), self.idle_connections());
        }
    }

    /// How many connections this worker is keeping. For tests and, later, a gauge.
    #[must_use]
    pub fn idle_connections(&self) -> usize {
        self.pool.borrow().idle()
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
    async fn through_h1<B>(
        &self,
        identity: &Arc<ReuseIdentity>,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        nominated: &[HeaderName],
        sending: Sending,
        body: B,
    ) -> Result<(ResponseHead, H1Body<TcpStream, B>), ExchangeError>
    where
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
                let opening = TcpStream::connect(identity.address());
                let socket = connect_within(self.limits.connect, opening).await?;
                // Worth having, not worth refusing an upstream over.
                let _unset = socket.set_nodelay(true);
                (socket, Instant::now())
            }
        };

        let exchange = Exchange::new(socket, Rc::clone(&self.blocks));
        let (answer, rest) = exchange
            .send(method, uri, headers, nominated, sending, body, &self.limits)
            .await?;

        // The request may still be going out; what is left of it goes with the body,
        // which drives it while the client reads the answer.
        let lease = Lease::in_use(Arc::clone(identity), opened, &self.pool);
        let mut head = answer.head;
        // The same for the answer: a name its `Connection` gave does not travel on, and
        // is not declared onwards either.
        filter_declaration(&mut head.headers, &answer.nominated);
        let persistent = answer.delivery.persistent
            && !crate::upstream::auth::challenges(head.status, &head.headers);
        let body = H1Body::new(
            rest,
            answer.delivery.framing,
            persistent,
            answer.nominated,
            self.limits,
        )
        .returning_to(lease);
        Ok((head, body))
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
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where the connections it accepts are served;
    /// without one there is nowhere to put them and the first connection panics.
    pub async fn serve(self: Rc<Self>, listener: usize, socket: TcpListener) {
        loop {
            match socket.accept().await {
                Ok((stream, _)) => {
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
        let deadlines = self.deadlines;
        let connection = Rc::new(Connection::open(self, listener));
        // Set when the engine hands over the first request, which is the end of the one
        // stretch its own deadlines do not cover.
        let asked = Rc::new(Cell::new(false));
        let asking = Rc::clone(&asked);
        // Every request clones a handle, as the engine wants futures that own what they
        // use. A handle of the connection's own keeps that count off a line of cache that
        // all the workers would otherwise write to.
        let service = service_fn(move |request| {
            asking.set(true);
            let connection = Rc::clone(&connection);
            async move {
                let response = connection.worker.handle(listener, request).await;
                Ok::<_, Infallible>(response)
            }
        });
        // Lent rather than given, so that it comes back once the engine is done with it.
        let (lent, back) = Lent::new(stream);
        let mut server = auto::Builder::new(OnThisWorker);
        // The engine's head timeout runs only with a timer to run on; without one it is
        // silently off, and a connection that stops part way through a head, or waits for
        // ever between requests, is held for ever. It restarts for each request head, so
        // it covers the wait before one as well as the head itself.
        server
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(deadlines.next_request);
        // What that timeout cannot see: the time before the engine has chosen which HTTP
        // the connection speaks — it waits for the first bytes with no deadline of its
        // own — and so a connection that never says anything, or stops part way through
        // the HTTP/2 preface. Bounded here instead, from accept to the first request.
        let cut_off = {
            let mut serving = std::pin::pin!(server.serve_connection(TokioIo::new(lent), service));
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

    async fn handle(&self, listener: usize, request: Request<Incoming>) -> Response<Body> {
        let came_in = Instant::now();
        let response = self.respond(listener, request).await;
        if let Some(counters) = self.proxy.metrics.listener(listener) {
            let took = u64::try_from(came_in.elapsed().as_nanos()).unwrap_or(u64::MAX);
            counters.responded(response.status(), took);
        }
        response
    }

    async fn respond(&self, listener: usize, request: Request<Incoming>) -> Response<Body> {
        let (mut head, body) = request.into_parts();
        // How the body is to be sent on, worked out from what arrived and before `direct`
        // takes the hop-by-hop fields off it — and before the body itself is touched,
        // because the path is chosen while there is still nothing to undo.
        let sending = sending_for(&head, &body);
        // What this request's own `Connection` named, read before routing takes the
        // hop-by-hop fields off the head. Afterwards there is nothing left to read them
        // from and everything it named looks like an ordinary field, so a trailer of that
        // name would travel on ([13 §4](../../docs/13-http1-upstream.md)).
        let nominated = crate::hop_by_hop::nominated(&head.headers);
        let directed = match self.proxy.direct(listener, &mut head) {
            Ok(directed) => directed,
            Err(answer) => return self.proxy.answer(listener, answer),
        };
        // A name it gave is not declared onwards either: the declaration says what the
        // trailers will hold, and it will not hold that.
        filter_declaration(&mut head.headers, &nominated);

        // Credentials can bind the upstream socket to this client, even when the
        // response is successful. Decide after rule filters and before either client
        // dispatches: hyper can return a socket to its pool before we see the response.
        if crate::upstream::auth::carries_credentials(&head.headers) {
            head.headers.insert(
                http::header::CONNECTION,
                http::HeaderValue::from_static("close"),
            );
        }

        let answered = match self.proxy.upstream {
            Upstream::Hyper => {
                let watch = Watch {
                    proxy: Arc::clone(&self.proxy),
                    upstream: directed.upstream_slot,
                };
                self.by_hyper(head, body, watch).await
            }
            Upstream::Ours => {
                // Before anything is looked for or opened: a place is what entitles a
                // request to a connection, so it is taken before one is sought.
                let Some(admitted) = self.admit() else {
                    return self.proxy.answer(listener, Answer::TooBusy);
                };
                self.by_ours(&directed, &head, &nominated, sending, body, admitted)
                    .await
            }
        };

        let upstream = self.proxy.metrics.upstream(directed.upstream_slot);
        let Some((mut head, body)) = answered else {
            if let Some(upstream) = upstream {
                upstream.failures.inc();
            }
            return self.proxy.answer(listener, Answer::UpstreamFailed);
        };
        if let Some(upstream) = upstream {
            upstream.responded(head.status);
        }
        strip_response(&mut head.headers);
        // The version is this hop's and not the upstream's: "Intermediaries that process
        // HTTP messages ... MUST send their own HTTP-version in forwarded messages" (RFC
        // 9110 §6.2). The engine's server still answers a client that spoke 1.0 in 1.0.
        head.version = Version::HTTP_11;
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(&mut head.headers);
        }
        Response::from_parts(head, body)
    }

    /// By the engine's client, which is what has always carried these requests.
    async fn by_hyper(
        &self,
        head: Parts,
        body: Incoming,
        watch: Watch,
    ) -> Option<(response::Parts, Body)> {
        let response = self
            .client
            .request(Request::from_parts(head, body))
            .await
            .ok()?;
        let (mut head, body) = response.into_parts();
        // HTTP has only the statuses from 100 to 599, and
        // [RFC 9110 §15](https://www.rfc-editor.org/rfc/rfc9110.html#section-15) says
        // "Values outside the range 100..599 are invalid"; the engine's client allows the
        // range above, which libraries use for errors of their own. An invalid status is
        // answered 502, which is the "process the response as if it had a 5xx" that §15
        // asks for, and the body goes unread so that the engine does not hand the
        // connection on. Only unread is in this end's gift here: a body already complete
        // leaves the connection in the engine's pool whatever this does with it.
        if !(100..=599).contains(&head.status.as_u16()) {
            return None;
        }
        // The engine's client consumes every other interim answer, so only a 101 arrives
        // here. No request asked for one — `Upgrade` is taken off every request — and this
        // proxy tunnels nothing, so passing it on would hand the client a switch it never
        // asked for. Dropping the answer unread drops the connection the engine set aside
        // for the switch ([13 §1](../../../docs/13-http1-upstream.md)).
        if head.status.is_informational() {
            return None;
        }
        // Read here, because `respond` takes the hop-by-hop fields off this head before
        // the trailers behind it arrive.
        let nominated = crate::hop_by_hop::nominated(&head.headers);
        // What may not travel on is not declared onwards either.
        filter_declaration(&mut head.headers, &nominated);
        Some((head, Body::Upstream(body, nominated, watch)))
    }

    /// By EdgeRush's own path. Never after the other has been tried: by the time one has
    /// failed the request may already have reached the upstream, and a second attempt
    /// would be a second request.
    async fn by_ours(
        &self,
        directed: &Directed,
        head: &Parts,
        nominated: &[HeaderName],
        sending: Sending,
        body: Incoming,
        admitted: Admitted,
    ) -> Option<(response::Parts, Body)> {
        let answer = match self
            .through_h1(
                &directed.endpoint,
                &head.method,
                &head.uri,
                &head.headers,
                nominated,
                sending,
                body,
            )
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                self.proxy.metrics.stopped(why_stopped(&error));
                return None;
            }
        };
        let (read, mut body) = answer;
        // Nothing need ever poll an empty body, so its connection would otherwise sit
        // until the body object was dropped.
        if body.is_end_stream() {
            body.settle();
        }
        let mut parts = Response::new(()).into_parts().0;
        parts.status = read.status;
        parts.version = read.version;
        parts.headers = read.headers;
        // The place goes with the body, which is what is still being worked on. Every
        // other way out of here has dropped it already.
        let watch = Watch {
            proxy: Arc::clone(&self.proxy),
            upstream: directed.upstream_slot,
        };
        Some((parts, Body::Ours(Box::new(body), admitted, watch)))
    }
}

impl Proxy {
    /// An answer of the data plane's own, counted by its reason.
    fn answer(&self, listener: usize, answer: Answer) -> Response<Body> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.answered(answer);
        }
        let mut response = Response::new(Body::Empty);
        *response.status_mut() = answer.status();
        response
    }

    /// Makes the head of a request that came in on a listener's socket the head of the
    /// request to send, target included, or says what to answer instead. All of it is done
    /// on one snapshot, which is let go of before anything is waited for; what is kept for
    /// the response is the rule, and only if it has something to do to the response, and
    /// the slot of the upstream's counters.
    fn direct(&self, listener: usize, head: &mut Parts) -> Result<Directed, Answer> {
        let snapshot = self.current.load();
        let listener = snapshot
            .listeners
            .get(listener)
            .copied()
            .flatten()
            .and_then(|position| snapshot.config.listeners.get(position))
            .ok_or(Answer::NoRoute)?;
        let forward = decide(&snapshot.config, listener, head, random())?;
        // An upstream the snapshot does not have is not known to happen.
        let upstream = forward.upstream.0;
        let endpoints = snapshot.endpoints.get(upstream).ok_or(Answer::NoBackend)?;
        let upstream_slot = *snapshot
            .upstream_slots
            .get(upstream)
            .ok_or(Answer::NoBackend)?;
        let at = pick_at(endpoints.len(), random()).ok_or(Answer::NoEndpoints)?;
        let endpoint = endpoints.get(at).ok_or(Answer::NoEndpoints)?;
        let identity = snapshot
            .destinations
            .at(upstream, at)
            .ok_or(Answer::NoEndpoints)?;

        head.uri = at_endpoint(&head.uri, endpoint).ok_or(Answer::BadTarget)?;
        head.version = Version::HTTP_11;
        // What the engine attached to the request is about the connection it came in on.
        head.extensions.clear();
        if let Some(counters) = self.metrics.upstream(upstream_slot) {
            counters.requests.inc();
        }
        let has_changes = forward.rule.response_headers.is_some();
        Ok(Directed {
            rule: has_changes.then(|| Arc::clone(forward.rule)),
            upstream_slot,
            endpoint: Arc::clone(identity),
        })
    }
}

/// What a request keeps of the snapshot it was directed on.
struct Directed {
    rule: Option<Arc<CompiledRule>>,
    upstream_slot: usize,
    /// The endpoint this request was directed to, taken from the same snapshot as the
    /// route so that no reload can come between the two.
    endpoint: Arc<ReuseIdentity>,
}

/// How a request's body is to be sent on.
///
/// Read from the request as it arrived rather than from the body's own account of itself:
/// a body that says how long it is may still end with trailers, and over HTTP/2 it always
/// may. What is certain is what the client framed it as
/// ([13 §4](../../docs/13-http1-upstream.md)).
fn sending_for(head: &Parts, body: &Incoming) -> Sending {
    // The engine says outright when there is no body, and that is the one thing a length
    // alone would not settle. A client that said its body is a length of nothing goes on
    // saying so: RFC 9110 §8.6 has a sender state a length for a method whose content
    // means something, and a server may answer 411 without one. One that said nothing,
    // as an HTTP/2 request ended by its headers does, is sent no framing either.
    if body.is_end_stream() {
        return match request_length(&head.headers) {
            Some(0) => Sending::Length(0),
            _ => Sending::None,
        };
    }
    if head.version == Version::HTTP_2 {
        // Framed as frames, with trailers allowed after any of them. There is no length
        // here that would still be true by the end.
        return Sending::Chunked;
    }
    if crate::hop_by_hop::is_chunked_request(&head.headers) {
        return Sending::Chunked;
    }
    match request_length(&head.headers) {
        Some(length) => Sending::Length(length),
        // No length and no coding, over HTTP/1.1, is no body at all.
        None => Sending::None,
    }
}

/// A request's `Content-Length`, where it has exactly one that is a plain number. Hyper
/// has already refused what it will refuse; anything left that does not read as a length
/// is treated as no length, and the body is framed by this end instead.
fn request_length(headers: &HeaderMap) -> Option<u64> {
    let mut lengths = headers.get_all(http::header::CONTENT_LENGTH).iter();
    let only = lengths.next()?;
    if lengths.next().is_some() {
        return None;
    }
    only.to_str().ok()?.trim().parse().ok()
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProxyError {
    /// An endpoint address that cannot be written into a request target.
    #[error("endpoint {0} cannot be part of a request target")]
    Endpoint(SocketAddr),
}

fn authority(endpoint: &SocketAddr) -> Result<Authority, ProxyError> {
    Authority::try_from(endpoint.to_string()).map_err(|_| ProxyError::Endpoint(*endpoint))
}

/// One of the endpoints, each as likely as any other; `None` if there are none.
fn pick_at(endpoints: usize, random: u64) -> Option<usize> {
    let count = u64::try_from(endpoints).ok()?;
    usize::try_from(random.checked_rem(count)?).ok()
}

/// The same path and query, at the endpoint: the form in which the client is told where to
/// connect. What it sends is the origin form, and the `Host` field is left as it is.
fn at_endpoint(target: &Uri, endpoint: &Authority) -> Option<Uri> {
    let mut parts = target.clone().into_parts();
    parts.scheme = Some(Scheme::HTTP);
    parts.authority = Some(endpoint.clone());
    Uri::from_parts(parts).ok()
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
    use edgerush_config::{Config, compile};
    use http::StatusCode;
    use http_body_util::{BodyExt, Empty, Full};
    use std::num::NonZeroUsize;
    use std::rc::Rc;
    use std::time::Duration;

    /// The engine must take a service, and a body, that cannot leave the thread they were
    /// made on: a worker's own things — the pool of upstream connections above all — will
    /// be exactly that, and an executor that wanted `Send` would refuse them. HTTP/2 is
    /// where it would refuse, because the engine spawns a future for every stream, so both
    /// protocols are asked here.
    #[test]
    fn the_engine_serves_what_cannot_leave_the_worker() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = socket.local_addr().unwrap();
            tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = socket.accept().await.unwrap();
                    let _detached = tokio::task::spawn_local(async move {
                        // An Rc is held for the life of the connection and cloned into
                        // every request: nothing here could be sent to another thread.
                        let answer = Rc::new(Bytes::from_static(b"on this worker"));
                        let service = service_fn(move |_| {
                            let answer = Rc::clone(&answer);
                            async move {
                                let body = Full::new(Bytes::clone(&answer));
                                Ok::<_, Infallible>(Response::new(body))
                            }
                        });
                        let _closed = auto::Builder::new(OnThisWorker)
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            });

            assert_eq!(asked_over_http1(address).await, "on this worker");
            assert_eq!(asked_over_http2(address).await, "on this worker");
        }));
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
                    let burst: Vec<_> = (0..20).map(|_| blocks.take()).collect();
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
    };

    /// How late a deadline may be seen to fire on a loaded machine.
    const SLACK: Duration = Duration::from_millis(400);

    /// Serves a worker for `upstream` on a listener of its own, and says where.
    async fn serving_worker(upstream: SocketAddr) -> SocketAddr {
        let worker = Worker::with_deadlines(sending_to(upstream), H1Limits::default(), SHORT);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
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
    /// way through the HTTP/2 preface — where the engine's own protocol detection waits
    /// with no deadline of its own — or is trickling a head. Without this each of them
    /// holds its connection for ever.
    #[tokio::test]
    async fn a_connection_without_a_first_request_is_closed_at_its_deadline() {
        use tokio::io::AsyncWriteExt;
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (upstream, _) = counting_upstream().await;
                let front = serving_worker(upstream).await;
                for said in [&b""[..], b"PRI * HT", b"GET / HTTP/1.1\r\nhost: exa"] {
                    let mut stream = TcpStream::connect(front).await.unwrap();
                    stream.write_all(said).await.unwrap();
                    let took = closed_after(&mut stream).await;
                    assert!(
                        took >= SHORT.first_request && took < SHORT.first_request + SLACK,
                        "{:?}: closed after {took:?}",
                        String::from_utf8_lossy(said)
                    );
                }
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
                        took >= SHORT.next_request && took < SHORT.next_request + SLACK,
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

    /// A worker takes on only so many exchanges at once, and answers the rest rather
    /// than opening another connection for them. The place is given back by every way out
    /// of an exchange, a failed one included, so a worker that has been full is not full
    /// for ever ([13 §7](../../docs/13-http1-upstream.md)).
    #[test]
    fn a_worker_full_of_exchanges_answers_rather_than_take_another() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            // An upstream that accepts and says nothing: every request sent to it stays
            // in hand, which is the only way to have a worker hold several at once.
            let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = backend.local_addr().unwrap();
            let held = Rc::new(RefCell::new(Vec::new()));
            let holding = Rc::clone(&held);
            let _accepting = tokio::task::spawn_local(async move {
                loop {
                    let (stream, _) = backend.accept().await.unwrap();
                    holding.borrow_mut().push(stream);
                }
            });

            // Short bounds so that the exchange which is meant to fail fails in a
            // second or two rather than in the default half-minute. Long enough that
            // nothing parked here is given up on before the test has looked at it.
            let limits = H1Limits {
                exchanges: 2,
                idle: Duration::from_secs(2),
                final_head: Duration::from_secs(2),
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(sending_to(upstream), limits);
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

            // Two that will not come back, waited for where the worker has committed to
            // them rather than where they were sent.
            for _ in 0..2 {
                let _parked = tokio::task::spawn_local(async move {
                    let _never = status_over_http1(front).await;
                });
            }
            until(|| held.borrow().len() == 2).await;

            assert_eq!(
                status_over_http1(front).await,
                StatusCode::SERVICE_UNAVAILABLE
            );

            // The upstream goes away, so both exchanges fail; a failure gives its place
            // back like any other ending, and the worker takes requests again.
            held.borrow_mut().clear();
            until(|| worker.in_flight.get() == 0).await;
            assert_eq!(status_over_http1(front).await, StatusCode::BAD_GATEWAY);
        }));
    }

    /// The same, by whichever client is named: how many idle connections a worker keeps
    /// is the data plane's bound, so it has to hold for both of them.
    fn sending_to_by(upstream: SocketAddr, by: Upstream) -> Arc<Proxy> {
        let yaml = format!(
            r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends: [{{ upstream: up, weight: 1 }}]
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
        );
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        let compiled = compile(&config).unwrap();
        Arc::new(Proxy::new(compiled, NonZeroUsize::MIN, by).unwrap())
    }

    /// How many connections a worker opens for `requests` sent one after another, when it
    /// may keep `idle_per_destination` of them.
    async fn connections_for(by: Upstream, keeping: usize, requests: usize) -> usize {
        let (upstream, opened) = counting_upstream().await;
        let limits = H1Limits {
            idle_per_destination: keeping,
            ..H1Limits::default()
        };
        let worker = Worker::with_limits(sending_to_by(upstream, by), limits);
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front = socket.local_addr().unwrap();
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
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
    fn the_bound_on_idle_connections_holds_for_both_clients() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            for by in [Upstream::Hyper, Upstream::Ours] {
                // Keeping none: every request after the first opens its own connection.
                assert_eq!(connections_for(by, 0, 3).await, 3, "{by:?} keeping none");
                // Keeping one: the first connection carries all three.
                assert_eq!(connections_for(by, 1, 3).await, 1, "{by:?} keeping one");
            }
        }));
    }

    /// What became of every connection is counted, and so is a worker's own holding.
    /// A benchmark that cannot tell a reused connection from a fresh one is measuring
    /// the wrong thing ([13 §7](../../docs/13-http1-upstream.md)).
    #[test]
    fn what_became_of_a_connection_is_counted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let (upstream, _opened) = counting_upstream().await;
            let proxy = sending_to_by(upstream, Upstream::Ours);
            let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

            for _ in 0..3 {
                assert_eq!(status_over_http1(front).await, StatusCode::OK);
            }
            // One connection opened for the first request, and taken again for the rest.
            let scrape = proxy.metrics.render(&["web".to_owned()], &[]);
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
            proxy
                .metrics
                .worker()
                .holding(worker.in_flight.get(), worker.idle_connections());
            let scrape = proxy.metrics.render(&["web".to_owned()], &[]);
            assert!(
                scrape.contains(
                    "edgerush_upstream_connections_idle 1
"
                ),
                "{scrape}"
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

            let proxy = sending_to_by(upstream, Upstream::Ours);
            let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let front = socket.local_addr().unwrap();
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

            assert_eq!(status_over_http1(front).await, StatusCode::BAD_GATEWAY);
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)]);
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
        for by in [Upstream::Hyper, Upstream::Ours] {
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

                let proxy = sending_to_by(upstream, by);
                let worker = Worker::with_limits(Arc::clone(&proxy), H1Limits::default());
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

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
                assert_eq!(answer.status(), StatusCode::OK, "{by:?}");
                assert!(
                    answer.into_body().collect().await.is_err(),
                    "{by:?} made a truncated body look whole"
                );

                let up = proxy.metrics.upstream_slot("up");
                let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)]);
                assert!(
                    scrape.contains("edgerush_upstream_body_failures_total{upstream=\"up\"} 1\n"),
                    "{by:?}: {scrape}"
                );
                // And the answer was counted a success, which is why the body needed
                // a counter of its own.
                assert!(
                    scrape.contains(
                        "edgerush_upstream_responses_total{upstream=\"up\",class=\"2xx\"} 1\n"
                    ),
                    "{by:?}: {scrape}"
                );
            }));
        }
    }

    /// A proxy of one listener that sends everything to `upstream`, by EdgeRush's own
    /// path because that is the path with a bound on it.
    fn sending_to(upstream: SocketAddr) -> Arc<Proxy> {
        let yaml = format!(
            r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends: [{{ upstream: up, weight: 1 }}]
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
        );
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        let compiled = compile(&config).unwrap();
        Arc::new(Proxy::new(compiled, NonZeroUsize::MIN, Upstream::Ours).unwrap())
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
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _detached = tokio::task::spawn_local(async move {
            let _closed = connection.await;
        });
        // Routed like any other request, so it needs a host to be routed by.
        let request = Request::builder()
            .uri("/")
            .header("host", "example.test")
            .body(Empty::<Bytes>::new())
            .unwrap();
        sender.send_request(request).await.unwrap().status()
    }

    /// What one HTTP/1.1 request to `address` answers, as text.
    async fn asked_over_http1(address: SocketAddr) -> String {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _detached = tokio::task::spawn_local(async move {
            let _closed = connection.await;
        });
        let request = Request::new(Empty::<Bytes>::new());
        collected(sender.send_request(request).await.unwrap()).await
    }

    /// The same over HTTP/2, which the engine tells apart by the preface the client sends.
    async fn asked_over_http2(address: SocketAddr) -> String {
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
        collected(sender.send_request(request).await.unwrap()).await
    }

    async fn collected(response: Response<Incoming>) -> String {
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(body.to_vec()).unwrap()
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
            Upstream::Ours,
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
        mut body: H1Body<TcpStream, http_body_util::Empty<Bytes>>,
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
                let proxy =
                    Arc::new(Proxy::new(config, NonZeroUsize::MIN, Upstream::Ours).unwrap());
                let worker = Worker::new(Arc::clone(&proxy));
                let identity = Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());

                for round in 0..3 {
                    let (head, body) = worker
                        .through_h1(
                            &identity,
                            &Method::GET,
                            &"/x".parse().unwrap(),
                            &HeaderMap::new(),
                            &[],
                            Sending::None,
                            http_body_util::Empty::<Bytes>::new(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(head.status, 200, "round {round}");

                    let (data, went_back) = drain(body, &worker.limits).await;
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
            yaml += &format!("  {name}: {{ address: \"127.0.0.1:{at}\", protocol: http }}\n");
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
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
                &Destinations::default(),
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
        let proxy = Proxy::new(
            config_with(&["web", "admin"]),
            NonZeroUsize::MIN,
            Upstream::Hyper,
        )
        .unwrap();
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
        let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN, Upstream::Hyper).unwrap();
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
        let proxy = Proxy::new(config_with(&["web"]), NonZeroUsize::MIN, Upstream::Hyper).unwrap();
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
}
