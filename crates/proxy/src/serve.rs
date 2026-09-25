//! Serving: connections come in, requests go through the request core, and what it
//! forwards goes to an endpoint of the chosen upstream and comes back. This is where
//! EdgeRush's own servers, of HTTP/1 and of HTTP/2, meet the core.
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
use crate::drain::Drain;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::linger::{self, Lent, linger};
use crate::metrics::{Answer, Metrics, Socket, Stopped};
use crate::random::random;
use crate::raw::{RawAnswer, RawHead};
use crate::request::decide;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::storage::Storage;
use crate::tls::{self, Tls, TlsError};
use crate::upstream::destination::{Destinations, Keys, ReuseIdentity};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, SMALL, Sizes};
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use crate::upstream::h1::exchange::{Exchange, ExchangeError, H1Body, nothing_to_say};
use crate::upstream::h1::pool::{Lease, Pool};
use arc_swap::ArcSwap;
use bytes::Bytes;
use edgerush_config::{Compiled, CompiledRule};
use edgerush_router::Fields;
use http::uri::{Authority, Scheme};
use http::{HeaderName, Method, Request, Response, Uri, Version};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
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
    Ours(Box<H1Body<TcpStream, RequestBody>>, Admitted, Watch),
    /// An answer of the data plane's own. It has no body, and never will have one.
    Empty,
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
            Self::Empty => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Ours(ours, ..) => ours.is_end_stream(),
            Self::Empty => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Ours(ours, ..) => ours.size_hint(),
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
    pool: Rc<RefCell<Pool<TcpStream>>>,
    /// What its exchanges read into, lent and taken back rather than made each time
    /// ([13 §7](../../docs/13-http1-upstream.md)).
    blocks: Rc<RefCell<Blocks>>,
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
}

/// Serves `socket`, a connection of `ours` that speaks HTTP/1, with our own server (14).
/// `asking` is set when the first request is handed over.
async fn serve_h1<S>(ours: Rc<Connection>, asking: Rc<Cell<bool>>, deadlines: Deadlines, socket: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let worker = Rc::clone(&ours.worker);
    let listener = ours.listener;
    let settings = h1::Settings {
        limits: worker.limits,
        bounds: Bounds {
            first_request: deadlines.first_request,
            // 14 §8's ten seconds for a head once it has begun, never longer than the wait
            // for it to begin.
            next_head: Bounds::default().next_head.min(deadlines.next_request),
            keep_alive: deadlines.next_request,
            idle: deadlines.idle,
            ..Bounds::default()
        },
        budget: h1::Budget::default(),
    };
    // The connection is kept by this for as long as it is served; each request's future
    // owns only a handle on the worker. It is that future itself, not one wrapped around
    // it, so that it is not moved into another on every request.
    let respond = move |head: RawHead, body, interim| {
        asking.set(true);
        Rc::clone(&ours.worker).handle_head(listener, head, body, Some(interim))
    };
    let _ended = h1::serve(
        socket,
        settings,
        Rc::clone(&worker.blocks),
        || worker.date.get(),
        &worker.drain,
        respond,
    )
    .await;
}

/// Serves `socket`, a connection of `ours` that speaks HTTP/2, over h2 (15 step 2).
/// `asking` is set when the first request is handed over.
async fn serve_h2<S>(ours: Rc<Connection>, asking: Rc<Cell<bool>>, deadlines: Deadlines, socket: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let worker = Rc::clone(&ours.worker);
    let listener = ours.listener;
    let storage = Rc::clone(worker.blocks.borrow().storage());
    let dating = Rc::clone(&worker);
    let date = Rc::new(move || dating.date.get());
    let respond = Rc::new(move |request: Request<RequestBody>, interim| {
        asking.set(true);
        Rc::clone(&ours.worker).handle(listener, request, Some(interim))
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
    /// By position in [`Proxy::listeners`]: what a connection to it is accepted with, if
    /// it is to speak TLS. Kept from the config before for as long as the certificates
    /// are the same, and with it the keys of the session tickets it has issued.
    tls: Vec<Option<Arc<Tls>>>,
}

impl Snapshot {
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
                let kept = previous
                    .and_then(|previous| previous.tls.get(position))
                    .and_then(Option::as_ref)
                    .filter(|kept| kept.is_for(source));
                match kept {
                    Some(kept) => Ok(Some(Arc::clone(kept))),
                    None => Tls::new(source)
                        .map(|tls| Some(Arc::new(tls)))
                        .map_err(|error| ProxyError::Tls {
                            listener: listener.name.clone(),
                            error,
                        }),
                }
            })
            .collect::<Result<_, _>>()?;
        let upstream_slots = config
            .upstreams
            .iter()
            .map(|upstream| metrics.upstream_slot(&upstream.name))
            .collect();
        let nothing_yet = Destinations::default();
        let previous_destinations =
            previous.map_or(&nothing_yet, |previous| &previous.destinations);
        let destinations = Destinations::reconcile(&config, previous_destinations, keys);
        Ok(Self {
            config,
            listeners,
            endpoints,
            upstream_slots,
            destinations,
            tls,
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
        Ok(Self {
            listeners,
            current: ArcSwap::from_pointee(snapshot),
            metrics,
            keys,
            draining: AtomicBool::new(false),
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
        Rc::new(Self {
            proxy,
            pool: Rc::new(RefCell::new(Pool::default())),
            blocks: Rc::new(RefCell::new(Blocks::new(
                Sizes::within(&limits, SMALL),
                Storage::new(limits.storage),
            ))),
            in_flight: Rc::new(Cell::new(0)),
            limits,
            deadlines,
            date: Cell::new(HttpDate::from_unix(unix_now())),
            drain: Rc::new(Drain::default()),
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
    /// again ([13 §3](../../docs/13-http1-upstream.md)). Spawned into the worker's
    /// `LocalSet` beside its listeners; a worker without it keeps what it should drop.
    pub async fn maintain(self: Rc<Self>) {
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
    ) -> Result<(RawAnswer, Box<H1Body<TcpStream, B>>), ExchangeError>
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
                let opening = TcpStream::connect(identity.address());
                let socket = connect_within(self.limits.connect, opening).await?;
                // Worth having, not worth refusing an upstream over.
                let _unset = socket.set_nodelay(true);
                (socket, Instant::now())
            }
        };

        let mut exchange = Exchange::new(socket, Rc::clone(&self.blocks));
        if let Some(interim) = interim {
            exchange = exchange.heard_by(interim);
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
                self.limits,
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
        let deadlines = self.deadlines;
        // The TLS of the config the connection came in under, which it keeps to its end.
        let tls = self
            .proxy
            .current
            .load()
            .tls
            .get(listener)
            .cloned()
            .flatten();
        let connection = Rc::new(Connection::open(self, listener));
        // Set when the engine hands over the first request, which is the end of the one
        // stretch its own deadlines do not cover.
        let asked = Rc::new(Cell::new(false));
        let (ours, ours_asking) = (Rc::clone(&connection), Rc::clone(&asked));
        // Lent rather than given, so that it comes back once the engine is done with it.
        let (lent, back) = Lent::new(stream);
        // Each served by our own server: HTTP/1 by the one of 14, HTTP/2 over h2 (15 step 2).
        let serving = async move {
            match tls {
                // Told apart by our own detector.
                None => match detect(lent).await {
                    Ok(Some((Protocol::Http1, replay))) => {
                        serve_h1(ours, ours_asking, deadlines, replay).await;
                    }
                    Ok(Some((Protocol::Http2, replay))) => {
                        serve_h2(ours, ours_asking, deadlines, replay).await;
                    }
                    // Closed having said nothing, or failed before saying enough.
                    Ok(None) | Err(_) => {}
                },
                // Told apart by what the handshake agreed on (ALPN).
                Some(tls) => {
                    // A handshake that fails has nobody to tell but the client, whom
                    // BoringSSL has sent its alert.
                    let Ok(secured) = tokio_boring::accept(tls.acceptor(), lent).await else {
                        return;
                    };
                    if secured.ssl().selected_alpn_protocol() == Some(tls::H2) {
                        serve_h2(ours, ours_asking, deadlines, secured).await;
                    } else {
                        serve_h1(ours, ours_asking, deadlines, secured).await;
                    }
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

    /// Answers a request that came in on `listener`. `interim` is where the server that
    /// read it wants the upstream's interim answers, if it passes them on
    /// ([14 §5](../../docs/14-downstream-server.md)).
    async fn handle(
        self: Rc<Self>,
        listener: usize,
        request: Request<RequestBody>,
        interim: Option<Interim>,
    ) -> Answered<Body> {
        let (head, body) = request.into_parts();
        self.handle_head(listener, head, body, interim).await
    }

    /// The same for a request's head of whatever kind: a map, or the raw head our own
    /// server reads ([14 §6](../../docs/14-downstream-server.md)).
    async fn handle_head<H: Forwarded>(
        self: Rc<Self>,
        listener: usize,
        head: H,
        body: RequestBody,
        interim: Option<Interim>,
    ) -> Answered<Body> {
        let came_in = Instant::now();
        let answered = self.respond_to(listener, head, body, interim).await;
        if let Some(counters) = self.proxy.metrics.listener(listener) {
            let took = u64::try_from(came_in.elapsed().as_nanos()).unwrap_or(u64::MAX);
            counters.responded(answered.status(), took);
        }
        answered
    }

    async fn respond_to<H: Forwarded>(
        &self,
        listener: usize,
        mut head: H,
        body: RequestBody,
        interim: Option<Interim>,
    ) -> Answered<Body> {
        // How the body is to be sent on, worked out from what arrived and before `direct`
        // takes the hop-by-hop fields off it — and before the body itself is touched,
        // because the path is chosen while there is still nothing to undo.
        let sending = sending_for(&head, &body);
        // What this request's own `Connection` named, read before routing takes the
        // hop-by-hop fields off the head. Afterwards there is nothing left to read them
        // from and everything it named looks like an ordinary field, so a trailer of that
        // name would travel on ([13 §4](../../docs/13-http1-upstream.md)).
        let nominated = crate::hop_by_hop::nominated(head.outgoing());
        let directed = match self.proxy.direct(listener, &mut head) {
            Ok(directed) => directed,
            Err(answer) => return self.proxy.answer(listener, answer).into(),
        };
        // A name it gave is not declared onwards either: the declaration says what the
        // trailers will hold, and it will not hold that.
        if let Err(rejection) = head.filter_declaration(&nominated) {
            return self.proxy.answer(listener, rejection.into()).into();
        }

        // Credentials can bind the upstream socket to this client, even when the
        // response is successful. Decide after rule filters and before either client
        // dispatches: hyper can return a socket to its pool before we see the response.
        if crate::upstream::auth::carries_credentials(head.outgoing())
            && let Err(rejection) = head.close_connection()
        {
            return self.proxy.answer(listener, rejection.into()).into();
        }

        // Before either client looks for a connection or opens one: a place is what
        // entitles a request to a connection, so it is taken before one is sought. The same
        // bound whichever client carries the request, so that the two are compared doing
        // the same work ([14 §2](../../docs/14-downstream-server.md)).
        let Some(admitted) = self.admit() else {
            return self.proxy.answer(listener, Answer::TooBusy).into();
        };
        let answered = self
            .by_ours(
                &directed, &head, &nominated, sending, body, admitted, interim,
            )
            .await;

        let upstream = self.proxy.metrics.upstream(directed.upstream_slot);
        let (mut answer, body) = match answered {
            Ok(answered) => answered,
            // The worker's own storage running out, or a client's body that cannot be read,
            // is not the upstream failing, and is not counted as though it were
            // ([14 §8](../../docs/14-downstream-server.md)).
            Err(answer @ (Answer::Exhausted | Answer::BadBody | Answer::BodyTimedOut)) => {
                return self.proxy.answer(listener, answer).into();
            }
            Err(answer) => {
                if let Some(upstream) = upstream {
                    upstream.failures.inc();
                }
                return self.proxy.answer(listener, answer).into();
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
        let edited = answer.filter_declaration(&nominated).and_then(|()| {
            answer.strip();
            changes.map_or(Ok(()), |changes| answer.apply(changes))
        });
        if edited.is_err() {
            return self.proxy.answer(listener, Answer::Edits).into();
        }
        // Written in this hop's version and not the upstream's: "Intermediaries that
        // process HTTP messages ... MUST send their own HTTP-version in forwarded messages"
        // (RFC 9110 §6.2). Our writer says HTTP/1.1, and so does a map made for HTTP/2.
        Answered::Raw(answer, body)
    }

    /// By EdgeRush's own path, the one there is.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn by_ours<H: Forwarded>(
        &self,
        directed: &Directed,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
    ) -> Result<(RawAnswer, Body), Answer> {
        let answer = match self
            .through_h1(
                &directed.endpoint,
                head.method(),
                head.uri(),
                head.outgoing(),
                nominated,
                sending,
                body,
                interim,
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
                    _ => Answer::UpstreamFailed,
                });
            }
        };
        let (read, mut body) = answer;
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
}

impl Proxy {
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

    /// Makes the head of a request that came in on a listener's socket the head of the
    /// request to send, target included, or says what to answer instead. All of it is done
    /// on one snapshot, which is let go of before anything is waited for; what is kept for
    /// the response is the rule, and only if it has something to do to the response, and
    /// the slot of the upstream's counters.
    fn direct<H: Forwarded>(&self, listener: usize, head: &mut H) -> Result<Directed, Answer> {
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

        let target = at_endpoint(head.uri(), endpoint).ok_or(Answer::BadTarget)?;
        head.set_uri(target);
        head.onward();
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
    if head.version() == Version::HTTP_2 {
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
    /// A listener's certificates that cannot be served.
    #[error("listener `{listener}`: {error}")]
    Tls {
        /// The listener's name.
        listener: String,
        /// What is wrong with them.
        error: TlsError,
    },
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
    async fn serving_worker(upstream: SocketAddr) -> SocketAddr {
        let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
        let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
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
  web: {{ address: "127.0.0.1:0", protocol: http }}
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
        backends: [{{ upstream: up, weight: 1 }}]
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
                );
                let config: Config = serde_saphyr::from_str(&yaml).unwrap();
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let front = socket.local_addr().unwrap();
                let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

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
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
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
        let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
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

        let other = vec![crate::tls::testing::certificate(&["example.test"])];
        proxy
            .reload(everything_secured_to(upstream, other))
            .unwrap();
        let after = tls(&proxy);
        assert!(!Arc::ptr_eq(&before, &after));

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
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

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
                    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
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
            let storage = worker.blocks.borrow().storage().used();
            proxy.metrics.worker().holding(
                worker.in_flight.get(),
                worker.idle_connections(),
                storage,
            );
            let scrape = proxy.metrics.render(&["web".to_owned()], &[]);
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
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

            assert_eq!(
                status_over_http2(front).await,
                StatusCode::SERVICE_UNAVAILABLE
            );
            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)]);
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
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

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
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)]);
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
            let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));

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
            assert_eq!(answer.status(), StatusCode::OK);
            assert!(
                answer.into_body().collect().await.is_err(),
                "a truncated body was made to look whole"
            );

            let up = proxy.metrics.upstream_slot("up");
            let scrape = proxy.metrics.render(&["web".to_owned()], &[("up", up)]);
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
        web.tls = Some(edgerush_config::Tls { certificates });
        compile(&config).unwrap()
    }

    fn everything_config(upstream: SocketAddr) -> Config {
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
}
