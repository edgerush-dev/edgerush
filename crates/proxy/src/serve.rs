//! Serving: connections come in, requests go through the request core, and what it
//! forwards goes to an endpoint of the chosen upstream and comes back. This is the adapter
//! between the HTTP engine (hyper) and the core, and the only place that knows both.
//!
//! Bodies stream in both directions and are never held here. Upstream connections are
//! HTTP/1.1 from hyper-util's pool for now.
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
use crate::metrics::{Answer, Metrics};
use crate::random::random;
use crate::request::decide;
use arc_swap::ArcSwap;
use edgerush_config::{Compiled, CompiledRule};
use http::request::Parts;
use http::uri::{Authority, Scheme};
use http::{Request, Response, Uri, Version};
use http_body_util::{Either, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};

/// How long accepting pauses after an error that is not about one connection, instead of
/// failing again at once, over and over.
pub(crate) const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

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

/// What is answered: the upstream's body as it arrives, or nothing.
type Body = Either<Incoming, Empty<Bytes>>;

/// A data plane: serves the listeners of a compiled config, forwards to its upstreams, and
/// takes a new config while it runs.
#[derive(Debug)]
pub struct Proxy {
    /// The names of the listeners of the config the data plane was made with. A socket is
    /// served as the listener at its position here, whatever configs come later.
    listeners: Vec<String>,
    current: ArcSwap<Snapshot>,
    /// Outside the snapshot, so that a reload resets no counter.
    metrics: Metrics,
    /// One for the life of the data plane: a reload does not throw warm connections away.
    /// Those to an endpoint that is no longer used grow idle and are closed.
    client: Client<HttpConnector, Incoming>,
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
}

impl Snapshot {
    fn new(config: Compiled, listeners: &[String], metrics: &Metrics) -> Result<Self, ProxyError> {
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
        Ok(Self {
            config,
            listeners,
            endpoints,
            upstream_slots,
        })
    }
}

impl Proxy {
    /// A data plane that runs `config`. Nothing is listened on or connected to yet.
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] for an endpoint address that cannot be part of a request
    /// target (one with an IPv6 zone).
    pub fn new(config: Compiled) -> Result<Self, ProxyError> {
        let listeners: Vec<String> = config
            .listeners
            .iter()
            .map(|listener| listener.name.clone())
            .collect();
        // A shard for every thread that may serve requests at the same time.
        let shards = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        let metrics = Metrics::new(shards, listeners.len());
        let current = ArcSwap::from_pointee(Snapshot::new(config, &listeners, &metrics)?);
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(connector);
        Ok(Self {
            listeners,
            current,
            metrics,
            client,
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
        let snapshot = Snapshot::new(config, &self.listeners, &self.metrics)?;
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

    /// Serves the connections that come in on `socket` as those of the listener at
    /// position `listener` of [`Proxy::listeners`], HTTP/1.1 and HTTP/2 alike. Never
    /// returns; dropping the future stops accepting, and connections already accepted
    /// carry on.
    ///
    /// # Panics
    ///
    /// Runs inside a worker's `LocalSet`, where the connections it accepts are served;
    /// without one there is nowhere to put them and the first connection panics.
    pub async fn serve(self: Arc<Self>, listener: usize, socket: TcpListener) {
        loop {
            match socket.accept().await {
                Ok((stream, _)) => {
                    let connection = Arc::clone(&self).serve_connection(listener, stream);
                    let _detached = tokio::task::spawn_local(connection);
                }
                Err(error) => {
                    if let Some(pause) = self.accept_failed(listener, &error) {
                        tokio::time::sleep(pause).await;
                    }
                }
            }
        }
    }

    /// Counts a failure to accept on the socket of the listener at position `listener`,
    /// and says how long to wait before accepting again: not at all after the failure of
    /// the one connection that was next in line, a moment after one that is not about a
    /// connection — out of file descriptors, say — and would only happen again at once.
    /// For whoever accepts by themselves and serves with [`Proxy::serve_connection`].
    pub fn accept_failed(&self, listener: usize, error: &io::Error) -> Option<Duration> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.accept_errors.inc();
        }
        (!is_about_one_connection(error)).then_some(ACCEPT_PAUSE)
    }

    /// Serves one connection, to its end, as one of the listener at position `listener`
    /// of [`Proxy::listeners`]. It may have been accepted anywhere — by another thread,
    /// which then hands it over as a socket of the standard library — as long as `stream`
    /// was made on the runtime that runs this.
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet` ([`OnThisWorker`]). An HTTP/2 connection
    /// panics without one, as the engine spawns a future for every stream.
    pub async fn serve_connection(self: Arc<Self>, listener: usize, stream: TcpStream) {
        // Worth having, not worth refusing a connection over.
        let _unset = stream.set_nodelay(true);
        let connection = Arc::new(Connection::open(self, listener));
        // Every request clones a handle, as the engine wants futures that own what they
        // use. A handle of the connection's own keeps that count off a line of cache that
        // all the workers would otherwise write to.
        let service = service_fn(move |request| {
            let connection = Arc::clone(&connection);
            async move {
                let response = connection.proxy.handle(listener, request).await;
                Ok::<_, Infallible>(response)
            }
        });
        // An error here is the end of one connection: the peer went away or spoke
        // nonsense. There is nobody to tell.
        let _closed = auto::Builder::new(OnThisWorker)
            .serve_connection(TokioIo::new(stream), service)
            .await;
    }

    async fn handle(&self, listener: usize, request: Request<Incoming>) -> Response<Body> {
        let came_in = Instant::now();
        let response = self.respond(listener, request).await;
        if let Some(counters) = self.metrics.listener(listener) {
            let took = u64::try_from(came_in.elapsed().as_nanos()).unwrap_or(u64::MAX);
            counters.responded(response.status(), took);
        }
        response
    }

    async fn respond(&self, listener: usize, request: Request<Incoming>) -> Response<Body> {
        let (mut head, body) = request.into_parts();
        let directed = match self.direct(listener, &mut head) {
            Ok(directed) => directed,
            Err(answer) => return self.answer(listener, answer),
        };
        let response = self.client.request(Request::from_parts(head, body)).await;
        // Counted where the request is now: it may have changed threads while it waited.
        let upstream = self.metrics.upstream(directed.upstream_slot);
        let Ok(response) = response else {
            if let Some(upstream) = upstream {
                upstream.failures.inc();
            }
            return self.answer(listener, Answer::UpstreamFailed);
        };
        if let Some(upstream) = upstream {
            upstream.responded(response.status());
        }
        let (mut head, body) = response.into_parts();
        strip_response(&mut head.headers);
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(&mut head.headers);
        }
        Response::from_parts(head, Either::Left(body))
    }

    /// An answer of the data plane's own, counted by its reason.
    fn answer(&self, listener: usize, answer: Answer) -> Response<Body> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.answered(answer);
        }
        let mut response = Response::new(Either::Right(Empty::new()));
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
        let endpoint = pick(endpoints, random()).ok_or(Answer::NoEndpoints)?;

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
        })
    }
}

/// What a request keeps of the snapshot it was directed on.
struct Directed {
    rule: Option<Arc<CompiledRule>>,
    upstream_slot: usize,
}

/// A connection that came in on a listener's socket: what its requests share, and what
/// counts it as open until the last of them is done, wherever that happens.
struct Connection {
    proxy: Arc<Proxy>,
    listener: usize,
}

impl Connection {
    fn open(proxy: Arc<Proxy>, listener: usize) -> Self {
        if let Some(counters) = proxy.metrics.listener(listener) {
            counters.accepted.inc();
            counters.active.inc();
        }
        Self { proxy, listener }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(counters) = self.proxy.metrics.listener(self.listener) {
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
fn pick(endpoints: &[Authority], random: u64) -> Option<&Authority> {
    let count = u64::try_from(endpoints.len()).ok()?;
    let position = usize::try_from(random.checked_rem(count)?).ok()?;
    endpoints.get(position)
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
    use http_body_util::{BodyExt, Full};
    use std::rc::Rc;

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
            Snapshot::new(config_with(names), &sockets, &metrics)
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
        let proxy = Proxy::new(config_with(&["web", "admin"])).unwrap();
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
        let endpoints = authorities(&["127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"]);
        let picked: Vec<&str> = (0..4)
            .map(|random| pick(&endpoints, random).unwrap().as_str())
            .collect();
        assert_eq!(
            picked,
            ["127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:1"]
        );
        assert_eq!(pick(&endpoints, u64::MAX).unwrap(), "127.0.0.1:1");
    }

    #[test]
    fn no_endpoints_is_nowhere_to_connect() {
        assert_eq!(pick(&[], 7), None);
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
        let proxy = Proxy::new(config_with(&["web"])).unwrap();
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
        let proxy = Proxy::new(config_with(&["web"])).unwrap();
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
