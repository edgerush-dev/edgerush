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

use crate::hop_by_hop::strip_response;
use crate::random::random;
use crate::request::decide;
use arc_swap::ArcSwap;
use edgerush_config::{Compiled, CompiledRule};
use http::request::Parts;
use http::uri::{Authority, Scheme};
use http::{Request, Response, StatusCode, Uri, Version};
use http_body_util::{Either, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// How long accepting pauses after an error that is not about one connection — out of
/// file descriptors, say — instead of failing again at once, over and over.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

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
}

impl Snapshot {
    fn new(config: Compiled, listeners: &[String]) -> Result<Self, ProxyError> {
        let endpoints = config
            .upstreams
            .iter()
            .map(|upstream| upstream.endpoints.iter().map(authority).collect())
            .collect::<Result<_, _>>()?;
        let listeners = listeners
            .iter()
            .map(|name| config.listeners.iter().position(|l| l.name == *name))
            .collect();
        Ok(Self {
            config,
            listeners,
            endpoints,
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
        let current = ArcSwap::from_pointee(Snapshot::new(config, &listeners)?);
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(connector);
        Ok(Self {
            listeners,
            current,
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
        let snapshot = Snapshot::new(config, &self.listeners)?;
        self.current.store(Arc::new(snapshot));
        Ok(())
    }

    /// Serves the connections that come in on `socket` as those of the listener at
    /// position `listener` of [`Proxy::listeners`], HTTP/1.1 and HTTP/2 alike. Never
    /// returns; dropping the future stops accepting, and connections already accepted
    /// carry on.
    pub async fn serve(self: Arc<Self>, listener: usize, socket: TcpListener) {
        let server = auto::Builder::new(TokioExecutor::new());
        loop {
            let stream = match socket.accept().await {
                Ok((stream, _)) => stream,
                Err(error) => {
                    if !is_about_one_connection(&error) {
                        tokio::time::sleep(ACCEPT_PAUSE).await;
                    }
                    continue;
                }
            };
            // Worth having, not worth refusing a connection over.
            let _unset = stream.set_nodelay(true);

            // Every request clones a handle, as the engine wants futures that own what they
            // use. A handle of the connection's own keeps that count off a line of cache
            // that all the workers would otherwise write to.
            let connection = Arc::new(Arc::clone(&self));
            let server = server.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let connection = Arc::clone(&connection);
                    async move { Ok::<_, Infallible>(connection.handle(listener, request).await) }
                });
                // An error here is the end of one connection: the peer went away or spoke
                // nonsense. There is nobody to tell.
                let _closed = server.serve_connection(TokioIo::new(stream), service).await;
            });
        }
    }

    async fn handle(&self, listener: usize, request: Request<Incoming>) -> Response<Body> {
        let (mut head, body) = request.into_parts();
        let rule = match self.direct(listener, &mut head) {
            Ok(rule) => rule,
            Err(status) => return answer(status),
        };
        let response = match self.client.request(Request::from_parts(head, body)).await {
            Ok(response) => response,
            Err(_) => return answer(StatusCode::BAD_GATEWAY),
        };
        let (mut head, body) = response.into_parts();
        strip_response(&mut head.headers);
        if let Some(changes) = rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(&mut head.headers);
        }
        Response::from_parts(head, Either::Left(body))
    }

    /// Makes the head of a request that came in on a listener's socket the head of the
    /// request to send, target included, or says what to answer instead. All of it is done
    /// on one snapshot, which is let go of before anything is waited for; what is kept for
    /// the response is the rule, and only if it has something to do to the response.
    fn direct(
        &self,
        listener: usize,
        head: &mut Parts,
    ) -> Result<Option<Arc<CompiledRule>>, StatusCode> {
        let snapshot = self.current.load();
        let listener = snapshot
            .listeners
            .get(listener)
            .copied()
            .flatten()
            .and_then(|position| snapshot.config.listeners.get(position))
            .ok_or(StatusCode::NOT_FOUND)?;
        let forward = decide(&snapshot.config, listener, head, random())
            .map_err(|rejection| rejection.status())?;
        // An upstream the snapshot does not have is not known to happen.
        let endpoints = snapshot
            .endpoints
            .get(forward.upstream.0)
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let endpoint = pick(endpoints, random()).ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

        head.uri = at_endpoint(&head.uri, endpoint).ok_or(StatusCode::BAD_REQUEST)?;
        head.version = Version::HTTP_11;
        // What the engine attached to the request is about the connection it came in on.
        head.extensions.clear();
        let has_changes = forward.rule.response_headers.is_some();
        Ok(has_changes.then(|| Arc::clone(forward.rule)))
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

/// An answer of our own: a status and nothing else.
fn answer(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Either::Right(Empty::new()));
    *response.status_mut() = status;
    response
}

/// Whether a failure to accept is the failure of the one connection that was next in line,
/// so that the one after it can be accepted at once.
fn is_about_one_connection(error: &io::Error) -> bool {
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
        let listeners = |names: &[&str]| {
            Snapshot::new(config_with(names), &sockets)
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
    fn an_answer_of_our_own_is_a_status_and_nothing_else() {
        let answer = answer(StatusCode::NOT_FOUND);
        assert_eq!(answer.status(), 404);
        assert!(answer.headers().is_empty());
    }
}
