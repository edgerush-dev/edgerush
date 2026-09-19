//! Serving: connections come in, requests go through the request core, and what it
//! forwards goes to an endpoint of the chosen upstream and comes back. This is the adapter
//! between the HTTP engine (hyper) and the core, and the only place that knows both.
//!
//! Bodies stream in both directions and are never held here. Upstream connections are
//! HTTP/1.1 from hyper-util's pool for now.

use crate::hop_by_hop::strip_response;
use crate::random::random;
use crate::request::decide;
use edgerush_config::Compiled;
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

/// A data plane for one compiled config: serves its listeners and forwards to its upstreams.
#[derive(Debug)]
pub struct Proxy {
    snapshot: Compiled,
    /// By position of the upstream, then of the endpoint: where to connect, in the form a
    /// request target takes, made once so that no request formats an address.
    endpoints: Vec<Vec<Authority>>,
    client: Client<HttpConnector, Incoming>,
}

impl Proxy {
    /// A data plane that runs `snapshot`. Nothing is listened on or connected to yet.
    ///
    /// # Errors
    ///
    /// Returns a [`ProxyError`] for an endpoint address that cannot be part of a request
    /// target (one with an IPv6 zone).
    pub fn new(snapshot: Compiled) -> Result<Self, ProxyError> {
        let endpoints = snapshot
            .upstreams
            .iter()
            .map(|upstream| upstream.endpoints.iter().map(authority).collect())
            .collect::<Result<_, _>>()?;
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(connector);
        Ok(Self {
            snapshot,
            endpoints,
            client,
        })
    }

    /// The config this data plane runs.
    #[must_use]
    pub fn snapshot(&self) -> &Compiled {
        &self.snapshot
    }

    /// Serves the connections that come in on `socket` as those of the listener at
    /// position `listener` of the snapshot, HTTP/1.1 and HTTP/2 alike. Never returns;
    /// dropping the future stops accepting, and connections already accepted carry on.
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
        let Some(listener) = self.snapshot.listeners.get(listener) else {
            return answer(StatusCode::INTERNAL_SERVER_ERROR);
        };
        let forward = match decide(&self.snapshot, listener, &mut head, random()) {
            Ok(forward) => forward,
            Err(rejection) => return answer(rejection.status()),
        };
        // An upstream the snapshot does not have is not known to happen.
        let Some(endpoints) = self.endpoints.get(forward.upstream.0) else {
            return answer(StatusCode::INTERNAL_SERVER_ERROR);
        };
        let Some(endpoint) = pick(endpoints, random()) else {
            return answer(StatusCode::SERVICE_UNAVAILABLE);
        };

        let Some(target) = at_endpoint(&head.uri, endpoint) else {
            return answer(StatusCode::BAD_REQUEST);
        };
        head.uri = target;
        head.version = Version::HTTP_11;
        // What the engine attached to the request is about the connection it came in on.
        head.extensions.clear();

        let response = match self.client.request(Request::from_parts(head, body)).await {
            Ok(response) => response,
            Err(_) => return answer(StatusCode::BAD_GATEWAY),
        };
        let (mut head, body) = response.into_parts();
        strip_response(&mut head.headers);
        if let Some(changes) = &forward.rule.response_headers {
            changes.apply(&mut head.headers);
        }
        Response::from_parts(head, Either::Left(body))
    }
}

/// Why a data plane cannot be made from a config.
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

    fn authorities(addresses: &[&str]) -> Vec<Authority> {
        addresses
            .iter()
            .map(|address| authority(&address.parse().unwrap()).unwrap())
            .collect()
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
