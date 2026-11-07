//! The control plane's own metrics ([08 §1](../../../docs/08-observability.md)): plain
//! HTTP/1.1, as the data plane serves its own, `GET` or `HEAD` [`PATH`] answered with the
//! latest text it is handed, in the Prometheus text format. This knows nothing of what the
//! text says.
//!
//! Anyone in the cluster can reach it, so what a client can make it hold is bounded, as the
//! data plane's scrape is: [`MOST`] connections at once, the next waiting in the backlog
//! until one ends, and each request's head within a deadline, the wait for the next one
//! on a connection kept open included.

use bytes::Bytes;
use http::header::{ALLOW, CONTENT_TYPE, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::convert::Infallible;
use std::pin::pin;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// Where the metrics are read.
pub const PATH: &str = "/metrics";

/// The most connections served at once: a scraper keeps one open, and a pair of them kept
/// for availability two, as the data plane's scrape counts them.
pub const MOST: usize = 10;

/// How long a request's head may take to come, the wait for it on a connection kept open
/// included: a scraper asks well within it, every 15 s to a minute, or connects again.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// The Prometheus text format, as its scrapers ask to be told.
const TEXT_FORMAT: HeaderValue =
    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");

/// Serves the metrics on `listener` until `stop`: each scrape answered with the latest of
/// `text`, each request's head within `deadline` ([`DEADLINE`] outside tests).
/// Connections still open end as it stops.
pub async fn serve(
    listener: TcpListener,
    text: watch::Receiver<Bytes>,
    deadline: Duration,
    stop: impl Future<Output = ()>,
) {
    let mut connections = JoinSet::new();
    let mut stop = pin!(stop);
    loop {
        let room = connections.len() < MOST;
        let socket = tokio::select! {
            () = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
            accepted = listener.accept(), if room => match accepted {
                Ok((socket, _)) => socket,
                // A connection that failed before it was taken: the next is taken as usual.
                Err(_) => continue,
            },
        };
        let _nodelay = socket.set_nodelay(true);
        let text = text.clone();
        connections.spawn(async move {
            let service = service_fn(move |request| {
                let answer = scrape(&request, &text);
                async move { Ok::<_, Infallible>(answer) }
            });
            // An error is the connection's end, whoever ended it.
            let _ended = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(deadline)
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
    }
}

/// The answer to one request: the text at [`PATH`], to `GET` or `HEAD`.
fn scrape(request: &Request<Incoming>, text: &watch::Receiver<Bytes>) -> Response<Full<Bytes>> {
    if request.uri().path() != PATH {
        return empty(StatusCode::NOT_FOUND);
    }
    if request.method() != Method::GET && request.method() != Method::HEAD {
        let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }
    let mut response = Response::new(Full::new(text.borrow().clone()));
    response.headers_mut().insert(CONTENT_TYPE, TEXT_FORMAT);
    response
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests;
