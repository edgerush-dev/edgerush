//! The control plane's readiness, for its readiness probe
//! ([04 §2](../../../docs/04-control-plane.md)): plain HTTP/1.1 on a listener of its own,
//! `GET` or `HEAD` [`PATH`] answered 200 while the replica is ready and 503 while not. This
//! knows nothing of what makes it ready.
//!
//! It is apart from the metrics, whose few connections at once anyone in the cluster could
//! fill, and with them every replica's probe. Here nothing bounds how many connections are
//! served at once, but each is one request, its head within a deadline, the probe's own
//! timeout, and its answer done within it too: a connection is held for no longer, so one
//! left idle cannot keep the probe out, and only a flood kept up without pause could.

use crate::accept;
use bytes::Bytes;
use http::header::{ALLOW, HeaderValue};
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

/// Where a readiness probe asks whether the replica is ready.
pub const PATH: &str = "/ready";

/// How long a connection may take, its request's head and its answer: a readiness probe's
/// own timeout, Kubernetes' default, which gives up on an answer later than that anyway.
pub const DEADLINE: Duration = Duration::from_secs(1);

/// Serves the replica's readiness on `listener` until `stop`: each probe answered as `ready`
/// is now, each connection one request, done within `deadline` ([`DEADLINE`] outside
/// tests). Connections still open end as it stops.
pub async fn serve(
    listener: TcpListener,
    ready: watch::Receiver<bool>,
    deadline: Duration,
    stop: impl Future<Output = ()>,
) {
    let mut connections = JoinSet::new();
    let mut stop = pin!(stop);
    loop {
        let socket = tokio::select! {
            () = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
            (socket, _) = accept::next(|| listener.accept()) => socket,
        };
        let _nodelay = socket.set_nodelay(true);
        let ready = ready.clone();
        connections.spawn(async move {
            let service = service_fn(move |request| {
                let answer = answer(&request, &ready);
                async move { Ok::<_, Infallible>(answer) }
            });
            let mut http1 = hyper::server::conn::http1::Builder::new();
            http1
                .timer(TokioTimer::new())
                .header_read_timeout(deadline)
                .keep_alive(false);
            // An error, or the deadline, is the connection's end, whoever ended it.
            let _ended = tokio::time::timeout(
                deadline,
                http1.serve_connection(TokioIo::new(socket), service),
            )
            .await;
        });
    }
}

/// The answer to one request: at [`PATH`], to `GET` or `HEAD`, 200 or 503 as the replica
/// is ready or not.
fn answer(request: &Request<Incoming>, ready: &watch::Receiver<bool>) -> Response<Full<Bytes>> {
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
    if *ready.borrow() {
        Response::new(Full::new(Bytes::from_static(b"ready\n")))
    } else {
        empty(StatusCode::SERVICE_UNAVAILABLE)
    }
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests;
