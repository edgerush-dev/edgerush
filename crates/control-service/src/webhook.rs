//! The admission webhook's listener ([04 §3](../../../docs/04-control-plane.md)): HTTPS,
//! its client asked for no certificate, as the API server presents none unless it is set up
//! to, over HTTP/2 or HTTP/1.1. Each review, a `POST` to [`PATH`], is handed to the caller
//! to answer: this knows nothing of what a review says.
//!
//! Anyone in the cluster can reach it, so what a client can make it hold is bounded: a
//! handshake within the stream's time, then [`Bounds`]. Whatever goes wrong with one
//! connection is that connection's alone, and said to no one.

use crate::server::HANDSHAKE;
use boring::ssl::SslAcceptor;
use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::convert::Infallible;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// Where reviews are sent: the path the webhook's registration names.
pub const PATH: &str = "/validate";

/// What a request may make the webhook hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// The most its body may hold.
    pub most: usize,
    /// How long its head, and then its body, may take to come.
    pub deadline: Duration,
}

impl Default for Bounds {
    /// 4 MiB, far more than a review of an Ingress holds, and the API server takes a
    /// refusal to read more as an outage, which admits the object; and 10 s, the API
    /// server's own default wait for a webhook.
    fn default() -> Self {
        Self {
            most: 4 << 20,
            deadline: Duration::from_secs(10),
        }
    }
}

/// What a review is answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// 200 with the review's answer, in JSON, or why there is none.
    pub status: StatusCode,
    /// The review's answer, or what is wrong, in plain text.
    pub body: Vec<u8>,
}

type Answers = Arc<dyn Fn(&[u8]) -> Answer + Send + Sync>;

/// Serves the webhook on `listener` until `stop`: the body of each review goes to `answer`,
/// each request held to `bounds`. Each connection takes the latest of `tls`, so that a
/// certificate replaced serves the next. Connections still open end as it stops.
pub async fn serve(
    listener: TcpListener,
    tls: watch::Receiver<Arc<SslAcceptor>>,
    bounds: Bounds,
    answer: impl Fn(&[u8]) -> Answer + Send + Sync + 'static,
    stop: impl Future<Output = ()>,
) {
    let answer: Answers = Arc::new(answer);
    let mut connections = JoinSet::new();
    let mut stop = pin!(stop);
    loop {
        let socket = tokio::select! {
            () = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => socket,
                // A connection that failed before it was taken: the next is taken as usual.
                Err(_) => continue,
            },
        };
        let _nodelay = socket.set_nodelay(true);
        let acceptor = tls.borrow().clone();
        let answer = Arc::clone(&answer);
        connections.spawn(async move {
            let handshake =
                tokio::time::timeout(HANDSHAKE, tokio_boring::accept(&acceptor, socket));
            let Ok(Ok(stream)) = handshake.await else {
                return;
            };
            let h2 = stream.ssl().selected_alpn_protocol() == Some(b"h2");
            let io = TokioIo::new(stream);
            let service = service_fn(move |request| respond(Arc::clone(&answer), bounds, request));
            // An error is the connection's end, whoever ended it.
            if h2 {
                let _ended = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            } else {
                let _ended = hyper::server::conn::http1::Builder::new()
                    .timer(TokioTimer::new())
                    .header_read_timeout(bounds.deadline)
                    .serve_connection(io, service)
                    .await;
            }
        });
    }
}

/// The answer to one request.
async fn respond(
    answer: Answers,
    bounds: Bounds,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.uri().path() != PATH {
        return Ok(plain(StatusCode::NOT_FOUND, "not the webhook's path"));
    }
    if request.method() != Method::POST {
        return Ok(plain(StatusCode::METHOD_NOT_ALLOWED, "a review is POSTed"));
    }
    let body = Limited::new(request.into_body(), bounds.most).collect();
    let body = match tokio::time::timeout(bounds.deadline, body).await {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(error)) if error.is::<LengthLimitError>() => {
            return Ok(plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "more than a review holds",
            ));
        }
        Ok(Err(_)) => return Ok(plain(StatusCode::BAD_REQUEST, "the body was cut short")),
        Err(_) => return Ok(plain(StatusCode::REQUEST_TIMEOUT, "the body took too long")),
    };
    let answered = answer(&body);
    let kind = if answered.status == StatusCode::OK {
        "application/json"
    } else {
        "text/plain; charset=utf-8"
    };
    Ok(response(answered.status, kind, answered.body))
}

fn plain(status: StatusCode, said: &'static str) -> Response<Full<Bytes>> {
    response(
        status,
        "text/plain; charset=utf-8",
        said.as_bytes().to_vec(),
    )
}

fn response(status: StatusCode, kind: &'static str, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(kind));
    response
}

#[cfg(test)]
mod tests;
