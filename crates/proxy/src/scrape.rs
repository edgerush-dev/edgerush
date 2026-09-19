//! The scrape endpoint: what was counted, over HTTP, on a socket of its own.
//!
//! It is not a listener of the config: no route leads to it, nothing of what it answers is
//! counted as traffic, and a data plane that was given no socket for it serves none.

use crate::serve::{ACCEPT_PAUSE, Proxy, is_about_one_connection};
use http::header::{ALLOW, CONTENT_TYPE};
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::net::TcpListener;

/// The Prometheus text format, as its scrapers ask to be told.
const TEXT_FORMAT: HeaderValue =
    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");
const ALLOWED: HeaderValue = HeaderValue::from_static("GET, HEAD");

impl Proxy {
    /// Serves [`Proxy::metrics`] at `/metrics` to whoever connects to `socket`, and nothing
    /// else. Never returns; dropping the future stops accepting.
    pub async fn serve_metrics(self: Arc<Self>, socket: TcpListener) {
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
            let proxy = Arc::clone(&self);
            let server = server.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let response = proxy.scrape(&request);
                    async move { Ok::<_, Infallible>(response) }
                });
                // The end of one scraper's connection, however it came.
                let _closed = server.serve_connection(TokioIo::new(stream), service).await;
            });
        }
    }

    fn scrape(&self, request: &Request<Incoming>) -> Response<Full<Bytes>> {
        if request.uri().path() != "/metrics" {
            return empty(StatusCode::NOT_FOUND);
        }
        if request.method() != Method::GET && request.method() != Method::HEAD {
            let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
            response.headers_mut().insert(ALLOW, ALLOWED);
            return response;
        }
        let mut response = Response::new(Full::new(Bytes::from(self.metrics())));
        response.headers_mut().insert(CONTENT_TYPE, TEXT_FORMAT);
        response
    }
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::default());
    *response.status_mut() = status;
    response
}
