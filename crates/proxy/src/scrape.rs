//! The scrape endpoint: what was counted, over HTTP, on a socket of its own.
//!
//! It is not a listener of the config: no route leads to it, nothing of what it answers is
//! counted as traffic, and a data plane that was given no socket for it serves none. It is
//! served by our own HTTP/1 server, held to the same bounds and deadlines as a worker's
//! connections, on blocks and an account of its own: scrapers speak HTTP/1.1, and no
//! worker's storage goes on them.

use crate::downstream::h1::connection::{self as h1, Answered};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h1::deadlines::Bounds;
use crate::head::Head;
use crate::linger::linger;
use crate::raw::RawHead;
use crate::serve::{ACCEPT_PAUSE, Proxy, is_about_one_connection, unix_now};
use crate::slots::Slots;
use crate::storage::Storage;
use crate::timers::Timers;
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, SMALL, Sizes};
use bytes::Bytes;
use http::header::{ALLOW, CONTENT_TYPE};
use http::{HeaderValue, Method, Response, StatusCode};
use http_body_util::Full;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio::net::TcpListener;

/// The Prometheus text format, as its scrapers ask to be told.
const TEXT_FORMAT: HeaderValue =
    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");
const ALLOWED: HeaderValue = HeaderValue::from_static("GET, HEAD");

impl Proxy {
    /// Serves [`Proxy::metrics`] at `/metrics` to whoever connects to `socket`, and nothing
    /// else. Never returns; dropping the future stops accepting.
    ///
    /// # Panics
    ///
    /// Runs inside a `LocalSet`, where each connection's task goes: our server's
    /// connections are not `Send`.
    pub async fn serve_metrics(self: Arc<Self>, socket: TcpListener) {
        let limits = H1Limits::default();
        let blocks = Rc::new(RefCell::new(Blocks::new(
            Sizes::within(&limits, SMALL),
            Storage::new(limits.storage),
        )));
        let settings = h1::Settings {
            limits,
            bounds: Bounds::default(),
            budget: h1::Budget::default(),
        };
        // The scrapes' own deadlines, waited on for as long as this serves.
        let timers = Timers::new();
        let _timing = tokio::task::spawn_local(Rc::clone(&timers).run());
        loop {
            let mut stream = match socket.accept().await {
                Ok((stream, _)) => stream,
                Err(error) => {
                    if !is_about_one_connection(&error) {
                        tokio::time::sleep(ACCEPT_PAUSE).await;
                    }
                    continue;
                }
            };
            let proxy = Arc::clone(&self);
            let blocks = Rc::clone(&blocks);
            let timers = Rc::clone(&timers);
            tokio::task::spawn_local(async move {
                // Dated as each answer is written: scrapes are too few for a worker's
                // cached date to be worth keeping here.
                let date = || HttpDate::from_unix(unix_now());
                let respond = |head: RawHead, _, _| {
                    let answer = Answered::Map(proxy.scrape(&head));
                    async move { answer }
                };
                // How it ended is the scraper's business; the socket is closed either way.
                // Scraping goes on while the data plane drains: that is when it is watched.
                let never = crate::drain::Drain::default();
                // Scrapes are too few for slots of the worker's to be worth keeping.
                let slots = Slots::default();
                let _ended = h1::serve(
                    &mut stream,
                    settings,
                    blocks,
                    timers,
                    date,
                    &never,
                    respond,
                    &slots,
                )
                .await;
                // Closed without a reset, so that one taking an answer with it is not
                // possible even with a request body left unread.
                linger(
                    stream,
                    settings.bounds.linger_quiet,
                    settings.bounds.linger_most,
                )
                .await;
            });
        }
    }

    fn scrape(&self, head: &RawHead) -> Response<Full<Bytes>> {
        if head.uri().path() != "/metrics" {
            return empty(StatusCode::NOT_FOUND);
        }
        if head.method() != Method::GET && head.method() != Method::HEAD {
            let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
            response.headers_mut().insert(ALLOW, ALLOWED);
            return response;
        }
        let mut response = Response::new(Full::new(Bytes::from(self.metrics())));
        response.headers_mut().insert(CONTENT_TYPE, TEXT_FORMAT);
        local(response)
    }
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::default());
    *response.status_mut() = status;
    local(response)
}

/// Marks an answer as the data plane's own, so that its head is paid for from the
/// provision.
fn local(mut response: Response<Full<Bytes>>) -> Response<Full<Bytes>> {
    response.extensions_mut().insert(h1::Local);
    response
}
