//! The scrape endpoint: what was counted, over HTTP, on a socket of its own.
//!
//! It is not a listener of the config: no route leads to it, nothing of what it answers is
//! counted as traffic, and a data plane that was given no socket for it serves none. It is
//! served by our own HTTP/1 server, held to the same bounds and deadlines as a worker's
//! connections, on blocks and an account of its own: scrapers speak HTTP/1.1, and no
//! worker's storage goes on them.
//!
//! It serves [`MOST`] connections at once, and its account holds what that many can need,
//! so that whoever reaches its address can take no more of the process's descriptors and
//! memory than that (08 §4).

use crate::downstream::h1::connection::{self as h1, Answered, STAGING};
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
use http_body::{Body, Frame, SizeHint};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// The Prometheus text format, as its scrapers ask to be told.
const TEXT_FORMAT: HeaderValue =
    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8");
const ALLOWED: HeaderValue = HeaderValue::from_static("GET, HEAD");

/// The most connections served at once. A scraper keeps one connection open and asks on it
/// again, and a pair of them kept for availability two. Past it, a connection waits in the
/// backlog until one ends, as a worker's do at its cap — HAProxy keeps its stats socket to
/// ten the same way — and the descriptors they hold come out of what the process keeps
/// back from its workers (03 §9).
pub(crate) const MOST: usize = 10;

/// The pieces the metrics text is handed to the server in. What the server has queued for
/// the socket is charged to the account until it has gone, and the text, made whole, would
/// be charged whole; in pieces it is charged a piece and the staging at a time, and the
/// text itself, made before it is asked for, is no more the account's than the answer an
/// upstream has made.
const PIECE: usize = 16 * 1024;

/// What one connection may hold of the account at most: a head of the largest size the
/// server reads, with the block a read past it takes, and an answer's staging with the
/// piece that goes over it (14 §8).
fn each(limits: &H1Limits) -> usize {
    limits.head + SMALL + STAGING + PIECE
}

/// The connections being served, and what wakes the accepting once one ends.
#[derive(Default)]
struct Open {
    count: Cell<usize>,
    ended: Notify,
}

/// One connection being served, counted until it is let go of, its lingering close
/// included: the descriptor is held until then.
struct Served(Rc<Open>);

impl Served {
    fn new(open: &Rc<Open>) -> Self {
        open.count.set(open.count.get() + 1);
        Self(Rc::clone(open))
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.0.count.set(self.0.count.get().saturating_sub(1));
        self.0.ended.notify_one();
    }
}

impl Proxy {
    /// Serves [`Proxy::metrics`] at `/metrics` to whoever connects to `socket`, and nothing
    /// else, to ten connections at once. Never returns; dropping the future stops
    /// accepting.
    ///
    /// # Panics
    ///
    /// Runs inside a `LocalSet`, where each connection's task goes: our server's
    /// connections are not `Send`.
    pub async fn serve_metrics(self: Arc<Self>, socket: TcpListener) {
        let limits = H1Limits::default();
        let blocks = Rc::new(RefCell::new(Blocks::new(
            Sizes::within(&limits, SMALL),
            Storage::new(MOST * each(&limits)),
        )));
        let settings = Rc::new(h1::Settings {
            limits: Rc::new(limits),
            bounds: Bounds::default(),
            budget: h1::Budget::default(),
        });
        // The scrapes' own deadlines, waited on for as long as this serves.
        let timers = Timers::new();
        let _timing = tokio::task::spawn_local(Rc::clone(&timers).run());
        let open = Rc::new(Open::default());
        loop {
            // One thread, so nothing ends between the look and the wait; and a connection
            // that ended with nobody waiting has left its wake behind.
            while open.count.get() >= MOST {
                open.ended.notified().await;
            }
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
            let settings = Rc::clone(&settings);
            let served = Served::new(&open);
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
                    &settings,
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
                // Its descriptor has gone: room for the next.
                drop(served);
            });
        }
    }

    fn scrape(&self, head: &RawHead) -> Response<Pieces> {
        if head.uri().path() != "/metrics" {
            return empty(StatusCode::NOT_FOUND);
        }
        if head.method() != Method::GET && head.method() != Method::HEAD {
            let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
            response.headers_mut().insert(ALLOW, ALLOWED);
            return response;
        }
        let mut response = Response::new(Pieces(Bytes::from(self.metrics())));
        response.headers_mut().insert(CONTENT_TYPE, TEXT_FORMAT);
        local(response)
    }
}

fn empty(status: StatusCode) -> Response<Pieces> {
    let mut response = Response::new(Pieces(Bytes::new()));
    *response.status_mut() = status;
    local(response)
}

/// Marks an answer as the data plane's own, so that its head is paid for from the
/// provision.
fn local(mut response: Response<Pieces>) -> Response<Pieces> {
    response.extensions_mut().insert(h1::Local);
    response
}

/// An answer's body, handed over [`PIECE`] at a time: each a slice of the one text, not a
/// copy of it.
struct Pieces(Bytes);

impl Body for Pieces {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let rest = &mut self.get_mut().0;
        if rest.is_empty() {
            return Poll::Ready(None);
        }
        let piece = rest.split_to(rest.len().min(PIECE));
        Poll::Ready(Some(Ok(Frame::data(piece))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    /// The text goes a piece at a time, every piece no larger than [`PIECE`], all of it and
    /// nothing else, and its length is known before the first piece, so the answer says it.
    #[test]
    fn the_metrics_text_is_handed_over_a_piece_at_a_time() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let text: Vec<u8> = (0..40 * 1024).map(|at| (at % 251) as u8).collect();
            let mut body = Pieces(Bytes::from(text.clone()));
            assert_eq!(body.size_hint().exact(), Some(40 * 1024));
            let mut pieces = Vec::new();
            while let Some(frame) = body.frame().await {
                pieces.push(frame.unwrap().into_data().unwrap());
            }
            let sizes: Vec<usize> = pieces.iter().map(Bytes::len).collect();
            assert_eq!(sizes, [PIECE, PIECE, 8 * 1024]);
            assert_eq!(pieces.concat(), text);
            assert!(body.is_end_stream());
        });
    }

    /// An answer with nothing in it ends at once.
    #[test]
    fn an_empty_answer_has_no_pieces() {
        let body = Pieces(Bytes::new());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    }
}
