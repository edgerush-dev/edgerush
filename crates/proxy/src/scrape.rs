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
//! memory than that (08 §4). The text is rendered at most once in [`FRESH`] and shared by
//! the scrapes that start within it.

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
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::Instant;

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

/// How long a metrics text is kept after it was made: a scrape that starts within it is
/// given that text, not a render of its own. Scrapes that arrive together, a pair kept for
/// availability or several scrapers on one schedule, then hold one text between them, and
/// what any of them reads is never more than this old.
pub(crate) const FRESH: Duration = Duration::from_secs(1);

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

/// The newest metrics text and when it was made, kept for [`FRESH`] and then let go of, so
/// that a data plane nobody is scraping holds no copy.
#[derive(Default)]
struct Rendered(RefCell<Option<(Instant, Bytes)>>);

impl Rendered {
    /// The text made less than [`FRESH`] ago, or a new one from `render`, kept from now.
    ///
    /// # Panics
    ///
    /// Runs inside a `LocalSet`: the text is let go of by a task of its own.
    fn text(self: &Rc<Self>, render: impl FnOnce() -> Bytes) -> Bytes {
        let now = Instant::now();
        if let Some((made, text)) = &*self.0.borrow()
            && now < *made + FRESH
        {
            return text.clone();
        }
        let text = render();
        *self.0.borrow_mut() = Some((now, text.clone()));
        let kept = Rc::clone(self);
        tokio::task::spawn_local(async move {
            tokio::time::sleep_until(now + FRESH).await;
            let mut held = kept.0.borrow_mut();
            // A text made since is another's to let go of.
            if held.as_ref().is_some_and(|(made, _)| *made == now) {
                *held = None;
            }
        });
        text
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
        let rendered = Rc::new(Rendered::default());
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
            let rendered = Rc::clone(&rendered);
            let served = Served::new(&open);
            tokio::task::spawn_local(async move {
                // Dated as each answer is written: scrapes are too few for a worker's
                // cached date to be worth keeping here.
                let date = || HttpDate::from_unix(unix_now());
                let respond = |head: RawHead, _, _| {
                    let answer = Answered::Map(proxy.scrape(&head, &rendered));
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
                    // Its own answers are not counted, so nothing is owed them.
                    |_| {},
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

    fn scrape(&self, head: &RawHead, rendered: &Rc<Rendered>) -> Response<Pieces> {
        if head.uri().path() != "/metrics" {
            return empty(StatusCode::NOT_FOUND);
        }
        if head.method() != Method::GET && head.method() != Method::HEAD {
            let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
            response.headers_mut().insert(ALLOW, ALLOWED);
            return response;
        }
        let text = rendered.text(|| Bytes::from(self.metrics()));
        let mut response = Response::new(Pieces(text));
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

    /// Runs `test` on a clock that moves only when told, inside a `LocalSet`.
    fn paused(test: impl std::future::Future<Output = ()>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, test);
    }

    /// A scrape that starts less than [`FRESH`] after the last render is given that text,
    /// the same bytes and not a copy; one that starts at [`FRESH`] or later renders anew.
    #[test]
    fn a_render_is_shared_by_the_scrapes_that_start_within_a_second() {
        paused(async {
            let rendered = Rc::new(Rendered::default());
            let renders = Cell::new(0);
            let render = || {
                renders.set(renders.get() + 1);
                Bytes::from(format!("render {}", renders.get()))
            };
            let first = rendered.text(render);
            tokio::time::advance(FRESH - Duration::from_millis(1)).await;
            let shared = rendered.text(render);
            assert_eq!((shared.as_ptr(), renders.get()), (first.as_ptr(), 1));
            tokio::time::advance(Duration::from_millis(1)).await;
            assert_eq!(rendered.text(render), "render 2");
        });
    }

    /// A text is let go of [`FRESH`] after it was made, whether or not anyone scrapes
    /// again, so that a data plane nobody is scraping holds none.
    #[test]
    fn a_render_is_let_go_of_a_second_after_it_was_made() {
        paused(async {
            let rendered = Rc::new(Rendered::default());
            rendered.text(|| Bytes::from_static(b"text"));
            tokio::time::advance(FRESH - Duration::from_millis(1)).await;
            assert!(rendered.0.borrow().is_some());
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert!(rendered.0.borrow().is_none());
        });
    }

    /// The task letting go of an older text leaves a newer one alone. A newer text is made
    /// only once the older is due to go, and which of the two then comes first is the
    /// scheduler's; here the newer is put in place before the older's task runs.
    #[test]
    fn a_newer_render_outlives_the_older_ones_letting_go() {
        paused(async {
            let rendered = Rc::new(Rendered::default());
            rendered.text(|| Bytes::from_static(b"older"));
            tokio::time::advance(FRESH - Duration::from_millis(1)).await;
            *rendered.0.borrow_mut() = Some((Instant::now(), Bytes::from_static(b"newer")));
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            let held = rendered.0.borrow().as_ref().map(|(_, text)| text.clone());
            assert_eq!(held.as_deref(), Some(&b"newer"[..]));
        });
    }

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
