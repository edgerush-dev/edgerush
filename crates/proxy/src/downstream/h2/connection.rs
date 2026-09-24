//! One HTTP/2 connection, served over h2 with EdgeRush's own settings
//! ([15 §3, step 2](../../../../../docs/15-http2-and-grpc.md)).
//!
//! The connection is driven by its accept loop, and every stream it accepts is a task of its
//! own on the worker: the request goes to the request core as any request does, and its
//! answer goes out through [`send_body`], within the room the client grants. A client that
//! resets a stream before it is answered takes the stream's exchange with it: the future
//! answering it is dropped where it stands, and with it whatever it held upstream.

use crate::downstream::h1::connection::Answered;
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h2::writer::{Outgoing, Responder, send_body};
use crate::request_body::RequestBody;
use crate::storage::Storage;
use bytes::Bytes;
use http::header::{DATE, HeaderValue};
use http::{Request, Response};
use http_body::Body;
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::rc::Rc;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite};

/// What an HTTP/2 connection is served with (15 §3).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    /// Streams a client may have open at once.
    pub(crate) streams: u32,
    /// What a client may send on one stream before it is given more.
    pub(crate) stream_window: u32,
    /// The same for the whole connection.
    pub(crate) connection_window: u32,
    /// The largest header list accepted, by RFC 9113's measure.
    pub(crate) header_list: u32,
    /// What h2 may hold of one stream's answer before it is written.
    pub(crate) send_buffer: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            streams: 100,
            stream_window: 1 << 20,
            connection_window: 16 << 20,
            header_list: 64 * 1024,
            send_buffer: 64 * 1024,
        }
    }
}

impl Settings {
    /// h2's server built with these, and every other bound set explicitly rather than
    /// left to h2's defaults, so that a new version of h2 cannot move them unseen.
    fn builder(&self) -> ::h2::server::Builder {
        let mut builder = ::h2::server::Builder::new();
        builder
            .max_concurrent_streams(self.streams)
            .initial_window_size(self.stream_window)
            .initial_connection_window_size(self.connection_window)
            .max_header_list_size(self.header_list)
            .header_table_size(4096)
            .max_frame_size(16_384)
            .max_send_buffer_size(self.send_buffer)
            .max_concurrent_reset_streams(50)
            .reset_stream_duration(std::time::Duration::from_secs(1))
            .max_pending_accept_reset_streams(20)
            .max_local_error_reset_streams(Some(1024));
        builder
    }
}

/// Serves an HTTP/2 connection, preface included, until it ends. Each request is handed to
/// `respond`, and `date` dates an answer that has no `Date` of its own.
pub(crate) async fn serve<S, R, F, B, D>(
    socket: S,
    settings: Settings,
    storage: Rc<Storage>,
    date: Rc<D>,
    respond: Rc<R>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Fn(Request<RequestBody>) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
{
    let Ok(mut connection) = settings.builder().handshake::<_, Outgoing>(socket).await else {
        return;
    };
    // Accepting is what drives the connection: it is polled for as long as the connection
    // lives, streams running beside it.
    while let Some(Ok((request, send))) = connection.accept().await {
        let (storage, date, respond) = (Rc::clone(&storage), Rc::clone(&date), Rc::clone(&respond));
        let _detached = tokio::task::spawn_local(async move {
            answer(request, Responder::new(send), &*respond, &storage, &*date).await;
        });
    }
}

/// Answers one stream.
async fn answer<R, F, B, D>(
    request: Request<::h2::RecvStream>,
    mut responder: Responder,
    respond: &R,
    storage: &Rc<Storage>,
    date: &D,
) where
    R: Fn(Request<RequestBody>) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    let request = request.map(|body| RequestBody::H2(IncomingH2::new(body)));
    let mut answering = std::pin::pin!(respond(request));
    let answered = poll_fn(|cx| {
        if responder.poll_reset(cx).is_ready() {
            return Poll::Ready(None);
        }
        answering.as_mut().poll(cx).map(Some)
    })
    .await;
    // Reset by the client: nothing is to be sent, and the exchange has gone.
    let Some(answered) = answered else {
        return;
    };
    let (mut head, body) = answered.into_response().into_parts();
    if !head.headers.contains_key(DATE)
        && let Ok(now) = HeaderValue::from_bytes(date().as_bytes())
    {
        head.headers.insert(DATE, now);
    }
    let end = body.is_end_stream();
    // A head h2 refuses is not sent, and the stream is reset when its responder goes.
    let Ok(mut stream) = responder.final_head(Response::from_parts(head, ()), end) else {
        return;
    };
    if !end {
        // However the sending ends, there is nobody left to tell: a reset stream or a
        // failed body has been reset already.
        let _sent = send_body(&mut stream, body, storage).await;
    }
}
