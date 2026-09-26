//! HTTP/0.9 over QUIC (`hq-interop`), which quic-interop-runner's cases other than HTTP/3
//! speak ([16 §8](../../../../../docs/16-http3.md)). Built into the interop image (the
//! `interop` feature) and into the tests, never into a release.
//!
//! A request is a client's bidirectional stream carrying `GET /path` and a line end; the
//! answer is the bytes of what the path names, then the stream's end. Each request goes
//! through the request core as a GET for that path, of the name the client asked for, and
//! the answer's body is written back as it is: no status, no fields. HTTP/0.9 has no way to
//! say that an answer failed, so a stream is reset as an HTTP/3 one would be, with HTTP/3's
//! codes, so that a cut answer is never taken for a whole one.

use crate::downstream::h1::connection::Answered;
use crate::downstream::h2::idle::Idle;
use crate::downstream::h3::body::IncomingH3;
use crate::downstream::h3::code;
use crate::downstream::h3::conn::{Slot, Stream};
use crate::downstream::h3::head::{self, RequestHead};
use crate::interim::Interim;
use crate::request_body::RequestBody;
use bytes::Bytes;
use http::{Request, Version};
use http_body::Body;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

/// The ALPN an `hq-interop` client asks for.
pub(crate) const ALPN: &[u8] = b"hq-interop";

/// The longest request line taken: a path, and nothing else, has no need of more.
const MOST_LINE: usize = 8 << 10;

/// A request read off a stream.
pub(crate) struct Asked {
    pub(crate) id: u64,
    pub(crate) head: RequestHead,
}

/// Why an answer was not sent whole.
enum Cut {
    /// The connection went, or the client stopped the stream: there is no one to tell.
    Gone,
    /// The answer's body failed.
    Failed,
    /// The client gave no room for longer than the stream's idle bound.
    Stalled,
}

/// Reads what an `hq-interop` connection's streams have, and says which requests are
/// whole. `lines` holds what came of each request line that has not ended yet; `streams`
/// has a slot made for each request found, as the HTTP/3 driver makes one.
pub(crate) fn requests(
    quic: &mut quiche::Connection,
    streams: &mut HashMap<u64, Slot>,
    lines: &mut HashMap<u64, Vec<u8>>,
    asked: &mut Vec<Asked>,
) {
    // With no name there is no authority, and every request is refused, as an HTTP/3 one
    // with neither `:authority` nor `Host` is.
    let authority = quic.server_name().unwrap_or_default().as_bytes().to_vec();
    let readable: Vec<u64> = quic.readable().collect();
    let mut buf = [0; 2048];
    for id in readable {
        // What comes after the line on a stream with a task, and anything on a stream the
        // client opened the other way, means nothing to HTTP/0.9: it is read and let go of.
        // A task's stream stops being read once the task ends (`Stream`), so a request is
        // never found on it twice.
        let is_request = id % 4 == 0 && !streams.contains_key(&id);
        while let Ok((read, fin)) = quic.stream_recv(id, &mut buf) {
            if !is_request {
                continue;
            }
            let line = lines.entry(id).or_default();
            line.extend_from_slice(&buf[..read]);
            let ended = line.iter().position(|&byte| byte == b'\n');
            if ended.is_none() && line.len() > MOST_LINE {
                lines.remove(&id);
                refuse(quic, id);
                break;
            }
            if ended.is_none() && !fin {
                continue;
            }
            let line = lines.remove(&id).unwrap_or_default();
            let line = &line[..ended.unwrap_or(line.len())];
            match head_of(line, &authority) {
                Some(head) => {
                    streams.insert(id, Slot::default());
                    asked.push(Asked { id, head });
                }
                None => refuse(quic, id),
            }
            break;
        }
    }
}

/// The request a line asks for: `GET`, a space and a path, then at most a carriage
/// return. Its head is the one an HTTP/3 request of the same fields has, held to the same
/// rules.
fn head_of(line: &[u8], authority: &[u8]) -> Option<RequestHead> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let path = line.strip_prefix(b"GET ")?;
    let fields: [(&[u8], &[u8]); 4] = [
        (b":method", b"GET"),
        (b":scheme", b"https"),
        (b":authority", authority),
        (b":path", path),
    ];
    head::request(&fields, MOST_LINE).ok()
}

/// Resets a stream whose request cannot be read, as HTTP/3 resets a malformed one.
fn refuse(quic: &mut quiche::Connection, id: u64) {
    // Fails only for a side already done.
    let _reset = quic.stream_shutdown(id, quiche::Shutdown::Write, code::MESSAGE_ERROR);
    let _stopped = quic.stream_shutdown(id, quiche::Shutdown::Read, code::MESSAGE_ERROR);
}

/// Answers one request with its answer's body, and nothing else.
pub(crate) async fn answer<R, F, B>(
    stream: Stream,
    head: RequestHead,
    respond: Rc<R>,
    idle: Duration,
) where
    R: Fn(Request<RequestBody>, Interim) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let interim = Interim::listened(false, Version::HTTP_3, true);
    let body = IncomingH3::new(Rc::clone(&stream.conn), stream.id, None, true, idle);
    let request = Request::from_parts(head.parts, RequestBody::H3(body));
    let answered = respond(request, interim).await;
    let (_, body) = answered.into_response().into_parts();
    let code = match send(&stream, body, &mut Idle::new(idle)).await {
        Ok(()) => return stream.answered(),
        // Reset as the stream goes, unless the client stopped it and quiche did.
        Err(Cut::Gone) => return,
        Err(Cut::Failed) => code::INTERNAL_ERROR,
        Err(Cut::Stalled) => code::REQUEST_CANCELLED,
    };
    stream.conn.with(|state| {
        // Fails only for a side already done.
        let _reset = state
            .quic
            .stream_shutdown(stream.id, quiche::Shutdown::Write, code);
    });
    stream.conn.stir();
}

/// Sends `body` on the stream, then the stream's end. Trailers have nowhere to go.
async fn send<B: Body<Data = Bytes>>(stream: &Stream, body: B, idle: &mut Idle) -> Result<(), Cut> {
    let mut body = pin!(body);
    while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        if let Ok(data) = frame.map_err(|_| Cut::Failed)?.into_data() {
            write(stream, &data, false, idle).await?;
        }
    }
    write(stream, &[], true, idle).await
}

/// Sends `data`, and the stream's end if `fin`, as the stream takes it.
async fn write(stream: &Stream, data: &[u8], fin: bool, idle: &mut Idle) -> Result<(), Cut> {
    let mut sent = 0;
    poll_fn(|cx| {
        loop {
            let written = stream.conn.with(|state| {
                if state.closed {
                    return Err(Cut::Gone);
                }
                match state.quic.stream_send(stream.id, &data[sent..], fin) {
                    Ok(written) => Ok(Some(written)),
                    Err(quiche::Error::Done) => Ok(None),
                    Err(_) => Err(Cut::Gone),
                }
            });
            match written {
                Err(cut) => return Poll::Ready(Err(cut)),
                Ok(Some(written)) => {
                    sent += written;
                    idle.moved();
                    stream.conn.stir();
                    // All of it, and its end if it ends the stream: quiche sets FIN only on
                    // a call that takes the last byte.
                    if sent == data.len() {
                        return Poll::Ready(Ok(()));
                    }
                    if written == 0 {
                        return wait(stream, idle, cx);
                    }
                }
                Ok(None) => return wait(stream, idle, cx),
            }
        }
    })
    .await
}

/// Waits for the driver to say the stream has room, for the idle bound at the most.
fn wait(stream: &Stream, idle: &mut Idle, cx: &mut Context<'_>) -> Poll<Result<(), Cut>> {
    stream.conn.with(|state| {
        if let Some(slot) = state.streams.get_mut(&stream.id) {
            slot.writer = Some(cx.waker().clone());
        }
    });
    match idle.waiting(cx) {
        Poll::Ready(()) => Poll::Ready(Err(Cut::Stalled)),
        Poll::Pending => Poll::Pending,
    }
}
