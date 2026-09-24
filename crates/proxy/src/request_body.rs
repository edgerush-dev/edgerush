//! A request's body, whichever engine read it off the client's connection.
//!
//! The request core and the upstream exchange take this, and nothing an engine defines:
//! what an engine hands over is wrapped where it hands it over, and what it reports going
//! wrong is sorted into [`RequestBodyError`] there too. There are three: hyper's HTTP/2
//! server, h2 used directly (15 step 1; the HTTP/2 server that replaces hyper's), and
//! EdgeRush's own HTTP/1 server, whose body cannot leave the worker and so makes this one
//! that cannot either ([14 §2](../../../docs/14-downstream-server.md)).

use crate::downstream::h1::connection::IncomingBody;
use crate::downstream::h2::body::IncomingH2;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use std::error::Error as StdError;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A request's body, as the engine that read it hands it over.
///
/// Named, and not a box or a trait object, for the reason the answer's body is: every
/// request would pay for that, and what is behind it could no longer say for itself whether
/// it has ended and how much of it is left. Those two are what decide how a body is sent on,
/// before a byte of it has been read, so each case passes them on as its engine gives them.
/// Data and trailers stay separate frames.
#[derive(Debug)]
pub(crate) enum RequestBody {
    /// Read by hyper's server, over HTTP/2.
    Hyper(Incoming),
    /// Read by EdgeRush's own HTTP/1 server, from the connection's own input.
    Ours(IncomingBody),
    /// Received by h2, used directly.
    #[expect(
        dead_code,
        reason = "made by the HTTP/2 driver of 15 step 2, not yet here"
    )]
    H2(IncomingH2),
}

/// What an engine reported going wrong, kept as the cause of a [`RequestBodyError`] without
/// its engine's type in the signature.
type Cause = Box<dyn StdError + Send + Sync>;

/// Why a request's body could not be read to its end.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestBodyError {
    /// The client stopped before the body's end: the connection closed short of the length
    /// it gave, or before the chunk that ends the body.
    #[error("the client stopped before the end of the request body")]
    Incomplete(#[source] Cause),
    /// The body broke its own framing: a chunk size or chunk line that cannot be read.
    #[error("the request body's framing could not be read")]
    Invalid(#[source] Cause),
    /// Anything else the engine reported, an HTTP/2 stream the client reset among them.
    #[error("the request body could not be read")]
    Other(#[source] Cause),
}

impl RequestBodyError {
    /// Sorts an error of hyper's by what it says about the body. hyper's server reports what
    /// its body decoder found as an I/O error of a kind that says which it was, under an
    /// error of its own, and says nothing more specific in public.
    fn from_hyper(error: hyper::Error) -> Self {
        let found = StdError::source(&error)
            .and_then(|cause| cause.downcast_ref::<io::Error>())
            .map(io::Error::kind);
        let cause = Box::new(error);
        match found {
            Some(io::ErrorKind::UnexpectedEof) => Self::Incomplete(cause),
            Some(io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput) => Self::Invalid(cause),
            _ => Self::Other(cause),
        }
    }

    /// Sorts an error of h2's: a connection that ended under the stream cut its body short;
    /// anything else, a reset above all, is the stream failing.
    pub(crate) fn from_h2(error: ::h2::Error) -> Self {
        if error.is_io() {
            Self::Incomplete(Box::new(error))
        } else {
            Self::Other(Box::new(error))
        }
    }
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = RequestBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RequestBodyError>>> {
        match self.get_mut() {
            Self::Hyper(incoming) => Pin::new(incoming)
                .poll_frame(cx)
                .map_err(RequestBodyError::from_hyper),
            Self::Ours(body) => Pin::new(body).poll_frame(cx),
            Self::H2(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Hyper(incoming) => incoming.is_end_stream(),
            Self::Ours(body) => body.is_end_stream(),
            Self::H2(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Hyper(incoming) => incoming.size_hint(),
            Self::Ours(body) => body.size_hint(),
            Self::H2(body) => body.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Each case of [`RequestBody`] read as its engine serves it: bytes a client wrote, over
    //! an in-memory connection, to the engine's own server, whose service wraps the body the
    //! way the data plane does and reads it to its end.

    use super::*;
    use http::{HeaderMap, Request, Response};
    use http_body_util::{BodyExt, Empty};
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;

    /// What a service saw of a body: its own account of itself before it was read, then
    /// what reading it gave.
    #[derive(Debug, Default, PartialEq)]
    struct Seen {
        ended_before_read: bool,
        exact_before_read: Option<u64>,
        data: Vec<u8>,
        trailers: Option<HeaderMap>,
        failed: Option<&'static str>,
    }

    async fn seen(body: RequestBody) -> Seen {
        let mut body = body;
        let mut seen = Seen {
            ended_before_read: body.is_end_stream(),
            exact_before_read: body.size_hint().exact(),
            ..Seen::default()
        };
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(data) => seen.data.extend_from_slice(&data),
                    Err(frame) => seen.trailers = frame.into_trailers().ok(),
                },
                Err(error) => {
                    seen.failed = Some(match error {
                        RequestBodyError::Incomplete(_) => "incomplete",
                        RequestBodyError::Invalid(_) => "invalid",
                        RequestBodyError::Other(_) => "other",
                    });
                    break;
                }
            }
        }
        seen
    }

    /// Reads a request's body as [`RequestBody`], says what it saw, and answers.
    async fn reading(
        request: Request<Incoming>,
        told: mpsc::UnboundedSender<Seen>,
    ) -> Result<Response<Empty<Bytes>>, Infallible> {
        let seen = seen(RequestBody::Hyper(request.into_body())).await;
        let _listening = told.send(seen);
        Ok(Response::new(Empty::new()))
    }

    /// What hyper's HTTP/1 server hands over for `written`, after which the client closes
    /// its side.
    async fn over_http1(written: &[u8]) -> Seen {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (told, mut hear) = mpsc::unbounded_channel();
        let service = service_fn(move |request| reading(request, told.clone()));
        let serving = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server), service);
        let writing = async move {
            let mut client = client;
            client.write_all(written).await.unwrap();
            // Held open long enough for the server to read what came, then closed: the
            // close is what makes a body cut short into one that ended early.
            tokio::time::sleep(Duration::from_millis(50)).await;
            client.shutdown().await.unwrap();
            drop(client);
        };
        let (_served, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(serving, writing)
        })
        .await
        .expect("the server never finished");
        hear.try_recv().expect("the service never saw the request")
    }

    /// A request body a test script hands out frame by frame, or fails part way through.
    struct Scripted(VecDeque<Result<Frame<Bytes>, &'static str>>);

    impl Body for Scripted {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, &'static str>>> {
            Poll::Ready(self.get_mut().0.pop_front())
        }
    }

    /// Where the server's stream futures go: this thread, as on a worker, because a body
    /// that may be one of EdgeRush's own server's cannot go anywhere else.
    #[derive(Clone, Copy)]
    struct OnThisThread;

    impl<F: std::future::Future<Output = ()> + 'static> hyper::rt::Executor<F> for OnThisThread {
        fn execute(&self, future: F) {
            let _detached = tokio::task::spawn_local(future);
        }
    }

    /// What hyper's HTTP/2 server hands over for a POST whose body is `frames`.
    async fn over_http2(frames: Vec<Result<Frame<Bytes>, &'static str>>) -> Seen {
        tokio::task::LocalSet::new()
            .run_until(over_http2_here(frames))
            .await
    }

    async fn over_http2_here(frames: Vec<Result<Frame<Bytes>, &'static str>>) -> Seen {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (told, mut hear) = mpsc::unbounded_channel();
        let service = service_fn(move |request| reading(request, told.clone()));
        let _serving = tokio::task::spawn_local(
            hyper::server::conn::http2::Builder::new(OnThisThread)
                .serve_connection(TokioIo::new(server), service),
        );
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client))
                .await
                .unwrap();
        let _driving = tokio::spawn(connection);
        let request = Request::post("http://example.test/")
            .body(Scripted(frames.into()))
            .unwrap();
        // Whether the client is answered does not matter: a body it failed may be reset
        // before the answer comes.
        let _answered = sender.send_request(request).await;
        tokio::time::timeout(Duration::from_secs(5), hear.recv())
            .await
            .expect("the service never finished")
            .expect("the service never saw the request")
    }

    fn fields(named: &[(&'static str, &'static str)]) -> HeaderMap {
        named
            .iter()
            .map(|(name, value)| (name.parse().unwrap(), value.parse().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn a_counted_body_says_its_length_and_ends_with_it() {
        let seen =
            over_http1(b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 5\r\n\r\nhello").await;
        assert_eq!(
            seen,
            Seen {
                ended_before_read: false,
                exact_before_read: Some(5),
                data: b"hello".to_vec(),
                ..Seen::default()
            }
        );
    }

    /// No body is one that has ended before anything was read, which is what has it sent on
    /// with no framing at all.
    #[tokio::test]
    async fn no_body_has_ended_before_it_is_read() {
        let seen = over_http1(b"GET / HTTP/1.1\r\nhost: a\r\n\r\n").await;
        assert_eq!(
            seen,
            Seen {
                ended_before_read: true,
                exact_before_read: Some(0),
                ..Seen::default()
            }
        );
    }

    #[tokio::test]
    async fn a_chunked_body_keeps_its_trailers_apart_from_its_data() {
        let seen = over_http1(
            b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\n\
              5\r\nhello\r\n0\r\nx-sum: 7\r\n\r\n",
        )
        .await;
        assert_eq!(
            seen,
            Seen {
                ended_before_read: false,
                exact_before_read: None,
                data: b"hello".to_vec(),
                trailers: Some(fields(&[("x-sum", "7")])),
                failed: None,
            }
        );
    }

    /// A body cut short is an error that says so, never a body that looks finished.
    #[tokio::test]
    async fn a_body_cut_short_is_incomplete() {
        for written in [
            &b"POST / HTTP/1.1\r\nhost: a\r\ncontent-length: 10\r\n\r\nshort"[..],
            b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhel",
        ] {
            let seen = over_http1(written).await;
            assert_eq!(
                seen.failed,
                Some("incomplete"),
                "{:?}: {seen:?}",
                String::from_utf8_lossy(written)
            );
        }
    }

    #[tokio::test]
    async fn a_chunk_that_cannot_be_read_is_invalid() {
        let seen = over_http1(
            b"POST / HTTP/1.1\r\nhost: a\r\ntransfer-encoding: chunked\r\n\r\nzz\r\nhello\r\n",
        )
        .await;
        assert_eq!(seen.failed, Some("invalid"), "{seen:?}");
    }

    /// Over HTTP/2 the trailers are a frame of their own after the data, and a length is
    /// never promised, because trailers may follow any frame.
    #[tokio::test]
    async fn an_http2_body_keeps_its_trailers_apart_from_its_data() {
        let seen = over_http2(vec![
            Ok(Frame::data(Bytes::from_static(b"hello"))),
            Ok(Frame::trailers(fields(&[("x-sum", "7")]))),
        ])
        .await;
        assert_eq!(seen.data, b"hello", "{seen:?}");
        assert_eq!(seen.trailers, Some(fields(&[("x-sum", "7")])), "{seen:?}");
        assert_eq!(seen.failed, None, "{seen:?}");
    }

    /// A client whose body fails resets its stream, and the reset reaches the body as an
    /// error rather than as an end.
    #[tokio::test]
    async fn an_http2_stream_the_client_reset_fails_the_body() {
        let seen = over_http2(vec![
            Ok(Frame::data(Bytes::from_static(b"hel"))),
            Err("the client gave up"),
        ])
        .await;
        assert_eq!(seen.failed, Some("other"), "{seen:?}");
    }
}
