//! Closing a client's connection without resetting it.
//!
//! A socket closed while bytes the peer sent are still unread is reset rather than closed,
//! and a reset takes with it whatever the peer had received and not yet read: an answer
//! written just before the close can be lost on its way to the client's application. This
//! is what happens when the engine ends a connection whose request body it never read — an
//! upstream refused an upload with a 413 and `Connection: close`, say, while the client is
//! still sending ([13 §5](../../docs/13-http1-upstream.md)).
//!
//! So a connection is not simply dropped when the engine is done with it. Its sending half
//! is shut, and what the client still sends is read and thrown away until the client
//! closes, goes quiet, or has had long enough — nginx's lingering close, with nginx's
//! bounds. None of what is read is looked at: the engine has finished with the
//! connection, and nothing more on it is a request.

use bytes::{Buf, Bytes};
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::Instant;

/// How long a lingering connection may stay quiet before it is closed: nginx's
/// `lingering_timeout`.
pub(crate) const QUIET: Duration = Duration::from_secs(5);

/// How long a connection lingers at most, however much the client keeps sending: nginx's
/// `lingering_time`. A client cannot hold a closing connection open for longer.
pub(crate) const MOST: Duration = Duration::from_secs(30);

/// Where a lent connection comes back to once the engine lets go of it.
pub(crate) type Returned = Rc<Cell<Option<TcpStream>>>;

/// A connection lent to the engine, which comes back when the engine drops it.
///
/// The engine owns what it serves and has no way to hand it back, so this does it on the
/// engine's behalf: reads and writes go straight through, and dropping it puts the
/// connection where the lender can take it up again.
pub(crate) struct Lent {
    /// Always there until the drop; an `Option` only so that the drop can move it out.
    stream: Option<TcpStream>,
    back: Returned,
    /// Bytes the client sent that were read before the engine had the connection, to be
    /// read first: what came with a PROXY header (20 §3). Boxed, as only a listener that
    /// reads one has any, so that every other connection pays a word for it.
    ahead: Option<Box<Bytes>>,
}

impl std::fmt::Debug for Lent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lent")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl Lent {
    /// Lends `stream`, and says where it will come back to.
    pub(crate) fn new(stream: TcpStream) -> (Self, Returned) {
        let back = Rc::new(Cell::new(None));
        let lent = Self {
            stream: Some(stream),
            back: Rc::clone(&back),
            ahead: None,
        };
        (lent, back)
    }

    /// Has `bytes`, already read from the connection, read before anything more is.
    pub(crate) fn read_first(&mut self, bytes: Bytes) {
        self.ahead = (!bytes.is_empty()).then(|| Box::new(bytes));
    }

    /// The connection, while it is lent. Missing only after the drop has taken it, when
    /// nothing can call this, so the error is for a case that does not arise.
    fn stream(&mut self) -> io::Result<Pin<&mut TcpStream>> {
        self.stream
            .as_mut()
            .map(Pin::new)
            .ok_or_else(|| io::ErrorKind::NotConnected.into())
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.back.set(Some(stream));
        }
    }
}

impl AsyncRead for Lent {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(ahead) = &mut this.ahead {
            let given = ahead.len().min(buf.remaining());
            buf.put_slice(&ahead[..given]);
            ahead.advance(given);
            if ahead.is_empty() {
                this.ahead = None;
            }
            return Poll::Ready(Ok(()));
        }
        match this.stream() {
            Ok(stream) => stream.poll_read(cx, buf),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl AsyncWrite for Lent {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().stream() {
            Ok(stream) => stream.poll_write(cx, buf),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().stream() {
            Ok(stream) => stream.poll_write_vectored(cx, bufs),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.stream
            .as_ref()
            .is_some_and(TcpStream::is_write_vectored)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().stream() {
            Ok(stream) => stream.poll_flush(cx),
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().stream() {
            Ok(stream) => stream.poll_shutdown(cx),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

/// Closes `stream` without resetting it: shuts its sending half, then reads and discards
/// until the client closes, stays quiet for `quiet`, or `most` has gone by.
///
/// A client that has already closed, which is how most connections end, costs one read
/// that finds the end at once.
/// Closes `stream` abortively: a reset, sent at once, whatever was still unsent thrown away.
/// For a connection whose answer was cut after its head went, which an orderly close would
/// let its client take for the whole answer when the close is what ends it (13 §7).
pub(crate) fn reset(stream: TcpStream) {
    // A socket that will not take it is closed in order, which is no worse than lingering.
    let _abortive = stream.set_zero_linger();
    drop(stream);
}

pub(crate) async fn linger(mut stream: TcpStream, quiet: Duration, most: Duration) {
    // The engine has usually shut it already. Doing it again is harmless, and where it had
    // not, it is what tells the client the answer is over.
    let _unsent = stream.shutdown().await;
    let until = Instant::now() + most;
    loop {
        let wait = (Instant::now() + quiet).min(until);
        match tokio::time::timeout_at(wait, discard(&mut stream)).await {
            Ok(Ok(read)) if read > 0 => {}
            // Closed, failed, quiet for too long, or out of time.
            _ => return,
        }
    }
}

/// Reads what has arrived on `stream` and throws it away; says how much there was.
///
/// Into a buffer that lives only while a read is tried, not across the wait for one: a
/// buffer held across it would be in the connection's task for as long as the task lived,
/// as its largest state, whether or not the connection ever lingered (14 §3).
fn discard(stream: &mut TcpStream) -> impl Future<Output = io::Result<usize>> + '_ {
    poll_fn(move |context| {
        let mut discarded = [MaybeUninit::<u8>::uninit(); 4096];
        let mut read = ReadBuf::uninit(&mut discarded);
        Pin::new(&mut *stream)
            .poll_read(context, &mut read)
            .map_ok(|()| read.filled().len())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// A connected pair: the client's end, and the server's as it was accepted.
    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    /// Everything the client is sent, to the end, or the error that ended it instead.
    async fn received(client: &mut TcpStream) -> io::Result<Vec<u8>> {
        let mut all = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut all))
            .await
            .expect("the connection neither ended nor failed")?;
        Ok(all)
    }

    /// The case this is for: a server that answers without reading what the client sent,
    /// and then lets go. The client reads only once the close has certainly reached it,
    /// which is when a reset would already have thrown the answer away.
    #[tokio::test]
    async fn an_answer_sent_before_the_close_survives_bytes_left_unread() {
        let (mut client, mut server) = pair().await;
        client.write_all(&[b'x'; 64 * 1024]).await.unwrap();
        // Time for the bytes to be in the server's hands, unread: a close with nothing
        // waiting to be read is an ordinary close, and would prove nothing.
        tokio::time::sleep(Duration::from_millis(200)).await;
        server.write_all(b"refused").await.unwrap();
        let lingering = tokio::spawn(linger(server, QUIET, MOST));

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(received(&mut client).await.unwrap(), b"refused");
        drop(client);
        tokio::time::timeout(Duration::from_secs(5), lingering)
            .await
            .expect("a closed client let the lingering go")
            .unwrap();
    }

    /// A client that closes ends the lingering at once.
    #[tokio::test]
    async fn a_client_that_has_closed_is_not_waited_for() {
        let (client, server) = pair().await;
        drop(client);
        let started = Instant::now();
        linger(server, Duration::from_secs(5), Duration::from_secs(30)).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    /// A client that stops sending and never closes is let go once it has been quiet.
    #[tokio::test]
    async fn a_quiet_client_is_let_go_after_the_quiet() {
        let (_client, server) = pair().await;
        let quiet = Duration::from_millis(300);
        let started = Instant::now();
        linger(server, quiet, Duration::from_secs(30)).await;
        let took = started.elapsed();
        assert!(took >= quiet && took < Duration::from_secs(5), "{took:?}");
    }

    /// A client that never stops sending is let go at the bound, however busy it keeps.
    #[tokio::test]
    async fn a_client_that_keeps_sending_is_let_go_at_the_bound() {
        let (mut client, server) = pair().await;
        let _sending = tokio::spawn(async move {
            while client.write_all(b"more").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let most = Duration::from_secs(1);
        let started = Instant::now();
        linger(server, Duration::from_millis(500), most).await;
        let took = started.elapsed();
        assert!(took >= most && took < Duration::from_secs(5), "{took:?}");
    }

    /// A lingering close holds no buffer while it waits for the client (14 §3): as small as
    /// a lingering can be, and far smaller than the 4 KiB it reads into.
    #[tokio::test]
    async fn a_lingering_close_holds_no_buffer_while_it_waits() {
        let (_client, server) = pair().await;
        let lingering = linger(server, QUIET, MOST);
        let size = std::mem::size_of_val(&lingering);
        assert!(size < 1024, "{size} bytes");
    }

    /// Lent to something that drops it, the connection comes back, and reads and writes
    /// went through to it while it was away.
    #[tokio::test]
    async fn a_lent_connection_comes_back_when_it_is_dropped() {
        let (mut client, server) = pair().await;
        let (mut lent, back) = Lent::new(server);
        lent.write_all(b"out").await.unwrap();
        client.write_all(b"in").await.unwrap();
        let mut two = [0; 2];
        lent.read_exact(&mut two).await.unwrap();
        assert_eq!(&two, b"in");
        let mut three = [0; 3];
        client.read_exact(&mut three).await.unwrap();
        assert_eq!(&three, b"out");

        assert!(back.take().is_none(), "back before it was let go");
        drop(lent);
        assert!(back.take().is_some(), "not back once it was let go");
    }
}
