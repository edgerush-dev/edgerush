//! A TLS stream whose vectored writes go out as one record.
//!
//! tokio-boring writes a vectored write's first piece and no more, and each write is sealed
//! as a TLS record of its own and sent with a system call of its own: an answer queued as its
//! head and its body went as two records and two sends where plain TCP makes one `writev`.
//! Over loopback the second send cost more than all the rest of TLS did. So the pieces are
//! gathered, up to a record's worth, and written once, as NGINX gathers a chain into its
//! `ssl_buffer` and HAProxy writes one output buffer.
//!
//! A write TLS could not finish has its record sealed and waiting in BoringSSL, which takes
//! it only from the same buffer, offered again: the gathered bytes are kept until then, and
//! the next write finishes them before anything else is gathered.
//!
//! Once a write has finished, a buffer larger than [`KEPT`] is let go of: the stream lives
//! as long as its connection, idle between requests included, and an idle connection is to
//! hold no request-sized allocation ([14 §8](../../../docs/14-downstream-server.md)). A
//! smaller one is kept, so that small answers, nearly all of them, gather into the same
//! memory each time; a large answer pays one allocation a record, small beside its copy and
//! its sealing.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The most one TLS record carries.
const RECORD: usize = 16 * 1024;

/// The largest buffer kept from one write to the next: what a head and a small answer
/// gather into.
const KEPT: usize = 4 * 1024;

/// A TLS stream, `S`, written to a record at a time.
#[derive(Debug)]
pub(crate) struct Gathered<S> {
    inner: S,
    /// What the last gathered write was, kept while TLS has not finished it.
    gathered: Vec<u8>,
    /// Whether `gathered` holds a write TLS has not finished.
    unfinished: bool,
}

impl<S> Gathered<S> {
    /// `inner`, its writes gathered.
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            gathered: Vec::new(),
            unfinished: false,
        }
    }
}

impl<S: AsyncWrite + Unpin> Gathered<S> {
    /// Writes what is gathered, and says how much of it went.
    fn poll_gathered(&mut self, context: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let written =
            std::task::ready!(Pin::new(&mut self.inner).poll_write(context, &self.gathered));
        self.unfinished = false;
        if self.gathered.capacity() > KEPT {
            self.gathered = Vec::new();
        }
        Poll::Ready(written)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Gathered<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(context, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Gathered<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(context, &[IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let offered: usize = bufs.iter().map(|buf| buf.len()).sum();
        if this.unfinished {
            // The same bytes are offered again, as a caller does whose write did not go:
            // what went of them is what went of what it offers.
            if offered < this.gathered.len() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offered less than the unfinished write it follows",
                )));
            }
            return this.poll_gathered(context);
        }
        let mut pieces = bufs.iter().filter(|buf| !buf.is_empty());
        let Some(first) = pieces.next() else {
            return Poll::Ready(Ok(0));
        };
        // One piece, or one a record's size by itself, goes as it is: there is nothing to
        // gather it with, and copying it would be all cost.
        if first.len() >= RECORD || offered == first.len() {
            return Pin::new(&mut this.inner).poll_write(context, first);
        }
        this.gathered.clear();
        // Made once, at its size, rather than grown piece by piece.
        this.gathered.reserve(offered.min(RECORD));
        for piece in std::iter::once(first).chain(pieces) {
            let room = RECORD - this.gathered.len();
            if room == 0 {
                break;
            }
            this.gathered
                .extend_from_slice(piece.get(..room.min(piece.len())).unwrap_or_default());
        }
        this.unfinished = true;
        this.poll_gathered(context)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    /// A stream standing in for TLS: every write is a record, kept with where its bytes
    /// were, and it can be made to leave the next writes unfinished.
    #[derive(Default)]
    struct Records {
        written: Vec<Vec<u8>>,
        /// Where each write's bytes were, and how many were offered.
        offered: Vec<(usize, usize)>,
        /// How many writes to come are left unfinished.
        refuse: usize,
    }

    impl AsyncWrite for Records {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            this.offered.push((buf.as_ptr() as usize, buf.len()));
            if this.refuse > 0 {
                this.refuse -= 1;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            this.written.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn write(stream: &mut Gathered<Records>, pieces: &[&[u8]]) -> io::Result<usize> {
        let slices: Vec<IoSlice<'_>> = pieces.iter().map(|piece| IoSlice::new(piece)).collect();
        poll_fn(|context| Pin::new(&mut *stream).poll_write_vectored(context, &slices)).await
    }

    #[tokio::test]
    async fn a_head_and_its_body_go_as_one_record() {
        let mut stream = Gathered::new(Records::default());
        let written = write(&mut stream, &[b"HTTP/1.1 200 OK\r\n\r\n", b"", b"hello"]).await;
        assert_eq!(written.unwrap(), 24);
        assert_eq!(
            stream.inner.written,
            [b"HTTP/1.1 200 OK\r\n\r\nhello".to_vec()]
        );
    }

    /// Gathered up to a record's worth: what is past it is the next write's.
    #[tokio::test]
    async fn no_more_is_gathered_than_one_record_holds() {
        let mut stream = Gathered::new(Records::default());
        let head = vec![b'h'; 1000];
        let body = vec![b'b'; RECORD];
        let written = write(&mut stream, &[&head, &body]).await.unwrap();
        assert_eq!(written, RECORD);
        assert_eq!(stream.inner.written[0][..1000], head[..]);
        assert_eq!(stream.inner.written[0].len(), RECORD);
    }

    /// A piece that fills a record, or the only piece there is, goes from where it is.
    #[tokio::test]
    async fn a_piece_that_needs_nothing_gathered_is_not_copied() {
        let mut stream = Gathered::new(Records::default());
        let big = vec![b'x'; RECORD + 10];
        let written = write(&mut stream, &[&big, b"tail"]).await.unwrap();
        assert_eq!(written, RECORD + 10);
        assert_eq!(stream.inner.offered, [(big.as_ptr() as usize, big.len())]);
        let alone = b"only this".to_vec();
        write(&mut stream, &[&alone]).await.unwrap();
        assert_eq!(
            stream.inner.offered[1],
            (alone.as_ptr() as usize, alone.len())
        );
    }

    /// A write TLS could not finish is offered again from the same buffer and nothing
    /// else, however much more the caller has by then, as BoringSSL requires.
    #[tokio::test]
    async fn an_unfinished_write_is_offered_again_as_it_was() {
        let mut stream = Gathered::new(Records {
            refuse: 1,
            ..Records::default()
        });
        let slices = [IoSlice::new(b"head "), IoSlice::new(b"body")];
        let first = poll_fn(|context| {
            Poll::Ready(Pin::new(&mut stream).poll_write_vectored(context, &slices))
        })
        .await;
        assert!(first.is_pending());
        let more = [
            IoSlice::new(b"head "),
            IoSlice::new(b"body"),
            IoSlice::new(b" more"),
        ];
        let second =
            poll_fn(|context| Pin::new(&mut stream).poll_write_vectored(context, &more)).await;
        assert_eq!(second.unwrap(), 9);
        assert_eq!(stream.inner.written, [b"head body".to_vec()]);
        let [(first_at, first_len), (again_at, again_len)] = stream.inner.offered[..] else {
            panic!("{:?}", stream.inner.offered);
        };
        assert_eq!((first_at, first_len), (again_at, again_len));
        // And then it gathers afresh.
        write(&mut stream, &[b"next ", b"one"]).await.unwrap();
        assert_eq!(stream.inner.written[1], b"next one");
    }

    #[tokio::test]
    async fn offering_less_than_the_unfinished_write_is_refused() {
        let mut stream = Gathered::new(Records {
            refuse: 1,
            ..Records::default()
        });
        let slices = [IoSlice::new(b"head "), IoSlice::new(b"body")];
        let first = poll_fn(|context| {
            Poll::Ready(Pin::new(&mut stream).poll_write_vectored(context, &slices))
        })
        .await;
        assert!(first.is_pending());
        let error = write(&mut stream, &[b"head"]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    /// Once a large gathered write has gone, the connection holds none of it while it
    /// waits for its next request (14 §8): a head and a record's worth of body, as a large
    /// answer is written.
    #[tokio::test]
    async fn a_finished_record_leaves_no_buffer_behind() {
        let mut stream = Gathered::new(Records::default());
        let head = vec![b'h'; 200];
        let body = vec![b'b'; RECORD];
        let written = write(&mut stream, &[&head, &body]).await.unwrap();
        assert_eq!(written, RECORD);
        assert_eq!(
            stream.gathered.capacity(),
            0,
            "kept by an idle connection's gatherer after a finished record"
        );
    }

    /// A record TLS could not finish keeps its bytes until it is offered again and goes,
    /// and only then is its buffer let go of.
    #[tokio::test]
    async fn an_unfinished_record_is_let_go_of_once_it_goes() {
        let mut stream = Gathered::new(Records {
            refuse: 1,
            ..Records::default()
        });
        let head = vec![b'h'; 200];
        let body = vec![b'b'; RECORD];
        let slices = [IoSlice::new(&head), IoSlice::new(&body)];
        let first = poll_fn(|context| {
            Poll::Ready(Pin::new(&mut stream).poll_write_vectored(context, &slices))
        })
        .await;
        assert!(first.is_pending());
        assert_eq!(stream.gathered.len(), RECORD, "kept for TLS to finish");
        let again =
            poll_fn(|context| Pin::new(&mut stream).poll_write_vectored(context, &slices)).await;
        assert_eq!(again.unwrap(), RECORD);
        assert_eq!(stream.gathered.capacity(), 0);
    }

    /// A small answer's buffer is kept and gathered into again: small answers allocate
    /// nothing after the first.
    #[tokio::test]
    async fn a_small_answers_buffer_is_kept_for_the_next() {
        let mut stream = Gathered::new(Records::default());
        write(&mut stream, &[b"HTTP/1.1 200 OK\r\n\r\n", b"hello"])
            .await
            .unwrap();
        let kept = stream.gathered.as_ptr();
        assert!(stream.gathered.capacity() > 0);
        write(&mut stream, &[b"HTTP/1.1 200 OK\r\n\r\n", b"again"])
            .await
            .unwrap();
        assert_eq!(stream.gathered.as_ptr(), kept);
        assert_eq!(stream.inner.written[1], b"HTTP/1.1 200 OK\r\n\r\nagain");
    }
}
