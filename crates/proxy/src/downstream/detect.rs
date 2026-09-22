//! Which HTTP a plaintext connection speaks, told from its first bytes
//! ([14 §7](../../../docs/14-downstream-server.md)).
//!
//! An HTTP/2 client with prior knowledge opens with a fixed 24-byte preface
//! ([RFC 9113 §3.4](https://www.rfc-editor.org/rfc/rfc9113.html#section-3.4)); anything
//! else is taken for HTTP/1. The bytes are compared as they arrive, so the first one that
//! differs settles it, and every byte read to decide is handed to the chosen engine before
//! anything more is read from the socket. The choice is made once: an engine that later
//! fails is never followed by another parser.
//!
//! No deadline is kept here. Detecting is part of the connection's first request, whose
//! deadline runs from accept and is not restarted by this.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// What an HTTP/2 client with prior knowledge sends first.
pub const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Which HTTP the connection speaks, as far as its first bytes say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP/1.0 or HTTP/1.1: anything that is not the HTTP/2 preface.
    Http1,
    /// HTTP/2 with prior knowledge.
    Http2,
}

/// What the bytes so far say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Judged {
    /// Settled.
    Is(Protocol),
    /// Everything so far is the start of the preface, and it is not all there yet.
    More,
}

/// Judges the first bytes of a connection. More than the preface is never looked at: it
/// is settled by then.
pub fn judge(seen: &[u8]) -> Judged {
    let compared = seen.len().min(PREFACE.len());
    if seen[..compared] != PREFACE[..compared] {
        Judged::Is(Protocol::Http1)
    } else if compared == PREFACE.len() {
        Judged::Is(Protocol::Http2)
    } else {
        Judged::More
    }
}

/// A connection with the bytes read to detect it put back in front: the engine reads them
/// first, as though nothing had been read, and then the connection itself.
#[derive(Debug)]
pub struct Replay<S> {
    prefix: [u8; PREFACE.len()],
    /// What of `prefix` is still to be handed over.
    start: usize,
    end: usize,
    inner: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for Replay<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.start < this.end {
            let given = (this.end - this.start).min(buf.remaining());
            buf.put_slice(&this.prefix[this.start..this.start + given]);
            this.start += given;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(context, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Replay<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(context, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

/// Reads from `socket` until its first bytes say which HTTP it speaks, and returns that
/// with the socket to hand to the engine, the bytes read put back in front.
///
/// A connection that closes before it has said anything is `None`: there is nothing to
/// serve. One that closes part way through what could still have been the preface is
/// HTTP/1, whose engine then sees the bytes it sent and the close, and ends it as HTTP/1
/// ends a connection that stops part way through a request.
///
/// # Errors
///
/// The socket's, if reading fails.
pub async fn detect<S: AsyncRead + Unpin>(
    mut socket: S,
) -> io::Result<Option<(Protocol, Replay<S>)>> {
    let mut prefix = [0; PREFACE.len()];
    let mut end = 0;
    let protocol = loop {
        // Never more than could still be the preface, so nothing past what settles it
        // is taken from the socket here.
        let read = socket.read(&mut prefix[end..]).await?;
        if read == 0 {
            if end == 0 {
                return Ok(None);
            }
            break Protocol::Http1;
        }
        end += read;
        if let Judged::Is(protocol) = judge(&prefix[..end]) {
            break protocol;
        }
    };
    Ok(Some((
        protocol,
        Replay {
            prefix,
            start: 0,
            end,
            inner: socket,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn every_start_of_the_preface_is_undecided_and_the_whole_of_it_is_http2() {
        assert_eq!(judge(b""), Judged::More);
        for end in 1..PREFACE.len() {
            assert_eq!(judge(&PREFACE[..end]), Judged::More, "{end}");
        }
        assert_eq!(judge(PREFACE), Judged::Is(Protocol::Http2));
        // What follows the preface is the connection's, not the detector's business.
        let mut more = PREFACE.to_vec();
        more.extend_from_slice(b"\0\0\x12\x04");
        assert_eq!(judge(&more), Judged::Is(Protocol::Http2));
    }

    /// A byte that differs settles it wherever it comes, the last included.
    #[test]
    fn a_difference_anywhere_is_http1_at_once() {
        for at in 0..PREFACE.len() {
            let mut bytes = PREFACE[..=at].to_vec();
            bytes[at] ^= 0x20;
            assert_eq!(judge(&bytes), Judged::Is(Protocol::Http1), "{at}");
        }
        assert_eq!(judge(b"GET / HTTP/1.1\r\n"), Judged::Is(Protocol::Http1));
        assert_eq!(judge(b"P"), Judged::More);
        assert_eq!(judge(b"PO"), Judged::Is(Protocol::Http1));
    }

    /// Reads what the engine would, to the end.
    async fn replayed<S: AsyncRead + Unpin>(mut replay: Replay<S>) -> Vec<u8> {
        let mut all = Vec::new();
        replay.read_to_end(&mut all).await.unwrap();
        all
    }

    /// However the bytes arrive, what is decided is the same and what the engine reads is
    /// every byte the client sent, in order, the ones read to decide included.
    #[tokio::test]
    async fn every_split_is_decided_alike_and_replayed_whole() {
        let mut h2 = PREFACE.to_vec();
        h2.extend_from_slice(b"frames");
        let cases: [(&[u8], Protocol); 4] = [
            (&h2, Protocol::Http2),
            (b"GET / HTTP/1.1\r\nhost: a\r\n\r\n", Protocol::Http1),
            (b"PRI * HTTP/1.1\r\n\r\n", Protocol::Http1),
            // A TLS ClientHello on a plaintext listener: HTTP/1 from its first byte, and
            // the HTTP/1 engine refuses it as it would any request line that is not one.
            (
                b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03",
                Protocol::Http1,
            ),
        ];
        for (sent, expected) in cases {
            for split in 1..=sent.len() {
                let (mut client, server) = tokio::io::duplex(64);
                let sending = sent.to_vec();
                let writer = tokio::spawn(async move {
                    for piece in sending.chunks(split) {
                        client.write_all(piece).await.unwrap();
                        tokio::task::yield_now().await;
                    }
                });
                let (protocol, replay) = detect(server).await.unwrap().unwrap();
                assert_eq!(protocol, expected, "{sent:?} in pieces of {split}");
                writer.await.unwrap();
                assert_eq!(
                    replayed(replay).await,
                    sent,
                    "{sent:?} in pieces of {split}"
                );
            }
        }
    }

    /// A connection that closes having said nothing has nothing to serve, and one that
    /// closes part way through what could have been the preface goes to HTTP/1 with what
    /// it sent.
    #[tokio::test]
    async fn a_close_before_it_is_decided() {
        let (client, server) = tokio::io::duplex(64);
        drop(client);
        assert!(detect(server).await.unwrap().is_none());

        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(b"PRI * HT").await.unwrap();
        drop(client);
        let (protocol, replay) = detect(server).await.unwrap().unwrap();
        assert_eq!(protocol, Protocol::Http1);
        assert_eq!(replayed(replay).await, b"PRI * HT");
    }

    /// Nothing past what settles it is read: the rest stays on the socket for the engine,
    /// however much the client has already sent.
    #[tokio::test]
    async fn nothing_past_the_preface_is_taken() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut sent = PREFACE.to_vec();
        sent.extend_from_slice(&[7; 100]);
        client.write_all(&sent).await.unwrap();
        let (_, replay) = detect(server).await.unwrap().unwrap();
        assert_eq!(replay.end, PREFACE.len());
        drop(client);
        assert_eq!(replayed(replay).await, sent);
    }
}
