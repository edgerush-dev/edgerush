//! A TCP connection opened to an endpoint, and whose doing it is when one cannot be
//! ([03 §6](../../../../docs/03-data-plane.md)).
//!
//! The socket is made apart from the connect, so that a failure to make one — the process
//! out of descriptors, the kernel out of buffers or memory — is known for the worker's own
//! shortage, which no endpoint caused and moving to another would not mend. A connect that
//! finds no local port free to the endpoint is the worker's too, but ports are counted per
//! destination, and another endpoint has ports of its own: it sets the endpoint aside, under
//! a name of its own. The rest — refused, reset, unreachable — is the endpoint's.

use crate::descriptors::{Descriptor, Descriptors};
use crate::upstream::destination::Aside;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpSocket, TcpStream};

/// Why a connection to an endpoint was not opened.
#[derive(Debug, thiserror::Error)]
pub enum Unconnected {
    /// The worker could not make a socket to connect with: its own shortage, not the
    /// endpoint's ([14 §8](../../../../docs/14-downstream-server.md)).
    #[error("no socket could be made to connect with: {0}")]
    Short(#[source] io::Error),
    /// No local port was free to the endpoint.
    #[error("no local port was free to the endpoint: {0}")]
    NoPort(#[source] io::Error),
    /// The endpoint's: refused, reset, unreachable, out of time, or a handshake that failed
    /// over the connection.
    #[error("the endpoint could not be connected to: {0}")]
    Endpoint(#[source] io::Error),
}

impl Unconnected {
    /// What the endpoint is set aside for, if it is, as far as the failure goes: a connect
    /// that got through and failed after, in a handshake, is not, whatever this says.
    pub fn aside(&self) -> Option<Aside> {
        match self {
            Self::Short(_) => None,
            Self::NoPort(_) => Some(Aside::NoPort),
            Self::Endpoint(_) => Some(Aside::Connect),
        }
    }
}

/// A socket of `address`'s family, for a connect.
///
/// # Errors
///
/// [`Unconnected::Short`], whatever the cause: nothing of the endpoint is asked yet.
pub fn socket(address: SocketAddr) -> Result<TcpSocket, Unconnected> {
    // A test's refusal is made as the operating system's would be, so that what is made of
    // it is what is made of a real one.
    let made = if refused_by_a_test() {
        Err(io::ErrorKind::OutOfMemory.into())
    } else if address.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    };
    made.map_err(Unconnected::Short)
}

/// Whose a failed connect is, `error` being what the connect said.
pub fn connect_failed(error: io::Error) -> Unconnected {
    match error.kind() {
        // EADDRNOTAVAIL, and EADDRINUSE from the port the connect picks itself.
        io::ErrorKind::AddrNotAvailable | io::ErrorKind::AddrInUse => Unconnected::NoPort(error),
        // ENOMEM, from the connect rather than the socket: the worker's all the same.
        io::ErrorKind::OutOfMemory => Unconnected::Short(error),
        _ => Unconnected::Endpoint(error),
    }
}

/// Connects to `address`.
///
/// # Errors
///
/// [`Unconnected`], saying whose the failure is.
pub async fn connect(address: SocketAddr) -> Result<TcpStream, Unconnected> {
    #[cfg(test)]
    if let Some(held) = HELD.with_borrow(|held| held.get(&address).copied()) {
        tokio::time::sleep(held).await;
    }
    socket(address)?
        .connect(address)
        .await
        .map_err(connect_failed)
}

/// The same, for a worker, counted against its share of the open files (03 §9): a socket
/// past the share, with no idle one to close for it, is not made, the worker's own
/// shortage.
///
/// # Errors
///
/// As [`connect`], and [`Unconnected::Short`] past the worker's share.
pub(crate) async fn connect_counted(
    address: SocketAddr,
    files: &Rc<Descriptors>,
) -> Result<Counted, Unconnected> {
    let Some(file) = files.take() else {
        return Err(Unconnected::Short(io::Error::other(
            "the worker's share of open files is spent",
        )));
    };
    let stream = connect(address).await?;
    Ok(Counted {
        stream,
        _file: file,
    })
}

/// A connection to an upstream, its file counted against its worker's share until it is
/// dropped.
#[derive(Debug)]
pub(crate) struct Counted {
    stream: TcpStream,
    _file: Descriptor,
}

impl Counted {
    /// `stream`, counted against no worker: a health probe's, which the process's reserve
    /// holds, or a test's.
    pub(crate) fn uncounted(stream: TcpStream) -> Self {
        Self {
            stream,
            _file: Descriptor::uncounted(),
        }
    }
}

impl std::ops::Deref for Counted {
    type Target = TcpStream;

    fn deref(&self) -> &TcpStream {
        &self.stream
    }
}

impl AsyncRead for Counted {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// Never, outside the tests.
#[cfg(not(test))]
const fn refused_by_a_test() -> bool {
    false
}

/// Whether a test has this thread short of sockets.
#[cfg(test)]
fn refused_by_a_test() -> bool {
    NO_SOCKETS.get()
}

#[cfg(test)]
thread_local! {
    /// Every socket this thread asks for is refused, as a process out of descriptors is.
    static NO_SOCKETS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Refuses every socket this thread asks for, or none, from now on: the worker's own
/// shortage, which no test can bring about for real without taking it from every test in
/// the process.
#[cfg(test)]
pub(crate) fn short_of_sockets(short: bool) {
    NO_SOCKETS.set(short);
}

#[cfg(test)]
thread_local! {
    /// How long a connect this thread makes to an address waits before it begins.
    static HELD: std::cell::RefCell<std::collections::HashMap<SocketAddr, std::time::Duration>> =
        std::cell::RefCell::default();
}

/// Has every connect this thread makes to `address` wait `held` before it begins, or none
/// from now on: an endpoint whose connects hang, as one whose node has gone does, which a
/// loopback cannot be made into.
#[cfg(test)]
pub(crate) fn hold_connects(address: SocketAddr, held: Option<std::time::Duration>) {
    HELD.with_borrow_mut(|holding| match held {
        Some(held) => holding.insert(address, held),
        None => holding.remove(&address),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connect_is_the_endpoints_but_for_ports_and_memory() {
        let whose = |kind: io::ErrorKind| connect_failed(kind.into());
        for kind in [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::TimedOut,
        ] {
            assert!(matches!(whose(kind), Unconnected::Endpoint(_)), "{kind:?}");
        }
        for kind in [io::ErrorKind::AddrNotAvailable, io::ErrorKind::AddrInUse] {
            assert!(matches!(whose(kind), Unconnected::NoPort(_)), "{kind:?}");
        }
        assert!(matches!(
            whose(io::ErrorKind::OutOfMemory),
            Unconnected::Short(_)
        ));
    }

    /// Ports are per destination: another endpoint has its own, so their running out sets
    /// this one aside, under a name of its own. The worker's own shortage of sockets does
    /// not.
    #[test]
    fn only_the_workers_own_shortage_sets_nothing_aside() {
        let error = || io::Error::from(io::ErrorKind::Other);
        assert_eq!(Unconnected::Short(error()).aside(), None);
        assert_eq!(Unconnected::NoPort(error()).aside(), Some(Aside::NoPort));
        assert_eq!(Unconnected::Endpoint(error()).aside(), Some(Aside::Connect));
    }

    #[tokio::test]
    async fn a_socket_that_cannot_be_made_is_the_workers_shortage() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        short_of_sockets(true);
        let refused = connect(address).await;
        short_of_sockets(false);
        assert!(matches!(refused, Err(Unconnected::Short(_))));
        assert!(connect(address).await.is_ok());
    }
}
