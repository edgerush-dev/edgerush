//! A TCP connection opened to an endpoint, and whose doing it is when one cannot be
//! ([03 §6](../../../../docs/03-data-plane.md)).
//!
//! The socket is made apart from the connect, so that a failure to make one — the process
//! out of descriptors, the kernel out of buffers or memory — is known for the worker's own
//! shortage, which no endpoint caused and moving to another would not mend. A connect that
//! finds no local port free to the endpoint is the worker's too, but ports are counted per
//! destination, and another endpoint has ports of its own: it sets the endpoint aside, under
//! a name of its own. The rest — refused, reset, unreachable — is the endpoint's.

use crate::upstream::destination::Aside;
use std::io;
use std::net::SocketAddr;
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
    socket(address)?
        .connect(address)
        .await
        .map_err(connect_failed)
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
