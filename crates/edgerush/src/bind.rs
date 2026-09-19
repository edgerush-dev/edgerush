//! Opening the sockets that are listened on.

use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener};

/// Connections the kernel holds for us until they are accepted; tokio's own choice. The
/// kernel lowers it to its limit (`net.core.somaxconn`).
const BACKLOG: i32 = 1024;

/// Whether a socket has its address to itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Port {
    /// Nobody else listens there.
    Own,
    /// Every worker has a socket of its own on the one address (`SO_REUSEPORT`), and the
    /// kernel deals the connections out among them.
    Shared,
}

/// Listens on `address`, ready to be handed to the runtime. An IPv6 socket takes IPv4 as
/// well, so that `[::]` is every address whatever the host's default
/// (`net.ipv6.bindv6only`) says.
pub(crate) fn listen(address: SocketAddr, port: Port) -> io::Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    if address.is_ipv6() {
        socket.set_only_v6(false)?;
    }
    // As std and tokio do: a restart need not wait for the connections of the process
    // before it to leave TIME_WAIT. On Windows the option means something else — two
    // processes on one port — and is left alone.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    if port == Port::Shared {
        share(&socket)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(BACKLOG)?;
    Ok(socket.into())
}

#[cfg(unix)]
fn share(socket: &Socket) -> io::Result<()> {
    socket.set_reuse_port(true)
}

/// Only where the kernel deals connections out among the sockets of one port.
#[cfg(not(unix))]
fn share(_: &Socket) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "a port cannot be shared between sockets here (no SO_REUSEPORT)",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;

    fn port_of(socket: &TcpListener) -> u16 {
        socket.local_addr().unwrap().port()
    }

    #[test]
    fn every_address_is_ipv4_and_ipv6() {
        let socket = listen("[::]:0".parse().unwrap(), Port::Own).unwrap();
        let port = port_of(&socket);
        TcpStream::connect(("127.0.0.1", port)).unwrap();
        TcpStream::connect(("::1", port)).unwrap();
    }

    #[test]
    fn an_ipv4_address_is_listened_on_as_it_is() {
        let socket = listen("127.0.0.1:0".parse().unwrap(), Port::Own).unwrap();
        TcpStream::connect(("127.0.0.1", port_of(&socket))).unwrap();
    }

    #[test]
    fn a_port_that_is_taken_is_an_error() {
        let first = listen("127.0.0.1:0".parse().unwrap(), Port::Own).unwrap();
        let taken = first.local_addr().unwrap();
        assert_eq!(
            listen(taken, Port::Own).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_shared_port_has_a_socket_for_every_worker() {
        let first = listen("127.0.0.1:0".parse().unwrap(), Port::Shared).unwrap();
        let address = first.local_addr().unwrap();
        let second = listen(address, Port::Shared).unwrap();
        assert_eq!(second.local_addr().unwrap(), address);
        // One that wants the port to itself does not get in.
        assert_eq!(
            listen(address, Port::Own).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );

        // Every connection is given to one of the two, and none is lost.
        let clients: Vec<TcpStream> = (0..32)
            .map(|_| TcpStream::connect(address).unwrap())
            .collect();
        let mut accepted = 0;
        let waited = std::time::Instant::now();
        while accepted < clients.len() {
            assert!(waited.elapsed() < std::time::Duration::from_secs(10));
            for socket in [&first, &second] {
                match socket.accept() {
                    Ok(_) => accepted += 1,
                    Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
                }
            }
        }
    }

    #[cfg(not(unix))]
    #[test]
    fn a_port_cannot_be_shared_where_the_kernel_does_not_deal_connections_out() {
        let error = listen("127.0.0.1:0".parse().unwrap(), Port::Shared).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn the_socket_does_not_block_so_that_the_runtime_can_take_it() {
        let socket = listen("127.0.0.1:0".parse().unwrap(), Port::Own).unwrap();
        assert_eq!(
            socket.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
