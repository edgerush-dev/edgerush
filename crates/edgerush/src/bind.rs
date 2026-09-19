//! Opening the sockets that are listened on.

use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener};

/// Connections the kernel holds for us until they are accepted; tokio's own choice. The
/// kernel lowers it to its limit (`net.core.somaxconn`).
const BACKLOG: i32 = 1024;

/// Listens on `address`, ready to be handed to the runtime. An IPv6 socket takes IPv4 as
/// well, so that `[::]` is every address whatever the host's default
/// (`net.ipv6.bindv6only`) says.
pub(crate) fn listen(address: SocketAddr) -> io::Result<TcpListener> {
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
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(BACKLOG)?;
    Ok(socket.into())
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
        let socket = listen("[::]:0".parse().unwrap()).unwrap();
        let port = port_of(&socket);
        TcpStream::connect(("127.0.0.1", port)).unwrap();
        TcpStream::connect(("::1", port)).unwrap();
    }

    #[test]
    fn an_ipv4_address_is_listened_on_as_it_is() {
        let socket = listen("127.0.0.1:0".parse().unwrap()).unwrap();
        TcpStream::connect(("127.0.0.1", port_of(&socket))).unwrap();
    }

    #[test]
    fn a_port_that_is_taken_is_an_error() {
        let first = listen("127.0.0.1:0".parse().unwrap()).unwrap();
        let taken = first.local_addr().unwrap();
        assert_eq!(listen(taken).unwrap_err().kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn the_socket_does_not_block_so_that_the_runtime_can_take_it() {
        let socket = listen("127.0.0.1:0".parse().unwrap()).unwrap();
        assert_eq!(
            socket.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
