//! Opening the sockets that are listened on.

use socket2::{Domain, Protocol, Socket, Type};
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{SocketAddr, TcpListener, UdpSocket};

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

/// A UDP socket on `address`, for HTTP/3 or a `udp` listener, ready to be handed to the
/// runtime: IPv4 as well on an IPv6 address, as [`listen`] does, and one of a group on a
/// shared port, where the kernel deals datagrams out by a hash of their addresses (16 §3,
/// 24 §3).
pub(crate) fn datagrams(address: SocketAddr, port: Port) -> io::Result<UdpSocket> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    if address.is_ipv6() {
        socket.set_only_v6(false)?;
    }
    if port == Port::Shared {
        share(&socket)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    Ok(socket.into())
}

/// Ports drawn for a listener with HTTP/3 before it is given up on. The first is the
/// system's own; the others are named at random from [`DYNAMIC`], since the system hands
/// ports out one after another, and once one lands in a run of ports the host keeps from a
/// protocol, the next do too. Windows keeps runs of each protocol's ports apart for Hyper-V
/// and WSL, more than 1,000 ports long: a machine measured kept some 2,300 of the 16,384
/// from TCP or UDP, about one in seven, so that all 15 named at random are refused about
/// once in 10^12 tries.
const DRAWS: usize = 16;

/// The dynamic ports (RFC 6335), which are also Windows' own range for them by default.
const DYNAMIC: std::ops::RangeInclusive<u16> = 49152..=65535;

/// Which of a listener's two sockets could not be opened.
#[derive(Debug)]
pub(crate) enum Refused {
    /// The TCP listener, on the address asked for.
    Tcp(io::Error),
    /// HTTP/3's UDP socket, on the address the TCP listener was given.
    Udp(SocketAddr, io::Error),
}

/// A TCP listener on `address` and HTTP/3's UDP socket on the port it was given (16 §5), as
/// [`listen`] and [`datagrams`] open them. Where `address` leaves the port to the operating
/// system, one whose UDP is refused — taken, or in a range the host excludes from UDP — is
/// let go and another drawn at random, up to [`DRAWS`] times: any port was asked for, so
/// long as both sockets have it. A port named is tried once.
pub(crate) fn listen_both(
    address: SocketAddr,
    port: Port,
) -> Result<(TcpListener, UdpSocket), Refused> {
    listen_both_with(address, port, datagrams, at_random())
}

/// Ports from [`DYNAMIC`], each drawn afresh.
fn at_random() -> impl FnMut() -> u16 {
    let keys = RandomState::new();
    let mut drawn: u64 = 0;
    move || {
        drawn += 1;
        let first = *DYNAMIC.start();
        let span = u64::from(DYNAMIC.end() - first) + 1;
        // Below `span`, so the sum stays inside the range.
        first + u16::try_from(keys.hash_one(drawn) % span).unwrap_or_default()
    }
}

/// [`listen_both`] with its UDP socket and its draws passed in. A draw of 0 leaves the
/// port to the system again.
fn listen_both_with<U>(
    address: SocketAddr,
    port: Port,
    mut udp: impl FnMut(SocketAddr, Port) -> io::Result<U>,
    mut draw: impl FnMut() -> u16,
) -> Result<(TcpListener, U), Refused> {
    let draws = if address.port() == 0 { DRAWS } else { 1 };
    let mut asked = address;
    let mut drawn = 1;
    loop {
        let again = drawn < draws;
        drawn += 1;
        let tcp = match listen(asked, port) {
            Ok(tcp) => tcp,
            // A port drawn may be taken or kept from TCP, as one the system gives may be
            // from UDP.
            Err(error) if again && asked.port() != 0 && another_may_do(&error) => {
                asked.set_port(draw());
                continue;
            }
            Err(error) => return Err(Refused::Tcp(error)),
        };
        let given = tcp.local_addr().map_err(Refused::Tcp)?;
        match udp(given, port) {
            Ok(udp) => return Ok((tcp, udp)),
            Err(error) if again && another_may_do(&error) => asked.set_port(draw()),
            Err(error) => return Err(Refused::Udp(given, error)),
        }
    }
}

/// A refusal of this one port, which another may not meet: taken, or kept from the
/// protocol (Windows' excluded ranges refuse a bind as access denied).
fn another_may_do(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied
    )
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
    fn a_datagram_socket_takes_ipv4_and_ipv6_on_every_address() {
        let socket = datagrams("[::]:0".parse().unwrap(), Port::Own).unwrap();
        let port = socket.local_addr().unwrap().port();
        for from in ["127.0.0.1:0", "[::1]:0"] {
            let client = UdpSocket::bind(from).unwrap();
            let to: SocketAddr = if from.starts_with('[') {
                format!("[::1]:{port}").parse().unwrap()
            } else {
                format!("127.0.0.1:{port}").parse().unwrap()
            };
            client.send_to(b"hello", to).unwrap();
        }
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut buf = [0; 16];
        for _ in 0..2 {
            let (len, _) = socket.recv_from(&mut buf).unwrap();
            assert_eq!(&buf[..len], b"hello");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_shared_datagram_port_has_a_socket_for_every_worker() {
        let first = datagrams("127.0.0.1:0".parse().unwrap(), Port::Shared).unwrap();
        let address = first.local_addr().unwrap();
        let second = datagrams(address, Port::Shared).unwrap();
        assert_eq!(second.local_addr().unwrap(), address);
        assert_eq!(
            datagrams(address, Port::Own).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
    }

    fn local() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    /// A UDP bind refused as Windows refuses a port in a range it excludes.
    fn excluded() -> io::Error {
        io::Error::from(io::ErrorKind::PermissionDenied)
    }

    /// Every draw left to the system, as the first is.
    fn by_the_system() -> u16 {
        0
    }

    #[test]
    fn a_listener_with_http3_has_one_port_for_both() {
        let (tcp, udp) = listen_both(local(), Port::Own).unwrap();
        assert_eq!(tcp.local_addr().unwrap(), udp.local_addr().unwrap());
    }

    #[test]
    fn a_port_left_to_the_system_whose_udp_is_refused_is_drawn_again() {
        let mut asked = Vec::new();
        let mut drawn = Vec::new();
        let mut random = at_random();
        let (tcp, udp) = listen_both_with(
            local(),
            Port::Own,
            |address, _| {
                asked.push(address.port());
                if asked.len() == 1 {
                    Err(excluded())
                } else {
                    Ok(address)
                }
            },
            || {
                drawn.push(random());
                *drawn.last().unwrap()
            },
        )
        .unwrap();
        assert_eq!(asked.len(), 2);
        // The last port drawn is the one both were given; any before it were taken.
        assert_eq!(asked[1], *drawn.last().unwrap());
        assert_eq!(tcp.local_addr().unwrap().port(), asked[1]);
        assert_eq!(udp.port(), asked[1]);
    }

    /// The system hands ports out one after another, so once one is in a run the host keeps
    /// from UDP, the ones it hands out next are too (#300): the draws after it are named
    /// elsewhere.
    #[test]
    fn a_run_of_ports_kept_from_udp_is_left_behind() {
        let mut first = None;
        let mut asked = 0;
        listen_both_with(
            local(),
            Port::Own,
            |address, _| {
                asked += 1;
                let first = *first.get_or_insert(address.port());
                // As long as the runs Windows keeps, either side of the first.
                if address.port().abs_diff(first) < 1_200 {
                    Err(excluded())
                } else {
                    Ok(())
                }
            },
            at_random(),
        )
        .unwrap();
        assert!(asked >= 2);
    }

    #[test]
    fn a_port_drawn_whose_tcp_is_taken_is_drawn_again() {
        let holder = listen(local(), Port::Own).unwrap();
        let taken = port_of(&holder);
        let mut asked = Vec::new();
        let mut draws = [taken, 0].into_iter();
        listen_both_with(
            local(),
            Port::Own,
            |address, _| {
                asked.push(address.port());
                if asked.len() == 1 {
                    Err(excluded())
                } else {
                    Ok(())
                }
            },
            || draws.next().unwrap(),
        )
        .unwrap();
        assert_eq!(draws.next(), None);
        assert_eq!(asked.len(), 2);
        assert!(!asked.contains(&taken));
    }

    #[test]
    fn a_port_whose_udp_is_taken_is_drawn_again_too() {
        let mut asked = 0;
        listen_both_with(
            local(),
            Port::Own,
            |_, _| {
                asked += 1;
                if asked == 1 {
                    Err(io::Error::from(io::ErrorKind::AddrInUse))
                } else {
                    Ok(())
                }
            },
            at_random(),
        )
        .unwrap();
        assert_eq!(asked, 2);
    }

    #[test]
    fn a_port_named_is_not_drawn_again() {
        // A port free for TCP, named.
        let named = listen(local(), Port::Own).unwrap().local_addr().unwrap();
        let mut asked = 0;
        let mut drawn = 0;
        let refused = listen_both_with(
            named,
            Port::Own,
            |_, _| -> io::Result<()> {
                asked += 1;
                Err(io::Error::from(io::ErrorKind::AddrInUse))
            },
            || {
                drawn += 1;
                0
            },
        )
        .unwrap_err();
        assert_eq!((asked, drawn), (1, 0));
        let Refused::Udp(at, error) = refused else {
            panic!("the TCP listener was refused: {refused:?}");
        };
        assert_eq!(at, named);
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn drawing_ends_after_its_draws() {
        let mut asked = 0;
        let refused = listen_both_with(
            local(),
            Port::Own,
            |_, _| -> io::Result<()> {
                asked += 1;
                Err(excluded())
            },
            by_the_system,
        )
        .unwrap_err();
        assert_eq!(asked, DRAWS);
        assert!(
            matches!(refused, Refused::Udp(_, error) if error.kind() == io::ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn a_refusal_another_port_would_not_mend_is_not_drawn_again() {
        let mut asked = 0;
        let refused = listen_both_with(
            local(),
            Port::Own,
            |_, _| -> io::Result<()> {
                asked += 1;
                Err(io::Error::from(io::ErrorKind::AddrNotAvailable))
            },
            by_the_system,
        )
        .unwrap_err();
        assert_eq!(asked, 1);
        assert!(
            matches!(refused, Refused::Udp(_, error) if error.kind() == io::ErrorKind::AddrNotAvailable)
        );
    }

    #[test]
    fn ports_drawn_at_random_spread_over_the_dynamic_range() {
        let mut random = at_random();
        let drawn: Vec<u16> = (0..1_000).map(|_| random()).collect();
        assert!(drawn.iter().all(|port| DYNAMIC.contains(port)));
        let (low, high) = (drawn.iter().min().unwrap(), drawn.iter().max().unwrap());
        // 1,000 draws from 16,384 ports: a spread under half of it is all but impossible.
        assert!(high - low > 8_192, "{low}..={high}");
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
