//! Replying from the right source on a wildcard HTTP/3 bind ([16 §3](../../../../../docs/16-http3.md)).
//!
//! A socket bound to a wildcard address (`0.0.0.0` or `[::]`) does not, on its own, tell
//! which of the host's addresses a datagram arrived on, and the kernel is free to send a
//! reply from a different one; a client, whose socket is connected to the address it used,
//! then drops the reply and the connection never forms. Every other QUIC server avoids this:
//! it asks the kernel for each datagram's destination (`IP_PKTINFO` / `IPV6_RECVPKTINFO`)
//! and sends each reply from it. This does the same, through `nix`'s safe wrappers, so that
//! `recv` learns the address quiche must take as the connection's local one and `send` sets
//! it back as the source. Only for a wildcard bind, and only on Linux; a listener on a
//! concrete address needs none of it and keeps the plain `recv_from`/`send_to` path.

use nix::libc;
use nix::sys::socket::sockopt::{Ipv4PacketInfo, Ipv6RecvPacketInfo};
use nix::sys::socket::{
    ControlMessage, ControlMessageOwned, MsgFlags, SockaddrStorage, recvmsg, sendmsg, setsockopt,
};
use std::io::{self, IoSlice, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::AsRawFd;
use tokio::io::Interest;
use tokio::net::UdpSocket;

/// Asks the kernel to report each datagram's destination address on `socket`, for a wildcard
/// bind. Harmless on a concrete bind, but called only for a wildcard one. `IP_PKTINFO` covers
/// IPv4 and, on a dual-stack `[::]` socket, IPv4-mapped clients; `IPV6_RECVPKTINFO` covers
/// IPv6.
pub(crate) fn enable(socket: &UdpSocket, ipv6: bool) -> io::Result<()> {
    setsockopt(socket, Ipv4PacketInfo, &true).map_err(errno)?;
    if ipv6 {
        setsockopt(socket, Ipv6RecvPacketInfo, &true).map_err(errno)?;
    }
    Ok(())
}

/// One datagram from a wildcard socket, with the address it arrived on. `Ok(None)` means the
/// socket had nothing ready. The destination address's port is the listener's, which the
/// caller knows; only its IP is read here.
#[allow(
    clippy::ptr_arg,
    reason = "nix's recvmsg takes an owned Vec, whose capacity it fills and length it sets"
)]
pub(crate) fn recv(
    socket: &UdpSocket,
    buffer: &mut [u8],
    control: &mut Vec<u8>,
) -> io::Result<Option<(usize, SocketAddr, Option<IpAddr>)>> {
    let ready = socket.try_io(Interest::READABLE, || {
        let mut slices = [IoSliceMut::new(buffer)];
        let message = recvmsg::<SockaddrStorage>(
            socket.as_raw_fd(),
            &mut slices,
            Some(control),
            MsgFlags::empty(),
        )
        .map_err(errno)?;
        let from = message
            .address
            .and_then(std_addr)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        // The destination, if a packet-info message carried one; where none did, the caller
        // falls back to the bound address.
        let dst = message
            .cmsgs()
            .ok()
            .and_then(|mut cmsgs| cmsgs.find_map(destination));
        Ok((message.bytes, from, dst))
    });
    match ready {
        Ok(got) => Ok(Some(got)),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error),
    }
}

/// Sends `bytes` to `to` from `from`, for the kernel to cut into datagrams of `segment` bytes
/// where `segment` is `Some` (UDP GSO). The source is set with a packet-info control message
/// of the family of `from`.
pub(crate) fn send(
    socket: &UdpSocket,
    bytes: &[u8],
    to: SocketAddr,
    from: IpAddr,
    segment: Option<u16>,
) -> io::Result<()> {
    // `from` and `to` are the same family as `recv` gave them: on a dual-stack socket a
    // peer reached over IPv4 is an IPv4-mapped IPv6 address both ways, so the source is set
    // with an IPv6 packet-info message carrying the mapped source.
    let info = PacketInfo::of(from);
    let address = SockaddrStorage::from(to);
    socket
        .try_io(Interest::WRITABLE, || {
            let slices = [IoSlice::new(bytes)];
            let fd = socket.as_raw_fd();
            let sent = match &segment {
                Some(segment) => sendmsg(
                    fd,
                    &slices,
                    &[info.control(), ControlMessage::UdpGsoSegments(segment)],
                    MsgFlags::empty(),
                    Some(&address),
                ),
                None => sendmsg(
                    fd,
                    &slices,
                    &[info.control()],
                    MsgFlags::empty(),
                    Some(&address),
                ),
            };
            sent.map_err(errno)
        })
        .map(drop)
}

/// A source address as the control message for it, keeping the `libc` struct alive while the
/// message borrows it.
enum PacketInfo {
    V4(libc::in_pktinfo),
    V6(libc::in6_pktinfo),
}

impl PacketInfo {
    fn of(from: IpAddr) -> Self {
        match from {
            IpAddr::V4(ip) => Self::V4(libc::in_pktinfo {
                ipi_ifindex: 0,
                // The source the kernel sends from.
                ipi_spec_dst: libc::in_addr {
                    s_addr: u32::from_ne_bytes(ip.octets()),
                },
                ipi_addr: libc::in_addr { s_addr: 0 },
            }),
            IpAddr::V6(ip) => Self::V6(libc::in6_pktinfo {
                ipi6_addr: libc::in6_addr {
                    s6_addr: ip.octets(),
                },
                ipi6_ifindex: 0,
            }),
        }
    }

    fn control(&self) -> ControlMessage<'_> {
        match self {
            Self::V4(info) => ControlMessage::Ipv4PacketInfo(info),
            Self::V6(info) => ControlMessage::Ipv6PacketInfo(info),
        }
    }
}

/// The destination address a received control message carries, if it is a packet-info one.
fn destination(cmsg: ControlMessageOwned) -> Option<IpAddr> {
    match cmsg {
        // `s_addr` holds the address in network order; its bytes are the octets as they are.
        ControlMessageOwned::Ipv4PacketInfo(info) => Some(IpAddr::V4(Ipv4Addr::from(
            info.ipi_addr.s_addr.to_ne_bytes(),
        ))),
        ControlMessageOwned::Ipv6PacketInfo(info) => {
            Some(IpAddr::V6(Ipv6Addr::from(info.ipi6_addr.s6_addr)))
        }
        _ => None,
    }
}

/// A `nix` address as `std`'s.
fn std_addr(address: SockaddrStorage) -> Option<SocketAddr> {
    if let Some(v4) = address.as_sockaddr_in() {
        return Some(SocketAddr::V4((*v4).into()));
    }
    if let Some(v6) = address.as_sockaddr_in6() {
        return Some(SocketAddr::V6((*v6).into()));
    }
    None
}

/// A `nix` error number as an `io::Error`, so a would-block reads as one to `try_io`.
fn errno(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

/// Control-message buffer room for a packet-info message of either family, for `recv`'s
/// reusable buffer.
pub(crate) fn control_space() -> Vec<u8> {
    nix::cmsg_space!(libc::in6_pktinfo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// Reads one datagram once the socket has it, waiting if it has none yet.
    async fn recv_one(
        socket: &UdpSocket,
        buffer: &mut [u8],
        control: &mut Vec<u8>,
    ) -> (usize, SocketAddr, Option<IpAddr>) {
        loop {
            socket.readable().await.unwrap();
            if let Some(got) = recv(socket, buffer, control).unwrap() {
                return got;
            }
        }
    }

    /// Sends once the socket has room, waiting if it has none yet.
    async fn send_one(socket: &UdpSocket, bytes: &[u8], to: SocketAddr, from: IpAddr) {
        loop {
            socket.writable().await.unwrap();
            match send(socket, bytes, to, from, None) {
                Ok(()) => return,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("send: {error}"),
            }
        }
    }

    /// On a wildcard bind, `recv` reports the address a datagram arrived on, not the bound
    /// wildcard: a client reaching the host on 127.0.0.2 is seen as having done so. Without
    /// the packet-info control message this would be `0.0.0.0` and a reply could not go back.
    #[tokio::test]
    async fn recv_reports_the_address_a_datagram_arrived_on() {
        let server = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        enable(&server, false).unwrap();
        let port = server.local_addr().unwrap().port();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // The whole of 127.0.0.0/8 is loopback on Linux: send to a second one.
        client.connect(("127.0.0.2", port)).await.unwrap();
        client.send(b"ping").await.unwrap();

        let (mut buffer, mut control) = ([0u8; 16], control_space());
        let (len, _from, dst) = recv_one(&server, &mut buffer, &mut control).await;
        assert_eq!(&buffer[..len], b"ping");
        assert_eq!(dst, Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))));
    }

    /// A reply sent with a source of 127.0.0.2 reaches a client connected to 127.0.0.2: the
    /// control message sets the source, so a client that accepts only that address takes it.
    #[tokio::test]
    async fn a_reply_goes_from_the_source_it_is_given() {
        let server = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        enable(&server, false).unwrap();
        let port = server.local_addr().unwrap().port();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(("127.0.0.2", port)).await.unwrap();
        client.send(b"ping").await.unwrap();

        let (mut buffer, mut control) = ([0u8; 16], control_space());
        let (_len, from, dst) = recv_one(&server, &mut buffer, &mut control).await;
        assert_eq!(dst, Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))));
        // Reply from the address it arrived on.
        send_one(&server, b"pong", from, dst.unwrap()).await;
        let mut reply = [0u8; 16];
        let n = client.recv(&mut reply).await.unwrap();
        assert_eq!(&reply[..n], b"pong");
    }
}
