//! One probe of an endpoint ([03 §6](../../../docs/03-data-plane.md)).
//!
//! Every probe opens a connection of its own — TLS as the upstream is reached, if it is —
//! and closes it after: a kept connection could hide a broken path (a firewall, a load
//! balancer between) that a new one would meet, as Pingora's documentation warns, and a
//! probe must not take a place in the pool that requests are waiting for.

use crate::gathered::Gathered;
use crate::proxy_protocol;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::secure::Socket;
use bytes::{Buf, Bytes};
use edgerush_config::{HealthCheck, Probe, UpstreamProtocol};
use http::uri::Scheme;
use http::{Request, StatusCode};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

/// What gRPC's health service calls a server that serves.
const SERVING: u64 = 1;

/// The most of an HTTP/1 answer read to find its status line.
const STATUS_LINE: usize = 1024;

/// Whether `destination` passes `check`: within its timeout, connection and all.
pub(crate) async fn passes(destination: &ReuseIdentity, check: &HealthCheck) -> bool {
    let timeout = Duration::from_secs(check.timeout_seconds);
    tokio::time::timeout(timeout, probe(destination, &check.probe))
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
}

async fn probe(destination: &ReuseIdentity, probe: &Probe) -> Option<bool> {
    // Connecting is all a plain TCP probe asks, and the backend need never see it.
    if *probe == Probe::Tcp && destination.secure().is_none() {
        return Some(connect_unseen(destination.address()).await.is_ok());
    }
    let mut socket = TcpStream::connect(destination.address()).await.ok()?;
    let _unset = socket.set_nodelay(true);
    // A backend that asks for a PROXY header is told the probe is the gateway's own
    // connection: v2's LOCAL, or a v1 line of its own two ends (20 §4).
    if let Some(version) = destination.proxy_protocol() {
        let (ours, theirs) = (socket.local_addr().ok()?, socket.peer_addr().ok()?);
        let header = proxy_protocol::own(version, ours, theirs);
        socket.write_all(header.as_bytes()).await.ok()?;
    }
    let (socket, authority) = match destination.secure() {
        None => (Socket::Plain(socket), destination.address().to_string()),
        Some(secure) => (
            Socket::Secured(Gathered::new(secure.connect(socket).await.ok()?)),
            secure.source().server_name.clone(),
        ),
    };
    // As a request's own: `https` exactly when the probe is secured.
    let scheme = destination.scheme();
    Some(match (probe, destination.protocol()) {
        // The handshake, as traffic's would be, was all it asked.
        (Probe::Tcp, _) => {
            drop(socket);
            true
        }
        (Probe::Http { path }, UpstreamProtocol::Http1) => http1(socket, path, &authority).await?,
        (Probe::Http { path }, UpstreamProtocol::Http2) => {
            http2(socket, &scheme, path, &authority).await?
        }
        (Probe::Grpc { service }, _) => grpc(socket, &scheme, service, &authority).await?,
    })
}

/// Connects to `address` only to see that it can, and resets the connection at once (20 §5).
/// The last ACK of the handshake is held back first (`TCP_QUICKACK` off, as HAProxy's plain
/// check does), so that as a rule the reset goes before it: the connection never completes
/// on the backend's side and never reaches its `accept`, and a database does not count an
/// empty connection against the gateway. Best effort: should the ACK go first, because this
/// thread was not run in time, the backend accepts and then sees a reset, which costs it no
/// more than a close would. Windows has no `TCP_QUICKACK`; there it is only reset.
///
/// # Errors
///
/// The connect's, or the socket's if it cannot be set up.
pub(crate) async fn connect_unseen(address: SocketAddr) -> io::Result<()> {
    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    #[cfg(target_os = "linux")]
    socket2::SockRef::from(&socket).set_tcp_quickack(false)?;
    let stream = socket.connect(address).await?;
    stream.set_zero_linger()?;
    drop(stream);
    Ok(())
}

/// `GET` over HTTP/1.1, the connection closed after: a 2xx passes.
async fn http1(mut socket: Socket, path: &str, authority: &str) -> Option<bool> {
    let asked = format!(
        "GET {path} HTTP/1.1\r\nhost: {authority}\r\nuser-agent: edgerush-health\r\nconnection: close\r\n\r\n"
    );
    socket.write_all(asked.as_bytes()).await.ok()?;
    let mut line = Vec::with_capacity(64);
    let mut byte = [0; 1];
    while !line.ends_with(b"\r\n") {
        if line.len() >= STATUS_LINE || socket.read(&mut byte).await.ok()? == 0 {
            return None;
        }
        line.push(byte[0]);
    }
    // `HTTP/1.x NNN reason`
    let status = line.get(9..12)?;
    let known =
        line.starts_with(b"HTTP/1.") && line.get(12).is_some_and(|b| *b == b' ' || *b == b'\r');
    Some(known && status.first() == Some(&b'2') && status.iter().all(u8::is_ascii_digit))
}

/// `GET` over HTTP/2 by prior knowledge, or by what TLS agreed on: a 2xx passes.
async fn http2(socket: Socket, scheme: &Scheme, path: &str, authority: &str) -> Option<bool> {
    let mut send = handshake(socket).await?;
    let asked = Request::get(format!("{scheme}://{authority}{path}"))
        .header("user-agent", "edgerush-health")
        .body(())
        .ok()?;
    let (answer, _) = send.send_request(asked, true).ok()?;
    let answer = answer.await.ok()?;
    Some(answer.status().is_success())
}

/// `grpc.health.v1.Health/Check` for `service`: `SERVING`, with `grpc-status` 0, passes.
async fn grpc(socket: Socket, scheme: &Scheme, service: &str, authority: &str) -> Option<bool> {
    let mut send = handshake(socket).await?;
    let asked = Request::post(format!(
        "{scheme}://{authority}/grpc.health.v1.Health/Check"
    ))
    .header("content-type", "application/grpc")
    .header("te", "trailers")
    .header("user-agent", "edgerush-health")
    .body(())
    .ok()?;
    let (answer, mut sending) = send.send_request(asked, false).ok()?;
    sending.send_data(check_request(service), true).ok()?;
    let answer = answer.await.ok()?;
    if answer.status() != StatusCode::OK {
        return Some(false);
    }
    // A trailers-only answer is a status and no message: not serving.
    if answer.headers().contains_key("grpc-status") {
        return Some(false);
    }
    let mut body = answer.into_body();
    let mut message = Vec::new();
    while let Some(data) = body.data().await {
        let data = data.ok()?;
        let _ = body.flow_control().release_capacity(data.len());
        // One small message is all it says; more than a page of it is not a health answer.
        if message.len() + data.len() > 4096 {
            return None;
        }
        message.extend_from_slice(&data);
    }
    let trailers = std::future::poll_fn(|cx| body.poll_trailers(cx))
        .await
        .ok()??;
    let ok = trailers
        .get("grpc-status")
        .is_some_and(|status| status == "0");
    Some(ok && serving(&message))
}

/// Opens HTTP/2 on `socket`, its connection driven beside the probe.
async fn handshake(socket: Socket) -> Option<::h2::client::SendRequest<Bytes>> {
    let (send, connection) = ::h2::client::handshake(socket).await.ok()?;
    let _driving = tokio::task::spawn_local(async move {
        let _ended = connection.await;
    });
    send.ready().await.ok()
}

/// `HealthCheckRequest { service }`, as one gRPC message: a byte saying it is not
/// compressed, its length, then field 1, a string, if the service is named.
fn check_request(service: &str) -> Bytes {
    let mut message = Vec::with_capacity(service.len() + 3);
    if !service.is_empty() {
        message.push(0x0a);
        let mut length = service.len();
        while length >= 0x80 {
            message.push((length as u8 & 0x7f) | 0x80);
            length >>= 7;
        }
        message.push(length as u8);
        message.extend_from_slice(service.as_bytes());
    }
    let mut framed = Vec::with_capacity(message.len() + 5);
    framed.push(0);
    framed.extend_from_slice(
        &u32::try_from(message.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    framed.extend_from_slice(&message);
    Bytes::from(framed)
}

/// Whether one gRPC message is `HealthCheckResponse { status: SERVING }`.
fn serving(framed: &[u8]) -> bool {
    let mut framed = framed;
    if framed.len() < 5 || framed.get_u8() != 0 {
        return false;
    }
    let length = framed.get_u32() as usize;
    let Some(mut message) = framed.get(..length) else {
        return false;
    };
    let mut status = None;
    while !message.is_empty() {
        let Some(tag) = varint(&mut message) else {
            return false;
        };
        match (tag >> 3, tag & 7) {
            (1, 0) => status = varint(&mut message),
            // Anything else is skipped by its wire type.
            (_, 0) => {
                if varint(&mut message).is_none() {
                    return false;
                }
            }
            (_, 2) => {
                let Some(skip) = varint(&mut message).and_then(|n| usize::try_from(n).ok()) else {
                    return false;
                };
                let Some(rest) = message.get(skip..) else {
                    return false;
                };
                message = rest;
            }
            _ => return false,
        }
    }
    status == Some(SERVING)
}

fn varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(message: &[u8]) -> Vec<u8> {
        let mut framed = vec![0];
        framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
        framed.extend_from_slice(message);
        framed
    }

    #[test]
    fn a_check_request_names_its_service() {
        assert_eq!(&check_request("")[..], &[0, 0, 0, 0, 0]);
        assert_eq!(
            &check_request("svc")[..],
            &[0, 0, 0, 0, 5, 0x0a, 3, b's', b'v', b'c']
        );
        let long = "s".repeat(200);
        let request = check_request(&long);
        assert_eq!(&request[5..8], &[0x0a, 0xc8, 0x01]);
        assert_eq!(request.len(), 5 + 3 + 200);
    }

    #[test]
    fn only_serving_is_serving() {
        assert!(serving(&framed(&[0x08, 1])));
        assert!(!serving(&framed(&[0x08, 2])), "NOT_SERVING");
        assert!(!serving(&framed(&[0x08, 3])), "SERVICE_UNKNOWN");
        assert!(!serving(&framed(&[0x08, 0])), "UNKNOWN");
        assert!(!serving(&framed(&[])), "no status at all");
        assert!(!serving(&framed(&[0x08])), "cut short");
        assert!(!serving(&[]));
        assert!(!serving(&[1, 0, 0, 0, 2, 0x08, 1]), "compressed");
        // Fields it does not know are skipped.
        assert!(serving(&framed(&[0x12, 2, b'h', b'i', 0x08, 1])));
        assert!(serving(&framed(&[0x18, 0x96, 0x01, 0x08, 1])));
    }
}
