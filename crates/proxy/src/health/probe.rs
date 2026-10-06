//! One probe of an endpoint ([03 §6](../../../docs/03-data-plane.md)).
//!
//! Every probe opens a connection of its own — TLS as the upstream is reached, if it is —
//! and closes it after: a kept connection could hide a broken path (a firewall, a load
//! balancer between) that a new one would meet, as Pingora's documentation warns, and a
//! probe must not take a place in the pool that requests are waiting for.

use crate::gathered::Gathered;
use crate::proxy_protocol;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::dial::{self, Unconnected};
use crate::upstream::secure::Socket;
use ::h2::client::SendRequest;
use bytes::{Buf, Bytes};
use edgerush_config::{HealthCheck, Probe, UpstreamProtocol};
use http::uri::Scheme;
use http::{Request, StatusCode};
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// What gRPC's health service calls a server that serves.
const SERVING: u64 = 1;

/// The most of an HTTP/1 answer read to find its status line, and of any line of its head.
const STATUS_LINE: usize = 1024;

/// The most informational answers an HTTP/1 probe passes over before the final one, and the
/// most fields each may have.
const INFORMATIONAL: usize = 8;
const INFORMATIONAL_FIELDS: usize = 64;

/// What a probe came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Probed {
    Passed,
    Failed,
    /// The worker had no socket to probe with: nothing is known of the endpoint either way
    /// ([03 §6](../../../docs/03-data-plane.md)).
    Unknown,
}

/// What a probe of `destination` by `check` came to: within its timeout, connection and all.
pub(crate) async fn probed(destination: &ReuseIdentity, check: &HealthCheck) -> Probed {
    let timeout = Duration::from_secs(check.timeout_seconds);
    tokio::time::timeout(timeout, probe(destination, &check.probe))
        .await
        .unwrap_or(Probed::Failed)
}

/// Whether `destination` passes `check`.
#[cfg(test)]
pub(crate) async fn passes(destination: &ReuseIdentity, check: &HealthCheck) -> bool {
    probed(destination, check).await == Probed::Passed
}

async fn probe(destination: &ReuseIdentity, probe: &Probe) -> Probed {
    let whose = |unconnected: Unconnected| match unconnected {
        Unconnected::Short(_) => Probed::Unknown,
        Unconnected::NoPort(_) | Unconnected::Endpoint(_) => Probed::Failed,
    };
    // Connecting is all a plain TCP probe asks, and the backend need never see it.
    if *probe == Probe::Tcp && destination.secure().is_none() {
        return connect_unseen(destination.address())
            .await
            .map_or_else(whose, |()| Probed::Passed);
    }
    let socket = match dial::connect(destination.address()).await {
        Ok(socket) => socket,
        Err(unconnected) => return whose(unconnected),
    };
    match over(socket, destination, probe).await {
        Some(true) => Probed::Passed,
        Some(false) | None => Probed::Failed,
    }
}

/// The probe made over `socket`, connected to `destination`: whether it passed, or nothing
/// where it could not be made.
async fn over(mut socket: TcpStream, destination: &ReuseIdentity, probe: &Probe) -> Option<bool> {
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
/// [`Unconnected`]: the connect's, saying whose it is, or the worker's own shortage where
/// the socket cannot be made or set up.
pub(crate) async fn connect_unseen(address: SocketAddr) -> Result<(), Unconnected> {
    let socket = dial::socket(address)?;
    #[cfg(target_os = "linux")]
    socket2::SockRef::from(&socket)
        .set_tcp_quickack(false)
        .map_err(Unconnected::Short)?;
    let stream = socket
        .connect(address)
        .await
        .map_err(dial::connect_failed)?;
    stream.set_zero_linger().map_err(Unconnected::Short)?;
    drop(stream);
    Ok(())
}

/// `GET` over HTTP/1.1, the connection closed after: a 2xx passes.
async fn http1(mut socket: Socket, path: &str, authority: &str) -> Option<bool> {
    let asked = format!(
        "GET {path} HTTP/1.1\r\nhost: {authority}\r\nuser-agent: edgerush-health\r\nconnection: close\r\n\r\n"
    );
    socket.write_all(asked.as_bytes()).await.ok()?;
    // Informational answers may come before the final one (RFC 9110 §15.2): Early Hints,
    // a 100 Continue nobody asked for. They are passed over, so many and no more, and the
    // final answer judged.
    for _ in 0..=INFORMATIONAL {
        let line = line_of(&mut socket).await?;
        // `HTTP/1.x NNN reason`
        let status = line.get(9..12)?;
        let known = line.starts_with(b"HTTP/1.")
            && line.get(12).is_some_and(|b| *b == b' ' || *b == b'\r')
            && status.iter().all(u8::is_ascii_digit);
        // 101 is final: a switch that a probe never asks for, and no pass.
        if known && status[0] == b'1' && status != b"101" {
            // Its fields, to the empty line that ends it.
            let mut fields = 0;
            while line_of(&mut socket).await? != b"\r\n" {
                fields += 1;
                if fields > INFORMATIONAL_FIELDS {
                    return None;
                }
            }
            continue;
        }
        return Some(known && status[0] == b'2');
    }
    None
}

/// A line of an HTTP/1 answer's head, CRLF included, of at most [`STATUS_LINE`] bytes.
async fn line_of(socket: &mut Socket) -> Option<Vec<u8>> {
    let mut line = Vec::with_capacity(64);
    let mut byte = [0; 1];
    while !line.ends_with(b"\r\n") {
        if line.len() >= STATUS_LINE || socket.read(&mut byte).await.ok()? == 0 {
            return None;
        }
        line.push(byte[0]);
    }
    Some(line)
}

/// `GET` over HTTP/2 by prior knowledge, or by what TLS agreed on: a 2xx passes.
async fn http2(socket: Socket, scheme: &Scheme, path: &str, authority: &str) -> Option<bool> {
    over_h2(socket, |mut send| async move {
        let asked = Request::get(format!("{scheme}://{authority}{path}"))
            .header("user-agent", "edgerush-health")
            .body(())
            .ok()?;
        let (answer, _) = send.send_request(asked, true).ok()?;
        let answer = answer.await.ok()?;
        Some(answer.status().is_success())
    })
    .await
}

/// `grpc.health.v1.Health/Check` for `service`: `SERVING`, with `grpc-status` 0, passes.
async fn grpc(socket: Socket, scheme: &Scheme, service: &str, authority: &str) -> Option<bool> {
    over_h2(socket, |send| grpc_check(send, scheme, service, authority)).await
}

async fn grpc_check(
    mut send: SendRequest<Bytes>,
    scheme: &Scheme,
    service: &str,
    authority: &str,
) -> Option<bool> {
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

/// Opens HTTP/2 on `socket` and asks what `ask` asks over it.
///
/// The connection is driven here, beside the asking, not in a task of its own: whatever ends
/// the probe — its answer, or its timeout dropping it — ends the connection and lets go of
/// its socket. A detached connection would outlive the probe and the 64 out at once, and a
/// backend that stopped reading would keep it for ever, its GOAWAY never written (03 §6).
async fn over_h2<A, F>(socket: Socket, ask: A) -> Option<bool>
where
    A: FnOnce(SendRequest<Bytes>) -> F,
    F: Future<Output = Option<bool>>,
{
    let (send, mut connection) = client().handshake(socket).await.ok()?;
    let passed = {
        let mut asking = pin!(async move { ask(send.ready().await.ok()?).await });
        let asked = poll_fn(|cx| {
            if let Poll::Ready(passed) = asking.as_mut().poll(cx) {
                return Poll::Ready(Some(passed));
            }
            // Ended, by the peer or by an error: the probe's stream hears so once it is gone.
            Pin::new(&mut connection).poll(cx).map(|_| None)
        })
        .await;
        match asked {
            Some(passed) => passed,
            None => {
                drop(connection);
                return asking.await;
            }
        }
        // `asking` goes here, and the probe's handles with it: h2 sees nothing left open.
    };
    // One turn, for h2 to say GOAWAY if the socket takes it now; not waited for, as an
    // HTTP/1 probe does not wait for its close.
    poll_fn(|cx| {
        let _turn = Pin::new(&mut connection).poll(cx);
        Poll::Ready(())
    })
    .await;
    passed
}

/// h2's client for a probe, every bound set as a request's client sets it (15 §3) rather
/// than left to h2's defaults, which take pushed streams without limit and header lists of
/// 16 MiB.
fn client() -> ::h2::client::Builder {
    let mut builder = ::h2::client::Builder::new();
    builder
        .enable_push(false)
        .max_concurrent_streams(0)
        .initial_max_send_streams(1)
        // A probe reads one small answer: RFC 9113's default windows hold it.
        .initial_window_size(65_535)
        .initial_connection_window_size(65_535)
        .max_header_list_size(64 * 1024)
        .header_table_size(4096)
        .max_frame_size(16_384)
        // A head and at most one small message to send.
        .max_send_buffer_size(16 * 1024)
        .max_concurrent_reset_streams(50)
        .reset_stream_duration(Duration::from_secs(1))
        .max_local_error_reset_streams(Some(1024));
    builder
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
