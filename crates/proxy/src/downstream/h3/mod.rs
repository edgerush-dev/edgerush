//! EdgeRush's own HTTP/3 server, over quiche under a driver of the worker's
//! ([16](../../../../docs/16-http3.md)).
//!
//! A worker serves an HTTP/3 listener with one task that owns its UDP socket
//! ([`listener`]), which reads datagrams in bounded batches, hands each to its connection
//! and admits new connections; one task for each connection ([`connection`]), which keeps
//! its timers, hands on what quiche has for its streams and sends what quiche wants sent;
//! and one task for each request, which goes to the request core as any request does.

pub(crate) mod body;
pub(crate) mod conn;
pub(crate) mod connection;
pub mod head;
// HTTP/0.9 for quic-interop-runner: the interop image's, and the tests' (16 §8).
#[cfg(any(test, feature = "interop"))]
pub(crate) mod hq;
pub(crate) mod listener;
pub(crate) mod send;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
pub(crate) mod writer;

use std::time::Duration;

/// The protocols a client may ask for over QUIC: HTTP/3, and in the interop build HTTP/0.9
/// as quic-interop-runner speaks it.
#[cfg(not(any(test, feature = "interop")))]
const PROTOCOLS: &[&[u8]] = quiche::h3::APPLICATION_PROTOCOL;
#[cfg(any(test, feature = "interop"))]
const PROTOCOLS: &[&[u8]] = &[b"h3", hq::ALPN];

/// HTTP/3's error codes that the server says (RFC 9114 §8.1).
pub(crate) mod code {
    /// Nothing is wrong: a stream not needed any more, a connection closed when done.
    pub(crate) const NO_ERROR: u64 = 0x100;
    /// The server failed: an answer broke off.
    pub(crate) const INTERNAL_ERROR: u64 = 0x102;
    /// Not processed at all, so a client may send it again (§4.1.1).
    pub(crate) const REQUEST_REJECTED: u64 = 0x10b;
    /// Given up after it was processed in part.
    pub(crate) const REQUEST_CANCELLED: u64 = 0x10c;
    /// Malformed (§4.1.2).
    pub(crate) const MESSAGE_ERROR: u64 = 0x10e;
}

/// What an HTTP/3 listener is served with (16 §6): starting values, to be measured.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    /// Requests a client may have open at once: HTTP/2's.
    pub(crate) streams: u64,
    /// What a client may send on one request before it is given more: HTTP/2's measured
    /// window. Auto-tuning past it is off until it is measured.
    pub(crate) stream_window: u64,
    /// The same for the whole connection.
    pub(crate) connection_window: u64,
    /// The largest request head, by RFC 9114 §4.2.2's measure: the H1 and H2 head limit.
    pub(crate) head_limit: usize,
    /// What quiche is told instead, twice the head limit: a head merely too large is
    /// answered 431 by the server, and one past this is refused with the connection (16 §2).
    pub(crate) field_section: u64,
    /// How long a connection may go without a packet before quiche closes it.
    pub(crate) idle_timeout: Duration,
    /// How long a connection may stay with no request open before it is told to go.
    pub(crate) keep_alive: Duration,
    /// How long a connection has, from its first packet, to finish its handshake: as long
    /// as its idle timeout, since under loss a handshake's retries back off past 10 s
    /// (16 §6).
    pub(crate) handshake: Duration,
    /// How long a connection has, from the end of its handshake, to send its first request:
    /// a handshake slowed by loss, and its PTO backing off, leaves the request all of it
    /// (16 §6), as NGINX and HAProxy time the two apart.
    pub(crate) first_request: Duration,
    /// How long a request's body, or the room for its answer, may be waited on with
    /// nothing coming.
    pub(crate) stream_idle: Duration,
    /// How long a draining connection's requests have to finish.
    pub(crate) drain_within: Duration,
    /// The largest datagram sent: what a path of IPv6's minimum MTU (1,280) carries, and so
    /// every path, one a client's NAT rebinds it to included. There is no path-MTU
    /// discovery: quiche's probes only a connection's first path, and never steps down on a
    /// black hole (16 §6).
    pub(crate) datagram: usize,
    /// Connections a worker holds on the listener before it admits no more.
    pub(crate) connections: usize,
    /// Handshakes under way on a worker past which a client must prove its address first.
    pub(crate) retry_above: usize,
    /// Datagrams read before everything else the worker has gets a turn.
    pub(crate) batch: usize,
    /// New connections admitted in one batch, as TCP's accept is paced (03 §3).
    pub(crate) admit_per_batch: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            streams: 100,
            stream_window: 4 << 20,
            connection_window: 16 << 20,
            head_limit: 64 << 10,
            field_section: 128 << 10,
            idle_timeout: Duration::from_secs(30),
            keep_alive: Duration::from_secs(30),
            handshake: Duration::from_secs(30),
            first_request: Duration::from_secs(10),
            stream_idle: Duration::from_secs(30),
            drain_within: Duration::from_secs(25),
            datagram: 1_232,
            connections: 32_768,
            retry_above: 1_024,
            batch: 64,
            admit_per_batch: 16,
        }
    }
}

impl Settings {
    /// quiche's transport settings, on a context the listener's TLS made.
    pub(crate) fn transport(
        &self,
        tls: boring::ssl::SslContextBuilder,
        ticket_key: &[u8],
    ) -> Result<quiche::Config, quiche::Error> {
        let mut config =
            quiche::Config::with_boring_ssl_ctx_builder(quiche::PROTOCOL_VERSION, tls)?;
        config.set_application_protos(PROTOCOLS)?;
        config.set_ticket_key(ticket_key)?;
        config
            .set_max_idle_timeout(u64::try_from(self.idle_timeout.as_millis()).unwrap_or(u64::MAX));
        config.set_max_send_udp_payload_size(self.datagram);
        config.set_initial_max_data(self.connection_window);
        config.set_initial_max_stream_data_bidi_remote(self.stream_window);
        // The server opens no request stream of its own.
        config.set_initial_max_stream_data_bidi_local(0);
        // The client's control and QPACK streams carry little, and a GREASE stream is
        // stopped unread.
        config.set_initial_max_stream_data_uni(64 << 10);
        config.set_initial_max_streams_bidi(self.streams);
        config.set_initial_max_streams_uni(16);
        config.set_max_stream_window(self.stream_window);
        config.set_max_connection_window(self.connection_window);
        // Rebinding is still followed (16 §3); a client is only asked not to move on purpose.
        config.set_disable_active_migration(true);
        // An ACK alone waits for a second packet or for the answer it can go with, at most
        // 20 ms, 5 short of the 25 ms advertised (16 §2, RFC 9000 §13.2).
        config.enable_delayed_ack(true);
        Ok(config)
    }

    /// quiche's HTTP/3 settings.
    pub(crate) fn h3(&self) -> Result<quiche::h3::Config, quiche::h3::Error> {
        let mut config = quiche::h3::Config::new()?;
        config.set_max_field_section_size(self.field_section);
        Ok(config)
    }
}
