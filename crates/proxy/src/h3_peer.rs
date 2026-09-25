//! A client and a server quiche connection joined in memory, and a scripted HTTP/3 peer.
//!
//! quiche does no I/O, so two of its connections can be driven against each other with no
//! socket and no runtime: [`Pipe`] hands every datagram one side sends to the other, and
//! records what went by so a test can assert on the wire (16 §8). Addresses are only
//! labels here, which is what lets a test move the client to a new one mid-connection.
//!
//! The client side can run quiche's own HTTP/3 client for well-behaved requests, or write
//! raw HTTP/3 onto its streams for everything a well-behaved client will not produce:
//! oversized frames, unknown frame and stream types, a third HEADERS, a request on a stream
//! past a GOAWAY. What it writes is encoded in the plainest form there is — QPACK literals
//! with literal names, never indexed, never Huffman — as the HTTP/2 peer writes HPACK. It is
//! deliberately not a codec: nothing it receives is decoded.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "a test toolkit: each test binary that includes it uses a different part, and \
              its helpers fail a test the way the test would"
)]

use boring::asn1::Asn1Time;
use boring::bn::BigNum;
use boring::ec::{EcGroup, EcKey};
use boring::hash::MessageDigest;
use boring::nid::Nid;
use boring::pkey::{PKey, Private};
use boring::ssl::{SslContextBuilder, SslMethod};
use boring::x509::extension::SubjectAlternativeName;
use boring::x509::{X509, X509NameBuilder};
use std::net::SocketAddr;

/// The name the test server's certificate carries.
pub(crate) const SERVER_NAME: &str = "h3.test";

/// Room for any datagram either side sends.
const DATAGRAM: usize = 65_535;

/// How many rounds [`Pipe::advance`] runs before it decides the two sides will never go
/// quiet. Every probe settles in a handful.
const ROUNDS: usize = 1_000;

/// HTTP/3 frame types (RFC 9114 §7.2).
pub(crate) mod frame {
    pub(crate) const DATA: u64 = 0x0;
    pub(crate) const HEADERS: u64 = 0x1;
    pub(crate) const SETTINGS: u64 = 0x4;
    pub(crate) const GOAWAY: u64 = 0x7;
    /// A reserved type, of the form 0x1f × N + 0x21 (RFC 9114 §7.2.8).
    pub(crate) const RESERVED: u64 = 0x21;
}

/// Unidirectional stream types (RFC 9114 §6.2, RFC 9204 §4.2).
pub(crate) mod stream_type {
    pub(crate) const CONTROL: u64 = 0x0;
    pub(crate) const QPACK_ENCODER: u64 = 0x2;
    /// A reserved type, of the form 0x1f × N + 0x21 (RFC 9114 §6.2.3).
    pub(crate) const RESERVED: u64 = 0x21;
}

/// HTTP/3 error codes (RFC 9114 §8.1, RFC 9204 §6).
pub(crate) mod code {
    pub(crate) const NO_ERROR: u64 = 0x100;
    pub(crate) const EXCESSIVE_LOAD: u64 = 0x107;
    pub(crate) const FRAME_UNEXPECTED: u64 = 0x105;
    pub(crate) const FRAME_ERROR: u64 = 0x106;
    pub(crate) const REQUEST_REJECTED: u64 = 0x10b;
    pub(crate) const REQUEST_CANCELLED: u64 = 0x10c;
    pub(crate) const STREAM_CREATION_ERROR: u64 = 0x103;
    pub(crate) const QPACK_DECOMPRESSION_FAILED: u64 = 0x200;
}

/// A self-signed certificate for [`SERVER_NAME`], with its key.
pub(crate) fn certificate() -> (X509, PKey<Private>) {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
    let mut subject = X509NameBuilder::new().unwrap();
    subject
        .append_entry_by_nid(Nid::COMMONNAME, SERVER_NAME)
        .unwrap();
    let subject = subject.build();
    let mut builder = X509::builder().unwrap();
    builder.set_version(2).unwrap();
    let serial = BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
    builder.set_serial_number(&serial).unwrap();
    builder.set_subject_name(&subject).unwrap();
    builder.set_issuer_name(&subject).unwrap();
    builder.set_pubkey(&key).unwrap();
    builder
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&Asn1Time::days_from_now(30).unwrap())
        .unwrap();
    let mut names = SubjectAlternativeName::new();
    names.dns(SERVER_NAME);
    let names = names.build(&builder.x509v3_context(None, None)).unwrap();
    builder.append_extension(&names).unwrap();
    builder.sign(&key, MessageDigest::sha256()).unwrap();
    (builder.build(), key)
}

/// A server TLS context carrying a fresh certificate, as a listener's would.
pub(crate) fn server_tls() -> SslContextBuilder {
    let (certificate, key) = certificate();
    let mut tls = SslContextBuilder::new(SslMethod::tls()).unwrap();
    tls.set_certificate(&certificate).unwrap();
    tls.set_private_key(&key).unwrap();
    tls
}

/// The transport settings both sides of a probe start from: enough credit that flow
/// control is never what a probe runs into unless it means to.
fn transport(config: &mut quiche::Config) {
    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();
    config.set_max_idle_timeout(30_000);
    config.set_initial_max_data(16 << 20);
    config.set_initial_max_stream_data_bidi_local(4 << 20);
    config.set_initial_max_stream_data_bidi_remote(4 << 20);
    config.set_initial_max_stream_data_uni(1 << 20);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.set_disable_active_migration(true);
}

/// A server configuration over `tls`.
pub(crate) fn server_config(tls: SslContextBuilder) -> quiche::Config {
    let mut config =
        quiche::Config::with_boring_ssl_ctx_builder(quiche::PROTOCOL_VERSION, tls).unwrap();
    transport(&mut config);
    config
}

/// A client configuration that does not check the server's certificate: the probes are
/// about QUIC and HTTP/3, not about the trust store.
pub(crate) fn client_config() -> quiche::Config {
    client_config_over(SslContextBuilder::new(SslMethod::tls()).unwrap())
}

/// The same over a TLS context of the caller's, one that shows a certificate of its own.
pub(crate) fn client_config_over(tls: SslContextBuilder) -> quiche::Config {
    let mut config =
        quiche::Config::with_boring_ssl_ctx_builder(quiche::PROTOCOL_VERSION, tls).unwrap();
    transport(&mut config);
    config.verify_peer(false);
    config
}

/// A client asking for `name`, that has sent nothing yet.
pub(crate) fn client_for(name: &str, config: &mut quiche::Config) -> quiche::Connection {
    quiche::connect(
        Some(name),
        &id(0xc1, 16),
        client_addr(),
        server_addr(),
        config,
    )
    .unwrap()
}

/// A connection ID of `len` bytes, all `byte`: distinct enough to be told apart on the
/// wire, and plainly not random.
pub(crate) fn id(byte: u8, len: usize) -> quiche::ConnectionId<'static> {
    quiche::ConnectionId::from_vec(vec![byte; len])
}

/// The client's address unless a test moves it.
pub(crate) fn client_addr() -> SocketAddr {
    "192.0.2.1:4433".parse().unwrap()
}

/// The server's address.
pub(crate) fn server_addr() -> SocketAddr {
    "198.51.100.1:443".parse().unwrap()
}

/// A datagram as it went by.
pub(crate) struct Datagram {
    pub(crate) bytes: Vec<u8>,
    pub(crate) from: SocketAddr,
    pub(crate) to: SocketAddr,
}

impl Datagram {
    /// The destination connection ID of the datagram's first packet, read as a server
    /// reads one: a short header's ID is `dcid_len` long.
    pub(crate) fn dcid(&self, dcid_len: usize) -> Vec<u8> {
        let mut bytes = self.bytes.clone();
        quiche::Header::from_slice(&mut bytes, dcid_len)
            .unwrap()
            .dcid
            .to_vec()
    }
}

/// A client and a server connection, and every datagram that has passed between them.
pub(crate) struct Pipe {
    pub(crate) client: quiche::Connection,
    pub(crate) server: quiche::Connection,
    /// The address the client's datagrams arrive from. A test changes it to rebind.
    pub(crate) client_from: SocketAddr,
    /// Datagrams the client sent, in order.
    pub(crate) to_server: Vec<Datagram>,
    /// Datagrams the server sent, in order.
    pub(crate) to_client: Vec<Datagram>,
}

impl Pipe {
    /// A handshake completed between a new client and a server accepted with `scid`,
    /// under `server` as it is at the moment of the accept.
    pub(crate) fn new(server: &mut quiche::Config, scid: &quiche::ConnectionId) -> Pipe {
        let mut pipe = Pipe::accepted(server, scid);
        pipe.advance();
        assert!(pipe.client.is_established() && pipe.server.is_established());
        pipe
    }

    /// A server accepted with `scid` and a client that has sent nothing yet.
    pub(crate) fn accepted(server: &mut quiche::Config, scid: &quiche::ConnectionId) -> Pipe {
        let client = quiche::connect(
            Some(SERVER_NAME),
            &id(0xc1, 16),
            client_addr(),
            server_addr(),
            &mut client_config(),
        )
        .unwrap();
        let server = quiche::accept(scid, None, server_addr(), client_addr(), server).unwrap();
        Pipe::of(client, server)
    }

    /// Two connections made elsewhere, for probes that stand between them (Retry).
    pub(crate) fn of(client: quiche::Connection, server: quiche::Connection) -> Pipe {
        Pipe {
            client,
            server,
            client_from: client_addr(),
            to_server: Vec::new(),
            to_client: Vec::new(),
        }
    }

    /// Everything the client wants to send, taken and kept; not delivered.
    pub(crate) fn client_flush(&mut self) -> Vec<Datagram> {
        let mut out = Vec::new();
        let mut buf = vec![0; DATAGRAM];
        loop {
            match self.client.send(&mut buf) {
                Ok((len, info)) => out.push(Datagram {
                    bytes: buf[..len].to_vec(),
                    from: self.client_from,
                    to: info.to,
                }),
                Err(quiche::Error::Done) => return out,
                Err(error) => panic!("client send: {error:?}"),
            }
        }
    }

    /// Hands `datagram` to the server.
    pub(crate) fn deliver_to_server(&mut self, datagram: Datagram) {
        let mut bytes = datagram.bytes.clone();
        let info = quiche::RecvInfo {
            from: datagram.from,
            to: datagram.to,
        };
        // A datagram the server rejects is part of what a probe observes, not a failure.
        let _ = self.server.recv(&mut bytes, info);
        self.to_server.push(datagram);
    }

    /// Hands `datagram` to the client. The client always sees its own address: a rebinding
    /// moves the address its datagrams arrive from, as a NAT does, not the client.
    pub(crate) fn deliver_to_client(&mut self, datagram: Datagram) {
        let mut bytes = datagram.bytes.clone();
        let info = quiche::RecvInfo {
            from: datagram.from,
            to: client_addr(),
        };
        let _ = self.client.recv(&mut bytes, info);
        self.to_client.push(datagram);
    }

    /// Moves datagrams both ways until neither side has anything left to send.
    pub(crate) fn advance(&mut self) {
        let mut buf = vec![0; DATAGRAM];
        for _ in 0..ROUNDS {
            let mut moved = false;
            for datagram in self.client_flush() {
                moved = true;
                self.deliver_to_server(datagram);
            }
            loop {
                match self.server.send(&mut buf) {
                    Ok((len, info)) => {
                        moved = true;
                        self.deliver_to_client(Datagram {
                            bytes: buf[..len].to_vec(),
                            from: info.from,
                            to: info.to,
                        });
                    }
                    Err(quiche::Error::Done) => break,
                    Err(error) => panic!("server send: {error:?}"),
                }
            }
            if !moved {
                return;
            }
        }
        panic!("the two sides never went quiet");
    }

    /// Every HTTP/3 event `server` has for the server side, until it has none, with the
    /// error that ended the poll if it was not `Done`.
    pub(crate) fn server_events(
        &mut self,
        server: &mut quiche::h3::Connection,
    ) -> (Vec<(u64, quiche::h3::Event)>, Option<quiche::h3::Error>) {
        poll_all(server, &mut self.server)
    }

    /// Every HTTP/3 event `client` has for the client side, until it has none.
    pub(crate) fn client_events(
        &mut self,
        client: &mut quiche::h3::Connection,
    ) -> (Vec<(u64, quiche::h3::Event)>, Option<quiche::h3::Error>) {
        poll_all(client, &mut self.client)
    }

    /// Opens the client's HTTP/3 control stream by hand, with an empty SETTINGS: what a
    /// raw peer must do before a server's HTTP/3 layer will take its requests.
    pub(crate) fn raw_client_control(&mut self) {
        let mut bytes = varint(stream_type::CONTROL);
        bytes.extend(frame_bytes(frame::SETTINGS, &[]));
        self.client.stream_send(2, &bytes, false).unwrap();
    }

    /// Writes `bytes` on the client's stream `stream`, as they are. quiche takes only what
    /// its congestion window allows, so the rest waits for datagrams to move; the server's
    /// HTTP/3 layer is not polled meanwhile, so what is written must fit the stream's credit.
    pub(crate) fn raw_client_send(&mut self, stream: u64, bytes: &[u8], fin: bool) {
        let mut written = 0;
        for _ in 0..ROUNDS {
            match self.client.stream_send(stream, &bytes[written..], fin) {
                Ok(taken) => written += taken,
                Err(quiche::Error::Done) => {}
                Err(error) => panic!("client stream_send: {error:?}"),
            }
            if written == bytes.len() {
                return;
            }
            self.advance();
        }
        panic!("the probe ran out of stream credit");
    }
}

fn poll_all(
    h3: &mut quiche::h3::Connection,
    conn: &mut quiche::Connection,
) -> (Vec<(u64, quiche::h3::Event)>, Option<quiche::h3::Error>) {
    let mut events = Vec::new();
    loop {
        match h3.poll(conn) {
            Ok(event) => events.push(event),
            Err(quiche::h3::Error::Done) => return (events, None),
            Err(error) => return (events, Some(error)),
        }
    }
}

/// A QUIC variable-length integer (RFC 9000 §16).
pub(crate) fn varint(value: u64) -> Vec<u8> {
    match value {
        0..=0x3f => vec![value as u8],
        0x40..=0x3fff => ((value as u16) | 0x4000).to_be_bytes().to_vec(),
        0x4000..=0x3fff_ffff => ((value as u32) | 0x8000_0000).to_be_bytes().to_vec(),
        _ => (value | 0xc000_0000_0000_0000).to_be_bytes().to_vec(),
    }
}

/// The type and length that start a frame declaring `len` bytes of payload.
pub(crate) fn frame_head(kind: u64, len: u64) -> Vec<u8> {
    let mut bytes = varint(kind);
    bytes.extend(varint(len));
    bytes
}

/// A whole frame.
pub(crate) fn frame_bytes(kind: u64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = frame_head(kind, payload.len() as u64);
    bytes.extend_from_slice(payload);
    bytes
}

/// A QPACK integer with an `n`-bit prefix, the prefix's other bits being `flags`
/// (RFC 7541 §5.1, which RFC 9204 §4.1.1 uses).
fn prefixed(flags: u8, n: u32, value: usize, out: &mut Vec<u8>) {
    let max = (1usize << n) - 1;
    if value < max {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | max as u8);
    let mut rest = value - max;
    while rest >= 0x80 {
        out.push((rest as u8 & 0x7f) | 0x80);
        rest >>= 7;
    }
    out.push(rest as u8);
}

/// A field section as QPACK encodes it with nothing but literals: no table, no Huffman.
pub(crate) fn field_section(fields: &[(&str, &str)]) -> Vec<u8> {
    // Required Insert Count 0 and Base 0: nothing refers to the dynamic table.
    let mut bytes = vec![0, 0];
    for (name, value) in fields {
        // Literal Field Line with Literal Name (RFC 9204 §4.5.6): 001, N = 0, H = 0.
        prefixed(0b0010_0000, 3, name.len(), &mut bytes);
        bytes.extend_from_slice(name.as_bytes());
        prefixed(0, 7, value.len(), &mut bytes);
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes
}

/// A HEADERS frame carrying `fields`.
pub(crate) fn headers(fields: &[(&str, &str)]) -> Vec<u8> {
    frame_bytes(frame::HEADERS, &field_section(fields))
}

/// The head of a GET for `/`, as a request stream starts.
pub(crate) fn get() -> Vec<(&'static str, &'static str)> {
    vec![
        (":method", "GET"),
        (":scheme", "https"),
        (":authority", SERVER_NAME),
        (":path", "/"),
    ]
}

/// `fields` as quiche's header type, for its own client and server.
pub(crate) fn h3_headers(fields: &[(&str, &str)]) -> Vec<quiche::h3::Header> {
    fields
        .iter()
        .map(|(name, value)| quiche::h3::Header::new(name.as_bytes(), value.as_bytes()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_take_the_shortest_form() {
        assert_eq!(varint(37), [0x25]);
        assert_eq!(varint(15_293), [0x7b, 0xbd]);
        assert_eq!(varint(494_878_333), [0x9d, 0x7f, 0x3e, 0x7d]);
        assert_eq!(
            varint(151_288_809_941_952_652),
            [0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c]
        );
    }

    #[test]
    fn prefixed_integers_continue_past_the_prefix() {
        let mut out = Vec::new();
        prefixed(0, 5, 1_337, &mut out);
        assert_eq!(out, [0x1f, 0x9a, 0x0a]);
    }
}
