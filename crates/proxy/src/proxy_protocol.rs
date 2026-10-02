//! The PROXY protocol's header, versions 1 and 2 ([20](../../../docs/20-proxy-protocol.md)):
//! read at the start of a connection from a load balancer that says who its client is, and
//! written ahead of a tunnel's bytes to a backend that asks to be told.
//!
//! [`read`] is pure. It is handed every byte the client has sent so far and says whether
//! that is not enough yet, a whole header and its length, or no header to take. Asked again
//! with more bytes, it reads them all again: a header is read once a connection, and a v1
//! line is at most [`V1_LIMIT`] bytes and a v2 header's part that is read at most [`HELD`],
//! so a reader that keeps nothing between calls costs little and cannot be led to another
//! answer by the same bytes cut differently.
//!
//! It holds a header to the spec (HAProxy's `doc/proxy-protocol.txt`) where HAProxy and
//! NGINX do not: v1's fields one space apart, no leading zeros, ports up to 65,535, the
//! address its family names; v1 `UNKNOWN` in its long form as well as its short; v2's
//! version, command, family and transport each one the spec names; a v2 `LOCAL` header
//! skipped by its whole length, whatever its family byte says. TLVs are skipped unread.
//!
//! The writers make what a tunnel sends: a v1 `TCP4` or `TCP6` line, or a v2 `PROXY`
//! header over `STREAM` with no TLVs; and what the gateway's own connections send, v2's
//! `LOCAL`, or a v1 line of their own two addresses.

// Reached only by its own tests and the fuzz targets until a listener reads a header
// (20, step 3). An expectation and not an allowance, so that the day a caller appears the
// compiler says this line has served its purpose.
#![cfg_attr(
    not(any(test, feature = "fuzzing")),
    expect(
        dead_code,
        reason = "reached only by its own tests until a listener reads a header"
    )
)]
// What is here is `pub` so that the fuzz targets, a crate of their own, can name it. The
// module is public only when they are being built, so in an ordinary build none of this is
// API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

/// A v2 header's first 12 bytes. It holds a NUL and a `QUIT`, so it reads as no other
/// protocol's start.
pub const SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";
/// The longest v1 line, CRLF included: `UNKNOWN` with two IPv6 addresses written out in
/// full.
pub const V1_LIMIT: usize = 107;
/// The most of a v2 header ever needed to read it: its fixed part and the largest address
/// block, two UNIX paths. Whatever its length says beyond that is TLVs, skipped by count.
pub const HELD: usize = V2_FIXED + UNIX_BLOCK;

/// How every v1 line starts.
const V1_START: &[u8] = b"PROXY ";
/// A v2 header's signature, version and command, family and transport, and length.
const V2_FIXED: usize = 16;
const VERSION_2: u8 = 2;
const LOCAL: u8 = 0;
const PROXY: u8 = 1;
const UNSPEC: u8 = 0;
const INET: u8 = 1;
const INET6: u8 = 2;
const UNIX: u8 = 3;
const STREAM: u8 = 1;
const DGRAM: u8 = 2;
const INET_BLOCK: usize = 2 * 4 + 2 * 2;
const INET6_BLOCK: usize = 2 * 16 + 2 * 2;
const UNIX_BLOCK: usize = 2 * 108;

/// What a whole header says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Header {
    /// The connection was made for a client: its address and the address it connected to.
    Proxied {
        /// The client.
        source: SocketAddr,
        /// Where the client connected: the load balancer's address, as the client saw it.
        destination: SocketAddr,
    },
    /// The connection's own two ends stand: v1 `UNKNOWN`, v2 `LOCAL`, or a v2 `PROXY`
    /// header whose family or transport is one the spec has a receiver fall back from
    /// (`UNSPEC`, `UNIX`, `DGRAM`).
    Local,
}

/// What the bytes a connection has sent so far come to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    /// Not enough yet.
    More,
    /// A whole header, `length` bytes long. A v2 header's TLVs are not needed to read it,
    /// so `length` may be more than has come: the caller skips the rest by count before it
    /// takes anything after the header.
    Whole {
        /// What it says.
        header: Header,
        /// Its length in bytes, from the first.
        length: usize,
    },
    /// No header to take.
    Refused(Refusal),
}

/// Why a connection's first bytes are not a header to take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// They start as neither version does.
    #[error("no PROXY header")]
    Missing,
    /// A v1 line that is not one: not ended by CRLF within 107 bytes, a lone CR or LF, or
    /// fields that are not the spec's.
    #[error("a malformed PROXY v1 line")]
    Line,
    /// A v2 signature with another version.
    #[error("a PROXY v2 signature with a version other than 2")]
    Version,
    /// A v2 command other than `LOCAL` and `PROXY`.
    #[error("a PROXY v2 command other than LOCAL and PROXY")]
    Command,
    /// A v2 family or transport the spec does not name.
    #[error("a PROXY v2 family or transport the spec does not name")]
    Family,
    /// A v2 length short of the address block its family needs.
    #[error("a PROXY v2 header too short for its addresses")]
    Short,
}

impl Refusal {
    /// Whether the bytes began as no header at all, rather than as one that is wrong.
    #[must_use]
    pub fn is_missing(self) -> bool {
        self == Self::Missing
    }
}

/// Reads the header at the start of `bytes`, everything a connection has sent so far.
#[must_use]
pub fn read(bytes: &[u8]) -> Read {
    if bytes.starts_with(&SIGNATURE) {
        return v2(bytes);
    }
    if bytes.starts_with(V1_START) {
        return v1(bytes);
    }
    // Not yet enough to tell, nothing at all included.
    if SIGNATURE.starts_with(bytes) || V1_START.starts_with(bytes) {
        return Read::More;
    }
    Read::Refused(Refusal::Missing)
}

/// A v1 line: `PROXY`, then its fields one space apart, then CRLF, within [`V1_LIMIT`].
fn v1(bytes: &[u8]) -> Read {
    let seen = bytes.get(..V1_LIMIT).unwrap_or(bytes);
    let full = bytes.len() >= V1_LIMIT;
    let refused = Read::Refused(Refusal::Line);
    // The first CR or LF must be the line's end, CR then LF: neither may come alone.
    let Some(at) = memchr::memchr2(b'\r', b'\n', seen) else {
        return if full { refused } else { Read::More };
    };
    if seen[at] == b'\n' {
        return refused;
    }
    match seen.get(at + 1) {
        Some(b'\n') => {}
        Some(_) => return refused,
        None if full => return refused,
        None => return Read::More,
    }
    match line(&seen[V1_START.len()..at]) {
        Some(header) => Read::Whole {
            header,
            length: at + 2,
        },
        None => refused,
    }
}

/// A v1 line's fields, between `PROXY ` and CRLF.
fn line(fields: &[u8]) -> Option<Header> {
    // The rest of an `UNKNOWN` line, if any, is the receiver's to ignore.
    if let Some(rest) = fields.strip_prefix(b"UNKNOWN") {
        return (rest.is_empty() || rest[0] == b' ').then_some(Header::Local);
    }
    // Two spaces in a row, or one at either end, make an empty field, which nothing takes.
    let mut fields = fields.split(|&byte| byte == b' ');
    let family = fields.next()?;
    let (source, destination) = (fields.next()?, fields.next()?);
    let (source_port, destination_port) = (fields.next()?, fields.next()?);
    if fields.next().is_some() {
        return None;
    }
    let (source, destination) = match family {
        b"TCP4" => (
            IpAddr::V4(text::<Ipv4Addr>(source)?),
            IpAddr::V4(text::<Ipv4Addr>(destination)?),
        ),
        b"TCP6" => (
            IpAddr::V6(text::<Ipv6Addr>(source)?),
            IpAddr::V6(text::<Ipv6Addr>(destination)?),
        ),
        _ => return None,
    };
    Some(Header::Proxied {
        source: SocketAddr::new(source, port(source_port)?),
        destination: SocketAddr::new(destination, port(destination_port)?),
    })
}

/// An address as text. The standard library's readings are the spec's: IPv4 as four
/// decimal octets with no leading zeros, IPv6 in any of its forms and either case.
fn text<T: FromStr>(field: &[u8]) -> Option<T> {
    std::str::from_utf8(field).ok()?.parse().ok()
}

/// A port as text: decimal, no sign, no leading zero, at most 65,535.
fn port(field: &[u8]) -> Option<u16> {
    let leading_zero = field.len() > 1 && field[0] == b'0';
    if field.is_empty() || field.len() > 5 || leading_zero {
        return None;
    }
    let mut value: u32 = 0;
    for &digit in field {
        if !digit.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(digit - b'0');
    }
    u16::try_from(value).ok()
}

/// A v2 header: what its fixed part says, then its addresses where they are to be read.
fn v2(bytes: &[u8]) -> Read {
    let Some(fixed) = bytes.get(..V2_FIXED) else {
        return Read::More;
    };
    if fixed[12] >> 4 != VERSION_2 {
        return Read::Refused(Refusal::Version);
    }
    let length = V2_FIXED + usize::from(u16::from_be_bytes([fixed[14], fixed[15]]));
    match fixed[12] & 0x0f {
        // Its family and transport are the spec's to ignore, and its length to skip.
        LOCAL => {
            return Read::Whole {
                header: Header::Local,
                length,
            };
        }
        PROXY => {}
        _ => return Read::Refused(Refusal::Command),
    }
    let (family, transport) = (fixed[13] >> 4, fixed[13] & 0x0f);
    if family > UNIX || transport > DGRAM {
        return Read::Refused(Refusal::Family);
    }
    let local = Read::Whole {
        header: Header::Local,
        length,
    };
    // Unspecified: the address information, if any, is the receiver's to ignore.
    if family == UNSPEC || transport == UNSPEC {
        return local;
    }
    let block = match family {
        INET => INET_BLOCK,
        INET6 => INET6_BLOCK,
        _ => UNIX_BLOCK,
    };
    if length < V2_FIXED + block {
        return Read::Refused(Refusal::Short);
    }
    // Valid, and not taken: the spec has a receiver fall back to the connection's own ends.
    if family == UNIX || transport == DGRAM {
        return local;
    }
    let Some(addresses) = bytes.get(V2_FIXED..V2_FIXED + block) else {
        return Read::More;
    };
    let (source, destination, ports) = if family == INET {
        let source: [u8; 4] = addresses[..4].try_into().unwrap_or_default();
        let destination: [u8; 4] = addresses[4..8].try_into().unwrap_or_default();
        (
            IpAddr::from(source),
            IpAddr::from(destination),
            &addresses[8..],
        )
    } else {
        let source: [u8; 16] = addresses[..16].try_into().unwrap_or_default();
        let destination: [u8; 16] = addresses[16..32].try_into().unwrap_or_default();
        (
            IpAddr::from(source),
            IpAddr::from(destination),
            &addresses[32..],
        )
    };
    Read::Whole {
        header: Header::Proxied {
            source: SocketAddr::new(source, u16::from_be_bytes([ports[0], ports[1]])),
            destination: SocketAddr::new(destination, u16::from_be_bytes([ports[2], ports[3]])),
        },
        length,
    }
}

/// Which version a backend is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// The text line.
    V1,
    /// The binary header.
    V2,
}

/// A header made to be sent, held where it was made: no allocation.
#[derive(Clone, Copy)]
pub struct Written {
    bytes: [u8; V1_LIMIT],
    length: usize,
}

impl Written {
    fn new() -> Self {
        Self {
            bytes: [0; V1_LIMIT],
            length: 0,
        }
    }

    fn put(&mut self, bytes: &[u8]) {
        // Never short: the longest header, a v1 line of two IPv6 addresses, is 104 bytes.
        let end = (self.length + bytes.len()).min(V1_LIMIT);
        let take = end - self.length;
        self.bytes[self.length..end].copy_from_slice(&bytes[..take]);
        self.length = end;
    }

    /// The header's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}

impl std::fmt::Debug for Written {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Written")
            .field(&self.as_bytes().escape_ascii().to_string())
            .finish()
    }
}

impl std::fmt::Write for Written {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.put(text.as_bytes());
        Ok(())
    }
}

/// The header that tells a backend a connection is `source`'s, made to `destination`.
/// The two are written in one family: both IPv4 when both are (an IPv4 address of a
/// dual-stack socket counts as one), else both IPv6, an IPv4 one as IPv4-mapped.
#[must_use]
pub fn proxied(version: Version, source: SocketAddr, destination: SocketAddr) -> Written {
    let (source_ip, destination_ip) = (source.ip().to_canonical(), destination.ip().to_canonical());
    let pair = match (source_ip, destination_ip) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => Pair::V4(source, destination),
        _ => Pair::V6(v6(source_ip), v6(destination_ip)),
    };
    let ports = (source.port(), destination.port());
    match version {
        Version::V1 => line_of(pair, ports),
        Version::V2 => binary(pair, ports),
    }
}

/// The header a connection of the gateway's own sends, from `ours` to `theirs`: v2's
/// `LOCAL`; in v1, a line of the connection's own two addresses, which the spec asks for
/// in place of `UNKNOWN`, which some receivers refuse.
#[must_use]
pub fn own(version: Version, ours: SocketAddr, theirs: SocketAddr) -> Written {
    match version {
        Version::V1 => proxied(Version::V1, ours, theirs),
        Version::V2 => {
            let mut written = Written::new();
            written.put(&SIGNATURE);
            written.put(&[VERSION_2 << 4 | LOCAL, UNSPEC, 0, 0]);
            written
        }
    }
}

/// Two addresses of one family.
#[derive(Clone, Copy)]
enum Pair {
    V4(Ipv4Addr, Ipv4Addr),
    V6(Ipv6Addr, Ipv6Addr),
}

fn v6(address: IpAddr) -> Ipv6Addr {
    match address {
        IpAddr::V4(address) => address.to_ipv6_mapped(),
        IpAddr::V6(address) => address,
    }
}

fn line_of(pair: Pair, (source_port, destination_port): (u16, u16)) -> Written {
    let mut written = Written::new();
    // Into the header's own bytes, which never run out (see `put`), so it cannot fail.
    let _infallible = match pair {
        Pair::V4(source, destination) => write!(
            written,
            "PROXY TCP4 {source} {destination} {source_port} {destination_port}\r\n"
        ),
        Pair::V6(source, destination) => write!(
            written,
            "PROXY TCP6 {source} {destination} {source_port} {destination_port}\r\n"
        ),
    };
    written
}

fn binary(pair: Pair, (source_port, destination_port): (u16, u16)) -> Written {
    let mut written = Written::new();
    written.put(&SIGNATURE);
    let (family, block) = match pair {
        Pair::V4(..) => (INET, INET_BLOCK),
        Pair::V6(..) => (INET6, INET6_BLOCK),
    };
    let [high, low] = u16::try_from(block).unwrap_or_default().to_be_bytes();
    written.put(&[VERSION_2 << 4 | PROXY, family << 4 | STREAM, high, low]);
    match pair {
        Pair::V4(source, destination) => {
            written.put(&source.octets());
            written.put(&destination.octets());
        }
        Pair::V6(source, destination) => {
            written.put(&source.octets());
            written.put(&destination.octets());
        }
    }
    written.put(&source_port.to_be_bytes());
    written.put(&destination_port.to_be_bytes());
    written
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn at(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    fn proxied(source: &str, destination: &str) -> Header {
        Header::Proxied {
            source: at(source),
            destination: at(destination),
        }
    }

    fn whole(header: Header, length: usize) -> Read {
        Read::Whole { header, length }
    }

    fn refused(refusal: Refusal) -> Read {
        Read::Refused(refusal)
    }

    /// A v2 header's fixed part: `version_command`, `family_transport`, and the length of
    /// what follows.
    fn fixed(version_command: u8, family_transport: u8, length: u16) -> Vec<u8> {
        let mut bytes = SIGNATURE.to_vec();
        bytes.push(version_command);
        bytes.push(family_transport);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes
    }

    /// A v2 `PROXY` header for TCP over IPv4, with `tlvs` after its addresses.
    fn v2_inet(tlvs: &[u8]) -> Vec<u8> {
        let mut bytes = fixed(0x21, 0x11, u16::try_from(12 + tlvs.len()).unwrap());
        bytes.extend_from_slice(&[192, 0, 2, 1, 198, 51, 100, 7]);
        bytes.extend_from_slice(&56324_u16.to_be_bytes());
        bytes.extend_from_slice(&443_u16.to_be_bytes());
        bytes.extend_from_slice(tlvs);
        bytes
    }

    #[test]
    fn a_v1_tcp4_line_is_read_and_what_follows_is_not() {
        let line = "PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n";
        let bytes = format!("{line}GET / HTTP/1.1\r\n");
        assert_eq!(
            read(bytes.as_bytes()),
            whole(proxied("192.168.0.1:56324", "192.168.0.11:443"), line.len())
        );
    }

    #[test]
    fn a_v1_tcp6_line_is_read_in_either_case() {
        let line = b"PROXY TCP6 2001:DB8::1 2001:db8:0:0:0:0:0:2 1 65535\r\n";
        assert_eq!(
            read(line),
            whole(
                proxied("[2001:db8::1]:1", "[2001:db8::2]:65535"),
                line.len()
            )
        );
    }

    #[test]
    fn v1_unknown_is_taken_short_and_long() {
        assert_eq!(read(b"PROXY UNKNOWN\r\n"), whole(Header::Local, 15));
        let long: &[u8] = b"PROXY UNKNOWN ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff 65535 65535\r\n";
        assert_eq!(long.len(), V1_LIMIT);
        assert_eq!(read(long), whole(Header::Local, V1_LIMIT));
        // What follows UNKNOWN is the receiver's to ignore, however it is spaced.
        assert_eq!(
            read(b"PROXY UNKNOWN  anything\r\n"),
            whole(Header::Local, 25)
        );
        assert_eq!(read(b"PROXY UNKNOWNX\r\n"), refused(Refusal::Line));
    }

    #[test]
    fn a_v1_line_must_end_within_107_bytes() {
        let mut long = b"PROXY UNKNOWN ".to_vec();
        long.resize(V1_LIMIT - 2, b'x');
        // 105 bytes: CR and LF still fit.
        assert_eq!(read(&long), Read::More);
        long.push(b'x');
        // 106: only a CR fits, and the LF would be the 108th byte.
        assert_eq!(read(&long), Read::More);
        let mut ended = long.clone();
        ended.push(b'\r');
        assert_eq!(read(&ended), refused(Refusal::Line));
        long.push(b'x');
        assert_eq!(read(&long), refused(Refusal::Line));
        long.extend_from_slice(b"\r\n");
        assert_eq!(read(&long), refused(Refusal::Line));
    }

    #[test]
    fn a_lone_cr_or_lf_is_refused() {
        for line in [
            &b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\n"[..],
            b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\rx\n",
            b"PROXY TCP4 192.0.2.1\n 192.0.2.2 1 2\r\n",
            b"PROXY TCP4 192.0.2.1\r 192.0.2.2 1 2\r\n",
            b"PROXY UNKNOWN\n\r\n",
        ] {
            assert_eq!(
                read(line),
                refused(Refusal::Line),
                "{}",
                line.escape_ascii()
            );
        }
    }

    #[test]
    fn v1_fields_are_held_to_the_spec() {
        for line in [
            // Spacing.
            "PROXY TCP4  192.0.2.1 192.0.2.2 1 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 1 2 \r\n",
            "PROXY  TCP4 192.0.2.1 192.0.2.2 1 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 1\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 1 2 3\r\n",
            "PROXY TCP4 192.0.2.1\t192.0.2.2 1 2\r\n",
            // Addresses.
            "PROXY TCP4 192.0.2.01 192.0.2.2 1 2\r\n",
            "PROXY TCP4 192.0.2.256 192.0.2.2 1 2\r\n",
            "PROXY TCP4 2001:db8::1 192.0.2.2 1 2\r\n",
            "PROXY TCP6 192.0.2.1 2001:db8::2 1 2\r\n",
            "PROXY TCP6 [2001:db8::1] 2001:db8::2 1 2\r\n",
            "PROXY TCP6 2001:db8::1%1 2001:db8::2 1 2\r\n",
            // Ports.
            "PROXY TCP4 192.0.2.1 192.0.2.2 01 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 65536 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 +1 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 100000 2\r\n",
            "PROXY TCP4 192.0.2.1 192.0.2.2 -1 2\r\n",
            // Families.
            "PROXY TCP5 192.0.2.1 192.0.2.2 1 2\r\n",
            "PROXY tcp4 192.0.2.1 192.0.2.2 1 2\r\n",
            "PROXY UDP4 192.0.2.1 192.0.2.2 1 2\r\n",
            "PROXY unknown\r\n",
            "PROXY \r\n",
        ] {
            assert_eq!(read(line.as_bytes()), refused(Refusal::Line), "{line:?}");
        }
        assert_eq!(
            read(b"PROXY TCP4 0.0.0.0 255.255.255.255 0 65535\r\n"),
            whole(proxied("0.0.0.0:0", "255.255.255.255:65535"), 44)
        );
    }

    #[test]
    fn what_starts_as_neither_version_is_no_header() {
        for bytes in [
            &b"GET / HTTP/1.1\r\n"[..],
            b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
            &[0x16, 0x03, 0x01, 0x02, 0x00][..],
            b"proxy TCP4 192.0.2.1 192.0.2.2 1 2\r\n",
            b"PROXYTCP4",
            b"\r\n\r\n\0\r\nQUIT\r",
            b"\r\n\r\n\n",
        ] {
            assert_eq!(
                read(bytes),
                refused(Refusal::Missing),
                "{}",
                bytes.escape_ascii()
            );
        }
    }

    #[test]
    fn too_little_to_tell_is_more() {
        for bytes in [&b""[..], b"P", b"PROXY", b"\r", b"\r\n\r\n\0\r\nQUI"] {
            assert_eq!(read(bytes), Read::More, "{}", bytes.escape_ascii());
        }
    }

    #[test]
    fn a_v2_tcp4_header_is_read_and_what_follows_is_not() {
        let mut bytes = v2_inet(&[]);
        bytes.extend_from_slice(b"\x16\x03\x01");
        assert_eq!(
            read(&bytes),
            whole(proxied("192.0.2.1:56324", "198.51.100.7:443"), 28)
        );
    }

    #[test]
    fn a_v2_tcp6_header_is_read() {
        let mut bytes = fixed(0x21, 0x21, 36);
        let source: Ipv6Addr = "2001:db8::1".parse().unwrap();
        let destination: Ipv6Addr = "2001:db8::2".parse().unwrap();
        bytes.extend_from_slice(&source.octets());
        bytes.extend_from_slice(&destination.octets());
        bytes.extend_from_slice(&[0, 1, 0, 2]);
        assert_eq!(
            read(&bytes),
            whole(proxied("[2001:db8::1]:1", "[2001:db8::2]:2"), 52)
        );
    }

    #[test]
    fn tlvs_are_skipped_by_count_and_need_not_have_come() {
        // An AUTHORITY TLV, then NOOP padding: neither read, both counted.
        let tlvs = b"\x02\x00\x0bexample.com\x04\x00\x03\0\0\0";
        let bytes = v2_inet(tlvs);
        let header = proxied("192.0.2.1:56324", "198.51.100.7:443");
        assert_eq!(read(&bytes), whole(header, 28 + tlvs.len()));
        // The addresses alone decide it.
        assert_eq!(read(&bytes[..28]), whole(header, 28 + tlvs.len()));
        assert_eq!(read(&bytes[..27]), Read::More);
        // The longest a length can say.
        let mut longest = fixed(0x21, 0x11, u16::MAX);
        longest.extend_from_slice(&bytes[16..28]);
        assert_eq!(read(&longest), whole(header, V2_FIXED + 65_535));
    }

    #[test]
    fn local_ignores_its_family_byte_and_skips_its_whole_length() {
        assert_eq!(read(&fixed(0x20, 0x00, 0)), whole(Header::Local, 16));
        // Whatever its family byte says, even what no PROXY header may say, and an address
        // block it should not have sent: skipped.
        let mut odd = fixed(0x20, 0xff, 12);
        odd.extend_from_slice(&[1; 12]);
        assert_eq!(read(&odd), whole(Header::Local, 28));
        assert_eq!(read(&fixed(0x20, 0x11, 300)), whole(Header::Local, 316));
    }

    #[test]
    fn a_unix_header_is_the_most_ever_held() {
        let length = u16::try_from(HELD - V2_FIXED).unwrap();
        assert_eq!(read(&fixed(0x21, 0x31, length)), whole(Header::Local, HELD));
    }

    #[test]
    fn only_bytes_that_start_as_neither_version_are_missing() {
        assert!(Refusal::Missing.is_missing());
        for refusal in [
            Refusal::Line,
            Refusal::Version,
            Refusal::Command,
            Refusal::Family,
            Refusal::Short,
        ] {
            assert!(!refusal.is_missing());
        }
    }

    #[test]
    fn v2_families_not_taken_fall_back_to_the_connections_own_ends() {
        // UNSPEC either way, UDP, and UNIX: the spec's fallback, whatever follows.
        for (family, length) in [
            (0x00, 0),
            (0x00, 12),
            (0x01, 0),
            (0x10, 0),
            (0x20, 0),
            (0x30, 0),
            (0x12, 12),
            (0x22, 36),
            (0x31, 216),
            (0x32, 216),
        ] {
            let bytes = fixed(0x21, family, length);
            assert_eq!(
                read(&bytes),
                whole(Header::Local, 16 + usize::from(length)),
                "family {family:#04x}"
            );
        }
    }

    #[test]
    fn v2_is_held_to_the_spec() {
        // Versions other than 2; v1's own number included.
        for version in [0x01, 0x11, 0x31, 0xf1] {
            assert_eq!(read(&fixed(version, 0x11, 12)), refused(Refusal::Version));
        }
        for command in [0x22, 0x2f] {
            assert_eq!(read(&fixed(command, 0x11, 12)), refused(Refusal::Command));
        }
        for family in [0x41, 0xf1, 0x13, 0x1f, 0x03] {
            assert_eq!(
                read(&fixed(0x21, family, 12)),
                refused(Refusal::Family),
                "{family:#04x}"
            );
        }
        // Lengths short of the address block.
        for (family, length) in [(0x11, 11), (0x11, 0), (0x21, 35), (0x12, 11), (0x31, 215)] {
            assert_eq!(
                read(&fixed(0x21, family, length)),
                refused(Refusal::Short),
                "{family:#04x}"
            );
        }
    }

    #[test]
    fn every_part_of_a_header_short_of_deciding_is_more() {
        let v2 = v2_inet(&[]);
        let local = fixed(0x20, 0, 0);
        let headers: [&[u8]; 4] = [
            b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\r\n",
            b"PROXY UNKNOWN\r\n",
            &v2,
            &local,
        ];
        for header in headers {
            for cut in 0..header.len() {
                let part = &header[..cut];
                assert_eq!(read(part), Read::More, "{}", part.escape_ascii());
            }
        }
    }

    #[test]
    fn v1_lines_are_written_as_the_spec_shows_them() {
        let line = |source, destination| {
            super::proxied(Version::V1, at(source), at(destination))
                .as_bytes()
                .to_vec()
        };
        assert_eq!(
            line("192.168.0.1:56324", "192.168.0.11:443"),
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n"
        );
        assert_eq!(
            line("[2001:db8::1]:1", "[2001:db8::2]:2"),
            b"PROXY TCP6 2001:db8::1 2001:db8::2 1 2\r\n"
        );
        // An IPv4 client of a dual-stack socket is IPv4.
        assert_eq!(
            line("[::ffff:192.0.2.1]:1", "[::ffff:192.0.2.2]:2"),
            b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\r\n"
        );
        // Two families: both written as IPv6.
        assert_eq!(
            line("192.0.2.1:1", "[2001:db8::2]:2"),
            b"PROXY TCP6 ::ffff:192.0.2.1 2001:db8::2 1 2\r\n"
        );
        // The longest a written line can be.
        let longest = "[1111:2222:3333:4444:5555:6666:7777:8888]:65535";
        let written = line(longest, longest);
        assert_eq!(written.len(), 104);
        assert_eq!(read(&written), whole(proxied(longest, longest), 104));
    }

    #[test]
    fn v2_headers_are_written_byte_for_byte() {
        let written = super::proxied(Version::V2, at("192.0.2.1:56324"), at("198.51.100.7:443"));
        assert_eq!(written.as_bytes(), v2_inet(&[]));
        let written = super::proxied(Version::V2, at("[2001:db8::1]:1"), at("192.0.2.2:2"));
        assert_eq!(written.as_bytes().len(), 52);
        assert_eq!(
            read(written.as_bytes()),
            whole(proxied("[2001:db8::1]:1", "[::ffff:192.0.2.2]:2"), 52)
        );
    }

    #[test]
    fn the_gateways_own_connections_say_so() {
        let (ours, theirs) = (at("10.0.0.5:40000"), at("10.0.1.9:5432"));
        let local = own(Version::V2, ours, theirs);
        assert_eq!(local.as_bytes(), fixed(0x20, 0x00, 0));
        assert_eq!(read(local.as_bytes()), whole(Header::Local, 16));
        // v1 names the connection's own ends rather than UNKNOWN, which some refuse.
        let line = own(Version::V1, ours, theirs);
        assert_eq!(
            line.as_bytes(),
            b"PROXY TCP4 10.0.0.5 10.0.1.9 40000 5432\r\n"
        );
    }

    /// The plainest reading of the spec, to hold `read` to: the whole of what has come,
    /// read with the standard library's string tools and a table of v2's protocol bytes.
    fn reference(bytes: &[u8]) -> Read {
        let starts = |start: &[u8]| bytes.len() >= start.len() && &bytes[..start.len()] == start;
        let begins = |start: &[u8]| bytes.len() < start.len() && start[..bytes.len()] == *bytes;
        if starts(&SIGNATURE) {
            reference_v2(bytes)
        } else if starts(b"PROXY ") {
            reference_v1(bytes)
        } else if begins(&SIGNATURE) || begins(b"PROXY ") {
            Read::More
        } else {
            refused(Refusal::Missing)
        }
    }

    fn reference_v1(bytes: &[u8]) -> Read {
        let window = &bytes[..bytes.len().min(107)];
        let end = window.windows(2).position(|pair| pair == b"\r\n");
        let first_break = window.iter().position(|&b| b == b'\r' || b == b'\n');
        let end = match (end, first_break) {
            (Some(end), Some(first)) if end == first => end,
            (None, None) if bytes.len() < 107 => return Read::More,
            (None, Some(first))
                if bytes.len() < 107 && first == window.len() - 1 && window[first] == b'\r' =>
            {
                return Read::More;
            }
            _ => return refused(Refusal::Line),
        };
        // Whatever follows UNKNOWN is ignored, UTF-8 or not.
        if bytes[6..end].split(|&b| b == b' ').next() == Some(b"UNKNOWN") {
            return whole(Header::Local, end + 2);
        }
        let Ok(text) = std::str::from_utf8(&bytes[6..end]) else {
            return refused(Refusal::Line);
        };
        let fields: Vec<&str> = text.split(' ').collect();
        let port = |field: &str| {
            field
                .parse::<u16>()
                .ok()
                .filter(|port| port.to_string() == field)
        };
        let pair = |source: &str, destination: &str, v4: bool| -> Option<(IpAddr, IpAddr)> {
            if v4 {
                Some((
                    IpAddr::V4(source.parse().ok()?),
                    IpAddr::V4(destination.parse().ok()?),
                ))
            } else {
                Some((
                    IpAddr::V6(source.parse().ok()?),
                    IpAddr::V6(destination.parse().ok()?),
                ))
            }
        };
        let parsed = match fields[..] {
            [
                family @ ("TCP4" | "TCP6"),
                source,
                destination,
                source_port,
                destination_port,
            ] => pair(source, destination, family == "TCP4").and_then(|(source, destination)| {
                Some((
                    SocketAddr::new(source, port(source_port)?),
                    SocketAddr::new(destination, port(destination_port)?),
                ))
            }),
            _ => None,
        };
        match parsed {
            Some((source, destination)) => whole(
                Header::Proxied {
                    source,
                    destination,
                },
                end + 2,
            ),
            None => refused(Refusal::Line),
        }
    }

    fn reference_v2(bytes: &[u8]) -> Read {
        if bytes.len() < 16 {
            return Read::More;
        }
        let length = 16 + usize::from(bytes[14]) * 256 + usize::from(bytes[15]);
        match bytes[12] {
            0x20 => return whole(Header::Local, length),
            0x21 => {}
            byte if byte >> 4 != 2 => return refused(Refusal::Version),
            _ => return refused(Refusal::Command),
        }
        // The spec's table of protocol bytes, and what each needs.
        let (needs, addresses) = match bytes[13] {
            0x00 | 0x01 | 0x02 | 0x10 | 0x20 | 0x30 => return whole(Header::Local, length),
            0x11 => (12, true),
            0x21 => (36, true),
            0x12 => (12, false),
            0x22 => (36, false),
            0x31 | 0x32 => (216, false),
            _ => return refused(Refusal::Family),
        };
        if length < 16 + needs {
            return refused(Refusal::Short);
        }
        if !addresses {
            return whole(Header::Local, length);
        }
        if bytes.len() < 16 + needs {
            return Read::More;
        }
        let block = &bytes[16..16 + needs];
        let ip = |octets: &[u8]| -> IpAddr {
            if octets.len() == 4 {
                IpAddr::from(<[u8; 4]>::try_from(octets).unwrap())
            } else {
                IpAddr::from(<[u8; 16]>::try_from(octets).unwrap())
            }
        };
        let size = (needs - 4) / 2;
        let ports = &block[2 * size..];
        whole(
            Header::Proxied {
                source: SocketAddr::new(
                    ip(&block[..size]),
                    u16::from_be_bytes([ports[0], ports[1]]),
                ),
                destination: SocketAddr::new(
                    ip(&block[size..2 * size]),
                    u16::from_be_bytes([ports[2], ports[3]]),
                ),
            },
            length,
        )
    }

    /// Any address, of either family; IPv4 ones as IPv4, as `proxied` writes them.
    fn address() -> impl Strategy<Value = IpAddr> {
        prop_oneof![
            any::<[u8; 4]>().prop_map(IpAddr::from),
            any::<[u8; 16]>().prop_map(|octets| IpAddr::from(octets).to_canonical()),
        ]
    }

    fn version() -> impl Strategy<Value = Version> {
        prop_oneof![Just(Version::V1), Just(Version::V2)]
    }

    /// v2 headers of any fixed part, and lines and headers made to be nearly right.
    fn headers() -> impl Strategy<Value = Vec<u8>> {
        let written = (address(), address(), any::<(u16, u16)>(), version()).prop_map(
            |(source, destination, ports, version)| {
                super::proxied(
                    version,
                    SocketAddr::new(source, ports.0),
                    SocketAddr::new(destination, ports.1),
                )
                .as_bytes()
                .to_vec()
            },
        );
        let lines = prop::string::string_regex(
            "PROXY (TCP4|TCP6|UNKNOWN|TCP)( [0-9a-fA-F:.]{0,12}){0,5}( ?\r?\n?)",
        )
        .unwrap()
        .prop_map(String::into_bytes);
        let any_fixed = (
            any::<u8>(),
            any::<u8>(),
            any::<u16>(),
            prop::collection::vec(any::<u8>(), 0..64),
        );
        let likely_fixed = (
            prop::sample::select(&[0x20_u8, 0x21][..]),
            prop::sample::select(&[0x00_u8, 0x11, 0x21, 0x12, 0x31, 0x41][..]),
            0_u16..300,
            prop::collection::vec(any::<u8>(), 0..300),
        );
        let make = |(version_command, family, length, rest): (u8, u8, u16, Vec<u8>)| {
            let mut bytes = fixed(version_command, family, length);
            bytes.extend_from_slice(&rest);
            bytes
        };
        prop_oneof![
            written,
            lines,
            any_fixed.prop_map(make),
            likely_fixed.prop_map(make),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// What the writers make is read back as the same two ends, whatever follows it,
        /// and every shorter part of it is not enough.
        #[test]
        fn what_is_written_is_read_back(
            source in address(),
            destination in address(),
            ports in any::<(u16, u16)>(),
            version in version(),
            after in prop::collection::vec(any::<u8>(), 0..32),
        ) {
            let source = SocketAddr::new(source, ports.0);
            let destination = SocketAddr::new(destination, ports.1);
            let written = super::proxied(version, source, destination);
            let header = written.as_bytes();
            let expected = match (source.ip(), destination.ip()) {
                (IpAddr::V4(_), IpAddr::V4(_)) => Header::Proxied { source, destination },
                _ => Header::Proxied {
                    source: SocketAddr::new(IpAddr::V6(v6(source.ip())), ports.0),
                    destination: SocketAddr::new(IpAddr::V6(v6(destination.ip())), ports.1),
                },
            };
            let mut bytes = header.to_vec();
            bytes.extend_from_slice(&after);
            prop_assert_eq!(read(&bytes), whole(expected, header.len()));
            for cut in 0..header.len() {
                prop_assert_eq!(read(&header[..cut]), Read::More);
            }
        }

        /// On headers nearly right, damaged anywhere, and on any bytes at all, `read`
        /// finds what the plainest reader finds.
        #[test]
        fn read_agrees_with_the_plain_reader(
            header in headers(),
            noise in prop::collection::vec(any::<u8>(), 0..64),
            at in any::<prop::sample::Index>(),
        ) {
            prop_assert_eq!(read(&header), reference(&header));
            prop_assert_eq!(read(&noise), reference(&noise));
            let mut damaged = header.clone();
            let from = at.index(header.len() + 1);
            damaged[from..].iter_mut().zip(&noise).for_each(|(byte, &n)| *byte ^= n);
            prop_assert_eq!(read(&damaged), reference(&damaged));
        }

        /// Once `read` has decided, more bytes change nothing.
        #[test]
        fn a_decision_stands_as_more_comes(
            header in headers(),
            more in prop::collection::vec(any::<u8>(), 1..64),
        ) {
            let decided = read(&header);
            if decided != Read::More {
                let mut longer = header.clone();
                longer.extend_from_slice(&more);
                prop_assert_eq!(read(&longer), decided);
            }
        }
    }
}
