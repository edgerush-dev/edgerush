//! Fuzzes the PROXY protocol header reader every listener with senders reads a connection's
//! first bytes with ([20 §3](../../../docs/20-proxy-protocol.md)): any bytes at all, as a
//! connection's first.
//!
//! Whatever it is given, the reader must not fail, must keep to its bounds (a v1 line
//! within 107 bytes of what came, a v2 header within its 16 bytes and 65,535 more), and once
//! it has decided, more bytes must not change its mind. Two ends it reads must be written
//! back by the writers into a header it reads as the same two ends.
//!
//! `cargo fuzz run proxy_header corpus/proxy_header seeds/proxy_header`.

#![no_main]

use edgerush_proxy::proxy_protocol::{
    HELD, Header, Read, SIGNATURE, V1_LIMIT, Version, proxied, read,
};
use libfuzzer_sys::fuzz_target;
use std::net::SocketAddr;

fuzz_target!(|data: &[u8]| {
    let decided = read(data);
    if let Read::Whole { header, length } = decided {
        if data.starts_with(&SIGNATURE) {
            assert!((16..=16 + 65_535).contains(&length));
            // What decided it lies within what is ever held.
            assert_eq!(read(&data[..data.len().min(HELD)]), decided);
        } else {
            assert!(length <= V1_LIMIT && length <= data.len());
            assert_eq!(&data[length - 2..length], b"\r\n");
        }
        if let Header::Proxied {
            source,
            destination,
        } = header
        {
            let version = if data.starts_with(&SIGNATURE) {
                Version::V2
            } else {
                Version::V1
            };
            let written = proxied(version, source, destination);
            let Read::Whole {
                header: again,
                length,
            } = read(written.as_bytes())
            else {
                panic!("a header written from what was read is not read back")
            };
            assert_eq!(length, written.as_bytes().len());
            let Header::Proxied {
                source: source_again,
                destination: destination_again,
            } = again
            else {
                panic!("two ends written are read back as none")
            };
            assert_eq!(canonical(source_again), canonical(source));
            assert_eq!(canonical(destination_again), canonical(destination));
        }
    }
    if decided != Read::More {
        let mut longer = data.to_vec();
        longer.extend_from_slice(b"\r\n\0more");
        assert_eq!(read(&longer), decided);
    }
});

/// An address as the writers write it: an IPv4-mapped one as IPv4 where its partner is
/// IPv4 too, which these comparisons do not need to know.
fn canonical(address: SocketAddr) -> SocketAddr {
    SocketAddr::new(address.ip().to_canonical(), address.port())
}
