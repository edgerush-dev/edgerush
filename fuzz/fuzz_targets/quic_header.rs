//! Fuzzes the reader that finds a datagram's connection IDs against quiche's own parser: any
//! bytes at all, read as a worker reads them before it knows which connection they are for.
//!
//! quiche's parser reads more of a header (a token, a version list) and so refuses more;
//! wherever it reads a header, ours must find the same form, version and IDs, and never
//! read past the datagram.
//!
//! `cargo fuzz run quic_header corpus/quic_header seeds/quic_header`.

#![no_main]

use edgerush_proxy::quic::header::{Header, VERSION_1, read};
use edgerush_proxy::quic::id::LEN;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|datagram: &[u8]| {
    let ours = read(datagram, LEN);
    let mut copy = datagram.to_vec();
    let Ok(theirs) = quiche::Header::from_slice(&mut copy, LEN) else {
        return;
    };
    match ours.expect("quiche read a header where ours read none") {
        Header::Short { dcid } => {
            assert_eq!(theirs.ty, quiche::Type::Short);
            assert_eq!(dcid, &theirs.dcid[..]);
        }
        Header::Long(long) => {
            assert_ne!(theirs.ty, quiche::Type::Short);
            assert_eq!(long.version, theirs.version);
            assert_eq!(long.dcid, &theirs.dcid[..]);
            assert_eq!(long.scid, &theirs.scid[..]);
            // quiche reads the type bits as version 1's whatever the version.
            if long.version == VERSION_1 {
                assert_eq!(long.is_initial(), theirs.ty == quiche::Type::Initial);
            }
        }
    }
});
