//! What a worker reads of a datagram to know where it goes: the version-independent part of
//! a QUIC header ([RFC 8999] §5), and whether it starts a version 1 connection.
//!
//! A datagram's packets all carry the same destination ID (RFC 9000 §12.2), so the first
//! packet's header is the datagram's. Nothing here is validated beyond what finding the IDs
//! needs: quiche checks the rest when it is handed the datagram.
//!
//! [RFC 8999]: https://www.rfc-editor.org/rfc/rfc8999.html

/// QUIC version 1 (RFC 9000).
pub const VERSION_1: u32 = 1;

/// The header of a datagram's first packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Header<'a> {
    /// A long header: a handshake, or a version negotiation.
    Long(Long<'a>),
    /// A short header: a packet of an established connection. Its ID's length is not on the
    /// wire; it is the length of the IDs this server issues.
    Short {
        /// The destination connection ID.
        dcid: &'a [u8],
    },
}

/// A long header's version-independent fields (RFC 8999 §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Long<'a> {
    /// The first byte, whose low seven bits belong to the version.
    pub first: u8,
    /// The version; 0 is a version negotiation.
    pub version: u32,
    /// The destination connection ID, up to 255 bytes in a version not ours.
    pub dcid: &'a [u8],
    /// The source connection ID.
    pub scid: &'a [u8],
}

impl Long<'_> {
    /// Whether this is a version 1 Initial: the one packet that may start a connection
    /// (RFC 9000 §17.2.2).
    pub fn is_initial(&self) -> bool {
        // Version 1's packet type is the first byte's bits 4 and 5; an Initial is 0.
        self.version == VERSION_1 && self.first & 0x30 == 0
    }
}

/// The header of `datagram`'s first packet, reading a short header's destination ID as
/// `short_dcid_len` bytes; `None` if the datagram is too short to hold one.
pub fn read(datagram: &[u8], short_dcid_len: usize) -> Option<Header<'_>> {
    let (&first, rest) = datagram.split_first()?;
    if first & 0x80 == 0 {
        let dcid = rest.get(..short_dcid_len)?;
        return Some(Header::Short { dcid });
    }
    let (version, rest) = rest.split_first_chunk::<4>()?;
    let (dcid, rest) = with_length(rest)?;
    let (scid, _) = with_length(rest)?;
    Some(Header::Long(Long {
        first,
        version: u32::from_be_bytes(*version),
        dcid,
        scid,
    }))
}

/// A field preceded by its length in one byte, and what follows it.
fn with_length(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&len, rest) = bytes.split_first()?;
    rest.split_at_checked(usize::from(len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::h3_peer::{Pipe, id, server_config, server_tls};
    use proptest::prelude::*;

    /// Our length for short headers, as the driver will read them.
    const SHORT: usize = crate::quic::id::LEN;

    /// What quiche's own parser finds in `datagram`, where it finds anything, and the same
    /// read by ours. quiche's reads more (a token, a version list) and so refuses more; where
    /// it reads a header, ours must read the same one.
    fn agrees_with_quiche(datagram: &[u8]) -> Result<(), TestCaseError> {
        let mut copy = datagram.to_vec();
        let Ok(theirs) = quiche::Header::from_slice(&mut copy, SHORT) else {
            return Ok(());
        };
        let ours = read(datagram, SHORT);
        match ours {
            Some(Header::Short { dcid }) => {
                prop_assert_eq!(theirs.ty, quiche::Type::Short);
                prop_assert_eq!(dcid, &theirs.dcid[..]);
            }
            Some(Header::Long(long)) => {
                prop_assert_ne!(theirs.ty, quiche::Type::Short);
                prop_assert_eq!(long.version, theirs.version);
                prop_assert_eq!(long.dcid, &theirs.dcid[..]);
                prop_assert_eq!(long.scid, &theirs.scid[..]);
                // quiche reads the type bits as version 1's whatever the version; version 2
                // (RFC 9369) numbers its types otherwise, so only version 1's are compared.
                if long.version == VERSION_1 {
                    prop_assert_eq!(long.is_initial(), theirs.ty == quiche::Type::Initial);
                } else {
                    prop_assert!(!long.is_initial());
                }
            }
            None => prop_assert!(false, "quiche read {theirs:?}, ours nothing"),
        }
        Ok(())
    }

    /// Every datagram of a real handshake and exchange, both ways, read as quiche reads it:
    /// Initials, Handshake packets, coalesced flights, and short headers under an ID of ours.
    #[test]
    fn a_real_connection_reads_as_quiche_reads_it() {
        let mut pipe = Pipe::new(&mut server_config(server_tls()), &id(0xa5, SHORT));
        pipe.client
            .stream_send(0, b"after the handshake", true)
            .unwrap();
        pipe.advance();

        let initial = read(&pipe.to_server[0].bytes, SHORT).unwrap();
        let Header::Long(initial) = initial else {
            panic!("the first datagram is a long header");
        };
        assert!(initial.is_initial());
        assert_eq!(initial.version, VERSION_1);
        let last = read(&pipe.to_server.last().unwrap().bytes, SHORT).unwrap();
        assert_eq!(
            last,
            Header::Short {
                dcid: &[0xa5; SHORT]
            }
        );
        for datagram in pipe.to_server.iter().chain(&pipe.to_client) {
            agrees_with_quiche(&datagram.bytes).unwrap();
        }
    }

    #[test]
    fn a_version_negotiation_and_an_unknown_version_are_long_headers() {
        // Version 0 with a list of versions after the IDs; the reader stops at the IDs.
        let mut negotiation = vec![0x80, 0, 0, 0, 0, 2, 0xd1, 0xd2, 1, 0x51];
        negotiation.extend(1_u32.to_be_bytes());
        let Some(Header::Long(long)) = read(&negotiation, SHORT) else {
            panic!("a version negotiation is a long header");
        };
        assert_eq!(
            (long.version, long.dcid, long.scid),
            (0, &[0xd1, 0xd2][..], &[0x51][..])
        );
        assert!(!long.is_initial());

        // A version we do not speak may carry IDs longer than version 1 allows: the reader
        // still finds them, so that a version negotiation can be sent back.
        let mut unknown = vec![0xc0, 0x0a, 0x1a, 0x2a, 0x3a, 30];
        unknown.extend([0x77; 30]);
        unknown.extend([0]);
        let Some(Header::Long(long)) = read(&unknown, SHORT) else {
            panic!("an unknown version is a long header");
        };
        assert_eq!(long.dcid.len(), 30);
        assert!(!long.is_initial());
    }

    #[test]
    fn a_version_1_packet_is_an_initial_only_with_type_zero() {
        for (first, initial) in [(0xc0, true), (0xd0, false), (0xe0, false), (0xf0, false)] {
            let datagram = [first, 0, 0, 0, 1, 0, 0];
            let Some(Header::Long(long)) = read(&datagram, SHORT) else {
                panic!("{first:#x} is a long header");
            };
            assert_eq!(long.is_initial(), initial, "{first:#x}");
        }
    }

    #[test]
    fn a_datagram_too_short_for_its_ids_is_not_read() {
        assert_eq!(read(&[], SHORT), None);
        assert_eq!(read(&[0x40; SHORT], SHORT), None);
        assert!(read(&[0x40; SHORT + 1], SHORT).is_some());
        // Long headers: cut before the version, the ID lengths, and inside each ID.
        let whole = [0xc0, 0, 0, 0, 1, 2, 0xd1, 0xd2, 2, 0x51, 0x52];
        for cut in 0..whole.len() {
            assert_eq!(read(&whole[..cut], SHORT), None, "cut at {cut}");
        }
        assert!(read(&whole, SHORT).is_some());
    }

    proptest! {
        /// Whatever the bytes, the reader never reads past them and agrees with quiche
        /// wherever quiche reads a header.
        #[test]
        fn any_bytes_read_as_quiche_reads_them(
            datagram in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            agrees_with_quiche(&datagram)?;
        }

        /// The same, over bytes shaped like long headers, which random bytes rarely are.
        #[test]
        fn long_headers_read_as_quiche_reads_them(
            first in 0x80_u8..=0xff,
            version in prop_oneof![Just(0_u32), Just(VERSION_1), any::<u32>()],
            dcid in proptest::collection::vec(any::<u8>(), 0..=21),
            scid in proptest::collection::vec(any::<u8>(), 0..=21),
            rest in proptest::collection::vec(any::<u8>(), 0..32),
        ) {
            let mut datagram = vec![first];
            datagram.extend(version.to_be_bytes());
            datagram.push(dcid.len() as u8);
            datagram.extend(&dcid);
            datagram.push(scid.len() as u8);
            datagram.extend(&scid);
            datagram.extend(&rest);
            agrees_with_quiche(&datagram)?;
            let Some(Header::Long(long)) = read(&datagram, SHORT) else {
                return Err(TestCaseError::fail("a well-formed long header was not read"));
            };
            prop_assert_eq!((long.version, long.dcid, long.scid), (version, &dcid[..], &scid[..]));
        }
    }
}
