//! Where a WebSocket stream's frames begin, one direction of it at a time
//! ([19 §6](../../../../docs/19-websocket.md)).
//!
//! The gateway never reads a message: it reads each frame's header (RFC 6455 §5.2, 2 to 14
//! bytes) and skips its payload, so that on drain it can put a Close frame of its own
//! between two frames, which §5.4 allows anywhere, a fragmented message's included. A frame
//! header cannot be found part way through a stream, so a reader follows its direction from
//! the switch on. It notes a Close frame going by, so that one side closing is not
//! answered by a second Close of the gateway's. A length RFC 6455 does not allow is the end
//! of following that direction: the reader says it is lost, and the tunnel is closed bare
//! on drain rather than risk a Close in the middle of a frame. Nothing here ever ends a
//! tunnel or changes a byte.
//!
//! Pure: it is given bytes as they arrive, cut wherever the reads cut them, and keeps what
//! it needs between one read and the next.

// `pub` so that the fuzz targets, a crate of their own, can name it; in an ordinary build
// the module is private and none of this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

/// A Close frame (RFC 6455 §5.5.1) saying 1001, "Going Away" (§7.4.1), as a server sends it:
/// unmasked.
pub const GOING_AWAY: [u8; 4] = [0x88, 0x02, 0x03, 0xe9];

/// The same Close frame as a client sends it, masked with `mask` (§5.3).
#[must_use]
pub fn going_away_masked(mask: [u8; 4]) -> [u8; 8] {
    [
        0x88,
        0x82,
        mask[0],
        mask[1],
        mask[2],
        mask[3],
        0x03 ^ mask[0],
        0xe9 ^ mask[1],
    ]
}

/// One direction of a WebSocket stream, followed frame by frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frames {
    at: At,
    /// A Close frame's header has gone by.
    closing: bool,
}

/// Where in the stream the next byte falls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum At {
    /// It begins a frame.
    Boundary,
    /// It is part of a header, of which these have come.
    Header { bytes: [u8; 14], seen: u8 },
    /// It is part of a payload, of which this much is still to come.
    Payload(u64),
    /// The stream could not be followed.
    Lost,
}

impl Default for Frames {
    fn default() -> Self {
        Self::new()
    }
}

impl Frames {
    /// A direction followed from its first byte, which begins a frame.
    #[must_use]
    pub fn new() -> Self {
        Self {
            at: At::Boundary,
            closing: false,
        }
    }

    /// Follows `bytes`, the next of the stream.
    pub fn read(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() && self.at != At::Lost {
            let taken = self.step(bytes);
            bytes = &bytes[taken..];
        }
    }

    /// Follows `bytes` as far as the first place in them where a frame begins, and says
    /// where that is: `Some(0)` if the next frame begins with them, `Some(bytes.len())` if
    /// it begins right after them. None if no frame begins in them, or the stream is lost.
    /// What comes after that place is not followed.
    pub fn until_boundary(&mut self, bytes: &[u8]) -> Option<usize> {
        let mut at = 0;
        loop {
            match self.at {
                At::Boundary => return Some(at),
                At::Lost => return None,
                At::Header { .. } | At::Payload(_) if at == bytes.len() => return None,
                At::Header { .. } | At::Payload(_) => at += self.step(&bytes[at..]),
            }
        }
    }

    /// Whether the next byte begins a frame.
    #[must_use]
    pub fn at_boundary(&self) -> bool {
        self.at == At::Boundary
    }

    /// Whether a Close frame has gone by.
    #[must_use]
    pub fn closing(&self) -> bool {
        self.closing
    }

    /// Whether the stream could not be followed.
    #[must_use]
    pub fn lost(&self) -> bool {
        self.at == At::Lost
    }

    /// Takes what it can of the front of `bytes`, which are not empty, and says how much.
    fn step(&mut self, bytes: &[u8]) -> usize {
        match self.at {
            At::Lost => bytes.len(),
            At::Payload(left) => {
                let taken = usize::try_from(left).map_or(bytes.len(), |left| left.min(bytes.len()));
                // A usize always fits in a u64 on the platforms EdgeRush builds for.
                let rest = left.saturating_sub(u64::try_from(taken).unwrap_or(u64::MAX));
                self.at = if rest == 0 {
                    At::Boundary
                } else {
                    At::Payload(rest)
                };
                taken
            }
            At::Boundary => {
                // The usual case: the whole header is here, and is read where it lies.
                if let Some(&second) = bytes.get(1) {
                    let whole = header_length(second);
                    if let Some(header) = bytes.get(..whole) {
                        self.begin(header);
                        return whole;
                    }
                }
                self.at = At::Header {
                    bytes: [0; 14],
                    seen: 0,
                };
                self.header(bytes)
            }
            At::Header { .. } => self.header(bytes),
        }
    }

    /// Takes header bytes from the front of `bytes` until the header is whole, then reads it.
    fn header(&mut self, bytes: &[u8]) -> usize {
        let At::Header {
            bytes: mut header,
            mut seen,
        } = self.at
        else {
            return 0;
        };
        let mut taken = 0;
        loop {
            let whole = if seen < 2 {
                2
            } else {
                header_length(header[1])
            };
            if usize::from(seen) == whole {
                self.begin(&header[..whole]);
                return taken;
            }
            let Some(&byte) = bytes.get(taken) else {
                self.at = At::Header {
                    bytes: header,
                    seen,
                };
                return taken;
            };
            header[usize::from(seen)] = byte;
            seen += 1;
            taken += 1;
        }
    }

    /// Reads a whole header: what follows it, and whether it is a Close's.
    fn begin(&mut self, header: &[u8]) {
        let length = match header[1] & 0x7f {
            126 => u64::from(u16::from_be_bytes([header[2], header[3]])),
            127 => {
                let mut eight = [0; 8];
                eight.copy_from_slice(&header[2..10]);
                let length = u64::from_be_bytes(eight);
                // RFC 6455 §5.2: "the most significant bit MUST be 0".
                if length >> 63 != 0 {
                    self.at = At::Lost;
                    return;
                }
                length
            }
            short => u64::from(short),
        };
        if header[0] & 0x0f == 0x8 {
            self.closing = true;
        }
        self.at = if length == 0 {
            At::Boundary
        } else {
            At::Payload(length)
        };
    }
}

/// How long a header is whose second byte is `second`, which says how many bytes of length
/// and of mask follow the first two.
fn header_length(second: u8) -> usize {
    let length = match second & 0x7f {
        126 => 2,
        127 => 8,
        _ => 0,
    };
    let mask = if second & 0x80 == 0 { 0 } else { 4 };
    2 + length + mask
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A frame of `opcode` with `length` bytes of payload, masked or not.
    fn frame(opcode: u8, length: usize, masked: bool) -> Vec<u8> {
        let mut out = vec![0x80 | opcode];
        let mask = if masked { 0x80 } else { 0 };
        match length {
            0..=125 => out.push(mask | u8::try_from(length).unwrap()),
            126..=0xffff => {
                out.push(mask | 126);
                out.extend_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
            }
            _ => {
                out.push(mask | 127);
                out.extend_from_slice(&u64::try_from(length).unwrap().to_be_bytes());
            }
        }
        if masked {
            out.extend_from_slice(&[1, 2, 3, 4]);
        }
        out.extend(std::iter::repeat_n(0x5a, length));
        out
    }

    /// Where frames begin in `stream`, the slow way: the whole of it at once, a frame at a
    /// time. Each boundary, and whether a Close went by; None if it could not be followed.
    fn reference(stream: &[u8]) -> Option<(Vec<usize>, bool)> {
        let mut boundaries = vec![0];
        let mut at = 0;
        let mut closing = false;
        while at < stream.len() {
            let first = stream[at];
            let Some(&second) = stream.get(at + 1) else {
                return Some((boundaries, closing));
            };
            let (extended, masked) = (second & 0x7f, second & 0x80 != 0);
            let size = match extended {
                126 => 2,
                127 => 8,
                _ => 0,
            };
            let header = 2 + size + if masked { 4 } else { 0 };
            if stream.len() < at + header {
                return Some((boundaries, closing));
            }
            let length = match extended {
                126 => u64::from(u16::from_be_bytes([stream[at + 2], stream[at + 3]])),
                127 => {
                    let length = u64::from_be_bytes(stream[at + 2..at + 10].try_into().unwrap());
                    if length >> 63 != 0 {
                        return None;
                    }
                    length
                }
                short => u64::from(short),
            };
            closing |= first & 0x0f == 0x8;
            let end = (at + header) as u64 + length;
            if end > stream.len() as u64 {
                return Some((boundaries, closing));
            }
            at = end as usize;
            boundaries.push(at);
        }
        Some((boundaries, closing))
    }

    #[test]
    fn frames_of_every_length_encoding_are_followed() {
        for length in [0, 1, 125, 126, 127, 0xffff, 0x1_0000, 70_000] {
            for masked in [false, true] {
                let mut stream = frame(0x2, length, masked);
                let first = stream.len();
                stream.extend(frame(0x1, 3, masked));
                let mut frames = Frames::new();
                assert_eq!(frames.until_boundary(&stream), Some(0));
                frames.read(&stream[..first - 1]);
                assert!(!frames.at_boundary(), "{length} {masked}");
                assert_eq!(frames.until_boundary(&stream[first - 1..]), Some(1));
                frames.read(&stream[first..]);
                assert!(frames.at_boundary(), "{length} {masked}");
                assert!(!frames.closing());
            }
        }
    }

    #[test]
    fn a_close_going_by_is_noted() {
        let mut frames = Frames::new();
        frames.read(&frame(0x1, 5, false));
        assert!(!frames.closing());
        frames.read(&GOING_AWAY);
        assert!(frames.closing());
        assert!(frames.at_boundary());
        let mut masked = Frames::new();
        masked.read(&going_away_masked([9, 8, 7, 6]));
        assert!(masked.closing() && masked.at_boundary());
    }

    #[test]
    fn the_gateways_close_frames_say_going_away() {
        // Unmasked: the status code is the payload as it stands.
        assert_eq!(u16::from_be_bytes([GOING_AWAY[2], GOING_AWAY[3]]), 1001);
        let mask = [0xa1, 0xb2, 0xc3, 0xd4];
        let masked = going_away_masked(mask);
        assert_eq!(&masked[2..6], &mask);
        let code = [masked[6] ^ mask[0], masked[7] ^ mask[1]];
        assert_eq!(u16::from_be_bytes(code), 1001);
    }

    /// A 64-bit length with its top bit set is one RFC 6455 does not allow: the stream is
    /// lost, and never found again.
    #[test]
    fn a_length_rfc_6455_forbids_loses_the_stream() {
        let mut stream = vec![0x82, 127];
        stream.extend_from_slice(&(1_u64 << 63).to_be_bytes());
        stream.extend(frame(0x1, 1, false));
        let mut frames = Frames::new();
        frames.read(&stream);
        assert!(frames.lost());
        assert!(!frames.at_boundary());
        assert_eq!(frames.until_boundary(&frame(0x1, 1, false)), None);
    }

    proptest! {
        /// However reads cut a stream, the reader finds the same boundaries as a reading of
        /// the whole of it, and ends in the same place.
        #[test]
        fn boundaries_do_not_depend_on_where_reads_cut(
            frames in proptest::collection::vec((0_u8..16, 0_usize..300, any::<bool>()), 0..12),
            noise in proptest::collection::vec(any::<u8>(), 0..40),
            cuts in proptest::collection::vec(0_usize..4000, 0..8),
        ) {
            let mut stream: Vec<u8> = frames
                .iter()
                .flat_map(|&(opcode, length, masked)| frame(opcode, length, masked))
                .collect();
            stream.extend(noise);
            let expected = reference(&stream);
            let mut cuts: Vec<usize> = cuts.into_iter().map(|cut| cut.min(stream.len())).collect();
            cuts.sort_unstable();
            cuts.push(stream.len());
            let mut reader = Frames::new();
            let mut found = vec![];
            let mut from = 0;
            for cut in cuts {
                // Each piece is followed to its first boundary, then the rest of it read, as
                // the tunnel does while waiting for a place to close at.
                let piece = &stream[from..cut];
                let mut at = 0;
                while let Some(boundary) = reader.until_boundary(&piece[at..]) {
                    let place = from + at + boundary;
                    if found.last() != Some(&place) {
                        found.push(place);
                    }
                    if at + boundary >= piece.len() {
                        break;
                    }
                    // Past this boundary: a frame's first byte.
                    reader.read(&piece[at + boundary..at + boundary + 1]);
                    at += boundary + 1;
                }
                from = cut;
            }
            match expected {
                None => prop_assert!(reader.lost()),
                Some((boundaries, closing)) => {
                    prop_assert!(!reader.lost());
                    prop_assert_eq!(&found, &boundaries);
                    prop_assert_eq!(reader.closing(), closing);
                    prop_assert_eq!(reader.at_boundary(), boundaries.last() == Some(&stream.len()));
                }
            }
        }
    }
}
