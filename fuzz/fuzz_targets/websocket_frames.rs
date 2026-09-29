//! Fuzzes the WebSocket frame reader a draining tunnel closes by
//! ([19 §6](../../../docs/19-websocket.md)): any bytes at all, as one direction of a stream,
//! cut into reads wherever the first bytes say.
//!
//! Wherever the reads cut it, the reader must find the frames a reading of the whole stream
//! at once finds, note the same Close going by, and lose the stream exactly where the whole
//! reading does. A Close of the gateway's goes where it says a frame begins, so a boundary
//! found anywhere else is one put into the middle of a frame.
//!
//! `cargo fuzz run websocket_frames corpus/websocket_frames seeds/websocket_frames`.

#![no_main]

use edgerush_proxy::websocket::frames::Frames;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&cuts, stream)) = data.split_first() else {
        return;
    };
    let expected = whole(stream);
    // Cut into pieces of 1 to 16 bytes, their sizes taken from the first byte in turn, so
    // that one input reads the same stream several ways.
    let mut reader = Frames::new();
    let mut found = Vec::new();
    let mut from = 0;
    let mut turn = 0_u32;
    while from < stream.len() {
        let size = 1 + usize::from(cuts.rotate_left(turn) & 0x0f);
        turn += 1;
        let piece = &stream[from..(from + size).min(stream.len())];
        let mut at = 0;
        while let Some(boundary) = reader.until_boundary(&piece[at..]) {
            let place = from + at + boundary;
            if found.last() != Some(&place) {
                found.push(place);
            }
            if at + boundary >= piece.len() {
                break;
            }
            reader.read(&piece[at + boundary..at + boundary + 1]);
            at += boundary + 1;
        }
        from += piece.len();
    }
    if stream.is_empty() {
        found.push(0);
    }
    match expected {
        None => assert!(reader.lost()),
        Some((boundaries, closing)) => {
            assert!(!reader.lost());
            assert_eq!(found, boundaries);
            assert_eq!(reader.closing(), closing);
            assert_eq!(
                reader.at_boundary(),
                boundaries.last() == Some(&stream.len())
            );
        }
    }
});

/// Where frames begin in `stream`, read all at once a frame at a time, and whether a Close
/// went by; None where a length RFC 6455 forbids was found.
fn whole(stream: &[u8]) -> Option<(Vec<usize>, bool)> {
    let mut boundaries = vec![0];
    let mut at = 0;
    let mut closing = false;
    while at < stream.len() {
        let first = stream[at];
        let Some(&second) = stream.get(at + 1) else {
            break;
        };
        let (extended, masked) = (second & 0x7f, second & 0x80 != 0);
        let size = match extended {
            126 => 2,
            127 => 8,
            _ => 0,
        };
        let header = 2 + size + if masked { 4 } else { 0 };
        if stream.len() < at + header {
            break;
        }
        let length = match extended {
            126 => u64::from(u16::from_be_bytes([stream[at + 2], stream[at + 3]])),
            127 => {
                let length = u64::from_be_bytes(stream[at + 2..at + 10].try_into().ok()?);
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
            break;
        }
        at = end as usize;
        boundaries.push(at);
    }
    Some((boundaries, closing))
}
