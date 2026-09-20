//! Fuzzes the writing of a request body against the reading of one: whatever frames go in
//! must come back out, byte for byte and in order.
//!
//! The two halves are written apart and have no reason to agree, which is what makes this
//! worth asking. A writer that frames a chunk wrongly and a reader that reads it back the
//! same wrong way would both pass their own tests; here they would have to be wrong in
//! exactly matching ways to pass, and the body that went in is the oracle for both.

#![no_main]

use edgerush_proxy::upstream::h1::H1Limits;
use edgerush_proxy::upstream::h1::codec::{BodyReader, BodyWriter, Framing, Piece, Sending};
use libfuzzer_sys::fuzz_target;

/// Frames taken out of the fuzzer's bytes: the first byte says how long the next frame is,
/// so a single input becomes a sequence of them, empty frames included.
fn frames(bytes: &[u8]) -> Vec<&[u8]> {
    let mut frames = Vec::new();
    let mut rest = bytes;
    while let Some((length, after)) = rest.split_first() {
        let length = usize::from(*length).min(after.len());
        let (frame, next) = after.split_at(length);
        frames.push(frame);
        rest = next;
        if frames.len() == 32 {
            break;
        }
    }
    frames
}

fuzz_target!(|bytes: &[u8]| {
    let frames = frames(bytes);
    let whole: Vec<u8> = frames.concat();
    let limits = H1Limits::default();

    for sending in [Sending::Chunked, Sending::Length(whole.len() as u64)] {
        let mut writer = BodyWriter::new(sending);
        let mut written = Vec::new();
        for frame in &frames {
            writer
                .data(&mut written, frame)
                .unwrap_or_else(|error| panic!("{frames:?} could not be written: {error}"));
        }
        writer
            .finish(&mut written, None, &[])
            .unwrap_or_else(|error| panic!("{frames:?} could not be finished: {error}"));

        let framing = match sending {
            Sending::Chunked => Framing::Chunked,
            Sending::Length(length) => Framing::Length(length),
            Sending::None => unreachable!("neither of the two above"),
        };
        let mut reader = BodyReader::new(framing);
        let mut taken = 0;
        let mut read = Vec::new();
        loop {
            let front = &written[taken..];
            match reader.read(front, true, &limits) {
                Err(error) => panic!("{written:?} could not be read back: {error}"),
                Ok(Piece::More) => panic!("{written:?} wanted more than was written"),
                Ok(Piece::Data { data, consumed }) => {
                    read.extend_from_slice(&front[data]);
                    taken += consumed;
                }
                Ok(Piece::End { consumed, .. }) => {
                    taken += consumed;
                    break;
                }
            }
        }
        // Every byte that was written was read, and nothing was left on the wire: bytes
        // left over are the next request reading the end of this one.
        assert_eq!(taken, written.len(), "{frames:?} as {sending:?}");
        assert_eq!(read, whole, "{frames:?} as {sending:?}");
        assert!(reader.is_done(), "{frames:?} as {sending:?}");
    }
});
