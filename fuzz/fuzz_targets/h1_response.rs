//! Fuzzes the reading of an upstream's answer: any bytes at all, read as a head, a
//! framing decision and a body.
//!
//! Whatever arrives, reading it must not panic and must never consume more than it was
//! handed. The property with teeth is the other one: **the same bytes cut into pieces are
//! the same answer**. A reader that keeps state between arrivals has somewhere for a
//! message to be read one way whole and another way in fragments, and that difference is
//! what leaves a connection out of step with the peer at the other end of it.
//!
//! What is compared is whether the message was taken, and what it was taken to mean —
//! not which complaint a refusal made. Which bound a bad message trips depends on how
//! much of it has arrived, and must: a reader holding a thousand bytes cannot yet know
//! about a bare newline in the thousand and twenty-seventh. Being refused is the part
//! that matters, and that part is the same either way.

#![no_main]

use edgerush_proxy::upstream::h1::H1Limits;
use edgerush_proxy::upstream::h1::codec::{
    Asked, BodyReader, CodecError, Framing, Head, HeadReader, Piece, ResponseHead, delivery,
};
use libfuzzer_sys::fuzz_target;

/// Small bounds, so that a target really reaches them on inputs a fuzzer will produce.
const fn limits() -> H1Limits {
    H1Limits {
        head: 1024,
        fields: 16,
        chunk_line: 64,
        trailers: 256,
        trailer_fields: 8,
    }
}

/// The head never ended, and nothing more is coming. The reader is never asked to say so
/// itself — it has no way of knowing — so the answer says it here.
const UNFINISHED: CodecError = CodecError::Truncated;

/// What reading a whole answer came to.
#[derive(PartialEq, Eq)]
struct Answer {
    head: ResponseHead,
    body: Vec<u8>,
    trailers: bool,
}

/// Reads an answer out of `bytes`, handing the reader `step` more bytes at a time: with a
/// step of the whole length that is one arrival, with a step of one it is byte by byte.
fn read(bytes: &[u8], step: usize) -> Result<Answer, CodecError> {
    let limits = limits();
    let step = step.max(1);

    let mut reader = HeadReader::default();
    let mut given = 0;
    let (head, used) = loop {
        match reader.read(&bytes[..given], &limits)? {
            Head::Read { head, consumed } => break (head, consumed),
            Head::More if given == bytes.len() => return Err(UNFINISHED),
            Head::More => given = bytes.len().min(given + step),
        }
    };

    let delivery = delivery(&head, Asked::Anything)?;
    let (body, trailers) = read_body(&bytes[used..], delivery.framing, step, &limits)?;
    Ok(Answer {
        head,
        body,
        trailers,
    })
}

/// Reads a body of `framing` out of `bytes`, `step` more at a time.
fn read_body(
    bytes: &[u8],
    framing: Framing,
    step: usize,
    limits: &H1Limits,
) -> Result<(Vec<u8>, bool), CodecError> {
    let mut reader = BodyReader::new(framing);
    let mut taken = 0;
    let mut given = 0;
    let mut body = Vec::new();
    loop {
        // Everything there is has been handed over, so what the peer had to say it has.
        let ended = given == bytes.len();
        let front = &bytes[taken..given];
        match reader.read(front, ended, limits)? {
            Piece::More if ended => return Err(UNFINISHED),
            Piece::More => given = bytes.len().min(given + step),
            Piece::Data { data, consumed } => {
                body.extend_from_slice(&front[data]);
                taken += consumed;
                assert!(taken <= bytes.len(), "more was consumed than was given");
            }
            Piece::End { trailers, consumed } => {
                taken += consumed;
                assert!(taken <= bytes.len(), "more was consumed than was given");
                return Ok((body, trailers.is_some()));
            }
        }
    }
}

fuzz_target!(|bytes: &[u8]| {
    // Nothing here may panic, whatever the bytes are.
    let whole = read(bytes, bytes.len());

    // And the same bytes, handed over a piece at a time, are the same answer. A reader
    // that reads them differently has a seam a message could be smuggled through.
    for step in [1, 2, 3, 7] {
        let piecemeal = read(bytes, step);
        match (&whole, &piecemeal) {
            (Ok(one), Ok(other)) => {
                assert!(one.head == other.head, "{bytes:?} at step {step}");
                assert_eq!(one.body, other.body, "{bytes:?} at step {step}");
                assert_eq!(one.trailers, other.trailers, "{bytes:?} at step {step}");
            }
            // Refused either way, which is what had to be the same.
            (Err(_), Err(_)) => {}
            _ => panic!("{bytes:?} read one way whole and another at step {step}"),
        }
    }
});
