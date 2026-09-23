//! Fuzzes the reading of a client's request: any bytes at all, read as a head, a framing
//! decision and a body.
//!
//! Whatever arrives, reading it must not panic and must never consume more than it was
//! handed. The property with teeth is the other one: **the same bytes cut into pieces are
//! the same request**. A reader that keeps state between arrivals has somewhere for a
//! request to be read one way whole and another way in fragments, and that difference is
//! where a request is smuggled past the reader that is meant to have ended it.
//!
//! The head reader judges every byte in the order it came, bounds included, so for the
//! head even the reason for a refusal must be the same however the bytes were cut. A body
//! is read by the reader both sides share, whose refusals may differ in which bound they
//! name, so there only being refused is compared.
//!
//! Seeded from the boundary corpus the proxy's tests check in, with new finds kept apart:
//! `cargo fuzz run h1_request corpus/h1_request ../crates/proxy/tests/corpus/h1_request`.

#![no_main]

use edgerush_proxy::downstream::h1::codec::{Head, HeadReader, RequestError, RequestHead, arrival};
use edgerush_proxy::upstream::h1::H1Limits;
use edgerush_proxy::upstream::h1::codec::{BodyReader, Framing, Piece};
use libfuzzer_sys::fuzz_target;

/// Small bounds, so that a target really reaches them on inputs a fuzzer will produce.
fn limits() -> H1Limits {
    H1Limits {
        head: 1024,
        request_line: 256,
        fields: 16,
        chunk_line: 64,
        trailers: 256,
        trailer_fields: 8,
        ..H1Limits::default()
    }
}

/// How reading the request went.
#[derive(Debug, PartialEq)]
enum Read {
    /// The head never ended before the bytes did.
    Unfinished,
    /// The head was refused, and why.
    Refused(RequestError),
    /// The head was read, and then its body was, or was refused.
    Request {
        head: RequestHead,
        consumed: usize,
        body: Option<(Vec<u8>, bool)>,
    },
}

/// Reads a request out of `bytes`, handing the reader `step` more bytes at a time: with a
/// step of the whole length that is one arrival, with a step of one it is byte by byte.
fn read(bytes: &[u8], step: usize) -> Read {
    let limits = limits();
    let step = step.max(1);

    let mut reader = HeadReader::default();
    let mut given = 0;
    let (head, consumed) = loop {
        match reader.read(&bytes[..given], &limits) {
            Err(error) => return Read::Refused(error),
            Ok(Head::Read { head, consumed }) => break (head, consumed),
            Ok(Head::More) if given == bytes.len() => return Read::Unfinished,
            Ok(Head::More) => given = bytes.len().min(given + step),
        }
    };
    assert!(consumed <= given, "more was consumed than was given");

    // The parser's field checks are all there are (14 §6): whatever it takes would make a
    // header map, name by name and value by value, without a field left out.
    for (name, value) in head.fields.view(&bytes[..consumed]).iter() {
        assert!(
            http::HeaderName::from_bytes(name).is_ok(),
            "a field name the parser took is no name: {name:?}"
        );
        assert!(
            http::HeaderValue::from_bytes(value).is_ok(),
            "a field value the parser took is no value: {value:?}"
        );
    }

    let body = match arrival(&head, &head.fields.view(&bytes[..consumed])) {
        Err(error) => {
            // Refused for its framing: an answer every client gets, and a status for it.
            let _status = error.status();
            None
        }
        Ok(arrival) => read_body(&bytes[consumed..], arrival.framing, step, &limits),
    };
    Read::Request {
        head,
        consumed,
        body,
    }
}

/// Reads a body of `framing` out of `bytes`, `step` more at a time; `None` if it was
/// refused or never ended.
fn read_body(
    bytes: &[u8],
    framing: Framing,
    step: usize,
    limits: &H1Limits,
) -> Option<(Vec<u8>, bool)> {
    let mut reader = BodyReader::new(framing);
    let mut taken = 0;
    let mut given = 0;
    let mut body = Vec::new();
    loop {
        let ended = given == bytes.len();
        let front = &bytes[taken..given];
        match reader.read(front, ended, limits).ok()? {
            Piece::More if ended => return None,
            Piece::More => given = bytes.len().min(given + step),
            Piece::Data { data, consumed } => {
                body.extend_from_slice(&front[data]);
                taken += consumed;
                assert!(taken <= given, "more was consumed than was given");
            }
            Piece::End { trailers, consumed } => {
                taken += consumed;
                assert!(taken <= given, "more was consumed than was given");
                return Some((body, trailers.is_some()));
            }
        }
    }
}

fuzz_target!(|bytes: &[u8]| {
    // Nothing here may panic, whatever the bytes are.
    let whole = read(bytes, bytes.len());

    // And the same bytes, handed over a piece at a time, are the same request.
    // A body refused for whichever bound is `None` either way, so the whole of what was
    // read can be compared.
    for step in [1, 2, 3, 7] {
        assert_eq!(whole, read(bytes, step), "{bytes:?} at step {step}");
    }
});
