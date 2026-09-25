//! Instruction counts for what a worker does with a QUIC datagram before quiche sees it
//! ([16 §3](../../../docs/16-http3.md)): reading the header, on every datagram, and reading
//! which worker an ID names, for a packet that is not for a connection of its own. And
//! making an ID, a few times a connection.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench quic`, see the repository
//! README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::quic::header::{Header, read};
use edgerush_proxy::quic::id::{Codec, Keys, LEN, Nonces};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn codec() -> Codec {
    Codec::new(&Keys::new([0x3c; 16], [0x5e; 32])).expect("a key")
}

/// A packet of an established connection: a short header under one of our IDs, and a
/// full-sized payload behind it.
fn short() -> Vec<u8> {
    let id = codec().encode(7, &[0x42; 14]).expect("an ID");
    let mut datagram = vec![0x41];
    datagram.extend(id);
    datagram.resize(1_350, 0xaa);
    datagram
}

/// A client's first Initial: a long header with its own 8-byte IDs, padded to 1,200 bytes.
fn initial() -> Vec<u8> {
    let mut datagram = vec![0xc3, 0, 0, 0, 1, 8];
    datagram.extend([0xd1; 8]);
    datagram.push(8);
    datagram.extend([0x51; 8]);
    datagram.resize(1_200, 0);
    datagram
}

#[library_benchmark]
#[bench::established(short())]
#[bench::first_initial(initial())]
fn read_header(datagram: Vec<u8>) -> bool {
    black_box(matches!(
        read(black_box(&datagram), LEN),
        Some(Header::Short { .. })
    ))
}

/// A codec and one of its IDs.
fn issued() -> (Codec, Vec<u8>) {
    (codec(), short()[1..=LEN].to_vec())
}

/// A codec and the counter a worker draws nonces from.
fn issuing() -> (Codec, Nonces) {
    (codec(), Nonces::starting_at(0x1234_5678))
}

#[library_benchmark]
#[bench::ours(issued())]
fn decode_id((mut codec, id): (Codec, Vec<u8>)) -> Option<u16> {
    black_box(codec.decode(black_box(&id)).expect("a block"))
}

#[library_benchmark]
#[bench::next(issuing())]
fn encode_id((mut codec, mut nonces): (Codec, Nonces)) -> [u8; LEN] {
    black_box(codec.encode(7, &nonces.draw()).expect("a block"))
}

#[library_benchmark]
#[bench::ours(issued())]
fn reset_token((codec, id): (Codec, Vec<u8>)) -> u128 {
    black_box(codec.reset_token(black_box(&id)).expect("a MAC"))
}

library_benchmark_group!(
    name = quic;
    benchmarks = read_header, decode_id, encode_id, reset_token
);
main!(library_benchmark_groups = quic);
