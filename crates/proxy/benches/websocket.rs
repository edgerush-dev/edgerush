//! Instruction counts for following a WebSocket's frames, which a tunnel does on every read
//! of either direction for the WebSocket's life ([19 §6](../../../docs/19-websocket.md)): a
//! read's worth (16 KiB) of small frames, the worst case, since each frame's header is read
//! and its payload skipped; the same in one large frame; and one chat message.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench websocket`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::websocket::frames::Frames;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// `count` masked text frames of `length` bytes each, as a client sends them.
fn frames(count: usize, length: u8) -> Vec<u8> {
    let mut stream = Vec::new();
    for _ in 0..count {
        stream.extend_from_slice(&[0x81, 0x80 | length, 1, 2, 3, 4]);
        stream.extend(std::iter::repeat_n(0x5a, usize::from(length)));
    }
    stream
}

/// One masked binary frame of 16 KiB less its header.
fn one_large() -> Vec<u8> {
    let length: u16 = 16 * 1024 - 8;
    let mut stream = vec![0x82, 0x80 | 126];
    stream.extend_from_slice(&length.to_be_bytes());
    stream.extend_from_slice(&[1, 2, 3, 4]);
    stream.extend(std::iter::repeat_n(0x5a, usize::from(length)));
    stream
}

#[library_benchmark]
#[bench::small_frames(frames(16 * 1024 / 38, 32))]
#[bench::one_large_frame(one_large())]
#[bench::one_message(frames(1, 100))]
fn follow(stream: Vec<u8>) -> bool {
    let mut reader = Frames::new();
    reader.read(black_box(&stream));
    black_box(reader.at_boundary())
}

library_benchmark_group!(name = websocket; benchmarks = follow);

main!(library_benchmark_groups = websocket);
