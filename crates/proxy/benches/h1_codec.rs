//! Head parsing costs, including the larger-header fallback and fragmented input.
#![allow(missing_docs, reason = "benchmark macros generate public items")]

use edgerush_proxy::upstream::h1::{H1Limits, codec::HeadReader};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn head(fields: usize) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\n{}\r\n",
        "x-example: value\r\n".repeat(fields)
    )
    .into_bytes()
}

#[library_benchmark]
#[bench::small(head(8), false)]
#[bench::boundary(head(16), false)]
#[bench::fallback(head(17), false)]
#[bench::large(head(128), false)]
#[bench::fragmented(head(8), true)]
fn read_head(bytes: Vec<u8>, fragmented: bool) -> Vec<u8> {
    let mut reader = HeadReader::default();
    let limits = H1Limits::default();
    if fragmented {
        for end in 0..bytes.len() {
            let _ = black_box(reader.read(black_box(&bytes[..end]), &limits));
        }
    }
    let _ = black_box(reader.read(black_box(&bytes), &limits));
    bytes
}

library_benchmark_group!(name = heads; benchmarks = read_head);
main!(library_benchmark_groups = heads);
