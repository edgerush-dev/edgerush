//! Instruction counts for what a TLS passthrough listener reads of a connection before it
//! knows where the connection goes: its ClientHello
//! ([17 §3](../../../docs/17-tcp-and-tls-passthrough.md)). Once a connection, when the whole
//! ClientHello has come; the reader reads everything again as more comes, so a ClientHello
//! that arrives in pieces costs about this for each.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench l4`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::l4::hello::{Hello, read};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// BoringSSL's ClientHello with a post-quantum key share, asking for `api.example.com`,
/// in one record; the same cut into three records; and one asking for no name. The fuzz
/// target's seeds.
const NAMED: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/named");
const ACROSS_RECORDS: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/across_records");
const UNNAMED: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/unnamed");

#[library_benchmark]
#[bench::one_record(NAMED)]
#[bench::three_records(ACROSS_RECORDS)]
#[bench::no_name(UNNAMED)]
fn read_hello(bytes: &[u8]) -> bool {
    black_box(matches!(read(black_box(bytes)), Hello::Whole(_)))
}

library_benchmark_group!(name = l4; benchmarks = read_hello);

main!(library_benchmark_groups = l4);
