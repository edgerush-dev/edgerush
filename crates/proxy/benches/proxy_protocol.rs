//! Instruction counts for the PROXY protocol header
//! ([20](../../../docs/20-proxy-protocol.md)): reading one at the start of a connection from
//! a load balancer, and writing one ahead of a tunnel's bytes. Once a connection each, on
//! listeners and upstreams that state it; a header that arrives in pieces is read again as
//! each comes.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench proxy_protocol`, see the
//! repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_proxy::proxy_protocol::{Read, Version, proxied, read};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::SocketAddr;

/// The fuzz target's seeds: a v1 line for IPv4 and for IPv6, and a v2 header for each, as
/// AWS's and Google's load balancers send them.
const V1_TCP4: &[u8] = include_bytes!("../../../fuzz/seeds/proxy_header/v1_tcp4");
const V1_TCP6: &[u8] = include_bytes!("../../../fuzz/seeds/proxy_header/v1_tcp6");
const V2_TCP4: &[u8] = include_bytes!("../../../fuzz/seeds/proxy_header/v2_tcp4");
const V2_TCP6: &[u8] = include_bytes!("../../../fuzz/seeds/proxy_header/v2_tcp6");
const V2_TLVS: &[u8] = include_bytes!("../../../fuzz/seeds/proxy_header/v2_tlvs");

#[library_benchmark]
#[bench::v1_tcp4(V1_TCP4)]
#[bench::v1_tcp6(V1_TCP6)]
#[bench::v2_tcp4(V2_TCP4)]
#[bench::v2_tcp6(V2_TCP6)]
#[bench::v2_with_tlvs(V2_TLVS)]
fn read_header(bytes: &[u8]) -> bool {
    black_box(matches!(read(black_box(bytes)), Read::Whole { .. }))
}

fn pair(source: &str, destination: &str) -> (SocketAddr, SocketAddr) {
    (
        source.parse().expect("an address"),
        destination.parse().expect("an address"),
    )
}

#[library_benchmark]
#[bench::v1_tcp4(Version::V1, pair("192.0.2.1:56324", "198.51.100.7:443"))]
#[bench::v1_tcp6(Version::V1, pair("[2001:db8::1]:56324", "[2001:db8::2]:443"))]
#[bench::v2_tcp4(Version::V2, pair("192.0.2.1:56324", "198.51.100.7:443"))]
#[bench::v2_tcp6(Version::V2, pair("[2001:db8::1]:56324", "[2001:db8::2]:443"))]
fn write_header(version: Version, (source, destination): (SocketAddr, SocketAddr)) -> usize {
    black_box(
        proxied(
            black_box(version),
            black_box(source),
            black_box(destination),
        )
        .as_bytes()
        .len(),
    )
}

library_benchmark_group!(name = proxy_protocol; benchmarks = read_header, write_header);

main!(library_benchmark_groups = proxy_protocol);
