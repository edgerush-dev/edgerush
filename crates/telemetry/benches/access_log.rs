//! Instruction counts for writing one access-log record as its line of JSON, into a buffer
//! with room for it, as a worker's batch has ([21 §4](../../../docs/21-access-logs.md)).
//!
//! Linux only (valgrind): `cargo bench -p edgerush-telemetry --bench access_log`, see the
//! repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_telemetry::access_log::{Kind, Protocol, Record};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// A request proxied over HTTP/1.1 from an IPv4 client, as most are.
fn proxied() -> Record<'static> {
    Record {
        time_ms: 1_759_569_153_123,
        kind: Kind::Request,
        id: Some("0199af5e-3a1b-7c2d-8e4f-5a6b7c8d9e0f"),
        listener: "web",
        client: Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))),
        peer: Some("10.0.0.5:51234".parse().expect("an address")),
        protocol: Some(Protocol::Http11),
        method: Some("GET"),
        host: Some("shop.example.com"),
        path: Some("/api/v1/orders/1234?expand=items&page=2"),
        status: Some(200),
        reason: None,
        route: Some("shop"),
        rule: Some(2),
        upstream: Some("orders"),
        endpoint: Some("10.1.2.3:8080".parse().expect("an address")),
        tries: Some(1),
        grpc_status: None,
        bytes_in: Some(0),
        bytes_out: Some(1_532),
        duration_us: Some(1_234),
        upstream_us: Some(1_101),
    }
}

fn from_ipv6() -> Record<'static> {
    Record {
        client: Some(IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0x85a3, 0, 0, 0x8a2e, 0x370, 0x7334,
        ))),
        peer: Some("[2001:db8::2]:51234".parse().expect("an address")),
        ..proxied()
    }
}

fn escaped() -> Record<'static> {
    Record {
        path: Some("/search?q=\"quoted\"\tand\\slashed"),
        ..proxied()
    }
}

fn batch() -> Vec<u8> {
    Vec::with_capacity(64 * 1024)
}

fn connection() -> Record<'static> {
    Record {
        kind: Kind::Connection,
        id: None,
        listener: "db",
        protocol: None,
        method: None,
        host: None,
        path: None,
        status: None,
        reason: Some("closed"),
        route: Some("postgres"),
        rule: None,
        upstream: Some("postgres"),
        endpoint: Some("10.1.2.4:5432".parse().expect("an address")),
        tries: None,
        bytes_in: Some(48_213),
        bytes_out: Some(1_204_771),
        duration_us: Some(93_412_007),
        upstream_us: None,
        ..proxied()
    }
}

// The buffer is made outside what is measured, as a worker's batch is there already, and
// handed back so that freeing it is not measured either.
#[library_benchmark]
#[bench::ipv4_request(proxied(), batch())]
#[bench::ipv6_request(from_ipv6(), batch())]
#[bench::escaped_path(escaped(), batch())]
#[bench::tcp_connection(connection(), batch())]
fn write(record: Record<'static>, mut out: Vec<u8>) -> Vec<u8> {
    black_box(&record).write(&mut out);
    out
}

library_benchmark_group!(name = access_log; benchmarks = write);

main!(library_benchmark_groups = access_log);
