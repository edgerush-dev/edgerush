//! Instruction counts for what forwarding asks of every request: whether its peer is a
//! trusted proxy, which of its headers only a trusted proxy may send, and — from a trusted
//! proxy — who its client is.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-filters`, see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_filters::forwarding::{HeaderNames, TrustedProxies, client_address};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::IpAddr;

/// The names of a browser's request, as a request's head is surveyed for them.
const NAMES: [&[u8]; 10] = [
    b"host",
    b"user-agent",
    b"accept",
    b"accept-language",
    b"accept-encoding",
    b"authorization",
    b"origin",
    b"x-request-id",
    b"referer",
    b"cookie",
];

fn defaults() -> HeaderNames {
    HeaderNames::new(["Forwarded", "X-Real-IP", "X-Forwarded-*"]).expect("valid names")
}

fn cloud() -> TrustedProxies {
    // A load balancer's private range and a CDN's handful of public ones.
    TrustedProxies::new([
        "10.0.0.0/8",
        "130.176.0.0/17",
        "15.158.0.0/16",
        "64.252.64.0/18",
        "2600:9000::/28",
    ])
    .expect("valid ranges")
}

fn address(text: &str) -> IpAddr {
    text.parse().expect("an address")
}

#[library_benchmark]
#[bench::default_names(defaults())]
#[bench::none(HeaderNames::default())]
fn survey(names: HeaderNames) -> (HeaderNames, usize) {
    let found = NAMES
        .iter()
        .filter(|name| black_box(&names).matches(black_box(name)))
        .count();
    (names, found)
}

#[library_benchmark]
#[bench::untrusted_v4(cloud(), address("203.0.113.7"))]
#[bench::trusted_v4_mapped(cloud(), address("::ffff:10.1.0.5"))]
#[bench::nobody(TrustedProxies::default(), address("10.1.0.5"))]
fn trusts(trusted: TrustedProxies, peer: IpAddr) -> (TrustedProxies, bool) {
    let trusts = black_box(&trusted).trusts(black_box(peer));
    (trusted, trusts)
}

#[library_benchmark]
#[bench::one_hop(cloud(), "198.51.100.9")]
#[bench::forged_then_cdn(cloud(), "1.2.3.4, 198.51.100.9, 130.176.1.2")]
fn walk(trusted: TrustedProxies, forwarded_for: &str) -> (TrustedProxies, IpAddr) {
    let client = client_address(
        black_box(address("10.1.0.5")),
        [black_box(forwarded_for.as_bytes())],
        black_box(&trusted),
    );
    (trusted, client)
}

library_benchmark_group!(name = forwarding; benchmarks = survey, trusts, walk);
main!(library_benchmark_groups = forwarding);
