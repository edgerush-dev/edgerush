//! Instruction counts for modifying a request's headers, once per request on rules that
//! ask for it.
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

use edgerush_filters::HeaderModifier;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn request() -> HeaderMap {
    let fields = [
        ("host", "api.example.com"),
        (
            "user-agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        ),
        ("accept", "application/json, text/plain, */*"),
        ("accept-language", "en-GB,en;q=0.5"),
        ("accept-encoding", "gzip, deflate, br, zstd"),
        (
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.e30.c2lnbmF0dXJl",
        ),
        ("origin", "https://app.example.com"),
        ("x-request-id", "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f"),
        ("x-debug", "1"),
        (
            "cookie",
            "session=8f14e45fceea167a5a36dedd4bea2543; theme=dark",
        ),
    ];
    let mut headers = HeaderMap::new();
    for (name, value) in fields {
        headers.append(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    headers
}

fn typical() -> HeaderModifier {
    HeaderModifier::new(
        [("x-gateway", "edgerush")],
        [("x-forwarded-for-team", "payments")],
        ["x-debug"],
    )
    .expect("valid modifier")
}

// The modifier and the headers are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::set_add_remove_one_each(typical(), request())]
#[bench::nothing_to_do(HeaderModifier::default(), request())]
fn apply(modifier: HeaderModifier, mut headers: HeaderMap) -> (HeaderModifier, HeaderMap) {
    black_box(&modifier).apply(black_box(&mut headers));
    (modifier, headers)
}

library_benchmark_group!(name = header_modifier; benchmarks = apply);
main!(library_benchmark_groups = header_modifier);
