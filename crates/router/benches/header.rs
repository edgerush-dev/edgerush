//! Instruction counts for checking a rule's header predicates against a request with a
//! browser's worth of headers.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-router`, see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_router::{HeaderPredicate, HeaderPredicates};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn request(version: &'static str) -> HeaderMap {
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
        ("referer", "https://app.example.com/orders"),
        ("x-request-id", "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f"),
        ("x-tenant", "acme"),
        ("x-api-version", version),
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

fn exact_rule() -> HeaderPredicates {
    HeaderPredicates::new([
        HeaderPredicate::exact("x-tenant", "acme").expect("valid predicate"),
        HeaderPredicate::exact("x-api-version", "2026-09").expect("valid predicate"),
        HeaderPredicate::exact("origin", "https://app.example.com").expect("valid predicate"),
    ])
}

/// The regex engine builds its matching cache on first use; a running proxy has long paid
/// for that, so the set-up does.
fn regex_rule() -> HeaderPredicates {
    let rule = HeaderPredicates::new([
        HeaderPredicate::regex("x-api-version", "2026-[0-9]{2}").expect("valid predicate")
    ]);
    assert!(rule.matches(&request("2026-09")));
    rule
}

// The rule and the request are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::three_exact_all_hold(exact_rule(), request("2026-09"))]
#[bench::three_exact_second_fails(exact_rule(), request("2025-01"))]
#[bench::no_predicates(HeaderPredicates::default(), request("2026-09"))]
#[bench::one_regex_holds(regex_rule(), request("2026-09"))]
fn matches(rule: HeaderPredicates, request: HeaderMap) -> (HeaderPredicates, HeaderMap, bool) {
    let holds = black_box(&rule).matches(black_box(&request));
    (rule, request, holds)
}

library_benchmark_group!(name = header; benchmarks = matches);
main!(library_benchmark_groups = header);
