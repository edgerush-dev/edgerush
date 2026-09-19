//! Instruction counts for the whole routing decision: 1 000 hosts with ten routes each,
//! a wildcard host and a catch-all that fall through onto all of them, and a request with a
//! browser's worth of headers. The docs' target for this is under a microsecond.
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

use edgerush_router::{
    HeaderPredicate, HeaderPredicates, HostClaim, HostPattern, PathPattern, QueryPredicates,
    RequestParts, RouteMatch, Router, WildcardLabels,
};
use http::Method;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn route_match(host: Option<&str>, path: PathPattern, value: u32) -> RouteMatch<u32> {
    RouteMatch {
        hosts: vec![HostClaim {
            pattern: host.map(|host| {
                HostPattern::parse(host, WildcardLabels::OneOrMore).expect("valid host")
            }),
            falls_through: true,
            value: (),
        }],
        path,
        method: None,
        headers: HeaderPredicates::default(),
        query: QueryPredicates::default(),
        value,
    }
}

fn router() -> Router<u32> {
    let mut matches = Vec::new();
    for host in 0..1_000 {
        let name = format!("host-{host}.example.com");
        for route in 0..9 {
            let path = PathPattern::prefix(&format!("/api/v{route}")).expect("valid path");
            matches.push(route_match(Some(&name), path, host * 10 + route));
        }
        // One route per host that wants a header, ahead of its plain twin by precedence.
        let mut canary = route_match(
            Some(&name),
            PathPattern::prefix("/api/v5").expect("valid path"),
            host * 10 + 9,
        );
        canary.headers = HeaderPredicates::new([
            HeaderPredicate::exact("x-canary", "always").expect("valid predicate")
        ]);
        matches.push(canary);
    }
    let root = || PathPattern::prefix("/").expect("valid path");
    matches.push(route_match(Some("*.example.com"), root(), 100_000));
    matches.push(route_match(None, root(), 100_001));
    Router::new(matches)
}

fn headers() -> HeaderMap {
    let fields = [
        ("host", "host-500.example.com"),
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

// The router and the headers are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::prefix_route_of_its_host(router(), headers(), "host-500.example.com", "/api/v3/users/42")]
#[bench::past_a_header_predicate_that_fails(
    router(),
    headers(),
    "host-500.example.com",
    "/api/v5/users/42"
)]
#[bench::falls_through_to_the_wildcard_host(
    router(),
    headers(),
    "host-500.example.com",
    "/other/page"
)]
#[bench::host_nobody_claims(router(), headers(), "unknown.example.org", "/api/v3/users/42")]
fn route(
    router: Router<u32>,
    headers: HeaderMap,
    host: &str,
    path: &str,
) -> (Router<u32>, HeaderMap, Option<u32>) {
    let request = RequestParts {
        host,
        path,
        query: "page=3&per_page=50",
        method: &Method::GET,
        headers: &headers,
    };
    let routed = black_box(&router).route(black_box(&request)).copied();
    (router, headers, routed)
}

library_benchmark_group!(name = routing; benchmarks = route);
main!(library_benchmark_groups = routing);
