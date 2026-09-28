//! Instruction counts for making a redirect's `Location`, once per request a redirect
//! answers.
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

use edgerush_filters::{PathModifier, Query, Redirect, Requested, Scheme};
use edgerush_router::PathPattern;
use http::HeaderValue;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

const PATH: &str = "/shop/catalogue/shoes/running";
const QUERY: &str = "size=42&colour=blue&utm_source=newsletter";

fn requested() -> Requested<'static> {
    Requested {
        scheme: Scheme::Http,
        host: "Shop.Example.com",
        path: PATH,
        query: Some(QUERY),
    }
}

/// `/old` to `/new` on the same origin: a relative `Location`.
fn path_moved() -> Redirect {
    let prefix = PathPattern::prefix("/shop").expect("valid prefix");
    let path = PathModifier::prefix(&prefix, "/store").expect("valid replacement");
    Redirect::new(301, None, None, None, Some(path), Query::Keep).expect("valid redirect")
}

/// Plain HTTP to HTTPS: the scheme stated, the host the request's.
fn https_asked() -> Redirect {
    Redirect::new(308, Some(Scheme::Https), None, None, None, Query::Keep).expect("valid redirect")
}

/// Another origin, all of it stated and made once.
fn origin_moved() -> Redirect {
    Redirect::new(
        301,
        Some(Scheme::Https),
        Some("www.example.org"),
        None,
        None,
        Query::Keep,
    )
    .expect("valid redirect")
}

// The redirect is handed back so that dropping it is not measured.
#[library_benchmark]
#[bench::moved_path(path_moved())]
#[bench::to_https(https_asked())]
#[bench::to_other_origin(origin_moved())]
fn location(redirect: Redirect) -> (Redirect, HeaderValue) {
    let location = black_box(&redirect)
        .location(black_box(&requested()))
        .expect("a valid location");
    (redirect, location)
}

library_benchmark_group!(name = redirect; benchmarks = location);
main!(library_benchmark_groups = redirect);
