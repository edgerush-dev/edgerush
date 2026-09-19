//! Instruction counts for the per-request path lookup, against one host with far more
//! routes than any real one: 10 000 exact paths and 1 110 prefixes nested three deep, and
//! for the regex cases ten or a hundred regex routes on top.
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

use edgerush_router::{PathIndex, PathPattern};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn large_index() -> PathIndex<u32> {
    index_with_regexes(0)
}

fn index_with_regexes(regexes: usize) -> PathIndex<u32> {
    let regexes = (0..regexes).map(|n| PathPattern::regex(&format!("/regex-{n}/[0-9]+/items")));
    let exact = (0..10_000).map(|n| PathPattern::exact(&format!("/pages/page-{n}.html")));
    let prefixes = (0..10).flat_map(|a| {
        let one = format!("/api-{a}");
        let below = (0..10).flat_map(move |b| {
            let two = format!("/api-{a}/v{b}");
            let below = (0..10).map(move |c| format!("/api-{a}/v{b}/resource-{c}"));
            std::iter::once(two).chain(below)
        });
        std::iter::once(one).chain(below)
    });
    let prefixes = prefixes.map(|prefix| PathPattern::prefix(&prefix));
    let patterns = regexes
        .chain(exact)
        .chain(prefixes)
        .chain([PathPattern::prefix("/")])
        .map(|pattern| pattern.expect("valid pattern"));
    PathIndex::new(patterns.zip(0..))
}

// Building the index happens in the argument expressions, outside the measured function;
// handing the index back keeps dropping it outside as well.

// The usual case: the best candidate serves the request.
#[library_benchmark]
#[bench::exact_hit(large_index(), "/pages/page-5000.html")]
#[bench::prefix_one_deep(large_index(), "/api-5/other/things/123")]
#[bench::prefix_three_deep(large_index(), "/api-5/v5/resource-5/items/123/details")]
#[bench::root_only(large_index(), "/nowhere/in/particular")]
fn best_candidate(index: PathIndex<u32>, path: &str) -> (PathIndex<u32>, Option<u32>) {
    let best = black_box(&index).lookup(black_box(path)).next().copied();
    (index, best)
}

// The worst case: every candidate is turned down by a later predicate.
#[library_benchmark]
#[bench::prefix_three_deep(large_index(), "/api-5/v5/resource-5/items/123/details")]
fn all_candidates(index: PathIndex<u32>, path: &str) -> (PathIndex<u32>, usize) {
    let candidates = black_box(&index).lookup(black_box(path)).count();
    (index, candidates)
}

/// The regex engine builds its matching cache the first time a regex is used (tens of
/// thousands of instructions, once per regex and thread). A running proxy has long paid
/// that, so the set-up pays it here.
fn warmed(regexes: usize, path: &str) -> PathIndex<u32> {
    let index = index_with_regexes(regexes);
    assert!(index.lookup(path).count() > 0, "nothing matches {path}");
    index
}

// What a host pays for having regex routes: nothing when an exact path matches, one run of
// each regex tried otherwise.
#[library_benchmark]
#[bench::exact_hit_runs_no_regex(warmed(10, "/pages/page-5000.html"), "/pages/page-5000.html")]
#[bench::first_of_ten_regexes(warmed(10, "/regex-0/12345/items"), "/regex-0/12345/items")]
#[bench::last_of_ten_regexes(warmed(10, "/regex-9/12345/items"), "/regex-9/12345/items")]
#[bench::prefix_after_ten_regexes(
    warmed(10, "/api-5/v5/resource-5/items/123"),
    "/api-5/v5/resource-5/items/123"
)]
#[bench::prefix_after_hundred_regexes(
    warmed(100, "/api-5/v5/resource-5/items/123"),
    "/api-5/v5/resource-5/items/123"
)]
fn with_regexes(index: PathIndex<u32>, path: &str) -> (PathIndex<u32>, Option<u32>) {
    let best = black_box(&index).lookup(black_box(path)).next().copied();
    (index, best)
}

library_benchmark_group!(
    name = path_index;
    benchmarks = best_candidate, all_candidates, with_regexes
);
main!(library_benchmark_groups = path_index);
