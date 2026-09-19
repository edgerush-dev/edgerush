//! Instruction counts for checking a rule's query parameter predicates against a typical
//! API query string.
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

use edgerush_router::{QueryPredicate, QueryPredicates};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

const PLAIN: &str = "page=3&per_page=50&sort=created_at&order=desc&status=open&tenant=acme&v=2";
const ENCODED: &str = "q=caf%C3%A9+au+lait&page=3&sort=created%5Fat&status=%6Fpen&tenant=acme&v=2";

fn two_exact() -> QueryPredicates {
    QueryPredicates::new([
        QueryPredicate::exact("tenant", "acme").expect("valid predicate"),
        QueryPredicate::exact("status", "open").expect("valid predicate"),
    ])
}

/// The regex engine builds its matching cache on first use; a running proxy has long paid
/// for that, so the set-up does.
fn one_regex() -> QueryPredicates {
    let rule =
        QueryPredicates::new([QueryPredicate::regex("v", "[0-9]+").expect("valid predicate")]);
    assert!(rule.matches(PLAIN));
    rule
}

// The rule is handed back so that dropping it is not measured.
#[library_benchmark]
#[bench::two_exact_hold(two_exact(), PLAIN)]
#[bench::two_exact_hold_in_an_encoded_query(two_exact(), ENCODED)]
#[bench::two_exact_parameter_missing(two_exact(), "page=3&per_page=50&sort=created_at")]
#[bench::no_predicates(QueryPredicates::default(), PLAIN)]
#[bench::one_regex_holds(one_regex(), PLAIN)]
fn matches(rule: QueryPredicates, query: &str) -> (QueryPredicates, bool) {
    let holds = black_box(&rule).matches(black_box(query));
    (rule, holds)
}

library_benchmark_group!(name = query; benchmarks = matches);
main!(library_benchmark_groups = query);
