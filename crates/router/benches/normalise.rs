//! Instruction counts for normalising a request path, which every request pays for once.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-router`, see the repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_router::{EncodedSlash, NormaliseError, normalise_path, normalise_path_with};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::borrow::Cow;
use std::hint::black_box;

// The result is handed back so that freeing a rewritten path is not measured.
#[library_benchmark]
#[bench::already_normal_short("/api/v1/users")]
#[bench::already_normal_long("/api/v1/accounts/1234567890/orders/0987654321/items/42/details.json")]
#[bench::canonical_encoding_kept("/files/annual%20report%202026.pdf")]
#[bench::decode_and_upper_case("/files/%61nnual%20report%c3%a9.pdf")]
#[bench::dot_segments_and_slashes("/api//v1/./users/../accounts/")]
#[bench::rejected("/api/v1/%2e%2e/admin")]
fn normalise(path: &str) -> Result<Cow<'_, str>, NormaliseError> {
    black_box(normalise_path(black_box(path)))
}

// A path with an encoded slash takes two passes, the first stopping at it: what a request
// to a rule that keeps one pays (03 §11).
#[library_benchmark]
#[bench::kept("/api/v4/projects/group%2Fproject/issues")]
#[bench::kept_and_decoded("/api/v4/projects/group%2fproject/%69ssues")]
fn encoded_slash(path: &str) -> Result<Cow<'_, str>, NormaliseError> {
    let path = black_box(path);
    match normalise_path(path) {
        Err(NormaliseError::EncodedSlash) => {
            black_box(normalise_path_with(path, EncodedSlash::Keep))
        }
        refused_or_not => black_box(refused_or_not),
    }
}

library_benchmark_group!(name = normalise_group; benchmarks = normalise, encoded_slash);
main!(library_benchmark_groups = normalise_group);
