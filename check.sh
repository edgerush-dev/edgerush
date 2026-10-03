#!/usr/bin/env bash
# The checks that must pass before a commit, as CLAUDE.md lists them.
#
# By exit status and not by reading the output: a build that fails produces no failing
# tests, and counting complaints in a log says a run went well when it never ran at all.
# Each check is named as it passes, and the first that does not stops the lot.
set -euo pipefail

cd "$(dirname "$0")"

say() { printf '%s\n' "-- $1"; }

# Linux gives a shell 1024 open files unless told otherwise, and the socket tests running
# side by side can need more; a run that hits the limit fails for a reason that is not the
# code's. Raised for this run alone, to 8192 or as far as the hard limit allows, and never
# lowered.
if [ "$(uname -s)" = Linux ]; then
    want=8192
    soft=$(ulimit -Sn)
    hard=$(ulimit -Hn)
    if [ "$soft" != unlimited ] && [ "$soft" -lt "$want" ]; then
        if [ "$hard" != unlimited ] && [ "$hard" -lt "$want" ]; then
            want=$hard
        fi
        ulimit -n "$want"
    fi
fi

say "fmt"
cargo fmt --check

say "dependencies"
# Advisories, licences, sources and crates in two versions (deny.toml), for the workspace's
# lock file and fuzz/'s. Both are checked against the one file, and each is told not to
# mind an entry that is there for the other: libFuzzer's licence is fuzz/'s, the benchmark
# harness's advisories the workspace's. The advisories are read from the RustSec database,
# so this needs the network, and a new advisory fails it until it is dealt with.
cargo deny check --allow license-exception-not-encountered
cargo deny --manifest-path fuzz/Cargo.toml check --allow advisory-not-detected

say "clippy"
cargo clippy --all-targets -- -D warnings

say "without test features"
# Clippy and the tests build every crate with its dev-dependencies' features added in: a
# feature the code uses but never asks for (tokio's macros, say) builds there and fails in
# the binary that ships.
cargo check --workspace

say "tests"
cargo test --workspace

say "docs"
# Rustdoc's warnings are not failures by default, and a stale link is exactly the kind of
# thing nobody reads a log to find.
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

say "benchmarks"
# The proxy's benchmarks reach its internals through its `fuzzing` feature, which nothing
# above turns on, and clippy's --all-targets leaves out a target whose required features
# are off: a signature they no longer match would go unnoticed until somebody ran one, as
# benches/places.rs did from edef9a4 to fe60d90. Built, not run: running needs valgrind.
cargo check -p edgerush-proxy --features fuzzing --benches

say "fuzz targets"
# A package of its own, outside the workspace: nothing above builds it, so a signature it
# no longer matches would go unnoticed until somebody went to fuzz something.
cargo check --manifest-path fuzz/Cargo.toml

say "fuzz targets, as cargo-fuzz builds them"
# With `--cfg fuzzing`, which cargo-fuzz sets: code under it (vendored h2's own fuzzing
# module, for one) builds only then, and a path dependency's lints are not capped at
# warnings as a registry dependency's are. Cargo keeps what this builds apart from the
# builds above, so it costs a second build of every crate once and little after.
RUSTFLAGS="${RUSTFLAGS:-} --cfg fuzzing" cargo check --manifest-path fuzz/Cargo.toml

printf '%s\n' "all checks passed"
