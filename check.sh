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

say "bench scripts"
# The Python that runs the benchmarks and reads their results (bench/, bench/ratelimit/)
# has tests of its own, and nothing else runs them: a script that reads a result wrongly
# is otherwise found only when a run's table comes out short. Standard library only, and
# a few seconds. Two runs, as bench/ is not a package and discovery does not go down into
# bench/ratelimit/.
python3 -m unittest discover -s bench -p 'test_*.py'
python3 -m unittest discover -s bench/ratelimit -p 'test_*.py'

say "dependencies"
# Advisories, licences, sources and crates in two versions (deny.toml), for the workspace's
# lock file and fuzz/'s. Both are checked against the one file, and each is told not to
# mind an entry that is there for the other: libFuzzer's licence is fuzz/'s, the benchmark
# harness's advisories the workspace's. The advisories are read from the RustSec database,
# so this needs the network, and a new advisory fails it until it is dealt with.
cargo deny check --allow license-exception-not-encountered
cargo deny --manifest-path fuzz/Cargo.toml check --allow advisory-not-detected
# And the published versions vendor/ holds patched copies of: advisories are looked up only
# for crates from a registry, and the copies are paths (vendor/bases/Cargo.toml).
cargo deny --manifest-path vendor/bases/Cargo.toml check advisories --allow advisory-not-detected

say "clippy"
cargo clippy --all-targets -- -D warnings

say "without test features"
# Clippy and the tests build every crate with its dev-dependencies' features added in: a
# feature the code uses but never asks for (tokio's macros, say) builds there and fails in
# the binary that ships.
cargo check --workspace

say "tests"
# By cargo-nextest, each test in a process of its own and every binary's tests side by side.
# `cargo test` runs one binary after another, waiting on each binary's slowest test in turn:
# on the laptop that made the tests 9 minutes rather than under 5.
#
# But for the rate-limiting service's cluster tests, which cargo runs after, as one process
# with nothing else beside it. Timed in tens of milliseconds, they keep one another apart by
# a lock in that process (`ALONE`, crates/ratelimit-service/tests/cluster.rs): in a process
# each, beside the rest of the workspace, the lock would hold nothing back. Named with
# --workspace, as `-p` would build the crates again with other features.
cargo nextest run --workspace -E 'not binary_id(edgerush-ratelimit-service::cluster)'
cargo test --workspace --test cluster

say "doc tests"
# nextest leaves these out.
cargo test --workspace --doc

say "vendored quiche's own tests"
# vendor/quiche is not a member of the workspace, so the tests above never reach it, and its
# own suite holds most of the tests of EdgeRush's changes to it (vendor/quiche/VENDORED.md).
# Its lock file is its own, and so is its target directory.
cargo test --manifest-path vendor/quiche/Cargo.toml --no-default-features \
    --features boringssl-boring-crate --lib --target-dir "${CARGO_TARGET_DIR:-target}/vendored"

say "loom"
# The pooled entries' concurrency under `--cfg loom`, every interleaving of their threads
# (crates/ratelimit/src/service/pooled/loom_tests.rs), and a backend pod's count raced for
# by workers (crates/protection/src/loom_tests.rs): the tests above run them only as the
# scheduler happens to. Optimised, as loom's search is slow unoptimised, and in a target
# directory of their own, as the flag rebuilds every crate.
RUSTFLAGS="${RUSTFLAGS:-} --cfg loom" cargo test --release -p edgerush-ratelimit \
    -p edgerush-protection --lib --target-dir "${CARGO_TARGET_DIR:-target}/loom" loom

say "a request's future, made in its slot"
# A request's future is made in its slot only while its head comes first in it, and the
# order is rustc's choice: as little as 16 bytes more in the future it awaits has had all
# 6 KB of it made beside the slot and copied in, some 600 more instructions an HTTP/1
# request, with every test passing (docs 14 §3). So the layout the binary is built with is
# read, as rustc's -Zprint-type-sizes prints it. Only a nightly compiler prints it, and
# RUSTC_BOOTSTRAP lets the pinned one, in a target directory of its own, as it rebuilds
# every crate; rustc prints only what it compiles, so the binary's own crate is built
# afresh each time. A debug build lays the future out as release does. Linux alone: the
# layout is the target's, and Windows' types are of other sizes.
if [ "$(uname -s)" = Linux ]; then
    layout="${CARGO_TARGET_DIR:-target}/layout"
    cargo clean -q -p edgerush --target-dir "$layout"
    RUSTC_BOOTSTRAP=1 cargo rustc -q -p edgerush --bin edgerush --target-dir "$layout" \
        -- -Zprint-type-sizes \
        | python3 bench/layout.py \
            '{async fn body of edgerush_proxy::serve::respond::<impl edgerush_proxy::Worker>::handle_head<edgerush_proxy::raw::RawHead>()}' \
            .head
fi

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
