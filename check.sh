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

say "fuzz targets"
# A package of its own, outside the workspace: nothing above builds it, so a signature it
# no longer matches would go unnoticed until somebody went to fuzz something.
cargo check --manifest-path fuzz/Cargo.toml

printf '%s\n' "all checks passed"
