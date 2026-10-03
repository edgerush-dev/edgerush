# AGENTS.md

Notes for AI coding agents working in this repository. The rules the code is reviewed
against are in [CONTRIBUTING.md](CONTRIBUTING.md): read it first and follow it. This file
adds what an agent tends to trip over here.

## The project

EdgeRush is a Kubernetes-native API gateway in Rust, at an early stage: the data plane runs
from a YAML file, and Kubernetes, the control plane and rate limiting are not built yet.
[README.md](README.md) has what works, how to build (BoringSSL needs cmake and libclang,
and NASM on Windows) and the layout of the crates.

## Before calling a change done

- `./check.sh` passes. Judge it by its exit status: a build that fails runs no tests, and
  output with no complaints in it is also what a run that never happened looks like.
- Each new test fails when the change is reverted or broken; a test that passes either way
  does not test it.
- No test was weakened, skipped or deleted to get there.

## Easy to get wrong here

- `fuzz/` is a package outside the workspace, so workspace commands do not reach it.
  Format it with `cargo fmt --manifest-path fuzz/Cargo.toml`.
- `vendor/quiche` and `vendor/h2` are patched copies of third-party crates; each has a
  `VENDORED.md` saying what is changed and why. Leave them alone unless the task is about
  them.
- The files under `crates/proxy/tests/corpus/` and `fuzz/seeds/` are wire bytes: a CRLF
  there is the case under test. Never normalise their line endings.
- The code cites design documents ("03 §6", "15 §3") that are not in this repository. If
  you cannot read one, do not guess what it says; ask.
- The pure crates (`router`, `config`, `filters`) do no I/O: clocks, sockets and randomness
  are passed in.
- No new dependency without an issue that agreed it.
- On Windows, run the proxy with `--workers 1`: workers share a port, which Windows cannot
  do.

## Commits

One logical change per commit, with its tests and docs. Commit and push only when the
person you are working for asks. Credit yourself with a `Co-Authored-By:` trailer.
