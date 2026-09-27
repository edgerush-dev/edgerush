# EdgeRush

A Kubernetes-native API gateway written in Rust: an operator, control planes and data
planes in one binary; Ingress and Gateway API served from the same data plane; cluster-wide
rate limiting with no external store.

**Status: just started.** The workspace, lints, tests and CI exist; the gateway does not
yet. This is a hobby project with no timelines.

## Building

The Rust toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on first use.

TLS is BoringSSL, built from source by `boring-sys`, which needs cmake and libclang
(`apt install cmake libclang-dev` on Ubuntu) and on Windows NASM as well. On Windows,
point `LIBCLANG_PATH` at the directory holding `libclang.dll` and `ASM_NASM` at
`nasm.exe`, for instance in the `[env]` table of `~/.cargo/config.toml`. Go is not needed.
A first build of BoringSSL takes a few minutes.

```sh
./check.sh
```

runs every check, stopping at the first that fails; all must pass before every commit,
on Windows and on Linux. There is no CI yet: the
workflow in `.github/workflows/ci.yml` is kept ready for when the repository is published.

## Benchmarks

Micro benchmarks count instructions under valgrind (`iai-callgrind`), so results do not
depend on how busy the machine is. They need Linux; WSL2 is enough.

```sh
cargo install iai-callgrind-runner --version 0.16.1   # must match Cargo.toml exactly
cargo bench -p edgerush-router
```

Valgrind must be recent: 3.18 (Ubuntu 22.04) runs but reports zero instructions for
current Rust; 3.27 works. Each run is compared with the previous one kept in `target/iai`.
From a checkout on a Windows drive, point `CARGO_TARGET_DIR` at the Linux filesystem.

## Fuzzing

Code that reads untrusted input has a fuzz target in `fuzz/`, a package of its own outside
the workspace. Fuzzing needs a nightly compiler (for the instrumentation only; EdgeRush
itself builds on the pinned stable toolchain) and Linux; WSL2 is enough.

```sh
rustup toolchain install nightly --profile minimal
cargo install cargo-fuzz
cargo +nightly fuzz list
mkdir -p fuzz/corpus/normalise
cargo +nightly fuzz run normalise fuzz/corpus/normalise fuzz/seeds/normalise -- -max_total_time=600
```

The first directory is the corpus the fuzzer grows (not kept in git); `fuzz/seeds/` holds a
few hand-written inputs per target that show it the input format.

Targets compare the real code with the slow reference implementations in the router's
`reference` module, the same ones the property tests use; `request_host` compares with the
`http` crate's reading instead, and `quic_header` with quiche's parser. An input that
fails is saved under `fuzz/artifacts/`; fix the bug and add the input to the unit tests.
Run a target after changing the code it covers. Being outside the workspace, the package
is formatted on its own: `cargo fmt --manifest-path fuzz/Cargo.toml`.

## Layout

```
crates/edgerush    the binary (operator, control plane and data plane will be subcommands); so far
                   `edgerush proxy --config file.yaml`, a data plane run from a file for development
crates/config      the config model and its compilation (`edgerush-config`): pure, format-free
crates/filters     built-in filters (`edgerush-filters`): pure, on plain `http` types
crates/proxy       the data plane (`edgerush-proxy`): the request core on plain `http` types, and the
                   serving and forwarding around it, the only code that touches hyper
crates/router      request matching (`edgerush-router`): pure logic, no I/O
crates/telemetry   metrics (`edgerush-telemetry`): counters sharded by thread, the Prometheus text format
bench              the macro benchmark: load generator, proxy and backend on one Linux machine
                   (bench/README.md)
fuzz               fuzz targets (cargo-fuzz; not part of the workspace)
vendor/quiche      quiche 0.30.0 with two changes, built in place of the published crate
                   (vendor/quiche/VENDORED.md; BSD-2-Clause, not part of the workspace)
vendor/h2          h2 0.4.19 with one addition, built in place of the published crate
                   (vendor/h2/VENDORED.md; MIT, not part of the workspace)
```

Crates are internal to this workspace and are not published.

## License

Apache-2.0 — see [LICENSE](LICENSE). `vendor/quiche` is quiche's, under its own BSD 2-Clause
licence ([vendor/quiche/COPYING](vendor/quiche/COPYING)), and `vendor/h2` is h2's, under its
own MIT licence ([vendor/h2/LICENSE](vendor/h2/LICENSE)).
