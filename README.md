# EdgeRush

A Kubernetes-native API gateway written in Rust: an operator, control planes and data
planes in one binary; Ingress and Gateway API served from the same data plane; cluster-wide
rate limiting with no external store.

**Status: just started.** The workspace, lints, tests and CI exist; the gateway does not
yet. This is a hobby project with no timelines.

## Building

The Rust toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on first use.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All three must pass before every commit, on Windows and on Linux. There is no CI yet: the
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

## Layout

```
crates/edgerush    the binary (operator, control plane and data plane will be subcommands)
crates/router      request matching (`edgerush-router`): pure logic, no I/O
```

Crates are internal to this workspace and are not published.

## License

Apache-2.0 — see [LICENSE](LICENSE).
