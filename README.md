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

All three must pass before every commit. They run in CI on Linux and Windows.

## Layout

```
crates/edgerush    the binary (operator, control plane and data plane will be subcommands)
crates/router      request matching (`edgerush-router`): pure logic, no I/O
```

Crates are internal to this workspace and are not published.

## License

Apache-2.0 — see [LICENSE](LICENSE).
