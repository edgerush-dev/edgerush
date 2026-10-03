# EdgeRush

A Kubernetes-native API gateway written in Rust: an operator, control planes and data
planes in one binary; Ingress and Gateway API served from the same data plane; cluster-wide
rate limiting with no external store.

**Status: the data plane runs; Kubernetes does not yet.** The proxy serves traffic from a
YAML file, the development harness; the control plane, the operator and the rate limiter
are still to be built. Not for production. A hobby project with no timelines.

## What it is for

- **One data plane for Ingress and Gateway API**, side by side, from ordinary Kubernetes
  objects. Ingress stays a first-class API.
- **Operator-driven.** `ControlPlane` and `DataPlane` resources become Deployments and
  Services; one control plane can drive many data planes, each its own failure domain.
- **Cluster-wide rate limiting with no external store.** Counters live in the data plane's
  memory and are shared between its pods peer to peer, off the request path, and limits
  keep working when the control plane is down.
- **Low added latency, measured.** Thread-per-core on Tokio, our own HTTP/1 server and
  client, compiled filter chains. `bench/` compares EdgeRush with NGINX, HAProxy, Envoy and
  Kong doing the same work on the same machine.

## What works today

The data plane, run from a file that is read again every second; a change takes over
without dropping a request.

- **Protocols:** HTTP/1.1, HTTP/2 and HTTP/3 (QUIC, on quiche) from clients; HTTP/1.1 and
  HTTP/2 to backends; gRPC; WebSocket over all three client protocols; TCP and TLS
  passthrough, routed by SNI.
- **TLS** on BoringSSL: certificates chosen by SNI and rotated by replacing their files;
  mTLS towards clients and towards backends.
- **Routing** with Gateway API's matches and precedence (host, path, method, headers,
  query); header changes, redirects, rewrites and mirrors; weighted backends.
- **Upstreams:** pooled connections; power-of-two-choices or round-robin, with slow start;
  HTTP, gRPC and TCP health checks; endpoints that cannot be connected to set aside;
  timeouts for the request and for each try; retries on HTTP and gRPC statuses, within a
  budget.
- **At the edge:** `X-Forwarded-*` and `Via`, believed only from listed proxies; PROXY
  protocol v1 and v2; request IDs; caps on connections and a fair share of each worker for
  every upstream; a drain on SIGTERM; Prometheus metrics.

Not yet: anything Kubernetes (the control plane, the operator, translating Ingress and
Gateway API), rate limiting, access logs, authentication. The request path comes first,
then rate limiting, then the control plane and the operator.

## Trying it

With a backend on port 9000, save this as `edgerush.yaml`:

```yaml
listeners:
  web:
    address: "127.0.0.1:8080"
    protocol: http
    proxy_protocol: off
    forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }
    request_id: generate
routes:
  - name: hello
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: false }
    rules:
      - matches:
          - path: { prefix: / }
        forward:
          backends:
            - { upstream: app, weight: 1 }
upstreams:
  app: { load_balancer: p2c, endpoints: ["127.0.0.1:9000"] }
```

```sh
cargo build --release -p edgerush
python3 -m http.server 9000 --bind 127.0.0.1 &   # a backend, if you have none
./target/release/edgerush proxy --config edgerush.yaml
curl -i http://127.0.0.1:8080/
```

The format asks for choices such as `forwarding` and `proxy_protocol` to be stated rather
than defaulted, as the control plane will state them. `bench/proxy.yaml` and
`fuzz/seeds/config/` have fuller examples, with TLS, HTTP/3, passthrough and more;
`edgerush proxy --help` lists the options. A worker runs on every CPU, which shares each
port between them; Windows cannot, so there add `--workers 1`.

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
on Windows and on Linux. One of them, the dependencies' advisories and licences
(`deny.toml`), needs `cargo-deny` (`cargo install --locked cargo-deny@0.20.2`, the version
CI uses) and the network, to read the RustSec advisory database. CI
(`.github/workflows/ci.yml`) runs part of them, on Linux and Windows, for every push to
`main` and every pull request; `check.sh` is the full set.

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

The macro benchmark, the proxy under load beside NGINX, HAProxy, Envoy and Kong, is in
`bench/` ([bench/README.md](bench/README.md)).

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

Targets compare the real code with the slow reference implementations in the router's and
the filters' `reference` modules, the same ones the property tests use; `request_host`
compares with the `http` crate's reading instead, and `quic_header` with quiche's parser.
`config` reads a harness config file and compiles it, holding both to not failing and what
compiles to being whole. An input that fails is saved under `fuzz/artifacts/`; fix the bug
and add the input to the unit tests. Run a target after changing the code it covers. Being
outside the workspace, the package is formatted on its own:
`cargo fmt --manifest-path fuzz/Cargo.toml`.

## Layout

```
crates/edgerush    the binary (operator, control plane and data plane will be subcommands); so far
                   `edgerush proxy --config file.yaml`, a data plane run from a file for development
crates/config      the config model and its compilation (`edgerush-config`): pure, format-free
crates/filters     built-in filters (`edgerush-filters`): pure, on plain `http` types
crates/proxy       the data plane (`edgerush-proxy`): listeners, TLS, our HTTP/1 server and client,
                   HTTP/2 on h2, HTTP/3 on quiche, upstream pools and the request core between them
crates/router      request matching (`edgerush-router`): pure logic, no I/O
crates/telemetry   metrics (`edgerush-telemetry`): counters sharded by thread, the Prometheus text format
bench              the macro benchmark: load generator, proxy and backend on one Linux machine
                   (bench/README.md)
fuzz               fuzz targets (cargo-fuzz; not part of the workspace)
interop            EdgeRush as a quic-interop-runner endpoint (interop/README.md)
vendor/quiche      quiche 0.30.0 with changes of ours, built in place of the published crate
                   (vendor/quiche/VENDORED.md; BSD-2-Clause, not part of the workspace)
vendor/h2          h2 0.4.19 with changes of ours, built in place of the published crate
                   (vendor/h2/VENDORED.md; MIT, not part of the workspace)
```

Crates are internal to this workspace and are not published.

## Contributing

Contributions are welcome, AI-assisted ones included: [CONTRIBUTING.md](CONTRIBUTING.md)
says how. Security problems are reported privately ([SECURITY.md](SECURITY.md)), and
everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).

## License

Apache-2.0 — see [LICENSE](LICENSE). `vendor/quiche` is quiche's, under its own BSD 2-Clause
licence ([vendor/quiche/COPYING](vendor/quiche/COPYING)), and `vendor/h2` is h2's, under its
own MIT licence ([vendor/h2/LICENSE](vendor/h2/LICENSE)).
