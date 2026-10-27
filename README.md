# EdgeRush

A Kubernetes-native API gateway written in Rust: an operator, control planes and data
planes in one binary; Ingress and Gateway API served from the same data plane; cluster-wide
rate limiting with no external store to run.

**Status: the data plane runs; Kubernetes is arriving.** The proxy serves traffic from a
YAML file, the development harness, and in a cluster follows Services' endpoints itself;
rate limiting runs, each pod on its own or through EdgeRush's rate-limiting service; the
control plane translates Ingresses but does not yet hand its config to data planes, and the
operator is still to be built. Not for production. A hobby project with no timelines.

## What it is for

- **One data plane for Ingress and Gateway API**, side by side, from ordinary Kubernetes
  objects. Ingress stays a first-class API.
- **Operator-driven.** `ControlPlane` and `DataPlane` resources become Deployments and
  Services; one control plane can drive many data planes, each its own failure domain.
- **Rate limiting per pod or across the cluster, with no external store to run.** One mode
  for each data plane: each pod limiting on what it sees, or counts kept in memory in
  EdgeRush's own rate-limiting service, packaged with it. Limits keep working when the
  control plane is down.
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
  every upstream; a drain on SIGTERM; Prometheus metrics; access logs, a line of JSON for
  each request, WebSocket and passthrough connection, to stdout or a file reopened on
  SIGUSR1.
- **Rate limiting:** token-bucket limits on route rules, keyed by the client's address (or,
  experimentally, by a request header), a request under several decided under all of them
  at once, and dry runs that count without refusing. A refusal is a 429 with `Retry-After`
  and the `RateLimit` field. A data plane runs one mode, `off` unless it says otherwise,
  which checks its limits and enforces none:
  - `local`: each proxy process limits on what it sees, with no service to run; its
    workers share each key's bucket, one atomic word decided by compare-and-swap.
  - `synchronous`: the key's owner in the rate-limiting service
    (`edgerush-ratelimit-service --mode synchronous`, three pods or more) decides every
    request, over a stream each worker keeps to each pod. A request waits for its owner at
    most a stated time; an owner that keeps failing is cut off by a breaker and tried again
    later; and what happens while an owner is out of reach is stated too: allow, deny, or
    the data plane's own buckets until it answers. The pods copy their keys to one another
    every cycle, so that a pod's keys carry on when it leaves.
  - `asynchronous`, experimental and parked: each process decides from its own copy of
    each key and reports to the service every cycle.

Kubernetes, begun:

- **`edgerush controller`**, the control plane for one data plane: it follows
  IngressClasses, Ingresses, Services and Secrets through the API server and translates
  them each time they change. Translated so far: Ingresses' rules and default backends,
  with paths of every type, to Services' ports, spoken to as their `appProtocol` says; TLS
  from Secrets, each certificate served for the hosts that name it alone; and plain HTTP
  redirected to HTTPS for the hosts an Ingress secures. Its config does not reach data
  planes yet: it is reported on stderr, or written as a file that `edgerush proxy` reads.
- **Endpoints from EndpointSlices:** an upstream that names a Service takes its endpoints
  from the data plane's own list and watch of EndpointSlices, so that they are followed
  with the control plane down; tested end to end in kind.
- **`edgerush validate`** translates manifests as the controller would, without a cluster,
  and reports what it left out and why.

Not yet: the rest of Kubernetes (the config reaching data planes, the operator,
ingress-nginx's annotations, Ingress status, Gateway API), authentication. The request
path came first; rate limiting and the control plane are now built side by side, and the
operator follows.

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
  app: { load_balancer: p2c, endpoints: { listed: ["127.0.0.1:9000"] } }
```

```sh
cargo build --release -p edgerush
python3 -m http.server 9000 --bind 127.0.0.1 &   # a backend, if you have none
./target/release/edgerush proxy --config edgerush.yaml
curl -i http://127.0.0.1:8080/
```

For a complete local demo with backends, see [Docker Compose](examples/docker-compose/README.md).
[Examples](examples/README.md) also has runnable HTTP and gRPC configs with routing tests
and `explain` commands.
Every field of the config file is described in its JSON Schema,
[schema/config.schema.json](schema/config.schema.json), made from the code; editors use
it for completion and checks. [all-features.yaml](examples/all-features.yaml) uses every
implemented feature once.

The format asks for choices such as `forwarding` and `proxy_protocol` to be stated rather
than defaulted, as the control plane will state them. `bench/proxy.yaml` and
`fuzz/seeds/config/` have fuller examples, with TLS, HTTP/3, passthrough and more;
`edgerush proxy --help` lists the options. A worker runs on every CPU, which shares each
port between them; Windows cannot, so there add `--workers 1`.

`edgerush explain` says where a request to a listener would go and why no other match took
it, from the same file and without running anything:

```sh
./target/release/edgerush explain --config edgerush.yaml --listener web     --client 203.0.113.7 --protocol 1.1 --method GET --url http://localhost/
```

`edgerush test --config edgerush.yaml tests.yaml` checks a file of such requests and where
each is expected to go, and, given what its upstream answers, the answer its client gets;
it exits 1 when one goes elsewhere or is answered otherwise: a config's tests for CI.

`edgerush validate --from-manifests manifests/ --data-plane data-plane.yaml` translates
Kubernetes manifests (a file or a directory of them, `kubectl get -o yaml` output included,
with an IngressClass whose controller is `edgerush.dev/ingress-controller`) as the control
plane would for one data plane, compiles the config as the data plane would,
and lists each object left out and why, each path whose backend is not there, what of a
`tls` is not served as it lists it and each path another Ingress already has; it
exits 1 when there is one. TLS Secrets, seldom in a repository, are taken as they will be
in the cluster unless `--check-secrets` asks for them to be checked. The data plane's settings
stand in for its `DataPlane` resource, which comes with the operator; `backend_authorities`,
the certificates in PEM trusted for a Service port spoken to over TLS
(`appProtocol: kubernetes.io/wss`), may be added, and an `https` listener, written with
`tls: { certificates: [], without_sni: none }`, presents the Ingresses' TLS Secrets.
`ingress_https_redirect: true` redirects plain HTTP to HTTPS for the hosts an Ingress's `tls`
lists, as a `DataPlane` does by default; an Ingress's `gateway.edgerush.dev/https-redirect: "false"`
or `"true"` overrides it for its own routes. `ingress_https_redirect_max_age` is how long, in
seconds, a client may keep that redirect (30 for a `DataPlane` by default):

```yaml
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: true
ingress_https_redirect_max_age: 30
listeners:
  http: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
```

`edgerush controller --data-plane data-plane.yaml`, run in a cluster, follows the
IngressClasses, Ingresses, Services and Secrets there (or in each `--namespace` given)
through the API server and translates them for that data plane each time they change, once
all have been listed. Until the config reaches data planes, each translation is said on
stderr as `validate` says it, and `--write-config config.yaml` writes the config as a file
`edgerush proxy` reads, without the certificates it names.

## Building

The Rust toolchain is pinned in `rust-toolchain.toml`; `rustup` installs it on first use.

TLS is BoringSSL, built from source by `boring-sys`, which needs cmake and libclang
(`apt install cmake libclang-dev` on Ubuntu) and on Windows NASM as well. On Windows,
which works for development but is not a recommended path, point `LIBCLANG_PATH` at the
directory holding `libclang.dll` and `ASM_NASM` at `nasm.exe`, for instance in the `[env]`
table of `~/.cargo/config.toml`. Go is not needed. A first build of BoringSSL takes a few
minutes.

```sh
./check.sh
```

runs every check, stopping at the first that fails; all must pass before every commit.
One of them, the dependencies' advisories and licences (`deny.toml`), needs `cargo-deny`
(`cargo install --locked cargo-deny@0.20.2`, the version CI uses) and the network, to read
the RustSec advisory database. The first run of `check.sh` builds BoringSSL a second
time, for vendored quiche's own tests, which have a lock file and a target directory
(`target/vendored`) of their own. CI (`.github/workflows/ci.yml`) runs `check.sh` itself,
on Linux, for every push to `main` and every pull request.

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
compiles to being whole; `test_file` reads, checks and runs a file of `edgerush test`'s
tests against a fixed config, holding all three to not failing; `rules_compile` compiles
a limit rule's three parts and decides a request with what compiles, holding an error to
its place in the part it is about. An input that fails is saved under `fuzz/artifacts/`; fix the bug
and add the input to the unit tests. Run a target after changing the code it covers. Being
outside the workspace, the package is formatted on its own:
`cargo fmt --manifest-path fuzz/Cargo.toml`.

## Layout

```
crates/edgerush    the binary (operator, control plane and data plane will be subcommands); so far
                   `edgerush proxy --config file.yaml`, a data plane run from a file for development,
                   `edgerush controller`, a control plane for one data plane, `edgerush explain`,
                   where a request would go and why, `edgerush test`, and `edgerush validate`,
                   which translates Kubernetes manifests
crates/certificates
                   a certificate and its key read and checked to belong together
                   (`edgerush-certificates`), by BoringSSL: what the data plane presents and what
                   translation admits are read by the same code; no I/O
crates/config      the config model and its compilation (`edgerush-config`): pure, format-free
crates/control-stream
                   the config stream between control plane and data planes
                   (`edgerush-control-stream`): pure, no I/O; its messages, what a control plane
                   sends each data plane, and how a data plane holds a config until it has its
                   certificates, then runs the two whole
crates/controller  the control plane (`edgerush-controller`): the objects a data plane's config is
                   made of, followed, translated once every kind is listed and again on each
                   change, a moment's changes at once, and each config that compiles handed on
crates/discovery   a Kubernetes Service's endpoints, from its EndpointSlices (`edgerush-discovery`):
                   which addresses take requests; pure, no I/O
crates/explain     where a request would go and why (`edgerush-explain`): what `edgerush explain`
                   prints and `edgerush test` checks, worked out by the request core without a
                   data plane; no I/O
crates/filters     built-in filters (`edgerush-filters`): pure, on plain `http` types
crates/kubernetes  EdgeRush's client of the Kubernetes API (`edgerush-kubernetes`): it lists and
                   watches the kinds EdgeRush reads and does nothing else, over h2 and BoringSSL
crates/ratelimit   rate limiting (`edgerush-ratelimit`): pure, no I/O; the admission models as
                   reference implementations behind one interface, an example that runs the same
                   clients through each (`cargo run -p edgerush-ratelimit --example timelines`),
                   local mode's limiter, the synchronous mode's owner, and the asynchronous
                   mode's protocol as two state machines
crates/ratelimit-sim
                   the rate-limit simulator (`edgerush-ratelimit-sim`): ways of limiting one key
                   across pods run against the contract, deterministic, no I/O; side by side
                   with `cargo run --release -p edgerush-ratelimit-sim --example compare`
crates/ratelimit-service
                   the rate-limiting service (`edgerush-ratelimit-service`): service pods, in the
                   synchronous or the asynchronous mode, over gRPC, the asynchronous mode's client,
                   and the `edgerush-ratelimit-service` binary
crates/proxy       the data plane (`edgerush-proxy`): listeners, TLS, our HTTP/1 server and client,
                   HTTP/2 on h2, HTTP/3 on quiche, upstream pools and the request core between them,
                   and each rate-limiting mode's side of the data plane
crates/router      request matching (`edgerush-router`): pure logic, no I/O
crates/rules       limit rules (`edgerush-rules`), a spike of the DSL question: expressions over the
                   request context in CEL's syntax for the part rules need, and rules as plain
                   fields, checked when compiled and decided without allocating; pure, no I/O
crates/telemetry   metrics and access logs (`edgerush-telemetry`): counters sharded by thread, the
                   Prometheus text format, an access-log record as a line of JSON
crates/translate   translation (`edgerush-translate`): Kubernetes objects into the config one data
                   plane runs, what the control plane does on every change; pure, no I/O
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
