# EdgeRush

A Kubernetes-native API gateway written in Rust: an operator, control planes and data
planes in one binary; Ingress and Gateway API served from the same data plane; cluster-wide
rate limiting with no external store to run.

**Status: the data plane runs, and the control plane runs it in a cluster; the operator is
next.** The proxy serves traffic from a YAML file, the development harness, or from a
control plane that translates Ingresses in a cluster and streams the config to it. Rate
limiting runs, each pod on its own or through EdgeRush's rate-limiting service. Not for
production. A hobby project with no timelines.

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

### The data plane

Its config is read from a file every second, or taken from a control plane's stream; a
change takes over without dropping a request.

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
  experimentally, by a request header), and dry runs that count without refusing. A
  refusal is a 429 with `Retry-After` and the `RateLimit` field. A data plane runs one
  mode, `off` (limits checked, none enforced) unless it says otherwise:
  - `local`: each proxy process limits on what it sees, with no service to run.
  - `synchronous`: the key's owner in the rate-limiting service
    (`edgerush-ratelimit-service --mode synchronous`, three pods or more, optionally over
    mutual TLS) decides every request. A request waits for its owner at most a stated
    time, and what happens while an owner is out of reach is stated too: allow, deny, or
    the data plane's own buckets until it answers. The pods copy their keys to one
    another, so a pod's keys carry on when it leaves.
  - `asynchronous`: each process decides every request from its own copy of each key,
    waiting on no one, and shares what it admitted through the service every cycle; a
    burst on a key it has not heard of yet may overshoot a little.

### In Kubernetes

- **`edgerush controller`**, the control plane for one data plane. It follows
  IngressClasses, Ingresses, Services and Secrets and translates them each time they
  change: Ingresses' rules and default backends, paths of every type, Services' ports
  spoken to as their `appProtocol` says, TLS from Secrets, and plain HTTP redirected to
  HTTPS for the hosts an Ingress secures. It streams each config to its data planes over
  gRPC and mutual TLS, writes each Ingress's status (replicas elect one writer by a
  Lease), and serves an admission webhook that refuses an Ingress it could not translate.
- **`edgerush proxy --control-plane`**, a data plane taking its config from that stream.
  Nothing is served until the first config runs; with the control plane gone, it serves
  on, and dials again.
- **Endpoints**, followed by the data plane itself so that they keep changing with the
  control plane down:
  - from a Service's EndpointSlices, which the data plane lists and watches;
  - from a DNS name, `dns: "web.shop.svc.cluster.local:8080"`, looked up again as each
    answer's time to live runs out; an Ingress annotated
    `gateway.edgerush.dev/service-upstream: "true"` reaches its backends this way, by the
    Service's own address, as a mesh may want;
  - preferring the endpoints EndpointSlices hint for the data plane's zone, for a Service
    that asks for hints (`trafficDistribution`). Spread the data plane's pods evenly over
    the zones: a zone's backends take what that zone's data planes receive.
- **Default-deny egress:** an address discovered or resolved is never sent to if it is
  loopback, link-local, multicast, the pod's own, the API server's, or one of the gateway's
  own pods, which the control plane names (`--own-name`).
- Tested end to end in kind (`kind/stream.sh`), the control plane gone and back among it.

**Not yet:** the operator, Gateway API, ingress-nginx's annotations, authentication.

## Trying it

### A data plane from a file

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

A worker runs on every CPU, sharing each port between them; Windows cannot, so there add
`--workers 1`. `edgerush proxy --help` lists the options.

The format asks for choices such as `forwarding` and `proxy_protocol` to be stated rather
than defaulted, as the control plane states them. More to start from:

- [Docker Compose](examples/docker-compose/README.md): a complete local demo with backends.
- [Examples](examples/README.md): runnable HTTP and gRPC configs, with routing tests and
  `explain` commands.
- [all-features.yaml](examples/all-features.yaml): every implemented feature, once.
- [schema/config.schema.json](schema/config.schema.json): every field of the config,
  made from the code; editors use it for completion and checks.
- `bench/proxy.yaml` and `fuzz/seeds/config/`: fuller configs, with TLS, HTTP/3,
  passthrough and more.

### Checking a config: `explain` and `test`

`edgerush explain` says where a request to a listener would go, and why no other match took
it, from the same file and without running anything:

```sh
./target/release/edgerush explain --config edgerush.yaml --listener web --client 203.0.113.7 --protocol 1.1 --method GET --url http://localhost/
```

`edgerush test --config edgerush.yaml tests.yaml` checks a file of such requests: where
each is expected to go and, given what its upstream answers, what its client gets. It exits
1 when one goes elsewhere or is answered otherwise, so a config's tests can run in CI.

### Checking manifests: `validate`

```sh
edgerush validate --from-manifests manifests/ --data-plane data-plane.yaml
```

translates Kubernetes manifests as the control plane would for one data plane, without a
cluster, and compiles the config as the data plane would. The manifests are a file or a
directory (`kubectl get -o yaml` output included), with an IngressClass whose controller is
`edgerush.dev/ingress-controller`. It reports, and exits 1 for:

- each object left out, and why;
- each path whose backend is not there;
- what of an Ingress's `tls` is not served as it lists it;
- each path another Ingress already has.

TLS Secrets, seldom in a repository, are taken as they will be in the cluster, unless
`--check-secrets` asks for them to be checked.

`data-plane.yaml` holds the data plane's settings, which stand in for its `DataPlane`
resource until the operator brings one:

```yaml
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: true
ingress_https_redirect_max_age: 30
listeners:
  http: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
```

| Setting | What it is |
|---|---|
| `ingress_class` | The IngressClass whose Ingresses this data plane serves. |
| `listeners` | As in a config. An Ingress is served on every `http` listener, and the hosts its `tls` lists on every `https` one; written with `tls: { certificates: [], without_sni: none }`, an `https` listener presents the Ingresses' TLS Secrets. |
| `cluster_domain` | The cluster's DNS domain, which Services' names end in. |
| `ingress_hosts_fall_through` | Whether an Ingress host stays a candidate on hosts a more specific one also covers; `false` is ingress-nginx's way. |
| `ingress_https_redirect` | Whether plain HTTP to a host an Ingress's `tls` lists is redirected to HTTPS, as a `DataPlane` does by default. An Ingress's `gateway.edgerush.dev/https-redirect: "false"` or `"true"` overrides it for its own routes. |
| `ingress_https_redirect_max_age` | How long, in seconds, a client may keep that redirect (30 for a `DataPlane` by default). |
| `backend_authorities` | Optional: certificates in PEM trusted for a Service port spoken to over TLS (`appProtocol: kubernetes.io/wss`). |
| `data_plane` | Optional: the data plane's own settings, as a config states them, written into every config. `own_service: edgerush.edge.svc.cluster.local` names the Service of the data plane's own pods, whose EndpointSlices say its zone. |

### In a cluster: `controller`

```sh
edgerush controller --data-plane data-plane.yaml
```

run in a cluster, follows the IngressClasses, Ingresses, Services and Secrets there and
translates them for that data plane each time they change, once all have been listed. Each
translation is reported on stderr as `validate` reports it. The rest is chosen by flags:

| Flags | What they do |
|---|---|
| `--namespace shop`, once for each | Follow these namespaces alone, not the whole cluster. |
| `--write-config config.yaml` | Write each config as a file `edgerush proxy --config` reads, without the certificates it names. |
| `--listen [::]:9443 --certificate tls.crt --key tls.key --authorities ca.crt` | Serve the config stream, gRPC over mutual TLS: each config goes to every data plane that dials with a certificate those authorities signed, and what each says of it is reported. |
| `--own-name edgerush-peers.edge.svc.cluster.local`, once for each | A headless Service whose pods are the gateway's own: the addresses it resolves to go on the stream, and no data plane sends a request to them. |
| `--status-service edge/edgerush`, or `--status-address 192.0.2.10` once for each | What each Ingress served says in its status: that Service's addresses, or these. Each Ingress of the class not served says none. |
| `--leader-lease edge/edgerush-control` | Replicas elect the one that writes status by this Lease. |
| `--webhook-listen [::]:8443 --webhook-certificate tls.crt --webhook-key tls.key` | Serve the admission webhook, HTTPS, reviews `POST`ed to `/validate`: an Ingress of the class that would not translate alone is refused, with why. |

A data plane dials the stream with
`edgerush proxy --control-plane edgerush-control.edge.svc:9443 --certificate tls.crt --key tls.key --authorities ca.crt`.
`kind/stream.sh` sets the whole thing up in a kind cluster, and its manifests
(`kind/stream/manifests.yaml`) are a working example.

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

Most targets compare the real code with the slow reference implementations in the router's
and the filters' `reference` modules, the same ones the property tests use. The others:

| Target | What it holds to |
|---|---|
| `request_host` | Agrees with the `http` crate's reading of a host. |
| `quic_header` | Agrees with quiche's parser. |
| `config` | Reading and compiling a config file never fail, and what compiles is whole. |
| `test_file` | Reading, checking and running a file of `edgerush test`'s tests never fail. |
| `rules_compile` | A limit rule's three parts compile or fail with an error placed in the part it is about, and what compiles decides a request without failing. |
| `admission_review` | Any bytes read as a webhook review never fail, and the answer carries the review's uid. |
| `dns_answer` | Any bytes read as a nameserver's answer never fail, and each address found is listed once, in order. |

An input that fails is saved under `fuzz/artifacts/`: fix the bug and add the input to the
unit tests. Run a target after changing the code it covers. Being
outside the workspace, the package is formatted on its own:
`cargo fmt --manifest-path fuzz/Cargo.toml`.

## Layout

```
crates/edgerush    the binary, one subcommand for each part (the operator's to come):
                   `edgerush proxy`, a data plane, its config from a file for development
                   (`--config file.yaml`) or from a control plane (`--control-plane`),
                   `edgerush controller`, a control plane for one data plane, `edgerush explain`,
                   where a request would go and why, `edgerush test`, and `edgerush validate`,
                   which translates Kubernetes manifests
crates/certificates
                   a certificate and its key read and checked to belong together
                   (`edgerush-certificates`), by BoringSSL: what the data plane presents and what
                   translation admits are read by the same code; no I/O
crates/config      the config model and its compilation (`edgerush-config`): pure, format-free
crates/control-service
                   the config stream over gRPC and mutual TLS (`edgerush-control-service`):
                   the control plane's server and the data plane's client, TLS 1.3 alone and
                   each end checked against the stream's own authorities
crates/control-stream
                   the config stream between control plane and data planes
                   (`edgerush-control-stream`): pure, no I/O; its messages, what a control plane
                   sends each data plane, and how a data plane holds a config until it has its
                   certificates, then runs the two whole
crates/controller  the control plane (`edgerush-controller`): the objects a data plane's config is
                   made of, followed, translated once every kind is listed and again on each
                   change, a moment's changes at once, and each config that compiles handed on
crates/crdgen      Rust types written from a CRD's schema (`edgerush-crdgen`), adapted from
                   kopium (its NOTICE): an enumerated string kept a `String`, a missing field
                   read empty, so that no object of a newer release fails its list; no I/O
crates/discovery   a Kubernetes Service's endpoints, from its EndpointSlices (`edgerush-discovery`):
                   which addresses take requests, and which are hinted for a zone; pure, no I/O
crates/dns         DNS's wire (`edgerush-dns`): A and AAAA queries written and a nameserver's
                   answers read, for upstreams that resolve a name; pure, no I/O
crates/explain     where a request would go and why (`edgerush-explain`): what `edgerush explain`
                   prints and `edgerush test` checks, worked out by the request core without a
                   data plane; no I/O
crates/filters     built-in filters (`edgerush-filters`): pure, on plain `http` types
crates/gateway-api the Gateway API kinds EdgeRush follows (`edgerush-gateway-api`), their types
                   written by `edgerush-crdgen` from the CRDs in its `crds/`, fetched from a
                   release by its `fetch.sh`; a test fails when the two differ; no I/O
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
own MIT licence ([vendor/h2/LICENSE](vendor/h2/LICENSE)). `crates/crdgen` is adapted from
kopium, under the same Apache-2.0 ([crates/crdgen/NOTICE](crates/crdgen/NOTICE)), and
`crates/gateway-api/crds` holds the Gateway API's CRDs as its releases publish them, under
the same Apache-2.0, copyright the Kubernetes Authors.
