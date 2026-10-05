# Examples

Start with [Docker Compose](docker-compose/README.md) for a complete local demo with
two HTTP backends. The configs below use the current file-driven data plane; Kubernetes
and rate limiting are not implemented yet.

| Example | Shows |
|---|---|
| [All features](all-features.yaml) | Every implemented feature once: four listeners (HTTP, TCP and TLS passthrough), weighted and mirrored routes, every filter, redirects, gRPC, WebSockets, timeouts, retries, health checks, with [routing checks](all-features-tests.yaml) |
| [Docker Compose](docker-compose/README.md) | Two NGINX backends, load balancing, health checks, rewrites, redirects, forwarding headers, access logs, metrics and config reloads |
| [Development config](dev-harness.yaml) | An API prefix rewritten to one upstream, with other requests sent to another; JSON access logs |
| [gRPC](grpc.yaml) | Service and method matching, HTTP/2 upstreams, health checks, retries on UNAVAILABLE and a canary mirror |

Run commands from the repository root. See [building](../README.md#building) for the
compiler and native dependencies. These examples are for local development.

## Routing tests and explanations

These commands need no running backends and open no listeners:

```sh
cargo run -p edgerush -- test --config examples/all-features.yaml examples/all-features-tests.yaml
cargo run -p edgerush -- test --config examples/dev-harness.yaml examples/dev-harness-tests.yaml
cargo run -p edgerush -- test --config examples/grpc.yaml examples/grpc-tests.yaml
cargo run -p edgerush -- test --config examples/docker-compose/config/edgerush.yaml examples/docker-compose/tests.yaml
cargo run -p edgerush -- explain --config examples/dev-harness.yaml --listener http --client 203.0.113.7 --protocol 1.1 --method GET --url http://localhost:8080/api/users
```

`cargo test -p edgerush --test examples` runs the same test files, as does the repository's
normal test suite. They check routing and headers; they do not exercise live health
checks, retry timing, random mirror delivery or network failures.

## The config file's schema

Every field of the config file, its choices, defaults and limits, is described in the
JSON Schema [schema/config.schema.json](../schema/config.schema.json), made from the
code. An editor with YAML support (VS Code's YAML extension, for one) completes fields,
checks values and shows each field's description from it; the examples here point at it
with their first line:

```yaml
# yaml-language-server: $schema=../schema/config.schema.json
```

Rules that involve more than one field, such as a health check's timeout being no longer
than its interval, are in the descriptions and checked when the config is loaded, with an
error that names the field.

## All features

[all-features.yaml](all-features.yaml) uses every implemented feature once, with notes on
what each part demonstrates:

| Listener | Address | Example traffic |
|---|---|---|
| `web` | `127.0.0.1:8080` | Weighted API backends, header/query conditions, rewrites, mirrors, redirects, gRPC, WebSocket and hostname-specific assets |
| `internal` | `127.0.0.1:8081` | Exact `/status` route on `localhost`, preserving the caller's request ID |
| `raw` | `127.0.0.1:10000` | TCP passthrough with an outgoing PROXY v2 header |
| `sni` | `127.0.0.1:10443` | TLS passthrough for `*.example.com`, including multiple subdomain labels |

Its routing tests run without network services. To serve traffic, supply HTTP backends on
ports 9000–9002, a gRPC backend with health service on 50051, a TCP backend accepting
PROXY v2 on 10001, and a TLS backend on 9443. A WebSocket request needs a
WebSocket-capable HTTP backend. Then run:

```sh
cargo run -p edgerush -- proxy --config examples/all-features.yaml --workers 1
```

The commented `secure` listener shows HTTPS and HTTP/3, certificate selection and
optional client validation. To try it locally, generate a short-lived development
certificate with OpenSSL (from the repository root):

```sh
mkdir -p examples/certs
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -keyout examples/certs/server-key.pem -out examples/certs/server-chain.pem
```

Enable the `secure` entry, keeping `client_validation` commented unless you supply your
own client CA. Replace the empty `certificates` map with the commented `server` entry,
written as `certs/server-chain.pem` relative to the YAML file, and add `secure` to the
`application` and `assets` routes' listeners. Restart the proxy to bind the added
listener. Use a client that trusts this certificate; HTTP/3 also needs an HTTP/3-capable
client and UDP on port 8443. For mTLS or upstream TLS, replace the commented CA
placeholders with real PEM contents and supply the named client certificate where used.
Generated files in `examples/certs/` are ignored by Git.

## Serve HTTP locally

Start HTTP backends on `127.0.0.1:9000`, `:9001` and `:9002`, then:

```sh
cargo run -p edgerush -- proxy --config examples/dev-harness.yaml --workers 1
```

In another terminal, request `http://127.0.0.1:8080/api/users` to reach an API backend
as `/users`, or `http://127.0.0.1:8080/` to reach the web backend. The response comes from
your backend. Each completed request writes a JSON access record to stdout. Config edits
are read again every second.

## Serve gRPC locally

Supply `helloworld.Greeter` backends on `127.0.0.1:50051` and `:50052`, plus a canary on
`:50061`. The main backends must implement `grpc.health.v1.Health/Check` and report
`SERVING` for `helloworld.Greeter`. Backend implementations and protobuf files are not
included.

```sh
cargo run -p edgerush -- proxy --config examples/grpc.yaml --workers 1
```

Point a gRPC client at `127.0.0.1:8080` using plaintext HTTP/2 (h2c).
`helloworld.Greeter/SayHello` has a 10% canary mirror; other methods of that service go
only to the main upstream. The HTTP and gRPC examples both use port 8080, so run one at
a time. One worker works on Windows too; Linux can use multiple workers.

## Future Kubernetes designs

[kubernetes/](kubernetes/README.md) preserves illustrative manifests for features that
are not built. They are not installation instructions or runnable examples.
