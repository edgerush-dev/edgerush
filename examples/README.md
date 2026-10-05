# Examples

Start with [Docker Compose](docker-compose/README.md) for a complete local demo with
two HTTP backends. The configs below use the current file-driven data plane; Kubernetes
and rate limiting are not implemented yet.

| Example | Shows |
|---|---|
| [Docker Compose](docker-compose/README.md) | Two NGINX backends, load balancing, health checks, rewrites, redirects, forwarding headers, access logs, metrics and config reloads |
| [Development config](dev-harness.yaml) | An API prefix rewritten to one upstream, with other requests sent to another; JSON access logs |
| [gRPC](grpc.yaml) | Service and method matching, HTTP/2 upstreams, health checks, retries on UNAVAILABLE and a canary mirror |

Run commands from the repository root. See [building](../README.md#building) for the
compiler and native dependencies. These examples are for local development.

## Routing tests and explanations

These commands need no running backends and open no listeners:

```sh
cargo run -p edgerush -- test --config examples/dev-harness.yaml examples/dev-harness-tests.yaml
cargo run -p edgerush -- test --config examples/grpc.yaml examples/grpc-tests.yaml
cargo run -p edgerush -- test --config examples/docker-compose/config/edgerush.yaml examples/docker-compose/tests.yaml
cargo run -p edgerush -- explain --config examples/dev-harness.yaml --listener http --client 203.0.113.7 --protocol 1.1 --method GET --url http://localhost:8080/api/users
```

`cargo test -p edgerush --test examples` runs the same test files, as does the repository's
normal test suite. They check routing and headers; they do not exercise live health
checks, retry timing, random mirror delivery or network failures.

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
