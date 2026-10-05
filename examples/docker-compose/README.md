# EdgeRush in Docker Compose

**Runs today.** EdgeRush in front of two NGINX backends, each of which answers with its
own name and the request as it reached it, so what the proxy did can be seen.

```sh
cd examples/docker-compose
docker compose up --build -d
curl -i http://127.0.0.1:8080/
```

Run the first command from the repository root; run the remaining commands in this
folder. Docker with Compose and a Linux container engine is required.

The build context is the repository root (`../..`), including uncommitted changes.
EdgeRush is compiled in the release profile with two build jobs by default; the first
build takes several minutes and several GB of memory. Later builds reuse the dependency
cache. To change parallelism, use `docker compose build --build-arg JOBS=4` before
`docker compose up -d`.

| Port | What |
|---|---|
| `127.0.0.1:8080` | the proxy: HTTP/1.1, and HTTP/2 by prior knowledge |
| `127.0.0.1:9090/metrics` | Prometheus metrics |

Both ports are published on IPv4 loopback only, so use `127.0.0.1`. The config is
`config/edgerush.yaml`. This demo reserves `172.30.89.0/24`; if it overlaps a local
network, change the subnet and service addresses in `compose.yaml` and the upstream
endpoints in the config together.

## Things to try

- **Load balancing.** Reload `http://127.0.0.1:8080/` in a browser: `served by`
  alternates between `web-1` and `web-2`. It is round robin, per request and per worker:
  the browser keeps one connection, so one worker, and the backends answer
  `/favicon.ico` with a 404 so that it is not asked for on every reload, taking every
  other turn. Separate `curl` runs each open a connection that either worker may take,
  so there it is about half each, not strictly alternate.
- **Forwarding headers.** The backend sees `x-forwarded-for`, `-proto` and `-host`, `via:
  1.1 edgerush` and the `x-request-id` EdgeRush made (also in the answer). The client
  address depends on how your container engine forwards the published port; Docker
  Desktop may show the Docker network's gateway (`172.30.89.1`).
- **Forwarding headers are not believed from strangers.**
  `curl -H "X-Forwarded-For: 1.2.3.4" http://127.0.0.1:8080/`: no proxy is trusted, so the
  claimed address is dropped and the real one sent instead.
- **A rewrite.** `curl http://127.0.0.1:8080/api/users`: the backend sees `GET /users`.
- **A redirect.** `curl -i "http://127.0.0.1:8080/old/page?x=1"`: `301` to
  `/new/page?x=1`.
- **A response header.** `curl -i http://127.0.0.1:8080/`: `x-served-by: edgerush`.
- **HTTP/2.** With an HTTP/2-capable curl, use
  `curl --http2-prior-knowledge http://127.0.0.1:8080/`. If your curl lacks HTTP/2, use
  `docker run --rm --network edgerush-demo_demo curlimages/curl -si --http2-prior-knowledge http://edgerush:8080/`.
- **Access logs.** `docker compose logs -f edgerush`: a line of JSON per request, with its
  route, rule, endpoint, tries and timings.
- **Metrics.** `curl -s http://127.0.0.1:9090/metrics | grep edgerush_upstream`
- **A backend goes away.** `docker compose stop web-2`. A request already on its way to
  `web-2` can fail, as this config asks for no retries. Within a few seconds the health check takes `web-2` out
  (`edgerush_upstream_healthy_endpoints` 1, `edgerush_upstream_set_aside_endpoints` 1)
  and every request goes to `web-1`. `docker compose start web-2` brings it back.
- **A live config change.** Edit `config/edgerush.yaml` (the `x-served-by` value, say, or
  `load_balancer: p2c`) and save: within a second the log says `config reloaded` and the
  change is in force, no request dropped. A config that cannot be run is logged as
  `config rejected, the one before it runs on:` with the reasons, and changes nothing.

```sh
docker compose down
```

stops and removes it all; the image `edgerush-demo:latest` stays.

## Check the config without Docker

From the repository root:

```sh
cargo run -p edgerush -- test --config examples/docker-compose/config/edgerush.yaml examples/docker-compose/tests.yaml
```

This checks redirects, rewriting and headers without making network requests. It is
also part of `cargo test -p edgerush --test examples`.
