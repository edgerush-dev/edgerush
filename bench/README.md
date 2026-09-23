# The macro benchmark

Load generator, proxy under test and backend on **one Linux machine**, each on CPUs of its
own, over loopback. It compares variants on the same machine in the same hour — EdgeRush
with itself (first of all its two threading models) and with NGINX, HAProxy, Envoy and
Kong set up to do the same work — and does not produce numbers that mean anything elsewhere.

```
oha / h2load  ──▶  proxy under test  ──▶  nginx (answers 200 and a few bytes)
  GEN_CPUS          PROXY_CPUS            BACKEND_CPUS
```

## What is needed

`nginx` (any flavour; it is run as the current user from `nginx.conf`, the system service
is not used; it is also one of the proxies under test), `haproxy`, `envoy` and `kong` for
those variants (none of them as a service),
`h2load` (`nghttp2-client`), [`oha`](https://github.com/hatoo/oha)
(`cargo install oha --locked`), `python3`, `curl`, `taskset`, and a release build:
`cargo build --release -p edgerush`.

## Running it

```sh
bench/run.sh prepare                # performance governor, turbo off (sudo; until reboot)
bench/run.sh ceiling [H1 H2 CHURN]  # generators against the backend, no proxy
bench/run.sh saturation             # closed loop: the most each model serves
bench/run.sh latency 25000 25000 2000   # open loop at these rates: h1, h2, churn
bench/run.sh carrying               # streamed bodies, a slow upstream, cancellation,
                                    # reload under load, and what idle connections cost
bench/run.sh hotpaths [RATE]        # H1 saturation and both streamed bodies (default 20/s)
bench/run.sh frontend               # the engine alone against NGINX, with no upstream
bench/run.sh idle                   # idle connections: never written to, after one
                                    # request, after one large head, at IDLE_COUNTS
bench/run.sh summary bench/results/<run>
```

Results go to `bench/results/<time>/` (not committed): the raw output of every
measurement, the proxy's CPU time around it, `environment.txt`, and the table that
`summary` prints. Settings are environment variables — `PROXY_CPUS`, `WORKERS`,
`GEN_CPUS`, `BACKEND_CPUS`, `DURATION`, `REPS`, `VARIANTS`, `IDLE_PER_DESTINATION`,
`IDLE_TOTAL`, `OUT`; the defaults are for a machine with 4 cores and 8 threads where CPUs
*n* and *n+4* are one core.

`IDLE_PER_DESTINATION` and `IDLE_TOTAL` are the bounds of
[13 §7](../../docs/13-http1-upstream.md) on how many idle upstream connections a worker
keeps — 8 and 256. Whichever client carries a request is held to them, so comparing the
two is comparing them at the same bounds; every run writes what they were into
`environment.txt`, because a gain that came of loosening a bound is not a gain.

## The variants

`own-server` puts EdgeRush's own downstream server in front of its own client
(`--downstream ours`, [14 §9](../../docs/14-downstream-server.md)); `ours` and
`ours-kernel` are served by the engine's server. Every EdgeRush variant reaches its
upstreams by EdgeRush's own client, the only one there is.

For the client optimisation rerun, use the original machine and fixed frequency with
`WORKERS=2 REPS=3 VARIANTS="ours nginx" IDLE_PER_DESTINATION=1024
IDLE_TOTAL=1024 STREAMED=8388608 bench/run.sh hotpaths`. Preserve the original CPU
affinity settings and verify the backend/generator ceiling first. This command keeps the
anchors interleaved and runs only H1 saturation, large responses and large uploads.
Idle memory now sums RSS over the proxy process tree, including nginx workers. Shared
pages may be counted more than once; this is not unique physical memory.

The focused parser instruction benchmark is
`cargo bench -p edgerush-proxy --features fuzzing --bench h1_codec` (Linux/Callgrind).
It covers 8, 16, 17 and 128 fields, plus a head arriving one byte at a time.

To locate the remaining upload CPU cost, build with symbols and profile only that
scenario on the benchmark machine (retain its CPU affinity and frequency settings):

```sh
cargo build --profile profiling -p edgerush
sudo -v
WORKERS=2 REPS=3 VARIANTS="ours own-server" \
IDLE_PER_DESTINATION=1024 IDLE_TOTAL=1024 STREAMED=8388608 \
bench/run.sh profile-body upload 20
```

Use `profile-body answer 20` for the unexplained response gain. Each repetition captures
cycles with DWARF call stacks, including kernel work, plus separate user/kernel
instructions and cycles, task-clock, context switches and faults. `*.self.txt` gives
exclusive symbol costs and `*.stacks.txt` their callers. Inspect the raw `.stat` files
for unavailable or multiplexed events. This mode supports EdgeRush client variants only;
attaching to nginx's master would miss the worker processes.

Sampling covers the middle `DURATION - 4` seconds (minimum duration 10 seconds).
Do not divide those counters by the whole run's request count, or feed cycle profiles to
`buckets.py`, whose headings assume user instruction samples. Profiled latency and CPU
include profiler overhead; keep adoption measurements on unprofiled `hotpaths` runs.
The binary hash and tracked-source patch accompany each profile directory.

For causal attribution, compare the original code, parser-only change, upload-loop-only
change and combined candidate on the same machine with interleaved controls. Response
polling calls the upload driver on every frame, even after the request has finished, so
the upload-loop change can affect answers without changing the response-copy code.
That is a hypothesis to isolate, not an explanation established by the macro rerun.

`VARIANTS` names what is run, in turns: `ours` — EdgeRush, the default —, `ours-kernel`
— the same without balancing connections at accept —, and `own-server` — EdgeRush's own
server in front of its own client (EdgeRush, `proxy.yaml`); then `nginx`
(`nginx-proxy.conf`), `haproxy` (`haproxy.cfg`), `envoy` (`envoy.yaml`) and `kong`
(`kong.yml`, without a database). The configs ask for the same
thing — the same hosts and rules, the same header changes on
request and response, HTTP/1.1 and cleartext HTTP/2 on one port, keep-alive connections to
the backend that any client's request may use, no access log — and each proxy gets
`WORKERS` workers on `PROXY_CPUS`. Where they cannot be the same it is said at the top of
the config. A config for another proxy is a change to review like code: a comparison is
worth as much as the care that went into the other side.

## The scenarios

| Name | Load | What it is for |
|---|---|---|
| `saturation-h1` | h2load, 256 HTTP/1.1 keep-alive connections, closed loop | The most the proxy serves when load is spread evenly over many connections |
| `saturation-h2` | h2load, 4 HTTP/2 connections (h2c) with 100 streams each, closed loop | The same when load sits on a few hot connections — where a worker that owns a connection cannot be helped by the others |
| `latency-h1` | oha, 256 connections, a fixed request rate | Latency (p50, p99, p99.9) at a load below the knee |
| `latency-h2` | oha, 4 connections × 100 streams, a fixed rate | Latency on a few hot connections |
| `churn` | oha, a new connection for every request, a fixed rate | The cost of accepting, and of a connection's first request |
| `streamed-answer` | oha, a body of `STREAMED` bytes coming back | What the answer's path costs when it is carrying something rather than passing a few bytes along |
| `streamed-request` | oha, the same body going out | The same for the request's path, where the body is read from the client and framed again |
| `slow-upstream` | oha against a backend that trickles at 256 KiB/s | An exchange held open for as long as an upstream takes, and the clocks that decide it is still alive |
| `cancelled` | the same, with a 200 ms timeout on every request | Clients that go away part way through an answer: everything the exchange holds has to go with them |
| `reload` | oha at a steady rate while the config is taken over once a second | The one in [10 §1](../../docs/10-testing.md): no failed request while a config changes. EdgeRush only — the others would need their own reload, which is a different thing to measure |
| `idle-memory` | `IDLE_CONNECTIONS` connections, answered and then left quiet, on a proxy started afresh | What a connection costs while nothing is happening on it, as the proxy's own resident memory |
| `frontend` | h2load at saturation, HTTP/1.1 and HTTP/2, for the benchmark's host and for one no route is for; instructions and cycles counted over every process of the variant | What the engine costs by itself: `bare-hyper` (hyper's server answering every request, `crates/proxy/examples/bare_hyper.rs`, built with `cargo build --release -p edgerush-proxy --example bare_hyper`) against `nginx-direct` (`nginx-direct.conf`, NGINX answering itself), and EdgeRush's own answer to an unrouted host (the engine and the request core) against its forwarding (all of it) |

Every request goes through routing on a config with several hosts and rules and through a
small filter chain (request and response header changes): `proxy.yaml`.

## Rules of the method

- **Latency is read at a fixed offered load, below saturation**, with latency counted from
  when a request was due (`oha --latency-correction`): a closed loop slows down with the
  server and hides its stalls. Pick the rates from the `saturation` run — about half and
  about three quarters of the slower variant.
- **Run `ceiling` first.** What generator and backend do without a proxy in between must
  be well above what the proxy reaches, or the benchmark measures them. Given the rates of
  a `latency` run, it also measures the latency they have between themselves at those
  rates — the tail of a generator that paces its requests in bursts is not the proxy's.
- **Variants take turns** (A, B, A, B, …), `REPS` times; the table shows the median and
  the range. A difference inside the range is not a difference.
- **Numbers of different runs of `run.sh` are not compared** unless a variant they share
  came out the same in both: the same settings have differed by 15% between two hours.
- **The ceiling decides the layout.** On four cores the defaults give the proxy two of
  them; if it then comes within about a third of the ceiling, give it fewer CPUs
  (`PROXY_CPUS=0,1 WORKERS=2`) rather than trust the number.
- **The machine must stay awake**: `run.sh` holds a sleep inhibitor for as long as it runs
  (through `sudo`), since a desktop left alone suspends itself in the middle of a run.
- **The frequency is fixed** (`prepare`): with turbo on, a laptop's clock follows its
  temperature. Absolute numbers are lower for it.
- **CPUs are pinned** with `taskset`, and the generator is kept off the proxy's cores —
  hyper-threads of one core are one core's worth of cache.
- Besides throughput and latency the table has the proxy's **CPU time per request** and
  each thread's share of it, which shows imbalance that throughput alone hides.
