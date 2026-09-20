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
bench/run.sh summary bench/results/<run>
```

Results go to `bench/results/<time>/` (not committed): the raw output of every
measurement, the proxy's CPU time around it, `environment.txt`, and the table that
`summary` prints. Settings are environment variables — `PROXY_CPUS`, `WORKERS`,
`GEN_CPUS`, `BACKEND_CPUS`, `DURATION`, `REPS`, `VARIANTS`, `OUT`; the defaults are for a
machine with 4 cores and 8 threads where CPUs *n* and *n+4* are one core.

## The variants

`VARIANTS` names what is run, in turns: `work-stealing` and `thread-per-core` (EdgeRush,
`proxy.yaml`), `nginx` (`nginx-proxy.conf`), `haproxy` (`haproxy.cfg`), `envoy`
(`envoy.yaml`) and `kong` (`kong.yml`, without a database). The configs ask for the same
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
