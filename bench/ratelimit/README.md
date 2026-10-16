# Rate limiters compared

Plays the rate-limiting simulator's workloads and faults (`crates/ratelimit-sim`) against
other gateways' cluster-wide rate limiting, each as it ships, in Docker Compose, so that the
simulator's numbers have real systems beside them.

| System | How instances share | Contract configured |
|---|---|---|
| `envoy` | Envoy's global rate limit: every request asks envoyproxy/ratelimit, which counts in Redis | 100 a second, fixed window |
| `gubernator` | `GLOBAL`: answers locally, hits batched to the key's owner every 100 ms (asked directly; no gateway) | leaky bucket, 100 a second, burst 100 |
| `apisix` | `limit-count`, Redis on every request | 100 in a 1 s fixed window |
| `apisix-delayed` | the same, counting locally and syncing every 100 ms (`sync_interval`) | the same |
| `kong` | `rate-limiting`, Redis policy | `second: 100`, fixed window |
| `traefik` | `rateLimit` with Redis | average 100, period 1 s, burst 100 |
| `tyk` | its distributed rate limiter: the limit divided among the gateways it knows of | rate 100 per 1 s |
| `haproxy` | stick tables copied between instances by `peers` (not added up) | `http_req_rate(1s)` over 100 refused |
| `envoy-wait` | as `envoy`, with a 1 s timeout on the ratelimit service rather than 20 ms | the same |
| `envoy-local` | Envoy's own token bucket, the limit divided among its instances (`local_cluster_rate_limit`) | 100 a second, bucket of 100 |
| `edgerush` | EdgeRush's rate-limiting service prototype: each worker decides from its copies, three service pods own the keys | token bucket, 100 a second, burst 100, by the client's address |

`edgerush` runs from binaries built on the host (`cargo build --release -p edgerush -p
edgerush-ratelimit-service`, copied to `$RLH_BIN`, `/home/claude/rlh-bin` by default) in an
image with the host's glibc; its limit is keyed by the client's address, the only key it
takes before authentication, so every run's load, from the host, is one key; faults on its
owner strike the service pod that owns that key, found as the protocol finds it.

Every instance gets one CPU. Clients reach instances on a front network; instances reach
Redis, the ratelimit service or each other on a back network, which is what faults touch.

## Running

Linux with Docker, Python 3 and passwordless `sudo` (faults use `tc` and `iptables` inside
the containers' network namespaces):

```sh
python3 harness.py smoke kong 2                  # up, then 150 at once on each instance
python3 harness.py sweep out.jsonl --sizes 2,4,8 --faults none,loss,owner-lost,owner-restart,restart,rolling,partition
python3 report.py out.jsonl
```

A sweep skips runs already in its output, so it can be stopped and started again. Faults,
as the simulator's, strike half way through a ten-second run: the owner lost or restarted
(the store where there is one; Gubernator's owning instance; otherwise the first
instance), the last instance restarted, every instance restarted in turn from 3 s, the
second half of the instances cut off from the first half and the store from 3 s to 7 s,
and 10% packet loss on the back network. An instance stopped has its traffic steered to
the next a quarter of a second before.

## Reading the numbers

For each size N, the same traffic is also played against one instance of the same system:
the headline ratio is N instances against that one, which is what sharing costs; the
second is against one token bucket holding 100 a second with a burst of 100, as the
simulator measures, which also charges a fixed window for its boundary. Conforming traffic
is filtered through that bucket, as the simulator's is, so a fixed window refuses some of
it even alone; both shares are shown.

`python3 -m unittest` runs the tests of the workloads and the report.
