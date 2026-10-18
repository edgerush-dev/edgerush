#!/usr/bin/env bash
# What "atomic Redis" can do at its best, alone: a GCRA rate limit as one Lua script, the
# check and the charge in one atomic step on the server's own clock, asked of one Redis by
# redis-benchmark with keys drawn at random from KEYS of them — no gateway, no network but
# the loopback. The ceiling any Redis-backed rate limiter works under, beside EdgeRush's
# `bench/run.sh ratelimit`.
#
#   bench/redis-ceiling.sh [KEYS...]           keys drawn from each count (default
#                                              1000 100000 1000000 10000000)
#
# For each count: decisions a second and their latency asked one at a time on CLIENTS
# connections (as a gateway's workers ask, a round trip each), and pipelined PIPELINE deep
# (Redis's best case); then the bytes Redis holds a key, with every key made once under a
# limit slow enough that none expires before it is counted. Redis on the backend's CPUs,
# redis-benchmark on the generator's, as bench/run.sh places them; in Docker, host
# networking. Results in OUT.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
: "${REDIS_IMAGE:=redis:8.6.7}"
: "${BACKEND_CPUS:=3,7}"
: "${GENERATOR_CPUS:=2,6}"
: "${REQUESTS:=2000000}"
: "${CLIENTS:=64}"
: "${PIPELINE:=16}"
: "${REDIS_IO_THREADS:=1}"
: "${OUT:=$here/results/redis-ceiling-$(date +%Y%m%d-%H%M%S)}"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1000 100000 1000000 10000000)
mkdir -p "$OUT"

# GCRA: KEYS[1] the key; ARGV[1] the emission interval and ARGV[2] the tolerance, (burst −
# 1) intervals, both in microseconds. The arrival time is stored as microseconds; a key
# expires once the bucket is full again. 1 admitted, 0 refused.
gcra='local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local tolerance = tonumber(ARGV[2])
local tat = tonumber(redis.call("GET", KEYS[1])) or now
if tat < now then tat = now end
if tat - now > tolerance then return 0 end
local new = tat + interval
redis.call("SET", KEYS[1], new, "PX", math.floor((new - now) / 1000) + 1)
return 1'

docker rm -f bench-redis-ceiling >/dev/null 2>&1 || true
docker run -d --rm --name bench-redis-ceiling --network host --cpuset-cpus "$BACKEND_CPUS" \
    "$REDIS_IMAGE" redis-server --port 6380 --bind 127.0.0.1 --save "" --appendonly no \
    --io-threads "$REDIS_IO_THREADS" --maxmemory 0 >/dev/null
trap 'docker rm -f bench-redis-ceiling >/dev/null 2>&1 || true' EXIT
sleep 2
cli() { docker exec bench-redis-ceiling redis-cli -p 6380 "$@"; }
sha=$(cli script load "$gcra")
{
    echo "redis $REDIS_IMAGE io-threads $REDIS_IO_THREADS on $BACKEND_CPUS; benchmark on $GENERATOR_CPUS"
    echo "requests $REQUESTS clients $CLIENTS pipeline $PIPELINE; script $sha"
} >"$OUT/settings"

bench() { # label, keys, pipeline, interval µs, tolerance µs
    docker run --rm --network host --cpuset-cpus "$GENERATOR_CPUS" "$REDIS_IMAGE" \
        redis-benchmark -h 127.0.0.1 -p 6380 -c "$CLIENTS" -n "$REQUESTS" -r "$2" -P "$3" \
        --threads 2 --csv EVALSHA "$sha" 1 "rl:__rand_int__" "$4" "$5" >"$OUT/$1.csv" 2>&1
}

for keys in "${counts[@]}"; do
    cli flushall >/dev/null
    # A limit no request reaches: 64,000 a second, a burst of 63,000, as run.sh's.
    bench "keys-$keys.one-at-a-time" "$keys" 1 16 1007984
    bench "keys-$keys.pipelined" "$keys" "$PIPELINE" 16 1007984
    # The bytes a key: every key made once (a key per request drawn; most are made more
    # than once at these counts, so twice as many requests as keys), under one a minute
    # with a burst of ten, which keeps each for ten minutes.
    cli flushall >/dev/null
    before=$(cli info memory | awk -F: '/^used_memory:/ {print $2}' | tr -d '\r')
    REQUESTS=$((keys * 3)) bench "keys-$keys.fill" "$keys" "$PIPELINE" 60000000 540000000
    after=$(cli info memory | awk -F: '/^used_memory:/ {print $2}' | tr -d '\r')
    held=$(cli dbsize | tr -d '\r')
    echo "keys $keys held $held used_memory $((after - before)) bytes_per_key $(((after - before) / (held > 0 ? held : 1)))" \
        | tee -a "$OUT/memory"
done
echo "results in $OUT"
