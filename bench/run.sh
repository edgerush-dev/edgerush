#!/usr/bin/env bash
# The macro benchmark of the data plane: load generator, proxy and backend on one Linux
# machine, each on CPUs of its own, over loopback. See bench/README.md.
#
#   bench/run.sh prepare                      fix the CPU frequency (sudo; until reboot)
#   bench/run.sh ceiling [H1 H2 CHURN]        what generator and backend do without a proxy,
#                                             and their latency at these rates
#   bench/run.sh saturation                   closed loop: the most each variant serves
#   bench/run.sh latency H1 H2 CHURN          open loop: latency at these request rates
#   bench/run.sh summary DIR                  the table of a finished run
#
# The variants — EdgeRush, and NGINX, HAProxy, Envoy and Kong set up to do the same
# — are run in turns, REPS times, so that whatever drifts, heat above all, drifts
# for all of them.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")

: "${PROXY_CPUS:=0,1,4,5}"   # two cores with both their threads
: "${WORKERS:=4}"            # one for each of PROXY_CPUS
: "${GEN_CPUS:=2,6}"
: "${BACKEND_CPUS:=3,7}"
: "${DURATION:=30}"          # seconds of every measurement
: "${REPS:=3}"
# How many idle upstream connections a worker keeps, which 13 §7 puts at 8 per
# destination and 256 in all. Whichever client carries the request is held to them,
# so a comparison of the two is a comparison at the same bounds — and a run that
# moves them says so in environment.txt, because a gain that came of loosening a
# bound is not a gain.
: "${IDLE_PER_DESTINATION:=8}"
: "${IDLE_TOTAL:=256}"
: "${VARIANTS:=thread-per-core}" # and: ours thread-per-core-kernel nginx haproxy envoy kong
: "${OUT:=$here/results/$(date +%Y%m%d-%H%M%S)}"

# Hundreds of connections on either side of the proxy, and more when they churn.
ulimit -n 65536

edgerush=$repo/target/release/edgerush
run=/tmp/edgerush-bench
backend=http://127.0.0.1:9000/
# The proxy is asked for by the name its routes are for, and the generators are told where
# that is: a `host` header would not do, as HTTP/2 names the host in the target.
host=bench.example.com
proxy=http://$host:8080/
proxy_at=127.0.0.1:8080
proxy_pid=

start_backend() {
    mkdir -p "$run/tmp"
    taskset -c "$BACKEND_CPUS" nginx -p "$run/" -c "$here/nginx.conf" -e "$run/error.log"
    await "$backend"
}

stop_backend() {
    [ -f "$run/nginx.pid" ] && kill "$(cat "$run/nginx.pid")" 2>/dev/null || true
}

start_proxy() { # variant
    local daemon=
    # Given unquoted below, so that it is two flags and their values rather than one
    # long argument. Only the variants that are EdgeRush are given it.
    local idle="--idle-per-destination $IDLE_PER_DESTINATION --idle-total $IDLE_TOTAL"
    case "$1" in
    envoy)
        taskset -c "$PROXY_CPUS" envoy -c "$here/envoy.yaml" --concurrency "$WORKERS" \
            --disable-hot-restart -l warn 2>>"$OUT/proxy.log" &
        ;;
    kong)
        # Kong goes into the background by itself and leaves the pid of its master.
        (
            export KONG_PREFIX="$run-kong" KONG_DATABASE=off KONG_DECLARATIVE_CONFIG="$here/kong.yml"
            export KONG_PROXY_LISTEN="127.0.0.1:8080 http2 reuseport backlog=4096" KONG_ADMIN_LISTEN=off
            export KONG_NGINX_WORKER_PROCESSES="$WORKERS" KONG_PROXY_ACCESS_LOG=off KONG_HEADERS=off
            export KONG_UPSTREAM_KEEPALIVE_POOL_SIZE=1024 KONG_UPSTREAM_KEEPALIVE_MAX_REQUESTS=0
            export KONG_UPSTREAM_KEEPALIVE_IDLE_TIMEOUT=3600 KONG_ANONYMOUS_REPORTS=off
            export KONG_NGINX_HTTP_KEEPALIVE_REQUESTS=1000000000 KONG_NGINX_HTTP_KEEPALIVE_TIMEOUT=3600s
            taskset -c "$PROXY_CPUS" kong start
        ) >>"$OUT/proxy.log" 2>&1
        daemon=$(cat "$run-kong/pids/nginx.pid")
        ;;
    nginx)
        mkdir -p "$run-proxy/tmp"
        sed "s/WORKERS/$WORKERS/" "$here/nginx-proxy.conf" >"$run-proxy/nginx.conf"
        taskset -c "$PROXY_CPUS" nginx -p "$run-proxy/" -c "$run-proxy/nginx.conf" \
            -e "$run-proxy/error.log" 2>>"$OUT/proxy.log" &
        ;;
    haproxy)
        # A thread for every CPU it may run on: WORKERS of them, if PROXY_CPUS is as many.
        taskset -c "$PROXY_CPUS" haproxy -db -f "$here/haproxy.cfg" 2>>"$OUT/proxy.log" &
        ;;
    thread-per-core-kernel)
        # Connections left where the kernel put them: what balancing is measured against.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$here/proxy.yaml" \
            --accept kernel --workers "$WORKERS" $idle \
            2>>"$OUT/proxy.log" &
        ;;
    ours)
        # The same proxy by EdgeRush's own upstream path rather than the engine's
        # client, which is the candidate of 13 section 8 step 6. Nothing else about
        # it changes, which is what makes the pair of them the measurement.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$here/proxy.yaml" \
            --upstream ours --workers "$WORKERS" $idle \
            2>>"$OUT/proxy.log" &
        ;;
    *)
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$here/proxy.yaml" \
            --workers "$WORKERS" $idle 2>>"$OUT/proxy.log" &
        ;;
    esac
    proxy_pid=${daemon:-$!}
    await "http://$proxy_at/"
}

stop_proxy() {
    [ -n "$proxy_pid" ] || return 0
    kill "$proxy_pid" 2>/dev/null || true
    wait "$proxy_pid" 2>/dev/null || true
    # One that is not a child of this script cannot be waited for, only looked for.
    for _ in $(seq 100); do
        kill -0 "$proxy_pid" 2>/dev/null || break
        sleep 0.1
    done
    proxy_pid=
}

await() { # url
    for _ in $(seq 100); do
        curl -s -o /dev/null -H "host: $host" "$1" && return
        sleep 0.1
    done
    echo "nothing answers at $1" >&2
    exit 1
}

# The time, and the CPU time of the proxy's threads so far — those of its worker processes
# too, if it has any — a line each: the thread, clock ticks in user and in kernel mode. Taken before and after a measurement, the difference is
# what the measurement cost and how the workers shared it.
cpu() { # file
    [ -n "$proxy_pid" ] || return 0
    {
        echo "time $(date +%s.%N) 0"
        for task in $(for pid in $proxy_pid $(pgrep -P "$proxy_pid"); do echo /proc/$pid/task/*; done); do
            awk '{ sub(/^[^)]*\) /, ""); print FILENAME, $12, $13 }' "$task/stat" 2>/dev/null || true
        done
    } >"$1"
}

measured() { # name, command...
    local name=$1
    shift
    cpu "$OUT/$name.cpu-before"
    # The clock of the proxy's CPUs, once a second: a machine that throttles measures its
    # cooling, and the table should show it.
    taskset -c "$GEN_CPUS" bash -c 'while sleep 1; do
        cat $(printf "/sys/devices/system/cpu/cpu%s/cpufreq/scaling_cur_freq " ${0//,/ })
    done' "$PROXY_CPUS" >"$OUT/$name.freq" 2>/dev/null &
    local sampler=$!
    taskset -c "$GEN_CPUS" "$@" >"$OUT/$name.out" 2>"$OUT/$name.err" || echo "failed: $name" >&2
    kill "$sampler" 2>/dev/null || true
    cpu "$OUT/$name.cpu-after"
    echo "done: $name"
}

# Closed loop: as many requests as come back.
saturation_h1() { # name, url, options...
    local name=$1 url=$2
    shift 2
    measured "$name" h2load --h1 -c256 -m1 -t2 -D "$DURATION" --warm-up-time=3 "$@" "$url"
}

saturation_h2() { # name, url, options...: few connections, many streams on each
    local name=$1 url=$2
    shift 2
    measured "$name" h2load -c4 -m100 -t2 -D "$DURATION" --warm-up-time=3 "$@" "$url"
}

# Open loop: a fixed rate, and latency counted from when a request was due. Requests that
# are under way when the time is up are waited for, not counted as failures.
oha_at() { # name, rate, url, options...
    local name=$1 rate=$2 url=$3
    shift 3
    measured "$name" oha -z "${DURATION}s" -w -q "$rate" --latency-correction --no-tui \
        --output-format json --connect-to "$host:8080:$proxy_at" "$@" "$url"
}

latency_h1() { oha_at "$1" "$2" "$3" -c 256; }
latency_h2() { oha_at "$1" "$2" "$3" --http2 -c 4 -p 100; }
churn() { oha_at "$1" "$2" "$3" -c 64 --disable-keepalive; }

each_variant() { # function, that is given: prefix of the names
    for rep in $(seq "$REPS"); do
        for variant in $VARIANTS; do
            start_proxy "$variant"
            "$1" "$variant.$rep"
            stop_proxy
            sleep 5 # let it cool, and the sockets of the run go
        done
    done
}

saturation_runs() {
    saturation_h1 "$1.saturation-h1" "$proxy" --connect-to="$proxy_at"
    saturation_h2 "$1.saturation-h2" "$proxy" --connect-to="$proxy_at"
}

latency_runs() {
    latency_h1 "$1.latency-h1" "$h1_rate" "$proxy"
    latency_h2 "$1.latency-h2" "$h2_rate" "$proxy"
    churn "$1.churn" "$churn_rate" "$proxy"
}

environment() {
    {
        date -Is
        uname -r
        git -C "$repo" log --oneline -1
        lscpu | grep -E 'Model name|MHz'
        echo "governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)," \
            "no_turbo $(cat /sys/devices/system/cpu/intel_pstate/no_turbo)"
        echo "proxy on $PROXY_CPUS ($WORKERS workers), generator on $GEN_CPUS," \
            "backend on $BACKEND_CPUS, ${DURATION}s, $REPS repetitions"
        echo "idle upstream connections: $IDLE_PER_DESTINATION per destination," \
            "$IDLE_TOTAL in all"
        h2load --version
        oha --version
        nginx -v 2>&1
        { command -v haproxy >/dev/null && haproxy -v | head -1; } || true
        { command -v envoy >/dev/null && envoy --version | grep -o "version: [^/]*/[0-9.]*"; } || true
        { command -v kong >/dev/null && kong version; } || true
    } >"$OUT/environment.txt"
}

command=${1:-}
case "$command" in
prepare)
    echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo >/dev/null
    echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor >/dev/null
    grep . /sys/devices/system/cpu/intel_pstate/no_turbo \
        /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    exit
    ;;
summary)
    exec python3 "$here/summary.py" "$2"
    ;;
ceiling | saturation | latency) ;;
*)
    sed -n '2,14p' "$0" >&2
    exit 2
    ;;
esac

# A machine that goes to sleep for want of a keyboard takes the measurement with it: sleep
# is held off until this script is gone.
if command -v systemd-inhibit >/dev/null; then
    sudo -n systemd-inhibit --what=sleep:idle --why="edgerush benchmark"         tail --pid=$$ -f /dev/null &
fi

mkdir -p "$OUT"
trap 'stop_proxy; stop_backend' EXIT
environment
start_backend
case "$command" in
ceiling)
    saturation_h1 backend.saturation-h1 "$backend"
    saturation_h2 backend.saturation-h2 "$backend"
    # Without a rate oha sends as fast as it can: its own ceiling.
    measured backend.oha-h1 oha -z "${DURATION}s" -c 256 --no-tui --output-format json "$backend"
    measured backend.oha-h2 oha -z "${DURATION}s" --http2 -c 4 -p 100 --no-tui \
        --output-format json "$backend"
    # The latency that is the generator's and the backend's own: what is left of the
    # proxy's when this is taken away is the proxy's.
    if [ $# -ge 4 ]; then
        latency_h1 backend.latency-h1 "$2" "$backend"
        latency_h2 backend.latency-h2 "$3" "$backend"
        churn backend.churn "$4" "$backend"
    fi
    ;;
saturation)
    each_variant saturation_runs
    ;;
latency)
    h1_rate=${2:?rate for h1} h2_rate=${3:?rate for h2} churn_rate=${4:?rate for churn}
    each_variant latency_runs
    ;;
esac
python3 "$here/summary.py" "$OUT"
