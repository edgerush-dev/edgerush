#!/usr/bin/env bash
# The macro benchmark of the data plane: load generator, proxy and backend on one Linux
# machine, each on CPUs of its own, over loopback. See bench/README.md.
#
#   bench/run.sh prepare                      fix the CPU frequency (sudo; until reboot)
#   bench/run.sh ceiling [H1 H2 CHURN]        what generator and backend do without a proxy,
#                                             and their latency at these rates
#   bench/run.sh saturation                   closed loop: the most each variant serves
#   bench/run.sh latency H1 H2 CHURN          open loop: latency at these request rates
#   bench/run.sh carrying [STREAMED SLOW]     streamed bodies, a slow upstream,
#                                             cancellation, reload under load, idle memory
#   bench/run.sh instructions                 where a worker's instructions go, by part
#   bench/run.sh profile-body upload|answer [RATE]  CPU stacks during streamed bodies
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
# A body big enough that neither end holds it whole, for the streamed scenarios,
# and how many connections are left idle for the one that weighs them.
: "${STREAMED:=8388608}"
: "${IDLE_CONNECTIONS:=2000}"
: "${VARIANTS:=thread-per-core}" # and: ours thread-per-core-kernel nginx haproxy envoy kong
: "${OUT:=$here/results/$(date +%Y%m%d-%H%M%S)}"

# Hundreds of connections on either side of the proxy, and more when they churn.
ulimit -n 65536

# The binary under test. `instructions` points this at a build with symbols in it.
edgerush=${EDGERUSH:-$repo/target/release/edgerush}
run=/tmp/edgerush-bench
# The proxy is given a copy rather than the file in the repository: one scenario
# rewrites it while the load is on, and the repository is not the place for that.
config=$run-config.yaml
backend=http://127.0.0.1:9000/
# The proxy is asked for by the name its routes are for, and the generators are told where
# that is: a `host` header would not do, as HTTP/2 names the host in the target.
host=bench.example.com
proxy=http://$host:8080/
proxy_at=127.0.0.1:8080
proxy_pid=

start_backend() {
    mkdir -p "$run/tmp"
    # What the streamed scenarios ask for, and what the trickling one trickles.
    [ -s "$run/big.bin" ] || head -c "$STREAMED" /dev/zero >"$run/big.bin"
    # What the trickling one trickles: small enough that a request finishes in a few
    # seconds at the rate that location is held to.
    [ -s "$run/slow.bin" ] || head -c $((STREAMED / 8)) /dev/zero >"$run/slow.bin"
    cp "$here/proxy.yaml" "$config"
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
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
            --accept kernel --workers "$WORKERS" $idle \
            2>>"$OUT/proxy.log" &
        ;;
    ours)
        # The same proxy by EdgeRush's own upstream path rather than the engine's
        # client, which is the candidate of 13 section 8 step 6. Nothing else about
        # it changes, which is what makes the pair of them the measurement.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
            --upstream ours --workers "$WORKERS" $idle \
            2>>"$OUT/proxy.log" &
        ;;
    *)
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
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
    local status=0
    taskset -c "$GEN_CPUS" "$@" >"$OUT/$name.out" 2>"$OUT/$name.err" || status=$?
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
    cpu "$OUT/$name.cpu-after"
    if [ "$status" -ne 0 ]; then
        echo "failed: $name" >&2
        return "$status"
    fi
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

# A body neither end holds whole, in each direction: what the paths cost when they
# are carrying something rather than passing a few bytes along.
streamed_answer() { oha_at "$1" "$2" "$proxy/big" -c 32; }
streamed_request() { oha_at "$1" "$2" "$proxy/sink" -c 32 -m POST -D "$run/big.bin"; }

# Sample the middle of a sustained body workload. Startup and draining requests are
# outside the perf window; CPU/request still comes from the ordinary measured snapshots,
# not from dividing these shorter-window counters by the whole run's request count.
profile_body_runs() (
    local name="$1.streamed-$body_direction" loader= counter= status=0
    # All background work is reaped, including when perf cannot access an event.
    trap 'for job in "$loader" "$counter"; do
        [ -z "$job" ] || kill "$job" 2>/dev/null || true
    done
    wait 2>/dev/null || true' EXIT
    if [ "$body_direction" = upload ]; then
        streamed_request "$name" "$streamed_rate" &
    else
        streamed_answer "$name" "$streamed_rate" &
    fi
    loader=$!
    sleep 2
    sudo -n perf stat -x, -p "$proxy_pid" \
        -e task-clock,cycles:u,cycles:k,instructions:u,instructions:k,context-switches,cpu-migrations,page-faults \
        -o "$OUT/$name.stat" -- sleep "$((DURATION - 4))" &
    counter=$!
    sudo -n perf record -q -e cycles -F 499 --call-graph dwarf,16384 \
        -p "$proxy_pid" -o "$OUT/$name.perf.data" -- sleep "$((DURATION - 4))" || status=$?
    wait "$counter" || status=$?
    counter=
    wait "$loader" || status=$?
    loader=
    [ "$status" -eq 0 ] || return "$status"
    sudo -n chown "$(id -u):$(id -g)" "$OUT/$name.perf.data" "$OUT/$name.stat"
    perf report -i "$OUT/$name.perf.data" --stdio --no-children \
        -s dso,symbol --percent-limit 0 >"$OUT/$name.self.txt" || return $?
    perf report -i "$OUT/$name.perf.data" --stdio --children \
        -g graph,0.5,caller >"$OUT/$name.stacks.txt" || return $?
    printf '%s\n' "cycles sampled across user and kernel; window $((DURATION - 4))s" \
        "Profiling overhead affects these runs; use hotpaths for adoption numbers." \
        >"$OUT/$name.profile-notes.txt"
)

# An upstream that trickles: the answer is read over as long as it takes, and the
# exchange is held open the whole time.
slow_upstream() { oha_at "$1" "$2" "$proxy/slow" -c 32; }

# Clients that give up part way through an answer, which is where an exchange has to
# let go of everything it is holding rather than wait to be asked again.
cancelled() { oha_at "$1" "$2" "$proxy/slow" -c 32 -t 200ms; }

# A config taken over again and again while the load is on. Only EdgeRush is asked
# for this: the others would need their own reload, which is a different thing to
# measure. What it is for is the one in 10 §1 — no failed request while it happens.
reload_under_load() { # name, rate
    (
        for turn in $(seq "$DURATION"); do
            sed "s/x-served-by, value: edgerush/x-served-by, value: edgerush-$turn/" \
                "$here/proxy.yaml" >"$config.next"
            mv "$config.next" "$config"
            sleep 1
        done
    ) &
    local rewriting=$!
    oha_at "$1" "$2" "$proxy" -c 64
    kill "$rewriting" 2>/dev/null || true
    wait "$rewriting" 2>/dev/null || true
    cp "$here/proxy.yaml" "$config"
}

# What connections cost while nothing is happening on them: the proxy's own memory
# with a few thousand open and answered, against the same proxy with none.
idle_memory() { # name
    local name=$1
    local quiet
    quiet=$(python3 "$here/rss.py" "$proxy_pid")
    taskset -c "$GEN_CPUS" python3 "$here/idle.py" "$proxy_at" "$host" \
        "$IDLE_CONNECTIONS" >"$OUT/$name.ready" 2>"$OUT/$name.err" &
    local holding=$!
    for _ in $(seq 600); do
        grep -q ready "$OUT/$name.ready" 2>/dev/null && break
        sleep 0.1
    done
    # A sweep runs once a second; give it one so that what is held is settled.
    sleep 2
    local held
    held=$(python3 "$here/rss.py" "$proxy_pid")
    {
        echo "connections $IDLE_CONNECTIONS"
        echo "rss_quiet_kb $quiet"
        echo "rss_held_kb $held"
        echo "per_connection_bytes $(( (held - quiet) * 1024 / IDLE_CONNECTIONS ))"
    } >"$OUT/$name.out"
    kill "$holding" 2>/dev/null || true
    wait "$holding" 2>/dev/null || true
    echo "done: $name"
}

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

# The rest of what 13 §8 step 6 asks a series to cover.
carrying_runs() {
    streamed_answer "$1.streamed-answer" "$streamed_rate"
    streamed_request "$1.streamed-request" "$streamed_rate"
    slow_upstream "$1.slow-upstream" "$slow_rate"
    cancelled "$1.cancelled" "$slow_rate"
    idle_memory "$1.idle-memory"
    case "$1" in
    # Only EdgeRush is asked to take a config over while it serves.
    nginx.* | haproxy.* | envoy.* | kong.*) ;;
    *) reload_under_load "$1.reload" "$h1_rate" ;;
    esac
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
profile-body)
    body_direction=${2:?choose upload or answer}
    case "$body_direction" in upload | answer) ;; *) exit 2 ;; esac
    streamed_rate=${3:-20}
    [[ "$DURATION" =~ ^[0-9]+$ ]] && [ "$DURATION" -ge 10 ] || {
        echo "profile-body needs an integer DURATION of at least 10 seconds" >&2
        exit 2
    }
    # -p attaches to every thread in EdgeRush, but not workers of a separate process.
    for variant in $VARIANTS; do
        case "$variant" in thread-per-core | thread-per-core-kernel | ours) ;;
        *) echo "profile-body supports EdgeRush client variants only" >&2; exit 2 ;;
        esac
    done
    command -v perf >/dev/null || { echo "perf is required" >&2; exit 2; }
    edgerush=${EDGERUSH:-$repo/target/profiling/edgerush}
    [ -x "$edgerush" ] || {
        echo "build first: cargo build --profile profiling -p edgerush" >&2
        exit 2
    }
    ;;
ceiling | saturation | latency | carrying | hotpaths | instructions) ;;
*)
    sed -n '2,15p' "$0" >&2
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
instructions)
    # What a worker's instructions go on, and how many of them one request takes. Sampled
    # on `instructions:u` rather than on time, so a share here is a share of the work
    # rather than of the wait; `perf stat` beside it gives the total to divide by the
    # requests that were served.
    for variant in $VARIANTS; do
        start_proxy "$variant"
        sudo -n perf record -q -e instructions:u -F 3999 -p "$proxy_pid"             -o "$OUT/$variant.perf.data" -- sleep "$DURATION" &
        recorder=$!
        sudo -n perf stat -e instructions:u,instructions:k,cycles -x, -p "$proxy_pid"             -o "$OUT/$variant.stat" -- sleep "$DURATION" &
        counter=$!
        saturation_h1 "$variant.instructions" "$proxy" --connect-to="$proxy_at"
        wait "$recorder" "$counter" 2>/dev/null || true
        sudo -n chown "$(id -u)" "$OUT/$variant.perf.data" 2>/dev/null || true
        perf report -i "$OUT/$variant.perf.data" --stdio --no-children -s sym             --percent-limit 0 2>/dev/null | python3 "$here/buckets.py"             >"$OUT/$variant.parts" || true
        stop_proxy
        echo "== $variant =="
        cat "$OUT/$variant.parts"
        sleep 5
    done
    exit
    ;;
profile-body)
    sha256sum "$edgerush" >"$OUT/binary.sha256"
    git -C "$repo" diff --binary HEAD >"$OUT/source.patch"
    each_variant profile_body_runs
    ;;
hotpaths)
    streamed_rate=${2:-20}
    hotpath_runs() {
        saturation_h1 "$1.saturation-h1" "$proxy" --connect-to="$proxy_at"
        streamed_answer "$1.streamed-answer" "$streamed_rate"
        streamed_request "$1.streamed-request" "$streamed_rate"
    }
    each_variant hotpath_runs
    ;;
carrying)
    # Low rates: every one of these is about what an exchange holds and for how long
    # rather than how many of them a worker gets through, and a body of megabytes at a
    # thousand a second is the generator's measurement rather than the proxy's.
    streamed_rate=${2:-20} slow_rate=${3:-100} h1_rate=${4:-10000}
    each_variant carrying_runs
    ;;
esac
python3 "$here/summary.py" "$OUT"
