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
#   bench/run.sh idle                         what idle connections cost: never written to,
#                                             after one request, after one large head,
#                                             and beside a steady load
#   bench/run.sh soak [MINUTES] [RATE]        EdgeRush under mixed load and reloads for a
#                                             long while: what it holds, every ten seconds
#   bench/run.sh h2 [RATE] [STREAMED]         HTTP/2 clients alone: few and one hot
#                                             connection, latency, streamed bodies, idle
#   bench/run.sh profile-body upload|answer [RATE]  CPU stacks during streamed bodies
#   bench/run.sh grpc                         unary gRPC calls: few connections, one hot one
#   bench/run.sh handshakes [RATE] [FLOOD]    (TLS=1) steady clients at RATE beside a flood of
#                                             FLOOD connections each made anew, by
#                                             variant: ours-abN accepts N at a time
#   bench/run.sh h3 [STREAMED]                HTTP/3 clients: few hot connections, many, one
#                                             hot one, streamed bodies at STREAMED a second,
#                                             and TLS HTTP/2 beside them to read them by;
#                                             what H3_HELD connections held open cost
#   bench/run.sh h3latency [RATE...]          HTTP/3 latency at each RATE a second (5000 12500
#                                             25000 unless said), in h2load's 10 ms bursts and
#                                             evenly where H2LOAD3_SMOOTH is, with curl
#                                             beside it where CURL3 is
#   bench/run.sh mixed [RATE...]              HTTP/1 and HTTP/2 over TLS and HTTP/3 at once:
#                                             each at RATE a second, then as fast as each goes
#   bench/run.sh passthrough CHURN [TLS_CHURN] [STREAMED]
#                                             TCP and TLS passthrough: kept connections at
#                                             saturation, a connection a request at CHURN (and
#                                             TLS_CHURN) a second, and streamed answers;
#                                             PROXY_PROTOCOL=1, a header in and one out
#   bench/run.sh websocket [RATE] [CHURN]     WebSocket over HTTP/1.1 upgrades, to an echo
#                                             backend: busy connections, messages at RATE
#                                             a second, large messages, a connection a
#                                             message at CHURN a second, and what open ones
#                                             cost held idle (without TLS)
#   bench/run.sh summary DIR                  the table of a finished run
#
# UPSTREAM_H2=1 has the proxy speak HTTP/2 to the backend, by prior knowledge, many
# requests on each connection: EdgeRush and HAProxy; NGINX by its gRPC proxy, in grpc.
#
# TLS=1 has clients reach the proxy over TLS — EdgeRush, NGINX and HAProxy, with one
# self-signed ECDSA P-256 certificate — for saturation, latency and h2. Its churn is a
# full handshake for every request.
#
# H3=1 (which is TLS=1 as well) has those three serve HTTP/3 on the same port, over UDP;
# the `h3` scenario sets it itself. Its generator is h2load built with HTTP/3 (H2LOAD3).
#
# ACCESS_LOG=1 has EdgeRush, NGINX and HAProxy log every request they serve to a file, the
# same record each, for the modes that keep the config they start with (saturation,
# latency); not with passthrough.
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
# The counts `idle` weighs each kind of idle connection at (14 §8).
: "${IDLE_COUNTS:=2000 20000}"
: "${TLS:=0}"
: "${H3:=0}"
[ "${1:-}" = h3 ] && H3=1
[ "${1:-}" = h3latency ] && H3=1
[ "${1:-}" = mixed ] && H3=1
[ "$H3" = 1 ] && TLS=1
# COUNT=1 counts, over every process of the proxy, the instructions and cycles each
# measurement costs, user and kernel apart (perf stat): from 2 s into the load, for
# DURATION - 4 seconds, so that it sees neither end of it; a request's share is by the rates.
: "${COUNT:=0}"
# h2load with HTTP/3 whose request timer may fire every millisecond rather than every ten,
# and which does not pad every packet (bench/README.md): an even rate, where H2LOAD3's comes
# in bursts. h3latency measures with both when it is there, and mixed with it.
: "${H2LOAD3_SMOOTH:=$HOME/tools/h2load3-smooth/bin/h2load}"
# curl with HTTP/3 (bench/README.md): h3latency's second client, a request every 10 ms on one
# connection beside the load, where it is there.
: "${CURL3:=$HOME/tools/curl3/bin/curl}"
# How many HTTP/3 connections the h3 mode holds open, a request every ten seconds on each, to
# weigh what one costs.
: "${H3_HELD:=1000}"
# The profile-guided build of EdgeRush that the ours-pgo variant runs.
: "${EDGERUSH_PGO:=}"
# PASSTHROUGH=1, which `passthrough` sets, has the proxies carry connections rather than
# serve them (17 step 4): passthrough.yaml for EdgeRush, NGINX's stream module
# (nginx-stream.conf), HAProxy in mode tcp (haproxy-tcp.cfg). The backend answers on a TLS
# port as well, 9443, for the tls listener at 8443 to carry clients to by name.
: "${PASSTHROUGH:=0}"
[ "${1:-}" = passthrough ] && PASSTHROUGH=1
# PROXY_PROTOCOL=1, with `passthrough`, has every connection come to the proxy with a PROXY
# header and go on to the backend with one (20 step 6): clients reach a front hop, HAProxy on
# the generator's CPUs, at 7080 and 7443, which sends each on with a v2 header to the proxy
# under test; that one believes it, as from a sender, and sends a header of its own to the
# backend, which listens for one. EdgeRush and HAProxy send v2; NGINX 1.28 sends only v1.
: "${PROXY_PROTOCOL:=0}"
# WEBSOCKET=1, which `websocket` sets, has bench/wsbench be the backend in NGINX's place: it
# answers requests 200 and WebSocket handshakes with a 101, and echoes every message. NGINX
# the proxy is told to pass the upgrade on, as its documentation has it; HAProxy and
# EdgeRush do so by themselves. WSBENCH is its binary
# (cargo build --release --manifest-path bench/wsbench/Cargo.toml).
: "${WEBSOCKET:=0}"
[ "${1:-}" = websocket ] && WEBSOCKET=1
: "${WSBENCH:=$here/wsbench/target/release/wsbench}"
# ACCESS_LOG=1 has every proxy log each request as EdgeRush's access log does (21 §5): the
# same fields as a line of JSON each, to a file, each proxy its fastest way. EdgeRush its own
# (a worker's batches, a logger thread); NGINX through `escape=json`, into a 64 KiB buffer
# written at least every second; HAProxy a JSON log-format into a ring, which it sends on
# over TCP in the background to a receiver (an HAProxy `log-forward`) on the generator's
# CPUs that writes the file. The receiver's work is not counted in HAProxy's, as the front
# hop's is not; EdgeRush's and NGINX's writing is counted in theirs. Each turn's records are
# counted, one line a request, into access-log-lines, its first kept as <turn>.access-log,
# and the file removed.
: "${ACCESS_LOG:=0}"
# h2load with HTTP/3: Ubuntu's is built without it (bench/README.md says how to build one).
: "${H2LOAD3:=$HOME/tools/h2load3/bin/h2load}"
# The loopback's MTU while HTTP/3 is measured (sudo). The loopback's own 64 KiB lets a QUIC
# stack that discovers its path's MTU, as NGINX's does, send datagrams of 44 KB that no
# Internet path carries, and its scenarios would measure that rather than the proxies:
# 1,500 bytes is an Ethernet path's (16 §8). Put back when the run ends; 0 leaves it alone.
: "${LOOPBACK_MTU:=1500}"
: "${UPSTREAM_H2:=0}"
# The CPUs' idle states (bench/residency.py): the policies every variant takes turns under,
# `normal` (every state the driver has) or the deepest state allowed by name, on every CPU
# (`C1E`), on the proxy's cores alone (`C1E-proxy`) or on every other core (`C1E-others`).
# How long each CPU spent in which state is written down for every measurement
# (RESIDENCY=1), what the OS asked for and, where perf counts it, what the hardware did.
: "${IDLE:=normal}"
: "${RESIDENCY:=1}"
: "${VARIANTS:=ours}" # and: ours-kernel nginx haproxy envoy kong
: "${OUT:=$here/results/$(date +%Y%m%d-%H%M%S)}"

# Hundreds of connections on either side of the proxy, and more when they churn.
ulimit -n 65536

# The binary under test. `instructions` points this at a build with symbols in it.
edgerush=${EDGERUSH:-$repo/target/release/edgerush}
run=/tmp/edgerush-bench
# The proxy is given a copy rather than the file in the repository: one scenario
# rewrites it while the load is on, and the repository is not the place for that.
config=$run-config.yaml
access_log=$run-access.log
backend=http://127.0.0.1:9000/
# The proxy is asked for by the name its routes are for, and the generators are told where
# that is: a `host` header would not do, as HTTP/2 names the host in the target.
host=bench.example.com
scheme=http
[ "$TLS" = 1 ] && scheme=https
proxy=$scheme://$host:8080/
proxy_at=127.0.0.1:8080
tls=$run/tls
proxy_pid=
# Set by the grpc mode: NGINX's gRPC proxy is given the gRPC service.
grpc_pass=0

start_backend() {
    # One left over from another run would answer in its place, from whatever config it
    # was started with, and nothing would say so.
    if curl -s -o /dev/null "$backend"; then
        echo "something already answers at $backend: stop it first (pgrep -fa 'nginx -p')" >&2
        exit 1
    fi
    mkdir -p "$run/tmp"
    # What the streamed scenarios ask for, and what the trickling one trickles.
    [ -s "$run/big.bin" ] || head -c "$STREAMED" /dev/zero >"$run/big.bin"
    # What the trickling one trickles: small enough that a request finishes in a few
    # seconds at the rate that location is held to.
    [ -s "$run/slow.bin" ] || head -c $((STREAMED / 8)) /dev/zero >"$run/slow.bin"
    # A gRPC message frame with nothing in it: uncompressed, of length 0. What the gRPC
    # scenario's calls send and what the backend's service answers.
    printf '\0\0\0\0\0' >"$run/grpc.bin"
    cp "$here/proxy.yaml" "$config"
    [ "$TLS" = 1 ] && secure
    if [ "$ACCESS_LOG" = 1 ]; then
        if [ "$PASSTHROUGH" = 1 ]; then
            echo "ACCESS_LOG is not set up for passthrough" >&2
            exit 2
        fi
        rm -f "$access_log"
        sed -i "s#request_id: generate }#request_id: generate, access_log: { file: \"$access_log\" } }#" "$config"
        grep -q 'access_log: { file:' "$config"
    fi
    if [ "$UPSTREAM_H2" = 1 ]; then
        sed -i 's#backend: { load_balancer: p2c, endpoints: \["127.0.0.1:9000"\] }#backend: { load_balancer: p2c, endpoints: ["127.0.0.1:9000"], protocol: http2 }#' "$config"
        grep -q 'protocol: http2' "$config"
    fi
    local backend_conf=$here/nginx.conf
    if [ "$PASSTHROUGH" = 1 ]; then
        cp "$here/passthrough.yaml" "$config"
        # The same answers over TLS on 9443, with the bench's certificate, for the
        # proxies' tls listeners to carry clients to.
        certificate
        backend_conf=$run-backend.conf
        sed "0,/^    server {/s##    server {\n        listen 127.0.0.1:9443 ssl reuseport backlog=4096;\n        ssl_certificate $tls/cert.pem;\n        ssl_certificate_key $tls/key.pem;#" \
            "$here/nginx.conf" >"$backend_conf"
        grep -q 'listen 127.0.0.1:9443 ssl' "$backend_conf"
        if [ "$PROXY_PROTOCOL" = 1 ]; then
            # Both of the backend's ports take a header first, and the proxies are told to
            # send one, believing the front hop's.
            sed -i 's#listen 127.0.0.1:9000 reuseport backlog=4096;#listen 127.0.0.1:9000 reuseport backlog=4096 proxy_protocol;#;
                s#listen 127.0.0.1:9443 ssl reuseport backlog=4096;#listen 127.0.0.1:9443 ssl reuseport backlog=4096 proxy_protocol;#' \
                "$backend_conf"
            [ "$(grep -c 'backlog=4096 proxy_protocol;' "$backend_conf")" = 2 ]
            sed -i 's#protocol: tcp, proxy_protocol: off#protocol: tcp, proxy_protocol: { senders: ["127.0.0.0/8"] }#;
                s#protocol: tls, proxy_protocol: off#protocol: tls, proxy_protocol: { senders: ["127.0.0.0/8"] }#;
                s#{ load_balancer: p2c, endpoints: \[#{ proxy_protocol: v2, load_balancer: p2c, endpoints: [#' "$config"
            [ "$(grep -c 'senders' "$config")" = 2 ]
            [ "$(grep -c 'proxy_protocol: v2' "$config")" = 2 ]
        fi
    fi
    if [ "$WEBSOCKET" = 1 ]; then
        [ -x "$WSBENCH" ] || { echo "no wsbench at $WSBENCH: build it first" >&2; exit 2; }
        taskset -c "$BACKEND_CPUS" "$WSBENCH" serve 127.0.0.1:9000 \
            --threads "$(cpus_in "$BACKEND_CPUS")" 2>>"$run/wsbench.log" &
        echo $! >"$run/wsbench.pid"
    else
        taskset -c "$BACKEND_CPUS" nginx -p "$run/" -c "$backend_conf" -e "$run/error.log"
    fi
    if [ "$PROXY_PROTOCOL" = 1 ]; then
        await "$backend" --haproxy-protocol
        start_front
    else
        await "$backend"
    fi
}

cpus_in() { # list: how many CPUs a comma-separated list names
    echo "$1" | tr ',' '\n' | wc -l
}

# The certificate every variant presents, made once.
certificate() {
    mkdir -p "$tls"
    [ -s "$tls/both.pem" ] || {
        openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
            -subj "/CN=$host" -addext "subjectAltName=DNS:$host" \
            -keyout "$tls/key.pem" -out "$tls/cert.pem" 2>/dev/null
        cat "$tls/cert.pem" "$tls/key.pem" >"$tls/both.pem"
    }
}

# The certificate every variant presents, and EdgeRush's config made to listen with it;
# NGINX's and HAProxy's are made as they start.
secure() {
    certificate
    python3 - "$config" "$tls" "$H3" <<'PY'
import json, sys
config, tls, h3 = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
text = open(config).read()
plain = 'web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, '
assert plain in text and "\ncertificates:" not in text
http3 = "http3: {}, " if h3 else ""
text = text.replace(plain, 'web: { address: "127.0.0.1:8080", protocol: https, proxy_protocol: off, '
                    f'tls: {{ certificates: [web] }}, {http3}')
# The config names the certificate's files; it never holds the key (07 §1 in the docs).
chain, key = json.dumps(f"{tls}/cert.pem"), json.dumps(f"{tls}/key.pem")
text += f"certificates: {{ web: {{ chain_file: {chain}, key_file: {key} }} }}\n"
open(config, "w").write(text)
PY
}

# PROXY_PROTOCOL's front hop: HAProxy on the generator's CPUs, carrying 7080 and 7443 to the
# proxy under test with a v2 header in front of each connection, as a cloud load balancer
# would. The same for every variant, and not counted in any variant's CPU.
front_pid=
start_front() {
    cat >"$run-front.cfg" <<'CFG'
global
    maxconn 30000
defaults
    mode tcp
    timeout connect 5s
    timeout client 3600s
    timeout server 3600s
listen plain
    bind 127.0.0.1:7080
    server proxy 127.0.0.1:8080 send-proxy-v2
listen named
    bind 127.0.0.1:7443
    server proxy 127.0.0.1:8443 send-proxy-v2
CFG
    taskset -c "$GEN_CPUS" haproxy -db -f "$run-front.cfg" 2>>"$OUT/front.log" &
    front_pid=$!
}

stop_backend() {
    if [ -n "$front_pid" ]; then
        kill "$front_pid" 2>/dev/null || true
        front_pid=
    fi
    [ -f "$run/nginx.pid" ] && kill "$(cat "$run/nginx.pid")" 2>/dev/null || true
    if [ -f "$run/wsbench.pid" ]; then
        kill "$(cat "$run/wsbench.pid")" 2>/dev/null || true
        rm -f "$run/wsbench.pid"
    fi
}

start_proxy() { # variant, or a name made of one: `ours@C1E` is ours under an idle policy
    set -- "${1%%@*}"
    local daemon=
    # Given unquoted below, so that it is two flags and their values rather than one
    # long argument. Only the variants that are EdgeRush are given it.
    local idle="--idle-per-destination $IDLE_PER_DESTINATION --idle-total $IDLE_TOTAL"
    case "$ACCESS_LOG.$1" in
    1.envoy | 1.kong | 1.nginx-direct)
        echo "$1 is not set up to log: leave it out of ACCESS_LOG runs" >&2
        exit 2
        ;;
    esac
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
    nginx-direct)
        # NGINX answering every request itself: the counterpart of EdgeRush's own answer to
        # a host no route is for.
        mkdir -p "$run-direct/tmp"
        sed "s/WORKERS/$WORKERS/" "$here/nginx-direct.conf" >"$run-direct/nginx.conf"
        taskset -c "$PROXY_CPUS" nginx -p "$run-direct/" -c "$run-direct/nginx.conf" \
            -e "$run-direct/error.log" 2>>"$OUT/proxy.log" &
        ;;
    nginx)
        # NGINX's proxy speaks HTTP/1.1 to its upstreams; its gRPC proxy speaks HTTP/2, and
        # the grpc mode gives the gRPC service to that.
        if [ "$UPSTREAM_H2" = 1 ] && [ "$grpc_pass" != 1 ]; then
            echo "NGINX cannot proxy to an HTTP/2 upstream: leave it out of UPSTREAM_H2 runs" >&2
            exit 2
        fi
        mkdir -p "$run-proxy/tmp"
        if [ "$PASSTHROUGH" = 1 ]; then
            sed "s/WORKERS/$WORKERS/" "$here/nginx-stream.conf" >"$run-proxy/nginx.conf"
            # Ubuntu builds the stream module apart (libnginx-mod-stream), and its own
            # config loads it by a path relative to a prefix this run replaces.
            local stream_module=/usr/lib/nginx/modules/ngx_stream_module.so
            if [ -f "$stream_module" ]; then
                sed -i "1i load_module $stream_module;" "$run-proxy/nginx.conf"
            fi
            if [ "$PROXY_PROTOCOL" = 1 ]; then
                # A header read from the front hop, believed (realip), and one sent on.
                sed -i 's#listen 127.0.0.1:\(8080\|8443\) reuseport backlog=4096;#listen 127.0.0.1:\1 reuseport backlog=4096 proxy_protocol;\n        proxy_protocol on;#;
                    s#^stream {#stream {\n    set_real_ip_from 127.0.0.0/8;#' "$run-proxy/nginx.conf"
                [ "$(grep -c 'proxy_protocol on;' "$run-proxy/nginx.conf")" = 2 ]
            fi
        else
            sed "s/WORKERS/$WORKERS/" "$here/nginx-proxy.conf" >"$run-proxy/nginx.conf"
            if [ "$ACCESS_LOG" = 1 ]; then
                # EdgeRush's fields, where NGINX has them; the route, rule, upstream and
                # tries are the one route's, written as they are.
                sed -i "s#^    access_log off;#    log_format edgerush escape=json '{\"time\":\"\$time_iso8601\",\"kind\":\"request\",\"id\":\"\$request_id\",\"listener\":\"web\",\"client\":\"\$remote_addr\",\"peer\":\"\$remote_addr:\$remote_port\",\"protocol\":\"\$server_protocol\",\"method\":\"\$request_method\",\"host\":\"\$host\",\"path\":\"\$request_uri\",\"status\":\$status,\"route\":\"bench\",\"rule\":2,\"upstream\":\"backend\",\"endpoint\":\"\$upstream_addr\",\"tries\":1,\"bytes_in\":\$request_length,\"bytes_out\":\$body_bytes_sent,\"duration_ms\":\$request_time,\"upstream_ms\":\$upstream_response_time}';\n    access_log $access_log edgerush buffer=64k flush=1s;#" \
                    "$run-proxy/nginx.conf"
                grep -q "access_log $access_log edgerush buffer=64k flush=1s;" "$run-proxy/nginx.conf"
            fi
        fi
        if [ "$WEBSOCKET" = 1 ]; then
            # The upgrade passed on (NGINX's WebSocket proxying), and a tunnel's idle bound
            # of an hour, as EdgeRush's is by default.
            sed -i 's#^        proxy_set_header Connection "";#        proxy_set_header Upgrade $http_upgrade;\n        proxy_set_header Connection $connection_upgrade;\n        proxy_read_timeout 3600s;\n        proxy_send_timeout 3600s;#;
                s#^http {#http {\n    map $http_upgrade $connection_upgrade {\n        default upgrade;\n        "" "";\n    }#' \
                "$run-proxy/nginx.conf"
            grep -q 'proxy_set_header Connection $connection_upgrade;' "$run-proxy/nginx.conf"
        fi
        if [ "$grpc_pass" = 1 ]; then
            sed -i "s#^        location / {#        location /bench.Echo/ {\n            grpc_pass grpc://backend;\n        }\n&#" \
                "$run-proxy/nginx.conf"
            grep -q 'grpc_pass grpc://backend' "$run-proxy/nginx.conf"
        fi
        if [ "$TLS" = 1 ]; then
            sed -i "s#listen 127.0.0.1:8080#listen 127.0.0.1:8080 ssl#;
                s#^http {#http {\n    ssl_certificate $tls/cert.pem;\n    ssl_certificate_key $tls/key.pem;#" \
                "$run-proxy/nginx.conf"
        fi
        if [ "$H3" = 1 ]; then
            # QUIC beside TCP on each server, the default one sharing the port among the
            # workers as its TCP socket does.
            sed -i "s#listen 127.0.0.1:8080 ssl reuseport backlog=4096 default_server;#&\n        listen 127.0.0.1:8080 quic reuseport default_server;#;
                s#listen 127.0.0.1:8080 ssl;#&\n        listen 127.0.0.1:8080 quic;#" \
                "$run-proxy/nginx.conf"
            grep -q 'quic reuseport' "$run-proxy/nginx.conf"
        fi
        taskset -c "$PROXY_CPUS" nginx -p "$run-proxy/" -c "$run-proxy/nginx.conf" \
            -e "$run-proxy/error.log" 2>>"$OUT/proxy.log" &
        ;;
    haproxy)
        # A thread for every CPU it may run on: WORKERS of them, if PROXY_CPUS is as many.
        local haproxy_cfg=$run-haproxy.cfg
        if [ "$PASSTHROUGH" = 1 ]; then
            cp "$here/haproxy-tcp.cfg" "$haproxy_cfg"
            if [ "$PROXY_PROTOCOL" = 1 ]; then
                sed -i 's#^    bind 127.0.0.1:\(8080\|8443\)$#    bind 127.0.0.1:\1 accept-proxy#;
                    s#^    server nginx \(127.0.0.1:9[0-9]*\)$#    server nginx \1 send-proxy-v2#' "$haproxy_cfg"
                [ "$(grep -c 'accept-proxy' "$haproxy_cfg")" = 2 ]
                [ "$(grep -c 'send-proxy-v2' "$haproxy_cfg")" = 2 ]
            fi
        else
            cp "$here/haproxy.cfg" "$haproxy_cfg"
            if [ "$ACCESS_LOG" = 1 ]; then
                # The host is captured as the request comes: a log-format cannot read a
                # request's headers. The ring holds about two seconds of records at
                # saturation, should the receiver fall behind.
                sed -i 's#^    unique-id-header x-request-id$#&\n    http-request capture req.hdr(host) len 128\n    log ring@access format rfc5424 local0\n    log-format "%{+json}o %(time)tr %(kind)[str(request)] %(id)ID %(listener)f %(client)ci %(peer_port)cp %(protocol)HV %(method)HM %(host)[capture.req.hdr(0)] %(path)HU %(status)ST %(route)b %(upstream)b %(endpoint)si %(endpoint_port)sp %(tries)rc %(bytes_in)U %(bytes_out)B %(duration_ms)Ta %(upstream_ms)Tr"#' \
                    "$haproxy_cfg"
                grep -q '^    log ring@access format rfc5424 local0$' "$haproxy_cfg"
                cat >>"$haproxy_cfg" <<'CFG'

ring access
    format rfc5424
    maxlen 4096
    size 67108864
    timeout connect 5s
    timeout server 3600s
    server receiver 127.0.0.1:5514
CFG
                start_receiver
            fi
        fi
        if [ "$TLS" = 1 ]; then
            sed -i "s#bind 127.0.0.1:8080#bind 127.0.0.1:8080 ssl crt $tls/both.pem alpn h2,http/1.1#" \
                "$haproxy_cfg"
        fi
        if [ "$H3" = 1 ]; then
            sed -i "s#^    bind 127.0.0.1:8080 ssl .*#&\n    bind quic4@127.0.0.1:8080 ssl crt $tls/both.pem alpn h3#" \
                "$haproxy_cfg"
            grep -q 'quic4@' "$haproxy_cfg"
        fi
        if [ "$UPSTREAM_H2" = 1 ]; then
            sed -i "s#server nginx 127.0.0.1:9000#server nginx 127.0.0.1:9000 proto h2#" "$haproxy_cfg"
        fi
        taskset -c "$PROXY_CPUS" haproxy -db -f "$haproxy_cfg" 2>>"$OUT/proxy.log" &
        ;;
    ours-kernel)
        # Connections left where the kernel put them: what balancing is measured against.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
            --accept kernel --workers "$WORKERS" $idle \
            2>>"$OUT/proxy.log" &
        ;;
    ours)
        # `/metrics` only where a run reads it, as the soak does: METRICS is its address.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
            --workers "$WORKERS" $idle ${METRICS:+--metrics "$METRICS"} \
            2>>"$OUT/proxy.log" &
        ;;
    ours-pgo)
        # The same source built with a profile of its own work (12, item 8): EDGERUSH_PGO.
        [ -x "$EDGERUSH_PGO" ] || { echo "ours-pgo needs EDGERUSH_PGO, a PGO build" >&2; exit 2; }
        taskset -c "$PROXY_CPUS" "$EDGERUSH_PGO" proxy --config "$config" \
            --workers "$WORKERS" $idle 2>>"$OUT/proxy.log" &
        ;;
    ours-ab*)
        # EdgeRush accepting N connections before its other work goes first: ours-ab1.
        taskset -c "$PROXY_CPUS" "$edgerush" proxy --config "$config" \
            --workers "$WORKERS" $idle --accept-batch "${1#ours-ab}" \
            2>>"$OUT/proxy.log" &
        ;;
    *)
        echo "no variant $variant" >&2
        exit 2
        ;;
    esac
    proxy_pid=${daemon:-$!}
    await "$scheme://$proxy_at/"
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
    stop_receiver
}

# With ACCESS_LOG=1, HAProxy's log receiver: an HAProxy that takes the records its ring sends
# over TCP and writes each to the access log as it came. On the generator's CPUs, as the
# front hop is, and not counted in the proxy's work.
receiver_pid=
start_receiver() {
    cat >"$run-receiver.cfg" <<'CFG'
global
    maxconn 100
log-forward access
    bind 127.0.0.1:5514
    log stdout format raw local0
CFG
    taskset -c "$GEN_CPUS" haproxy -db -f "$run-receiver.cfg" >>"$access_log" 2>>"$OUT/proxy.log" &
    receiver_pid=$!
}

# Once the proxy has gone, a moment for the receiver to write what it was sent, and then it
# goes too.
stop_receiver() {
    [ -n "$receiver_pid" ] || return 0
    sleep 1
    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
    receiver_pid=
}

await() { # url, curl options...
    local url=$1
    shift
    for _ in $(seq 100); do
        curl -sk -o /dev/null -H "host: $host" "$@" "$url" && return
        sleep 0.1
    done
    echo "nothing answers at $url" >&2
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

# What the proxy holds while a measurement runs, every half second: the resident memory of
# all its processes, in KiB (NGINX's workers included; pages they share counted in each).
# With COUNT=1, also its instructions and cycles over a window inside the load; with
# RESIDENCY=1, the CPUs' idle states over that window. The watchers run on the generator's
# CPUs, as the clock sampler does.
watchers=
watch_proxy() { # name
    watchers=
    [ -n "$proxy_pid" ] || return 0
    local pids
    pids=$(echo "$proxy_pid" $(pgrep -P "$proxy_pid") | tr ' ' ',')
    taskset -c "$GEN_CPUS" bash -c 'while sleep 0.5; do
        awk "/^VmRSS:/ { kib += \$2 } END { print kib }" $(printf "/proc/%s/status " ${0//,/ }) 2>/dev/null
    done' "$pids" >"$OUT/$1.rss" &
    watchers=$!
    if [ "$COUNT" = 1 ] && [ "$DURATION" -ge 8 ]; then
        # perf's count comes on stderr, into a file of this user's rather than one perf makes
        # as root, so that the window's length can follow it.
        (
            sleep 2
            sudo -n perf stat -x, -p "$pids" \
                -e instructions:u,instructions:k,cycles:u,cycles:k -- sleep "$((DURATION - 4))"
            echo "# window $((DURATION - 4))" >&2
        ) >/dev/null 2>"$OUT/$1.stat" &
        watchers="$watchers $!"
    fi
    if [ "$RESIDENCY" = 1 ] && [ "$DURATION" -ge 8 ]; then
        # The same window: each CPU's idle states, and the hardware's counts where perf has
        # them, raw, into `<name>.idle` (JSON).
        (
            sleep 2
            taskset -c "$GEN_CPUS" python3 "$here/residency.py" window "$((DURATION - 4))" \
                "$OUT/$1.idle" "$PROXY_CPUS"
        ) >/dev/null 2>>"$OUT/residency.log" &
        watchers="$watchers $!"
    fi
}
unwatch_proxy() {
    [ -n "$watchers" ] || return 0
    local first=${watchers%% *}
    kill "$first" 2>/dev/null || true
    # The counter ends by itself, inside the load; it is waited for, not stopped.
    wait $watchers 2>/dev/null || true
    watchers=
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
    watch_proxy "$name"
    local status=0
    taskset -c "$GEN_CPUS" "$@" >"$OUT/$name.out" 2>"$OUT/$name.err" || status=$?
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
    unwatch_proxy
    cpu "$OUT/$name.cpu-after"
    if [ "$status" -ne 0 ]; then
        echo "failed: $name" >&2
        return "$status"
    fi
    echo "done: $name"
}

# Three generators at once, one a protocol, each the row of its own (`<name>-h1` and so
# on); what the proxy spent and held over the whole goes to `<name>-all`. The commands are
# split on spaces, so nothing in them may hold one.
mixed_once() { # name, HTTP/1 command, HTTP/2 command, HTTP/3 command
    local name=$1 status=0
    shift
    cpu "$OUT/$name-all.cpu-before"
    watch_proxy "$name-all"
    local protocol pids=
    for protocol in h1 h2 h3; do
        # shellcheck disable=SC2086
        taskset -c "$GEN_CPUS" $1 >"$OUT/$name-$protocol.out" 2>"$OUT/$name-$protocol.err" &
        pids="$pids $!"
        shift
    done
    local pid
    for pid in $pids; do
        wait "$pid" || status=$?
    done
    unwatch_proxy
    cpu "$OUT/$name-all.cpu-after"
    if [ "$status" -ne 0 ]; then
        echo "failed: $name" >&2
        return 0
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

# The same over HTTP/3, by h2load built with it.
h3load() { # name, options...
    local name=$1
    shift
    measured "$name" "$H2LOAD3" --alpn-list=h3 -D "$DURATION" --warm-up-time=3 "$@" \
        --connect-to="$proxy_at" "$proxy"
}

# Open loop: a fixed rate, and latency counted from when a request was due. Requests that
# are under way when the time is up are waited for, not counted as failures.
oha_at() { # name, rate, url, options...
    local name=$1 rate=$2 url=$3
    shift 3
    local insecure=
    [ "$TLS" = 1 ] && insecure=--insecure
    measured "$name" oha -z "${DURATION}s" -w -q "$rate" --latency-correction --no-tui \
        --output-format json --connect-to "$host:8080:$proxy_at" $insecure "$@" "$url"
}

latency_h1() { oha_at "$1" "$2" "$3" -c 256; }
latency_h2() { oha_at "$1" "$2" "$3" --http2 -c 4 -p 100; }

# A rate over HTTP/3 by h2load, which every proxy's HTTP/3 takes (oha's does not), shared by
# its connections, each request's time logged for summary.py's percentiles. h2load keeps the
# rate while a connection has streams to spare, and the rate reached says if not. It is not
# an even rate: h2load's timer fires at most every 10 ms, so each connection sends a hundredth
# of its rate at once, in bursts, where oha's HTTP/1 and HTTP/2 rates come a millisecond
# apart (16 §8).
h3_at() { # name, rate, connections, streams, url, options...; H3GEN may name the h2load
    local name=$1 rate=$2 clients=$3 streams=$4 url=$5
    shift 5
    measured "$name" "${H3GEN:-$H2LOAD3}" --alpn-list=h3 -D "$DURATION" --warm-up-time=3 \
        -t2 -c"$clients" -m"$streams" --rps=$((rate / clients)) \
        --log-file="$OUT/$name.requests" --connect-to="$proxy_at" "$@" "$url"
}
latency_h3() { h3_at "$1" "$2" 4 100 "$3"; }

# One client's view beside a load: curl asking once every 10 ms on one HTTP/3 connection,
# from the end of h2load's warm-up to a second before its end, each request's times in
# `<name>.probe` (connections made, time to first byte, total). A second generator's
# measure of the same proxy, to read h2load's by.
probe_h3() { # name
    sleep 3
    taskset -c "$GEN_CPUS" "$CURL3" -sk --http3-only --rate 100/s --out-null \
        --connect-to "$host:8080:$proxy_at" \
        -w '%{num_connects} %{time_starttransfer} %{time_total}\n' \
        "${proxy}?probe=[1-$(((DURATION - 1) * 100))]" >"$OUT/$1.probe" 2>"$OUT/$1.probe-err"
}

# What HTTP/3 connections cost held open: COUNT of them on a proxy started afresh, a request
# every ten seconds on each, weighed once they are all up and have asked once.
held_h3() { # name, count
    local name=$1 count=$2
    stop_proxy
    start_proxy "${name%%.*}"
    sleep 1
    local quiet
    quiet=$(python3 "$here/rss.py" "$proxy_pid")
    taskset -c "$GEN_CPUS" "$H2LOAD3" --alpn-list=h3 -t2 -c"$count" -m1 --rps=0.1 -D 15 \
        --connect-to="$proxy_at" "$proxy" >"$OUT/$name.load" 2>&1 &
    local load=$!
    sleep 11
    local held
    held=$(python3 "$here/rss.py" "$proxy_pid")
    wait "$load" || true
    {
        echo "kind h3"
        echo "connections $count"
        # h2load's count of the requests answered, one or two a connection: that all of
        # them were up.
        echo "answered $(awk '/^requests:/ { print $8 + 0 }' "$OUT/$name.load")"
        echo "rss_quiet_kb $quiet"
        echo "rss_held_kb $held"
        echo "per_connection_bytes $(( (held - quiet) * 1024 / count ))"
    } >"$OUT/$name.out"
    echo "done: $name"
}

# One connection carrying hundreds of streams, as a gRPC client's does: everything it asks
# for lands on the one worker that owns it (15 §8).
hot_h2() { # name, url, options...
    local name=$1 url=$2
    shift 2
    measured "$name" h2load -c1 -m256 -t1 -D "$DURATION" --warm-up-time=3 "$@" "$url"
}
churn() { # name, rate, url, options...
    local name=$1 rate=$2 url=$3
    shift 3
    oha_at "$name" "$rate" "$url" -c 64 --disable-keepalive "$@"
}

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
idle_memory() { # name, [kind, count]: as bench/idle.py takes them
    local name=$1 kind=${2:-one-request} count=${3:-$IDLE_CONNECTIONS}
    # On a proxy started afresh. One that has just carried large bodies holds memory it
    # has freed and not handed back, and a small cost per connection disappears into it:
    # measured after the other scenarios, NGINX read as 0 bytes a connection, where a
    # fresh one shows 600-900.
    stop_proxy
    start_proxy "${name%%.*}"
    sleep 1
    local quiet
    quiet=$(python3 "$here/rss.py" "$proxy_pid")
    taskset -c "$GEN_CPUS" python3 "$here/idle.py" "$proxy_at" "$host" \
        "$count" "$kind" >"$OUT/$name.ready" 2>"$OUT/$name.err" &
    local holding=$!
    for _ in $(seq 1200); do
        grep -q ready "$OUT/$name.ready" 2>/dev/null && break
        sleep 0.1
    done
    # A sweep runs once a second; give it one so that what is held is settled. Not for
    # connections that have said nothing: the first request's deadline, ten seconds from
    # accept, would close them first.
    [ "$kind" = silent ] || sleep 2
    local held open
    held=$(python3 "$here/rss.py" "$proxy_pid")
    # What is weighed is what is still open, which is not always all that were opened.
    open=$(ss -Htn state established "( sport = :${proxy_at##*:} )" | wc -l)
    {
        echo "kind $kind"
        echo "connections $count"
        echo "open $open"
        echo "rss_quiet_kb $quiet"
        echo "rss_held_kb $held"
        echo "per_connection_bytes $(( (held - quiet) * 1024 / (open > 0 ? open : 1) ))"
    } >"$OUT/$name.out"
    kill "$holding" 2>/dev/null || true
    wait "$holding" 2>/dev/null || true
    echo "done: $name"
}

# What idle connections cost on a proxy that is busy at the same time (14 §8): the same
# load runs throughout, so what the idle ones add is read against a proxy doing the same
# work without them rather than against one doing nothing.
busy_idle_memory() { # name, count
    local name=$1 count=$2 rate=${BUSY_RATE:-10000}
    stop_proxy
    start_proxy "${name%%.*}"
    sleep 1
    taskset -c "$GEN_CPUS" oha -z 60s -q "$rate" -c 64 --no-tui \
        --connect-to "$host:8080:$proxy_at" "$proxy" >/dev/null 2>&1 &
    local load=$!
    sleep 5
    local busy
    busy=$(python3 "$here/rss.py" "$proxy_pid")
    taskset -c "$GEN_CPUS" python3 "$here/idle.py" "$proxy_at" "$host" \
        "$count" one-request >"$OUT/$name.ready" 2>"$OUT/$name.err" &
    local holding=$!
    for _ in $(seq 1200); do
        grep -q ready "$OUT/$name.ready" 2>/dev/null && break
        sleep 0.1
    done
    sleep 2
    local held open
    held=$(python3 "$here/rss.py" "$proxy_pid")
    # The idle ones only: the load's own connections are open too.
    open=$(( $(ss -Htn state established "( sport = :${proxy_at##*:} )" | wc -l) - 64 ))
    {
        echo "kind busy"
        echo "connections $count"
        echo "open $open"
        echo "rss_quiet_kb $busy"
        echo "rss_held_kb $held"
        echo "per_connection_bytes $(( (held - busy) * 1024 / (open > 0 ? open : 1) ))"
    } >"$OUT/$name.out"
    kill "$holding" "$load" 2>/dev/null || true
    wait "$holding" "$load" 2>/dev/null || true
    echo "done: $name"
}

# With ACCESS_LOG=1, once the proxy has gone: how many records the turn `name` wrote, and
# the first of them; the file is removed for the next.
logged() { # name
    local lines=0
    [ -f "$access_log" ] && lines=$(wc -l <"$access_log")
    echo "$1 $lines" >>"$OUT/access-log-lines"
    [ -f "$access_log" ] && head -1 "$access_log" >"$OUT/$1.access-log"
    rm -f "$access_log"
}

each_variant() { # function, that is given: prefix of the names
    local policies
    read -ra policies <<<"$IDLE"
    local count=${#policies[@]} rep turn policy label
    for rep in $(seq "$REPS"); do
        # The policies in a new order each repetition, so that what drifts is shared out.
        for turn in $(seq 0 $((count - 1))); do
            policy=${policies[$(((turn + rep - 1) % count))]}
            set_idle "$policy"
            for variant in $VARIANTS; do
                label=$variant
                [ "$policy" = normal ] || label="$variant@$policy"
                start_proxy "$variant"
                "$1" "$label.$rep"
                stop_proxy
                [ "$ACCESS_LOG" = 1 ] && logged "$label.$rep"
                sleep 5 # let it cool, and the sockets of the run go
            done
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
    case "${1%%[.@]*}" in
    # Only EdgeRush is asked to take a config over while it serves.
    nginx | haproxy | envoy | kong) ;;
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
        local cpuidle=/sys/devices/system/cpu/cpuidle
        echo "idle driver $(cat $cpuidle/current_driver 2>/dev/null)," \
            "governor $(cat $cpuidle/current_governor_ro $cpuidle/current_governor 2>/dev/null | head -1);" \
            "states $(cat /sys/devices/system/cpu/cpu0/cpuidle/state*/name 2>/dev/null | tr '\n' ' ')"
        local disabled
        disabled=$(awk '$2 == 1 { print $1 }' "$idle_saved" |
            sed 's#/sys/devices/system/cpu/##; s#/cpuidle/#:#; s#/disable##' | tr '\n' ' ')
        echo "idle policies: $IDLE; disabled when the run began: ${disabled:-none}"
        echo "proxy on $PROXY_CPUS ($WORKERS workers), generator on $GEN_CPUS," \
            "backend on $BACKEND_CPUS, ${DURATION}s, $REPS repetitions"
        echo "idle upstream connections: $IDLE_PER_DESTINATION per destination," \
            "$IDLE_TOTAL in all"
        [ "$TLS" = 1 ] && echo "clients over TLS: $(openssl version), ECDSA P-256 certificate"
        [ "$UPSTREAM_H2" = 1 ] && echo "upstream spoken to in HTTP/2 (prior knowledge)"
        h2load --version
        [ "$H3" = 1 ] && echo "HTTP/3 by $("$H2LOAD3" --version)"
        [ "$H3" = 1 ] && echo "loopback MTU $(cat /sys/class/net/lo/mtu)"
        [ "$PASSTHROUGH" = 1 ] && echo "passthrough: tcp at 8080 to the backend's 9000," \
            "tls at 8443 to its 9443 by name"
        [ "$WEBSOCKET" = 1 ] && echo "websocket: wsbench at 9000 answers and echoes;" \
            "$(sha256sum "$WSBENCH" | cut -c1-12)"
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
        case "$variant" in ours | ours-kernel) ;;
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
ceiling | saturation | latency | carrying | hotpaths | frontend | instructions | idle | soak | h2 | grpc | handshakes | h3 | h3latency | mixed | passthrough | websocket) ;;
*)
    sed -n '2,48p' "$0" >&2
    exit 2
    ;;
esac

# A machine that goes to sleep for want of a keyboard takes the measurement with it: sleep
# is held off until this script is gone.
if command -v systemd-inhibit >/dev/null; then
    sudo -n systemd-inhibit --what=sleep:idle --why="edgerush benchmark"         tail --pid=$$ -f /dev/null &
fi

mkdir -p "$OUT"
loopback_mtu=
restore_mtu() {
    if [ -n "$loopback_mtu" ]; then sudo -n ip link set lo mtu "$loopback_mtu"; fi
}
# The idle states as the run found them, put back however it ends; a policy is applied for
# the turns taken under it (each_variant), and modes that take no turns run under the first.
idle_saved=$run-idle-saved
python3 "$here/residency.py" save >"$idle_saved"
set_idle() { # policy
    local path value
    python3 "$here/residency.py" plan "$1" "$PROXY_CPUS" | while read -r path value; do
        [ "$(cat "$path")" = "$value" ] || echo "$value" | sudo -n tee "$path" >/dev/null
    done
}
restore_idle() {
    local path value
    while read -r path value; do
        [ "$(cat "$path")" = "$value" ] || echo "$value" | sudo -n tee "$path" >/dev/null
    done <"$idle_saved"
}
trap 'stop_proxy; stop_backend; restore_mtu; restore_idle' EXIT
trap 'exit 1' INT TERM HUP
for policy in $IDLE; do
    python3 "$here/residency.py" plan "$policy" "$PROXY_CPUS" >/dev/null
done
if [ "$H3" = 1 ] && [ "$LOOPBACK_MTU" != 0 ]; then
    loopback_mtu=$(cat /sys/class/net/lo/mtu)
    sudo -n ip link set lo mtu "$LOOPBACK_MTU"
fi
environment
set_idle "${IDLE%% *}"
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
    # requests that were served. HTTP/1 and HTTP/2 at saturation, over TLS with TLS=1, and
    # HTTP/3 (4 connections × 100 streams) as well with H3=1.
    loads="h1 h2"
    [ "$H3" = 1 ] && loads="h1 h2 h3"
    for variant in $VARIANTS; do
        start_proxy "$variant"
        for load in $loads; do
            sudo -n perf record -q -e instructions:u -F 3999 -p "$proxy_pid" \
                -o "$OUT/$variant.$load.perf.data" -- sleep "$DURATION" &
            recorder=$!
            sudo -n perf stat -e instructions:u,instructions:k,cycles -x, -p "$proxy_pid" \
                -o "$OUT/$variant.$load.stat" -- sleep "$DURATION" &
            counter=$!
            case "$load" in
            h1) saturation_h1 "$variant.instructions-h1" "$proxy" --connect-to="$proxy_at" ;;
            h2) saturation_h2 "$variant.instructions-h2" "$proxy" --connect-to="$proxy_at" ;;
            h3) h3load "$variant.instructions-h3" -t2 -c4 -m100 ;;
            esac
            wait "$recorder" "$counter" 2>/dev/null || true
            sudo -n chown "$(id -u)" "$OUT/$variant.$load.perf.data" 2>/dev/null || true
            perf report -i "$OUT/$variant.$load.perf.data" --stdio --no-children -s sym \
                --percent-limit 0 2>/dev/null | python3 "$here/buckets.py" \
                >"$OUT/$variant.$load.parts" || true
            echo "== $variant $load =="
            cat "$OUT/$variant.$load.parts"
            sleep 2
        done
        stop_proxy
        sleep 5
    done
    exit
    ;;
profile-body)
    sha256sum "$edgerush" >"$OUT/binary.sha256"
    git -C "$repo" diff --binary HEAD >"$OUT/source.patch"
    each_variant profile_body_runs
    ;;
frontend)
    # What serving costs by itself against NGINX, and what the upstream adds: every
    # variant at saturation for the benchmark's host (nginx-direct answers it itself;
    # EdgeRush forwards it) and for a host no route is for (answered by each itself:
    # EdgeRush's 404 is its server and the request core). Counted over every process of
    # the variant, so NGINX's workers are included.
    frontend_runs() {
        local host_as
        for host_as in "bench.example.com:served" "nowhere.example.com:unrouted"; do
            local url="http://${host_as%%:*}:8080/" as=${host_as##*:}
            local pids
            pids=$(echo "$proxy_pid" $(pgrep -P "$proxy_pid") | tr ' ' ',')
            sudo -n perf stat -x, -p "$pids" -o "$OUT/$1.$as-h1.stat" \
                -e instructions:u,instructions:k,cycles -- sleep "$((DURATION - 2))" &
            local counter=$!
            saturation_h1 "$1.$as-h1" "$url" --connect-to="$proxy_at"
            wait "$counter" || true
            pids=$(echo "$proxy_pid" $(pgrep -P "$proxy_pid") | tr ' ' ',')
            sudo -n perf stat -x, -p "$pids" -o "$OUT/$1.$as-h2.stat" \
                -e instructions:u,instructions:k,cycles -- sleep "$((DURATION - 2))" &
            counter=$!
            saturation_h2 "$1.$as-h2" "$url" --connect-to="$proxy_at"
            wait "$counter" || true
        done
    }
    each_variant frontend_runs
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
idle)
    # What idle connections cost, each kind at each count on a proxy started afresh: never
    # written to, after one request, and after one head of most of the 64 KiB a head may
    # be (14 section 9's measurement checkpoint).
    idle_runs() {
        for kind in silent one-request large-head; do
            for count in $IDLE_COUNTS; do
                idle_memory "$1.idle-$kind-$count" "$kind" "$count"
            done
        done
        # And held beside a load, which is where they are in a real deployment.
        for count in $IDLE_COUNTS; do
            busy_idle_memory "$1.idle-busy-$count" "$count"
        done
    }
    each_variant idle_runs
    ;;
grpc)
    # Unary gRPC calls over HTTP/2, a message each way and a status in the trailers:
    # few connections with many calls on each, and one hot connection (15 §8). Best with
    # UPSTREAM_H2=1, which is how a gRPC backend is spoken to.
    grpc_call=(-d "$run/grpc.bin" -H "content-type: application/grpc" -H "te: trailers")
    # NGINX in it too, by its gRPC proxy, which speaks HTTP/2 to the backend.
    grpc_pass=1
    grpc_runs() {
        saturation_h2 "$1.saturation-grpc" "${proxy}bench.Echo/Call" --connect-to="$proxy_at" \
            "${grpc_call[@]}"
        hot_h2 "$1.hot-grpc" "${proxy}bench.Echo/Call" --connect-to="$proxy_at" "${grpc_call[@]}"
    }
    each_variant grpc_runs
    ;;
handshakes)
    # A flood of new TLS connections, a full handshake each, beside steady keep-alive
    # clients at RATE: what the steady clients' latency comes to, by variant — above all
    # by how many connections a worker accepts at a time (ours-abN; 03 §3). The flood has CPUs
    # of its own, FLOOD_CPUS, so that it does not slow the steady generator down.
    [ "$TLS" = 1 ] || { echo "handshakes is a TLS scenario: TLS=1" >&2; exit 2; }
    steady_rate=${2:-10000} flood_connections=${3:-256}
    : "${FLOOD_CPUS:=4,5}"
    handshake_runs() {
        taskset -c "$FLOOD_CPUS" oha -z "$((DURATION + 6))s" -c "$flood_connections" \
            --disable-keepalive --insecure --no-tui --output-format json \
            --connect-to "$host:8080:$proxy_at" "$proxy" >"$OUT/$1.flood.out" 2>&1 &
        local flood=$!
        sleep 3
        latency_h1 "$1.steady-h1" "$steady_rate" "$proxy"
        wait "$flood" || true
    }
    each_variant handshake_runs
    ;;
soak)
    # EdgeRush under a steady mixed load for a long while (14 §9, step 5): requests at a
    # fixed rate, idle connections held, uploads trickling, and the config taken over
    # every half minute. Every ten seconds what the process holds is written down; the
    # summary compares the first minutes with the last, and flat is the pass.
    minutes=${2:-30} rate=${3:-25000}
    METRICS=127.0.0.1:9090
    start_proxy ours
    taskset -c "$GEN_CPUS" python3 "$here/idle.py" "$proxy_at" "$host" \
        "$IDLE_CONNECTIONS" refreshed >"$OUT/soak.idle-ready" 2>"$OUT/soak.idle-err" &
    holding=$!
    # -w: requests under way when the time is up are waited for, not counted as errors.
    taskset -c "$GEN_CPUS" oha -z "${minutes}m" -w -q "$rate" -c 256 --no-tui \
        --output-format json --connect-to "$host:8080:$proxy_at" "$proxy" \
        >"$OUT/soak.load.json" 2>"$OUT/soak.load.err" &
    load=$!
    taskset -c "$GEN_CPUS" oha -z "${minutes}m" -w -q 5 -c 4 --no-tui --output-format json \
        --connect-to "$host:8080:$proxy_at" -m POST -D "$run/big.bin" "$proxy/sink" \
        >"$OUT/soak.uploads.json" 2>"$OUT/soak.uploads.err" &
    uploads=$!
    (
        turn=0
        while sleep 30; do
            turn=$((turn + 1))
            sed "s/x-served-by, value: edgerush/x-served-by, value: edgerush-$turn/" \
                "$here/proxy.yaml" >"$config.next"
            mv "$config.next" "$config"
        done
    ) &
    rewriting=$!
    echo "seconds,rss_kb,fds,client_sockets,storage_bytes,exchanges,idle_upstream" >"$OUT/soak.csv"
    began=$(date +%s)
    while kill -0 "$load" 2>/dev/null; do
        scrape=$(curl -s "http://$METRICS/metrics" || true)
        gauge() { awk -v name="$1" '$1 == name { print $2 }' <<<"$scrape"; }
        echo "$(( $(date +%s) - began )),$(python3 "$here/rss.py" "$proxy_pid")," \
            "$(ls "/proc/$proxy_pid/fd" | wc -l),$(ss -Htn state established \
            "( sport = :${proxy_at##*:} )" | wc -l),$(gauge edgerush_worker_storage_bytes)," \
            "$(gauge edgerush_upstream_exchanges_active),$(gauge edgerush_upstream_connections_idle)" \
            | tr -d ' ' >>"$OUT/soak.csv"
        sleep 10
    done
    kill "$rewriting" "$uploads" "$holding" 2>/dev/null || true
    wait "$load" "$rewriting" "$uploads" "$holding" 2>/dev/null || true
    cp "$here/proxy.yaml" "$config"
    stop_proxy
    python3 "$here/soak.py" "$OUT" | tee "$OUT/soak.summary"
    exit 0
    ;;
h2)
    # What HTTP/2 clients get, by themselves: the comparison a replacement of the HTTP/2
    # server is measured by (15 §7, steps 0 and 3). Few hot connections and a single one at
    # saturation, latency at RATE, 8 MiB bodies each way over a few connections' streams at
    # STREAMED a second, and idle connections after one request at IDLE_COUNTS.
    h2_rate=${2:-25000} streamed_rate=${3:-20}
    h2_runs() {
        saturation_h2 "$1.saturation-h2" "$proxy" --connect-to="$proxy_at"
        hot_h2 "$1.hot-h2" "$proxy" --connect-to="$proxy_at"
        latency_h2 "$1.latency-h2" "$h2_rate" "$proxy"
        oha_at "$1.streamed-answer-h2" "$streamed_rate" "$proxy/big" --http2 -c 4 -p 8
        oha_at "$1.streamed-request-h2" "$streamed_rate" "$proxy/sink" --http2 -c 4 -p 8 \
            -m POST -D "$run/big.bin"
        local count
        for count in $IDLE_COUNTS; do
            idle_memory "$1.idle-h2-$count" h2 "$count"
        done
    }
    each_variant h2_runs
    ;;
passthrough)
    # What TCP and TLS passthrough cost (17 step 4). Clients speak HTTP to the backend
    # through the tunnels, so every byte of it is carried and none is read: over kept
    # connections at saturation (HTTP/1 through the tcp listener, HTTP/2 over TLS through
    # the tls one), with a connection made for every request at CHURN a second (the
    # accept, the ClientHello read and the connection to the backend each time), and 8 MiB
    # answers at STREAMED a second.
    # A TLS connection a request costs the client and the backend a handshake each, which
    # the laptop's generator and backend keep up with at about 1,400 a second: TLS_CHURN is
    # half of CHURN unless said.
    churn_rate=${2:?rate for churn} tls_churn_rate=${3:-$((${2:-0} / 2))} streamed_rate=${4:-20}
    named=https://$host:8443/
    named_proxy_at=127.0.0.1:8443
    if [ "$PROXY_PROTOCOL" = 1 ]; then
        # Clients go to the front hop, which goes on to the proxy with a header.
        proxy_at=127.0.0.1:7080
        named_proxy_at=127.0.0.1:7443
    fi
    named_at=(--connect-to "$host:8443:$named_proxy_at" --insecure)
    passthrough_runs() {
        saturation_h1 "$1.saturation-tcp" "$proxy" --connect-to="$proxy_at"
        saturation_h2 "$1.saturation-tls" "$named" --connect-to="$named_proxy_at"
        churn "$1.churn-tcp" "$churn_rate" "$proxy"
        churn "$1.churn-tls" "$tls_churn_rate" "$named" "${named_at[@]}"
        streamed_answer "$1.streamed-tcp" "$streamed_rate"
    }
    each_variant passthrough_runs
    ;;
websocket)
    # WebSocket over HTTP/1.1 upgrades (19 step 5), by bench/wsbench against its own echo:
    # 256 connections kept busy, a 64-byte message and its echo at a time on each (busy);
    # RATE messages a second spread over 256 connections, each timed from when it was due
    # (paced); 16 KiB messages over 16 connections (large); a connection for every message
    # at CHURN a second, timed from when it was due to its echo (churn: the handshake, the
    # message, and a Close each way after); and, without TLS, what open WebSockets cost held
    # idle, at IDLE_COUNTS.
    ws_rate=${2:-25000} churn_rate=${3:-1000}
    ws_url=ws://$proxy_at/ws
    [ "$TLS" = 1 ] && ws_url=wss://$proxy_at/ws
    wsload() { # name, command, options...
        local name=$1 command=$2
        shift 2
        measured "$name" "$WSBENCH" "$command" "$ws_url" --host "$host" --seconds "$DURATION" \
            --threads "$(cpus_in "$GEN_CPUS")" "$@"
    }
    websocket_runs() {
        wsload "$1.ws-busy" echo --connections 256
        wsload "$1.ws-paced" echo --connections 256 --rate "$ws_rate"
        wsload "$1.ws-large" echo --connections 16 --size 16384
        wsload "$1.ws-churn" churn --rate "$churn_rate"
        if [ "$TLS" != 1 ]; then
            local count
            for count in $IDLE_COUNTS; do
                idle_memory "$1.ws-idle-$count" websocket "$count"
            done
        fi
    }
    each_variant websocket_runs
    ;;
h3latency)
    # HTTP/3 latency at each RATE (5,000, 12,500 and 25,000 a second unless said), 4
    # connections of 100 streams, in h2load's 10 ms bursts (h3_at): whether a gap between
    # the proxies grows with the load, as queueing does, or stays at any rate, as a delay of
    # its own does (16 step 6).
    [ -x "$H2LOAD3" ] || { echo "no h2load with HTTP/3 at $H2LOAD3" >&2; exit 2; }
    rates="${*:2}"
    rates=${rates:-5000 12500 25000}
    h3latency_runs() {
        local rate
        for rate in $rates; do
            latency_h3 "$1.latency-h3-$rate" "$rate" "$proxy"
            # The same rate evenly, where the smooth h2load is: latency-h3s; and again with
            # curl's one client beside it, where that is: latency-h3c.
            if [ -x "$H2LOAD3_SMOOTH" ]; then
                H3GEN=$H2LOAD3_SMOOTH latency_h3 "$1.latency-h3s-$rate" "$rate" "$proxy"
                if [ -x "$CURL3" ]; then
                    probe_h3 "$1.latency-h3c-$rate" &
                    local probe=$!
                    H3GEN=$H2LOAD3_SMOOTH latency_h3 "$1.latency-h3c-$rate" "$rate" "$proxy"
                    wait "$probe" || true
                fi
            fi
        done
    }
    each_variant h3latency_runs
    ;;
mixed)
    # HTTP/1 and HTTP/2 over TLS and HTTP/3 against one proxy at once, as an edge that
    # browsers reach is served. Open loop at each RATE a second for each protocol (5,000 and
    # 10,000 unless said), each protocol's latency its own row; then closed loop, each as
    # fast as it goes, to see how the proxy shares itself among them. The three generators
    # share GEN_CPUS, so the open loop is h2load for all three — far lighter than oha, one
    # timer and one measure of latency for all — the smooth one where it is there, every
    # request's time logged. `<name>-all` is what the proxy spent on the three together.
    rates="${*:2}"
    rates=${rates:-5000 10000}
    mixed_gen=$H2LOAD3
    [ -x "$H2LOAD3_SMOOTH" ] && mixed_gen=$H2LOAD3_SMOOTH
    mixed_runs() {
        local rate name
        for rate in $rates; do
            name="$1.mixed-$rate"
            mixed_once "$name" \
                "$mixed_gen --h1 -D $DURATION --warm-up-time=3 -t1 -c64 -m1 --rps=$((rate / 64)) --log-file=$OUT/$name-h1.requests --connect-to=$proxy_at $proxy" \
                "$mixed_gen -D $DURATION --warm-up-time=3 -t1 -c4 -m100 --rps=$((rate / 4)) --log-file=$OUT/$name-h2.requests --connect-to=$proxy_at $proxy" \
                "$mixed_gen --alpn-list=h3 -D $DURATION --warm-up-time=3 -t1 -c4 -m100 --rps=$((rate / 4)) --log-file=$OUT/$name-h3.requests --connect-to=$proxy_at $proxy"
        done
        name="$1.mixed-closed"
        mixed_once "$name" \
            "h2load --h1 -c256 -m1 -t1 -D $DURATION --warm-up-time=3 --connect-to=$proxy_at $proxy" \
            "h2load -c4 -m100 -t1 -D $DURATION --warm-up-time=3 --connect-to=$proxy_at $proxy" \
            "$H2LOAD3 --alpn-list=h3 -c4 -m100 -t1 -D $DURATION --warm-up-time=3 --connect-to=$proxy_at $proxy"
    }
    each_variant mixed_runs
    ;;
h3)
    # What HTTP/3 clients get (16 step 6): few hot connections, many connections, a single
    # hot one, and 8 MiB answers over a few connections' streams; HTTP/2 over TLS beside
    # them, on the same proxy and port, to read them by. Then latency at RATE, and 8 MiB
    # uploads at STREAMED a second, as the h2 mode measures HTTP/2's, by h2load at a fixed
    # rate with each request's time logged.
    [ -x "$H2LOAD3" ] || { echo "no h2load with HTTP/3 at $H2LOAD3" >&2; exit 2; }
    h3_rate=${2:-25000} streamed_rate=${3:-20}
    h3_runs() {
        h3load "$1.saturation-h3" -t2 -c4 -m100
        h3load "$1.many-h3" -t2 -c256 -m1
        h3load "$1.hot-h3" -t1 -c1 -m256
        saturation_h2 "$1.saturation-h2tls" "$proxy" --connect-to="$proxy_at"
        measured "$1.streamed-answer-h3" "$H2LOAD3" --alpn-list=h3 -t2 -c4 -m8 \
            -D "$DURATION" --warm-up-time=3 --connect-to="$proxy_at" "$proxy/big"
        latency_h3 "$1.latency-h3" "$h3_rate" "$proxy"
        h3_at "$1.streamed-request-h3" "$streamed_rate" 4 8 "$proxy/sink" -d "$run/big.bin"
        # Last: it starts the proxy afresh.
        held_h3 "$1.held-h3-$H3_HELD" "$H3_HELD"
    }
    each_variant h3_runs
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
