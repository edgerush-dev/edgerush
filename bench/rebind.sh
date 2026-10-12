#!/usr/bin/env bash
# HTTP/3 datagrams that reach a worker other than their connection's owner, under NAT
# rebinding and connection churn, and what forwarding them costs (16 §3).
#
#   bench/rebind.sh [OUTDIR]
#
# The client runs in a namespace of its own behind a router namespace that masquerades it to
# random ports, as a NAT does (bench/window.sh's namespaces). Emptying the router's conntrack
# table moves every client to a new port at once: NAT rebinding at its worst. The proxy (4
# workers on 0,1,4,5) listens on 10.77.0.1:8443, the backend (NGINX) on 127.0.0.1:9000 on
# 3,7, the client (h2load with HTTP/3) on 2,6. Each phase is a fresh h2load run; the proxy runs
# throughout. Read per phase: requests, the proxy's CPU, datagrams the host received, and the
# proxy's forwarded and inbox_full counts. Settings: DUR (20 s a phase), WORKERS (4) on
# PROXY_CPUS, SHAPES ("multi single"), CHURN (1 runs it). Needs sudo, nftables, conntrack,
# nginx, an h2load and a curl built with HTTP/3, and a release build.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")
out=${1:-$here/results/rebind-$(date +%Y%m%d-%H%M%S)}
EDGERUSH=${EDGERUSH:-$repo/target/release/edgerush}
H2LOAD3=${H2LOAD3:-$HOME/tools/h2load3/bin/h2load}
DUR=${DUR:-20}
run=/tmp/edgerush-rebind
mkdir -p "$run/tmp" "$out"
host=bench.example.com
url=https://$host:8443/

in_rt() { sudo ip netns exec edgerush-rt "$@"; }
in_cli() { sudo ip netns exec edgerush-cli "$@"; }
proxy_pid=
cleanup() {
    [ -n "$proxy_pid" ] && kill "$proxy_pid" 2>/dev/null || true
    [ -f "$run/nginx.pid" ] && kill "$(cat "$run/nginx.pid")" 2>/dev/null || true
    sudo ip netns del edgerush-cli 2>/dev/null || true
    sudo ip netns del edgerush-rt 2>/dev/null || true
    sudo ip link del veth-er-h 2>/dev/null || true
}
trap cleanup EXIT
cleanup
pgrep -a "edgerush|nginx|haproxy|h2load|oha" && { echo "something is still running" >&2; exit 1; }

# client 10.77.1.2 — router (NAT) — host 10.77.0.1
sudo ip netns add edgerush-cli
sudo ip netns add edgerush-rt
sudo ip link add veth-er-h type veth peer name veth-rt-h
sudo ip link add veth-er-c type veth peer name veth-rt-c
sudo ip link set veth-rt-h netns edgerush-rt
sudo ip link set veth-rt-c netns edgerush-rt
sudo ip link set veth-er-c netns edgerush-cli
sudo ip addr add 10.77.0.1/24 dev veth-er-h
sudo ip link set veth-er-h up
sudo ip route replace 10.77.1.0/24 via 10.77.0.2
in_rt ip addr add 10.77.0.2/24 dev veth-rt-h
in_rt ip addr add 10.77.1.1/24 dev veth-rt-c
in_rt ip link set veth-rt-h up
in_rt ip link set veth-rt-c up
in_rt sysctl -q -w net.ipv4.ip_forward=1
in_cli ip addr add 10.77.1.2/24 dev veth-er-c
in_cli ip link set veth-er-c up
in_cli ip link set lo up
in_cli ip route add default via 10.77.1.1
in_rt nft -f - <<'NFT'
table ip nat {
    chain post {
        type nat hook postrouting priority 100;
        oifname "veth-rt-h" ip saddr 10.77.1.0/24 masquerade random
    }
}
NFT

# The certificate and the config: bench/proxy.yaml with an HTTPS listener serving HTTP/3.
tls=$run/tls
mkdir -p "$tls"
[ -s "$tls/cert.pem" ] || openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -days 30 -subj "/CN=$host" -addext "subjectAltName=DNS:$host" \
    -keyout "$tls/key.pem" -out "$tls/cert.pem" 2>/dev/null
python3 - "$here/proxy.yaml" "$run/proxy.yaml" "$tls" <<'PY'
import json, sys
source, target, tls = sys.argv[1:]
text = open(source).read()
plain = 'web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, '
assert plain in text
text = text.replace(plain, 'web: { address: "10.77.0.1:8443", protocol: https, proxy_protocol: off, '
                    'tls: { certificates: [web], without_sni: web }, http3: {}, ')
chain, key = json.dumps(f"{tls}/cert.pem"), json.dumps(f"{tls}/key.pem")
text += f"certificates: {{ web: {{ chain_file: {chain}, key_file: {key} }} }}\n"
open(target, "w").write(text)
PY

taskset -c 3,7 nginx -p "$run/" -c "$here/nginx.conf" -e "$run/error.log"
ulimit -n 65536
taskset -c "${PROXY_CPUS:-0,1,4,5}" "$EDGERUSH" proxy --config "$run/proxy.yaml" --workers "${WORKERS:-4}" \
    --idle-per-destination 1024 --idle-total 1024 --metrics 127.0.0.1:9090 >>"$out/proxy.log" 2>&1 &
proxy_pid=$!
for _ in $(seq 100); do
    curl -s -o /dev/null http://127.0.0.1:9090/metrics && break
    sleep 0.1
done
in_cli "${CURL3:-$HOME/tools/curl3/bin/curl}" -sk --http3-only -o /dev/null -w "probe %{http_code} %{http_version}\n" \
    --connect-to "$host:8443:10.77.0.1:8443" "$url" | tee "$out/probe.txt"

{
    echo "edgerush $(git -C "$repo" rev-parse --short HEAD), $(sha256sum "$EDGERUSH" | cut -c1-16)"
    echo "h2load $("$H2LOAD3" --version | head -1)"
    uname -r
} >"$out/environment.txt"

counter() { # event
    curl -s http://127.0.0.1:9090/metrics | awk -v e="event=\"$1\"" \
        '$1 ~ /^edgerush_listener_quic_datagrams_total/ && index($1, e) { n += $NF } END { print n + 0 }'
}
udp_in() { awk '/^Udp:/ { if (seen) print $2; seen = 1 }' /proc/net/snmp; }
cpu_ticks() { awk '{ print $14 + $15 }' "/proc/$proxy_pid/stat"; }
hz=$(getconf CLK_TCK)

echo "phase,requests,seconds,req_per_s,cpu_us_per_req,udp_in,forwarded,forwarded_share,inbox_full,flushes" >"$out/phases.csv"
measure() { # phase, flushes-every (0: none, once: one flush at 2 s), h2load options...
    local phase=$1 every=$2
    shift 2
    local f0 i0 u0 c0 t0 flushes=0
    f0=$(counter forwarded) i0=$(counter inbox_full) u0=$(udp_in) c0=$(cpu_ticks) t0=$(date +%s.%N)
    in_cli taskset -c 2,6 "$H2LOAD3" --alpn-list=h3 "$@" \
        --connect-to=10.77.0.1:8443 "$url" >"$out/$phase.h2load" 2>&1 &
    local load=$!
    if [ "$every" = once ]; then
        sleep 1
        in_rt conntrack -F >/dev/null 2>&1
        flushes=1
    elif [ "$every" != 0 ]; then
        while kill -0 "$load" 2>/dev/null; do
            sleep "$every"
            in_rt conntrack -F >/dev/null 2>&1
            flushes=$((flushes + 1))
        done
    fi
    wait "$load" || true
    local f1 i1 u1 c1 t1
    f1=$(counter forwarded) i1=$(counter inbox_full) u1=$(udp_in) c1=$(cpu_ticks) t1=$(date +%s.%N)
    python3 - "$out/$phase.h2load" "$phase" "$t0" "$t1" "$c0" "$c1" "$hz" "$u0" "$u1" "$f0" "$f1" \
        "$i0" "$i1" "$flushes" >>"$out/phases.csv" <<'PY'
import re, sys
path, phase, t0, t1, c0, c1, hz, u0, u1, f0, f1, i0, i1, flushes = sys.argv[1:]
text = open(path).read()
done = re.findall(r"requests: \d+ total, \d+ started, (\d+) done, (\d+) succeeded", text)
succeeded = sum(int(s) for _, s in done)
seconds = float(t1) - float(t0)
cpu_us = (int(c1) - int(c0)) / int(hz) * 1e6
udp = int(u1) - int(u0)
fwd = int(f1) - int(f0)
print(f"{phase},{succeeded},{seconds:.1f},{succeeded / seconds:.0f},"
      f"{cpu_us / max(succeeded, 1):.1f},{udp},{fwd},{fwd / max(udp, 1):.4f},"
      f"{int(i1) - int(i0)},{flushes}")
PY
    tail -n 1 "$out/phases.csv"
    grep -E "requests:|status codes:" "$out/$phase.h2load" | head -2
}

# Two shapes: 16 connections of 32 streams, where a datagram carries many requests, and 256
# connections of one stream, where it carries about one: forwarding's cost a request at its
# largest. Three rounds, the phases in the same order in each.
for round in 1 2 3; do
    for shape in ${SHAPES:-multi single}; do
        case $shape in
            multi) load=(-t2 -c16 -m32 -D "$DUR") ;;
            single) load=(-t2 -c256 -m1 -D "$DUR") ;;
        esac
        measure "$shape-steady-$round" 0 "${load[@]}"
        measure "$shape-rebind-once-$round" once "${load[@]}"
        measure "$shape-rebind-5s-$round" 5 "${load[@]}"
        measure "$shape-rebind-1s-$round" 1 "${load[@]}"
    done
done

# Churn: connections made and closed back to back, ten requests on each.
if [ "${CHURN:-1}" = 1 ]; then
churn() {
    local end=$((SECONDS + DUR))
    while [ "$SECONDS" -lt "$end" ]; do
        in_cli taskset -c 2,6 "$H2LOAD3" --alpn-list=h3 -t2 -c32 -m1 -n320 \
            --connect-to=10.77.0.1:8443 "$url" 2>&1 | grep -E "requests:"
    done
}
f0=$(counter forwarded) u0=$(udp_in) c0=$(cpu_ticks) t0=$(date +%s.%N)
churn >"$out/churn.h2load"
f1=$(counter forwarded) u1=$(udp_in) c1=$(cpu_ticks) t1=$(date +%s.%N)
python3 - "$out/churn.h2load" "$t0" "$t1" "$c0" "$c1" "$hz" "$u0" "$u1" "$f0" "$f1" >>"$out/phases.csv" <<'PY'
import re, sys
path, t0, t1, c0, c1, hz, u0, u1, f0, f1 = sys.argv[1:]
succeeded = sum(int(s) for s in re.findall(r"(\d+) succeeded", open(path).read()))
seconds = float(t1) - float(t0)
cpu_us = (int(c1) - int(c0)) / int(hz) * 1e6
udp, fwd = int(u1) - int(u0), int(f1) - int(f0)
print(f"churn,{succeeded},{seconds:.1f},{succeeded / seconds:.0f},{cpu_us / max(succeeded, 1):.1f},"
      f"{udp},{fwd},{fwd / max(udp, 1):.4f},0,0")
PY
tail -n 1 "$out/phases.csv"
fi
curl -s http://127.0.0.1:9090/metrics | grep -E "^edgerush_listener_quic_datagrams_total" >"$out/quic-counters.txt"
echo "done: $out/phases.csv"
