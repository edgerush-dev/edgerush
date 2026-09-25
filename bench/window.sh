#!/usr/bin/env bash
# The downstream HTTP/2 stream window measured by uploads over a delayed path (15 §3).
#
#   bench/window.sh [RTTS] [VARIANTS]     e.g. bench/window.sh "20 100" "1048576 4194304 nginx"
#
# A variant is a stream window in bytes — EdgeRush built with it, the connection window
# left as it is — or `nginx` (bench/nginx-proxy.conf). Each is sent four 8 MiB uploads on
# one HTTP/2 connection, three times, and the time of each is printed: the first shows the
# connection's slow start, the rest what the window allows once TCP is warm.
#
# The client runs in a network namespace of its own behind a router namespace, which adds
# the delay on its own interfaces: delay on the sending host's own device holds packets its
# socket is still charged for, and TCP throttles it. The CPUs are held out of deep idle
# states while it runs, as waking from them cost a first upload more than a second on the
# laptop. Needs sudo, nginx, h2load, and a checkout that builds.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")
rtts=${1:-20 100}
variants=${2:-1048576 4194304 16777216 nginx}
run=/tmp/edgerush-window
out=$here/results/window-$(date +%Y%m%d-%H%M%S)
mkdir -p "$run/tmp" "$run/bin" "$out"
source_file=crates/proxy/src/downstream/h2/connection.rs

for variant in $variants; do
    [ "$variant" = nginx ] && continue
    (
        cd "$repo"
        # The source is put back however the build ends.
        trap 'git checkout -q "$source_file"' EXIT
        sed -i "s/stream_window: [0-9]* << 20,/stream_window: $variant,/" "$source_file"
        grep -q "stream_window: $variant," "$source_file"
        cargo build -q --release -p edgerush
        cp target/release/edgerush "$run/bin/edgerush-$variant"
        git checkout -q "$source_file"
    )
done
(cd "$repo" && git diff --quiet "$source_file")
sha256sum "$run"/bin/edgerush-* | tee "$out/binaries.txt"

in_rt() { sudo ip netns exec edgerush-rt "$@"; }
in_cli() { sudo ip netns exec edgerush-cli "$@"; }
proxy_pid= latency_pid=
cleanup() {
    [ -n "$proxy_pid" ] && kill "$proxy_pid" 2>/dev/null || true
    [ -n "$latency_pid" ] && sudo kill "$latency_pid" 2>/dev/null || true
    [ -f "$run/nginx.pid" ] && kill "$(cat "$run/nginx.pid")" 2>/dev/null || true
    sudo ip netns del edgerush-cli 2>/dev/null || true
    sudo ip netns del edgerush-rt 2>/dev/null || true
    sudo ip link del veth-er-h 2>/dev/null || true
}
trap cleanup EXIT
cleanup

# client 10.77.1.2 — router — host 10.77.0.1
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

# Held for as long as the file stays open.
sudo python3 -c '
import os, signal, struct
fd = os.open("/dev/cpu_dma_latency", os.O_WRONLY)
os.write(fd, struct.pack("i", 0))
signal.pause()' &
latency_pid=$!

head -c $((8 << 20)) /dev/zero >"$run/big.bin"
taskset -c 6,7 nginx -p "$run/" -c "$here/nginx.conf" -e "$run/error.log"
sed 's/127.0.0.1:8080/10.77.0.1:8080/' "$here/proxy.yaml" >"$run/proxy.yaml"

start() {
    if [ "$1" = nginx ]; then
        mkdir -p "$run-proxy/tmp"
        sed "s/WORKERS/2/; s/127.0.0.1:8080/10.77.0.1:8080/" "$here/nginx-proxy.conf" >"$run-proxy/nginx.conf"
        taskset -c 2,3 nginx -p "$run-proxy/" -c "$run-proxy/nginx.conf" -e "$run-proxy/error.log" \
            2>>"$out/proxy.log" &
    else
        taskset -c 2,3 "$run/bin/edgerush-$1" proxy --config "$run/proxy.yaml" --workers 2 \
            2>>"$out/proxy.log" &
    fi
    proxy_pid=$!
    for _ in $(seq 50); do
        curl -s -o /dev/null --noproxy '*' http://10.77.0.1:8080/ && return
        sleep 0.1
    done
    echo "variant $1 did not start" >&2
    exit 1
}
stop() {
    kill "$proxy_pid"
    wait "$proxy_pid" 2>/dev/null || true
    proxy_pid=
    sleep 0.5
}

for rtt in $rtts; do
    in_rt tc qdisc replace dev veth-rt-h root netem delay $((rtt / 2))ms limit 100000
    in_rt tc qdisc replace dev veth-rt-c root netem delay $((rtt / 2))ms limit 100000
    for variant in $variants; do
        start "$variant"
        for rep in 1 2 3; do
            sudo rm -f "$run/requests.log"
            in_cli taskset -c 4,5 h2load -n 4 -c 1 -m 1 --log-file="$run/requests.log" \
                -H ':authority: bench.example.com' -d "$run/big.bin" \
                http://10.77.0.1:8080/sink >"$out/rtt$rtt-$variant-$rep.txt" 2>&1
            sudo cat "$run/requests.log" | awk -v rtt="$rtt" -v variant="$variant" -v rep="$rep" '
                $2 != 200 { failed = 1 }
                { took[NR] = $3 / 1000 }
                END {
                    if (failed || NR != 4) { print "rtt=" rtt " " variant " rep=" rep ": failed"; exit }
                    printf "rtt=%s %-9s rep=%s first %5.0f ms, then %5.0f %5.0f %5.0f ms\n",
                        rtt, variant, rep, took[1], took[2], took[3], took[4]
                }' | tee -a "$out/summary.txt"
        done
        stop
    done
done
echo "results in $out"
