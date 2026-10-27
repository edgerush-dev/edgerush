#!/usr/bin/env bash
# The data plane's EndpointSlice watch, end to end in a kind cluster (03 §6):
#
#   kind/discovery.sh 1.37       make the cluster, run every check, delete the cluster
#   KEEP=1 kind/discovery.sh 1.37   the same, the cluster left for a look
#
# The data plane is built from the repository as it is checked out, a release build on this
# host, and put alone in an image of this host's distribution (kind/discovery/Dockerfile),
# which is loaded into the cluster: Linux only, on Ubuntu 26.04 as the laptop is. A backend Service in
# `shop` is served through a data plane in `edge`, whose service account may only list and
# watch EndpointSlices in `shop`; it is reached by `kubectl port-forward`. Then:
#   - the backend is served;
#   - scaled to three pods, every one of them is;
#   - scaled to none, the upstream answers 503, and scaled back, it is served again;
#   - with no permission to list, a data plane just started waits 15 s for its first list,
#     says it serves without it, answers 503; given the permission, it serves.
# Each check says PASS or FAIL; the first FAIL ends the run with status 1. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the data plane's image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# agnhost's netexec, the backend: it answers /hostname with its pod's name.
AGNHOST="registry.k8s.io/e2e-test-images/agnhost:2.56@sha256:352a050380078cb2a1c246357a0dfa2fcf243ee416b92ff28b44a01d1b4b0294"

version=${1:?usage: kind/discovery.sh <Kubernetes version>}
name="edgerush-${version//./-}"
kubeconfig=$(kind/cluster.sh up "$version")
export KUBECONFIG="$kubeconfig"
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }
forward_pid=
finish() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    if [ "${KEEP:-}" != 1 ]; then kind/cluster.sh down "$version" >/dev/null; fi
}
trap finish EXIT

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "== building the data plane and its image"
cargo build -q --release --locked -p edgerush
image=target/kind/image
rm -rf "$image" && mkdir -p "$image"
cp target/release/edgerush "$image/"
sed "s|BASE|$BASE|" kind/discovery/Dockerfile > "$image/Dockerfile"
docker build -q -t edgerush:kind "$image" >/dev/null
kind load docker-image edgerush:kind --name "$name" >/dev/null

echo "== deploying"
sed "s|AGNHOST|$AGNHOST|" kind/discovery/manifests.yaml | k apply -f - >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush --timeout=120s >/dev/null

# The data plane on localhost:$port, through a port-forward started afresh after a restart.
port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    k -n edge port-forward deploy/edgerush "$port:8080" >/dev/null 2>&1 &
    forward_pid=$!
    for _ in $(seq 1 50); do
        curl -s -o /dev/null "http://127.0.0.1:$port/" && return 0
        sleep 0.2
    done
    fail "the data plane could not be reached through a port-forward"
}
status() { curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/hostname"; }
# Waits up to $2 seconds for `status` to be $1.
answers() {
    for _ in $(seq 1 $(($2 * 5))); do
        [ "$(status)" = "$1" ] && return 0
        sleep 0.2
    done
    return 1
}
forward

answers 200 10 || fail "the backend is not served: $(status)"
first=$(curl -s "http://127.0.0.1:$port/hostname")
pass "the backend is served, by $first"

k -n shop scale deploy/web --replicas=3 >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
seen=
for _ in $(seq 1 100); do
    seen=$(for _ in 1 2 3 4 5 6; do curl -s "http://127.0.0.1:$port/hostname"; echo; done | sort -u | grep -c .)
    [ "$seen" = 3 ] && break
    sleep 0.2
done
[ "$seen" = 3 ] || fail "scaled to three, $seen pods were served"
pass "scaled to three, every pod is served"

k -n shop scale deploy/web --replicas=0 >/dev/null
answers 503 30 || fail "scaled to none, the upstream answers $(status), not 503"
pass "scaled to none, 503"
k -n shop scale deploy/web --replicas=1 >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
answers 200 30 || fail "scaled back, the upstream answers $(status)"
pass "scaled back, served"

echo "== a data plane started without permission to list"
k -n shop delete rolebinding edgerush >/dev/null
k -n edge rollout restart deploy/edgerush >/dev/null
k -n edge rollout status deploy/edgerush --timeout=120s >/dev/null
# The pod before it drains for up to 30 s: until it is gone, a port-forward may land on it.
for _ in $(seq 1 300); do
    [ "$(k -n edge get pod -l app=edgerush -o name | grep -c .)" = 1 ] && break
    sleep 0.2
done
pod=$(k -n edge get pod -l app=edgerush -o jsonpath='{.items[0].metadata.name}')
said=
for _ in $(seq 1 150); do
    said=$(k -n edge logs "$pod" 2>/dev/null | grep -c "serving before the first EndpointSlices of shop" || true)
    [ "$said" -ge 1 ] && break
    sleep 0.2
done
[ "$said" -ge 1 ] || fail "it never said it serves before its first list"
# The 15 s it waited, from its first line to the one that says it serves anyway.
k -n edge logs --timestamps "$pod" > "target/kind/$name-edgerush.log"
started=$(head -1 "target/kind/$name-edgerush.log" | cut -d' ' -f1)
serving=$(grep "serving before the first EndpointSlices of shop" "target/kind/$name-edgerush.log" | cut -d' ' -f1)
waited=$(( $(date -d "$serving" +%s) - $(date -d "$started" +%s) ))
[ "$waited" -ge 14 ] && [ "$waited" -le 20 ] || fail "it waited ${waited} s for its first list, not about 15"
grep -q "EndpointSlices of namespace shop: .*403" "target/kind/$name-edgerush.log" ||
    fail "the refusal to list was not said"
forward
answers 503 10 || fail "without a list, the upstream answers $(status), not 503"
pass "without permission it waited ${waited} s, said so and the 403, and answers 503"
sed "s|AGNHOST|$AGNHOST|" kind/discovery/manifests.yaml | k apply -f - >/dev/null
# Failures are waited out up to 30 s apart.
answers 200 45 || fail "given the permission, the upstream answers $(status)"
pass "given the permission, served"
