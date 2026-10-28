#!/usr/bin/env bash
# The config stream, end to end in a kind cluster (02 §4, 03 §10): an Ingress applied is
# translated by the control plane and served by a data plane that takes its config from it.
#
#   kind/stream.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/stream.sh 1.37   the same, the cluster left for a look
#
# The control plane and the data plane are one binary, built from the repository as it is
# checked out, a release build on this host, put alone in an image of this host's
# distribution (kind/discovery/Dockerfile) and loaded into the cluster: Linux only, on
# Ubuntu 26.04 as the laptop is. The stream's authority and each end's certificate are made
# here with openssl, and handed to each as a Secret. A backend Service in `shop` sits behind
# an Ingress of class `edge`, for host shop.example; the control plane in `edge` follows
# `shop`, and the data plane in `edge` dials it at edgerush-control.edge.svc:9443 and is
# reached by `kubectl port-forward`. Then:
#   - the Ingress is served;
#   - an Ingress changed is served as it now says;
#   - with the control plane gone, the data plane serves on;
#   - a data plane started while it is gone waits for its first config, serving nothing,
#     and serves once the control plane is back.
# Each check says PASS or FAIL; the first FAIL ends the run with status 1. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# agnhost's netexec, the backend: it answers /hostname with its pod's name.
AGNHOST="registry.k8s.io/e2e-test-images/agnhost:2.56@sha256:352a050380078cb2a1c246357a0dfa2fcf243ee416b92ff28b44a01d1b4b0294"

version=${1:?usage: kind/stream.sh <Kubernetes version>}
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
# What the data plane last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    echo "-- the data plane's last lines:"
    k -n edge logs deploy/edgerush --tail=15 2>&1 | sed 's/^/   /'
    exit 1
}

echo "== building the binary and its image"
cargo build -q --release --locked -p edgerush
image=target/kind/image
rm -rf "$image" && mkdir -p "$image"
cp target/release/edgerush "$image/"
sed "s|BASE|$BASE|" kind/discovery/Dockerfile > "$image/Dockerfile"
docker build -q -t edgerush:kind "$image" >/dev/null
kind load docker-image edgerush:kind --name "$name" >/dev/null

echo "== the stream's certificates"
pki=target/kind/stream-pki
rm -rf "$pki" && mkdir -p "$pki"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
    -subj "/CN=edgerush stream" -keyout "$pki/ca.key" -out "$pki/ca.crt" 2>/dev/null
# A certificate of the authority's for $1, named $2.
issue() {
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$2" \
        -keyout "$pki/$1.key" -out "$pki/$1.csr" 2>/dev/null
    openssl x509 -req -in "$pki/$1.csr" -CA "$pki/ca.crt" -CAkey "$pki/ca.key" -days 1 \
        -extfile <(printf 'subjectAltName=DNS:%s' "$2") -out "$pki/$1.crt" 2>/dev/null
}
issue control edgerush-control.edge.svc
issue data-plane edgerush.edge

echo "== deploying"
k create namespace edge --dry-run=client -o yaml | k apply -f - >/dev/null
for end in control data-plane; do
    k -n edge create secret generic "edgerush-$end-stream" \
        --from-file=tls.crt="$pki/$end.crt" --from-file=tls.key="$pki/$end.key" \
        --from-file=ca.crt="$pki/ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
done
sed "s|AGNHOST|$AGNHOST|" kind/stream/manifests.yaml | k apply -f - >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush-control --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush --timeout=120s >/dev/null

# The data plane on localhost:$port, through a port-forward started afresh after a restart,
# on a port of its own each time, so that none waits for the last to let its port go.
port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n edge port-forward deploy/edgerush "$port:8080" >/dev/null 2>&1 &
    forward_pid=$!
    for _ in $(seq 1 50); do
        curl -s -o /dev/null "http://127.0.0.1:$port/" && return 0
        sleep 0.2
    done
    fail "the data plane could not be reached through a port-forward"
}
status() { curl -s -o /dev/null -w '%{http_code}' -H "host: $1" "http://127.0.0.1:$port/hostname"; }
# Waits up to $3 seconds for `status` of host $1 to be $2.
answers() {
    for _ in $(seq 1 $(($3 * 5))); do
        [ "$(status "$1")" = "$2" ] && return 0
        sleep 0.2
    done
    return 1
}
# The one data-plane pod there is, once there is one alone.
pod() {
    for _ in $(seq 1 300); do
        [ "$(k -n edge get pod -l app=edgerush -o name | grep -c .)" = 1 ] && break
        sleep 0.2
    done
    k -n edge get pod -l app=edgerush -o jsonpath='{.items[0].metadata.name}'
}
# Waits up to $3 seconds for the logs of deploy or pod $1 to have a line with $2 in it.
said() {
    for _ in $(seq 1 $(($3 * 5))); do
        k -n edge logs "$1" 2>/dev/null | grep -q -- "$2" && return 0
        sleep 0.2
    done
    return 1
}

said deploy/edgerush "config from the control plane runs" 60 ||
    fail "the data plane never ran a config: $(k -n edge logs deploy/edgerush | tail -5)"
forward
answers shop.example 200 30 || fail "the Ingress is not served: $(status shop.example)"
said deploy/edgerush-control "runs config" 10 ||
    fail "the control plane never heard the data plane run its config"
pass "the Ingress is served, by $(curl -s -H 'host: shop.example' "http://127.0.0.1:$port/hostname")"

k -n shop patch ingress web --type=json \
    -p='[{"op": "replace", "path": "/spec/rules/0/host", "value": "other.example"}]' >/dev/null
answers other.example 200 30 || fail "the Ingress changed is not served: $(status other.example)"
answers shop.example 404 10 || fail "the host taken away still answers $(status shop.example)"
pass "the Ingress changed is served as it now says"

echo "== the control plane gone"
k -n edge scale deploy/edgerush-control --replicas=0 >/dev/null
for _ in $(seq 1 300); do
    [ "$(k -n edge get pod -l app=edgerush-control -o name | grep -c .)" = 0 ] && break
    sleep 0.2
done
said deploy/edgerush "dialled again in" 30 || fail "the data plane never said its stream ended"
answers other.example 200 5 || fail "with the control plane gone, $(status other.example)"
pass "with the control plane gone, the data plane serves on"

k -n edge rollout restart deploy/edgerush >/dev/null
k -n edge rollout status deploy/edgerush --timeout=120s >/dev/null
pod=$(pod)
said "$pod" "waiting for the first config from the control plane" 30 ||
    fail "a data plane started without its control plane did not say it waits"
said "$pod" "dialled again in" 30 || fail "it never said why it cannot dial"
k -n edge logs "$pod" | grep -q " is on " &&
    fail "it bound a listener before its first config"
pass "a data plane started while it is gone waits, serving nothing"

k -n edge scale deploy/edgerush-control --replicas=1 >/dev/null
k -n edge rollout status deploy/edgerush-control --timeout=120s >/dev/null
# Its waits are drawn below a bound that doubles up to 30 s.
said "$pod" "config from the control plane runs" 45 ||
    fail "it never ran a config once the control plane was back"
forward
answers other.example 200 30 || fail "once the control plane is back, $(status other.example)"
pass "once the control plane is back, it serves"
