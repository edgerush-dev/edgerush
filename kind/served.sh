#!/usr/bin/env bash
# What the control plane serves, kept across its replicas' restarts (docs/04 §2, #474): the
# replicas share what translation cannot read back from the objects, so that a restarted or
# upgraded control plane serves what it served before, a refused edit's last accepted
# version among it.
#
#   kind/served.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/served.sh 1.37   the same, the cluster left for a look
#
# The binary is built and installed as kind/operator.sh does it. ControlPlane `edge` runs
# two replicas; DataPlane `web` limits locally, and serves Ingress `shop`. Then:
#   - RateLimitPolicy `holder`, generous, holds the limit name `taken`; `shop`, made after
#     it, limits each client of the Ingress to 2 a minute: the data plane answers 429 once
#     a client is past it;
#   - `shop` edited to take the name `taken`, which `holder` holds, is refused, its last
#     accepted version applying: still 429;
#   - the controller rolled, a pod at a time, as an upgrade does: each new pod ready only
#     once it holds the leader's snapshot, the last old one's lease taken by a new one,
#     which takes the newest; the data plane's pods rolled after, their configs all from the
#     new replicas: still 429, the edit still refused, though no replica that saw `shop`'s
#     first version runs. Without the snapshot, the new replicas would apply no version of
#     `shop`, and every request would be answered;
#   - the leader's pod deleted, as a crash: the other replica leads from what it followed,
#     the pod made in its place follows it; the data plane's pods rolled again: still 429.
#
# Linux only (kind, docker); on Ubuntu 26.04 as the laptop is.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# agnhost's netexec, the backend: it answers /hostname with its pod's name.
AGNHOST="registry.k8s.io/e2e-test-images/agnhost:2.56@sha256:352a050380078cb2a1c246357a0dfa2fcf243ee416b92ff28b44a01d1b4b0294"

version=${1:?usage: kind/served.sh <Kubernetes version>}
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
# What the controllers last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    for pod in $(k -n edge get pods -l app.kubernetes.io/component=controller -o name 2>/dev/null); do
        echo "-- $pod's last lines:"
        k -n edge logs "$pod" --tail=30 2>&1 | sed 's/^/   /'
    done
    exit 1
}

# Waits up to $2 seconds for `$1` (a command line) to succeed.
until_true() {
    for _ in $(seq 1 $(($2 * 5))); do
        eval "$1" >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    return 1
}
# A condition of resource $1 (kind/name) in namespace $2: "<status> <reason>".
condition() {
    k -n "$2" get "$1" -o jsonpath="{.status.conditions[?(@.type==\"$3\")].status} {.status.conditions[?(@.type==\"$3\")].reason}" 2>/dev/null
}
# Waits up to $5 seconds for condition $3 of $1 in $2 to be "$4".
condition_is() {
    for _ in $(seq 1 $(($5 * 5))); do
        [ "$(condition "$1" "$2" "$3")" = "$4" ] && return 0
        sleep 0.2
    done
    return 1
}

echo "== building the binary and its image"
cargo build -q --release --locked -p edgerush
image=target/kind/image
rm -rf "$image" && mkdir -p "$image"
cp target/release/edgerush "$image/"
sed "s|BASE|$BASE|" kind/discovery/Dockerfile > "$image/Dockerfile"
docker build -q -t edgerush:dev "$image" >/dev/null
kind load docker-image edgerush:dev --name "$name" >/dev/null

echo "== installing"
install=crates/planes/install
k apply --server-side -f "$install/gateway.edgerush.dev_controlplanes.yaml" \
    -f "$install/gateway.edgerush.dev_dataplanes.yaml" \
    -f "$install/gateway.edgerush.dev_platformdefaults.yaml" \
    -f "$install/gateway.edgerush.dev_ratelimitpolicies.yaml" \
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev \
    crd/platformdefaults.gateway.edgerush.dev crd/ratelimitpolicies.gateway.edgerush.dev >/dev/null
k apply --server-side -f "$install/operator.yaml" >/dev/null
k -n edgerush-system rollout status deploy/edgerush-operator --timeout=120s >/dev/null ||
    fail "the operator never ran"

echo "== the backend, and a control plane of two replicas"
k create namespace edge >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: shop, namespace: edge }
spec:
  replicas: 1
  selector: { matchLabels: { app: shop } }
  template:
    metadata: { labels: { app: shop } }
    spec:
      containers:
        - name: shop
          image: $AGNHOST
          args: ["netexec", "--http-port=8080"]
          ports: [{ name: http, containerPort: 8080 }]
          readinessProbe: { httpGet: { path: /healthz, port: 8080 }, periodSeconds: 1 }
---
apiVersion: v1
kind: Service
metadata: { name: shop, namespace: edge }
spec:
  selector: { app: shop }
  ports: [{ name: http, port: 80, targetPort: http }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: shop, namespace: edge }
spec:
  ingressClassName: edge.web
  rules:
    - host: shop.example
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: shop, port: { name: http } } }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: edge, namespace: edge }
spec:
  replicas: 2
  webhook: { enabled: false }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: web, namespace: edge }
spec:
  controlPlaneRef: { name: edge }
  ingress: { httpPort: 80, httpsPort: 443 }
  rateLimiting: { mode: Local }
EOF
condition_is controlplane/edge edge Programmed "True Programmed" 180 ||
    fail "ControlPlane edge not programmed: $(condition controlplane/edge edge Programmed)"
k -n edge rollout status deploy/edgerush-controller-edge --timeout=180s >/dev/null ||
    fail "the controller's two replicas never were ready"
k -n edge rollout status deploy/shop --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush-proxy-web --timeout=180s >/dev/null ||
    fail "the data plane never ran"

port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n edge port-forward deploy/edgerush-proxy-web "$port:80" >/dev/null 2>&1 &
    forward_pid=$!
}
status() { curl -s -o /dev/null -w '%{http_code}' -H "host: shop.example" "http://127.0.0.1:$port/hostname"; }
# Whether the data plane is served at all, through a forward made afresh if need be.
served() {
    for _ in $(seq 1 12); do
        forward
        until_true '[ "$(status)" = 200 ] || [ "$(status)" = 429 ]' 10 && return 0
    done
    return 1
}
# Whether a client past its limit is answered 429: of eight requests in a row, one is.
limited() {
    for _ in $(seq 1 8); do
        [ "$(status)" = 429 ] && return 0
    done
    return 1
}
served || fail "the Ingress is not served through the data plane: $(status)"
pass "a control plane of two replicas serves the Ingress"

echo "== a limit, and an edit of it refused"
limit() {
    k apply -f - >/dev/null <<EOF
apiVersion: gateway.edgerush.dev/v1alpha1
kind: RateLimitPolicy
metadata: { name: $1, namespace: edge }
spec:
  targetRefs: [{ group: networking.k8s.io, kind: Ingress, name: shop }]
  limits: [{ name: $2, key: Client, model: { bucket: { rate: "$3", burst: $4 } } }]
EOF
}
limit holder taken 1000/s 1000
until_true 'k -n edge get ratelimitpolicy holder' 30
sleep 2
limit shop per-client 2/min 2
until_true 'limited' 60 || fail "the policy's limit never applies: $(status)"
pass "RateLimitPolicy shop limits each client to 2 a minute"
limit shop taken 2/min 2
until_true '[ "$(k -n edge get ratelimitpolicy shop -o jsonpath="{.status.ancestors[0].conditions[?(@.type==\"Accepted\")].status}")" = False ]' 60 ||
    fail "the edit taking a name RateLimitPolicy holder holds is not refused: $(k -n edge get ratelimitpolicy shop -o jsonpath='{.status}')"
limited || fail "the refused edit's last accepted version does not apply"
pass "shop's edit, taking the name holder holds, is refused, its last accepted version applying"

# The data plane's pods made again: each config they run is the new replicas'.
roll_data_plane() {
    k -n edge rollout restart deploy/edgerush-proxy-web >/dev/null
    k -n edge rollout status deploy/edgerush-proxy-web --timeout=180s >/dev/null ||
        fail "the data plane's pods never rolled"
    served || fail "the Ingress is not served once the data plane's pods rolled: $(status)"
}
controller_pods() {
    k -n edge get pods -l app.kubernetes.io/component=controller \
        --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'
}

echo "== the controller rolled, as an upgrade"
before=$(controller_pods)
k -n edge rollout restart deploy/edgerush-controller-edge >/dev/null
k -n edge rollout status deploy/edgerush-controller-edge --timeout=300s >/dev/null ||
    fail "the controller never rolled: a new replica never held a snapshot"
until_true '[ "$(k -n edge get deploy edgerush-controller-edge -o jsonpath="{.status.replicas}")" = 2 ]' 120 ||
    fail "the old replicas never went"
for pod in $before; do
    until_true "! k -n edge get pod $pod" 120 || fail "old replica $pod never went"
done
roll_data_plane
until_true 'limited' 30 ||
    fail "rolled, the control plane no longer applies shop's last accepted version: $(status)"
pass "the controller rolled, no replica of before running, shop's last accepted version still applies"

echo "== the leader's pod deleted, as a crash"
# The pod the Lease names its holder.
holder() {
    local held
    held=$(k -n edge get lease edgerush-controller-edge -o jsonpath='{.spec.holderIdentity}')
    echo "${held%%_*}"
}
# Whether the Lease names a replica that runs: one gone holds it until it expires, as the
# last old one did through the roll.
held_by_one_running() { [ -n "$(holder)" ] && [[ " $(controller_pods) " == *" $(holder) "* ]]; }
until_true held_by_one_running 60 || fail "the Lease never names a replica that runs: $(holder)"
leader=$(holder)
k -n edge delete pod "$leader" --wait=false >/dev/null
until_true "held_by_one_running && [ \"\$(holder)\" != $leader ]" 120 ||
    fail "no other replica took the lease from $leader: $(holder)"
k -n edge rollout status deploy/edgerush-controller-edge --timeout=180s >/dev/null ||
    fail "the replica made in $leader's place never held a snapshot"
roll_data_plane
until_true 'limited' 30 ||
    fail "the leader gone, the control plane no longer applies shop's last accepted version: $(status)"
pass "the leader's pod deleted, the others lead and follow on, shop's last accepted version still applies"
