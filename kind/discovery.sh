#!/usr/bin/env bash
# The data plane's EndpointSlice watch, end to end in a kind cluster (03 §6):
#
#   kind/discovery.sh 1.37       make the cluster, run every check, delete the cluster
#   KEEP=1 kind/discovery.sh 1.37   the same, the cluster left for a look
#
# The binary is built from the repository as it is checked out, a release build on this
# host, put alone in an image of this host's distribution (kind/discovery/Dockerfile),
# tagged edgerush:dev as install/operator.yaml names it, and loaded into the cluster: Linux
# only, on Ubuntu 26.04 as the laptop is. Everything runs through the operator (05): the
# install applied as a user would, then ControlPlane `edge` and DataPlane `web`, in `edge`.
# The control plane watches `shop`, labelled team=web, as PlatformDefaults allows it, where
# a backend Service sits behind an Ingress of class `edge.web`; the data plane's one
# permission, to list and watch EndpointSlices, is bound there and in `edge` alone (09 §1).
# It is reached by `kubectl port-forward`, on the Ingress's port. Then:
#   - the backend is served;
#   - scaled to three pods, every one of them is;
#   - scaled to none, the upstream answers 503, and scaled back, it is served again;
#   - with the operator stopped and the data plane's binding in `shop` deleted, a data
#     plane just started waits 15 s for its first list, says it serves without it, and the
#     403, and answers 503; the operator back puts the binding back, and it serves.
# Each check says PASS or FAIL; the first FAIL ends the run with status 1. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the image is: this host's distribution, the binary built on it.
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
# What the data plane and the operator last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    echo "-- the data plane's last lines:"
    k -n edge logs deploy/edgerush-proxy-web --tail=15 2>&1 | sed 's/^/   /'
    echo "-- the operator's last lines:"
    k -n edgerush-system logs deploy/edgerush-operator --tail=10 2>&1 | sed 's/^/   /'
    exit 1
}

echo "== building the binaries and their image"
cargo build -q --release --locked -p edgerush -p edgerush-ratelimit-service
image=target/kind/image
rm -rf "$image" && mkdir -p "$image"
cp target/release/edgerush target/release/edgerush-ratelimit-service "$image/"
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

echo "== deploying"
# The data plane's pods, by the labels the operator gives them.
PROXY=app.kubernetes.io/component=proxy,gateway.edgerush.dev/data-plane=web
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata: { name: shop, labels: { team: web } }
---
apiVersion: v1
kind: Namespace
metadata: { name: edge }
---
# The backend: agnhost's netexec, which answers /hostname with the name of its pod.
apiVersion: apps/v1
kind: Deployment
metadata: { name: web, namespace: shop }
spec:
  replicas: 1
  selector: { matchLabels: { app: web } }
  template:
    metadata: { labels: { app: web } }
    spec:
      containers:
        - name: web
          image: $AGNHOST
          args: ["netexec", "--http-port=8080"]
          ports: [{ name: http, containerPort: 8080 }]
          readinessProbe: { httpGet: { path: /healthz, port: 8080 }, periodSeconds: 1 }
---
apiVersion: v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  selector: { app: web }
  ports: [{ name: http, port: 80, targetPort: http }]
---
# Every host's requests, to the backend.
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: web, namespace: shop }
spec:
  ingressClassName: edge.web
  rules:
    - http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: web, port: { name: http } } }
---
# What the platform allows: ControlPlanes of edge may watch team web's namespaces.
apiVersion: gateway.edgerush.dev/v1alpha1
kind: PlatformDefaults
metadata: { name: default }
spec:
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: edge } } }
      namespaces: { from: Selector, selector: { matchLabels: { team: web } } }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: edge, namespace: edge }
spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { team: web } } } }
---
# Of Ingress alone, class edge.web; one pod, on a ClusterIP Service, as kind has no load
# balancer.
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: web, namespace: edge }
spec:
  controlPlaneRef: { name: edge }
  ingress: { httpPort: 80, httpsPort: 443 }
  service: { type: ClusterIP }
EOF
# Waits up to $3 seconds for condition $2 of $1 (kind/name) in edge to be True.
holds() {
    for _ in $(seq 1 $(($3 * 5))); do
        [ "$(k -n edge get "$1" -o jsonpath="{.status.conditions[?(@.type==\"$2\")].status}" 2>/dev/null)" = True ] && return 0
        sleep 0.2
    done
    return 1
}
holds controlplane/edge Accepted 60 || fail "ControlPlane edge was not accepted: $(k -n edge get controlplane edge -o jsonpath='{.status}')"
holds dataplane/web Accepted 60 || fail "DataPlane web was not accepted: $(k -n edge get dataplane web -o jsonpath='{.status}')"
for _ in $(seq 1 300); do
    k -n edge get deploy edgerush-controller-edge edgerush-proxy-web >/dev/null 2>&1 && break
    sleep 0.2
done
k -n shop rollout status deploy/web --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush-controller-edge --timeout=180s >/dev/null ||
    fail "the control plane never ran"
k -n edge rollout status deploy/edgerush-proxy-web --timeout=180s >/dev/null ||
    fail "the data plane never ran"

# The data plane on localhost:$port, through a port-forward started afresh after a restart,
# to the Ingress's port.
port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    k -n edge port-forward deploy/edgerush-proxy-web "$port:80" >/dev/null 2>&1 &
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
# The Ingress's port, bound once its first config runs.
for _ in $(seq 1 300); do
    k -n edge logs deploy/edgerush-proxy-web 2>/dev/null | grep 'listener "port-80" is on' >/dev/null && break
    sleep 0.2
done
forward

answers 200 30 || fail "the backend is not served: $(status)"
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
# The operator stopped first, as it would put the binding straight back.
k -n edgerush-system scale deploy/edgerush-operator --replicas=0 >/dev/null
for _ in $(seq 1 300); do
    [ -z "$(k -n edgerush-system get pod -l app.kubernetes.io/component=operator -o name)" ] && break
    sleep 0.2
done
[ -z "$(k -n edgerush-system get pod -l app.kubernetes.io/component=operator -o name)" ] ||
    fail "the operator never stopped"
k -n shop delete rolebinding edgerush-proxy-edge.web >/dev/null
k -n edge rollout restart deploy/edgerush-proxy-web >/dev/null
k -n edge rollout status deploy/edgerush-proxy-web --timeout=120s >/dev/null
# The pod before it drains for up to 30 s: until it is gone, a port-forward may land on it.
for _ in $(seq 1 300); do
    [ "$(k -n edge get pod -l "$PROXY" -o name | grep -c .)" = 1 ] && break
    sleep 0.2
done
pod=$(k -n edge get pod -l "$PROXY" -o jsonpath='{.items[0].metadata.name}')
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
k -n edgerush-system scale deploy/edgerush-operator --replicas=1 >/dev/null
k -n edgerush-system rollout status deploy/edgerush-operator --timeout=120s >/dev/null ||
    fail "the operator never ran again"
# Failures are waited out up to 30 s apart.
answers 200 60 || fail "given the permission again, the upstream answers $(status)"
k -n shop get rolebinding edgerush-proxy-edge.web >/dev/null 2>&1 ||
    fail "the operator never put the data plane's binding back"
pass "the operator back put the binding back, and it serves"
