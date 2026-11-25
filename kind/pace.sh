#!/usr/bin/env bash
# Gateway-first at the conformance suite's pace, in a kind cluster (docs/05 §3, 10): what
# the suite does with Gateways, done before the suite does it.
#
#   kind/pace.sh 1.37                       make the cluster, run, delete the cluster
#   KEEP=1 GATEWAYS=20 kind/pace.sh 1.37    20 Gateways at once, the cluster left for a look
#
# The suite writes its own Gateways, of a class it is given, in a namespace of its own: four
# base Gateways, then some hundred over the run, at most some forty live at once (its
# parallel tests'), each deleted and waited on until gone. It waits 180 s for a Gateway to
# be Accepted and Programmed of its generation with an address, and for every Pod of its
# namespaces to be Ready; 60 s for a listener's conditions; 30 s for an object deleted,
# a namespace too, to be gone. Here, as there, the ControlPlane sits in a namespace of its
# own, watching every namespace, and the Gateways in another, `infra`:
#   - its class is accepted before any Gateway of it is written, as the suite waits for
#     first (docs/04 §6);
#   - GATEWAYS Gateways (50 unless said, above the suite's forty) made at once, each its
#     DataPlane generated, are each Accepted and Programmed of their generation, their
#     listeners too, with their Service's address, within 180 s, and every Pod of `infra`
#     Ready; the time the last took is said;
#   - a request from a Pod in another namespace, as the suite's runner is (docs/04 §7), to
#     a Gateway's status address reaches the backend of its route;
#   - a listener added to a live Gateway is Programmed of the new generation within 60 s,
#     listed on its Service and served at its status address; taken away, it is gone from
#     both;
#   - every Gateway deleted at once, each is gone within 30 s, and all made for them within
#     60 s, none stuck on the operator's finalizer;
#   - GATEWAYS Gateways made again and Programmed, their namespace deleted is gone within
#     30 s, as the suite's are at its end.
# Each check says PASS or FAIL; the first FAIL ends the run with status 1. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# agnhost's netexec, the backend: it answers /hostname with its pod's name.
AGNHOST="registry.k8s.io/e2e-test-images/agnhost:2.56@sha256:352a050380078cb2a1c246357a0dfa2fcf243ee416b92ff28b44a01d1b4b0294"

version=${1:?usage: kind/pace.sh <Kubernetes version>}
gateways=${GATEWAYS:-50}
name="edgerush-${version//./-}"
kubeconfig=$(kind/cluster.sh up "$version")
export KUBECONFIG="$kubeconfig"
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }
finish() {
    if [ "${KEEP:-}" != 1 ]; then kind/cluster.sh down "$version" >/dev/null; fi
}
trap finish EXIT

pass() { echo "PASS: $*"; }
# What the operator and the controller last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    echo "-- the operator's last lines:"
    k -n edgerush-system logs deploy/edgerush-operator --tail=20 2>&1 | sed 's/^/   /'
    echo "-- the controller's last lines:"
    k -n pace logs deploy/edgerush-controller-pace --tail=20 2>&1 | sed 's/^/   /'
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
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }

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
k create -f crates/gateway-api/crds/ >/dev/null
k apply --server-side -f "$install/gateway.edgerush.dev_controlplanes.yaml" \
    -f "$install/gateway.edgerush.dev_dataplanes.yaml" \
    -f "$install/gateway.edgerush.dev_platformdefaults.yaml" \
    -f "$install/gateway.edgerush.dev_ratelimitpolicies.yaml" \
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev \
    crd/platformdefaults.gateway.edgerush.dev crd/ratelimitpolicies.gateway.edgerush.dev \
    crd/gateways.gateway.networking.k8s.io >/dev/null
k apply --server-side -f "$install/operator.yaml" >/dev/null
k -n edgerush-system rollout status deploy/edgerush-operator --timeout=120s >/dev/null ||
    fail "the operator never ran"

echo "== the ControlPlane, watching every namespace"
# Class pace's DataPlanes are ClusterIP Services, whose address is there from the start,
# as a load balancer's would be once given; ControlPlanes of namespace pace may watch
# every namespace.
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: PlatformDefaults
metadata: { name: default }
spec:
  gatewayClasses:
    - name: pace
      dataPlane: { service: { type: ClusterIP } }
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: pace } } }
      namespaces: { from: All }
EOF
k create namespace pace >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: pace, namespace: pace }
spec:
  watch: { namespaces: { from: All } }
  webhook: { enabled: false }
EOF
condition_is controlplane/pace pace Programmed "True Programmed" 180 ||
    fail "ControlPlane pace not programmed: $(condition controlplane/pace pace Programmed)"
condition_is gatewayclass/pace "" Accepted "True Accepted" 60 ||
    fail "GatewayClass pace not accepted: $(condition gatewayclass/pace "" Accepted)"
pass "ControlPlane pace, watching every namespace, is programmed and its class accepted"

# Namespace $1 with a backend, `shop`, as the suite's namespaces have theirs.
namespace() {
    k create namespace "$1" >/dev/null
    k apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: shop, namespace: $1 }
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
metadata: { name: shop, namespace: $1 }
spec:
  selector: { app: shop }
  ports: [{ name: http, port: 80, targetPort: http }]
EOF
}
# Gateway g<$2> of namespace $1, of class pace, made as the suite makes its own: alone.
gateway() {
    k create -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: g$2, namespace: $1 }
spec:
  gatewayClassName: pace
  listeners: [{ name: http, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }]
EOF
}
# GATEWAYS Gateways made in namespace $1 at once, each by a client of its own.
make_all() {
    local pids=() i
    for i in $(seq 1 "$gateways"); do
        gateway "$1" "$i" &
        pids+=($!)
    done
    for i in "${pids[@]}"; do wait "$i" || fail "a Gateway could not be made"; done
}
# The Gateways of namespace $1 the suite would take as ready: Accepted and Programmed of
# their generation, each listener too, and an address that is their Service's.
ready() {
    k -n "$1" get gateways,services -o json | jq -r '
        ([.items[] | select(.kind == "Service")
            | { key: .metadata.name, value: .spec.clusterIP }] | from_entries) as $ips
        | .items[] | select(.kind == "Gateway") | . as $g | .metadata.generation as $gen
        | def current($conditions):
            all(("Accepted", "Programmed"); . as $t | any($conditions[]?;
                .type == $t and .status == "True" and .observedGeneration == $gen));
          select(current($g.status.conditions)
            and ($g.status.listeners | length) == ($g.spec.listeners | length)
            and all($g.status.listeners[]; current(.conditions))
            and any($g.status.addresses[]?;
                .value == $ips["edgerush-proxy-" + $g.metadata.name + "-pace"]))
        | .metadata.name'
}
# Waits up to $2 seconds for all GATEWAYS Gateways of namespace $1 to be ready, and every
# Pod of it too; says how long it took.
all_ready() {
    local start count=0
    start=$(now)
    for _ in $(seq 1 "$2"); do
        count=$(ready "$1" | wc -l)
        [ "$count" -eq "$gateways" ] && break
        sleep 1
    done
    [ "$count" -eq "$gateways" ] ||
        fail "$count of $gateways Gateways ready after $2 s; not ready: $(comm -13 <(ready "$1" | sort) <(seq 1 "$gateways" | sed 's/^/g/' | sort) | head -5 | tr '\n' ' ')"
    k -n "$1" wait --for condition=Ready pods --all --timeout="$2s" >/dev/null ||
        fail "not every Pod of $1 is Ready: $(k -n "$1" get pods --no-headers | grep -v ' Running ' | head -5)"
    echo "   $gateways Gateways and every Pod of $1 ready in $(since "$start") s"
}

echo "== $gateways Gateways made at once"
namespace infra
make_all infra
all_ready infra 180
pass "$gateways Gateways made at once are each Accepted and Programmed with their Service's address, and every Pod Ready, within 180 s"

echo "== a request from a Pod to a Gateway's status address"
# The client, in a namespace other than the Gateways', as the suite's runner is: kind gives
# no load balancer, so a Gateway's status address is its Service's cluster IP, which only
# Pods reach (docs/04 §7).
k -n default run client --image="$AGNHOST" --restart=Never -- pause >/dev/null
k -n default wait --for condition=Ready pod/client --timeout=120s >/dev/null ||
    fail "the client Pod is not Ready: $(k -n default get pod client --no-headers 2>&1)"
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: shop, namespace: infra }
spec:
  parentRefs: [{ name: g1 }]
  rules: [{ backendRefs: [{ name: shop, port: 80 }] }]
EOF
address=$(k -n infra get gateway g1 -o jsonpath='{.status.addresses[0].value}')
[ -n "$address" ] || fail "Gateway g1's status says no address"
# What the backend answers to a request from the client to $1: the name of its pod.
fetch() { k -n default exec client -- curl -s --max-time 2 "http://$1/hostname" 2>/dev/null; }
until_true '[[ "$(fetch "$address:8080")" == shop-* ]]' 60 ||
    fail "a request from the client to Gateway g1's status address $address:8080 answers '$(fetch "$address:8080")', not the backend"
pass "a request from a Pod in another namespace to a Gateway's status address reaches its route's backend"

echo "== a listener added to a live Gateway"
k -n infra patch gateway g1 --type json -p \
    '[{"op":"add","path":"/spec/listeners/-","value":{"name":"extra","port":8082,"protocol":"HTTP"}}]' >/dev/null
start=$(now)
until_true '[ "$(ready infra | grep -cx g1)" = 1 ] && [ "$(k -n infra get gateway g1 -o jsonpath={.metadata.generation})" = 2 ]' 60 ||
    fail "Gateway g1's listener added is not Programmed of generation 2: $(k -n infra get gateway g1 -o jsonpath='{.status.listeners}')"
echo "   Programmed of generation 2 in $(since "$start") s"
until_true '[ -n "$(k -n infra get service edgerush-proxy-g1-pace -o jsonpath="{.spec.ports[?(@.port==8082)].port}")" ]' 60 ||
    fail "the listener added is not listed on Gateway g1's Service"
until_true '[[ "$(fetch "$address:8082")" == shop-* ]]' 60 ||
    fail "the listener added to Gateway g1 does not serve its route at $address:8082: '$(fetch "$address:8082")'"
pass "a listener added to a live Gateway is Programmed of the new generation, listed on its Service and served at its status address"
k -n infra patch gateway g1 --type json -p '[{"op":"remove","path":"/spec/listeners/1"}]' >/dev/null
until_true '[ "$(ready infra | grep -cx g1)" = 1 ] && [ "$(k -n infra get gateway g1 -o jsonpath={.metadata.generation})" = 3 ]' 60 ||
    fail "Gateway g1, its listener taken away, is not Programmed of generation 3: $(k -n infra get gateway g1 -o jsonpath='{.status.listeners}')"
until_true '[ -z "$(k -n infra get service edgerush-proxy-g1-pace -o jsonpath="{.spec.ports[?(@.port==8082)].port}")" ]' 60 ||
    fail "the listener taken away is still listed on Gateway g1's Service"
pass "a listener taken away from a live Gateway is gone from its status and its Service"
k -n infra delete httproute shop >/dev/null

echo "== every Gateway deleted at once"
start=$(now)
pids=()
for i in $(seq 1 "$gateways"); do
    k -n infra delete gateway "g$i" --timeout=30s >/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" || fail "a Gateway deleted is not gone within 30 s"; done
echo "   every Gateway gone in $(since "$start") s"
left() {
    {
        k -n infra get dataplanes -o name
        k -n infra get deployments,services,secrets,rolebindings,pods \
            -l gateway.edgerush.dev/control-plane=pace -o name
        k -n infra get deployments,services,secrets,rolebindings,pods \
            -l gateway.networking.k8s.io/gateway-class-name=pace -o name
    } 2>/dev/null | sort -u
}
until_true '[ -z "$(left)" ]' 60 ||
    fail "made for the Gateways deleted, still there after 60 s: $(left | head -5 | tr '\n' ' ')"
echo "   all made for them gone in $(since "$start") s"
pass "$gateways Gateways deleted at once are each gone within 30 s, and all made for them within 60 s"

echo "== $gateways Gateways, their namespace deleted"
make_all infra
all_ready infra 180
start=$(now)
k delete namespace infra --timeout=30s >/dev/null ||
    fail "namespace infra, with $gateways Gateways, is not gone within 30 s: $(k -n infra get dataplanes,pods --no-headers 2>&1 | head -5)"
echo "   namespace infra gone in $(since "$start") s"
pass "a namespace of $gateways Gateways deleted is gone within 30 s"
