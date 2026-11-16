#!/usr/bin/env bash
# The operator, end to end in a kind cluster (docs/05): a ControlPlane and a DataPlane
# applied become a working gateway, and what the operator made is kept as it states it.
#
#   kind/operator.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/operator.sh 1.37   the same, the cluster left for a look
#
# The binary is built from the repository as it is checked out, a release build on this
# host, put alone in an image of this host's distribution (kind/discovery/Dockerfile),
# tagged edgerush:dev as install/operator.yaml names it, and loaded into the cluster: Linux
# only, on Ubuntu 26.04 as the laptop is. The install is applied as a user would: the
# CRDs, the four roles, the admission policy and the operator, and not Gateway API's CRDs,
# Ingress alone being a valid install. A backend Service in `edge` sits behind an Ingress
# of class `edge.web`, for host shop.example. Then:
#   - ControlPlane `edge` (its webhook off, judged below on a ControlPlane of its own) and DataPlane
#     `web`, with a Gateway, applied are both accepted, the ControlPlane programmed and the
#     DataPlane not, waiting on Gateway API's CRDs, and the Ingress is served through the
#     data plane, reached by `kubectl port-forward`, on certificates the control plane
#     signs: two resources make a working gateway, of Ingress alone;
#   - a ControlPlane deleted there, without Gateway API's CRDs, is gone;
#   - Gateway API's CRDs installed, the operator makes the GatewayClass and the Gateway,
#     and the DataPlane is programmed, without a restart;
#   - a GatewayClass naming EdgeRush's controller that no ControlPlane made, as the
#     conformance suite writes one, is refused, Accepted false of reason Unsupported, of its
#     generation, and again of the next once edited (docs/05 §3); one of another controller
#     is left as the API server defaulted it, and `edge`'s is its control plane's, accepted;
#     one of a ControlPlane's name made by hand before it is refused as well, the
#     ControlPlane saying it is not the operator's, and its control plane, running, neither
#     accepts the class nor touches the Gateway of it; the ControlPlane deleted leaves it;
#   - the control plane writes the data plane's Service (docs/05 §2), owned by DataPlane
#     `web`, a LoadBalancer that keeps the client's address: Ingress's two ports listed,
#     and the Gateway's listener's as it is bound, which follows the listener's port as
#     the DataPlane changes it; the address a load balancer gives it, written by hand as
#     kind has none, said by the Ingress's status and the Gateway's;
#   - the data plane's Deployment's image edited by hand is put back, and a label added by
#     hand is kept;
#   - a second DataPlane of `edge`, `api`, is accepted and programmed, its own Service and
#     address; each data plane serves its own Gateway's routes and not the other's, a route
#     attached to both Gateways is served by each and its status holds a parent of each;
#     `api`'s `ingress` taken away deletes its IngressClass, its Gateway kept and served;
#     `api` deleted takes what was made for it and its parent of that route, and `web`
#     serves on, the controller neither rolled nor restarted;
#   - a ControlPlane of `edge`'s name in another namespace is not accepted, its status
#     naming the one that holds it, and nothing is made for it; given the operator's
#     finalizer by hand and deleted, it deletes nothing of `edge`'s;
#   - the watch scope's allowance (docs/05 §4): a ControlPlane watching every namespace
#     not accepted, nothing made for it, until an allowance lets it, then bound over the
#     cluster; a Selector scope within a selector allowance bound in the namespaces it
#     selects alone, one beyond it not accepted and bound nowhere; the allowance narrowed
#     taking back the rights it granted, the rest kept;
#   - a Selector scope followed (docs/04 §1): ControlPlane `teams` serves the namespaces of
#     team shop through DataPlane `front`, its controller told the selector; a namespace
#     labelled in is bound and served, one labelled out dropped and its bindings gone, and
#     the controller is neither rolled nor restarted;
#   - the admission webhook (docs/05 §2, 04 §3): ControlPlane `hooked`, signing with an
#     authority the platform provides that is not there yet, has no registration and says
#     it waits for its bundle; the authority made with openssl, the bundle published in its
#     status, the webhook is registered trusting it and asked of its own namespace alone;
#     an Ingress of its class that would be left out is refused with why, one that
#     translates admitted, one of another class admitted however it is; the webhook turned
#     off, the registration and the listener gone, that Ingress admitted and judged by the
#     control plane alone; on again and the ControlPlane deleted, the registration gone;
#   - a ServiceAccount of a name the operator would use, made by hand, is left alone, and
#     its ControlPlane says so;
#   - `web` at a fixed 2, then autoscaled from 2, has an autoscaler that runs it at 2, and
#     its Deployment never states fewer on the way, its replicas handed over to the
#     autoscaler (docs/05 §2); its `max` taken away and `min` made 1 deletes the autoscaler,
#     and `web` runs 1, not scaled back;
#   - `web`'s `gateway` taken away deletes its Gateway, its IngressClass kept, and the
#     Ingress is still served through it;
#   - DataPlane `web` deleted takes with it everything made for it, its IngressClass and
#     bindings included, and, the last of `edge`'s, the Ingress of its class loses the
#     address it said (docs/04 §6); ControlPlane `edge` deleted, its GatewayClass and its
#     ClusterRoleBinding too.
# Each check says PASS or FAIL; the first FAIL ends the run with status 1. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# agnhost's netexec, the backend: it answers /hostname with its pod's name.
AGNHOST="registry.k8s.io/e2e-test-images/agnhost:2.56@sha256:352a050380078cb2a1c246357a0dfa2fcf243ee416b92ff28b44a01d1b4b0294"

version=${1:?usage: kind/operator.sh <Kubernetes version>}
name="edgerush-${version//./-}"
kubeconfig=$(kind/cluster.sh up "$version")
export KUBECONFIG="$kubeconfig"
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }
forward_pid=
watch_pid=
counts=$(mktemp)
finish() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    [ -n "$watch_pid" ] && kill "$watch_pid" 2>/dev/null || true
    rm -f "$counts"
    if [ "${KEEP:-}" != 1 ]; then kind/cluster.sh down "$version" >/dev/null; fi
}
trap finish EXIT

pass() { echo "PASS: $*"; }
# What the operator last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    echo "-- the operator's last lines:"
    k -n edgerush-system logs deploy/edgerush-operator --tail=20 2>&1 | sed 's/^/   /'
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
# Whether namespace $1 holds anything labelled for ControlPlane $2.
made_in() {
    [ -n "$(k -n "$1" get serviceaccounts,deployments,services,rolebindings \
        -l "gateway.edgerush.dev/control-plane=$2" -o name 2>/dev/null)" ]
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
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev \
    crd/platformdefaults.gateway.edgerush.dev >/dev/null
k apply --server-side -f "$install/operator.yaml" >/dev/null
k -n edgerush-system rollout status deploy/edgerush-operator --timeout=120s >/dev/null ||
    fail "the operator never ran"

echo "== the backend"
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
EOF

echo "== two resources, without Gateway API's CRDs"
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: edge, namespace: edge }
spec:
  webhook: { enabled: false }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: web, namespace: edge }
spec:
  controlPlaneRef: { name: edge }
  gateway:
    listeners: [{ name: http, port: 8080, protocol: HTTP }]
  ingress: { httpPort: 80, httpsPort: 443 }
EOF
condition_is controlplane/edge edge Accepted "True Accepted" 60 ||
    fail "ControlPlane edge not accepted: $(condition controlplane/edge edge Accepted)"
condition_is dataplane/web edge Accepted "True Accepted" 60 ||
    fail "DataPlane web not accepted: $(condition dataplane/web edge Accepted)"
condition_is controlplane/edge edge Programmed "True Programmed" 180 ||
    fail "ControlPlane edge not programmed: $(condition controlplane/edge edge Programmed)"
condition_is dataplane/web edge Programmed "False Pending" 60 ||
    fail "DataPlane web, its Gateway's CRD missing: $(condition dataplane/web edge Programmed)"
k -n edge get dataplane web -o jsonpath='{.status.conditions[?(@.type=="Programmed")].message}' |
    grep -F "Gateway edge/web is not made until" >/dev/null ||
    fail "DataPlane web does not say it waits on Gateway API's CRDs"
k -n edge rollout status deploy/edgerush-proxy-web --timeout=180s >/dev/null ||
    fail "the data plane never ran"
pass "without Gateway API's CRDs, the ControlPlane is programmed and the DataPlane says it waits on them"

port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n edge port-forward deploy/edgerush-proxy-web "$port:80" >/dev/null 2>&1 &
    forward_pid=$!
}
status() { curl -s -o /dev/null -w '%{http_code}' -H "host: $1" "http://127.0.0.1:$port/hostname"; }
k -n edge rollout status deploy/shop --timeout=120s >/dev/null
served=
for _ in $(seq 1 12); do
    forward
    if until_true '[ "$(status shop.example)" = 200 ]' 10; then
        served=yes
        break
    fi
done
[ -n "$served" ] || fail "the Ingress is not served through the data plane: $(status shop.example)"
pass "two resources make a working gateway of Ingress alone: the Ingress is served through the data plane"

k create namespace alone >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: alone, namespace: alone }
spec:
  webhook: { enabled: false }
EOF
until_true 'made_in alone alone' 60 || fail "nothing was made for ControlPlane alone"
k -n alone delete controlplane alone --wait=false >/dev/null
until_true '! k -n alone get controlplane alone' 60 ||
    fail "ControlPlane alone, deleted without Gateway API's CRDs, is never gone"
pass "a ControlPlane deleted without Gateway API's CRDs is gone"

echo "== Gateway API's CRDs installed"
# The operator's pod and its restarts: the same once the CRDs are taken in.
operator_pod() {
    k -n edgerush-system get pods -l app.kubernetes.io/component=operator \
        -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].restartCount}{" "}{end}'
}
before=$(operator_pod)
k create -f crates/gateway-api/crds/ >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/gatewayclasses.gateway.networking.k8s.io crd/gateways.gateway.networking.k8s.io >/dev/null
until_true 'k get gatewayclass edge && k -n edge get gateway web' 60 ||
    fail "the GatewayClass and the Gateway are not made once Gateway API's CRDs are installed"
condition_is dataplane/web edge Programmed "True Programmed" 60 ||
    fail "DataPlane web not programmed: $(condition dataplane/web edge Programmed)"
[ "$(operator_pod)" = "$before" ] ||
    fail "the operator was restarted: before '$before', now '$(operator_pod)'"
pass "Gateway API's CRDs installed: the GatewayClass and the Gateway made, the DataPlane programmed, no restart"

echo "== GatewayClasses no ControlPlane made"
# GatewayClass $1's Accepted: "<status> <reason> <observedGeneration>/<generation>".
class_accepted() {
    k get gatewayclass "$1" -o jsonpath='{.status.conditions[?(@.type=="Accepted")].status} {.status.conditions[?(@.type=="Accepted")].reason} {.status.conditions[?(@.type=="Accepted")].observedGeneration}/{.metadata.generation}' 2>/dev/null
}
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: outsider
spec:
  controllerName: edgerush.dev/gateway-controller
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: elsewhere
spec:
  controllerName: example.com/gateway-controller
EOF
until_true '[ "$(class_accepted outsider)" = "False Unsupported 1/1" ]' 60 ||
    fail "GatewayClass outsider, of no ControlPlane, says '$(class_accepted outsider)'"
k patch gatewayclass outsider --type=merge -p '{"spec":{"description":"Edited."}}' >/dev/null
until_true '[ "$(class_accepted outsider)" = "False Unsupported 2/2" ]' 60 ||
    fail "GatewayClass outsider, edited, says '$(class_accepted outsider)'"
[ "$(class_accepted elsewhere)" = "Unknown Pending /1" ] ||
    fail "GatewayClass elsewhere, another controller's, says '$(class_accepted elsewhere)'"
until_true '[ "$(class_accepted edge | cut -d" " -f1-2)" = "True Accepted" ]' 60 ||
    fail "GatewayClass edge, its ControlPlane's, says '$(class_accepted edge)'"
k delete gatewayclass outsider elsewhere >/dev/null
pass "a class of EdgeRush's controller that no ControlPlane made is refused, Unsupported, of each generation; another controller's left alone, edge's accepted"
# One of a ControlPlane's name, made by hand before it: refused too, and its control plane
# neither accepts it nor serves its Gateway, so that the two never write it in turn.
k create namespace squat >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: squat
spec:
  controllerName: edgerush.dev/gateway-controller
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: squat, namespace: squat }
spec:
  webhook: { enabled: false }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: front, namespace: squat }
spec:
  controlPlaneRef: { name: squat }
  gateway:
    listeners: [{ name: http, port: 8080, protocol: HTTP }]
EOF
condition_is controlplane/squat squat Programmed "False NotOurs" 60 ||
    fail "ControlPlane squat, its class made by hand: $(condition controlplane/squat squat Programmed)"
until_true '[ "$(class_accepted squat)" = "False Unsupported 1/1" ]' 60 ||
    fail "GatewayClass squat, made by hand, says '$(class_accepted squat)'"
k -n squat rollout status deploy/edgerush-controller-squat --timeout=120s >/dev/null ||
    fail "ControlPlane squat's controller never runs"
# Ten seconds of its controller running: the class stays refused, the Gateway untouched.
for _ in $(seq 1 50); do
    [ "$(class_accepted squat)" = "False Unsupported 1/1" ] ||
        fail "GatewayClass squat, with its ControlPlane's controller running, says '$(class_accepted squat)'"
    sleep 0.2
done
[ "$(condition gateway/front squat Accepted)" = "Unknown Pending" ] ||
    fail "Gateway squat/front, of a class not made for its ControlPlane, says '$(condition gateway/front squat Accepted)'"
k -n squat delete controlplane squat --wait=false >/dev/null
k -n squat delete dataplane front --wait=false >/dev/null
until_true '! k -n squat get controlplane squat' 90 ||
    fail "ControlPlane squat, deleted, is never gone"
k get gatewayclass squat >/dev/null ||
    fail "GatewayClass squat, made by hand, was deleted with ControlPlane squat"
k delete gatewayclass squat >/dev/null
k delete namespace squat --wait=false >/dev/null
pass "a class of a ControlPlane's name made by hand is refused, and its control plane takes it no more than the operator does"

echo "== the data plane's Service"
# The ports the data plane's Service lists, as `port/protocol`, in its order.
ports_of() {
    k -n edge get service edgerush-proxy-web \
        -o jsonpath='{range .spec.ports[*]}{.port}/{.protocol}{" "}{end}' 2>/dev/null
}
until_true '[ "$(ports_of)" = "80/TCP 443/TCP 8080/TCP " ]' 60 ||
    fail "the data plane's Service lists '$(ports_of)', not Ingress's two ports and the listener's"
shape=$(k -n edge get service edgerush-proxy-web -o jsonpath='{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].name} {.spec.type} {.spec.externalTrafficPolicy} {.spec.sessionAffinity}')
[ "$shape" = "DataPlane/web LoadBalancer Local None" ] ||
    fail "the data plane's Service is '$shape'"
pass "the control plane writes the data plane's Service, owned by its DataPlane, Ingress's two ports and the listener's listed"
k -n edge patch dataplane web --type=json \
    -p '[{"op":"replace","path":"/spec/gateway/listeners/0/port","value":8081}]' >/dev/null
until_true '[ "$(ports_of)" = "80/TCP 443/TCP 8081/TCP " ]' 60 ||
    fail "the listener's port changed, the data plane's Service lists '$(ports_of)'"
k -n edge patch dataplane web --type=json \
    -p '[{"op":"replace","path":"/spec/gateway/listeners/0/port","value":8080}]' >/dev/null
until_true '[ "$(ports_of)" = "80/TCP 443/TCP 8080/TCP " ]' 60 ||
    fail "the listener's port changed back, the data plane's Service lists '$(ports_of)'"
pass "the data plane's Service follows the listener's port as the DataPlane changes it"
# Kind has no load balancer: its address written as a cloud's controller would.
k -n edge patch service edgerush-proxy-web --subresource=status --type=merge \
    -p '{"status":{"loadBalancer":{"ingress":[{"ip":"192.0.2.10"}]}}}' >/dev/null
until_true '[ "$(k -n edge get ingress shop -o jsonpath="{.status.loadBalancer.ingress[0].ip}")" = 192.0.2.10 ]' 60 ||
    fail "the Ingress's status says '$(k -n edge get ingress shop -o jsonpath='{.status.loadBalancer.ingress}')', not the load balancer's address"
until_true '[ "$(k -n edge get gateway web -o jsonpath="{.status.addresses[0].value}")" = 192.0.2.10 ]' 60 ||
    fail "the Gateway's status says '$(k -n edge get gateway web -o jsonpath='{.status.addresses}')', not the load balancer's address"
pass "the Ingress's status and the Gateway's say the address the data plane's Service's load balancer gives"

echo "== kept as stated"
k -n edge label deploy/edgerush-proxy-web team=shop >/dev/null
k -n edge set image deploy/edgerush-proxy-web '*=edgerush:by-hand' >/dev/null
until_true '[ "$(k -n edge get deploy/edgerush-proxy-web -o jsonpath="{.spec.template.spec.containers[0].image}")" = edgerush:dev ]' 60 ||
    fail "the image edited by hand is not put back"
pass "a field the operator states, edited by hand, is put back"
[ "$(k -n edge get deploy/edgerush-proxy-web -o jsonpath='{.metadata.labels.team}')" = shop ] ||
    fail "a label added by hand is gone"
pass "a label added by hand is kept"

echo "== a second DataPlane"
# The controller's pods and the Deployment's generation: the same as DataPlanes come and go.
edge_controllers() {
    echo "$(k -n edge get deploy edgerush-controller-edge -o jsonpath='{.metadata.generation}')" \
        "$(k -n edge get pods -l gateway.edgerush.dev/control-plane=edge,app.kubernetes.io/component=controller \
            -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].restartCount}{" "}{end}')"
}
before=$(edge_controllers)
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: api, namespace: edge }
spec:
  controlPlaneRef: { name: edge }
  gateway:
    listeners: [{ name: http, port: 8080, protocol: HTTP }]
  ingress: { httpPort: 80, httpsPort: 443 }
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: web-only, namespace: edge }
spec:
  parentRefs: [{ name: web }]
  hostnames: [web.example]
  rules: [{ backendRefs: [{ name: shop, port: 80 }] }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: api-only, namespace: edge }
spec:
  parentRefs: [{ name: api }]
  hostnames: [api.example]
  rules: [{ backendRefs: [{ name: shop, port: 80 }] }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: both, namespace: edge }
spec:
  parentRefs: [{ name: web }, { name: api }]
  hostnames: [both.example]
  rules: [{ backendRefs: [{ name: shop, port: 80 }] }]
EOF
condition_is dataplane/api edge Accepted "True Accepted" 60 ||
    fail "a second DataPlane of edge not accepted: $(condition dataplane/api edge Accepted)"
k -n edge rollout status deploy/edgerush-proxy-api --timeout=180s >/dev/null ||
    fail "DataPlane api's data plane never ran"
condition_is dataplane/api edge Programmed "True Programmed" 120 ||
    fail "DataPlane api not programmed: $(condition dataplane/api edge Programmed)"
condition_is dataplane/web edge Programmed "True Programmed" 10 ||
    fail "DataPlane web, beside api: $(condition dataplane/web edge Programmed)"
until_true '[ "$(k -n edge get service edgerush-proxy-api -o jsonpath="{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].name}")" = DataPlane/api ]' 60 ||
    fail "DataPlane api has no Service of its own: $(k -n edge get service edgerush-proxy-api -o name 2>&1)"
k -n edge patch service edgerush-proxy-api --subresource=status --type=merge \
    -p '{"status":{"loadBalancer":{"ingress":[{"ip":"192.0.2.11"}]}}}' >/dev/null
until_true '[ "$(k -n edge get gateway api -o jsonpath="{.status.addresses[0].value}")" = 192.0.2.11 ]' 60 ||
    fail "Gateway api's status says '$(k -n edge get gateway api -o jsonpath='{.status.addresses}')', not its Service's address"
[ "$(k -n edge get gateway web -o jsonpath='{.status.addresses[0].value}')" = 192.0.2.10 ] ||
    fail "Gateway web's status says '$(k -n edge get gateway web -o jsonpath='{.status.addresses}')' beside api"
pass "a second DataPlane of one ControlPlane is accepted and programmed, its own Service and address"

# Port-forwards to port $2 of DataPlane $1's data plane.
plane() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n edge port-forward "deploy/edgerush-proxy-$1" "$port:$2" >/dev/null 2>&1 &
    forward_pid=$!
}
# Waits up to $5 seconds, a port-forward begun again every 10, for host $3 to answer $4
# through port $2 of DataPlane $1's data plane.
plane_answers() {
    for _ in $(seq 1 $(($5 / 10))); do
        plane "$1" "$2"
        until_true "[ \"\$(status $3)\" = $4 ]" 10 && return 0
    done
    return 1
}
plane_answers api 8080 api.example 200 120 || fail "api's route is not served by api: $(status api.example)"
[ "$(status both.example)" = 200 ] || fail "the route of both is not served by api: $(status both.example)"
[ "$(status web.example)" = 404 ] || fail "web's route is served by api: $(status web.example)"
plane_answers web 8080 web.example 200 120 || fail "web's route is not served by web: $(status web.example)"
[ "$(status both.example)" = 200 ] || fail "the route of both is not served by web: $(status both.example)"
[ "$(status api.example)" = 404 ] || fail "api's route is served by web: $(status api.example)"
# Each parent of the route of both, as `name=Accepted`, sorted.
parents_of_both() {
    k -n edge get httproute both \
        -o jsonpath='{range .status.parents[*]}{.parentRef.name}={.conditions[?(@.type=="Accepted")].status}{"\n"}{end}' |
        sort | tr '\n' ' '
}
until_true '[ "$(parents_of_both)" = "api=True web=True " ]' 60 ||
    fail "the route of both Gateways has parents '$(parents_of_both)'"
pass "each data plane serves its own Gateway's routes and not the other's, a route of both served by each, a parent of each in its status"

k -n edge patch dataplane api --type=merge -p '{"spec":{"ingress":null}}' >/dev/null
until_true '! k get ingressclass edge.api' 60 ||
    fail "DataPlane api's ingress taken away left its IngressClass: $(k get ingressclass edge.api -o name 2>&1)"
k -n edge get gateway api >/dev/null || fail "DataPlane api's ingress taken away took its Gateway too"
condition_is dataplane/api edge Programmed "True Programmed" 60 ||
    fail "DataPlane api, its ingress taken away: $(condition dataplane/api edge Programmed)"
plane_answers api 8080 api.example 200 60 || fail "api's route is not served once its ingress is taken away: $(status api.example)"
pass "a DataPlane's ingress taken away deletes its IngressClass, its Gateway kept and served"

k -n edge delete dataplane api --wait=false >/dev/null
until_true '[ -z "$(k -n edge get deploy,sa,pdb,rolebindings,gateways,services -l gateway.edgerush.dev/data-plane=api -o name 2>/dev/null)" ] && ! k -n edge get service edgerush-proxy-api && ! k get ingressclass edge.api' 90 ||
    fail "DataPlane api deleted left: $(k -n edge get deploy,sa,pdb,rolebindings,gateways,services -l gateway.edgerush.dev/data-plane=api -o name; k get ingressclass edge.api -o name 2>&1)"
until_true '! k -n edge get dataplane api' 30 || fail "DataPlane api is never gone"
plane_answers web 8080 web.example 200 60 || fail "web's route is not served once api is gone: $(status web.example)"
[ "$(status both.example)" = 200 ] || fail "the route of both is not served by web once api is gone: $(status both.example)"
until_true '[ "$(parents_of_both)" = "web=True " ]' 60 ||
    fail "the route of both, Gateway api gone, has parents '$(parents_of_both)'"
condition_is dataplane/web edge Programmed "True Programmed" 10 ||
    fail "DataPlane web, api gone: $(condition dataplane/web edge Programmed)"
[ "$(edge_controllers)" = "$before" ] ||
    fail "the controller was rolled or restarted: before '$before', now '$(edge_controllers)'"
pass "the second DataPlane deleted takes what was made for it, its Gateway's parent gone from the route, and the first serves on, the controller not restarted"
k -n edge delete httproute web-only api-only both >/dev/null

echo "== refused"
k create namespace late >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: edge, namespace: late }
spec:
  webhook: { enabled: false }
EOF
condition_is controlplane/edge late Accepted "False NameTaken" 60 ||
    fail "a name taken: $(condition controlplane/edge late Accepted)"
k -n late get controlplane edge -o jsonpath='{.status.conditions[?(@.type=="Accepted")].message}' |
    grep -F "edge/edge" >/dev/null || fail "a name taken does not say which holds it"
sleep 3
! made_in late edge || fail "something was made for a ControlPlane whose name is taken"
pass "a ControlPlane of a name taken is not accepted, says which holds it, and nothing is made for it"
# Given the operator's finalizer by hand and deleted, it deletes nothing of the holder's,
# whose objects carry the same names; and gone, it takes no name once the holder is
# deleted below.
k -n late patch controlplane edge --type=merge     -p '{"metadata":{"finalizers":["gateway.edgerush.dev/operator"]}}' >/dev/null
k -n late delete controlplane edge --wait=false >/dev/null
until_true '! k -n late get controlplane edge' 60 || fail "ControlPlane late/edge is never gone"
k get gatewayclass edge >/dev/null 2>&1 &&
    [ -n "$(k get clusterrolebindings -l gateway.edgerush.dev/control-plane=edge -o name)" ] ||
    fail "deleting a ControlPlane of the same name elsewhere deleted the holder's objects"
pass "a ControlPlane of the same name elsewhere, given the finalizer and deleted, deletes nothing of the holder's"

echo "== the watch scope's allowance"
# Whether a binding of ControlPlane $2 named $3 is in namespace $1 ("" for the cluster's).
# kubectl takes no label selector beside a name, so the label is read and compared.
bound() {
    local of
    if [ -z "$1" ]; then
        of=$(k get clusterrolebinding "$3" -o jsonpath='{.metadata.labels.gateway\.edgerush\.dev/control-plane}' 2>/dev/null)
    else
        of=$(k -n "$1" get rolebinding "$3" -o jsonpath='{.metadata.labels.gateway\.edgerush\.dev/control-plane}' 2>/dev/null)
    fi
    [ "$of" = "$2" ]
}
# PlatformDefaults `default`, letting ControlPlanes of namespace `wide` watch every
# namespace and those of `picked` the namespaces of team a; `$1` set to "narrowed", the
# first allowance taken away.
allowance() {
    {
        cat <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: PlatformDefaults
metadata: { name: default }
spec:
  watchScopes:
EOF
        [ "${1:-}" = narrowed ] || cat <<'EOF'
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: wide } } }
      namespaces: { from: All }
EOF
        cat <<'EOF'
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: picked } } }
      namespaces: { from: Selector, selector: { matchLabels: { team: a } } }
EOF
    } | k apply -f - >/dev/null
}

k create namespace wide >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: wide, namespace: wide }
spec:
  watch: { namespaces: { from: All } }
  webhook: { enabled: false }
EOF
condition_is controlplane/wide wide Accepted "False ScopeNotAllowed" 60 ||
    fail "a scope beyond Same with no allowance: $(condition controlplane/wide wide Accepted)"
sleep 3
! made_in wide wide || fail "something was made for a ControlPlane watching every namespace"
[ -z "$(k get clusterrolebindings -l gateway.edgerush.dev/control-plane=wide -o name)" ] ||
    fail "a ClusterRoleBinding was made for a ControlPlane watching every namespace"
pass "with no allowance, a ControlPlane watching every namespace is not accepted, and nothing is made for it"

k create namespace picked >/dev/null
k create namespace team-a >/dev/null
k create namespace team-b >/dev/null
k label namespace team-a team=a >/dev/null
k label namespace team-b team=b >/dev/null
allowance
condition_is controlplane/wide wide Accepted "True Accepted" 60 ||
    fail "a scope of All, allowed: $(condition controlplane/wide wide Accepted)"
until_true 'bound "" wide edgerush-controller-wide' 60 ||
    fail "ControlPlane wide, allowed All, is not bound over the cluster"
pass "an allowance of All lets a ControlPlane watch every namespace: accepted, bound over the cluster"

k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: picked, namespace: picked }
spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { team: a, tier: web } } } }
  webhook: { enabled: false }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: loose, namespace: picked }
spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { tier: web } } } }
  webhook: { enabled: false }
EOF
k label namespace team-a tier=web >/dev/null
k label namespace team-b tier=web >/dev/null
condition_is controlplane/picked picked Accepted "True Accepted" 60 ||
    fail "a Selector within the allowance: $(condition controlplane/picked picked Accepted)"
until_true 'bound team-a picked edgerush-controller-picked' 60 ||
    fail "ControlPlane picked is not bound in team-a, which it selects"
! bound team-b picked edgerush-controller-picked ||
    fail "ControlPlane picked is bound in team-b, which it does not select"
pass "a Selector scope within a selector allowance is accepted, bound in the namespaces it selects alone"
condition_is controlplane/loose picked Accepted "False ScopeNotAllowed" 60 ||
    fail "a Selector beyond the allowance: $(condition controlplane/loose picked Accepted)"
k -n picked get controlplane loose -o jsonpath='{.status.conditions[?(@.type=="Accepted")].message}' |
    grep -F "Selector of team=a" >/dev/null || fail "a Selector beyond the allowance does not say what is allowed"
sleep 3
! bound team-b loose edgerush-controller-loose && ! bound team-a loose edgerush-controller-loose ||
    fail "ControlPlane loose, beyond its allowance, was bound"
pass "a Selector scope beyond its allowance is not accepted, says what is allowed, and is bound nowhere"

allowance narrowed
condition_is controlplane/wide wide Accepted "False ScopeNotAllowed" 60 ||
    fail "the allowance narrowed: $(condition controlplane/wide wide Accepted)"
until_true '! bound "" wide edgerush-controller-wide' 60 ||
    fail "the allowance narrowed, ControlPlane wide is still bound over the cluster"
bound "" wide edgerush-controller-wide-cluster && k -n wide get deploy edgerush-controller-wide >/dev/null ||
    fail "the allowance narrowed took more than the rights it granted"
pass "an allowance narrowed takes back the rights it granted, and keeps the rest"
k delete platformdefaults default >/dev/null

echo "== a Selector scope followed"
# ControlPlane `teams` watches the namespaces of team shop, as an allowance lets it, and
# serves them through DataPlane `front`: shop-a is of team shop from the start; shop-b is
# labelled in later, shop-a out, each followed with no restart of the controller.
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: PlatformDefaults
metadata: { name: default }
spec:
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: teams } } }
      namespaces: { from: Selector, selector: { matchLabels: { team: shop } } }
EOF
# A backend in namespace $1 behind an Ingress of class teams.front for host $2.
team_backend() {
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
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: shop, namespace: $1 }
spec:
  ingressClassName: teams.front
  rules:
    - host: $2
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: shop, port: { name: http } } }
EOF
}
k create namespace teams >/dev/null
k create namespace shop-a >/dev/null
k create namespace shop-b >/dev/null
k label namespace shop-a team=shop >/dev/null
team_backend shop-a a.example
team_backend shop-b b.example
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: teams, namespace: teams }
spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { team: shop } } } }
  webhook: { enabled: false }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: front, namespace: teams }
spec:
  controlPlaneRef: { name: teams }
  ingress: { httpPort: 80, httpsPort: 443 }
EOF
condition_is controlplane/teams teams Programmed "True Programmed" 180 ||
    fail "ControlPlane teams not programmed: $(condition controlplane/teams teams Programmed)"
k -n teams rollout status deploy/edgerush-proxy-front --timeout=180s >/dev/null ||
    fail "DataPlane front's data plane never ran"
k -n shop-a rollout status deploy/shop --timeout=120s >/dev/null
k -n shop-b rollout status deploy/shop --timeout=120s >/dev/null
k -n teams get deploy edgerush-controller-teams -o jsonpath='{.spec.template.spec.containers[0].args}' |
    grep -F '"--namespace-selector","team=shop"' >/dev/null ||
    fail "the controller is not told its selector: $(k -n teams get deploy edgerush-controller-teams -o jsonpath='{.spec.template.spec.containers[0].args}')"
front() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n teams port-forward deploy/edgerush-proxy-front "$port:80" >/dev/null 2>&1 &
    forward_pid=$!
}
# Waits up to $3 seconds, a port-forward begun again every 10, for host $1 to answer $2.
answers() {
    for _ in $(seq 1 $(($3 / 10))); do
        front
        until_true "[ \"\$(status $1)\" = $2 ]" 10 && return 0
    done
    return 1
}
answers a.example 200 120 || fail "shop-a, of team shop, is not served: $(status a.example)"
[ "$(status b.example)" = 404 ] || fail "shop-b, not of team shop, is served: $(status b.example)"
pass "a Selector scope serves the namespaces it selects alone"

# The controller's pods and the Deployment's generation: the same as namespaces come and go.
controllers() {
    echo "$(k -n teams get deploy edgerush-controller-teams -o jsonpath='{.metadata.generation}')" \
        "$(k -n teams get pods -l gateway.edgerush.dev/control-plane=teams,app.kubernetes.io/component=controller \
            -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].restartCount}{" "}{end}')"
}
before=$(controllers)
k label namespace shop-b team=shop >/dev/null
until_true 'bound shop-b teams edgerush-controller-teams && bound shop-b teams edgerush-proxy-teams.front' 60 ||
    fail "shop-b, labelled in, is not bound"
answers b.example 200 120 || fail "shop-b, labelled in, is not served: $(status b.example)"
[ "$(status a.example)" = 200 ] || fail "shop-a stopped being served as shop-b came in: $(status a.example)"
[ "$(controllers)" = "$before" ] ||
    fail "the controller was rolled or restarted: before '$before', now '$(controllers)'"
pass "a namespace labelled into a Selector scope is bound and served, the controller not restarted"

k label namespace shop-a team- >/dev/null
answers a.example 404 120 || fail "shop-a, labelled out, is still served: $(status a.example)"
[ "$(status b.example)" = 200 ] || fail "shop-b stopped being served as shop-a left: $(status b.example)"
until_true '! bound shop-a teams edgerush-controller-teams && ! bound shop-a teams edgerush-proxy-teams.front' 60 ||
    fail "shop-a, labelled out, is still bound"
[ "$(controllers)" = "$before" ] ||
    fail "the controller was rolled or restarted: before '$before', now '$(controllers)'"
pass "a namespace labelled out of a Selector scope is dropped and its bindings gone, the controller not restarted"
k delete -n teams dataplane front --timeout=90s >/dev/null
k delete -n teams controlplane teams --timeout=90s >/dev/null
k delete platformdefaults default >/dev/null

echo "== the admission webhook"
# ControlPlane `hooked`, its webhook on as by default, signing with an authority the
# platform provides, not there yet: nothing is registered until the control plane can
# publish that authority's bundle.
registration() {
    k get validatingwebhookconfiguration edgerush-controller-hooked "$@" 2>/dev/null
}
k create namespace hooked >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: hooked, namespace: hooked }
spec:
  authority: { secretRef: { name: hooked-ca } }
---
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: front, namespace: hooked }
spec:
  controlPlaneRef: { name: hooked }
  ingress: { httpPort: 80, httpsPort: 443 }
EOF
# `rollout status` fails at once on a Deployment not made yet: waited for first.
until_true 'k -n hooked get deploy edgerush-controller-hooked' 60 ||
    fail "ControlPlane hooked's controller was never made"
k -n hooked rollout status deploy/edgerush-controller-hooked --timeout=180s >/dev/null ||
    fail "ControlPlane hooked's controller never ran"
until_true 'k -n hooked logs deploy/edgerush-controller-hooked | grep -F "Secret hooked/hooked-ca: the authority provided is not there"' 60 ||
    fail "the controller does not say it waits for the authority provided"
k -n hooked get controlplane hooked -o jsonpath='{.status.conditions[?(@.type=="Programmed")].message}' |
    grep -F "ValidatingWebhookConfiguration edgerush-controller-hooked is not made until the control plane publishes its authority's bundle" >/dev/null ||
    fail "ControlPlane hooked does not say its registration waits for its bundle: $(condition controlplane/hooked hooked Programmed)"
! registration >/dev/null || fail "the webhook was registered before its authority's bundle was published"
pass "the webhook is not registered while its authority is not there, and its ControlPlane says it waits"

# The platform's authority: an intermediate its root signed, with that root as the chain.
pki=target/kind/operator-pki
rm -rf "$pki" && mkdir -p "$pki"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
    -subj "/CN=platform root" -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$pki/root.key" -out "$pki/root.crt" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=hooked" \
    -keyout "$pki/hooked.key" -out "$pki/hooked.csr" 2>/dev/null
openssl x509 -req -in "$pki/hooked.csr" -CA "$pki/root.crt" -CAkey "$pki/root.key" -days 30 \
    -extfile <(printf 'basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n') \
    -out "$pki/hooked.crt" 2>/dev/null
k -n hooked create secret generic hooked-ca --type=kubernetes.io/tls \
    --from-file=tls.crt="$pki/hooked.crt" --from-file=tls.key="$pki/hooked.key" \
    --from-file=ca.crt="$pki/root.crt" >/dev/null
until_true 'registration' 90 ||
    fail "the webhook is not registered once its authority is there: $(condition controlplane/hooked hooked Programmed)"
published=$(k -n hooked get controlplane hooked -o jsonpath='{.status.authority.bundle}')
[ -n "$published" ] || fail "ControlPlane hooked's status publishes no bundle"
[ "$(registration -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | base64 -d)" = "$published" ] ||
    fail "the registration does not trust the bundle its ControlPlane's status publishes"
# Its workloads trust the authority provided, not the platform's root (05 §2).
[ "$(printf '%s\n' "$published" | openssl x509 -noout -subject)" = "$(openssl x509 -noout -subject -in "$pki/hooked.crt")" ] ||
    fail "the bundle published is not the authority provided: $(printf '%s\n' "$published" | openssl x509 -noout -subject)"
[ "$(registration -o jsonpath='{.webhooks[0].namespaceSelector.matchLabels.kubernetes\.io/metadata\.name}')" = hooked ] ||
    fail "the webhook is asked of more than its own namespace: $(registration -o jsonpath='{.webhooks[0].namespaceSelector}')"
condition_is controlplane/hooked hooked Programmed "True Programmed" 60 ||
    fail "ControlPlane hooked, registered: $(condition controlplane/hooked hooked Programmed)"
pass "registered once its authority's bundle is published, trusting that bundle, asked of its own scope"

# An Ingress of the data plane's class that, alone, does not translate.
bad_ingress() {
    k -n hooked apply "$@" -f - 2>&1 <<'EOF'
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: rewritten
  annotations: { nginx.ingress.kubernetes.io/rewrite-target: / }
spec:
  ingressClassName: hooked.front
  rules: [{ host: rewritten.example }]
EOF
}
# The API server takes up a registration within moments; until then it admits all.
refused=
for _ in $(seq 1 150); do
    if ! refused=$(bad_ingress --dry-run=server); then
        break
    fi
    refused=
    sleep 0.2
done
[ -n "$refused" ] || fail "an Ingress the data plane would leave out was never refused"
echo "$refused" | grep -q 'denied the request: not supported yet: annotations' ||
    fail "refused, but not for why: $refused"
pass "an Ingress of the class that would be left out is refused at admission, with why, through the operator's registration"
k -n hooked apply --dry-run=server -f - >/dev/null <<'EOF' ||
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: plain }
spec:
  ingressClassName: hooked.front
  rules:
    - host: plain.example
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: shop, port: { number: 80 } } }
EOF
    fail "an Ingress that translates was refused"
k -n hooked apply --dry-run=server -f - >/dev/null <<'EOF' ||
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: theirs
  annotations: { nginx.ingress.kubernetes.io/rewrite-target: / }
spec:
  ingressClassName: nginx
  rules: [{ host: theirs.example }]
EOF
    fail "an Ingress of another class was refused"
pass "an Ingress that translates is admitted, and one of another class, however it is"

# Turned off: no registration, no listener, and the Ingress judged in status alone.
k -n hooked patch controlplane hooked --type=merge -p '{"spec":{"webhook":{"enabled":false}}}' >/dev/null
until_true '! registration' 60 || fail "the webhook turned off is still registered"
k -n hooked get deploy edgerush-controller-hooked -o jsonpath='{.spec.template.spec.containers[0].args}' |
    grep -F -- --webhook-listen >/dev/null && fail "the webhook turned off is still listened for"
k -n hooked rollout status deploy/edgerush-controller-hooked --timeout=180s >/dev/null ||
    fail "ControlPlane hooked's controller, its webhook off, never ran"
admitted=$(bad_ingress) || fail "with the webhook off, an Ingress that would be left out is refused: $admitted"
until_true 'k -n hooked logs deploy/edgerush-controller-hooked | grep -F "hooked/rewritten"' 60 ||
    fail "with the webhook off, the control plane says nothing of the Ingress left out"
pass "the webhook turned off: no registration and no listener, and the Ingress admitted, judged by the control plane alone"
k -n hooked delete ingress rewritten >/dev/null

# On again, then the ControlPlane deleted: its registration goes with it.
k -n hooked patch controlplane hooked --type=merge -p '{"spec":{"webhook":{"enabled":true}}}' >/dev/null
until_true 'registration' 90 || fail "the webhook turned on again is not registered"
k -n hooked delete dataplane front --timeout=90s >/dev/null
k -n hooked delete controlplane hooked --wait=false >/dev/null
until_true '! registration && ! k -n hooked get controlplane hooked' 90 ||
    fail "ControlPlane hooked deleted left its registration"
pass "a ControlPlane deleted takes its webhook's registration"

echo "== not the operator's"
k create namespace own >/dev/null
k -n own create serviceaccount edgerush-controller-own >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: own, namespace: own }
spec:
  webhook: { enabled: false }
EOF
condition_is controlplane/own own Programmed "False NotOurs" 60 ||
    fail "an object not the operator's: $(condition controlplane/own own Programmed)"
[ -z "$(k -n own get serviceaccount edgerush-controller-own -o jsonpath='{.metadata.labels}')" ] ||
    fail "the ServiceAccount made by hand was taken over"
pass "an object of a name the operator would use, made by hand, is left alone, and said"

echo "== an autoscaler taking over the count"
deployment_replicas() { k -n edge get deploy edgerush-proxy-web -o jsonpath='{.spec.replicas}'; }
k -n edge patch dataplane web --type=merge -p '{"spec":{"replicas":{"min":2}}}' >/dev/null
until_true '[ "$(deployment_replicas)" = 2 ]' 60 ||
    fail "DataPlane web of 2 replicas runs $(deployment_replicas)"
# Every count the Deployment states from here on, each time it changes.
k -n edge get deploy edgerush-proxy-web --watch-only \
    -o jsonpath='{.spec.replicas}{"\n"}' >"$counts" 2>&1 &
watch_pid=$!
sleep 2
k -n edge patch dataplane web --type=merge -p '{"spec":{"replicas":{"min":2,"max":4}}}' >/dev/null
until_true 'k -n edge get hpa edgerush-proxy-web' 60 ||
    fail "DataPlane web autoscaled has no HorizontalPodAutoscaler"
# Past the autoscaler's first sync, 15 seconds: replicas removed would be 1 until it.
sleep 20
kill "$watch_pid" 2>/dev/null || true
watch_pid=
[ -s "$counts" ] || fail "the Deployment's watch saw no change at all"
! grep -qvx 2 "$counts" ||
    fail "DataPlane web autoscaled from 2 stated other counts on the way: $(tr '\n' ' ' <"$counts")"
[ "$(deployment_replicas)" = 2 ] ||
    fail "DataPlane web autoscaled from 2 runs $(deployment_replicas)"
pass "a DataPlane made autoscaled hands its count to its autoscaler, never fewer on the way"

echo "== an autoscaler no longer asked for"
k -n edge patch dataplane web --type=merge -p '{"spec":{"replicas":{"min":1,"max":null}}}' >/dev/null
until_true '! k -n edge get hpa edgerush-proxy-web' 60 ||
    fail "DataPlane web no longer autoscaled left its HorizontalPodAutoscaler: $(k -n edge get hpa edgerush-proxy-web -o name 2>&1)"
until_true '[ "$(deployment_replicas)" = 1 ]' 60 ||
    fail "DataPlane web of 1 replica runs $(deployment_replicas)"
# Past the autoscaler's sync, 15 seconds: one left behind would scale it back to 2.
sleep 20
[ "$(deployment_replicas)" = 1 ] || fail "DataPlane web of 1 replica was scaled to $(deployment_replicas)"
condition_is dataplane/web edge Programmed "True Programmed" 60 ||
    fail "DataPlane web, no longer autoscaled: $(condition dataplane/web edge Programmed)"
pass "a DataPlane no longer autoscaled deletes its autoscaler and runs the count it states"

echo "== a Gateway no longer asked for"
k -n edge patch dataplane web --type=merge -p '{"spec":{"gateway":null}}' >/dev/null
until_true '! k -n edge get gateway web' 60 ||
    fail "DataPlane web's gateway taken away left its Gateway: $(k -n edge get gateway web -o name 2>&1)"
k get ingressclass edge.web >/dev/null || fail "DataPlane web's gateway taken away took its IngressClass too"
condition_is dataplane/web edge Programmed "True Programmed" 60 ||
    fail "DataPlane web, its gateway taken away: $(condition dataplane/web edge Programmed)"
plane_answers web 80 shop.example 200 120 || fail "the Ingress is not served once web's gateway is taken away: $(status shop.example)"
pass "a DataPlane's gateway taken away deletes its Gateway, its IngressClass kept, the Ingress served"

echo "== deleted"
shop_says() { k -n edge get ingress shop -o jsonpath='{.status.loadBalancer.ingress}'; }
[ "$(k -n edge get ingress shop -o jsonpath='{.status.loadBalancer.ingress[0].ip}')" = 192.0.2.10 ] ||
    fail "before DataPlane web is deleted, the Ingress's status says '$(shop_says)'"
k -n edge delete dataplane web --wait=false >/dev/null
until_true '[ -z "$(k -n edge get deploy,sa,pdb,hpa,rolebindings,gateways -l gateway.edgerush.dev/data-plane=web -o name 2>/dev/null)" ] && ! k get ingressclass edge.web' 90 ||
    fail "DataPlane web deleted left: $(k -n edge get deploy,sa,pdb,rolebindings -l gateway.edgerush.dev/data-plane=web -o name; k get ingressclass edge.web -o name 2>&1)"
until_true '! k -n edge get dataplane web' 30 || fail "DataPlane web is never gone"
pass "a DataPlane deleted takes everything made for it, its IngressClass included"
# The last DataPlane of ControlPlane edge: its controller, serving none, follows on.
until_true '[ -z "$(shop_says)" ]' 60 ||
    fail "DataPlane web deleted, the Ingress of its class still says '$(shop_says)'"
pass "the last DataPlane deleted, the Ingress of its class loses the address it said"
k -n edge delete controlplane edge --wait=false >/dev/null
until_true '! made_in edge edge && ! k get gatewayclass edge && [ -z "$(k get clusterrolebindings -l gateway.edgerush.dev/control-plane=edge -o name)" ]' 90 ||
    fail "ControlPlane edge deleted left: $(k get gatewayclass edge -o name 2>&1; k get clusterrolebindings -l gateway.edgerush.dev/control-plane=edge -o name)"
until_true '! k -n edge get controlplane edge' 30 || fail "ControlPlane edge is never gone"
pass "a ControlPlane deleted takes everything made for it, its GatewayClass and ClusterRoleBinding included"
