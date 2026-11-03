#!/usr/bin/env bash
# The config stream, end to end in a kind cluster (02 §4, 03 §10): an Ingress applied is
# translated by the control plane and served by a data plane that takes its config from it;
# and the admission webhook, which the cluster's API server asks (04 §3).
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
#   - the data plane named as a Gateway, the control plane says the Gateway API's CRDs are
#     not installed, and the Ingress is served on its own port, 80; installed, each kind is
#     followed, its CRD read by name alone as RBAC allows, and every request to the Ingress
#     answered as they come and the Gateway is made, its `http` listener sharing the port;
#     an HTTPRoute attached to the Gateway, of this controller's class, is served beside
#     the Ingress, from one data plane, and on the Gateway's other port alone, where the
#     Ingress is not; the GatewayClass, the Gateway and the
#     HTTPRoute say they are accepted, of their generation, the Gateway the data plane's
#     address and its route, and programmed once the data plane runs it, as the replicas
#     report; an HTTPRoute in `blog` led to `shop`'s Service
#     answers 500 until a ReferenceGrant in `shop` allows it, and again once it is deleted;
#     an HTTPRoute and an Ingress of one host and path are ranked by when the cluster made
#     them, whichever API each is of: the older takes the requests, the other is said
#     shadowed;
#     an HTTPRoute's redirect that states only a host sends the client to the listener's
#     port;
#   - the Ingress's status says the data plane's Service's address;
#   - an Ingress changed is served as it now says; left out, it says no address, and
#     taken back, the address again;
#   - asked to reach its Service by the Service's own address, it is served through the
#     cluster's DNS and kube-proxy, and served again once it no longer asks;
#   - an EndpointSlice written by hand to point at the data plane's own pod is not sent
#     to, and that is said; nor one pointed at a control-plane pod, whose address the
#     control plane resolves by its headless Service and sends on the stream, and again
#     once a replica that took over is a new pod;
#   - with the node labelled with a zone, and the data plane told its own Service, an
#     EndpointSlice written by hand that hints one backend pod for that zone and another
#     for one elsewhere has every request go to the first, and to both once one is
#     unhinted;
#   - the control plane's leader gone, the other replica takes its Lease and writes status;
#   - requests without pause to the Ingress and the HTTPRoute, through the control plane
#     stopped and back, are every one answered 200;
#   - with the control plane gone, the data plane serves on;
#   - a data plane started while it is gone waits for its first config, serving nothing,
#     and serves once the control plane is back;
#   - the webhook registered, an Ingress of the class that would be left out is refused,
#     with why, one that would not is admitted, and one of another class, however it is.
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
# The zone a cloud's nodes are labelled with, before any pod is placed: the EndpointSlice
# controller writes each endpoint's zone from its node's (03 §6).
k label nodes --all topology.kubernetes.io/zone=zone-a --overwrite >/dev/null
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
# The name the API server asks the webhook by.
issue webhook edgerush-control.edge.svc

echo "== deploying"
k create namespace edge --dry-run=client -o yaml | k apply -f - >/dev/null
for end in control data-plane; do
    k -n edge create secret generic "edgerush-$end-stream" \
        --from-file=tls.crt="$pki/$end.crt" --from-file=tls.key="$pki/$end.key" \
        --from-file=ca.crt="$pki/ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
done
k -n edge create secret tls edgerush-control-webhook \
    --cert="$pki/webhook.crt" --key="$pki/webhook.key" --dry-run=client -o yaml |
    k apply -f - >/dev/null
sed "s|AGNHOST|$AGNHOST|" kind/stream/manifests.yaml | k apply -f - >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush-control --timeout=120s >/dev/null
k -n edge rollout status deploy/edgerush --timeout=120s >/dev/null

# The data plane on localhost:$port, through a port-forward started afresh after a restart,
# on a port of its own each time, so that none waits for the last to let its port go; to
# the pod's port $1, or 80, the Ingress HTTP port, which the Gateway's `http` listener shares.
# A port that speaks TLS is reached as $2, a name its certificate of the stream's authority
# carries.
port=18080
forward() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
    port=$((port + 1))
    k -n edge port-forward deploy/edgerush "$port:${1:-80}" >/dev/null 2>&1 &
    forward_pid=$!
    for _ in $(seq 1 50); do
        if [ -n "${2:-}" ]; then
            curl -s -o /dev/null --cacert "$pki/ca.crt" --resolve "$2:$port:127.0.0.1"                 "https://$2:$port/" && return 0
        else
            curl -s -o /dev/null "http://127.0.0.1:$port/" && return 0
        fi
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
# Here and below, grep reads a log whole: with -q it stops at the first match, kubectl,
# still writing, ends on SIGPIPE, and pipefail takes the pipeline for a failure.
said() {
    for _ in $(seq 1 $(($3 * 5))); do
        k -n edge logs "$1" 2>/dev/null | grep -- "$2" >/dev/null && return 0
        sleep 0.2
    done
    return 1
}

said deploy/edgerush "config from the control plane runs" 60 ||
    fail "the data plane never ran a config: $(k -n edge logs deploy/edgerush | tail -5)"
# Whichever replica the data plane dialled.
said --selector=app=edgerush-control "runs config" 10 ||
    fail "the control plane never heard the data plane run its config"

# Waits up to $2 seconds for every control-plane replica's logs, whole, to have a line
# with $1 in it.
control_said() {
    for _ in $(seq 1 $(($2 * 5))); do
        local all=yes
        for replica in $(k -n edge get pod -l app=edgerush-control -o name); do
            k -n edge logs "$replica" 2>/dev/null | grep -- "$1" >/dev/null || all=
        done
        [ -n "$all" ] && return 0
        sleep 0.2
    done
    return 1
}
# Whether a control-plane replica was refused anything of the Gateway API.
refused_any() {
    for replica in $(k -n edge get pod -l app=edgerush-control -o name); do
        k -n edge logs "$replica" 2>/dev/null |
            grep -E "(customresourcedefinitions|gatewayclasses|gateways|listenersets|httproutes|referencegrants).*(403|forbidden)" &&
            return 0
    done
    return 1
}
control_said "httproutes.gateway.networking.k8s.io: not installed" 30 ||
    fail "the control plane never said the Gateway API's CRDs are not installed"
said deploy/edgerush 'listener "port-80" is on 0.0.0.0:80' 60 ||
    fail "without the CRDs, the data plane never bound the Ingress HTTP port: $(k -n edge logs deploy/edgerush | tail -5)"
forward
answers shop.example 200 30 || fail "without the CRDs, the Ingress is not served: $(status shop.example)"
pass "without the Gateway API's CRDs, the control plane says so, and the Ingress is served on its own port, 80"
# Requests without pause to the Ingress from before the CRDs are installed until its
# Gateway, made after them, is served: Ingress's port, which asks nothing of the Gateway,
# is never let go, and every request is answered (04 §2).
installing=$(mktemp -d)
(
    while [ ! -e "$installing/stop" ]; do
        echo "$(status shop.example)"
    done
) >"$installing/answers" &
loading=$!
sleep 2
# The CRDs are too large for kubectl's client-side record of what it applied.
k create -f crates/gateway-api/crds/ >/dev/null
control_said "httproutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0)" 60 ||
    fail "the control plane never said the Gateway API's CRDs are served"
control_said "listenersets.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0)" 5 ||
    fail "the control plane never said ListenerSets are served"
sleep 3
k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: edge }
spec:
  gatewayClassName: edgerush
  allowedListeners: { namespaces: { from: All } }
  listeners:
    - { name: http, port: 80, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }
    - { name: alt, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: web, namespace: shop }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [route.example]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]
EOF
said deploy/edgerush 'listener "port-8080" is on 0.0.0.0:8080' 60 ||
    fail "the data plane never bound its Gateway's other listener: $(k -n edge logs deploy/edgerush | tail -5)"
answers route.example 200 30 || fail "the HTTPRoute is not served: $(status route.example)"
sleep 2
touch "$installing/stop"
wait "$loading"
total=$(grep -c . "$installing/answers")
# A count of none is grep's failure.
other=$(grep -vc '^200$' "$installing/answers" || true)
[ "$other" = 0 ] && [ "$total" -gt 20 ] ||
    fail "$other of $total requests to the Ingress as the Gateway API came were not answered 200: $(grep -v '^200$' "$installing/answers" | sort | uniq -c | head -5)"
rm -r "$installing"
k -n edge logs deploy/edgerush | grep 'listener "port-80" has drained' >/dev/null &&
    fail "the Ingress HTTP port was let go as the Gateway took it"
pass "the CRDs installed and the Gateway made, all $total requests to the Ingress were answered 200, its port shared with the Gateway's, by $(curl -s -H 'host: shop.example' "http://127.0.0.1:$port/hostname")"
answers shop.example 200 5 || fail "beside the HTTPRoute, the Ingress is not served: $(status shop.example)"
# On the Gateway's other port, the HTTPRoute alone: Ingress is served on its own.
forward 8080
answers route.example 200 30 || fail "on the Gateway's other listener, the HTTPRoute answers $(status route.example)"
answers shop.example 404 5 || fail "on the Gateway's other listener, the Ingress answers $(status shop.example)"
forward
pass "on a port of the Gateway's other than Ingress's, the HTTPRoute is served and the Ingress is not"
refused_any && fail "a control-plane replica was refused something of the Gateway API"
answers shop.example 200 10 || fail "with the Gateway API followed, $(status shop.example)"
pass "installed, the Gateway API's kinds are followed, their CRDs read by name alone, and an HTTPRoute is served beside the Ingress"

# Each object's status as the leader writes it, of the object's generation (04 §6).
stated() { # what, jsonpath, seconds, the object
    local want=$1 path=$2 seconds=$3
    shift 3
    for _ in $(seq 1 $((seconds * 5))); do
        [ "$(k get "$@" -o jsonpath="$path" 2>/dev/null)" = "$want" ] && return 0
        sleep 0.2
    done
    return 1
}
said_of() { k get "$@" -o jsonpath='{.status}'; }
condition() { echo "{.status.conditions[?(@.type==\"$1\")].$2}"; }
stated True "$(condition Accepted status)" 30 gatewayclass edgerush ||
    fail "the GatewayClass says $(said_of gatewayclass edgerush)"
stated True "$(condition SupportedVersion status)" 5 gatewayclass edgerush ||
    fail "the GatewayClass's version is not said supported: $(said_of gatewayclass edgerush)"
stated True "$(condition Accepted status)" 30 -n edge gateway web ||
    fail "the Gateway says $(said_of -n edge gateway web)"
generation=$(k -n edge get gateway web -o jsonpath='{.metadata.generation}')
stated "$generation" "$(condition Accepted observedGeneration)" 5 -n edge gateway web ||
    fail "the Gateway's status is not of generation $generation: $(said_of -n edge gateway web)"
stated 192.0.2.10 '{.status.addresses[0].value}' 5 -n edge gateway web ||
    fail "the Gateway does not say the data plane's address: $(said_of -n edge gateway web)"
stated 1 '{.status.listeners[0].attachedRoutes}' 5 -n edge gateway web ||
    fail "the Gateway's listener does not count its route: $(said_of -n edge gateway web)"
stated "edgerush.dev/gateway-controller True"     '{.status.parents[0].controllerName} {.status.parents[0].conditions[?(@.type=="Accepted")].status}'     30 -n shop httproute web || fail "the HTTPRoute says $(said_of -n shop httproute web)"
# Programmed once every ready data-plane pod runs the Gateway's generation, as the replica
# it reaches reports it and the leader merges the reports.
stated True "$(condition Programmed status)" 30 -n edge gateway web ||
    fail "the Gateway is not said programmed: $(said_of -n edge gateway web)"
stated True '{.status.listeners[0].conditions[?(@.type=="Programmed")].status}' 5 -n edge gateway web ||
    fail "the Gateway's listener is not said programmed: $(said_of -n edge gateway web)"
k -n edge get configmap -l gateway.edgerush.dev/acks=edgerush-control -o name | grep -q . ||
    fail "no replica reports what its data planes run"
refused_any && fail "a control-plane replica was refused a status of the Gateway API"
pass "the GatewayClass, the Gateway and the HTTPRoute say they are accepted, the Gateway its address and its route, each of its generation, and the Gateway programmed once its data plane runs it"

# A listener on a port the data plane cannot bind, its own metrics' (03 §10): that
# listener alone is not programmed, still pending, saying the pod and why; the Gateway and
# its other listeners are, as the config runs on every pod (04 §6).
of_listener() { echo "{.status.listeners[?(@.name==\"$1\")].conditions[?(@.type==\"Programmed\")].$2}"; }
k -n edge patch gateway web --type=json \
    -p '[{"op":"add","path":"/spec/listeners/-","value":{"name":"taken","port":9100,"protocol":"HTTP"}}]' >/dev/null
stated False "$(of_listener taken status)" 60 -n edge gateway web ||
    fail "a listener on a port the data plane cannot bind is not said unprogrammed: $(said_of -n edge gateway web)"
stated Pending "$(of_listener taken reason)" 5 -n edge gateway web ||
    fail "a listener the data plane cannot bind is not said pending: $(said_of -n edge gateway web)"
k -n edge get gateway web -o jsonpath="$(of_listener taken message)" | grep -q '^data plane .*9100' ||
    fail "a listener the data plane cannot bind does not say the pod and why: $(said_of -n edge gateway web)"
stated True "$(of_listener http status)" 30 -n edge gateway web ||
    fail "beside a listener not bound, the others are not said programmed: $(said_of -n edge gateway web)"
stated True "$(condition Programmed status)" 5 -n edge gateway web ||
    fail "beside a listener not bound, the Gateway is not said programmed: $(said_of -n edge gateway web)"
answers route.example 200 5 || fail "beside a listener not bound, the HTTPRoute answers $(status route.example)"
k -n edge patch gateway web --type=json -p '[{"op":"remove","path":"/spec/listeners/2"}]' >/dev/null
stated "http alt" '{.status.listeners[*].name}' 30 -n edge gateway web ||
    fail "the listener taken away is still said: $(said_of -n edge gateway web)"
pass "a listener on a port its data plane cannot bind alone is not programmed, pending, saying the pod and why; the Gateway and its other listeners are"

# A ListenerSet of shop's, which the Gateway takes from every namespace, adds a listener of
# its own, bound beside the Gateway's; a route naming the ListenerSet is served on it; the
# ListenerSet, the route's parent and the Gateway's count of ListenerSets say so (04 §6).
k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: ListenerSet
metadata: { name: team, namespace: shop }
spec:
  parentRef: { name: web, namespace: edge }
  listeners: [{ name: team, port: 9090, protocol: HTTP, hostname: team.example }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: team, namespace: shop }
spec:
  parentRefs: [{ kind: ListenerSet, name: team }]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]
EOF
said deploy/edgerush 'listener "port-9090" is on 0.0.0.0:9090' 60 ||
    fail "the data plane never bound the ListenerSet's listener: $(k -n edge logs deploy/edgerush | tail -5)"
forward 9090
answers team.example 200 30 || fail "the route of the ListenerSet is not served: $(status team.example)"
forward
stated True "$(condition Accepted status)" 30 -n shop listenerset team ||
    fail "the ListenerSet is not said accepted: $(said_of -n shop listenerset team)"
stated True "$(condition Programmed status)" 30 -n shop listenerset team ||
    fail "the ListenerSet is not said programmed: $(said_of -n shop listenerset team)"
stated 1 '{.status.listeners[0].attachedRoutes}' 5 -n shop listenerset team ||
    fail "the ListenerSet's listener does not count its route: $(said_of -n shop listenerset team)"
stated "ListenerSet True" \
    '{.status.parents[0].parentRef.kind} {.status.parents[0].conditions[?(@.type=="Accepted")].status}' \
    30 -n shop httproute team || fail "the route of the ListenerSet says $(said_of -n shop httproute team)"
stated 1 '{.status.attachedListenerSets}' 30 -n edge gateway web ||
    fail "the Gateway does not count the ListenerSet: $(said_of -n edge gateway web)"
k -n shop delete httproute team >/dev/null
k -n shop delete listenerset team >/dev/null
pass "a ListenerSet the Gateway takes adds a listener of its own, a route naming it served there, each said in status"

# Two HTTPS listeners of the Gateway on one port, each with a certificate of its own, are
# told apart by hostname: a request on a connection made for one, by its SNI, for the
# other's host is answered 421, over HTTP/1.1 and HTTP/2, and one for its own host is
# served (03 §3, 04 §2). The certificates are Secrets of shop's, a namespace the control
# plane watches, which a ReferenceGrant lets the Gateway reach.
for name in a b; do
    issue "shared-$name" "$name.example"
    k -n shop create secret tls "shared-$name" --cert="$pki/shared-$name.crt" \
        --key="$pki/shared-$name.key" --dry-run=client -o yaml | k apply -f - >/dev/null
done
k -n edge patch gateway web --type=json -p '[
  {"op":"add","path":"/spec/listeners/-","value":{"name":"shared-a","port":8443,"protocol":"HTTPS","hostname":"a.example","allowedRoutes":{"namespaces":{"from":"All"}},"tls":{"certificateRefs":[{"name":"shared-a","namespace":"shop"}]}}},
  {"op":"add","path":"/spec/listeners/-","value":{"name":"shared-b","port":8443,"protocol":"HTTPS","hostname":"b.example","allowedRoutes":{"namespaces":{"from":"All"}},"tls":{"certificateRefs":[{"name":"shared-b","namespace":"shop"}]}}}
]' >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata: { name: shared-certificates, namespace: shop }
spec:
  from: [{ group: gateway.networking.k8s.io, kind: Gateway, namespace: edge }]
  to: [{ group: "", kind: Secret }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: shared, namespace: shop }
spec:
  parentRefs:
    - { name: web, namespace: edge, sectionName: shared-a }
    - { name: web, namespace: edge, sectionName: shared-b }
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]
EOF
said deploy/edgerush 'listener "port-8443" is on 0.0.0.0:8443' 60 ||
    fail "the data plane never bound the HTTPS listeners' port: $(k -n edge logs deploy/edgerush | tail -5)"
stated True "$(of_listener shared-a status)" 60 -n edge gateway web ||
    fail "the HTTPS listener is not said programmed: $(said_of -n edge gateway web)"
forward 8443 a.example
# The status of a request for host $2 on a connection asking for $1, verified against the
# stream's authority, by curl's options $3.
over_tls() {
    curl -s -o /dev/null -w '%{http_code}' --cacert "$pki/ca.crt" --resolve "$1:$port:127.0.0.1" \
        ${3:-} -H "host: $2" "https://$1:$port/hostname"
}
for _ in $(seq 1 150); do
    [ "$(over_tls a.example a.example)" = 200 ] && break
    sleep 0.2
done
[ "$(over_tls a.example a.example)" = 200 ] || fail "a.example over TLS answers $(over_tls a.example a.example)"
[ "$(over_tls b.example b.example)" = 200 ] || fail "b.example over TLS answers $(over_tls b.example b.example)"
for http in --http1.1 --http2; do
    [ "$(over_tls a.example b.example "$http")" = 421 ] ||
        fail "b.example on a connection made for a.example, $http, answers $(over_tls a.example b.example "$http")"
done
forward
k -n shop delete httproute shared >/dev/null
listeners=$(k -n edge get gateway web -o jsonpath='{.spec.listeners[*].name}' | wc -w)
k -n edge patch gateway web --type=json -p "[{\"op\":\"remove\",\"path\":\"/spec/listeners/$((listeners - 1))\"},{\"op\":\"remove\",\"path\":\"/spec/listeners/$((listeners - 2))\"}]" >/dev/null
k -n shop delete referencegrant shared-certificates >/dev/null
k -n shop delete secret shared-a shared-b >/dev/null
pass "two HTTPS listeners on one port are told apart: a request for one's host on a connection made for the other is answered 421, over HTTP/1.1 and HTTP/2"

k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: across, namespace: blog }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [across.example]
  rules: [{ backendRefs: [{ name: web, namespace: shop, port: 80 }] }]
EOF
answers across.example 500 30 || fail "with no ReferenceGrant, another namespace's Service answers $(status across.example)"
k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata: { name: from-blog, namespace: shop }
spec:
  from: [{ group: gateway.networking.k8s.io, kind: HTTPRoute, namespace: blog }]
  to: [{ group: "", kind: Service, name: web }]
EOF
answers across.example 200 30 || fail "with a ReferenceGrant, another namespace's Service answers $(status across.example)"
k -n shop delete referencegrant from-blog >/dev/null
answers across.example 500 30 || fail "its ReferenceGrant deleted, another namespace's Service answers $(status across.example)"
pass "an HTTPRoute reaches another namespace's Service only while a ReferenceGrant there allows it"

# The HTTPRoute's backend is not there, so it answers 500 where it takes the requests, and
# the Ingress's is, 200. Each has a host of its own too, to know once it is served.
# Creation times are to the second, so the second is made a second on.
ranked_route() {
    k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: ranked, namespace: shop }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [ranked.example, ranked-route.example]
  rules: [{ backendRefs: [{ name: not-there, port: 80 }] }]
EOF
    answers ranked-route.example 500 30 || fail "the HTTPRoute is not served: $(status ranked-route.example)"
}
ranked_route
sleep 1.5
k apply -f - >/dev/null <<EOF
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: ranked, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: ranked.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
    - host: ranked-ingress.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
EOF
answers ranked-ingress.example 200 30 || fail "the Ingress is not served: $(status ranked-ingress.example)"
answers ranked.example 500 5 || fail "beside an older HTTPRoute, the Ingress's path answers $(status ranked.example)"
control_said "Ingress shop/ranked: path / (Prefix) on ranked.example is already HTTPRoute shop/ranked's, which comes first" 30 ||
    fail "the control plane never said the Ingress's path is the older HTTPRoute's"
k -n shop delete httproute ranked >/dev/null
answers ranked-route.example 404 30 || fail "the HTTPRoute deleted still answers $(status ranked-route.example)"
answers ranked.example 200 5 || fail "the HTTPRoute gone, the Ingress's path answers $(status ranked.example)"
ranked_route
answers ranked.example 200 5 || fail "beside a newer HTTPRoute, the Ingress's path answers $(status ranked.example)"
k -n shop delete httproute ranked >/dev/null
k -n shop delete ingress ranked >/dev/null
pass "an HTTPRoute and an Ingress of one host and path are ranked by age: the older serves it, the other said shadowed"

# A redirect that states only a host sends the client to the listener's port.
k apply -f - >/dev/null <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: moved, namespace: shop }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [moved.example]
  rules: [{ filters: [{ type: RequestRedirect, requestRedirect: { hostname: example.org } }] }]
EOF
# On the Gateway's `alt` listener, whose port is not the scheme's own and so is stated.
forward 8080
answers moved.example 302 30 || fail "the redirecting HTTPRoute answers $(status moved.example)"
location=$(curl -s -o /dev/null -w '%{redirect_url}' -H 'host: moved.example' "http://127.0.0.1:$port/hostname")
[ "$location" = "http://example.org:8080/hostname" ] ||
    fail "the redirect sends the client to $location, not the listener's port"
forward
k -n shop delete httproute moved >/dev/null
pass "an HTTPRoute's redirect of a host alone sends the client there on the listener's port"

# What the Ingress's status says, its first address.
says() { k -n shop get ingress web -o jsonpath='{.status.loadBalancer.ingress[0].ip}'; }
# Waits up to $2 seconds for the Ingress's status to say $1.
saying() {
    for _ in $(seq 1 $(($2 * 5))); do
        [ "$(says)" = "$1" ] && return 0
        sleep 0.2
    done
    return 1
}
saying 192.0.2.10 30 || fail "the Ingress's status says '$(says)', not the data plane's address"
pass "the Ingress's status says the data plane's address"

k -n shop patch ingress web --type=json \
    -p='[{"op": "replace", "path": "/spec/rules/0/host", "value": "other.example"}]' >/dev/null
answers other.example 200 30 || fail "the Ingress changed is not served: $(status other.example)"
answers shop.example 404 10 || fail "the host taken away still answers $(status shop.example)"
pass "the Ingress changed is served as it now says"

# An annotation translation does not support leaves the Ingress out whole.
k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target=/ >/dev/null
saying "" 30 || fail "left out, the Ingress's status still says '$(says)'"
k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target- >/dev/null
saying 192.0.2.10 30 || fail "taken back, the Ingress's status says '$(says)'"
answers other.example 200 30 || fail "taken back, the Ingress is not served: $(status other.example)"
pass "left out, the Ingress says no address, and taken back, the address again"

# The data plane resolves the Service's name through the cluster's DNS, as its pod's
# resolv.conf names it, and kube-proxy chooses the pod; a data plane that could not would
# answer 503 for want of an endpoint.
# How many configs the data plane has run from the control plane.
ran() { k -n edge logs deploy/edgerush | grep -c "config from the control plane runs"; }
before=$(ran)
k -n shop annotate ingress web gateway.edgerush.dev/service-upstream=true >/dev/null
for _ in $(seq 1 150); do
    [ "$(ran)" -gt "$before" ] && break
    sleep 0.2
done
[ "$(ran)" -gt "$before" ] || fail "the data plane never ran the config that reaches the Service's address"
answers other.example 200 30 || fail "reached by its Service's address, $(status other.example)"
for _ in $(seq 1 20); do
    [ "$(status other.example)" = 200 ] || fail "reached by its Service's address, $(status other.example)"
done
k -n shop annotate ingress web gateway.edgerush.dev/service-upstream- >/dev/null
answers other.example 200 30 || fail "back on its endpoints, $(status other.example)"
pass "reached by its Service's own address, the Ingress is served through the cluster's DNS"

# A Service of no selector, and its one EndpointSlice written by hand, pointed at $1: the
# data plane must say it is not sent to, as $2, and answer 503 for want of an endpoint.
refused() {
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata: { name: loop, namespace: shop }
spec:
  ports: [{ name: http, port: 80, targetPort: 8080 }]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: loop-1
  namespace: shop
  labels: { kubernetes.io/service-name: loop }
addressType: IPv4
ports: [{ name: http, port: 8080 }]
endpoints: [{ addresses: ["$1"] }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: loop, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: loop.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: loop, port: { name: http } } } }
EOF
    said deploy/edgerush "upstream shop/loop/http: $1:8080 is not sent to, as $2" 60 ||
        fail "the data plane never said it refuses $1, as $2"
    answers loop.example 503 10 || fail "an Ingress led to $1 answers $(status loop.example)"
    k -n shop delete ingress loop >/dev/null
    k -n shop delete service loop >/dev/null
    k -n shop delete endpointslice loop-1 >/dev/null
}
# The control-plane pod newest made, and its address.
control() { k -n edge get pod -l app=edgerush-control --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].status.podIP}'; }

own=$(k -n edge get pod -l app=edgerush -o jsonpath='{.items[0].status.podIP}')
[ -n "$own" ] || fail "the data plane has no pod address"
refused "$own" "one of the data plane's own"
pass "an EndpointSlice pointed at the data plane's own pod is not sent to, and that is said"
peer=$(control)
[ -n "$peer" ] || fail "the control plane has no pod address"
refused "$peer" "one of the gateway's own pods"
pass "nor one pointed at a control-plane pod, named on the stream"

# Two backend pods, by an EndpointSlice written by hand: the first hinted for zone-a, the
# data plane's, as its own Service's slices say it is; the second for zone-b. Every request
# goes to the first, and to both once the second is unhinted, which turns preference off.
k -n shop scale deploy/web --replicas=2 >/dev/null
k -n shop rollout status deploy/web --timeout=120s >/dev/null
# jsonpath ends no line, which `read` says by failing: whether both were found is checked.
read -r near near_ip far far_ip < <(k -n shop get pod -l app=web \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.podIP} {end}') || true
[ -n "$far_ip" ] || fail "two backend pods were not found"
zoned() {
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata: { name: zoned, namespace: shop }
spec:
  ports: [{ name: http, port: 80, targetPort: 8080 }]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: zoned-1
  namespace: shop
  labels: { kubernetes.io/service-name: zoned }
addressType: IPv4
ports: [{ name: http, port: 8080 }]
endpoints:
  - { addresses: ["$near_ip"], zone: zone-a, hints: { forZones: [{ name: zone-a }] } }
  - { addresses: ["$far_ip"], zone: zone-b$1 }
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: zoned, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: zoned.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: zoned, port: { name: http } } } }
EOF
}
# Which pods answered $1 requests to zoned.example, each once, in order.
answered() {
    for _ in $(seq 1 "$1"); do
        curl -s -H 'host: zoned.example' "http://127.0.0.1:$port/hostname"
        echo
    done | sort -u | tr '\n' ' '
}
zoned ", hints: { forZones: [{ name: zone-b }] }"
answers zoned.example 200 30 || fail "the zoned Ingress is not served: $(status zoned.example)"
# Until the data plane has its zone and both endpoints, a request may go either way.
for _ in $(seq 1 50); do
    [ "$(answered 10)" = "$near " ] && break
    sleep 0.2
done
[ "$(answered 30)" = "$near " ] || fail "requests did not keep to the zone: $(answered 30)"
zoned ""
both=$(printf '%s\n' "$near" "$far" | sort | tr '\n' ' ')
for _ in $(seq 1 50); do
    [ "$(answered 20)" = "$both" ] && break
    sleep 0.2
done
[ "$(answered 40)" = "$both" ] || fail "with an endpoint unhinted, not both pods answered: $(answered 40)"
k -n shop delete ingress zoned >/dev/null
k -n shop delete service zoned >/dev/null
k -n shop delete endpointslice zoned-1 >/dev/null
k -n shop scale deploy/web --replicas=1 >/dev/null
pass "requests keep to the endpoints hinted for the data plane's zone, and go to all once one is unhinted"

echo "== the leader gone"
# Who holds the Lease: a pod's name, and when it started.
holder() { k -n edge get lease edgerush-control -o jsonpath='{.spec.holderIdentity}'; }
leader=$(holder)
leader=${leader%_*}
[ -n "$leader" ] || fail "no replica holds the Lease"
said "$leader" "leading, by Lease edge/edgerush-control" 10 || fail "the leader never said it leads"
k -n edge delete pod "$leader" --wait=false >/dev/null
# Another takes it once it goes unrenewed for 15 s.
took=
for _ in $(seq 1 200); do
    took=$(holder)
    took=${took%_*}
    [ -n "$took" ] && [ "$took" != "$leader" ] && break
    sleep 0.2
done
[ -n "$took" ] && [ "$took" != "$leader" ] || fail "no other replica took the Lease from $leader"
k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target=/ >/dev/null
saying "" 30 || fail "with $took leading, the status still says '$(says)'"
k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target- >/dev/null
saying 192.0.2.10 30 || fail "with $took leading, the status says '$(says)'"
k -n edge rollout status deploy/edgerush-control --timeout=120s >/dev/null
pass "the leader gone, $took took the Lease and writes status"
# The replica that took the deleted one's place is a new pod, at a new address, which the
# control plane comes to resolve and sends again.
for _ in $(seq 1 150); do
    [ "$(k -n edge get pod -l app=edgerush-control --field-selector=status.phase=Running -o name | grep -c .)" = 2 ] && break
    sleep 0.2
done
newest=$(control)
[ -n "$newest" ] && [ "$newest" != "$peer" ] || fail "no new control-plane pod took the deleted one's place"
refused "$newest" "one of the gateway's own pods"
pass "a control-plane pod made since is not sent to either, named on the stream as it changed"

echo "== an outage of the control plane"
# Requests without pause to the Ingress and the HTTPRoute, from before the control plane
# is stopped until after it is back and the data plane has dialled it again: every one
# answered as before (the stage's exit, 11).
outage=$(mktemp -d)
(
    while [ ! -e "$outage/stop" ]; do
        for host in other.example route.example; do
            echo "$host $(status "$host")"
        done
    done
) >"$outage/answers" &
loading=$!
replicas=$(k -n edge get deploy edgerush-control -o jsonpath='{.spec.replicas}')
sleep 3
k -n edge scale deploy/edgerush-control --replicas=0 >/dev/null
for _ in $(seq 1 300); do
    [ "$(k -n edge get pod -l app=edgerush-control -o name | grep -c .)" = 0 ] && break
    sleep 0.2
done
said deploy/edgerush "dialled again in" 30 || fail "the data plane never said its stream ended"
sleep 10
k -n edge scale deploy/edgerush-control --replicas="$replicas" >/dev/null
k -n edge rollout status deploy/edgerush-control --timeout=120s >/dev/null
# Back, a replica says the data plane runs its config: the one it ran, kept.
back=
for _ in $(seq 1 300); do
    k -n edge logs -l app=edgerush-control --tail=-1 2>/dev/null | grep -q "runs config" && back=yes && break
    sleep 0.2
done
[ -n "$back" ] || fail "no replica back says the data plane runs its config"
sleep 3
touch "$outage/stop"
wait "$loading"
total=$(grep -c . "$outage/answers")
# A count of none is grep's failure.
other=$(grep -vc ' 200$' "$outage/answers" || true)
[ "$other" = 0 ] && [ "$total" -gt 50 ] ||
    fail "$other of $total requests through the outage were not answered 200: $(grep -v ' 200$' "$outage/answers" | sort | uniq -c | head -5)"
rm -r "$outage"
pass "through the control plane's outage and its return, all $total requests to the Ingress and the HTTPRoute were answered 200"

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
k -n edge logs "$pod" | grep " is on " >/dev/null &&
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

echo "== the admission webhook"
k apply -f - >/dev/null <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata: { name: edgerush }
webhooks:
  - name: ingresses.edgerush.dev
    rules:
      - apiGroups: [networking.k8s.io]
        apiVersions: [v1]
        operations: [CREATE, UPDATE]
        resources: [ingresses]
    clientConfig:
      service: { namespace: edge, name: edgerush-control, path: /validate, port: 443 }
      caBundle: $(base64 -w0 "$pki/ca.crt")
    failurePolicy: Ignore
    sideEffects: None
    admissionReviewVersions: [v1]
    timeoutSeconds: 5
EOF
# The API server takes up a registration within moments; until then it admits all.
refused=
for _ in $(seq 1 150); do
    if ! refused=$(k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target=/ 2>&1); then
        break
    fi
    k -n shop annotate ingress web nginx.ingress.kubernetes.io/rewrite-target- >/dev/null
    refused=
    sleep 0.2
done
[ -n "$refused" ] || fail "an Ingress the data plane would leave out was never refused"
echo "$refused" | grep -q 'denied the request: not supported yet: annotations' ||
    fail "refused, but not for why: $refused"
pass "an Ingress of the class that would be left out is refused, with why"

k -n shop annotate ingress web gateway.edgerush.dev/https-redirect=false >/dev/null ||
    fail "an Ingress that translates was refused"
k -n shop apply --dry-run=server -f - >/dev/null <<EOF ||
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
