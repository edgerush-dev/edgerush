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
#   - the Ingress is served, and its status says the data plane's Service's address;
#   - the data plane named as a Gateway, the control plane says the Gateway API's CRDs are
#     not installed and serves Ingress as ever; installed, each kind is followed, its CRD
#     read by name alone as RBAC allows, and an HTTPRoute attached to the Gateway, of this
#     controller's class, its listener on the port the data plane binds, is served beside
#     the Ingress, from one data plane; an HTTPRoute in `blog` led to `shop`'s Service
#     answers 500 until a ReferenceGrant in `shop` allows it, and again once it is deleted;
#     an HTTPRoute and an Ingress of one host and path are ranked by when the cluster made
#     them, whichever API each is of: the older takes the requests, the other is said
#     shadowed;
#     an HTTPRoute's redirect that states only a host sends the client to the listener's
#     port;
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
forward
answers shop.example 200 30 || fail "the Ingress is not served: $(status shop.example)"
# Whichever replica the data plane dialled.
said --selector=app=edgerush-control "runs config" 10 ||
    fail "the control plane never heard the data plane run its config"
pass "the Ingress is served, by $(curl -s -H 'host: shop.example' "http://127.0.0.1:$port/hostname")"

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
            grep -E "(customresourcedefinitions|gatewayclasses|gateways|httproutes|referencegrants).*(403|forbidden)" &&
            return 0
    done
    return 1
}
control_said "httproutes.gateway.networking.k8s.io: not installed" 30 ||
    fail "the control plane never said the Gateway API's CRDs are not installed"
pass "without the Gateway API's CRDs, the control plane says so, and serves the Ingress"
# The CRDs are too large for kubectl's client-side record of what it applied.
k create -f crates/gateway-api/crds/ >/dev/null
control_said "httproutes.gateway.networking.k8s.io: served at v1 (Gateway API v1.7.0)" 60 ||
    fail "the control plane never said the Gateway API's CRDs are served"
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
  listeners: [{ name: http, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: web, namespace: shop }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [route.example]
  rules: [{ backendRefs: [{ name: web, port: 80 }] }]
EOF
answers route.example 200 30 || fail "the HTTPRoute is not served: $(status route.example)"
answers shop.example 200 5 || fail "beside the HTTPRoute, the Ingress is not served: $(status shop.example)"
refused_any && fail "a control-plane replica was refused something of the Gateway API"
answers shop.example 200 10 || fail "with the Gateway API followed, $(status shop.example)"
pass "installed, the Gateway API's kinds are followed, their CRDs read by name alone, and an HTTPRoute is served beside the Ingress"

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
answers moved.example 302 30 || fail "the redirecting HTTPRoute answers $(status moved.example)"
location=$(curl -s -o /dev/null -w '%{redirect_url}' -H 'host: moved.example' "http://127.0.0.1:$port/hostname")
[ "$location" = "http://example.org:8080/hostname" ] ||
    fail "the redirect sends the client to $location, not the listener's port"
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
