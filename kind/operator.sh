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
#   - ControlPlane `edge` (its webhook off, which waits for its registration) and DataPlane
#     `web`, with a Gateway, applied are both accepted, the ControlPlane programmed and the
#     DataPlane not, waiting on Gateway API's CRDs, and the Ingress is served through the
#     data plane, reached by `kubectl port-forward`, on certificates the control plane
#     signs: two resources make a working gateway, of Ingress alone;
#   - a ControlPlane deleted there, without Gateway API's CRDs, is gone;
#   - Gateway API's CRDs installed, the operator makes the GatewayClass and the Gateway,
#     and the DataPlane is programmed, without a restart;
#   - the data plane's Deployment's image edited by hand is put back, and a label added by
#     hand is kept;
#   - a second DataPlane of `edge` is not accepted, and nothing is made for it;
#   - a ControlPlane of `edge`'s name in another namespace is not accepted, its status
#     naming the one that holds it, and nothing is made for it; given the operator's
#     finalizer by hand and deleted, it deletes nothing of `edge`'s;
#   - the watch scope's allowance (docs/05 §4): a ControlPlane watching every namespace
#     not accepted, nothing made for it, until an allowance lets it, then bound over the
#     cluster; a Selector scope within a selector allowance bound in the namespaces it
#     selects alone, one beyond it not accepted and bound nowhere; the allowance narrowed
#     taking back the rights it granted, the rest kept;
#   - a ServiceAccount of a name the operator would use, made by hand, is left alone, and
#     its ControlPlane says so;
#   - DataPlane `web` deleted takes with it everything made for it, its IngressClass and
#     bindings included; ControlPlane `edge` deleted, its GatewayClass and its
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
finish() {
    [ -n "$forward_pid" ] && kill "$forward_pid" 2>/dev/null || true
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

echo "== kept as stated"
k -n edge label deploy/edgerush-proxy-web team=shop >/dev/null
k -n edge set image deploy/edgerush-proxy-web '*=edgerush:by-hand' >/dev/null
until_true '[ "$(k -n edge get deploy/edgerush-proxy-web -o jsonpath="{.spec.template.spec.containers[0].image}")" = edgerush:dev ]' 60 ||
    fail "the image edited by hand is not put back"
pass "a field the operator states, edited by hand, is put back"
[ "$(k -n edge get deploy/edgerush-proxy-web -o jsonpath='{.metadata.labels.team}')" = shop ] ||
    fail "a label added by hand is gone"
pass "a label added by hand is kept"

echo "== refused"
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: second, namespace: edge }
spec:
  controlPlaneRef: { name: edge }
  ingress: { httpPort: 80, httpsPort: 443 }
EOF
condition_is dataplane/second edge Accepted "False OneDataPlaneOnly" 60 ||
    fail "a second DataPlane: $(condition dataplane/second edge Accepted)"
sleep 3
[ -z "$(k -n edge get deploy,sa -l gateway.edgerush.dev/data-plane=second -o name)" ] ||
    fail "something was made for the second DataPlane"
pass "a second DataPlane of one ControlPlane is not accepted, and nothing is made for it"
k delete -n edge dataplane second >/dev/null

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

echo "== deleted"
k -n edge delete dataplane web --wait=false >/dev/null
until_true '[ -z "$(k -n edge get deploy,sa,pdb,hpa,rolebindings,gateways -l gateway.edgerush.dev/data-plane=web -o name 2>/dev/null)" ] && ! k get ingressclass edge.web' 90 ||
    fail "DataPlane web deleted left: $(k -n edge get deploy,sa,pdb,rolebindings -l gateway.edgerush.dev/data-plane=web -o name; k get ingressclass edge.web -o name 2>&1)"
until_true '! k -n edge get dataplane web' 30 || fail "DataPlane web is never gone"
pass "a DataPlane deleted takes everything made for it, its IngressClass included"
k -n edge delete controlplane edge --wait=false >/dev/null
until_true '! made_in edge edge && ! k get gatewayclass edge && [ -z "$(k get clusterrolebindings -l gateway.edgerush.dev/control-plane=edge -o name)" ]' 90 ||
    fail "ControlPlane edge deleted left: $(k get gatewayclass edge -o name 2>&1; k get clusterrolebindings -l gateway.edgerush.dev/control-plane=edge -o name)"
until_true '! k -n edge get controlplane edge' 30 || fail "ControlPlane edge is never gone"
pass "a ControlPlane deleted takes everything made for it, its GatewayClass and ClusterRoleBinding included"
