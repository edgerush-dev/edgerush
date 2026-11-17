#!/usr/bin/env bash
# What the install ships, judged by a real API server in a kind cluster (docs/05 §1, §2;
# docs/10): the ControlPlane, DataPlane and PlatformDefaults CRDs, the five ClusterRoles and
# the admission policies, on a control plane's Services, on what it writes beyond its own
# namespace and on generated DataPlanes, applied as a user would; beside Gateway API's
# CRDs, which the golden cases' Gateways need.
#
#   kind/planes.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/planes.sh 1.37   the same, the cluster left for a look
#
# Then:
#   - a ControlPlane and DataPlanes of each kind (Ingress, Gateway API, both) are admitted,
#     and so is every golden case's resource of crates/planes;
#   - each resource the CRDs' schema and CEL rules are to refuse is refused, and says why:
#     a name that is no DNS label or too long for its Service's, a watch scope's selector
#     without Selector or Selector without one, no replicas, a DataPlane with neither
#     ingress nor gateway, a gateway of no listener or of one the Gateway would refuse,
#     Ingress short of a port or with one port twice, max below min, one port's settings
#     given twice, an enumerated value written in another case, a file access log of no
#     path, a LoadBalancer's fields on another type, and, on an update, a LoadBalancer's
#     class changed; a PlatformDefaults not named default, an allowance of a selector of
#     no requirement, for ControlPlanes or for what they watch, with a selector where it
#     takes none or none where it takes one, of a scope it cannot allow, or of an operator
#     Kubernetes does not have; a DataPlane naming a Gateway beside a gateway or ingress of
#     its own; a Gateway-first entry holding ingress, or two entries of one class;
#   - a Service holding externalIPs is refused written as a control plane's ServiceAccount,
#     and admitted written by another; one without them is admitted from either;
#   - beyond its own namespace, under the role bound there for a generated DataPlane, a
#     control plane's ServiceAccount writes a data plane's identity Secret and Service
#     labelled with its ControlPlane's name and namespace, and is refused one unlabelled,
#     one labelled as another's, another's Secret relabelled as its own, and, labelled as
#     its own, a ServiceAccount's token, an opaque Secret or one of another name, and a
#     Service selecting another workload or nothing, named for other pods, or ExternalName;
#     in its own namespace, and for anyone else, the policy leaves them alone;
#   - a DataPlane naming a Gateway, or a ControlPlane of another namespace, is refused
#     written by anyone but the operator's ServiceAccount, one of its name in another
#     namespace among them, and admitted from the operator's; its spec changed by another
#     is refused, its labels changed admitted.
# Nothing is built: no image is needed. Each check says PASS or FAIL; the first FAIL ends
# the run with status 1. Nothing here reads or writes the user's own kubeconfig: every
# kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

version=${1:?usage: kind/planes.sh <Kubernetes version>}
kubeconfig=$(kind/cluster.sh up "$version")
export KUBECONFIG="$kubeconfig"
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }
finish() {
    if [ "${KEEP:-}" != 1 ]; then kind/cluster.sh down "$version" >/dev/null; fi
}
trap finish EXIT

pass() { echo "PASS: $*"; }
fail() {
    echo "FAIL: $*"
    exit 1
}

install=crates/planes/install
k create -f crates/gateway-api/crds/ >/dev/null
k apply --server-side -f "$install/gateway.edgerush.dev_controlplanes.yaml" \
    -f "$install/gateway.edgerush.dev_dataplanes.yaml" \
    -f "$install/gateway.edgerush.dev_platformdefaults.yaml" \
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev \
    crd/platformdefaults.gateway.edgerush.dev crd/gateways.gateway.networking.k8s.io >/dev/null
k create namespace edge >/dev/null

# `admitted <what> <yaml>`: the object, applied as a dry run on the server, which judges it
# by every rule and admission policy, and stores nothing.
admitted() {
    local what=$1 yaml=$2 out
    if out=$(k apply --dry-run=server -f - <<<"$yaml" 2>&1); then
        pass "$what is admitted"
    else
        fail "$what is refused: $out"
    fi
}

# `refused <what> <why> <yaml>`: the same, refused, with `why` in what the server says.
refused() {
    local what=$1 why=$2 yaml=$3 out
    if out=$(k apply --dry-run=server -f - <<<"$yaml" 2>&1); then
        fail "$what is admitted"
    elif [[ $out != *"$why"* ]]; then
        fail "$what is refused, but not for \"$why\": $out"
    else
        pass "$what is refused: $why"
    fi
}

# `name <n>`: a DNS label of `n` characters, refused, where it is, for its length alone.
name() { printf 'p%.0s' $(seq "$1"); }
control_plane() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: ControlPlane\nmetadata: { name: %s, namespace: edge }\n%s\n' "$1" "${2:-}"
}
data_plane() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: DataPlane\nmetadata: { name: %s, namespace: edge }\nspec:\n  controlPlaneRef: { name: payments }\n%s\n' "$1" "$2"
}

# What is to be admitted.
admitted "a ControlPlane of every default" "$(control_plane payments)"
admitted "a ControlPlane of the longest name" "$(control_plane "$(name 43)")"
admitted "a ControlPlane watching what it selects" "$(control_plane payments 'spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { team: payments } } } }
  replicas: 2
  authority: { secretRef: { name: payments-ca } }
  webhook: { enabled: false }')"
admitted "a DataPlane of Ingress" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }')"
admitted "a DataPlane of the longest name" "$(data_plane "$(name 48)" '  ingress: { httpPort: 80, httpsPort: 443 }')"
admitted "a DataPlane of Gateway API" "$(data_plane api '  replicas: { min: 3, max: 20 }
  gateway:
    listeners:
      - { name: http, port: 80, protocol: HTTP }
      - { name: https, port: 443, protocol: HTTPS, hostname: "*.example.com", tls: { certificateRefs: [{ name: wildcard }] } }
    allowedListeners: { namespaces: { from: Same } }
    tls:
      frontend:
        default: {}
        perPort:
          - { port: 443, tls: { validation: { caCertificateRefs: [{ group: "", kind: ConfigMap, name: client-ca }] } } }
  ipFamily: Dual
  ports:
    - { port: 443, http3: { forceRetry: true }, proxyProtocol: { senders: [10.0.0.0/16] }, requestId: Pass, accessLog: { type: File, path: /var/log/edgerush.log } }
    - { port: 80, forwarding: { trustedProxies: [10.0.0.0/8], trustedOnlyHeaders: [X-Forwarded-*] }, accessLog: { type: Stdout } }
  service: { type: LoadBalancer, loadBalancerClass: example.com/lb, loadBalancerSourceRanges: [203.0.113.0/24] }
  resources: { requests: { cpu: 1500m, memory: 1Gi } }')"
admitted "a DataPlane of both" "$(data_plane both '  gateway: { listeners: [{ name: http, port: 8080, protocol: HTTP }] }
  ingress: { httpPort: 80, httpsPort: 443 }
  service: { type: ClusterIP }')"

# The golden tests' resources (crates/planes/tests/golden), which the types read leniently,
# a field the CRD requires read as empty where it is left out: each is what a user may write.
for case in crates/planes/tests/golden/*/; do
    for namespace in $(sed -n 's/.*namespace: \([a-z0-9-]*\).*/\1/p' "$case/resource.yaml" | sort -u); do
        k get namespace "$namespace" >/dev/null 2>&1 || k create namespace "$namespace" >/dev/null
    done
    admitted "the golden case $(basename "$case")" "$(cat "$case/resource.yaml")"
done

# What the ControlPlane's rules refuse.
refused "a ControlPlane whose name is no DNS label" "must be a DNS label" "$(control_plane pay.ments)"
refused "a ControlPlane of a name too long for its Service's" "at most 43" "$(control_plane "$(name 44)")"
refused "a ControlPlane selecting without a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { from: Selector } } }')"
refused "a ControlPlane of its own namespace with a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { selector: { matchLabels: { a: b } } } } }')"
refused "a ControlPlane of no replicas" "spec.replicas" "$(control_plane payments 'spec: { replicas: 0 }')"
refused "a ControlPlane's authority naming no Secret" "spec.authority.secretRef" "$(control_plane payments 'spec: { authority: {} }')"

# What the PlatformDefaults' rules admit and refuse.
platform_defaults() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: PlatformDefaults\nmetadata: { name: %s }\n%s\n' "$1" "${2:-}"
}
# One allowance for the ControlPlanes of namespace `edge`, of `$1`.
allowing() {
    platform_defaults default "spec:
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: edge } } }
      namespaces: $1"
}
admitted "PlatformDefaults of no allowance" "$(platform_defaults default)"
admitted "PlatformDefaults of an allowance of All and one of a selector" "$(platform_defaults default 'spec:
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: edge } } }
      namespaces: { from: All }
    - controlPlanes: { namespaceSelector: { matchExpressions: [{ key: tenant, operator: In, values: [payments] }] } }
      namespaces: { from: Selector, selector: { matchLabels: { team: payments } } }')"
refused "PlatformDefaults not named default" "named default" "$(platform_defaults other)"
refused "an allowance of a selector of no requirement" "at least one requirement" "$(allowing '{ from: Selector, selector: {} }')"
refused "an allowance for the ControlPlanes of every namespace" "at least one requirement" "$(platform_defaults default 'spec:
  watchScopes:
    - controlPlanes: { namespaceSelector: {} }
      namespaces: { from: All }')"
refused "an allowance selecting without a selector" "selector must be given" "$(allowing '{ from: Selector }')"
refused "an allowance of All with a selector" "selector must be given" "$(allowing '{ from: All, selector: { matchLabels: { a: b } } }')"
refused "an allowance of its own namespace alone" "spec.watchScopes[0].namespaces.from" "$(allowing '{ from: Same }')"
refused "an allowance of an operator Kubernetes does not have" "operator must be" "$(allowing '{ from: Selector, selector: { matchExpressions: [{ key: a, operator: Near }] } }')"
admitted "PlatformDefaults of a Gateway-first entry" "$(platform_defaults default 'spec:
  gatewayClasses:
    - name: payments
      dataPlane: { replicas: { min: 2, max: 10 }, service: { type: LoadBalancer } }')"
refused "a Gateway-first entry holding ingress" "unknown field" "$(platform_defaults default 'spec:
  gatewayClasses:
    - name: payments
      dataPlane: { ingress: { httpPort: 80, httpsPort: 443 } }')"
refused "two Gateway-first entries of one class" "Duplicate value" "$(platform_defaults default 'spec:
  gatewayClasses: [{ name: payments }, { name: payments }]')"

# What the DataPlane's rules refuse.
refused "a DataPlane whose name is no DNS label" "must be a DNS label" "$(data_plane pub.lic '  ingress: { httpPort: 80, httpsPort: 443 }')"
refused "a DataPlane of a name too long for its Service's" "at most 48" "$(data_plane "$(name 49)" '  ingress: { httpPort: 80, httpsPort: 443 }')"
refused "a DataPlane of neither API" "serves ingress, gateway or both" "$(data_plane public '  replicas: { min: 1 }')"
refused "a DataPlane naming a Gateway beside a gateway of its own" "serves that Gateway alone" "$(data_plane api '  gatewayRef: { name: web }
  gateway: { listeners: [{ name: http, port: 80, protocol: HTTP }] }')"
refused "a DataPlane naming a Gateway beside ingress" "serves that Gateway alone" "$(data_plane api '  gatewayRef: { name: web }
  ingress: { httpPort: 80, httpsPort: 443 }')"
refused "a DataPlane naming no ControlPlane" "spec.controlPlaneRef" "apiVersion: gateway.edgerush.dev/v1alpha1
kind: DataPlane
metadata: { name: public, namespace: edge }
spec: { ingress: { httpPort: 80, httpsPort: 443 } }"
refused "a gateway of no listener" "spec.gateway.listeners" "$(data_plane api '  gateway: { listeners: [] }')"
refused "a gateway's HTTP listener with TLS" "tls must not be specified" "$(data_plane api '  gateway: { listeners: [{ name: http, port: 80, protocol: HTTP, tls: { certificateRefs: [{ name: c }] } }] }')"
refused "a gateway's two listeners of one name" "Duplicate value" "$(data_plane api '  gateway: { listeners: [{ name: http, port: 80, protocol: HTTP }, { name: http, port: 81, protocol: HTTP }] }')"
refused "Ingress without its HTTPS port" "spec.ingress.httpsPort" "$(data_plane public '  ingress: { httpPort: 80 }')"
refused "Ingress of one port twice" "httpPort and httpsPort must differ" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 80 }')"
refused "replicas of max below min" "max must not be below min" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  replicas: { min: 3, max: 2 }')"
refused "one port's settings given twice" "Duplicate value" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  ports: [{ port: 443, requestId: Pass }, { port: 443, requestId: Generate }]')"
refused "an IP family in lower case" "spec.ipFamily" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  ipFamily: ipv4')"
refused "a file access log of no path" "path must be given" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  ports: [{ port: 80, accessLog: { type: File } }]')"
refused "a ClusterIP Service with a load balancer class" "a LoadBalancer's alone" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { type: ClusterIP, loadBalancerClass: example.com/lb }')"

# A rule on a change: made, then changed.
k apply -f - <<<"$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/lb }')" >/dev/null
refused "a LoadBalancer's class changed" "loadBalancerClass is fixed" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/other }')"
admitted "a LoadBalancer's other fields changed" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/lb, annotations: { a: b } }')"
k delete dataplane -n edge public >/dev/null

# The admission policy. The control plane's ServiceAccount, as the operator names it, holds
# the Service writes of its own namespace, as the operator binds them.
k create serviceaccount -n edge edgerush-controller-payments >/dev/null
k create rolebinding -n edge edgerush-controller-payments-own \
    --clusterrole edgerush-control-plane-own \
    --serviceaccount edge:edgerush-controller-payments >/dev/null
service() {
    printf 'apiVersion: v1\nkind: Service\nmetadata: { name: public, namespace: edge }\nspec:\n  ports: [{ port: 80 }]\n%s\n' "${1:-}"
}
# Created, not applied: apply reads the Service first, which the role does not allow.
as_control_plane=(--as system:serviceaccount:edge:edgerush-controller-payments)
# A policy just made is taken in by the API server within moments: wait until it refuses.
for _ in $(seq 1 30); do
    if ! k "${as_control_plane[@]}" create --dry-run=server -f - <<<"$(service '  externalIPs: [192.0.2.10]')" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
out=$(k "${as_control_plane[@]}" create --dry-run=server -f - <<<"$(service '  externalIPs: [192.0.2.10]')" 2>&1) \
    && fail "a Service with externalIPs is admitted from the control plane's ServiceAccount"
[[ $out == *"may hold no externalIPs"* ]] \
    || fail "a Service with externalIPs is refused from the control plane's ServiceAccount, but not by the policy: $out"
pass "a Service with externalIPs is refused from the control plane's ServiceAccount"
out=$(k "${as_control_plane[@]}" create --dry-run=server -f - <<<"$(service)" 2>&1) \
    || fail "a Service without externalIPs is refused from the control plane's ServiceAccount: $out"
pass "a Service without externalIPs is admitted from the control plane's ServiceAccount"
out=$(k create --dry-run=server -f - <<<"$(service '  externalIPs: [192.0.2.10]')" 2>&1) \
    || fail "a Service with externalIPs is refused from the cluster's admin: $out"
pass "a Service with externalIPs is admitted from another"

# The policy beyond its own namespace. Namespace team-gateway holds a DataPlane generated
# for one of its Gateways, and the role the operator binds there for it, to the same
# ServiceAccount.
k create namespace team-gateway >/dev/null
k create rolebinding -n team-gateway edgerush-controller-payments-generated.web-payments \
    --clusterrole edgerush-control-plane-generated \
    --serviceaccount edge:edgerush-controller-payments >/dev/null
# A Secret or Service of `namespace`, labelled `labels`, its data or spec `rest`.
labelled() {
    printf 'apiVersion: v1\nkind: %s\nmetadata:\n  name: %s\n  namespace: %s\n  labels: { %s }\n%s\n' \
        "$1" "$2" "$3" "$4" "$5"
}
ours='gateway.edgerush.dev/control-plane: payments, gateway.edgerush.dev/control-plane-namespace: edge'
theirs='gateway.edgerush.dev/control-plane: blog, gateway.edgerush.dev/control-plane-namespace: blog-system'
# A data plane's identity, shaped as the control plane writes one.
identity="type: kubernetes.io/tls
stringData: { tls.crt: a, tls.key: b, ca.crt: c }"
# The data plane's Service, selecting `$1`'s pods.
selecting() {
    printf 'spec:\n  ports: [{ port: 80 }]\n  selector: { app.kubernetes.io/component: proxy, gateway.edgerush.dev/control-plane: payments, gateway.edgerush.dev/namespace: team-gateway, gateway.edgerush.dev/data-plane: %s }\n%s' \
        "$1" "${2:-}"
}
identity_secret=edgerush-proxy-web-payments-identity
for _ in $(seq 1 30); do
    if ! k "${as_control_plane[@]}" create --dry-run=server -f - \
        <<<"$(labelled Secret "$identity_secret" team-gateway 'a: b' "$identity")" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
beyond() {
    local what=$1 written=$2 out
    out=$(k "${as_control_plane[@]}" create --dry-run=server -f - <<<"$written" 2>&1) \
        && fail "$what is admitted from the control plane's ServiceAccount beyond its namespace"
    [[ $out == *"outside its own namespace, an EdgeRush control plane writes only"* ]] \
        || fail "$what is refused beyond its namespace, but not by the policy: $out"
    pass "$what is refused from the control plane's ServiceAccount beyond its namespace"
}
beyond "a Secret not labelled as its own" "$(labelled Secret "$identity_secret" team-gateway 'a: b' "$identity")"
beyond "a Secret labelled as another ControlPlane's" "$(labelled Secret "$identity_secret" team-gateway "$theirs" "$identity")"
beyond "a Secret labelled with its name of another namespace" \
    "$(labelled Secret "$identity_secret" team-gateway 'gateway.edgerush.dev/control-plane: payments, gateway.edgerush.dev/control-plane-namespace: blog-system' "$identity")"
# Labelled as its own, but no data plane's identity: a token Kubernetes would fill for a
# ServiceAccount of team-gateway's, and an opaque Secret.
k -n team-gateway create serviceaccount admin >/dev/null
beyond "a ServiceAccount's token Secret labelled as its own" "apiVersion: v1
kind: Secret
metadata:
  name: $identity_secret
  namespace: team-gateway
  labels: { $ours }
  annotations: { kubernetes.io/service-account.name: admin }
type: kubernetes.io/service-account-token"
beyond "an opaque Secret labelled as its own" "$(labelled Secret "$identity_secret" team-gateway "$ours" 'type: Opaque
stringData: { a: b }')"
beyond "a Secret of another name labelled as its own" "$(labelled Secret probe team-gateway "$ours" "$identity")"
beyond "a Service not labelled as its own" "$(labelled Service edgerush-proxy-web-payments team-gateway 'a: b' "$(selecting web-payments)")"
# Labelled as its own, but selecting another workload of team-gateway's, of no selector,
# of another data plane's name, or an ExternalName.
beyond "a Service labelled as its own selecting another workload" "$(labelled Service edgerush-proxy-web-payments team-gateway "$ours" 'spec:
  ports: [{ port: 80 }]
  selector: { app: database }')"
beyond "a Service labelled as its own selecting nothing" "$(labelled Service edgerush-proxy-web-payments team-gateway "$ours" 'spec:
  ports: [{ port: 80 }]')"
beyond "a Service labelled as its own not named for the pods it selects" "$(labelled Service edgerush-proxy-other team-gateway "$ours" "$(selecting web-payments)")"
beyond "an ExternalName Service labelled as its own" "$(labelled Service edgerush-proxy-web-payments team-gateway "$ours" "$(selecting web-payments '
  type: ExternalName
  externalName: example.com')")"
out=$(k "${as_control_plane[@]}" create --dry-run=server -f - \
    <<<"$(labelled Service edgerush-proxy-web-payments team-gateway "$ours" "$(selecting web-payments)")" 2>&1) \
    || fail "the data plane's Service, labelled as its own, is refused beyond its namespace: $out"
pass "the data plane's Service, labelled as its own, is admitted beyond its namespace"
# Its own Secret, made and then written again; another's, made by its team, written over.
k "${as_control_plane[@]}" create -f - \
    <<<"$(labelled Secret "$identity_secret" team-gateway "$ours" "$identity")" >/dev/null \
    || fail "the data plane's identity Secret, labelled as its own, is refused beyond its namespace"
pass "the data plane's identity Secret, labelled as its own, is admitted beyond its namespace"
out=$(k "${as_control_plane[@]}" replace -f - \
    <<<"$(labelled Secret "$identity_secret" team-gateway "$ours" 'type: kubernetes.io/tls
stringData: { tls.crt: d, tls.key: e, ca.crt: f }')" 2>&1) \
    || fail "its own identity Secret written again is refused beyond its namespace: $out"
pass "its own identity Secret written again is admitted beyond its namespace"
k create -f - <<<"$(labelled Secret edgerush-proxy-teams-own-identity team-gateway 'a: b' "$identity")" >/dev/null
out=$(k "${as_control_plane[@]}" replace -f - \
    <<<"$(labelled Secret edgerush-proxy-teams-own-identity team-gateway "$ours" "$identity")" 2>&1) \
    && fail "another's Secret relabelled as its own is admitted from the control plane's ServiceAccount"
[[ $out == *"labelled with its ControlPlane's name and namespace"* ]] \
    || fail "another's Secret relabelled is refused, but not by the policy: $out"
pass "another's Secret relabelled as its own is refused from the control plane's ServiceAccount"
# In its own namespace, and from anyone else, nothing of it applies.
out=$(k "${as_control_plane[@]}" create --dry-run=server -f - <<<"$(labelled Secret probe edge 'a: b' 'type: Opaque
stringData: { a: b }')" 2>&1) \
    || fail "a Secret not labelled as its own is refused in the control plane's own namespace: $out"
pass "a Secret not labelled as its own is admitted in the control plane's own namespace"
out=$(k create --dry-run=server -f - <<<"$(labelled Secret probe team-gateway 'a: b' 'type: Opaque
stringData: { a: b }')" 2>&1) \
    || fail "a Secret not labelled as a control plane's is refused from the cluster's admin: $out"
pass "a Secret not labelled as a control plane's is admitted from another"
k delete namespace team-gateway --wait=false >/dev/null

# The policy on generated DataPlanes. The operator's ServiceAccount, as operator.yaml names
# it, given every right here, as the policy and not RBAC is what is judged; and one of its
# name in another namespace, which is anyone's.
k create namespace edgerush-system >/dev/null
k create serviceaccount -n edgerush-system edgerush-operator >/dev/null
k create serviceaccount -n edge edgerush-operator >/dev/null
k create clusterrolebinding edgerush-operator-planes-test --clusterrole cluster-admin \
    --serviceaccount edgerush-system:edgerush-operator \
    --serviceaccount edge:edgerush-operator >/dev/null
as_operator=(--as system:serviceaccount:edgerush-system:edgerush-operator)
as_impostor=(--as system:serviceaccount:edge:edgerush-operator)
generated() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: DataPlane\nmetadata: { name: web-payments, namespace: edge }\nspec:\n  controlPlaneRef: { name: payments%s }\n%s\n' "$1" "$2"
}
naming_a_gateway=$(generated '' '  gatewayRef: { name: web }')
elsewhere=$(generated ', namespace: edge-system' '  ingress: { httpPort: 80, httpsPort: 443 }')
for _ in $(seq 1 30); do
    if ! k create --dry-run=server -f - <<<"$naming_a_gateway" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
refused "a DataPlane naming a Gateway, written by hand" "is the operator's to write" "$naming_a_gateway"
refused "a DataPlane naming a ControlPlane of another namespace, written by hand" "is the operator's to write" "$elsewhere"
out=$(k "${as_impostor[@]}" create --dry-run=server -f - <<<"$naming_a_gateway" 2>&1) \
    && fail "a DataPlane naming a Gateway is admitted from a ServiceAccount of the operator's name in another namespace"
[[ $out == *"is the operator's to write"* ]] \
    || fail "a DataPlane naming a Gateway is refused from an impostor, but not by the policy: $out"
pass "a DataPlane naming a Gateway is refused from a ServiceAccount of the operator's name in another namespace"
for written in "$naming_a_gateway" "$elsewhere"; do
    out=$(k "${as_operator[@]}" create --dry-run=server -f - <<<"$written" 2>&1) \
        || fail "a generated DataPlane is refused from the operator's ServiceAccount: $out"
done
pass "a DataPlane naming a Gateway, or a ControlPlane of another namespace, is admitted from the operator's ServiceAccount"
admitted "a DataPlane naming its own namespace's ControlPlane outright" "$(generated ', namespace: edge' '  ingress: { httpPort: 80, httpsPort: 443 }')"
# Made by the operator, then changed by another.
k "${as_operator[@]}" create -f - <<<"$naming_a_gateway" >/dev/null
out=$(k patch dataplane -n edge web-payments --type merge -p '{"spec":{"replicas":{"min":5}}}' 2>&1) \
    && fail "a generated DataPlane's spec is changed by hand"
[[ $out == *"is the operator's to write"* ]] \
    || fail "a generated DataPlane's spec changed by hand is refused, but not by the policy: $out"
pass "a generated DataPlane's spec changed by hand is refused"
out=$(k patch dataplane -n edge web-payments --type merge -p '{"spec":{"gatewayRef":null,"ingress":{"httpPort":80,"httpsPort":443}}}' 2>&1) \
    && fail "a generated DataPlane is turned into another by hand"
pass "a generated DataPlane turned into another by hand is refused"
out=$(k label dataplane -n edge web-payments team=payments 2>&1) \
    || fail "a generated DataPlane's labels changed by hand are refused: $out"
pass "a generated DataPlane's labels changed by hand are admitted"
k "${as_operator[@]}" delete dataplane -n edge web-payments >/dev/null

echo "every check passed"
