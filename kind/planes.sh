#!/usr/bin/env bash
# What the install ships, judged by a real API server in a kind cluster (docs/05 §1, §2;
# docs/10): the ControlPlane and DataPlane CRDs, the four ClusterRoles and the admission
# policy on a control plane's Services, applied as a user would.
#
#   kind/planes.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/planes.sh 1.37   the same, the cluster left for a look
#
# Then:
#   - a ControlPlane and DataPlanes of each kind (Ingress, Gateway API, both) are admitted,
#     and so is every golden case's resource of crates/planes;
#   - each resource the CRDs' schema and CEL rules are to refuse is refused, and says why:
#     a name that is no DNS label, a watch scope's selector without Selector or Selector
#     without one, no replicas, a DataPlane with neither ingress nor gateway, a gateway of
#     no listener or of one the Gateway would refuse, Ingress short of a port or with one
#     port twice, max below min, one port's settings given twice, an enumerated value
#     written in another case, a file access log of no path, a LoadBalancer's fields on
#     another type, and, on an update, a LoadBalancer's class changed;
#   - a Service holding externalIPs is refused written as a control plane's ServiceAccount,
#     and admitted written by another; one without them is admitted from either.
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
k apply --server-side -f "$install/gateway.edgerush.dev_controlplanes.yaml" \
    -f "$install/gateway.edgerush.dev_dataplanes.yaml" \
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev >/dev/null
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

control_plane() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: ControlPlane\nmetadata: { name: %s, namespace: edge }\n%s\n' "$1" "${2:-}"
}
data_plane() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: DataPlane\nmetadata: { name: %s, namespace: edge }\nspec:\n  controlPlaneRef: { name: payments }\n%s\n' "$1" "$2"
}

# What is to be admitted.
admitted "a ControlPlane of every default" "$(control_plane payments)"
admitted "a ControlPlane watching what it selects" "$(control_plane payments 'spec:
  watch: { namespaces: { from: Selector, selector: { matchLabels: { team: payments } } } }
  replicas: 2
  authority: { secretRef: { name: payments-ca } }
  webhook: { enabled: false }')"
admitted "a DataPlane of Ingress" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }')"
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
refused "a ControlPlane selecting without a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { from: Selector } } }')"
refused "a ControlPlane of its own namespace with a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { selector: { matchLabels: { a: b } } } } }')"
refused "a ControlPlane of no replicas" "spec.replicas" "$(control_plane payments 'spec: { replicas: 0 }')"
refused "a ControlPlane's authority naming no Secret" "spec.authority.secretRef" "$(control_plane payments 'spec: { authority: {} }')"

# What the DataPlane's rules refuse.
refused "a DataPlane whose name is no DNS label" "must be a DNS label" "$(data_plane pub.lic '  ingress: { httpPort: 80, httpsPort: 443 }')"
refused "a DataPlane of neither API" "serves ingress, gateway or both" "$(data_plane public '  replicas: { min: 1 }')"
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

echo "every check passed"
