#!/usr/bin/env bash
# The upstream Gateway API conformance suite against EdgeRush, in a kind cluster
# (docs/04 §7, 10). Arguments after the Kubernetes version are the suite's own flags:
#
#   kind/conformance.sh 1.37 --conformance-profiles=GATEWAY-HTTP
#   kind/conformance.sh 1.37 --conformance-profiles=GATEWAY-HTTP --run-test=HTTPRouteSimpleSameNamespace
#   KEEP=1 kind/conformance.sh 1.37 ...      the cluster left for a look
#
# The suite is given no features: it reads them from the class's status.supportedFeatures,
# written from the controller's table (docs/04 §6, §7), so what it tests is what the class
# claims. A profile not yet in the table is tried with --supported-features=... given here,
# as is a single test: with --run-test the suite reads no class.
#
# EdgeRush is installed through the operator: a ControlPlane `edgerush` watching every
# namespace, whose class, `edgerush`, is the suite's, its Gateways' data planes on ClusterIP
# Services through the class's PlatformDefaults entry. The suite runs as a Pod in the
# cluster, as only Pods reach a cluster IP; its image (conformance/Dockerfile) holds the Go
# toolchain that builds it, so this host needs Docker and kind alone. What the suite says is
# in target/kind/conformance-<version>.log, its report in
# target/kind/conformance-<version>-report.yaml; the exit status is the suite's. Nothing here
# reads or writes the user's own kubeconfig: every kubectl is given the cluster's.
set -euo pipefail

cd "$(dirname "$0")/.."

# What the data plane's image is: this host's distribution, the binary built on it.
BASE="ubuntu:26.04@sha256:c098726dd9b9733f3e885f8de0ef375206a311b0a317eadc40d347b1115fb944"
# The suite's whole run, its parallel tests' 180 s waits many times over.
TIMEOUT_MINUTES=60

version=${1:?usage: kind/conformance.sh <Kubernetes version> [flags for the suite...]}
shift
name="edgerush-${version//./-}"
log="target/kind/conformance-$version.log"
report="target/kind/conformance-$version-report.yaml"
kubeconfig=$(kind/cluster.sh up "$version")
export KUBECONFIG="$kubeconfig"
k() { kubectl --kubeconfig "$kubeconfig" "$@"; }
finish() {
    if [ -n "${logs:-}" ]; then kill "$logs" 2>/dev/null || true; fi
    if [ "${KEEP:-}" != 1 ]; then kind/cluster.sh down "$version" >/dev/null; fi
}
trap finish EXIT

# What the operator and the controller last said, with the failure, for a look.
fail() {
    echo "FAIL: $*"
    echo "-- the operator's last lines:"
    k -n edgerush-system logs deploy/edgerush-operator --tail=20 2>&1 | sed 's/^/   /'
    echo "-- the controller's last lines:"
    k -n edgerush logs deploy/edgerush-controller-edgerush --tail=20 2>&1 | sed 's/^/   /'
    exit 1
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

echo "== building the binaries and their image"
cargo build -q --release --locked -p edgerush -p edgerush-ratelimit-service
image=target/kind/image
rm -rf "$image" && mkdir -p "$image"
cp target/release/edgerush target/release/edgerush-ratelimit-service "$image/"
sed "s|BASE|$BASE|" kind/discovery/Dockerfile > "$image/Dockerfile"
docker build -q -t edgerush:dev "$image" >/dev/null
kind load docker-image edgerush:dev --name "$name" >/dev/null

echo "== building the conformance runner's image"
docker build -q -t edgerush-conformance:dev conformance >/dev/null
kind load docker-image edgerush-conformance:dev --name "$name" >/dev/null

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
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: PlatformDefaults
metadata: { name: default }
spec:
  gatewayClasses:
    - name: edgerush
      dataPlane: { service: { type: ClusterIP } }
  watchScopes:
    - controlPlanes: { namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: edgerush } } }
      namespaces: { from: All }
EOF
k create namespace edgerush >/dev/null
k apply -f - >/dev/null <<'EOF'
apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: { name: edgerush, namespace: edgerush }
spec:
  watch: { namespaces: { from: All } }
  webhook: { enabled: false }
EOF
condition_is controlplane/edgerush edgerush Programmed "True Programmed" 180 ||
    fail "ControlPlane edgerush not programmed: $(condition controlplane/edgerush edgerush Programmed)"
condition_is gatewayclass/edgerush "" Accepted "True Accepted" 60 ||
    fail "GatewayClass edgerush not accepted: $(condition gatewayclass/edgerush "" Accepted)"

echo "== the suite: $*"
# The runner may do whatever the suite asks of the cluster, which is made for it alone.
k -n default create serviceaccount conformance >/dev/null
k create clusterrolebinding conformance --clusterrole=cluster-admin \
    --serviceaccount=default:conformance >/dev/null
# The suite's status is written beside its report, and the Pod kept running, so that both
# can be read from it once the suite is done.
k -n default run conformance --image=edgerush-conformance:dev --image-pull-policy=Never \
    --restart=Never --overrides='{"spec":{"serviceAccountName":"conformance"}}' --command -- \
    sh -c 'conformance.test "$@"; echo $? > /tmp/status; exec sleep infinity' sh \
    -test.v -test.run '^TestConformance$' "-test.timeout=${TIMEOUT_MINUTES}m" \
    --gateway-class=edgerush --report-output=/tmp/report.yaml \
    --organization=edgerush-dev --project=edgerush --url=https://github.com/edgerush-dev/edgerush \
    "--version=$(git describe --always --dirty)" --contact=https://github.com/edgerush-dev/edgerush/issues \
    "$@" >/dev/null
k -n default wait --for condition=Ready pod/conformance --timeout=120s >/dev/null ||
    fail "the runner's Pod is not Ready: $(k -n default get pod conformance --no-headers 2>&1)"
k -n default logs -f conformance > "$log" 2>&1 &
logs=$!
echo "   the suite's log: $log"

for _ in $(seq 1 $((TIMEOUT_MINUTES * 12 + 12))); do
    status=$(k -n default exec conformance -- cat /tmp/status 2>/dev/null) && break
    phase=$(k -n default get pod conformance -o jsonpath='{.status.phase}')
    [ "$phase" = Running ] || fail "the runner's Pod is $phase, the suite unfinished"
    sleep 5
done
[ -n "${status:-}" ] || fail "the suite unfinished after $TIMEOUT_MINUTES minutes"
echo "== the suite's exit status: $status"
k -n default exec conformance -- cat /tmp/report.yaml > "$report" 2>/dev/null ||
    { rm -f "$report"; fail "the suite wrote no report"; }
echo "   the report: $report"
exit "$status"
