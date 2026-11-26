#!/usr/bin/env bash
# What the install ships, judged by a real API server in a kind cluster (docs/05 §1, §2;
# docs/10): the ControlPlane, DataPlane, PlatformDefaults and RateLimitPolicy CRDs, the five
# ClusterRoles and the admission policies, on a control plane's Services, on what it writes beyond its own
# namespace, on generated DataPlanes and on a DataPlane's key cap, applied as a user would; beside Gateway API's
# CRDs, which the golden cases' Gateways need.
#
#   kind/planes.sh 1.37          make the cluster, run every check, delete the cluster
#   KEEP=1 kind/planes.sh 1.37   the same, the cluster left for a look
#
# Then:
#   - a ControlPlane and DataPlanes of each kind (Ingress, Gateway API, both) are admitted,
#     and so is every golden case's resource of crates/planes, and DataPlanes limiting
#     locally under a limit of each model, with rate limiting disabled, synchronously, and
#     asynchronously of every default and of each setting; a ControlPlane's rate-limiting
#     service of every default, which are three pods, 256 shards and two copies, of every
#     field, and, dropped, declared again of other shards; and RateLimitPolicies on each
#     level, whole and by section, of limits, of ignore alone, of both, of either beside
#     the other empty, and of ["*"];
#   - each resource the CRDs' schema and CEL rules are to refuse is refused, and says why:
#     a name that is no DNS label or too long for its Service's, a watch scope's selector
#     without Selector or Selector without one, no replicas, a DataPlane with neither
#     ingress nor gateway, a gateway of no listener or of one the Gateway would refuse,
#     Ingress short of a port or with one port twice, max below min, one port's settings
#     given twice, an enumerated value written in another case, a file access log of no
#     path, a LoadBalancer's fields on another type, and, on an update, a LoadBalancer's
#     class changed; a rate-limiting service of no pods, of no shards or more than a pod
#     takes, of fewer than no copies, or whose keys may take nothing, and, on an update,
#     its shards changed or left to their default; a PlatformDefaults not named default,
#     an allowance of a selector of no requirement, for ControlPlanes or for what they
#     watch, with a selector where it takes none or none where it takes one, of a scope it
#     cannot allow, or of an operator Kubernetes does not have; a DataPlane naming a
#     Gateway beside a gateway or ingress of its own; a Gateway-first entry holding
#     ingress, or two entries of one class; rate limiting of a mode the DataPlane does not
#     take or of none, a bound on keys with it disabled or that, with half as much again,
#     its memory limit cannot hold (by an admission policy), the synchronous mode without how
#     a request waits or that given in another mode, a wait without what decides an owner
#     unheard, that in lower case, or of no time, a reconnect after no timeout, a wait
#     without how pods are checked, checks of no interval, of no time, marking a pod down
#     by no miss or not saying how many, the asynchronous mode's settings in
#     another or a cycle of no time, a
#     limit of no model or of two, keyed by what it does not know, a rate or a
#     window of no unit, a rolling window of one slot, a method in lower case, a path
#     prefix that is no path, and two limits of one name; a RateLimitPolicy of no target or
#     of more than 16, of a kind it does not take or one of another group, of targets of
#     two levels, of a section on some alone, or naming one twice, naming an Ingress's
#     section, of neither a limit nor a name to ignore (either list may be empty), of "*"
#     beside a name, ignoring a name twice, or of a limit a DataPlane's rules would refuse;
#   - a naming of the rate-limiting service's pods in a ControlPlane's status is read back
#     as it was written, a member id of 64 bits among it, and is refused of no generation,
#     of a member id that is no number or begins with a zero, or of no table;
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
#     is refused, its labels changed admitted;
#   - a control plane's ServiceAccount adds Served to the status of a DataPlane of its own,
#     in its namespace or generated for it, and is refused Served on another control
#     plane's, and any change to the other conditions: Accepted changed or of another
#     generation, Programmed taken away, a condition of another type added; the operator
#     changes them as ever.
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
    -f "$install/gateway.edgerush.dev_ratelimitpolicies.yaml" \
    -f "$install/roles.yaml" -f "$install/admission.yaml" >/dev/null
k wait --for condition=Established --timeout=60s \
    crd/controlplanes.gateway.edgerush.dev crd/dataplanes.gateway.edgerush.dev \
    crd/platformdefaults.gateway.edgerush.dev crd/ratelimitpolicies.gateway.edgerush.dev \
    crd/gateways.gateway.networking.k8s.io >/dev/null
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
admitted "a ControlPlane of the longest name" "$(control_plane "$(name 38)")"
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
refused "a ControlPlane of a name too long for its Services'" "at most 38" "$(control_plane "$(name 39)")"
refused "a ControlPlane selecting without a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { from: Selector } } }')"
refused "a ControlPlane of its own namespace with a selector" "selector must be given" "$(control_plane payments 'spec: { watch: { namespaces: { selector: { matchLabels: { a: b } } } } }')"
refused "a ControlPlane of no replicas" "spec.replicas" "$(control_plane payments 'spec: { replicas: 0 }')"
refused "a ControlPlane's authority naming no Secret" "spec.authority.secretRef" "$(control_plane payments 'spec: { authority: {} }')"

# A ControlPlane's rate-limiting service: `serving <service>`, a ControlPlane declaring it.
serving() { control_plane payments "spec: { rateLimiting: { service: $1 } }"; }
admitted "a rate-limiting service of every default" "$(serving '{}')"
defaults=$(k apply --dry-run=server -o jsonpath='{.spec.rateLimiting.service}' -f - <<<"$(serving '{}')")
[ "$defaults" = '{"copies":2,"forwarderDecides":true,"replicas":3,"shards":256}' ] \
    || fail "a rate-limiting service left to its defaults is $defaults"
pass "a rate-limiting service left to its defaults has three pods, 256 shards and two copies"
admitted "a rate-limiting service of every field" "$(serving '{ replicas: 5, resources: { requests: { cpu: "2" }, limits: { memory: 16Gi } }, shards: 1024, copies: 1, forwarderDecides: false, maxKeyBytes: 12884901888 }')"
refused "a rate-limiting service of no pods" "spec.rateLimiting.service.replicas" "$(serving '{ replicas: 0 }')"
refused "a rate-limiting service of no shards" "spec.rateLimiting.service.shards" "$(serving '{ shards: 0 }')"
refused "a rate-limiting service of more shards than a pod takes" "spec.rateLimiting.service.shards" "$(serving '{ shards: 65536 }')"
refused "a rate-limiting service of fewer than no copies" "spec.rateLimiting.service.copies" "$(serving '{ copies: -1 }')"
refused "a rate-limiting service whose keys may take nothing" "spec.rateLimiting.service.maxKeyBytes" "$(serving '{ maxKeyBytes: 0 }')"

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
# A DataPlane's rate limiting: `limiting <rateLimiting>`, a DataPlane of Ingress with it.
limiting() {
    data_plane public "  ingress: { httpPort: 80, httpsPort: 443 }
  rateLimiting: $1"
}
# `limit <fields>`: one limit, of name per-client, with `fields`, limited locally.
limit() {
    limiting "{ mode: Local, global: [{ name: per-client, $1 }] }"
}
admitted "a DataPlane limiting locally under a limit of each model" "$(limiting '
    mode: Local
    maxKeyBytes: 1048576
    global:
      - { name: per-client, key: Client, model: { bucket: { rate: 100/s, burst: 200 } } }
      - { name: writes, match: { methods: [POST], pathPrefix: /v1/ }, key: Client, model: { fixedWindow: { limit: 10, window: 1m } }, dryRun: true }
      - { name: hourly, key: Client, model: { rollingWindow: { limit: 100, window: 1h, slots: 60 } } }')"
admitted "a DataPlane whose rate limiting is disabled" "$(limiting '{ mode: Disabled }')"
waits='{ timeoutMs: 20, whenUnreachable: Local, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } }'
admitted "a DataPlane limiting synchronously" "$(limiting "
    mode: Synchronous
    maxKeyBytes: 1048576
    synchronous: $waits
    global: [{ name: per-client, key: Client, model: { bucket: { rate: 100/s, burst: 200 } } }]")"
reconnect=$(k apply --dry-run=server -o jsonpath='{.spec.rateLimiting.synchronous.timeoutsBeforeReconnect}' -f - <<<"$(limiting "{ mode: Synchronous, synchronous: $waits }")")
[ "$reconnect" = 2 ] || fail "a synchronous wait left to its default reconnects after $reconnect"
pass "a synchronous wait left to its default reconnects after two timeouts in a row"
admitted "a synchronous wait reconnecting after one timeout" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, timeoutsBeforeReconnect: 1, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } } }')"
admitted "a DataPlane limiting asynchronously, of every default" "$(limiting '{ mode: Asynchronous }')"
admitted "a DataPlane limiting asynchronously, of each setting" "$(limiting '{ mode: Asynchronous, asynchronous: { cycleMs: 20, pollQuietKeys: true, scheduleKeys: false } }')"
refused "a rate-limiting mode the DataPlane does not take" "spec.rateLimiting.mode" "$(limiting '{ mode: Off }')"
refused "rate limiting of no mode" "spec.rateLimiting.mode" "$(limiting '{ global: [] }')"
refused "a bound on keys with rate limiting disabled" "not Disabled" "$(limiting '{ mode: Disabled, maxKeyBytes: 1048576 }')"
# `capped <memory> <maxKeyBytes>`: a DataPlane limiting locally within that cap, its pods'
# memory limit that. A cap and half as much again must be below the limit (06 §8), as an
# admission policy has it: 512 Mi holds 357,913,941 bytes so, and no more.
capped() {
    data_plane public "  ingress: { httpPort: 80, httpsPort: 443 }
  resources: { limits: { memory: $1 } }
  rateLimiting: { mode: Local, maxKeyBytes: $2 }"
}
# A policy is enforced some seconds after it is made: until then, the cap it refuses is let
# through. Waited for here, at most a minute, so that what follows judges the policy itself.
for _ in $(seq 30); do
    k apply --dry-run=server -f - <<<"$(capped 512Mi 357913942)" >/dev/null 2>&1 || break
    sleep 2
done
admitted "a bound on keys its memory limit holds with half as much again" "$(capped 512Mi 357913941)"
admitted "a bound on keys with no memory limit" "$(limiting '{ mode: Local, maxKeyBytes: 1099511627776 }')"
admitted "a bound on keys its memory limit, in bytes, holds with half as much again" "$(capped 536870912 357913941)"
refused "a bound on keys its memory limit cannot hold with half as much again" "half as much again" "$(capped 512Mi 357913942)"
refused "a bound on keys its memory limit, in bytes, cannot hold with half as much again" "half as much again" "$(capped 536870912 357913942)"
refused "a bound on keys its memory limit, quoted bytes, cannot hold with half as much again" "half as much again" "$(capped '"536870912"' 357913942)"
refused "the synchronous mode without how a request waits" "synchronous must be given" "$(limiting '{ mode: Synchronous }')"
refused "how a request waits for its owner, in another mode" "synchronous must be given" "$(limiting "{ mode: Asynchronous, synchronous: $waits }")"
refused "a synchronous wait without what decides an owner unheard" "spec.rateLimiting.synchronous.whenUnreachable" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } } }')"
refused "what decides an owner unheard, in lower case" "spec.rateLimiting.synchronous.whenUnreachable" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: allow, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } } }')"
refused "a wait for the owner of no time" "spec.rateLimiting.synchronous.timeoutMs" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 0, whenUnreachable: Allow, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } } }')"
refused "a reconnect after no timeout" "spec.rateLimiting.synchronous.timeoutsBeforeReconnect" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, timeoutsBeforeReconnect: 0, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 3 } } }')"
refused "a synchronous wait without how pods are checked" "spec.rateLimiting.synchronous.healthCheck" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow } }')"
refused "checks of no interval" "spec.rateLimiting.synchronous.healthCheck.intervalMs" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, healthCheck: { intervalMs: 0, timeoutMs: 1000, misses: 3 } } }')"
refused "a check of no time" "spec.rateLimiting.synchronous.healthCheck.timeoutMs" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, healthCheck: { intervalMs: 100, timeoutMs: 0, misses: 3 } } }')"
refused "a pod marked down by no miss" "spec.rateLimiting.synchronous.healthCheck.misses" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, healthCheck: { intervalMs: 100, timeoutMs: 1000, misses: 0 } } }')"
refused "checks without how many misses" "spec.rateLimiting.synchronous.healthCheck.misses" "$(limiting '{ mode: Synchronous, synchronous: { timeoutMs: 20, whenUnreachable: Allow, healthCheck: { intervalMs: 100, timeoutMs: 1000 } } }')"
refused "the asynchronous mode's settings in another" "asynchronous is for the Asynchronous mode alone" "$(limiting "{ mode: Synchronous, synchronous: $waits, asynchronous: { cycleMs: 20 } }")"
refused "a cycle of no time" "spec.rateLimiting.asynchronous.cycleMs" "$(limiting '{ mode: Asynchronous, asynchronous: { cycleMs: 0 } }')"
refused "a limit of no model" "one of bucket, fixedWindow and rollingWindow" "$(limit 'key: Client, model: {}')"
refused "a limit of two models" "one of bucket, fixedWindow and rollingWindow" "$(limit 'key: Client, model: { bucket: { rate: 1/s, burst: 1 }, fixedWindow: { limit: 1, window: 1s } }')"
refused "a limit keyed by what it does not know" "spec.rateLimiting.global[0].key" "$(limit 'key: Header, model: { bucket: { rate: 1/s, burst: 1 } }')"
refused "a rate of no unit" "spec.rateLimiting.global[0].model.bucket.rate" "$(limit 'key: Client, model: { bucket: { rate: "100", burst: 1 } }')"
refused "a window of no unit" "spec.rateLimiting.global[0].model.fixedWindow.window" "$(limit 'key: Client, model: { fixedWindow: { limit: 1, window: "60" } }')"
refused "a rolling window of one slot" "spec.rateLimiting.global[0].model.rollingWindow.slots" "$(limit 'key: Client, model: { rollingWindow: { limit: 1, window: 1s, slots: 1 } }')"
refused "a method in lower case" "spec.rateLimiting.global[0].match.methods[0]" "$(limit 'match: { methods: [post] }, key: Client, model: { bucket: { rate: 1/s, burst: 1 } }')"
refused "a path prefix that is no path" "spec.rateLimiting.global[0].match.pathPrefix" "$(limit 'match: { pathPrefix: v1 }, key: Client, model: { bucket: { rate: 1/s, burst: 1 } }')"
refused "two limits of one name" "Duplicate value" "$(limiting '{ mode: Local, global: [{ name: a, key: Client, model: { bucket: { rate: 1/s, burst: 1 } } }, { name: a, key: Client, model: { bucket: { rate: 2/s, burst: 1 } } }] }')"

# What a RateLimitPolicy's rules admit and refuse: `policy <spec>`, a policy of `spec`'s
# fields, each line indented as spec's.
policy() {
    printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: RateLimitPolicy\nmetadata: { name: checkout, namespace: edge }\nspec:\n%s\n' "$1"
}
# `target <kind> <name> [section]`: a target of Gateway API's, or Ingress's.
target() {
    local group=gateway.networking.k8s.io
    if [ "$1" = Ingress ]; then group=networking.k8s.io; fi
    printf '{ group: %s, kind: %s, name: %s%s }' "$group" "$1" "$2" "${3:+, sectionName: $3}"
}
per_client='  limits: [{ name: per-client, key: Client, model: { bucket: { rate: 10/s, burst: 10 } } }]'
admitted "a RateLimitPolicy on a Gateway" "$(policy "  targetRefs: [$(target Gateway web)]
$per_client")"
admitted "a RateLimitPolicy on two listeners" "$(policy "  targetRefs: [$(target Gateway web http), $(target Gateway web https)]
$per_client")"
admitted "a RateLimitPolicy on a ListenerSet" "$(policy "  targetRefs: [$(target ListenerSet team)]
$per_client")"
admitted "a RateLimitPolicy on a ListenerSet's listener" "$(policy "  targetRefs: [$(target ListenerSet team http)]
$per_client")"
admitted "a RateLimitPolicy on an HTTPRoute and a GRPCRoute" "$(policy "  targetRefs: [$(target HTTPRoute shop), $(target GRPCRoute orders)]
$per_client")"
admitted "a RateLimitPolicy on rules of an HTTPRoute and a GRPCRoute" "$(policy "  targetRefs: [$(target HTTPRoute shop checkout), $(target GRPCRoute orders place)]
$per_client")"
admitted "a RateLimitPolicy on an Ingress, of ignore alone" "$(policy "  targetRefs: [$(target Ingress shop)]
  ignore: [platform-per-ip]")"
admitted "a RateLimitPolicy of limits and ignore" "$(policy "  targetRefs: [$(target HTTPRoute shop checkout)]
  limits:
    - { name: writes, match: { methods: [POST], pathPrefix: /cart/ }, key: Client, model: { fixedWindow: { limit: 10, window: 1m } }, dryRun: true }
    - { name: hourly, key: Client, model: { rollingWindow: { limit: 100, window: 1h, slots: 60 } } }
  ignore: [platform-per-ip, platform-writes]")"
admitted "a RateLimitPolicy of limits, ignoring nothing" "$(policy "  targetRefs: [$(target Gateway web)]
$per_client
  ignore: []")"
admitted "a RateLimitPolicy of no limit, of ignore" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: []
  ignore: [platform-per-ip]")"
admitted "a RateLimitPolicy ignoring every less specific limit" "$(policy "  targetRefs: [$(target HTTPRoute health)]
  ignore: [\"*\"]")"
# `routes <n>`: targets of `n` HTTPRoutes, each its own.
routes() {
    local i targets
    targets=$(target HTTPRoute r1)
    for i in $(seq 2 "$1"); do targets="$targets, $(target HTTPRoute "r$i")"; done
    printf '%s' "$targets"
}
admitted "a RateLimitPolicy of 16 targets" "$(policy "  targetRefs: [$(routes 16)]
$per_client")"
refused "a RateLimitPolicy of no target" "spec.targetRefs" "$(policy "  targetRefs: []
$per_client")"
refused "a RateLimitPolicy naming no targets" "spec.targetRefs" "$(policy "$per_client")"
refused "a RateLimitPolicy of 17 targets" "spec.targetRefs" "$(policy "  targetRefs: [$(routes 17)]
$per_client")"
refused "a RateLimitPolicy on a Service" "a target is a Gateway, ListenerSet, HTTPRoute or GRPCRoute" "$(policy "  targetRefs: [{ group: '', kind: Service, name: shop }]
$per_client")"
refused "a RateLimitPolicy on a TLSRoute" "a target is a Gateway, ListenerSet, HTTPRoute or GRPCRoute" "$(policy "  targetRefs: [$(target TLSRoute shop)]
$per_client")"
refused "a RateLimitPolicy on an Ingress of Gateway API's group" "a target is a Gateway, ListenerSet, HTTPRoute or GRPCRoute" "$(policy "  targetRefs: [{ group: gateway.networking.k8s.io, kind: Ingress, name: shop }]
$per_client")"
refused "a RateLimitPolicy on a Gateway of Ingress's group" "a target is a Gateway, ListenerSet, HTTPRoute or GRPCRoute" "$(policy "  targetRefs: [{ group: networking.k8s.io, kind: Gateway, name: web }]
$per_client")"
refused "a RateLimitPolicy on a Gateway and a route" "of one level" "$(policy "  targetRefs: [$(target Gateway web), $(target HTTPRoute shop)]
$per_client")"
refused "a RateLimitPolicy on a Gateway and a ListenerSet" "of one level" "$(policy "  targetRefs: [$(target Gateway web), $(target ListenerSet team)]
$per_client")"
refused "a RateLimitPolicy on a route and an Ingress" "of one level" "$(policy "  targetRefs: [$(target HTTPRoute shop), $(target Ingress shop)]
$per_client")"
refused "a RateLimitPolicy on a listener and a Gateway" "name a section, or none" "$(policy "  targetRefs: [$(target Gateway web http), $(target Gateway api)]
$per_client")"
refused "a RateLimitPolicy on a rule and a route" "name a section, or none" "$(policy "  targetRefs: [$(target HTTPRoute shop checkout), $(target GRPCRoute orders)]
$per_client")"
refused "a RateLimitPolicy naming a target twice" "named once" "$(policy "  targetRefs: [$(target HTTPRoute shop), $(target HTTPRoute shop)]
$per_client")"
refused "a RateLimitPolicy naming a rule twice" "named once" "$(policy "  targetRefs: [$(target HTTPRoute shop checkout), $(target HTTPRoute shop checkout)]
$per_client")"
refused "a RateLimitPolicy naming an Ingress's section" "an Ingress has no sections" "$(policy "  targetRefs: [$(target Ingress shop rule)]
$per_client")"
refused "a RateLimitPolicy of neither limits nor ignore" "a limit, a name to ignore, or both" "$(policy "  targetRefs: [$(target Gateway web)]")"
refused "a RateLimitPolicy of no limit" "a limit, a name to ignore, or both" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: []")"
refused "a RateLimitPolicy ignoring nothing" "a limit, a name to ignore, or both" "$(policy "  targetRefs: [$(target Gateway web)]
  ignore: []")"
refused "a RateLimitPolicy of no limit, ignoring nothing" "a limit, a name to ignore, or both" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: []
  ignore: []")"
refused "a RateLimitPolicy ignoring \"*\" and a name" "stands alone" "$(policy "  targetRefs: [$(target Gateway web)]
  ignore: [\"*\", platform-per-ip]")"
refused "a RateLimitPolicy ignoring a name twice" "Duplicate value" "$(policy "  targetRefs: [$(target Gateway web)]
  ignore: [platform-per-ip, platform-per-ip]")"
refused "a RateLimitPolicy's limit of no model" "one of bucket, fixedWindow and rollingWindow" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: [{ name: per-client, key: Client, model: {} }]")"
refused "a RateLimitPolicy's limit keyed by what it does not know" "spec.limits[0].key" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: [{ name: per-client, key: Header, model: { bucket: { rate: 1/s, burst: 1 } } }]")"
refused "a RateLimitPolicy's two limits of one name" "Duplicate value" "$(policy "  targetRefs: [$(target Gateway web)]
  limits: [{ name: a, key: Client, model: { bucket: { rate: 1/s, burst: 1 } } }, { name: a, key: Client, model: { bucket: { rate: 2/s, burst: 1 } } }]")"

# A rule on a change: made, then changed.
k apply -f - <<<"$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/lb }')" >/dev/null
refused "a LoadBalancer's class changed" "loadBalancerClass is fixed" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/other }')"
admitted "a LoadBalancer's other fields changed" "$(data_plane public '  ingress: { httpPort: 80, httpsPort: 443 }
  service: { loadBalancerClass: example.com/lb, annotations: { a: b } }')"
k delete dataplane -n edge public >/dev/null
k apply -f - <<<"$(serving '{ shards: 64 }')" >/dev/null
refused "a rate-limiting service's shards changed" "shards is fixed" "$(serving '{ shards: 128 }')"
refused "a rate-limiting service's shards left to their default" "shards is fixed" "$(serving '{}')"
admitted "a rate-limiting service's other fields changed" "$(serving '{ shards: 64, replicas: 5, copies: 1 }')"
k apply -f - <<<"$(control_plane payments)" >/dev/null
[ -z "$(k get controlplane -n edge payments -o jsonpath='{.spec.rateLimiting}')" ] \
    || fail "a rate-limiting service is not dropped"
admitted "a rate-limiting service declared again, of other shards" "$(serving '{ shards: 128 }')"

# The control plane's naming of the service's pods, in the ControlPlane's status: a member
# id of 64 bits read back as it was written. Each naming after the first replaces the one
# there whole, as the control plane writes it.
naming() {
    k patch controlplane -n edge payments --subresource=status --type json \
        -p "[{\"op\":\"add\",\"path\":\"/status/rateLimiting/naming\",\"value\":$1}]" 2>&1
}
named='{"generation":3,"latest":{"term":12884901888,"version":7},"named":[{"address":"10.0.0.7:7000","member":"9223372036854775807"},{"address":"[2001:db8::7]:7000","member":"0"}]}'
out=$(k patch controlplane -n edge payments --subresource=status --type merge \
    -p "{\"status\":{\"rateLimiting\":{\"naming\":$named}}}" 2>&1) || fail "a naming is refused: $out"
kept=$(k get controlplane -n edge payments -o jsonpath='{.status.rateLimiting.naming}')
[ "$kept" = "$named" ] || fail "a naming written as $named is read back as $kept"
pass "a naming is kept in status as it was written"
for case in \
    'of no generation|{"generation":0,"latest":{"term":0,"version":0},"named":[]}|status.rateLimiting.naming.generation' \
    'of a member id that is no number|{"generation":1,"latest":{"term":0,"version":0},"named":[{"member":"7a","address":"10.0.0.7:7000"}]}|status.rateLimiting.naming.named[0].member' \
    'of a member id with a leading zero|{"generation":1,"latest":{"term":0,"version":0},"named":[{"member":"07","address":"10.0.0.7:7000"}]}|status.rateLimiting.naming.named[0].member' \
    'of no table|{"generation":1,"named":[]}|status.rateLimiting.naming.latest'; do
    IFS='|' read -r what written why <<<"$case"
    out=$(naming "$written") && fail "a naming $what is admitted"
    [[ $out == *"$why"* ]] || fail "a naming $what is refused, but not for \"$why\": $out"
    pass "a naming $what is refused: $why"
done
k delete controlplane -n edge payments >/dev/null

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

# The policy on a control plane's DataPlane status writes. Its roles patch the status, to
# apply Served (docs/05 §1); the operator's Accepted and Programmed are the operator's.
# DataPlanes of its own namespace, of another control plane's there, and one generated in
# team-status, a namespace of its watch scope, where its role is bound; their status the
# operator's.
k create namespace team-status >/dev/null
k create rolebinding -n team-status edgerush-controller-payments-watched \
    --clusterrole edgerush-control-plane \
    --serviceaccount edge:edgerush-controller-payments >/dev/null
# kubectl patch reads what it patches, which the control plane, applying without reading,
# has no right to: granted here alone, as the policy and not RBAC is what is judged.
k create clusterrole edgerush-planes-test-status-reader --verb get \
    --resource dataplanes.gateway.edgerush.dev/status >/dev/null
for namespace in edge team-status; do
    k create rolebinding -n "$namespace" edgerush-planes-test-status-reader \
        --clusterrole edgerush-planes-test-status-reader \
        --serviceaccount edge:edgerush-controller-payments >/dev/null
done
operators_status='{"status":{"conditions":[
  {"type":"Accepted","status":"True","reason":"Accepted","message":"Accepted.","lastTransitionTime":"2026-11-20T00:00:00Z","observedGeneration":1},
  {"type":"Programmed","status":"True","reason":"Programmed","message":"Made.","lastTransitionTime":"2026-11-20T00:00:00Z","observedGeneration":1}]}}'
served='{"type":"Served","status":"True","reason":"Served","message":"generation 1 is what its data planes are given","lastTransitionTime":"2026-11-20T00:00:00Z","observedGeneration":1}'
k apply -f - <<<"$(data_plane ours '  ingress: { httpPort: 80, httpsPort: 443 }')" >/dev/null
k apply -f - <<<"$(printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: DataPlane\nmetadata: { name: theirs, namespace: edge }\nspec:\n  controlPlaneRef: { name: blog }\n  ingress: { httpPort: 80, httpsPort: 443 }\n')" >/dev/null
k "${as_operator[@]}" create -f - <<<"$(printf 'apiVersion: gateway.edgerush.dev/v1alpha1\nkind: DataPlane\nmetadata: { name: web-payments, namespace: team-status }\nspec:\n  controlPlaneRef: { name: payments, namespace: edge }\n  gatewayRef: { name: web }\n')" >/dev/null
for target in "edge ours" "edge theirs" "team-status web-payments"; do
    read -r namespace name <<<"$target"
    k "${as_operator[@]}" patch dataplane -n "$namespace" "$name" --subresource=status \
        --type merge -p "$operators_status" >/dev/null
done
# `$1` as a JSON patch of DataPlane `$3` of `$2`'s status, from the control plane's
# ServiceAccount, server-side only.
status_patch() {
    k "${as_control_plane[@]}" patch dataplane -n "$2" "$3" --subresource=status \
        --type json -p "$1" --dry-run=server 2>&1
}
add_served="[{\"op\":\"add\",\"path\":\"/status/conditions/-\",\"value\":$served}]"
for _ in $(seq 1 30); do
    if ! status_patch '[{"op":"replace","path":"/status/conditions/0/status","value":"False"}]' edge ours >/dev/null; then
        break
    fi
    sleep 1
done
out=$(status_patch "$add_served" edge ours) \
    || fail "Served on its own DataPlane is refused from the control plane's ServiceAccount: $out"
pass "Served on its own DataPlane is admitted from the control plane's ServiceAccount"
out=$(status_patch "$add_served" team-status web-payments) \
    || fail "Served on a DataPlane generated for it is refused from the control plane's ServiceAccount: $out"
pass "Served on a DataPlane generated for it is admitted from the control plane's ServiceAccount"
for change in \
    'Accepted changed|[{"op":"replace","path":"/status/conditions/0/status","value":"False"}]' \
    'Accepted made of another generation|[{"op":"replace","path":"/status/conditions/0/observedGeneration","value":2}]' \
    'Programmed taken away|[{"op":"remove","path":"/status/conditions/1"}]' \
    'a condition of another type added|[{"op":"add","path":"/status/conditions/-","value":{"type":"Other","status":"True","reason":"R","message":"","lastTransitionTime":"2026-11-20T00:00:00Z"}}]'; do
    what=${change%%|*}
    out=$(status_patch "${change#*|}" edge ours) \
        && fail "$what is admitted from the control plane's ServiceAccount"
    [[ $out == *"changes only a DataPlane's Served condition"* ]] \
        || fail "$what is refused from the control plane's ServiceAccount, but not by the policy: $out"
    pass "$what is refused from the control plane's ServiceAccount"
done
out=$(status_patch "$add_served" edge theirs) \
    && fail "Served on another control plane's DataPlane is admitted from the control plane's ServiceAccount"
[[ $out == *"only of a DataPlane naming its ControlPlane"* ]] \
    || fail "Served on another control plane's DataPlane is refused, but not by the policy: $out"
pass "Served on another control plane's DataPlane is refused from the control plane's ServiceAccount"
out=$(k "${as_operator[@]}" patch dataplane -n edge ours --subresource=status --type json \
    -p '[{"op":"replace","path":"/status/conditions/0/status","value":"False"}]' --dry-run=server 2>&1) \
    || fail "the operator's Accepted is refused from the operator: $out"
pass "the operator's Accepted is admitted from the operator"
k delete dataplane -n edge ours theirs >/dev/null
k "${as_operator[@]}" delete dataplane -n team-status web-payments >/dev/null
k delete namespace team-status --wait=false >/dev/null

echo "every check passed"
