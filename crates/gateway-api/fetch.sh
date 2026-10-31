#!/usr/bin/env bash
# The Gateway API CRDs the types are written from, fetched from a release's standard
# channel into crds/, the ones there before replaced:
#
#   crates/gateway-api/fetch.sh v1.7.0
#
# Then write the types again and look at what changed:
#
#   EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-gateway-api --test generated
#
# Only the standard channel is read: its fields are those the types have, and the
# experimental channel's CRDs hold every one of them (docs/05 §6). A kind joins KINDS when
# EdgeRush starts to follow it.
set -euo pipefail

cd "$(dirname "$0")"

VERSION="${1:?usage: fetch.sh <release, such as v1.7.0>}"
KINDS=(gatewayclasses gateways httproutes referencegrants)
BASE="https://raw.githubusercontent.com/kubernetes-sigs/gateway-api/${VERSION}/config/crd/standard"

for kind in "${KINDS[@]}"; do
    file="gateway.networking.k8s.io_${kind}.yaml"
    curl --fail --silent --show-error --location --output "crds/${file}.part" "${BASE}/${file}"
    # A release's CRD says its own version; one that says another is not what was asked for.
    grep -q "gateway.networking.k8s.io/bundle-version: ${VERSION}$" "crds/${file}.part" \
        || { echo "crds/${file}: not from ${VERSION}" >&2; rm -f "crds/${file}.part"; exit 1; }
    mv "crds/${file}.part" "crds/${file}"
done
echo "${VERSION}" > crds/VERSION
