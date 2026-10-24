#!/usr/bin/env bash
# Kubernetes clusters to test against, one per Kubernetes version, made with kind.
#
#   kind/cluster.sh up 1.36      create cluster edgerush-1-36 and wait until it is ready
#   kind/cluster.sh down 1.36    delete it
#   kind/cluster.sh versions     the versions this script can make
#
# Each cluster's kubeconfig is a file of its own, target/kind/edgerush-<version>.kubeconfig,
# printed by `up`. Nothing here reads or writes the user's own kubeconfig: kind and kubectl
# otherwise add to ~/.kube/config and switch its current context, and on a machine that
# also reaches real clusters a test must never land on one of them.
#
# The node images are pinned by digest to the kind release that built them, as kind asks:
# an image from another release may not start. Moving kind means taking that release's
# image list with it.
set -euo pipefail

cd "$(dirname "$0")/.."

KIND_VERSION=v0.33.0

# From the release notes of kind v0.33.0.
image_for() {
    case "$1" in
        1.37) echo "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5" ;;
        1.36) echo "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed" ;;
        1.35) echo "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0" ;;
        1.34) echo "kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d" ;;
        *) return 1 ;;
    esac
}
VERSIONS="1.34 1.35 1.36 1.37"

usage() {
    echo "usage: $0 up <version> | down <version> | versions   (versions: $VERSIONS)" >&2
    exit 2
}

# A path kind and kubectl read the same way on Windows (Git Bash) and Linux.
native_path() {
    if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi
}

check_kind() {
    local have
    have=$(kind version | awk '{print $2}')
    if [ "$have" != "$KIND_VERSION" ]; then
        echo "kind $KIND_VERSION is needed, found $have" >&2
        exit 1
    fi
}

[ $# -ge 1 ] || usage
command=$1

if [ "$command" = versions ]; then
    echo "$VERSIONS"
    exit 0
fi

[ $# -eq 2 ] || usage
version=$2
image=$(image_for "$version") || { echo "no image for Kubernetes $version" >&2; usage; }
name="edgerush-${version//./-}"
mkdir -p target/kind
kubeconfig=$(native_path "$PWD/target/kind/$name.kubeconfig")
# Belt and braces: anything that falls back to the environment finds this file too.
export KUBECONFIG="$kubeconfig"

check_kind

case "$command" in
    up)
        # kind's progress goes to stderr, so that what `up` prints is the kubeconfig alone.
        kind create cluster --name "$name" --image "$image" --kubeconfig "$kubeconfig" --wait 180s >&2
        server=$(kubectl --kubeconfig "$kubeconfig" version -o json | awk -F'"' '/"serverVersion"/ {s=1} s && /"gitVersion"/ {print $4; exit}')
        case "$server" in
            "v$version".*) ;;
            *)
                echo "cluster $name answers as $server, not Kubernetes $version" >&2
                exit 1
                ;;
        esac
        kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready nodes --all --timeout=120s >/dev/null
        echo "$kubeconfig"
        ;;
    down)
        kind delete cluster --name "$name" --kubeconfig "$kubeconfig"
        rm -f "target/kind/$name.kubeconfig"
        ;;
    *)
        usage
        ;;
esac
