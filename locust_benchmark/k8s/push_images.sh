#!/usr/bin/env bash
# Pushes locust-hsfs:<name> (from build_images.sh) to REGISTRY in k8s/cluster.env.
# The tag is <name>-<first 8 characters of the SDK ref>, for each SDK in sdk.env or the names given.
# With PULL_SECRET set, logs the local Docker in to the registry with it first; otherwise log in yourself.
#   k8s/push_images.sh          all of SDKS
#   k8s/push_images.sh main     one
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
source "$root/sdk.env"
source "$here/cluster.env"

if [[ "$REGISTRY" == *"<"* ]]; then
    echo "set REGISTRY in k8s/cluster.env first" >&2
    exit 1
fi
if [ -n "${PULL_SECRET:-}" ]; then
    config="$(kubectl -n "$NAMESPACE" get secret "$PULL_SECRET" -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d)"
    host="${REGISTRY%%/*}"
    auth="$(jq -r --arg h "$host" '.auths[$h].auth // empty' <<<"$config" | base64 -d)"
    if [ -z "$auth" ]; then
        auth="$(jq -r --arg h "$host" '.auths[$h] | "\(.username):\(.password)"' <<<"$config")"
    fi
    printf '%s' "${auth#*:}" | docker login "$host" -u "${auth%%:*}" --password-stdin >/dev/null
fi

for sdk in ${@:-$SDKS}; do
    ref_var="${sdk}_REF"
    target="$REGISTRY:$sdk-${!ref_var:0:8}"
    docker tag "locust-hsfs:$sdk" "$target"
    docker push -q "$target"
done
