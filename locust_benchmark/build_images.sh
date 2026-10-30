#!/usr/bin/env bash
# Builds locust-hsfs:<name> for each SDK in sdk.env, or only the names given.
#   ./build_images.sh            all of SDKS
#   ./build_images.sh main       one
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
source "$root/sdk.env"

for sdk in ${@:-$SDKS}; do
    repo_var="${sdk}_REPO" ref_var="${sdk}_REF"
    if [ -z "${!repo_var:-}" ] || [ -z "${!ref_var:-}" ]; then
        echo "sdk.env has no ${sdk}_REPO / ${sdk}_REF" >&2
        exit 1
    fi
    echo "==> locust-hsfs:$sdk from ${!repo_var}@${!ref_var}"
    docker build -t "locust-hsfs:$sdk" \
        --build-arg SDK_REPO="${!repo_var}" --build-arg SDK_REF="${!ref_var}" "$root"
done
