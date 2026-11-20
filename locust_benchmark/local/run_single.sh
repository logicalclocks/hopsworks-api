#!/usr/bin/env bash
# Single locust process on the local Docker, settings from local/locust.conf.
# All users share one process, so keep users = 1 (see README.md, Limitations); use local/run.sh for more.
# Extra arguments go to locust and override the file, e.g. local/run_single.sh --users 1 --run-time 30
#   SDK=<name>        image locust-hsfs:<name> (default main)
#   API_KEY_FILE=...  Hopsworks API key (default locust_benchmark/.api_key)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"

docker run --rm \
    --user "$(id -u):$(id -g)" -e HOME=/tmp -e USER=locust \
    -v "$root:/work" -w /work \
    -v "${API_KEY_FILE:-$root/.api_key}:/secrets/api_key:ro" -e HOPSWORKS_API_KEY_FILE=/secrets/api_key \
    -e LOCUST_HOPSWORKS_CONFIG=/work/local/hopsworks_config.json \
    "locust-hsfs:${SDK:-main}" --config local/locust.conf "$@"
