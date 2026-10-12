#!/usr/bin/env bash
# Distributed run on the local Docker: one master and one worker container per user.
# Each worker runs exactly one user; set the count with `users` in local/locust.conf.
#   SDK=<name>        image locust-hsfs:<name> from build_images.sh (default main)
#   API_KEY_FILE=...  Hopsworks API key (default locust_benchmark/.api_key)
#   MASTER_ARGS="..." extra locust arguments for the master, overriding local/locust.conf.
#                     Do not pass --users here; it would break the one-user-per-worker split.
# Report: results/report_local_<SDK>_u<users>.html
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"

if grep -q '"<' "$here/hopsworks_config.json"; then
    echo "local/hopsworks_config.json still has <placeholders>; set host and project first" >&2
    exit 1
fi
WORKERS="$(awk -F= '$1 ~ /^[[:space:]]*users[[:space:]]*$/ {gsub(/[[:space:]]/, "", $2); print $2}' "$here/locust.conf")"
if ! [[ "$WORKERS" =~ ^[1-9][0-9]*$ ]]; then
    echo "local/locust.conf must set users to a positive number, got '$WORKERS'" >&2
    exit 1
fi
sdk="${SDK:-main}"
export WORKERS
export IMAGE="locust-hsfs:$sdk"
export API_KEY_FILE="${API_KEY_FILE:-$root/.api_key}"
export HOST_UID="$(id -u)" HOST_GID="$(id -g)"
export MASTER_ARGS="--html results/report_local_${sdk}_u$WORKERS.html ${MASTER_ARGS:-}"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image $IMAGE not found; run ./build_images.sh $sdk" >&2
    exit 1
fi
if [ ! -f "$API_KEY_FILE" ]; then
    echo "API key file $API_KEY_FILE not found" >&2
    exit 1
fi

cd "$here"
trap 'docker compose down --remove-orphans >/dev/null 2>&1' EXIT
# Wait on the master alone.
# Aborting when the first worker exits would stop the master before it writes the report.
docker compose up -d --scale worker="$WORKERS"
docker compose logs -f &
logs=$!
docker compose wait master >/dev/null || true
kill "$logs" 2>/dev/null || true
exit "$(docker inspect -f '{{.State.ExitCode}}' "$(docker compose ps -aq master)")"
