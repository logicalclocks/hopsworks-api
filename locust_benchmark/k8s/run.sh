#!/usr/bin/env bash
# Runs the locust test inside the cluster (k8s/locust-job.yaml).
# One run is a master Job and its Service, and a worker Job with one pod per user.
# Settings: k8s/locust.conf and k8s/hopsworks_config.json for the test, k8s/cluster.env for the cluster, and per run:
#   SDK=<name>       SDK from sdk.env; its image must be pushed (default main)
#   USERS=<n>        users, which is also the number of worker pods (default 4)
#   DURATION=<s>     run time in seconds (default 300)
#   LABEL=<s>        report label (default run)
#   API_KEY_FILE=... Hopsworks API key (default locust_benchmark/.api_key)
# The report is copied to results/report_k8s_<LABEL>_<SDK>_u<USERS>_<DURATION>s.html.
# Master and worker logs and a mid-run node CPU sample go to logs/.
# The Jobs and the Service are deleted at the end; the ConfigMap and Secret stay for the next run.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
source "$root/sdk.env"
source "$here/cluster.env"

export SDK="${SDK:-main}" USERS="${USERS:-4}" DURATION="${DURATION:-300}"
label="${LABEL:-run}"
ref_var="${SDK}_REF"
if [ -z "${!ref_var:-}" ]; then
    echo "sdk.env has no ${SDK}_REF" >&2
    exit 1
fi
export IMAGE="$REGISTRY:$SDK-${!ref_var:0:8}"
export NAME="locust-$SDK-u$USERS-$(date +%H%M%S)"
export NAMESPACE MASTER_CPU WORKER_CPU MEMORY_REQUEST MEMORY_LIMIT
# Optional settings become whole YAML fragments, so the template stays valid without them.
if [ -n "${PULL_SECRET:-}" ]; then
    export PULL_SECRETS="[{name: $PULL_SECRET}]"
else
    export PULL_SECRETS="[]"
fi
if [ -n "${PREFERRED_NODE:-}" ]; then
    export NODE_MATCH="{key: kubernetes.io/hostname, operator: In, values: [$PREFERRED_NODE]}"
else
    export NODE_MATCH="{key: kubernetes.io/hostname, operator: Exists}"
fi
api_key_file="${API_KEY_FILE:-$root/.api_key}"
if [[ "$REGISTRY" == *"<"* ]] || grep -q '"<' "$here/hopsworks_config.json"; then
    echo "k8s/cluster.env or k8s/hopsworks_config.json still has <placeholders>" >&2
    exit 1
fi
if [ ! -f "$api_key_file" ]; then
    echo "API key file $api_key_file not found" >&2
    exit 1
fi
out="report_k8s_${label}_${SDK}_u${USERS}_${DURATION}s"
mkdir -p "$root/results" "$root/logs"

# The pods see the files flat in /work.
# The k8s configuration goes in under the names common.py and locust.conf readers expect.
kubectl -n "$NAMESPACE" create secret generic locust-api-key \
    --from-file=api_key="$api_key_file" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create configmap locust-files \
    --from-file="$root/locustfile.py" --from-file="$root/common.py" --from-file="$root/setup_data.py" \
    --from-file=locust.conf="$here/locust.conf" \
    --from-file=hopsworks_config.json="$here/hopsworks_config.json" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

cleanup() {
    kubectl -n "$NAMESPACE" delete job "$NAME-master" "$NAME-worker" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl -n "$NAMESPACE" delete service "$NAME-master" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

envsubst '${NAME} ${NAMESPACE} ${IMAGE} ${PULL_SECRETS} ${NODE_MATCH} ${USERS} ${DURATION} ${MASTER_CPU} ${WORKER_CPU} ${MEMORY_REQUEST} ${MEMORY_LIMIT}' \
    <"$here/locust-job.yaml" | kubectl apply -f - >/dev/null
echo "==> $NAME: $SDK ($IMAGE), $USERS users, ${DURATION}s"

master_pod=""
for _ in $(seq 60); do
    master_pod="$(kubectl -n "$NAMESPACE" get pods -l job-name="$NAME-master" -o name | head -1)"
    [ -n "$master_pod" ] && break
    sleep 2
done

deadline=$((SECONDS + DURATION + 600))
sampled=false
while true; do
    logs="$(kubectl -n "$NAMESPACE" logs "$master_pod" 2>/dev/null || true)"
    if grep -q "REPORT_READY" <<<"$logs"; then break; fi
    phase="$(kubectl -n "$NAMESPACE" get "$master_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$phase" = "Failed" ]; then echo "master pod failed" >&2; break; fi
    if ! $sampled && grep -q "All users spawned" <<<"$logs"; then
        # Node CPU and worker placement once load is running, for the record.
        sleep $((DURATION / 2))
        {
            date -u +%FT%TZ
            kubectl top nodes
            kubectl -n "$NAMESPACE" get pods -l job-name="$NAME-worker" -o wide --no-headers | awk '{print $7}' | sort | uniq -c
        } >"$root/logs/${out}_nodes.txt" 2>&1
        sampled=true
    fi
    if [ "$SECONDS" -gt "$deadline" ]; then echo "timed out waiting for the report" >&2; break; fi
    sleep 10
done

kubectl -n "$NAMESPACE" logs "$master_pod" >"$root/logs/${out}_master.log" 2>&1 || true
kubectl -n "$NAMESPACE" logs -l job-name="$NAME-worker" --tail=-1 --prefix --max-log-requests=64 \
    >"$root/logs/${out}_workers.log" 2>&1 || true
if kubectl -n "$NAMESPACE" cp "${master_pod#pod/}:/results/report.html" "$root/results/$out.html" >/dev/null 2>&1; then
    echo "==> report: results/$out.html"
else
    echo "==> report not copied" >&2
fi
sed -n '/Type     Name/,/^$/p' "$root/logs/${out}_master.log" | head -24
echo "==> tracebacks in worker logs: $(grep -c Traceback "$root/logs/${out}_workers.log" || true)"
