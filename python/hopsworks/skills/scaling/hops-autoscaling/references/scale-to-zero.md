# Scale to zero when idle

## What a minimum of 0 does

- **Knative mode:** Knative's autoscaler removes the last pod after `scale_to_zero_retention_seconds` of no traffic and holds the next request while a pod starts. Nothing else to configure.
- **Standard mode:** the KEDA HTTP add-on fronts the deployment. The deployment's hosts route to the add-on's interceptor instead of straight to the predictor; the interceptor counts requests, KEDA removes the last replica after `idle_cooldown_seconds` without one, and the next request is held by the interceptor (up to `cold_start_timeout_seconds`) while a replica starts and passes its readiness probe, then forwarded. While active the deployment runs at least one replica and autoscales between 1 and `max_instances` on its metric as usual.

Requirements in Standard mode: KEDA installed on the cluster, the KEDA autoscaler (the default where KEDA is installed; KServe's HPA cannot scale to zero), and a predictor without a transformer.

A fresh deployment gets a grace period before it may rest: the larger of the idle cooldown and the cold-start timeout, counted from its creation, so a long image pull or model load does not end in an immediate scale-down.

## What the user sees

- `hops deployment status <name>` and `deployment.get_state().status` say `Idle` at zero and `Running` once a replica is up; `available_predictor_instances` is 0 at rest.
- The first request after an idle period takes the cold start: image already on the node, container start, model load, readiness. A Python predictor with a small model wakes in 10-30 s; an LLM reloads its weights into the GPU on every wake and takes as long as its first start, so set `cold_start_timeout_seconds` above that and expect the first completion to be slow. A request held longer than the timeout fails.
- Requests must reach the deployment through its endpoint (the gateway): `deployment.predict()`, the inference URL, the OpenAI-compatible URL for vLLM. Traffic sent straight to the predictor Service from inside the cluster never passes the interceptor, counts for nothing, and the deployment scales to zero underneath it.

## Choosing the timeouts

| Workload | `idle_cooldown_seconds` | `cold_start_timeout_seconds` |
|---|---|---|
| small Python model, bursty | 60-300 | 120 |
| LLM on a GPU, used in sessions | 600-1800 | model load time plus margin (300-900) |
| anything with a strict latency SLO | do not scale to zero; keep a minimum of 1 | |

The timeouts belong to the minimum of 0: raising the minimum drops them, lowering it again takes the defaults unless they are set again.

## Verifying

1. Start the deployment; it reports `Running` with one instance.
2. Leave it alone for the grace period plus the idle cooldown; `status` turns `Idle`, `available_predictor_instances` 0.
3. Send one request through the endpoint; it returns within the cold-start timeout, the status is `Running` and the instance count is back to 1.
4. Under load, the instance count climbs toward `max_instances` on the metric; after the load stops it falls back to 1 after the stabilization window, then to 0 after the idle cooldown.

In Grafana the Standard and vLLM dashboards show the replica count, the KEDA trigger values and whether KEDA considers the deployment active; at zero the engine and request panels are empty by nature, only the replica panels show the zero.
