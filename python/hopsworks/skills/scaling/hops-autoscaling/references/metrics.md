# Scale metrics, modes and autoscalers

## Modes and autoscalers

| Mode | How a new deployment gets it | Autoscaler | Scales to zero |
|---|---|---|---|
| Standard (default) | `knative_mode` unset or `False` | KEDA where the cluster has it (`Autoscaler.KEDA`, the default), else KServe's own HPA (`Autoscaler.HPA`, cpu and memory only) | With `min_instances=0`, through the KEDA HTTP add-on: predictor without a transformer only |
| Knative | `knative_mode=True` | Knative's request-based autoscaler (no `autoscaler` field; it is rejected) | With `min_instances=0`, natively |

Equal `min_instances` and `max_instances` run a fixed replica count: the metric, target and autoscaler are cleared. A minimum of 0 with a maximum of 1 is one instance while active and zero when idle. An LLM deployment defaults to a fixed count of one.

A deployment created without a mode runs in Standard mode; an update that omits the mode keeps the stored one. The mode changes only while the deployment is stopped.

## Metrics

Target units are per instance. "Default target" is what the backend fills when `target` is left out.

| `ScaleMetric` | Measures | Default target | Standard: KEDA | Standard: HPA | Knative | Model servers |
|---|---|---|---|---|---|---|
| `CPU` | cpu utilization, % of the request | 80 | yes | yes | no | all |
| `MEMORY` | memory utilization, % of the request | 80 | yes | yes | no | all |
| `CONCURRENCY` | requests in flight, counted at the gateway (Standard: by the KEDA HTTP add-on) | 100 | predictor without a transformer | no | yes | Standard: all but vLLM; Knative: all |
| `RPS` | requests per second, counted at the gateway | 200 | predictor without a transformer | no | yes | Standard: all but vLLM; Knative: all |
| `QUEUE_DEPTH` | `vllm:num_requests_waiting`, requests queued in the engine | 5 | yes | no | no | vLLM |
| `KV_CACHE_USAGE` | `vllm:kv_cache_usage_perc`, KV-cache in use, % | 80 | yes | no | no | vLLM |
| `RUNNING_REQUESTS` | `vllm:num_requests_running`, requests being generated | 32 | yes | no | no | vLLM |
| `QUEUE_TIME` | `vllm:request_queue_time_seconds`, average over the last minute, ms | 1000 | yes | no | no | vLLM |
| `TIME_TO_FIRST_TOKEN` | `vllm:time_to_first_token_seconds`, average, ms | 2000 | yes | no | no | vLLM |
| `REQUEST_LATENCY` | `vllm:e2e_request_latency_seconds`, average, ms | 10000 | yes | no | no | vLLM |

Defaults when `scale_metric` is left out on a range: a vLLM predictor gets `QUEUE_DEPTH` under KEDA, everything else `CPU` at 80%; Knative mode gets `CONCURRENCY` at 100.

How KEDA sizes the deployment: replicas = ceil(metric value / target), with the value summed over the replicas for the engine metrics and multiplied by the replica count for the latency ones, so a latency metric grows and shrinks the deployment in proportion to how far the average sits from its target.

## KEDA-only settings

| Field | Meaning | Default |
|---|---|---|
| `scale_down_stabilization_window_seconds` | how long the metric must stay below target before instances are removed (0-3600) | 300 |
| `scale_up_stabilization_window_seconds` | how long it must stay above target before instances are added (0-3600) | 0 |
| `additional_scale_metrics` | further `(metric, target)` pairs; the most demanding decides the count; one request metric per deployment at most | none |
| `idle_cooldown_seconds` | with a minimum of 0: quiet seconds before the last instance goes (0-3600) | 300 |
| `cold_start_timeout_seconds` | with a minimum of 0: how long the first request is held while the deployment wakes (1-3600) | 600 |

All of them are rejected under KServe's HPA (which ignores them) and in Knative mode. Knative's own knobs (`stable_window_seconds`, `panic_window_percentage`, `panic_threshold_percentage`, `scale_to_zero_retention_seconds`) are rejected in Standard mode.

## What the backend refuses (HTTP 422, the message says which)

- A minimum of 0 under KServe's HPA, or on a cluster without KEDA.
- A minimum of 0, or a request metric, on a deployment with a transformer (the HTTP add-on fronts the predictor alone). A transformer's own minimum is at least 1.
- `CONCURRENCY` or `RPS` on a vLLM deployment in Standard mode (use the engine metrics).
- An engine metric on a non-vLLM model server.
- KEDA settings under the HPA; Knative settings in Standard mode; an `autoscaler` in Knative mode.
- `idle_scale_to_zero=False` next to a minimum of 0 (they contradict each other); a maximum of 0.

## The idle flag

`idle_scale_to_zero` is the backend's reading of the minimum: it reports `True` whenever the minimum is 0. Setting it to `True` on the client lowers the minimum to 0; raising the minimum above 0 clears a flag that was only read back. There is no need to set it by hand: set the minimum.
