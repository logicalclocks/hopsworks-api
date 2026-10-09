# When a deployment does not scale

Work from the stored configuration outward.

## The configuration is not what was sent

Read `deployment.predictor.scaling_configuration` back after `start()` or `save()`. The backend fills defaults and settles the configuration: a metric on a fixed count is cleared, a legacy row without an autoscaler runs under KEDA where it is installed, a minimum of 0 with a maximum of 1 runs one instance while active. Raising the minimum from 0 drops the idle timeouts.

A 422 on create or save names the rule that was broken (see the list in [metrics.md](metrics.md)). The usual ones: a minimum of 0 or a request metric next to a transformer; an engine metric on a Python server; KEDA settings under KServe's HPA; a minimum of 0 on a cluster without KEDA.

## It never scales out

- **The metric does not see the load.** Request metrics and scale to zero count at the gateway: load sent straight to the predictor Service inside the cluster is invisible. Send it through `deployment.predict()` or the inference URL.
- **The target is never reached.** Utilization metrics are a percentage of the *request*, not the limit: a predictor requesting 2 cores that uses 1 sits at 50%. Lower the target or the request. With `CONCURRENCY`, a target of 100 needs 100 requests in flight per instance before a second one appears; for a slow predictor a target of a handful is what makes a few callers scale it.
- **The maximum is already reached.** `max_instances`, or the administrator's `kube_serving_max_num_instances`, caps it; on a GPU cluster a second vLLM replica also needs a second free GPU, and stays `Pending` without one.
- **KEDA cannot read the metric.** On the cluster, `kubectl get scaledobject -n <namespace>` shows `READY` and `ACTIVE`; `kubectl describe scaledobject <name>-predictor` lists the triggers and the last error (a Prometheus query that returns nothing, an unreachable metrics server). The Grafana Standard and vLLM dashboards have a "KEDA scaler errors" panel.
- **A new replica cannot start.** `hops deployment logs <name>` and **hops-kubectl-debug** (pod events: image pull, scheduling, OOM). The scale-out is decided; the pod is what is missing.

## It never scales in, or scales in too fast

- Scale-in waits for `scale_down_stabilization_window_seconds` (KEDA, default 300) or the HPA's own 5-minute window after the metric drops; scale to zero waits for `idle_cooldown_seconds` on top. A fresh deployment also gets the initial grace period.
- Something keeps sending requests: health checks, a load generator left running, a client retrying. The request panels of the dashboard show it.
- Too fast: a cpu metric with a low target on a predictor that idles at 0% scales in the moment the burst ends; lengthen the scale-down window.

## It scales to zero but does not wake

- The first request must go through the endpoint (the interceptor); the cold start must finish within `cold_start_timeout_seconds`, which for an LLM means the full weight load. Raise the timeout.
- `kubectl get interceptorroute,ingress -n <namespace>`: the deployment's hosts must route to the `<name>-keda-interceptor` Service, and no older Ingress may point straight at the predictor.
- The status stays `Idle` with a pending pod: the usual pod problems (image pull, no GPU free); **hops-kubectl-debug**.

## Knative mode specifics

Knative scales on its own metrics and knobs; the KEDA settings are rejected there. A Knative deployment with a transformer scales each component on its own concurrency. Switching a Knative deployment to Standard mode keeps a concurrency or rps metric only where the HTTP add-on can measure it (predictor without a transformer, not vLLM); elsewhere the backend swaps in cpu at 80%.
