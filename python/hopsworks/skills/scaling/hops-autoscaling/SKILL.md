---
name: hops-autoscaling
description: Use when configuring or debugging how a Hopsworks deployment scales, for model deployments and agent deployments alike. Auto-invoke when the user mentions autoscaling, scale to zero, idle deployments, min/max instances, replicas, KEDA, HPA, Knative mode versus Standard mode, scale metrics (CPU, memory, concurrency, RPS, queue depth, KV cache, time to first token), or asks why a deployment does not scale up or down. Input a deployment (existing or about to be created) and a load profile; output a scaling configuration that holds, and proof that it scales.
---

# Hopsworks Deployment Autoscaling

Every KServe deployment on Hopsworks, whether it serves a model (**hops-online-inference**) or an agent (**hops-agent-deployment**), carries a scaling configuration per component: the predictor, and the transformer when there is one. The configuration says how many instances run (`min_instances` to `max_instances`), which metric moves the count and what target it chases, and, in Standard mode, which autoscaler does the work. The same `PredictorScalingConfig` object goes into `model.deploy(...)`, `ms.create_predictor(...)`, `ms.create_endpoint(...)` and `ms.deploy_agent(...)`.

## Contract
- **Input:** a deployment, existing or about to be created, and what its traffic looks like (steady, bursty, idle most of the day, GPU-bound LLM).
- **Output:** a scaling configuration the backend accepts and that the deployment follows under load, with the instance count observed.
- **Pre-condition:** the deployment's model server and components are decided (a transformer rules out scale to zero and the request metrics in Standard mode); for anything beyond KServe's own cpu/memory HPA the cluster runs KEDA.

## Smoke-test (cheap pre/post-flight)
```bash
hops deployment info <name>                       # mode, components; status via `hops deployment status <name>`
hops deployment status <name>                     # Running, Idle (at zero), Updating, ...
```
```python
d = project.get_model_serving().get_deployment("<name>")
d.predictor.scaling_configuration.describe()      # what the backend stored, defaults filled in
d.get_state().available_predictor_instances       # the live replica count, polled while load runs
```
Inside a terminal with cluster access, **hops-kubectl-debug** shows the autoscaler objects in the project namespace: `kubectl get scaledobject,hpa -n <namespace>` (KEDA's `<name>-predictor` ScaledObject and its `keda-hpa-<name>-predictor` HPA, or KServe's own HPA).

## Ask the user (only when state is ambiguous)
- **Idle most of the time?** Then a minimum of 0 (scale to zero when idle) saves the GPU or the cores, at the price of a cold start on the first request; an LLM reloads its weights on every wake.
- **What should trigger a scale-out?** Resource pressure (cpu, memory), the request rate at the door (concurrency, rps), or, for vLLM, engine pressure (queue depth, KV cache, running requests, latency). The metric decides, not the mode.
- **Fixed count or a range?** Equal `min_instances` and `max_instances` run a fixed replica count with no autoscaler (the default for LLM deployments, which rarely fit more than one GPU replica).
- **Which mode?** Standard (the default for new deployments: a plain Deployment scaled by KEDA, or by KServe's HPA where KEDA is absent) or Knative (`knative_mode=True`, Knative's request-based autoscaler). Knative is the opt-in; change the mode only while the deployment is stopped.

## Steps (generic, non-binding)
1. **Pick the mode.** Leave `knative_mode` unset for Standard; set `knative_mode=True` only when Knative's autoscaler is wanted (its stable window, panic window and scale-to-zero retention knobs exist there alone). The mode cannot change while the deployment runs.
2. **Pick the range.** `min_instances` 0 means scale to zero when idle in both modes (Standard: through the KEDA HTTP add-on, predictor without a transformer only); 1 or more keeps that many running. Through the SDK the minimum is 1 unless set (the UI defaults a new deployment to 0 where KEDA is installed), so pass `min_instances=0` for scale to zero. `max_instances` caps the scale-out; the cluster administrator caps it in turn (`kube_serving_max_num_instances`).
3. **Pick the metric and its target.** Standard mode: `CPU` or `MEMORY` (utilization %, under KEDA or the HPA), `CONCURRENCY` or `RPS` (per instance, measured at the gateway by the KEDA HTTP add-on; predictor without a transformer, not vLLM), or a vLLM engine metric under KEDA. Knative mode: `CONCURRENCY` or `RPS`. The full table with units, defaults and rules is in [references/metrics.md](references/metrics.md).
4. **Tune KEDA where it matters.** `scale_down_stabilization_window_seconds` (how long the metric must stay low before instances go, default 300) and `scale_up_stabilization_window_seconds` (default 0) damp flapping; `additional_scale_metrics` lets a second metric scale the same deployment, the most demanding one winning (e.g. queue depth plus KV-cache usage for vLLM).
5. **Set the idle timeouts with a minimum of 0.** `idle_cooldown_seconds` (default 300) is the quiet period before the last instance goes; `cold_start_timeout_seconds` (default 600) is how long the first request is held while the deployment wakes, so set it above the model's load time. Details, the wake sequence and the LLM caveat: [references/scale-to-zero.md](references/scale-to-zero.md).
6. **Deploy or save, then read back.** The backend fills defaults and refuses what the cluster cannot do (a 422 whose message says why, e.g. a minimum of 0 under KServe's HPA, a request metric next to a transformer, an engine metric on a Python server, KEDA knobs under the HPA). Read `scaling_configuration` back after `start()` or `save()`; it is the truth, not the object you sent.
7. **Prove it.** Send load through the deployment's endpoint (`deployment.predict(...)` or the inference URL; traffic sent straight to the predictor Service inside the cluster is invisible to the request metrics and to scale to zero) and poll `get_state().available_predictor_instances`; expect a scale-out within a few minutes and a scale-in after the stabilization window or the idle cooldown. The Grafana dashboards linked from the deployment page ("Deployment Metrics (Standard)" and "vLLM Deployment Metrics", or the Knative one) show replicas, the KEDA trigger values and the request and engine panels. What to look at when it does not scale: [references/troubleshooting.md](references/troubleshooting.md).

```python
from hsml.scaling_config import PredictorScalingConfig, ScaleMetric, Autoscaler

# A Python model deployment that scales on the requests in flight and rests at zero when idle.
scaling = PredictorScalingConfig(
    min_instances=0,                      # scale to zero when idle (KEDA HTTP add-on holds the first request)
    max_instances=4,
    scale_metric=ScaleMetric.CONCURRENCY, # concurrent requests per instance, measured at the gateway
    target=20,
    scale_down_stabilization_window_seconds=120,
    idle_cooldown_seconds=300,
    cold_start_timeout_seconds=120,
)
deployment = model.deploy(name="fraud", scaling_configuration=scaling, environment="pandas-inference-pipeline")

# A vLLM deployment on engine pressure: scale out when more than 8 requests are being generated
# per replica, or when the KV cache passes 80%, whichever happens first.
llm_scaling = PredictorScalingConfig(
    min_instances=1,
    max_instances=2,
    scale_metric=ScaleMetric.RUNNING_REQUESTS,
    target=8,
    additional_scale_metrics=[(ScaleMetric.KV_CACHE_USAGE, 80)],
)

# An agent: the same object, through deploy_agent.
ms = project.get_model_serving()
agent = ms.deploy_agent(name="support", script_file=..., scaling_configuration=scaling)

# Raise the minimum of a running deployment (the idle flag follows the minimum).
d = ms.get_deployment("fraud")
d.predictor.scaling_configuration.min_instances = 1
d.save()
```

## Toolset
- **CLI:** `hops deployment create ... --knative|--standard` picks the mode; `hops deployment info|status <name>`; the scaling fields themselves are set through the SDK.
- **SDK:** `hsml.scaling_config.PredictorScalingConfig` / `TransformerScalingConfig`, `ScaleMetric`, `Autoscaler`; `deployment.predictor.scaling_configuration` (read back, edit, `deployment.save()`); `deployment.get_state()`.
- **Cluster:** `kubectl get scaledobject,hpa -n <namespace>` and the Grafana dashboards, through **hops-kubectl-debug** and the deployment page.

## Next steps
- Deploy the model this scales: **hops-online-inference**; the agent: **hops-agent-deployment**.
- Give the predictor the resources the metric assumes (cpu and memory requests are what utilization is measured against): the resources section of **hops-online-inference**.
- Watch the deployment once it scales: **hops-monitoring**.
