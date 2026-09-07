---
name: hops-online-inference
description: Use when writing code for model deployment, online inference, predictor scripts, or on-demand transformations in Hopsworks. Auto-invoke when user wants to deploy models, write predictor.py files, retrieve precomputed features for serving, create on-demand transformation functions, or configure model serving. Input registered model + online feature view; output a running KServe endpoint.
---

# Hopsworks Online Inference — Python SDK Best Practices

An **online inference pipeline** is one of the three FTI pipelines (Feature, Training, Inference): a separate program that runs 24/7 behind a network endpoint, accepts prediction requests, builds feature vectors (precomputed features from the online store + on-demand + passed features), calls `model.predict`, and logs its inputs and outputs for monitoring and debugging. What you deploy is the pipeline, not the model alone. The model is one step inside it.

## Contract

- **Input:** a registered model + an online-serving feature view.
- **Output:** a running KServe HTTP endpoint serving predictions.
- **Pre-condition:** the model is registered in the model registry (**hops-train**), and every feature group backing the feature view is `online_enabled` (unless all features are on-demand).

## Smoke-test (cheap pre/post-flight)

`hops deployment list` / `hops deployment info <name>` / `hops deployment logs <name>` exist, but `list`/`info` currently render a blank Status even for a RUNNING deployment — confirm state with `hops deployment status <name>` (its own command) or the Python `deployment.is_running()` instead. `hops deployment delete <name>` prompts; pass `--yes` for non-interactive cleanup. The CLI `hops deployment predict --data` wants the KServe v2 shape `{"instances": [[...]]}`, not the Python `inputs=[{...}]` dict.

The whole deploy→serve→smoke loop runs from the CLI — this is what the terminal
kickoff flow uses. The model must already be registered (`hops model list`):

```bash
hops deployment create <model_name> --name <name> --version 1 --env pandas-inference-pipeline
hops deployment start <name>
hops deployment status <name>                                   # poll until READY
hops deployment predict <name> --data '{"instances": [{ <one known-good row> }]}'
hops deployment delete <name> --yes --force                    # delete prompts; --force skips the running check
```

A sane number back from `predict` (not an HTTP 500) confirms the udf runs on the
scalar serving path. `create` requires the model name as the positional and the
deployment name via `--name`; recreate over a stale deployment with
`hops deployment delete <name> --yes --force` first (there is no TTY in a job/terminal).

## Ask the user (only when state is ambiguous)

- **Resources:** CPU cores, memory, GPUs for the predictor (requests vs limits).
- **Environment:** which Python environment the predictor runs in.
- **Scaling:** min/max instances, scale-to-zero, and target concurrency (`ScaleMetric.CONCURRENCY`).
- **Before deleting** — `deployment.delete()` / `hops deployment delete --yes` tears down the running endpoint irreversibly; confirm the exact name with the user, and never tear down a deployment you created as a side effect (temp or test ones included) unless they asked.

## Model Deployment Overview

Hopsworks Model Serving deploys an online inference pipeline as an HTTP endpoint using KServe. The endpoint exposes a **deployment API** (serving keys + request parameters + return type), which is the contract clients depend on. Keep it more stable than the model signature: as long as the deployment API is unchanged you can swap the model version or move a precomputed feature to an on-demand one without breaking clients.

Library versions must match across the feature, training, and inference pipelines (e.g. the `joblib` used to pickle the model in training must be able to unpickle it here). The Hopsworks feature/training/inference base container images are version-aligned for this reason; if you customize an environment, install compatible versions.

> **sklearn serving skew.** The KServe sklearn serving image pins a specific scikit-learn (e.g. 1.3.x). A model pickled with a different scikit-learn in your training venv (e.g. 1.8.x) can fail to unpickle on the KServe deployment. Either pin training to the serving image's scikit-learn, or deploy as a `python` deployment with a cloned environment that has your training versions plus your `predictor.py`. Batch and interactive inference run in your own environment, so they have no such skew.

> **`get_model` defaults to v1, not latest.** `mr.get_model("name")` with no `version=` loads version 1 (with a warning), so a deployment keeps serving v1 after you register v2. Pass `version=` explicitly, or `mr.get_best_model("name", metric=..., direction=...)`.

Supported frameworks:

| Framework | Model Server | Requires predictor.py | Notes |
|---|---|---|---|
| Scikit-learn | PYTHON | No | Auto-loaded from model files |
| Python (custom) | PYTHON | **Yes** | Custom pickle/joblib/any model |
| PyTorch | PYTHON | **Yes** | Custom script required |
| TensorFlow | TF_SERVING | No | Script not supported |
| LLM (vLLM) | VLLM | Optional | OpenAI-compatible endpoint |

---

## Deploying a Model

The model comes from the registry (registered by the training pipeline, see
**hops-train**; `mr.python / sklearn / tensorflow / torch / llm .create_model(...)`
then `model.save(dir)`).

```python
import hopsworks
from hsml.resources import PredictorResources, Resources
from hsml.scaling_config import PredictorScalingConfig, ScaleMetric

project = hopsworks.login()
mr = project.get_model_registry()
model = mr.get_model("fraud_model", version=1)     # version= explicitly, see above

# `script_file` must be a path INSIDE the Hopsworks filesystem, not a local path.
# Upload predictor.py first (here, next to the model's files), then point at it.
# A local path fails with HTTP 400 errorCode 240016 "Predictor script does not exist".
script_dir = f"/Projects/{project.name}/Models/{model.name}/{model.version}/Files"
project.get_dataset_api().upload("predictor.py", script_dir, overwrite=True)

deployment = model.deploy(
    name="fraud_predictor",
    description="Real-time fraud detection",
    script_file=f"{script_dir}/predictor.py",   # Hopsworks path, not local
    resources=PredictorResources(
        requests=Resources(cores=1, memory=1024, gpus=0),
        limits=Resources(cores=2, memory=2048, gpus=0),
    ),
    scaling_configuration=PredictorScalingConfig(
        min_instances=1,
        max_instances=3,
        scale_metric=ScaleMetric.CONCURRENCY,   # required — omitting it fails with HTTP 422
        target=70,                              # target concurrent requests per pod
    ),
    environment="inference-pipeline",  # Python environment name
)

deployment.start(await_running=600)  # wait up to 600 seconds
```

### Predict and manage

```python
result = deployment.predict(inputs=[[1.0, 2.0, 3.0]])                 # one instance
result = deployment.predict(inputs=[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # batch
result = deployment.predict(data={"instances": [[1.0, 2.0, 3.0]]})    # KServe v2 protocol

deployment.is_running()
deployment.get_state().status
deployment.get_logs(component="predictor", tail=50)   # prints; read_logs(...) returns the string
deployment.get_endpoint_url()                          # base URL
deployment.get_inference_url()                         # with :predict suffix
deployment.stop(await_stopped=120)
deployment.delete()
```

## Robustness and latency

An online inference pipeline is a 24/7 operational service: make it robust to missing request parameters, missing or delayed precomputed features, and slow/failing third-party calls. Log errors to stdout/stderr (Hopsworks ships them to OpenSearch) and design fallbacks (impute from training statistics, use default or cached last-known values, or fall back to a simpler model) rather than letting the request fail. Set low timeouts on any network/feature lookups.

Total latency is the sum of every step (feature lookup + ODTs + MDTs + `model.predict` + logging + network), so define an SLO (p99 latency, allowed downtime) on the deployment API. For the lowest latency use a single predictor container (a separate transformer container adds a network hop), keep ODTs as low-latency Python UDFs at request time, and rely on the asynchronous logging below.

---

## Writing predictor.py Files

A predictor script must define a `Predict` class with `__init__` and `predict` methods. Do the expensive setup once in `__init__`: load the model, `hopsworks.login()`, get the feature view and `fv.init_serving(training_dataset_version=...)`. In `predict`, build the vectors with `fv.get_feature_vectors(entry=inputs, request_parameters=..., passed_features=..., return_type="pandas")` and call the model.

**Loading model files (important).** At serving time the model files mount under
`MODEL_FILES_PATH`, NOT `ARTIFACT_FILES_PATH` — that variable points at the
directory holding only the predictor script, so `joblib.load(ARTIFACT_FILES_PATH + "/model.pkl")`
fails with `FileNotFoundError`. Use a resolver that searches the known mounts and
call it from `__init__`:

```python
import os, glob

def load_model_file(name):
    """Resolve a file saved alongside the model. Model files mount under
    MODEL_FILES_PATH at serving time (ARTIFACT_FILES_PATH holds only this
    script); fall back to the standard mount roots."""
    for root in (os.environ.get("MODEL_FILES_PATH"),
                 os.environ.get("ARTIFACT_FILES_PATH"),
                 "/mnt/models", "/mnt/artifacts"):
        if root:
            hits = glob.glob(f"{root}/**/{name}", recursive=True)
            if hits:
                return hits[0]
    raise FileNotFoundError(f"{name} not found under the model/artifact mounts")
```

Include this helper in each `predictor.py`; the skeletons load with
`joblib.load(load_model_file("model.pkl"))`.

Four concrete `predictor.py` skeletons — basic, feature-store lookup, on-demand features, and passed features — are in [references/predictors.md](references/predictors.md).

### Features at request time

The feature view is the read interface: `get_feature_vector(s)`, `passed_features`,
`request_parameters`, `get_inference_helper`, `transform=False`, and the value
priority (`request_parameters` > `passed_features` > online store > on-demand
computation) are documented in [hops-fv](../hops-fv/SKILL.md). For serving, the
two things that bite:

- **Every feature group in the view must be `online_enabled`** or `init_serving()` raises. The only exception is a view whose features are all on-demand.
- **On-demand transformations (ODTs)** compute features at request time from `request_parameters`. They are registered on the **feature group** (not the view) so the same versioned function also runs in the feature pipeline, which is what keeps them equivalent across backfill and serving. `fv.request_parameters` lists what a view needs; a missing parameter fails the request. ODTs cannot use training statistics, external feature groups do not support them, and a default-mode `@udf` runs on a **scalar** online, so Series-only methods surface as an HTTP 500 on the first predict. Definition, attachment, context, local testing and the transformation store: [hops-transformations](../hops-transformations/SKILL.md).

```python
print(fv.request_parameters)             # e.g. ["store_lat", "store_lon"]
vector = fv.get_feature_vector(
    entry={"tx_id": 42},
    request_parameters={"store_lat": 40.7128, "store_lon": -74.0060},
)
```

---

## Deployment Configuration

### Resources

```python
from hsml.resources import PredictorResources, TransformerResources, Resources

predictor_resources = PredictorResources(
    requests=Resources(cores=1, memory=1024, gpus=0),  # minimum
    limits=Resources(cores=2, memory=4096, gpus=0),    # maximum
)
```

### Scaling

```python
from hsml.scaling_config import PredictorScalingConfig, ScaleMetric

scaling = PredictorScalingConfig(
    min_instances=1,              # minimum pods (0 for scale-to-zero)
    max_instances=5,              # maximum pods
    scale_metric=ScaleMetric.CONCURRENCY,  # or ScaleMetric.RPS
    target=70,                    # target concurrent requests per pod
    stable_window_seconds=60,     # averaging interval
    scale_to_zero_retention_seconds=300,  # keep last pod for 5 min
)
```

### Inference Logger

An online inference pipeline should log its inputs and outputs so the deployment can be monitored and debugged. Logging the model inputs and predictions also gives you the feature/prediction data needed for monitoring drift and model performance over time. Hopsworks logs are written asynchronously so they do not add latency to the prediction response.

```python
from hsml.inference_logger import InferenceLogger

logger = InferenceLogger(
    mode="ALL",  # "ALL", "PREDICTIONS", "MODEL_INPUTS", "NONE"
)

deployment = model.deploy(
    name="my_deployment",
    inference_logger=logger,
    ...
)
```

### Inference Batcher

```python
from hsml.inference_batcher import InferenceBatcher

batcher = InferenceBatcher(
    enabled=True,
    max_batch_size=32,
    max_latency=500,   # ms
    timeout=2000,      # ms
)
```

### Transformer (Pre/Post Processing)

A transformer runs in a separate container and processes requests before the predictor:

```python
from hsml.transformer import Transformer
from hsml.resources import TransformerResources, Resources

transformer = Transformer(
    script_file="transformer.py",
    resources=TransformerResources(
        requests=Resources(cores=1, memory=512, gpus=0),
    ),
)

deployment = model.deploy(
    name="my_deployment",
    transformer=transformer,
    ...
)
```

---

## Deployment Without a Model (Custom HTTP Server)

Deploy a custom server without a model from the registry:

```python
from hsml.predictor import Predictor

predictor = Predictor.for_server(
    name="custom_server",
    script_file="server.py",
    resources=PredictorResources(...),
)

deployment = predictor.deploy()
deployment.start()
```

---

## Next Steps

- Train and register the model this serves: **hops-train**.
- Build the online feature view it looks up: **hops-fv**. On-demand and model-dependent transforms: **hops-transformations**.
- Predictor dependencies: [hops-environments](../../platform/hops-environments/SKILL.md) — clone an inference env and install requirements.
- Offline scoring instead of a live endpoint: **hops-batch-inference**.
