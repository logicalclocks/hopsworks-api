---
name: hops-online-inference
description: Use when writing code for model deployment, online inference, predictor scripts, or on-demand transformations in Hopsworks. Auto-invoke when user wants to deploy models, write predictor.py files, retrieve precomputed features for serving, create on-demand transformation functions, or configure model serving. Input registered model + online feature view; output a running KServe endpoint.
---

# Hopsworks Online Inference — Python SDK Best Practices

An **online inference pipeline** is one of the three FTI pipelines (Feature, Training, Inference): a separate program that runs 24/7 behind a network endpoint, accepts prediction requests, builds feature vectors (precomputed features from the online store + on-demand + passed features), calls `model.predict`, and logs its inputs and outputs for monitoring and debugging. What you deploy is the pipeline, not the model alone. The model is one step inside it.

## Contract

- **Input:** a registered model + an online-serving feature view.
- **Output:** a running KServe HTTP endpoint serving predictions.
- **Pre-condition:** the model is registered in the model registry, and every feature group backing the feature view is `online_enabled` (unless all features are on-demand).

## Ask the user (only when state is ambiguous)

- **Resources:** CPU cores, memory, GPUs for the predictor (requests vs limits).
- **Environment:** which Python environment the predictor runs in.
- **Scaling:** min/max instances, scale-to-zero, and target concurrency (`ScaleMetric.CONCURRENCY`).

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

### 1. Save Model to Registry

```python
import hopsworks

project = hopsworks.login()
mr = project.get_model_registry()

# Create model metadata
model = mr.python.create_model(
    name="fraud_model",
    version=1,
    description="Fraud detection model",
    metrics={"accuracy": 0.95},
    input_example={"features": [1.0, 2.0, 3.0]},
)

# Save model files to registry
model.save("./model_dir")  # directory containing model artifacts
```

Framework-specific model creation:

```python
# Scikit-learn
model = mr.sklearn.create_model("my_sklearn_model", ...)

# TensorFlow
model = mr.tensorflow.create_model("my_tf_model", ...)

# PyTorch
model = mr.torch.create_model("my_torch_model", ...)

# Python (generic)
model = mr.python.create_model("my_python_model", ...)

# LLM
model = mr.llm.create_model("my_llm", ...)
```

### 2. Deploy the Model

```python
from hsml.resources import PredictorResources, Resources
from hsml.scaling_config import PredictorScalingConfig, ScaleMetric

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

# Start the deployment
deployment.start(await_running=600)  # wait up to 600 seconds
```

### 3. Make Predictions

```python
# Simple prediction
result = deployment.predict(inputs=[[1.0, 2.0, 3.0]])

# Batch prediction
result = deployment.predict(inputs=[
    [1.0, 2.0, 3.0],
    [4.0, 5.0, 6.0],
])

# KServe v2 protocol
result = deployment.predict(data={"instances": [[1.0, 2.0, 3.0]]})
```

### 4. Manage Deployment

```python
# Check status
print(deployment.is_running())
state = deployment.get_state()
print(state.status)

# Get logs
deployment.get_logs(component="predictor", tail=50)

# Get URLs
print(deployment.get_endpoint_url())    # base URL
print(deployment.get_inference_url())   # with :predict suffix

# Stop
deployment.stop(await_stopped=120)

# Delete
deployment.delete()
```

## Smoke-test

**CLI smoke-test:** `hops deployment list` / `hops deployment info <name>` / `hops deployment logs <name>` exist, but `list`/`info` currently render a blank Status even for a RUNNING deployment — confirm state with `hops deployment status <name>` (its own command) or the Python `deployment.is_running()` instead. `hops deployment delete <name>` prompts; pass `--yes` for non-interactive cleanup. The CLI `hops deployment predict --data` wants the KServe v2 shape `{"instances": [[...]]}`, not the Python `inputs=[{...}]` dict.

### Deploy from the CLI (no Python)

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

## Robustness and latency

An online inference pipeline is a 24/7 operational service: make it robust to missing request parameters, missing or delayed precomputed features, and slow/failing third-party calls. Log errors to stdout/stderr (Hopsworks ships them to OpenSearch) and design fallbacks (impute from training statistics, use default or cached last-known values, or fall back to a simpler model) rather than letting the request fail. Set low timeouts on any network/feature lookups.

Total latency is the sum of every step (feature lookup + ODTs + MDTs + `model.predict` + logging + network), so define an SLO (p99 latency, allowed downtime) on the deployment API. For the lowest latency use a single predictor container (a separate transformer container adds a network hop), keep ODTs as low-latency Python UDFs at request time, and rely on the asynchronous logging above.

---

## Writing predictor.py Files

A predictor script must define a `Predict` class with `__init__` and `predict` methods.

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

Include this helper in each `predictor.py` below; the examples load with
`joblib.load(load_model_file("model.pkl"))`.

Four concrete `predictor.py` skeletons — basic, feature-store lookup, on-demand features, and passed features — are in [references/predictors.md](references/predictors.md).


---

## Retrieving Precomputed Features

Feature views provide the interface for looking up precomputed features from the online store during inference.

### Requirement: All Feature Groups Must Be Online-Enabled

For `get_feature_vector()` to work, **all** feature groups in the feature view must have `online_enabled=True`. The only exception is if all features are on-demand (computed at runtime).

### Single Feature Vector

```python
fv.init_serving(training_dataset_version=1)

vector = fv.get_feature_vector(
    entry={"user_id": 123},
    return_type="list",       # "list", "pandas", "polars", "numpy"
    transform=True,           # apply model-dependent transformations
)
```

### Batch Feature Vectors

```python
vectors = fv.get_feature_vectors(
    entry=[{"user_id": 123}, {"user_id": 456}],
    return_type="pandas",
)
```

### Feature Value Priority

When multiple sources provide the same feature, this priority applies (highest first):

1. **`request_parameters`** — on-demand transformation inputs
2. **`passed_features`** — runtime application values
3. **Online feature store** — precomputed stored values
4. **On-demand computation** — computed by transformation functions

### Inference Helper Columns

Features not used in the model but useful for post-processing (e.g., customer name for display):

```python
# Retrieved separately from feature vectors
helpers = fv.get_inference_helper(
    entry={"user_id": 123},
    return_type="dict",
)
```

---

## On-Demand Transformation Functions

On-demand transformations (ODTs) compute features at request time from request parameters (and optionally precomputed features). They are registered on **feature groups** (not feature views) because they also run in feature pipelines, and they are automatically included in feature views that select those features. This is what keeps ODTs equivalent across offline (training/backfill) and online (serving) execution: the same versioned function and its source code is used in both, avoiding training/serving skew. Contrast with model-dependent transformations (MDTs), which are registered on the feature view, run after reading from the feature store, are specific to one model, and may use training-data statistics.

### Key Constraints

- On-demand transformations **cannot** use training statistics (no `statistics` parameter); that is what distinguishes an ODT from an MDT
- On-demand transformations are `TransformationType.ON_DEMAND` (set automatically when attached to a feature group)
- External feature groups do **not** support on-demand transformations

### Defining On-Demand Transformations

```python
from hopsworks import udf

# Simple on-demand feature
@udf(float)
def distance_to_store(latitude, longitude, store_lat, store_lon):
    import math
    dlat = math.radians(store_lat - latitude)
    dlon = math.radians(store_lon - longitude)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(latitude)) * math.cos(math.radians(store_lat)) * math.sin(dlon/2)**2
    return 6371 * 2 * math.asin(math.sqrt(a))

# On-demand feature with context
@udf(str)
def time_bucket(timestamp, context):
    hour = timestamp.hour if hasattr(timestamp, 'hour') else 0
    bucket_size = context.get("bucket_hours", 4)
    bucket = (hour // bucket_size) * bucket_size
    return f"{bucket:02d}-{bucket + bucket_size:02d}"
```

### Attaching to a Feature Group

When you pass transformation functions to a feature group constructor, they are **automatically** typed as `ON_DEMAND`:

```python
fg = fs.get_or_create_feature_group(
    name="enriched_transactions",
    version=1,
    primary_key=["tx_id"],
    event_time="timestamp",
    online_enabled=True,
    stream=True,
    features=[
        Feature("tx_id", "bigint"),
        Feature("latitude", "double"),
        Feature("longitude", "double"),
        Feature("amount", "double"),
        Feature("timestamp", "timestamp"),
    ],
    transformation_functions=[
        distance_to_store("latitude", "longitude", "store_lat", "store_lon"),
    ],
)
```

The on-demand feature `distance_to_store` will:
- Appear as a feature with `on_demand=True` in the feature group schema
- Be automatically computed when inserted (with `transform=True`)
- Be included in feature views that select from this feature group
- Require `store_lat` and `store_lon` as `request_parameters` during `get_feature_vector()`

### Using On-Demand Features in Feature Views

On-demand features from feature groups are automatically carried into feature views:

```python
fv = fs.create_feature_view(
    name="tx_fv",
    query=fg.select_all(),
    labels=["is_fraud"],
)

# Check required request parameters
print(fv.request_parameters)  # e.g., ["store_lat", "store_lon"]

# Feature vector retrieval — must provide request_parameters
fv.init_serving(training_dataset_version=1)

vector = fv.get_feature_vector(
    entry={"tx_id": 42},
    request_parameters={"store_lat": 40.7128, "store_lon": -74.0060},
)
```

### Saving/Registering Transformation Functions

You can also save transformation functions to the feature store for reuse:

```python
# Create (lazy — does not persist)
tf = fs.create_transformation_function(
    transformation_function=distance_to_store,
    version=1,
)

# Persist to backend
tf.save()

# Retrieve later
tf = fs.get_transformation_function("distance_to_store", version=1)

# List all
all_tfs = fs.get_transformation_functions()
```

### Setting Transformation Context

For transformations that accept a `context` parameter:

```python
tf = time_bucket("timestamp")
tf.transformation_context = {"bucket_hours": 6}

fg = fs.get_or_create_feature_group(
    name="my_fg",
    transformation_functions=[tf],
    ...
)
```

### Testing On-Demand Transformations Locally

```python
# Test offline (batch) execution
executor = tf.executor(online=False)
result = executor.execute(pd.Series(["2025-03-15 14:30:00"]))

# Test online (single value) execution
executor = tf.executor(online=True, context={"bucket_hours": 6})
result = executor.execute("2025-03-15 14:30:00")
```

---

## Deployment Configuration

### Resources

```python
from hsml.resources import PredictorResources, TransformerResources, Resources

# Predictor resources
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

## Complete Example: End-to-End Online Inference Pipeline

```python
import hopsworks
from hsml.resources import PredictorResources, Resources
from hsml.scaling_config import PredictorScalingConfig, ScaleMetric

# 1. Connect
project = hopsworks.login()
fs = project.get_feature_store()
mr = project.get_model_registry()
ms = project.get_model_serving()

# 2. Get or train model (assume model.pkl saved locally)
model = mr.python.create_model(
    name="fraud_detector",
    version=1,
    description="XGBoost fraud detection model",
    metrics={"f1": 0.92},
    feature_view=fs.get_feature_view("fraud_fv", version=1),
    training_dataset_version=1,
)
model.save("./model_artifacts")

# 3. Deploy with predictor script (upload it to the Hopsworks FS first)
script_dir = f"/Projects/{project.name}/Models/{model.name}/{model.version}/Files"
project.get_dataset_api().upload("predictor.py", script_dir, overwrite=True)
deployment = model.deploy(
    name="fraud_predictor",
    script_file=f"{script_dir}/predictor.py",
    resources=PredictorResources(
        requests=Resources(cores=1, memory=1024, gpus=0),
        limits=Resources(cores=2, memory=2048, gpus=0),
    ),
    scaling_configuration=PredictorScalingConfig(
        min_instances=1,
        max_instances=5,
        scale_metric=ScaleMetric.CONCURRENCY,  # required, else HTTP 422
        target=50,
    ),
)

# 4. Start serving
deployment.start(await_running=600)

# 5. Test prediction
result = deployment.predict(inputs=[{"user_id": 42}])
print(result)

# 6. Check status
print(f"Running: {deployment.is_running()}")
print(f"URL: {deployment.get_inference_url()}")
deployment.get_logs(component="predictor", tail=20)
```

The corresponding `predictor.py`:

```python
import os
import joblib
import hopsworks

class Predict:
    def __init__(self):
        self.model = joblib.load(load_model_file("model.pkl"))
        
        project = hopsworks.login()
        fs = project.get_feature_store()
        self.fv = fs.get_feature_view("fraud_fv", version=1)
        self.fv.init_serving(training_dataset_version=1)

    def predict(self, inputs):
        feature_vectors = self.fv.get_feature_vectors(
            entry=inputs,
            return_type="pandas",
        )
        predictions = self.model.predict(feature_vectors).tolist()
        return {"predictions": predictions}
```

---

## Quick Reference

| Task | Code |
|---|---|
| Save model | `model = mr.python.create_model(...); model.save("./dir")` |
| Deploy model | `deployment = model.deploy(name=..., script_file=...)` |
| Start deployment | `deployment.start(await_running=600)` |
| Predict | `deployment.predict(inputs=[[1, 2, 3]])` |
| Stop deployment | `deployment.stop()` |
| Delete deployment | `deployment.delete()` |
| Get logs | `deployment.get_logs(component="predictor", tail=50)` |
| Get deployments | `ms.get_deployments()` |
| Init online serving | `fv.init_serving(training_dataset_version=1)` |
| Get feature vector | `fv.get_feature_vector(entry={"pk": val})` |
| With on-demand | `fv.get_feature_vector(entry=..., request_parameters=...)` |
| With passed features | `fv.get_feature_vector(entry=..., passed_features=...)` |
| Define on-demand UDF | `@udf(float)\ndef my_fn(feature): ...` |
| Attach to FG | `fs.get_or_create_feature_group(..., transformation_functions=[my_fn("col")])` |
| Save TF to store | `fs.create_transformation_function(my_fn).save()` |
| Get TF from store | `fs.get_transformation_function("name", version=1)` |

---

## Next Steps

- Train and register the model this serves: **hops-train**.
- Build the online feature view it looks up: **hops-fv**.
- Predictor dependencies: [hops-environments](../hops-environments/SKILL.md) — clone an inference env and install requirements.
- Offline scoring instead of a live endpoint: **hops-batch-inference**.
