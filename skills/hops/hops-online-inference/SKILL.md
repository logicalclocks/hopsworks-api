---
name: hopsworks-online-inference
description: Use when writing code for model deployment, online inference, predictor
  scripts, or on-demand transformations in Hopsworks. Auto-invoke when user wants to
  deploy models, write predictor.py files, retrieve precomputed features for serving,
  create on-demand transformation functions, or configure model serving.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Online Inference — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hsml/` and `/tmp/hopsworks-api/python/hsfs/`

## Model Deployment Overview

Hopsworks Model Serving deploys models as HTTP endpoints using KServe. Supported frameworks:

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
from hsml.scaling_config import PredictorScalingConfig

deployment = model.deploy(
    name="fraud_predictor",
    description="Real-time fraud detection",
    script_file="predictor.py",       # required for Python/PyTorch
    resources=PredictorResources(
        requests=Resources(cores=1, memory=1024, gpus=0),
        limits=Resources(cores=2, memory=2048, gpus=0),
    ),
    scaling_configuration=PredictorScalingConfig(
        min_instances=1,
        max_instances=3,
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

---

## Writing predictor.py Files

A predictor script must define a `Predict` class with `__init__` and `predict` methods.

### Basic Predictor

```python
# predictor.py
import os
import joblib

class Predict:
    def __init__(self):
        """Called once when the deployment starts.
        
        Load model, initialize feature view, set up resources here.
        The model files are available in the current working directory.
        """
        self.model = joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl")

    def predict(self, inputs):
        """Called for each inference request.
        
        Parameters:
            inputs: List of input instances (list of lists)
            
        Returns:
            dict with "predictions" key containing the results
        """
        return {"predictions": self.model.predict(inputs).tolist()}
```

### Predictor with Feature Store Lookup

The most common pattern: look up precomputed features from the online feature store, then predict.

```python
# predictor.py
import os
import joblib
import hopsworks

class Predict:
    def __init__(self):
        # Load model
        self.model = joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl")
        
        # Connect to feature store
        project = hopsworks.login()
        fs = project.get_feature_store()
        
        # Initialize feature view for online serving
        self.fv = fs.get_feature_view("fraud_features_fv", version=1)
        self.fv.init_serving(training_dataset_version=1)

    def predict(self, inputs):
        """inputs: list of dicts with primary keys, e.g. [{"user_id": 123}]"""
        # Look up precomputed features from online store
        feature_vectors = self.fv.get_feature_vectors(
            entry=inputs,
            return_type="pandas",
        )
        
        # Predict
        predictions = self.model.predict(feature_vectors).tolist()
        return {"predictions": predictions}
```

### Predictor with On-Demand Features

Combine precomputed features with on-demand features computed at request time:

```python
# predictor.py
import os
import joblib
import hopsworks

class Predict:
    def __init__(self):
        self.model = joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl")
        
        project = hopsworks.login()
        fs = project.get_feature_store()
        
        self.fv = fs.get_feature_view("recommendation_fv", version=1)
        self.fv.init_serving(training_dataset_version=1)

    def predict(self, inputs):
        """inputs: list of dicts with primary keys AND request parameters.
        
        Example: [{"user_id": 123, "query_text": "running shoes", "current_location": "NYC"}]
        """
        entries = []
        request_params = []
        
        for inp in inputs:
            # Separate primary keys from request parameters
            entries.append({"user_id": inp["user_id"]})
            request_params.append({
                "query_text": inp.get("query_text", ""),
                "current_location": inp.get("current_location", ""),
            })
        
        # Get feature vectors with on-demand features computed
        feature_vectors = self.fv.get_feature_vectors(
            entry=entries,
            request_parameters=request_params,
            return_type="pandas",
        )
        
        predictions = self.model.predict(feature_vectors).tolist()
        return {"predictions": predictions}
```

### Predictor with Passed Features

Provide feature values from the application that override or supplement stored features:

```python
# predictor.py
class Predict:
    def __init__(self):
        # ... init model and feature view ...
        pass

    def predict(self, inputs):
        """inputs: [{"user_id": 123, "session_duration": 45.2, "device": "mobile"}]"""
        entries = []
        passed = []
        
        for inp in inputs:
            entries.append({"user_id": inp["user_id"]})
            passed.append({
                "session_duration": inp["session_duration"],
                "device": inp["device"],
            })
        
        vectors = self.fv.get_feature_vectors(
            entry=entries,
            passed_features=passed,
            return_type="pandas",
        )
        return {"predictions": self.model.predict(vectors).tolist()}
```

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

On-demand transformations compute features at request time. They are attached to **feature groups** and automatically included in feature views that select those features.

### Key Constraints

- On-demand transformations **cannot** use training statistics (no `statistics` parameter)
- On-demand transformations are `TransformationType.ON_DEMAND` (set automatically when attached to a feature group)
- External feature groups do **not** support on-demand transformations

### Defining On-Demand Transformations

```python
from hsfs import udf

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
from hsml.scaling_config import PredictorScalingConfig

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

# 3. Deploy with predictor script
deployment = model.deploy(
    name="fraud_predictor",
    script_file="predictor.py",
    resources=PredictorResources(
        requests=Resources(cores=1, memory=1024, gpus=0),
        limits=Resources(cores=2, memory=2048, gpus=0),
    ),
    scaling_configuration=PredictorScalingConfig(
        min_instances=1,
        max_instances=5,
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
        self.model = joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl")
        
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
