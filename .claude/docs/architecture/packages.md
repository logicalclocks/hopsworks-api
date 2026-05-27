# Packages — Detailed Breakdown

All four packages live under `python/` and share `python/pyproject.toml`.

## `hopsworks` — Main Entry Point

User-facing library.
Primary import: `import hopsworks`.

Key entry points:

```python
project = hopsworks.login(host=..., port=443, project=..., api_key_value=...)

fs  = project.get_feature_store()
mr  = project.get_model_registry()
ms  = project.get_model_serving()
ds  = project.get_dataset_api()
```

`Project` is a thin facade.
Each sub-API is a class in `core/` instantiated lazily via `get_*()` methods.

## `hopsworks_common` — Shared Base

Infrastructure used by all other packages.
Not user-facing directly, but a lot of entities are exposed via aliases in `hopsworks`.

- `client/base.py` — `Client` base: session, retries, request/response cycle
- `client/auth.py` — API key and certificate auth
- `client/exceptions.py` — `RestAPIError`, `FeatureStoreException`, `NoHopsworksConnectionError`
- `connection.py` — connection singleton, state management
- `util.py` — `Encoder` (JSON), `VersionWarning`, feature name coercion
- `constants.py` — shared string constants (headers, endpoints, env vars)
- `decorators.py` — `@connected`, `@not_connected`, `@catch_not_found`, optional-dep guards
- `usage.py` — optional telemetry

`Encoder` is a custom `json.JSONEncoder` that calls `.to_dict()` on objects before falling back to the default.
All domain objects implement `.to_dict()` and a `from_response_json()` class method.

## `hsfs` — Feature Store

Create, read, and update features, feature groups, and feature views.
The largest package.

Core user flow:

```python
fs = project.get_feature_store()

fg = fs.get_or_create_feature_group("transactions", version=1, ...)
fg.insert(df)

fv = fs.get_feature_view("transaction_features", version=1)
df = fv.get_batch_data(start_time=..., end_time=...)
vec = fv.get_online_feature_vector({"customer_id": 42})
```

`FeatureGroupEngine` and `FeatureViewEngine` in `core/` coordinate HTTP calls (via `core/<entity>_api.py`) with local computation (via `engine/python.py` or `engine/spark.py`).
The engine is selected at connection time via the `engine` parameter of `hopsworks.login()`.

## `hsml` — ML Registry and Serving

Register trained models, create deployments, run inference.

Core user flow:

```python
mr = project.get_model_registry()
model = mr.torch.create_model("my_model", version=1, metrics={"acc": 0.97})
model.save("./saved_model")

ms = project.get_model_serving()
dep = ms.create_deployment(predictor=..., name="my_deployment")
dep.start()
dep.predict({"instances": [[1, 2, 3]]})
```

Framework subclasses (`TorchModel`, `SkLearnModel`, `TensorFlowModel`, `PythonModel`, `LlmModel`) add framework-specific `save()` and `load()` logic.
Accessed via `mr.<framework>.create_model(...)`.
