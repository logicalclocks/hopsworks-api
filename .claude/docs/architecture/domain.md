# Domain Concepts

## Feature Store

The Feature Store is the central platform component for developing and operating ML models at scale.
It solves the training-serving skew problem by providing a single source of truth for features used both during training and at inference time.
Feature pipelines write to feature groups; models read from feature views.

### Feature

A metadata descriptor for one column: name, data type, whether it is a primary key, partition key, or event time, and an optional description.
Lives inside a `FeatureGroup` definition; not a standalone entity.

### FeatureGroup

A versioned table of features with a primary key, an optional event time column (recording when feature values were observed), and an optional partition key (for efficient time- or category-scoped queries).

Each feature group has two storage layers:

- Online store — holds only the latest feature values per primary key; optimized for low-latency real-time serving
- Offline store — holds the full history of feature values; used for training data creation and batch scoring

Three variants:

- `FeatureGroup` — features computed and written by your own pipelines (Python, Spark, or Flink)
- `ExternalFeatureGroup` — read-only access to features managed in an external system (Snowflake, S3, Redshift, …); Hopsworks does not copy the data
- `SpineGroup` — an entity-time index used to anchor point-in-time joins against other feature groups, preventing data leakage

### FeatureView

A named, versioned logical grouping of features from one or more feature groups.
Does not store feature data — it is the specification of which features a model needs and how to retrieve them.
The same feature view is used for both training data creation and online serving, ensuring the features are computed identically in both contexts.

Key operations:

- `get_training_data(...)` — materializes a training dataset with point-in-time correct joins
- `get_batch_data(start_time, end_time)` — retrieves a batch of historical feature values for batch scoring
- `get_online_feature_vector(entity_dict)` — retrieves the latest feature values for a single entity from the online store
- `get_online_feature_vectors(entity_dict)` — batch variant of the above

A feature view can include labels (target variables) for supervised learning and carries transformation functions that are applied consistently between training and serving.

### Query, Filter, Join

`Query` is the result of `feature_group.select(features)`.
It is a lazy descriptor — it describes a retrieval, not an execution.
Compose queries with joins and filters:

```python
q = fg1.select(["amount", "timestamp"]).join(
    fg2.select(["merchant_name"]),
    on=["merchant_id"],
    join_type="left",
).filter(fg1.amount > 0)
```

`Filter` objects are created via comparison operators on `Feature`: `fg.amount > 0`, `fg.category.isin(["A", "B"])`.

### StorageConnector

Describes an external data source used either to back an `ExternalFeatureGroup` or to read raw data into a feature pipeline.
Types: `S3Connector`, `SnowflakeConnector`, `RedshiftConnector`, `JdbcConnector`, `GcsConnector`, `BigQueryConnector`, `KafkaConnector`, `HopsFSConnector`.

### TransformationFunction

A user-defined Python function attached to a `FeatureView` that transforms raw feature values (normalization, encoding, embedding lookups, etc.).
Applied consistently at training time and serving time.
Stored and versioned in Hopsworks alongside the feature view.

## ML Registry and Serving

### Model

A versioned artifact in the model registry, private to a project.
Stores model files in their native framework format, plus metrics, input/output schema, sample test data, provenance links to source notebooks and datasets, and environment metadata.
Framework subclasses (`TorchModel`, `TensorFlowModel`, `SkLearnModel`, `PythonModel`, `LlmModel`) add `save()` and `load()` appropriate for each framework.
Integrates with KServe for deployment.

### Deployment

A running inference service deployed on Kubernetes via KServe.
Wraps a `Predictor` (required) and optionally a `Transformer`.
Exposed via REST and gRPC endpoints secured with API key authentication.
Key methods: `start()`, `stop()`, `predict(data)`, `get_logs()`.

### Predictor

Configures the model server: which model to serve, the serving runtime (Python, TensorFlow Serving, or vLLM), instance count, resource limits, and scaling behavior.

### Transformer

Optional pre- and post-processing layer that runs before and after the predictor.
Typically used to fetch online features from the Feature Store and enrich the raw inference request with them before passing it to the model.
Not available for vLLM deployments.

### InferenceLogger

Captures transformer and predictor inputs and outputs to a Kafka topic within the project.
Used for audit trails, monitoring, and online learning pipelines.

### InferenceBatcher

Batches multiple concurrent inference requests to improve throughput, at the cost of a small increase in latency per request.

## Operational Objects

- `Project` — top-level container; all feature groups, models, jobs, and secrets belong to a project
- `Job` — a scheduled or on-demand execution unit (Python, Spark, or Flink)
- `Execution` — a single run of a `Job`; carries logs, state, and duration
- `Environment` — a named Python environment for jobs and notebooks
- `Secret` — an encrypted key-value pair scoped to a project or shared across projects
- `Alert` — a notification rule triggered by feature group or deployment events
- `KafkaTopic` / `KafkaSchema` — Kafka topics and Avro schemas managed within a project
