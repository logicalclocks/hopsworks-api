# Deployment schema

The request contract of a deployment: the fields a client sends, their types and nullability, the response shape, and the batch limit.
Inferred from the feature view a model was registered with, published with the deployment revision, and enforced in the pod.
The parent skill's "Default predictor (no script)" section covers the common path; this file is the detail.

## Reading the contract

```python
schema = deployment.schema
schema.describe()                  # one row per field: group, name, type, nullable
schema.names                       # positional order of array rows
schema.unresolved                  # fields whose type is unknown
schema.max_batch_rows              # rows a request may carry, default 512
schema.to_json_schema()            # {"request": ..., "response": ...}
schema.to_openapi(deployment.name, url=deployment.get_inference_url())
```

Fields come in four groups, in this order, which is also the order of array rows: serving keys of the view, passed features named in `passed_features=`, request parameters of on-demand transformations, and the view's extra logging columns.
Request parameter types come from the transformation function's argument annotations, so annotate them (`def amount_ratio(amount: float, budget: float)`); an unannotated argument is unresolved and accepts any value.
Refine a schema with `schema=` on `deploy()` to pin types and descriptions; the field set itself cannot change.

Non-Python clients read it from the backend with a `SERVING`-scoped API key:

```text
GET /hopsworks-api/api/project/<id>/serving/<serving id>/schema?format=schema|jsonschema|openapi
```

`&schemaId=<id>` returns a previous revision's document. The serving id comes from `GET .../serving?name=<name>`. 404 with error code 240037 means the deployment has no schema, or the id is not published.

## Errors

Validation runs in the client, then in the pod before any predictor code.
Every error carries `code`, `message`, `schema_id` and `request_id`; messages name exception types only, so the pod log holds the detail.

| Status | Code | When |
|---|---|---|
| 400 | `SCHEMA_VALIDATION` | the request does not match the schema, with `errors` naming row, field and reason |
| 400 | `FEATURE_LOOKUP_FAILED` | the feature store refused the lookup for a reason other than a missing entity |
| 404 | `ENTITY_NOT_FOUND` | a row's serving keys match no entity; the whole batch is rejected |
| 413 | `BATCH_TOO_LARGE` | more rows than `schema.max_batch_rows` |
| 422 | `TRANSFORMATION_FAILED` | a transformation raised; the message names request parameters with no recorded type |
| 500 | `MODEL_FAILED` | the model raised |
| 500 | `CONTRACT_VIOLATION` | the pod produced the wrong number of results, or unpublished columns |
| 503 | `FEATURE_STORE_UNAVAILABLE` | the online store or the feature store API could not be reached |

A request carries either `instances` or `inputs`, never both, and a batch is all objects or all arrays.

## Enforcement

The KServe wrapper builds the enforcer from the pod's own environment (`SERVING_SCHEMA_ID`), so a pod validates the contract of its own revision, never the deployment's current configuration.
`SERVING_SCHEMA_ENFORCER` records which component validates: the transformer when the deployment has one, else the predictor.
A custom `script_file` deployed with `schema=` or `passed_features=` gets the same 400 and 413 without validating anything itself.

**Enforcement covers KServe REST V1 only.** Any other protocol, gRPC included, is served unchecked with a warning at startup, and client-side validation is REST-only as well.
Enforcement also needs an inference image whose wrapper carries it; an older image serves the deployment without checking.

Changing the batch limit through `SERVING_MAX_BATCH_ROWS` in `env_vars=` publishes a new schema id, since the limit is part of the content.
After changing the view or enabling logging, run `deployment.reinfer_schema(); deployment.save()` to publish the new contract.

## Feature logging

When the view has logging enabled, every request is logged with `request_id`, `td_version`, the model name and version, so `deployment.create_model_monitoring(...)` needs no extra code.
Declare the reserved extra columns `deployment_name`, `deployment_version`, `deployment_schema_id` (strings) and `request_row` (int) and the predictor fills them, which tells deployments apart in the log.

Logging is asynchronous only: the request thread queues the rows and answers, a background thread builds the frame, and the wrapper hands it to the inference-logger sidecar.
A logging failure never fails a request.
Both buffers are bounded by `FEATURE_LOGGER_QUEUE_SIZE` **rows** (default 1000), because one request carries a whole batch; rows beyond that are dropped and counted in the pod log and in a Prometheus counter.
The rows only arrive with an inference image whose wrapper carries the CloudEvents delivery fix; on older images the predictor log shows `Failed to send events:`.
