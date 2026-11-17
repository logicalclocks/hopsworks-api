# Online inference: where the time goes

### Where the time goes

Tracing times the on-demand and model-dependent transformations of the feature view a model (or feature view) deployment serves.
Create the deployment with tracing to be able to use it; this adds a collector sidecar to each predictor pod, so a deployment created without it has to be updated with tracing, which restarts it.
A traced deployment serves one worker per replica, so its per-process metrics count every request; size its replicas for that.

```python
deployment = model.deploy(tracing={"enabled": True, "sampling_percentage": 10})

# an existing deployment: restarts its pods
deployment.tracing = {"enabled": True}
deployment.save()
```

A deployment created with tracing can then be switched off and on, and resampled, while it runs, with no restart; the running pods apply it within about a minute:

```python
deployment.set_tracing(active=False)                           # no metrics, no traces, no timing code
deployment.set_tracing(active=True, sampling_percentage=50)
```

The same over REST: `PUT /hopsworks-api/api/project/{project_id}/serving/{deployment_id}/tracing` with `{"active": false}` or `{"active": true, "samplingPercentage": 50}`; omitted fields keep their values.
It answers 400 (`240062`) for a deployment created without tracing, and the deployment page has the same switch, and a dropdown for the sampling, on its X-ray latencies section.
`sampling_percentage` can be a fraction: `0.01` traces one request in 10,000.

Traces are kept only in the online store, in the project's `serving_traces` feature group, and expire after its time to live (36 hours by default; change it with `fg.enable_ttl(ttl=...)` on that feature group); nothing is materialized offline.
They are bounded: at most 60 traces per minute per predictor pod whatever the sampling, and paused on every model and feature view deployment while less than 20% of the online store's disk tablespace (`deployment.tracing.paused_reason == "DISK_NEARLY_FULL"`) or of its RonDB DataMemory (`"MEMORY_NEARLY_FULL"`) is free; it resumes once both have 10 points more free.
The cluster administrator sets the limits (`serving_tracing_online_ttl_hours`, `serving_tracing_max_traces_per_minute`, `serving_tracing_pause_free_percent`).

While tracing is on, every request is counted on the predictor's Prometheus endpoint:

| Metric | Labels | Meaning |
|---|---|---|
| `hopsworks_transformations_calls_total`, `hopsworks_transformations_seconds_total` | `feature_view`, `feature_view_version`, `kind` | Count and total time of all on-demand (`kind="on_demand"`) or all model-dependent (`kind="model_dependent"`) transformations of one feature vector; the average is `rate(seconds) / rate(calls)` |
| `hopsworks_transformations_window_min_seconds`, `..._window_max_seconds` | the above | Fastest and slowest feature vector in the last complete 60 s window |
| `hopsworks_transformation_function_calls_total`, `..._seconds_total`, `..._errors_total`, `..._window_min_seconds`, `..._window_max_seconds` | the above, `transformation`, `outputs` | The same per transformation function; `outputs` names it, since one function can be attached to several features |
| `hopsworks_transformation_dag_edge` | the above, `depends_on` | 1 per edge of the transformation DAG; `depends_on` is the `outputs` of the function it consumes, empty for a function fed only by features |
| `hopsworks_prediction_stage_seconds` (histogram), `hopsworks_prediction_stage_window_min_seconds`, `..._window_max_seconds` | `stage`, `name` | Time per request in `feature_lookup` (including transformations), `transformations`, `model_predict` and `feature_log`, of the feature view or model `name` (`<name>:<version>`) |

The time per stage, over the last `1m`, `10m`, `1h`, `6h` or `24h`, is also one call: `GET /hopsworks-api/api/project/{project_id}/serving/{deployment_id}/latency?window=1h` returns one entry per stage and name with `requests`, `minSeconds`, `p50Seconds`, `p95Seconds`, `p99Seconds` and `maxSeconds`; the deployment page shows it as Request time by stage.

The Grafana deployment dashboard ("View in Grafana" in the Metrics section of the deployment page) charts them under "Feature view transformations".

A sample of the requests (10% unless `sampling_percentage` says otherwise) is also traced, and the deployment page shows them in its X-ray latencies section: `predict` > `feature_lookup` > `transformations` (first row only) > `on_demand_transformations` > one span per function in DAG order, then `model_dependent_transformations`, then `model_predict` and `feature_log`, with the background `feature_log_write` and `feature_view.log` in the same trace.
Traces are written to the online store through a collector sidecar, so keep the sampling low on a busy deployment.

---
