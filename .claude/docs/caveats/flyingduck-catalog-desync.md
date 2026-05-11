# FlyingDuck catalog says "Table does not exist" after a successful materialization

Reading a HUDI feature group via the Python engine in 4.x returns:

```
pyarrow._flight.FlightServerError: Catalog Error: Table with name
<project>.<fg_name>_<version> does not exist!
Did you mean "pg_description"?
```

even when the feature group's commit history is non-empty (`fg.commit_details()` returns commits) and the offline materialization job has finished with `SUCCEEDED`.
The python engine in 4.x dropped the Hive fallback (`ValueError: Reading data with Hive is not supported when using hopsworks client version >= 4.0`), so once Arrow Flight refuses the read there is no second path.

This is a backend / infra issue, not an SDK bug.
The end-to-end debug walkthrough below was done against a live cluster and pinned the failure to FlyingDuck's per-query registration step in `flyingduck/src/query_engine.py:_register_featuregroups`.

## What the SDK actually sends

The Python engine resolves the AF payload via `QueryConstructorApi.construct_query(query, hqs=True)`. The payload includes the table name, the connectors dict and the feature schema:

```json
{
  "query_string": "SELECT \"fg0\".\"day\" \"day\", \"fg0\".\"value\" \"value\" FROM \"onboarding_project_test.probe_own_1\" \"fg0\"",
  "features": { "onboarding_project_test.probe_own_1": [{"name": "day", "type": "date"}, ...] },
  "connectors": { "onboarding_project_test.probe_own_1": {"feature_group_id": 1168, "time_travel_type": "hudi", "type": null, "alias": "...", "filters": null} }
}
```

The table identifier (`<project>.<fg_name>_<version>`) is identical on both sides of the payload, so this is not a name mismatch.

## What FlyingDuck does with that payload

`flyingduck/src/query_engine.py` opens a fresh in-memory DuckDB connection per query, then loops over `connectors` and calls:

- `HudiHopsFSClient.get_featuregroup_parquet_paths(fg_name, filters, feature_group_id)` for `time_travel_type == "hudi"`.
- That resolves the HUDI path through `FSUtils.get_featuregroup_path` which simply maps `<project>.<fg_name>_<version>` to `<warehouse_path>/<project>_featurestore.db/<fg_name>_<version>` (warehouse default `/apps/hive/warehouse`).
- It then reads `.hoodie/hoodie.properties` and lists `.hoodie/timeline/` (or its pre-version-7 equivalent) for commit timestamps, and walks the partition tree for parquet files.

If any of those listings fails (returns empty, raises, or hits zero files), `hopsfs_query_engine.register` either registers an empty zero-row view or aborts before `CREATE TEMP VIEW` runs. If the loop never runs (connector silently absent, time-travel type unrecognised), no view is created at all. Either way the subsequent `duckdb_con.execute(query_string)` raises DuckDB's `Catalog Error: Table ... does not exist`, which the server wraps in `FlightServerError(str(e))` via `_log_exceptions`.

`Catalog Error` is the **DuckDB-side** symptom; the cause is upstream in the HopsFS listing step.

## Likely root causes on a healthy-data cluster

When `fg.commit_details()` reports a real commit but the read still fails:

1. **HopsFS visibility from the FlyingDuck pod.** The pod connects to HopsFS as its own user. If that user lacks read on `/apps/hive/warehouse/<other_project>_featurestore.db/...` (cross-project share without the matching ACL) or on its own project's warehouse path (newly created project where the FlyingDuck identity has not been granted), the recursive listing returns empty and registration drops the view.
2. **Stale warehouse mount.** The pod sees a different HopsFS root or a snapshot that pre-dates the commit. Rare in production but possible after a kube restart with persistent volumes.
3. **Write/read race on a fresh project.** `fg.insert(...)` returns after the Kafka write but the offline materialization job (`<fg>_<v>_offline_fg_materialization`) runs asynchronously. Even when the SDK reports `FINISHED/SUCCEEDED`, the HopsFS metadata visible to FlyingDuck may lag for a beat. Wait an additional cycle and retry before declaring a permanent failure.

## Probes that helped confirm the diagnosis

```python
# 1. Commit exists in metadata.
fg.commit_details()
# {1778161074323: {'committedOn': '20260507133754323', 'rowsUpdated': 0, 'rowsInserted': 30, ...}}

# 2. Materialization ran.
fg.materialization_job.get_executions()[-1].final_status  # SUCCEEDED

# 3. SDK payload is well-formed; connectors carry time_travel_type=hudi.
fs_query = fg.select_all()._query_constructor_api.construct_query(fg.select_all(), hqs=True)
import json; json.loads(fs_query.hqs_payload)
```

If 1 and 2 pass but `fg.read()` still raises the catalog error, the next hop is the FlyingDuck pod logs (`/opt/flyingduck/logs/` in the standard chart) for the per-query trace ID. That log will show whether `_register_featuregroups` was reached, which connector branch was taken, and whether the recursive HopsFS listing returned zero files.

## Two CLI / SDK fixes spotted while investigating

- `arrow_flight_client._disable_feature_query_service_client()` crashed with `AttributeError` on a brand-new session because the `None` branch tried to call `.ArrowFlightClient(...)` on the module global instead of constructing one. Fixed in this branch.
- `hops <command> --json` mixed SDK chatter with the JSON payload because the SDK calls `logging.basicConfig(stream=sys.stdout)` at import time and the login banner is a bare `print`. `output.set_json_mode(True)` now snapshots `sys.stdout`, swaps it for `sys.stderr`, patches the existing root-logger handler in place, and raises the noisy SDK loggers to WARNING so JSON survives the trip. Fixed in this branch.
