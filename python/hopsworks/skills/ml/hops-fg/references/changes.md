# Deleting rows and evolving the schema

## Deleting rows

`fg.remove_rows(df)` deletes the rows matching the DataFrame's keys. Both stores
of an online-enabled FG are affected by default; `storage="offline"` /
`storage="online"` deletes from one store only, and a single-store delete is
never reconciled later.

```python
import polars as pl

# primary_key column(s) + event_time (+ partition columns, if any)
rows_to_delete = pl.DataFrame({
    "trans_id": [101, 202, 303],
    "event_ts": ["2026-01-01", "2026-01-02", "2026-01-03"],
})

fg.remove_rows(rows_to_delete)
```

- **Offline (Delta) with an `event_time`:** the merge key is the primary key **plus** `event_time` (plus partition columns). A primary-key-only DataFrame fails with `DeltaError: No field named <event_time>`.
- **Online:** matches on the primary key alone; the other columns are still required for the offline merge but do not affect which online rows go.
- **Stream FGs** (DELTA, ICEBERG, HUDI all supported): inserts reach the offline table through the materialization job while the delete hits it directly. Deleting a row whose insert is not yet materialized removes it online and the job then writes it offline; run the materialization job first, or re-run the delete after it.
- **Embedding-index FGs:** online delete is unsupported (the online data lives in the vector DB). The delete is offline-only with a warning; `storage="online"` raises.
- `fg.commit_delete_record(...)` is the deprecated name and always deletes offline only.

## Evolving the schema

Two cases, split by whether downstream consumers can be disturbed.

**Add a column: append in place, same version.** Appending keeps the feature
group version, so projects reading the FG downstream keep working.
`get_or_create_feature_group` returns the existing FG and ignores a changed
`features=` list, so re-running a pipeline never adds columns, and `fg.insert()`
with extra columns fails with `Features are not compatible with Feature Group
schema`. Append explicitly instead:

```python
from hsfs.feature import Feature

fg.append_features([Feature("score", "double"), Feature("tier", "string")])
```

CLI: `hops fg feature add <name> --features "score:double:Risk score,tier:string"`: the spec is `name:type[:description]`, so set a description per column.

Rules and consequences:
- Append-only. New columns cannot be primary or partition keys.
- Existing rows are not backfilled. They read null for the new column until reinserted.
- Feature views over this FG keep their old projection and do not see the new columns. Build a new feature view (via `fg.select(...)`) to use them.

**Drop a column, rename, or change a type: new version.** The backend rejects
these in place: a feature group used downstream must not change shape under its
consumers. Create the next version with the new schema and migrate readers to
it:

```python
fg_v2 = fs.get_or_create_feature_group(name="my_fg", version=2, primary_key=[...], features=[...])
```

`hops fg delete` then recreate is data loss, not schema evolution. Reserve it for a throwaway FG that nothing reads yet.
