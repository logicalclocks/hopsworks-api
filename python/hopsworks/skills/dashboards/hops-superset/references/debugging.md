# Debugging Superset

Part of **hops-superset**.

| Symptom | Likely cause | Fix |
|---|---|---|
| `Item with key "X" is not registered` | Legacy viz_type removed | Swap to modern key from §3 |
| `Empty query?` (histogram) | Used `all_columns_x` | Use `column` (single string) |
| `Empty query?` (bar) | Used `groupby` instead of `x_axis` | Set `x_axis`, optionally add `x_axis_force_categorical: true` |
| `Dataset ... already exists` | Non-idempotent create | Use `ensure_dataset` (list-then-create) |
| `400 {"message":{"description":["Unknown field."]}}` | SDK signature includes `description` but Superset REST rejects it | Drop `description` from `create_dataset`/`create_chart`/`create_dashboard` calls |
| Chart renders but absent from dashboard | Only `position_json` was set | Also call `update_chart(cid, dashboards=[dashboard_id])` |
| Trino query "Schema not found" | Missing catalog prefix | Use `SELECT * FROM delta.<project>_featurestore.<fg>_<version>` (`hudi.` / `iceberg.` for those formats) |
| Trino query "Table not found" | Missing `_<version>` suffix | FG tables are always suffixed with version (`customers_1`, not `customers`) |
| Bars come out unsorted | `x_axis_sort` doesn't match a metric label | Make `x_axis_sort` equal to the metric's `label` field |
| Time-series bar shows one bar | X axis is a string but `x_axis_force_categorical` missing | Add `x_axis_force_categorical: true` |
| Auth error from `api._request` | Session expired across long runs | Re-fetch `project.get_superset_api()` |
