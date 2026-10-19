# Reading feature groups

## Basic Read

```python
# Full read (offline store); specify dataframe_type to get Polars, Pandas, etc.
df = fg.read(dataframe_type="polars")         # or "pandas", "spark", "numpy"

# Online store read
df = fg.read(online=True, dataframe_type="polars")
```

## Reading from a shared store

An FG in a **shared** store (the STORE column from `hops fg list`, e.g. imported
public tables) is NOT reachable through this project's default feature store
handle: `project.get_feature_store().get_feature_group(name, version=1)` returns
`None` for it. Pass the store name explicitly:

```python
shared_fs = project.get_feature_store(name="<that_store>")   # the STORE value
fg = shared_fs.get_feature_group("<name>", version=1)
df = fg.read(dataframe_type="polars")
```

In a job environment `fs.get_feature_groups()` / `get_all()` may be absent, so
resolve the shared FG by store name as above rather than enumerating.

## Time-Filtered Read

Read a slice by event time (requires `event_time` set on the FG):

```python
df = fg.read(start_time="2026-01-01", end_time="2026-03-01", dataframe_type="polars")
```

## Point-in-Time Read (Time Travel)

```python
df = fg.read(wallclock_time="2026-01-15", dataframe_type="polars")
```

## Filtered Read

Apply filters before reading to push predicates down and reduce data transfer:

```python
# Single filter
df = fg.filter(fg.amount > 100).read(dataframe_type="polars")

# Combined filters
df = fg.filter((fg.amount > 100) & (fg.status == "active")).read(dataframe_type="polars")
```

## Preview Rows

Quick preview without reading the entire FG:

```python
print(fg.show(n=10))  # show() RETURNS a DataFrame, it does not print; wrap in print() in scripts
```
