---
name: hops-eda
description: Exploratory Data Analysis before training an ML model
---

## Usage

Polars (default — fits-in-memory):

```bash
python3 /hopsfs/Users/${HOPS_USER}/.claude/skills/ml/hops-eda/scripts/fv-eda.py <feature_view_name> [feature_view_version] [start_time] [end_time]
```

PySpark (use when the feature view is too large for Polars):

```bash
python3 /hopsfs/Users/${HOPS_USER}/.claude/skills/ml/hops-eda/scripts/fv-eda-pyspark.py <feature_view_name> [feature_view_version] [start_time] [end_time]
```

`start_time` / `end_time` are pushed into `fv.query.read(start_time=..., end_time=...)`, so the underlying feature group must have an `event_time` column.

This analyzes available data, looking for:
1. Data type (numerical, categorical (ordinal or, lists, embeddings, etc)
2. Missing data
