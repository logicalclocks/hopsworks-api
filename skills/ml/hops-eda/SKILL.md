---
name: hops-eda
description: Exploratory Data Analysis before training an ML model. Auto-invoke when the user wants to profile feature-view training data, check for leakage, or produce an EDA report before building a feature view or training.
---

# Exploratory Data Analysis

Profile a feature view's training data before training: run the bundled profiler,
then deepen with target / leakage / per-feature analysis. The *what to look for*
lives in [hops-eda-checklist](../hops-eda-checklist/SKILL.md); this skill is the
*how to run it*.

## Contract
- **Input:** a feature view, the prediction problem type, and a split strategy.
- **Output:** an EDA profile written as `eda-<ml-system-name>.md`.
- **Pre-condition:** the feature view already exists.

## Smoke-test (cheap pre/post-flight)
Two ready profilers ship with this skill — run one before writing anything. They
produce the Section-1 profile (dtypes, semantic types, null %, unique counts,
numeric stats, datetime ranges, missing-data summary) as text on stdout.
```bash
# Polars (in-memory) — args: <fv_name> [version] [start_time] [end_time]
python3 ~/.claude/skills/hops-eda/scripts/fv-eda.py <fv_name> 1 > eda-<ml-system>.md

# PySpark (large data) — same args, builds a Spark session
python3 ~/.claude/skills/hops-eda/scripts/fv-eda-pyspark.py <fv_name> 1 > eda-<ml-system>.md
```
`start_time`/`end_time` require an `event_time` on the underlying FG.

## Ask the user (only when state is ambiguous)
- Prediction problem type (classification / regression / forecasting).
- Split strategy — infer it (e.g. time-based for time-series), AskUserQuestion only if unclear: `random`, `time-based`, `grouped`, `entity-based`.

Sizing: under ~10 GB use Polars; over ~100 GB use PySpark; in between, judgement call.

## Steps
1. **Run the Section-1 profile** with the bundled script above. Do not re-write what it covers.
2. **Go deeper** on the dimensions the script does not cover — target analysis, per-feature analysis, and leakage detection. The full dimension list is in [hops-eda-checklist](../hops-eda-checklist/SKILL.md). Leakage is the expensive one; do not skip it.
3. **Extend or write** a small Polars program for the deep analysis (PySpark + a Hopsworks job for >100 GB — see **hops-job**).
4. **Save** the result as `eda-<ml-system-name>.md` for the feature-view and training steps.

## Toolset
- **Scripts:** `fv-eda.py` (Polars), `fv-eda-pyspark.py` (Spark) — bundled under this skill's `scripts/`.
- **Checklist:** [hops-eda-checklist](../hops-eda-checklist/SKILL.md) — the dimensions to cover.

## Next Steps
- Select features and build the view: **hops-fv**. Then train: **hops-train**.
- Inspect raw data first: **hops-data-discovery**, **hops-trino-sql**.
