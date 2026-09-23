---
name: hops-eda
description: Exploratory Data Analysis before training an ML model. Auto-invoke when the user wants to profile feature-view training data, check for leakage or train/test contamination, decide what to profile, or produce an EDA report before building a feature view or training.
---

# Exploratory Data Analysis

Profile a feature view's training data before training: run the bundled profiler,
then deepen with target, per-feature and leakage analysis. The full list of
dimensions is [references/checklist.md](references/checklist.md); this file is
how to run it and what must never be skipped.

## Contract
- **Input:** a feature view, the prediction problem type, and a split strategy.
- **Output:** an EDA report. Inside a `/hops-ml` system it is `<slug>/eda.md`
  and its leakage findings go into `training.leakage` in `system.yaml`;
  otherwise `eda-<ml-system-name>.md`.
- **Pre-condition:** the feature view already exists.

## Must check, in this order
1. **Leakage first.** Features created after the label time or that encode the
   label, status columns that are consequences of the outcome, aggregates
   without point-in-time correctness, target-derived encodings applied before
   the split, suspicious names (`churned`, `outcome`, `resolved`, `post_`,
   `future_`, ...), and columns with implausibly high target correlation.
2. **Split contamination.** No entity in two parts for a grouped split; no
   window straddling the train/test boundary for a time split.
3. **Target.** Class balance or distribution, label missingness, drift over time.
4. **Serving availability.** Every candidate feature must exist at prediction time.

## Smoke-test (cheap pre/post-flight)
Two ready profilers ship with this skill; run one before writing anything. They
produce the Section-1 profile (dtypes, semantic types, null %, unique counts,
numeric stats, datetime ranges, missing-data summary) as text on stdout.
```bash
# Polars (in-memory): args <fv_name> [version] [start_time] [end_time]
python3 ~/.claude/skills/hops-eda/scripts/fv-eda.py <fv_name> 1 > eda.md

# PySpark (large data): same args, builds a Spark session
python3 ~/.claude/skills/hops-eda/scripts/fv-eda-pyspark.py <fv_name> 1 > eda.md
```
`start_time`/`end_time` require an `event_time` on the underlying FG.

## Ask the user (only when state is ambiguous)
- Prediction problem type (classification / regression / forecasting).
- Split strategy: infer it (time-ordered for data that generalises to new
  periods, grouped by entity for new entities); AskUserQuestion only if unclear.

Sizing: under ~10 GB use Polars; over ~100 GB use PySpark; in between, judgement call.

## Steps
1. **Run the Section-1 profile** with the bundled script above. Do not re-write what it covers.
2. **Go deeper** on target analysis, per-feature analysis and leakage, with the
   dimensions in [references/checklist.md](references/checklist.md).
3. **Extend or write** a small Polars program for the deep analysis (PySpark + a Hopsworks job for >100 GB, see **hops-job**).
4. **Save** the report and, inside a system, the leaky column names under `training.leakage`.

## Short checks on a new dataset
A new training round after a new feature, and every scheduled retrain, run the
short checks instead of the full profile: schema matches the feature view,
freshness (the newest event time), label maturity (no row younger than
`label_maturity` in any part), and the leakage list `eda.md` recorded. The
template's `training_pipeline.short_checks` implements them.

## Toolset
- **Scripts:** `fv-eda.py` (Polars), `fv-eda-pyspark.py` (Spark), under this skill's `scripts/`.
- **Checklist:** [references/checklist.md](references/checklist.md).

## Next Steps
- Select features and build the view: **hops-fv**. Then train: **hops-train**.
- Inspect raw data first: **hops-data-discovery**, **hops-trino-sql**.
