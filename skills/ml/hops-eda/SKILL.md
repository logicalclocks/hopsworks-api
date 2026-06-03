---
name: hops-eda
description: Exploratory Data Analysis before training an ML model
---

## Usage
This skill helps write a program to do EDA on training data for a model. The input will be a feature view and the output will be EDA over a subset or all of the data that will be used to train the model.


## Expected Inputs

- Feature View with set of features
- Prediction problem type
- Split strategy. Infer the split strategy (e.g., time-based for time-series data) but if it is unclear, then AskUserQuestion about the desired split strategy.
  - random
  - time-based
  - grouped
  - entity-based

If the data is under say 10Bs, use Polars. If larger than 100GBs, prefer to use PySpark. If it between 10-100 GBs, make a judgement call on Polars vs PySpark.

## Quick start: run the bundled profiler

This skill ships two ready profilers — start with them before writing anything, they cover the Section 1 profile (dtypes, semantic types, null %, unique counts, numeric stats, datetime ranges, missing-data summary):

```bash
# Polars (in-memory) — args: <fv_name> [version] [start_time] [end_time]
python3 ~/.claude/skills/hops-eda/scripts/fv-eda.py <fv_name> 1 > eda-<ml-system>.md

# PySpark (large data) — same args, builds a Spark session
python3 ~/.claude/skills/hops-eda/scripts/fv-eda-pyspark.py <fv_name> 1 > eda-<ml-system>.md
```

The scripts print a text profile to stdout (no plots, no file unless you redirect). `start_time`/`end_time` require an `event_time` on the underlying FG. Then go deeper with the checklist below (target analysis, leakage, per-feature).

### 1. Run basic dataset profiling
- Row count
- Column count
- Data types
- Missing values
- Unique counts
- Duplicate rows
- Duplicate entity-time rows
- Constant columns
- Near-constant columns
- Min/max for numeric columns
- Min/max timestamps
- Cardinality for categorical columns
- Memory footprint
- Approximate class balance if classification

For large datasets, use sampled profiling plus approximate statistics where appropriate.

### 2. Analyze the target

For classification:
- Class distribution
- Minority class rate
- Label missingness
- Label imbalance severity
- Target drift over time, if timestamp exists

For regression:
- Distribution
- Outliers
- Zero inflation
- Negative values, if unexpected
- Skewness
- Temporal drift

For forecasting:
- Series length
- Frequency
- Gaps
- Seasonality hints
- Missing intervals
- Entity-level coverage

### 3. Analyze features

For each feature group:

Numerical:
- Missingness
- Distribution
- Outliers
- Correlation with target
- Monotonic signals
- Temporal stability

Categorical:
- Cardinality
- Rare categories
- Unseen-category risk
- Target rate by category
- High-cardinality leakage risk

Temporal:
- Event ordering
- Feature timestamp vs label timestamp
- Lookahead risk
- Seasonality
- Recency effects

Text:
- Missingness
- Length distribution
- Language or encoding issues
- Potential PII
- Need for embeddings or text preprocessing

Identifiers:
- Check whether ID-like columns are accidentally predictive
- Warn if IDs are used directly as features
- Suggest grouped splits where appropriate

### 4. Detect leakage risks

Check for:

- Features created after the label time
- Columns that directly encode the label
- Status columns that are consequences of the outcome
- Aggregates computed using future data
- Rolling/window features without point-in-time correctness
- Duplicate entities across train/test split
- Target-derived encodings before splitting
- Temporal split violations
- Columns with suspiciously high target correlation
- Features with names like:
  - `churned`
  - `outcome`
  - `approved`
  - `declined`
  - `post_`
  - `future_`
  - `after_`
  - `resolved`
  - `closed`
  - `chargeback`
  - `defaulted`


### 5. Write the program to perform EDA

For the basic profile, prefer the bundled scripts above (`fv-eda.py` / `fv-eda-pyspark.py`) rather than re-writing them. For the deeper analysis (target, leakage, per-feature) that the scripts do not cover, extend them or write a small Polars program; for >100GB, write a PySpark program and run it as a Hopsworks job (use **hops-job** skill).
Save the results as a `eda-<ml-system-name>.md` file (redirect the script's stdout, or write it) for use when building the feature view and training pipeline.

## Next Steps

- Select features and build the view: **hops-fv**. Then train: **hops-train**.
- Inspect raw data first: **hops-data-discovery**, **hops-trino-sql**.
