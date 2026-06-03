---
name: hops-eda-checklist
description: The dimensions to cover when doing EDA on feature-view training data before training a model in Hopsworks, covering dataset profiling, target analysis, per-feature analysis, and leakage detection. Force into context when planning or reviewing EDA, deciding what to profile, or checking for label leakage and train/test contamination. Reference companion to the hops-eda action skill.
---

# EDA Checklist (pre-training)

## Concept
Before training, profile the feature-view data to catch the failures that silently
wreck a model: label leakage, train/test contamination, target imbalance, and
features that will not be available (or will look different) at serving time. This
is the *what to look for*; [hops-eda](../hops-eda/SKILL.md) is the *how to run it*.
The bundled profiler (`fv-eda.py`) covers Section 1; the rest you reason through or
extend the script.

EDA sits between the feature pipeline and the training pipeline in the FTI
architecture. You profile the reusable, model-independent features as they sit in
the feature store, *before* model-dependent transformations (MDTs: scaling,
one-hot encoding) are applied at training-read time. Findings here drive feature
selection and split policy in the training pipeline.

## Key facts / rules

### 1. Dataset profile (the profiler covers this)
Row/column count, dtypes, missing values, unique counts, duplicate rows, duplicate
entity-time rows, constant + near-constant columns, numeric min/max, timestamp
min/max, categorical cardinality, memory footprint, approximate class balance.
For large data, sample and use approximate statistics.

### 2. Target analysis
- **Classification:** class distribution, minority-class rate, label missingness, imbalance severity, target drift over time.
- **Regression:** distribution, outliers, zero-inflation, unexpected negatives, skewness, temporal drift.
- **Forecasting:** series length, frequency, gaps, seasonality hints, missing intervals, per-entity coverage.

### 3. Per-feature analysis
- **Numerical:** missingness, distribution, outliers, predictive-power signals (target correlation, mutual information, monotonic trend, predictive power score), temporal stability.
- **Categorical:** cardinality, rare categories, unseen-category risk, target rate by category, high-cardinality leakage.
- **Temporal:** event ordering, feature timestamp vs label timestamp, lookahead risk, seasonality, recency.
- **Text:** missingness, length distribution, language/encoding issues, potential PII, embedding/preprocessing need.
- **Identifiers:** whether ID-like columns are accidentally predictive; warn if IDs are used directly as features; suggest grouped splits.

### 4. Leakage detection (the expensive failure)
Leaky features are *infeasible* features: they carry information not available at
prediction time, so a model that relies on them cannot reproduce its training
performance in inference. Check for:
- Features created after the label time, or that directly encode the label.
- Status columns that are consequences of the outcome.
- Aggregates / rolling windows computed with future data (no point-in-time correctness); for time-series splits, a missing gap between `train_end` and `test_start` lets rolling windows straddle the boundary.
- Duplicate entities across the train/test split.
- Target-derived encodings applied before splitting.
- Temporal split violations.
- Columns with suspiciously high target correlation.
- Suspicious feature names: `churned`, `outcome`, `approved`, `declined`, `post_`, `future_`, `after_`, `resolved`, `closed`, `chargeback`, `defaulted`.

## Commands / API (if applicable)
```bash
# Section-1 profile via the bundled profiler (see hops-eda for full usage)
python3 ~/.claude/skills/hops-eda/scripts/fv-eda.py <fv_name> 1 > eda-<ml-system>.md
```

## Docs
- Hopsworks documentation: https://docs.hopsworks.ai
- Point-in-time correct training data and splits: `python/hsfs/feature_view.py` in the hopsworks-api source.

## Related skills
- [hops-eda](../hops-eda/SKILL.md) — runs the profiler and writes the EDA report (the action).
- [hops-fv](../../hops/hops-fv/SKILL.md) — point-in-time correct splits and transformations that address what EDA finds.
- [hops-train](../../hops/hops-train/SKILL.md) — training once the data is understood.
