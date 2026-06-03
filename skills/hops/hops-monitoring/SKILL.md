---
name: hops-monitoring
description: Use when writing Python code to monitor a Hopsworks feature store with descriptive statistics, feature/drift monitoring, Great Expectations data validation, alerts, and inference feature logging. Auto-invoke when the user wants to monitor feature drift, set up data quality checks, validate data on ingestion, configure alerts, compute statistics, or use Great Expectations with Hopsworks.
---

# Hopsworks Monitoring & Data Quality

The operate side of an FTI system: watch feature data for drift, validate it on
ingestion, and alert when it breaks. Works on both feature groups (ingestion-time)
and feature views (serving-time). Monitoring is built on logs, so the inference
pipeline must log its inputs and outputs first (see Inference feature logging).

**Drift vocabulary** — the kind of drift sets the reference/detection windows:
- *Data ingestion drift*: a new batch differs from existing feature-group data. Detect on the **feature group**.
- *Feature drift*: recent inference features differ from the training dataset. Detect on the **feature view** vs its training-dataset baseline.
- *Concept drift*: the feature→label relationship changed. Not a distribution compare; compare predictions to outcomes (ROC AUC / MSE). Needs outcomes, not this skill's statistics.

## Contract
- **Input:** an existing feature group or feature view.
- **Output:** a `StatisticsConfig`, a feature-monitoring config, an attached
  `ExpectationSuite`, or an alert — bound to that FG/FV.
- **Pre-condition:** the FG/FV exists and (for monitoring) has statistics enabled.

## Smoke-test (cheap pre/post-flight)
```bash
hops fg list                       # confirm the FG exists
hops fg stats <name> --version 1   # latest descriptive statistics (no Python)
hops fg stats <name> --compute     # recompute on current data
```

## Cluster & client requirements (check these first — both bite)
- **Feature monitoring** (`create_feature_monitoring` / `create_statistics_monitoring`)
  needs the **feature-monitoring service enabled cluster-wide**. If it is off, every
  call fails with `errorCode 270234 "Feature monitoring is not enabled."`. Statistics,
  validation, and alerts do **not** need it. Verify before writing a monitoring pipeline.
- **Great Expectations validation on insert** runs **client-side** and needs
  `great_expectations` installed in the *client* env. The suite attaches without it,
  but `insert()` then raises `ModuleNotFoundError`. Either `pip install great_expectations`
  or insert with `validation_options={"run_validation": False}`.
- **Alerts** need a **receiver** (Slack/email/PagerDuty/webhook) already configured in
  the project's alert settings. `create_alert` references it by name.

## Ask the user (only when state is ambiguous)
- Monitor at **ingestion** (feature group) or **serving** (feature view)?
- Statistics-only, or **drift detection** against a reference (rolling window / fixed
  value / training dataset)?
- Hard gate on bad data (`validation_ingestion_policy="strict"`) or log-and-pass (`"always"`)?

## Steps (generic, non-binding)
1. Confirm what the cluster allows: statistics, validation, and alerts always work; feature monitoring needs the cluster service (see above).
2. Enable statistics on the FG/FV if not already (`StatisticsConfig`).
3. Attach the control you need: an expectation suite (validation), a monitoring config (drift), or an alert.
4. Verify with the smoke-test or a `get_*` readback; wire an alert receiver if you want notifications.

## Statistics
Descriptive statistics are computed on insert; they power monitoring and transformations.
```python
from hsfs.statistics_config import StatisticsConfig

fg = fs.get_or_create_feature_group(
    name="my_fg", version=1,
    statistics_config=StatisticsConfig(
        enabled=True,           # default True
        correlations=True,      # feature correlations
        histograms=True,        # histograms
        exact_uniqueness=True,  # exact distinct / entropy / uniqueness
        columns=["amount"],     # subset; None = all
    ),
    # ...
)
# statistics_config=False disables all of it (use for very large volumes).

stats = fg.get_statistics()                       # latest
for fds in stats.feature_descriptive_statistics:  # per feature
    print(fds.feature_name, fds.mean, fds.stddev)
fg.compute_statistics()                           # recompute now
```

## Feature monitoring (drift)
Compares statistics on a detection window against a reference baseline on a schedule.
**Requires the cluster service (see above).** Builder chain returns a config; `.save()`
persists it.
```python
config = fg.create_feature_monitoring(
    name="amount_drift",
    feature_name="amount",              # None = all features
    cron_expression="0 0 12 ? * * *",   # daily noon UTC
).with_detection_window(
    time_offset="1d", window_length="1d", row_percentage=0.1,
).with_reference_window(                # one of: window / value / training dataset
    time_offset="1w1d", window_length="1d",
).compare_on(
    metric="mean", threshold=0.5, relative=True, strict=False,
).save()

# Reference alternatives:
#   .with_reference_value(value=0.0)
#   .with_reference_training_dataset(training_dataset_version=1)

# Statistics-only (no comparison): fg.create_statistics_monitoring(...).with_detection_window(...).save()

config.run_job()                        # run immediately
config.disable(); config.enable()       # toggle schedule
config.get_history(start_time="2026-01-01", end_time="2026-06-01", with_statistics=True)
fg.get_feature_monitoring_configs(name="amount_drift")
config.delete()
```
Feature views use the **same API** (`fv.create_feature_monitoring(...)`), and commonly
compare serving data to a training-dataset baseline with `.with_reference_training_dataset(...)`.

**Comparison metrics** — numerical: `mean`, `min`, `max`, `sum`, `std_dev`, `count`,
`completeness`, `distinctness`, `entropy`, `uniqueness`, `approximate_num_distinct_values`,
`exact_num_distinct_values`. Categorical: the same minus the numeric aggregates.

## Data validation (Great Expectations)
Attach an expectation suite; it validates on every `insert()`. **`GeExpectation` requires
`meta`** (third arg); omitting it raises `TypeError`.
```python
from hsfs.expectation_suite import ExpectationSuite
from hsfs.ge_expectation import GeExpectation

suite = ExpectationSuite(
    expectation_suite_name="tx_quality",
    expectations=[
        GeExpectation(expectation_type="expect_column_values_to_not_be_null",
                      kwargs={"column": "amount"}, meta={}),
        GeExpectation(expectation_type="expect_column_values_to_be_between",
                      kwargs={"column": "amount", "min_value": 0, "max_value": 1000}, meta={}),
    ],
    meta={},
)
fg.save_expectation_suite(
    suite,
    run_validation=True,
    validation_ingestion_policy="always",  # "always" = ingest anyway; "strict" = reject failing data
    overwrite=True,
)

suite = fg.get_expectation_suite()
report = fg.validate(df)                                  # validate without inserting
hist = fg.get_validation_history(expectation_id=1)        # NOT get_validation_reports

# insert() returns (job, validation_report); report.success is the gate
job, report = fg.insert(df)
if report and not report.success:
    for r in report.results:
        if not r.success:
            print("failed:", r.expectation_config)
```

## Alerting
Triggers on validation or drift events; delivered through a pre-configured receiver.
```python
fg.create_alert(receiver="data_team_slack",
                status="feature_validation_failure", severity="critical")
fg.create_alert(receiver="data_team_email",
                status="feature_monitor_shift_detected", severity="warning")
fg.get_alerts()
```
FG statuses: `feature_validation_success` / `_warning` / `_failure`,
`feature_monitor_shift_detected` / `_undetected`. FV statuses: the two `feature_monitor_*`.
Severities: `info`, `warning`, `critical`.

## Inference feature logging (serving-time monitoring)
Enable on the feature view, then log served vectors and predictions for post-hoc drift.
Log **both untransformed and transformed features**: untransformed feature data drives
feature drift and debugging; transformed feature data drives model monitoring and SHAP.
Predictions drive concept/prediction drift. Unify a request's inputs and outputs in one
logging feature group so debugging and monitoring read from a single table.
```python
fv = fs.create_feature_view(name="my_fv", query=query, logging_enabled=True)
vector = fv.get_feature_vector(entry={"customer_id": 1}, logging_data=True)
fv.log(logging_data=vector, predictions=pred)   # also: untransformed_features, transformed_features, model
```

## Toolset
- **CLI:** `hops fg stats <name> [--compute]`, `hops fg info <name>` (statistics only; monitoring/validation/alerts are SDK).
- **SDK:** `StatisticsConfig`, `fg/fv.create_feature_monitoring()`, `fg.save_expectation_suite()`, `fg.get_validation_history()`, `fg/fv.create_alert()`, `fv.log()`.
- **Source:** `python/hsfs/feature_group.py`, `feature_view.py`, `core/feature_monitoring_config.py`, `expectation_suite.py`, `ge_expectation.py`.

## Next steps
- [hops-fg](../hops-fg/SKILL.md) — set `statistics_config` / attach a suite at creation.
- [hops-fv](../hops-fv/SKILL.md) — enable `logging_enabled`; serving-time monitoring.
- [hops-online-inference](../hops-online-inference/SKILL.md) — log feature vectors from a deployment.
- [hops-superset](../hops-superset/SKILL.md) — chart the statistics / monitoring history.
