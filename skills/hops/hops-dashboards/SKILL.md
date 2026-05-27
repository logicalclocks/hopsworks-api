---
name: hopsworks-dashboards
description: Use when writing Python code for Hopsworks dashboards, charts, feature
  monitoring, data validation, statistics, or alerting. Auto-invoke when user wants
  to create dashboards, monitor feature drift, set up data quality checks, configure
  alerts, compute statistics, or use Great Expectations with Hopsworks.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Dashboards, Monitoring & Data Quality — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hsfs/`

## Dashboards and Charts

Hopsworks feature store supports custom dashboards composed of HTML-based charts. Charts can be backed by data preparation jobs that refresh them on demand.

### Creating a Chart

A chart is an HTML file registered with the feature store. It can optionally be linked to a Hopsworks job that prepares the data it displays.

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

# Chart with a data preparation job (has a refresh button in the UI)
fs.create_chart(
    title="Monthly Sales",
    description="Bar chart of monthly sales by region",
    url="/Resources/charts/monthly_sales.html",
    job_id=123,  # ID of the job that generates chart data
)

# Static chart (no job — no refresh button)
fs.create_chart(
    title="Architecture Diagram",
    description="System architecture overview",
    url="/Resources/charts/architecture.html",
)
```

#### Chart Properties

| Property | Type | Description |
|---|---|---|
| `id` | `int` | Chart identifier |
| `title` | `str` | Chart title |
| `description` | `str` | Chart description |
| `url` | `str` | Path to the HTML file |
| `job` | `Job` | Data preparation job (optional) |
| `width` | `int` | Width in dashboard grid units |
| `height` | `int` | Height in dashboard grid units |
| `x` | `int` | X position in dashboard grid |
| `y` | `int` | Y position in dashboard grid |

### Creating a Dashboard

A dashboard is a named collection of charts with grid layout positions.

```python
# Get existing charts
chart1 = fs.get_chart(chart_id=1)
chart1.width = 6
chart1.height = 4
chart1.x = 0
chart1.y = 0

chart2 = fs.get_chart(chart_id=2)
chart2.width = 6
chart2.height = 4
chart2.x = 6
chart2.y = 0

# Create dashboard with charts
fs.create_dashboard(
    name="Sales Overview",
    charts=[chart1, chart2],
)
```

### Managing Dashboards and Charts

```python
# List all
dashboards = fs.get_dashboards()
charts = fs.get_charts()

# Get by ID
dashboard = fs.get_dashboard(dashboard_id=1)
chart = fs.get_chart(chart_id=1)

# Update
chart.title = "Updated Title"
chart.update()

dashboard.name = "New Name"
dashboard.update()

# Delete
chart.delete()
dashboard.delete()
```

---

## Statistics

Hopsworks computes descriptive statistics on feature group data during inserts and on training datasets. Statistics power monitoring, transformations, and data quality.

### Statistics Configuration

```python
from hsfs.statistics_config import StatisticsConfig

fg = fs.get_or_create_feature_group(
    name="my_fg",
    version=1,
    statistics_config=StatisticsConfig(
        enabled=True,           # enable statistics (default: True)
        correlations=True,      # compute feature correlations
        histograms=True,        # compute histograms
        exact_uniqueness=True,  # compute exact distinct counts
        columns=["col1", "col2"],  # only these columns (None = all)
    ),
    ...
)

# Disable all statistics (for large data volumes)
fg = fs.get_or_create_feature_group(
    name="large_fg",
    statistics_config=False,
    ...
)
```

### Available Statistics Per Feature

**Basic (always computed when enabled):**

| Statistic | Description |
|---|---|
| `count` | Total values |
| `completeness` | Fraction of non-null values |
| `num_non_null_values` | Count of non-null values |
| `num_null_values` | Count of null values |
| `approx_num_distinct_values` | Approximate distinct count |

**Numerical features:**

| Statistic | Description |
|---|---|
| `min` / `max` | Range |
| `sum` | Total sum |
| `mean` | Average |
| `stddev` | Standard deviation |
| `percentiles` | Distribution percentiles |

**With `exact_uniqueness=True`:**

| Statistic | Description |
|---|---|
| `distinctness` | Fraction of distinct values |
| `entropy` | Information entropy |
| `uniqueness` | Fraction of values occurring exactly once |
| `exact_num_distinct_values` | Precise distinct count |

**With `correlations=True` / `histograms=True`:**

Extended statistics stored in `extended_statistics` dict: `correlations`, `histogram`, `kll`, `unique_values`.

### Retrieving Statistics

```python
# Get latest statistics for a feature group
stats = fg.get_statistics()

# Access per-feature statistics
for feature_stats in stats.feature_descriptive_statistics:
    print(f"{feature_stats.feature_name}: mean={feature_stats.mean}, stddev={feature_stats.stddev}")

# Compute fresh statistics on current data
fg.compute_statistics()
```

---

## Feature Monitoring

Feature monitoring detects data drift by comparing statistics on recent data against a reference baseline. Available on both feature groups and feature views.

### Statistics-Only Monitoring (No Comparison)

Compute statistics on a schedule without comparing to a reference:

```python
fg = fs.get_feature_group(name="transactions", version=1)

config = fg.create_statistics_monitoring(
    name="daily_stats",
    feature_name="amount",         # None = all features
    description="Daily statistics on transaction amounts",
    cron_expression="0 0 12 ? * * *",  # daily at noon UTC
).with_detection_window(
    time_offset="1d",              # look at last day's data
    window_length="1d",
    row_percentage=0.1,            # sample 10% of rows
).save()
```

### Feature Monitoring with Comparison (Drift Detection)

Compare recent data statistics against a reference to detect drift:

```python
config = fg.create_feature_monitoring(
    name="amount_drift",
    feature_name="amount",
    description="Detect drift in transaction amounts",
    cron_expression="0 0 12 ? * * *",
).with_detection_window(
    time_offset="1d",
    window_length="1d",
).with_reference_window(
    time_offset="1w1d",       # same day last week
    window_length="1d",
).compare_on(
    metric="mean",
    threshold=0.5,
    relative=True,            # relative difference
    strict=False,             # >= threshold triggers shift
).save()
```

### Reference Options

Three ways to define the baseline for comparison:

**1. Rolling time window** — compare to historical data:
```python
config.with_reference_window(
    time_offset="1w1d",
    window_length="1d",
    row_percentage=0.2,
)
```

**2. Specific value** — compare against a fixed number:
```python
config.with_reference_value(value=0.0)
```

**3. Training dataset** — compare against training data statistics:
```python
config.with_reference_training_dataset(training_dataset_version=1)
```

### Comparison Metrics

Different metrics available depending on feature type:

**Numerical features:** `mean`, `max`, `min`, `sum`, `std_dev`, `count`, `completeness`, `num_records_non_null`, `num_records_null`, `distinctness`, `entropy`, `uniqueness`, `approximate_num_distinct_values`, `exact_num_distinct_values`

**Categorical features:** `completeness`, `num_records_non_null`, `num_records_null`, `distinctness`, `entropy`, `uniqueness`, `approximate_num_distinct_values`, `exact_num_distinct_values`

### compare_on Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `metric` | `str` | required | Statistic to compare |
| `threshold` | `float` | required | Difference threshold to trigger shift |
| `relative` | `bool` | `False` | Use relative difference instead of absolute |
| `strict` | `bool` | `False` | Use strict comparison (`>` vs `>=`) |

### Managing Monitoring Configs

```python
# Run monitoring job immediately
job = config.run_job()

# Enable / disable scheduling
config.disable()
config.enable()

# Get monitoring history
results = config.get_history(
    start_time="2025-01-01",
    end_time="2025-03-01",
    with_statistics=True,
)

for result in results:
    print(f"Feature: {result.feature_name}")
    print(f"Shift detected: {result.shift_detected}")
    print(f"Difference: {result.difference}")
    if result.detection_statistics:
        print(f"Detection mean: {result.detection_statistics.mean}")
    if result.reference_statistics:
        print(f"Reference mean: {result.reference_statistics.mean}")

# Retrieve configs
configs = fg.get_feature_monitoring_configs()
config = fg.get_feature_monitoring_configs(name="amount_drift")

# Delete
config.delete()
```

### Feature Monitoring on Feature Views

Same API as feature groups:

```python
fv = fs.get_feature_view(name="my_fv", version=1)

config = fv.create_feature_monitoring(
    name="serving_drift",
    feature_name="user_age",
    cron_expression="0 0 */6 ? * * *",  # every 6 hours
).with_detection_window(
    time_offset="6h",
    window_length="6h",
).with_reference_training_dataset(
    training_dataset_version=1,  # compare to training baseline
).compare_on(
    metric="mean",
    threshold=0.3,
    relative=True,
).save()
```

---

## Data Validation (Great Expectations)

Attach expectation suites to feature groups to validate data on ingestion.

### Attach an Expectation Suite

```python
from hsfs.expectation_suite import ExpectationSuite
from hsfs.ge_expectation import GeExpectation

suite = ExpectationSuite(
    expectation_suite_name="transaction_validations",
    expectations=[
        GeExpectation(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "amount"},
        ),
        GeExpectation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "amount", "min_value": 0, "max_value": 100000},
        ),
        GeExpectation(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "currency", "value_set": ["USD", "EUR", "GBP"]},
        ),
    ],
    meta={},
)

fg.save_expectation_suite(
    expectation_suite=suite,
    run_validation=True,             # validate on every insert
    validation_ingestion_policy="always",  # "always" or "strict"
    overwrite=True,                  # replace existing suite
)
```

### Validation Ingestion Policies

| Policy | Behavior |
|---|---|
| `"always"` | Always ingest data, even if validation fails |
| `"strict"` | Only ingest data that passes all expectations |

### Manage Expectations

```python
# Retrieve the suite
suite = fg.get_expectation_suite()

# Add/replace/remove expectations
suite.add_expectation(GeExpectation(
    expectation_type="expect_column_mean_to_be_between",
    kwargs={"column": "amount", "min_value": 10, "max_value": 500},
))

# Get validation reports
reports = fg.get_validation_reports()
for report in reports:
    print(f"Success: {report.success}")
    print(f"Ingestion result: {report.ingestion_result}")
    for result in report.results:
        print(f"  {result.expectation_config}: {result.success}")
```

### Validation on Insert

When `run_validation=True`, validation runs automatically on every `insert()`:

```python
job, validation_report = fg.insert(df)

if validation_report and not validation_report.success:
    print("Validation failed!")
    for result in validation_report.results:
        if not result.success:
            print(f"  Failed: {result.expectation_config}")
```

---

## Alerting

Set up alerts that trigger on validation failures, monitoring drift, or job events. Alerts are delivered via email, Slack, PagerDuty, or webhooks.

### Alert Statuses

**Feature group alerts:**

| Status | Trigger |
|---|---|
| `feature_validation_success` | Expectation suite passes |
| `feature_validation_warning` | Validation has warnings |
| `feature_validation_failure` | Expectation suite fails |
| `feature_monitor_shift_detected` | Monitoring detects drift |
| `feature_monitor_shift_undetected` | Monitoring finds no drift |

**Feature view alerts:**

| Status | Trigger |
|---|---|
| `feature_monitor_shift_detected` | Monitoring detects drift |
| `feature_monitor_shift_undetected` | Monitoring finds no drift |

**Severity levels:** `"info"`, `"warning"`, `"critical"`

### Create Alerts on Feature Groups

```python
# Alert when validation fails
fg.create_alert(
    receiver="my_slack_receiver",
    status="feature_validation_failure",
    severity="critical",
)

# Alert when drift detected
fg.create_alert(
    receiver="my_email_receiver",
    status="feature_monitor_shift_detected",
    severity="warning",
)
```

### Create Alerts on Feature Views

```python
fv.create_alert(
    receiver="my_slack_receiver",
    status="feature_monitor_shift_detected",
    severity="critical",
)
```

### Manage Alerts

```python
# List alerts
alerts = fg.get_alerts()

# Get specific alert
alert = fg.get_alert(alert_id=1)
```

---

## Feature Logging (Inference Monitoring)

Feature views support logging feature vectors during inference for post-hoc monitoring and debugging.

### Enable Feature Logging

```python
# Enable at creation
fv = fs.create_feature_view(
    name="my_fv",
    query=query,
    logging_enabled=True,
    ...
)
```

### Log Feature Vectors

```python
# Get feature vector with logging metadata
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    logging_data=True,
)

# Make prediction
prediction = model.predict(vector)

# Log the feature vector and prediction
fv.log(vector, predictions=prediction)
```

The `log()` method accepts:

| Parameter | Description |
|---|---|
| `untransformed_features` | Raw input features |
| `transformed_features` | Features after transformation |
| `predictions` | Model predictions |
| `inference_helper_columns` | Helper columns |
| `request_parameters` | On-demand feature parameters |
| `event_time` | Inference timestamp |
| `model` / `model_name` / `model_version` | Model metadata |

---

## Complete Example: Monitoring Pipeline

```python
import hopsworks
from hsfs.statistics_config import StatisticsConfig

# 1. Connect
project = hopsworks.login()
fs = project.get_feature_store()

# 2. Get feature group with statistics enabled
fg = fs.get_feature_group(name="transactions", version=1)

# 3. Set up data validation
from hsfs.expectation_suite import ExpectationSuite
from hsfs.ge_expectation import GeExpectation

suite = ExpectationSuite(
    expectation_suite_name="tx_quality",
    expectations=[
        GeExpectation(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "amount"},
        ),
        GeExpectation(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "amount", "min_value": 0, "max_value": 1000000},
        ),
    ],
    meta={},
)
fg.save_expectation_suite(suite, run_validation=True, validation_ingestion_policy="strict")

# 4. Set up feature monitoring for drift detection
config = fg.create_feature_monitoring(
    name="amount_drift_detection",
    feature_name="amount",
    cron_expression="0 0 12 ? * * *",
).with_detection_window(
    time_offset="1d",
    window_length="1d",
).with_reference_window(
    time_offset="1w1d",
    window_length="1d",
).compare_on(
    metric="mean",
    threshold=0.5,
    relative=True,
).save()

# 5. Set up alerts
fg.create_alert(
    receiver="data_team_slack",
    status="feature_validation_failure",
    severity="critical",
)
fg.create_alert(
    receiver="data_team_slack",
    status="feature_monitor_shift_detected",
    severity="warning",
)

# 6. Create a dashboard chart
fs.create_chart(
    title="Transaction Amount Distribution",
    description="Histogram of daily transaction amounts",
    url="/Resources/charts/tx_amounts.html",
    job_id=456,
)

# 7. Check monitoring history
results = config.get_history(
    start_time="2025-01-01",
    end_time="2025-04-01",
    with_statistics=True,
)
for r in results:
    print(f"{r.monitoring_time}: shift={r.shift_detected}, diff={r.difference}")
```

---

## Quick Reference

| Task | Code |
|---|---|
| Create chart | `fs.create_chart(title=..., description=..., url=..., job_id=...)` |
| Create dashboard | `fs.create_dashboard(name=..., charts=[...])` |
| List dashboards | `fs.get_dashboards()` |
| Configure statistics | `StatisticsConfig(enabled=True, correlations=True, ...)` |
| Compute statistics | `fg.compute_statistics()` |
| Get statistics | `fg.get_statistics()` |
| Statistics-only monitoring | `fg.create_statistics_monitoring(name=...).with_detection_window(...).save()` |
| Drift detection | `fg.create_feature_monitoring(name=..., feature_name=...).with_detection_window(...).with_reference_window(...).compare_on(...).save()` |
| Run monitoring now | `config.run_job()` |
| Get monitoring history | `config.get_history(start_time=..., end_time=...)` |
| Attach expectation suite | `fg.save_expectation_suite(suite, run_validation=True)` |
| Get validation reports | `fg.get_validation_reports()` |
| Create alert (FG) | `fg.create_alert(receiver=..., status=..., severity=...)` |
| Create alert (FV) | `fv.create_alert(receiver=..., status=..., severity=...)` |
| Enable feature logging | `fs.create_feature_view(..., logging_enabled=True)` |
| Log predictions | `fv.log(vector, predictions=pred)` |
