# Superset chart param schemas

`create_chart` takes a JSON-string `params` blob whose shape is viz-type specific. Reusable metric/filter building blocks, a copy-paste block per viz type, and a replace-by-name idempotency helper.

## 4. Creating charts — param schemas

`create_chart` takes a JSON-string `params` blob whose shape is **viz-type
specific**. The shared `COUNT_METRIC` below is a valid adhoc SQL metric
reusable across every viz:

```python
import json

COUNT_METRIC = {
    "expressionType": "SQL",
    "sqlExpression": "COUNT(*)",
    "label": "count",
    "optionName": "metric_count",
    "hasCustomLabel": True,
}

# Or an aggregate on a column:
SUM_AMOUNT = {
    "expressionType": "SIMPLE",
    "column": {"column_name": "amount", "type": "DOUBLE"},
    "aggregate": "SUM",
    "label": "total_amount",
    "optionName": "metric_sum_amount",
    "hasCustomLabel": True,
}

# adhoc_filters (reusable building block):
# Simple column filter:
#   {"expressionType": "SIMPLE", "subject": "country", "operator": "==",
#    "comparator": "US", "clause": "WHERE"}
# Raw SQL filter:
#   {"expressionType": "SQL", "sqlExpression": "amount > 100", "clause": "WHERE"}
```

### big_number_total — single KPI

```python
params = {
    "viz_type": "big_number_total",
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "subheader": "Total customers",
}
```

### big_number — KPI with trend line

```python
params = {
    "viz_type": "big_number",
    "metric": COUNT_METRIC,
    "granularity_sqla": "created_at",     # temporal column
    "time_range": "No filter",
    "adhoc_filters": [],
    "compare_lag": "1",
    "compare_suffix": "vs prev",
    "y_axis_format": "SMART_NUMBER",
}
```

### pie

```python
params = {
    "viz_type": "pie",
    "groupby": ["category_col"],
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 100,
    "sort_by_metric": True,
    "show_legend": True,
    "label_type": "key_percent",          # or "key", "value", "percent"
    "donut": False,
    "innerRadius": 30,
    "outerRadius": 70,
}
```

### echarts_timeseries_bar — bar chart (use for categorical bars too)

This replaces the legacy `dist_bar`. For a **categorical** (non-temporal)
x-axis, set `x_axis_force_categorical: true`.

```python
params = {
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "some_category_col",        # this is the grouping column, NOT `groupby`
    "x_axis_force_categorical": True,     # REQUIRED for string x_axis
    "metrics": [COUNT_METRIC],
    "groupby": [],                        # only for series breakdown (stacked)
    "adhoc_filters": [],
    "row_limit": 100,
    "orientation": "vertical",            # or "horizontal"
    "order_desc": True,
    # Sort bars by metric:
    "timeseries_limit_metric": COUNT_METRIC,
    "x_axis_sort": "count",               # must match the metric's `label`
    "x_axis_sort_asc": False,
    "show_legend": False,
    "y_axis_format": "SMART_NUMBER",
}
```

For a **time-series** bar chart, use a datetime column as `x_axis` and drop
`x_axis_force_categorical`. Add `time_grain_sqla` (e.g. `"P1D"`, `"P1M"`).

### echarts_timeseries_line

```python
params = {
    "viz_type": "echarts_timeseries_line",
    "x_axis": "event_ts",
    "time_grain_sqla": "P1D",             # P1D=day, PT1H=hour, P1M=month, P1Y=year
    "metrics": [COUNT_METRIC],
    "groupby": [],                        # series breakdown columns
    "adhoc_filters": [],
    "row_limit": 10000,
    "show_legend": True,
    "markerEnabled": False,
    "y_axis_format": "SMART_NUMBER",
}
```

### histogram_v2

```python
params = {
    "viz_type": "histogram_v2",
    "column": "numeric_col",              # SINGLE string (not `all_columns_x` array)
    "groupby": [],                        # optional series breakdown
    "adhoc_filters": [],
    "row_limit": 50000,
    "bins": 20,
    "normalize": False,
    "cumulative": False,
    "x_axis_title": "Value",              # NOT `x_axis_label`
    "y_axis_title": "Count",              # NOT `y_axis_label`
    "x_axis_format": "SMART_NUMBER",
    "y_axis_format": "SMART_NUMBER",
}
```

### table — tabular view

```python
params = {
    "viz_type": "table",
    "query_mode": "aggregate",            # or "raw" for row-level
    "groupby": ["col1", "col2"],
    "metrics": [COUNT_METRIC],
    "adhoc_filters": [],
    "row_limit": 100,
    "order_by_cols": [json.dumps(["count", False])],  # list of JSON-encoded [col, asc]
    "show_cell_bars": True,
    "table_timestamp_format": "smart_date",
}
```

### pivot_table_v2

```python
params = {
    "viz_type": "pivot_table_v2",
    "groupbyRows": ["row_col"],
    "groupbyColumns": ["pivot_col"],
    "metrics": [COUNT_METRIC],
    "adhoc_filters": [],
    "row_limit": 10000,
    "aggregateFunction": "Sum",
    "rowTotals": True,
    "colTotals": True,
}
```

### heatmap_v2

```python
params = {
    "viz_type": "heatmap_v2",
    "x_axis": "x_col",
    "groupby": "y_col",
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 10000,
    "normalize_across": "heatmap",        # or "x", "y"
    "linear_color_scheme": "schemeBlues",
}
```

### bubble_v2 (scatter with size)

```python
params = {
    "viz_type": "bubble_v2",
    "x_axis": {"expressionType": "SIMPLE",
               "column": {"column_name": "feature_x", "type": "DOUBLE"},
               "aggregate": "AVG", "label": "x"},
    "y_axis": {"expressionType": "SIMPLE",
               "column": {"column_name": "feature_y", "type": "DOUBLE"},
               "aggregate": "AVG", "label": "y"},
    "size": COUNT_METRIC,
    "entity": "group_col",
    "adhoc_filters": [],
    "row_limit": 1000,
}
```

### box_plot

```python
params = {
    "viz_type": "box_plot",
    "x_axis": "category_col",
    "x_axis_force_categorical": True,
    "metrics": [SUM_AMOUNT],              # numeric metric(s) to summarize
    "groupby": [],
    "adhoc_filters": [],
    "row_limit": 10000,
    "whiskerOptions": "Tukey",            # or "Min/max", "2/98 percentiles", etc.
}
```

### funnel

```python
params = {
    "viz_type": "funnel",
    "groupby": ["stage_col"],
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 10,
    "sort_by_metric": True,
}
```

### Replace-by-name helper for idempotent chart scripts

```python
def replace_chart(api, slice_name, **kwargs):
    for c in list_all(api, "chart"):
        if c.get("slice_name") == slice_name:
            api.delete_chart(c["id"])
    return api.create_chart(slice_name=slice_name, **kwargs)["id"]
```

