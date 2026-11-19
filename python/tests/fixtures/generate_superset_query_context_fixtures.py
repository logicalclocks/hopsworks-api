r"""Generate superset_query_context_fixtures.json from the Superset frontend code.

Each case's query_context is what the Superset frontend builds for its params on
save: superset_query_context_golden/ bundles the 12 plugins' buildQuery.ts,
superset-ui-core/src/query and the chart-controls operators with esbuild and runs
them in node. Needs git, node and npm. Against a Superset tag, from python/:

    git clone --depth 1 --branch 6.0.0 --filter=blob:none --sparse \
        https://github.com/apache/superset /tmp/superset
    git -C /tmp/superset sparse-checkout set \
        superset-frontend/packages superset-frontend/plugins
    python tests/fixtures/generate_superset_query_context_fixtures.py /tmp/superset

Then run tests/core/test_superset_query_context.py: a failing case is a change in
the frontend that superset_query_context.py has to port.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


COUNT = {
    "expressionType": "SQL",
    "sqlExpression": "COUNT(*)",
    "label": "count",
    "optionName": "metric_count",
    "hasCustomLabel": True,
}
SUM_AMOUNT = {
    "expressionType": "SIMPLE",
    "column": {"column_name": "amount", "type": "DOUBLE"},
    "aggregate": "SUM",
    "label": "total_amount",
    "optionName": "metric_sum_amount",
    "hasCustomLabel": True,
}
AVG_X = {
    "expressionType": "SIMPLE",
    "column": {"column_name": "feature_x", "type": "DOUBLE"},
    "aggregate": "AVG",
}
F_US = {
    "expressionType": "SIMPLE",
    "subject": "country",
    "operator": "==",
    "comparator": "US",
    "clause": "WHERE",
}
F_SQL = {
    "expressionType": "SQL",
    "sqlExpression": "amount > 100 -- big",
    "clause": "WHERE",
}
F_HAVING = {
    "expressionType": "SQL",
    "sqlExpression": "COUNT(*) > 1",
    "clause": "HAVING",
}
F_NULL = {
    "expressionType": "SIMPLE",
    "subject": "region",
    "operator": "IS NOT NULL",
    "clause": "WHERE",
}
F_IN = {
    "expressionType": "SIMPLE",
    "subject": "country",
    "operator": "IN",
    "comparator": ["US", "FR"],
    "clause": "WHERE",
}

CASES = {
    "big_number_total": {
        "viz_type": "big_number_total",
        "metric": COUNT,
        "adhoc_filters": [],
        "y_axis_format": "SMART_NUMBER",
        "subheader": "Total customers",
    },
    "big_number": {
        "viz_type": "big_number",
        "metric": COUNT,
        "x_axis": "created_at",
        "time_grain_sqla": "P1D",
        "time_range": "No filter",
        "adhoc_filters": [],
        "compare_lag": "1",
        "compare_suffix": "vs prev",
        "y_axis_format": "SMART_NUMBER",
    },
    "big_number__granularity_rolling": {
        "viz_type": "big_number",
        "metric": SUM_AMOUNT,
        "granularity_sqla": "created_at",
        "rolling_type": "cumsum",
        "resample_method": "zerofill",
        "resample_rule": "1D",
        "adhoc_filters": [F_US],
    },
    "big_number__raw_rolling": {
        "viz_type": "big_number",
        "metric": COUNT,
        "x_axis": "created_at",
        "aggregation": "raw",
        "rolling_type": "mean",
        "rolling_periods": "7",
        "min_periods": 2,
        "adhoc_filters": [],
    },
    "pie": {
        "viz_type": "pie",
        "groupby": ["category_col"],
        "metric": COUNT,
        "adhoc_filters": [],
        "row_limit": 100,
        "sort_by_metric": True,
        "show_legend": True,
        "label_type": "key_percent",
        "donut": False,
        "innerRadius": 30,
        "outerRadius": 70,
    },
    "echarts_timeseries_bar": {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "some_category_col",
        "x_axis_force_categorical": True,
        "metrics": [COUNT],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 100,
        "orientation": "vertical",
        "order_desc": True,
        "timeseries_limit_metric": COUNT,
        "x_axis_sort": "count",
        "x_axis_sort_asc": False,
        "show_legend": False,
        "y_axis_format": "SMART_NUMBER",
    },
    "echarts_timeseries_bar__extra_sort_metric": {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "some_category_col",
        "metrics": [COUNT],
        "groupby": [],
        "timeseries_limit_metric": SUM_AMOUNT,
        "x_axis_sort": "total_amount",
        "x_axis_sort_asc": True,
        "row_limit": 10,
    },
    "echarts_timeseries_bar__sort_by_x_axis": {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "some_category_col",
        "x_axis_force_categorical": True,
        "metrics": [COUNT],
        "groupby": [],
        "x_axis_sort": "some_category_col",
        "x_axis_sort_asc": True,
    },
    "echarts_timeseries_line": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_ts",
        "time_grain_sqla": "P1D",
        "metrics": [COUNT],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 10000,
        "show_legend": True,
        "markerEnabled": False,
        "y_axis_format": "SMART_NUMBER",
    },
    "echarts_timeseries_line__compare_difference": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_ts",
        "time_grain_sqla": "P1W",
        "metrics": [COUNT, SUM_AMOUNT],
        "groupby": ["country"],
        "time_compare": ["1 week ago"],
        "comparison_type": "difference",
        "rolling_type": "mean",
        "rolling_periods": 7,
        "resample_method": "zerofill",
        "resample_rule": "1D",
        "contributionMode": "row",
        "truncate_metric": True,
        "limit": 5,
        "timeseries_limit_metric": COUNT,
        "order_desc": False,
        "adhoc_filters": [F_US, F_SQL, F_HAVING, F_NULL, F_IN],
        "time_range": "Last year",
    },
    "echarts_timeseries_line__compare_values_adhoc_axis": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": {
            "label": "day",
            "sqlExpression": "DATE(event_ts)",
            "expressionType": "SQL",
        },
        "time_grain_sqla": "P1D",
        "metrics": [COUNT],
        "groupby": ["country"],
        "time_compare": ["1 year ago", "2 years ago"],
        "comparison_type": "values",
        "truncate_metric": True,
        "show_empty_columns": True,
        "forecastEnabled": True,
        "forecastPeriods": "10",
        "forecastInterval": "0.8",
        "extra_form_data": {
            "filters": [{"col": "region", "op": "IN", "val": ["EU"]}],
            "time_range": "Last month",
            "time_grain_sqla": "P1M",
        },
        "extra_filters": [
            {"col": "__time_range", "val": "Last week"},
            {"col": "city", "op": "==", "val": "Paris"},
        ],
        "where": "1 = 1",
    },
    "echarts_timeseries_line__truncate_one_metric": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_ts",
        "metrics": [COUNT],
        "groupby": ["country"],
        "truncate_metric": True,
        "x_axis_sort": "event_ts",
        "x_axis_sort_asc": True,
    },
    "echarts_timeseries_line__truncate_two_metrics": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_ts",
        "metrics": [COUNT, SUM_AMOUNT],
        "groupby": ["country"],
        "truncate_metric": True,
        "rolling_type": "sum",
        "rolling_periods": "3",
    },
    "echarts_timeseries_line__explicit_orderby": {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_ts",
        "metrics": [COUNT],
        "groupby": ["country"],
        "orderby": [[SUM_AMOUNT, False]],
        "order_desc": False,
        "timeseries_limit_metric": COUNT,
        "limit": 5,
    },
    "histogram_v2": {
        "viz_type": "histogram_v2",
        "column": "numeric_col",
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 50000,
        "bins": 20,
        "normalize": False,
        "cumulative": False,
        "x_axis_title": "Value",
        "y_axis_title": "Count",
        "x_axis_format": "SMART_NUMBER",
        "y_axis_format": "SMART_NUMBER",
    },
    "histogram_v2__groupby_default_bins": {
        "viz_type": "histogram_v2",
        "column": "numeric_col",
        "groupby": ["country"],
        "row_limit": "500",
    },
    "table": {
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": ["col1", "col2"],
        "metrics": [COUNT],
        "adhoc_filters": [],
        "row_limit": 100,
        "order_by_cols": [json.dumps(["count", False])],
        "show_cell_bars": True,
        "table_timestamp_format": "smart_date",
    },
    "table__raw": {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["col1", "col2"],
        "groupby": ["ignored"],
        "metrics": [COUNT],
        "order_by_cols": [json.dumps(["col1", True])],
        "row_limit": 50,
        "adhoc_filters": [F_US],
    },
    "table__percent_totals_temporal": {
        "viz_type": "table",
        "groupby": ["created_at", "country"],
        "metrics": [COUNT],
        "percent_metrics": [SUM_AMOUNT],
        "percent_metric_calculation": "all_records",
        "show_totals": True,
        "time_grain_sqla": "P1M",
        "temporal_columns_lookup": {"created_at": True},
        "timeseries_limit_metric": SUM_AMOUNT,
        "order_desc": True,
        "row_limit": 1000,
    },
    "table__server_pagination_compare": {
        "viz_type": "table",
        "groupby": ["country"],
        "metrics": [COUNT],
        "percent_metrics": [COUNT],
        "server_pagination": True,
        "server_page_length": 10,
        "row_limit": 100,
        "time_compare": ["1 year ago", "custom"],
        "start_date_offset": "2024-01-01",
        "comparison_type": "values",
        "extra_form_data": {"interactive_groupby": ["region"]},
    },
    "pivot_table_v2": {
        "viz_type": "pivot_table_v2",
        "groupbyRows": ["row_col"],
        "groupbyColumns": ["pivot_col"],
        "metrics": [COUNT],
        "adhoc_filters": [],
        "row_limit": 10000,
        "aggregateFunction": "Sum",
        "rowTotals": True,
        "colTotals": True,
    },
    "pivot_table_v2__temporal_limit_metric": {
        "viz_type": "pivot_table_v2",
        "groupbyRows": ["created_at", "row_col"],
        "groupbyColumns": ["row_col"],
        "metrics": [COUNT, SUM_AMOUNT],
        "series_limit_metric": SUM_AMOUNT,
        "order_desc": False,
        "time_grain_sqla": "P1M",
        "granularity_sqla": "created_at",
        "series_limit": 10,
    },
    "heatmap_v2": {
        "viz_type": "heatmap_v2",
        "x_axis": "x_col",
        "groupby": "y_col",
        "metric": COUNT,
        "adhoc_filters": [],
        "row_limit": 10000,
        "normalize_across": "heatmap",
        "linear_color_scheme": "schemeBlues",
    },
    "heatmap_v2__sorted_normalized": {
        "viz_type": "heatmap_v2",
        "x_axis": "x_col",
        "groupby": "y_col",
        "metric": SUM_AMOUNT,
        "sort_x_axis": "value_asc",
        "sort_y_axis": "alpha_desc",
        "normalize_across": "y",
        "time_grain_sqla": "P1D",
    },
    "heatmap_v2__normalize_x": {
        "viz_type": "heatmap_v2",
        "x_axis": "x_col",
        "groupby": "y_col",
        "metric": COUNT,
        "normalize_across": "x",
        "sort_x_axis": "alpha_asc",
    },
    "bubble_v2": {
        "viz_type": "bubble_v2",
        "x": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "feature_x", "type": "DOUBLE"},
            "aggregate": "AVG",
            "label": "x",
        },
        "y": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "feature_y", "type": "DOUBLE"},
            "aggregate": "AVG",
            "label": "y",
        },
        "size": COUNT,
        "entity": "group_col",
        "adhoc_filters": [],
        "row_limit": 1000,
    },
    "bubble_v2__series_orderby": {
        "viz_type": "bubble_v2",
        "x": AVG_X,
        "y": SUM_AMOUNT,
        "size": COUNT,
        "entity": "group_col",
        "series": "country",
        "orderby": COUNT,
        "order_desc": False,
        "row_limit": 100,
    },
    "box_plot": {
        "viz_type": "box_plot",
        "columns": ["category_col"],
        "metrics": [SUM_AMOUNT],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 10000,
        "whiskerOptions": "Tukey",
    },
    "box_plot__percentile_temporal": {
        "viz_type": "box_plot",
        "columns": ["created_at"],
        "groupby": ["country"],
        "metrics": [SUM_AMOUNT, COUNT],
        "time_grain_sqla": "P1W",
        "temporal_columns_lookup": {"created_at": True},
        "whiskerOptions": "5/95 percentiles",
    },
    "box_plot__granularity_minmax": {
        "viz_type": "box_plot",
        "granularity_sqla": "created_at",
        "groupby": ["country"],
        "metrics": [COUNT],
        "whiskerOptions": "Min/max (no outliers)",
    },
    "funnel": {
        "viz_type": "funnel",
        "groupby": ["stage_col"],
        "metric": COUNT,
        "adhoc_filters": [],
        "row_limit": 10,
        "sort_by_metric": True,
    },
    "unknown__echarts_area": {
        "viz_type": "echarts_area",
        "x_axis": "event_ts",
        "metrics": [COUNT],
        "groupby": ["country"],
    },
}


def main(superset: Path) -> None:
    here = Path(__file__).parent
    with tempfile.TemporaryDirectory() as tmp:
        harness = Path(tmp) / "harness"
        shutil.copytree(here / "superset_query_context_golden", harness)
        subprocess.run(["npm", "install", "--silent"], cwd=harness, check=True)
        env = {**os.environ, "SUPERSET_FRONTEND": str(superset / "superset-frontend")}
        subprocess.run(["node", "build.mjs"], cwd=harness, env=env, check=True)
        payloads = {n: {**p, "datasource": "7__table"} for n, p in CASES.items()}
        out = subprocess.run(
            ["node", "golden.cjs"],
            cwd=harness,
            input=json.dumps(payloads),
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    query_contexts = json.loads(out)
    golden = {
        name: {
            "viz_type": params["viz_type"],
            "params": params,
            "query_context": query_contexts[name],
        }
        for name, params in CASES.items()
    }
    (here / "superset_query_context_fixtures.json").write_text(
        json.dumps(golden, indent=2)
    )


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
