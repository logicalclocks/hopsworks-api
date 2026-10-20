# End to end: a feature group to a dashboard

Part of **hops-superset**.

```python
import json
import hopsworks

# Columns below (state, age, …) are illustrative: swap for real ones from
# `hops fg feature list <FG_NAME> --version <FG_VERSION>`.
PROJECT_NAME = "<your_project>"        # don't hardcode; set to project.name after login
FG_NAME, FG_VERSION = "customers", 1

COUNT_METRIC = {
    "expressionType": "SQL", "sqlExpression": "COUNT(*)",
    "label": "count", "optionName": "metric_count", "hasCustomLabel": True,
}


def main():
    project = hopsworks.login()
    api = project.get_superset_api()

    trino_db_id = find_trino_db_id(api)
    schema = f"{PROJECT_NAME}_featurestore"
    sql = f"SELECT * FROM delta.{schema}.{FG_NAME}_{FG_VERSION}"

    dataset_id = ensure_dataset(
        api, database_id=trino_db_id, schema=schema, name=FG_NAME, sql=sql,
    )

    chart_ids = {
        "total": replace_chart(
            api, slice_name="Total Customers",
            viz_type="big_number_total", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "big_number_total", "metric": COUNT_METRIC,
                "adhoc_filters": [], "y_axis_format": "SMART_NUMBER",
            }),
        ),
        "by_state": replace_chart(
            api, slice_name="Top States",
            viz_type="echarts_timeseries_bar", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "state", "x_axis_force_categorical": True,
                "metrics": [COUNT_METRIC], "groupby": [], "adhoc_filters": [],
                "row_limit": 20, "orientation": "vertical", "order_desc": True,
                "timeseries_limit_metric": COUNT_METRIC,
                "x_axis_sort": "count", "x_axis_sort_asc": False,
                "y_axis_format": "SMART_NUMBER",
            }),
        ),
        "age_hist": replace_chart(
            api, slice_name="Age Distribution",
            viz_type="histogram_v2", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "histogram_v2", "column": "age",
                "groupby": [], "adhoc_filters": [], "row_limit": 50000,
                "bins": 20, "x_axis_title": "Age", "y_axis_title": "Customers",
            }),
        ),
    }

    chart_slices = {"total": "Total Customers", "by_state": "Top States",
                    "age_hist": "Age Distribution"}
    dashboard_id = ensure_dashboard(
        api, "Customers Overview", chart_ids, chart_slices,
    )
    print(f"Dashboard id: {dashboard_id}")


if __name__ == "__main__":
    main()
```
