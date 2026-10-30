#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Ported from the Apache Superset 6.0.0 frontend, licensed to the Apache
#   Software Foundation (ASF) under the Apache License, Version 2.0:
#     superset-frontend/packages/superset-ui-core/src/query/
#       buildQueryContext.ts, buildQueryObject.ts, extractQueryFields.ts,
#       extractExtras.ts, processFilters.ts, convertFilter.ts,
#       processExtraFormData.ts, normalizeTimeColumn.ts, normalizeOrderBy.ts,
#       getXAxis.ts, getColumnLabel.ts, getMetricLabel.ts, DatasourceKey.ts
#     superset-frontend/packages/superset-ui-chart-controls/src/operators/
#     superset-frontend/plugins/plugin-chart-echarts/src/
#       BigNumber/BigNumberTotal, BigNumber/BigNumberWithTrendline, Pie,
#       Timeseries, Histogram, Heatmap, Bubble, BoxPlot, Funnel (buildQuery.ts)
#     superset-frontend/plugins/plugin-chart-table/src/buildQuery.ts
#     superset-frontend/plugins/plugin-chart-pivot-table/src/plugin/buildQuery.ts
#   Cross-checked against superset/migrations/shared/migrate_viz (query_functions.py,
#   processors.py), Superset's Python port of that frontend code.
#
"""Build the query_context Superset's explore view saves with a chart.

Superset answers `GET /api/v1/chart/<id>/data` from the chart's saved
query_context, and only its frontend builds one. This module builds the same
JSON as the Superset 6.0.0 frontend for the viz types the hops-superset skill
documents. Other viz types get the base query, as Superset's own fallback.

The frontend code is JavaScript: an absent key (undefined) differs from null,
and JSON.stringify drops it. `_UNDEFINED` stands for undefined here and
`_js_json` drops it the same way.
"""

from __future__ import annotations

import copy
import json
import math
import re
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


class _Undefined:
    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "undefined"


_UNDEFINED: Any = _Undefined()

DTTM_ALIAS = "__timestamp"
TIME_COMPARISON_SEPARATOR = "__"
CONTRIBUTION_SUFFIX = "__contribution"
COMPARISON_TYPES = ("values", "difference", "percentage", "ratio")
UNARY_OPERATORS = ("IS NOT NULL", "IS NULL")
EXTRA_FORM_DATA_OVERRIDE_EXTRA_KEYS = (
    "relative_start",
    "relative_end",
    "time_grain_sqla",
)
EXTRA_FORM_DATA_OVERRIDE_REGULAR_MAPPINGS = {
    "granularity": "granularity",
    "granularity_sqla": "granularity",
    "time_column": "time_column",
    "time_grain": "time_grain",
    "time_range": "time_range",
    "time_compare": "time_compare",
}
QUERY_FIELD_ALIASES = {
    "metric": "metrics",
    "metric_2": "metrics",
    "secondary_metric": "metrics",
    "x": "metrics",
    "y": "metrics",
    "size": "metrics",
    "all_columns": "columns",
    "series": "groupby",
    "order_by_cols": "orderby",
}
BUILD_QUERY_OBJECT_KEYS = (
    "annotation_layers",
    "extra_form_data",
    "time_range",
    "since",
    "until",
    "row_limit",
    "row_offset",
    "order_desc",
    "limit",
    "timeseries_limit_metric",
    "granularity",
    "url_params",
    "custom_params",
    "series_columns",
    "series_limit",
    "series_limit_metric",
    "group_others_when_limit_reached",
)


# region JavaScript semantics


def _get(obj: Any, key: str) -> Any:
    """`obj?.key`."""
    return obj.get(key, _UNDEFINED) if isinstance(obj, dict) else _UNDEFINED


def _nullish(value: Any) -> bool:
    return value is None or value is _UNDEFINED


def _truthy(value: Any) -> bool:
    if _nullish(value) or isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return value != 0 and not math.isnan(value)
    if isinstance(value, str):
        return value != ""
    return True


def _or(value: Any, fallback: Any) -> Any:
    """`value || fallback`."""
    return value if _truthy(value) else fallback


def _coalesce(value: Any, fallback: Any) -> Any:
    """`value ?? fallback`."""
    return fallback if _nullish(value) else value


def _default(value: Any, fallback: Any) -> Any:
    """A destructuring default, which only replaces undefined."""
    return fallback if value is _UNDEFINED else value


def _is_empty(value: Any) -> bool:
    """The isEmpty of lodash."""
    if isinstance(value, (str, list, dict)):
        return len(value) == 0
    return True


def _number(value: Any) -> float | int:
    """`Number(value)`."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if value is None:
        return 0
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        try:
            result = float(text)
        except ValueError:
            return math.nan
        return int(result) if result.is_integer() else result
    return math.nan


def _js_string(value: Any) -> str:
    """`String(value)` for the values a number is parsed from."""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return json.dumps(value)
    return ""


def _parse_int(value: Any) -> float | int:
    """`parseInt(String(value), 10)`."""
    match = re.match(r"\s*([+-]?\d+)", _js_string(value))
    return int(match.group(1)) if match else math.nan


def _parse_float(value: Any) -> float | int:
    """`parseFloat(value)`."""
    text = _js_string(value)
    match = re.match(r"\s*([+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?)", text)
    if not match:
        return math.nan
    result = float(match.group(1))
    return int(result) if result.is_integer() else result


def _length(value: Any) -> Any:
    return len(value) if isinstance(value, (str, list)) else _UNDEFINED


def _spread(value: Any) -> list:
    """`[...value]` for an array or a string."""
    return list(value) if isinstance(value, (list, str)) else []


def _js_json(value: Any) -> Any:
    """What JSON.stringify keeps: no undefined key, undefined and NaN in arrays as null."""
    if isinstance(value, dict):
        return {k: _js_json(v) for k, v in value.items() if v is not _UNDEFINED}
    if isinstance(value, (list, tuple)):
        return [None if v is _UNDEFINED else _js_json(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def ensure_is_array(value: Any) -> list:
    if _nullish(value):
        return []
    return value if isinstance(value, list) else [value]


def _remove_duplicates(items: list, key: Callable[[Any], Any]) -> list:
    seen = []
    result = []
    for item in items:
        item_key = key(item)
        if item_key not in seen:
            seen.append(item_key)
            result.append(item)
    return result


# region Columns and metrics


def is_physical_column(column: Any) -> bool:
    return isinstance(column, str)


def is_adhoc_column(column: Any) -> bool:
    return (
        not isinstance(column, str)
        and _get(column, "sqlExpression") is not _UNDEFINED
        and _get(column, "label") is not _UNDEFINED
        and _get(column, "expressionType") in (_UNDEFINED, "SQL")
    )


def is_query_form_column(column: Any) -> bool:
    return is_physical_column(column) or is_adhoc_column(column)


def is_query_form_metric(metric: Any) -> bool:
    return isinstance(metric, str) or _get(metric, "expressionType") in (
        "SIMPLE",
        "SQL",
    )


def get_column_label(column: Any) -> Any:
    if is_physical_column(column):
        return column
    if _truthy(_get(column, "label")):
        return column["label"]
    return _get(column, "sqlExpression")


def get_metric_label(metric: Any) -> Any:
    if isinstance(metric, str):
        return metric
    if _truthy(_get(metric, "label")):
        return metric["label"]
    if _get(metric, "expressionType") == "SIMPLE":
        column = metric["column"]
        name = _or(_get(column, "columnName"), _get(column, "column_name"))
        return f"{metric['aggregate']}({name})"
    return _get(metric, "sqlExpression")


def is_x_axis_set(form_data: dict) -> bool:
    return is_query_form_column(_get(form_data, "x_axis"))


def get_x_axis_column(form_data: dict) -> Any:
    if not (
        _truthy(_get(form_data, "granularity_sqla"))
        or _truthy(_get(form_data, "x_axis"))
    ):
        return _UNDEFINED
    if is_x_axis_set(form_data):
        return form_data["x_axis"]
    return DTTM_ALIAS


def get_x_axis_label(form_data: dict) -> Any:
    column = get_x_axis_column(form_data)
    return get_column_label(column) if _truthy(column) else _UNDEFINED


# region Query object


def extract_query_fields(form_data: dict, aliases: dict | None = None) -> dict:
    query_field_aliases = {**QUERY_FIELD_ALIASES, **(aliases or {})}
    query_mode = _get(form_data, "query_mode")
    columns: list = []
    metrics: list = []
    orderby: list = []
    for key, value in form_data.items():
        if key == "query_mode" or _nullish(value):
            continue
        normalized_key = _or(query_field_aliases.get(key), key)
        if query_mode == "aggregate" and normalized_key == "columns":
            continue
        if query_mode == "raw" and normalized_key in ("groupby", "metrics"):
            continue
        if normalized_key == "groupby":
            normalized_key = "columns"
        target = {"metrics": metrics, "columns": columns, "orderby": orderby}.get(
            normalized_key
        )
        if target is not None:
            target.extend(value if isinstance(value, list) else [value])
    return {
        "columns": _remove_duplicates(
            [column for column in columns if column != ""], get_column_label
        ),
        "metrics": _UNDEFINED
        if query_mode == "raw"
        else _remove_duplicates(metrics, get_metric_label),
        "orderby": [
            json.loads(item) if isinstance(item, str) else item for item in orderby
        ]
        if orderby
        else _UNDEFINED,
    }


def extract_extras(form_data: dict) -> dict:
    reserved_columns_to_query_field = {
        "__time_range": "time_range",
        "__time_col": "granularity_sqla",
        "__time_grain": "time_grain_sqla",
        "__granularity": "granularity",
        "__time_compare": "time_compare",
    }
    applied_time_extras: dict = {}
    filters: list = []
    extras: dict = {}
    extract: dict = {
        "filters": filters,
        "extras": extras,
        "applied_time_extras": applied_time_extras,
    }
    for extra_filter in _or(_get(form_data, "extra_filters"), []):
        col = _get(extra_filter, "col")
        if col in reserved_columns_to_query_field:
            extract[reserved_columns_to_query_field[col]] = _get(extra_filter, "val")
            applied_time_extras[col] = _get(extra_filter, "val")
        else:
            filters.append(extra_filter)
    extras["time_grain_sqla"] = _or(
        _get(extract, "time_grain_sqla"), _get(form_data, "time_grain_sqla")
    )
    extract["granularity"] = _or(
        _or(_get(extract, "granularity_sqla"), _get(form_data, "granularity")),
        _get(form_data, "granularity_sqla"),
    )
    extract.pop("granularity_sqla", None)
    extract.pop("time_grain_sqla", None)
    return extract


def _sanitize_clause(clause: str) -> str:
    return f"({clause}\n)" if "--" in clause else f"({clause})"


def _convert_filter(adhoc_filter: dict) -> dict:
    result = {
        "col": _get(adhoc_filter, "subject"),
        "op": _get(adhoc_filter, "operator"),
    }
    if _get(adhoc_filter, "operator") not in UNARY_OPERATORS:
        result["val"] = _get(adhoc_filter, "comparator")
    return result


def process_filters(form_data: dict) -> dict:
    extras = _default(_get(form_data, "extras"), {})
    simple_where = _default(_get(form_data, "filters"), [])
    freeform_where = []
    freeform_having = []
    if _truthy(_get(form_data, "where")):
        freeform_where.append(form_data["where"])
    for adhoc_filter in _or(_get(form_data, "adhoc_filters"), []):
        clause = _get(adhoc_filter, "clause")
        if _get(adhoc_filter, "expressionType") == "SIMPLE":
            if clause == "WHERE":
                simple_where.append(_convert_filter(adhoc_filter))
        elif clause == "WHERE":
            freeform_where.append(adhoc_filter["sqlExpression"])
        else:
            freeform_having.append(adhoc_filter["sqlExpression"])
    extras["having"] = " AND ".join(_sanitize_clause(c) for c in freeform_having)
    extras["where"] = " AND ".join(_sanitize_clause(c) for c in freeform_where)
    return {"filters": simple_where, "extras": extras}


def _override_extra_form_data(query_object: dict, overrides: dict) -> dict:
    overridden = dict(query_object)
    overridden_extras = _default(_get(overridden, "extras"), {})
    for key, target in EXTRA_FORM_DATA_OVERRIDE_REGULAR_MAPPINGS.items():
        if _get(overrides, key) is not _UNDEFINED:
            overridden[target] = overrides[key]
    for key in EXTRA_FORM_DATA_OVERRIDE_EXTRA_KEYS:
        if key in overrides:
            overridden_extras[key] = overrides[key]
    if overridden_extras:
        overridden["extras"] = overridden_extras
    return overridden


def build_query_object(form_data: dict, query_fields: dict | None = None) -> dict:
    fd = {key: _get(form_data, key) for key in BUILD_QUERY_OBJECT_KEYS}
    residual = {k: v for k, v in form_data.items() if k not in BUILD_QUERY_OBJECT_KEYS}
    extra_form_data = _or(fd["extra_form_data"], {})
    append_adhoc_filters = _default(_get(extra_form_data, "adhoc_filters"), [])
    append_filters = _default(_get(extra_form_data, "filters"), [])
    custom_form_data = _default(_get(extra_form_data, "custom_form_data"), {})
    overrides = {
        k: v
        for k, v in extra_form_data.items()
        if k not in ("adhoc_filters", "filters", "custom_form_data")
    }
    fields = extract_query_fields(residual, query_fields)
    extras = extract_extras(form_data)
    extras_and_filters = process_filters(
        {
            **form_data,
            **extras,
            "filters": [*extras["filters"], *append_filters],
            "adhoc_filters": [
                *_or(_get(form_data, "adhoc_filters"), []),
                *append_adhoc_filters,
            ],
        }
    )

    def optional_number(value: Any) -> Any:
        number = _number(value)
        return _UNDEFINED if _nullish(value) or math.isnan(number) else number

    series_limit_metric = fd["series_limit_metric"]
    query_object = {
        "time_range": _or(fd["time_range"], _UNDEFINED),
        "since": _or(fd["since"], _UNDEFINED),
        "until": _or(fd["until"], _UNDEFINED),
        "granularity": _or(fd["granularity"], _UNDEFINED),
        **extras,
        **extras_and_filters,
        **fields,
        "annotation_layers": _default(fd["annotation_layers"], []),
        "row_limit": optional_number(fd["row_limit"]),
        "row_offset": optional_number(fd["row_offset"]),
        "series_columns": fd["series_columns"],
        "series_limit": _coalesce(
            fd["series_limit"], 0 if _nullish(fd["limit"]) else _number(fd["limit"])
        ),
        "series_limit_metric": _coalesce(
            series_limit_metric
            if is_query_form_metric(series_limit_metric)
            else _UNDEFINED,
            fd["timeseries_limit_metric"],
        ),
        "group_others_when_limit_reached": _coalesce(
            fd["group_others_when_limit_reached"], False
        ),
        "order_desc": True if fd["order_desc"] is _UNDEFINED else fd["order_desc"],
        "url_params": _or(_default(fd["url_params"], {}), _UNDEFINED),
        "custom_params": _default(fd["custom_params"], {}),
    }
    query_object = _override_extra_form_data(query_object, overrides)
    return {**query_object, "custom_form_data": custom_form_data}


def normalize_order_by(query_object: dict) -> dict:
    orderby = _get(query_object, "orderby")
    if isinstance(orderby, list) and orderby:
        clause = orderby[0]
        if (
            isinstance(clause, list)
            and len(clause) == 2
            and not _is_empty(clause[0])
            and isinstance(clause[1], bool)
        ):
            return query_object
    clone = {
        k: v
        for k, v in query_object.items()
        if k not in ("series_limit_metric", "legacy_order_by", "order_desc", "orderby")
    }
    is_asc = not _truthy(_get(query_object, "order_desc"))
    for key in ("series_limit_metric", "legacy_order_by"):
        value = _get(query_object, key)
        if not _nullish(value) and not _is_empty(value):
            return {**clone, "orderby": [[value, is_asc]]}
    metrics = _get(query_object, "metrics")
    if isinstance(metrics, list) and metrics:
        return {**clone, "orderby": [[metrics[0], is_asc]]}
    return clone


def normalize_time_column(form_data: dict, query_object: dict) -> dict:
    if not is_x_axis_set(form_data):
        return query_object
    columns = _get(query_object, "columns")
    x_axis = form_data["x_axis"]
    if not isinstance(columns, list):
        return query_object
    axis_idx = next(
        (
            i
            for i, col in enumerate(columns)
            if (
                is_physical_column(col) and is_physical_column(x_axis) and col == x_axis
            )
            or (
                is_adhoc_column(col)
                and is_adhoc_column(x_axis)
                and col["sqlExpression"] == x_axis["sqlExpression"]
            )
        ),
        -1,
    )
    if axis_idx < 0 or not _truthy(x_axis):
        return query_object
    time_grain = _get(_get(query_object, "extras"), "time_grain_sqla")
    mutated = list(columns)
    if is_adhoc_column(columns[axis_idx]):
        mutated[axis_idx] = {
            "timeGrain": time_grain,
            "columnType": "BASE_AXIS",
            **columns[axis_idx],
        }
    else:
        mutated[axis_idx] = {
            "timeGrain": time_grain,
            "columnType": "BASE_AXIS",
            "sqlExpression": x_axis,
            "label": x_axis,
            "expressionType": "SQL",
            "isColumnReference": True,
        }
    new_query_object = {k: v for k, v in query_object.items() if k != "is_timeseries"}
    new_query_object["columns"] = mutated
    return new_query_object


def _datasource_object(key: str) -> dict:
    parts = key.split("__")
    return {
        "id": _parse_int(parts[0]),
        "type": "query" if len(parts) > 1 and parts[1] == "query" else "table",
    }


def _build_query_context(
    form_data: dict,
    build_query: Callable[[dict], list] = lambda base: [base],
    query_fields: dict | None = None,
) -> dict:
    queries = build_query(build_query_object(form_data, query_fields))
    for query in queries:
        if isinstance(_get(query, "post_processing"), list):
            query["post_processing"] = [
                p for p in query["post_processing"] if _truthy(p)
            ]
    if is_x_axis_set(form_data):
        queries = [normalize_time_column(form_data, query) for query in queries]
    return {
        "datasource": _datasource_object(form_data["datasource"]),
        "force": _or(_get(form_data, "force"), False),
        "queries": queries,
        "form_data": form_data,
        "result_format": _or(_get(form_data, "result_format"), "json"),
        "result_type": _or(_get(form_data, "result_type"), "full"),
    }


# region Post-processing operators


def _extract_extra_metrics(form_data: dict) -> list:
    x_axis_sort = _get(form_data, "x_axis_sort")
    limit_metric = (
        ensure_is_array(_get(form_data, "timeseries_limit_metric")) or [_UNDEFINED]
    )[0]
    metrics = _get(form_data, "metrics")
    if (
        not _truthy(_length(_or(_get(form_data, "groupby"), [])))
        and _truthy(limit_metric)
        and get_metric_label(limit_metric) == x_axis_sort
        and not (
            isinstance(metrics, list)
            and any(get_metric_label(m) == x_axis_sort for m in metrics)
        )
    ):
        return [limit_metric]
    return []


def _metric_offsets_map(form_data: dict, query_object: dict) -> dict:
    offsets = ensure_is_array(_get(form_data, "time_compare"))
    return {
        f"{label}{TIME_COMPARISON_SEPARATOR}{offset}": label
        for label in map(
            get_metric_label, ensure_is_array(_get(query_object, "metrics"))
        )
        for offset in offsets
    }


def _is_time_comparison(form_data: dict, query_object: dict) -> bool:
    return _get(form_data, "comparison_type") in COMPARISON_TYPES and bool(
        _metric_offsets_map(form_data, query_object)
    )


def _series_columns(query_object: dict) -> list:
    return ensure_is_array(
        _or(_get(query_object, "series_columns"), _get(query_object, "columns"))
    )


def _pivot_operator(form_data: dict, query_object: dict) -> Any:
    metric_labels = [
        get_metric_label(m)
        for m in [
            *ensure_is_array(_get(query_object, "metrics")),
            *_extract_extra_metrics(form_data),
        ]
    ]
    x_axis_label = get_x_axis_label(form_data)
    if _truthy(x_axis_label) and metric_labels:
        return {
            "operation": "pivot",
            "options": {
                "index": [x_axis_label],
                "columns": [get_column_label(c) for c in _series_columns(query_object)],
                "aggregates": {m: {"operator": "mean"} for m in metric_labels},
                "drop_missing_columns": not _truthy(
                    _get(form_data, "show_empty_columns")
                ),
            },
        }
    return _UNDEFINED


def _time_compare_pivot_operator(form_data: dict, query_object: dict) -> Any:
    offsets_map = _metric_offsets_map(form_data, query_object)
    x_axis_label = get_x_axis_label(form_data)
    if _is_time_comparison(form_data, query_object) and _truthy(x_axis_label):
        return {
            "operation": "pivot",
            "options": {
                "index": [x_axis_label],
                "columns": [get_column_label(c) for c in _series_columns(query_object)],
                "drop_missing_columns": not _truthy(
                    _get(form_data, "show_empty_columns")
                ),
                "aggregates": {
                    m: {"operator": "mean"}
                    for m in [*offsets_map.values(), *offsets_map.keys()]
                },
            },
        }
    return _UNDEFINED


def _rolling_window_operator(form_data: dict, query_object: dict) -> Any:
    if _is_time_comparison(form_data, query_object):
        offsets_map = _metric_offsets_map(form_data, query_object)
        columns = [*offsets_map.values(), *offsets_map.keys()]
    else:
        columns = [
            m if isinstance(m, str) else _get(m, "label")
            for m in ensure_is_array(_get(query_object, "metrics"))
        ]
    columns_map = {c: c for c in columns if c is not _UNDEFINED}
    rolling_type = _get(form_data, "rolling_type")
    if rolling_type == "cumsum":
        return {
            "operation": "cum",
            "options": {"operator": "sum", "columns": columns_map},
        }
    if rolling_type in ("sum", "mean", "std"):
        window = _parse_int(_get(form_data, "rolling_periods"))
        min_periods = _parse_int(_get(form_data, "min_periods"))
        return {
            "operation": "rolling",
            "options": {
                "rolling_type": rolling_type,
                "window": 1 if math.isnan(window) else window,
                "min_periods": 0 if math.isnan(min_periods) else min_periods,
                "columns": columns_map,
            },
        }
    return _UNDEFINED


def _time_compare_operator(form_data: dict, query_object: dict) -> Any:
    comparison_type = _get(form_data, "comparison_type")
    offsets_map = _metric_offsets_map(form_data, query_object)
    if _is_time_comparison(form_data, query_object) and comparison_type != "values":
        return {
            "operation": "compare",
            "options": {
                "source_columns": list(offsets_map.values()),
                "compare_columns": list(offsets_map.keys()),
                "compare_type": comparison_type,
                "drop_original_columns": True,
            },
        }
    return _UNDEFINED


def _resample_operator(form_data: dict, query_object: dict) -> Any:
    zero_fill = _get(form_data, "resample_method") == "zerofill"
    method = "asfreq" if zero_fill else _get(form_data, "resample_method")
    rule = _get(form_data, "resample_rule")
    if _truthy(method) and _truthy(rule):
        return {
            "operation": "resample",
            "options": {
                "method": method,
                "rule": rule,
                "fill_value": 0 if zero_fill else None,
            },
        }
    return _UNDEFINED


def _rename_operator(form_data: dict, query_object: dict) -> Any:
    metrics = ensure_is_array(_get(query_object, "metrics"))
    columns = _series_columns(query_object)
    time_offsets = ensure_is_array(_get(form_data, "time_compare"))
    truncate_metric = _get(form_data, "truncate_metric")
    comparison_type = _get(form_data, "comparison_type")
    is_comparison = _is_time_comparison(form_data, query_object)
    if not (
        metrics
        and _truthy(get_x_axis_label(form_data))
        and (
            is_comparison
            or ((columns or len(time_offsets) > 1) and _truthy(truncate_metric))
        )
    ):
        return _UNDEFINED
    rename_pairs = []
    if is_comparison:
        for with_offset, metric in _metric_offsets_map(form_data, query_object).items():
            offset_label = next(
                (o for o in time_offsets if o in with_offset), _UNDEFINED
            )
            rename_pairs.append(
                (
                    with_offset
                    if comparison_type == "values"
                    else TIME_COMPARISON_SEPARATOR.join(
                        [comparison_type, metric, with_offset]
                    ),
                    f"{metric}, {offset_label}" if len(metrics) > 1 else offset_label,
                )
            )
    if (
        comparison_type not in ("difference", "percentage", "ratio")
        and len(metrics) == 1
        and not rename_pairs
    ):
        rename_pairs.append((get_metric_label(metrics[0]), None))
    if not rename_pairs:
        return _UNDEFINED
    return {
        "operation": "rename",
        "options": {"columns": dict(rename_pairs), "level": 0, "inplace": True},
    }


def _contribution_operator(form_data: dict, time_shifts: Any) -> Any:
    if _truthy(_get(form_data, "contributionMode")):
        return {
            "operation": "contribution",
            "options": {
                "orientation": form_data["contributionMode"],
                "time_shifts": time_shifts,
            },
        }
    return _UNDEFINED


def _sort_operator(form_data: dict) -> Any:
    x_axis_label = get_x_axis_label(form_data)
    sortable_labels = [
        label
        for label in [
            x_axis_label,
            *map(get_metric_label, ensure_is_array(_get(form_data, "metrics"))),
            *map(get_metric_label, _extract_extra_metrics(form_data)),
        ]
        if _truthy(label)
    ]
    x_axis_sort = _get(form_data, "x_axis_sort")
    ascending = _get(form_data, "x_axis_sort_asc")
    if (
        not _nullish(x_axis_sort)
        and not _nullish(ascending)
        and x_axis_sort in sortable_labels
        and _is_empty(_get(form_data, "groupby"))
    ):
        if x_axis_sort == x_axis_label:
            return {
                "operation": "sort",
                "options": {"is_sort_index": True, "ascending": ascending},
            }
        return {
            "operation": "sort",
            "options": {"by": x_axis_sort, "ascending": ascending},
        }
    return _UNDEFINED


def _prophet_operator(form_data: dict) -> Any:
    x_axis_label = get_x_axis_label(form_data)
    if _truthy(_get(form_data, "forecastEnabled")) and _truthy(x_axis_label):
        return {
            "operation": "prophet",
            "options": {
                "time_grain": _get(form_data, "time_grain_sqla"),
                "periods": _parse_int(_get(form_data, "forecastPeriods")),
                "confidence_interval": _parse_float(
                    _get(form_data, "forecastInterval")
                ),
                "yearly_seasonality": _get(form_data, "forecastSeasonalityYearly"),
                "weekly_seasonality": _get(form_data, "forecastSeasonalityWeekly"),
                "daily_seasonality": _get(form_data, "forecastSeasonalityDaily"),
                "index": x_axis_label,
            },
        }
    return _UNDEFINED


def _boxplot_operator(form_data: dict, query_object: dict) -> Any:
    whisker_options = _get(form_data, "whiskerOptions")
    if not _truthy(whisker_options):
        return _UNDEFINED
    percentiles = _UNDEFINED
    match = re.search(r"(\d+)/(\d+) percentiles", str(whisker_options))
    if whisker_options == "Tukey":
        whisker_type = "tukey"
    elif whisker_options == "Min/max (no outliers)":
        whisker_type = "min/max"
    elif match:
        whisker_type = "percentile"
        percentiles = [int(match.group(1)), int(match.group(2))]
    else:
        raise ValueError(f"Unsupported whisker type: {whisker_options}")
    return {
        "operation": "boxplot",
        "options": {
            "whisker_type": whisker_type,
            "percentiles": percentiles,
            "groupby": [
                get_column_label(c) for c in ensure_is_array(_get(form_data, "groupby"))
            ],
            "metrics": [
                get_metric_label(m)
                for m in ensure_is_array(_get(query_object, "metrics"))
            ],
        },
    }


# region Per viz type buildQuery


def _temporal_column(column: str, time_grain: Any) -> dict:
    return {
        "timeGrain": time_grain,
        "columnType": "BASE_AXIS",
        "sqlExpression": column,
        "label": column,
        "expressionType": "SQL",
    }


def _big_number(form_data: dict) -> dict:
    time_column = (
        ensure_is_array(get_x_axis_column(form_data))
        if is_x_axis_set(form_data)
        else []
    )

    def build(base: dict) -> list:
        queries = [
            {
                **base,
                "columns": list(time_column),
                **({} if time_column else {"is_timeseries": True}),
                "post_processing": [
                    p
                    for p in [
                        _pivot_operator(form_data, base),
                        _rolling_window_operator(form_data, base),
                        _resample_operator(form_data, base),
                        {"operation": "flatten"},
                    ]
                    if _truthy(p)
                ],
            }
        ]
        if _get(form_data, "aggregation") == "raw":
            queries.append(
                {**base, "columns": [], "is_timeseries": False, "post_processing": []}
            )
        return queries

    return _build_query_context(form_data, build)


def _pie(form_data: dict) -> dict:
    metric = _get(form_data, "metric")
    metric_label = get_metric_label(metric)

    def build(base: dict) -> list:
        query = dict(base)
        if _truthy(_get(form_data, "sort_by_metric")):
            query["orderby"] = [[metric, False]]
        query["post_processing"] = [
            {
                "operation": "contribution",
                "options": {
                    "columns": [metric_label],
                    "rename_columns": [f"{metric_label}{CONTRIBUTION_SUFFIX}"],
                },
            }
        ]
        return [query]

    return _build_query_context(form_data, build)


def _timeseries(form_data: dict) -> dict:
    groupby = _get(form_data, "groupby")
    x_axis_set = is_x_axis_set(form_data)

    def build(base: dict) -> list:
        is_comparison = _is_time_comparison(form_data, base)
        time_offsets = (
            form_data.get("time_compare", _UNDEFINED) if is_comparison else []
        )
        return [
            {
                **base,
                "metrics": [
                    *_or(_get(base, "metrics"), []),
                    *_extract_extra_metrics(form_data),
                ],
                "columns": [
                    *(
                        ensure_is_array(get_x_axis_column(form_data))
                        if x_axis_set
                        else []
                    ),
                    *ensure_is_array(groupby),
                ],
                "series_columns": groupby,
                **({} if x_axis_set else {"is_timeseries": True}),
                "orderby": _get(normalize_order_by(base), "orderby"),
                "time_offsets": time_offsets,
                "post_processing": [
                    _time_compare_pivot_operator(form_data, base)
                    if is_comparison
                    else _pivot_operator(form_data, base),
                    _rolling_window_operator(form_data, base),
                    _time_compare_operator(form_data, base),
                    _resample_operator(form_data, base),
                    _rename_operator(form_data, base),
                    _contribution_operator(form_data, time_offsets),
                    _sort_operator(form_data),
                    {"operation": "flatten"},
                    _prophet_operator(form_data),
                ],
            }
        ]

    return _build_query_context(form_data, build)


def _histogram(form_data: dict) -> dict:
    column = _get(form_data, "column")
    groupby = _default(_get(form_data, "groupby"), [])

    def build(base: dict) -> list:
        bins = _number(_get(form_data, "bins"))
        return [
            {
                **base,
                "columns": [*_spread(groupby), column],
                "post_processing": [
                    {
                        "operation": "histogram",
                        "options": {
                            "column": get_column_label(column),
                            "groupby": [get_column_label(g) for g in groupby],
                            "bins": 5 if math.isnan(bins) else bins,
                            "cumulative": _get(form_data, "cumulative"),
                            "normalize": _get(form_data, "normalize"),
                        },
                    }
                ],
                "metrics": _UNDEFINED,
            }
        ]

    return _build_query_context(form_data, build)


def _table_query_mode(form_data: dict) -> str:
    mode = _get(form_data, "query_mode")
    if mode in ("aggregate", "raw"):
        return mode
    raw_columns = _get(form_data, "all_columns")
    return (
        "raw" if _truthy(raw_columns) and _truthy(_length(raw_columns)) else "aggregate"
    )


def _table(form_data: dict) -> dict:
    percent_metrics = _get(form_data, "percent_metrics")
    order_desc = _default(_get(form_data, "order_desc"), False)
    extra_form_data = _get(form_data, "extra_form_data")
    query_mode = _table_query_mode(form_data)
    sort_by_metric = (
        ensure_is_array(_get(form_data, "timeseries_limit_metric")) or [_UNDEFINED]
    )[0]
    time_grain_sqla = _or(
        _get(extra_form_data, "time_grain_sqla"), _get(form_data, "time_grain_sqla")
    )
    form_data_copy = (
        {**form_data, "include_time": False} if query_mode == "raw" else form_data
    )
    time_compare = ensure_is_array(_get(form_data, "time_compare"))
    calculation_mode = _or(_get(form_data, "percent_metric_calculation"), "row_limit")
    server_pagination = _truthy(_get(form_data, "server_pagination"))

    def build(base: dict) -> list:
        metrics = _get(base, "metrics")
        orderby = _default(_get(base, "orderby"), [])
        columns = _default(_get(base, "columns"), [])
        extras = _default(_get(base, "extras"), {})
        post_processing = []
        is_comparison = _is_time_comparison(form_data, base)
        time_offsets = []
        if is_comparison:
            time_offsets = [s for s in time_compare if s not in ("custom", "inherit")]
            if "custom" in time_compare:
                time_offsets = [*time_offsets, _get(form_data, "start_date_offset")]
            if "inherit" in time_compare:
                time_offsets = [*time_offsets, "inherit"]
        extra_time_compare = _get(extra_form_data, "time_compare")
        if _truthy(extra_time_compare) and extra_time_compare not in time_offsets:
            time_offsets = [extra_time_compare]

        if query_mode == "aggregate":
            metrics = _or(metrics, [])
            if _truthy(sort_by_metric):
                orderby = [[sort_by_metric, not _truthy(order_desc)]]
            elif metrics:
                orderby = [[metrics[0], False]]
            if _truthy(percent_metrics) and _truthy(_length(percent_metrics)):
                labels = [get_metric_label(m) for m in percent_metrics]
                if is_comparison:
                    labels = [
                        label
                        for metric in labels
                        for label in [metric, *(f"{metric}__{s}" for s in time_offsets)]
                    ]
                labels = _remove_duplicates(labels, lambda label: label)
                metrics = _remove_duplicates(
                    [*metrics, *percent_metrics], get_metric_label
                )
                post_processing.append(
                    {
                        "operation": "contribution",
                        "options": {
                            "columns": labels,
                            "rename_columns": [f"%{m}" for m in labels],
                        },
                    }
                )
            if time_offsets:
                post_processing.append(_time_compare_operator(form_data, base))
            temporal_lookup = _get(form_data, "temporal_columns_lookup")
            temporal_column = None
            kept = []
            for col in columns:
                if (
                    temporal_column is None
                    and is_physical_column(col)
                    and _truthy(time_grain_sqla)
                    and _truthy(_get(temporal_lookup, col))
                ):
                    temporal_column = _temporal_column(col, time_grain_sqla)
                else:
                    kept.append(col)
            columns = [temporal_column, *kept] if temporal_column else kept

        more = {}
        if server_pagination:
            page_size = _get(form_data_copy, "server_page_length")
            more = {"row_limit": page_size, "row_offset": 0 * _number(page_size)}
        query = {
            **base,
            "columns": columns,
            "extras": extras,
            "orderby": orderby,
            "metrics": metrics,
            "post_processing": post_processing,
            "time_offsets": time_offsets,
            **more,
        }
        extra_queries = []
        if (
            calculation_mode == "all_records"
            and _truthy(percent_metrics)
            and _truthy(_length(percent_metrics))
        ):
            extra_queries.append(
                {
                    **query,
                    "columns": [],
                    "metrics": percent_metrics,
                    "post_processing": [],
                    "row_limit": 0,
                    "row_offset": 0,
                    "orderby": [],
                    "is_timeseries": False,
                }
            )
        if (
            _truthy(_length(metrics))
            and _truthy(_get(form_data, "show_totals"))
            and query_mode == "aggregate"
        ):
            extra_queries.append(
                {
                    **query,
                    "columns": [],
                    "row_limit": 0,
                    "row_offset": 0,
                    "post_processing": [],
                    "order_desc": _UNDEFINED,
                    "orderby": _UNDEFINED,
                }
            )
        interactive_groupby = _get(extra_form_data, "interactive_groupby")
        if _truthy(interactive_groupby) and _truthy(query["columns"]):
            query["columns"] = _remove_duplicates(
                [*query["columns"], *interactive_groupby],
                lambda c: c if isinstance(c, str) else id(c),
            )
        if server_pagination:
            row_limit = _number(_get(form_data, "row_limit"))
            return [
                query,
                {
                    **query,
                    "time_offsets": [],
                    "row_limit": row_limit,
                    "row_offset": 0,
                    "post_processing": [],
                    "is_rowcount": True,
                },
                *extra_queries,
            ]
        return [query, *extra_queries]

    return _build_query_context(form_data_copy, build)


def _pivot_table(form_data: dict) -> dict:
    extra_form_data = _get(form_data, "extra_form_data")
    time_grain_sqla = _or(
        _get(extra_form_data, "time_grain_sqla"), _get(form_data, "time_grain_sqla")
    )
    temporal_lookup = _get(form_data, "temporal_columns_lookup")
    unique = _remove_duplicates(
        [
            *ensure_is_array(_default(_get(form_data, "groupbyColumns"), [])),
            *ensure_is_array(_default(_get(form_data, "groupbyRows"), [])),
        ],
        lambda c: c if isinstance(c, str) else id(c),
    )
    columns = [
        _temporal_column(col, time_grain_sqla)
        if is_physical_column(col)
        and _truthy(time_grain_sqla)
        and (
            _truthy(_get(temporal_lookup, col))
            or _get(form_data, "granularity_sqla") == col
        )
        else col
        for col in unique
    ]

    def build(base: dict) -> list:
        series_limit_metric = _get(base, "series_limit_metric")
        metrics = _get(base, "metrics")
        order_desc = not _truthy(_get(base, "order_desc"))
        orderby = _UNDEFINED
        if _truthy(series_limit_metric):
            orderby = [[series_limit_metric, order_desc]]
        elif isinstance(metrics, list) and metrics and _truthy(metrics[0]):
            orderby = [[metrics[0], order_desc]]
        return [{**base, "orderby": orderby, "columns": columns}]

    return _build_query_context(form_data, build)


def _heatmap(form_data: dict) -> dict:
    groupby = _get(form_data, "groupby")
    metric = get_metric_label(_get(form_data, "metric"))
    columns = [
        *ensure_is_array(get_x_axis_column(form_data)),
        *ensure_is_array(groupby),
    ]
    orderby = []
    for sort, index in (
        (_get(form_data, "sort_x_axis"), 0),
        (_get(form_data, "sort_y_axis"), 1),
    ):
        if _truthy(sort):
            chosen = (
                metric
                if "value" in sort
                else (columns[index] if index < len(columns) else _UNDEFINED)
            )
            orderby.append([chosen, "asc" in sort])
    normalize_across = _get(form_data, "normalize_across")
    if normalize_across == "x":
        group_by = get_column_label(_get(form_data, "x_axis"))
    elif normalize_across == "y":
        group_by = get_column_label(groupby)
    else:
        group_by = _UNDEFINED

    def build(base: dict) -> list:
        return [
            {
                **base,
                "columns": columns,
                "orderby": orderby,
                "post_processing": [
                    {
                        "operation": "rank",
                        "options": {"metric": metric, "group_by": group_by},
                    }
                ],
            }
        ]

    return _build_query_context(form_data, build)


def _bubble(form_data: dict) -> dict:
    columns = [
        *ensure_is_array(_get(form_data, "entity")),
        *ensure_is_array(_get(form_data, "series")),
    ]

    def build(base: dict) -> list:
        orderby = _get(base, "orderby")
        return [
            {
                **base,
                "columns": columns,
                "orderby": [[orderby[0], not _truthy(_get(base, "order_desc"))]]
                if _truthy(orderby)
                else _UNDEFINED,
            }
        ]

    return _build_query_context(form_data, build)


def _box_plot(form_data: dict) -> dict:
    columns = ensure_is_array(_get(form_data, "columns"))
    granularity_sqla = _get(form_data, "granularity_sqla")
    time_grain_sqla = _get(form_data, "time_grain_sqla")
    temporal_lookup = _get(form_data, "temporal_columns_lookup")
    if not columns and _truthy(granularity_sqla):
        columns = [granularity_sqla]
    distributed = [
        _temporal_column(col, time_grain_sqla)
        if is_physical_column(col)
        and _truthy(time_grain_sqla)
        and _truthy(_get(temporal_lookup, col))
        else col
        for col in columns
    ]

    def build(base: dict) -> list:
        return [
            {
                **base,
                "columns": [*distributed, *ensure_is_array(_get(form_data, "groupby"))],
                "series_columns": _get(form_data, "groupby"),
                "post_processing": [_boxplot_operator(form_data, base)],
            }
        ]

    return _build_query_context(form_data, build)


def _funnel(form_data: dict) -> dict:
    def build(base: dict) -> list:
        query = dict(base)
        if _truthy(_get(form_data, "sort_by_metric")):
            query["orderby"] = [[_get(form_data, "metric"), False]]
        return [query]

    return _build_query_context(form_data, build)


BUILD_QUERY = {
    "big_number_total": _build_query_context,
    "big_number": _big_number,
    "pie": _pie,
    "echarts_timeseries_bar": _timeseries,
    "echarts_timeseries_line": _timeseries,
    "histogram_v2": _histogram,
    "table": _table,
    "pivot_table_v2": _pivot_table,
    "heatmap_v2": _heatmap,
    "bubble_v2": _bubble,
    "box_plot": _box_plot,
    "funnel": _funnel,
}


def build_query_context(
    datasource_id: int, viz_type: str, params: dict, datasource_type: str = "table"
) -> dict:
    """Build the query_context the Superset 6.0.0 explore view saves for a chart.

    Parameters:
        datasource_id: Superset dataset ID.
        viz_type: Superset viz type.
        params: Chart form data, as stored in the chart's params.
        datasource_type: Superset datasource type.

    Returns:
        The query_context as a JSON-ready dict.

    Raises:
        ValueError: The params hold a value Superset's frontend also refuses.
    """
    form_data = {
        **copy.deepcopy(params),
        "datasource": f"{datasource_id}__{datasource_type}",
        "viz_type": viz_type,
        "force": False,
        "result_format": "json",
        "result_type": "full",
    }
    return _js_json(BUILD_QUERY.get(viz_type, _build_query_context)(form_data))
