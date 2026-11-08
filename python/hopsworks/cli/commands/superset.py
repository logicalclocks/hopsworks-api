"""``hops superset`` — manage Superset datasets, charts, and dashboards.

Superset in Hopsworks is exposed via ``project.get_superset_api()``. That
module handles Superset's CSRF dance and session tokens for us, so the CLI
just wraps each method in a Click command.
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Any

import click
from hopsworks.cli import output, session
from hopsworks_common import util


@click.group("superset")
def superset_group() -> None:
    """Superset commands (datasets, charts, dashboards)."""


# region Datasets ----------------------------------------------------------


@superset_group.group("dataset")
def superset_dataset_group() -> None:
    """Superset dataset subcommands."""


@superset_dataset_group.command("list")
@click.pass_context
def superset_dataset_list(ctx: click.Context) -> None:
    """List every Superset dataset visible to the current user.

    Args:
        ctx: Click context.
    """
    payload = _api(ctx).list_datasets()
    _render(payload, ["id", "table_name", "schema", "database"])


@superset_dataset_group.command("info")
@click.argument("dataset_id", type=int)
@click.pass_context
def superset_dataset_info(ctx: click.Context, dataset_id: int) -> None:
    """Show details for a Superset dataset.

    Args:
        ctx: Click context.
        dataset_id: Superset dataset ID.
    """
    result = _api(ctx).get_dataset(dataset_id)
    output.print_json(result)


@superset_dataset_group.command("create")
@click.option(
    "--database-id",
    "database_id",
    type=int,
    required=True,
    help="Superset database ID.",
)
@click.option(
    "--table-name",
    "table_name",
    required=True,
    help="Table name (physical) or virtual name.",
)
@click.option("--schema", help="Schema the table lives in.")
@click.option("--sql", help="SQL query for a virtual dataset.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def superset_dataset_create(
    ctx: click.Context,
    database_id: int,
    table_name: str,
    schema: str | None,
    sql: str | None,
    description: str,
) -> None:
    """Create a Superset dataset.

    Args:
        ctx: Click context.
        database_id: Superset database ID.
        table_name: Table name.
        schema: Optional schema.
        sql: Optional SQL for a virtual dataset.
        description: Description.
    """
    try:
        result = _api(ctx).create_dataset(
            database_id=database_id,
            table_name=table_name,
            schema=schema,
            sql=sql,
            description=description,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create dataset failed: {exc}") from exc
    output.success("✓ Created Superset dataset (id=%s)", _id(result))


@superset_dataset_group.command("delete")
@click.argument("dataset_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def superset_dataset_delete(ctx: click.Context, dataset_id: int, yes: bool) -> None:
    """Delete a Superset dataset.

    Args:
        ctx: Click context.
        dataset_id: Superset dataset ID.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete Superset dataset {dataset_id}?", abort=True)
    try:
        _api(ctx).delete_dataset(dataset_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted Superset dataset %s", dataset_id)


# region Charts ------------------------------------------------------------


@superset_group.group("chart")
def superset_chart_group() -> None:
    """Superset chart (slice) subcommands."""


@superset_chart_group.command("list")
@click.pass_context
def superset_chart_list(ctx: click.Context) -> None:
    """List every Superset chart visible to the current user.

    Args:
        ctx: Click context.
    """
    payload = _api(ctx).list_charts()
    _render(payload, ["id", "slice_name", "viz_type", "datasource_id"])


@superset_chart_group.command("info")
@click.argument("chart_id", type=int)
@click.pass_context
def superset_chart_info(ctx: click.Context, chart_id: int) -> None:
    """Show details for a Superset chart.

    Args:
        ctx: Click context.
        chart_id: Superset chart ID.
    """
    output.print_json(_api(ctx).get_chart(chart_id))


@superset_chart_group.command("create")
@click.option("--name", "slice_name", required=True, help="Slice (chart) name.")
@click.option(
    "--viz-type", "viz_type", required=True, help="Viz type, e.g. bar, pie, table."
)
@click.option(
    "--datasource-id",
    "datasource_id",
    type=int,
    required=True,
    help="Superset dataset ID.",
)
@click.option("--params", required=True, help="JSON-string of viz parameters.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def superset_chart_create(
    ctx: click.Context,
    slice_name: str,
    viz_type: str,
    datasource_id: int,
    params: str,
    description: str,
) -> None:
    """Create a Superset chart (slice).

    Args:
        ctx: Click context.
        slice_name: Slice name.
        viz_type: Visualization type.
        datasource_id: Dataset the chart reads from.
        params: JSON-string of visualization parameters.
        description: Description.
    """
    try:
        result = _api(ctx).create_chart(
            slice_name=slice_name,
            viz_type=viz_type,
            datasource_id=datasource_id,
            params=params,
            description=description,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create chart failed: {exc}") from exc
    output.success("✓ Created Superset chart (id=%s)", _id(result))


@superset_chart_group.command(
    "generate",
    epilog="""\b
Examples:
  hops superset chart generate --dataset 12 --type xy --x event_time --y 'SUM(amount)' --group-by category --time-grain P1D --name 'Spend per day'
  hops superset chart generate --dataset 12 --type big_number --metric 'COUNT(*)'
  hops superset chart generate --dataset 12 --type table --columns 'category,SUM(amount)' --filter 'country IN SE,NO'
  hops superset chart generate --dataset 12 --config @chart.json   (fields: hops superset chart types TYPE)
""",
)
@click.option(
    "--dataset",
    "dataset_id",
    required=True,
    help="Superset dataset ID or UUID (hops superset dataset list).",
)
@click.option(
    "--type",
    "chart_type",
    help="Chart type: xy, table, pie, big_number, or any of hops superset chart types.",
)
@click.option(
    "--kind",
    type=click.Choice(["line", "bar", "area", "scatter"]),
    help="Kind of xy chart (default line).",
)
@click.option(
    "--x", "x", help="X-axis column of an xy chart; default the dataset's time column."
)
@click.option(
    "--y",
    "y",
    multiple=True,
    callback=lambda _c, _p, v: _split(v),
    help="Metrics of an xy chart, comma-separated: SUM(amount), a saved metric name, or a SQL expression such as COUNT(*).",
)
@click.option(
    "--group-by",
    "group_by",
    multiple=True,
    callback=lambda _c, _p, v: _split(v),
    help="Series breakdown columns of an xy chart, comma-separated.",
)
@click.option(
    "--columns",
    multiple=True,
    callback=lambda _c, _p, v: _split(v),
    help="Columns of a table chart, comma-separated; SUM(amount) aggregates.",
)
@click.option("--metric", help="Metric of a pie or big_number chart, as for --y.")
@click.option("--dimension", help="Slice column of a pie chart.")
@click.option(
    "--time-grain",
    "time_grain",
    help="Time grain of an xy chart: P1D, P1W, P1M, PT1H...",
)
@click.option(
    "--filter",
    "filters",
    multiple=True,
    help="Filter, repeatable: 'country = SE', 'amount > 100', 'region IN EU,US', 'email IS NULL'.",
)
@click.option(
    "--config",
    "config_json",
    help="Full chart config as JSON or @file (hops superset chart types TYPE); the flags above override its fields.",
)
@click.option("--name", help="Chart name; Superset derives one when omitted.")
@click.option(
    "--preview",
    is_flag=True,
    help="Do not save: print an Explore link to the unsaved chart.",
)
@click.pass_context
def superset_chart_generate(
    ctx: click.Context,
    dataset_id: str,
    chart_type: str | None,
    kind: str | None,
    x: str | None,
    y: list[str],
    group_by: list[str],
    columns: list[str],
    metric: str | None,
    dimension: str | None,
    time_grain: str | None,
    filters: tuple[str, ...],
    config_json: str | None,
    name: str | None,
    preview: bool,
) -> None:
    """Create a Superset chart from a typed config, which Superset validates against the dataset.

    Args:
        ctx: Click context.
        dataset_id: Superset dataset ID or UUID.
        chart_type: Chart type.
        kind: xy chart kind.
        x: X-axis column.
        y: xy metrics.
        group_by: xy series columns.
        columns: Table columns.
        metric: Pie or big_number metric.
        dimension: Pie slice column.
        time_grain: xy time grain.
        filters: Filter expressions.
        config_json: Full config as JSON or @file.
        name: Chart name.
        preview: Leave the chart unsaved.
    """
    config = _chart_config(
        config_json=config_json,
        chart_type=chart_type,
        kind=kind,
        x=x,
        y=y,
        group_by=group_by,
        columns=columns,
        metric=metric,
        dimension=dimension,
        time_grain=time_grain,
        filters=filters,
    )
    dataset: int | str = int(dataset_id) if dataset_id.isdigit() else dataset_id
    try:
        result = _api(ctx).generate_chart(dataset, config, name=name, save=not preview)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Generate chart failed: {exc}") from exc
    chart = result.get("chart") or {}
    if preview:
        url = _superset_link(f"explore/?form_data_key={result.get('form_data_key')}")
    else:
        url = _superset_link(f"explore/?slice_id={chart.get('id')}")
    if output.JSON_MODE:
        output.print_json({**result, "hopsworks_url": url})
        return
    for warning in result.get("warnings") or []:
        click.echo(f"warning: {warning}", err=True)
    if preview:
        output.success("✓ Unsaved chart: %s", url)
    else:
        output.success(
            '✓ Created Superset chart %s "%s": %s',
            chart.get("id"),
            chart.get("slice_name"),
            url,
        )


@superset_chart_group.command("types")
@click.argument("chart_type", required=False)
@click.pass_context
def superset_chart_types(ctx: click.Context, chart_type: str | None) -> None:
    """List the chart types generate takes, or print one type's config schema with examples.

    Args:
        ctx: Click context.
        chart_type: Chart type to describe.
    """
    if chart_type is None:
        if output.JSON_MODE:
            output.print_json(list(_CHART_TYPES))
            return
        output.print_table(["TYPE", "FIELDS"], [list(t) for t in _CHART_TYPES.items()])
        return
    try:
        output.print_json(_api(ctx).chart_type_schema(chart_type))
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Chart type {chart_type}: {exc}") from exc


@superset_chart_group.command("delete")
@click.argument("chart_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def superset_chart_delete(ctx: click.Context, chart_id: int, yes: bool) -> None:
    """Delete a Superset chart.

    Args:
        ctx: Click context.
        chart_id: Superset chart ID.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete Superset chart {chart_id}?", abort=True)
    try:
        _api(ctx).delete_chart(chart_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted Superset chart %s", chart_id)


# region Dashboards --------------------------------------------------------


@superset_group.group("dashboard")
def superset_dashboard_group() -> None:
    """Superset dashboard subcommands."""


@superset_dashboard_group.command("list")
@click.pass_context
def superset_dashboard_list(ctx: click.Context) -> None:
    """List every Superset dashboard visible to the current user.

    Args:
        ctx: Click context.
    """
    payload = _api(ctx).list_dashboards()
    _render(payload, ["id", "dashboard_title", "published", "slug"])


@superset_dashboard_group.command("info")
@click.argument("dashboard_id", type=int)
@click.pass_context
def superset_dashboard_info(ctx: click.Context, dashboard_id: int) -> None:
    """Show details for a Superset dashboard.

    Args:
        ctx: Click context.
        dashboard_id: Superset dashboard ID.
    """
    output.print_json(_api(ctx).get_dashboard(dashboard_id))


@superset_dashboard_group.command("create")
@click.argument("title")
@click.option("--published/--draft", default=False, help="Publish immediately.")
@click.option("--slug", help="Optional URL slug.")
@click.pass_context
def superset_dashboard_create(
    ctx: click.Context, title: str, published: bool, slug: str | None
) -> None:
    """Create a Superset dashboard.

    Args:
        ctx: Click context.
        title: Dashboard title.
        published: Publish immediately.
        slug: Optional URL slug.
    """
    try:
        result = _api(ctx).create_dashboard(
            dashboard_title=title, published=published, slug=slug
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create dashboard failed: {exc}") from exc
    output.success("✓ Created Superset dashboard %s (id=%s)", title, _id(result))


@superset_dashboard_group.command(
    "update",
    epilog="""\b
Examples:
  hops superset dashboard update 7 --title 'Fraud overview' --published --refresh 300
  hops superset dashboard update 7 --metadata '{"color_scheme": "supersetColors"}'
  hops superset dashboard update 7 --css @print.css
""",
)
@click.argument("dashboard_id")
@click.option("--title", help="Dashboard title.")
@click.option("--description", help="Dashboard description.")
@click.option("--slug", help="URL slug.")
@click.option("--published/--draft", default=None, help="Publish or unpublish.")
@click.option("--css", help="Custom CSS, as text or @file.")
@click.option(
    "--refresh",
    "refresh_frequency",
    type=int,
    help="Auto-refresh interval in seconds; 0 turns it off.",
)
@click.option(
    "--cross-filters/--no-cross-filters",
    default=None,
    help="Turn cross filtering on or off.",
)
@click.option(
    "--filter-bar",
    "filter_bar_orientation",
    type=click.Choice(["vertical", "horizontal"]),
    help="Native filter bar orientation.",
)
@click.option(
    "--tags",
    callback=lambda _c, _p, v: [int(t) for t in v.split(",")] if v else None,
    help="Replace the dashboard's tags with these tag IDs, comma-separated.",
)
@click.option(
    "--metadata",
    "metadata_json",
    help="json_metadata keys to set, as JSON or @file; merged into the existing metadata.",
)
@click.option(
    "--position",
    "position_json",
    help="Complete layout (position_json) as JSON or @file; replaces the current one.",
)
@click.pass_context
def superset_dashboard_update(
    ctx: click.Context,
    dashboard_id: str,
    title: str | None,
    description: str | None,
    slug: str | None,
    published: bool | None,
    css: str | None,
    refresh_frequency: int | None,
    cross_filters: bool | None,
    filter_bar_orientation: str | None,
    tags: list[int] | None,
    metadata_json: str | None,
    position_json: str | None,
) -> None:
    """Change only the given fields of a Superset dashboard; everything else is left as it is.

    Args:
        ctx: Click context.
        dashboard_id: Dashboard ID or slug.
        title: Dashboard title.
        description: Dashboard description.
        slug: URL slug.
        published: Publish or unpublish.
        css: Custom CSS or @file.
        refresh_frequency: Refresh interval in seconds.
        cross_filters: Cross filtering on or off.
        filter_bar_orientation: Filter bar orientation.
        tags: Tag IDs.
        metadata_json: json_metadata overrides as JSON or @file.
        position_json: Layout as JSON or @file.
    """
    fields: dict[str, Any] = {
        "dashboard_title": title,
        "description": description,
        "slug": slug,
        "published": published,
        "css": _text_arg(css),
        "refresh_frequency": refresh_frequency,
        "cross_filters_enabled": cross_filters,
        "filter_bar_orientation": filter_bar_orientation.upper()
        if filter_bar_orientation
        else None,
        "tags": tags,
        "json_metadata_overrides": _json_arg(metadata_json, "--metadata"),
        "position_json": _json_arg(position_json, "--position"),
    }
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        raise click.UsageError("Nothing to update: pass at least one option.")
    identifier: int | str = (
        int(dashboard_id) if dashboard_id.isdigit() else dashboard_id
    )
    try:
        result = _api(ctx).patch_dashboard(identifier, **fields)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Update dashboard failed: {exc}") from exc
    dashboard = result.get("dashboard") or {}
    url = _superset_link(f"dashboard/{dashboard.get('id', dashboard_id)}/")
    if output.JSON_MODE:
        output.print_json({**result, "hopsworks_url": url})
        return
    output.success(
        "✓ Updated Superset dashboard %s (%s): %s",
        dashboard.get("id", dashboard_id),
        ", ".join(sorted(fields)),
        url,
    )


@superset_dashboard_group.command("delete")
@click.argument("dashboard_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def superset_dashboard_delete(ctx: click.Context, dashboard_id: int, yes: bool) -> None:
    """Delete a Superset dashboard.

    Args:
        ctx: Click context.
        dashboard_id: Superset dashboard ID.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete Superset dashboard {dashboard_id}?", abort=True)
    try:
        _api(ctx).delete_dashboard(dashboard_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted Superset dashboard %s", dashboard_id)


# region Helpers -----------------------------------------------------------


def _api(ctx: click.Context) -> Any:
    project = session.get_project(ctx)
    from hopsworks_common import client

    # The SDK reaches Superset over in-cluster service DNS, so from outside it
    # fails on the first request with a traceback; say so before trying.
    if client._is_external():
        raise click.ClickException(
            "Superset is reachable only inside the cluster: run this in a "
            "Hopsworks terminal, or as a Hopsworks job from an external client."
        )
    return project.get_superset_api()


def _items(payload: Any) -> list[dict[str, Any]]:
    """Normalize Superset's response envelope to a plain list of items.

    Superset wraps list responses in ``{"result": [...], "count": ...}``; we
    strip that so the table renderer gets a flat list.

    Args:
        payload: Raw response dict.

    Returns:
        The list of items, or an empty list when the payload is unexpected.
    """
    if isinstance(payload, dict):
        for key in ("result", "items"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
    if isinstance(payload, list):
        return payload
    return []


def _render(payload: Any, columns: list[str]) -> None:
    items = _items(payload)
    if output.JSON_MODE:
        output.print_json(items)
        return
    rows = [[str(item.get(col, "")) for col in columns] for item in items]
    output.print_table([c.upper() for c in columns], rows)


def _id(payload: Any) -> Any:
    if isinstance(payload, dict):
        return payload.get("id") or payload.get("result", {}).get("id", "?")
    return "?"


# The core chart types of Superset 7.0's generate_chart and the fields each requires.
_CHART_TYPES = {
    "xy": "y; optional x, kind, group_by, time_grain",
    "table": "columns",
    "pie": "dimension, metric",
    "big_number": "metric",
    "pivot_table": "rows, metrics",
    "mixed_timeseries": "x, y, y_secondary",
    "gauge": "metric",
    "treemap_v2": "groupby, metric",
    "histogram": "column",
    "box_plot": "metrics, distribute_across",
    "waterfall": "see hops superset chart types waterfall",
    "handlebars": "handlebars_template",
}

_AGGREGATE = re.compile(
    r"^\s*(SUM|COUNT|AVG|MIN|MAX|COUNT_DISTINCT|MEDIAN)\s*\(\s*([A-Za-z_][\w.]*)\s*\)\s*$",
    re.IGNORECASE,
)
_FILTER = re.compile(
    r"^\s*(\S+)\s+(IS NOT NULL|IS NULL|NOT IN|IN|NOT LIKE|ILIKE|LIKE|>=|<=|!=|=|>|<)\s*(.*?)\s*$",
    re.IGNORECASE,
)


def _split(values: tuple[str, ...]) -> list[str]:
    """Comma-separated items, splitting only on commas outside parentheses and quotes."""
    return [part for v in values for part in _comma_items(v)]


def _comma_items(text: str) -> list[str]:
    items, current, depth, quote = [], [], 0, None
    for ch in text:
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)
        elif ch == "," and depth == 0:
            items.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    items.append("".join(current).strip())
    return [item for item in items if item]


def _metric(text: str) -> dict[str, Any]:
    """SUM(col) aggregates a column, a bare name is a saved metric, anything else is a SQL expression."""
    match = _AGGREGATE.match(text)
    if match:
        return {"name": match.group(2), "aggregate": match.group(1).upper()}
    if re.fullmatch(r"[A-Za-z_]\w*", text):
        return {"name": text, "saved_metric": True}
    return {"sql_expression": text, "label": text}


def _column(text: str) -> dict[str, Any]:
    """A plain column, or an aggregate when written as SUM(col)."""
    match = _AGGREGATE.match(text)
    if match:
        return {"name": match.group(2), "aggregate": match.group(1).upper()}
    return {"name": text}


def _filter(text: str) -> dict[str, Any]:
    match = _FILTER.match(text)
    if not match:
        raise click.BadParameter(
            f"{text!r}: expected 'column op value', e.g. 'country = SE'",
            param_hint="--filter",
        )
    column, op, raw = match.group(1), match.group(2).upper(), match.group(3)
    if op in ("IS NULL", "IS NOT NULL"):
        return {"column": column, "op": op}
    if op in ("IN", "NOT IN"):
        return {
            "column": column,
            "op": op,
            "value": [_scalar(v) for v in _comma_items(raw)],
        }
    return {"column": column, "op": op, "value": _scalar(raw)}


def _scalar(text: str) -> str | int | float:
    """A quoted value is a string as written; an unquoted one a number when it reads as one."""
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return text[1:-1]
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            pass
    return text


def _json_arg(value: str | None, option: str) -> Any:
    """Parse JSON given inline or as @file."""
    text = _text_arg(value)
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"not valid JSON: {exc}", param_hint=option) from exc


def _text_arg(value: str | None) -> str | None:
    if value is not None and value.startswith("@"):
        return pathlib.Path(value[1:]).read_text()
    return value


def _chart_config(
    config_json: str | None,
    chart_type: str | None,
    kind: str | None,
    x: str | None,
    y: list[str],
    group_by: list[str],
    columns: list[str],
    metric: str | None,
    dimension: str | None,
    time_grain: str | None,
    filters: tuple[str, ...],
) -> dict[str, Any]:
    config: dict[str, Any] = _json_arg(config_json, "--config") or {}
    flags: dict[str, Any] = {
        "chart_type": chart_type,
        "kind": kind,
        "x": _column(x) if x else None,
        "y": [_metric(m) for m in y] or None,
        "group_by": [_column(c) for c in group_by] or None,
        "columns": [_column(c) for c in columns] or None,
        "metric": _metric(metric) if metric else None,
        "dimension": _column(dimension) if dimension else None,
        "time_grain": time_grain,
        "filters": [_filter(f) for f in filters] or None,
    }
    config.update({k: v for k, v in flags.items() if v is not None})
    if not config.get("chart_type"):
        raise click.UsageError("Pass --type, or a --config with chart_type.")
    return config


def _superset_link(path: str) -> str | None:
    """A link to `path` under Superset, through the Hopsworks address users open; None where it is unknown."""
    try:
        return util._get_hostname_replaced_url(f"hopsworks-api/superset/{path}")
    except Exception:  # noqa: BLE001
        return None
