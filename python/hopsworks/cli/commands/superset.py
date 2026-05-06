"""``hops superset`` — manage Superset datasets, charts, and dashboards.

Superset in Hopsworks is exposed via ``project.get_superset_api()``. That
module handles Superset's CSRF dance and session tokens for us, so the CLI
just wraps each method in a Click command.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


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
