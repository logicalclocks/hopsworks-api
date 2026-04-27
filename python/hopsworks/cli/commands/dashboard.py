"""``hops dashboard`` — CRUD for Hopsworks internal dashboards.

These are Hopsworks native dashboards that host ``hops chart`` charts in a
grid layout. Distinct from Superset dashboards (``hops superset dashboard``).
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


@click.group("dashboard")
def dashboard_group() -> None:
    """Hopsworks native dashboard commands."""


@dashboard_group.command("list")
@click.pass_context
def dashboard_list(ctx: click.Context) -> None:
    """List every dashboard in the active project.

    Args:
        ctx: Click context.
    """
    api = _api(ctx)
    try:
        dashboards = api.list_dashboards()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list dashboards: {exc}") from exc
    rows = [
        [
            d.get("id", "?"),
            d.get("name", "?"),
            len(d.get("charts") or []),
        ]
        for d in dashboards
    ]
    output.print_table(["ID", "NAME", "CHARTS"], rows)


@dashboard_group.command("info")
@click.argument("dashboard_id", type=int)
@click.pass_context
def dashboard_info(ctx: click.Context, dashboard_id: int) -> None:
    """Show details and member charts for a dashboard.

    Args:
        ctx: Click context.
        dashboard_id: Dashboard ID.
    """
    api = _api(ctx)
    try:
        dashboard = api.get_dashboard(dashboard_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Dashboard {dashboard_id} not found: {exc}"
        ) from exc
    if output.JSON_MODE:
        output.print_json(dashboard)
        return
    rows = [
        ["ID", dashboard.get("id", "?")],
        ["Name", dashboard.get("name", "?")],
        ["Charts", len(dashboard.get("charts") or [])],
    ]
    output.print_table(["FIELD", "VALUE"], rows)
    charts = dashboard.get("charts") or []
    if charts:
        output.info("")
        chart_rows = [
            [c.get("id", "?"), c.get("title", "?"), c.get("url", "")] for c in charts
        ]
        output.print_table(["CHART ID", "TITLE", "URL"], chart_rows)


@dashboard_group.command("create")
@click.argument("name")
@click.pass_context
def dashboard_create(ctx: click.Context, name: str) -> None:
    """Create a new empty dashboard.

    Args:
        ctx: Click context.
        name: Dashboard name.
    """
    api = _api(ctx)
    try:
        dashboard = api.create_dashboard(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create failed: {exc}") from exc
    output.success("✓ Created dashboard %s (id=%s)", name, dashboard.get("id", "?"))


@dashboard_group.command("delete")
@click.argument("dashboard_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def dashboard_delete(ctx: click.Context, dashboard_id: int, yes: bool) -> None:
    """Delete a dashboard by ID.

    Args:
        ctx: Click context.
        dashboard_id: Dashboard ID.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete dashboard {dashboard_id}?", abort=True)
    api = _api(ctx)
    try:
        api.delete_dashboard(dashboard_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted dashboard %s", dashboard_id)


@dashboard_group.command("add-chart")
@click.argument("dashboard_id", type=int)
@click.option(
    "--chart", "chart_id", type=int, required=True, help="Chart ID to attach."
)
@click.pass_context
def dashboard_add_chart(ctx: click.Context, dashboard_id: int, chart_id: int) -> None:
    """Attach a chart to a dashboard.

    Args:
        ctx: Click context.
        dashboard_id: Dashboard ID.
        chart_id: Chart ID to attach.
    """
    api = _api(ctx)
    try:
        api.add_chart(dashboard_id, chart_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Attach failed: {exc}") from exc
    output.success("✓ Added chart %s to dashboard %s", chart_id, dashboard_id)


@dashboard_group.command("remove-chart")
@click.argument("dashboard_id", type=int)
@click.option(
    "--chart", "chart_id", type=int, required=True, help="Chart ID to detach."
)
@click.pass_context
def dashboard_remove_chart(
    ctx: click.Context, dashboard_id: int, chart_id: int
) -> None:
    """Detach a chart from a dashboard.

    Args:
        ctx: Click context.
        dashboard_id: Dashboard ID.
        chart_id: Chart ID to detach.
    """
    api = _api(ctx)
    try:
        api.remove_chart(dashboard_id, chart_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Detach failed: {exc}") from exc
    output.success("✓ Removed chart %s from dashboard %s", chart_id, dashboard_id)


def _api(ctx: click.Context) -> Any:
    project = session.get_project(ctx)
    return project.get_dashboard_api()
