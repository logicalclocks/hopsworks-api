"""``hops chart`` — CRUD for Hopsworks internal charts.

These are the native Hopsworks charts attached to projects via
``/project/{id}/charts`` — distinct from Superset charts (``hops superset``).
The backend stores chart layout (width/height/x/y) which callers supply when
creating new charts.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


@click.group("chart")
def chart_group() -> None:
    """Hopsworks native chart commands."""


@chart_group.command("list")
@click.pass_context
def chart_list(ctx: click.Context) -> None:
    """List every chart in the active project.

    Args:
        ctx: Click context.
    """
    api = _api(ctx)
    try:
        charts = api.list_charts()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list charts: {exc}") from exc
    rows = [
        [
            c.get("id", "?"),
            c.get("title", "?"),
            output.first_line(c.get("description"), empty=""),
            c.get("url", ""),
        ]
        for c in charts
    ]
    output.print_table(["ID", "TITLE", "DESCRIPTION", "URL"], rows)


@chart_group.command("info")
@click.argument("chart_id", type=int)
@click.pass_context
def chart_info(ctx: click.Context, chart_id: int) -> None:
    """Show details for a single chart.

    Args:
        ctx: Click context.
        chart_id: Chart ID.
    """
    api = _api(ctx)
    try:
        chart = api.get_chart(chart_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Chart {chart_id} not found: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(chart)
        return
    rows = [[k, str(v)] for k, v in chart.items()]
    output.print_table(["FIELD", "VALUE"], rows)


@chart_group.command("create")
@click.argument("title")
@click.option(
    "--url", required=True, help="URL to render (e.g. Plotly HTML in HopsFS)."
)
@click.option("--description", default="", help="Free-form description.")
@click.option("--width", type=int, default=6, show_default=True, help="Grid width.")
@click.option("--height", type=int, default=4, show_default=True, help="Grid height.")
@click.option("--x", type=int, default=0, show_default=True, help="Grid x position.")
@click.option("--y", type=int, default=0, show_default=True, help="Grid y position.")
@click.option("--job", "job_name", help="Optional Hopsworks job to associate.")
@click.pass_context
def chart_create(
    ctx: click.Context,
    title: str,
    url: str,
    description: str,
    width: int,
    height: int,
    x: int,
    y: int,
    job_name: str | None,
) -> None:
    """Create a new chart.

    Args:
        ctx: Click context.
        title: Chart title.
        url: URL to render.
        description: Description.
        width: Grid width.
        height: Grid height.
        x: Grid x.
        y: Grid y.
        job_name: Optional job association.
    """
    api = _api(ctx)
    try:
        chart = api.create_chart(
            title=title,
            url=url,
            description=description,
            width=width,
            height=height,
            x=x,
            y=y,
            job_name=job_name,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create failed: {exc}") from exc
    output.success("✓ Created chart %s (id=%s)", title, chart.get("id", "?"))


@chart_group.command("update")
@click.argument("chart_id", type=int)
@click.option("--title", help="New title.")
@click.option("--url", help="New URL.")
@click.option("--description", help="New description.")
@click.option("--width", type=int, help="New grid width.")
@click.option("--height", type=int, help="New grid height.")
@click.option("--x", type=int, help="New grid x position.")
@click.option("--y", type=int, help="New grid y position.")
@click.pass_context
def chart_update(
    ctx: click.Context,
    chart_id: int,
    title: str | None,
    url: str | None,
    description: str | None,
    width: int | None,
    height: int | None,
    x: int | None,
    y: int | None,
) -> None:
    """Update selected fields of a chart.

    Args:
        ctx: Click context.
        chart_id: Chart ID.
        title: New title.
        url: New URL.
        description: New description.
        width: New width.
        height: New height.
        x: New x.
        y: New y.
    """
    api = _api(ctx)
    patch: dict[str, Any] = {
        k: v
        for k, v in {
            "title": title,
            "url": url,
            "description": description,
            "width": width,
            "height": height,
            "x": x,
            "y": y,
        }.items()
        if v is not None
    }
    if not patch:
        raise click.UsageError("At least one field must be provided.")
    try:
        api.update_chart(chart_id, **patch)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Update failed: {exc}") from exc
    output.success("✓ Updated chart %s", chart_id)


@chart_group.command("delete")
@click.argument("chart_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def chart_delete(ctx: click.Context, chart_id: int, yes: bool) -> None:
    """Delete a chart by ID.

    Args:
        ctx: Click context.
        chart_id: Chart ID.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete chart {chart_id}?", abort=True)
    api = _api(ctx)
    try:
        api.delete_chart(chart_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted chart %s", chart_id)


def _api(ctx: click.Context) -> Any:
    project = session.get_project(ctx)
    return project.get_chart_api()
