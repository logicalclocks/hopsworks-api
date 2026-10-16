"""``hops admin`` — platform administration commands for ``HOPS_ADMIN`` accounts.

``hops admin costs`` wraps ``hopsworks.get_costs_api()``: the Kubernetes cost of
each namespace (one per project) over a recent window, computed by OpenCost.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


_WINDOWS = ("hour", "day", "week", "month", "year")
_HEADERS = [
    "NAMESPACE",
    "CPU",
    "GPU",
    "RAM",
    "STORAGE",
    "NETWORK",
    "OTHER",
    "TOTAL",
    "SHARE",
    "EFFICIENCY",
]


@click.group("admin")
def admin_group() -> None:
    """Platform administration commands (needs the HOPS_ADMIN role)."""


def _money(value: float | None) -> str:
    return f"{value or 0.0:.2f}"


def _percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.0%}"


def _table_row(row: Any, total: float) -> list[str]:
    share = row.total_cost / total if total else None
    # OpenCost has no efficiency for capacity nobody requested.
    efficiency = row.total_efficiency if row.kind == "NAMESPACE" else None
    return [
        row.namespace or "",
        _money(row.cpu_cost),
        _money(row.gpu_cost),
        _money(row.ram_cost),
        _money(row.pv_cost),
        _money(row.network_cost),
        _money(row.other_cost),
        _money(row.total_cost),
        _percent(share),
        _percent(efficiency),
    ]


@admin_group.command("costs")
@click.option(
    "--window",
    type=click.Choice(_WINDOWS, case_sensitive=False),
    default="week",
    show_default=True,
    help="Time window ending now.",
)
@click.pass_context
def admin_costs(ctx: click.Context, window: str) -> None:
    """Show the Kubernetes cost of each namespace over a recent window.

    Each project runs in its own namespace. Costs come from OpenCost; month
    and year are assembled from daily history Hopsworks has recorded so far.

    Args:
        ctx: Click context.
        window: One of hour, day, week, month, year.
    """
    import hopsworks

    session.get_project(ctx)
    try:
        costs = hopsworks.get_costs_api().get_namespace_costs(window)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not get namespace costs: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(costs.to_dict())
        return
    rows = sorted(costs.items, key=lambda r: r.total_cost or 0.0, reverse=True)
    output.print_table(_HEADERS, [_table_row(r, costs.total_cost) for r in rows])
    click.echo(
        f"Total {_money(costs.total_cost)} {costs.currency or ''}, "
        f"idle {_money(costs.idle_cost)}, "
        f"efficiency {_percent(costs.efficiency)}, "
        f"{(costs.window or window).lower()} from {costs.start} to {costs.end} (UTC)"
    )
