"""``hops admin`` — platform administration commands for ``HOPS_ADMIN`` accounts.

``hops admin costs`` wraps ``hopsworks.get_costs_api()``: the Kubernetes cost of
each namespace (one per project) over a recent window, computed by OpenCost.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session
from hopsworks_common.client.exceptions import RestAPIError


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


def _efficiency(row: Any) -> str:
    # OpenCost has no efficiency for capacity nobody requested.
    if row.kind != "NAMESPACE":
        return "-"
    if row.cpu_no_requests and row.ram_no_requests:
        return "No requests"
    return _percent(row.total_efficiency)


def _table_row(row: Any) -> list[str]:
    return [
        row.namespace or "",
        _money(row.cpu_cost),
        _money(row.gpu_cost),
        _money(row.ram_cost),
        _money(row.pv_cost),
        _money(row.network_cost),
        _money(row.other_cost),
        _money(row.total_cost),
        _percent(row.share),
        _efficiency(row),
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
    When some buckets of the window have no data, a coverage line says how
    many do; missing buckets are unknown cost, not zero.

    Args:
        ctx: Click context.
        window: One of hour, day, week, month, year.
    """
    import hopsworks

    session.get_project(ctx)
    try:
        costs = hopsworks.get_costs_api().get_namespace_costs(window)
    except RestAPIError as exc:
        reason = exc.user_message or exc
        raise click.ClickException(f"Could not get namespace costs: {reason}") from exc
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not get namespace costs: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(costs.to_dict())
        return
    rows = sorted(costs.items, key=lambda r: r.total_cost or 0.0, reverse=True)
    output.print_table(_HEADERS, [_table_row(r) for r in rows])
    coverage = costs.coverage
    if coverage.observed_buckets < coverage.total_buckets:
        starting = (
            f", starting {coverage.first_observed}" if coverage.first_observed else ""
        )
        click.echo(
            f"Data for {coverage.observed_buckets} of {coverage.total_buckets} "
            f"buckets{starting}"
        )
    click.echo(
        f"Total {_money(costs.total_cost)} {costs.currency or ''}, "
        f"idle {_money(costs.idle_cost)}, "
        f"efficiency {_percent(costs.efficiency)}, "
        f"{(costs.window or window).lower()} from {costs.start} to {costs.end} (UTC)"
    )
