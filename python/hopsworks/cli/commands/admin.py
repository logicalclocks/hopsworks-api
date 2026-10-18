"""``hops admin`` — platform administration commands for ``HOPS_ADMIN`` accounts.

``hops admin costs`` wraps ``hopsworks.get_costs_api()``: the Kubernetes cost of
each namespace (one per project), or of each department, over a recent window,
computed by OpenCost.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session
from hopsworks_common.client.exceptions import RestAPIError


_WINDOWS = ("hour", "day", "week", "month", "year")
_VIEWS = ("namespace", "department")
_WORKLOAD_KINDS = ("NAMESPACE", "DEPARTMENT", "UNASSIGNED")
_COST_HEADERS = [
    "CPU",
    "GPU",
    "RAM",
    "STORAGE",
    "NETWORK",
    "OTHER",
    "TOTAL",
    "GPU WORKLOADS",
    "CPU WORKLOADS",
    "SHARE",
    "EFFICIENCY",
]
_HEADERS = {
    "namespace": ["NAMESPACE", "DEPARTMENT", *_COST_HEADERS],
    "department": ["DEPARTMENT", "NAMESPACES", *_COST_HEADERS],
}


@click.group("admin")
def admin_group() -> None:
    """Platform administration commands (needs the HOPS_ADMIN role)."""


def _money(value: float | None) -> str:
    return f"{value or 0.0:.2f}"


def _percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.0%}"


def _efficiency(row: Any) -> str:
    # OpenCost has no efficiency for capacity nobody requested.
    if row.kind not in _WORKLOAD_KINDS:
        return "-"
    if row.cpu_no_requests and row.ram_no_requests:
        return "No requests"
    return _percent(row.total_efficiency)


def _workload_money(row: Any, value: float) -> str:
    return _money(value) if row.kind in _WORKLOAD_KINDS else "-"


def _label_columns(row: Any, by: str) -> list[str]:
    if by == "namespace":
        return [row.namespace or "", row.department_name or "-"]
    if row.kind == "DEPARTMENT":
        label = row.department_name or str(row.department_id)
    elif row.kind == "UNASSIGNED":
        label = "Not a project"
    else:
        label = row.namespace or ""
    count = "-" if row.namespace_count is None else str(row.namespace_count)
    return [label, count]


def _table_row(row: Any, by: str) -> list[str]:
    return [
        *_label_columns(row, by),
        _money(row.cpu_cost),
        _money(row.gpu_cost),
        _money(row.ram_cost),
        _money(row.pv_cost),
        _money(row.network_cost),
        _money(row.other_cost),
        _money(row.total_cost),
        _workload_money(row, row.gpu_workload_cost),
        _workload_money(row, row.cpu_workload_cost),
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
@click.option(
    "--by",
    type=click.Choice(_VIEWS, case_sensitive=False),
    default="namespace",
    show_default=True,
    help="One row per namespace, or per department.",
)
@click.pass_context
def admin_costs(ctx: click.Context, window: str, by: str) -> None:
    """Show the Kubernetes cost of each namespace or department over a recent window.

    Each project runs in its own namespace. Costs come from OpenCost; week,
    month and year also read the daily history Hopsworks has recorded so far.
    When some buckets of the window have no data, a coverage line says how
    many do; missing buckets are unknown cost, not zero.

    --by department sums each department's namespaces into one row, counting
    projects in their current department. Namespaces that are no project's,
    or are shared by projects of different departments, show as one
    "Not a project" row.

    Args:
        ctx: Click context.
        window: One of hour, day, week, month, year.
        by: One of namespace, department.
    """
    import hopsworks

    session.get_project(ctx)
    try:
        api = hopsworks.get_costs_api()
        if by == "department":
            costs = api.get_department_costs(window)
        else:
            costs = api.get_namespace_costs(window)
    except RestAPIError as exc:
        reason = exc.user_message or exc
        raise click.ClickException(f"Could not get {by} costs: {reason}") from exc
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not get {by} costs: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(costs.to_dict())
        return
    rows = sorted(costs.items, key=lambda r: r.total_cost or 0.0, reverse=True)
    output.print_table(_HEADERS[by], [_table_row(r, by) for r in rows])
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
    click.echo(
        f"GPU workloads: {_money(costs.gpu_workload_cost)} | "
        f"CPU workloads: {_money(costs.cpu_workload_cost)}"
    )
