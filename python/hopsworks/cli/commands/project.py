"""``hops project`` — list available projects, switch the active one, and show its cost."""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import auth, config, output, session


@click.group("project")
def project_group() -> None:
    """List and switch Hopsworks projects."""


@project_group.command("list")
@click.pass_context
def project_list(ctx: click.Context) -> None:
    """Show every project the current user is a member of.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    cfg = _require_auth(ctx)
    auth.login(host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project)
    from hopsworks_common.core import project_api  # noqa: PLC0415

    # One request: the team list already carries each project's id and name,
    # where the SDK's _get_projects() would fetch every project again by name.
    teams = project_api.ProjectApi()._get_project_teams() or []
    rows = []
    for team in sorted(
        teams, key=lambda t: t.get("project", {}).get("name", "").lower()
    ):
        project = team.get("project", {})
        active = "*" if project.get("name") == cfg.project else ""
        rows.append(
            [
                project.get("id", "?"),
                project.get("name", "?"),
                team.get("teamRole", ""),
                active,
            ]
        )
    output.print_table(["ID", "NAME", "ROLE", "ACTIVE"], rows)


@project_group.command("use")
@click.argument("name")
@click.pass_context
def project_use(ctx: click.Context, name: str) -> None:
    """Switch the default project to ``name`` and cache its identifiers.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        name: Project name to activate.
    """
    cfg = _require_auth(ctx)
    try:
        project = auth.login(
            host=cfg.host or "", api_key_value=cfg.api_key, project=name
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not switch to project '{name}': {exc}"
        ) from exc

    cfg.project = getattr(project, "name", name)
    cfg.project_id = getattr(project, "id", None)
    cfg.feature_store_id = _feature_store_id(project)
    config.save(cfg)
    output.success("✓ Active project set to %s", cfg.project)


def _feature_store_id(project: Any) -> int | None:
    """The project's feature store id, without building a feature store object.

    ``project.get_feature_store()`` imports hsfs, and with it pandas, pyarrow and
    the engine: close to three seconds on a laptop, spent on a number the REST
    response already carries and that only ``hops project info`` ever prints.
    ``None`` when the project has no feature store.
    """
    from hopsworks_common import (  # noqa: PLC0415 - keeps `hops --help` light
        client,
        util,
    )

    _client = client._get_instance()
    store = util._append_feature_store_suffix(getattr(project, "name", "") or "")
    try:
        raw = _client._send_request(
            "GET", ["project", _client._project_id, "featurestores", store]
        )
    except Exception:  # noqa: BLE001 - a project need not have a feature store
        return None
    return raw.get("featurestoreId")


@project_group.command("info")
@click.pass_context
def project_info(ctx: click.Context) -> None:
    """Print details about the currently active project.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    cfg = _require_auth(ctx)
    project = auth.login(
        host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project
    )
    rows = [
        ["Host", cfg.host or "?"],
        ["Project", getattr(project, "name", cfg.project or "?")],
        ["Project ID", str(getattr(project, "id", cfg.project_id or "?"))],
        ["Feature store ID", str(cfg.feature_store_id or "?")],
        ["Mode", cfg.mode()],
    ]
    rows.extend(_compute_quota_rows())
    output.print_table(["FIELD", "VALUE"], rows)


def _compute_quota_rows() -> list[list[str]]:
    """Rows for the compute the project may use: used and limit per resource.

    A quota Hopsworks could not read is shown as unavailable, never as a value.
    """
    from hopsworks_common.core import rest  # noqa: PLC0415

    try:
        quota = rest._send_request("GET", rest._project_path("kube", "quota"))
    except Exception:  # noqa: BLE001 - the rest of the info still prints
        return [["Compute quota", "unavailable"]]
    method = quota.get("method") or "NONE"
    if method == "NONE":
        return [["Compute quota", "none"]]
    source = quota.get("source")
    rows = [["Compute quota", f"{method} ({source})" if source else method]]
    for label, key, scale, unit in (
        ("CPU", "cpu", 1, " cores"),
        ("Memory", "memory", 1024**3, " GiB"),
        ("GPU", "gpu", 1, ""),
    ):
        limit = quota.get(key)
        if limit and limit.get("limit") is not None:
            used = (limit.get("used") or 0) / scale
            text = f"{used:g} of {limit['limit'] / scale:g}{unit} used"
            if limit.get("canBorrow"):
                text += f", {limit['canBorrow'] / scale:g} more can be borrowed"
            rows.append([label, text])
    for warning in quota.get("warnings") or []:
        rows.append(["Warning", str(warning)])
    return rows


@project_group.command("costs")
@click.option(
    "--window",
    type=click.Choice(("hour", "day", "week", "month", "year"), case_sensitive=False),
    default="week",
    show_default=True,
    help="Time window ending now.",
)
@click.option(
    "--project",
    "project_name",
    default=None,
    help="Project to show; the active project when not given.",
)
@click.pass_context
def project_costs(ctx: click.Context, window: str, project_name: str | None) -> None:
    """Show the Kubernetes cost of a project over a recent window.

    Only the project's data owners can see it. Costs come from OpenCost; week,
    month and year also read the daily history Hopsworks has recorded so far.
    When some buckets of the window have no data, a coverage line says how
    many do; missing buckets are unknown cost, not zero.

    Args:
        ctx: Click context.
        window: One of hour, day, week, month, year.
        project_name: Project to show, or None for the active project.
    """
    import hopsworks
    from hopsworks.cli.commands.admin import print_costs

    session.get_project(ctx)
    try:
        costs = hopsworks.get_costs_api().get_project_costs(
            window, project=project_name
        )
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not get project costs: {exc}") from exc
    print_costs(costs, "namespace", window, show_idle=False)


def _require_auth(ctx: click.Context) -> config.HopsConfig:
    return session.require_auth(ctx)
