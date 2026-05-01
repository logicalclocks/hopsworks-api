"""``hops project`` — list available projects and switch the active one."""

from __future__ import annotations

import click
from hopsworks.cli import auth, config, output, session


@click.group("project")
def project_group() -> None:
    """List and switch Hopsworks projects."""


@project_group.command("list")
@click.pass_context
def project_list(ctx: click.Context) -> None:
    """Show every project the current user can access.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    cfg = _require_auth(ctx)
    project_obj = auth.login(
        host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project
    )
    import hopsworks  # noqa: PLC0415

    api = hopsworks.get_project_api() if hasattr(hopsworks, "get_project_api") else None
    projects = []
    if api is not None:
        try:
            projects = api.get_projects()
        except Exception:  # noqa: BLE001
            projects = []
    if not projects:
        # Fallback: the current session exposes at least the active project.
        projects = [project_obj]

    rows = []
    for p in projects:
        active = " *" if getattr(p, "name", None) == cfg.project else ""
        rows.append([getattr(p, "id", "?"), f"{getattr(p, 'name', '?')}{active}"])
    output.print_table(["ID", "NAME"], rows)


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
    try:
        fs = project.get_feature_store()
        cfg.feature_store_id = getattr(fs, "id", None)
    except Exception:  # noqa: BLE001 - some projects have no FS
        cfg.feature_store_id = None
    config.save(cfg)
    output.success("✓ Active project set to %s", cfg.project)


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
    output.print_table(["FIELD", "VALUE"], rows)


def _require_auth(ctx: click.Context) -> config.HopsConfig:
    return session.require_auth(ctx)
