"""``hops mlsystem`` — the project's registry of ML systems.

A system is registered by where its code lives, so every member of the project
sees it in the Hopsworks UI: a HopsFS directory in the project, or the Git
repository of a system built from an external client. ``hops build`` and
``/hops-build`` register systems themselves; these commands are for listing
them, for registering or removing one by hand, and for deleting a system with
what it created.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import click
from hopsworks.cli import output, session


def code_location(path: Path, project: str | None = None) -> str:
    """Where a system's code is, as the registry stores it.

    A directory under the Hopsworks terminal's HopsFS mount becomes its HopsFS path
    (``/Projects/<project>/...``); any other directory becomes the URL of its Git
    repository's ``origin``.

    Raises:
        click.ClickException: when the directory is neither in HopsFS nor in a Git repository with an origin.
    """
    resolved = path.resolve()
    home = os.environ.get("HOPSFS_USER_HOME_DIR", "")
    if "/Users/" in home:
        mount = Path(home.split("/Users/", 1)[0]).resolve()
        try:
            relative = resolved.relative_to(mount)
        except ValueError:
            relative = None
        if relative is not None:
            name = (
                project
                or os.environ.get("PROJECT_NAME")
                or os.environ.get("HOPSWORKS_PROJECT")
            )
            if not name:
                raise click.ClickException(
                    "cannot tell which project this HopsFS mount belongs to"
                )
            return f"/Projects/{name}/{relative.as_posix()}"
    origin = subprocess.run(
        ["git", "-C", str(resolved), "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    if not origin:
        raise click.ClickException(
            f"{resolved} is neither in the project's HopsFS nor in a Git repository with an origin"
        )
    return origin


def register(ctx: click.Context, path: Path, name: str | None = None) -> dict:
    """Register the system at `path` with the project's registry; returns the stored entry."""
    from hopsworks_common.core import ml_system_api

    project = session.get_project(ctx)
    return ml_system_api._register(
        code_location(path, getattr(project, "name", None)), name
    )


def _when(value):
    # The API sends dates as epoch milliseconds.
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return value


@click.group("mlsystem")
def mlsystem_group() -> None:
    """The ML systems registered in this project."""


@mlsystem_group.command("list")
@click.pass_context
def mlsystem_list(ctx: click.Context) -> None:
    """List the project's ML systems, newest first, with whether you can open each one's code.

    Args:
        ctx: Click context.
    """
    from hopsworks_common.core import ml_system_api

    session.get_project(ctx)
    systems = ml_system_api._list()
    if output.JSON_MODE:
        output.print_json(systems)
        return
    access = {True: "yes", False: "no", None: "repository"}
    output.print_table(
        ["ID", "NAME", "OWNER", "UPDATED", "CODE ACCESS", "PATH"],
        [
            [
                s.get("id"),
                s.get("name"),
                s.get("ownerName") or s.get("owner"),
                output.format_ts(_when(s.get("lastUpdated"))),
                access.get(s.get("accessible"), "-"),
                s.get("pathToCode"),
            ]
            for s in systems
        ],
    )


@mlsystem_group.command("register")
@click.argument(
    "path",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=".",
)
@click.option("--name", help="Display name; defaults to the directory name.")
@click.pass_context
def mlsystem_register(ctx: click.Context, path: Path, name: str | None) -> None:
    """Register the ML system in PATH (default: the current directory), or refresh its entry.

    Args:
        ctx: Click context.
        path: The system's directory, the one holding its system.yaml.
        name: Display name.
    """
    entry = register(ctx, path, name)
    output.success(f"Registered {entry.get('name')} at {entry.get('pathToCode')}")


def _find(system: str) -> dict:
    from hopsworks_common.core import ml_system_api

    matches = [
        s
        for s in ml_system_api._list()
        if str(s.get("id")) == system or s.get("name") == system
    ]
    if not matches:
        raise click.ClickException(f"no ML system {system!r} in this project")
    if len(matches) > 1:
        raise click.ClickException(
            f"{len(matches)} systems are named {system!r}; use its id"
        )
    return matches[0]


def _local_dir(entry: dict) -> Path | None:
    """The system's directory under the terminal's HopsFS mount, None when its code is elsewhere."""
    code = str(entry.get("pathToCode") or "")
    home = os.environ.get("HOPSFS_USER_HOME_DIR", "")
    found = code.split("/", 3)
    if not code.startswith("/Projects/") or len(found) < 4 or "/Users/" not in home:
        return None
    return Path(home.split("/Users/", 1)[0]) / found[3]


@mlsystem_group.command("remove")
@click.argument("system")
@click.pass_context
def mlsystem_remove(ctx: click.Context, system: str) -> None:
    """Remove SYSTEM (an id or a name) from the registry; its code is left in place.

    Args:
        ctx: Click context.
        system: The id or name of the system.
    """
    from hopsworks_common.core import ml_system_api

    session.get_project(ctx)
    entry = _find(system)
    ml_system_api._remove(entry["id"])
    output.success(f"Removed {entry.get('name')} from the registry")


@mlsystem_group.command("delete")
@click.argument("system")
@click.option(
    "--assets",
    is_flag=True,
    help="Also delete what the system created: its app, deployments, jobs, models, "
    "feature view, the feature groups it writes, cloned environments and Resources/<slug>.",
)
@click.option(
    "--repo",
    is_flag=True,
    help="Also delete its GitHub repository (implies --assets), when the build created it for this system alone.",
)
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="The system's directory, when its code is not in this project's HopsFS.",
)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def mlsystem_delete(
    ctx: click.Context,
    system: str,
    assets: bool,
    repo: bool,
    path: Path | None,
    yes: bool,
) -> None:
    """Delete SYSTEM (an id or a name): its registry entry, and with --assets what it created.

    Assets are read from the system's system.yaml and deleted downstream first,
    skipping any already gone; the run stops at the first failure. The
    repository and the registry entry are deleted last, so after a failure the
    system is still listed and the same command can be run again. The code
    directory is kept.

    Args:
        ctx: Click context.
        system: The id or name of the system.
        assets: Delete the assets the system created.
        repo: Delete the system's GitHub repository too.
        path: The system's directory.
        yes: Skip confirmation when True.
    """
    import yaml
    from hopsworks.cli import teardown
    from hopsworks_common.core import ml_system_api

    project = session.get_project(ctx)
    entry = _find(system)
    assets = assets or repo
    steps: list = []
    doc: dict = {}
    directory = path or _local_dir(entry)
    if assets:
        spec = directory / "system.yaml" if directory else None
        if spec is None or not spec.is_file():
            raise click.ClickException(
                f"cannot read the system.yaml of {entry.get('name')}; pass its directory with --path"
            )
        doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
        deleter = teardown.Deleter(project)
        steps = [
            (str(a), lambda a=a: deleter.delete(a))
            for a in teardown.inventory(doc, directory.name)
        ]
    if repo:
        found = teardown.repo_of(doc)
        label = f"repository {found[1]}/{found[2]}" if found else "repository"
        steps.append(
            (
                label,
                lambda: teardown.delete_repo(
                    doc, directory.name, getattr(project, "name", "")
                ),
            )
        )
    steps.append(
        (
            f"registry entry {entry.get('name')}",
            lambda: ml_system_api._remove(entry["id"]) or "deleted",
        )
    )

    if not output.JSON_MODE:
        click.echo(f"Deleting {entry.get('name')} ({entry.get('pathToCode')}):")
        for label, _ in steps:
            click.echo(f"  {label}")
    if not yes and not output.JSON_MODE:
        click.confirm("Delete all of these?", abort=True)
    outcomes: list[dict] = []

    def report(label: str, outcome: str) -> None:
        outcomes.append({"step": label, "outcome": outcome})
        if not output.JSON_MODE:
            click.echo(
                f"{outcome:>10}  {label}"
                if len(outcome) <= 10
                else f"  {label}: {outcome}"
            )

    failed = teardown.run(steps, report)
    if output.JSON_MODE:
        output.print_json(
            {"system": entry.get("id"), "steps": outcomes, "failed": failed}
        )
    if failed:
        raise click.ClickException(
            f"stopped at {failed}; {entry.get('name')} is still registered, so run the same delete again once that is fixed"
        )
    output.success(
        f"Deleted {entry.get('name')}; its code in {directory or entry.get('pathToCode')} is kept"
    )
