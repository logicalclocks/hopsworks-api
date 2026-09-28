"""``hops mlsystem`` — the project's registry of ML systems.

A system is registered by where its code lives, so every member of the project
sees it in the Hopsworks UI: a HopsFS directory in the project, or the Git
repository of a system built from an external client. ``hops build`` and
``/hops-build`` register systems themselves; these commands are for listing
them and for registering or removing one by hand.
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
    matches = [
        s
        for s in ml_system_api._list()
        if str(s.get("id")) == system or s.get("name") == system
    ]
    if not matches:
        raise click.ClickException(f"no ML system {system!r} in this project")
    if len(matches) > 1:
        raise click.ClickException(
            f"{len(matches)} systems are named {system!r}; remove one by id"
        )
    ml_system_api._remove(matches[0]["id"])
    output.success(f"Removed {matches[0].get('name')} from the registry")
