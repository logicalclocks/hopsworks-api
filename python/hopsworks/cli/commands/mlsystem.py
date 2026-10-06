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
from hopsworks.cli.commands.build import build_cmd


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
    "feature view, the feature groups it writes, cloned environments, Resources/<slug> "
    "and its code directory.",
)
@click.option(
    "--repo",
    is_flag=True,
    help="Also delete its GitHub repository (implies --assets), or only its branch when "
    "the repository holds other builds.",
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
    skipping any already gone; the run stops at the first failure. Then the
    repository (with --repo; only the system's branch when the repository holds
    other builds), the code directory and the registry entry, so after a failure
    the system is still listed and the same command can be run again. Without
    --assets only the registry entry goes and the code is kept.

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
    slug = directory.name if directory else str(entry.get("name"))
    if assets:
        spec = directory / "system.yaml" if directory else None
        if directory is not None and not directory.exists():
            # An earlier run got as far as deleting the code; only the entry is left.
            output.info(f"{directory} is gone, so its assets were deleted before it")
        elif spec is None or not spec.is_file():
            raise click.ClickException(
                f"cannot read the system.yaml of {entry.get('name')}; pass its directory with --path"
            )
        else:
            doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
            others = tuple(
                str(s.get("pathToCode", "")).rstrip("/").rsplit("/", 1)[-1]
                for s in ml_system_api._list()
                if s.get("id") != entry.get("id")
            )
            deleter = teardown.Deleter(project, other_slugs=others)
            steps = [
                (str(a), lambda a=a: deleter.delete(a))
                for a in teardown.inventory(doc, slug)
            ]
            if repo:
                found = teardown.repo_of(doc)
                label = f"repository {found[1]}/{found[2]}" if found else "repository"
                steps.append(
                    (
                        label,
                        lambda: teardown.delete_repo(
                            doc, slug, getattr(project, "name", ""), directory
                        ),
                    )
                )
            steps.append(
                (f"code directory {directory}", lambda: teardown.delete_code(directory))
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
    kept = (
        ""
        if assets
        else f"; its code in {directory or entry.get('pathToCode')} is kept"
    )
    output.success(f"Deleted {entry.get('name')}{kept}")


@mlsystem_group.command("status")
@click.argument("system")
@click.option(
    "--hours",
    type=click.IntRange(min=1),
    default=24,
    show_default=True,
    help="How far back to read the job runs.",
)
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="The system's directory, when its code is not in this project's HopsFS.",
)
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Where to write the HTML report; default: status/report.html in the system directory.",
)
@click.option("--no-summary", is_flag=True, help="Skip the summary Claude writes.")
@click.pass_context
def mlsystem_status(
    ctx: click.Context,
    system: str,
    hours: int,
    path: Path | None,
    out: Path | None,
    no_summary: bool,
) -> None:
    """Report the health of SYSTEM (an id or a name) as an HTML page.

    Reads the jobs, deployments and apps the system's system.yaml records: each
    job's runs in the last HOURS with the log tail of every failure, and each
    deployment and app with its state and its pods (readiness, restarts, the last
    termination reason, CPU and memory against the limits, from kubectl). Claude
    writes a short summary of what failed and why. The Factory Status button runs this
    and shows the page.

    Args:
        ctx: Click context.
        system: The id or name of the system.
        hours: How far back to read job runs.
        path: The system's directory.
        out: Where to write the report.
        no_summary: Skip the Claude summary.
    """
    import yaml
    from hopsworks.cli import health

    project = session.get_project(ctx)
    entry = _find(system)
    directory = path or _local_dir(entry)
    spec = directory / "system.yaml" if directory else None
    if spec is None or not spec.is_file():
        raise click.ClickException(
            f"cannot read the system.yaml of {entry.get('name')}; pass its directory with --path"
        )
    doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
    facts = health.collect(project, doc, directory.name, hours)
    if output.JSON_MODE:
        output.print_json(facts)
        return
    summary = None if no_summary else health.summarize(facts)
    target = out or directory / "status" / "report.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(health.render(facts, summary), encoding="utf-8")
    c = facts["counts"]
    output.success(
        f"{facts['overall']}: {c['failed_runs']} of {c['runs']} job runs failed in {hours} h, "
        f"{c['unhealthy_services']} of {c['services']} deployments and apps unhealthy; report in {target}"
    )


# `hops build` is how a system is created; listed here too, as the same command.
mlsystem_group.add_command(build_cmd, name="create")
