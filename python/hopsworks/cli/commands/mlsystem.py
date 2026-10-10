"""``hops factory system`` — the systems the project's factories built.

A system is registered by where its code lives, so every member of the project
sees it in the Hopsworks UI: a HopsFS directory in the project, or the Git
repository of a system built from an external client. ``hops factory run`` and
the build commands register systems themselves; these commands list them,
register or remove one by hand, report a system's health and delete it with
what it created. A change to a built system, an analytics layer's data marts
included, is a change request: ``hops factory run <factory> <system> --change``.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import click
from hopsworks.cli import output, session


def hopsfs_mount() -> Path | None:
    """The HopsFS mount the terminal's home directory is under, None outside a Hopsworks terminal.

    Returns:
        The directory holding `Users/<user>`, the home `HOPSFS_USER_HOME_DIR` names.
    """
    home = Path(os.environ.get("HOPSFS_USER_HOME_DIR", ""))
    return home.parent.parent if home.parent.name == "Users" else None


def code_location(path: Path, project: str | None = None) -> str:
    """Where a system's code is, as the registry stores it.

    A directory under the Hopsworks terminal's HopsFS mount becomes its HopsFS path
    (``/Projects/<project>/...``); any other directory becomes the URL of its Git
    repository's ``origin``.

    Args:
        path: The system's directory.
        project: The project's name, when the environment does not say.

    Returns:
        The HopsFS path or the Git URL.

    Raises:
        click.ClickException: when the directory is neither in HopsFS nor in a Git repository with an origin.
    """
    resolved = path.resolve()
    mount = hopsfs_mount()
    if mount is not None:
        try:
            relative = resolved.relative_to(mount.resolve())
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


def register(
    ctx: click.Context, path: Path, name: str | None = None, factory: str | None = None
) -> dict:
    """Register the system at `path`, built by `factory`, with the project's registry; returns the stored entry.

    The factory version and definition digest its system.yaml records go along, so the
    registry records the definition the system was generated from, not the factory's current one.

    Args:
        ctx: Click context.
        path: The system's directory.
        name: Its display name.
        factory: The factory that built it.

    Returns:
        The registry entry.
    """
    from hopsworks.cli import system_doc
    from hopsworks_common.core import ml_system_api

    project = session.get_project(ctx)
    recorded = system_doc.read(path)[0].get("factory") or {}
    if not isinstance(recorded, dict) or recorded.get("name") not in (None, factory):
        recorded = {}
    return ml_system_api._register(
        code_location(path, getattr(project, "name", None)),
        name,
        factory or recorded.get("name"),
        factory_version=recorded.get("version"),
        factory_digest=recorded.get("digest"),
    )


def _when(value):
    # The API sends dates as epoch milliseconds.
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return value


@click.group("system")
def system_group() -> None:
    """The systems the project's factories built: list, register, status, remove and delete them."""


@system_group.command("list")
@click.option("--factory", help="Only the systems this factory built.")
@click.pass_context
def system_list(ctx: click.Context, factory: str | None) -> None:
    """List the project's systems, newest first, with the factory that built each and whether you can open its code.

    Args:
        ctx: Click context.
        factory: Only this factory's systems.
    """
    from hopsworks_common.core import ml_system_api

    session.get_project(ctx)
    systems = ml_system_api._list(factory)
    if output.JSON_MODE:
        output.print_json(systems)
        return
    access = {True: "yes", False: "no", None: "repository"}

    def code_access(s: dict) -> str:
        if s.get("available") is False:
            return "unavailable"
        return access.get(s.get("accessible"), "-")

    output.print_table(
        ["ID", "NAME", "FACTORY", "OWNER", "UPDATED", "CODE ACCESS", "PATH"],
        [
            [
                s.get("id"),
                s.get("name"),
                f"{s.get('factory') or '-'} v{s.get('factoryVersion') or '?'}",
                s.get("ownerName") or s.get("owner"),
                output.format_ts(_when(s.get("lastUpdated"))),
                code_access(s),
                s.get("pathToCode"),
            ]
            for s in systems
        ],
    )


@system_group.command("register")
@click.argument(
    "path",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=".",
)
@click.option("--name", help="Display name; defaults to the directory name.")
@click.option(
    "--factory",
    help="The factory that built it (hops factory list); a new entry defaults to ml-batch.",
)
@click.pass_context
def system_register(
    ctx: click.Context, path: Path, name: str | None, factory: str | None
) -> None:
    """Register the system in PATH (default: the current directory), or refresh its entry.

    Args:
        ctx: Click context.
        path: The system's directory, the one holding its system.yaml.
        name: Display name.
        factory: The factory that built it.
    """
    entry = register(ctx, path, name, factory)
    output.success(f"Registered {entry.get('name')} at {entry.get('pathToCode')}")


def _find(system: str) -> dict:
    """The registry entry of `system`: its id, its name, or its directory's name (the slug)."""
    from hopsworks_common.core import ml_system_api

    if system.isdigit():
        with contextlib.suppress(Exception):
            return ml_system_api._get(int(system))
    matches = [
        s
        for s in ml_system_api._list()
        if system
        in (
            str(s.get("id")),
            s.get("name"),
            str(s.get("pathToCode") or "").rstrip("/").rsplit("/", 1)[-1],
        )
    ]
    if not matches:
        raise click.ClickException(f"no system {system!r} in this project")
    if len(matches) > 1:
        raise click.ClickException(
            f"{len(matches)} systems are named {system!r}; use its id"
        )
    return matches[0]


def _local_dir(entry: dict) -> Path | None:
    """The system's directory under the terminal's HopsFS mount, None when its code is elsewhere."""
    code = str(entry.get("pathToCode") or "")
    mount = hopsfs_mount()
    found = code.split("/", 3)
    if not code.startswith("/Projects/") or len(found) < 4 or mount is None:
        return None
    return mount / found[3]


def _other_docs(entry: dict) -> dict[str, dict]:
    """The system.yaml of every other registered system, by slug; empty for one whose code this terminal cannot read."""
    import yaml
    from hopsworks_common.core import ml_system_api

    docs: dict[str, dict] = {}
    for other in ml_system_api._list():
        if other.get("id") == entry.get("id"):
            continue
        slug = str(other.get("pathToCode") or "").rstrip("/").rsplit("/", 1)[-1]
        directory = _local_dir(other)
        doc: dict = {}
        if directory is not None:
            with contextlib.suppress(OSError, yaml.YAMLError):
                doc = yaml.safe_load((directory / "system.yaml").read_text("utf-8"))
        docs[slug] = doc if isinstance(doc, dict) else {}
    return docs


@system_group.command("remove")
@click.argument("system")
@click.pass_context
def system_remove(ctx: click.Context, system: str) -> None:
    """Remove SYSTEM (an id, name or slug) from the registry; its code is left in place.

    Args:
        ctx: Click context.
        system: The id, name or slug of the system.
    """
    from hopsworks_common.core import ml_system_api

    session.get_project(ctx)
    entry = _find(system)
    ml_system_api._remove(entry["id"])
    output.success(f"Removed {entry.get('name')} from the registry")


@system_group.command("delete")
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
def system_delete(
    ctx: click.Context,
    system: str,
    assets: bool,
    repo: bool,
    path: Path | None,
    yes: bool,
) -> None:
    """Delete SYSTEM (an id, name or slug): its registry entry, and with --assets what it created.

    An analytics layer's assets are its jobs and tables, never the tables it
    reads, and its directory. Any other system's assets are read from its
    system.yaml and deleted downstream first, skipping any already gone; the
    run stops at the first failure. Then the
    repository (with --repo; only the system's branch when the repository holds
    other builds), the code directory and the registry entry, so after a failure
    the system is still listed and the same command can be run again. Without
    --assets only the registry entry goes and the code is kept.

    Args:
        ctx: Click context.
        system: The id, name or slug of the system.
        assets: Delete the assets the system created.
        repo: Delete the system's GitHub repository too.
        path: The system's directory.
        yes: Skip confirmation when True.
    """
    import yaml
    from hopsworks.cli import teardown
    from hopsworks.cli.commands import analytics
    from hopsworks_common.core import ml_system_api

    project = session.get_project(ctx)
    entry = _find(system)
    assets = assets or repo
    steps: list = []
    kept: list[str] = []
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
        elif "layer" in (doc := yaml.safe_load(spec.read_text(encoding="utf-8")) or {}):
            if repo:
                raise click.ClickException(
                    "an analytics layer shares its repository with the other layers; delete it without --repo"
                )
            steps = [
                (
                    f"jobs, tables and directory of the layer {directory}",
                    lambda: analytics.delete_layer(ctx, directory, doc),
                )
            ]
        else:
            others = _other_docs(entry)
            deleter = teardown.Deleter(project, other_slugs=tuple(others))
            planned, kept = teardown.plan(doc, slug, others)
            steps = [(str(a), lambda a=a: deleter.delete(a)) for a in planned]
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
        for line in kept:
            click.echo(f"  {line}")
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
            {
                "system": entry.get("id"),
                "steps": outcomes,
                "kept": kept,
                "failed": failed,
            }
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


@system_group.command("status")
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
def system_status(
    ctx: click.Context,
    system: str,
    hours: int,
    path: Path | None,
    out: Path | None,
    no_summary: bool,
) -> None:
    """Report the health of SYSTEM (an id, name or slug) as an HTML page.

    Reads the jobs, deployments and apps the system's system.yaml records: each
    job's runs in the last HOURS with the log tail of every failure, and each
    deployment and app with its state and its pods (readiness, restarts, the last
    termination reason, CPU and memory against the limits, from kubectl). Claude
    writes a short summary of what failed and why. An analytics layer's report
    reads its jobs and tables instead (freshness, rejected rows, file layout).
    The Factory Status button runs this and shows the page.

    Args:
        ctx: Click context.
        system: The id, name or slug of the system.
        hours: How far back to read job runs.
        path: The system's directory.
        out: Where to write the report.
        no_summary: Skip the Claude summary.
    """
    import yaml
    from hopsworks.cli import health
    from hopsworks.cli.commands import analytics

    project = session.get_project(ctx)
    entry = _find(system)
    directory = path or _local_dir(entry)
    spec = directory / "system.yaml" if directory else None
    if spec is None or not spec.is_file():
        raise click.ClickException(
            f"cannot read the system.yaml of {entry.get('name')}; pass its directory with --path"
        )
    doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
    if "layer" in doc:
        analytics.layer_status(project, directory, doc, hours, no_summary, out)
        return
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


def _system_path(slug_or_dir: str) -> Path | None:
    """The directory of a system: SLUG_OR_DIR itself, ./SLUG, the current directory when it is SLUG, or an analytics layer's in its analytics repository."""
    from hopsworks.cli.commands import analytics

    cwd = Path.cwd()
    here = [cwd] if cwd.name == slug_or_dir else []
    for candidate in (Path(slug_or_dir), cwd / slug_or_dir, *here):
        if (candidate / "system.yaml").is_file():
            return candidate
    return next((d for d in analytics._layer_dirs(cwd) if d.name == slug_or_dir), None)


def _require_path(slug_or_dir: str) -> Path:
    found = _system_path(slug_or_dir)
    if found is None:
        raise click.ClickException(f"no system {slug_or_dir!r} under {Path.cwd()}")
    return found


@system_group.command("dir")
@click.argument("slug")
def system_dir(slug: str) -> None:
    """Print the directory of the system SLUG under the current directory: ./SLUG, or an analytics layer's in its analytics repository.

    Args:
        slug: The system.
    """
    click.echo(_require_path(slug))


@system_group.command("write-doc")
@click.argument("slug_or_dir")
@click.option(
    "--expected-sha256",
    "expected",
    required=True,
    help="The sha256 of the system.yaml the edit started from.",
)
@click.option(
    "--from",
    "source",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="The edited document; it is removed afterwards.",
)
def system_write_doc(slug_or_dir: str, expected: str, source: Path) -> None:
    """Replace the system.yaml of SLUG_OR_DIR with the edited document in --from.

    Exits 3 when system.yaml changed since the edit read it, and 2 when the edit is not a valid system.yaml.

    Args:
        slug_or_dir: The system's slug or directory.
        expected: The sha256 of the system.yaml the edit started from.
        source: The edited document.
    """
    from hopsworks.cli import system_doc

    try:
        target = _require_path(slug_or_dir)
        written = system_doc.replace(
            target, source.read_text(encoding="utf-8"), expected.lower()
        )
    finally:
        source.unlink(missing_ok=True)
    if output.JSON_MODE:
        output.print_json({"path": str(target / "system.yaml"), "sha256": written})
        return
    output.success(f"Wrote {target / 'system.yaml'} ({written})")


@system_group.group("lease")
def lease_group() -> None:
    """The build lease of a system: which invocation is building it, until when."""


@lease_group.command("show")
@click.argument("slug_or_dir")
def lease_show(slug_or_dir: str) -> None:
    """Print the build lease of SLUG_OR_DIR, if any; exits 1 when there is none.

    Args:
        slug_or_dir: The system's slug or directory.
    """
    from hopsworks.cli import system_doc

    held = system_doc.lease(_require_path(slug_or_dir))
    if held is None:
        raise click.ClickException("no build lease")
    held = {**held, "expired": system_doc.expired(held)}
    if output.JSON_MODE:
        output.print_json(held)
        return
    for key, value in held.items():
        click.echo(f"{key}: {value}")


def _ttl(minutes: int):
    from datetime import timedelta

    return timedelta(minutes=minutes)


_TTL = click.option(
    "--ttl-minutes",
    type=click.IntRange(min=1),
    default=240,
    show_default=True,
    help="How long the lease lasts without a renewal.",
)
_TOKEN = click.option(
    "--token",
    envvar="HOPS_LEASE_TOKEN",
    help="The lease's token; default: $HOPS_LEASE_TOKEN.",
)


@lease_group.command("acquire")
@click.argument("slug_or_dir")
@_TOKEN
@_TTL
def lease_acquire(slug_or_dir: str, token: str | None, ttl_minutes: int) -> None:
    """Take the build lease of SLUG_OR_DIR and print its token; exits 4 while another invocation holds it.

    Args:
        slug_or_dir: The system's slug or directory.
        token: A token of a lease already held, to adopt.
        ttl_minutes: How long the lease lasts without a renewal.
    """
    from hopsworks.cli import system_doc

    held = system_doc.acquire(_require_path(slug_or_dir), token, _ttl(ttl_minutes))
    if output.JSON_MODE:
        output.print_json(held)
        return
    click.echo(held["token"])


@lease_group.command("renew")
@click.argument("slug_or_dir")
@_TOKEN
@_TTL
def lease_renew(slug_or_dir: str, token: str | None, ttl_minutes: int) -> None:
    """Extend the build lease TOKEN holds; exits 4 when the lease is no longer TOKEN's.

    Args:
        slug_or_dir: The system's slug or directory.
        token: The lease's token.
        ttl_minutes: How long the lease lasts without a renewal.
    """
    from hopsworks.cli import system_doc

    if not token:
        raise click.UsageError("no --token and no HOPS_LEASE_TOKEN")
    held = system_doc.renew(_require_path(slug_or_dir), token, _ttl(ttl_minutes))
    output.success(f"Lease renewed until {held['expires']}")


@lease_group.command("release")
@click.argument("slug_or_dir")
@_TOKEN
def lease_release(slug_or_dir: str, token: str | None) -> None:
    """Give up the build lease TOKEN holds; a lease held with another token is left alone.

    Args:
        slug_or_dir: The system's slug or directory.
        token: The lease's token.
    """
    from hopsworks.cli import system_doc

    if system_doc.release(_require_path(slug_or_dir), token):
        output.success("Lease released")
    else:
        output.info("No lease held with that token")


@system_group.command("delete-assets")
@click.argument("system")
@click.option("--job", "jobs", multiple=True, help="A job to delete; repeatable.")
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="A feature group to delete, as NAME or NAME:VERSION; repeatable.",
)
@click.option(
    "--path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="The system's directory, when its code is not in this project's HopsFS.",
)
@click.pass_context
def system_delete_assets(
    ctx: click.Context,
    system: str,
    jobs: tuple[str, ...],
    tables: tuple[str, ...],
    path: Path | None,
) -> None:
    """Delete some of what SYSTEM (an id, name or slug) built: the jobs, then the feature groups.

    A build runs this when a change removes part of a system; the system and
    its registry entry stay. Every feature group is checked before anything
    is deleted: one the system reads, or one tagged as a lower analytics
    layer (any analytics table, for an ML system), stops the delete with
    nothing gone. What is already gone is skipped, so it can be run again.

    Args:
        ctx: Click context.
        system: The id, name or slug of the system.
        jobs: The jobs to delete.
        tables: The feature groups to delete, as NAME or NAME:VERSION.
        path: The system's directory.
    """
    import yaml
    from hopsworks.cli.commands import analytics

    session.get_project(ctx)
    entry = _find(system)
    directory = path or _local_dir(entry)
    spec = directory / "system.yaml" if directory else None
    if spec is None or not spec.is_file():
        raise click.ClickException(
            f"cannot read the system.yaml of {entry.get('name')}; pass its directory with --path"
        )
    doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
    analytics.delete_assets(
        ctx,
        doc,
        list(jobs),
        [
            {"name": name, "version": int(version or 1)}
            for name, _, version in (t.partition(":") for t in tables)
        ],
    )
