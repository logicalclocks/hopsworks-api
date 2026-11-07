"""``hops semantic``: Semantic Layer projects, builds, tags, discovery, metric queries, tests and Superset dashboards.

Wraps ``project.get_semantic_layer_api()``. The query verbs follow MetricFlow's
``mf`` CLI: metrics and group-bys are comma-separated, ``--where`` repeats,
``--order-by`` (alias ``--order``) sorts descending on a leading ``-``,
``query --explain`` prints the SQL instead of running it, and ``--saved-query``
runs a saved query instead of ``--metrics``.
``init`` scaffolds a dbt project over feature groups, optionally in a new private GitHub repository cloned through Hopsworks git.
``time-spine create`` writes the time spine feature group that cumulative metrics and offsets join to.
"""

from __future__ import annotations

import contextlib
import json
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import click
from hopsworks.cli import output, session
from hopsworks.cli.commands import fg as fg_cmd
from hopsworks.cli.commands import git as git_cmd
from hopsworks_common.client.exceptions import (
    GitException,
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.core.semantic_layer_api import _SPINE_GRAINS
from hopsworks_common.semantic_layer import _GRAINS


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from hopsworks_common.semantic_layer import SemanticBuild, SemanticProject


@click.group("semantic")
def semantic_group() -> None:
    """Semantic Layer: metrics over feature groups, compiled from YAML and queried."""


def _api(ctx: click.Context) -> Any:
    return session.get_project(ctx).get_semantic_layer_api()


def _semantic_project(ctx: click.Context, name: str) -> SemanticProject:
    sp = _api(ctx).get_project(name)
    if sp is None:
        raise click.ClickException(
            f"No semantic project named '{name}'. Run `hops semantic list`."
        )
    return sp


def _split(
    _ctx: click.Context, _param: click.Parameter, values: tuple[str, ...]
) -> list[str] | None:
    """Flatten a repeatable, comma-separated option into one list."""
    items = [item.strip() for value in values for item in value.split(",")]
    return [item for item in items if item] or None


def _split_given(
    _ctx: click.Context, _param: click.Parameter, values: tuple[str, ...]
) -> list[str] | None:
    """As ``_split``, but an option given only empty, as ``--group-by ""``, is an empty list rather than ``None``."""
    if not values:
        return None
    return _split(_ctx, _param, values) or []


def _build_label(build: SemanticBuild | None) -> str:
    return f"{build.id} {build.status}" if build else "-"


def _parse_tags(
    _ctx: click.Context, _param: click.Parameter, values: tuple[str, ...]
) -> dict[str, Any] | None:
    """Read repeated ``NAME=VALUE`` options into tags, each value JSON or a plain string."""
    tags = {}
    for value in values:
        name, sep, raw = value.partition("=")
        if not sep or not name.strip():
            raise click.BadParameter(
                f"expected NAME=VALUE, got {value!r}", param_hint="--tag"
            )
        tags[name.strip()] = output.parse_tag_value(raw)
    return tags or None


# The gateway caches each caller's session for up to a minute; a little longer covers the request.
_SERVING_WAIT_SECONDS = 90
_SERVING_POLL_SECONDS = 5
# Where init clones a repository, and writes a scaffold unless --path names another directory.
_SEMANTIC_DIR = "Resources/semantic"
_CLONE_TIMEOUT_SECONDS = 600


# region Projects


@semantic_group.command("list")
@click.pass_context
def list_cmd(ctx: click.Context) -> None:
    """List the project's semantic projects with their current and newest builds.

    Args:
        ctx: Click context.
    """
    projects = _api(ctx).get_projects()
    if output.JSON_MODE:
        output.print_json([sp.to_dict() for sp in projects])
        return
    output.print_table(
        ["NAME", "ID", "FORMAT", "MODE", "CURRENT BUILD", "LATEST BUILD", "ROOT PATH"],
        [
            [
                sp.name,
                sp.id,
                sp.format,
                sp.deployment_mode,
                sp.current_build_id or "-",
                _build_label(sp.latest_build),
                sp.root_path,
            ]
            for sp in projects
        ],
    )
    for sp in projects:
        if sp.missing_mandatory_tags:
            output.warn(
                "%s lacks mandatory tags: %s (hops semantic tag set %s <name> <value>).",
                sp.name,
                ", ".join(sp.missing_mandatory_tags),
                sp.name,
            )


@semantic_group.command("create")
@click.argument("name")
@click.option(
    "--path",
    "root_path",
    required=True,
    help="Directory of the definitions (a dbt project's root), relative to the "
    "project, under /hopsfs/, or /Projects/<project>/...",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["AUTO", "DBT", "NATIVE", "MANIFEST_JSON"], case_sensitive=False),
    default="AUTO",
    show_default=True,
    help="Format of the definitions; AUTO detects it from the files.",
)
@click.option(
    "--mode",
    type=click.Choice(["FOLLOW", "PINNED"], case_sensitive=False),
    default="FOLLOW",
    show_default=True,
    help="FOLLOW serves every new valid build; PINNED serves a build until "
    "another is promoted.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    callback=_parse_tags,
    metavar="NAME=VALUE",
    help="Schematised tag to attach, the value JSON or a plain string; repeatable. "
    "Pass the mandatory semantic project tags here, or the create is refused.",
)
@click.pass_context
def create_cmd(
    ctx: click.Context,
    name: str,
    root_path: str,
    fmt: str,
    mode: str,
    tags: dict[str, Any] | None,
) -> None:
    """Register a directory of semantic definitions and start its first compile.

    Args:
        ctx: Click context.
        name: Semantic project name.
        root_path: Directory of the definitions.
        fmt: Format of the definitions.
        mode: Deployment mode.
        tags: Tags to attach in the same request.
    """
    sp = _api(ctx).create_project(
        name,
        root_path,
        format=fmt.upper(),
        deployment_mode=mode.upper(),
        **({"tags": tags} if tags else {}),
    )
    if output.JSON_MODE:
        output.print_json(sp.to_dict())
        return
    output.success(
        "Created semantic project %s (id=%s) on %s; its first compile runs in the "
        "background. Follow it with `hops semantic builds %s`.",
        sp.name,
        sp.id,
        sp.root_path,
        sp.name,
    )


@semantic_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def delete_cmd(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a semantic project and its builds; the YAML files stay in place.

    Args:
        ctx: Click context.
        name: Semantic project name.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete semantic project {name} and its builds?", abort=True)
    _api(ctx).delete_project(name)
    if output.JSON_MODE:
        output.print_json({"deleted": name})
        return
    output.success("Deleted semantic project %s.", name)


@semantic_group.command("show")
@click.argument("name")
@click.pass_context
def show_cmd(ctx: click.Context, name: str) -> None:
    """Show a semantic project's settings, the build that answers queries, and whether it is stale.

    Args:
        ctx: Click context.
        name: Semantic project name.
    """
    sp = _semantic_project(ctx, name)
    try:
        serving = sp.serving()
        served = f"build {serving.build_id}" + (
            " (stale: the newest compile is invalid or running)"
            if serving.stale
            else ""
        )
    except SemanticLayerException as exc:
        serving = None
        served = exc.error_msg or str(exc)
    tags = sp.get_tags()
    if output.JSON_MODE:
        output.print_json(
            {
                **sp.to_dict(),
                "tags": tags,
                "serving": serving.to_dict() if serving else None,
            }
        )
        return
    output.print_table(
        ["FIELD", "VALUE"],
        [
            ["name", sp.name],
            ["id", sp.id],
            ["root path", sp.root_path],
            ["git repository", sp.git_repository_id or "-"],
            ["format", sp.format],
            ["mode", sp.deployment_mode],
            ["current build", sp.current_build_id or "-"],
            ["latest build", _build_label(sp.latest_build)],
            ["gateway serves", served],
            ["tags", ", ".join(sorted(tags)) or "-"],
            ["missing mandatory tags", ", ".join(sp.missing_mandatory_tags) or "-"],
        ],
    )


@semantic_group.command("update")
@click.argument("name")
@click.option("--path", "root_path", help="New directory of the definitions.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["AUTO", "DBT", "NATIVE", "MANIFEST_JSON"], case_sensitive=False),
    help="New format of the definitions.",
)
@click.option(
    "--mode",
    type=click.Choice(["FOLLOW", "PINNED"], case_sensitive=False),
    help="New deployment mode.",
)
@click.pass_context
def update_cmd(
    ctx: click.Context,
    name: str,
    root_path: str | None,
    fmt: str | None,
    mode: str | None,
) -> None:
    """Change a semantic project's root path, format or deployment mode.

    A new root path or format compiles the definitions again in the background.

    Args:
        ctx: Click context.
        name: Semantic project name.
        root_path: New directory of the definitions.
        fmt: New format.
        mode: New deployment mode.
    """
    if root_path is None and fmt is None and mode is None:
        raise click.UsageError("Give at least one of --path, --format and --mode.")
    sp = _semantic_project(ctx, name).update(
        root_path=root_path,
        format=fmt.upper() if fmt else None,
        deployment_mode=mode.upper() if mode else None,
    )
    if output.JSON_MODE:
        output.print_json(sp.to_dict())
        return
    output.success(
        "Updated semantic project %s: %s, %s, %s.",
        sp.name,
        sp.root_path,
        sp.format,
        sp.deployment_mode,
    )
    if root_path is not None or fmt is not None:
        output.info(
            "It compiles again; follow it with `hops semantic builds %s`.", sp.name
        )


@semantic_group.command(
    "init",
    epilog="""\b
Examples:
  hops semantic init sales --from-fg orders --from-fg customers:2
      writes Resources/semantic/sales/{dbt_project.yml,models/semantic.yml} and registers it
  hops semantic init sales --from-fg orders --repo new
      the same in a new private GitHub repository, cloned through Hopsworks git
  hops semantic init sales --repo https://github.com/acme/metrics.git
      clones an existing repository into Resources/semantic/ and registers it
""",
)
@click.argument("name")
@click.option(
    "--from-fg",
    "from_fg",
    multiple=True,
    metavar="FG[:VERSION]",
    help="Scaffold a semantic model for this feature group, at its latest version "
    "unless one is given; repeatable.",
)
@click.option(
    "--path",
    "root_path",
    help="HopsFS directory to write the scaffold to, relative to the project, under "
    f"/hopsfs/, or /Projects/<project>/...; default {_SEMANTIC_DIR}/<name>.",
)
@click.option(
    "--repo",
    help="'new' for a new private GitHub repository (needs the GitHub CLI logged "
    "in), or the URL of an existing repository to clone and register.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["AUTO", "DBT", "NATIVE", "MANIFEST_JSON"], case_sensitive=False),
    help="Format of the definitions; default DBT for a scaffold, AUTO for an "
    "existing repository.",
)
@click.option(
    "--mode",
    type=click.Choice(["FOLLOW", "PINNED"], case_sensitive=False),
    default="FOLLOW",
    show_default=True,
    help="FOLLOW serves every new valid build; PINNED serves a build until "
    "another is promoted.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    callback=_parse_tags,
    metavar="NAME=VALUE",
    help="Schematised tag to attach to the semantic project; repeatable.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="With --repo new, create the repository and register your GitHub CLI "
    "token with Hopsworks without asking.",
)
@click.pass_context
def init_cmd(
    ctx: click.Context,
    name: str,
    from_fg: tuple[str, ...],
    root_path: str | None,
    repo: str | None,
    fmt: str | None,
    mode: str,
    tags: dict[str, Any] | None,
    yes: bool,
) -> None:
    """Scaffold a dbt project over feature groups, register it as a semantic project, and compile it.

    Each --from-fg feature group becomes a semantic model: its primary key the primary
    entity, its event time the time dimension (day grain), its string features
    categorical dimensions, and a row count and a sum per numeric feature its measures.
    One simple metric per measure is written commented out: uncomment the ones to serve.
    With --repo new the project lives in a new private GitHub repository, hops-semantic-<name>
    or the next free name, cloned through Hopsworks git into Resources/semantic/, so a pull
    recompiles it and each build records its commit. Hopsworks clones with the GitHub token
    registered in your account; when there is none, init registers `gh auth token`, after asking.

    Args:
        ctx: Click context.
        name: Semantic project name, also the dbt project's name.
        from_fg: Feature groups to scaffold, each name[:version].
        root_path: HopsFS directory of the scaffold.
        repo: 'new', or the URL of an existing repository.
        fmt: Format of the definitions.
        mode: Deployment mode.
        tags: Tags to attach to the semantic project.
        yes: Skip the confirmations of --repo new.
    """
    existing = repo is not None and repo.lower() != "new"
    if repo is not None and root_path is not None:
        raise click.UsageError(
            f"--path is where a scaffold is written; with --repo the repository is cloned into {_SEMANTIC_DIR}/<repository>."
        )
    if existing and from_fg:
        raise click.UsageError(
            "--from-fg scaffolds a new project; an existing repository (--repo <url>) is registered as it is."
        )
    api = _api(ctx)
    if api.get_project(name) is not None:
        raise click.ClickException(
            f"Semantic project '{name}' already exists; `hops semantic show {name}` shows it."
        )
    # Checked before anything is created, so a refused registration leaves no files, clone or repository.
    missing = sorted(api._mandatory_tags() - set(tags or {}))
    if missing:
        raise click.ClickException(
            f"Semantic projects here must carry the tags {', '.join(missing)}; pass each with "
            "--tag NAME=VALUE."
        )
    fmt = (fmt or ("AUTO" if existing else "DBT")).upper()
    url = repo if existing else None
    git_repository_id = None
    if existing:
        clone = _clone(
            ctx, repo, git_cmd.provider_for_host(urlparse(repo).hostname or "")
        )
        path, git_repository_id = clone.path, clone.id
    else:
        feature_groups = [fg_cmd._get_fg(ctx, *_fg_ref(ref)) for ref in from_fg]
        # The semantic layer reads cached and stream feature groups through their offline table only.
        tableless = [
            f"{fg.name} version {fg.version}"
            for fg in feature_groups
            if fg_cmd._fg_type_label(fg) in ("external", "spine")
        ]
        if tableless:
            raise click.ClickException(
                f"{', '.join(tableless)}: semantic models read only feature groups with an "
                "offline table, and external feature groups and spine groups have none."
            )
        files = _scaffold(
            name,
            feature_groups,
            getattr(session.get_feature_store(ctx), "name", None),
        )
        if repo is None:
            path = _write_scaffold(ctx, root_path or f"{_SEMANTIC_DIR}/{name}", files)
        else:
            path, git_repository_id, url = _new_repo(ctx, name, files, yes)
    try:
        sp = api.create_project(
            name,
            path,
            git_repository_id=git_repository_id,
            format=fmt,
            deployment_mode=mode.upper(),
            **({"tags": tags} if tags else {}),
        )
    except Exception as exc:
        tag_args = "".join(
            f" --tag {shlex.quote(f'{k}={json.dumps(v) if not isinstance(v, str) else v}')}"
            for k, v in (tags or {}).items()
        )
        raise click.ClickException(
            f"Registering {name} failed: {exc}. The definitions are in {path}; once the cause is "
            f"fixed, register them with `hops semantic create {name} --path {shlex.quote(path)}"
            f" --format {fmt}{tag_args}`."
        ) from exc
    build = None
    # A scaffold without feature groups has no semantic model yet, which no compile accepts.
    if existing or from_fg:
        try:
            build = sp.compile(wait=True)
        except SemanticBuildException as exc:
            build = exc.build
        if build.status == "VALID" and build.warning_count and build.issues is None:
            build = sp.get_build(build.id)
    if output.JSON_MODE:
        output.print_json(
            {
                "semantic_project": sp.to_dict(),
                "repository": url,
                "build": build.to_dict() if build else None,
            }
        )
    else:
        output.success(
            "Created semantic project %s on %s%s.",
            sp.name,
            sp.root_path,
            f", cloned from {url}" if url else "",
        )
        if build:
            _render_build(build)
        if not existing:
            output.info(
                "%s models/semantic.yml, then run `hops semantic compile %s --wait`.",
                "Uncomment the metrics to serve in"
                if from_fg
                else "Add semantic models and metrics to",
                name,
            )
    if build and build.status != "VALID":
        ctx.exit(1)


# endregion

# region Tags


@semantic_group.group("tag")
def tag_group() -> None:
    """Schematised tags on a semantic project; a metric's tags are set in its definition."""


@tag_group.command("list")
@click.argument("name")
@click.pass_context
def tag_list_cmd(ctx: click.Context, name: str) -> None:
    """List the tags attached to a semantic project.

    Args:
        ctx: Click context.
        name: Semantic project name.
    """
    tags = _semantic_project(ctx, name).get_tags()
    if output.JSON_MODE:
        output.print_json(tags)
        return
    output.print_table(
        ["NAME", "VALUE"],
        [[tag, json.dumps(value, default=str)] for tag, value in tags.items()],
    )


@tag_group.command("get")
@click.argument("name")
@click.argument("tag")
@click.pass_context
def tag_get_cmd(ctx: click.Context, name: str, tag: str) -> None:
    """Print the value of one tag of a semantic project, as JSON.

    Args:
        ctx: Click context.
        name: Semantic project name.
        tag: Tag schema name.
    """
    value = _semantic_project(ctx, name).get_tag(tag)
    if value is None:
        raise click.ClickException(f"Semantic project '{name}' has no tag '{tag}'.")
    click.echo(json.dumps(value, indent=2 if output.JSON_MODE else None, default=str))


@tag_group.command("set")
@click.argument("name")
@click.argument("tag")
@click.argument("value")
@click.pass_context
def tag_set_cmd(ctx: click.Context, name: str, tag: str, value: str) -> None:
    """Attach a tag to a semantic project, or replace its value.

    Args:
        ctx: Click context.
        name: Semantic project name.
        tag: Tag schema name.
        value: Tag value, JSON or a plain string.
    """
    parsed = output.parse_tag_value(value)
    _semantic_project(ctx, name).add_tag(tag, parsed)
    if output.JSON_MODE:
        output.print_json({"name": tag, "value": parsed})
        return
    output.success("Set tag %s on semantic project %s.", tag, name)


@tag_group.command("delete")
@click.argument("name")
@click.argument("tag")
@click.pass_context
def tag_delete_cmd(ctx: click.Context, name: str, tag: str) -> None:
    """Remove a tag from a semantic project.

    Args:
        ctx: Click context.
        name: Semantic project name.
        tag: Tag schema name.
    """
    _semantic_project(ctx, name).delete_tag(tag)
    if output.JSON_MODE:
        output.print_json({"deleted": tag})
        return
    output.success("Removed tag %s from semantic project %s.", tag, name)


# endregion

# region Builds


def _render_build(build: SemanticBuild) -> None:
    if output.JSON_MODE:
        output.print_json(build.to_dict())
        return
    summary = (
        f"Build {build.id}: {build.status} "
        f"({build.error_count or 0} errors, {build.warning_count or 0} warnings)"
        + (f" at commit {build.git_commit}" if build.git_commit else "")
    )
    if build.status == "VALID":
        output.success(summary)
    elif build.status == "COMPILING":
        output.info(summary)
    else:
        output.error(summary)
    if build.issues:
        output.print_table(
            ["LEVEL", "RULE", "LOCATION", "ELEMENT", "MESSAGE"],
            [
                [
                    issue.level,
                    issue.rule or "-",
                    ":".join(
                        str(part)
                        for part in (issue.file, issue.line, issue.column)
                        if part is not None
                    )
                    or "-",
                    issue.element or "-",
                    issue.message,
                ]
                for issue in build.issues
            ],
        )


@semantic_group.command("compile")
@click.argument("name")
@click.option("--wait", is_flag=True, help="Wait for the compile and show its issues.")
@click.option(
    "--fail-on-error",
    is_flag=True,
    help="Exit 1 when the build is INVALID or FAILED, for CI. Implies --wait.",
)
@click.pass_context
def compile_cmd(ctx: click.Context, name: str, wait: bool, fail_on_error: bool) -> None:
    """Compile a semantic project's definitions into a new build.

    Args:
        ctx: Click context.
        name: Semantic project name.
        wait: Block until the compile ends.
        fail_on_error: Exit 1 on an INVALID or FAILED build.
    """
    sp = _semantic_project(ctx, name)
    wait = wait or fail_on_error
    try:
        build = sp.compile(wait=wait)
    except SemanticBuildException as exc:
        _render_build(exc.build)
        if fail_on_error:
            ctx.exit(1)
        return
    if wait and build.warning_count and build.issues is None:
        build = sp.get_build(build.id)
    _render_build(build)
    if not wait and not output.JSON_MODE:
        output.info(
            "Follow it with `hops semantic builds %s`, or pass --wait.", sp.name
        )


@semantic_group.command("builds")
@click.argument("name")
@click.pass_context
def builds_cmd(ctx: click.Context, name: str) -> None:
    """List a semantic project's builds, newest first; * marks the one answering queries.

    Args:
        ctx: Click context.
        name: Semantic project name.
    """
    sp = _semantic_project(ctx, name)
    builds = sp.get_builds()
    if output.JSON_MODE:
        output.print_json(
            [{**b.to_dict(), "current": b.id == sp.current_build_id} for b in builds]
        )
        return
    output.print_table(
        [
            "ID",
            "STATUS",
            "CURRENT",
            "COMMIT",
            "ERRORS",
            "WARNINGS",
            "CREATED",
            "CREATOR",
        ],
        [
            [
                b.id,
                b.status,
                "*" if b.id == sp.current_build_id else "",
                b.git_commit or "-",
                b.error_count or 0,
                b.warning_count or 0,
                b.created or "-",
                b.creator or "-",
            ]
            for b in builds
        ],
    )


@semantic_group.command("build")
@click.argument("name")
@click.argument("build_id", type=int)
@click.pass_context
def build_cmd(ctx: click.Context, name: str, build_id: int) -> None:
    """Show one build with its validation issues, each at its file and line.

    Args:
        ctx: Click context.
        name: Semantic project name.
        build_id: Build id, from `hops semantic builds`.
    """
    _render_build(_semantic_project(ctx, name).get_build(build_id))


@semantic_group.command("promote")
@click.argument("name")
@click.argument("build_id", type=int)
@click.pass_context
def promote_cmd(ctx: click.Context, name: str, build_id: int) -> None:
    """Make a valid build the one that answers queries.

    Args:
        ctx: Click context.
        name: Semantic project name.
        build_id: Id of a VALID build.
    """
    sp = _semantic_project(ctx, name).promote(build_id)
    if output.JSON_MODE:
        output.print_json(sp.to_dict())
        return
    output.success("Build %s now answers queries on %s.", sp.current_build_id, sp.name)


# endregion

# region Discovery


@semantic_group.command("metrics")
@click.argument("name")
@click.option(
    "--group-by",
    "group_by",
    multiple=True,
    callback=_split,
    help="Only metrics that can be grouped by all of these, comma-separated.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    help="Only metrics whose definitions set this tag schema; repeat for several, "
    "all of which must be set.",
)
@click.pass_context
def metrics_cmd(
    ctx: click.Context, name: str, group_by: list[str] | None, tags: tuple[str, ...]
) -> None:
    """List the metrics of a semantic project's current build, with the tag schemas each sets.

    Args:
        ctx: Click context.
        name: Semantic project name.
        group_by: Group-by names the metrics must support.
        tags: Tag schema names the metrics must set.
    """
    sp = _semantic_project(ctx, name)
    metrics = sp.list_metrics(tag=list(tags) or None)
    if group_by:
        compatible = set(sp.compatible_metrics(group_by))
        metrics = [m for m in metrics if m.name in compatible]
    if output.JSON_MODE:
        output.print_json([m.to_dict() for m in metrics])
        return
    output.print_table(
        ["NAME", "TYPE", "AGG", "LABEL", "TAGS", "DESCRIPTION"],
        [
            [
                m.name,
                m.type,
                m.agg or "-",
                m.label or "-",
                ",".join(sorted(m.tags or {})) or "-",
                output.first_line(m.description),
            ]
            for m in metrics
        ],
    )


@semantic_group.command("saved-queries")
@click.argument("name")
@click.pass_context
def saved_queries_cmd(ctx: click.Context, name: str) -> None:
    """List the saved queries of a semantic project's current build; run one with --saved-query on query or sql.

    Args:
        ctx: Click context.
        name: Semantic project name.
    """
    saved_queries = _semantic_project(ctx, name).list_saved_queries()
    if output.JSON_MODE:
        output.print_json([q.to_dict() for q in saved_queries])
        return
    output.print_table(
        ["NAME", "LABEL", "METRICS", "GROUP-BYS"],
        [
            [
                q.name,
                q.label or "-",
                ",".join(q.metrics) or "-",
                ",".join(q.group_by) or "-",
            ]
            for q in saved_queries
        ],
    )


@semantic_group.command("dimensions")
@click.argument("name")
@click.option(
    "--metrics",
    multiple=True,
    callback=_split,
    help="Only dimensions every one of these metrics supports, comma-separated.",
)
@click.pass_context
def dimensions_cmd(ctx: click.Context, name: str, metrics: list[str] | None) -> None:
    """List the dimensions metrics can be grouped by.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
    """
    dimensions = _semantic_project(ctx, name).dimensions(metrics=metrics)
    if output.JSON_MODE:
        output.print_json([d.to_dict() for d in dimensions])
        return
    output.print_table(
        ["NAME", "TYPE", "GRANULARITY", "DESCRIPTION"],
        [
            [
                d.name,
                d.type,
                d.granularity or "-",
                output.first_line(d.description),
            ]
            for d in dimensions
        ],
    )


@semantic_group.command("entities")
@click.argument("name")
@click.option(
    "--metrics",
    multiple=True,
    callback=_split,
    help="Only entities every one of these metrics can be grouped by, comma-separated.",
)
@click.pass_context
def entities_cmd(ctx: click.Context, name: str, metrics: list[str] | None) -> None:
    """List the entities: the keys that join semantic models, and group-bys too.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
    """
    entities = _semantic_project(ctx, name).entities(metrics=metrics)
    if output.JSON_MODE:
        output.print_json([e.to_dict() for e in entities])
        return
    output.print_table(
        ["NAME", "DESCRIPTION"],
        [[e.name, output.first_line(e.description)] for e in entities],
    )


@semantic_group.command("dimension-values")
@click.argument("name")
@click.argument("group_by")
@click.option(
    "--metrics",
    multiple=True,
    callback=_split,
    help="Only values that occur in the rows of these metrics, comma-separated.",
)
@click.option(
    "--limit", type=int, default=100, show_default=True, help="Most values to list."
)
@click.pass_context
def dimension_values_cmd(
    ctx: click.Context,
    name: str,
    group_by: str,
    metrics: list[str] | None,
    limit: int,
) -> None:
    """List the distinct values of a dimension, read on the query engine as you.

    Args:
        ctx: Click context.
        name: Semantic project name.
        group_by: Dimension name, such as customer__country.
        metrics: Metric names.
        limit: Most values to list.
    """
    values = _semantic_project(ctx, name).dimension_values(
        group_by, metrics=metrics, limit=limit
    )
    if output.JSON_MODE:
        output.print_json(values)
        return
    for value in values:
        click.echo("" if value is None else value)


# endregion

# region Queries


def _query_options(fn: Callable) -> Callable:
    """The query flags ``sql`` and ``query`` share, spelled as in ``mf query``."""
    options = [
        click.option(
            "--metrics",
            multiple=True,
            callback=_split,
            help="Metrics, comma-separated: --metrics revenue,order_count. "
            "Required unless --saved-query is given.",
        ),
        click.option(
            "--saved-query",
            "saved_query",
            help="Run a saved query (hops semantic saved-queries) instead of --metrics; "
            "--group-by, --order-by and --limit replace its own, --where adds to its filters. "
            '--group-by "" asks for its totals and --order-by "" drops its sort.',
        ),
        click.option(
            "--group-by",
            "group_by",
            multiple=True,
            callback=_split_given,
            help="Group-bys, comma-separated: --group-by metric_time__month,"
            "customer__country. Suffix a time dimension with a grain (__day ... "
            "__year) or a date part (__extract_dow).",
        ),
        click.option(
            "--where",
            multiple=True,
            help="Filter in MetricFlow's syntax, repeatable: "
            "--where \"{{ Dimension('customer__country') }} = 'SE'\".",
        ),
        click.option(
            "--order-by",
            "--order",
            "order_by",
            multiple=True,
            callback=_split_given,
            help="Columns to sort by, comma-separated; a leading - sorts "
            "descending: --order-by -revenue,metric_time__month.",
        ),
        click.option("--limit", type=int, help="Maximum number of rows."),
        click.option(
            "--start-time",
            help="Earliest metric_time as ISO-8601, inclusive and truncated to the "
            "query's time grain.",
        ),
        click.option(
            "--end-time",
            help="Latest metric_time as ISO-8601, inclusive and truncated to the "
            "query's time grain: a monthly query ending 2026-02-01 includes February.",
        ),
    ]
    for option in reversed(options):
        fn = option(fn)
    return fn


def _metrics_or_saved_query(
    metrics: list[str] | None, saved_query: str | None
) -> dict[str, Any]:
    if metrics and saved_query:
        raise click.UsageError(
            "Give --metrics or --saved-query, not both: a saved query names its own metrics."
        )
    if saved_query:
        return {"saved_query": saved_query}
    if not metrics:
        raise click.UsageError("Give --metrics, or --saved-query to run a saved query.")
    return {"metrics": metrics}


def _query_kwargs(
    metrics: list[str] | None,
    saved_query: str | None,
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    return {
        **_metrics_or_saved_query(metrics, saved_query),
        "group_by": group_by,
        "where": list(where) or None,
        "order_by": order_by,
        "limit": limit,
        "time_range": (start_time, end_time) if start_time or end_time else None,
    }


@semantic_group.command("sql")
@click.argument("name")
@_query_options
@click.pass_context
def sql_cmd(
    ctx: click.Context,
    name: str,
    metrics: list[str] | None,
    saved_query: str | None,
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
) -> None:
    """Print the SQL a metric query or a saved query compiles to, without running it.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
        saved_query: Saved query name, instead of metrics.
        group_by: Group-by names.
        where: Text filters.
        order_by: Sort columns.
        limit: Row limit.
        start_time: Earliest metric_time, inclusive.
        end_time: Latest metric_time, inclusive.
    """
    kwargs = _query_kwargs(
        metrics, saved_query, group_by, where, order_by, limit, start_time, end_time
    )
    sql = _semantic_project(ctx, name).sql(**kwargs)
    if output.JSON_MODE:
        output.print_json({"sql": sql})
        return
    click.echo(sql)


@semantic_group.command("query")
@click.argument("name")
@_query_options
@click.option(
    "--output",
    "-o",
    "fmt",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    show_default=True,
    help="How to print the rows; --json is the same as --output json.",
)
@click.option(
    "--explain",
    is_flag=True,
    help="Print the SQL instead of running it, as `hops semantic sql` does.",
)
@click.pass_context
def query_cmd(
    ctx: click.Context,
    name: str,
    metrics: list[str] | None,
    saved_query: str | None,
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
    fmt: str,
    explain: bool,
) -> None:
    """Run a metric query or a saved query as you on the query engine and print the rows.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
        saved_query: Saved query name, instead of metrics.
        group_by: Group-by names.
        where: Text filters.
        order_by: Sort columns.
        limit: Row limit.
        start_time: Earliest metric_time, inclusive.
        end_time: Latest metric_time, inclusive.
        fmt: table, json or csv.
        explain: Print the SQL instead of the rows.
    """
    if explain:
        ctx.invoke(
            sql_cmd,
            name=name,
            metrics=metrics,
            saved_query=saved_query,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        return
    import pandas as pd

    kwargs = _query_kwargs(
        metrics, saved_query, group_by, where, order_by, limit, start_time, end_time
    )
    df = _semantic_project(ctx, name).query(**kwargs, dataframe_type="pandas")
    if output.JSON_MODE or fmt == "json":
        # NaN is not JSON; a missing value prints as null.
        output.print_json(
            df.astype(object).where(df.notna(), None).to_dict(orient="records")
        )
    elif fmt == "csv":
        df.to_csv(sys.stdout, index=False)
    else:
        output.print_table(
            [str(column) for column in df.columns],
            [
                ["" if pd.isna(value) else str(value) for value in row]
                for row in df.itertuples(index=False)
            ],
        )


# endregion

# region Superset


@semantic_group.command("dashboard")
@click.argument("name")
@click.option(
    "--metrics",
    multiple=True,
    callback=_split,
    help="Metrics to chart, one to eight, comma-separated: --metrics revenue,orders. "
    "Required unless --saved-query is given.",
)
@click.option(
    "--group-by",
    "group_by",
    multiple=True,
    callback=_split,
    help="Dimensions for the breakdown table and the native filters, comma-separated; "
    "default the first categorical dimension the metrics share.",
)
@click.option(
    "--saved-query",
    "saved_query",
    help="Chart a saved query's metrics and group-bys, over its own view, instead of "
    "--metrics and --group-by.",
)
@click.option(
    "--title",
    help="Dashboard title; default '<semantic project>: <metric labels>', or "
    "'<semantic project>: <saved query label>' with --saved-query. "
    "A dashboard of yours with this title is updated in place.",
)
@click.pass_context
def dashboard_cmd(
    ctx: click.Context,
    name: str,
    metrics: list[str] | None,
    group_by: list[str] | None,
    saved_query: str | None,
    title: str | None,
) -> None:
    """Build a Superset dashboard of metrics, or of a saved query, over the semantic project's view, and print its URL.

    Needs Superset 7.0 with semantic dashboards on. The dashboard has a line chart per
    metric over metric_time, a big number for the first metric, a breakdown table and
    native filters, and you own it.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
        group_by: Dimensions for the breakdown table and the filters.
        saved_query: Saved query name, instead of metrics and group-bys.
        title: Dashboard title.
    """
    _metrics_or_saved_query(metrics, saved_query)
    if saved_query and group_by:
        raise click.UsageError(
            "--group-by does not combine with --saved-query, which names its own group-bys."
        )
    sp = _semantic_project(ctx, name)
    if saved_query:
        dashboard = sp.create_dashboard(saved_query=saved_query, title=title)
    else:
        dashboard = sp.create_dashboard(metrics, group_by=group_by, title=title)
    if output.JSON_MODE:
        output.print_json(dashboard)
        return
    click.echo(
        f'Created dashboard {dashboard["id"]} "{dashboard["title"]}": {dashboard["url"]}'
    )


# endregion

# region Tests


def _await_serving(sp: SemanticProject, build_id: int) -> int:
    """Wait for the gateway to serve `build_id`; return the build it serves when the wait ends."""
    deadline = time.monotonic() + _SERVING_WAIT_SECONDS
    served = sp.serving().build_id
    while served != build_id and time.monotonic() < deadline:
        time.sleep(_SERVING_POLL_SECONDS)
        served = sp.serving().build_id
    return served


@semantic_group.command("test")
@click.argument("name")
@click.option(
    "--metrics",
    multiple=True,
    callback=_split,
    help="Only these metrics, comma-separated; every metric by default.",
)
@click.option(
    "--no-compile",
    is_flag=True,
    help="Test the build that answers queries now, without compiling first.",
)
@click.pass_context
def test_cmd(
    ctx: click.Context, name: str, metrics: list[str] | None, no_compile: bool
) -> None:
    """Compile, then run every metric once on the query engine; exit 1 if anything fails.

    A compile checks the definitions against the feature groups; running each metric's
    SQL as you also catches what only the query engine sees, such as a type it cannot
    aggregate or a feature group you cannot read. Each metric runs ungrouped, so it reads
    its whole feature groups once: narrow it with --metrics on large data. A metric that
    requires metric_time, such as a cumulative metric with a window or a derived metric
    with an offset, runs grouped by metric_time at its default grain instead.
    The metrics run on the build just compiled, so the command exits 1 when that build does not answer queries: a PINNED project before its promote, or a gateway still serving the previous build.
    --no-compile tests the build that answers queries now.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names to run.
        no_compile: Skip the compile.
    """
    sp = _semantic_project(ctx, name)
    if not no_compile:
        try:
            build = sp.compile(wait=True)
        except SemanticBuildException as exc:
            _render_build(exc.build)
            ctx.exit(1)
        if not output.JSON_MODE:
            _render_build(build)
        if sp.current_build_id != build.id:
            output.error(
                "Build %s does not answer queries until it is promoted "
                "(hops semantic promote %s %s); --no-compile tests build %s, which does.",
                build.id,
                name,
                build.id,
                sp.current_build_id,
            )
            ctx.exit(1)
        served = _await_serving(sp, build.id)
        if served != build.id:
            output.error(
                "The gateway still serves build %s, not build %s, after %s s; "
                "run the test again once it serves the new build.",
                served,
                build.id,
                _SERVING_WAIT_SECONDS,
            )
            ctx.exit(1)
    defined = sp.list_metrics()
    names = metrics or [m.name for m in defined]
    by_time = {m.name for m in defined if m.requires_metric_time}
    results = []
    for metric in names:
        group_by = ["metric_time"] if metric in by_time else None
        try:
            rows = sp.query(
                [metric], group_by=group_by, limit=1, dataframe_type="python"
            )
            results.append({"metric": metric, "status": "PASS", "rows": len(rows)})
        except SemanticLayerException as exc:
            results.append(
                {"metric": metric, "status": "FAIL", "error": exc.error_msg or str(exc)}
            )
    failed = [r for r in results if r["status"] == "FAIL"]
    if output.JSON_MODE:
        output.print_json(results)
    else:
        output.print_table(
            ["METRIC", "STATUS", "DETAIL"],
            [
                [
                    r["metric"],
                    r["status"],
                    output.first_line(r.get("error")) if "error" in r else "",
                ]
                for r in results
            ],
        )
        summary = f"{len(results) - len(failed)} of {len(results)} metrics ran"
        if failed:
            output.error(summary)
        else:
            output.success(summary)
    if failed:
        ctx.exit(1)


# endregion

# region Time spines


@semantic_group.group("time-spine")
def time_spine_group() -> None:
    """Time spine feature groups, which cumulative metrics, offsets and custom granularities join to."""


def _parse_custom(
    _ctx: click.Context, _param: click.Parameter, values: tuple[str, ...]
) -> dict[str, str] | None:
    """Read repeated ``NAME=FREQ`` options into custom granularities."""
    custom = {}
    for value in values:
        name, sep, freq = value.partition("=")
        if not sep or not name.strip() or not freq.strip():
            raise click.BadParameter(
                f"expected NAME=FREQ, got {value!r}", param_hint="--custom"
            )
        custom[name.strip()] = freq.strip()
    return custom or None


@time_spine_group.command(
    "create",
    epilog="""\b
Examples:
  hops semantic time-spine create --start 2020-01-01 --end 2030-12-31
      feature group time_spine_day, version 1, column date_day
  hops semantic time-spine create --start 2020-01-01 --end 2030-12-31 \\
      --custom fiscal_quarter=Q-JAN --custom fiscal_year=Y-JAN
      adds the start of each row's fiscal quarter and year, for a year ending in January
""",
)
@click.option(
    "--start",
    required=True,
    help="First day of the spine, ISO-8601; it moves back to the start of its period.",
)
@click.option("--end", required=True, help="Last day of the spine, ISO-8601, included.")
@click.option(
    "--grain",
    type=click.Choice(_SPINE_GRAINS),
    default="day",
    show_default=True,
    help="One row per period of this grain; weeks start on Monday.",
)
@click.option(
    "--custom",
    multiple=True,
    callback=_parse_custom,
    metavar="NAME=FREQ",
    help="A custom granularity column, repeatable: FREQ is a pandas period frequency "
    "such as Q-JAN, and each row holds the start of its period at that frequency.",
)
@click.pass_context
def time_spine_create_cmd(
    ctx: click.Context,
    start: str,
    end: str,
    grain: str,
    custom: dict[str, str] | None,
) -> None:
    """Write the time spine feature group time_spine_<grain>, version 1: one row per period from --start through --end.

    A semantic project's time spine model at that grain binds it by name. On an
    existing spine the rows are inserted, so a wider range extends it.

    Args:
        ctx: Click context.
        start: First day of the spine.
        end: Last day of the spine.
        grain: Grain of the spine.
        custom: Custom granularity names to pandas period frequencies.
    """
    try:
        fg = _api(ctx).create_time_spine(
            start, end, granularity=grain, custom_granularities=custom
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    columns = [feature.name for feature in fg.features or []]
    if output.JSON_MODE:
        output.print_json(
            {
                "feature_group": fg.name,
                "version": fg.version,
                "granularity": grain,
                "start": start,
                "end": end,
                "columns": columns,
            }
        )
        return
    output.success(
        "Wrote the %s time spine from %s to %s to feature group %s version %s (%s).",
        grain,
        start,
        end,
        fg.name,
        fg.version,
        ", ".join(columns),
    )


# endregion

# region Scaffold

_DBT_PROJECT_YML = """\
name: {name}
version: "1.0.0"
config-version: 2
profile: hopsworks
model-paths: ["models"]
"""
_NUMERIC_TYPES = frozenset(
    {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "float",
        "double",
        "real",
        "decimal",
    }
)
_STRING_TYPES = frozenset({"string", "varchar", "char"})
_TIME_TYPES = frozenset({"timestamp", "date"})
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
# The words the semantic layer's expression parser (Calcite 1.42) does not read as a column name, such as
# `value` or `date`; a feature named so is quoted in its `expr`.
_SQL_KEYWORDS = frozenset(
    """
    abs all allocate allow alter and any are array array_max_cardinality as asensitive
    asof asymmetric at atomic authorization avg begin begin_frame begin_partition
    between bigint binary bit blob boolean both by call called cardinality cascaded
    case cast ceil ceiling char char_length character character_length check classifier
    clob close coalesce collate collect column commit condition connect constraint
    contains convert corr corresponding count covar_pop covar_samp create cross cube
    cume_dist current current_row current_transform_group_for_type cursor cycle date
    datetime day deallocate dec decimal declare default define delete dense_rank deref
    describe deterministic disallow disconnect distinct double drop dynamic each
    element else empty end end_frame end_partition equals escape every except exec
    execute exists exp explain extend external extract false fetch filter first_value
    float floor for foreign frame_row free friday from full function fusion get global
    grant group grouping groups having hold hour identity import in indicator initial
    inner inout insensitive insert int integer intersect intersection interval into is
    join json_array json_arrayagg json_exists json_object json_objectagg json_query
    json_scope json_value lag language large last_value lateral lead leading left like
    like_regex limit ln local lower match match_condition match_number match_recognize
    matches max measure measures member merge method min minus minute mod modifies
    module monday month multiset national natural nchar nclob new next no none
    normalize not nth_value ntile null nullif numeric occurrences_regex octet_length of
    offset old omit on one only open or order ordinal out outer over overlaps overlay
    parameter partition pattern per percent percent_rank percentile_cont
    percentile_disc period permute portion position position_regex power precedes
    precision prepare prev primary procedure qualify range rank reads real recursive
    ref references referencing regr_avgx regr_avgy regr_count regr_intercept regr_r2
    regr_slope regr_sxx regr_sxy regr_syy release reset result return returns revoke
    right rollback rollup row row_number rows running safe_cast safe_offset
    safe_ordinal saturday savepoint scope scroll search second seek select sensitive
    set show similar skip smallint some specific specifictype sql sqlexception sqlstate
    sqlwarning sqrt start static stddev_pop stddev_samp stream submultiset subset
    substring substring_regex succeeds sum sunday symmetric system system_time table
    tablesample then thursday time timestamp timezone_hour timezone_minute tinyint to
    trailing translate translate_regex translation treat trigger trim trim_array true
    truncate try_cast tuesday uescape union unique unknown unnest unsigned update upper
    upsert using uuid value value_of values var_pop var_samp varbinary varchar variant
    varying versioning wednesday when whenever where width_bucket window with within
    without year
    """.split()  # noqa: SIM905 - one word per line would take 368 lines
)
# The compiler's NAMING rule refuses these names and the time grains, and its RESERVED_KEYWORD rule these
# words as entity, dimension and measure names (hopsworks-semantic validate/Validator.java).
_RESERVED_NAMES = frozenset({"metric_time", "mf_internal_uuid"}) | _GRAINS
_ELEMENT_KEYWORDS = frozenset(
    """
    and as create distinct for from full having in inner into is join left like natural not
    null on or right select union using where with
    """.split()  # noqa: SIM905
)


def _fg_ref(ref: str) -> tuple[str, int | None]:
    name, sep, version = ref.rpartition(":")
    if not sep:
        return ref, None
    if not version.isdigit():
        raise click.BadParameter(
            f"expected FG or FG:VERSION, got {ref!r}", param_hint="--from-fg"
        )
    return name, int(version)


def _base_type(hive_type: str | None) -> str:
    return re.split(r"[(<]", (hive_type or "").lower(), maxsplit=1)[0].strip()


def _snake(raw: str) -> str:
    """``raw`` in lower snake case, starting with a letter and ending with a letter or digit, with no ``__``."""
    name = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return name if name[:1].isalpha() else f"f_{name}".rstrip("_")


def _valid_name(name: str) -> bool:
    """Whether a ``_snake`` name passes the compiler's NAMING and RESERVED_KEYWORD rules."""
    return (
        len(name) > 1 and name not in _RESERVED_NAMES and name not in _ELEMENT_KEYWORDS
    )


def _element_name(raw: str) -> str:
    """``raw`` as a name the compiler accepts, suffixed ``_value`` when it is too short, reserved or an SQL keyword."""
    name = _snake(raw)
    return name if _valid_name(name) else f"{name}_value"


def _column(raw: str) -> str:
    """A feature as an ``expr``: bare when it parses as a column name, else a quoted identifier in a YAML string."""
    if _IDENTIFIER.match(raw) and raw not in _SQL_KEYWORDS:
        return raw
    quoted = '"' + raw.replace('"', '""') + '"'
    return "'" + quoted.replace("'", "''") + "'"


def _claim(name: str, prefix: str, free: Callable[[str], bool]) -> str:
    """``name``, or ``<prefix>_<name>`` when it is not free, numbered when that is not free either."""
    candidate, n = name, 1
    while not free(candidate):
        candidate = f"{prefix}_{name}" if n == 1 else f"{prefix}_{name}_{n}"
        n += 1
    return candidate


def _entity_name(column: str, model: str) -> str:
    """``customer`` for a key ``customer_id``, the model for ``id``; the whole column when the stem is not a valid name."""
    if column == "id":
        return model
    stem = _snake(column[:-3]) if column.endswith("_id") else ""
    return stem if _valid_name(stem) else _element_name(column)


def _label(name: str) -> str:
    return name.replace("_", " ").capitalize()


def _scaffold(
    name: str, feature_groups: Sequence[Any], own_feature_store: str | None = None
) -> dict[str, str]:
    """The files of a dbt project with one semantic model per feature group, keyed by their path in the project.

    Each model pins its feature group version in ``config.meta.hopsworks``, so a new version changes no metric until the YAML does.
    A single-column primary key is the primary entity, and a column named like another model's single-column key a foreign entity; any other key makes the model its own ``primary_entity``.
    A ``timestamp`` or ``date`` event time is the time dimension, with day grain; without one the model has no measures, since every measure needs a time dimension.
    String features are categorical dimensions; the measures are a row count and a sum per other numeric feature.
    One simple metric per measure is written commented out, for the author to choose, name and describe.
    Element names are the column names in lower snake case, suffixed ``_value`` when the compiler refuses the name itself (one character, a time grain, ``metric_time`` or an SQL keyword).
    A name another element already has, here or as another kind of element in another model, or a dimension name another model with the same primary entity has, is prefixed with the model's name.

    Args:
        name: The dbt project's name.
        feature_groups: Feature groups, with ``name``, ``version``, ``features``, ``primary_key``, ``event_time``, ``description`` and ``feature_store_name``.
        own_feature_store: The project's feature store; a feature group in another is named with its feature store.

    Returns:
        ``dbt_project.yml`` and ``models/semantic.yml``.
    """
    models: list[tuple[Any, str]] = []
    names: set[str] = set()
    for fg in feature_groups:
        model = _element_name(fg.name)
        if model in names:
            model = _claim(f"{model}_v{fg.version}", model, lambda c: c not in names)
        names.add(model)
        models.append((fg, model))
    join_keys = {
        fg.primary_key[0]: _entity_name(fg.primary_key[0], model)
        for fg, model in models
        if len(fg.primary_key or []) == 1
    }
    # Reserved first, so a dimension or measure of an earlier model cannot take a name a join needs.
    kinds = dict.fromkeys(join_keys.values(), "entity")
    pairs: set[tuple[str, str]] = set()
    metrics: list[str] = []
    lines = ["version: 2", ""]
    if not models:
        lines.append(
            "# Add one semantic model per feature group under `semantic_models:`, and the metrics under"
        )
        lines.append("# `metrics:`; the hops-semantic skill has the format.")
    else:
        lines.append("semantic_models:")
    for fg, model in models:
        lines += _semantic_model(
            fg, model, join_keys, kinds, pairs, metrics, own_feature_store
        )
    if metrics:
        lines += [
            "",
            "# One simple metric per measure. Uncomment the ones to serve, with the `metrics:` line, give them",
            f"# labels and descriptions, then run `hops semantic compile {name} --wait`.",
            "# A metric carries schematised tags in config.meta.hopsworks.tags, tag schema name to value.",
            "# metrics:",
            *metrics,
        ]
    return {
        "dbt_project.yml": _DBT_PROJECT_YML.format(name=name),
        "models/semantic.yml": "\n".join(lines) + "\n",
    }


def _semantic_model(
    fg: Any,
    model: str,
    join_keys: dict[str, str],
    kinds: dict[str, str],
    pairs: set[tuple[str, str]],
    metrics: list[str],
    own_feature_store: str | None,
) -> list[str]:
    """The YAML lines of one semantic model; ``kinds`` and ``pairs``, every model's element names and (primary entity, dimension) pairs, gain this model's."""
    types = {f.name: _base_type(f.type) for f in fg.features}
    descriptions = {f.name: f.description for f in fg.features if f.description}
    primary_key = list(fg.primary_key or [])
    event_time = fg.event_time if types.get(fg.event_time) in _TIME_TYPES else None
    taken: set[str] = set()
    primary = model

    def claim(name: str, kind: str) -> str:
        def free(candidate: str) -> bool:
            other = kinds.get(candidate)
            return (
                candidate not in taken
                and (other is None or (other == kind and kind != "measure"))
                and (
                    kind not in ("time", "categorical")
                    or (primary, candidate) not in pairs
                )
            )

        claimed = _claim(name, model, free)
        taken.add(claimed)
        kinds[claimed] = kind
        if kind in ("time", "categorical"):
            pairs.add((primary, claimed))
        return claimed

    entities = []
    primary_entity = None
    if len(primary_key) == 1:
        primary = claim(_entity_name(primary_key[0], model), "entity")
        entities.append((primary, "primary", primary_key[0]))
    else:
        primary_entity = primary = claim(model, "entity")
    entities += [
        (claim(join_keys[column], "entity"), "foreign", column)
        for column in types
        if column in join_keys and primary_key != [column]
    ]
    keys = set(primary_key) | {column for _, _, column in entities}
    time_dimension = claim(_element_name(event_time), "time") if event_time else None
    dimensions = [
        (claim(_element_name(column), "categorical"), column)
        for column, kind in types.items()
        if kind in _STRING_TYPES and column not in keys
    ]

    lines = ["", f"  - name: {model}"]
    if fg.description:
        lines.append(f"    description: {json.dumps(fg.description)}")
    lines += [
        f"    model: ref('{fg.name}')",
        "    config:",
        "      meta:",
        "        hopsworks:",
        f"          feature_group: {fg.name}",
        f"          version: {fg.version}",
    ]
    feature_store = getattr(fg, "feature_store_name", None)
    if feature_store and own_feature_store and feature_store != own_feature_store:
        lines.append(f"          feature_store: {feature_store}")
    if time_dimension:
        lines += ["    defaults:", f"      agg_time_dimension: {time_dimension}"]
    if primary_entity:
        lines.append(f"    primary_entity: {primary_entity}")
    if entities:
        lines.append("    entities:")
        for entity, kind, column in entities:
            lines += [
                f"      - name: {entity}",
                f"        type: {kind}",
                f"        expr: {_column(column)}",
            ]
    if time_dimension or dimensions:
        lines.append("    dimensions:")
    if time_dimension:
        lines += [
            f"      - name: {time_dimension}",
            "        type: time",
            f"        expr: {_column(event_time)}",
            "        type_params:",
            "          time_granularity: day",
        ]
    for dimension, column in dimensions:
        lines += [
            f"      - name: {dimension}",
            "        type: categorical",
            f"        expr: {_column(column)}",
        ]
        if column in descriptions:
            lines.append(f"        description: {json.dumps(descriptions[column])}")

    if not event_time:
        reason = (
            f"its event time {fg.event_time} is not a timestamp or date"
            if fg.event_time
            else "the feature group has no event time"
        )
        lines.append(
            f"    # No measures: {reason}, and every measure needs a time dimension."
        )
        return lines
    measures = [
        (
            f"{model}_count",
            "count",
            primary_key[0] if primary_key else event_time,
            f"Rows of feature group {fg.name}.",
        )
    ]
    measures += [
        (_element_name(column), "sum", column, descriptions.get(column))
        for column, kind in types.items()
        if kind in _NUMERIC_TYPES and column not in keys
    ]
    lines.append("    measures:")
    for measure, agg, column, description in measures:
        claimed = claim(measure, "measure")
        lines += [
            f"      - name: {claimed}",
            f"        agg: {agg}",
            f"        expr: {_column(column)}",
        ]
        if description:
            lines.append(f"        description: {json.dumps(description)}")
        metrics += [
            f"#   - name: {claimed}",
            f"#     label: {_label(claimed)}",
            "#     type: simple",
            "#     type_params:",
            f"#       measure: {claimed}",
        ]
    return lines


def _write_local(directory: Path, files: dict[str, str]) -> Path:
    for relative, content in files.items():
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return directory


def _ensure_dir(datasets: Any, directory: str) -> None:
    parts = directory.strip("/").split("/")
    # /Projects/<project> exists; each directory below it is created when missing.
    for depth in range(3, len(parts) + 1):
        current = "/" + "/".join(parts[:depth])
        if not datasets.exists(current):
            datasets.mkdir(current)


def _write_scaffold(ctx: click.Context, path: str, files: dict[str, str]) -> str:
    """Upload the scaffold to the HopsFS directory ``path``, which must not exist; return its absolute path."""
    absolute = _api(ctx)._absolute_path(path)
    datasets = session.get_project(ctx).get_dataset_api()
    if datasets.exists(absolute):
        raise click.ClickException(
            f"{absolute} already exists. Pass another --path, or register it as it is "
            f"with `hops semantic create <name> --path {path}`."
        )
    parent, _, leaf = absolute.rpartition("/")
    _ensure_dir(datasets, parent)
    with tempfile.TemporaryDirectory() as tmp:
        datasets.upload(str(_write_local(Path(tmp) / leaf, files)), parent)
    return absolute


def _sh(
    args: list[str], cwd: str | Path | None = None, timeout: float = 120
) -> subprocess.CompletedProcess:
    """Run a program with its output captured; a missing program exits 127 and a timeout 124, as in a shell."""
    try:
        return subprocess.run(
            args, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except FileNotFoundError:
        return subprocess.CompletedProcess(args, 127, "", f"{args[0]}: not found")
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args, 124, "", f"{' '.join(args[:3])} timed out after {timeout:.0f} s"
        )


def _checked(args: list[str], cwd: str | Path | None = None) -> str:
    proc = _sh(args, cwd=cwd)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise click.ClickException(f"`{' '.join(args[:3])}` failed: {detail}")
    return proc.stdout.strip()


def _clone(
    ctx: click.Context, url: str, provider: str | None, init_name: str | None = None
) -> Any:
    """Clone ``url`` through Hopsworks git into Resources/semantic/; return the git repository."""
    project = session.get_project(ctx)
    parent = _api(ctx)._absolute_path(_SEMANTIC_DIR)
    _ensure_dir(project.get_dataset_api(), parent)
    try:
        # GitApi prints where to follow the clone; stdout is kept for the command's own output.
        with contextlib.redirect_stdout(sys.stderr):
            return project.get_git_api()._clone(
                url, parent, provider=provider, timeout=_CLONE_TIMEOUT_SECONDS
            )
    except GitException as exc:
        resume = (
            f" The repository exists; register it with `hops semantic init {init_name} --repo {url}`."
            if init_name
            else ""
        )
        raise click.ClickException(f"Cloning {url} failed: {exc}.{resume}") from exc


def _free_repo_name(owner: str, name: str, datasets: Any, parent: str) -> str:
    """``hops-semantic-<name>``, or the first ``-<n>`` after it that is free on GitHub and in ``parent``."""
    base = f"hops-semantic-{name}"
    for n in range(1, 100):
        candidate = base if n == 1 else f"{base}-{n}"
        taken = _sh(["gh", "repo", "view", f"{owner}/{candidate}", "--json", "name"])
        if taken.returncode != 0 and not datasets.exists(f"{parent}/{candidate}"):
            return candidate
    raise click.ClickException(f"No free repository name after {base}-99.")


def _new_repo(
    ctx: click.Context, name: str, files: dict[str, str], yes: bool
) -> tuple[str, int, str]:
    """Push the scaffold to a new private GitHub repository and clone it through Hopsworks git.

    Returns:
        The clone's HopsFS path, its git repository id, and the repository URL.
    """
    status = _sh(["gh", "auth", "status"])
    if status.returncode != 0:
        raise click.ClickException(
            "--repo new needs the GitHub CLI logged in, and `gh auth status` failed: "
            f"{(status.stderr or status.stdout).strip()} Log in with `gh auth login` "
            "(`github-login` in a Hopsworks terminal) and run init again."
        )
    owner = _checked(["gh", "api", "user", "--jq", ".login"])
    parent = _api(ctx)._absolute_path(_SEMANTIC_DIR)
    repo = _free_repo_name(
        owner, name, session.get_project(ctx).get_dataset_api(), parent
    )
    url = f"https://github.com/{owner}/{repo}.git"
    register = git_cmd.find_provider("GitHub", "github.com") is None
    if not yes and output.JSON_MODE:
        # JSON mode cannot ask, and creating a repository or storing a token needs consent.
        raise click.ClickException(
            "--repo new creates a GitHub repository and may register your GitHub token; "
            "with --json, pass --yes to confirm both."
        )
    if not yes:
        click.confirm(
            f"Create the private GitHub repository {owner}/{repo} with the scaffold, "
            f"and clone it into {parent}/{repo}?",
            abort=True,
        )
        if register:
            click.confirm(
                "Hopsworks clones a private repository with the GitHub token registered "
                "in your account, and you have none. Register your GitHub CLI token "
                "(`gh auth token`) as it?",
                abort=True,
            )
    if register:
        token = _sh(["gh", "auth", "token"])
        if token.returncode != 0 or not token.stdout.strip():
            raise click.ClickException(
                f"`gh auth token` failed: {token.stderr.strip()}"
            )
        git_cmd.register_provider("GitHub", owner, token.stdout.strip(), "github.com")
        output.info(
            "Registered your GitHub CLI token for github.com (username %s).", owner
        )
    with tempfile.TemporaryDirectory() as tmp:
        local = _write_local(Path(tmp) / repo, files)
        _checked(["git", "init", "--initial-branch=main"], cwd=local)
        _checked(["git", "add", "-A"], cwd=local)
        identity = []
        if _sh(["git", "config", "user.email"], cwd=local).returncode != 0:
            identity = [
                "-c",
                f"user.name={owner}",
                "-c",
                f"user.email={owner}@users.noreply.github.com",
            ]
        _checked(
            ["git", *identity, "commit", "-m", f"Scaffold semantic project {name}"],
            cwd=local,
        )
        _checked(
            [
                "gh",
                "repo",
                "create",
                f"{owner}/{repo}",
                "--private",
                "--source",
                ".",
                "--remote",
                "origin",
                "--push",
            ],
            cwd=local,
        )
    output.info("Created %s.", url)
    clone = _clone(ctx, url, "GitHub", init_name=name)
    return clone.path, clone.id, url


# endregion
