"""``hops semantic``: Semantic Layer projects, builds, discovery and metric queries.

Wraps ``project.get_semantic_layer_api()``. The query verbs follow MetricFlow's
``mf`` CLI: metrics and group-bys are comma-separated, ``--where`` repeats, and
``--order-by`` (alias ``--order``) sorts descending on a leading ``-``.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import click
from hopsworks.cli import output, session
from hopsworks_common.client.exceptions import SemanticBuildException


if TYPE_CHECKING:
    from collections.abc import Callable

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


def _build_label(build: SemanticBuild | None) -> str:
    return f"{build.id} {build.status}" if build else "-"


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
@click.pass_context
def create_cmd(
    ctx: click.Context, name: str, root_path: str, fmt: str, mode: str
) -> None:
    """Register a directory of semantic definitions and start its first compile.

    Args:
        ctx: Click context.
        name: Semantic project name.
        root_path: Directory of the definitions.
        fmt: Format of the definitions.
        mode: Deployment mode.
    """
    sp = _api(ctx).create_project(
        name, root_path, format=fmt.upper(), deployment_mode=mode.upper()
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
@click.pass_context
def metrics_cmd(ctx: click.Context, name: str) -> None:
    """List the metrics of a semantic project's current build.

    Args:
        ctx: Click context.
        name: Semantic project name.
    """
    metrics = _semantic_project(ctx, name).list_metrics()
    if output.JSON_MODE:
        output.print_json([m.to_dict() for m in metrics])
        return
    output.print_table(
        ["NAME", "TYPE", "AGG", "LABEL", "DESCRIPTION"],
        [
            [
                m.name,
                m.type,
                m.agg or "-",
                m.label or "-",
                output.first_line(m.description),
            ]
            for m in metrics
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


# endregion

# region Queries


def _query_options(fn: Callable) -> Callable:
    """The query flags ``sql`` and ``query`` share, spelled as in ``mf query``."""
    options = [
        click.option(
            "--metrics",
            multiple=True,
            required=True,
            callback=_split,
            help="Metrics, comma-separated: --metrics revenue,order_count.",
        ),
        click.option(
            "--group-by",
            "group_by",
            multiple=True,
            callback=_split,
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
            callback=_split,
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


def _query_kwargs(
    metrics: list[str],
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    return {
        "metrics": metrics,
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
    metrics: list[str],
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
) -> None:
    """Print the SQL a metric query compiles to, without running it.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
        group_by: Group-by names.
        where: Text filters.
        order_by: Sort columns.
        limit: Row limit.
        start_time: Earliest metric_time, inclusive.
        end_time: Latest metric_time, inclusive.
    """
    sql = _semantic_project(ctx, name).sql(
        **_query_kwargs(metrics, group_by, where, order_by, limit, start_time, end_time)
    )
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
@click.pass_context
def query_cmd(
    ctx: click.Context,
    name: str,
    metrics: list[str],
    group_by: list[str] | None,
    where: tuple[str, ...],
    order_by: list[str] | None,
    limit: int | None,
    start_time: str | None,
    end_time: str | None,
    fmt: str,
) -> None:
    """Run a metric query as you on the query engine and print the rows.

    Args:
        ctx: Click context.
        name: Semantic project name.
        metrics: Metric names.
        group_by: Group-by names.
        where: Text filters.
        order_by: Sort columns.
        limit: Row limit.
        start_time: Earliest metric_time, inclusive.
        end_time: Latest metric_time, inclusive.
        fmt: table, json or csv.
    """
    import pandas as pd

    df = _semantic_project(ctx, name).query(
        **_query_kwargs(
            metrics, group_by, where, order_by, limit, start_time, end_time
        ),
        dataframe_type="pandas",
    )
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
