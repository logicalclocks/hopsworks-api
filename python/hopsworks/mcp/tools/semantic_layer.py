#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Semantic layer tools: discover and query the metrics a project defines over its feature groups.

The names and arguments follow dbt-mcp's semantic layer tools, so an agent written for dbt finds the same tools here.
Tabular answers are CSV, cut at `MAX_RESPONSE_CHARS` characters, as dbt-mcp does.
"""

from __future__ import annotations

import csv
import io
import json
from typing import TYPE_CHECKING, Any

import hopsworks
from fastmcp import (
    Context,  # noqa: TC002
)
from hopsworks.mcp.models.semantic_layer import (
    SemanticBuild,
    SemanticIssue,
    SemanticProject,
    Tag,
)
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common.client.exceptions import (
    SemanticBuildException,
    SemanticLayerException,
)


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from fastmcp import FastMCP
    from hopsworks_common import semantic_layer


MAX_RESPONSE_CHARS = 50_000


def _cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, default=str) if value else ""
    return value


def _cut_notice() -> str:
    return (
        f"... cut at {MAX_RESPONSE_CHARS} characters: "
        "ask for fewer rows with a limit, filters or fewer group-bys.\n"
    )


def _capped(text: str) -> str:
    """``text`` cut at the last line within MAX_RESPONSE_CHARS, then a line saying so, when it is longer."""
    if len(text) <= MAX_RESPONSE_CHARS:
        return text
    cut = text.rfind("\n", 0, MAX_RESPONSE_CHARS) + 1
    return text[:cut] + _cut_notice()


def _csv(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """CSV of the whole rows within MAX_RESPONSE_CHARS, then a line saying so when some were left out.

    A quoted cell can hold newlines, so the cut falls between rows, never at a line inside one.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        line = io.StringIO()
        csv.writer(line, lineterminator="\n").writerow([_cell(value) for value in row])
        if buffer.tell() + len(line.getvalue()) > MAX_RESPONSE_CHARS:
            return buffer.getvalue() + _cut_notice()
        buffer.write(line.getvalue())
    return buffer.getvalue()


def _semantic_project(name: str) -> semantic_layer.SemanticProject:
    try:
        project = hopsworks.get_current_project()
    except hopsworks.ProjectException:
        raise RuntimeError(
            "No active Hopsworks project found, use login tool."
        ) from None
    sp = project.get_semantic_layer_api().get_project(name)
    if sp is None:
        raise ValueError(
            f"No semantic project named '{name}'; list_semantic_projects lists them."
        )
    return sp


def _build(sp: semantic_layer.SemanticProject) -> SemanticBuild:
    try:
        build = sp.compile(wait=True)
    except SemanticBuildException as exc:
        build = exc.build
    if build.warning_count and build.issues is None:
        build = sp.get_build(build.id)
    return SemanticBuild(
        build_id=build.id,
        status=build.status,
        error_count=build.error_count,
        warning_count=build.warning_count,
        issues=[SemanticIssue(**issue.to_dict()) for issue in build.issues or []],
    )


class SemanticLayerTools:
    """Tools for the semantic layer: governed metrics over feature groups, discovered and queried by dimension and time grain."""

    def __init__(self, mcp: FastMCP):
        """Register the semantic layer tools.

        Parameters:
            mcp: The MCP server instance.
        """
        self.mcp = mcp
        for tool in (
            self.list_semantic_projects,
            self.list_metrics,
            self.get_dimensions,
            self.get_entities,
            self.get_dimension_values,
            self.query_metrics,
            self.get_metrics_compiled_sql,
            self.get_semantic_project_tags,
        ):
            self.mcp.tool(tags=[TAGS.SEMANTIC_LAYER, TAGS.READ, TAGS.STATEFUL])(tool)
        for tool in (self.validate_semantic_project, self.compile_semantic_project):
            self.mcp.tool(tags=[TAGS.SEMANTIC_LAYER, TAGS.WRITE, TAGS.STATEFUL])(tool)

    async def list_semantic_projects(
        self, ctx: Context | None = None
    ) -> list[SemanticProject]:
        """List the current project's semantic projects, each with the build that answers its queries.

        A semantic project is a set of metric definitions (dbt or MetricFlow YAML) over feature groups; every other semantic layer tool takes its name.
        `stale` is set while the newest compile is invalid or still running, so answers come from an older build.

        Parameters:
            ctx: The MCP context, provided automatically.

        Returns:
            The semantic projects; `not_serving` says why one answers no queries yet.
        """
        project = hopsworks.get_current_project()
        if ctx:
            await ctx.info(f"Listing the semantic projects of {project.name}...")
        result = []
        for sp in project.get_semantic_layer_api().get_projects():
            entry = SemanticProject(
                name=sp.name, root_path=sp.root_path, deployment_mode=sp.deployment_mode
            )
            try:
                serving = sp.serving()
                entry.serving_build_id = serving.build_id
                entry.stale = serving.stale
            except SemanticLayerException as exc:
                entry.not_serving = exc.error_msg or str(exc)
            result.append(entry)
        return result

    async def list_metrics(
        self,
        semantic_project: str,
        tag: list[str] | None = None,
        ctx: Context | None = None,
    ) -> str:
        """List the metrics a semantic project serves, as CSV with the columns name, type, label, description and tags.

        Parameters:
            semantic_project: Name of the semantic project.
            tag: Tag schema names; only metrics whose definitions set all of them are listed.
            ctx: The MCP context, provided automatically.

        Returns:
            The metrics as CSV; `tags` is a JSON object of tag schema name to value.
        """
        if ctx:
            await ctx.info(f"Listing the metrics of {semantic_project}...")
        metrics = _semantic_project(semantic_project).list_metrics(tag=tag)
        return _csv(
            ["name", "type", "label", "description", "tags"],
            [[m.name, m.type, m.label, m.description, m.tags] for m in metrics],
        )

    async def get_dimensions(
        self,
        semantic_project: str,
        metrics: list[str],
        ctx: Context | None = None,
    ) -> str:
        """List the dimensions every one of the metrics can be grouped and filtered by, as CSV.

        A time dimension, `metric_time` included, takes a grain suffix in a query, such as `metric_time__month`.

        Parameters:
            semantic_project: Name of the semantic project.
            metrics: Metric names.
            ctx: The MCP context, provided automatically.

        Returns:
            CSV with the columns name, type, granularities (space-separated), entity_path and description.
        """
        if ctx:
            await ctx.info(f"Listing the dimensions of {', '.join(metrics)}...")
        dimensions = _semantic_project(semantic_project).dimensions(metrics=metrics)
        return _csv(
            ["name", "type", "granularities", "entity_path", "description"],
            [
                [
                    d.name,
                    d.type,
                    d.queryable_granularities,
                    "__".join(d.entity_path or []),
                    d.description,
                ]
                for d in dimensions
            ],
        )

    async def get_entities(
        self,
        semantic_project: str,
        metrics: list[str] | None = None,
        ctx: Context | None = None,
    ) -> str:
        """List the entities, the keys that join semantic models, which metrics can also be grouped by, as CSV.

        Parameters:
            semantic_project: Name of the semantic project.
            metrics: Metric names, or none for the entities of every semantic model.
            ctx: The MCP context, provided automatically.

        Returns:
            CSV with the columns name, entity_path and description.
        """
        if ctx:
            await ctx.info(f"Listing the entities of {semantic_project}...")
        entities = _semantic_project(semantic_project).entities(metrics=metrics)
        return _csv(
            ["name", "entity_path", "description"],
            [[e.name, "__".join(e.entity_path or []), e.description] for e in entities],
        )

    async def get_dimension_values(
        self,
        semantic_project: str,
        dimension: str,
        metrics: list[str] | None = None,
        limit: int = 100,
        ctx: Context | None = None,
    ) -> str:
        """List the distinct values of a dimension, for example to write a filter, as one CSV column.

        Parameters:
            semantic_project: Name of the semantic project.
            dimension: Qualified dimension name, such as `customer__country`.
            metrics: Metric names whose semantic models restrict the values, or none for every value.
            limit: Maximum number of values.
            ctx: The MCP context, provided automatically.

        Returns:
            CSV with one column named after the dimension.
        """
        if ctx:
            await ctx.info(f"Reading the values of {dimension}...")
        values = _semantic_project(semantic_project).dimension_values(
            dimension, metrics=metrics, limit=limit
        )
        return _csv([dimension], [[value] for value in values])

    async def query_metrics(
        self,
        semantic_project: str,
        metrics: list[str],
        group_by: list[str] | None = None,
        where: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = 100,
        ctx: Context | None = None,
    ) -> str:
        """Run a metric query on Trino as the logged-in user and return its rows as CSV.

        Prefer this to writing SQL over feature groups: the metric definitions hold the joins, filters and aggregations the owners agreed on.

        Parameters:
            semantic_project: Name of the semantic project.
            metrics: Metric names.
            group_by: Qualified group-bys, such as `metric_time__month` or `customer__country`; a time dimension takes a grain suffix from `__day` to `__year`.
            where: Filters in MetricFlow's syntax, combined with AND, such as `{{ Dimension('customer__country') }} = 'SE'`.
            order_by: Metric or group-by names; a leading `-` sorts descending.
            limit: Maximum number of rows.
            ctx: The MCP context, provided automatically.

        Returns:
            The rows as CSV, one column per group-by and metric: the whole rows within MAX_RESPONSE_CHARS characters, then a line saying so when some were left out.
        """
        if ctx:
            await ctx.info(f"Querying {', '.join(metrics)} on {semantic_project}...")
        df = _semantic_project(semantic_project).query(
            metrics,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
            dataframe_type="pandas",
        )
        rows = df.astype(object).where(df.notna(), None)
        return _csv(list(df.columns), rows.itertuples(index=False, name=None))

    async def get_metrics_compiled_sql(
        self,
        semantic_project: str,
        metrics: list[str],
        group_by: list[str] | None = None,
        where: list[str] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = 100,
        ctx: Context | None = None,
    ) -> str:
        """Return the Trino SQL a metric query compiles to, without running it.

        Use it to explain an answer, or as the governed SQL behind a dashboard dataset.

        Parameters:
            semantic_project: Name of the semantic project.
            metrics: Metric names.
            group_by: Qualified group-bys, as for query_metrics.
            where: Filters in MetricFlow's syntax, as for query_metrics.
            order_by: Metric or group-by names; a leading `-` sorts descending.
            limit: Maximum number of rows.
            ctx: The MCP context, provided automatically.

        Returns:
            The SQL statement.
        """
        if ctx:
            await ctx.info(f"Compiling the SQL of {', '.join(metrics)}...")
        return _semantic_project(semantic_project).sql(
            metrics, group_by=group_by, where=where, order_by=order_by, limit=limit
        )

    async def get_semantic_project_tags(
        self, semantic_project: str, ctx: Context | None = None
    ) -> list[Tag]:
        """List the schematised tags attached to a semantic project, such as its owner or certification.

        A metric's own tags are in its definition and come with list_metrics.

        Parameters:
            semantic_project: Name of the semantic project.
            ctx: The MCP context, provided automatically.

        Returns:
            The tags, each a schema name and its value.
        """
        if ctx:
            await ctx.info(f"Reading the tags of {semantic_project}...")
        tags = _semantic_project(semantic_project).get_tags()
        return [Tag(name=name, value=value) for name, value in tags.items()]

    async def validate_semantic_project(
        self, semantic_project: str, ctx: Context | None = None
    ) -> SemanticBuild:
        """Check a semantic project's definitions, as they are in HopsFS now, and report every error and warning at its file and line.

        It compiles, as compile_semantic_project does: definitions that changed become a new build, and unchanged ones reuse theirs. In FOLLOW mode a VALID build then answers queries, which undoes a promote of an older build; PINNED mode serves only promoted builds.

        Parameters:
            semantic_project: Name of the semantic project.
            ctx: The MCP context, provided automatically.

        Returns:
            The build's status, VALID, INVALID or FAILED, and its issues.
        """
        if ctx:
            await ctx.info(f"Validating {semantic_project}...")
        return _build(_semantic_project(semantic_project))

    async def compile_semantic_project(
        self, semantic_project: str, ctx: Context | None = None
    ) -> SemanticBuild:
        """Compile a semantic project's definitions into a build and wait for the result.

        A VALID build answers queries at once in FOLLOW mode, and after a promote in PINNED mode; an INVALID one never does, and the last valid build keeps serving.

        Parameters:
            semantic_project: Name of the semantic project.
            ctx: The MCP context, provided automatically.

        Returns:
            The build's id, status and issues, each at its file and line.
        """
        if ctx:
            await ctx.info(f"Compiling {semantic_project}...")
        return _build(_semantic_project(semantic_project))
