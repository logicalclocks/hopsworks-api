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
"""Semantic projects of the Hopsworks Semantic Layer, their builds, and the metrics and group-bys they serve."""

from __future__ import annotations

import datetime
import json
import time
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import humps
import requests
from hopsworks_apigen import public
from hopsworks_common import usage, util
from hopsworks_common.client.exceptions import (
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.core.constants import HAS_PYARROW
from hopsworks_common.decorators import _uses_polars, _uses_pyarrow


if TYPE_CHECKING:
    from collections.abc import Sequence

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from hopsworks_common.core.semantic_layer_api import SemanticLayerApi


# MetricFlow's TimeGranularity values; a group-by name ending in `__<grain>` selects that grain.
_GRAINS = frozenset(
    {
        "nanosecond",
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    }
)
_DATE_PART_PREFIX = "extract_"
_FINAL_BUILD_STATUSES = frozenset({"VALID", "INVALID", "FAILED"})
_COMPILE_POLL_SECONDS = 2.0
_COMPILE_TIMEOUT_SECONDS = 600.0
_DATAFRAME_TYPES = ("pandas", "polars", "pyarrow", "python")


def _known_fields(cls: type, json_decamelized: dict[str, Any]) -> dict[str, Any]:
    # A newer backend may send fields this client does not know yet.
    return {
        key: value
        for key, value in json_decamelized.items()
        if key in cls.__dataclass_fields__
    }


@public("hopsworks.semantic_layer.SemanticIssue")
@dataclass
class SemanticIssue:
    """A validation error or warning found while compiling a semantic project.

    `level` is `ERROR` or `WARNING`; only errors make a build `INVALID`.
    `rule` is the validation that raised it, such as `UNIQUE_NAMES`.
    `file`, `line` and `column` locate it in the YAML, with `file` relative to the project's root path, and are `None` when the issue is not tied to one place.
    `element` names the definition it concerns, such as `metric:bookings`.
    """

    level: str | None = None
    rule: str | None = None
    message: str | None = None
    file: str | None = None
    line: int | None = None
    column: int | None = None
    element: str | None = None

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> SemanticIssue | None:
        if not json_dict:
            return None
        return cls(**_known_fields(cls, humps.decamelize(json_dict)))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __str__(self) -> str:
        location = ":".join(
            str(part)
            for part in (self.file, self.line, self.column)
            if part is not None
        )
        head = " ".join(
            part for part in (self.level, self.rule, location, self.element) if part
        )
        return f"{head}: {self.message}" if head else str(self.message)


@public("hopsworks.semantic_layer.SemanticBuild")
@dataclass
class SemanticBuild:
    """One compile of a semantic project's definitions, immutable once it ends.

    `status` is `COMPILING` while the compile runs, then `VALID`, `INVALID` when the definitions have errors, or `FAILED` when the compile could not run.
    Only a `VALID` build can serve queries.
    `git_commit` is the commit the definitions were read at, and `None` outside a git repository.
    `fingerprint` identifies the files compiled, so compiling unchanged files returns their existing build.
    `issues` is filled by [`SemanticProject.get_build`][hopsworks_common.semantic_layer.SemanticProject.get_build] and is `None` on builds from a listing.
    """

    id: int | None = None
    semantic_project_id: int | None = None
    git_commit: str | None = None
    fingerprint: str | None = None
    status: str | None = None
    error_count: int | None = None
    warning_count: int | None = None
    created: str | None = None
    creator: str | None = None
    issues: list[SemanticIssue] | None = None

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> SemanticBuild | None:
        if not json_dict:
            return None
        return cls._from_decamelized(humps.decamelize(json_dict))

    @classmethod
    def _from_decamelized(cls, json_decamelized: dict[str, Any]) -> SemanticBuild:
        fields = _known_fields(cls, json_decamelized)
        if fields.get("issues") is not None:
            fields["issues"] = [
                SemanticIssue(**_known_fields(SemanticIssue, issue))
                for issue in fields["issues"]
            ]
        return cls(**fields)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)


@public("hopsworks.semantic_layer.SemanticMetric")
@dataclass
class SemanticMetric:
    """A metric a semantic project serves.

    `type` is `SIMPLE`, `RATIO` or `DERIVED`.
    `agg` is the aggregation of a simple metric, such as `SUM` or `COUNT_DISTINCT`, and `None` for ratio and derived metrics.
    `queryable_granularities` are the grains `metric_time` can be grouped by for this metric.
    `requires_metric_time` is set when every query of the metric must group by `metric_time`.
    `filter` is the metric's own filter, applied to every query of it.
    `tags` are the schematised tags its definition sets in `config.meta.hopsworks.tags`, tag schema name to value, checked against the tag schemas when the project compiles.
    """

    name: str | None = None
    label: str | None = None
    description: str | None = None
    type: str | None = None
    agg: str | None = None
    queryable_granularities: list[str] | None = None
    requires_metric_time: bool | None = None
    filter: str | None = None
    tags: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> SemanticMetric | None:
        if not json_dict:
            return None
        fields = _known_fields(cls, humps.decamelize(json_dict))
        # Tag schema names and the keys inside tag values are the user's, so they keep their case.
        fields["tags"] = json_dict.get("tags") or {}
        return cls(**fields)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@public("hopsworks.semantic_layer.SemanticServing")
@dataclass
class SemanticServing:
    """The build a semantic project answers queries from.

    `build_id` is that build.
    `stale` is set while it is older than the project's newest compile, because that compile is invalid or still running.
    """

    build_id: int | None = None
    stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@public("hopsworks.semantic_layer.SemanticGroupBy")
@dataclass
class SemanticGroupBy:
    """A dimension, time dimension or entity that metrics can be grouped and filtered by.

    `name` is the qualified name to pass as a group-by, such as `listing__country_latest`; a time dimension also takes a grain suffix, as in `metric_time__month`.
    `type` is `CATEGORICAL`, `TIME` or `ENTITY`.
    `entity_path` is the chain of entities that qualifies the name, empty only for `metric_time`.
    `dimension` is the name the dimension has in its semantic model.
    For a time dimension, `granularity` is its finest grain and `queryable_granularities` every grain it can be grouped by, and `date_parts_supported` says whether date parts such as `dow` can be extracted from it.
    """

    name: str | None = None
    type: str | None = None
    description: str | None = None
    label: str | None = None
    entity_path: list[str] | None = None
    dimension: str | None = None
    granularity: str | None = None
    queryable_granularities: list[str] | None = None
    date_parts_supported: bool | None = None

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> SemanticGroupBy | None:
        if not json_dict:
            return None
        return cls(**_known_fields(cls, humps.decamelize(json_dict)))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@public("hopsworks.semantic_layer.SemanticProject")
class SemanticProject:
    """Metric definitions in a HopsFS directory, compiled into builds and queried through the semantic gateway.

    Get one with [`SemanticLayerApi.get_project`][hopsworks_common.core.semantic_layer_api.SemanticLayerApi.get_project].
    Discovery and queries are answered from the current build, which is the last valid one; an answer from a build older than the newest compile logs a warning.

    Group-bys are qualified names such as `"listing__country_latest"` or `"metric_time__month"`.
    A grain suffix from `__nanosecond` to `__year` sets the grain of a time dimension, and a suffix `__extract_<part>`, with `<part>` one of `year`, `quarter`, `month`, `day`, `dow` and `doy`, extracts a date part from it.
    A dict `{"name": "metric_time", "grain": "month"}` or `{"name": "metric_time", "date_part": "dow"}` says the same explicitly.

    Example: Compile, discover and query
        ```python
        import hopsworks

        project = hopsworks.login()
        sl = project.get_semantic_layer_api()
        sales = sl.get_project("sales")

        sales.compile()
        print([m.name for m in sales.list_metrics()])
        print([d.name for d in sales.dimensions(metrics=["revenue"])])

        df = sales.query(
            metrics=["revenue", "order_count"],
            group_by=["metric_time__month", "customer__country"],
            where=["{{ Dimension('customer__country') }} != 'XX'"],
            order_by=["-revenue"],
            limit=100,
        )
        ```
    """

    def __init__(
        self,
        id: int | None = None,
        project_id: int | None = None,
        name: str | None = None,
        root_path: str | None = None,
        git_repository_id: int | None = None,
        format: str | None = None,
        sql_engine: str | None = None,
        deployment_mode: str | None = None,
        current_build_id: int | None = None,
        latest_build: dict[str, Any] | SemanticBuild | None = None,
        creator: str | None = None,
        created: str | None = None,
        missing_mandatory_tags: list[dict[str, Any] | str] | None = None,
        tags: list[dict[str, Any]] | None = None,
        semantic_layer_api: SemanticLayerApi | None = None,
        **kwargs,
    ) -> None:
        self._id = id
        self._project_id = project_id
        self._name = name
        self._root_path = root_path
        self._git_repository_id = git_repository_id
        self._format = format
        self._sql_engine = sql_engine
        self._deployment_mode = deployment_mode
        self._current_build_id = current_build_id
        self._latest_build = (
            SemanticBuild._from_decamelized(latest_build)
            if isinstance(latest_build, dict)
            else latest_build
        )
        self._creator = creator
        self._created = created
        self._missing_mandatory_tags = [
            tag["name"] if isinstance(tag, dict) else tag
            for tag in missing_mandatory_tags or []
        ]
        # Present only when the response was expanded with its tags; get_tags() reads them otherwise.
        self._tags = None if tags is None else {t["name"]: t.get("value") for t in tags}
        self._semantic_layer_api = semantic_layer_api

    @classmethod
    def from_response_json(
        cls,
        json_dict: dict[str, Any] | None,
        semantic_layer_api: SemanticLayerApi | None = None,
    ) -> SemanticProject | None:
        if not json_dict:
            return None
        return cls(**humps.decamelize(json_dict), semantic_layer_api=semantic_layer_api)

    def update_from_response_json(self, json_dict: dict[str, Any]) -> SemanticProject:
        self.__init__(
            **humps.decamelize(json_dict),
            semantic_layer_api=self._semantic_layer_api,
        )
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "project_id": self._project_id,
            "name": self._name,
            "root_path": self._root_path,
            "git_repository_id": self._git_repository_id,
            "format": self._format,
            "sql_engine": self._sql_engine,
            "deployment_mode": self._deployment_mode,
            "current_build_id": self._current_build_id,
            "latest_build": self._latest_build.to_dict()
            if self._latest_build
            else None,
            "creator": self._creator,
            "created": self._created,
            "missing_mandatory_tags": self._missing_mandatory_tags,
            **({"tags": self._tags} if self._tags is not None else {}),
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self) -> str:
        return (
            f"SemanticProject(name={self._name!r}, id={self._id!r}, "
            f"current_build_id={self._current_build_id!r})"
        )

    @public
    @property
    def id(self) -> int | None:
        """Id of the semantic project, also the `environmentId` dbt Semantic Layer clients connect with."""
        return self._id

    @public
    @property
    def project_id(self) -> int | None:
        """Id of the Hopsworks project that owns the semantic project."""
        return self._project_id

    @public
    @property
    def name(self) -> str | None:
        """Name of the semantic project, unique within its Hopsworks project."""
        return self._name

    @public
    @property
    def root_path(self) -> str | None:
        """HopsFS directory the definitions are read from, for example a dbt project."""
        return self._root_path

    @public
    @property
    def git_repository_id(self) -> int | None:
        """Id of the Hopsworks git repository holding `root_path`, whose pulls and checkouts recompile the project."""
        return self._git_repository_id

    @public
    @property
    def format(self) -> str | None:
        """Format of the definitions: `DBT`, `NATIVE`, `MANIFEST_JSON`, or `AUTO` to detect it from the files under `root_path`."""
        return self._format

    @public
    @property
    def sql_engine(self) -> str | None:
        """Query engine the generated SQL runs on."""
        return self._sql_engine

    @public
    @property
    def deployment_mode(self) -> str | None:
        """`FOLLOW` to serve every new valid build, or `PINNED` to serve a build until another is promoted."""
        return self._deployment_mode

    @public
    @property
    def current_build_id(self) -> int | None:
        """Id of the build that answers queries, or `None` until a build is valid."""
        return self._current_build_id

    @public
    @property
    def latest_build(self) -> SemanticBuild | None:
        """The newest build, which may be invalid or still compiling."""
        return self._latest_build

    @public
    @property
    def creator(self) -> str | None:
        """Username of the member who registered the semantic project."""
        return self._creator

    @public
    @property
    def created(self) -> str | None:
        """When the semantic project was registered, as an ISO-8601 UTC timestamp."""
        return self._created

    @public
    @property
    def missing_mandatory_tags(self) -> list[str]:
        """Names of the mandatory semantic project tags the project lacks, empty when it has them all or on a community cluster."""
        return self._missing_mandatory_tags

    # region Builds

    @public
    @usage._method_logger
    def compile(self, wait: bool = True) -> SemanticBuild:
        """Compile the definitions under `root_path` into a build.

        In `FOLLOW` mode a valid build starts answering queries at once; in `PINNED` mode it waits for [`promote`][hopsworks_common.semantic_layer.SemanticProject.promote].
        An invalid build never answers queries, so the project keeps serving its last valid build.

        Parameters:
            wait: Whether to block until the compile ends; without it the build is returned in status `COMPILING`.

        Returns:
            The build, in a final status when `wait` is set.

        Raises:
            hopsworks.client.exceptions.SemanticBuildException: When `wait` is set and the build is `INVALID` or `FAILED`; its `issues` say what to fix.
            hopsworks.client.exceptions.SemanticLayerException: When `wait` is set and the compile has not ended after ten minutes.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        api = self._semantic_layer_api
        if not wait:
            build = api._compile(self, wait)
            self._latest_build = build
            return build
        # One budget for the whole wait, every request included: the server's own wait and the polls.
        deadline = time.monotonic() + _COMPILE_TIMEOUT_SECONDS

        def budget() -> tuple[float, float]:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SemanticLayerException(
                    f"The compile of semantic project '{self._name}' has not ended after "
                    f"{_COMPILE_TIMEOUT_SECONDS:.0f} seconds."
                )
            return (min(30.0, remaining), remaining)

        try:
            build = api._compile(self, wait, timeout=budget())
            while build.status not in _FINAL_BUILD_STATUSES:
                time.sleep(_COMPILE_POLL_SECONDS)
                build = api._get_build(self, build.id, timeout=budget())
            # A compile in FOLLOW mode moves the build queries use.
            self.update_from_response_json(api._get(self, timeout=budget()))
            if build.status == "VALID":
                return build
            if build.issues is None:
                build = api._get_build(self, build.id, timeout=budget())
        except requests.Timeout as err:
            raise SemanticLayerException(
                f"The compile of semantic project '{self._name}' has not ended after "
                f"{_COMPILE_TIMEOUT_SECONDS:.0f} seconds."
            ) from err
        raise SemanticBuildException(self._name, build)

    @public
    @usage._method_logger
    def get_builds(self) -> list[SemanticBuild]:
        """The project's retained builds, newest first, without their issues.

        Returns:
            The builds.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._semantic_layer_api._get_builds(self)

    @public
    @usage._method_logger
    def get_build(self, build_id: int) -> SemanticBuild:
        """One build with its validation issues.

        Parameters:
            build_id: Id of the build.

        Returns:
            The build, with `issues` filled.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the build does not exist or the backend encounters an error.
        """
        return self._semantic_layer_api._get_build(self, build_id)

    @public
    @usage._method_logger
    def promote(self, build_id: int) -> SemanticProject:
        """Make a valid build the one that answers queries, in either deployment mode.

        In `FOLLOW` mode the next valid compile replaces it again.
        This object is updated in place.

        Parameters:
            build_id: Id of a `VALID` build of this project.

        Returns:
            This semantic project, with its new `current_build_id`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the build is not valid or the backend encounters an error.
        """
        return self.update_from_response_json(
            self._semantic_layer_api._promote(self, build_id)
        )

    @public
    @usage._method_logger
    def update(
        self,
        root_path: str | None = None,
        format: Literal["AUTO", "DBT", "NATIVE", "MANIFEST_JSON"] | None = None,
        deployment_mode: Literal["FOLLOW", "PINNED"] | None = None,
    ) -> SemanticProject:
        """Change where the definitions are read from, how they are read, or which build answers queries.

        A new root path or format compiles the definitions again in the background, and the git repository becomes the one the new root path is in.
        A setting left as `None` keeps its value; the name cannot change.
        This object is updated in place.

        Parameters:
            root_path: Directory of the definitions, relative to the project, under `/hopsfs/`, or `/Projects/<project>/...`.
            format: Format of the definitions; `AUTO` detects it from the files.
            deployment_mode: `FOLLOW` serves every new valid build; `PINNED` serves a build until another is promoted.

        Returns:
            This semantic project, with its new settings.

        Raises:
            ValueError: If no setting is given.
            hopsworks.client.exceptions.RestAPIError: If the path is outside the project or the backend encounters another error.
        """
        if root_path is None and format is None and deployment_mode is None:
            raise ValueError(
                "Give at least one of root_path, format and deployment_mode."
            )
        return self.update_from_response_json(
            self._semantic_layer_api._update(self, root_path, format, deployment_mode)
        )

    # endregion

    # region Tags

    @public
    @usage._method_logger
    def add_tag(self, name: str, value: Any) -> None:
        """Attach a schematised tag to the semantic project, or replace its value.

        The tag schema `name` must exist, and `value` must match it; administrators create the schemas.
        The value can be any JSON value: a string, number, boolean, list or object.
        Schematised tags need Hopsworks Enterprise; other editions refuse the request.

        ```python
        sales.add_tag("ownership", {"owner": "finance", "certified": True})
        ```

        Parameters:
            name: Name of the tag schema.
            value: Value of the tag.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the schema does not exist, the value does not match it, or the backend encounters another error.
        """
        self._semantic_layer_api._add_tag(self, name, value)

    @public
    @usage._method_logger
    def delete_tag(self, name: str) -> None:
        """Remove a tag from the semantic project.

        A tag that a mandatory tag rule requires can still be removed; the project then lists it as missing.

        Parameters:
            name: Name of the tag schema.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._semantic_layer_api._delete_tag(self, name)

    @public
    def get_tag(self, name: str) -> Any | None:
        """The value of one tag attached to the semantic project.

        Parameters:
            name: Name of the tag schema.

        Returns:
            The tag value, or `None` if the project has no such tag.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._semantic_layer_api._get_tag(self, name)

    @public
    def get_tags(self) -> dict[str, Any]:
        """Every tag attached to the semantic project.

        Metric tags are not included: they are part of each metric's definition, in [`SemanticMetric.tags`][hopsworks_common.semantic_layer.SemanticMetric].

        Returns:
            Tag schema names to tag values, empty when the project has none.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._semantic_layer_api._get_tags(self)

    # endregion

    # region Discovery

    @public
    @usage._method_logger
    def list_metrics(
        self, tag: str | Sequence[str] | None = None
    ) -> list[SemanticMetric]:
        """Every metric of the current build, or the metrics that carry every given tag.

        Parameters:
            tag: Tag schema names; only metrics whose definitions set all of them are listed.

        Returns:
            The metrics, each with its tags.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example because no build is valid yet.
        """
        return self._semantic_layer_api._metrics(self, _as_list(tag))

    @public
    @usage._method_logger
    def serving(self) -> SemanticServing:
        """The build the gateway answers this project's queries from, and whether a newer compile is pending or invalid.

        The gateway keeps each caller's view of the project for up to a minute, so a newly served build can take that long to show.

        Returns:
            The serving build.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example because no build is valid yet.
        """
        return self._semantic_layer_api._serving(self)

    @public
    @usage._method_logger
    def dimensions(
        self, metrics: str | Sequence[str] | None = None
    ) -> list[SemanticGroupBy]:
        """Dimensions and time dimensions every one of the metrics can be grouped by.

        Parameters:
            metrics: Metric names, or `None` for the dimensions of every semantic model.

        Returns:
            The dimensions.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example for an unknown metric.
        """
        return self._semantic_layer_api._group_bys(
            self, "dimensions", _as_list(metrics)
        )

    @public
    @usage._method_logger
    def entities(
        self, metrics: str | Sequence[str] | None = None
    ) -> list[SemanticGroupBy]:
        """Entities every one of the metrics can be grouped by.

        Parameters:
            metrics: Metric names, or `None` for the entities of every semantic model.

        Returns:
            The entities.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example for an unknown metric.
        """
        return self._semantic_layer_api._group_bys(self, "entities", _as_list(metrics))

    @public
    @usage._method_logger
    def group_bys(
        self, metrics: str | Sequence[str] | None = None
    ) -> list[SemanticGroupBy]:
        """Everything the metrics can be grouped by: dimensions, time dimensions, entities and `metric_time`.

        Parameters:
            metrics: Metric names, or `None` for every semantic model.

        Returns:
            The group-bys.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example for an unknown metric.
        """
        return self._semantic_layer_api._group_bys(self, "group-bys", _as_list(metrics))

    @public
    @usage._method_logger
    def compatible_metrics(
        self, group_by: str | dict[str, Any] | Sequence[str | dict[str, Any]]
    ) -> list[str]:
        """Names of the metrics that can be grouped by all of the given group-bys.

        Parameters:
            group_by: Group-bys, as qualified names or dicts.

        Returns:
            The metric names.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request, for example for an unknown group-by.
        """
        return self._semantic_layer_api._compatible_metrics(
            self, [_group_by_ref(ref) for ref in _as_list(group_by)]
        )

    @public
    @usage._method_logger
    def dimension_values(
        self,
        group_by: str | dict[str, Any],
        metrics: str | Sequence[str] | None = None,
        limit: int = 1000,
    ) -> list[Any]:
        """Distinct values of one dimension, for example to fill a filter's choices.

        Parameters:
            group_by: The dimension, as a qualified name or dict.
            metrics: Metric names whose semantic models restrict the values, or `None` for every value.
            limit: Maximum number of values returned.

        Returns:
            The values.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the request or the query fails.
        """
        request: dict[str, Any] = {"groupBy": _group_by_ref(group_by), "limit": limit}
        if metrics is not None:
            request["metrics"] = _as_list(metrics)
        return self._semantic_layer_api._dimension_values(self, request)

    # endregion

    # region Queries

    @public
    @usage._method_logger
    def sql(
        self,
        metrics: str | Sequence[str],
        group_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
        where: str | Sequence[str] | None = None,
        filters: Sequence[dict[str, Any] | tuple] | None = None,
        having: Sequence[dict[str, Any] | tuple] | None = None,
        order_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        time_range: tuple | dict[str, Any] | None = None,
        group_limit: dict[str, Any] | None = None,
    ) -> str:
        """The SQL a metric query compiles to, without running it.

        Parameters:
            metrics: Metric names.
            group_by: Group-bys, as qualified names or dicts.
            where:
                Text filters in MetricFlow's call syntax, such as `"{{ Dimension('listing__country_latest') }} = 'us'"`.
                Several filters are combined with `AND`.
            filters:
                Structured filters, each a dict `{"group_by", "op", "value"}` or a tuple `(group_by, op, value)`.
                `op` is one of `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL` and `LIKE`; values are bound as literals, never parsed as SQL.
                A date or datetime value is sent as ISO-8601; a timezone-aware one is compared in UTC.
            having: Filters on metric values, each a dict `{"metric", "op", "value"}` or a tuple `(metric, op, value)`.
            order_by:
                Metric or group-by column names; a leading `-`, as in `"-revenue"`, sorts descending.
                NULLs sort last.
            limit: Maximum number of rows.
            offset: Number of rows to skip.
            time_range:
                A `(start, end)` tuple or a `{"start", "end"}` dict of ISO-8601 strings, dates or datetimes, either of which may be `None`.
                A timezone-aware datetime, or a string with an offset, is converted to UTC; one without is taken as UTC.
                It keeps `metric_time` from `start` through `end`, both inclusive, each truncated to the query's time grain: a monthly query ending `2026-02-01` includes all of February.
            group_limit: Keep only the top groups, as a dict `{"group_by", "top", "metric", "descending"}`, for example the five countries with the most revenue.

        Returns:
            The SQL statement.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the query, for example for an unknown metric or group-by.
        """
        return self._semantic_layer_api._sql(
            self,
            _query_request(
                metrics,
                group_by=group_by,
                where=where,
                filters=filters,
                having=having,
                order_by=order_by,
                limit=limit,
                offset=offset,
                time_range=time_range,
                group_limit=group_limit,
            ),
        )

    @public
    @usage._method_logger
    def query(
        self,
        metrics: str | Sequence[str],
        group_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
        where: str | Sequence[str] | None = None,
        filters: Sequence[dict[str, Any] | tuple] | None = None,
        having: Sequence[dict[str, Any] | tuple] | None = None,
        order_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        time_range: tuple | dict[str, Any] | None = None,
        group_limit: dict[str, Any] | None = None,
        dataframe_type: Literal["pandas", "polars", "pyarrow", "python"] = "pandas",
    ) -> pd.DataFrame | pl.DataFrame | pa.Table | list[dict[str, Any]]:
        """Run a metric query on the query engine, as the caller, and return its rows.

        The arguments are those of [`sql`][hopsworks_common.semantic_layer.SemanticProject.sql].
        Results come back as an Arrow stream when `pyarrow` is installed, which keeps column types, and as JSON otherwise.
        The gateway caps the number of rows it returns, and a result that may have been cut by the cap logs a warning.

        Parameters:
            metrics: Metric names.
            group_by: Group-bys, as qualified names or dicts.
            where: Text filters in MetricFlow's call syntax.
            filters: Structured filters, each a dict `{"group_by", "op", "value"}` or a tuple `(group_by, op, value)`.
            having: Filters on metric values, each a dict `{"metric", "op", "value"}` or a tuple `(metric, op, value)`.
            order_by: Metric or group-by column names; a leading `-` sorts descending.
            limit: Maximum number of rows.
            offset: Number of rows to skip.
            time_range: A `(start, end)` tuple or a `{"start", "end"}` dict bounding `metric_time`, both ends inclusive and truncated to the query's time grain.
            group_limit: Keep only the top groups, as a dict `{"group_by", "top", "metric", "descending"}`.
            dataframe_type: `pandas`, `polars`, `pyarrow`, or `python` for a list of rows, each a dict keyed by column name.

        Returns:
            The rows, one column per group-by and metric.

        Raises:
            ValueError: For an unknown `dataframe_type`.
            ModuleNotFoundError: When `dataframe_type` is `polars` or `pyarrow` and that package is not installed.
            hopsworks.client.exceptions.SemanticLayerException: If the gateway refuses the query or the query engine fails to run it.
        """
        if dataframe_type not in _DATAFRAME_TYPES:
            raise ValueError(
                f"dataframe_type {dataframe_type!r} is not one of {', '.join(_DATAFRAME_TYPES)}."
            )
        result = self._semantic_layer_api._query(
            self,
            _query_request(
                metrics,
                group_by=group_by,
                where=where,
                filters=filters,
                having=having,
                order_by=order_by,
                limit=limit,
                offset=offset,
                time_range=time_range,
                group_limit=group_limit,
            ),
            arrow=HAS_PYARROW,
        )
        return _to_dataframe(result, dataframe_type)

    # endregion


# region Request building


def _as_list(value: Any) -> list[Any] | None:
    if value is None:
        return None
    if isinstance(value, (str, dict)):
        return [value]
    return list(value)


def _group_by_ref(value: str | dict[str, Any]) -> dict[str, Any]:
    """Turn a qualified group-by name or dict into the gateway's object form.

    The suffix rule is MetricFlow's `StructuredLinkableSpecName.from_name`, extended with `__extract_<part>`, which MetricFlow accepts only in object form.
    A name whose last segment is neither a grain nor a date part is sent whole, for the gateway to resolve.
    """
    if isinstance(value, dict):
        ref: dict[str, Any] = {"name": value["name"]}
        grain = value.get("grain")
        date_part = value.get("date_part", value.get("datePart"))
        if grain:
            ref["grain"] = grain.lower()
        if date_part:
            ref["datePart"] = date_part.lower()
        return ref
    name, dunder, suffix = value.rpartition("__")
    suffix = suffix.lower()
    if dunder and suffix in _GRAINS:
        return {"name": name, "grain": suffix}
    if dunder and suffix.startswith(_DATE_PART_PREFIX):
        return {"name": name, "datePart": suffix[len(_DATE_PART_PREFIX) :]}
    return {"name": value}


def _order_by(value: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"name": value["name"], "descending": bool(value.get("descending"))}
    if value.startswith("-"):
        return {"name": value[1:], "descending": True}
    return {"name": value, "descending": False}


def _filter(value: dict[str, Any] | tuple) -> dict[str, Any]:
    if isinstance(value, dict):
        group_by = value.get("group_by", value.get("groupBy"))
        op, operand = value["op"], value.get("value")
    else:
        group_by, op, *rest = value
        operand = rest[0] if rest else None
    result = {"groupBy": _group_by_ref(group_by), "op": op.upper()}
    if operand is not None:
        result["value"] = (
            [_instant(v) for v in operand]
            if isinstance(operand, (list, tuple, set))
            else _instant(operand)
        )
    return result


def _having(value: dict[str, Any] | tuple) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"metric": value["metric"], "op": value["op"], "value": value["value"]}
    metric, op, operand = value
    return {"metric": metric, "op": op, "value": operand}


def _instant(value: Any) -> Any:
    """A date or datetime as ISO-8601; any other value unchanged."""
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def _time_range(value: tuple | dict[str, Any]) -> dict[str, str]:
    start, end = (
        (value.get("start"), value.get("end")) if isinstance(value, dict) else value
    )
    bounds = {"start": _instant(start), "end": _instant(end)}
    return {key: bound for key, bound in bounds.items() if bound is not None}


def _group_limit(value: dict[str, Any]) -> dict[str, Any]:
    group_by = value.get("group_by", value.get("groupBy"))
    return {
        "groupBy": [_group_by_ref(ref) for ref in _as_list(group_by) or []],
        "top": value["top"],
        "metric": value["metric"],
        "descending": bool(value.get("descending", True)),
    }


def _query_request(
    metrics: str | Sequence[str],
    group_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
    where: str | Sequence[str] | None = None,
    filters: Sequence[dict[str, Any] | tuple] | None = None,
    having: Sequence[dict[str, Any] | tuple] | None = None,
    order_by: str | dict[str, Any] | Sequence[str | dict[str, Any]] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    time_range: tuple | dict[str, Any] | None = None,
    group_limit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the gateway's `Query` body, leaving out every argument that is `None`."""
    request: dict[str, Any] = {
        "metrics": _as_list(metrics) or [],
        "groupBy": None
        if group_by is None
        else [_group_by_ref(ref) for ref in _as_list(group_by)],
        "where": _as_list(where),
        "filters": None if filters is None else [_filter(f) for f in filters],
        "having": None if having is None else [_having(h) for h in having],
        "orderBy": None
        if order_by is None
        else [_order_by(o) for o in _as_list(order_by)],
        "limit": limit,
        "offset": offset,
        "timeRange": None if time_range is None else _time_range(time_range),
        "groupLimit": None if group_limit is None else _group_limit(group_limit),
    }
    return {key: value for key, value in request.items() if value is not None}


# endregion

# region Results


def _columns_and_rows(result: dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    names = [column["name"] for column in result.get("columns") or []]
    return names, result.get("rows") or []


def _to_dataframe(
    result: pa.Table | dict[str, Any], dataframe_type: str
) -> pd.DataFrame | pl.DataFrame | pa.Table | list[dict[str, Any]]:
    """Convert an Arrow table, or the gateway's JSON result, to the requested type."""
    if dataframe_type == "polars":
        return _to_polars(result)
    if dataframe_type == "pyarrow":
        return _to_arrow(result)
    if isinstance(result, dict):
        names, rows = _columns_and_rows(result)
        if dataframe_type == "python":
            return [dict(zip(names, row, strict=False)) for row in rows]
        import pandas as pd

        return pd.DataFrame(rows, columns=names)
    if dataframe_type == "python":
        return result.to_pylist()
    return result.to_pandas()


@_uses_polars
def _to_polars(result: pa.Table | dict[str, Any]) -> pl.DataFrame:
    import polars as pl

    if isinstance(result, dict):
        names, rows = _columns_and_rows(result)
        return pl.DataFrame(rows, schema=names, orient="row")
    return pl.from_arrow(result)


@_uses_pyarrow
def _to_arrow(result: pa.Table | dict[str, Any]) -> pa.Table:
    import pyarrow as pa

    if isinstance(result, dict):
        names, rows = _columns_and_rows(result)
        return pa.table(
            {name: [row[i] for row in rows] for i, name in enumerate(names)}
        )
    return result


# endregion
