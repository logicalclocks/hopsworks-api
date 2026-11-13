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
"""Hopsworks Semantic Layer client.

Semantic projects and their builds live on the Hopsworks REST API.
Discovery and queries go to the semantic gateway with the same credential and TLS settings as the Hopsworks client: at `/semantic-layer/v1` on the Hopsworks host from outside the cluster, and at the gateway's consul name from inside it.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import secrets
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

from hopsworks_apigen import public
from hopsworks_common import client, decorators, semantic_layer, tag, usage
from hopsworks_common.client import auth
from hopsworks_common.client.exceptions import RestAPIError, SemanticLayerException
from hopsworks_common.core import job_api
from hopsworks_common.core.variable_api import VariableApi
from hopsworks_common.decorators import _uses_pyarrow


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    import pandas as pd
    import pyarrow as pa
    import requests
    from hopsworks_common import execution, project
    from hsfs.feature_group import FeatureGroup


_logger = logging.getLogger(__name__)

# Overrides the gateway address, as `https://<host>:<port>`, for a cluster that exposes it elsewhere.
_GATEWAY_URL_ENV = "HOPSWORKS_SEMANTIC_LAYER_URL"
_GATEWAY_SERVICE_NAME = "semantic-gateway.service"
_GATEWAY_PORT = 18443
_GATEWAY_PATH = "semantic-layer/v1"
_JSON = "application/json"
# The most query history records the control plane answers in one page.
_HISTORY_PAGE = 1000
_ARROW_STREAM = "application/vnd.apache.arrow.stream"
# The end-of-stream marker of the Arrow IPC streaming format, which only a complete stream ends with.
_ARROW_END_OF_STREAM = b"\xff\xff\xff\xff\x00\x00\x00\x00"
# The gateway sends nothing until its answer is ready, so the read timeout bounds a whole query.
# 330 s outlasts the gateway's QUERY_TIMEOUT_SECONDS and the ingress proxy timeout, both 300 s
# by default, so the gateway's own 504 arrives first.
_GATEWAY_TIMEOUT = (30, 330)
# Seconds to connect and to read the semantic project listing, which every lookup by name runs.
_CONTROL_PLANE_TIMEOUT = (30, 120)
_GROUP_BY_KEYS = {
    "dimensions": "dimensions",
    "entities": "entities",
    "group-bys": "groupBys",
}
_HINTS = {
    "SEMANTIC_LAYER_NO_VALID_BUILD": "Compile the semantic project and fix its errors: queries need one valid build.",
    "SEMANTIC_LAYER_FORBIDDEN": "The caller must be a member of the project, with an API key that has the FEATURESTORE and TRINO scopes.",
}
_SPINE_GRAINS = ("second", "minute", "hour", "day", "week", "month", "quarter", "year")
_SUB_DAY_GRAINS = frozenset({"second", "minute", "hour"})
# Export statuses of a run that wrote nothing on purpose; any other failure is FAILED.
_EXPORT_REFUSALS = frozenset({"TRUNCATED", "TARGET_EXISTS"})
_SOURCE_VERSIONS_HEADER = "X-Semantic-Source-Versions"
_DEFINITION_HEADER = "X-Semantic-Definition"
# The width of semantic_export.last_message.
_EXPORT_MESSAGE_CHARS = 1000


@public("hopsworks.core.semantic_layer_api.SemanticLayerApi")
class SemanticLayerApi:
    """The semantic projects of a Hopsworks project: metric definitions compiled from YAML and queried as metrics.

    A semantic project reads dbt or MetricFlow semantic YAML from a HopsFS directory, usually a dbt project in a Hopsworks git repository.
    Its semantic models are feature groups, and its queries run on Trino as the caller, so members see only the data they can already read.
    Each [`SemanticProject`][hopsworks_common.semantic_layer.SemanticProject] compiles, lists its metrics and dimensions, and turns metric queries into SQL or rows.

    Example: Register and query a dbt project
        ```python
        import hopsworks

        project = hopsworks.login()
        sl = project.get_semantic_layer_api()

        sales = sl.create_project("sales", "Jupyter/analytics/dbt")
        sales.compile()
        df = sales.query(metrics=["revenue"], group_by=["metric_time__month"])
        ```
    """

    def __init__(self, project: project.Project | None = None) -> None:
        if project is not None:
            self._project_id = project.id
            self._project_name = project.name
        else:
            _client = client._get_instance()
            self._project_id = _client._project_id
            self._project_name = _client._project_name
        self._gateway_url: str | None = None

    # region Semantic projects

    @public
    @usage._method_logger
    def get_projects(self) -> list[semantic_layer.SemanticProject]:
        """Every semantic project of the Hopsworks project.

        Returns:
            The semantic projects, each with its newest build.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        response = (
            _client._send_request("GET", self._path(), timeout=_CONTROL_PLANE_TIMEOUT)
            or {}
        )
        return [
            semantic_layer.SemanticProject.from_response_json(item, self)
            for item in response.get("items") or []
        ]

    @public
    @usage._method_logger
    def get_project(self, name: str) -> semantic_layer.SemanticProject | None:
        """A semantic project by name.

        Parameters:
            name: Name of the semantic project.

        Returns:
            The semantic project, or `None` if the Hopsworks project has none with that name.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return next((sp for sp in self.get_projects() if sp.name == name), None)

    @public
    @usage._method_logger
    def create_project(
        self,
        name: str,
        root_path: str,
        git_repository_id: int | None = None,
        format: Literal["AUTO", "DBT", "NATIVE", "MANIFEST_JSON"] = "AUTO",
        deployment_mode: Literal["FOLLOW", "PINNED"] = "FOLLOW",
        tags: dict[str, Any] | None = None,
    ) -> semantic_layer.SemanticProject:
        """Register a directory of semantic definitions, which starts its first compile in the background.

        Parameters:
            name: Name of the semantic project, unique in the Hopsworks project, lowercase letters, digits and underscores starting with a letter.
            root_path:
                Directory of the definitions, relative to the project root, under `/hopsfs/` in a Hopsworks terminal, or an absolute `/Projects/<project>/...` path.
                For a dbt project it is the directory holding `dbt_project.yml`.
            git_repository_id: Hopsworks git repository holding `root_path`, or `None` to detect it from the path.
            format: `DBT` for dbt project YAML, `NATIVE` for MetricFlow YAML, `MANIFEST_JSON` for a `semantic_manifest.json`, or `AUTO` to detect it from the files.
            deployment_mode: `FOLLOW` to serve every new valid build, or `PINNED` to serve a build until another is promoted.
            tags:
                Schematised tags to attach in the same request, tag schema name to value, such as `{"ownership": {"owner": "finance"}}`.
                A cluster with mandatory semantic project tags refuses a project without them, so pass them here.

        Returns:
            The semantic project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the name is taken or invalid, the path is outside the project, a tag is unknown, invalid or a missing mandatory one, or the backend encounters another error.
        """
        body: dict[str, Any] = {
            "name": name,
            "rootPath": self._absolute_path(root_path),
            "format": format.upper(),
            "deploymentMode": deployment_mode.upper(),
        }
        if git_repository_id is not None:
            body["gitRepositoryId"] = git_repository_id
        if tags:
            # Each value is sent as JSON text, as the single-tag PUT sends it, so it reads back the same.
            body["tags"] = [
                {"name": name, "value": json.dumps(value)}
                for name, value in tags.items()
            ]
        _client = client._get_instance()
        response = _client._send_request(
            "POST",
            self._path(),
            headers={"content-type": _JSON},
            data=json.dumps(body),
        )
        return semantic_layer.SemanticProject.from_response_json(response, self)

    @public
    @usage._method_logger
    def delete_project(self, name: str) -> None:
        """Delete a semantic project and its builds; the definitions in HopsFS are left in place.

        Parameters:
            name: Name of the semantic project.

        Raises:
            hopsworks.client.exceptions.SemanticLayerException: If the Hopsworks project has no semantic project with that name.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        sp = self.get_project(name)
        if sp is None:
            raise SemanticLayerException(
                f"Project '{self._project_name}' has no semantic project named '{name}'.",
                error_code="SEMANTIC_LAYER_NOT_FOUND",
            )
        _client = client._get_instance()
        _client._send_request("DELETE", self._path(sp.id))

    # endregion

    # region Time spines

    @public
    @usage._method_logger
    def create_time_spine(
        self,
        start: str | datetime.date,
        end: str | datetime.date,
        granularity: Literal[
            "second", "minute", "hour", "day", "week", "month", "quarter", "year"
        ] = "day",
        custom_granularities: Mapping[str, str | Callable[[Any], Any]] | None = None,
    ) -> FeatureGroup:
        """Write the time spine that cumulative metrics, offsets, `join_to_timespine` and custom granularities join to.

        The spine is the offline feature group `time_spine_<granularity>`, version 1, with one row per period from `start` through `end`.
        Its primary key `date_<granularity>` holds the start of each period: a date for a day or a coarser grain, a timestamp below a day.
        A semantic project's time spine model at that grain binds the feature group by this name, unless its `config.meta.hopsworks.feature_group` names another.
        When the feature group exists the rows are inserted into it, so a wider range extends the spine, as long as the custom granularities are the ones it already has.
        The spine holds one row per period, so a fine grain over a long range is large: ten years are 3,653 days but 315 million seconds.

        Example: A daily spine with a fiscal quarter and a fiscal year
            ```python
            sl = project.get_semantic_layer_api()
            sl.create_time_spine(
                "2020-01-01",
                "2030-12-31",
                custom_granularities={
                    "fiscal_quarter": "Q-JAN",
                    "fiscal_year": lambda day: f"FY{day.year + (day.month > 1)}",
                },
            )
            ```

        Parameters:
            start: First day or instant of the spine, as an ISO-8601 string, a date or a datetime; it is moved back to the start of its period.
            end: Last day or instant of the spine, included.
            granularity: Grain of the spine; weeks start on Monday.
            custom_granularities:
                One column per custom granularity the time spine model declares, column name to definition.
                A string is a pandas period frequency, such as `"Q-JAN"` for the quarters of a fiscal year that ends in January, and each row holds the start of its period at that frequency, typed like `date_<granularity>`.
                A callable takes each row's `date_<granularity>` value and returns that row's value of the column.

        Returns:
            The feature group, with the rows written.

        Raises:
            ValueError: For a granularity not listed, a `start` or `end` that is not a date, an `end` before `start`, or a string that is not a pandas period frequency.
            hopsworks.client.exceptions.RestAPIError: If the backend fails to create or write the feature group, for example when an existing spine has other columns.
        """
        df = _time_spine_frame(start, end, granularity, custom_granularities or {})
        fs = client._get_connection()._get_feature_store(self._project_name)
        fg = fs.get_or_create_feature_group(
            name=f"time_spine_{granularity}",
            version=1,
            description=f"Time spine, one row per {granularity}, for the semantic layer.",
            primary_key=[f"date_{granularity}"],
            # The compiler binds a time spine without a declared column to its feature group's event time.
            event_time=f"date_{granularity}",
            online_enabled=False,
            statistics_config=False,
        )
        fg.insert(df, wait=True)
        return fg

    # endregion

    # region Control plane

    def _path(self, *tail: Any) -> list[Any]:
        return ["project", self._project_id, "semantic", "projects", *tail]

    def _absolute_path(self, root_path: str) -> str:
        if root_path.startswith("/hopsfs/"):
            root_path = root_path[len("/hopsfs/") :]
        if root_path.startswith("/"):
            return root_path
        return f"/Projects/{self._project_name}/{root_path.strip('/')}"

    def _compile(
        self,
        sp: semantic_layer.SemanticProject,
        wait: bool,
        timeout: tuple[float, float] | None = None,
    ) -> semantic_layer.SemanticBuild:
        _client = client._get_instance()
        response = _client._send_request(
            "POST",
            self._path(sp.id, "builds"),
            query_params={"wait": "true"} if wait else None,
            timeout=timeout,
        )
        return semantic_layer.SemanticBuild.from_response_json(response)

    def _get_builds(
        self, sp: semantic_layer.SemanticProject
    ) -> list[semantic_layer.SemanticBuild]:
        _client = client._get_instance()
        response = _client._send_request("GET", self._path(sp.id, "builds")) or {}
        return [
            semantic_layer.SemanticBuild.from_response_json(item)
            for item in response.get("items") or []
        ]

    def _get_build(
        self,
        sp: semantic_layer.SemanticProject,
        build_id: int,
        timeout: tuple[float, float] | None = None,
    ) -> semantic_layer.SemanticBuild:
        _client = client._get_instance()
        return semantic_layer.SemanticBuild.from_response_json(
            _client._send_request(
                "GET", self._path(sp.id, "builds", build_id), timeout=timeout
            )
        )

    def _get(
        self, sp: semantic_layer.SemanticProject, timeout: tuple[float, float] | None
    ) -> dict[str, Any]:
        _client = client._get_instance()
        return _client._send_request("GET", self._path(sp.id), timeout=timeout)

    def _get_by_id(self, sp_id: int) -> semantic_layer.SemanticProject:
        _client = client._get_instance()
        return semantic_layer.SemanticProject.from_response_json(
            _client._send_request("GET", self._path(sp_id)), self
        )

    def _update(
        self,
        sp: semantic_layer.SemanticProject,
        root_path: str | None,
        format: str | None,
        deployment_mode: str | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if root_path is not None:
            body["rootPath"] = self._absolute_path(root_path)
        if format is not None:
            body["format"] = format.upper()
        if deployment_mode is not None:
            body["deploymentMode"] = deployment_mode.upper()
        _client = client._get_instance()
        return _client._send_request(
            "PUT",
            self._path(sp.id),
            headers={"content-type": _JSON},
            data=json.dumps(body),
        )

    def _transfer_ownership(
        self, sp: semantic_layer.SemanticProject, email: str
    ) -> dict[str, Any]:
        _client = client._get_instance()
        return _client._send_request(
            "PUT",
            self._path(sp.id, "owner"),
            headers={"content-type": _JSON},
            data=json.dumps({"email": email}),
        )

    def _promote(
        self, sp: semantic_layer.SemanticProject, build_id: int
    ) -> dict[str, Any]:
        _client = client._get_instance()
        return _client._send_request(
            "POST", self._path(sp.id, "builds", build_id, "promote")
        )

    def _add_tag(
        self, sp: semantic_layer.SemanticProject, name: str, value: Any
    ) -> None:
        _client = client._get_instance()
        _client._send_request(
            "PUT",
            self._path(sp.id, "tags", name),
            headers={"content-type": _JSON},
            data=json.dumps(value),
        )

    def _delete_tag(self, sp: semantic_layer.SemanticProject, name: str) -> None:
        _client = client._get_instance()
        _client._send_request("DELETE", self._path(sp.id, "tags", name))

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def _get_tags(self, sp: semantic_layer.SemanticProject) -> dict[str, Any]:
        _client = client._get_instance()
        response = _client._send_request("GET", self._path(sp.id, "tags"))
        return {t.name: t.value for t in tag.Tag.from_response_json(response)}

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return=None)
    def _get_tag(self, sp: semantic_layer.SemanticProject, name: str) -> Any | None:
        _client = client._get_instance()
        response = _client._send_request("GET", self._path(sp.id, "tags", name))
        return {t.name: t.value for t in tag.Tag.from_response_json(response)}.get(name)

    def _mandatory_tags(self) -> set[str]:
        """Names of the tags a semantic project of this Hopsworks project must carry, by cluster or project rule."""
        _client = client._get_instance()
        names: set[str] = set()
        for path in (
            ["mandatorytags"],
            ["project", self._project_id, "mandatorytags", "project"],
        ):
            response = _client._send_request("GET", path) or {}
            names.update(
                item["name"]
                for item in response.get("items") or []
                if item.get("semanticProjectSpecific")
            )
        return names

    # endregion

    # region Superset

    def _create_dashboard(
        self, sp: semantic_layer.SemanticProject, request: dict[str, Any]
    ) -> dict[str, Any]:
        return self._superset_request(
            sp,
            "POST",
            "dashboards",
            headers={"content-type": _JSON},
            data=json.dumps(request),
        )

    def _explore_url(self, sp: semantic_layer.SemanticProject, metric: str) -> str:
        return self._superset_request(
            sp, "GET", "explore", query_params={"metric": metric}
        )["url"]

    def _superset_request(
        self,
        sp: semantic_layer.SemanticProject,
        method: str,
        resource: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        _client = client._get_instance()
        try:
            return _client._send_request(method, self._path(sp.id, resource), **kwargs)
        except RestAPIError as err:
            raise _semantic_layer_error(sp, err.response) from err

    # endregion

    # region Exports and clients

    def _exports(
        self, sp: semantic_layer.SemanticProject
    ) -> list[semantic_layer.SemanticExport]:
        _client = client._get_instance()
        response = _client._send_request("GET", self._path(sp.id, "exports")) or {}
        return [
            semantic_layer.SemanticExport.from_response_json(item)
            for item in response.get("items") or []
        ]

    def _start_export(
        self, sp: semantic_layer.SemanticProject, name: str, wait: bool
    ) -> execution.Execution:
        _client = client._get_instance()
        started = _client._send_request(
            "POST", self._path(sp.id, "exports", name, "run")
        )
        job = job_api.JobApi().get_job(started["jobName"])
        run = job._execution_api._get(job, started["executionId"])
        if wait:
            return job._execution_engine._wait_until_finished(job, run)
        return run

    def _report_export(
        self, sp: semantic_layer.SemanticProject, name: str, report: dict[str, Any]
    ) -> None:
        _client = client._get_instance()
        _client._send_request(
            "PUT",
            self._path(sp.id, "exports", name, "status"),
            headers={"content-type": _JSON},
            data=json.dumps(report),
        )

    def _session(self, sp: semantic_layer.SemanticProject) -> dict[str, Any]:
        """The caller's gateway session on the semantic project, which lists its exports and the Trino rules revision."""
        _client = client._get_instance()
        return _client._send_request("GET", ["semantic", "sessions", sp.id]) or {}

    def _query_history(
        self,
        sp: semantic_layer.SemanticProject,
        limit: int,
        status: list[str] | None,
        user: str | None,
    ) -> list[dict[str, Any]]:
        _client = client._get_instance()
        items: list[dict[str, Any]] = []
        cursor = None
        while len(items) < limit:
            params: dict[str, Any] = {"limit": min(limit - len(items), _HISTORY_PAGE)}
            if cursor:
                params["cursor"] = cursor
            if status:
                params["status"] = ",".join(status)
            if user:
                params["user"] = user
            page = (
                _client._send_request(
                    "GET", self._path(sp.id, "queries"), query_params=params
                )
                or {}
            )
            items.extend(page.get("items") or [])
            cursor = page.get("nextCursor")
            if not cursor:
                break
        return items[:limit]

    def _connection(self, sp: semantic_layer.SemanticProject) -> dict[str, Any]:
        _client = client._get_instance()
        return _client._send_request("GET", self._path(sp.id, "connection"))

    # endregion

    # region Gateway

    def _gateway_base_url(self) -> str:
        if self._gateway_url is not None:
            return self._gateway_url
        override = os.environ.get(_GATEWAY_URL_ENV)
        if override:
            self._gateway_url = override.rstrip("/")
            return self._gateway_url
        base_url = client._get_instance()._base_url.rstrip("/")
        domain = VariableApi()._get_service_discovery_domain()
        # An external client logged in at Payara's consul name, such as a job using an API key, is
        # inside the cluster too, and Payara does not route /semantic-layer/ as the ingress does.
        in_cluster = not client._is_external() or (
            bool(domain)
            and (urlparse(base_url).hostname or "").endswith(f".service.{domain}")
        )
        if not in_cluster:
            # The ingress routes /semantic-layer/ on the Hopsworks host to the gateway.
            self._gateway_url = base_url
        elif not domain:
            raise SemanticLayerException(
                "The semantic gateway cannot be found: the cluster's service discovery "
                f"domain is not set. Set {_GATEWAY_URL_ENV} to its https://<host>:<port>."
            )
        else:
            self._gateway_url = (
                f"https://{_GATEWAY_SERVICE_NAME}.{domain}:{_GATEWAY_PORT}"
            )
        return self._gateway_url

    def _gateway_request(
        self,
        method: str,
        sp: semantic_layer.SemanticProject,
        resource: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        accept: str = _JSON,
    ) -> requests.Response:
        _client = client._get_instance()
        url = f"{self._gateway_base_url()}/{_GATEWAY_PATH}/projects/{sp.id}/{resource}"
        headers = {"Accept": accept}
        if body is not None:
            headers["Content-Type"] = _JSON
        data = None if body is None else json.dumps(body)

        def send() -> requests.Response:
            return _client._session.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                auth=_client._auth,
                verify=_client._verify,
                timeout=_GATEWAY_TIMEOUT,
                # The body of an Arrow answer is read by _read_arrow_stream, which tells
                # a failure after the headers from one before them.
                stream=accept == _ARROW_STREAM,
            )

        response = send()
        if (
            response.status_code == 401
            and isinstance(_client._auth, auth.BearerAuth)
            and not _client._is_external()
        ):
            # A pod's JWT is rotated on disk; base.Client._retry_token_expired re-reads it the same way.
            _client._auth = auth.BearerAuth(_client._read_jwt())
            response.close()
            response = send()
        if response.status_code // 100 != 2:
            raise _semantic_layer_error(sp, response)
        return response

    def _gateway_json(
        self,
        method: str,
        sp: semantic_layer.SemanticProject,
        resource: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _json_of(sp, self._gateway_request(method, sp, resource, params, body))

    def _serving(
        self, sp: semantic_layer.SemanticProject
    ) -> semantic_layer.SemanticServing:
        # Not _gateway_json: asking whether the build is stale should not also warn that it is.
        result = self._gateway_request("GET", sp, "metrics").json() or {}
        return semantic_layer.SemanticServing(
            build_id=result.get("buildId"), stale=result.get("stale") is True
        )

    def _metrics(
        self, sp: semantic_layer.SemanticProject, tags: list[str] | None = None
    ) -> list[semantic_layer.SemanticMetric]:
        params = {"tag": tags} if tags else None
        result = self._gateway_json("GET", sp, "metrics", params=params)
        return [
            semantic_layer.SemanticMetric.from_response_json(metric)
            for metric in result.get("metrics") or []
        ]

    def _saved_queries(
        self, sp: semantic_layer.SemanticProject
    ) -> list[semantic_layer.SemanticSavedQuery]:
        result = self._gateway_json("GET", sp, "saved-queries")
        return [
            semantic_layer.SemanticSavedQuery.from_response_json(saved_query)
            for saved_query in result.get("savedQueries") or []
        ]

    def _group_bys(
        self,
        sp: semantic_layer.SemanticProject,
        resource: str,
        metrics: list[str] | None,
    ) -> list[semantic_layer.SemanticGroupBy]:
        params = {"metrics": ",".join(metrics)} if metrics else None
        result = self._gateway_json("GET", sp, resource, params=params)
        return [
            semantic_layer.SemanticGroupBy.from_response_json(group_by)
            for group_by in result.get(_GROUP_BY_KEYS[resource]) or []
        ]

    def _compatible_metrics(
        self, sp: semantic_layer.SemanticProject, group_by: list[dict[str, Any]]
    ) -> list[str]:
        result = self._gateway_json(
            "POST", sp, "compatible-metrics", body={"groupBy": group_by}
        )
        return result.get("metrics") or []

    def _dimension_values(
        self, sp: semantic_layer.SemanticProject, request: dict[str, Any]
    ) -> list[Any]:
        result = self._gateway_json("POST", sp, "dimension-values", body=request)
        return result.get("values") or []

    def _sql(self, sp: semantic_layer.SemanticProject, request: dict[str, Any]) -> str:
        return self._gateway_json("POST", sp, "sql", body=request)["sql"]

    def _query(
        self,
        sp: semantic_layer.SemanticProject,
        request: dict[str, Any],
        arrow: bool,
    ) -> pa.Table | dict[str, Any]:
        """Run a query, returning an Arrow table, or the JSON result when the gateway answers in JSON."""
        response = self._gateway_request(
            "POST", sp, "query", body=request, accept=_ARROW_STREAM if arrow else _JSON
        )
        if response.headers.get("Content-Type", "").startswith(_ARROW_STREAM):
            _warn_if_stale(
                sp,
                response.headers.get("X-Semantic-Stale"),
                response.headers.get("X-Semantic-Build"),
            )
            table = _read_arrow_stream(sp, response)
            # An Arrow stream cannot flag truncation, so a result that fills the cap may have been cut.
            # An older gateway sends no cap.
            max_rows = response.headers.get("X-Semantic-Max-Rows")
            if max_rows and table.num_rows >= int(max_rows):
                _logger.warning(
                    "The result of the query on semantic project '%s' may be cut at the "
                    "gateway's row limit of %s rows; add a limit, filters or fewer group-bys.",
                    sp.name,
                    max_rows,
                )
            return table
        result = _json_of(sp, response)
        if result.get("truncated"):
            _logger.warning(
                "The result of the query on semantic project '%s' was cut at %s rows "
                "by the gateway's row cap; add a limit or narrow the query.",
                sp.name,
                result.get("rowCount"),
            )
        return result

    # endregion


def _json_of(
    sp: semantic_layer.SemanticProject, response: requests.Response
) -> dict[str, Any]:
    result = response.json() or {}
    _warn_if_stale(sp, result.get("stale"), result.get("buildId"))
    return result


def _warn_if_stale(
    sp: semantic_layer.SemanticProject, stale: Any, build_id: Any
) -> None:
    if stale is True or str(stale).lower() == "true":
        _logger.warning(
            "Semantic project '%s' answered from build %s, which is older than its "
            "newest compile: that compile is invalid or still running.",
            sp.name,
            build_id,
        )


def _semantic_layer_error(
    sp: semantic_layer.SemanticProject, response: requests.Response
) -> SemanticLayerException:
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {}
    code = body.get("errorCode")
    error_msg = body.get("errorMsg") or response.reason
    usr_msg = body.get("usrMsg")
    status = f"HTTP {response.status_code}" + (f", {code}" if code else "")
    message = f"Semantic layer request on '{sp.name}' failed ({status}): {error_msg}"
    for extra in (usr_msg, _HINTS.get(code)):
        if extra:
            message += f" {extra}"
    return SemanticLayerException(
        message,
        status_code=response.status_code,
        error_code=code,
        error_msg=error_msg,
        usr_msg=usr_msg,
    )


def _time_spine_frame(
    start: str | datetime.date,
    end: str | datetime.date,
    granularity: str,
    custom_granularities: Mapping[str, str | Callable[[Any], Any]],
) -> pd.DataFrame:
    import pandas as pd

    # Offsets, not frequency strings, since the strings differ between pandas 1 and 2.
    periods = {
        "second": pd.offsets.Second(),
        "minute": pd.offsets.Minute(),
        "hour": pd.offsets.Hour(),
        "day": pd.offsets.Day(),
        # Weeks ending on Sunday start on Monday, as MetricFlow's do.
        "week": pd.offsets.Week(weekday=6),
        "month": pd.offsets.MonthEnd(),
        "quarter": pd.offsets.QuarterEnd(startingMonth=12),
        "year": pd.offsets.YearEnd(),
    }
    if granularity not in periods:
        raise ValueError(
            f"Granularity {granularity!r} is not one of {', '.join(_SPINE_GRAINS)}."
        )
    if pd.Timestamp(end) < pd.Timestamp(start):
        raise ValueError(
            f"The end of the time spine, {end}, is before its start, {start}."
        )
    starts = pd.Series(
        pd.period_range(
            pd.Timestamp(start), pd.Timestamp(end), freq=periods[granularity]
        ).start_time
    )

    def typed(instants: pd.Series) -> pd.Series:
        return instants if granularity in _SUB_DAY_GRAINS else instants.dt.date

    column = f"date_{granularity}"
    df = pd.DataFrame({column: typed(starts)})
    for name, definition in custom_granularities.items():
        if callable(definition):
            df[name] = df[column].map(definition)
            continue
        try:
            df[name] = typed(starts.dt.to_period(definition).dt.start_time)
        except ValueError as err:
            raise ValueError(
                f"Custom granularity '{name}': {definition!r} is not a pandas period "
                "frequency, such as 'Q-JAN'."
            ) from err
    return df


@_uses_pyarrow
def _read_arrow_stream(
    sp: semantic_layer.SemanticProject, response: requests.Response
) -> pa.Table:
    """The table of an Arrow answer, which raises unless the body is read whole and ends with the end-of-stream marker.

    The gateway sends its headers before the first batch, so a query that fails later can only end the body early.
    pyarrow reads a stream that stops between two batches as a complete one, so the marker is checked here.
    """
    import pyarrow as pa

    try:
        content = response.content
        source = pa.BufferReader(content)
        reader = pa.ipc.open_stream(source)
        batches = []
        end = source.tell()
        while True:
            try:
                batches.append(reader.read_next_batch())
            except StopIteration:
                break
            end = source.tell()
        complete = content[end:].endswith(_ARROW_END_OF_STREAM)
    # requests' errors are OSErrors too.
    except (pa.ArrowInvalid, OSError) as err:
        raise _cut_off(sp, str(err)) from err
    if not complete:
        raise _cut_off(sp, "the Arrow stream has no end-of-stream marker")
    return pa.Table.from_batches(batches, schema=reader.schema)


def _cut_off(sp: semantic_layer.SemanticProject, cause: str) -> SemanticLayerException:
    return SemanticLayerException(
        f"The result of the query on semantic project '{sp.name}' was cut off before "
        f"its end, so no rows are returned: {cause}. The query failed on the query "
        "engine or the connection to the gateway broke; run it again."
    )


# region Export job


def _run_export(
    semantic_project_id: int, export_name: str, generation: int
) -> dict[str, Any]:
    """Run one export of a semantic project: the body of the export's job, which calls it after `hopsworks.login()`.

    The saved query runs in full, past the gateway's result cache.
    A result the gateway's row cap cut ends the run as `TRUNCATED`, and a target feature group the export did not create as `TARGET_EXISTS`, both before anything is written.
    A result that stops before its end fails the run, also before anything is written.
    Otherwise the first run creates the Delta feature group and fills it; a later run replaces every row, or with a lookback the rows from the window start on, in one commit that keeps the feature group's id.
    The window starts at the execution's logical date, `HOPS_LOGICAL_DATE` (the start of a scheduled run's interval, now for a run without one), minus the lookback, truncated to the `metric_time` grain.

    The outcome is reported to the export's status with `generation` and the execution id from `HOPSWORKS_JOB_EXECUTION_ID`; a run that wrote nothing raises after reporting, so the job execution fails.
    A successful run of an export without a lookback also reports what it materialised, so that the gateway can answer queries of exactly its saved query from the feature group (see `_materialization`).

    Returns:
        The status reported.
    """
    api = SemanticLayerApi()
    sp = api._get_by_id(semantic_project_id)
    export = next((e for e in api._exports(sp) if e.name == export_name), None)
    if export is None:
        raise SemanticLayerException(
            f"Semantic project '{sp.name}' has no export '{export_name}'.",
            error_code="SEMANTIC_LAYER_NOT_FOUND",
        )
    logical_time = _logical_time()
    execution_id = os.environ.get("HOPSWORKS_JOB_EXECUTION_ID")
    report: dict[str, Any] = {
        "generation": generation,
        "executionId": int(execution_id) if execution_id else None,
        "logicalTime": logical_time.replace(microsecond=0).isoformat() + "Z",
        "featureGroupId": export.feature_group_id,
        "rows": None,
    }
    api._report_export(sp, export_name, {**report, "status": "RUNNING"})
    rules_revision, policy_free = _rules_at_start(api, sp, export_name)
    try:
        _write_export(
            api,
            sp,
            export,
            logical_time,
            report,
            rules_revision if policy_free else None,
        )
    except Exception as err:
        refused = (
            isinstance(err, SemanticLayerException)
            and err.error_code in _EXPORT_REFUSALS
        )
        report["status"] = err.error_code if refused else "FAILED"
        report["message"] = str(err)[:_EXPORT_MESSAGE_CHARS]
        api._report_export(sp, export_name, report)
        raise
    report["status"] = "SUCCEEDED"
    api._report_export(sp, export_name, report)
    _logger.info("Export %s of %s: %s", export_name, sp.name, report["message"])
    return report


def _rules_at_start(
    api: SemanticLayerApi, sp: semantic_layer.SemanticProject, export_name: str
) -> tuple[str | None, bool]:
    """The Trino rules revision at the run's start and whether the session lists the export as policy-free; `(None, False)` when the session cannot be read."""
    try:
        session = api._session(sp)
    except Exception as err:
        _logger.warning(
            "Export %s of %s records no materialisation: the session cannot be read: %s",
            export_name,
            sp.name,
            err,
        )
        return None, False
    entry = next(
        (e for e in session.get("exports") or [] if e.get("name") == export_name),
        {},
    )
    return session.get("rulesRevision"), entry.get("policyFree") is True


def _write_export(
    api: SemanticLayerApi,
    sp: semantic_layer.SemanticProject,
    export: semantic_layer.SemanticExport,
    logical_time: datetime.datetime,
    report: dict[str, Any],
    rules_revision: str | None = None,
) -> None:
    """Query the export's saved query and write its rows, setting `rows`, `featureGroupId`, `message` and, when one may be recorded, `materialization` in `report`.

    `rules_revision` is the Trino rules revision at the run's start, `None` unless the session then listed the export as policy-free.
    """
    import pandas as pd

    saved_query = next(
        (q for q in api._saved_queries(sp) if q.name == export.saved_query), None
    )
    if saved_query is None:
        raise SemanticLayerException(
            f"Semantic project '{sp.name}' has no saved query '{export.saved_query}'."
        )
    table, parents, headers = _complete_result(api, sp, export.saved_query)
    report["parentFeatureGroupIds"] = parents
    group_bys = [c for c in table.column_names if c not in saved_query.metrics]
    event_time, grain = _metric_time(group_bys)
    if export.lookback and event_time is None:
        raise SemanticLayerException(
            f"Export '{export.name}' has a lookback, which needs the saved query to group by metric_time."
        )
    fs = client._get_connection()._get_feature_store(api._project_name)
    # Lets a later run recognise a feature group a first run created but could not report.
    marker = f"[semantic export {sp.id}/{export.name}]"
    description = (
        saved_query.description
        or f"Export {export.name} of saved query {saved_query.name} of semantic project {sp.name}."
    )
    fg = fs.get_or_create_feature_group(
        name=export.feature_group,
        version=export.version or 1,
        description=f"{description} {marker}",
        primary_key=[c for c in group_bys if c != event_time] or group_bys,
        event_time=event_time,
        time_travel_format="DELTA",
        online_enabled=False,
        statistics_config=False,
    )
    unreported = export.feature_group_id is None and marker in (fg.description or "")
    if fg.id is not None and fg.id != export.feature_group_id and not unreported:
        raise SemanticLayerException(
            f"Feature group {export.feature_group} version {fg.version} exists and export "
            f"'{export.name}' did not create it, so nothing was written. Delete it, or give "
            "the export another alias or version.",
            error_code="TARGET_EXISTS",
        )
    replace: bool | datetime.datetime = True
    if fg.id is None:
        message = f"Created feature group {export.feature_group} version {fg.version}."
    elif not export.lookback:
        message = "Replaced every row."
    elif grain not in semantic_layer._GRAINS:
        message = (
            f"Replaced every row: {event_time} has a custom granularity, which a "
            "lookback window cannot be truncated to."
        )
    else:
        replace = _window_start(logical_time, export.lookback, grain)
        table = _rows_from(table, event_time, replace)
        message = f"Replaced the rows from {replace.isoformat()}Z on."
    write_options: dict[str, Any] = {"replace": replace}
    execution_id = report.get("executionId")
    app_id = f"semantic-export-{sp.id}-{export.name}"
    attempt = _transaction_version() if execution_id is not None else None
    if attempt is not None:
        write_options["commit_properties"] = {"app_id": app_id, "version": attempt}
    try:
        fg.insert(
            table.to_pandas(types_mapper=pd.ArrowDtype), write_options=write_options
        )
    finally:
        # A first run that fails after creating the feature group still owns it;
        # unrecorded, it would make every later run TARGET_EXISTS.
        report["featureGroupId"] = fg.id
    report["rows"] = table.num_rows
    report["message"] = message
    materialization = _materialization(
        export, fg, table, parents, headers, rules_revision, app_id, attempt
    )
    if materialization is not None:
        report["materialization"] = materialization


def _materialization(
    export: semantic_layer.SemanticExport,
    fg: FeatureGroup,
    table: pa.Table,
    parents: list[int],
    headers: Mapping[str, str],
    rules_revision: str | None,
    app_id: str,
    attempt: int | None,
) -> dict[str, Any] | None:
    """What a run wrote that the gateway may answer queries from, or `None` when any condition fails.

    Every condition of the contract must hold: the export has no lookback, so the run replaced every row; the session listed the export as policy-free at the run's start, under `rules_revision`; the gateway pinned every relation the query scanned (`X-Semantic-Source-Versions`, covering the feature groups it read) and named the build and the saved query's definition; and the Delta version of the run's own commit is found by its application transaction.
    """
    try:
        source_versions = json.loads(headers.get(_SOURCE_VERSIONS_HEADER) or "null")
        build_id = int(headers.get("X-Semantic-Build") or "")
    except ValueError:
        return None
    definition = headers.get(_DEFINITION_HEADER)
    if (
        export.lookback
        or not rules_revision
        or not isinstance(source_versions, dict)
        or not source_versions
        or not set(map(str, parents)) <= set(source_versions)
        or not definition
        or attempt is None
        or fg.id is None
    ):
        return None
    output_version = _output_version(fg, app_id, attempt)
    if output_version is None:
        return None
    return {
        "buildId": build_id,
        "definition": definition,
        "sourceVersions": {str(k): int(v) for k, v in source_versions.items()},
        "featureGroupId": fg.id,
        "outputVersion": output_version,
        "outputColumns": table.column_names,
        "rulesRevision": rules_revision,
    }


_OUTPUT_VERSION_SEARCH = 100


def _transaction_version() -> int:
    """The version of this write's application transaction: a random 63-bit number, so each attempt marks one commit.

    A re-run of an execution keeps its id, so the id would mark two commits, and a clock can repeat a reading.
    Delta does not require application transaction versions to grow.
    """
    return secrets.randbits(63)


def _output_version(fg: FeatureGroup, app_id: str, attempt: int) -> int | None:
    """The Delta version of the commit that carries the run's application transaction `(app_id, attempt)`, `None` when it is not found.

    The log is read with delta-rs from the head back: the commit carrying the transaction is the first version at which `DeltaTable.transaction_version(app_id)` is `attempt`, since each attempt's version is its own.
    Another writer may have committed after the run, so up to `_OUTPUT_VERSION_SEARCH` versions are read.
    """
    try:
        # The export job's process, not library code: hsfs is there once the job has logged in.
        from deltalake import DeltaTable
        from hsfs.core import delta_engine

        engine = delta_engine.DeltaEngine(
            feature_store_id=fg.feature_store_id,
            feature_store_name=fg.feature_store_name,
            feature_group=fg,
            spark_context=None,
            spark_session=None,
        )
        table = DeltaTable(
            engine._get_delta_rs_location(),
            storage_options=engine._get_delta_rs_storage_options() or None,
        )
        version = table.version()
        if table.transaction_version(app_id) != attempt:
            return None
        for _ in range(_OUTPUT_VERSION_SEARCH):
            if version == 0:
                return 0
            table.load_as_version(version - 1)
            if table.transaction_version(app_id) != attempt:
                return version
            version -= 1
    except Exception as err:
        _logger.warning("The Delta log of %s cannot be read: %s", fg.name, err)
    return None


def _complete_result(
    api: SemanticLayerApi, sp: semantic_layer.SemanticProject, saved_query: str
) -> tuple[pa.Table, list[int], Mapping[str, str]]:
    """The saved query's full result, the ids of the feature groups it read, which become the export's lineage parents, and the gateway's response headers.

    The query reads its sources, never an export, each at the head of its Delta log when the gateway can pin it.
    """
    response = api._gateway_request(
        "POST",
        sp,
        "query",
        body={
            "savedQuery": saved_query,
            "cache": False,
            "useExports": False,
            "pinVersions": True,
        },
        accept=_ARROW_STREAM,
    )
    if not response.headers.get("Content-Type", "").startswith(_ARROW_STREAM):
        raise SemanticLayerException(
            f"The gateway answered saved query '{saved_query}' without an Arrow stream."
        )
    table = _read_arrow_stream(sp, response)
    max_rows = response.headers.get("X-Semantic-Max-Rows")
    flagged = response.headers.get("X-Semantic-Truncated", "").lower() == "true"
    # The gateway streams Arrow, so its headers go out before it knows whether the cap
    # cut the result: one that fills the cap counts as cut, whatever the flag says.
    at_cap = bool(max_rows) and table.num_rows >= int(max_rows)
    if flagged or at_cap:
        raise SemanticLayerException(
            f"Saved query '{saved_query}' reached the gateway's cap of {max_rows} "
            "rows, so it may be incomplete and nothing was written. Narrow the "
            "saved query, or raise the gateway's MAX_ROWS.",
            error_code="TRUNCATED",
        )
    read = response.headers.get("X-Semantic-Feature-Groups", "")
    return (
        table,
        [int(fg_id) for fg_id in read.split(",") if fg_id.strip()],
        response.headers,
    )


def _metric_time(group_bys: list[str]) -> tuple[str | None, str | None]:
    """The `metric_time` column of an export and its grain, `None` without a suffix; a date part such as `metric_time__extract_dow` is a key column instead."""
    for column in group_bys:
        name, _, suffix = column.partition("__")
        if name == "metric_time" and not suffix.startswith("extract_"):
            return column, suffix.lower() or None
    return None, None


def _logical_time() -> datetime.datetime:
    """The execution's logical date as naive UTC, or now for an execution without one."""
    import pandas as pd

    value = os.environ.get("HOPS_LOGICAL_DATE")
    if not value:
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    instant = pd.Timestamp(value)
    if instant.tzinfo is not None:
        instant = instant.tz_convert("UTC").tz_localize(None)
    return instant.to_pydatetime()


def _window_start(
    logical_time: datetime.datetime, lookback: str, grain: str
) -> datetime.datetime:
    """`logical_time` minus a lookback such as `7 days`, truncated to `grain`."""
    import pandas as pd

    match = re.fullmatch(r"\s*(\d+)\s*([A-Za-z]+?)s?\s*", lookback)
    unit = match.group(2).lower() if match else None
    if unit not in _SPINE_GRAINS:
        raise SemanticLayerException(
            f"Lookback {lookback!r} is not a number and a grain from second to year, such as '7 days'."
        )
    count = int(match.group(1))
    offset = (
        pd.DateOffset(months=3 * count)
        if unit == "quarter"
        else pd.DateOffset(**{f"{unit}s": count})
    )
    return _truncate((pd.Timestamp(logical_time) - offset).to_pydatetime(), grain)


def _truncate(instant: datetime.datetime, grain: str) -> datetime.datetime:
    """The start of the period at `grain` that holds `instant`; weeks start on Monday, as MetricFlow's do."""
    midnight = instant.replace(hour=0, minute=0, second=0, microsecond=0)
    if grain == "year":
        return midnight.replace(month=1, day=1)
    if grain == "quarter":
        return midnight.replace(month=(instant.month - 1) // 3 * 3 + 1, day=1)
    if grain == "month":
        return midnight.replace(day=1)
    if grain == "week":
        return midnight - datetime.timedelta(days=instant.weekday())
    if grain == "day":
        return midnight
    if grain == "hour":
        return instant.replace(minute=0, second=0, microsecond=0)
    if grain == "minute":
        return instant.replace(second=0, microsecond=0)
    if grain == "second":
        return instant.replace(microsecond=0)
    if grain == "millisecond":
        return instant.replace(microsecond=instant.microsecond // 1000 * 1000)
    return instant


@_uses_pyarrow
def _rows_from(table: pa.Table, column: str, start: datetime.datetime) -> pa.Table:
    """The rows whose `column` is at or after `start`, a naive UTC datetime; a null is not."""
    import pyarrow as pa
    import pyarrow.compute as pc

    values = table[column]
    bound = start.date() if pa.types.is_date(values.type) else start
    return table.filter(pc.greater_equal(values, pa.scalar(bound, type=values.type)))


# endregion
