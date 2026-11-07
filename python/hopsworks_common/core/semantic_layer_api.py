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

import json
import logging
import os
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

from hopsworks_apigen import public
from hopsworks_common import client, decorators, semantic_layer, tag, usage
from hopsworks_common.client import auth
from hopsworks_common.client.exceptions import RestAPIError, SemanticLayerException
from hopsworks_common.core.variable_api import VariableApi
from hopsworks_common.decorators import _uses_pyarrow


if TYPE_CHECKING:
    import datetime
    from collections.abc import Callable, Mapping

    import pandas as pd
    import pyarrow as pa
    import requests
    from hopsworks_common import project
    from hsfs.feature_group import FeatureGroup


_logger = logging.getLogger(__name__)

# Overrides the gateway address, as `https://<host>:<port>`, for a cluster that exposes it elsewhere.
_GATEWAY_URL_ENV = "HOPSWORKS_SEMANTIC_LAYER_URL"
_GATEWAY_SERVICE_NAME = "semantic-gateway.service"
_GATEWAY_PORT = 18443
_GATEWAY_PATH = "semantic-layer/v1"
_JSON = "application/json"
_ARROW_STREAM = "application/vnd.apache.arrow.stream"
# The gateway sends nothing until its answer is ready, so the read timeout bounds a whole query.
# 330 s outlasts the gateway's QUERY_TIMEOUT_SECONDS and the ingress proxy timeout, both 300 s
# by default, so the gateway's own 504 arrives first.
_GATEWAY_TIMEOUT = (30, 330)
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
        response = _client._send_request("GET", self._path()) or {}
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
            )

        response = send()
        if (
            response.status_code == 401
            and isinstance(_client._auth, auth.BearerAuth)
            and not _client._is_external()
        ):
            # A pod's JWT is rotated on disk; base.Client._retry_token_expired re-reads it the same way.
            _client._auth = auth.BearerAuth(_client._read_jwt())
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
            table = _read_arrow_stream(response.content)
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
def _read_arrow_stream(content: bytes) -> pa.Table:
    import pyarrow as pa

    return pa.ipc.open_stream(content).read_all()
