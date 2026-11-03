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
Discovery and queries go to the semantic gateway, at `/semantic-layer/v1` on the Hopsworks host, with the same credential and TLS settings as the Hopsworks client.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any, Literal

from hopsworks_apigen import public
from hopsworks_common import client, semantic_layer, usage
from hopsworks_common.client import auth
from hopsworks_common.client.exceptions import SemanticLayerException
from hopsworks_common.core.variable_api import VariableApi
from hopsworks_common.decorators import _uses_pyarrow


if TYPE_CHECKING:
    import pyarrow as pa
    import requests
    from hopsworks_common import project


_logger = logging.getLogger(__name__)

# Overrides the gateway address, as `https://<host>:<port>`, for a cluster that exposes it elsewhere.
_GATEWAY_URL_ENV = "HOPSWORKS_SEMANTIC_LAYER_URL"
_GATEWAY_SERVICE_NAME = "semantic-gateway.service"
_GATEWAY_PORT = 18443
_GATEWAY_PATH = "semantic-layer/v1"
_JSON = "application/json"
_ARROW_STREAM = "application/vnd.apache.arrow.stream"
# Only the connect is bounded: a query may run for as long as the gateway's own timeout allows.
_GATEWAY_TIMEOUT = (30, None)
_GROUP_BY_KEYS = {
    "dimensions": "dimensions",
    "entities": "entities",
    "group-bys": "groupBys",
}
_HINTS = {
    "SEMANTIC_LAYER_NO_VALID_BUILD": "Compile the semantic project and fix its errors: queries need one valid build.",
    "SEMANTIC_LAYER_FORBIDDEN": "The caller must be a member of the project, with an API key that has the FEATURESTORE and TRINO scopes.",
}


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

        Returns:
            The semantic project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the name is taken or invalid, the path is outside the project, or the backend encounters another error.
        """
        body: dict[str, Any] = {
            "name": name,
            "rootPath": self._absolute_path(root_path),
            "format": format.upper(),
            "deploymentMode": deployment_mode.upper(),
        }
        if git_repository_id is not None:
            body["gitRepositoryId"] = git_repository_id
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
        self, sp: semantic_layer.SemanticProject, wait: bool
    ) -> semantic_layer.SemanticBuild:
        _client = client._get_instance()
        response = _client._send_request(
            "POST",
            self._path(sp.id, "builds"),
            query_params={"wait": "true"} if wait else None,
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
        self, sp: semantic_layer.SemanticProject, build_id: int
    ) -> semantic_layer.SemanticBuild:
        _client = client._get_instance()
        return semantic_layer.SemanticBuild.from_response_json(
            _client._send_request("GET", self._path(sp.id, "builds", build_id))
        )

    def _promote(
        self, sp: semantic_layer.SemanticProject, build_id: int
    ) -> dict[str, Any]:
        _client = client._get_instance()
        return _client._send_request(
            "POST", self._path(sp.id, "builds", build_id, "promote")
        )

    # endregion

    # region Gateway

    def _gateway_base_url(self) -> str:
        if self._gateway_url is not None:
            return self._gateway_url
        override = os.environ.get(_GATEWAY_URL_ENV)
        if override:
            self._gateway_url = override.rstrip("/")
        elif client._is_external():
            # The ingress routes /semantic-layer/ on the Hopsworks host to the gateway.
            self._gateway_url = client._get_instance()._base_url.rstrip("/")
        else:
            domain = VariableApi()._get_service_discovery_domain()
            if not domain:
                raise SemanticLayerException(
                    "The semantic gateway cannot be found: the cluster's service discovery "
                    f"domain is not set. Set {_GATEWAY_URL_ENV} to its https://<host>:<port>."
                )
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
            raise _gateway_error(sp, response)
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
        self, sp: semantic_layer.SemanticProject
    ) -> list[semantic_layer.SemanticMetric]:
        result = self._gateway_json("GET", sp, "metrics")
        return [
            semantic_layer.SemanticMetric.from_response_json(metric)
            for metric in result.get("metrics") or []
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
            return _read_arrow_stream(response.content)
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


def _gateway_error(
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


@_uses_pyarrow
def _read_arrow_stream(content: bytes) -> pa.Table:
    import pyarrow as pa

    return pa.ipc.open_stream(content).read_all()
