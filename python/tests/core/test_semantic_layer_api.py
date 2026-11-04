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

import json
import logging
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest
import requests
from hopsworks_common.client import auth
from hopsworks_common.client.exceptions import (
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.core.semantic_layer_api import SemanticLayerApi


SP_JSON = {
    "id": 7,
    "projectId": 119,
    "name": "sales",
    "rootPath": "/Projects/demo/Jupyter/repo/dbt",
    "gitRepositoryId": 3,
    "format": "DBT",
    "sqlEngine": "TRINO",
    "deploymentMode": "FOLLOW",
    "currentBuildId": 12,
    "latestBuild": {"id": 12, "semanticProjectId": 7, "status": "VALID"},
    "creator": "meb10000",
    "created": "2026-11-03T18:00:00Z",
}


def _response(
    status=200,
    body=None,
    content=None,
    content_type="application/json",
    headers=None,
    reason="OK",
):
    response = requests.Response()
    response.status_code = status
    response.reason = reason
    if content is None:
        content = b"" if body is None else json.dumps(body).encode()
    response._content = content
    response.headers["Content-Type"] = content_type
    response.headers.update(headers or {})
    return response


def _arrow_stream(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


@pytest.fixture
def http(mocker):
    """The Hopsworks client, external unless a test says otherwise; only its HTTP layer is fake."""
    instance = MagicMock()
    instance._project_id = 119
    instance._project_name = "demo"
    instance._base_url = "https://hopsworks.example.com:443"
    instance._auth = auth.ApiKeyAuth("key.secret")
    instance._verify = "/tmp/ca_chain.pem"
    instance._is_external.return_value = True
    instance._send_request.return_value = {"count": 1, "items": [SP_JSON]}
    mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
    mocker.patch("hopsworks_common.client._is_external", return_value=True)
    mocker.patch(
        "hopsworks_common.core.semantic_layer_api.VariableApi._get_service_discovery_domain",
        return_value="consul",
    )
    return instance


@pytest.fixture
def sales(http):
    return SemanticLayerApi().get_project("sales")


def _gateway_call(http, index=-1):
    call = http._session.request.call_args_list[index]
    return call.args[0], call.args[1], call.kwargs


class TestControlPlane:
    def test_get_projects_lists_the_project_scope(self, http):
        projects = SemanticLayerApi().get_projects()

        assert [sp.name for sp in projects] == ["sales"]
        call = http._send_request.call_args
        assert call.args == ("GET", ["project", 119, "semantic", "projects"])
        assert projects[0].latest_build.status == "VALID"

    def test_get_projects_without_items(self, http):
        # The REST API leaves `items` out of an empty collection.
        http._send_request.return_value = {"count": 0}

        assert SemanticLayerApi().get_projects() == []

    def test_get_project_is_none_for_an_unknown_name(self, http):
        assert SemanticLayerApi().get_project("missing") is None

    def test_create_sends_an_absolute_root_path(self, http):
        http._send_request.return_value = SP_JSON

        SemanticLayerApi().create_project("sales", "Jupyter/repo/dbt", format="dbt")

        call = http._send_request.call_args
        assert call.args == ("POST", ["project", 119, "semantic", "projects"])
        assert json.loads(call.kwargs["data"]) == {
            "name": "sales",
            "rootPath": "/Projects/demo/Jupyter/repo/dbt",
            "format": "DBT",
            "deploymentMode": "FOLLOW",
        }

    @pytest.mark.parametrize(
        "root_path",
        [
            "/hopsfs/Jupyter/repo/dbt",
            "/Projects/demo/Jupyter/repo/dbt",
            "Jupyter/repo/dbt/",
        ],
    )
    def test_create_accepts_terminal_and_absolute_paths(self, http, root_path):
        http._send_request.return_value = SP_JSON

        SemanticLayerApi().create_project(
            "sales", root_path, git_repository_id=3, deployment_mode="pinned"
        )

        body = json.loads(http._send_request.call_args.kwargs["data"])
        assert body["rootPath"] == "/Projects/demo/Jupyter/repo/dbt"
        assert body["gitRepositoryId"] == 3
        assert body["deploymentMode"] == "PINNED"

    def test_delete_resolves_the_name_to_the_id(self, http):
        SemanticLayerApi().delete_project("sales")

        assert http._send_request.call_args.args == (
            "DELETE",
            ["project", 119, "semantic", "projects", 7],
        )

    def test_delete_of_an_unknown_name_raises(self, http):
        with pytest.raises(SemanticLayerException) as err:
            SemanticLayerApi().delete_project("missing")

        assert err.value.error_code == "SEMANTIC_LAYER_NOT_FOUND"
        assert http._send_request.call_count == 1

    def test_promote_updates_the_project_in_place(self, http, sales):
        http._send_request.return_value = {**SP_JSON, "currentBuildId": 9}

        assert sales.promote(9) is sales

        assert http._send_request.call_args.args == (
            "POST",
            ["project", 119, "semantic", "projects", 7, "builds", 9, "promote"],
        )
        assert sales.current_build_id == 9

    def test_update_puts_only_the_given_settings_with_an_absolute_root(
        self, http, sales
    ):
        http._send_request.return_value = {**SP_JSON, "deploymentMode": "PINNED"}

        assert sales.update(root_path="Resources/v2", deployment_mode="pinned") is sales

        call = http._send_request.call_args
        assert call.args == ("PUT", ["project", 119, "semantic", "projects", 7])
        assert json.loads(call.kwargs["data"]) == {
            "rootPath": "/Projects/demo/Resources/v2",
            "deploymentMode": "PINNED",
        }
        assert sales.deployment_mode == "PINNED"

    def test_update_needs_a_setting(self, http, sales):
        with pytest.raises(ValueError):
            sales.update()

    def test_get_builds_newest_first_without_issues(self, http, sales):
        http._send_request.return_value = {
            "count": 2,
            "items": [{"id": 12, "status": "INVALID"}, {"id": 11, "status": "VALID"}],
        }

        builds = sales.get_builds()

        assert [b.id for b in builds] == [12, 11]
        assert builds[0].issues is None


def _control_plane(*builds):
    """A `_send_request` that answers the compile POST and build GETs with `builds` in order, and the project GET."""
    remaining = list(builds)

    def answer(method, path, **kwargs):
        if method == "GET" and path == ["project", 119, "semantic", "projects", 7]:
            return {**SP_JSON, "currentBuildId": 13}
        return remaining.pop(0)

    return answer


class TestCompile:
    def test_wait_posts_with_wait_true_and_returns_the_valid_build(self, http, sales):
        http._send_request.reset_mock()  # the fixture listed the projects
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "VALID", "errorCount": 0}
        )

        build = sales.compile()

        post = http._send_request.call_args_list[0]
        assert post.args == (
            "POST",
            ["project", 119, "semantic", "projects", 7, "builds"],
        )
        assert post.kwargs["query_params"] == {"wait": "true"}
        assert build.id == 13

    def test_a_compile_refreshes_the_build_queries_use(self, http, sales):
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "VALID", "errorCount": 0}
        )

        sales.compile()

        assert sales.current_build_id == 13

    def test_every_request_of_a_waited_compile_is_bounded(self, http, sales, mocker):
        http._send_request.reset_mock()  # the fixture listed the projects
        mocker.patch("hopsworks_common.semantic_layer.time.sleep")
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "COMPILING"}, {"id": 13, "status": "VALID"}
        )

        sales.compile()

        for call in http._send_request.call_args_list:
            connect, read = call.kwargs["timeout"]
            assert 0 < connect <= 30
            assert 0 < read <= 600

    def test_a_stalled_request_ends_the_wait_with_a_semantic_layer_error(
        self, http, sales
    ):
        http._send_request.side_effect = requests.Timeout("read timed out")

        with pytest.raises(SemanticLayerException, match="has not ended"):
            sales.compile()

    def test_without_wait_returns_the_compiling_build(self, http, sales):
        http._send_request.return_value = {"id": 13, "status": "COMPILING"}

        build = sales.compile(wait=False)

        assert http._send_request.call_args.kwargs["query_params"] is None
        assert build.status == "COMPILING"

    def test_invalid_build_raises_with_the_issues_of_the_single_build_get(
        self, http, sales
    ):
        # The POST response carries no issues; only the single-build GET does.
        issue = {
            "level": "ERROR",
            "rule": "UNIQUE_NAMES",
            "message": "metric bookings is defined twice",
            "file": "models/semantic.yml",
            "line": 12,
            "column": 5,
            "element": "metric:bookings",
        }
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "INVALID", "errorCount": 1, "warningCount": 0},
            {"id": 13, "status": "INVALID", "errorCount": 1, "issues": [issue]},
        )

        with pytest.raises(SemanticBuildException) as err:
            sales.compile()

        assert http._send_request.call_args.args == (
            "GET",
            ["project", 119, "semantic", "projects", 7, "builds", 13],
        )
        assert err.value.build.id == 13
        assert [i.rule for i in err.value.issues] == ["UNIQUE_NAMES"]
        assert "is INVALID with 1 errors" in str(err.value)
        assert (
            "ERROR UNIQUE_NAMES models/semantic.yml:12:5 metric:bookings: "
            "metric bookings is defined twice"
        ) in str(err.value)

    def test_failed_build_raises_even_without_issues(self, http, sales):
        http._send_request.reset_mock()  # the fixture listed the projects
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "FAILED", "issues": []}
        )

        with pytest.raises(SemanticBuildException) as err:
            sales.compile()

        assert err.value.issues == []
        methods = [call.args[0] for call in http._send_request.call_args_list]
        assert methods == [
            "POST",
            "GET",
        ]  # the compile and the project, no GET for issues

    def test_a_build_still_compiling_after_the_wait_is_polled(
        self, http, sales, mocker
    ):
        mocker.patch("hopsworks_common.semantic_layer.time.sleep")
        http._send_request.side_effect = _control_plane(
            {"id": 13, "status": "COMPILING"},
            {"id": 13, "status": "COMPILING"},
            {"id": 13, "status": "VALID"},
        )

        assert sales.compile().status == "VALID"


class TestGateway:
    def test_external_clients_use_the_hopsworks_host_and_credential(self, http, sales):
        http._session.request.return_value = _response(
            body={"metrics": [{"name": "revenue", "type": "SIMPLE", "agg": "SUM"}]}
        )

        metrics = sales.list_metrics()

        method, url, kwargs = _gateway_call(http)
        assert method == "GET"
        assert (
            url
            == "https://hopsworks.example.com:443/semantic-layer/v1/projects/7/metrics"
        )
        assert kwargs["auth"] is http._auth
        assert kwargs["verify"] == "/tmp/ca_chain.pem"
        assert metrics[0].agg == "SUM"

    def test_requests_bound_the_read_past_the_gateway_query_timeout(self, http, sales):
        http._session.request.return_value = _response(body={"metrics": []})

        sales.list_metrics()

        connect, read = _gateway_call(http)[2]["timeout"]
        assert connect is not None
        assert read is not None and read > 300

    def test_internal_clients_use_the_gateway_service(self, http, sales, mocker):
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        http._session.request.return_value = _response(body={"metrics": []})

        sales.list_metrics()

        assert _gateway_call(http)[1] == (
            "https://semantic-gateway.service.consul:18443/semantic-layer/v1/projects/7/metrics"
        )

    def test_external_clients_at_a_consul_host_use_the_gateway_service(
        self, http, sales
    ):
        http._base_url = "https://hopsworks.glassfish.service.consul:28181"
        http._session.request.return_value = _response(body={"metrics": []})

        sales.list_metrics()

        assert _gateway_call(http)[1] == (
            "https://semantic-gateway.service.consul:18443/semantic-layer/v1/projects/7/metrics"
        )

    def test_the_url_can_be_overridden(self, http, sales, monkeypatch):
        monkeypatch.setenv(
            "HOPSWORKS_SEMANTIC_LAYER_URL", "https://gw.example.com:8443/"
        )
        http._session.request.return_value = _response(body={"dimensions": []})

        sales.dimensions()

        assert _gateway_call(http)[1] == (
            "https://gw.example.com:8443/semantic-layer/v1/projects/7/dimensions"
        )

    def test_an_expired_pod_jwt_is_reread_once(self, http, sales, mocker):
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        http._is_external.return_value = False
        http._auth = auth.BearerAuth("old")
        http._read_jwt.return_value = "new"
        http._session.request.side_effect = [
            _response(status=401, body={"errorCode": "SEMANTIC_LAYER_UNAUTHENTICATED"}),
            _response(body={"metrics": []}),
        ]

        sales.list_metrics()

        assert http._session.request.call_count == 2
        assert http._auth._token == "new"

    @pytest.mark.parametrize(
        ("method", "resource", "key"),
        [
            ("dimensions", "dimensions", "dimensions"),
            ("entities", "entities", "entities"),
            ("group_bys", "group-bys", "groupBys"),
        ],
    )
    def test_discovery_passes_metrics_comma_separated(
        self, http, sales, method, resource, key
    ):
        http._session.request.return_value = _response(
            body={
                key: [
                    {
                        "name": "metric_time",
                        "type": "TIME",
                        "entityPath": [],
                        "queryableGranularities": ["DAY", "MONTH"],
                    }
                ]
            }
        )

        group_bys = getattr(sales, method)(metrics=["revenue", "order_count"])

        _, url, kwargs = _gateway_call(http)
        assert url.endswith(f"/projects/7/{resource}")
        assert kwargs["params"] == {"metrics": "revenue,order_count"}
        assert group_bys[0].queryable_granularities == ["DAY", "MONTH"]

    def test_discovery_without_metrics_sends_no_filter(self, http, sales):
        http._session.request.return_value = _response(body={"dimensions": []})

        sales.dimensions()

        assert _gateway_call(http)[2]["params"] is None

    def test_compatible_metrics_parses_its_group_bys(self, http, sales):
        http._session.request.return_value = _response(body={"metrics": ["revenue"]})

        assert sales.compatible_metrics(
            ["metric_time__month", "customer__country"]
        ) == ["revenue"]

        assert json.loads(_gateway_call(http)[2]["data"]) == {
            "groupBy": [
                {"name": "metric_time", "grain": "month"},
                {"name": "customer__country"},
            ]
        }

    def test_dimension_values_body(self, http, sales):
        http._session.request.return_value = _response(body={"values": ["SE", "US"]})

        values = sales.dimension_values(
            "customer__country", metrics="revenue", limit=10
        )

        assert values == ["SE", "US"]
        method, url, kwargs = _gateway_call(http)
        assert (method, url.rsplit("/", 1)[1]) == ("POST", "dimension-values")
        assert json.loads(kwargs["data"]) == {
            "groupBy": {"name": "customer__country"},
            "metrics": ["revenue"],
            "limit": 10,
        }

    def test_sql_posts_the_query_body(self, http, sales):
        http._session.request.return_value = _response(
            body={"sql": "SELECT 1", "buildId": 12, "stale": False}
        )

        sql = sales.sql(
            ["revenue"],
            group_by="metric_time__month",
            order_by="-revenue",
            limit=5,
        )

        assert sql == "SELECT 1"
        method, url, kwargs = _gateway_call(http)
        assert method == "POST"
        assert url.endswith("/projects/7/sql")
        assert kwargs["headers"]["Content-Type"] == "application/json"
        assert json.loads(kwargs["data"]) == {
            "metrics": ["revenue"],
            "groupBy": [{"name": "metric_time", "grain": "month"}],
            "orderBy": [{"name": "revenue", "descending": True}],
            "limit": 5,
        }

    def test_a_stale_answer_logs_a_warning(self, http, sales, caplog):
        http._session.request.return_value = _response(
            body={"sql": "SELECT 1", "buildId": 11, "stale": True}
        )

        with caplog.at_level(logging.WARNING):
            sales.sql(["revenue"])

        assert "answered from build 11" in caplog.text

    def test_serving_reports_the_build_and_staleness_without_warning(
        self, http, sales, caplog
    ):
        http._session.request.return_value = _response(
            body={"metrics": [], "buildId": 11, "stale": True}
        )

        with caplog.at_level(logging.WARNING):
            serving = sales.serving()

        assert (serving.build_id, serving.stale) == (11, True)
        assert _gateway_call(http)[1].endswith("/semantic-layer/v1/projects/7/metrics")
        assert caplog.text == ""


class TestErrors:
    def test_gateway_error_body_maps_to_the_exception(self, http, sales):
        http._session.request.return_value = _response(
            status=409,
            reason="Conflict",
            body={
                "errorCode": "SEMANTIC_LAYER_NO_VALID_BUILD",
                "errorMsg": "Semantic project 7 has no valid build",
                "usrMsg": "",
            },
        )

        with pytest.raises(SemanticLayerException) as err:
            sales.list_metrics()

        assert err.value.status_code == 409
        assert err.value.error_code == "SEMANTIC_LAYER_NO_VALID_BUILD"
        assert err.value.error_msg == "Semantic project 7 has no valid build"
        assert "HTTP 409, SEMANTIC_LAYER_NO_VALID_BUILD" in str(err.value)
        assert "Compile the semantic project" in str(err.value)

    def test_execution_failure_carries_the_engine_message(self, http, sales):
        http._session.request.return_value = _response(
            status=502,
            body={
                "errorCode": "SEMANTIC_LAYER_EXECUTION_FAILED",
                "errorMsg": "Query failed",
                "usrMsg": "line 1:8: Column 'x' cannot be resolved",
            },
        )

        with pytest.raises(SemanticLayerException) as err:
            sales.query(["revenue"])

        assert err.value.usr_msg == "line 1:8: Column 'x' cannot be resolved"
        assert "Column 'x' cannot be resolved" in str(err.value)

    def test_a_non_json_error_keeps_the_http_reason(self, http, sales):
        http._session.request.return_value = _response(
            status=503,
            reason="Service Unavailable",
            content=b"<html>upstream down</html>",
            content_type="text/html",
        )

        with pytest.raises(SemanticLayerException) as err:
            sales.list_metrics()

        assert err.value.error_code is None
        assert err.value.error_msg == "Service Unavailable"


class TestQuery:
    JSON_RESULT = {
        "columns": [
            {"name": "metric_time__month", "type": "DATE"},
            {"name": "revenue", "type": "DOUBLE"},
        ],
        "rows": [["2026-01-01", 10.5], ["2026-02-01", None]],
        "rowCount": 2,
        "truncated": False,
        "sql": "SELECT ...",
        "buildId": 12,
        "stale": False,
    }

    def test_arrow_is_requested_when_pyarrow_is_installed(self, http, sales):
        table = pa.table({"metric_time__month": ["2026-01-01"], "revenue": [10.5]})
        http._session.request.return_value = _response(
            content=_arrow_stream(table),
            content_type="application/vnd.apache.arrow.stream",
            headers={"X-Semantic-Build": "12", "X-Semantic-Stale": "false"},
        )

        df = sales.query(["revenue"], group_by=["metric_time__month"])

        assert _gateway_call(http)[2]["headers"]["Accept"] == (
            "application/vnd.apache.arrow.stream"
        )
        assert isinstance(df, pd.DataFrame)
        assert df.to_dict(orient="list") == {
            "metric_time__month": ["2026-01-01"],
            "revenue": [10.5],
        }

    @pytest.mark.parametrize(
        ("dataframe_type", "kind"),
        [("pyarrow", pa.Table), ("python", list), ("pandas", pd.DataFrame)],
    )
    def test_arrow_result_conversions(self, http, sales, dataframe_type, kind):
        table = pa.table({"revenue": [1.0, 2.0]})
        http._session.request.return_value = _response(
            content=_arrow_stream(table),
            content_type="application/vnd.apache.arrow.stream",
        )

        result = sales.query(["revenue"], dataframe_type=dataframe_type)

        assert isinstance(result, kind)
        if dataframe_type == "python":
            assert result == [{"revenue": 1.0}, {"revenue": 2.0}]

    def test_json_is_requested_without_pyarrow(self, http, sales, mocker):
        mocker.patch("hopsworks_common.semantic_layer.HAS_PYARROW", False)
        http._session.request.return_value = _response(body=self.JSON_RESULT)

        df = sales.query(["revenue"], group_by=["metric_time__month"])

        assert _gateway_call(http)[2]["headers"]["Accept"] == "application/json"
        assert list(df.columns) == ["metric_time__month", "revenue"]
        assert df["revenue"].iloc[0] == 10.5

    def test_json_rows_as_python_dicts(self, http, sales, mocker):
        mocker.patch("hopsworks_common.semantic_layer.HAS_PYARROW", False)
        http._session.request.return_value = _response(body=self.JSON_RESULT)

        rows = sales.query(["revenue"], dataframe_type="python")

        assert rows == [
            {"metric_time__month": "2026-01-01", "revenue": 10.5},
            {"metric_time__month": "2026-02-01", "revenue": None},
        ]

    def test_a_json_answer_to_an_arrow_request_is_still_read(self, http, sales):
        http._session.request.return_value = _response(body=self.JSON_RESULT)

        table = sales.query(["revenue"], dataframe_type="pyarrow")

        assert table.column_names == ["metric_time__month", "revenue"]
        assert table.num_rows == 2

    def test_a_truncated_json_result_logs_a_warning(self, http, sales, mocker, caplog):
        mocker.patch("hopsworks_common.semantic_layer.HAS_PYARROW", False)
        http._session.request.return_value = _response(
            body={**self.JSON_RESULT, "truncated": True}
        )

        with caplog.at_level(logging.WARNING):
            sales.query(["revenue"])

        assert "cut at 2 rows" in caplog.text

    def test_an_arrow_result_at_the_row_cap_logs_a_warning(self, http, sales, caplog):
        http._session.request.return_value = _response(
            content=_arrow_stream(pa.table({"revenue": [1.0, 2.0]})),
            content_type="application/vnd.apache.arrow.stream",
            headers={"X-Semantic-Max-Rows": "2"},
        )

        with caplog.at_level(logging.WARNING):
            sales.query(["revenue"], dataframe_type="pyarrow")

        assert "may be cut at the gateway's row limit of 2 rows" in caplog.text

    @pytest.mark.parametrize(
        "headers",
        [{"X-Semantic-Max-Rows": "3"}, {}],
        ids=["below_the_cap", "older_gateway_without_the_header"],
    )
    def test_an_arrow_result_without_a_reached_cap_logs_nothing(
        self, http, sales, caplog, headers
    ):
        http._session.request.return_value = _response(
            content=_arrow_stream(pa.table({"revenue": [1.0, 2.0]})),
            content_type="application/vnd.apache.arrow.stream",
            headers=headers,
        )

        with caplog.at_level(logging.WARNING):
            table = sales.query(["revenue"], dataframe_type="pyarrow")

        assert table.num_rows == 2
        assert caplog.text == ""

    def test_an_unknown_dataframe_type_fails_before_any_request(self, http, sales):
        with pytest.raises(ValueError, match="dataframe_type"):
            sales.query(["revenue"], dataframe_type="spark")

        http._session.request.assert_not_called()
