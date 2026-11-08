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

import datetime
import json
import logging
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest
import requests
from hopsworks_common.client import auth
from hopsworks_common.client.exceptions import (
    RestAPIError,
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.core import semantic_layer_api
from hopsworks_common.core.semantic_layer_api import (
    SemanticLayerApi,
    _time_spine_frame,
)
from hopsworks_common.semantic_layer import SemanticExport, SemanticSavedQuery
from hopsworks_common.tag import Tag


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
    def test_mandatory_tags_join_cluster_and_project_rules_for_semantic_projects(
        self, http
    ):
        rules = {
            "mandatorytags": {
                "items": [
                    {"name": "owner", "semanticProjectSpecific": True},
                    {"name": "pii", "featureGroupSpecific": True},
                ]
            },
            "project": {
                "items": [
                    {"name": "domain", "semanticProjectSpecific": True},
                    {"name": "owner", "semanticProjectSpecific": True},
                    {"name": "certified", "semanticMetricSpecific": True},
                ]
            },
        }
        http._send_request.side_effect = lambda method, path: rules[path[0]]

        assert SemanticLayerApi()._mandatory_tags() == {"owner", "domain"}
        assert [c.args[1] for c in http._send_request.call_args_list] == [
            ["mandatorytags"],
            ["project", 119, "mandatorytags", "project"],
        ]

    def test_no_mandatory_tags_when_no_rule_has_items(self, http):
        http._send_request.side_effect = [{"count": 0}, None]

        assert SemanticLayerApi()._mandatory_tags() == set()

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

    def test_create_sends_tags_as_json_text_in_the_same_request(self, http):
        http._send_request.return_value = SP_JSON

        SemanticLayerApi().create_project(
            "sales",
            "Jupyter/repo/dbt",
            tags={"ownership": {"owner": "finance"}, "domain": "sales", "tier": 1},
        )

        body = json.loads(http._send_request.call_args.kwargs["data"])
        assert body["tags"] == [
            {"name": "ownership", "value": '{"owner": "finance"}'},
            {"name": "domain", "value": '"sales"'},
            {"name": "tier", "value": "1"},
        ]
        assert http._send_request.call_count == 1

    def test_create_without_tags_sends_none(self, http):
        http._send_request.return_value = SP_JSON

        SemanticLayerApi().create_project("sales", "Jupyter/repo/dbt", tags={})

        assert "tags" not in json.loads(http._send_request.call_args.kwargs["data"])

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


class TestTags:
    def test_add_tag_puts_the_value_as_json_under_the_project(self, http, sales):
        sales.add_tag("ownership", {"owner": "finance", "certified": True})

        call = http._send_request.call_args
        assert call.args == (
            "PUT",
            ["project", 119, "semantic", "projects", 7, "tags", "ownership"],
        )
        assert json.loads(call.kwargs["data"]) == {
            "owner": "finance",
            "certified": True,
        }

    def test_get_tags_decodes_each_value(self, http, sales):
        http._send_request.return_value = {
            "count": 2,
            "items": [
                {"name": "ownership", "value": '{"owner": "finance"}'},
                {"name": "domain", "value": '"sales"'},
            ],
        }

        assert sales.get_tags() == {
            "ownership": {"owner": "finance"},
            "domain": "sales",
        }
        assert http._send_request.call_args.args == (
            "GET",
            ["project", 119, "semantic", "projects", 7, "tags"],
        )

    def test_get_tag_reads_one_tag(self, http, sales):
        http._send_request.return_value = {"name": "domain", "value": '"sales"'}

        assert sales.get_tag("domain") == "sales"
        assert http._send_request.call_args.args[1][-2:] == ["tags", "domain"]

    def test_a_missing_tag_is_none_and_a_project_without_tags_is_empty(
        self, http, sales
    ):
        not_found = RestAPIError(
            "url", _response(404, body={"errorCode": Tag.NOT_FOUND_ERROR_CODE})
        )
        http._send_request.side_effect = not_found

        assert sales.get_tag("domain") is None
        assert sales.get_tags() == {}

    def test_delete_tag(self, http, sales):
        sales.delete_tag("domain")

        assert http._send_request.call_args.args == (
            "DELETE",
            ["project", 119, "semantic", "projects", 7, "tags", "domain"],
        )


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

    def test_list_metrics_filters_by_every_tag_and_reads_their_tags(self, http, sales):
        http._session.request.return_value = _response(
            body={
                "metrics": [
                    {"name": "revenue", "type": "SIMPLE", "tags": {"domain": "sales"}},
                ]
            }
        )

        metrics = sales.list_metrics(tag=["domain", "ownership"])

        _, url, kwargs = _gateway_call(http)
        assert url.endswith("/projects/7/metrics")
        assert kwargs["params"] == {"tag": ["domain", "ownership"]}
        assert metrics[0].tags == {"domain": "sales"}

    def test_list_metrics_takes_one_tag_and_sends_no_filter_without_one(
        self, http, sales
    ):
        http._session.request.return_value = _response(body={"metrics": []})

        sales.list_metrics(tag="domain")
        assert _gateway_call(http)[2]["params"] == {"tag": ["domain"]}

        sales.list_metrics()
        assert _gateway_call(http)[2]["params"] is None

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


class TestSavedQueries:
    SAVED_QUERY = {
        "name": "revenue_by_country",
        "label": "Revenue by country",
        "description": "...",
        "metrics": ["revenue", "orders"],
        "groupBy": ["metric_time__month", "customer__country"],
        "where": ["{{ Dimension('customer__country') }} != 'XX'"],
        "orderBy": [
            {"name": "metric_time__month"},
            {"name": "revenue", "descending": True},
        ],
        "limit": None,
        "tags": {"owner": {"team": "finance"}},
    }

    def test_list_saved_queries_reads_the_gateway_answer(self, http, sales, caplog):
        http._session.request.return_value = _response(
            body={"savedQueries": [self.SAVED_QUERY], "buildId": 11, "stale": False}
        )

        saved_queries = sales.list_saved_queries()

        method, url, kwargs = _gateway_call(http)
        assert method == "GET"
        assert url.endswith("/semantic-layer/v1/projects/7/saved-queries")
        assert kwargs["data"] is None
        assert saved_queries == [
            SemanticSavedQuery(
                name="revenue_by_country",
                label="Revenue by country",
                description="...",
                metrics=["revenue", "orders"],
                group_by=["metric_time__month", "customer__country"],
                where=["{{ Dimension('customer__country') }} != 'XX'"],
                order_by=[
                    {"name": "metric_time__month", "descending": False},
                    {"name": "revenue", "descending": True},
                ],
                tags={"owner": {"team": "finance"}},
            )
        ]
        assert caplog.text == ""

    def test_a_stale_answer_warns(self, http, sales, caplog):
        http._session.request.return_value = _response(
            body={"savedQueries": [self.SAVED_QUERY], "buildId": 11, "stale": True}
        )

        with caplog.at_level(logging.WARNING):
            saved_queries = sales.list_saved_queries()

        assert [q.name for q in saved_queries] == ["revenue_by_country"]
        assert "answered from build 11" in caplog.text

    def test_no_saved_queries(self, http, sales):
        http._session.request.return_value = _response(
            body={"savedQueries": [], "buildId": 11, "stale": False}
        )

        assert sales.list_saved_queries() == []

    def test_query_sends_the_saved_query_with_its_refinements_and_no_metrics(
        self, http, sales
    ):
        table = pa.table({"metric_time__month": ["2026-01-01"], "revenue": [10.5]})
        http._session.request.return_value = _response(
            content=_arrow_stream(table),
            content_type="application/vnd.apache.arrow.stream",
        )

        df = sales.query(
            saved_query="revenue_by_country",
            group_by=["metric_time__month"],
            where=["{{ Dimension('customer__country') }} = 'SE'"],
            limit=5,
        )

        method, url, kwargs = _gateway_call(http)
        assert (method, url.rsplit("/", 1)[1]) == ("POST", "query")
        assert json.loads(kwargs["data"]) == {
            "savedQuery": "revenue_by_country",
            "groupBy": [{"name": "metric_time", "grain": "month"}],
            "where": ["{{ Dimension('customer__country') }} = 'SE'"],
            "limit": 5,
        }
        assert df["revenue"].tolist() == [10.5]

    @pytest.mark.parametrize(
        ("kwargs", "body"),
        [
            ({"group_by": []}, {"savedQuery": "revenue_by_country", "groupBy": []}),
            ({"order_by": []}, {"savedQuery": "revenue_by_country", "orderBy": []}),
            ({"metrics": []}, {"savedQuery": "revenue_by_country"}),
        ],
    )
    def test_an_empty_group_by_or_order_by_replaces_the_saved_one(
        self, http, sales, kwargs, body
    ):
        http._session.request.return_value = _response(
            body={"sql": "SELECT 1", "buildId": 12, "stale": False}
        )

        sales.sql(saved_query="revenue_by_country", **kwargs)

        assert json.loads(_gateway_call(http)[2]["data"]) == body

    def test_sql_of_a_saved_query(self, http, sales):
        http._session.request.return_value = _response(
            body={"sql": "SELECT 1", "buildId": 12, "stale": False}
        )

        assert sales.sql(saved_query="revenue_by_country") == "SELECT 1"

        method, url, kwargs = _gateway_call(http)
        assert (method, url.rsplit("/", 1)[1]) == ("POST", "sql")
        assert json.loads(kwargs["data"]) == {"savedQuery": "revenue_by_country"}

    @pytest.mark.parametrize(
        "call",
        [
            lambda sp: sp.query("revenue", saved_query="revenue_by_country"),
            lambda sp: sp.sql(["revenue"], saved_query="revenue_by_country"),
        ],
    )
    def test_metrics_and_a_saved_query_together_fail_before_any_request(
        self, http, sales, call
    ):
        with pytest.raises(ValueError, match="names its own metrics"):
            call(sales)

        http._session.request.assert_not_called()

    def test_an_unknown_saved_query_raises_not_found(self, http, sales):
        http._session.request.return_value = _response(
            status=404,
            reason="Not Found",
            body={
                "errorCode": "SEMANTIC_LAYER_NOT_FOUND",
                "errorMsg": "no saved query named 'nope'",
            },
        )

        with pytest.raises(SemanticLayerException) as err:
            sales.query(saved_query="nope")

        assert err.value.status_code == 404
        assert err.value.error_code == "SEMANTIC_LAYER_NOT_FOUND"
        assert "no saved query named 'nope'" in str(err.value)


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


class TestSuperset:
    DASHBOARD = {
        "dashboardId": 12,
        "title": "Sales overview",
        "url": "https://h/hopsworks-api/superset/dashboard/12/?projectId=119",
    }

    def test_create_dashboard_posts_the_request_and_returns_id_title_and_url(
        self, http, sales
    ):
        http._send_request.return_value = self.DASHBOARD

        dashboard = sales.create_dashboard(
            ["revenue", "orders"], group_by="customer__country", title="Sales overview"
        )

        call = http._send_request.call_args
        assert call.args == (
            "POST",
            ["project", 119, "semantic", "projects", 7, "dashboards"],
        )
        assert json.loads(call.kwargs["data"]) == {
            "metrics": ["revenue", "orders"],
            "groupBy": ["customer__country"],
            "title": "Sales overview",
        }
        assert dashboard == {
            "id": 12,
            "title": "Sales overview",
            "url": self.DASHBOARD["url"],
        }

    def test_create_dashboard_leaves_out_what_is_not_given(self, http, sales):
        http._send_request.return_value = self.DASHBOARD

        sales.create_dashboard("revenue")

        assert json.loads(http._send_request.call_args.kwargs["data"]) == {
            "metrics": ["revenue"]
        }

    @pytest.mark.parametrize(
        ("title", "body"),
        [
            ("Revenue", {"savedQuery": "revenue_by_country", "title": "Revenue"}),
            (None, {"savedQuery": "revenue_by_country"}),
        ],
    )
    def test_create_dashboard_from_a_saved_query_sends_its_name_and_the_title(
        self, http, sales, title, body
    ):
        http._send_request.return_value = self.DASHBOARD

        dashboard = sales.create_dashboard(
            saved_query="revenue_by_country", title=title
        )

        call = http._send_request.call_args
        assert call.args[1][-1] == "dashboards"
        assert json.loads(call.kwargs["data"]) == body
        assert dashboard["id"] == 12

    def test_empty_metrics_and_group_bys_go_with_a_saved_query(self, http, sales):
        http._send_request.return_value = self.DASHBOARD

        sales.create_dashboard([], group_by=[], saved_query="revenue_by_country")

        assert json.loads(http._send_request.call_args.kwargs["data"]) == {
            "savedQuery": "revenue_by_country"
        }

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"metrics": "revenue", "saved_query": "q"}, "leave out metrics"),
            ({"group_by": "customer__country", "saved_query": "q"}, "group_by"),
            ({}, "Give metrics or saved_query"),
        ],
    )
    def test_create_dashboard_needs_metrics_or_a_saved_query_alone(
        self, http, sales, kwargs, message
    ):
        requests_before = http._send_request.call_count

        with pytest.raises(ValueError, match=message):
            sales.create_dashboard(**kwargs)

        assert http._send_request.call_count == requests_before

    def test_explore_url_names_the_metric(self, http, sales):
        http._send_request.return_value = {"url": "https://h/explore/?form_data=x"}

        assert sales.explore_url("revenue") == "https://h/explore/?form_data=x"

        call = http._send_request.call_args
        assert call.args == (
            "GET",
            ["project", 119, "semantic", "projects", 7, "explore"],
        )
        assert call.kwargs["query_params"] == {"metric": "revenue"}

    @pytest.mark.parametrize(
        ("call", "status", "body"),
        [
            (
                lambda sp: sp.create_dashboard(["revnue"]),
                400,
                {
                    "errorCode": 590004,
                    "errorMsg": "Illegal argument provided",
                    "usrMsg": "unknown metric 'revnue'",
                },
            ),
            (
                lambda sp: sp.explore_url("revenue"),
                503,
                {
                    "errorCode": 590014,
                    "errorMsg": "Superset dashboards from the semantic layer are not enabled",
                },
            ),
            (
                lambda sp: sp.create_dashboard(["revenue"]),
                503,
                {"errorCode": 590015, "errorMsg": "Superset did not answer"},
            ),
            (
                lambda sp: sp.create_dashboard(saved_query="nope"),
                404,
                {
                    "errorCode": 590001,
                    "errorMsg": "Semantic project not found",
                    "usrMsg": "Semantic project sales has no saved query nope.",
                },
            ),
        ],
    )
    def test_a_refusal_raises_a_semantic_layer_exception(
        self, http, sales, call, status, body
    ):
        http._send_request.side_effect = RestAPIError(
            "https://h/hopsworks-api/api/project/119/semantic/projects/7",
            _response(status=status, body=body),
        )

        with pytest.raises(SemanticLayerException) as err:
            call(sales)

        assert err.value.status_code == status
        assert err.value.error_code == body["errorCode"]
        assert err.value.error_msg == body["errorMsg"]
        assert f"HTTP {status}, {body['errorCode']}" in str(err.value)
        assert body.get("usrMsg", body["errorMsg"]) in str(err.value)
        assert isinstance(err.value.__cause__, RestAPIError)


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

    def test_the_cache_is_read_unless_told_not_to(self, http, sales):
        http._session.request.return_value = _response(
            content=_arrow_stream(pa.table({"revenue": [1.0]})),
            content_type="application/vnd.apache.arrow.stream",
        )

        sales.query(["revenue"])
        sales.query(["revenue"], read_cache=False)

        bodies = [
            json.loads(c.kwargs["data"]) for c in http._session.request.call_args_list
        ]
        assert "cache" not in bodies[0]
        assert bodies[1] == {"metrics": ["revenue"], "cache": False}


class TestTimeSpine:
    @pytest.fixture
    def connection(self, http, mocker):
        connection = MagicMock(name="Connection")
        mocker.patch("hopsworks_common.client._get_connection", return_value=connection)
        return connection

    @pytest.fixture
    def fs(self, connection):
        return connection._get_feature_store.return_value

    def test_create_writes_the_spine_feature_group_in_the_projects_feature_store(
        self, connection, fs
    ):
        fg = fs.get_or_create_feature_group.return_value

        result = SemanticLayerApi().create_time_spine(
            "2026-01-30",
            datetime.date(2026, 2, 2),
            custom_granularities={
                "fiscal_quarter": "Q-JAN",
                "fiscal_year": lambda day: f"FY{day.year + (day.month > 1)}",
            },
        )

        assert result is fg
        connection._get_feature_store.assert_called_once_with("demo")
        fs.get_or_create_feature_group.assert_called_once_with(
            name="time_spine_day",
            version=1,
            description="Time spine, one row per day, for the semantic layer.",
            primary_key=["date_day"],
            event_time="date_day",
            online_enabled=False,
            statistics_config=False,
        )
        df = fg.insert.call_args.args[0]
        assert fg.insert.call_args.kwargs == {"wait": True}
        assert df.to_dict(orient="list") == {
            "date_day": [
                datetime.date(2026, 1, 30),
                datetime.date(2026, 1, 31),
                datetime.date(2026, 2, 1),
                datetime.date(2026, 2, 2),
            ],
            "fiscal_quarter": [datetime.date(2025, 11, 1)] * 2
            + [datetime.date(2026, 2, 1)] * 2,
            "fiscal_year": ["FY2026", "FY2026", "FY2027", "FY2027"],
        }

    def test_the_feature_group_and_its_key_are_named_after_the_grain(self, fs):
        SemanticLayerApi().create_time_spine(
            "2026-01-01", "2026-01-01", granularity="month"
        )

        kwargs = fs.get_or_create_feature_group.call_args.kwargs
        assert (kwargs["name"], kwargs["primary_key"]) == (
            "time_spine_month",
            ["date_month"],
        )

    @pytest.mark.parametrize(
        ("granularity", "start", "end", "expected"),
        [
            (
                "week",
                "2026-01-01",
                "2026-01-12",
                [
                    datetime.date(2025, 12, 29),
                    datetime.date(2026, 1, 5),
                    datetime.date(2026, 1, 12),
                ],
            ),
            (
                "month",
                "2026-01-15",
                "2026-03-01",
                [
                    datetime.date(2026, 1, 1),
                    datetime.date(2026, 2, 1),
                    datetime.date(2026, 3, 1),
                ],
            ),
            (
                "quarter",
                "2026-05-05",
                "2026-07-01",
                [datetime.date(2026, 4, 1), datetime.date(2026, 7, 1)],
            ),
            ("year", "2026-05-05", "2026-12-31", [datetime.date(2026, 1, 1)]),
        ],
    )
    def test_each_row_is_the_start_of_a_period_from_the_one_holding_start(
        self, granularity, start, end, expected
    ):
        df = _time_spine_frame(start, end, granularity, {})

        assert list(df.columns) == [f"date_{granularity}"]
        assert df[f"date_{granularity}"].tolist() == expected

    def test_a_spine_below_a_day_holds_timestamps(self):
        df = _time_spine_frame(
            "2026-01-31 22:30", "2026-02-01 00:10", "hour", {"fiscal_month": "M"}
        )

        assert df["date_hour"].tolist() == [
            pd.Timestamp("2026-01-31 22:00"),
            pd.Timestamp("2026-01-31 23:00"),
            pd.Timestamp("2026-02-01 00:00"),
        ]
        assert df["fiscal_month"].tolist() == [
            pd.Timestamp("2026-01-01"),
            pd.Timestamp("2026-01-01"),
            pd.Timestamp("2026-02-01"),
        ]

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (("2026-01-01", "2026-12-31", "fortnight", {}), "not one of second"),
            (("2026-02-01", "2026-01-01", "day", {}), "is before its start"),
            (("2026-02-15", "2026-02-01", "month", {}), "is before its start"),
            (
                ("2026-01-01", "2026-01-31", "day", {"fq": "quarterly"}),
                "Custom granularity 'fq': 'quarterly' is not a pandas period frequency",
            ),
        ],
    )
    def test_bad_arguments_fail_before_the_feature_group_is_touched(
        self, fs, args, message
    ):
        start, end, granularity, custom = args

        with pytest.raises(ValueError, match=message):
            SemanticLayerApi().create_time_spine(start, end, granularity, custom)

        fs.get_or_create_feature_group.assert_not_called()


EXPORT_JSON = {
    "name": "revenue_daily",
    "savedQuery": "revenue_by_country",
    "exportAs": "TABLE",
    "featureGroup": "revenue_daily",
    "featureGroupId": None,
    "version": 1,
    "schedule": "0 0 2 * * ?",
    "lookback": None,
    "enabled": True,
    "jobName": "semantic_export_sales_revenue_daily",
    "lastStatus": "NEVER_RUN",
}


HISTORY_JSON = {
    "executionId": "3f8c2a52-0000-4000-8000-000000000001",
    "user": "meb10000",
    "surface": "REST",
    "engine": "TRINO",
    "buildId": 12,
    "status": "SUCCEEDED",
    "error": None,
    "rows": 42,
    "startedAt": "2026-11-08T10:00:00.123Z",
    "finishedAt": "2026-11-08T10:00:01.456Z",
    "durationMs": 1333,
    "cache": "miss",
    "servedFrom": None,
    "trinoQueryId": "20261108_100000_00012_abcde",
    "request": {"metrics": ["revenue"]},
    "sql": "SELECT 1",
}


class TestQueryHistory:
    def test_pages_with_the_cursor_until_the_limit(self, http, sales):
        failed = {**HISTORY_JSON, "status": "FAILED", "rows": None, "error": "boom"}
        http._send_request.reset_mock()
        http._send_request.side_effect = [
            {"items": [HISTORY_JSON, failed], "nextCursor": "c1"},
            {"items": [HISTORY_JSON], "nextCursor": "c2"},
        ]

        frame = sales.query_history(limit=3, status=["failed", "SUCCEEDED"], user="bob")

        calls = http._send_request.call_args_list
        assert [c.args for c in calls] == [
            ("GET", ["project", 119, "semantic", "projects", 7, "queries"])
        ] * 2
        assert calls[0].kwargs["query_params"] == {
            "limit": 3,
            "status": "FAILED,SUCCEEDED",
            "user": "bob",
        }
        assert calls[1].kwargs["query_params"]["cursor"] == "c1"
        assert calls[1].kwargs["query_params"]["limit"] == 1
        assert list(frame.columns) == [
            "execution_id",
            "user",
            "surface",
            "engine",
            "build_id",
            "status",
            "error",
            "rows",
            "started_at",
            "finished_at",
            "duration_ms",
            "cache",
            "served_from",
            "trino_query_id",
            "request",
            "sql",
        ]
        assert len(frame) == 3
        assert frame["status"].tolist() == ["SUCCEEDED", "FAILED", "SUCCEEDED"]
        assert frame["rows"].tolist()[0] == 42
        assert frame["rows"].isna().tolist() == [False, True, False]
        assert str(frame["started_at"].iloc[0]) == "2026-11-08 10:00:00.123000+00:00"
        assert frame["request"].iloc[0] == {"metrics": ["revenue"]}

    def test_stops_at_the_last_page(self, http, sales):
        http._send_request.reset_mock()
        http._send_request.return_value = {"items": [], "nextCursor": None}

        frame = sales.query_history()

        assert http._send_request.call_count == 1
        assert http._send_request.call_args.kwargs["query_params"] == {"limit": 100}
        assert frame.empty
        assert "started_at" in frame.columns


class TestExports:
    def test_exports_lists_the_control_plane_answer(self, http, sales):
        http._send_request.return_value = {"items": [EXPORT_JSON]}

        exports = sales.exports()

        http._send_request.assert_called_with(
            "GET", ["project", 119, "semantic", "projects", 7, "exports"]
        )
        assert exports == [SemanticExport.from_response_json(EXPORT_JSON)]

    def test_no_exports(self, http, sales):
        http._send_request.return_value = {}

        assert sales.exports() == []

    @pytest.mark.parametrize("wait", [False, True])
    def test_run_export_starts_the_job_and_returns_its_execution(
        self, http, sales, mocker, wait
    ):
        http._send_request.return_value = {
            "jobName": "semantic_export_sales_revenue_daily",
            "executionId": 77,
        }
        get_job = mocker.patch(
            "hopsworks_common.core.job_api.JobApi.get_job", autospec=True
        )
        job = get_job.return_value

        result = sales.run_export("revenue_daily", wait=wait)

        http._send_request.assert_called_with(
            "POST",
            [
                "project",
                119,
                "semantic",
                "projects",
                7,
                "exports",
                "revenue_daily",
                "run",
            ],
        )
        assert get_job.call_args.args[1] == "semantic_export_sales_revenue_daily"
        job._execution_api._get.assert_called_once_with(job, 77)
        execution = job._execution_api._get.return_value
        if wait:
            job._execution_engine._wait_until_finished.assert_called_once_with(
                job, execution
            )
            assert result is job._execution_engine._wait_until_finished.return_value
        else:
            job._execution_engine._wait_until_finished.assert_not_called()
            assert result is execution


class TestConnectionInfo:
    CONNECTION = {
        "environmentId": 7,
        "graphqlUrl": "https://hopsworks.example.com/semantic-layer/api/graphql",
        "adbcUrl": "grpc+tls://flight.example.com:18446",
        "jdbcUrl": "jdbc:arrow-flight-sql://flight.example.com:18446?environmentId=7",
        "flightHost": "flight.example.com",
        "flightPort": 18446,
        "env": {
            "DBT_SL_GRAPHQL_URL_FORMAT": "https://{server_host}/semantic-layer/api/graphql",
            "DBT_SL_ADBC_URL_FORMAT": "grpc+tls://flight.example.com:18446",
        },
        "caPem": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
    }

    def test_the_ca_is_written_and_the_env_points_at_it(self, http, sales, tmp_path):
        http._send_request.return_value = self.CONNECTION
        ca_path = str(tmp_path / "ca.pem")

        info = sales.connection_info(ca_path=ca_path)

        http._send_request.assert_called_with(
            "GET", ["project", 119, "semantic", "projects", 7, "connection"]
        )
        with open(ca_path) as ca_file:
            assert ca_file.read() == self.CONNECTION["caPem"]
        assert info == {
            "environment_id": 7,
            "graphql_url": "https://hopsworks.example.com/semantic-layer/api/graphql",
            "adbc_url": "grpc+tls://flight.example.com:18446",
            "jdbc_url": "jdbc:arrow-flight-sql://flight.example.com:18446?environmentId=7",
            "flight_host": "flight.example.com",
            "flight_port": 18446,
            "env": {
                **self.CONNECTION["env"],
                "SSL_CERT_FILE": ca_path,
                "REQUESTS_CA_BUNDLE": ca_path,
            },
            "ca_path": ca_path,
        }

    def test_the_default_ca_file_is_named_after_the_project_and_semantic_project(
        self, http, sales, mocker, tmp_path
    ):
        http._send_request.return_value = self.CONNECTION
        mocker.patch(
            "hopsworks_common.semantic_layer._temp_dir", return_value=str(tmp_path)
        )

        info = sales.connection_info()

        assert info["ca_path"] == str(tmp_path / "hopsworks_semantic_119_7_ca.pem")

    def test_without_a_private_ca_nothing_is_written(self, http, sales):
        http._send_request.return_value = {**self.CONNECTION, "caPem": None}

        info = sales.connection_info()

        assert info["ca_path"] is None
        assert info["env"] == self.CONNECTION["env"]


def _written(replace):
    """The write options of an attempt marked 77 of export revenue_daily of semantic project 7, which replaces `replace`."""
    return {
        "replace": replace,
        "commit_properties": {
            "app_id": "semantic-export-7-revenue_daily",
            "version": 77,
        },
    }


class TestExportJob:
    @pytest.fixture(autouse=True)
    def attempt(self, mocker):
        mocker.patch(
            "hopsworks_common.core.semantic_layer_api._transaction_version",
            return_value=77,
        )

    SAVED_QUERY = {
        "name": "revenue_by_country",
        "description": "Daily revenue per country",
        "metrics": ["revenue"],
        "groupBy": ["metric_time__DAY", "customer__country"],
    }
    ROWS = pa.table(
        {
            "metric_time__day": pa.array(
                [datetime.datetime(2026, 10, d) for d in (20, 30, 31)],
                pa.timestamp("us"),
            ),
            "customer__country": ["SE", "SE", "NO"],
            "revenue": [1.0, 2.0, 3.0],
        }
    )

    @pytest.fixture
    def job(self, http, mocker, monkeypatch):
        """A run of export revenue_daily of semantic project 7, against fake control plane, gateway and feature store."""
        monkeypatch.setenv("HOPSWORKS_JOB_EXECUTION_ID", "77")
        monkeypatch.setenv("HOPS_LOGICAL_DATE", "2026-11-07T02:00:00Z")
        state = SimpleNamespace(
            export=dict(EXPORT_JSON),
            rows=self.ROWS,
            headers={"X-Semantic-Max-Rows": "1000000"},
            reports=[],
            started=[],
            session={},
            fg=MagicMock(name="FeatureGroup", id=None, version=1, description=None),
        )

        def control_plane(method, path, **kwargs):
            if path[:2] == ["semantic", "sessions"]:
                if isinstance(state.session, Exception):
                    raise state.session
                return state.session
            if path[-1] == "exports":
                return {"items": [state.export]}
            if path[-1] == "status":
                body = json.loads(kwargs["data"])
                (
                    state.started if body["status"] == "RUNNING" else state.reports
                ).append(body)
                return None
            return SP_JSON

        def gateway(method, url, **kwargs):
            if url.endswith("/saved-queries"):
                return _response(
                    body={"savedQueries": [self.SAVED_QUERY], "buildId": 12}
                )
            return _response(
                content=_arrow_stream(state.rows),
                content_type="application/vnd.apache.arrow.stream",
                headers=state.headers,
            )

        http._send_request.side_effect = control_plane
        http._session.request.side_effect = gateway
        connection = MagicMock(name="Connection")
        mocker.patch("hopsworks_common.client._get_connection", return_value=connection)
        state.fs = connection._get_feature_store.return_value
        state.fs.get_or_create_feature_group.return_value = state.fg

        def insert(frame, write_options):
            state.started_before_write = len(state.started)
            state.written = frame
            state.write_options = write_options
            if state.fg.id is None:
                state.fg.id = 501

        state.fg.insert.side_effect = insert
        return state

    def test_the_feature_groups_the_query_read_are_reported_as_parents(self, http, job):
        job.headers = {**job.headers, "X-Semantic-Feature-Groups": "11,12"}

        semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.reports[-1]["parentFeatureGroupIds"] == [11, 12]

    def test_a_run_reports_running_before_it_writes(self, http, job):
        semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.started_before_write == 1
        assert job.started[0]["status"] == "RUNNING"
        assert (job.started[0]["generation"], job.started[0]["executionId"]) == (3, 77)

    def test_a_query_that_read_no_feature_group_reports_no_parents(self, http, job):
        semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.reports[-1]["parentFeatureGroupIds"] == []

    def test_the_first_run_creates_the_feature_group_with_every_row(self, http, job):
        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        method, url, kwargs = _gateway_call(http)
        assert (method, url) == (
            "POST",
            "https://hopsworks.example.com:443/semantic-layer/v1/projects/7/query",
        )
        assert json.loads(kwargs["data"]) == {
            "savedQuery": "revenue_by_country",
            "cache": False,
            "useExports": False,
            "pinVersions": True,
        }
        assert kwargs["headers"]["Accept"] == "application/vnd.apache.arrow.stream"
        job.fs.get_or_create_feature_group.assert_called_once_with(
            name="revenue_daily",
            version=1,
            description="Daily revenue per country [semantic export 7/revenue_daily]",
            primary_key=["customer__country"],
            event_time="metric_time__day",
            time_travel_format="DELTA",
            online_enabled=False,
            statistics_config=False,
        )
        assert job.write_options == _written(True)
        assert len(job.written) == 3
        assert str(job.written["customer__country"].dtype) == "string[pyarrow]"
        assert job.reports == [report]
        assert report == {
            "generation": 3,
            "executionId": 77,
            "logicalTime": "2026-11-07T02:00:00Z",
            "featureGroupId": 501,
            "parentFeatureGroupIds": [],
            "rows": 3,
            "status": "SUCCEEDED",
            "message": "Created feature group revenue_daily version 1.",
        }
        assert http._send_request.call_args_list[-1].args[:2] == (
            "PUT",
            [
                "project",
                119,
                "semantic",
                "projects",
                7,
                "exports",
                "revenue_daily",
                "status",
            ],
        )

    def test_a_later_run_with_a_lookback_replaces_the_window_only(self, job):
        job.export.update(featureGroupId=501, lookback="7 days")
        job.fg.id = 501

        report = semantic_layer_api._run_export(7, "revenue_daily", 4)

        # 2026-11-07T02:00 minus 7 days, truncated to the day.
        start = datetime.datetime(2026, 10, 31)
        assert job.write_options == _written(start)
        assert job.written["customer__country"].tolist() == ["NO"]
        assert (report["status"], report["rows"]) == ("SUCCEEDED", 1)
        assert report["message"] == "Replaced the rows from 2026-10-31T00:00:00Z on."

    def test_an_empty_window_replaces_the_window_with_nothing(self, job):
        job.export.update(featureGroupId=501, lookback="1 day")
        job.fg.id = 501

        report = semantic_layer_api._run_export(7, "revenue_daily", 4)

        assert job.write_options == _written(datetime.datetime(2026, 11, 6))
        assert len(job.written) == 0
        assert list(job.written.columns) == [
            "metric_time__day",
            "customer__country",
            "revenue",
        ]
        assert report["rows"] == 0

    def test_a_later_run_without_a_lookback_replaces_every_row(self, job):
        job.export["featureGroupId"] = 501
        job.fg.id = 501

        report = semantic_layer_api._run_export(7, "revenue_daily", 4)

        assert job.write_options == _written(True)
        assert report["message"] == "Replaced every row."

    @pytest.mark.parametrize(
        "headers",
        [
            {"X-Semantic-Truncated": "true", "X-Semantic-Max-Rows": "4"},
            {"X-Semantic-Max-Rows": "3"},
            {"X-Semantic-Truncated": "false", "X-Semantic-Max-Rows": "3"},
        ],
        ids=["flagged", "at_the_cap", "at_the_cap_whatever_the_flag_says"],
    )
    def test_a_cut_result_writes_nothing_and_reports_truncated(self, job, headers):
        job.headers = headers

        with pytest.raises(SemanticLayerException, match="gateway's cap") as err:
            semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert err.value.error_code == "TRUNCATED"
        job.fs.get_or_create_feature_group.assert_not_called()
        assert [(r["status"], r["rows"]) for r in job.reports] == [("TRUNCATED", None)]

    def test_a_result_below_the_cap_is_written(self, job):
        job.headers = {"X-Semantic-Truncated": "false", "X-Semantic-Max-Rows": "4"}

        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert report["status"] == "SUCCEEDED"

    @pytest.mark.parametrize("recorded", [None, 499])
    def test_a_feature_group_the_export_did_not_create_is_not_written(
        self, job, recorded
    ):
        job.export["featureGroupId"] = recorded
        job.fg.id = 501

        with pytest.raises(SemanticLayerException, match="did not create it"):
            semantic_layer_api._run_export(7, "revenue_daily", 3)

        job.fg.insert.assert_not_called()
        assert job.reports[0]["status"] == "TARGET_EXISTS"
        assert job.reports[0]["featureGroupId"] == recorded

    def test_a_feature_group_a_first_run_created_but_did_not_report_is_taken_over(
        self, job
    ):
        job.fg.id = 501
        job.fg.description = (
            "Daily revenue per country [semantic export 7/revenue_daily]"
        )

        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.write_options == _written(True)
        assert (report["status"], report["featureGroupId"]) == ("SUCCEEDED", 501)

    def test_another_exports_marker_does_not_take_a_feature_group_over(self, job):
        job.fg.id = 501
        job.fg.description = "[semantic export 7/revenue_weekly]"

        with pytest.raises(SemanticLayerException, match="did not create it"):
            semantic_layer_api._run_export(7, "revenue_daily", 3)

    def test_a_failure_is_reported_and_raised(self, job):
        job.fg.insert.side_effect = RuntimeError("HopsFS is down")

        with pytest.raises(RuntimeError, match="HopsFS is down"):
            semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.reports[0]["status"] == "FAILED"
        assert job.reports[0]["message"] == "HopsFS is down"

    def test_a_first_run_failing_after_creating_the_feature_group_reports_it(self, job):
        def insert(frame, write_options):
            job.fg.id = 501
            raise RuntimeError("the write failed")

        job.fg.insert.side_effect = insert

        with pytest.raises(RuntimeError):
            semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert (job.reports[0]["status"], job.reports[0]["featureGroupId"]) == (
            "FAILED",
            501,
        )

    def test_a_lookback_without_metric_time_fails(self, job):
        job.export["lookback"] = "7 days"
        job.rows = job.rows.drop_columns(["metric_time__day"])

        with pytest.raises(SemanticLayerException, match="group by metric_time"):
            semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.reports[0]["status"] == "FAILED"

    def test_a_custom_grain_falls_back_to_replacing_every_row(self, job):
        job.export.update(featureGroupId=501, lookback="7 days")
        job.fg.id = 501
        job.rows = job.rows.rename_columns(
            ["metric_time__fiscal_quarter", "customer__country", "revenue"]
        )

        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert job.write_options == _written(True)
        assert "custom granularity" in report["message"]

    def test_without_other_group_bys_metric_time_is_the_key(self, job):
        job.rows = job.rows.drop_columns(["customer__country"])

        semantic_layer_api._run_export(7, "revenue_daily", 3)

        kwargs = job.fs.get_or_create_feature_group.call_args.kwargs
        assert (kwargs["primary_key"], kwargs["event_time"]) == (
            ["metric_time__day"],
            "metric_time__day",
        )

    def test_a_run_without_a_logical_date_uses_now(self, job, monkeypatch):
        monkeypatch.delenv("HOPS_LOGICAL_DATE")

        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        logical = datetime.datetime.fromisoformat(report["logicalTime"][:-1])
        assert abs(
            datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - logical
        ) < datetime.timedelta(minutes=1)

    def test_an_unknown_export_raises_before_any_query(self, http, job):
        with pytest.raises(SemanticLayerException, match="no export 'nope'"):
            semantic_layer_api._run_export(7, "nope", 3)

        http._session.request.assert_not_called()
        assert job.reports == []


class TestExportMaterialization:
    """What a run reports it materialised (HWORKS-3740 API contract §3, runner steps 1 to 4)."""

    RULES = "b" * 64

    @pytest.fixture
    def run(self, http, mocker, monkeypatch):
        job = TestExportJob.job.__wrapped__(TestExportJob(), http, mocker, monkeypatch)
        job.export["featureGroupId"] = 501
        job.fg.id = 501
        job.session = {
            "rulesRevision": self.RULES,
            "exports": [
                {"name": "other", "policyFree": False},
                {"name": "revenue_daily", "policyFree": True},
            ],
        }
        job.headers = {
            **job.headers,
            "X-Semantic-Build": "12",
            "X-Semantic-Definition": "a" * 64,
            "X-Semantic-Source-Versions": '{"11":4,"12":0}',
            "X-Semantic-Feature-Groups": "11",
        }
        job.output_version = mocker.patch(
            "hopsworks_common.core.semantic_layer_api._output_version", return_value=9
        )
        mocker.patch(
            "hopsworks_common.core.semantic_layer_api._transaction_version",
            return_value=77,
        )
        return job

    def test_a_full_refresh_run_reports_what_it_materialised(self, http, run):
        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert report["materialization"] == {
            "buildId": 12,
            "definition": "a" * 64,
            "sourceVersions": {"11": 4, "12": 0},
            "featureGroupId": 501,
            "outputVersion": 9,
            "outputColumns": ["metric_time__day", "customer__country", "revenue"],
            "rulesRevision": self.RULES,
        }
        run.output_version.assert_called_once_with(
            run.fg, "semantic-export-7-revenue_daily", 77
        )
        assert run.write_options == _written(True)
        assert "materialization" not in run.started[0]
        session_calls = [
            c
            for c in http._send_request.call_args_list
            if c.args[1][:2] == ["semantic", "sessions"]
        ]
        assert len(session_calls) == 1

    @pytest.mark.parametrize(
        "change",
        [
            lambda run: run.export.update(lookback="7 days"),
            lambda run: run.session.update(rulesRevision=None),
            lambda run: run.session["exports"][1].update(policyFree=False),
            lambda run: run.session.update(exports=[]),
            lambda run: setattr(run, "session", RuntimeError("Payara is down")),
            lambda run: run.headers.pop("X-Semantic-Source-Versions"),
            lambda run: run.headers.update({"X-Semantic-Feature-Groups": "11,13"}),
            lambda run: run.headers.update({"X-Semantic-Source-Versions": "{}"}),
            lambda run: run.headers.pop("X-Semantic-Definition"),
            lambda run: run.headers.pop("X-Semantic-Build"),
            lambda run: setattr(run.output_version, "return_value", None),
        ],
        ids=[
            "lookback",
            "no_rules_revision",
            "not_policy_free",
            "not_listed",
            "session_unreadable",
            "unpinned",
            "a_source_not_pinned",
            "no_sources",
            "no_definition",
            "no_build",
            "output_commit_not_found",
        ],
    )
    def test_any_condition_failing_reports_none(self, run, change):
        change(run)

        report = semantic_layer_api._run_export(7, "revenue_daily", 3)

        assert report["status"] == "SUCCEEDED"
        assert "materialization" not in report


class _DeltaLog:
    """A Delta log for `_output_version`: the version of each commit's `semantic-export-7-daily` transaction."""

    def __init__(self, transactions):
        self.transactions = transactions
        self.at = len(transactions) - 1
        self.loaded = []

    def __call__(self, location, storage_options=None):
        return self

    def version(self):
        return self.at

    def load_as_version(self, version):
        self.loaded.append(version)
        self.at = version

    def transaction_version(self, app_id):
        assert app_id == "semantic-export-7-daily"
        return self.transactions[self.at]


def _fake_delta_engine(mocker):
    """The Delta engine module of hsfs, faked with the packages above it: importing hsfs needs the generated alias modules."""
    engine = SimpleNamespace(DeltaEngine=MagicMock(name="DeltaEngine"))
    core = SimpleNamespace(delta_engine=engine)
    mocker.patch.dict(
        sys.modules,
        {
            "hsfs": SimpleNamespace(core=core),
            "hsfs.core": core,
            "hsfs.core.delta_engine": engine,
        },
    )


def test_each_write_attempt_has_its_own_transaction_version():
    versions = {semantic_layer_api._transaction_version() for _ in range(1000)}
    assert len(versions) == 1000
    assert all(0 <= v < 2**63 for v in versions)


class TestOutputVersion:
    @pytest.fixture
    def log(self, mocker):
        def install(transactions):
            delta_log = _DeltaLog(transactions)
            mocker.patch("deltalake.DeltaTable", delta_log)
            _fake_delta_engine(mocker)
            return delta_log

        return install

    def test_the_head_is_the_runs_commit(self, log):
        delta_log = log([None, 76, 77])

        assert (
            semantic_layer_api._output_version(
                MagicMock(), "semantic-export-7-daily", 77
            )
            == 2
        )
        assert delta_log.loaded == [1]

    def test_a_later_commit_of_another_writer_is_skipped(self, log):
        log([None, 76, 77, 77, 77])

        assert (
            semantic_layer_api._output_version(
                MagicMock(), "semantic-export-7-daily", 77
            )
            == 2
        )

    def test_the_first_commit_of_the_table(self, log):
        log([77])

        assert (
            semantic_layer_api._output_version(
                MagicMock(), "semantic-export-7-daily", 77
            )
            == 0
        )

    def test_a_log_without_the_runs_transaction(self, log):
        log([None, 76, 78])

        assert (
            semantic_layer_api._output_version(
                MagicMock(), "semantic-export-7-daily", 77
            )
            is None
        )

    def test_an_unreadable_log(self, mocker):
        mocker.patch("deltalake.DeltaTable", side_effect=OSError("HopsFS is down"))
        _fake_delta_engine(mocker)

        assert (
            semantic_layer_api._output_version(
                MagicMock(), "semantic-export-7-daily", 77
            )
            is None
        )


class TestExportWindow:
    @pytest.mark.parametrize(
        ("lookback", "grain", "start"),
        [
            ("7 days", "day", datetime.datetime(2026, 10, 31)),
            ("1 day", "hour", datetime.datetime(2026, 11, 6, 2)),
            ("2 weeks", "week", datetime.datetime(2026, 10, 19)),
            ("1 month", "month", datetime.datetime(2026, 10, 1)),
            ("1 quarter", "quarter", datetime.datetime(2026, 7, 1)),
            ("1 year", "year", datetime.datetime(2025, 1, 1)),
            (" 3 Hours ", "minute", datetime.datetime(2026, 11, 6, 23, 30)),
        ],
    )
    def test_the_window_starts_a_lookback_before_truncated_to_the_grain(
        self, lookback, grain, start
    ):
        logical = datetime.datetime(2026, 11, 7, 2, 30, 15)

        assert semantic_layer_api._window_start(logical, lookback, grain) == start

    def test_month_arithmetic_keeps_the_month_end(self):
        assert semantic_layer_api._window_start(
            datetime.datetime(2026, 3, 31, 12), "1 month", "day"
        ) == datetime.datetime(2026, 2, 28)

    @pytest.mark.parametrize("lookback", ["seven days", "7", "7 fortnights", "-1 day"])
    def test_a_lookback_that_is_not_a_count_and_a_grain_is_refused(self, lookback):
        with pytest.raises(SemanticLayerException, match="is not a number and a grain"):
            semantic_layer_api._window_start(
                datetime.datetime(2026, 11, 7), lookback, "day"
            )

    @pytest.mark.parametrize(
        ("columns", "found"),
        [
            (["metric_time__day", "c"], ("metric_time__day", "day")),
            (["c", "metric_time__MONTH"], ("metric_time__MONTH", "month")),
            (["metric_time__extract_dow", "c"], (None, None)),
            (["metric_time"], ("metric_time", None)),
            (["booking__metric_time__day"], (None, None)),
        ],
    )
    def test_the_metric_time_column(self, columns, found):
        assert semantic_layer_api._metric_time(columns) == found

    def test_rows_from_keeps_dates_and_aware_timestamps_at_or_after_the_start(self):
        table = pa.table(
            {
                "d": pa.array(
                    [datetime.date(2026, 11, 1), datetime.date(2026, 11, 2), None]
                ),
                "t": pa.array(
                    [
                        datetime.datetime(2026, 11, 1, 23),
                        datetime.datetime(2026, 11, 2),
                        None,
                    ],
                    pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        start = datetime.datetime(2026, 11, 2)

        assert semantic_layer_api._rows_from(table, "d", start).num_rows == 1
        assert semantic_layer_api._rows_from(table, "t", start).num_rows == 1
