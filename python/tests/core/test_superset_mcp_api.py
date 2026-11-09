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
from unittest.mock import Mock

import pytest
import requests
from hopsworks_common.client.exceptions import SupersetException
from hopsworks_common.core import superset_mcp_api
from hopsworks_common.core.superset_api import SupersetApi
from hopsworks_common.core.superset_mcp_api import _SupersetMcpClient


def _response(
    body: dict, sse: bool = False, headers: dict | None = None, status: int = 200
):
    response = Mock(spec=requests.Response)
    response.status_code = status
    if sse:
        response.headers = {"content-type": "text/event-stream", **(headers or {})}
        response.text = f"event: message\ndata: {json.dumps(body)}\n\n"
    else:
        response.headers = {"content-type": "application/json", **(headers or {})}
        response.json.return_value = body
        response.text = json.dumps(body)
    return response


def _tool_result(payload: dict, is_error: bool = False) -> dict:
    return {
        "content": [{"type": "text", "text": json.dumps(payload)}],
        "isError": is_error,
    }


class TestSupersetMcpClient:
    @pytest.fixture
    def hopsworks_client(self, mocker):
        hw = Mock()
        hw._send_request.return_value = {"token": "jwt-1", "expiresInSeconds": 600}
        mocker.patch(
            "hopsworks_common.core.superset_mcp_api.client._get_instance",
            return_value=hw,
        )
        return hw

    @pytest.fixture
    def mcp(self, hopsworks_client):
        return _SupersetMcpClient(119, "consul")

    def _serve(self, mcp, *responses):
        mcp._http = Mock()
        mcp._http.post.side_effect = list(responses)
        return mcp._http.post

    def test_calls_the_tool_as_the_project_user_and_returns_its_payload(
        self, mcp, hopsworks_client
    ):
        post = self._serve(
            mcp,
            _response({"jsonrpc": "2.0", "id": 1, "result": {}}),
            _response({}, status=202),
            _response(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": _tool_result({"chart": {"id": 7}}),
                },
                sse=True,
            ),
        )

        result = mcp._call("generate_chart", {"request": {"dataset_id": 3}})

        assert result == {"chart": {"id": 7}}
        hopsworks_client._send_request.assert_called_once_with(
            "GET",
            ["project", 119, "superset", "mcp", "token"],
            timeout=superset_mcp_api._TIMEOUT_SECONDS,
        )
        url = post.call_args_list[0].args[0]
        assert url == "http://app.superset-mcp.service.consul:5008/mcp"
        methods = [c.kwargs["json"]["method"] for c in post.call_args_list]
        assert methods == ["initialize", "notifications/initialized", "tools/call"]
        call = post.call_args_list[2].kwargs
        assert call["headers"]["Authorization"] == "Bearer jwt-1"
        assert call["json"]["params"] == {
            "name": "generate_chart",
            "arguments": {"request": {"dataset_id": 3}},
        }

    def test_a_stateful_server_gets_its_session_id_back(self, mcp, hopsworks_client):
        post = self._serve(
            mcp,
            _response(
                {"jsonrpc": "2.0", "id": 1, "result": {}},
                headers={"Mcp-Session-Id": "s-9"},
            ),
            _response({}, status=202),
            _response({"jsonrpc": "2.0", "id": 2, "result": _tool_result({})}),
        )

        mcp._call("get_chart_type_schema", {"chart_type": "xy"})

        assert "Mcp-Session-Id" not in post.call_args_list[0].kwargs["headers"]
        assert post.call_args_list[2].kwargs["headers"]["Mcp-Session-Id"] == "s-9"

    def test_the_token_is_reused_until_it_nears_expiry(self, mcp, hopsworks_client):
        # Request ids run across calls: initialize 1, tool 2, then initialize 3, tool 4.
        self._serve(
            mcp,
            _response({"jsonrpc": "2.0", "id": 1, "result": {}}),
            _response({}, status=202),
            _response({"jsonrpc": "2.0", "id": 2, "result": _tool_result({})}),
            _response({"jsonrpc": "2.0", "id": 3, "result": {}}),
            _response({}, status=202),
            _response({"jsonrpc": "2.0", "id": 4, "result": _tool_result({})}),
        )

        mcp._call("get_chart_type_schema", {"chart_type": "xy"})
        mcp._call("get_chart_type_schema", {"chart_type": "pie"})

        assert hopsworks_client._send_request.call_count == 1

        mcp._token_deadline = 0
        self._serve(
            mcp,
            _response({"jsonrpc": "2.0", "id": 5, "result": {}}),
            _response({}, status=202),
            _response({"jsonrpc": "2.0", "id": 6, "result": _tool_result({})}),
        )
        mcp._call("get_chart_type_schema", {"chart_type": "xy"})

        assert hopsworks_client._send_request.call_count == 2

    def test_a_tool_error_raises_with_supersets_message(self, mcp, hopsworks_client):
        self._serve(
            mcp,
            _response({"jsonrpc": "2.0", "id": 1, "result": {}}),
            _response({}, status=202),
            _response(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": _tool_result(
                        {"success": False, "error": {"message": "column amt not found"}}
                    ),
                }
            ),
        )

        with pytest.raises(SupersetException, match="column amt not found"):
            mcp._call("generate_chart", {"request": {}})

    def test_an_mcp_error_result_raises(self, mcp, hopsworks_client):
        self._serve(
            mcp,
            _response({"jsonrpc": "2.0", "id": 1, "result": {}}),
            _response({}, status=202),
            _response(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "content": [{"type": "text", "text": "permission denied"}],
                        "isError": True,
                    },
                }
            ),
        )

        with pytest.raises(SupersetException, match="permission denied"):
            mcp._call("update_dashboard", {"request": {"identifier": 1}})

    def test_a_refused_token_raises_and_is_dropped(self, mcp, hopsworks_client):
        self._serve(mcp, _response({}, status=401))

        with pytest.raises(SupersetException, match="refused the Hopsworks token"):
            mcp._call("generate_chart", {"request": {}})
        assert mcp._token is None

    def test_an_absent_service_says_so(self, mcp, hopsworks_client):
        mcp._http = Mock()
        mcp._http.post.side_effect = requests.ConnectionError(
            "Name or service not known"
        )

        with pytest.raises(SupersetException, match="not reachable"):
            mcp._call("generate_chart", {"request": {}})

    def test_a_slow_service_raises_a_superset_exception(self, mcp, hopsworks_client):
        mcp._http = Mock()
        mcp._http.post.side_effect = requests.ReadTimeout("read timed out")

        with pytest.raises(SupersetException, match="did not answer within"):
            mcp._call("generate_chart", {"request": {}})


class TestSupersetApiMcpMethods:
    @pytest.fixture
    def api(self, mocker):
        variable_api = Mock()
        variable_api._get_service_discovery_domain.return_value = "consul"
        mocker.patch(
            "hopsworks_common.core.superset_api.VariableApi",
            return_value=variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.superset_api.client._is_external",
            return_value=False,
        )
        project = Mock()
        project.id = 119
        api = SupersetApi(project=project)
        api._mcp_client = Mock()
        return api

    def test_generate_chart_saves_by_default(self, api):
        api.generate_chart(3, {"chart_type": "xy", "y": [{"name": "n"}]}, name="N")

        api._mcp_client._call.assert_called_once_with(
            "generate_chart",
            {
                "request": {
                    "dataset_id": 3,
                    "config": {"chart_type": "xy", "y": [{"name": "n"}]},
                    "save_chart": True,
                    "chart_name": "N",
                }
            },
        )

    def test_chart_type_schema_asks_for_examples(self, api):
        api.chart_type_schema("pie")

        api._mcp_client._call.assert_called_once_with(
            "get_chart_type_schema", {"chart_type": "pie", "include_examples": True}
        )

    def test_patch_dashboard_sends_only_the_given_fields(self, api):
        api.patch_dashboard(7, dashboard_title="T", refresh_frequency=300)

        api._mcp_client._call.assert_called_once_with(
            "update_dashboard",
            {
                "request": {
                    "identifier": 7,
                    "dashboard_title": "T",
                    "refresh_frequency": 300,
                }
            },
        )

    def test_outside_the_cluster_it_refuses_before_calling(self, api, mocker):
        mocker.patch(
            "hopsworks_common.core.superset_api.client._is_external",
            return_value=True,
        )
        api._mcp_client = None

        with pytest.raises(RuntimeError, match="only available from within"):
            api.generate_chart(3, {"chart_type": "xy"})
