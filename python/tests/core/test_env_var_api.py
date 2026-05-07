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
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core.env_var_api import EnvVarsApi
from hopsworks_common.env_var import EnvVar


def _list_response(items: list[dict]) -> dict:
    return {"items": items, "type": "envVarDTO"}


def _single_response(name: str, value: str) -> dict:
    return _list_response([{"name": name, "value": value}])


def _make_rest_api_error(error_code: int, status_code: int = 404) -> RestAPIError:
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.reason = "Not Found"
    mock_response.content = b"{}"
    mock_response.json.return_value = {
        "errorCode": error_code,
        "errorMsg": "not found",
        "usrMsg": "not found",
    }
    return RestAPIError("http://localhost/users/envvars/X", mock_response)


def _patch_client(mocker, send_request_return):
    client_instance = MagicMock()
    if isinstance(send_request_return, Exception):
        client_instance._send_request.side_effect = send_request_return
    else:
        client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.env_var_api.client.get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestEnvVarsApi:
    def test_get_env_vars_returns_list(self, mocker):
        api = EnvVarsApi()
        _patch_client(
            mocker,
            _list_response(
                [
                    {"name": "OPENAI_API_KEY", "value": "sk-1"},
                    {"name": "HF_TOKEN", "value": "hf-1"},
                ]
            ),
        )
        result = api.get_env_vars()
        assert len(result) == 2
        assert result[0].name == "OPENAI_API_KEY"
        assert result[0].value == "sk-1"
        assert result[1].name == "HF_TOKEN"

    def test_get_env_vars_empty(self, mocker):
        api = EnvVarsApi()
        _patch_client(mocker, _list_response([]))
        assert api.get_env_vars() == []

    def test_get_env_var_returns_single(self, mocker):
        api = EnvVarsApi()
        _patch_client(mocker, _single_response("OPENAI_API_KEY", "sk-1"))
        result = api.get_env_var("OPENAI_API_KEY")
        assert isinstance(result, EnvVar)
        assert result.name == "OPENAI_API_KEY"
        assert result.value == "sk-1"

    def test_get_env_var_not_found_returns_none(self, mocker):
        api = EnvVarsApi()
        _patch_client(mocker, _make_rest_api_error(EnvVar.NOT_FOUND_ERROR_CODE))
        assert api.get_env_var("MISSING") is None

    def test_get_returns_value(self, mocker):
        api = EnvVarsApi()
        _patch_client(mocker, _single_response("OPENAI_API_KEY", "sk-1"))
        assert api.get("OPENAI_API_KEY") == "sk-1"

    def test_get_returns_none_when_missing(self, mocker):
        api = EnvVarsApi()
        _patch_client(mocker, _make_rest_api_error(EnvVar.NOT_FOUND_ERROR_CODE))
        assert api.get("MISSING") is None

    def test_create_env_var_posts(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(mocker, _single_response("OPENAI_API_KEY", "sk-1"))
        result = api.create_env_var("OPENAI_API_KEY", "sk-1")
        assert result.name == "OPENAI_API_KEY"
        c._send_request.assert_called_once()
        method, path = c._send_request.call_args.args[:2]
        assert method == "POST"
        assert path == ["users", "envvars"]
        sent = json.loads(c._send_request.call_args.kwargs["data"])
        assert sent == {"name": "OPENAI_API_KEY", "value": "sk-1"}

    def test_update_env_var_puts(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(mocker, _single_response("OPENAI_API_KEY", "sk-2"))
        result = api.update_env_var("OPENAI_API_KEY", "sk-2")
        assert result.value == "sk-2"
        method, path = c._send_request.call_args.args[:2]
        assert method == "PUT"
        assert path == ["users", "envvars", "OPENAI_API_KEY"]

    def test_set_env_var_creates_when_absent(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _make_rest_api_error(EnvVar.NOT_FOUND_ERROR_CODE),  # get_env_var
            _single_response("NEW_VAR", "v"),  # POST
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client.get_instance",
            return_value=client,
        )
        result = api.set_env_var("NEW_VAR", "v")
        assert result.name == "NEW_VAR"
        assert client._send_request.call_args_list[1].args[0] == "POST"

    def test_set_env_var_updates_when_present(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _single_response("EXISTING", "old"),  # get_env_var returns hit
            _single_response("EXISTING", "new"),  # PUT
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client.get_instance",
            return_value=client,
        )
        result = api.set_env_var("EXISTING", "new")
        assert result.value == "new"
        assert client._send_request.call_args_list[1].args[0] == "PUT"

    def test_delete_env_var_calls_delete(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(mocker, None)
        api.delete_env_var("OPENAI_API_KEY")
        method, path = c._send_request.call_args.args[:2]
        assert method == "DELETE"
        assert path == ["users", "envvars", "OPENAI_API_KEY"]

    def test_delete_alias(self, mocker):
        # The plain `delete(name)` alias must still work — it's there for
        # parity with SecretsApi.delete and existing user code.
        api = EnvVarsApi()
        c = _patch_client(mocker, None)
        api.delete("OPENAI_API_KEY")
        method, path = c._send_request.call_args.args[:2]
        assert method == "DELETE"
        assert path == ["users", "envvars", "OPENAI_API_KEY"]

    def test_delete_all_calls_delete(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(mocker, None)
        api.delete_all()
        method, path = c._send_request.call_args.args[:2]
        assert method == "DELETE"
        assert path == ["users", "envvars"]

    def test_other_error_propagates(self, mocker):
        api = EnvVarsApi()
        # Not a NOT_FOUND code — should propagate as RestAPIError.
        _patch_client(mocker, _make_rest_api_error(160068, status_code=400))
        with pytest.raises(RestAPIError):
            api.get_env_var("ANY")


class TestEnvVar:
    def test_from_response_json_empty(self):
        assert EnvVar.from_response_json({"items": []}) == []

    def test_from_response_json_no_items_key(self):
        assert EnvVar.from_response_json({}) == []

    def test_from_response_json_single(self):
        result = EnvVar.from_response_json(
            {"items": [{"name": "X", "value": "y", "addedOn": None, "updatedOn": None}]}
        )
        assert len(result) == 1
        assert result[0].name == "X"
        assert result[0].value == "y"

    def test_repr(self):
        env_var = EnvVar(name="X", value="y")
        assert repr(env_var) == "EnvVar('X')"
