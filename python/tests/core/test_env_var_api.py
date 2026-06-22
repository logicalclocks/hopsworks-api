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


def _single_response(
    name: str,
    value: str,
    visibility: str = "PRIVATE",
    project_id_scope: int | None = None,
) -> dict:
    item = {"name": name, "value": value, "visibility": visibility}
    if project_id_scope is not None:
        item["projectIdScope"] = project_id_scope
    return _list_response([item])


def _secret_response(
    name: str,
    secret_name: str,
    value: str | None = None,
    visibility: str = "PRIVATE",
    project_id_scope: int | None = None,
) -> dict:
    item = {
        "name": name,
        "secretName": secret_name,
        "secretBacked": True,
        "visibility": visibility,
    }
    if project_id_scope is not None:
        item["projectIdScope"] = project_id_scope
    if value is not None:
        item["value"] = value
    return _list_response([item])


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
        "hopsworks_common.core.env_var_api.client._get_instance",
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
                    {
                        "name": "OPENAI_API_KEY",
                        "value": "sk-1",
                        "visibility": "PRIVATE",
                    },
                    {
                        "name": "HF_TOKEN",
                        "value": "hf-1",
                        "visibility": "PROJECT",
                        "projectIdScope": 11,
                    },
                ]
            ),
        )
        result = api.get_env_vars()
        assert len(result) == 2
        assert result[0].name == "OPENAI_API_KEY"
        assert result[0].value == "sk-1"
        assert result[0].visibility == "PRIVATE"
        assert result[1].name == "HF_TOKEN"
        assert result[1].visibility == "PROJECT"
        assert result[1].project_id_scope == 11

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
        assert result.visibility == "PRIVATE"

    def test_get_env_var_returns_secret_backed_metadata(self, mocker):
        api = EnvVarsApi()
        _patch_client(
            mocker,
            _secret_response(
                "OPENAI_API_KEY",
                "my_secret",
                visibility="PROJECT",
                project_id_scope=11,
            ),
        )
        result = api.get_env_var("OPENAI_API_KEY")
        assert isinstance(result, EnvVar)
        assert result.name == "OPENAI_API_KEY"
        assert result.value is None
        assert result.secret_name == "my_secret"
        assert result.secret_backed is True
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11

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
        c = _patch_client(
            mocker,
            _single_response(
                "OPENAI_API_KEY", "sk-1", visibility="PROJECT", project_id_scope=11
            ),
        )
        result = api.create_env_var(
            "OPENAI_API_KEY",
            "sk-1",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.name == "OPENAI_API_KEY"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        c._send_request.assert_called_once()
        method, path = c._send_request.call_args.args[:2]
        assert method == "POST"
        assert path == ["users", "envvars"]
        sent = json.loads(c._send_request.call_args.kwargs["data"])
        assert sent == {
            "name": "OPENAI_API_KEY",
            "value": "sk-1",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_create_env_var_from_secret_posts_secret_name(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(
            mocker,
            _secret_response(
                "OPENAI_API_KEY",
                "my_secret",
                visibility="PROJECT",
                project_id_scope=11,
            ),
        )
        result = api.create_env_var(
            "OPENAI_API_KEY",
            secret_name="my_secret",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.name == "OPENAI_API_KEY"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        method, path = c._send_request.call_args.args[:2]
        assert method == "POST"
        assert path == ["users", "envvars"]
        sent = json.loads(c._send_request.call_args.kwargs["data"])
        assert sent == {
            "name": "OPENAI_API_KEY",
            "secretName": "my_secret",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_update_env_var_puts(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(
            mocker,
            _single_response(
                "OPENAI_API_KEY", "sk-2", visibility="PROJECT", project_id_scope=11
            ),
        )
        result = api.update_env_var(
            "OPENAI_API_KEY",
            "sk-2",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.value == "sk-2"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        method, path = c._send_request.call_args.args[:2]
        assert method == "PUT"
        assert path == ["users", "envvars", "OPENAI_API_KEY"]
        sent = json.loads(c._send_request.call_args.kwargs["data"])
        assert sent == {
            "name": "OPENAI_API_KEY",
            "value": "sk-2",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_update_env_var_from_secret_puts_secret_name(self, mocker):
        api = EnvVarsApi()
        c = _patch_client(
            mocker,
            _secret_response(
                "OPENAI_API_KEY",
                "my_secret",
                visibility="PROJECT",
                project_id_scope=11,
            ),
        )
        result = api.update_env_var(
            "OPENAI_API_KEY",
            secret_name="my_secret",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.secret_name == "my_secret"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        method, path = c._send_request.call_args.args[:2]
        assert method == "PUT"
        assert path == ["users", "envvars", "OPENAI_API_KEY"]
        sent = json.loads(c._send_request.call_args.kwargs["data"])
        assert sent == {
            "name": "OPENAI_API_KEY",
            "secretName": "my_secret",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_set_env_var_creates_when_absent(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _make_rest_api_error(EnvVar.NOT_FOUND_ERROR_CODE),  # get_env_var
            _single_response(
                "NEW_VAR", "v", visibility="PROJECT", project_id_scope=11
            ),  # POST
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client._get_instance",
            return_value=client,
        )
        result = api.set_env_var(
            "NEW_VAR",
            "v",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.name == "NEW_VAR"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        assert client._send_request.call_args_list[1].args[0] == "POST"
        sent = json.loads(client._send_request.call_args_list[1].kwargs["data"])
        assert sent == {
            "name": "NEW_VAR",
            "value": "v",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_set_env_var_updates_when_present(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _single_response(
                "EXISTING", "old", visibility="PRIVATE"
            ),  # get_env_var returns hit
            _single_response(
                "EXISTING", "new", visibility="PROJECT", project_id_scope=11
            ),  # PUT
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client._get_instance",
            return_value=client,
        )
        result = api.set_env_var(
            "EXISTING",
            "new",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.value == "new"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        assert client._send_request.call_args_list[1].args[0] == "PUT"
        sent = json.loads(client._send_request.call_args_list[1].kwargs["data"])
        assert sent == {
            "name": "EXISTING",
            "value": "new",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_set_env_var_from_secret_creates_when_absent(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _make_rest_api_error(EnvVar.NOT_FOUND_ERROR_CODE),  # get_env_var
            _secret_response(
                "NEW_VAR", "my_secret", visibility="PROJECT", project_id_scope=11
            ),  # POST
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client._get_instance",
            return_value=client,
        )
        result = api.set_env_var(
            "NEW_VAR",
            secret_name="my_secret",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.name == "NEW_VAR"
        assert result.secret_name == "my_secret"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        assert client._send_request.call_args_list[1].args[0] == "POST"
        sent = json.loads(client._send_request.call_args_list[1].kwargs["data"])
        assert sent == {
            "name": "NEW_VAR",
            "secretName": "my_secret",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

    def test_set_env_var_from_secret_updates_when_present(self, mocker):
        api = EnvVarsApi()
        client = MagicMock()
        client._send_request.side_effect = [
            _secret_response(
                "EXISTING",
                "old_secret",
                visibility="PRIVATE",
            ),  # get_env_var returns hit
            _secret_response(
                "EXISTING", "new_secret", visibility="PROJECT", project_id_scope=11
            ),  # PUT
        ]
        mocker.patch(
            "hopsworks_common.core.env_var_api.client._get_instance",
            return_value=client,
        )
        result = api.set_env_var(
            "EXISTING",
            secret_name="new_secret",
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert result.secret_name == "new_secret"
        assert result.visibility == "PROJECT"
        assert result.project_id_scope == 11
        assert client._send_request.call_args_list[1].args[0] == "PUT"
        sent = json.loads(client._send_request.call_args_list[1].kwargs["data"])
        assert sent == {
            "name": "EXISTING",
            "secretName": "new_secret",
            "visibility": "PROJECT",
            "projectIdScope": 11,
        }

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
            {
                "items": [
                    {
                        "name": "X",
                        "value": "y",
                        "visibility": "PROJECT",
                        "projectIdScope": 11,
                        "addedOn": None,
                        "updatedOn": None,
                    }
                ]
            }
        )
        assert len(result) == 1
        assert result[0].name == "X"
        assert result[0].value == "y"
        assert result[0].visibility == "PROJECT"
        assert result[0].project_id_scope == 11

    def test_from_response_json_secret_backed(self):
        result = EnvVar.from_response_json(
            {
                "items": [
                    {
                        "name": "OPENAI_API_KEY",
                        "secretName": "my_secret",
                        "secretBacked": True,
                        "visibility": "PROJECT",
                        "projectIdScope": 11,
                        "addedOn": None,
                        "updatedOn": None,
                    }
                ]
            }
        )
        assert len(result) == 1
        assert result[0].name == "OPENAI_API_KEY"
        assert result[0].value is None
        assert result[0].secret_name == "my_secret"
        assert result[0].secret_backed is True
        assert result[0].visibility == "PROJECT"
        assert result[0].project_id_scope == 11

    def test_to_dict_secret_backed_includes_metadata(self):
        env_var = EnvVar(
            name="X",
            value="y",
            secret_name="my_secret",
            secret_backed=True,
            visibility="PROJECT",
            project_id_scope=11,
        )
        assert env_var.to_dict() == {
            "name": "X",
            "value": "***",
            "visibility": "PROJECT",
            "project_id_scope": 11,
            "added_on": None,
            "updated_on": None,
            "secret_name": "my_secret",
            "secret_backed": True,
        }

    def test_repr(self):
        env_var = EnvVar(name="X", value="y")
        assert repr(env_var) == "EnvVar('X')"
