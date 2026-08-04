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
from hopsworks_common.core.users_api import UsersApi


def _profile(user_id: int = 1, email: str = "alice@example.com") -> dict:
    return {
        "id": user_id,
        "email": email,
        "firstname": "Alice",
        "lastname": "Smith",
        "username": "alice",
        "status": 2,
        "role": [{"groupName": "HOPS_USER"}],
    }


def _patch_client(mocker, send_request_return):
    client_instance = MagicMock()
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.users_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestUsersApi:
    def test_get_users_parses_items_envelope(self, mocker):
        api = UsersApi()
        _patch_client(mocker, {"items": [_profile()], "count": 1})

        users = api.get_users()

        assert len(users) == 1
        assert users[0].email == "alice@example.com"
        assert users[0].roles == ["HOPS_USER"]

    def test_get_users_handles_bare_list(self, mocker):
        api = UsersApi()
        _patch_client(mocker, [_profile()])

        users = api.get_users()

        assert len(users) == 1
        assert users[0].first_name == "Alice"

    def test_get_user_returns_admin_user(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        user = api.get_user(1)

        assert user.id == 1
        assert user.last_name == "Smith"
        get_call = client_instance._send_request.call_args_list[0]
        assert get_call.args[1] == ["admin", "users", 1]

    def test_get_user_returns_none_on_404(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)
        response = MagicMock()
        response.status_code = 404
        client_instance._send_request.side_effect = RestAPIError("url", response)

        assert api.get_user(999) is None

    def test_get_user_reraises_non_404_errors(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)
        response = MagicMock()
        response.status_code = 500
        client_instance._send_request.side_effect = RestAPIError("url", response)

        with pytest.raises(RestAPIError):
            api.get_user(999)

    def test_register_user_sends_expected_query_params(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.register_user("alice@example.com", "Alice", "Smith", role="HOPS_ADMIN")

        post_call = client_instance._send_request.call_args_list[0]
        assert post_call.args[0] == "POST"
        assert post_call.args[1] == ["admin", "users"]
        query_params = post_call.kwargs["query_params"]
        assert query_params["accountType"] == "M_ACCOUNT_TYPE"
        assert query_params["email"] == "alice@example.com"
        assert query_params["givenName"] == "Alice"
        assert query_params["surname"] == "Smith"
        assert query_params["role"] == "HOPS_ADMIN"
        assert "password" not in query_params
        assert "status" not in query_params

    def test_register_user_invalid_role_raises_without_request(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)

        with pytest.raises(ValueError, match="Role must be one of the following"):
            api.register_user("alice@example.com", "Alice", "Smith", role="OWNER")

        client_instance._send_request.assert_not_called()

    def test_register_user_invalid_status_raises_without_request(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)

        with pytest.raises(ValueError, match="Status must be one of the following"):
            api.register_user("alice@example.com", "Alice", "Smith", status="ACCEPTED")

        client_instance._send_request.assert_not_called()

    def test_set_role_sends_raw_string_body(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.set_role(1, "HOPS_ADMIN")

        put_call = client_instance._send_request.call_args_list[0]
        assert put_call.args[0] == "PUT"
        assert put_call.args[1] == ["admin", "users", 1, "role"]
        assert put_call.kwargs["data"] == "HOPS_ADMIN"

    def test_set_role_invalid_role_raises_without_request(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)

        with pytest.raises(ValueError, match="Role must be one of the following"):
            api.set_role(1, "OWNER")

        client_instance._send_request.assert_not_called()

    def test_update_user_sends_max_num_projects(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.update_user(1, 10)

        put_call = client_instance._send_request.call_args_list[0]
        assert put_call.args[1] == ["admin", "users", 1]
        assert json.loads(put_call.kwargs["data"]) == {"maxNumProjects": 10}

    def test_activate_user_sends_empty_json_body(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.activate_user(1)

        put_call = client_instance._send_request.call_args_list[0]
        assert put_call.args[1] == ["admin", "users", 1, "accepted"]
        assert json.loads(put_call.kwargs["data"]) == {}

    def test_reject_user_sends_no_body(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.reject_user(1)

        put_call = client_instance._send_request.call_args_list[0]
        assert put_call.args[1] == ["admin", "users", 1, "rejected"]
        assert "data" not in put_call.kwargs

    def test_resend_confirmation_email_sends_no_body(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, _profile())

        api.resend_confirmation_email(1)

        put_call = client_instance._send_request.call_args_list[0]
        assert put_call.args[1] == ["admin", "users", 1, "pending"]
        assert "data" not in put_call.kwargs

    def test_delete_user_sends_delete(self, mocker):
        api = UsersApi()
        client_instance = _patch_client(mocker, None)

        api.delete_user(42)

        delete_call = client_instance._send_request.call_args_list[0]
        assert delete_call.args[0] == "DELETE"
        assert delete_call.args[1] == ["admin", "users", 42]
