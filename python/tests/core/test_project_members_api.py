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
from hopsworks_common.core.project_members_api import ProjectMembersApi
from hopsworks_common.project_member import ProjectMember


def _member(email: str, role: str, first_name: str = "Alice") -> dict:
    return {
        "project": {"id": 119},
        "user": {"email": email, "firstname": first_name, "lastname": "Smith"},
        "teamRole": role,
    }


def _patch_client(mocker, send_request_return):
    client_instance = MagicMock()
    client_instance._send_request.return_value = send_request_return
    client_instance._project_id = 119
    mocker.patch(
        "hopsworks_common.core.project_members_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestProjectMembersApi:
    def test_get_members_parses_bare_list(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(
            mocker, [_member("alice@example.com", "Data owner")]
        )

        members = api.get_members()

        assert len(members) == 1
        assert members[0].email == "alice@example.com"
        assert members[0].role == "Data owner"
        assert members[0].first_name == "Alice"
        get_call = client_instance._send_request.call_args_list[0]
        assert get_call.args[0] == "GET"
        assert get_call.args[1] == ["project", 119, "projectMembers"]

    def test_add_member_posts_body_and_refetches(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            None,
            [_member("bob@example.com", "Data scientist")],
        ]

        member = api.add_member("bob@example.com", "Data scientist")

        assert member.email == "bob@example.com"
        assert member.role == "Data scientist"
        post_call = client_instance._send_request.call_args_list[0]
        assert post_call.args[0] == "POST"
        assert post_call.args[1] == ["project", 119, "projectMembers"]
        body = json.loads(post_call.kwargs["data"])
        assert body == {
            "projectTeam": [
                {
                    "projectTeamPK": {
                        "projectId": 119,
                        "teamMember": "bob@example.com",
                    },
                    "teamRole": "Data scientist",
                }
            ]
        }

    def test_add_member_invalid_role_raises_without_request(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        with pytest.raises(ValueError, match="Role must be one of the following"):
            api.add_member("bob@example.com", "Superuser")

        client_instance._send_request.assert_not_called()

    def test_update_role_posts_form_data_and_refetches(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            None,
            [_member("bob@example.com", "Observer")],
        ]

        member = api.update_role("bob@example.com", "Observer")

        assert member.role == "Observer"
        post_call = client_instance._send_request.call_args_list[0]
        assert post_call.args[0] == "POST"
        assert post_call.args[1] == [
            "project",
            119,
            "projectMembers",
            "bob@example.com",
        ]
        assert post_call.kwargs["data"] == {"role": "Observer"}

    def test_update_role_invalid_role_raises_without_request(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        with pytest.raises(ValueError, match="Role must be one of the following"):
            api.update_role("bob@example.com", "Under removal")

        client_instance._send_request.assert_not_called()

    def test_remove_member_sends_delete_with_query_params(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        api.remove_member("bob@example.com", delete_home_dir=True)

        delete_call = client_instance._send_request.call_args_list[0]
        assert delete_call.args[0] == "DELETE"
        assert delete_call.args[1] == [
            "project",
            119,
            "projectMembers",
            "bob@example.com",
        ]
        assert delete_call.kwargs["query_params"] == {"deleteHomeDir": "true"}

    def test_remove_member_defaults_delete_home_dir_false(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        api.remove_member("bob@example.com")

        delete_call = client_instance._send_request.call_args_list[0]
        assert delete_call.kwargs["query_params"] == {"deleteHomeDir": "false"}


class TestProjectMember:
    def test_update_role_delegates_to_api_and_updates_local_state(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data owner")
        )[0]
        member._project_members_api = MagicMock()

        result = member.update_role("Observer")

        member._project_members_api.update_role.assert_called_once_with(
            "bob@example.com", "Observer"
        )
        assert result is member
        assert member.role == "Observer"

    def test_remove_delegates_to_api(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data owner")
        )[0]
        member._project_members_api = MagicMock()

        member.remove(delete_home_dir=True)

        member._project_members_api.remove_member.assert_called_once_with(
            "bob@example.com", delete_home_dir=True
        )
