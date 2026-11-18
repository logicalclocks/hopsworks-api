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


def _member(email: str, role: str, first_name: str = "Alice", custom_role=None) -> dict:
    member = {
        "project": {"id": 119},
        "user": {"email": email, "firstname": first_name, "lastname": "Smith"},
        "teamRole": role,
    }
    if custom_role:
        member["customRole"] = {"id": 100, "name": custom_role}
    return member


def _roles(*names: str) -> dict:
    return {"items": [{"name": name} for name in names]}


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

    def test_add_member_raises_runtime_error_when_not_found_after_add(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [None, []]

        with pytest.raises(RuntimeError, match="could not be found"):
            api.add_member("bob@example.com", "Data scientist")

    def test_add_member_invalid_role_raises_before_any_change(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(
            mocker, _roles("Data owner", "Data scientist", "Team lead")
        )

        with pytest.raises(ValueError, match="Team lead"):
            api.add_member("bob@example.com", "Superuser")

        # Only the role list was read; nothing was posted.
        assert [c.args[0] for c in client_instance._send_request.call_args_list] == [
            "GET"
        ]
        assert client_instance._send_request.call_args.args[1] == [
            "project",
            119,
            "roles",
        ]

    def test_add_member_with_a_built_in_role_reads_no_role_list(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            None,
            [_member("bob@example.com", "Observer")],
        ]

        api.add_member("bob@example.com", "observer")

        assert [c.args[0] for c in client_instance._send_request.call_args_list] == [
            "POST",
            "GET",
        ]

    def test_add_member_with_a_custom_role_sends_its_name(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            _roles("Data owner", "Team lead"),
            None,
            [_member("bob@example.com", "Data owner", custom_role="Team lead")],
        ]

        member = api.add_member("bob@example.com", "team LEAD")

        body = json.loads(
            client_instance._send_request.call_args_list[1].kwargs["data"]
        )
        assert body["projectTeam"][0]["teamRole"] == "Team lead"
        assert member.role == "Data owner"
        assert member.custom_role == "Team lead"

    def test_add_members_reads_the_role_list_once(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            _roles("Team lead", "Reviewer"),
            None,
            [],
        ]

        api.add_members({"a@example.com": "Team lead", "b@example.com": "Reviewer"})

        assert [c.args[0] for c in client_instance._send_request.call_args_list] == [
            "GET",
            "POST",
            "GET",
        ]

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

    def test_update_role_invalid_role_raises_before_any_change(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, _roles("Data owner", "Observer"))

        with pytest.raises(ValueError, match="Role must be one of the following"):
            api.update_role("bob@example.com", "Under removal")

        assert [c.args[0] for c in client_instance._send_request.call_args_list] == [
            "GET"
        ]

    def test_update_role_to_a_custom_role_sends_its_name(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            _roles("Team lead"),
            None,
            [_member("bob@example.com", "Data owner", custom_role="Team lead")],
        ]

        member = api.update_role("bob@example.com", "team lead")

        assert client_instance._send_request.call_args_list[1].kwargs["data"] == {
            "role": "Team lead"
        }
        assert member.custom_role == "Team lead"

    def test_add_member_accepts_any_case_and_sends_canonical(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            None,
            [_member("bob@example.com", "Data owner")],
        ]

        api.add_member("bob@example.com", "data OWNER")

        # the backend is given the canonical casing regardless of what the caller typed
        body = json.loads(
            client_instance._send_request.call_args_list[0].kwargs["data"]
        )
        assert body["projectTeam"][0]["teamRole"] == "Data owner"

    def test_update_role_accepts_any_case_and_sends_canonical(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)
        client_instance._send_request.side_effect = [
            None,
            [_member("bob@example.com", "Feature store restricted")],
        ]

        api.update_role("bob@example.com", "feature store restricted")

        assert client_instance._send_request.call_args_list[0].kwargs["data"] == {
            "role": "Feature store restricted"
        }

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

    def test_remove_member_names_the_data_owner_that_takes_over_the_files(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        api.remove_member("bob@example.com", new_file_owner="carol@example.com")

        delete_call = client_instance._send_request.call_args_list[0]
        assert delete_call.kwargs["query_params"] == {
            "deleteHomeDir": "false",
            "newFileOwner": "carol@example.com",
        }

    def test_remove_member_omits_the_file_owner_when_none_is_chosen(self, mocker):
        # The backend picks the longest-serving data owner when the parameter is absent, so sending
        # it empty would be a different request than "no preference".
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        api.remove_member("bob@example.com")

        assert (
            "newFileOwner"
            not in client_instance._send_request.call_args_list[0].kwargs[
                "query_params"
            ]
        )

    def test_transfer_ownership_puts_to_the_new_owners_owner_path(self, mocker):
        api = ProjectMembersApi()
        client_instance = _patch_client(mocker, None)

        api.transfer_ownership("carol@example.com")

        client_instance._send_request.assert_called_once_with(
            "PUT", ["project", 119, "projectMembers", "carol@example.com", "owner"]
        )


class TestProjectMember:
    def test_update_role_delegates_to_api_and_updates_local_state(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data owner")
        )[0]
        member._project_members_api = MagicMock()
        member._project_members_api.update_role.return_value = (
            ProjectMember.from_response_json(
                _member("bob@example.com", "Data owner", custom_role="Team lead")
            )[0]
        )

        result = member.update_role("team lead")

        member._project_members_api.update_role.assert_called_once_with(
            "bob@example.com", "team lead"
        )
        assert result is member
        # The backend's answer, not the name the caller typed: the base role and the custom name.
        assert member.role == "Data owner"
        assert member.custom_role == "Team lead"

    def test_a_built_in_member_has_no_custom_role(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data scientist")
        )[0]

        assert member.role == "Data scientist"
        assert member.custom_role is None

    def test_remove_delegates_to_api(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data owner")
        )[0]
        member._project_members_api = MagicMock()

        member.remove(delete_home_dir=True)

        member._project_members_api.remove_member.assert_called_once_with(
            "bob@example.com", delete_home_dir=True, new_file_owner=None
        )

    def test_remove_passes_the_chosen_file_owner_through(self, mocker):
        member = ProjectMember.from_response_json(
            _member("bob@example.com", "Data owner")
        )[0]
        member._project_members_api = MagicMock()

        member.remove(new_file_owner="carol@example.com")

        member._project_members_api.remove_member.assert_called_once_with(
            "bob@example.com",
            delete_home_dir=False,
            new_file_owner="carol@example.com",
        )
