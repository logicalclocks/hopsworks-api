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

from hopsworks_common.user import AdminUser


class TestAdminUser:
    def test_from_response_json_renames_and_extracts_roles(self):
        json_dict = {
            "id": 1,
            "email": "alice@example.com",
            "firstname": "Alice",
            "lastname": "Smith",
            "username": "alice",
            "status": 2,
            "role": [{"groupName": "HOPS_ADMIN"}, {"groupName": "HOPS_USER"}],
            "unknownField": "ignored",
        }

        admin_user = AdminUser.from_response_json(json_dict)

        assert admin_user.id == 1
        assert admin_user.first_name == "Alice"
        assert admin_user.last_name == "Smith"
        assert admin_user.status == 2
        assert admin_user.roles == ["HOPS_ADMIN", "HOPS_USER"]
        assert not hasattr(admin_user, "unknown_field")

    def test_from_response_json_handles_missing_role(self):
        admin_user = AdminUser.from_response_json({"id": 1, "email": "a@b.com"})

        assert admin_user.roles == []

    def test_from_response_json_returns_none_for_empty_input(self):
        assert AdminUser.from_response_json(None) is None
        assert AdminUser.from_response_json({}) is None

    def test_from_response_json_list_parses_items_envelope(self):
        json_dict = {
            "items": [
                {"id": 1, "email": "a@b.com", "role": []},
                {"id": 2, "email": "c@d.com", "role": []},
            ],
            "count": 2,
        }

        admin_users = AdminUser.from_response_json_list(json_dict)

        assert [u.id for u in admin_users] == [1, 2]

    def test_from_response_json_list_handles_bare_list(self):
        json_dict = [{"id": 1, "email": "a@b.com", "role": []}]

        admin_users = AdminUser.from_response_json_list(json_dict)

        assert len(admin_users) == 1

    def test_from_response_json_list_handles_empty_input(self):
        assert AdminUser.from_response_json_list(None) == []
        assert AdminUser.from_response_json_list({}) == []
