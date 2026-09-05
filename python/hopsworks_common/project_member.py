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
from __future__ import annotations

import json

import humps
from hopsworks_apigen import public
from hopsworks_common import user as user_mod
from hopsworks_common import util
from hopsworks_common.core import project_members_api


@public("hopsworks.project_member.ProjectMember")
class ProjectMember:
    """Represents a user's membership and role in a Hopsworks project.

    Use [`Project.get_members`][hopsworks_common.project.Project.get_members]
    to list the members of a project, or
    [`ProjectMembersApi`][hopsworks_common.core.project_members_api.ProjectMembersApi]
    for the full set of member-management operations.
    """

    def __init__(
        self,
        project=None,
        user=None,
        team_role=None,
        timestamp=None,
        **kwargs,
    ):
        self._project_id = project.get("id") if isinstance(project, dict) else None
        self._user = user_mod.User.from_response_json(user)
        self._role = team_role
        self._timestamp = timestamp
        self._project_members_api = project_members_api.ProjectMembersApi()

    @classmethod
    def from_response_json(cls, json_dict) -> list[ProjectMember]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict) and "items" in json_decamelized:
            json_decamelized = json_decamelized["items"]
        if isinstance(json_decamelized, dict):
            json_decamelized = [json_decamelized]
        return [cls(**member) for member in json_decamelized]

    @public
    @property
    def email(self) -> str | None:
        """Email address of the project member."""
        return self._user.email if self._user else None

    @public
    @property
    def first_name(self) -> str | None:
        """First name of the project member."""
        return self._user.first_name if self._user else None

    @public
    @property
    def last_name(self) -> str | None:
        """Last name of the project member."""
        return self._user.last_name if self._user else None

    @public
    @property
    def username(self) -> str | None:
        """Hopsworks username of the project member."""
        return self._user.username if self._user else None

    @public
    @property
    def user(self) -> user_mod.User | None:
        """The [`User`][hopsworks_common.user.User] backing this membership."""
        return self._user

    @public
    @property
    def role(self) -> str | None:
        """Project role of the member, for example `"Data owner"` or `"Data scientist"`."""
        return self._role

    @public
    @property
    def project_id(self) -> int | None:
        """Id of the project this membership belongs to."""
        return self._project_id

    @public
    def update_role(self, role: str) -> ProjectMember:
        """Change this member's role in the project.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            member = next(m for m in project.get_members() if m.email == "alice@example.com")
            member.update_role("Observer")
            ```

        Parameters:
            role: The new project role, one of `Data owner`, `Data scientist`,
                `Observer`, `Feature store restricted`.

        Returns:
            This `ProjectMember`, updated with the new role.

        Raises:
            ValueError: If `role` is not a settable project role.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner.
        """
        self._project_members_api.update_role(self.email, role)
        self._role = role
        return self

    @public
    def remove(self, delete_home_dir: bool = False) -> None:
        """Remove this member from the project.

        Danger: Deletes the member's project files when `delete_home_dir=True`
            All files under this member's home directory in the project are
            permanently deleted and cannot be recovered.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            member = next(m for m in project.get_members() if m.email == "alice@example.com")
            member.remove()
            ```

        Parameters:
            delete_home_dir: Whether to also delete the member's home directory in the project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if a data scientist tries to remove someone other than themselves.
        """
        self._project_members_api.remove_member(
            self.email, delete_home_dir=delete_home_dir
        )

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "user": self._user.to_dict() if self._user else None,
            "teamRole": self._role,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"ProjectMember({self.email!r}, {self._role!r})"
