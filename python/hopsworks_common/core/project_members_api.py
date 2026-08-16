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
from typing import Literal, get_args

from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common import project_member as project_member_mod


_ROLE_ARG = Literal[
    "Data owner",
    "Data scientist",
    "Observer",
    "Feature store restricted",
]
_ROLES = get_args(_ROLE_ARG)


def _normalize_role(role: str) -> str:
    """Match a role case-insensitively and return it in the casing the backend expects.

    The roles read as prose rather than as identifiers, so `data owner` is an easy thing to write;
    accepting it costs nothing and the backend still receives the canonical `Data owner`.
    """
    if isinstance(role, str):
        for canonical in _ROLES:
            if role.casefold() == canonical.casefold():
                return canonical
    raise ValueError(f"Role must be one of the following: {_ROLES}.")


@public("hopsworks.core.project_members_api.ProjectMembersApi")
class ProjectMembersApi:
    """API for managing the members of a Hopsworks project.

    Use [`Project.get_members_api`][hopsworks_common.project.Project.get_members_api]
    to get an instance of this class.
    """

    def __init__(self, project_id: int | None = None) -> None:
        """Members of one project.

        Parameters:
            project_id: Project whose members this reads and writes. Defaults to the
                connection's project. A `Project` object for another authorized project
                constructs one for that project, since every path here is built as
                `project/<id>/projectMembers` and would otherwise add and remove members
                of the login project instead.
        """
        self._project_id = project_id

    def _pid(self) -> int:
        """The project to address: the one this instance was built for, or the connection's."""
        return (
            self._project_id
            if self._project_id is not None
            else client._get_instance()._project_id
        )

    @public
    def get_members(self) -> list[project_member_mod.ProjectMember]:
        """Get all members of the project.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            for member in project.get_members_api().get_members():
                print(member.email, member.role)
            ```

        Returns:
            List of `ProjectMember` objects, one per user who has access to the project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["project", self._pid(), "projectMembers"]
        return project_member_mod.ProjectMember.from_response_json(
            _client._send_request("GET", path_params)
        )

    def _find_member(self, email: str) -> project_member_mod.ProjectMember | None:
        for member in self.get_members():
            if member.email and member.email.lower() == email.lower():
                return member
        return None

    @public
    def add_members(
        self, emails_and_roles: dict[str, _ROLE_ARG]
    ) -> list[project_member_mod.ProjectMember]:
        """Add one or more users to the project in a single call.

        Parameters:
            emails_and_roles:
                Mapping of a member's email to the project role to grant them,
                one of `Data owner`, `Data scientist`, `Observer`,
                `Feature store restricted`.
                Roles are matched regardless of case.

        Returns:
            List of the newly added `ProjectMember` objects.

        Raises:
            ValueError: If any role is not a settable project role.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner.
        """
        emails_and_roles = {
            email: _normalize_role(role) for email, role in emails_and_roles.items()
        }

        _client = client._get_instance()
        path_params = ["project", self._pid(), "projectMembers"]
        headers = {"content-type": "application/json"}
        body = {
            "projectTeam": [
                {
                    "projectTeamPK": {
                        "projectId": self._pid(),
                        "teamMember": email,
                    },
                    "teamRole": role,
                }
                for email, role in emails_and_roles.items()
            ]
        }
        _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(body)
        )
        added = {email.lower() for email in emails_and_roles}
        return [m for m in self.get_members() if m.email and m.email.lower() in added]

    @public
    def add_member(
        self, email: str, role: _ROLE_ARG
    ) -> project_member_mod.ProjectMember:
        """Add a single user to the project.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.get_members_api().add_member("alice@example.com", "Data scientist")
            ```

        Parameters:
            email: Email address of the user to add.
            role: The project role to grant, one of `Data owner`, `Data scientist`,
                `Observer`, `Feature store restricted`.
                Matched regardless of case.

        Returns:
            The newly added `ProjectMember`.

        Raises:
            ValueError: If `role` is not a settable project role.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner.
            RuntimeError: If the add succeeded but `email` could not be found
                in the project's member list immediately afterwards.
        """
        added = self.add_members({email: role})
        if not added:
            raise RuntimeError(
                f"'{email}' was added to the project but could not be found in "
                "the member list immediately afterwards; call get_members() to check."
            )
        return added[0]

    @public
    def update_role(
        self, email: str, role: _ROLE_ARG
    ) -> project_member_mod.ProjectMember | None:
        """Change a project member's role by email.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.get_members_api().update_role("alice@example.com", "Observer")
            ```

        Parameters:
            email: Email address of the member to update.
            role: The new project role, one of `Data owner`, `Data scientist`,
                `Observer`, `Feature store restricted`.
                Matched regardless of case.

        Returns:
            The updated `ProjectMember`, or `None` if the member could not be
            found immediately after the update.

        Raises:
            ValueError: If `role` is not a settable project role.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner or `email` is the project owner.
        """
        role = _normalize_role(role)
        _client = client._get_instance()
        path_params = ["project", self._pid(), "projectMembers", email]
        _client._send_request("POST", path_params, data={"role": role})
        return self._find_member(email)

    @public
    def remove_member(self, email: str, delete_home_dir: bool = False) -> None:
        """Remove a member from the project by email.

        Danger: Deletes the member's project files when `delete_home_dir=True`
            All files under this member's home directory in the project are
            permanently deleted and cannot be recovered.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.get_members_api().remove_member("alice@example.com")
            ```

        Parameters:
            email: Email address of the member to remove.
            delete_home_dir: Whether to also delete the member's home directory in the project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if a data scientist tries to remove someone other than themselves.
        """
        _client = client._get_instance()
        path_params = ["project", self._pid(), "projectMembers", email]
        query_params = {"deleteHomeDir": "true" if delete_home_dir else "false"}
        _client._send_request("DELETE", path_params, query_params=query_params)
