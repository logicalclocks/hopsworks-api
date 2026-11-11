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

from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common import project_member as project_member_mod


_ROLES = ("Data owner", "Data scientist", "Observer", "Feature store restricted")


def _match_role(role: str, names) -> str | None:
    if isinstance(role, str):
        for name in names:
            if role.casefold() == name.casefold():
                return name
    return None


def _project_role_names() -> list[str]:
    _client = client._get_instance()
    path_params = ["project", _client._project_id, "roles"]
    response = _client._send_request("GET", path_params) or {}
    return [item["name"] for item in response.get("items", [])]


def _resolve_roles(roles: list[str]) -> list[str]:
    """Match role names case-insensitively and return them in the spelling the backend expects.

    The roles read as prose rather than as identifiers, so `data owner` is an easy thing to write;
    accepting it costs nothing and the backend still receives the canonical `Data owner`.
    A name that is not built-in is looked up among the project's roles, which include the custom roles a cluster administrator defined, so a misspelt role is refused before any change is sent.
    """
    project_roles = None
    resolved = []
    for role in roles:
        name = _match_role(role, _ROLES)
        if name is None:
            if project_roles is None:
                project_roles = _project_role_names()
            name = _match_role(role, project_roles)
        if name is None:
            raise ValueError(
                f"Role must be one of the following: {tuple(project_roles)}."
            )
        resolved.append(name)
    return resolved


@public("hopsworks.core.project_members_api.ProjectMembersApi")
class ProjectMembersApi:
    """API for managing the members of a Hopsworks project.

    Use [`Project.get_members_api`][hopsworks_common.project.Project.get_members_api]
    to get an instance of this class.
    """

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
        path_params = ["project", _client._project_id, "projectMembers"]
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
        self, emails_and_roles: dict[str, str]
    ) -> list[project_member_mod.ProjectMember]:
        """Add one or more users to the project in a single call.

        Parameters:
            emails_and_roles:
                Mapping of a member's email to the project role to grant them,
                one of `Data owner`, `Data scientist`, `Observer`,
                `Feature store restricted`, or a custom project role.
                Roles are matched regardless of case.

        Returns:
            List of the newly added `ProjectMember` objects.

        Raises:
            ValueError: If any role is not a role of the project.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner or may not give the role.
        """
        roles = _resolve_roles(list(emails_and_roles.values()))
        emails_and_roles = dict(zip(emails_and_roles, roles, strict=True))

        _client = client._get_instance()
        path_params = ["project", _client._project_id, "projectMembers"]
        headers = {"content-type": "application/json"}
        body = {
            "projectTeam": [
                {
                    "projectTeamPK": {
                        "projectId": _client._project_id,
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
    def add_member(self, email: str, role: str) -> project_member_mod.ProjectMember:
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
                `Observer`, `Feature store restricted`, or a custom project role.
                Matched regardless of case.

        Returns:
            The newly added `ProjectMember`.

        Raises:
            ValueError: If `role` is not a role of the project.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner or may not give the role.
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
        self, email: str, role: str
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
                `Observer`, `Feature store restricted`, or a custom project role.
                Matched regardless of case.

        Returns:
            The updated `ProjectMember`, or `None` if the member could not be
            found immediately after the update.

        Raises:
            ValueError: If `role` is not a role of the project.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner, may not give the role, or `email` is the project owner.
        """
        role = _resolve_roles([role])[0]
        _client = client._get_instance()
        path_params = ["project", _client._project_id, "projectMembers", email]
        _client._send_request("POST", path_params, data={"role": role})
        return self._find_member(email)

    @public
    def remove_member(
        self,
        email: str,
        delete_home_dir: bool = False,
        new_file_owner: str | None = None,
    ) -> None:
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
            new_file_owner: Email address of the data owner that takes over the removed member's home directory.
                Defaults to the data owner who has been in the project longest.
                Ignored when `delete_home_dir` is set, since there is then nothing to take over.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if a data scientist tries to remove someone other than themselves.
        """
        _client = client._get_instance()
        path_params = ["project", _client._project_id, "projectMembers", email]
        query_params = {"deleteHomeDir": "true" if delete_home_dir else "false"}
        if new_file_owner is not None:
            query_params["newFileOwner"] = new_file_owner
        _client._send_request("DELETE", path_params, query_params=query_params)

    @public
    def transfer_ownership(self, email: str) -> None:
        """Hand the project over to another data owner of the project.

        Only the current owner can do this.
        The previous owner stays a data owner, so they can afterwards be removed like any other member.
        This also works in projects whose members are managed by group mapping, since no membership changes.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.get_members_api().transfer_ownership("carol@example.com")
            ```

        Parameters:
            email: Email address of the data owner who becomes the project owner.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller does not own the project or the new owner is not a data owner of it.
        """
        _client = client._get_instance()
        path_params = ["project", _client._project_id, "projectMembers", email, "owner"]
        _client._send_request("PUT", path_params)
