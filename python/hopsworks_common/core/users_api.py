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
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.user import AdminUser


_ROLE_ARG = Literal["HOPS_ADMIN", "HOPS_USER", "HOPS_SERVICE_USER"]
_ROLES = get_args(_ROLE_ARG)

_REGISTER_STATUS_ARG = Literal[
    "NEW_MOBILE_ACCOUNT",
    "VERIFIED_ACCOUNT",
    "ACTIVATED_ACCOUNT",
    "DEACTIVATED_ACCOUNT",
    "BLOCKED_ACCOUNT",
    "LOST_MOBILE",
    "SPAM_ACCOUNT",
    "TEMP_PASSWORD",
]
_REGISTER_STATUSES = get_args(_REGISTER_STATUS_ARG)


def _validate_role(role: str) -> None:
    if role not in _ROLES:
        raise ValueError(f"Role must be one of the following: {_ROLES}.")


@public("hopsworks.core.users_api.UsersApi")
class UsersApi:
    """API for administering platform users in Hopsworks.

    This is an admin-only capability, distinct from per-project membership
    (see [`ProjectMembersApi`][hopsworks_common.core.project_members_api.ProjectMembersApi]):
    the calling account must hold the `HOPS_ADMIN` platform role.

    Use [`hopsworks.get_users_api`][hopsworks.get_users_api] to get an instance of this class.
    """

    @public
    def get_users(self) -> list[AdminUser]:
        """Get all registered platform users.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            for user in users_api.get_users():
                print(user.email, user.roles)
            ```

        Returns:
            List of all platform users known to this Hopsworks instance.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a platform admin.
        """
        _client = client._get_instance()
        path_params = ["admin", "users"]
        return AdminUser.from_response_json_list(
            _client._send_request("GET", path_params)
        )

    @public
    def get_user(self, user_id: int) -> AdminUser | None:
        """Get a single platform user by id.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            user = users_api.get_user(42)
            ```

        Parameters:
            user_id: Id of the platform user, as returned on `AdminUser.id`.

        Returns:
            The AdminUser object, or `None` if no user with this id exists.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id]
        try:
            response = _client._send_request("GET", path_params)
        except RestAPIError as e:
            if e.response.status_code == 404:
                return None
            raise
        return AdminUser.from_response_json(response)

    @public
    def register_user(
        self,
        email: str,
        first_name: str,
        last_name: str,
        password: str | None = None,
        max_num_projects: int | None = None,
        role: _ROLE_ARG = "HOPS_USER",
        status: _REGISTER_STATUS_ARG | None = None,
    ) -> AdminUser:
        """Register a new platform user account.

        Only email/password accounts are supported; SSO/remote account
        registration is not exposed through this method.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            new_user = users_api.register_user(
                email="alice@example.com",
                first_name="Alice",
                last_name="Smith",
                role="HOPS_USER",
                status="ACTIVATED_ACCOUNT",
            )
            if new_user.password:
                # Hand this off securely (e.g. a secrets manager); never log or print it.
                temporary_password = new_user.password
            ```

        Parameters:
            email: Email address of the new user, used as their login.
            first_name: Given name of the new user.
            last_name: Surname of the new user.
            password: Initial password for the new account.
                If not provided, the backend generates a temporary one, returned
                on the `password` attribute of the resulting `AdminUser`.
            max_num_projects: Maximum number of projects the new user is allowed
                to own. Defaults to the backend-configured default when not set.
            role: Platform role to assign, one of `HOPS_ADMIN`, `HOPS_USER`, `HOPS_SERVICE_USER`.
            status: Initial account status, one of `NEW_MOBILE_ACCOUNT`,
                `VERIFIED_ACCOUNT`, `ACTIVATED_ACCOUNT`, `DEACTIVATED_ACCOUNT`,
                `BLOCKED_ACCOUNT`, `LOST_MOBILE`, `SPAM_ACCOUNT`, `TEMP_PASSWORD`.
                Defaults to `TEMP_PASSWORD`, requiring the user to set a new
                password on first login.

        Returns:
            The newly registered AdminUser.

        Raises:
            ValueError: If `role` or `status` is not one of the supported values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the email is already registered.
        """
        _validate_role(role)
        if status is not None and status not in _REGISTER_STATUSES:
            raise ValueError(
                f"Status must be one of the following: {_REGISTER_STATUSES}."
            )
        _client = client._get_instance()
        query_params = {
            "accountType": "M_ACCOUNT_TYPE",
            "email": email,
            "givenName": first_name,
            "surname": last_name,
            "role": role,
        }
        if password is not None:
            query_params["password"] = password
        if max_num_projects is not None:
            query_params["maxNumProjects"] = max_num_projects
        if status is not None:
            query_params["status"] = status
        path_params = ["admin", "users"]
        response = _client._send_request("POST", path_params, query_params=query_params)
        return AdminUser.from_response_json(response)

    @public
    def set_role(self, user_id: int, role: _ROLE_ARG) -> AdminUser:
        """Change a platform user's role.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.set_role(42, "HOPS_ADMIN")
            ```

        Parameters:
            user_id: Id of the platform user.
            role: New platform role, one of `HOPS_ADMIN`, `HOPS_USER`, `HOPS_SERVICE_USER`.

        Returns:
            The updated AdminUser.

        Raises:
            ValueError: If `role` is not one of the supported values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if a platform admin tries to change their own role.
        """
        _validate_role(role)
        _client = client._get_instance()
        path_params = ["admin", "users", user_id, "role"]
        headers = {"content-type": "text/plain"}
        response = _client._send_request("PUT", path_params, headers=headers, data=role)
        return AdminUser.from_response_json(response)

    @public
    def update_user(self, user_id: int, max_num_projects: int) -> AdminUser:
        """Change the maximum number of projects a platform user is allowed to own.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.update_user(42, max_num_projects=10)
            ```

        Parameters:
            user_id: Id of the platform user to update.
            max_num_projects: New maximum number of projects the user may own.

        Returns:
            The updated AdminUser.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id]
        headers = {"content-type": "application/json"}
        body = {"maxNumProjects": max_num_projects}
        response = _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps(body)
        )
        return AdminUser.from_response_json(response)

    @public
    def activate_user(self, user_id: int) -> AdminUser:
        """Activate a pending platform user account.

        If the account has no platform role assigned yet, it is granted `HOPS_USER`.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.activate_user(42)
            ```

        Parameters:
            user_id: Id of the platform user to activate.

        Returns:
            The updated AdminUser.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id, "accepted"]
        headers = {"content-type": "application/json"}
        response = _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps({})
        )
        return AdminUser.from_response_json(response)

    @public
    def reject_user(self, user_id: int) -> AdminUser:
        """Reject a platform user's registration request.

        Marks the account as spam; the user is no longer able to log in.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.reject_user(42)
            ```

        Parameters:
            user_id: Id of the platform user to reject.

        Returns:
            The updated AdminUser.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id, "rejected"]
        response = _client._send_request("PUT", path_params)
        return AdminUser.from_response_json(response)

    @public
    def resend_confirmation_email(self, user_id: int) -> AdminUser:
        """Resend the account confirmation email to a platform user.

        Only works while the account is still in its initial unconfirmed state;
        it does not change the status of an already-confirmed account.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.resend_confirmation_email(42)
            ```

        Parameters:
            user_id: Id of the platform user.

        Returns:
            The updated AdminUser.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the account is already confirmed.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id, "pending"]
        response = _client._send_request("PUT", path_params)
        return AdminUser.from_response_json(response)

    @public
    def delete_user(self, user_id: int) -> None:
        """Delete a platform user account.

        Danger: Potentially dangerous operation
            This permanently removes the user account. The backend rejects the
            request if the user still owns any projects; remove or transfer
            those projects first.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            users_api = hopsworks.get_users_api()

            users_api.delete_user(42)
            ```

        Parameters:
            user_id: Id of the platform user to delete.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the user still owns projects.
        """
        _client = client._get_instance()
        path_params = ["admin", "users", user_id]
        _client._send_request("DELETE", path_params)
