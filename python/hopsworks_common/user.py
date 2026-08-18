#
#   Copyright 2021 Logical Clocks AB
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
from dataclasses import asdict, dataclass, field
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util


@public("hopsworks.user.User", "hsfs.user.User")
@dataclass
class User:
    email: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    href: str | None = None
    username: str | None = None
    status: str | None = None
    secret: str | None = None
    chosen_password: str | None = None
    repeated_password: str | None = None
    tos: bool | None = None
    two_factor: bool | None = None
    user_account_type: str | None = None
    tours_state: bool | None = None
    max_num_projects: int | None = None
    test_user: bool | None = None
    num_active_projects: int | None = None
    num_remaining_projects: int | None = None
    # Cluster roles, such as HOPS_ADMIN. Only the user's own profile carries these; a User
    # parsed from an embedded reference, for instance a project member, leaves this empty.
    roles: list[str] = field(default_factory=list)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any] | None) -> User | None:
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            if "firstname" in json_decamelized:
                json_decamelized["first_name"] = json_decamelized.pop("firstname")
                json_decamelized["last_name"] = json_decamelized.pop("lastname")
            # The backend sends the cluster roles as `role`, a list of group objects.
            role = json_decamelized.pop("role", None)
            if role:
                json_decamelized["roles"] = [
                    group.get("group_name") for group in role
                ]
            # Remove keys that are not part of the dataclass
            for key in set(json_decamelized.keys()) - set(
                User.__dataclass_fields__.keys()
            ):
                json_decamelized.pop(key)
            return cls(**json_decamelized)
        return None

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@public("hopsworks.user.AdminUser")
@dataclass
class AdminUser:
    """Represents a platform user as seen by a Hopsworks platform administrator.

    Use [`UsersApi`][hopsworks_common.core.users_api.UsersApi] to list, register,
    or otherwise manage platform users; get an instance of it with
    [`hopsworks.get_users_api`][hopsworks.get_users_api].
    """

    id: int | None = None
    email: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    account_type: str | None = None
    two_factor: bool | None = None
    tours_state: int | None = None
    # Raw UserAccountStatus code from hopsworks-ee, e.g. 2 = activated,
    # 6 = spam/rejected, 7 = temp password (not yet activated).
    status: int | None = None
    max_num_projects: int | None = None
    num_active_projects: int | None = None
    activated: str | None = None
    last_visited_at: str | None = None
    roles: list[str] = field(default_factory=list)
    remote_user_groups: list[str] = field(default_factory=list)
    # Auto-generated temporary password. Only set on the object returned by
    # UsersApi.register_user when no password was supplied at registration
    # time; None in every other case.
    password: str | None = None

    @classmethod
    def _from_decamelized(cls, json_decamelized: dict[str, Any]) -> AdminUser:
        if "firstname" in json_decamelized:
            json_decamelized["first_name"] = json_decamelized.pop("firstname")
            json_decamelized["last_name"] = json_decamelized.pop("lastname")
        role = json_decamelized.pop("role", None)
        json_decamelized["roles"] = (
            [group.get("group_name") for group in role] if role else []
        )
        for key in set(json_decamelized.keys()) - set(cls.__dataclass_fields__):
            json_decamelized.pop(key)
        return cls(**json_decamelized)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any] | None) -> AdminUser | None:
        if not json_dict:
            return None
        return cls._from_decamelized(humps.decamelize(json_dict))

    @classmethod
    def from_response_json_list(
        cls, json_dict: dict[str, Any] | list[dict[str, Any]] | None
    ) -> list[AdminUser]:
        if not json_dict:
            return []
        json_decamelized = humps.decamelize(json_dict)
        items = (
            json_decamelized["items"]
            if isinstance(json_decamelized, dict) and "items" in json_decamelized
            else json_decamelized
        )
        return [cls._from_decamelized(item) for item in items]

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
