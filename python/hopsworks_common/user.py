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
from dataclasses import asdict, dataclass
from typing import Any

import humps
from hopsworks_common import util


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

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any] | None) -> User | None:
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            if "firstname" in json_decamelized:
                json_decamelized["first_name"] = json_decamelized.pop("firstname")
                json_decamelized["last_name"] = json_decamelized.pop("lastname")
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
