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
from typing import Any, Dict, Optional

import humps
from hopsworks_common import util


@dataclass
class User:
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    href: Optional[str] = None
    username: Optional[str] = None
    status: Optional[str] = None
    secret: Optional[str] = None
    chosen_password: Optional[str] = None
    repeated_password: Optional[str] = None
    tos: Optional[bool] = None
    two_factor: Optional[bool] = None
    user_account_type: Optional[str] = None
    tours_state: Optional[bool] = None
    max_num_projects: Optional[int] = None
    test_user: Optional[bool] = None
    num_active_projects: Optional[int] = None
    num_remaining_projects: Optional[int] = None

    @classmethod
    def from_response_json(cls, json_dict: Optional[Dict[str, Any]]) -> Optional[User]:
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            if "firstname" in json_decamelized.keys():
                json_decamelized["first_name"] = json_decamelized.pop("firstname")
                json_decamelized["last_name"] = json_decamelized.pop("lastname")
            # Remove keys that are not part of the dataclass
            for key in set(json_decamelized.keys()) - set(
                User.__dataclass_fields__.keys()
            ):
                json_decamelized.pop(key)
            return cls(**json_decamelized)
        else:
            return None

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
