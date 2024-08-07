#
#   Copyright 2024 Hopsworks AB
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

from typing import Any, Dict

import humps


class OnlineConfig:
    def __init__(
        self,
        online_comments = None,
        **kwargs,
    ):
        self._online_comments = online_comments

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "OnlineConfig":
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "onlineConfig": self._online_comments,
        }

    @property
    def online_comments(self):
        return self._online_comments
