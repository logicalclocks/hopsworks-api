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

from typing import Any, Dict, List

import humps


class OnlineConfig:
    """
    Metadata object used to provide online Feature Store configuration information for a feature group.
    """

    def __init__(
        self,
        online_comments: List[str] = None,
        table_space: str = None,
        **kwargs,
    ):
        self._online_comments = online_comments
        self._table_space = table_space

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "OnlineConfig":
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "onlineComments": self._online_comments,
            "tableSpace": self._table_space,
        }

    @property
    def online_comments(self) -> List[str]:
        """List of comments applied to online feature store table."""
        return self._online_comments

    @online_comments.setter
    def online_comments(self, online_comments: List[str]) -> None:
        self._online_comments = online_comments

    @property
    def table_space(self) -> str:
        """Table space of online feature store table for storing data on disk."""
        return self._table_space

    @table_space.setter
    def table_space(self, table_space: str) -> None:
        self._table_space = table_space
