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

import json
from typing import Any, Dict, Optional

import humps
from hopsworks_common import util


class IngestionRun:
    """
    Metadata object used to provide Ingestion Run information for a feature group.
    """

    def __init__(
        self,
        id: Optional[int] = None,
        starting_offsets: str = None,
        ending_offsets: str = None,
        current_offsets: str = None,
        total_entries: int = None,
        remaining_entries: int = None,
        **kwargs,
    ):
        self._id = id
        self._starting_offsets = starting_offsets
        self._ending_offsets = ending_offsets
        self._current_offsets = current_offsets
        self._total_entries = total_entries
        self._remaining_entries = remaining_entries

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "IngestionRun":
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "startingOffsets": self._starting_offsets,
            "endingOffsets": self._ending_offsets,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> int:
        return self._id

    @property
    def starting_offsets(self) -> str:
        return self._starting_offsets

    @starting_offsets.setter
    def starting_offsets(self, starting_offsets: str) -> None:
        self._starting_offsets = starting_offsets

    @property
    def ending_offsets(self) -> str:
        return self._ending_offsets

    @ending_offsets.setter
    def ending_offsets(self, ending_offsets: str) -> None:
        self._ending_offsets = ending_offsets

    @property
    def current_offsets(self) -> str:
        return self._current_offsets
    
    @property
    def total_entries(self) -> int:
        return self._total_entries
    
    @property
    def remaining_entries(self) -> int:
        return self._remaining_entries
