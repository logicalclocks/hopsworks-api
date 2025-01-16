#
#   Copyright 2025 Hopsworks AB
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
from typing import Any, Dict

import humps
from hopsworks_common import util


class OnlineIngestionResult:
    """
    Metadata object used to provide Online Ingestion Batch Result information for a feature group.
    """

    def __init__(
        self,
        online_ingestion_id: int = None,
        status: str = None,
        rows: int = None,
        **kwargs,
    ):
        self._online_ingestion_id = online_ingestion_id
        self._status = status
        self._rows = rows

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> OnlineIngestionResult:
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        if "count" not in json_decamelized:
            return cls(**json_decamelized)
        elif json_decamelized["count"] == 1:
            return cls(**json_decamelized["items"][0])
        elif json_decamelized["count"] > 1:
            return [cls(**item) for item in json_decamelized["items"]]
        else:
            return None

    def to_dict(self):
        return {
            "onlineIngestionId": self._online_ingestion_id,
            "status": self._status,
            "rows": self._rows,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def online_ingestion_id(self) -> int:
        return self._online_ingestion_id

    @property
    def status(self) -> str:
        return self._status

    @property
    def rows(self) -> int:
        return self._rows
