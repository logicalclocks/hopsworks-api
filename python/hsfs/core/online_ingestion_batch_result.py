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
from typing import Any, Dict

import humps
from hopsworks_common import util


class OnlineIngestionBatchResult:
    """
    Metadata object used to provide Online Ingestion Batch Result information for a feature group.
    """

    def __init__(
        self,
        id: str = None,
        batch_size: int = None,
        status: str = None,
        **kwargs,
    ):
        self._id = id
        self._batch_size = batch_size
        self._status = status

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> OnlineIngestionBatchResult:
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
            "id": self._id,
            "batchSize": self._batch_size,
            "status": self._status,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> str:
        return self._id

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def status(self) -> str:
        return self._status
