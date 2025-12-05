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

from typing import Any

import humps


class Dataset:
    def __init__(
        self,
        id,
        name,
        dataset_type,
        attributes: dict[str, Any],
        **kwargs,
    ) -> None:
        self._id = id
        self._name = name
        self._description = kwargs.get("description")
        self._dataset_type = dataset_type
        self._path = attributes["path"]

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> list[Dataset]:
        json_decamelized = humps.decamelize(json_dict)["items"]
        for dataset in json_decamelized:
            _ = dataset.pop("type", None)
        return [cls(**dataset) for dataset in json_decamelized]

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def dataset_type(self) -> str:
        return self._dataset_type

    @property
    def path(self) -> str:
        return self._path
