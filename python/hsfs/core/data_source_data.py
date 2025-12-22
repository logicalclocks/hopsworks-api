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

from typing import (
    TYPE_CHECKING,
    Any,
)

import humps


if TYPE_CHECKING:
    from hsfs import feature


class DataSourceData:
    """Metadata object used to provide Data source data information for a feature group."""

    def __init__(
        self,
        limit: int | None = None,
        features: list[feature.Feature] | None = None,
        preview: list[dict] | None = None,
        schema_fetch_failed: bool | None = False,
        schema_fetch_in_progress: bool | None = False,
        schema_fetch_logs: str | None = None,
        supported_resources: list[str] | None = None,
        **kwargs,
    ):
        self._limit = limit
        self._features = features
        self._preview = preview
        self._schema_fetch_failed = schema_fetch_failed
        self._schema_fetch_in_progress = schema_fetch_in_progress
        self._schema_fetch_logs = schema_fetch_logs
        self._supported_resources = supported_resources

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> DataSourceData:
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        return cls(**json_decamelized)

    @property
    def limit(self) -> int | None:
        return self._limit

    @property
    def features(self) -> list[feature.Feature] | None:
        return self._features

    @property
    def preview(self) -> list[dict] | None:
        return self._preview

    @property
    def schema_fetch_failed(self) -> bool | None:
        return self._schema_fetch_failed

    @property
    def schema_fetch_in_progress(self) -> bool | None:
        return self._schema_fetch_in_progress

    @property
    def schema_fetch_logs(self) -> str | None:
        return self._schema_fetch_logs

    @property
    def supported_resources(self) -> list[str] | None:
        return self._supported_resources
