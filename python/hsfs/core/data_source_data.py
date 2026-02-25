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
from hopsworks_apigen import public


if TYPE_CHECKING:
    from hsfs import feature


@public
class DataSourceData:
    """Metadata object used to provide Data source data information for a feature group."""

    def __init__(
        self,
        limit: int | None = None,
        features: list[feature.Feature] | None = None,
        preview: list[dict] | None = None,
        schema_fetch_failed: bool = False,
        schema_fetch_in_progress: bool = False,
        schema_fetch_logs: str = "",
        supported_resources: list[str] | None = None,
        **kwargs,
    ):
        self._limit = limit
        self._features = features if features else []
        self._preview = preview if preview else []
        self._schema_fetch_failed = schema_fetch_failed
        self._schema_fetch_in_progress = schema_fetch_in_progress
        self._schema_fetch_logs = schema_fetch_logs
        self._supported_resources = supported_resources if supported_resources else []

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> DataSourceData:
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        return cls(**json_decamelized)

    @public
    @property
    def limit(self) -> int | None:
        """The total number of rows available, if it is known."""
        return self._limit

    @public
    @property
    def features(self) -> list[feature.Feature]:
        """The list of features in the data source."""
        return self._features

    @public
    @property
    def preview(self) -> list[dict]:
        """The preview of the data source."""
        return self._preview

    @public
    @property
    def schema_fetch_failed(self) -> bool:
        """Whether the schema fetch has failed."""
        return self._schema_fetch_failed

    @public
    @property
    def schema_fetch_in_progress(self) -> bool:
        """Whether the schema fetch is in progress."""
        return self._schema_fetch_in_progress

    @public
    @property
    def schema_fetch_logs(self) -> str:
        """The logs of the schema fetch."""
        return self._schema_fetch_logs

    @public
    @property
    def supported_resources(self) -> list[str]:
        """The list of supported resources."""
        return self._supported_resources
