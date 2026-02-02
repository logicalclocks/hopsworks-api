#
#   Copyright 2026 Hopsworks AB
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
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_common import util


if TYPE_CHECKING:
    from hsfs.core.chart import Chart


class Dashboard:
    """Metadata object used to provide Dashboard information."""

    def __init__(
        self,
        id: int | None = None,
        name: str | None = None,
        charts: list[Chart] | None = None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._charts = charts

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> list[Dashboard]:
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)

        if isinstance(json_decamelized, list):
            return [cls(**item) for item in json_decamelized]
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "charts": self._charts,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> int | None:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def charts(self) -> list[Chart] | None:
        return self._charts

    @charts.setter
    def charts(self, charts: list[Chart]) -> None:
        self._charts = charts

    def delete(self) -> None:
        """Delete the dashboard from the feature store.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hsfs.core.dashboard_api import DashboardApi

        DashboardApi().delete_dashboard(self.id)

    def update(self) -> None:
        """Update the dashboard in the feature store.

        Updates the dashboard metadata with the current values of this object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hsfs.core.dashboard_api import DashboardApi

        DashboardApi().update_dashboard(self)
