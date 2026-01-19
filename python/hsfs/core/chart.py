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
from typing import (
    Any,
)

import humps
from hopsworks_common import util
from hsfs.core.job import Job


class Chart:
    """Metadata object used to provide Chart information."""

    def __init__(
        self,
        id: int | None = None,
        title: str | None = None,
        description: str | None = None,
        url: str | None = None,
        job: Job | None = None,
        **kwargs,
    ):
        self._id = id
        self._title = title
        self._description = description
        self._url = url
        self._job = job

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> list[Chart]:
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)

        if isinstance(json_decamelized, list):
            return [cls(**item) for item in json_decamelized]
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "title": self._title,
            "description": self._description,
            "url": self._url,
            "job": self._job,
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
    def title(self) -> str | None:
        return self._title

    @title.setter
    def title(self, title: str) -> None:
        self._title = title

    @property
    def description(self) -> str | None:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description

    @property
    def url(self) -> str | None:
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        self._url = url

    @property
    def job(self) -> Job | None:
        return self._job

    @job.setter
    def job(self, job: Job) -> None:
        self._job = job

    def delete(self) -> None:
        """Delete the chart from the feature store.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hsfs.core.chart_api import ChartApi

        ChartApi().delete_chart(self.id)

    def update(self) -> None:
        """Update the chart in the feature store.

        Updates the chart metadata with the current values of this object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hsfs.core.chart_api import ChartApi

        ChartApi().update_chart(self)
