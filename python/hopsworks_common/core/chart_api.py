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
"""REST-backed API for Hopsworks charts.

Charts are a UI-side concept attached to a project and rendered on
dashboards; they are not part of the feature-store or model-registry domain
and therefore do not have richer SDK models. This module exposes a thin CRUD
wrapper that follows the codebase's "HTTP calls live in
``core/<entity>_api.py``" rule.
"""

from __future__ import annotations

from typing import Any

from hopsworks_apigen import public
from hopsworks_common.core import rest


@public("hopsworks.core.chart_api.ChartApi")
class ChartApi:
    """REST wrapper over ``/project/{id}/charts``.

    The backend returns charts as plain dicts with layout fields
    (``width``/``height``/``x``/``y``) that are NOT NULL; callers creating
    charts must supply them.

    HTTP calls go through :mod:`hopsworks_common.core.rest` so this module
    does not reach into the SDK's private client surface
    (``_send_request`` / ``_project_id``).
    """

    def _path(self) -> list[Any]:
        return rest.project_path("charts")

    @public
    def list_charts(self) -> list[dict[str, Any]]:
        """Return every chart visible in the current project.

        Returns:
            A list of chart dicts with fields ``id``, ``title``, ``url``, …
        """
        payload = rest.send_request("GET", self._path())
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("items") or payload.get("charts") or []
        return []

    @public
    def get_chart(self, chart_id: int) -> dict[str, Any]:
        """Fetch a single chart by ID.

        Args:
            chart_id: Chart identifier.

        Returns:
            The chart dict as returned by the backend.
        """
        return rest.send_request("GET", [*self._path(), chart_id])

    @public
    def create_chart(
        self,
        title: str,
        url: str,
        description: str = "",
        width: int = 6,
        height: int = 4,
        x: int = 0,
        y: int = 0,
        job_name: str | None = None,
    ) -> dict[str, Any]:
        """Create a chart.

        Args:
            title: Chart title.
            url: URL to render (typically a Plotly HTML asset in HopsFS).
            description: Free-form description.
            width: Dashboard width in grid units.
            height: Dashboard height in grid units.
            x: Dashboard x position in grid units.
            y: Dashboard y position in grid units.
            job_name: Optional Hopsworks job to associate.

        Returns:
            The created chart dict.
        """
        body: dict[str, Any] = {
            "title": title,
            "description": description,
            "url": url,
            "width": width,
            "height": height,
            "x": x,
            "y": y,
        }
        if job_name:
            body["job"] = {"name": job_name}
        return rest.send_request("POST", self._path(), json_body=body)

    @public
    def update_chart(self, chart_id: int, **fields: Any) -> dict[str, Any]:  # noqa: D417
        """Update selected fields of a chart.

        Accepts any subset of ``title``, ``description``, ``url``, ``width``,
        ``height``, ``x``, ``y``, ``job`` as keyword arguments.

        Args:
            chart_id: Chart identifier.

        Returns:
            The updated chart dict.
        """
        return rest.send_request("PUT", [*self._path(), chart_id], json_body=fields)

    @public
    def delete_chart(self, chart_id: int) -> None:
        """Delete a chart by ID.

        Args:
            chart_id: Chart identifier.
        """
        rest.send_request("DELETE", [*self._path(), chart_id])
