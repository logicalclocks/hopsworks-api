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
"""REST-backed API for Hopsworks dashboards.

Dashboards are collections of charts laid out on a grid. This module keeps
the HTTP surface in one place (per the codebase rule) so the ``hops
dashboard`` CLI and any future SDK callers share a single implementation.
"""

from __future__ import annotations

from typing import Any

from hopsworks_apigen import public
from hopsworks_common.core import rest


@public("hopsworks.core.dashboard_api.DashboardApi")
class DashboardApi:
    """REST wrapper over ``/project/{id}/dashboards``.

    HTTP calls go through :mod:`hopsworks_common.core.rest` so this module
    does not reach into the SDK's private client surface
    (``_send_request`` / ``_project_id``).
    """

    def _path(self) -> list[Any]:
        return rest.project_path("dashboards")

    @public
    def list_dashboards(self) -> list[dict[str, Any]]:
        """Return every dashboard in the current project.

        Returns:
            A list of dashboard dicts, each with an ``id``, ``name`` and
            embedded ``charts`` list.
        """
        payload = rest.send_request("GET", self._path())
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("items") or payload.get("dashboards") or []
        return []

    @public
    def get_dashboard(self, dashboard_id: int) -> dict[str, Any]:
        """Fetch a single dashboard by ID.

        Args:
            dashboard_id: Dashboard identifier.

        Returns:
            The dashboard dict.
        """
        return rest.send_request("GET", [*self._path(), dashboard_id])

    @public
    def create_dashboard(self, name: str) -> dict[str, Any]:
        """Create an empty dashboard.

        Args:
            name: Dashboard name.

        Returns:
            The created dashboard dict.
        """
        return rest.send_request(
            "POST", self._path(), json_body={"name": name, "charts": []}
        )

    @public
    def update_dashboard(self, dashboard_id: int, **fields: Any) -> dict[str, Any]:  # noqa: D417
        """Update selected fields of a dashboard.

        Accepts ``name`` and/or ``charts`` as keyword arguments.

        Args:
            dashboard_id: Dashboard identifier.

        Returns:
            The updated dashboard dict.
        """
        return rest.send_request("PUT", [*self._path(), dashboard_id], json_body=fields)

    @public
    def delete_dashboard(self, dashboard_id: int) -> None:
        """Delete a dashboard by ID.

        Args:
            dashboard_id: Dashboard identifier.
        """
        rest.send_request("DELETE", [*self._path(), dashboard_id])

    @public
    def add_chart(self, dashboard_id: int, chart_id: int) -> dict[str, Any]:
        """Attach an existing chart to a dashboard.

        The backend represents membership by embedding the chart list in the
        dashboard, so we fetch, mutate, and PUT.

        Args:
            dashboard_id: Dashboard identifier.
            chart_id: Chart identifier to attach.

        Returns:
            The updated dashboard dict.
        """
        dashboard = self.get_dashboard(dashboard_id)
        charts = list(dashboard.get("charts") or [])
        if any(c.get("id") == chart_id for c in charts):
            return dashboard
        charts.append({"id": chart_id})
        return self.update_dashboard(
            dashboard_id, name=dashboard.get("name"), charts=charts
        )

    @public
    def remove_chart(self, dashboard_id: int, chart_id: int) -> dict[str, Any]:
        """Detach a chart from a dashboard.

        Args:
            dashboard_id: Dashboard identifier.
            chart_id: Chart identifier to remove.

        Returns:
            The updated dashboard dict.
        """
        dashboard = self.get_dashboard(dashboard_id)
        charts = [c for c in (dashboard.get("charts") or []) if c.get("id") != chart_id]
        return self.update_dashboard(
            dashboard_id, name=dashboard.get("name"), charts=charts
        )
