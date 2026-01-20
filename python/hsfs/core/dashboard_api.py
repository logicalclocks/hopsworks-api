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

from hopsworks_common import client
from hsfs.core.dashboard import Dashboard


class DashboardApi:
    def create_dashboard(self, dashboard: Dashboard) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dashboards",
        ]

        _client._send_request(
            "POST",
            path_params,
            headers={"content-type": "application/json"},
            data=dashboard.json(),
        )

    def get_dashboards(self) -> list[Dashboard]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dashboards",
        ]

        return Dashboard.from_response_json(
            _client._send_request(
                "GET",
                path_params,
                headers={"content-type": "application/json"},
            )
        )

    def get_dashboard(self, dashboard_id: int) -> Dashboard:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dashboards",
            dashboard_id,
        ]

        return Dashboard.from_response_json(
            _client._send_request(
                "GET",
                path_params,
                headers={"content-type": "application/json"},
            )
        )

    def update_dashboard(self, dashboard: Dashboard) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dashboards",
            dashboard._id,
        ]

        _client._send_request(
            "PUT",
            path_params,
            headers={"content-type": "application/json"},
            data=dashboard.json(),
        )

    def delete_dashboard(self, dashboard_id: int) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "dashboards",
            dashboard_id,
        ]

        _client._send_request(
            "DELETE",
            path_params,
            headers={"content-type": "application/json"},
        )
