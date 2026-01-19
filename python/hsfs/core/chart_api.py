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
from hsfs.core.chart import Chart


class ChartApi:

    def create_chart(self, chart: Chart) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "charts",
        ]

        _client._send_request(
            "POST",
            path_params,
            headers={"content-type": "application/json"},
            data=chart.json(),
        )

    def get_charts(self) -> list[Chart]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "charts",
        ]

        return Chart.from_response_json(_client._send_request(
            "GET",
            path_params,
            headers={"content-type": "application/json"},
        ))
    
    def get_chart(self, chart_id: int) -> Chart:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "charts",
            chart_id,
        ]

        return Chart.from_response_json(_client._send_request(
            "GET",
            path_params,
            headers={"content-type": "application/json"},
        ))
    
    def update_chart(self, chart: Chart) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "charts",
            chart._id,
        ]

        _client._send_request(
            "PUT",
            path_params,
            headers={"content-type": "application/json"},
            data=chart.json(),
        )
    
    def delete_chart(self, chart_id: int) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "charts",
            chart_id,
        ]

        _client._send_request(
            "DELETE",
            path_params,
            headers={"content-type": "application/json"},
        )