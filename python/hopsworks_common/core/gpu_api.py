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
from hopsworks_common import gpu_availability as gpu_availability_mod


class GpuApi:
    def _get_gpu_availability(
        self, project_id: int
    ) -> gpu_availability_mod.GpuAvailability:
        """Get the GPUs a project can use.

        Parameters:
            project_id: ID of the project, which need not be the client's active project.

        Returns:
            The project's GPU availability.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = ["project", project_id, "kube", "gpu"]
        return gpu_availability_mod.GpuAvailability.from_response_json(
            _client._send_request("GET", path_params)
        )
