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

from hopsworks_common import project
from hopsworks_common.gpu_availability import GpuAvailability


RUN_AI_RESPONSE = {
    "provider": "RUN_AI",
    "nodePool": "default",
    "project": {"name": "demo", "quota": 2.0, "allocatedNonPreemptible": 1.0},
    "department": {"name": "default", "expected": "research"},
    "nodePoolGpus": {"total": 4.0, "unallocated": 1.0, "sampledAt": "t"},
    "availableNonPreemptible": 1.0,
    "availablePreemptible": 1.0,
    "warnings": ["RUN_AI_DEPARTMENT_DRIFT"],
}


class TestGpuAvailability:
    def test_from_response_json_decamelizes_nested_fields(self):
        gpus = GpuAvailability.from_response_json(RUN_AI_RESPONSE)

        assert gpus.provider == "RUN_AI"
        assert gpus.node_pool == "default"
        assert gpus.project["allocated_non_preemptible"] == 1.0
        assert gpus.department["expected"] == "research"
        assert gpus.node_pool_gpus["sampled_at"] == "t"
        assert gpus.available_non_preemptible == 1.0
        assert gpus.available_preemptible == 1.0
        assert gpus.warnings == ["RUN_AI_DEPARTMENT_DRIFT"]

    def test_kubernetes_provider_leaves_run_ai_fields_empty(self):
        gpus = GpuAvailability.from_response_json(
            {"provider": "KUBERNETES", "nodePoolGpus": {"total": 4.0}, "warnings": []}
        )

        assert gpus.project is None
        assert gpus.available_preemptible is None
        assert gpus.node_pool_gpus == {"total": 4.0}

    def test_project_requests_its_kube_gpu_resource(self, mocker):
        client = mocker.MagicMock()
        client._project_id = 119
        client._send_request.return_value = RUN_AI_RESPONSE
        mocker.patch("hopsworks_common.client._get_instance", return_value=client)

        gpus = project.Project(
            project_id=119, project_name="demo"
        ).get_gpu_availability()

        client._send_request.assert_called_once_with(
            "GET", ["project", 119, "kube", "gpu"]
        )
        assert gpus.available_non_preemptible == 1.0

    def test_project_requests_its_own_gpus_when_the_client_is_on_another_project(
        self, mocker
    ):
        client = mocker.MagicMock()
        client._project_id = 221
        client._send_request.return_value = RUN_AI_RESPONSE
        mocker.patch("hopsworks_common.client._get_instance", return_value=client)

        project.Project(project_id=119, project_name="demo").get_gpu_availability()

        client._send_request.assert_called_once_with(
            "GET", ["project", 119, "kube", "gpu"]
        )
