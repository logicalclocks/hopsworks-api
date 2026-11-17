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

import humps
from hopsworks_apigen import public


@public("hopsworks.gpu_availability.GpuAvailability")
class GpuAvailability:
    """GPUs a project can use, as reported by the Hopsworks cluster.

    When NVIDIA Run:ai manages the cluster's GPUs, the numbers come from Run:ai for the project's Run:ai project, its department and its default node pool.
    Otherwise only the cluster's GPU inventory is filled in.
    Counts are in whole GPUs; `None` means unknown, or unlimited for a quota or limit.

    Use [`Project.get_gpu_availability`][hopsworks_common.project.Project.get_gpu_availability] to get an instance of this class.
    """

    def __init__(
        self,
        provider=None,
        node_pool=None,
        project=None,
        department=None,
        node_pool_gpus=None,
        available_non_preemptible=None,
        available_preemptible=None,
        warnings=None,
        **kwargs,
    ):
        self._provider = provider
        self._node_pool = node_pool
        self._project = project
        self._department = department
        self._node_pool_gpus = node_pool_gpus
        self._available_non_preemptible = available_non_preemptible
        self._available_preemptible = available_preemptible
        self._warnings = warnings or []

    @classmethod
    def from_response_json(cls, json_dict) -> GpuAvailability:
        return cls(**humps.decamelize(json_dict))

    @public
    @property
    def provider(self) -> str | None:
        """Where the numbers come from: `RUN_AI` or `KUBERNETES`."""
        return self._provider

    @public
    @property
    def node_pool(self) -> str | None:
        """The Run:ai node pool the numbers are for."""
        return self._node_pool

    @public
    @property
    def project(self) -> dict | None:
        """The Run:ai project's `name`, `phase`, `quota`, `limit`, `allocated`, `allocated_non_preemptible` and `requested` GPUs."""
        return self._project

    @public
    @property
    def department(self) -> dict | None:
        """The Run:ai department's `name`, `quota`, `limit` and allocation.

        `expected` names the department the project should be in when a Run:ai administrator has to move it.
        """
        return self._department

    @public
    @property
    def node_pool_gpus(self) -> dict | None:
        """The node pool's `total`, `allocated` and `unallocated` (idle) GPUs, with the sample time `sampled_at`."""
        return self._node_pool_gpus

    @public
    @property
    def available_non_preemptible(self) -> float | None:
        """GPUs that notebooks, terminals and model deployments can still get within the project's quota."""
        return self._available_non_preemptible

    @public
    @property
    def available_preemptible(self) -> float | None:
        """Idle GPUs a batch job could borrow over quota right now.

        This is an estimate: a job asking for more is still submitted and waits in the project's queue.
        """
        return self._available_preemptible

    @public
    @property
    def warnings(self) -> list[str]:
        """Conditions that make the numbers incomplete, such as `RUN_AI_UNREACHABLE`, `RUN_AI_PROJECT_NOT_BOUND` or `RUN_AI_METRICS_MISSING`."""
        return self._warnings

    def __repr__(self) -> str:
        return (
            f"GpuAvailability(provider={self._provider!r}, "
            f"available_non_preemptible={self._available_non_preemptible!r}, "
            f"available_preemptible={self._available_preemptible!r}, warnings={self._warnings!r})"
        )
