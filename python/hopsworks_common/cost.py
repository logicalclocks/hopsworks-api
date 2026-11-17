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
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util


if TYPE_CHECKING:
    import pandas as pd


@public("hopsworks.cost.CostRow")
@dataclass
class CostRow:
    """Cost of one Kubernetes namespace, or one department, over a time window, as computed by OpenCost.

    Costs are in the currency of the enclosing [`NamespaceCosts`][hopsworks_common.cost.NamespaceCosts].
    `kind` is `NAMESPACE` for a namespace, `IDLE` for node capacity no pod requested (namespace `__idle__`), and `UNMOUNTED` for unmounted volumes and load balancers (namespace `__unmounted__`).
    A `NAMESPACE` row carries the `department_id` and `department_name` of the projects in it, both `None` when the namespace is no project's or is shared by projects of different departments.
    The department view has `DEPARTMENT` rows, one per department with `namespace` `None`, and an `UNASSIGNED` row for the namespaces that have no single department, alongside the same `IDLE` and `UNMOUNTED` rows.
    `namespace_count` is the number of namespaces summed into a `DEPARTMENT` or `UNASSIGNED` row, and `None` on every other row.
    A department holds its projects' whole history, so a project moved to another department moves its past cost with it.
    `other_cost` sums load balancer, shared and external costs.
    The cost columns sum to `total_cost`, and costs can be slightly negative after OpenCost's adjustments.
    `gpu_workload_cost` and `cpu_workload_cost` split `total_cost` by workload instead of by resource: a workload (Job, Deployment, StatefulSet, SparkApplication and the like) that requested GPU-hours in a bucket counts its whole cost, CPU, RAM and storage included, as `gpu_workload_cost`, and every other workload counts as `cpu_workload_cost`.
    The two sum to `total_cost` for a `NAMESPACE`, `DEPARTMENT` or `UNASSIGNED` row and are 0 for `IDLE` and `UNMOUNTED` rows, whereas `gpu_cost` is the cost of the GPUs alone.
    Pods without a controller share one workload per namespace, so they are classed together.
    The `*_hours_requested` and `*_hours_used` columns are CPU core-hours and RAM byte-hours.
    Efficiencies are used divided by requested hours, cost-weighted over CPU and RAM for `total_efficiency`, and `None` when nothing was requested.
    `cpu_no_requests` and `ram_no_requests` mean the namespace used that resource without requesting any, where OpenCost would report 100% efficiency.
    `share` is the fraction of the window's total cost, and `None` when that total is not positive, when the row is inside a [`CostBucket`][hopsworks_common.cost.CostBucket], or in a project report from [`CostsApi.get_project_costs`][hopsworks_common.core.costs_api.CostsApi.get_project_costs], whose only row would always be 1.
    """

    namespace: str | None = None
    kind: str | None = None
    department_id: int | None = None
    department_name: str | None = None
    namespace_count: int | None = None
    cpu_cost: float = 0.0
    gpu_cost: float = 0.0
    ram_cost: float = 0.0
    pv_cost: float = 0.0
    network_cost: float = 0.0
    other_cost: float = 0.0
    total_cost: float = 0.0
    gpu_workload_cost: float = 0.0
    cpu_workload_cost: float = 0.0
    cpu_efficiency: float | None = None
    ram_efficiency: float | None = None
    total_efficiency: float | None = None
    cpu_core_hours_requested: float = 0.0
    cpu_core_hours_used: float = 0.0
    ram_byte_hours_requested: float = 0.0
    ram_byte_hours_used: float = 0.0
    cpu_no_requests: bool = False
    ram_no_requests: bool = False
    share: float | None = None

    @classmethod
    def _from_decamelized(cls, json_decamelized: dict[str, Any]) -> CostRow:
        # A newer backend may send fields this client does not know yet,
        # and OpenCost reports a NaN cost as null.
        return cls(
            **{
                key: 0.0 if value is None and key.endswith("_cost") else value
                for key, value in json_decamelized.items()
                if key in cls.__dataclass_fields__
            }
        )

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any] | None) -> CostRow | None:
        if not json_dict:
            return None
        return cls._from_decamelized(humps.decamelize(json_dict))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)


@public("hopsworks.cost.CostBucket")
@dataclass
class CostBucket:
    """Per-namespace or per-department cost over one slice of a window, for plotting cost over time.

    An hour window is split into 10-minute slices, a day into hours, a week and a month into days, and a year into calendar months.
    `start` and `end` are ISO-8601 UTC timestamps.
    A bucket with `observed` false has no data, for example because OpenCost was not running yet or the data is past retention, so its cost is unknown rather than zero.
    A bucket with `partial` true has not ended yet, so its cost is still growing.
    """

    start: str | None = None
    end: str | None = None
    observed: bool = False
    partial: bool = False
    items: list[CostRow] = field(default_factory=list)

    @classmethod
    def _from_decamelized(cls, json_decamelized: dict[str, Any]) -> CostBucket:
        return cls(
            start=json_decamelized.get("start"),
            end=json_decamelized.get("end"),
            observed=bool(json_decamelized.get("observed")),
            partial=bool(json_decamelized.get("partial")),
            items=[
                CostRow._from_decamelized(item)
                for item in json_decamelized.get("items") or []
            ],
        )

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any] | None) -> CostBucket | None:
        if not json_dict:
            return None
        return cls._from_decamelized(humps.decamelize(json_dict))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)


@public("hopsworks.cost.CostCoverage")
@dataclass
class CostCoverage:
    """How many of the buckets of a window have any cost data.

    The totals and rows of a [`NamespaceCosts`][hopsworks_common.cost.NamespaceCosts] cover only the observed buckets, so a window with `observed_buckets` below `total_buckets` reports less than the full window.
    `first_observed` is the ISO-8601 UTC start of the first observed bucket, and `None` when no bucket has data.
    """

    observed_buckets: int = 0
    total_buckets: int = 0
    first_observed: str | None = None

    @classmethod
    def _from_decamelized(cls, json_decamelized: dict[str, Any]) -> CostCoverage:
        return cls(
            observed_buckets=json_decamelized.get("observed_buckets") or 0,
            total_buckets=json_decamelized.get("total_buckets") or 0,
            first_observed=json_decamelized.get("first_observed"),
        )

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> CostCoverage | None:
        if not json_dict:
            return None
        return cls._from_decamelized(humps.decamelize(json_dict))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)


@public("hopsworks.cost.NamespaceCosts")
@dataclass
class NamespaceCosts:
    """Kubernetes cost per namespace, or per department, over a time window.

    The cost is computed by OpenCost and proxied by Hopsworks.
    Each Hopsworks project runs in its own namespace, so a namespace row is the compute and storage cost of that project.
    In the administrator reports, `total_cost` and `idle_cost` cover the whole cluster, and `efficiency` is the cost-weighted efficiency of all namespace rows.
    In a project report from [`CostsApi.get_project_costs`][hopsworks_common.core.costs_api.CostsApi.get_project_costs] they cover that project alone: `items` and the bucket rows are the project's `NAMESPACE` row, and `idle_cost` is 0.
    `gpu_workload_cost` and `cpu_workload_cost` are the sums of the namespace rows' workload split, so with `idle_cost` and the `UNMOUNTED` row they add up to `total_cost`.
    `start` and `end` are ISO-8601 UTC timestamps.
    The totals and `items` cover only the buckets with data, as reported by `coverage`, and `items` are the per-row sums of the bucket rows, most expensive first.
    The department view has the same totals as the namespace view for the same window, with its namespace rows grouped into `DEPARTMENT` and `UNASSIGNED` rows.

    Use [`CostsApi.get_namespace_costs`][hopsworks_common.core.costs_api.CostsApi.get_namespace_costs], [`CostsApi.get_department_costs`][hopsworks_common.core.costs_api.CostsApi.get_department_costs] or [`CostsApi.get_project_costs`][hopsworks_common.core.costs_api.CostsApi.get_project_costs] to get an instance of this class.
    """

    window: str | None = None
    start: str | None = None
    end: str | None = None
    currency: str | None = None
    total_cost: float = 0.0
    idle_cost: float = 0.0
    gpu_workload_cost: float = 0.0
    cpu_workload_cost: float = 0.0
    efficiency: float | None = None
    coverage: CostCoverage = field(default_factory=CostCoverage)
    items: list[CostRow] = field(default_factory=list)
    buckets: list[CostBucket] = field(default_factory=list)

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | None
    ) -> NamespaceCosts | None:
        if not json_dict:
            return None
        json_decamelized = humps.decamelize(json_dict)
        # The REST response nests the totals; to_dict() writes them at the top level.
        totals = json_decamelized.get("totals") or json_decamelized
        return cls(
            window=json_decamelized.get("window"),
            start=json_decamelized.get("start"),
            end=json_decamelized.get("end"),
            currency=json_decamelized.get("currency"),
            total_cost=totals.get("total_cost") or 0.0,
            idle_cost=totals.get("idle_cost") or 0.0,
            gpu_workload_cost=totals.get("gpu_workload_cost") or 0.0,
            cpu_workload_cost=totals.get("cpu_workload_cost") or 0.0,
            efficiency=totals.get("efficiency"),
            coverage=CostCoverage._from_decamelized(
                json_decamelized.get("coverage") or {}
            ),
            items=[
                CostRow._from_decamelized(item)
                for item in json_decamelized.get("items") or []
            ],
            buckets=[
                CostBucket._from_decamelized(bucket)
                for bucket in json_decamelized.get("buckets") or []
            ],
        )

    @public
    def to_pandas(self) -> pd.DataFrame:
        """Return the rows as a DataFrame, one row per namespace or department.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            costs = hopsworks.get_costs_api().get_namespace_costs("month")

            df = costs.to_pandas().sort_values("total_cost", ascending=False)
            ```

        Returns:
            A DataFrame with one column per [`CostRow`][hopsworks_common.cost.CostRow] attribute.
        """
        import pandas as pd

        columns = list(CostRow.__dataclass_fields__)
        df = pd.DataFrame([row.to_dict() for row in self.items], columns=columns)
        # Nullable integers: with a None anywhere, plain pandas turns them into floats.
        return df.astype({"department_id": "Int64", "namespace_count": "Int64"})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)
