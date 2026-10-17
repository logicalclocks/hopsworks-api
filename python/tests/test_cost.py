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

from hopsworks_common.cost import CostBucket, CostCoverage, CostRow, NamespaceCosts


class TestNamespaceCosts:
    def test_from_response_json(self, backend_fixtures):
        json = backend_fixtures["cost"]["get_namespace_costs"]["response"]

        costs = NamespaceCosts.from_response_json(json)

        assert costs.window == "WEEK"
        assert costs.start == "2026-10-09T09:00:00Z"
        assert costs.end == "2026-10-16T09:00:00Z"
        assert costs.currency == "USD"
        assert costs.total_cost == 41.2
        assert costs.idle_cost == 5.1
        assert costs.gpu_workload_cost == 30.0
        assert costs.cpu_workload_cost == 6.1
        assert costs.efficiency == 0.42
        assert costs.coverage == CostCoverage(
            observed_buckets=1,
            total_buckets=2,
            first_observed="2026-10-09T00:00:00Z",
        )
        fraud = costs.items[0]
        assert fraud == CostRow(
            namespace="fraud",
            kind="NAMESPACE",
            department_id=7,
            department_name="risk",
            cpu_cost=3.1,
            gpu_cost=0,
            ram_cost=1.2,
            pv_cost=0.4,
            network_cost=0,
            other_cost=0,
            total_cost=4.7,
            gpu_workload_cost=1.5,
            cpu_workload_cost=3.2,
            cpu_efficiency=0.31,
            ram_efficiency=0.55,
            total_efficiency=0.37,
            cpu_core_hours_requested=10.0,
            cpu_core_hours_used=3.1,
            ram_byte_hours_requested=2000.0,
            ram_byte_hours_used=1100.0,
            cpu_no_requests=False,
            ram_no_requests=False,
            share=0.1141,
        )

    def test_from_response_json_departments(self, backend_fixtures):
        json = backend_fixtures["cost"]["get_department_costs"]["response"]

        costs = NamespaceCosts.from_response_json(json)

        risk, unassigned, idle = costs.items
        assert (risk.kind, risk.namespace) == ("DEPARTMENT", None)
        assert (risk.department_id, risk.department_name) == (7, "risk")
        assert risk.namespace_count == 2
        assert risk.gpu_workload_cost + risk.cpu_workload_cost == risk.total_cost
        assert unassigned.kind == "UNASSIGNED"
        assert unassigned.department_id is None
        assert unassigned.department_name is None
        assert unassigned.namespace_count == 3
        assert idle.namespace == "__idle__"
        assert idle.namespace_count is None
        assert costs.buckets[0].items[0].department_name == "risk"
        assert costs.buckets[0].items[0].namespace_count == 2

    def test_from_response_json_null_cost_is_zero_and_unknown_keys_dropped(
        self, backend_fixtures
    ):
        json = backend_fixtures["cost"]["get_namespace_costs"]["response"]

        idle = NamespaceCosts.from_response_json(json).items[1]

        assert idle.kind == "IDLE"
        assert idle.pv_cost == 0.0
        assert idle.total_efficiency is None
        assert idle.share is None
        assert idle.department_name is None
        assert idle.namespace_count is None

    def test_from_response_json_buckets(self, backend_fixtures):
        json = backend_fixtures["cost"]["get_namespace_costs"]["response"]

        buckets = NamespaceCosts.from_response_json(json).buckets

        assert len(buckets) == 2
        assert buckets[0].start == "2026-10-09T00:00:00Z"
        assert buckets[0].observed is True
        assert buckets[0].partial is False
        assert buckets[1].observed is False
        assert buckets[1].partial is True
        assert buckets[0].items == [
            CostRow(
                namespace="fraud",
                kind="NAMESPACE",
                total_cost=0.6,
                gpu_workload_cost=0.4,
                cpu_workload_cost=0.2,
            )
        ]
        assert buckets[1].items == []

    def test_from_response_json_empty(self):
        assert NamespaceCosts.from_response_json(None) is None
        assert NamespaceCosts.from_response_json({}) is None

    def test_from_response_json_without_totals(self):
        costs = NamespaceCosts.from_response_json({"window": "HOUR"})

        assert costs.total_cost == 0.0
        assert costs.gpu_workload_cost == 0.0
        assert costs.cpu_workload_cost == 0.0
        assert costs.efficiency is None
        assert costs.coverage == CostCoverage()
        assert costs.items == []
        assert costs.buckets == []

    def test_to_pandas(self, backend_fixtures):
        json = backend_fixtures["cost"]["get_namespace_costs"]["response"]

        df = NamespaceCosts.from_response_json(json).to_pandas()

        assert list(df["namespace"]) == ["fraud", "__idle__"]
        assert list(df.columns) == list(CostRow.__dataclass_fields__)
        assert {"share", "cpu_no_requests", "cpu_core_hours_used"} <= set(df.columns)
        assert df["total_cost"].sum() == 4.7 + 5.1
        assert list(df["gpu_workload_cost"]) == [1.5, 0]
        assert list(df["cpu_workload_cost"]) == [3.2, 0]

    def test_to_pandas_empty_keeps_columns(self):
        df = NamespaceCosts(window="HOUR").to_pandas()

        assert df.empty
        assert list(df.columns) == list(CostRow.__dataclass_fields__)


class TestCostBucket:
    def test_from_response_json(self):
        bucket = CostBucket.from_response_json(
            {
                "start": "a",
                "end": "b",
                "items": [{"namespace": "n", "kind": "NAMESPACE", "totalCost": 1.5}],
            }
        )

        assert bucket.items[0].total_cost == 1.5
        assert bucket.items[0].cpu_cost == 0.0
        assert bucket.observed is False
        assert bucket.partial is False


class TestCostCoverage:
    def test_from_response_json(self):
        coverage = CostCoverage.from_response_json(
            {"observedBuckets": 0, "totalBuckets": 7, "firstObserved": None}
        )

        assert coverage == CostCoverage(observed_buckets=0, total_buckets=7)

    def test_from_response_json_empty(self):
        assert CostCoverage.from_response_json(None) is None


class TestCostRow:
    def test_from_response_json(self):
        row = CostRow.from_response_json(
            {
                "namespace": "n",
                "kind": "UNMOUNTED",
                "pvCost": 2.0,
                "totalCost": None,
                "gpuWorkloadCost": None,
            }
        )

        assert row.pv_cost == 2.0
        assert row.total_cost == 0.0
        assert row.gpu_workload_cost == 0.0
        assert row.cpu_workload_cost == 0.0
        assert row.share is None
        assert row.cpu_no_requests is False

    def test_from_response_json_no_requests(self):
        row = CostRow.from_response_json(
            {
                "namespace": "n",
                "kind": "NAMESPACE",
                "cpuCoreHoursUsed": 2.0,
                "cpuNoRequests": True,
                "ramNoRequests": True,
                "share": -0.01,
            }
        )

        assert row.cpu_core_hours_used == 2.0
        assert row.cpu_core_hours_requested == 0.0
        assert row.cpu_no_requests is True
        assert row.ram_no_requests is True
        assert row.share == -0.01
