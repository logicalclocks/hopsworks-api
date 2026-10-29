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

from typing import Literal, get_args

from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common.core import project_api
from hopsworks_common.cost import NamespaceCosts


_WINDOW_ARG = Literal["hour", "day", "week", "month", "year"]
_WINDOWS = get_args(_WINDOW_ARG)


def _normalize_window(window: str) -> str:
    """Match a window case-insensitively and return it in the backend's upper-case form."""
    if isinstance(window, str) and window.casefold() in _WINDOWS:
        return window.upper()
    raise ValueError(f"Window must be one of the following: {_WINDOWS}.")


@public("hopsworks.core.costs_api.CostsApi")
class CostsApi:
    """API for inspecting the Kubernetes cost of each namespace, or each department, in a Hopsworks cluster.

    The cluster-wide views, [`get_namespace_costs`][hopsworks_common.core.costs_api.CostsApi.get_namespace_costs] and [`get_department_costs`][hopsworks_common.core.costs_api.CostsApi.get_department_costs], are admin-only: the calling account must hold the `HOPS_ADMIN` platform role, and an API key needs the `ADMIN` scope, which keys created by `hops setup` do not have.
    [`get_project_costs`][hopsworks_common.core.costs_api.CostsApi.get_project_costs] is for a project's data owners and shows that project alone.
    The cost is computed by OpenCost, which must be installed with the cluster.

    Use [`hopsworks.get_costs_api`][hopsworks.get_costs_api] to get an instance of this class.
    """

    @public
    def get_namespace_costs(self, window: _WINDOW_ARG = "week") -> NamespaceCosts:
        """Get the cost of each Kubernetes namespace over a recent time window.

        Each Hopsworks project runs in its own namespace, so this is the cost per project, plus the cluster's idle capacity and unmounted volumes.
        Hour and day are computed live by OpenCost.
        Week, month and year combine the daily history Hopsworks records with live OpenCost data for the days not recorded yet, so a stored day is not recomputed and the window covers only the days since recording started or still within OpenCost's retention.
        The returned `coverage` says how many of the window's buckets have data, and a bucket without data is unknown cost rather than zero.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            costs = hopsworks.get_costs_api().get_namespace_costs("month")

            print(costs.total_cost, costs.currency)
            for row in costs.items:
                print(row.namespace, row.total_cost)
            ```

        Parameters:
            window: Time window ending now, one of `hour`, `day`, `week`, `month`, `year`, matched regardless of case.

        Returns:
            The per-namespace costs, totals and per-bucket breakdown for the window.

        Raises:
            ValueError: If `window` is not one of the supported values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a platform admin or the API key lacks the `ADMIN` scope (HTTP 403, error code 320004 for the scope), cost tracking is not enabled on the cluster (HTTP 404, error code 100083, the `enable_opencost` variable is off), or OpenCost is not reachable (HTTP 503, error code 100081, with the cause in `user_message`).
        """
        return self._get_costs("namespaces", window)

    @public
    def get_department_costs(self, window: _WINDOW_ARG = "week") -> NamespaceCosts:
        """Get the cost of each department over a recent time window.

        This is the namespace view of [`get_namespace_costs`][hopsworks_common.core.costs_api.CostsApi.get_namespace_costs] grouped by the department of each namespace's projects, with the same totals and buckets.
        Each department gets one `DEPARTMENT` row, whose `namespace_count` says how many namespaces it sums.
        Projects are counted in their current department, so a project moved to another department moves its whole history with it, and a project created without a department is in the default department.
        A namespace that is no project's, such as Hopsworks' own or `kube-system`, or that is shared by projects of different departments, is summed into one `UNASSIGNED` row.
        The `IDLE` and `UNMOUNTED` rows are the same as in the namespace view.

        Example:
            ```python
            import hopsworks

            hopsworks.login()
            costs = hopsworks.get_costs_api().get_department_costs("month")

            for row in costs.items:
                if row.kind == "DEPARTMENT":
                    print(row.department_name, row.namespace_count, row.total_cost)
            ```

        Parameters:
            window: Time window ending now, one of `hour`, `day`, `week`, `month`, `year`, matched regardless of case.

        Returns:
            The per-department costs, totals and per-bucket breakdown for the window.

        Raises:
            ValueError: If `window` is not one of the supported values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a platform admin or the API key lacks the `ADMIN` scope (HTTP 403, error code 320004 for the scope), cost tracking is not enabled on the cluster (HTTP 404, error code 100083, the `enable_opencost` variable is off), or OpenCost is not reachable (HTTP 503, error code 100081, with the cause in `user_message`).
        """
        return self._get_costs("departments", window)

    @public
    def get_project_costs(
        self, window: _WINDOW_ARG = "week", project: str | int | None = None
    ) -> NamespaceCosts:
        """Get the cost of one project over a recent time window.

        Only the project's data owners can call it.
        The result has the shape of [`get_namespace_costs`][hopsworks_common.core.costs_api.CostsApi.get_namespace_costs] with the project's own `NAMESPACE` row alone: no other project, no idle capacity, and no `share`.
        Its totals are the project's, and `coverage` is the cluster's, so a bucket with data and no row is one in which the project cost nothing.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()
            costs = hopsworks.get_costs_api().get_project_costs("month")

            print(costs.total_cost, costs.currency)
            for bucket in costs.buckets:
                if not bucket.observed:
                    print(bucket.start, "no data")
                    continue
                print(bucket.start, sum(row.total_cost for row in bucket.items))
            ```

        Parameters:
            window: Time window ending now, one of `hour`, `day`, `week`, `month`, `year`, matched regardless of case.
            project: Name or id of the project, the id when several projects share a name; the project logged in to when not given.

        Returns:
            The project's costs, totals and per-bucket breakdown for the window.

        Raises:
            ValueError: If `window` is not one of the supported values, or no project is given and none is logged in to.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a data owner of the project (HTTP 403), cost tracking is not enabled on the cluster (HTTP 404, error code 100083), the project runs in a namespace shared with other projects, whose cost cannot be split between them (HTTP 409, error code 100084), or OpenCost is not reachable (HTTP 503, error code 100081).
        """
        query_params = {"window": _normalize_window(window)}
        _client = client._get_instance()
        if project is not None:
            project_id = (
                project
                if isinstance(project, int)
                else project_api.ProjectApi()._get_project(project).id
            )
        else:
            project_id = getattr(_client, "_project_id", None)
            if project_id is None:
                raise ValueError("No project given and none is logged in to.")
        return NamespaceCosts.from_response_json(
            _client._send_request(
                "GET", ["project", project_id, "costs"], query_params=query_params
            )
        )

    def _get_costs(self, view: str, window: str) -> NamespaceCosts:
        query_params = {"window": _normalize_window(window)}
        _client = client._get_instance()
        path_params = ["admin", "costs", view]
        return NamespaceCosts.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )
