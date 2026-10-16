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
    """API for inspecting the Kubernetes cost of each namespace in a Hopsworks cluster.

    This is an admin-only capability: the calling account must hold the `HOPS_ADMIN` platform role.
    The cost is computed by OpenCost, which must be installed with the cluster.

    Use [`hopsworks.get_costs_api`][hopsworks.get_costs_api] to get an instance of this class.
    """

    @public
    def get_namespace_costs(self, window: _WINDOW_ARG = "week") -> NamespaceCosts:
        """Get the cost of each Kubernetes namespace over a recent time window.

        Each Hopsworks project runs in its own namespace, so this is the cost per project, plus the cluster's idle capacity and unmounted volumes.
        Hour, day and week are computed live by OpenCost.
        Month and year are assembled from daily history that Hopsworks records, so they only cover the days since that recording started.

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
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a platform admin or OpenCost is not reachable.
        """
        query_params = {"window": _normalize_window(window)}
        _client = client._get_instance()
        path_params = ["admin", "costs", "namespaces"]
        return NamespaceCosts.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )
