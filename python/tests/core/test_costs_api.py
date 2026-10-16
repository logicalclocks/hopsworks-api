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

from unittest.mock import MagicMock

import pytest
from hopsworks_common.core.costs_api import CostsApi


def _patch_client(mocker, send_request_return):
    client_instance = MagicMock()
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.costs_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestCostsApi:
    def test_get_namespace_costs_defaults_to_week(self, mocker, backend_fixtures):
        client_instance = _patch_client(
            mocker, backend_fixtures["cost"]["get_namespace_costs"]["response"]
        )

        costs = CostsApi().get_namespace_costs()

        client_instance._send_request.assert_called_once_with(
            "GET",
            ["admin", "costs", "namespaces"],
            query_params={"window": "WEEK"},
        )
        assert costs.total_cost == 41.2
        assert costs.gpu_workload_cost == 30.0
        assert costs.items[0].namespace == "fraud"
        assert costs.items[0].cpu_workload_cost == 3.2

    @pytest.mark.parametrize(
        ("window", "sent"),
        [
            ("hour", "HOUR"),
            ("Day", "DAY"),
            ("WEEK", "WEEK"),
            ("month", "MONTH"),
            ("year", "YEAR"),
        ],
    )
    def test_get_namespace_costs_sends_window_upper_case(self, mocker, window, sent):
        client_instance = _patch_client(mocker, {"window": sent})

        CostsApi().get_namespace_costs(window)

        assert client_instance._send_request.call_args.kwargs["query_params"] == {
            "window": sent
        }

    @pytest.mark.parametrize("window", ["fortnight", "", None, "7d"])
    def test_get_namespace_costs_rejects_unknown_window(self, mocker, window):
        client_instance = _patch_client(mocker, {})

        with pytest.raises(ValueError, match="Window must be one of"):
            CostsApi().get_namespace_costs(window)

        client_instance._send_request.assert_not_called()
