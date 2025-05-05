#
#   Copyright 2025 Hopsworks AB
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

import logging
import time
from datetime import datetime
from typing import List, Optional

from hopsworks_common import alert_receiver, alert_route, client


class AlertsEngine:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    def await_receiver(self, receiver_name, timeout: Optional[float] = 120):
        """
        Waits for a receiver to be created. This is useful when creating a new receiver
        and you want to ensure that it is available before proceeding with other operations.
        :param receiver_name: The name of the receiver to wait for.
        :type receiver_name: str
        :param timeout: The maximum time to wait for the receiver to be created, in seconds.
        :type timeout: float
        :raises TimeoutError: If the receiver is not created within the specified timeout.
        :return: The created receiver.
        :rtype: AlertReceiver
        """
        if receiver_name is None:
            raise ValueError("Receiver name cannot be None.")

        start_time = datetime.now()

        def passed():
            return (datetime.now() - start_time).total_seconds()

        receiver = self._get_receiver(receiver_name)
        self._log.info("Waiting for receiver to be created.")
        while receiver is None:
            if timeout is not None and passed() > timeout:
                raise TimeoutError(
                    f"Receiver {receiver_name} was not created in {timeout} seconds."
                )
            time.sleep(5)
            receiver = self._get_receiver(receiver_name)

        self._log.info("Receiver created.")
        return receiver

    def _get_receiver(self, name):
        _client = client.get_instance()
        # should only wait for project receivers
        if not name.startswith(f"{_client._project_name}__"):
            name = f"{_client._project_name}__{name}"
        path_params = ["project", _client._project_id, "alerts", "receivers"]
        query_params = {"expand": True}
        headers = {"content-type": "application/json"}
        receivers = alert_receiver.AlertReceiver.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )
        if receivers is not None:
            for receiver in receivers:
                if receiver.name == name:
                    return receiver
        return None

    def await_route(
        self, receiver_name: str, match: List[dict], timeout: Optional[float] = 120
    ):
        """
        Waits for a route to be created. This is useful when creating a new route
        and you want to ensure that it is available before proceeding with other operations.
        :param receiver_name: The name of the receiver to wait for.
        :match: The match criteria for the route.
        :param timeout: The maximum time to wait for the route to be created, in seconds.
        :type timeout: float
        :raises TimeoutError: If the route is not created within the specified timeout.
        :return: The created route.
        :rtype: AlertRoute
        """
        if receiver_name is None:
            raise ValueError("Receiver name cannot be None.")

        start_time = datetime.now()

        def passed():
            return (datetime.now() - start_time).total_seconds()

        route = self._get_route(receiver_name, match)
        self._log.info("Waiting for route to be created.")
        while route is None:
            if timeout is not None and passed() > timeout:
                raise TimeoutError(
                    f"Route for receiver {receiver_name} was not created in {timeout} seconds."
                )
            time.sleep(5)
            route = self._get_route(receiver_name, match)

        self._log.info("Route created.")
        return route

    def _get_route(self, name: str, match: List[dict]):
        _client = client.get_instance()
        # should only wait for project receivers
        if not name.startswith(f"{_client._project_name}__"):
            name = f"{_client._project_name}__{name}"
        path_params = ["project", _client._project_id, "alerts", "routes"]
        query_params = {"loaded": True}  # we want to get the loaded routes
        headers = {"content-type": "application/json"}
        routes = alert_route.AlertRoute.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )
        if routes is not None:
            for route in routes:
                if route._receiver == name and all(
                    item["key"] in route._match
                    and route._match[item["key"]] == item["value"]
                    for item in match
                ):
                    return route
        return None
