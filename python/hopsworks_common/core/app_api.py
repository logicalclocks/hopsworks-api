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
import logging
from typing import TYPE_CHECKING

from hopsworks_apigen import public
from hopsworks_common import client, usage, util


if TYPE_CHECKING:
    from hopsworks_common import app


@public("hopsworks.core.app_api.AppApi")
class AppApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    @public
    @usage.method_logger
    def get_apps(self) -> list[app.App]:
        """Get all apps in the project.

        Returns:
            List of App objects.
        """
        from hopsworks_common import app

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "apps"]
        headers = {"content-type": "application/json"}
        response = _client._send_request("GET", path_params, headers=headers)
        return app.App.from_response_json_list(response)

    @public
    @usage.method_logger
    def get_app(self, name: str) -> app.App | None:
        """Get an app by name.

        Parameters:
            name: Name of the app.

        Returns:
            App object, or None if not found.
        """
        apps = self.get_apps()
        for a in apps:
            if a.name == name:
                return a
        return None

    @public
    @usage.method_logger
    def create_app(
        self,
        name: str,
        app_path: str,
        environment: str = "python-app-pipeline",
        memory: int = 2048,
        cores: float = 1.0,
    ) -> app.App | None:
        """Create a new Streamlit app.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()
            apps = project.get_app_api()

            app = apps.create_app(
                "my_dashboard",
                app_path="Resources/app.py",
            )

            app.run()
            print(app.app_url)
            ```

        Parameters:
            name: Name of the app.
            app_path: Path to the Streamlit .py file in HopsFS.
            environment: Python environment name (default: "python-app-pipeline").
            memory: Memory in MB (default: 2048).
            cores: CPU cores (default: 1.0).

        Returns:
            The created App object.
        """
        _client = client.get_instance()

        if not app_path.startswith("hdfs://"):
            app_path = "hdfs://" + util.convert_to_abs(app_path, _client._project_name)

        config = {
            "type": "pythonAppJobConfiguration",
            "appName": name,
            "appPath": app_path,
            "resourceConfig": {
                "memory": memory,
                "cores": cores,
                "gpus": 0,
                "shmSize": 128,
            },
        }
        config["environmentName"] = environment

        path_params = ["project", _client._project_id, "jobs", name]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps(config)
        )

        # Return the app from the apps endpoint
        return self.get_app(name)

    def _start(self, app_name: str):
        """Start an app execution."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
        ]
        headers = {"content-type": "text/plain"}
        return _client._send_request("POST", path_params, headers=headers)

    def _stop(self, app_name: str, execution_id: int):
        """Stop an app execution."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
            execution_id,
            "status",
        ]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps({"state": "stopped"})
        )

    def _delete(self, app_name: str):
        """Delete an app."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
        ]
        _client._send_request("DELETE", path_params)
