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
import time

import humps
from hopsworks_apigen import public
from hopsworks_common import client, usage, util
from hopsworks_common.client.exceptions import JobExecutionException
from hopsworks_common.core import app_api


_logger = logging.getLogger(__name__)

SERVING_POLL_INTERVAL = 3.0
SERVING_TIMEOUT = 600.0

# Terminal failure states reported by the backend (mirrors JobState.getFinalStates()).
# Image-pull and pod-scheduling failures surface as INITIALIZATION_FAILED or
# APP_MASTER_START_FAILED, not FAILED, so all must be checked to fail fast.
_FAILED_STATES = frozenset(
    {
        "FAILED",
        "KILLED",
        "FRAMEWORK_FAILURE",
        "APP_MASTER_START_FAILED",
        "INITIALIZATION_FAILED",
        "SUBMISSION_FAILED",
    }
)


@public("hopsworks.app.App")
class App:
    """Represents a Hopsworks App (Streamlit application)."""

    def __init__(
        self,
        job_id=None,
        name=None,
        state=None,
        final_status=None,
        serving=None,
        app_url=None,
        app_path=None,
        execution_id=None,
        execution_start=None,
        creator=None,
        creator_firstname=None,
        creator_lastname=None,
        environment_name=None,
        cpu_usage=None,
        memory_usage=None,
        cpu_requested=None,
        memory_requested=None,
        **kwargs,
    ):
        self._job_id = job_id
        self._name = name
        self._state = state
        self._final_status = final_status
        self._serving = serving or False
        self._app_url = app_url
        self._app_path = app_path
        self._execution_id = execution_id
        self._execution_start = execution_start
        self._creator = creator
        self._creator_firstname = creator_firstname
        self._creator_lastname = creator_lastname
        self._environment_name = environment_name
        self._cpu_usage = cpu_usage
        self._memory_usage = memory_usage
        self._cpu_requested = cpu_requested
        self._memory_requested = memory_requested

        self._app_api = app_api.AppApi()

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        return None

    @classmethod
    def from_response_json_list(cls, json_list):
        if json_list and isinstance(json_list, dict):
            json_decamelized = humps.decamelize(json_list)
            json_list = json_decamelized.get("items")
        if json_list and isinstance(json_list, list):
            return [cls.from_response_json(item) for item in json_list]
        return []

    @public
    @property
    def name(self) -> str:
        """Name of the app."""
        return self._name

    @public
    @property
    def state(self) -> str:
        """Current state of the app (RUNNING, KILLED, FAILED, STOPPED, etc.)."""
        return self._state

    @public
    @property
    def serving(self) -> bool:
        """Whether the app is serving (Streamlit health check passed)."""
        return self._serving

    @public
    @property
    def app_url(self) -> str | None:
        """URL to the Streamlit UI, or None if not serving.

        Example:
            ```python
            apps = project.get_app_api()
            app = apps.get_app("my_dashboard")
            if app.serving:
                print(app.app_url)
            ```
        """
        if self._serving and self._app_url:
            _client = client.get_instance()
            return _client._base_url.rstrip("/") + "/hopsworks-api/" + self._app_url
        return None

    @public
    @property
    def app_path(self) -> str | None:
        """Path to the Streamlit .py file in HopsFS."""
        return self._app_path

    @public
    @property
    def execution_id(self) -> int | None:
        """ID of the current/latest execution."""
        return self._execution_id

    @public
    @property
    def environment_name(self) -> str | None:
        """Python environment name."""
        return self._environment_name

    @public
    @property
    def cpu_requested(self) -> str | None:
        """Requested CPU cores."""
        return self._cpu_requested

    @public
    @property
    def memory_requested(self) -> str | None:
        """Requested memory."""
        return self._memory_requested

    @public
    @usage.method_logger
    def run(self, await_serving: bool = True) -> App:
        """Start the app.

        Example:
            ```python
            apps = project.get_app_api()
            app = apps.get_app("my_dashboard")
            app.run()
            print(app.app_url)
            ```

        Parameters:
            await_serving: If True, wait until the app is serving before returning.

        Returns:
            Self, with updated state.

        Raises:
            hopsworks.client.exceptions.JobExecutionException: If the app fails to start or the serving timeout is exceeded.
        """
        _logger.info("Starting app: %s", self._name)
        self._app_api._start(self._name)

        if await_serving:
            _logger.info("Waiting for app to become ready...")
            return self._wait_for_serving()

        return self._refresh()

    @public
    @usage.method_logger
    def stop(self) -> App:
        """Stop the app.

        Returns:
            Self, with updated state.
        """
        if not self._execution_id:
            _logger.info("App is not running.")
            return self
        _logger.info("Stopping app: %s", self._name)
        self._app_api._stop(self._name, self._execution_id)
        # Poll until the state is final
        elapsed = 0.0
        while elapsed < 30.0:
            self._refresh()
            if self._state in ("KILLED", "STOPPED", "FAILED", "FINISHED"):
                return self
            time.sleep(SERVING_POLL_INTERVAL)
            elapsed += SERVING_POLL_INTERVAL
        _logger.warning("Timed out waiting for app to stop.")
        return self._refresh()

    @public
    @usage.method_logger
    def delete(self):
        """Delete the app entirely.

        This stops the app if running and removes the job configuration.
        """
        _logger.info("Deleting app: %s", self._name)
        self._app_api._delete(self._name)

    @public
    @usage.method_logger
    def get_logs(self) -> dict[str, str]:
        """Get stdout and stderr logs for the latest app execution.

        Returns:
            Dictionary with ``stdout`` and ``stderr`` log content.

        Raises:
            hopsworks.client.exceptions.JobExecutionException: If the app has no execution.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when retrieving logs.
        """
        if not self._execution_id:
            raise JobExecutionException(
                f"Cannot get logs for app {self._name!r}: no execution is available."
            )

        stdout = self._app_api._get_log(self._name, self._execution_id, "out") or {}
        stderr = self._app_api._get_log(self._name, self._execution_id, "err") or {}

        return {
            "stdout": stdout.get("log") or "",
            "stderr": stderr.get("log") or "",
        }

    @public
    def get_url(self) -> str:
        """Get URL to view the app in Hopsworks UI.

        Returns:
            The URL to the app page in the Hopsworks UI.
        """
        _client = client.get_instance()
        return util.get_hostname_replaced_url(
            "/p/" + str(_client._project_id) + "/apps"
        )

    def _refresh(self) -> App:
        """Re-fetch app state from the backend."""
        updated = self._app_api.get_app(self._name)
        self._state = updated._state
        self._final_status = updated._final_status
        self._serving = updated._serving
        self._app_url = updated._app_url
        self._execution_id = updated._execution_id
        self._execution_start = updated._execution_start
        return self

    def _wait_for_serving(self) -> App:
        """Poll until the app reaches Serving state or fails.

        Raises:
            hopsworks.client.exceptions.JobExecutionException: If the app reaches an error state or the serving timeout is exceeded.
        """
        elapsed = 0.0
        while elapsed < SERVING_TIMEOUT:
            self._refresh()
            if self._serving:
                if self.app_url:
                    _logger.info("App is serving at:\n%s", self.app_url)
                return self
            if self._state in _FAILED_STATES:
                raise JobExecutionException(
                    f"App failed to start. State: {self._state}"
                )
            _logger.info("Waiting for app to start. Current state: %s", self._state)
            time.sleep(SERVING_POLL_INTERVAL)
            elapsed += SERVING_POLL_INTERVAL

        raise JobExecutionException(
            f"Timed out waiting for app to reach Serving state after {SERVING_TIMEOUT}s. "
            f"Current state: {self._state}"
        )

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return f"App({self._name!r}, state={self._state!r}, serving={self._serving!r})"

    def __repr__(self):
        return self.__str__()
