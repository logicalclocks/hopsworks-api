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
r"""Live check: a predictor that raises in __init__ fails its start with its traceback in the error.

Not part of the unit suite (`pytest tests`). Run from the python directory:

    HOPSWORKS_LIVE_HOST=<host or url> HOPSWORKS_LIVE_KEY_FILE=<file holding the API key> \
        HOPSWORKS_LIVE_PROJECT=<project name> HOPSWORKS_LIVE_PROJECT_ID=<project id> \
        pytest tests_integration/live/test_failed_start_log_tail.py

The project must exist and the key's user must be allowed to deploy in it; login selects it by name,
so it never prompts, and the id must match. The test refuses a name that already exists,
creates one model and one deployment named hopsable_test_<time>, deletes by id exactly the model version and
the deployment it created, and polls until they answer 404. A crashing
predictor is restarted, so the start is repeated to catch a failure seen right after a restart.

A missing environment variable or an unreachable cluster fails the run: there is no skip.
"""

import contextlib
import os
import tempfile
import time
import urllib.parse

import hopsworks
import pytest
import requests
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError
from hsml.client.exceptions import ModelServingException


STARTS = 5
REQUEST_TIMEOUT_SECONDS = 120
START_TIMEOUT_SECONDS = 600
CLEANUP_BUDGET_SECONDS = 600
MARKER = "boom-hable-135"
RAISING_PREDICTOR = (
    "class Predict:\n"
    "    def __init__(self):\n"
    f"        raise RuntimeError('{MARKER}')\n"
    "\n"
    "    def predict(self, inputs):\n"
    "        return inputs\n"
)


def _required_env(name):
    value = os.environ.get(name)
    if not value:
        pytest.fail(
            f"{name} is not set: the live lane needs it, see the module docstring."
        )
    return value


@pytest.fixture(scope="module")
def project():
    host_value = _required_env("HOPSWORKS_LIVE_HOST").strip()
    host = urllib.parse.urlparse(host_value).hostname or host_value
    project_name = _required_env("HOPSWORKS_LIVE_PROJECT")
    project_id = int(_required_env("HOPSWORKS_LIVE_PROJECT_ID"))
    key_file = _required_env("HOPSWORKS_LIVE_KEY_FILE")
    send = requests.Session.send

    def bounded_send(session, request, **kwargs):
        # The SDK sends without a timeout; give every request of the module, login included, a finite one.
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = REQUEST_TIMEOUT_SECONDS
        return send(session, request, **kwargs)

    requests.Session.send = bounded_send
    try:
        selected = hopsworks.login(
            host=host,
            port=443,
            project=project_name,
            api_key_file=key_file,
            hostname_verification=False,
            engine="python",
        )
        assert selected.id == project_id, (
            f"project {project_name} is id {selected.id}, not {project_id}"
        )
        yield selected
    finally:
        try:
            hopsworks.logout()
        finally:
            requests.Session.send = send


@pytest.fixture
def raising_deployment(project):
    name = f"hopsable_test_{int(time.time() * 1000)}"
    registry = project.get_model_registry()
    serving = project.get_model_serving()
    assert serving.get_deployment(name) is None, f"deployment {name} already exists"
    assert not registry.get_models(name), f"model {name} already exists"
    created = {}
    with tempfile.TemporaryDirectory(prefix="hable135-") as work:
        model_dir = os.path.join(work, "model")
        os.makedirs(model_dir)
        with open(os.path.join(model_dir, "weights.txt"), "w") as f:
            f.write("1\n")
        script = os.path.join(work, "predict_raises.py")
        with open(script, "w") as f:
            f.write(RAISING_PREDICTOR)
        try:
            model = registry.python.create_model(name)
            model.save(model_dir)
            created["model"] = f"{model.name}_{model.version}"
            deployment = model.deploy(name=name, script_file=script)
            created["deployment"] = deployment.id
            yield deployment
        finally:
            _delete_created(project, created)


def _delete_created(project, created):
    """Delete the deployment id and model version this test created, then poll until both answer 404, within one budget."""
    deadline = time.monotonic() + CLEANUP_BUDGET_SECONDS
    _client = client._get_instance()

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            pytest.fail(f"cleanup of {created} exceeded {CLEANUP_BUDGET_SECONDS}s")
        return min(REQUEST_TIMEOUT_SECONDS, left)

    def get(path):
        try:
            return _client._send_request("GET", path, timeout=remaining())
        except RestAPIError as err:
            if err.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND:
                return None
            raise

    if "deployment" in created:
        path = ["project", project.id, "serving", created["deployment"]]
        # An already stopped or failed deployment refuses the stop; the delete follows either way.
        with contextlib.suppress(RestAPIError):
            _client._send_request("POST", path, {"action": "stop"}, timeout=remaining())
        _client._send_request("DELETE", path, timeout=remaining())
        while get(path):
            time.sleep(min(2, remaining()))
    if "model" in created:
        path = [
            "project",
            project.id,
            "modelregistries",
            project.id,
            "models",
            created["model"],
        ]
        _client._send_request("DELETE", path, timeout=remaining())
        while get(path):
            time.sleep(min(2, remaining()))


def test_each_failed_start_carries_the_predictor_traceback(raising_deployment):
    missing = []
    for attempt in range(1, STARTS + 1):
        with pytest.raises(ModelServingException) as failed:
            raising_deployment.start(await_running=START_TIMEOUT_SECONDS)
        if MARKER not in str(failed.value):
            missing.append((attempt, str(failed.value)[-300:]))
        raising_deployment.stop(await_stopped=START_TIMEOUT_SECONDS)
    assert not missing, f"starts without the traceback in the error: {missing}"
