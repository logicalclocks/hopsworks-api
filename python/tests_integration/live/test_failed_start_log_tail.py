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
creates one model and one deployment named hopsable_test_<time>, finds by that exact name, deletes the deployment and
model versions it created, polls until they answer 404, and fails naming any it could not delete. A crashing
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
from hsml.engine import serving_engine


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


@pytest.fixture(scope="module")
def raising_deployment(project):
    name = f"hopsable_test_{int(time.time() * 1000)}"
    registry = project.get_model_registry()
    serving = project.get_model_serving()
    assert serving.get_deployment(name) is None, f"deployment {name} already exists"
    assert not registry.get_models(name), f"model {name} already exists"
    with tempfile.TemporaryDirectory(prefix="hable135-") as work:
        model_dir = os.path.join(work, "model")
        os.makedirs(model_dir)
        with open(os.path.join(model_dir, "weights.txt"), "w") as f:
            f.write("1\n")
        script = os.path.join(work, "predict_raises.py")
        with open(script, "w") as f:
            f.write(RAISING_PREDICTOR)
        # "unknown" while a create's call may have taken effect, "created" once it returned.
        creates = {}
        try:
            model = registry.python.create_model(name)
            creates["model"] = "unknown"
            model.save(model_dir)
            creates["model"] = "created"
            creates["deployment"] = "unknown"
            deployment = model.deploy(name=name, script_file=script)
            creates["deployment"] = "created"
            yield deployment
        finally:
            _delete_named(project, name, creates)


def _delete_named(project, name, creates):
    """Delete the deployment and every model version named `name` (free before the test), deployment first.

    The name is planned before any create, so a create whose reply was lost is found by it too: one
    with an unknown outcome is looked up until it shows or the budget is spent. Each resource is
    deleted on its own (404 counts as deleted), and the test fails naming everything left.
    """
    deadline = time.monotonic() + CLEANUP_BUDGET_SECONDS
    _client = client._get_instance()

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"cleanup budget of {CLEANUP_BUDGET_SECONDS}s spent")
        return min(REQUEST_TIMEOUT_SECONDS, left)

    def send(method, path, query=None):
        """The answer, or None when the resource answers 404."""
        try:
            return _client._send_request(method, path, query, timeout=remaining())
        except RestAPIError as err:
            if err.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND:
                return None
            raise

    serving = ["project", project.id, "serving"]
    models = ["project", project.id, "modelregistries", project.id, "models"]

    def find(kind):
        if kind == "deployment":
            found = send("GET", serving, {"name": name})
            found = found if isinstance(found, list) else [found] if found else []
            return [serving + [d["id"]] for d in found if d.get("name") == name]
        found = send("GET", models, {"filter_by": f"name_eq:{name}"}) or {}
        return [
            models + [f"{m['name']}_{m['version']}"]
            for m in found.get("items", [])
            if m.get("name") == name
        ]

    left = []
    for kind in ("deployment", "model"):
        if kind not in creates:
            continue
        paths, why = None, None
        while True:
            try:
                paths = find(kind)
                if paths or creates[kind] == "created":
                    break
                why = "not found: its create's outcome is unknown"
            except Exception as err:
                why = repr(err)
            try:
                time.sleep(min(2, remaining()))
            except TimeoutError:
                break
        if not paths:
            if paths is None or creates[kind] == "unknown":
                left.append((kind, name, why))
            continue
        for path in paths:
            try:
                if kind == "deployment":
                    # An already stopped or failed deployment refuses the stop; the delete follows either way.
                    with contextlib.suppress(RestAPIError):
                        send("POST", path, {"action": "stop"})
                send("DELETE", path)
                while send("GET", path) is not None:
                    time.sleep(min(2, remaining()))
            except Exception as err:
                left.append((kind, path[-1], repr(err)))
    if left:
        pytest.fail(f"cleanup left these resources: {left}")


def test_each_failed_start_carries_the_predictor_traceback(raising_deployment):
    missing = []
    for attempt in range(1, STARTS + 1):
        with pytest.raises(ModelServingException) as failed:
            raising_deployment.start(await_running=START_TIMEOUT_SECONDS)
        if MARKER not in str(failed.value):
            missing.append((attempt, str(failed.value)[-300:]))
        raising_deployment.stop(await_stopped=START_TIMEOUT_SECONDS)
    assert not missing, f"starts without the traceback in the error: {missing}"


def test_the_log_budget_bounds_the_diagnosis(raising_deployment, monkeypatch):
    budget = 0.5
    tails = []
    tail = serving_engine._predictor_log_tail

    def timed(serving, deployment_instance):
        started = time.monotonic()
        result = tail(serving, deployment_instance)
        tails.append((time.monotonic() - started, result))
        return result

    monkeypatch.setattr(serving_engine, "_FAILED_START_LOG_SECONDS", budget)
    monkeypatch.setattr(serving_engine, "_predictor_log_tail", timed)
    with pytest.raises(ModelServingException) as failed:
        raising_deployment.start(await_running=START_TIMEOUT_SECONDS)
    raising_deployment.stop(await_stopped=START_TIMEOUT_SECONDS)

    assert len(tails) == 1
    elapsed, result = tails[0]
    # The step bounds each socket wait at 5 s, so the deadline is checked at least that often.
    assert elapsed < budget + 5, f"the diagnosis took {elapsed:.1f}s"
    assert result == "" or result.startswith("\nLast predictor log lines (")
    assert str(failed.value).endswith(result)
