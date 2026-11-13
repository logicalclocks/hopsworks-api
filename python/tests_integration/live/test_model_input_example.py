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
r"""Live check: a model saves an input example holding Decimal and date values, as a SQL source reads them.

Not part of the unit suite (`pytest tests`). Run from the python directory:

    HOPSWORKS_LIVE_HOST=<host or url> HOPSWORKS_LIVE_KEY_FILE=<file holding the API key> \
        HOPSWORKS_LIVE_PROJECT=<project name> HOPSWORKS_LIVE_PROJECT_ID=<project id> \
        pytest tests_integration/live/test_model_input_example.py

The key's user must be allowed to write models in the project. The test refuses a name that already
exists, saves one model version named hopsabletestexample<time>, deletes it and polls until it
answers 404, and fails naming it when it could not. A missing environment variable or an unreachable
cluster fails the run: there is no skip.
"""

import decimal
import os
import tempfile
import time
import urllib.parse
from datetime import date

import hopsworks
import pandas as pd
import pytest
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


REQUEST_TIMEOUT_SECONDS = 120
CLEANUP_BUDGET_SECONDS = 300


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
    selected = hopsworks.login(
        host=host,
        port=443,
        project=project_name,
        api_key_file=_required_env("HOPSWORKS_LIVE_KEY_FILE"),
        hostname_verification=False,
        engine="python",
    )
    try:
        assert selected.id == project_id, (
            f"project {project_name} is id {selected.id}, not {project_id}"
        )
        yield selected
    finally:
        hopsworks.logout()


@pytest.fixture
def model_name(project):
    name = f"hopsabletestexample{int(time.time() * 1000)}"
    registry = project.get_model_registry()
    assert not registry.get_models(name), f"model {name} already exists"
    yield name
    path = ["project", project.id, "modelregistries", project.id, "models", f"{name}_1"]
    _client = client._get_instance()

    def present():
        try:
            _client._send_request("GET", path, timeout=REQUEST_TIMEOUT_SECONDS)
        except RestAPIError as err:
            if err.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND:
                return False
            raise
        return True

    deadline = time.monotonic() + CLEANUP_BUDGET_SECONDS
    while present():
        try:
            _client._send_request("DELETE", path, timeout=REQUEST_TIMEOUT_SECONDS)
        except RestAPIError as err:
            if err.response.status_code != RestAPIError.STATUS_CODE_NOT_FOUND:
                raise
        assert time.monotonic() < deadline, f"model {name} v1 still exists"
        time.sleep(2)


def test_an_input_example_with_decimal_and_date_values_is_saved(project, model_name):
    example = pd.DataFrame(
        {
            "seats": [6],
            "monthly_spend": [decimal.Decimal("91.63")],
            "signup_date": [date(2025, 4, 12)],
        }
    )
    with tempfile.TemporaryDirectory() as model_dir:
        with open(os.path.join(model_dir, "weights.txt"), "w") as f:
            f.write("1\n")
        model = project.get_model_registry().python.create_model(
            model_name, input_example=example
        )
        model.save(model_dir)
    saved = project.get_model_registry().get_model(model_name, version=1)
    assert saved.input_example == [6, 91.63, "2025-04-12"]
