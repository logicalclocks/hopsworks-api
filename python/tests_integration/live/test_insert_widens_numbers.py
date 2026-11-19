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
r"""Live check: the Python engine inserts int32 and float32 columns into bigint and double features.

Not part of the unit suite (`pytest tests`). Run from the python directory:

    HOPSWORKS_LIVE_HOST=<host or url> HOPSWORKS_LIVE_KEY_FILE=<file holding the API key> \
        HOPSWORKS_LIVE_PROJECT=<project name> HOPSWORKS_LIVE_PROJECT_ID=<project id> \
        pytest tests_integration/live/test_insert_widens_numbers.py

The key's user must be allowed to create feature groups in the project. The test refuses a name that
already exists, creates one online-enabled cached feature group named hopsable_test_<time>, deletes it
by id and polls until it answers 404, and fails naming it when it could not. A missing environment
variable or an unreachable cluster fails the run: there is no skip.
"""

import os
import time
import urllib.parse

import hopsworks
import numpy as np
import pandas as pd
import pytest
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError
from hsfs.core.feature_group_api import FeatureGroupApi
from hsfs.feature import Feature


REQUEST_TIMEOUT_SECONDS = 120
CLEANUP_BUDGET_SECONDS = 300
ONLINE_BUDGET_SECONDS = 300


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


def _exists(fs, name):
    try:
        return bool(fs.get_feature_groups(name))
    except RestAPIError as err:
        if err.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND:
            return False
        raise


@pytest.fixture
def feature_group(project):
    fs = project.get_feature_store()
    name = f"hopsable_test_{int(time.time() * 1000)}"
    assert not _exists(fs, name), f"feature group {name} already exists"
    fg = fs.create_feature_group(
        name,
        version=1,
        primary_key=["customer_id"],
        online_enabled=True,
        time_travel_format="DELTA",
        features=[
            Feature("customer_id", type="bigint"),
            Feature("score", type="double"),
        ],
    )
    yield fg
    if fg.id is None:
        assert not _exists(fs, name), f"feature group {name} has no id"
        return
    path = [
        "project",
        project.id,
        "featurestores",
        fs.id,
        "featuregroups",
        fg.id,
    ]
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
            FeatureGroupApi()._delete(fg)
        except RestAPIError as err:
            if err.response.status_code != RestAPIError.STATUS_CODE_NOT_FOUND:
                raise
        assert time.monotonic() < deadline, (
            f"feature group {name} v1 (id {fg.id}) still exists"
        )
        time.sleep(2)


def _batch(ids, scores):
    # pandas aggregations over a SQL int column give int32
    return pd.DataFrame(
        {
            "customer_id": np.array(ids, dtype=np.int32),
            "score": np.array(scores, dtype=np.float32),
        }
    )


def test_int32_and_float32_insert_into_bigint_and_double(feature_group):
    # The first insert creates the table; the second appends to it.
    feature_group.insert(_batch([1, 2], [0.5, 1.5]), wait=True)
    feature_group.insert(_batch([3], [2.5]), wait=True)

    declared = {f.name: f.type for f in feature_group.features}
    assert declared == {"customer_id": "bigint", "score": "double"}

    offline = feature_group.read(dataframe_type="pandas").sort_values("customer_id")
    assert str(offline["customer_id"].dtype) == "int64"
    assert str(offline["score"].dtype) == "float64"
    assert offline["customer_id"].tolist() == [1, 2, 3]
    assert offline["score"].tolist() == [0.5, 1.5, 2.5]

    deadline = time.monotonic() + ONLINE_BUDGET_SECONDS
    while True:
        online = feature_group.read(online=True, dataframe_type="pandas")
        if len(online) == 3:
            break
        assert time.monotonic() < deadline, (
            f"online rows after {ONLINE_BUDGET_SECONDS}s: {online}"
        )
        time.sleep(5)
    online = online.sort_values("customer_id")
    assert online["customer_id"].tolist() == [1, 2, 3]
    assert online["score"].tolist() == [0.5, 1.5, 2.5]
