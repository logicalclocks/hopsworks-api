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
"""`hops fg update --online` against a real project.

Creates an offline feature group, turns it online with the command, reads it back from the online
store and deletes it. Runs only with HOPS_LIVE=1 and a cluster login in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("HOPS_LIVE") != "1", reason="needs HOPS_LIVE=1 and a cluster login"
)


def _hops(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hopsworks.cli", *args], capture_output=True, text=True
    )


def test_an_offline_group_goes_online_with_its_rows():
    import hopsworks
    import pandas as pd

    name = f"online_update_{uuid.uuid4().hex[:8]}"
    fs = hopsworks.login().get_feature_store()
    fg = fs.create_feature_group(
        name,
        version=1,
        primary_key=["id"],
        online_enabled=False,
        statistics_config=False,
        description="test_fg_update_online fixture, deleted by the test",
    )
    try:
        fg.insert(
            pd.DataFrame({"id": range(20), "v": [i * 0.5 for i in range(20)]}),
            wait=True,
        )
        enabled = _hops("fg", "update", name, "--version", "1", "--online")
        assert enabled.returncode == 0, enabled.stderr
        assert "Copied 20 offline rows online" in enabled.stdout + enabled.stderr
        again = _hops(
            "fg", "update", name, "--version", "1", "--online", "--no-backfill"
        )
        assert again.returncode == 0, again.stderr
        assert "already online" in again.stdout + again.stderr
        online = fs.get_feature_group(name, version=1).select_all().read(online=True)
        assert len(online) == 20
    finally:
        fs.get_feature_group(name, version=1).delete()
