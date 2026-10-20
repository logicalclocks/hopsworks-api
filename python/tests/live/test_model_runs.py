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
"""`hops model runs` against a real project holding a runs fixture.

The fixture is a model whose versions were registered as runs of sweeps (hops-train, "Runs and
sweeps"). Runs only with HOPS_LIVE=1 and a cluster login in HOPSWORKS_HOST, HOPSWORKS_API_KEY and
HOPSWORKS_PROJECT; HOPS_RUNS_FIXTURE names the model (default runs_fixture_dealwin).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("HOPS_LIVE") != "1", reason="needs HOPS_LIVE=1 and a cluster login"
)
FIXTURE = os.environ.get("HOPS_RUNS_FIXTURE", "runs_fixture_dealwin")


def _runs(*args: str) -> list[dict]:
    out = subprocess.run(
        [
            sys.executable,
            "-m",
            "hopsworks.cli",
            "model",
            "runs",
            FIXTURE,
            *args,
            "--json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def test_every_sweep_shares_one_split_and_ranks_its_runs():
    runs = _runs()
    sweeps = {r["sweep_id"] for r in runs if r.get("sweep_id")}
    assert sweeps, "the fixture holds no run.json runs"
    for sweep in sweeps:
        rows = _runs("--sweep", sweep)
        assert {r["sweep_id"] for r in rows} == {sweep}
        assert len({r["training_dataset_version"] for r in rows}) == 1
        best = [r for r in rows if r["best"]]
        assert len(best) == 1
        metric, goal = best[0]["metric"], best[0]["metric_goal"]
        scores = [r["metrics"][metric] for r in rows]
        assert best[0]["metrics"][metric] == (
            max(scores) if goal == "max" else min(scores)
        )
        assert all(
            isinstance(v, (str, int, float, bool))
            for r in rows
            for v in r["params"].values()
        )


def test_an_unknown_sweep_lists_nothing():
    assert _runs("--sweep", "no-such-sweep") == []
