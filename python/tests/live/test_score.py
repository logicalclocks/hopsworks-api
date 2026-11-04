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
"""`hops score` against a real project with the scoring-pipeline environment.

Makes feature groups of deals and contacts, then checks: a first run, a deal scored on run A
and closed before run B (absent from the current ranking), the all-closed run (the current run
is empty and named), and `--delete` leaving no job, schedule or scores group. Not covered here:
the CRM path with `hops fg ingest` and the external group refusal, which need data sources. Runs
only with HOPS_LIVE=1 and a cluster login in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT; the project needs the scoring-pipeline environment.
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


def _out(done: subprocess.CompletedProcess) -> str:
    return done.stdout + done.stderr


def test_scores_current_run_and_delete():
    import hopsworks
    import numpy as np
    import pandas as pd

    tag = uuid.uuid4().hex[:6]
    name, deals, contacts = f"sc{tag}", f"deals_{tag}", f"contacts_{tag}"
    fs = hopsworks.login().get_feature_store()
    rng = np.random.default_rng(0)
    n = 300
    closed = pd.Timestamp("2026-06-01") + pd.to_timedelta(
        rng.integers(0, 120, n), unit="D"
    )
    frame = pd.DataFrame(
        {
            "id": np.arange(n),
            "contact_id": np.arange(n) % 50,
            "amount": rng.uniform(1, 100, n),
            "won": rng.integers(0, 2, n).astype(object),
            "closed_at": closed,
        }
    )
    frame.loc[:4, ["won", "closed_at"]] = None  # five open deals
    created = []
    try:
        for fg_name, df, pk in (
            (deals, frame, ["id"]),
            (
                contacts,
                pd.DataFrame({"id": np.arange(50), "region": list("abcde") * 10}),
                ["id"],
            ),
        ):
            fg = fs.create_feature_group(
                fg_name, version=1, primary_key=pk, statistics_config=False
            )
            fg.insert(df, wait=True)
            created.append(fg)
        args = (
            "score",
            name,
            "--entity",
            deals,
            "--target",
            "won",
            "--positive",
            "1",
            "--link",
            f"{deals}.contact_id={contacts}.id",
            "--cutoff",
            f"{deals}.closed_at",
            "--time",
            f"{deals}.closed_at",
        )
        first = _hops(*args)
        assert first.returncode == 0, _out(first)
        shown = _hops("score", name, "--show")
        assert "5 of 5 query rows scored" in _out(shown) or "scored_rows" in _out(shown)

        # Close deal 0 and score again: it has no current score after run B.
        fg = fs.get_feature_group(deals, version=1)
        row = frame.iloc[[0]].copy()
        row["won"], row["closed_at"] = 1, pd.Timestamp("2026-10-01")
        fg.insert(row, wait=True)
        second = _hops("score", name)
        assert second.returncode == 0, _out(second)
        ranking = _hops("score", name, "--show", "--top", "50", "--json")
        assert '"id": 0,' not in _out(ranking)

        # Close the rest: the current run scored none and says so.
        rest = frame.iloc[1:5].copy()
        rest["won"], rest["closed_at"] = 0, pd.Timestamp("2026-10-02")
        fg.insert(rest, wait=True)
        empty = _hops("score", name)
        assert empty.returncode == 0, _out(empty)
        assert "scored none" in _out(_hops("score", name, "--show"))

        deleted = _hops("score", name, "--delete", "--yes")
        assert deleted.returncode == 0, _out(deleted)
        assert fs.get_feature_group(f"{name}_scores", version=1) is None
        assert fs.get_feature_group(f"{name}_score_runs", version=1) is None
        assert "not found" in _out(_hops("job", "info", f"{name}-score")).lower()
    finally:
        _hops("score", name, "--delete", "--yes")
        for fg in created:
            fg.delete()
