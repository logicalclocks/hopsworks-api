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
"""The time cutoff of `hops score` through the real structured-data-models sampler.

Needs structured-data-models at the pinned commit and pyg-lib (Linux), as the
scoring-pipeline environment ships them. Runs only with HOPS_LIVE=1.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest
from hopsworks.score import kumo_job
from hopsworks.score.kumo_job import Group, Inputs, Link, Plan


pytestmark = pytest.mark.skipif(
    os.environ.get("HOPS_LIVE") != "1", reason="needs HOPS_LIVE=1 and sdm with pyg-lib"
)


def test_an_activity_after_the_cutoff_is_not_sampled_for_the_deal():
    import sdm  # noqa: PLC0415 - only where the pinned library is installed

    plan = Plan(
        name="t",
        entity="deals",
        keys=["id"],
        target="won",
        groups={
            "deals": Group(
                1,
                [
                    ["id", "bigint"],
                    ["won", "boolean"],
                    ["closed_at", "timestamp"],
                    ["size", "double"],
                ],
            ),
            "activities": Group(
                1,
                [
                    ["id", "bigint"],
                    ["deal_id", "bigint"],
                    ["occurred_at", "timestamp"],
                    ["marker", "double"],
                ],
            ),
        },
        links=[Link("activities", ["deal_id"], "deals", ["id"])],
        cutoff="closed_at",
        time={"activities": "occurred_at"},
    )
    context = pd.DataFrame(
        {
            "id": [1, 2],
            "won": [True, False],
            "closed_at": pd.to_datetime(["2026-02-01", "2026-02-01"]),
            "size": [1.0, 2.0],
        }
    )
    query = pd.DataFrame(
        {"id": [3], "won": [None], "closed_at": [pd.NaT], "size": [3.0]}
    )
    activities = pd.DataFrame(
        {
            "id": [10, 11, 12],
            "deal_id": [1, 1, 3],
            "occurred_at": pd.to_datetime(["2026-01-15", "2026-03-01", "2026-01-20"]),
            "marker": [1.0, 2.0, 3.0],  # 2.0 happened after deal 1 closed
        }
    )
    inputs = Inputs(query=query, context=context, related={"activities": activities})
    sampler, link = kumo_job.build_relational(sdm, plan, inputs)
    started = pd.Timestamp("2026-11-04")
    task = kumo_job.task_frame(plan, context.assign(won=[True, False]), started, True)
    tt = sdm.TableTensor.from_pandas(df=task, stypes=kumo_job._stypes(plan, True))
    _, related = sampler(
        tt,
        task_link=link,
        num_neighbors=list(kumo_job.NUM_NEIGHBORS),
        task_time_column=kumo_job.CUTOFF,
    ).to("cpu")
    markers = set(related.tables["activities"].to_pandas()["marker"].tolist())
    assert 1.0 in markers
    assert 2.0 not in markers

    # Control: without the cutoff the same sampler does reach the later activity.
    _, uncut = sampler(
        tt, task_link=link, num_neighbors=list(kumo_job.NUM_NEIGHBORS)
    ).to("cpu")
    assert 2.0 in set(uncut.tables["activities"].to_pandas()["marker"].tolist())
