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
is empty and named), scores of a run with no run row never current, and `--delete` leaving no job,
schedule or scores group. The admission and link checks run against the cluster's Trino: an
oversized entity group refused before any read, the related caps counted on the rows reachable
from the selection, a non-unique link refused. Not covered here: the CRM path with
`hops fg ingest` and the external group refusal, which need data sources. Runs only with HOPS_LIVE=1 and a cluster login in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT; the project needs the scoring-pipeline environment.
"""

from __future__ import annotations

import contextlib
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

        # A run killed after its scores insert and before its run row is never current.
        scores_fg = fs.get_feature_group(f"{name}_scores", version=1)
        orphan = scores_fg.read().head(2)
        orphan["run_id"] = "orphan"
        orphan["scored_at"] = pd.Timestamp("2099-01-01")
        scores_fg.insert(orphan, wait=True)
        assert "orphan" not in _out(_hops("score", name, "--show", "--json"))

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
        assert "not found" in _out(_hops("job", "info", f"kumo-score-{name}")).lower()
    finally:
        _hops("score", name, "--delete", "--yes")
        for fg in created:
            fg.delete()


class _Cluster:
    """Feature groups of one test, deleted at the end; Trino through the project's own client."""

    def __init__(self):
        import hopsworks
        from hopsworks.score import kumo_job

        self.project = hopsworks.login()
        self.fs = self.project.get_feature_store()
        self.tag = uuid.uuid4().hex[:6]
        self.groups = []
        self.sql = kumo_job.TrinoSql(self.project, self.fs.name)

    def make(self, stem, frame, pk):
        fg = self.fs.create_feature_group(
            f"{stem}_{self.tag}", version=1, primary_key=pk, statistics_config=False
        )
        fg.insert(frame, wait=True)
        self.groups.append(fg)
        return f"{stem}_{self.tag}"

    def plan(self, entity, links, **kw):
        from hopsworks.cli.commands import score

        names = {entity, *(g for text in links for g in _groups(text))}
        infos = {n: score._info(self.fs.get_feature_group(n, version=1)) for n in names}
        return score.build_plan(
            "adm", entity, infos, [score.parse_link(t) for t in links], "won", **kw
        )

    def drop(self):
        for fg in self.groups:
            with contextlib.suppress(Exception):
                fg.delete()


def _groups(link):
    return [side.split(".")[0] for side in link.replace("+", "=").split("=")]


@pytest.fixture
def cluster():
    c = _Cluster()
    yield c
    c.drop()


def _crm_frames(n_deals=60, per_deal=3):
    import numpy as np
    import pandas as pd

    deals = pd.DataFrame(
        {
            "id": np.arange(n_deals),
            "contact_id": np.arange(n_deals) % 20,
            "won": (np.arange(n_deals) % 2).astype(object),
            "closed_at": pd.Timestamp("2026-01-01")
            + pd.to_timedelta(np.arange(n_deals), unit="D"),
        }
    )
    deals.loc[:1, ["won", "closed_at"]] = None  # two open deals
    contacts = pd.DataFrame({"id": np.arange(20), "account_id": np.arange(20) % 5})
    accounts = pd.DataFrame({"id": np.arange(5), "plan": list("abcde")})
    activities = pd.DataFrame(
        {
            "id": np.arange(n_deals * per_deal),
            "deal_id": np.repeat(np.arange(n_deals), per_deal),
            "kind": "call",
        }
    )
    return deals, contacts, accounts, activities


def test_oversized_entity_group_is_refused_before_any_read(cluster, monkeypatch):
    from hopsworks.score import kumo_job

    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    plan = cluster.plan(d, [f"{d}.contact_id={c}.id"], cutoff=f"{d}.closed_at")
    monkeypatch.setattr(kumo_job, "ENTITY_BYTES_MAX", 50)
    reader = kumo_job.Reader(cluster.sql)
    with pytest.raises(kumo_job.ScoreRefused, match="over the cap of 50 bytes"):
        kumo_job.load_inputs(plan, reader)
    assert reader.reads == []
    assert [g for kind, g, _ in reader.calls if kind == "admission"]


def test_related_caps_count_only_the_rows_reachable_from_the_selection(
    cluster, monkeypatch
):
    from hopsworks.score import kumo_job

    deals, contacts, accounts, activities = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    a = cluster.make("accounts", accounts, ["id"])
    act = cluster.make("activities", activities, ["id"])
    plan = cluster.plan(
        d,
        [
            f"{d}.contact_id={c}.id",
            f"{c}.account_id={a}.id",
            f"{act}.deal_id={d}.id",
        ],
        cutoff=f"{d}.closed_at",
        context_rows=2,
        query_rows=10,
    )
    # The selection: the 2 open deals and the 2 most recently closed.
    closed = deals[deals["won"].notna()].sort_values("closed_at", ascending=False)
    chosen = deals[deals["won"].isna()]["id"].tolist() + closed["id"].head(2).tolist()
    want_contacts = contacts[
        contacts["id"].isin(deals[deals["id"].isin(chosen)]["contact_id"])
    ]
    want = {
        c: len(want_contacts),
        a: accounts["id"].isin(want_contacts["account_id"]).sum(),
        act: activities["deal_id"].isin(chosen).sum(),
    }
    assert want[act] < len(activities)

    reader = kumo_job.Reader(cluster.sql)
    _, stats = kumo_job.admit(plan, reader)
    assert {g: s["rows"] for g, s in stats.items() if g != d} == want
    assert reader.reads == []

    # A cap between the reachable count and the whole table admits; one below it refuses.
    monkeypatch.setattr(kumo_job, "RELATED_ROWS_MAX", sum(want.values()))
    kumo_job.admit(plan, kumo_job.Reader(cluster.sql))
    monkeypatch.setattr(kumo_job, "RELATED_ROWS_MAX", sum(want.values()) - 1)
    reader = kumo_job.Reader(cluster.sql)
    with pytest.raises(kumo_job.ScoreRefused, match="reachable from the selected rows"):
        kumo_job.load_inputs(plan, reader)
    assert reader.reads == []


def test_non_unique_right_side_of_a_link_is_refused(cluster):
    import pandas as pd
    from hopsworks.cli.commands import score
    from hopsworks.score import kumo_job

    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    dup = pd.concat([contacts, contacts.head(1)], ignore_index=True).assign(
        row=lambda f: range(len(f))
    )
    c = cluster.make("contacts", dup, ["row"])
    plan = cluster.plan(d, [f"{d}.contact_id={c}.id"])
    with pytest.raises(kumo_job.ScoreRefused, match="is not unique, 21 rows hold 20"):
        score.check_unique(plan, cluster.sql)


def test_unrelated_namesakes_and_a_matching_input_survive(cluster):
    """Namesakes the system did not make are refused by setup and left by `--delete`.

    An unmarked `<name>_scores`, an unmarked `kumo-score-<name>` job, and an input named
    like an output.
    """
    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    base = (
        "--target",
        "won",
        "--positive",
        "1",
        "--link",
        f"{d}.contact_id={c}.id",
    )
    job_api = cluster.project.get_job_api()

    # An unrelated <name>_scores.
    name = f"own{cluster.tag}"
    foreign = cluster.fs.create_feature_group(
        f"{name}_scores", version=1, primary_key=["id"], statistics_config=False
    )
    foreign.insert(deals[["id"]], wait=True)
    cluster.groups.append(foreign)
    refused = _hops("score", name, "--entity", d, *base)
    assert refused.returncode != 0
    assert f"{name}_scores v1 exists without the marker" in _out(refused)
    assert job_api.get_job(f"kumo-score-{name}") is None
    _hops("score", name, "--delete", "--yes")
    assert cluster.fs.get_feature_group(f"{name}_scores", version=1) is not None

    # An unrelated kumo-score-<name> job.
    name2 = f"own2{cluster.tag}"
    dataset = cluster.project.get_dataset_api()
    script = os.path.join(os.path.dirname(__file__), "__init__.py")
    path = dataset.upload(script, "Resources", overwrite=True)
    config = job_api.get_configuration("PYTHON")
    config["appPath"] = path
    other = job_api.create_job(f"kumo-score-{name2}", config)
    try:
        refused = _hops("score", name2, "--entity", d, *base)
        assert refused.returncode != 0
        assert f"job kumo-score-{name2} exists and was not made" in _out(refused)
        rerun = _hops("score", name2)
        assert f"job kumo-score-{name2} exists and was not made" in _out(rerun)
        _hops("score", name2, "--delete", "--yes")
        kept = job_api.get_job(f"kumo-score-{name2}")
        assert kept is not None and kept.config["appPath"] == config["appPath"]
    finally:
        other.delete()

    # An input named like an output of the system.
    name3 = f"own3{cluster.tag}"
    inp = f"{name3}_scores"
    made = cluster.fs.create_feature_group(
        inp, version=1, primary_key=["id"], statistics_config=False
    )
    made.insert(deals, wait=True)
    cluster.groups.append(made)
    refused = _hops(
        "score", name3, "--entity", inp, "--target", "won", "--positive", "1"
    )
    assert refused.returncode != 0
    assert "is an input" in _out(refused)
    assert job_api.get_job(f"kumo-score-{name3}") is None
    _hops("score", name3, "--delete", "--yes")
    assert cluster.fs.get_feature_group(inp, version=1) is not None


@pytest.mark.skipif(
    os.environ.get("HOPS_EE_SCORING_CLASS") is None,
    reason="needs the hopsworks-ee per-class admission (platformClass scoring) on the "
    "cluster; HOPS_EE_SCORING_CLASS holds that class's execution ceiling",
)
def test_a_full_scoring_class_answers_429_and_nothing_starts(cluster):
    """Start one more system than the class admits: the last start is refused with 429."""
    ceiling = int(os.environ["HOPS_EE_SCORING_CLASS"])
    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    names = [f"cls{i}{cluster.tag}" for i in range(ceiling + 1)]
    try:
        outs = [
            _hops(
                "score",
                n,
                "--entity",
                d,
                "--target",
                "won",
                "--positive",
                "1",
                "--link",
                f"{d}.contact_id={c}.id",
            )
            for n in names
        ]
        assert all(o.returncode == 0 for o in outs[:-1]), [_out(o) for o in outs]
        last = outs[-1]
        assert last.returncode != 0
        assert f"No run of kumo-score-{names[-1]} started" in _out(last)
        assert "Retry in " in _out(last)
        assert (
            not cluster.project.get_job_api()
            .get_job(f"kumo-score-{names[-1]}")
            .get_executions()
        )
    finally:
        for n in names:
            _hops("score", n, "--delete", "--yes")


def test_a_view_on_contacts_v1_reads_v1_when_v2_exists(cluster):
    import json
    import shlex

    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    v1 = cluster.fs.get_feature_group(c, version=1)
    v2 = cluster.fs.create_feature_group(
        c, version=2, primary_key=["id"], statistics_config=False
    )
    v2.insert(contacts.assign(id=contacts["id"] + 1000), wait=True)
    cluster.groups.append(v2)
    fv = cluster.fs.create_feature_view(
        f"view_{cluster.tag}",
        version=1,
        query=cluster.fs.get_feature_group(d, version=1)
        .select_all()
        .join(v1.select_all(), left_on=["contact_id"], right_on=["id"], prefix="c_"),
    )
    name = f"fv{cluster.tag}"
    try:
        done = _hops(
            "score",
            name,
            "--entity",
            d,
            "--target",
            "won",
            "--positive",
            "1",
            "--fv",
            fv.name,
        )
        assert done.returncode == 0, _out(done)
        job = cluster.project.get_job_api().get_job(f"kumo-score-{name}")
        args = shlex.split(job.config["defaultArgs"])
        plan = json.loads(args[args.index("--config") + 1])
        assert plan["groups"][c]["version"] == 1
        assert plan["groups"][d]["version"] == 1

        # A --link naming contacts reads its latest, v2: refused against the view's v1.
        refused = _hops(
            "score",
            name,
            "--entity",
            d,
            "--target",
            "won",
            "--positive",
            "1",
            "--fv",
            fv.name,
            "--link",
            f"{d}.contact_id={c}.id",
        )
        assert refused.returncode != 0
        assert f"while the feature view reads {c} v1" in _out(refused)
    finally:
        _hops("score", name, "--delete", "--yes")
        fv.delete()


def test_a_group_reached_two_ways_reads_the_union_in_any_link_order(cluster):
    """A deal's own account is read even when its contact has another account.

    Links deals->contacts, contacts->accounts and deals->accounts, in two orders.
    """
    from hopsworks.score import kumo_job

    deals, contacts, accounts, _ = _crm_frames()
    deals["account_id"] = (deals["id"] + 1) % 5  # never the contact's account
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    a = cluster.make("accounts", accounts, ["id"])
    links = [
        f"{d}.contact_id={c}.id",
        f"{c}.account_id={a}.id",
        f"{d}.account_id={a}.id",
    ]
    closed = deals[deals["won"].notna()].sort_values(
        ["closed_at", "id"], ascending=[False, True]
    )
    chosen = deals[
        deals["id"].isin(
            deals[deals["won"].isna()]["id"].tolist() + closed["id"].head(2).tolist()
        )
    ]
    picked_contacts = contacts[contacts["id"].isin(chosen["contact_id"])]
    want = {
        c: set(picked_contacts["id"])
        | set(contacts[contacts["account_id"].isin(chosen["account_id"])]["id"]),
        a: set(chosen["account_id"]) | set(picked_contacts["account_id"]),
    }
    assert set(chosen["account_id"]) - set(picked_contacts["account_id"])

    for order in (links, links[::-1]):
        plan = cluster.plan(
            d, order, cutoff=f"{d}.closed_at", context_rows=2, query_rows=10
        )
        reader = kumo_job.Reader(cluster.sql)
        pinned, stats = kumo_job.admit(plan, reader)
        frames = kumo_job.read_related(pinned, reader)
        assert {g: set(f["id"]) for g, f in frames.items()} == want
        assert {g: len(f) for g, f in frames.items()} == {
            g: s["rows"] for g, s in stats.items() if g != d
        }


def test_the_reads_see_the_state_admission_pinned(cluster, monkeypatch):
    """Rows ingested between admission and the reads are not read.

    The read caps hold chunk by chunk.
    """
    from hopsworks.score import kumo_job

    deals, _, _, activities = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    act = cluster.make("activities", activities, ["id"])
    plan = cluster.plan(
        d,
        [f"{act}.deal_id={d}.id"],
        cutoff=f"{d}.closed_at",
        context_rows=2,
        query_rows=10,
    )
    reader = kumo_job.Reader(cluster.sql)
    pinned, stats = kumo_job.admit(plan, reader)
    assert set(pinned.pins) == {d, act}

    # Between admission and the reads: more activities for every deal, and a new open deal.
    extra = activities.assign(id=activities["id"] + 100_000)
    cluster.fs.get_feature_group(act, version=1).insert(extra, wait=True)
    new_deal = deals.head(1).assign(id=10_000)
    cluster.fs.get_feature_group(d, version=1).insert(new_deal, wait=True)

    inputs = kumo_job.read_inputs(pinned, reader)
    assert len(inputs.related[act]) == stats[act]["rows"]
    assert 10_000 not in set(inputs.query["id"])
    assert len(inputs.query) == 2
    # The same plan unpinned sees the new state, so the pin is what kept it out.
    latest, later = kumo_job.admit(plan, kumo_job.Reader(cluster.sql))
    assert later[act]["rows"] > stats[act]["rows"]
    assert latest.pins[act] > pinned.pins[act]

    # The related cap, lowered after admission, stops the read chunk by chunk.
    monkeypatch.setattr(kumo_job, "FETCH_ROWS", 2)
    monkeypatch.setattr(kumo_job, "RELATED_ROWS_MAX", stats[act]["rows"] - 1)
    with pytest.raises(kumo_job.ScoreRefused, match="Stopped before keeping more"):
        kumo_job.read_related(pinned, kumo_job.Reader(cluster.sql))
