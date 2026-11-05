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

Makes feature groups of deals and contacts, then checks, awaiting each execution: the default
start returns before its run ends and a second start gets the 409; run A's rows; a run stopped
between its scores insert and its run row is never current; a deal closed before run B is absent
from the current ranking; the all-closed run is empty and named; `--delete` leaves no job,
schedule or scores group, also for a scheduled system. The admission and link checks run against the cluster's Trino: an
oversized entity group refused before any read, the related caps counted on the rows reachable
from the selection, a non-unique link refused. Not covered here: the CRM path with
`hops fg ingest` and the external group refusal, which need data sources. Runs only with HOPS_LIVE=1 and a cluster login in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT; the project needs the scoring-pipeline environment.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import sys
import time
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


RUN_DEADLINE = 1200
POLL_SECONDS = 10


def _started(done: subprocess.CompletedProcess) -> int:
    """The execution id the default start printed; it says started, never finished."""
    assert done.returncode == 0, _out(done)
    match = re.search(r"started: execution #(\d+)", _out(done))
    assert match, _out(done)
    assert "Read the scores once the run has ended" in _out(done)
    return int(match.group(1))


def _execution(project, job_name: str, execution_id: int):
    job = project.get_job_api().get_job(job_name)
    return next((e for e in job.get_executions() if e.id == execution_id), None)


def _await(project, job_name: str, execution_id: int, deadline: float = RUN_DEADLINE):
    """That execution once it is final, within `deadline` seconds."""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        execution = _execution(project, job_name, execution_id)
        if execution is not None and execution.success is not None:
            return execution
        time.sleep(POLL_SECONDS)
    pytest.fail(f"{job_name} execution #{execution_id} not final after {deadline} s")


def _show(name: str) -> dict:
    done = _hops("score", name, "--show", "--top", "50", "--json")
    assert done.returncode == 0, _out(done)
    return json.loads(done.stdout)


def _run_ids(sql, table: str) -> set:
    return {r[0] for r in sql.rows(f'SELECT DISTINCT run_id FROM "{table}"')}


def test_publication_current_run_kill_window_and_delete(cluster):
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(0)
    n = 300
    deals = pd.DataFrame(
        {
            "id": np.arange(n),
            "contact_id": np.arange(n) % 50,
            "amount": rng.uniform(1, 100, n),
            "won": rng.integers(0, 2, n).astype(object),
            "closed_at": pd.Timestamp("2026-06-01")
            + pd.to_timedelta(rng.integers(0, 120, n), unit="D"),
        }
    )
    deals.loc[:4, ["won", "closed_at"]] = None  # five open deals
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make(
        "contacts",
        pd.DataFrame({"id": np.arange(50), "region": list("abcde") * 10}),
        ["id"],
    )
    name = f"sc{cluster.tag}"
    job_name = f"kumo-score-{name}"
    project = cluster.project
    args = (
        "score",
        name,
        "--entity",
        d,
        "--target",
        "won",
        "--positive",
        "1",
        "--link",
        f"{d}.contact_id={c}.id",
        "--cutoff",
        f"{d}.closed_at",
        "--time",
        f"{d}.closed_at",
    )
    try:
        # The default start returns while the execution has not ended.
        started = time.monotonic()
        first = _started(_hops(*args))
        assert time.monotonic() - started < RUN_DEADLINE
        assert _execution(project, job_name, first).success is None

        # A second start while it runs: Hopsworks answers 409 and the verb says so.
        again = _hops("score", name)
        assert again.returncode != 0
        assert f"job {job_name} is already running" in _out(again)

        assert _await(project, job_name, first).success is True
        run_a = _show(name)
        assert run_a["run"]["scored_rows"] == run_a["run"]["query_rows"] == 5
        assert {r["id"] for r in run_a["scores"]} == set(range(5))
        assert all(0 <= r["score"] <= 1 for r in run_a["scores"])
        assert {r["run_id"] for r in run_a["scores"]} == {run_a["run"]["run_id"]}

        # Killed after its scores insert, before its run row: never current.
        job = project.get_job_api().get_job(job_name)
        held = job.run(
            args=f"{job.config['defaultArgs']} --hold-before-publish 900",
            await_termination=False,
        )
        scores_table, runs_table = f"{name}_scores_1", f"{name}_score_runs_1"
        end = time.monotonic() + RUN_DEADLINE
        new: set = set()
        while not new and time.monotonic() < end:
            time.sleep(POLL_SECONDS)
            new = _run_ids(cluster.sql, scores_table) - {run_a["run"]["run_id"]}
        assert len(new) == 1, "the held run wrote no scores within the deadline"
        held.stop()
        assert _await(project, job_name, held.id).success is False
        (killed,) = new
        rows = cluster.sql.rows(
            f"SELECT count(*) FROM \"{scores_table}\" WHERE run_id = '{killed}'"
        )
        assert rows[0][0] == 5
        assert killed not in _run_ids(cluster.sql, runs_table)
        assert _show(name)["run"]["run_id"] == run_a["run"]["run_id"]

        # Close deal 0 and score again: it has no current score after run B.
        fg = cluster.fs.get_feature_group(d, version=1)
        row = deals.iloc[[0]].copy()
        row["won"], row["closed_at"] = 1, pd.Timestamp("2026-10-01")
        fg.insert(row, wait=True)
        second = _started(_hops("score", name))
        assert _await(project, job_name, second).success is True
        run_b = _show(name)
        assert run_b["run"]["run_id"] not in (run_a["run"]["run_id"], killed)
        assert {r["id"] for r in run_b["scores"]} == {1, 2, 3, 4}
        assert run_b["run"]["scored_rows"] == 4

        # Close the rest: the current run scored none and says so; --wait polls to the end.
        rest = deals.iloc[1:5].copy()
        rest["won"], rest["closed_at"] = 0, pd.Timestamp("2026-10-02")
        fg.insert(rest, wait=True)
        waited = _hops("score", name, "--wait", "--timeout", str(RUN_DEADLINE))
        assert waited.returncode == 0, _out(waited)
        assert "scored none" in _out(waited)
        run_c = _show(name)
        assert run_c["run"]["run_id"] != run_b["run"]["run_id"]
        assert run_c["run"]["scored_rows"] == 0 and run_c["scores"] == []
        assert "scored none" in _out(_hops("score", name, "--show"))

        deleted = _hops("score", name, "--delete", "--yes")
        assert deleted.returncode == 0, _out(deleted)
        assert cluster.fs.get_feature_group(f"{name}_scores", version=1) is None
        assert cluster.fs.get_feature_group(f"{name}_score_runs", version=1) is None
        assert project.get_job_api().get_job(job_name) is None
    finally:
        _hops("score", name, "--delete", "--yes")


def test_show_of_a_system_that_never_ran_says_so(cluster):
    done = _hops("score", f"never{cluster.tag}", "--show")
    assert done.returncode != 0
    assert f"never{cluster.tag} has no completed run" in _out(done)
    assert "Traceback" not in _out(done)


def test_a_scheduled_system_is_torn_down_whole(cluster):
    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    name = f"sch{cluster.tag}"
    job_name = f"kumo-score-{name}"
    api = cluster.project.get_job_api()
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
            "--link",
            f"{d}.contact_id={c}.id",
            "--schedule",
            "daily",
            "--at",
            "3",
        )
        execution = _started(done)
        assert "Scheduled" in _out(done)
        schedule = api.get_job(job_name).job_schedule
        assert schedule is not None
        assert schedule.cron_expression.split()[2] == "3"
        assert _await(cluster.project, job_name, execution).success is True

        deleted = _hops("score", name, "--delete", "--yes")
        assert deleted.returncode == 0, _out(deleted)
        assert api.get_job(job_name) is None
        for group in (f"{name}_scores", f"{name}_score_runs"):
            assert cluster.fs.get_feature_group(group, version=1) is None
    finally:
        _hops("score", name, "--delete", "--yes")


class _Cluster:
    """Feature groups of one test, deleted at the end; Trino through the project's own client."""

    def __init__(self):
        import hopsworks
        from hopsworks.score import kumo_job

        self.project = hopsworks.login()
        self.fs = self.project.get_feature_store()
        self.tag = uuid.uuid4().hex[:6]
        self.groups = []
        from hopsworks.cli import session

        self.sql = kumo_job.TrinoSql(self.project, self.fs.name, session.trino_verify())

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


def test_non_ascii_text_crossing_the_byte_cap_is_refused_at_admission(
    cluster, monkeypatch
):
    import pandas as pd
    from hopsworks.score import kumo_job

    n = 10
    notes = pd.DataFrame(
        {
            "id": range(n),
            "won": [i % 2 for i in range(n)],
            "note": ["\U0001f600" * 20] * n,
        }
    )
    g = cluster.make("notes", notes, ["id"])
    plan = cluster.plan(g, [])
    cap = 500
    characters, octets = cluster.sql.rows(
        f'SELECT sum(length(note)), sum(length(to_utf8(note))) FROM "{g}_1"'
    )[0]
    assert characters < cap < octets
    monkeypatch.setattr(kumo_job, "ENTITY_BYTES_MAX", cap)
    reader = kumo_job.Reader(cluster.sql)
    with pytest.raises(
        kumo_job.ScoreRefused, match=r"bytes \(UTF-8\) after projection"
    ):
        kumo_job.load_inputs(plan, reader)
    assert reader.reads == []


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


NOT_ENFORCED = (
    "does not enforce one scoring run per system or the scoring platform class"
)


def _admission(job) -> tuple:
    return (job.config.get("maxConcurrentExecutions"), job.config.get("platformClass"))


def test_a_rerun_writes_the_admission_fields_reads_them_back_and_starts(cluster):
    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    name = f"adm{cluster.tag}"
    job_name = f"kumo-score-{name}"
    project = cluster.project
    api = project.get_job_api()
    try:
        setup = _hops(
            "score", name, "--entity", d, "--target", "won", "--positive", "1",
            "--link", f"{d}.contact_id={c}.id",
        )  # fmt: skip
        job = api.get_job(job_name)
        if NOT_ENFORCED in _out(setup):
            # A backend that drops the fields: nothing starts.
            assert setup.returncode != 0
            assert not job.get_executions()
            assert _admission(job) != (1, "scoring")
            return
        first = _started(setup)
        assert _await(project, job_name, first).success is True

        # The run limit edited away; a member may change it, not the platform class (130103).
        job = api.get_job(job_name)
        job.config.pop("maxConcurrentExecutions", None)
        job.save()
        assert _admission(api.get_job(job_name)) == (None, "scoring")

        rerun = _hops("score", name)
        stored = api.get_job(job_name)
        if NOT_ENFORCED in _out(rerun):
            assert rerun.returncode != 0
            assert _admission(stored) != (1, "scoring")
            assert [e.id for e in stored.get_executions()] == [first]
            return
        second = _started(rerun)
        assert second != first
        assert _admission(stored) == (1, "scoring")
        assert _await(project, job_name, second).success is True
    finally:
        _hops("score", name, "--delete", "--yes")


MAX_HELD_SYSTEMS = 4


def test_a_start_the_platform_refuses_says_429_and_starts_nothing(cluster):
    """Hold executions until the platform admission refuses a start, then read the CLI's words.

    Needs the hopsworks-ee platform admission on the cluster (platformClass and its per-creator
    limit). The limit's value is not assumed: held executions are added, one system each, until
    a start is refused. A backend that keeps no platformClass, or refuses nothing within
    MAX_HELD_SYSTEMS held executions, skips with that reason.
    """
    from hopsworks_common.client.exceptions import RestAPIError

    deals, contacts, _, _ = _crm_frames()
    d = cluster.make("deals", deals, ["id"])
    c = cluster.make("contacts", contacts, ["id"])
    project = cluster.project
    api = project.get_job_api()
    names = [f"cap{i}{cluster.tag}" for i in range(MAX_HELD_SYSTEMS)]
    held = []
    refused_name = None
    try:
        for i, name in enumerate(names):
            job_name = f"kumo-score-{name}"
            setup = _hops(
                "score", name, "--entity", d, "--target", "won", "--positive", "1",
                "--link", f"{d}.contact_id={c}.id",
            )  # fmt: skip
            if i == 0 and NOT_ENFORCED in _out(setup):
                pytest.skip(
                    "needs the hopsworks-ee platform admission: this backend does not "
                    f"keep platformClass ({_out(setup).strip()})"
                )
            if setup.returncode != 0:
                refused_name = name
                break
            assert _await(project, job_name, _started(setup)).success is True
            job = api.get_job(job_name)
            try:
                execution = job.run(
                    args=f"{job.config['defaultArgs']} --hold-before-publish 900",
                    await_termination=False,
                )
            except RestAPIError as err:
                assert err.response.status_code == 429, str(err)
                refused_name = name
                break
            held.append((job_name, execution))

        if refused_name is None:
            pytest.skip(
                f"the platform admission refused no start within {MAX_HELD_SYSTEMS} held "
                "executions: it is not enforced on this cluster"
            )
        assert held, (
            "the first start was refused: no held execution to read the limit from"
        )
        for job_name, execution in held:
            assert _execution(project, job_name, execution.id).success is None
        job_name = f"kumo-score-{refused_name}"
        before = [e.id for e in api.get_job(job_name).get_executions()]
        again = _hops("score", refused_name)
        assert again.returncode != 0
        assert f"No run of {job_name} started" in _out(again)
        assert "Retry in " in _out(again)
        assert [e.id for e in api.get_job(job_name).get_executions()] == before
    finally:
        for _, execution in held:
            with contextlib.suppress(Exception):
                execution.stop()
        for name in names:
            _hops("score", name, "--delete", "--yes")


def test_a_view_on_contacts_v1_reads_v1_when_v2_exists(cluster):
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
        from hopsworks.score import kumo_job

        plan = kumo_job.Plan.from_arg(args[args.index("--config") + 1])
        assert plan.groups[c].version == 1
        assert plan.groups[d].version == 1

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
