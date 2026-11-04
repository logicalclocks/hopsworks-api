"""Tests for ``hops score``: links, refusals, admission, reads, schedule and the current run.

SQL runs on real pandas frames in an in-memory SQLite database; no part of the code
under test is replaced.
"""

from __future__ import annotations

import hashlib
import sqlite3

import pandas as pd
import pytest
from hopsworks.cli.commands import score
from hopsworks.cli.commands.score import GroupInfo
from hopsworks.score import kumo_job
from hopsworks.score.kumo_job import Link, ScoreRefused


class SqliteSql:
    """The `rows` and `frame` interface of kumo_job.TrinoSql, over real frames."""

    def __init__(self, **frames: pd.DataFrame):
        self.conn = sqlite3.connect(":memory:")
        # The Trino functions the seeded sample order calls.
        self.conn.create_function("to_utf8", 1, lambda v: v)
        self.conn.create_function(
            "md5", 1, lambda v: hashlib.md5(str(v).encode()).hexdigest()
        )
        self.conn.create_function("concat", -1, lambda *a: "".join(map(str, a)))
        for name, frame in frames.items():
            frame.to_sql(name, self.conn, index=False)

    def rows(self, sql):
        return self.conn.execute(sql).fetchall()

    def frame(self, sql):
        return pd.read_sql_query(sql, self.conn)


def info(name, columns, pk=(), fk=(), cached=True, version=1):
    return GroupInfo(name, version, dict(columns), list(pk), list(fk), cached)


@pytest.fixture
def crm():
    return {
        "deals": info(
            "deals",
            {
                "id": "bigint",
                "contact_id": "bigint",
                "won": "boolean",
                "created_at": "timestamp",
                "closed_at": "timestamp",
            },
            pk=["id"],
            fk=["contact_id"],
        ),
        "contacts": info(
            "contacts",
            {"id": "bigint", "region": "string", "created_at": "timestamp"},
            pk=["id"],
        ),
        "activities": info(
            "activities",
            {
                "id": "bigint",
                "deal_id": "bigint",
                "occurred_at": "timestamp",
                "kind": "string",
            },
            pk=["id"],
            fk=["deal_id"],
        ),
    }


def plan(infos, links=(), **kw):
    return score.build_plan(
        "pipe", "deals", infos, [score.parse_link(x) for x in links], "won", **kw
    )


# region: links


def test_parse_link_single_and_composite():
    assert score.parse_link("deals.contact_id=contacts.id") == Link(
        "deals", ["contact_id"], "contacts", ["id"]
    )
    link = score.parse_link("a.k1+a.k2=b.k1+b.k2")
    assert (link.left, link.left_cols, link.right, link.right_cols) == (
        "a",
        ["k1", "k2"],
        "b",
        ["k1", "k2"],
    )


@pytest.mark.parametrize(
    "text",
    [
        "deals.contact_id",
        "deals=contacts.id",
        "a.k1+b.k2=c.x+c.y",
        "a.k1+a.k2=b.k1",
        "a.x=a.y",
    ],
)
def test_parse_link_refuses_malformed(text):
    with pytest.raises(ScoreRefused):
        score.parse_link(text)


def test_no_link_never_joins_on_equal_names(crm):
    # deals.id and contacts.id share a name; with contacts named by --time and no link
    # the verb refuses and lists the foreign-key candidate, not id=id.
    with pytest.raises(ScoreRefused) as err:
        plan(crm, time=["contacts.created_at"])
    msg = str(err.value)
    assert "contacts is not linked to deals" in msg
    assert "--link deals.contact_id=contacts.id" in msg
    assert "deals.id=contacts.id" not in msg


def test_no_foreign_key_names_the_columns_to_pass(crm):
    crm["deals"].foreign_key.clear()
    with pytest.raises(ScoreRefused, match="No foreign key is recorded"):
        plan(crm, time=["contacts.created_at"])


def test_shared_id_names_link_on_the_given_columns(crm):
    p = plan(crm, ["deals.contact_id=contacts.id"])
    assert [str(x) for x in p.links] == ["deals.contact_id=contacts.id"]
    assert set(p.groups) == {"deals", "contacts"}


def test_two_ways_to_join_one_pair_is_ambiguous(crm):
    crm["deals"].columns["owner_id"] = "bigint"
    with pytest.raises(ScoreRefused, match="two links join deals and contacts"):
        plan(crm, ["deals.contact_id=contacts.id", "deals.owner_id=contacts.id"])


def test_composite_key_link_is_kept_whole():
    infos = {
        "deals": info(
            "deals", {"a": "bigint", "b": "string", "won": "boolean"}, pk=["a", "b"]
        ),
        "snap": info("snap", {"a": "bigint", "b": "string", "v": "double"}),
    }
    p = plan(infos, ["deals.a+deals.b=snap.a+snap.b"])
    assert p.links[0].left_cols == ["a", "b"] and p.keys == ["a", "b"]
    sql = kumo_job.related_sql(p, "snap", ["a", "b"], [(1, "x"), (2, "y'z")])
    assert "(\"a\", \"b\") IN ((1, 'x'), (2, 'y''z'))" in sql


def test_link_checks_columns_and_types(crm):
    with pytest.raises(ScoreRefused, match="contacts has no column nope"):
        plan(crm, ["deals.contact_id=contacts.nope"])
    crm["contacts"].columns["id"] = "string"
    with pytest.raises(ScoreRefused, match="types must match"):
        plan(crm, ["deals.contact_id=contacts.id"])


def test_non_unique_right_side_refuses_with_counts(crm):
    p = plan(crm, ["deals.contact_id=contacts.id"])
    sql = SqliteSql(
        contacts_1=pd.DataFrame({"id": [1, 2, 2, 3], "region": list("abcd")})
    )
    with pytest.raises(
        ScoreRefused, match=r"contacts.id is not unique, 4 rows hold 3 distinct"
    ):
        score.check_unique(p, sql)
    ok = SqliteSql(contacts_1=pd.DataFrame({"id": [1, 2, 3], "region": list("abc")}))
    score.check_unique(p, ok)


def test_external_group_is_refused_with_the_ingest_command(crm):
    crm["contacts"].cached = False
    with pytest.raises(
        ScoreRefused, match=r"contacts is an external feature group.*hops fg ingest"
    ):
        plan(crm, ["deals.contact_id=contacts.id"])


def test_more_than_two_hops_is_refused():
    infos = {
        "deals": info(
            "deals", {"id": "bigint", "x": "bigint", "won": "boolean"}, pk=["id"]
        ),
        "a": info("a", {"id": "bigint", "x": "bigint"}),
        "b": info("b", {"id": "bigint", "x": "bigint"}),
        "c": info("c", {"id": "bigint", "x": "bigint"}),
    }
    with pytest.raises(ScoreRefused, match="3 hops"):
        plan(infos, ["deals.x=a.id", "a.x=b.id", "b.x=c.id"])


def test_time_and_cutoff_must_be_timestamps_of_the_right_group(crm):
    with pytest.raises(ScoreRefused, match="not a timestamp"):
        plan(crm, ["deals.contact_id=contacts.id"], time=["contacts.region"])
    with pytest.raises(ScoreRefused, match="--cutoff must be a column of deals"):
        plan(crm, ["deals.contact_id=contacts.id"], cutoff="contacts.created_at")
    p = plan(
        crm,
        ["deals.contact_id=contacts.id"],
        time=["deals.created_at"],
        cutoff="deals.closed_at",
    )
    assert p.time == {"deals": "created_at"} and p.cutoff == "closed_at"


# region: caps and admission


def _tables():
    deals = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "contact_id": [10, 10, 11, 12, 11],
            "won": [True, False, None, True, None],
            "created_at": pd.to_datetime(["2026-01-01"] * 5),
            "closed_at": pd.to_datetime(
                ["2026-02-01", "2026-03-01", None, "2026-04-01", None]
            ),
        }
    )
    contacts = pd.DataFrame(
        {
            "id": [10, 11, 12, 13],
            "region": list("abcd"),
            "created_at": pd.to_datetime(["2025-01-01"] * 4),
        }
    )
    activities = pd.DataFrame(
        {
            "id": range(1, 7),
            "deal_id": [1, 3, 3, 5, 99, 4],
            "occurred_at": pd.to_datetime(["2026-01-15"] * 6),
            "kind": list("xyxyxy"),
        }
    )
    return SqliteSql(deals_1=deals, contacts_1=contacts, activities_1=activities)


def test_entity_over_the_byte_cap_refuses_before_any_read(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "ENTITY_BYTES_MAX", 50)
    p = plan(crm, ["deals.contact_id=contacts.id"])
    reader = kumo_job.Reader(_tables())
    with pytest.raises(
        ScoreRefused,
        match=r"deals: 5 rows, \d+ bytes after projection, over the cap of 50",
    ):
        kumo_job.load_inputs(p, reader)
    assert reader.reads == []
    assert {g for k, g, _ in reader.calls if k == "admission"} == {"deals", "contacts"}


def test_related_caps_refuse_before_any_read(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "RELATED_ROWS_MAX", 3)
    p = plan(crm, ["deals.contact_id=contacts.id"])
    reader = kumo_job.Reader(_tables())
    with pytest.raises(ScoreRefused, match="4 rows, over the cap of 3"):
        kumo_job.load_inputs(p, reader)
    assert reader.reads == []


def test_context_rows_above_the_measured_size_refuse(crm):
    p = plan(crm, [], context_rows=2000)
    with pytest.raises(ScoreRefused, match="over 1000"):
        kumo_job.admit(p, kumo_job.Reader(_tables()))


def test_reads_are_bounded_and_filtered_by_the_selected_keys(crm):
    p = plan(
        crm,
        ["deals.contact_id=contacts.id", "activities.deal_id=deals.id"],
        time=["deals.created_at", "activities.occurred_at"],
        cutoff="deals.closed_at",
        context_rows=2,
    )
    reader = kumo_job.Reader(_tables())
    inputs = kumo_job.load_inputs(p, reader)
    assert sorted(inputs.query["id"]) == [3, 5]  # target null
    assert list(inputs.context["id"]) == [4, 2]  # the 2 most recent labels
    # contacts of the selected deals only (10, 11, 12), activities of deals 2..5 only
    assert sorted(inputs.related["contacts"]["id"]) == [10, 11, 12]
    assert sorted(inputs.related["activities"]["deal_id"]) == [3, 3, 4, 5]


def test_query_with_no_rows_reads_nothing_else(crm):
    crm_sql = _tables()
    crm_sql.conn.execute("UPDATE deals_1 SET won = 0 WHERE won IS NULL")
    p = plan(crm, ["deals.contact_id=contacts.id"])
    reader = kumo_job.Reader(crm_sql)
    inputs = kumo_job.load_inputs(p, reader)
    assert inputs.query.empty and inputs.related == {}
    assert [g for _, g, _ in reader.reads] == ["deals"]


def test_where_picks_the_query_rows_and_leaves_them_out_of_the_context(crm):
    p = plan(crm, [], where="id IN (1, 3)")
    reader = kumo_job.Reader(_tables())
    inputs = kumo_job.load_inputs(p, reader)
    assert sorted(inputs.query["id"]) == [1, 3]
    assert 1 not in set(inputs.context["id"])


def test_missing_weights_are_refused_clearly(monkeypatch):
    monkeypatch.delenv("KUMO_RELATIONAL_WEIGHTS", raising=False)
    with pytest.raises(ScoreRefused, match="KUMO_RELATIONAL_WEIGHTS is not set"):
        kumo_job.weights_path()


# region: schedule


def test_scheduled_time_is_stable_and_differs_between_names():
    a = score.resolve_schedule("auto", 119, "deals")
    assert a == score.resolve_schedule("auto", 119, "deals")
    others = {score.resolve_schedule("auto", 119, f"sys{i}") for i in range(20)}
    assert score.resolve_schedule("auto", 119, "leads") != a
    assert len(others) > 10
    assert score.resolve_schedule("auto", 120, "deals") != a


def test_explicit_cron_is_kept_as_given():
    assert score.resolve_schedule("0 30 2 * * ?", 1, "x") == "0 30 2 * * ?"
    assert score.resolve_schedule("@daily", 1, "x") == "0 0 0 * * ?"
    assert score.resolve_schedule("@hourly", 1, "x") == "0 0 * * * ?"


@pytest.mark.parametrize(
    "cron", ["0 */5 * * * ?", "0 * * * * ?", "*/10 0 * * * ?", "0 0,30 * * * ?"]
)
def test_cron_faster_than_hourly_is_refused_with_the_floor(cron):
    with pytest.raises(
        ScoreRefused, match="more often than hourly. The floor is one run per hour"
    ):
        score.resolve_schedule(cron, 1, "x")


def test_cron_that_is_not_quartz_is_refused():
    with pytest.raises(ScoreRefused, match="not a Quartz cron"):
        score.resolve_schedule("30 2 * * *", 1, "x")


# region: one run per system


class _Execution:
    def __init__(self, ident, state):
        self.id, self.state = ident, state


class _Job:
    def __init__(self, *executions):
        self._executions = list(executions)

    def get_executions(self):
        return self._executions


def test_a_running_execution_refuses_a_second_run_and_names_it():
    job = _Job(_Execution(7, "RUNNING"), _Execution(6, "FINISHED"))
    with pytest.raises(
        ScoreRefused, match=r"job kumo-score-deals has execution #7 RUNNING"
    ):
        score.refuse_if_running(job, "deals")


def test_ended_executions_allow_a_run():
    score.refuse_if_running(
        _Job(_Execution(6, "FINISHED"), _Execution(5, "FAILED")), "deals"
    )


# region: the current run


def _scores(run_id, ids, scores):
    return pd.DataFrame({"id": ids, "run_id": run_id, "score": scores})


def _runs(*rows):
    return pd.DataFrame(
        rows,
        columns=[
            "run_id",
            "started_at",
            "completed_at",
            "query_rows",
            "scored_rows",
            "model_version",
        ],
    )


def test_a_closed_deal_leaves_the_current_ranking_on_the_next_run():
    sql = SqliteSql(
        pipe_scores_1=pd.concat(
            [_scores("A", [1, 2], [0.9, 0.2]), _scores("B", [2], [0.3])]
        ),
        pipe_score_runs_1=_runs(
            ("A", "2026-11-01 10:00", "2026-11-01 10:05", 2, 2, "m"),
            ("B", "2026-11-02 10:00", "2026-11-02 10:05", 1, 1, "m"),
        ),
    )
    run, ranking = score.current_ranking(sql, "pipe")
    assert run["run_id"] == "B"
    assert list(ranking["id"]) == [2]  # deal 1 closed: no current score


def test_all_closed_gives_an_empty_run_that_is_named():
    sql = SqliteSql(
        pipe_scores_1=_scores("A", [1], [0.9]),
        pipe_score_runs_1=_runs(
            ("A", "2026-11-01 10:00", "2026-11-01 10:05", 1, 1, "m"),
            ("B", "2026-11-02 10:00", "2026-11-02 10:01", 0, 0, "m"),
        ),
    )
    run, ranking = score.current_ranking(sql, "pipe")
    assert run["run_id"] == "B" and run["scored_rows"] == 0
    assert ranking is None


def test_scores_without_a_run_row_are_never_current():
    # A run killed after its scores insert and before its run row.
    sql = SqliteSql(
        pipe_scores_1=pd.concat(
            [_scores("A", [1], [0.9]), _scores("B", [1, 2], [0.5, 0.4])]
        ),
        pipe_score_runs_1=_runs(
            ("A", "2026-11-01 10:00", "2026-11-01 10:05", 1, 1, "m")
        ),
    )
    run, ranking = score.current_ranking(sql, "pipe")
    assert run["run_id"] == "A"
    assert list(ranking["id"]) == [1]


# region: publication order


def _run_row():
    return pd.DataFrame({"run_id": ["R"]})


def test_the_run_row_is_written_only_after_the_scores_insert_returned():
    calls = []
    kumo_job.publish(
        pd.DataFrame({"id": [1]}),
        _run_row(),
        lambda df: calls.append("scores"),
        lambda df: calls.append("run"),
        lambda msg: None,
    )
    assert calls == ["scores", "run"]


def test_a_failed_scores_insert_leaves_no_run_row():
    calls = []

    def failing(df):
        raise RuntimeError("insert failed")

    with pytest.raises(RuntimeError):
        kumo_job.publish(
            pd.DataFrame({"id": [1]}),
            _run_row(),
            failing,
            lambda df: calls.append("run"),
            lambda m: None,
        )
    assert calls == []


def test_zero_query_rows_writes_only_the_run_row():
    calls = []
    kumo_job.publish(
        pd.DataFrame({"id": []}),
        _run_row(),
        lambda df: calls.append("scores"),
        lambda df: calls.append("run"),
        lambda msg: None,
    )
    assert calls == ["run"]
