"""Unit tests for ``hops score``: links, refusals, the generated SQL text, caps, schedule, publication.

Nothing here executes SQL. The SQL runs against the cluster's Trino in tests/live/test_score.py.
"""

from __future__ import annotations

import json
import types

import click
import pandas as pd
import pytest
import requests
from hopsworks.cli import waits
from hopsworks.cli.commands import score
from hopsworks.cli.commands.score import GroupInfo
from hopsworks.score import kumo_job
from hopsworks.score.kumo_job import Link, ScoreRefused
from hopsworks_common import client, util
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.job import Job


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
    link = p.links[0]
    assert score.unique_sql(p, link) == (
        'SELECT (SELECT count(*) FROM "contacts_1"), '
        '(SELECT count(*) FROM (SELECT DISTINCT "id" FROM "contacts_1"))'
    )
    with pytest.raises(
        ScoreRefused, match=r"contacts.id is not unique, 4 rows hold 3 distinct"
    ):
        score.check_counts(link, 4, 3)
    score.check_counts(link, 3, 3)


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


def _accounts(crm):
    crm["contacts"].columns["account_id"] = "bigint"
    crm["accounts"] = info("accounts", {"id": "bigint", "plan": "string"}, pk=["id"])


def test_context_rows_above_the_measured_size_refuse(crm):
    p = plan(crm, [], context_rows=2000)
    with pytest.raises(ScoreRefused, match="over 1000"):
        kumo_job.check_caps(p)


def test_reach_sql_counts_only_rows_reachable_from_the_selected_rows(crm):
    p = plan(
        crm,
        ["deals.contact_id=contacts.id", "activities.deal_id=deals.id"],
        cutoff="deals.closed_at",
        context_rows=2,
    )
    sql = kumo_job.reach_sql(p)
    # The selection: the query rows and the context rows, bounded as the reads are.
    assert (
        'WITH sel AS ((SELECT "contact_id", "id" FROM "deals_1" WHERE "won" IS NULL'
        in sql
    )
    assert 'LIMIT 10000) UNION (SELECT "contact_id", "id" FROM "deals_1" WHERE' in sql
    assert "LIMIT 2))" in sql
    # Each related group is filtered by the keys of the group it is reached from.
    assert (
        'r0 AS (SELECT "id", "region", "created_at" FROM "contacts_1" t '
        'WHERE EXISTS (SELECT 1 FROM sel s WHERE s."contact_id" = t."id"))'
    ) in sql
    assert 'FROM "activities_1" t WHERE EXISTS' in sql
    assert 's."id" = t."deal_id"' in sql
    # No count runs over a whole related table.
    assert 'FROM "contacts_1")' not in sql and 'FROM "activities_1")' not in sql
    assert sql.count("UNION ALL") == 1


def test_reach_sql_follows_two_hops_from_the_previous_group(crm):
    _accounts(crm)
    p = plan(crm, ["deals.contact_id=contacts.id", "contacts.account_id=accounts.id"])
    sql = kumo_job.reach_sql(p)
    assert 'FROM sel s WHERE s."contact_id" = t."id"' in sql
    assert 'FROM r0 s WHERE s."account_id" = t."id"' in sql


def test_reach_sql_matches_composite_keys_on_every_column(crm):
    crm["deals"].columns["region"] = "string"
    crm["contacts"].columns["region"] = "string"
    p = plan(crm, ["deals.contact_id+deals.region=contacts.id+contacts.region"])
    sql = kumo_job.reach_sql(p)
    assert 's."contact_id" = t."id" AND s."region" = t."region"' in sql


def test_reach_sql_is_none_without_related_groups(crm):
    assert kumo_job.reach_sql(plan(crm, [])) is None


def test_a_third_hop_is_refused_by_the_reach_chain(crm):
    _accounts(crm)
    crm["accounts"].columns["org_id"] = "bigint"
    crm["orgs"] = info("orgs", {"id": "bigint"}, pk=["id"])
    p = kumo_job.Plan(
        "pipe",
        "deals",
        ["id"],
        "won",
        {g: kumo_job.Group(1, [["id", "bigint"]]) for g in crm if g != "activities"},
        [
            Link("deals", ["contact_id"], "contacts", ["id"]),
            Link("contacts", ["account_id"], "accounts", ["id"]),
            Link("accounts", ["org_id"], "orgs", ["id"]),
        ],
    )
    with pytest.raises(ScoreRefused, match="more than 2 hops"):
        kumo_job.reach_sql(p)


def test_entity_over_the_byte_cap_refuses(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "ENTITY_BYTES_MAX", 50)
    p = plan(crm, ["deals.contact_id=contacts.id"])
    with pytest.raises(
        ScoreRefused,
        match=r"deals: 5 rows, 51 bytes after projection, over the cap of 50",
    ):
        kumo_job.check_admission(p, (5, 51), {})
    kumo_job.check_admission(p, (5, 50), {})


def test_related_rows_over_the_cap_refuse_on_the_reachable_count(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "RELATED_ROWS_MAX", 10)
    p = plan(crm, ["deals.contact_id=contacts.id", "activities.deal_id=deals.id"])
    with pytest.raises(
        ScoreRefused, match=r"reachable .*: 11 \(contacts 4, activities 7\)"
    ):
        kumo_job.check_admission(
            p, (1, 1), {"contacts": (4, 40), "activities": (7, 70)}
        )
    kumo_job.check_admission(p, (1, 1), {"contacts": (4, 40), "activities": (6, 60)})


def test_related_bytes_over_the_cap_refuse_on_the_reachable_count(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "RELATED_BYTES_MAX", 100)
    p = plan(crm, ["deals.contact_id=contacts.id"])
    with pytest.raises(ScoreRefused, match=r"101 bytes after projection"):
        kumo_job.check_admission(p, (1, 1), {"contacts": (2, 101)})
    kumo_job.check_admission(p, (1, 1), {"contacts": (2, 100)})


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


# region: one run per system, kept by the server


def _response(status, body):
    response = requests.Response()
    response.status_code = status
    response.reason = "Conflict" if status == 409 else "Error"
    response._content = json.dumps(body).encode()
    return response


class _Job:
    """A job whose run the server refuses, with a real response."""

    def __init__(self, response):
        self.response = response
        self.config = {score.MAX_CONCURRENT: 1}

    def run(self, **_):
        raise RestAPIError("http://hopsworks/jobs/executions", self.response)


def test_the_servers_409_on_the_execution_start_is_a_clear_refusal():
    body = {
        "errorCode": 130010,
        "errorMsg": "Job kumo-score-deals has execution #7 running.",
        "usrMsg": "Execution #7 is running.",
    }
    with pytest.raises(click.ClickException) as raised:
        score._run_and_report(None, _Job(_response(409, body)), "deals", 20, 60, None)
    message = raised.value.message
    assert (
        "job kumo-score-deals is already running: Execution #7 is running." in message
    )
    assert "hops job stop kumo-score-deals" in message


def test_the_409_message_falls_back_to_the_error_message():
    err = RestAPIError(
        "http://hopsworks/jobs/executions",
        _response(409, {"errorMsg": "Execution #9 is running."}),
    )
    assert "Execution #9 is running." in str(score.run_refusal(err, "deals"))
    assert score.run_refusal(click.ClickException("x"), "deals") is None


def test_other_failures_of_the_execution_start_pass_through():
    with pytest.raises(click.ClickException, match="Could not run job"):
        score._run_and_report(
            None, _Job(_response(500, {"errorMsg": "boom"})), "deals", 20, 60, None
        )


class _Client:
    """The HTTP client of the SDK; records every request the CLI sends."""

    def __init__(self, states):
        self.requests = []
        self.states = list(states)
        self._project_id = 1

    def _send_request(self, method, path, **kw):
        self.requests.append((method, path))
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return {"id": 7, "state": state, "jobType": "PYTHON"}


class _StartedJob:
    config = {score.MAX_CONCURRENT: 1}
    name = "kumo-score-deals"
    job_type = "PYTHON"

    def run(self, **_):
        return types.SimpleNamespace(id=7, state="INITIALIZING")


class _Clock:
    """A clock the sleeps advance, so a deadline is reached without waiting."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def http(monkeypatch):
    fake = _Client(["RUNNING"])
    monkeypatch.setattr(client, "_get_instance", lambda: fake)
    return fake


@pytest.fixture
def clock(monkeypatch):
    fake = _Clock()
    monkeypatch.setattr(score, "time", fake)
    monkeypatch.setattr(waits, "time", fake)
    return fake


def test_the_default_run_starts_one_execution_and_makes_no_status_call(http, clock):
    score._run_and_report(None, _StartedJob(), "deals", 20, 60, None)
    assert http.requests == []
    assert clock.sleeps == []


def test_wait_stops_at_the_deadline_and_says_how_to_read_the_run(http, clock):
    with pytest.raises(click.ClickException, match="still RUNNING after 35 s") as err:
        score._run_and_report(
            None, _StartedJob(), "deals", 20, 60, None, wait=True, timeout=35
        )
    assert "hops score deals --show" in err.value.message
    assert http.requests
    assert clock.now >= 35
    assert clock.now < 35 + 12


def test_wait_polls_every_ten_to_twelve_seconds(http, clock):
    with pytest.raises(click.ClickException):
        score._run_and_report(
            None, _StartedJob(), "deals", 20, 60, None, wait=True, timeout=120
        )
    assert len(clock.sleeps) >= 5
    assert all(10 <= x <= 12 for x in clock.sleeps)
    assert len(set(clock.sleeps)) > 1


def test_the_poll_interval_stays_within_ten_to_twelve_seconds():
    assert all(10 <= score.poll_interval() <= 12 for _ in range(1000))


def test_the_sdk_carries_max_concurrent_executions_in_the_job_config():
    config = {"type": "pythonJobConfiguration", "appPath": "hdfs://p/a.py"}
    config[score.MAX_CONCURRENT] = 1
    sent = util._validate_job_conf(config, "proj")
    assert json.loads(json.dumps(sent))["maxConcurrentExecutions"] == 1
    response = {
        "id": 1,
        "name": "kumo-score-deals",
        "creationTime": 0,
        "jobType": "PYTHON",
        "creator": {},
        "config": dict(sent),
    }
    assert Job.from_response_json(response).config["maxConcurrentExecutions"] == 1


# region: the current run


def test_the_current_run_is_read_from_the_runs_table_only():
    sql = score.current_run_sql("pipe")
    assert (
        'FROM "pipe_score_runs_1" ORDER BY completed_at DESC, run_id DESC LIMIT 1'
        in sql
    )
    ranking = score.ranking_sql("pipe", "20261101T100000-ab12cd", 5)
    assert "\"pipe_scores_1\" WHERE run_id = '20261101T100000-ab12cd'" in ranking
    assert ranking.endswith("ORDER BY score DESC LIMIT 5")


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
