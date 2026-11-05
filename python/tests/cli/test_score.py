"""Unit tests for ``hops score``: links, refusals, the generated SQL text, caps, schedule, publication.

Nothing here executes SQL or calls a service. The SQL, the job start, the 409 of a second start
and the --wait polling run against the cluster in tests/live/test_score.py.
"""

from __future__ import annotations

import json

import click
import pandas as pd
import pytest
import requests
from hopsworks.cli.commands import score
from hopsworks.cli.commands.score import GroupInfo
from hopsworks.score import kumo_job
from hopsworks.score.kumo_job import Link, ScoreRefused
from hopsworks_common import util
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
    sql = kumo_job.related_sql(p, "snap")
    assert 'WHERE s."a" = t."a" AND s."b" = t."b"' in sql


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


@pytest.mark.parametrize(
    ("time", "cutoff", "message"),
    [
        ("deals.nope", "deals.closed_at", "deals has no column nope"),
        ("deals.won", "deals.closed_at", "deals.won is boolean, not a timestamp"),
        ("deals.created_at", "deals.nope", "deals has no column nope"),
        ("deals.created_at", "deals.won", "deals.won is boolean, not a timestamp"),
    ],
)
def test_time_and_cutoff_on_different_entity_columns_are_both_checked(
    crm, time, cutoff, message
):
    with pytest.raises(ScoreRefused, match=message):
        plan(crm, [], time=[time], cutoff=cutoff)


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
        'p0 AS (SELECT "id", "region", "created_at" FROM "contacts_1" t '
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
    assert 'FROM p0 s WHERE s."account_id" = t."id"' in sql


def test_reach_sql_matches_composite_keys_on_every_column(crm):
    crm["deals"].columns["region"] = "string"
    crm["contacts"].columns["region"] = "string"
    p = plan(crm, ["deals.contact_id+deals.region=contacts.id+contacts.region"])
    sql = kumo_job.reach_sql(p)
    assert 's."contact_id" = t."id" AND s."region" = t."region"' in sql


THREE_LINKS = [
    "deals.contact_id=contacts.id",
    "contacts.account_id=accounts.id",
    "deals.account_id=accounts.id",
]


def _three_links(crm, links):
    _accounts(crm)
    crm["deals"].columns["account_id"] = "bigint"
    return plan(crm, links, cutoff="deals.closed_at")


def test_a_group_reached_two_ways_is_the_union_of_both_paths(crm):
    p = _three_links(crm, THREE_LINKS)
    sql = kumo_job.related_sql(p, "accounts")
    # The deal's own account, directly; and the accounts of the deal's contact.
    assert (
        'p0 AS (SELECT "id", "plan" FROM "accounts_1" t '
        'WHERE EXISTS (SELECT 1 FROM sel s WHERE s."account_id" = t."id"))'
    ) in sql
    assert (
        'p3 AS (SELECT "id", "plan" FROM "accounts_1" t '
        'WHERE EXISTS (SELECT 1 FROM p1 s WHERE s."account_id" = t."id"))'
    ) in sql
    assert sql.endswith(
        'SELECT "id", "plan" FROM (SELECT "id", "plan", row_number() OVER '
        '(PARTITION BY "id") AS "__n" FROM (SELECT "id", "plan" FROM p0 UNION ALL '
        'SELECT "id", "plan" FROM p3)) WHERE "__n" = 1'
    )
    # Contacts of the deal, and the contacts of the deal's account.
    contacts = kumo_job.related_sql(p, "contacts")
    assert "FROM p1 UNION ALL" in contacts and "FROM p2)" in contacts
    assert 'FROM p0 s WHERE s."id" = t."account_id"' in contacts


@pytest.mark.parametrize("order", [[0, 1, 2], [2, 1, 0], [1, 2, 0]])
def test_the_order_of_the_links_changes_nothing(crm, order):
    first = _three_links(crm, THREE_LINKS)
    other = plan(crm, [THREE_LINKS[i] for i in order], cutoff="deals.closed_at")
    for group in ("contacts", "accounts"):
        assert kumo_job.related_sql(other, group) == kumo_job.related_sql(first, group)
    assert kumo_job.reach_sql(other) == kumo_job.reach_sql(first)


def test_admission_counts_the_rows_the_reads_take(crm):
    p = _three_links(crm, THREE_LINKS)
    reach = kumo_job.reach_sql(p)
    for group in ("contacts", "accounts"):
        read = kumo_job.related_sql(p, group)
        body = read.split(" SELECT ", 1)[0].removeprefix("WITH ")
        assert reach.startswith(f"WITH {body}")
        assert read[len(body) + len("WITH ") :].strip() in reach


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
    with pytest.raises(
        ScoreRefused, match="orgs not reachable from deals within 2 hops"
    ):
        kumo_job.reach_sql(p)


def test_entity_over_the_byte_cap_refuses(crm, monkeypatch):
    monkeypatch.setattr(kumo_job, "ENTITY_BYTES_MAX", 50)
    p = plan(crm, ["deals.contact_id=contacts.id"])
    with pytest.raises(
        ScoreRefused,
        match=r"deals: 5 rows, 51 bytes \(UTF-8\) after projection, over the cap of 50",
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
    with pytest.raises(ScoreRefused, match=r"101 bytes \(UTF-8\) after projection"):
        kumo_job.check_admission(p, (1, 1), {"contacts": (2, 101)})
    kumo_job.check_admission(p, (1, 1), {"contacts": (2, 100)})


def test_missing_weights_are_refused_clearly(monkeypatch):
    monkeypatch.delenv("KUMO_RELATIONAL_WEIGHTS", raising=False)
    with pytest.raises(ScoreRefused, match="KUMO_RELATIONAL_WEIGHTS is not set"):
        kumo_job.weights_path()


# region: schedule


def test_scheduled_time_is_stable_and_differs_between_names():
    a = score.schedule_cron("daily", 119, "deals")
    assert a == score.schedule_cron("daily", 119, "deals")
    assert score.schedule_cron("daily", 119, "leads") != a
    assert score.schedule_cron("daily", 120, "deals") != a
    others = {score.schedule_cron("daily", 119, f"sys{i}") for i in range(20)}
    assert len(others) > 10


def test_daily_and_hourly_share_the_hashed_minute():
    daily = score.schedule_cron("daily", 119, "deals").split()
    hourly = score.schedule_cron("hourly", 119, "deals").split()
    assert daily[0] == hourly[0] == "0"
    assert daily[1] == hourly[1] and 0 <= int(daily[1]) <= 59
    assert 0 <= int(daily[2]) <= 23 and daily[3:] == ["*", "*", "?"]
    assert hourly[2:] == ["*", "*", "*", "?"]


def test_at_sets_the_hour_of_a_daily_run_and_keeps_the_minute():
    hashed = score.schedule_cron("daily", 119, "deals").split()
    at = score.schedule_cron("daily", 119, "deals", at_hour=6).split()
    assert at[1] == hashed[1] and at[2] == "6"


@pytest.mark.parametrize(
    ("every", "at", "message"),
    [
        ("hourly", 6, "--at goes with --schedule daily only"),
        ("daily", 24, "--at takes an hour from 0 to 23"),
        ("0 30 2 * * ?", None, "--schedule takes daily or hourly"),
        ("@daily", None, "--schedule takes daily or hourly"),
    ],
)
def test_schedule_takes_no_raw_cron_and_at_only_for_daily(every, at, message):
    with pytest.raises(ScoreRefused, match=message):
        score.schedule_cron(every, 1, "x", at)


def test_the_schedule_option_offers_daily_and_hourly_only():
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(score.score_cmd, ["x", "--schedule", "0 30 2 * * ?"])
    assert result.exit_code == 2
    assert "'daily', 'hourly'" in result.output


# region: one run per system, kept by the server


def _response(status, body, headers=None):
    response = requests.Response()
    response.status_code = status
    response.reason = {409: "Conflict", 429: "Too Many Requests"}.get(status, "Error")
    response.headers.update(headers or {})
    response._content = json.dumps(body).encode()
    return response


def _refusal(status, body, headers=None):
    err = RestAPIError(
        "http://hopsworks/jobs/executions", _response(status, body, headers)
    )
    # The CLI sees the SDK's error as the cause of job_cmd._run's ClickException.
    try:
        raise click.ClickException("Could not run job") from err
    except click.ClickException as wrapped:
        return score.run_refusal(wrapped, "deals")


def test_a_429_says_nothing_started_and_when_to_retry():
    body = {"usrMsg": "Scoring class is full."}
    message = str(_refusal(429, body, {"Retry-After": "120"}))
    assert message.startswith("No run of kumo-score-deals started")
    assert "Retry in 120 s with `hops score deals`." in message
    assert "already running" not in message
    assert "Retry later" in str(_refusal(429, body))
    assert "Retry later" in str(_refusal(429, body, {"Retry-After": "soon"}))


def test_the_409_wording_prefers_the_user_message_then_the_error_message():
    both = {"usrMsg": "Execution #7 is running.", "errorMsg": "Job has #7."}
    message = str(_refusal(409, both))
    assert (
        "job kumo-score-deals is already running: Execution #7 is running." in message
    )
    assert "hops job stop kumo-score-deals" in message
    assert "Execution #9 is running." in str(
        _refusal(409, {"errorMsg": "Execution #9 is running."})
    )
    assert _refusal(500, both) is None
    assert score.run_refusal(click.ClickException("x"), "deals") is None


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


# region: ownership


@pytest.fixture
def python_engine(monkeypatch):
    """FeatureGroup objects need the engine type the connection would set."""
    import hopsworks_common.connection

    monkeypatch.setattr(hopsworks_common.connection, "_hsfs_engine_type", "python")


def _group(name, description):
    from hsfs.feature import Feature
    from hsfs.feature_group import FeatureGroup

    return FeatureGroup(
        name=name,
        version=1,
        featurestore_id=1,
        description=description,
        primary_key=["id"],
        features=[Feature("id", "bigint")],
        partition_key=[],
        time_travel_format="DELTA",
    )


def _job(name, app_path, default_args, **extra):
    return Job.from_response_json(
        {
            "id": 1,
            "name": name,
            "creationTime": 0,
            "jobType": "PYTHON",
            "creator": {},
            "config": {
                "type": "pythonJobConfiguration",
                "appPath": app_path,
                "defaultArgs": default_args,
                "maxConcurrentExecutions": 1,
                "platformClass": "scoring",
                **extra,
            },
        }
    )


APP = "hdfs:///Projects/p/Resources/hops-score/sales/kumo_job.py"
ARGS = "--system sales --config '{}'"


def test_only_marked_resources_are_deleted(python_engine):
    mine = _group("sales_scores", "hops-score:sales Scores of sales.")
    other = _group("sales_score_runs", "Runs of the sales team, by hand.")
    owned, foreign = score.owned_resources(
        "sales", _job("kumo-score-sales", APP, ARGS), [mine, other]
    )
    assert [obj for _, obj in owned][1:] == [mine]
    assert owned[0][0] == "job kumo-score-sales and its schedule"
    assert foreign == ["feature group sales_score_runs v1"]


@pytest.mark.parametrize(
    ("app_path", "args"),
    [
        ("hdfs:///Projects/p/Resources/jobs/kumo-score-sales/kumo_job.py", ARGS),
        (APP, "--config '{}'"),
        (APP, "--system sales2 --config '{}'"),
        ("hdfs:///Projects/p/Resources/hops-score/sales2/kumo_job.py", ARGS),
        ("hdfs:///Projects/p/Resources/hops-score/sales/other.py", ARGS),
        (APP, "--system 'unclosed"),
    ],
)
def test_an_unrelated_job_of_the_same_name_survives(app_path, args):
    job = _job("kumo-score-sales", app_path, args)
    owned, foreign = score.owned_resources("sales", job, [])
    assert owned == [] and foreign == ["job kumo-score-sales"]
    with pytest.raises(ScoreRefused, match="job kumo-score-sales exists and was not"):
        score.check_job(job, "sales")
    score.check_job(_job("kumo-score-sales", APP, ARGS), "sales")
    score.check_job(None, "sales")


@pytest.mark.parametrize(
    "description",
    [None, "", "Scores of sales.", "hops-score:sales2 Scores", "xhops-score:sales"],
)
def test_an_unrelated_output_group_is_neither_adopted_nor_deleted(
    python_engine, description
):
    fg = _group("sales_scores", description)
    owned, foreign = score.owned_resources("sales", None, [fg])
    assert owned == [] and foreign == ["feature group sales_scores v1"]
    with pytest.raises(ScoreRefused, match="sales_scores v1 exists without the marker"):
        kumo_job.check_output_group(fg, "sales")
    kumo_job.check_output_group(_group("sales_scores", "hops-score:sales x"), "sales")
    kumo_job.check_output_group(None, "sales")


@pytest.mark.parametrize(
    ("entity", "links"),
    [
        ("pipe_scores", []),
        ("deals", ["deals.contact_id=pipe_score_runs.id"]),
    ],
)
def test_an_input_named_like_an_output_is_refused_before_anything(crm, entity, links):
    crm[entity] = crm.pop("deals") if entity != "deals" else crm["deals"]
    crm[entity].name = entity
    if links:
        crm["pipe_score_runs"] = crm.pop("contacts")
    with pytest.raises(ScoreRefused, match="is an input, and `hops score pipe` writes"):
        score.build_plan(
            "pipe", entity, crm, [score.parse_link(x) for x in links], "won"
        )


@pytest.mark.parametrize(
    "extra",
    [
        {"platformClass": None},
        {"platformClass": "embedding"},
        {"maxConcurrentExecutions": 2},
        {"maxConcurrentExecutions": None},
    ],
)
def test_the_run_limit_and_class_are_not_the_marker(extra):
    # A backend without these fields drops them from the stored config; the job is still
    # the system's, and hops score sets them again on the next run.
    job = _job("kumo-score-sales", APP, ARGS, **extra)
    score.check_job(job, "sales")


def test_the_job_writes_the_marker_it_is_checked_by():
    assert kumo_job.group_marked(
        f"{kumo_job.owner_marker('sales')} Scores of sales.", "sales"
    )
    assert kumo_job.job_marked(
        {
            "appPath": "Resources/hops-score/sales/kumo_job.py",
            "defaultArgs": ARGS,
            "maxConcurrentExecutions": 1,
            "platformClass": "scoring",
        },
        "sales",
        score.SCRIPT.name,
    )


# region: feature view versions


def _fg(name, version, cols):
    from hsfs.feature import Feature
    from hsfs.feature_group import FeatureGroup

    return FeatureGroup(
        name=name,
        version=version,
        featurestore_id=1,
        primary_key=["id"],
        partition_key=[],
        features=[Feature(c, "bigint") for c in cols],
        time_travel_format="DELTA",
    )


def test_the_view_carries_each_groups_recorded_version(python_engine):
    deals = _fg("deals", 1, ["id", "contact_id"])
    contacts = _fg("contacts", 1, ["id", "account_id"])
    accounts = _fg("accounts", 3, ["id"])
    query = deals.select_all().join(
        contacts.select_all().join(
            accounts.select_all(), left_on=["account_id"], right_on=["id"]
        ),
        left_on=["contact_id"],
        right_on=["id"],
    )
    links, pins = score.links_from_query(query)
    assert pins == {"deals": 1, "contacts": 1, "accounts": 3}
    assert [str(x) for x in links] == [
        "deals.contact_id=contacts.id",
        "contacts.account_id=accounts.id",
    ]


def test_a_view_reading_one_group_at_two_versions_is_refused(python_engine):
    deals = _fg("deals", 1, ["id", "contact_id", "owner_id"])
    query = (
        deals.select_all()
        .join(
            _fg("contacts", 1, ["id"]).select_all(),
            left_on=["contact_id"],
            right_on=["id"],
        )
        .join(
            _fg("contacts", 2, ["id"]).select_all(),
            left_on=["owner_id"],
            right_on=["id"],
            prefix="o_",
        )
    )
    with pytest.raises(ScoreRefused, match="reads contacts at versions 1 and 2"):
        score.links_from_query(query)


def test_a_link_naming_a_view_group_with_a_newer_version_is_refused():
    link = score.parse_link("activities.contact_id=contacts.id")
    pins = {"deals": 1, "contacts": 1}
    with pytest.raises(
        ScoreRefused,
        match="latest version is 2, while the feature view reads contacts v1",
    ):
        score.check_link_pins([link], pins, {"contacts": 2})
    score.check_link_pins([link], pins, {"contacts": 1})
    score.check_link_pins([score.parse_link("activities.deal_id=x.id")], pins, {})


# region: pinned input


def test_pinned_versions_reach_every_query_after_admission(crm):
    from dataclasses import replace

    p = plan(crm, ["deals.contact_id=contacts.id"], cutoff="deals.closed_at")
    assert kumo_job.pin_sql(p) == (
        "SELECT 'contacts', max(version) FROM \"contacts_1$history\" UNION ALL "
        "SELECT 'deals', max(version) FROM \"deals_1$history\""
    )
    pinned = replace(p, pins={"deals": 7, "contacts": 3})
    for sql in (
        kumo_job.query_sql(pinned),
        kumo_job.context_sql(pinned),
        kumo_job.admission_sql(pinned, "deals"),
    ):
        assert 'FROM "deals_1" FOR VERSION AS OF 7 ' in sql + " "
    for sql in (
        kumo_job.reach_sql(pinned),
        kumo_job.related_sql(pinned, "contacts"),
    ):
        assert '"deals_1" FOR VERSION AS OF 7 ' in sql
        assert 'FROM "contacts_1" FOR VERSION AS OF 3 t WHERE' in sql
    unique = score.unique_sql(pinned, pinned.links[0])
    assert unique.count('"contacts_1" FOR VERSION AS OF 3') == 2
    assert "FOR VERSION" not in kumo_job.query_sql(p)


def test_the_pins_of_the_cli_never_travel_to_the_job(crm):
    p = plan(crm, ["deals.contact_id=contacts.id"])
    assert json.loads(p.to_json())["pins"] == {}


def test_a_read_stops_before_keeping_rows_or_bytes_past_the_cap():
    assert kumo_job.row_bytes((12, "abc", None, 1.5)) == 2 + 3 + 3
    assert kumo_job.row_bytes(("é", "😀", None)) == 2 + 4
    kumo_job.check_read("contacts", 100, 1_000, 100, 1_000)
    with pytest.raises(ScoreRefused, match="contacts: the read reached 101 rows"):
        kumo_job.check_read("contacts", 101, 10, 100, 1_000)
    with pytest.raises(ScoreRefused, match=r"1001 bytes \(UTF-8\), over what is left"):
        kumo_job.check_read("contacts", 1, 1_001, 100, 1_000)


def test_the_byte_cap_counts_utf8_octets_in_sql_and_in_the_read(crm):
    p = plan(crm, ["deals.contact_id=contacts.id"])
    sql = kumo_job.admission_sql(p, "contacts")
    assert "length(to_utf8(cast(" in sql
    assert "coalesce(length(to_utf8(" in sql
    # 40 four-byte characters: 40 characters, under a cap of 100; 160 bytes, over it.
    chunk = [("😀" * 20, "😀" * 20)]
    assert sum(len(v) for v in chunk[0]) == 40
    size = sum(kumo_job.row_bytes(r) for r in chunk)
    assert size == 160
    with pytest.raises(ScoreRefused, match=r"160 bytes \(UTF-8\), over what is left"):
        kumo_job.check_read("contacts", len(chunk), size, 100, 100)
