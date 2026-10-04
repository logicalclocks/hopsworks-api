"""`hops medallion`: recording a silver layer from the Factory's answers, and deleting one."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner
from hopsworks.cli import session
from hopsworks.cli.commands import medallion, mlsystem
from hopsworks.cli.main import cli


ANSWERS = {
    "slug": "customers-silver",
    "name": "Customers silver",
    "description": "Clean customers and orders",
    "sources": [
        {"name": "crm_customers", "version": 1},
        {"name": "shop_orders", "cadence": "daily"},
    ],
    "tasks": ["deduplicate", "cast_types", "mask_pii"],
    "extra_tasks": "anonymize the email column of crm_customers",
    "engine": "dbt_trino",
    "cadence": "hourly",
    "lifecycle": "dev",
}


def _silver(tmp_path, monkeypatch, answers, registered):
    path = tmp_path / "answers.json"
    path.write_text(json.dumps(answers), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        mlsystem,
        "register",
        lambda ctx, target, name=None: registered.append((target, name)) or {},
    )
    return CliRunner().invoke(
        cli, ["medallion", "silver", "--answers", str(path), "--no-launch"]
    )


def test_silver_records_the_layer_from_the_answers_and_registers_it(
    tmp_path, monkeypatch
):
    registered = []
    done = _silver(tmp_path, monkeypatch, ANSWERS, registered)
    assert done.exit_code == 0, done.output
    target = tmp_path / "customers-silver"
    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))
    assert (
        doc["layer"]["kind"] == "silver" and doc["layer"]["name"] == "Customers silver"
    )
    # A source without a cadence takes the layer's default.
    assert doc["sources"] == [
        {
            "name": "crm_customers",
            "version": 1,
            "cadence": "hourly",
            "arrival_column": None,
        },
        {
            "name": "shop_orders",
            "version": 1,
            "cadence": "daily",
            "arrival_column": None,
        },
    ]
    assert doc["tasks"] == ["deduplicate", "cast_types", "mask_pii"]
    assert doc["extra_tasks"].startswith("anonymize")
    # One job per cadence the sources use, each with its own cron and catch-up.
    assert doc["schedule"] == {
        "cadence": "hourly",
        "catchup": True,
        "cadences": {
            "hourly": {"cron": "0 0 * * * ?", "max_catchup_runs": 48},
            "daily": {"cron": "0 0 1 * * ?", "max_catchup_runs": 14},
        },
    }
    assert doc["phases"]["profile"] == {"status": "pending"}
    # Nothing built yet, so the whole spec is a change to apply.
    assert doc["outputs"]["applied_spec"] == {}
    assert "Logs/factory/<slug>/" in (target / "AGENTS.md").read_text(encoding="utf-8")
    assert "logs-*/" in (target / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert registered == [(target, "Customers silver")]
    assert 'claude "/hops-silver customers-silver"' in done.output


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"slug": "Bad Slug"}, "slug must be"),
        ({"sources": []}, "at least one bronze feature group"),
        ({"tasks": ["teleport"]}, "unknown task 'teleport'"),
        ({"engine": "spark"}, "engine must be one of"),
        ({"cadence": "monthly"}, "cadence must be one of"),
        ({"surprise": 1}, "unknown answer 'surprise'"),
    ],
)
def test_silver_refuses_answers_it_cannot_build(tmp_path, monkeypatch, change, problem):
    done = _silver(tmp_path, monkeypatch, {**ANSWERS, **change}, [])
    assert done.exit_code != 0 and problem in done.output
    assert not (tmp_path / ANSWERS["slug"] / "system.yaml").exists()


def test_silver_does_not_overwrite_an_existing_layer(tmp_path, monkeypatch):
    assert _silver(tmp_path, monkeypatch, ANSWERS, []).exit_code == 0
    again = _silver(tmp_path, monkeypatch, ANSWERS, [])
    assert again.exit_code != 0 and "already holds a system.yaml" in again.output


def test_a_layer_is_not_listed_as_an_ml_system_by_hops_build(tmp_path, monkeypatch):
    from hopsworks.cli.commands import build

    assert _silver(tmp_path, monkeypatch, ANSWERS, []).exit_code == 0
    (tmp_path / "churn").mkdir()
    (tmp_path / "churn" / "system.yaml").write_text("system: {name: churn}\n")
    assert build._systems(tmp_path) == [tmp_path / "churn"]


def test_delete_with_assets_removes_the_job_tables_directory_then_the_entry(
    tmp_path, monkeypatch
):
    target = tmp_path / "customers-silver"
    target.mkdir()
    (target / "system.yaml").write_text(
        yaml.safe_dump(
            {
                "outputs": {
                    "tables": [{"name": "customers", "version": 1}],
                    "rejects": [{"name": "customers_rejects", "version": 1}],
                    "job": {"name": "customers-silver-silver"},
                }
            }
        ),
        encoding="utf-8",
    )
    events = []
    job = SimpleNamespace(delete=lambda: events.append("job"))
    fs = SimpleNamespace(
        get_feature_group=lambda name, version: SimpleNamespace(
            name=name,
            version=version,
            get_tags=lambda: {"medallion_table": {"layer": "silver"}},
            delete=lambda: events.append(f"fg {name} v{version}"),
        )
    )
    project = SimpleNamespace(
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: job),
        get_feature_store=lambda: fs,
    )
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    from hopsworks_common.core import ml_system_api

    entry = {"id": 7, "name": "Customers silver", "pathToCode": "x"}
    monkeypatch.setattr(ml_system_api, "_list", lambda: [entry])
    monkeypatch.setattr(ml_system_api, "_remove", lambda i: events.append(f"entry {i}"))
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)

    done = CliRunner().invoke(
        cli, ["medallion", "delete", "Customers silver", "--assets", "--yes"]
    )
    assert done.exit_code == 0, done.output
    assert events == [
        "job",
        "fg customers v1",
        "fg customers_rejects v1",
        "entry 7",
    ]
    assert not target.exists()


def test_tasks_offered_by_the_factory_are_the_ones_the_skill_documents():
    tasks = medallion.TEMPLATE.parents[1] / "SKILL.md"
    text = tasks.read_text(encoding="utf-8")
    for task in medallion.TASKS:
        assert f"| `{task}` |" in text, task


def test_silver_records_the_settings_with_their_defaults(tmp_path, monkeypatch):
    answers = {
        **ANSWERS,
        "history": "full",
        "deletes": "propagate",
        "lookback": "7d",
        "max_reject_pct": 2,
    }
    assert _silver(tmp_path, monkeypatch, answers, []).exit_code == 0
    doc = yaml.safe_load(
        (tmp_path / "customers-silver" / "system.yaml").read_text(encoding="utf-8")
    )
    assert doc["history"] == "full" and doc["deletes"] == "propagate"
    assert doc["schema_changes"] == "fail"
    assert doc["late_data"] == {"lookback": "7d"}
    assert doc["quality"] == {"max_reject_pct": 2, "alert_on_failure": True}
    # Stale after a missed run plus slack, per cadence.
    assert doc["freshness"] == {"max_age_hours": {"hourly": 2, "daily": 26}}
    assert doc["schedule"]["catchup"] is True


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"history": "scd9"}, "history must be one of"),
        ({"lookback": "3h"}, "lookback must be one of"),
        ({"max_reject_pct": 120}, "max_reject_pct must be"),
        ({"freshness_hours": 0}, "freshness_hours must be"),
    ],
)
def test_silver_refuses_settings_it_cannot_build(
    tmp_path, monkeypatch, change, problem
):
    done = _silver(tmp_path, monkeypatch, {**ANSWERS, **change}, [])
    assert done.exit_code != 0 and problem in done.output


def test_delete_stops_on_a_feature_group_error_other_than_missing(
    tmp_path, monkeypatch
):
    target = tmp_path / "customers-silver"
    target.mkdir()
    (target / "system.yaml").write_text(
        yaml.safe_dump(
            {"outputs": {"tables": [{"name": "gone"}, {"name": "customers"}]}}
        ),
        encoding="utf-8",
    )
    events = []

    def get_feature_group(name, version):
        if name == "gone":
            return
        raise RuntimeError("403 Forbidden")

    project = SimpleNamespace(
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: None),
        get_feature_store=lambda: SimpleNamespace(get_feature_group=get_feature_group),
    )
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    from hopsworks_common.core import ml_system_api

    monkeypatch.setattr(
        ml_system_api, "_list", lambda: [{"id": 7, "name": "L", "pathToCode": "x"}]
    )
    monkeypatch.setattr(ml_system_api, "_remove", lambda i: events.append(i))
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)
    done = CliRunner().invoke(cli, ["medallion", "delete", "L", "--assets", "--yes"])
    assert done.exit_code != 0
    # The layer stays listed and its directory kept, to be deleted again.
    assert events == [] and target.exists()


def _delta_table(path, rows, days, commit_ms):
    """A Delta table of `rows` rows spread over `days`, its commit dated `commit_ms`."""
    from datetime import datetime, timedelta, timezone

    import pandas as pd
    from deltalake import write_deltalake

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_deltalake(
        str(path),
        pd.DataFrame(
            {
                "id": range(rows),
                "ts": [start + timedelta(days=days * i / rows) for i in range(rows)],
            }
        ),
    )
    # Back-date the commit, as an old write would be.
    log = path / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines() if line]
    for action in actions:
        if "commitInfo" in action:
            action["commitInfo"]["timestamp"] = commit_ms
    log.write_text("".join(json.dumps(a) + "\n" for a in actions))


def test_status_reports_freshness_rejects_and_layout(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from hopsworks.cli import health, silver_status

    db = tmp_path / "featurestore" / "demo_featurestore.db"
    old = int((datetime.now(timezone.utc) - timedelta(hours=40)).timestamp() * 1000)
    fresh = int(datetime.now(timezone.utc).timestamp() * 1000)
    _delta_table(db / "orders_1", 90, 10, old)
    _delta_table(db / "orders_rejects_1", 10, 10, fresh)
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))

    class Cursor:
        def execute(self, sql):
            self.n = 90 if '"orders_1"' in sql else 10

        def fetchone(self):
            return (self.n,)

    conn = SimpleNamespace(cursor=Cursor, close=lambda: None)
    project = SimpleNamespace(
        name="Demo",
        get_trino_api=lambda: SimpleNamespace(connect=lambda **kw: conn),
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: None),
    )
    doc = {
        "layer": {"name": "Orders silver", "kind": "silver"},
        "freshness": {"max_age_hours": 26},
        "quality": {"max_reject_pct": 5},
        "outputs": {
            "tables": [{"name": "orders", "version": 1}],
            "rejects": [{"name": "orders_rejects", "version": 1}],
            "job": {"name": "orders-silver"},
        },
    }
    facts = silver_status.collect(project, doc, "orders-silver")
    orders = facts["tables"][0]
    assert orders["rows"] == 90 and orders["reject_pct"] == 10.0
    assert orders["layout"]["active_files"] == 1
    assert any(p.startswith("stale:") for p in orders["problems"])
    assert any(p.startswith("rejects: 10.0%") for p in orders["problems"])
    assert facts["overall"] == "degraded" and facts["counts"]["table_problems"] == 1
    # The job is gone, which the report shows as missing.
    assert facts["jobs"][0]["missing"] is True
    page = health.render(facts, None)
    assert "tablesSection" in page and '"tables": [' in page


def test_backfill_runs_the_job_over_all_of_history(tmp_path, monkeypatch):
    target = tmp_path / "customers-silver"
    target.mkdir()
    (target / "system.yaml").write_text(
        yaml.safe_dump({"outputs": {"job": {"name": "customers-silver-silver"}}}),
        encoding="utf-8",
    )
    calls = []
    job = SimpleNamespace(
        run=lambda **kw: (
            calls.append(kw) or SimpleNamespace(id=9, final_status="SUCCEEDED")
        )
    )
    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(get_job=lambda name: job)
        ),
    )
    from hopsworks_common.core import ml_system_api

    monkeypatch.setattr(
        ml_system_api, "_list", lambda: [{"id": 7, "name": "L", "pathToCode": "x"}]
    )
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)
    done = CliRunner().invoke(cli, ["medallion", "backfill", "L"])
    assert done.exit_code == 0, done.output
    [call] = calls
    assert call["start_time"].year == 1970 and call["await_termination"] is True
    assert call["end_time"] > call["start_time"]


def test_partition_advisor_partitions_only_large_tables(tmp_path):
    advisor = _load_advisor()
    _delta_table(tmp_path / "small_1", 1000, 30, 0)
    facts = advisor.evidence(tmp_path / "small_1", "ts")
    assert facts["rows"] == 1000 and facts["files"] == 1
    assert 29 * 86_400 < facts["span_seconds"] <= 30 * 86_400
    assert advisor.recommend(facts)["decision"] == "none"

    gib = advisor.GIB
    big = {"total_bytes": 400 * gib, "rows": 10**10, "span_seconds": 365 * 86_400}
    # 400 GiB over a year: about 47 MiB an hour, 1.1 GiB a day.
    assert advisor.recommend(big)["granularity"] == "day"
    huge = {**big, "total_bytes": 4000 * gib}
    assert advisor.recommend(huge)["granularity"] == "hour"
    sparse = {**big, "total_bytes": 20 * gib}
    assert advisor.recommend(sparse)["granularity"] == "week"
    tiny_rate = {**big, "total_bytes": 2 * gib}
    assert advisor.recommend(tiny_rate)["decision"] == "none"


def _load_advisor():
    import importlib.util

    path = (
        medallion.TEMPLATE.parents[2]
        / "hops-partitioning"
        / "scripts"
        / "partition_advisor.py"
    )
    spec = importlib.util.spec_from_file_location("partition_advisor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _layer_dir(tmp_path, doc):
    target = tmp_path / "customers-silver"
    target.mkdir()
    (target / "system.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
    return target


def _delete(monkeypatch, target, tags, args):
    from hopsworks_common.core import ml_system_api

    events = []
    fs = SimpleNamespace(
        get_feature_group=lambda name, version: SimpleNamespace(
            name=name,
            version=version,
            get_tags=lambda: tags.get(name, {}),
            delete=lambda: events.append(f"fg {name}"),
        )
    )
    job = SimpleNamespace(delete=lambda: events.append("job"))
    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(get_job=lambda name: job),
            get_feature_store=lambda: fs,
        ),
    )
    monkeypatch.setattr(
        ml_system_api, "_list", lambda: [{"id": 7, "name": "L", "pathToCode": "x"}]
    )
    monkeypatch.setattr(ml_system_api, "_remove", lambda i: events.append("entry"))
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)
    done = CliRunner().invoke(cli, ["medallion", "delete", "L", *args, "--yes"])
    return done, events


LAYERED = {
    "sources": [{"name": "crm", "version": 1}],
    "phases": {"profile": {"status": "done"}},
    "layer": {"status": "built"},
    "outputs": {
        "tables": [{"name": "customers"}],
        "job": {"name": "silver-job"},
        "gold": {"tables": [{"name": "customer_360"}], "job": {"name": "gold-job"}},
    },
}


def test_delete_silver_only_keeps_gold_the_entry_and_the_directory(
    tmp_path, monkeypatch
):
    target = _layer_dir(tmp_path, LAYERED)
    done, events = _delete(monkeypatch, target, {}, ["--assets", "--layer", "silver"])
    assert done.exit_code == 0, done.output
    assert events == ["job", "fg customers"]
    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))
    assert doc["outputs"]["tables"] == [] and doc["outputs"]["job"] == {}
    assert doc["outputs"]["gold"]["tables"] == [{"name": "customer_360"}]
    assert doc["phases"]["profile"] == {"status": "pending"}


def test_delete_gold_and_silver_removes_the_entry_last(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, LAYERED)
    done, events = _delete(monkeypatch, target, {}, ["--assets"])
    assert done.exit_code == 0, done.output
    assert events == ["job", "fg customers", "job", "fg customer_360", "entry"]
    assert not target.exists()


@pytest.mark.parametrize(
    ("doc", "tags"),
    [
        # A source listed among the outputs by mistake.
        ({**LAYERED, "outputs": {"tables": [{"name": "crm"}]}}, {}),
        # A table tagged bronze.
        (LAYERED, {"customers": {"medallion_table": '{"layer": "bronze"}'}}),
    ],
)
def test_delete_never_deletes_bronze(tmp_path, monkeypatch, doc, tags):
    target = _layer_dir(tmp_path, doc)
    done, events = _delete(monkeypatch, target, tags, ["--assets", "--layer", "silver"])
    assert done.exit_code != 0 and "source of truth" in done.output
    # Refused before anything was deleted.
    assert events == [] and target.exists()


def test_layer_needs_assets(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, LAYERED)
    done, events = _delete(monkeypatch, target, {}, ["--layer", "gold"])
    assert done.exit_code != 0 and "pass --assets too" in done.output
    assert events == []


def test_silver_refuses_an_unknown_source_cadence(tmp_path, monkeypatch):
    answers = {**ANSWERS, "sources": [{"name": "crm", "cadence": "monthly"}]}
    done = _silver(tmp_path, monkeypatch, answers, [])
    assert done.exit_code != 0 and "source crm: cadence must be one of" in done.output


def test_backfill_runs_every_job_slowest_first(tmp_path, monkeypatch):
    target = tmp_path / "customers-silver"
    target.mkdir()
    jobs = [
        {"name": "l-silver-hourly", "cadence": "hourly"},
        {"name": "l-silver-weekly", "cadence": "weekly"},
        {"name": "l-silver-daily", "cadence": "daily"},
    ]
    (target / "system.yaml").write_text(
        yaml.safe_dump({"outputs": {"jobs": jobs}}), encoding="utf-8"
    )
    ran = []

    def get_job(name):
        return SimpleNamespace(
            run=lambda **kw: (
                ran.append(name) or SimpleNamespace(id=1, final_status="SUCCEEDED")
            )
        )

    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(get_job=get_job)
        ),
    )
    from hopsworks_common.core import ml_system_api

    monkeypatch.setattr(
        ml_system_api, "_list", lambda: [{"id": 7, "name": "L", "pathToCode": "x"}]
    )
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)
    done = CliRunner().invoke(cli, ["medallion", "backfill", "L"])
    assert done.exit_code == 0, done.output
    assert ran == ["l-silver-weekly", "l-silver-daily", "l-silver-hourly"]
    # The directory's name finds the layer too.
    monkeypatch.setattr(
        ml_system_api,
        "_list",
        lambda: [{"id": 7, "name": "L", "pathToCode": "/Projects/p/Users/u/l-silver"}],
    )
    assert CliRunner().invoke(cli, ["medallion", "backfill", "l-silver"]).exit_code == 0


def test_status_reads_every_job_and_each_tables_cadence_target(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from hopsworks.cli import silver_status

    db = tmp_path / "featurestore" / "demo_featurestore.db"
    old = int((datetime.now(timezone.utc) - timedelta(hours=5)).timestamp() * 1000)
    _delta_table(db / "clicks_1", 10, 1, old)
    _delta_table(db / "products_1", 10, 1, old)
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))
    project = SimpleNamespace(
        name="Demo",
        get_trino_api=lambda: (_ for _ in ()).throw(RuntimeError("no trino")),
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: None),
    )
    doc = {
        "freshness": {"max_age_hours": {"hourly": 2, "daily": 26}},
        "outputs": {
            "tables": [
                {"name": "clicks", "cadence": "hourly"},
                {"name": "products", "cadence": "daily"},
            ],
            "jobs": [
                {"name": "l-silver-hourly", "cadence": "hourly"},
                {"name": "l-silver-daily", "cadence": "daily"},
            ],
        },
    }
    facts = silver_status.collect(project, doc, "l")
    assert [j["name"] for j in facts["jobs"]] == ["l-silver-hourly", "l-silver-daily"]
    clicks, products = facts["tables"]
    # Five hours old: stale for an hourly table, fresh for a daily one.
    assert clicks["max_age_hours"] == 2 and clicks["problems"]
    assert products["max_age_hours"] == 26 and not products["problems"]
