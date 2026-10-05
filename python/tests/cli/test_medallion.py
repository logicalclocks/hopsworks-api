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
    target = tmp_path / "hops-customers" / "customers-silver"
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
        (tmp_path / "hops-customers" / "customers-silver" / "system.yaml").read_text(
            encoding="utf-8"
        )
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


def _run(monkeypatch, target, tags, argv):
    """Run `hops medallion <argv>` on the layer at `target`, recording what is deleted."""
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
    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(
                get_job=lambda name: SimpleNamespace(
                    delete=lambda: events.append(f"job {name}")
                )
            ),
            get_feature_store=lambda: fs,
        ),
    )
    monkeypatch.setattr(
        ml_system_api, "_list", lambda: [{"id": 7, "name": "L", "pathToCode": "x"}]
    )
    monkeypatch.setattr(ml_system_api, "_remove", lambda i: events.append("entry"))
    monkeypatch.setattr(mlsystem, "_local_dir", lambda e: target)
    done = CliRunner().invoke(cli, ["medallion", *argv])
    return done, events


def _delete(monkeypatch, target, tags, args):
    return _run(monkeypatch, target, tags, ["delete", "L", *args, "--yes"])


def _doc(target):
    return yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))


SILVER = {
    "layer": {"kind": "silver"},
    "sources": [
        {"name": "crm", "version": 1, "cadence": "daily"},
        {"name": "clicks", "version": 1, "cadence": "hourly"},
    ],
    "schedule": {
        "cadence": "daily",
        "cadences": {
            "daily": {"cron": "0 0 1 * * ?", "max_catchup_runs": 14},
            "hourly": {"cron": "0 0 * * * ?", "max_catchup_runs": 48},
        },
    },
    "freshness": {"max_age_hours": {"daily": 26, "hourly": 2}},
    "outputs": {
        "tables": [
            {"name": "customers", "cadence": "daily"},
            {"name": "sessions", "cadence": "hourly"},
        ],
        "rejects": [{"name": "sessions_rejects"}],
        "jobs": [
            {"name": "l-silver-daily", "cadence": "daily", "tables": ["customers"]},
            {"name": "l-silver-hourly", "cadence": "hourly", "tables": ["sessions"]},
        ],
        "applied_spec": {
            "sources": [
                {"name": "crm", "version": 1, "cadence": "daily"},
                {"name": "clicks", "version": 1, "cadence": "hourly"},
            ]
        },
    },
}

GOLD = {
    "layer": {"kind": "gold", "slug": "sales-gold"},
    "sources": [{"name": "customers", "version": 1}],
    "marts": [
        {
            "slug": "sales",
            "tables": [
                {"name": "fct_orders", "kind": "fact"},
                {"name": "dim_customer", "kind": "dimension"},
            ],
            "jobs": [
                {"name": "g-sales-daily", "tables": ["fct_orders", "dim_customer"]}
            ],
        },
        {
            "slug": "churn",
            "tables": [
                {"name": "fct_churn", "kind": "fact"},
                {"name": "dim_customer", "kind": "dimension", "shared": True},
            ],
            "jobs": [
                {"name": "g-churn-weekly", "tables": ["fct_churn"]},
                {"name": "g-churn-hourly", "tables": ["fct_churn"]},
            ],
        },
    ],
}


def test_delete_silver_with_assets_removes_the_entry_last(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    done, events = _delete(monkeypatch, target, {}, ["--assets"])
    assert done.exit_code == 0, done.output
    assert events == [
        "job l-silver-daily",
        "job l-silver-hourly",
        "fg customers",
        "fg sessions",
        "fg sessions_rejects",
        "entry",
    ]
    assert not target.exists()


def test_delete_gold_deletes_every_mart_and_a_shared_dimension_once(
    tmp_path, monkeypatch
):
    target = _layer_dir(tmp_path, GOLD)
    done, events = _delete(monkeypatch, target, {}, ["--assets"])
    assert done.exit_code == 0, done.output
    assert sorted(e for e in events if e.startswith("fg")) == [
        "fg dim_customer",
        "fg fct_churn",
        "fg fct_orders",
    ]
    assert events[-1] == "entry"


def test_delete_without_assets_only_forgets_the_entry(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    done, events = _delete(monkeypatch, target, {}, [])
    assert done.exit_code == 0, done.output
    assert events == ["entry"] and target.exists()


@pytest.mark.parametrize(
    ("doc", "tags", "why"),
    [
        # A source listed among the outputs by mistake.
        (
            {**SILVER, "outputs": {"tables": [{"name": "crm"}]}},
            {},
            "a source of this layer",
        ),
        (SILVER, {"customers": {"medallion_table": '{"layer": "bronze"}'}}, "bronze"),
        # Gold never deletes silver.
        (GOLD, {"fct_orders": {"medallion_table": '{"layer": "silver"}'}}, "silver"),
    ],
)
def test_delete_never_deletes_a_lower_layer(tmp_path, monkeypatch, doc, tags, why):
    target = _layer_dir(tmp_path, doc)
    done, events = _delete(monkeypatch, target, tags, ["--assets"])
    assert done.exit_code != 0 and why in done.output
    # Refused before anything was deleted.
    assert events == [] and target.exists()


def test_mart_delete_keeps_tables_another_mart_lists(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    done, events = _run(
        monkeypatch,
        target,
        {},
        ["mart-delete", "L", "churn", "--tables", "--yes"],
    )
    assert done.exit_code == 0, done.output
    assert events == ["job g-churn-weekly", "job g-churn-hourly", "fg fct_churn"]
    doc = _doc(target)
    assert [m["slug"] for m in doc["marts"]] == ["sales"]
    assert "dim_customer are kept" in doc["decisions"][-1]["what"]


def test_mart_delete_without_tables_deletes_only_the_jobs(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    done, events = _run(monkeypatch, target, {}, ["mart-delete", "L", "sales", "--yes"])
    assert done.exit_code == 0, done.output
    assert events == ["job g-sales-daily"]


def test_job_delete_in_gold_keeps_tables_another_job_writes(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    done, events = _run(
        monkeypatch,
        target,
        {},
        ["job-delete", "L", "g-churn-hourly", "--tables", "--yes"],
    )
    assert done.exit_code == 0, done.output
    assert events == ["job g-churn-hourly"]
    churn = _doc(target)["marts"][1]
    assert [j["name"] for j in churn["jobs"]] == ["g-churn-weekly"]
    assert len(churn["tables"]) == 2


def test_job_delete_in_silver_drops_the_cadence(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    done, events = _run(
        monkeypatch,
        target,
        {},
        ["job-delete", "L", "l-silver-hourly", "--tables", "--yes"],
    )
    assert done.exit_code == 0, done.output
    assert events == ["job l-silver-hourly", "fg sessions", "fg sessions_rejects"]
    doc = _doc(target)
    assert [s["name"] for s in doc["sources"]] == ["crm"]
    # Nothing pending, so applying the spec does not rebuild the job.
    assert doc["outputs"]["applied_spec"]["sources"] == [
        {"name": "crm", "version": 1, "cadence": "daily"}
    ]
    assert list(doc["schedule"]["cadences"]) == ["daily"]
    assert doc["freshness"]["max_age_hours"] == {"daily": 26}
    assert [t["name"] for t in doc["outputs"]["tables"]] == ["customers"]
    assert doc["outputs"]["rejects"] == []


def test_job_delete_refuses_an_unknown_job(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    done, events = _run(monkeypatch, target, {}, ["job-delete", "L", "nope", "--yes"])
    assert done.exit_code != 0 and "No job 'nope'" in done.output
    assert events == []


def _answers_file(tmp_path, answers):
    path = tmp_path / "answers.json"
    path.write_text(json.dumps(answers), encoding="utf-8")
    return str(path)


MART = {
    "slug": "returns",
    "name": "Returns",
    "cadence": "weekly",
    "requirements": {
        "analysts": "merchandising",
        "example_queries": "returns in March 2024 are about 4% of orders",
        "grain": {"represents": "one returned article", "type": "transaction"},
        "on_check_failure": "quarantine",
    },
}


def test_mart_add_appends_a_draft_mart(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    done, _ = _run(
        monkeypatch,
        target,
        {},
        ["mart-add", "L", "--answers", _answers_file(tmp_path, MART), "--no-launch"],
    )
    assert done.exit_code == 0, done.output
    mart = _doc(target)["marts"][-1]
    assert mart["slug"] == "returns" and mart["status"] == "draft"
    assert mart["freshness_hours"] == 170 and mart["applied"] == {}
    assert mart["phases"]["requirements"] == {"status": "pending"}
    assert '"/hops-gold customers-silver returns"' in done.output


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"slug": "sales"}, "already has a data mart 'sales'"),
        ({"cadence": "monthly"}, "cadence must be one of"),
        ({"requirements": {"grain": {"type": "daily"}}}, "grain.type must be one of"),
        ({"requirements": {"on_check_failure": "ignore"}}, "on_check_failure must be"),
        ({"requirements": {"vibes": 1}}, "unknown requirement 'vibes'"),
        ({"requirements": {}}, "needs example_queries"),
    ],
)
def test_mart_add_refuses_bad_answers(tmp_path, monkeypatch, change, problem):
    target = _layer_dir(tmp_path, GOLD)
    path = _answers_file(tmp_path, {**MART, **change})
    done, _ = _run(monkeypatch, target, {}, ["mart-add", "L", "--answers", path])
    assert done.exit_code != 0 and problem in done.output


def test_mart_update_keeps_tables_and_jobs(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, GOLD)
    path = _answers_file(tmp_path, {**MART, "slug": "ignored", "cadence": "hourly"})
    done, _ = _run(
        monkeypatch,
        target,
        {},
        ["mart-update", "L", "sales", "--answers", path, "--no-launch"],
    )
    assert done.exit_code == 0, done.output
    sales = _doc(target)["marts"][0]
    assert sales["slug"] == "sales" and sales["cadence"] == "hourly"
    assert sales["requirements"]["analysts"] == "merchandising"
    assert [j["name"] for j in sales["jobs"]] == ["g-sales-daily"]


def test_marts_are_only_in_gold(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    path = _answers_file(tmp_path, MART)
    done, _ = _run(monkeypatch, target, {}, ["mart-add", "L", "--answers", path])
    assert done.exit_code != 0 and "is a silver layer" in done.output


def test_add_tables_adds_sources_and_a_job_cadence(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    answers = {
        "description": "weekly article catalogue",
        "sources": [{"name": "articles", "cadence": "weekly"}],
    }
    path = _answers_file(tmp_path, answers)
    done, _ = _run(
        monkeypatch, target, {}, ["add-tables", "L", "--answers", path, "--no-launch"]
    )
    assert done.exit_code == 0, done.output
    doc = _doc(target)
    assert doc["sources"][-1]["name"] == "articles"
    assert doc["schedule"]["cadences"]["weekly"]["max_catchup_runs"] == 4
    assert doc["freshness"]["max_age_hours"] == {
        "hourly": 2,
        "daily": 26,
        "weekly": 170,
    }
    assert doc["additions"][-1]["status"] == "pending"
    assert '"/hops-silver customers-silver apply"' in done.output


def test_add_tables_refuses_a_known_source(tmp_path, monkeypatch):
    target = _layer_dir(tmp_path, SILVER)
    path = _answers_file(tmp_path, {"sources": [{"name": "crm"}]})
    done, _ = _run(monkeypatch, target, {}, ["add-tables", "L", "--answers", path])
    assert done.exit_code != 0 and "crm v1 is already a source" in done.output


GOLD_ANSWERS = {
    "slug": "sales-gold",
    "name": "Sales gold",
    "queries": "revenue by week and region",
    "modeling": "snowflake",
    "sources": [{"name": "customers"}, {"name": "orders", "version": 2}],
    "standards": {"naming": "our naming"},
    "mart": MART,
}


def test_gold_records_the_layer_and_its_first_mart(tmp_path, monkeypatch):
    registered = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        mlsystem,
        "register",
        lambda ctx, target, name=None: registered.append((target, name)) or {},
    )
    path = _answers_file(tmp_path, GOLD_ANSWERS)
    done = CliRunner().invoke(
        cli, ["medallion", "gold", "--answers", path, "--no-launch"]
    )
    assert done.exit_code == 0, done.output
    target = tmp_path / "hops-sales" / "sales-gold"
    doc = _doc(target)
    assert doc["layer"]["kind"] == "gold" and doc["layer"]["modeling"] == "snowflake"
    assert doc["sources"] == [
        {"name": "customers", "version": 1},
        {"name": "orders", "version": 2},
    ]
    # The user's standard is kept, the rest proposed.
    assert doc["standards"]["naming"] == "our naming"
    assert doc["standards"]["quality"] == medallion.DEFAULT_STANDARDS["quality"]
    assert [m["slug"] for m in doc["marts"]] == ["returns"]
    assert "Data marts" in (target / "AGENTS.md").read_text(encoding="utf-8")
    assert registered == [(target, "Sales gold")]
    assert 'claude "/hops-gold sales-gold"' in done.output


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"sources": []}, "at least one silver feature group"),
        ({"modeling": "galaxy"}, "modeling must be one of"),
        ({"mart": None}, "needs a data mart"),
        ({"standards": {"style": "x"}}, "standards takes"),
    ],
)
def test_gold_refuses_bad_answers(tmp_path, monkeypatch, change, problem):
    monkeypatch.chdir(tmp_path)
    path = _answers_file(tmp_path, {**GOLD_ANSWERS, **change})
    done = CliRunner().invoke(cli, ["medallion", "gold", "--answers", path])
    assert done.exit_code != 0 and problem in done.output
    assert not (tmp_path / "hops-sales" / "sales-gold").exists()


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


def test_status_reads_gold_tables_with_their_marts_freshness():
    from hopsworks.cli import silver_status

    doc = {**GOLD, "marts": [{**GOLD["marts"][0], "freshness_hours": 30}]}
    tables = medallion.layer_tables(doc)
    assert [(t["name"], t["kind"], t["mart"]) for t in tables] == [
        ("fct_orders", "gold", "sales"),
        ("dim_customer", "gold", "sales"),
    ]
    assert silver_status._freshness(doc, tables[0]) == 30
    assert [j["mart"] for j in medallion.layer_jobs(GOLD)] == [
        "sales",
        "churn",
        "churn",
    ]


def _created(tmp_path, monkeypatch, kind, answers):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(mlsystem, "register", lambda ctx, target, name=None: {})
    done = CliRunner().invoke(
        cli,
        [
            "medallion",
            kind,
            "--answers",
            _answers_file(tmp_path, answers),
            "--no-launch",
        ],
    )
    assert done.exit_code == 0, done.output
    return done


def test_silver_and_gold_share_one_medallion_repository(tmp_path, monkeypatch):
    _created(tmp_path, monkeypatch, "silver", {**ANSWERS, "slug": "shop-silver"})
    repo = tmp_path / "hops-shop"
    silver = _doc(repo / "shop-silver")
    assert silver["layer"]["repo"] == {"name": "hops-shop"}
    # The silver layer builds orders, which the gold layer reads.
    silver["outputs"]["tables"] = [{"name": "orders", "version": 1}]
    (repo / "shop-silver" / "system.yaml").write_text(
        yaml.safe_dump(silver), encoding="utf-8"
    )
    gold = {
        **GOLD_ANSWERS,
        "slug": "sales-gold",
        "sources": [{"name": "orders"}],
    }
    _created(tmp_path, monkeypatch, "gold", gold)
    # Its slug names another medallion, but it joins the silver layer's.
    assert (repo / "sales-gold" / "system.yaml").is_file()
    assert _doc(repo / "sales-gold")["layer"]["repo"] == {"name": "hops-shop"}
    assert not (tmp_path / "hops-sales").exists()
    assert (repo / ".git").is_dir() and not (repo / "sales-gold" / ".git").exists()
    found = CliRunner().invoke(cli, ["medallion", "dir", "sales-gold"])
    assert found.exit_code == 0 and found.output.strip() == str(repo / "sales-gold")


def test_a_layer_can_name_its_repository(tmp_path, monkeypatch):
    _created(tmp_path, monkeypatch, "gold", {**GOLD_ANSWERS, "repo": "hops-retail"})
    assert (tmp_path / "hops-retail" / "sales-gold" / "system.yaml").is_file()
    bad = _answers_file(tmp_path, {**GOLD_ANSWERS, "repo": "../escape"})
    done = CliRunner().invoke(cli, ["medallion", "gold", "--answers", bad])
    assert done.exit_code != 0 and "repo must be hops-" in done.output
