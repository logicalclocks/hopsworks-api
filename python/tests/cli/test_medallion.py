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
    "sources": [{"name": "crm_customers", "version": 1}, {"name": "shop_orders"}],
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
    assert doc["sources"] == [
        {"name": "crm_customers", "version": 1, "arrival_column": None},
        {"name": "shop_orders", "version": 1, "arrival_column": None},
    ]
    assert doc["tasks"] == ["deduplicate", "cast_types", "mask_pii"]
    assert doc["extra_tasks"].startswith("anonymize")
    assert doc["schedule"] == {"cadence": "hourly", "cron": "0 0 * * * ?"}
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
            delete=lambda: events.append(f"fg {name} v{version}")
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
