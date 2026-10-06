"""`hops factory`: the built-in factories, the project's own, and what a factory definition may say."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
import yaml
from click.testing import CliRunner
from hopsworks.cli import factory_spec, session
from hopsworks.cli.commands import factory, medallion, mlsystem
from hopsworks.cli.main import cli
from hopsworks_common.core import factory_api


REVIEW = """\
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: churn-review
title: Churn review
description: A weekly review of churn drivers.
form:
  sections:
    - id: basics
      title: Basics
      fields:
        - {id: name, type: slug, label: Name, required: true}
        - {id: question, type: textarea, label: "What should it answer?", required: true}
        - {id: cadence, type: choice, label: Cadence, options: [daily, weekly], default: weekly}
        - {id: weeks, key: compare.weeks, type: number, label: Weeks, min: 1}
    - id: sources
      title: Sources
      collapsed: true
      fields:
        - id: tables
          type: list
          label: Tables
          fields:
            - {id: table, type: feature_group, label: Table, required: true}
            - {id: cadence, type: choice, label: Refresh, options: [{value: daily, label: Daily}, weekly]}
phases:
  - {key: build, label: Build, minutes: 20}
  - {key: verify, label: Verify}
build:
  skills: [hops-superset]
  instructions: Build a {dashboard} over the chosen tables.
list:
  columns:
    - {label: Cadence, from: requirements.cadence}
"""


def _definition(text: str = REVIEW, version: int = 2) -> dict:
    spec = yaml.safe_load(text)
    return {
        "name": spec["name"],
        "title": spec["title"],
        "version": version,
        "currentVersion": version,
        "definition": text,
        "spec": spec,
    }


@pytest.fixture
def logged_in(monkeypatch):
    monkeypatch.setattr(session, "get_project", lambda ctx: mock.Mock(name="churndemo"))


# region What a definition may say


def test_a_well_formed_definition_has_no_problems():
    assert factory_spec.text_problems(REVIEW) == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (("name: churn-review", "name: Churn Review"), "name must be"),
        (("type: slug", "type: text"), "needs a field of type slug"),
        (("options: [daily, weekly]", "options: []"), "options must be"),
        (
            ("min: 1}", "min: 1, when: {field: cadence, equals: weekly}}"),
            "when is not supported",
        ),
        (("  instructions: Build", "  notes: Build"), "build.instructions is required"),
        (("type: number", "type: component"), "type must be one of"),
        (("key: compare.weeks", "key: name"), "is used by another field"),
        (("from: requirements.cadence", "from: x; rm"), "dotted path"),
        (("id: cadence", "id: name"), "unique"),
        (("key: verify", "key: system"), "a block of system.yaml"),
    ],
)
def test_each_rule_names_what_is_wrong(change, problem):
    found = factory_spec.text_problems(REVIEW.replace(*change))
    assert any(problem in p for p in found), found


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("!!python/object/apply:os.system [x]", "not valid YAML"),
        ("a: 1\na: 2", "not valid YAML"),
        ("x" * (factory_spec.MAX_LENGTH + 1), "the limit is"),
        ("- a list", "mapping"),
    ],
)
def test_unsafe_or_oversized_yaml_is_refused(text, problem):
    assert any(problem in p for p in factory_spec.text_problems(text))


def test_answers_are_checked_as_the_form_checks_them():
    spec = yaml.safe_load(REVIEW)
    good = {
        "name": "q3-review",
        "question": "why",
        "cadence": "weekly",
        "compare": {"weeks": 4},
        "tables": [{"table": {"name": "orders", "version": 1}, "cadence": "daily"}],
    }
    assert factory_spec.answer_problems(spec, good) == []
    assert factory_spec.slug_of(spec, good) == "q3-review"
    bad = {
        "name": "Q3",
        "cadence": "monthly",
        "compare": {"weeks": 0},
        "tables": [{"cadence": "hourly"}],
    }
    assert factory_spec.answer_problems(spec, bad) == [
        "Name must be lowercase letters, digits and hyphens, starting with a letter.",
        "What should it answer? is required.",
        "Cadence must be one of daily, weekly.",
        "Weeks is out of range.",
        "Tables 1: Table is required.",
        "Tables 1: Refresh must be one of daily, weekly.",
    ]


@pytest.mark.parametrize("name", factory_spec.BUILTINS)
def test_the_shipped_built_ins_are_valid(name):
    resources = (
        Path(__file__).resolve().parents[4]
        / "hopsworks-ee/hopsworks-common/src/main/resources/factories"
    )
    if not resources.is_dir():
        pytest.skip("hopsworks-ee is not beside this checkout")
    assert factory_spec.text_problems((resources / f"{name}.yaml").read_text()) == []


# endregion

# region Managing factories


def test_list_shows_built_in_and_project_factories(monkeypatch, logged_in):
    monkeypatch.setattr(
        factory_api,
        "_list",
        lambda: [
            {
                "name": "mlsystem",
                "title": "ML system",
                "builtin": True,
                "currentVersion": 1,
                "systems": 3,
                "enabled": True,
            },
            {
                "name": "churn-review",
                "title": "Churn review",
                "builtin": False,
                "currentVersion": 2,
                "systems": 0,
                "enabled": False,
            },
        ],
    )
    done = CliRunner().invoke(cli, ["factory", "list"])
    assert done.exit_code == 0, done.output
    rows = [line.split() for line in done.output.splitlines()]
    assert ["mlsystem", "ML", "system", "built-in", "1", "3", "yes"] in rows
    assert ["churn-review", "Churn", "review", "project", "2", "0", "no"] in rows


def test_factory_help_lists_run_and_the_system_commands():
    listed = CliRunner().invoke(cli, ["factory", "--help"])
    for command in ("list", "import", "export", "clone", "delete", "run", "system"):
        assert f"  {command} " in listed.output
    assert "  create " not in listed.output
    for name in ("mlsystem", "medallion", "create"):
        assert factory.factory_group.get_command(None, name) is None
    for name in ("mlsystem", "medallion", "build"):
        assert CliRunner().invoke(cli, [name, "--help"]).exit_code != 0


def test_validate_runs_without_a_cluster(tmp_path):
    good = tmp_path / "good.yaml"
    good.write_text(REVIEW)
    bad = tmp_path / "bad.yaml"
    bad.write_text(REVIEW.replace("kind: Factory", "kind: Job"))
    assert CliRunner().invoke(cli, ["factory", "validate", str(good)]).exit_code == 0
    refused = CliRunner().invoke(cli, ["factory", "validate", str(bad)])
    assert refused.exit_code != 0 and "kind must be Factory" in refused.output


def test_import_reviews_the_instructions_then_creates(tmp_path, monkeypatch, logged_in):
    path = tmp_path / "review.yaml"
    path.write_text(REVIEW)
    created = []
    monkeypatch.setattr(
        factory_api,
        "_create",
        lambda text, name=None: (
            created.append((text, name))
            or {"name": name or "churn-review", "currentVersion": 1}
        ),
    )
    declined = CliRunner().invoke(cli, ["factory", "import", str(path)], input="n\n")
    assert "Claude Code will follow these instructions" in declined.output
    assert "Build a {dashboard}" in declined.output
    assert created == []
    done = CliRunner().invoke(
        cli, ["factory", "import", str(path), "--name", "q-review"], input="y\n"
    )
    assert done.exit_code == 0, done.output
    assert created == [(REVIEW, "q-review")]


def test_clone_renames_and_retitles_the_source(monkeypatch, logged_in):
    monkeypatch.setattr(factory_api, "_get", lambda name, version=None: _definition())
    created = []
    monkeypatch.setattr(
        factory_api,
        "_create",
        lambda text, name=None: created.append((text, name)) or {"name": name},
    )
    done = CliRunner().invoke(
        cli, ["factory", "clone", "churn-review", "churn-review-2"]
    )
    assert done.exit_code == 0, done.output
    text, name = created[0]
    assert name == "churn-review-2"
    assert 'title: "Churn review (copy)"' in text
    assert text.replace('title: "Churn review (copy)"', "title: Churn review") == REVIEW


def test_delete_reports_what_the_cluster_refuses(monkeypatch, logged_in):
    from hopsworks_common.client.exceptions import RestAPIError

    response = mock.Mock(status_code=409, content=b"x")
    response.json.return_value = {
        "errorCode": 150108,
        "usrMsg": "Delete these systems first: q3-review",
    }

    def refuse(name):
        raise RestAPIError("url", response)

    monkeypatch.setattr(factory_api, "_delete", refuse)
    done = CliRunner().invoke(cli, ["factory", "delete", "churn-review", "--yes"])
    assert done.exit_code != 0
    assert "q3-review" in done.output


# endregion

# region Running a factory


def _recording(monkeypatch, tmp_path):
    """Run factories in tmp_path, recording registrations and launches."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(factory_api, "_get", lambda name, version=None: _definition())
    registered, launched = [], []
    monkeypatch.setattr(
        mlsystem,
        "register",
        lambda ctx, target, name=None, factory=None: (
            registered.append((target.name, name, factory)) or {}
        ),
    )
    monkeypatch.setattr(
        medallion,
        "_launch",
        lambda target, launch, request=None: launched.append(request),
    )
    return registered, launched


def test_run_asks_each_question_then_resumes_from_system_yaml(
    tmp_path, monkeypatch, logged_in
):
    registered, launched = _recording(monkeypatch, tmp_path)
    typed = [
        "",  # the name is required, so it is asked again
        "q3-review",
        "why",
        "",  # cadence: the default, weekly
        "4",
        "y",  # add a table
        "orders:2",
        "daily",
        "n",
    ]
    done = CliRunner().invoke(
        cli, ["factory", "run", "churn-review"], input="\n".join(typed) + "\n"
    )
    assert done.exit_code == 0, done.output
    assert "Name is required" in done.output
    doc = yaml.safe_load((tmp_path / "q3-review" / "system.yaml").read_text())
    assert doc["requirements"] == {
        "name": "q3-review",
        "question": "why",
        "cadence": "weekly",
        "compare": {"weeks": 4},
        "tables": [{"table": {"name": "orders", "version": 2}, "cadence": "daily"}],
    }
    # A recorded system is resumed from its system.yaml: no answers, nothing asked.
    again = CliRunner().invoke(cli, ["factory", "run", "churn-review", "q3-review"])
    assert again.exit_code == 0, again.output
    assert "Resuming q3-review" in again.output
    assert launched == ["/hops-factory-churn-review q3-review"] * 2
    assert registered == [("q3-review", "q3-review", "churn-review")]
    other = REVIEW.replace("name: churn-review", "name: other-review")
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(other)
    )
    refused = CliRunner().invoke(cli, ["factory", "run", "other-review", "q3-review"])
    assert refused.exit_code != 0
    assert "hops factory run churn-review q3-review" in refused.output
    unknown = CliRunner().invoke(cli, ["factory", "run", "churn-review", "nope"])
    assert unknown.exit_code != 0 and "no system 'nope'" in unknown.output
    preset = CliRunner().invoke(
        cli, ["factory", "run", "churn-review", "--preset", "q4"]
    )
    assert preset.exit_code != 0 and "this factory has none" in preset.output


def test_create_writes_the_system_and_its_build_command(
    tmp_path, monkeypatch, logged_in
):
    registered, launched = _recording(monkeypatch, tmp_path)
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "name": "q3-review",
                "question": "why",
                "cadence": "daily",
                "compare": {"weeks": 9},
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "churn-review", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "q3-review" / "system.yaml").read_text())
    assert doc["factory"]["name"] == "churn-review"
    assert doc["factory"]["version"] == 2
    assert [p["key"] for p in doc["factory"]["phases"]] == ["build", "verify"]
    assert doc["requirements"] == {
        "name": "q3-review",
        "question": "why",
        "cadence": "daily",
        "compare": {"weeks": 9},
    }
    assert doc["build"] == {"status": "pending"} and doc["verify"] == {
        "status": "pending"
    }
    command = (
        tmp_path / "q3-review" / ".claude" / "commands" / "hops-factory-churn-review.md"
    ).read_text()
    assert "Build a {dashboard} over the chosen tables." in command
    assert "**hops-superset**" in command and "`build` (Build)" in command
    assert registered == [("q3-review", "q3-review", "churn-review")]
    assert launched == ["/hops-factory-churn-review q3-review"]


def test_create_refuses_answers_the_form_would_refuse(tmp_path, monkeypatch, logged_in):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(factory_api, "_get", lambda name, version=None: _definition())
    answers = tmp_path / "answers.json"
    answers.write_text(json.dumps({"name": "Bad Name"}))
    done = CliRunner().invoke(
        cli, ["factory", "run", "churn-review", "--answers", str(answers)]
    )
    assert done.exit_code != 0 and "Name must be lowercase" in done.output
    assert not (tmp_path / "Bad Name").exists()


CHANGED = (
    REVIEW
    + """\
changes:
  - id: drop-week
    label: Drop a week
    instructions: Remove the week from the review and rebuild the dashboard.
    form:
      sections:
        - id: pick
          title: Week
          fields:
            - {id: week, type: entry, label: Week, from: outputs.weeks, value: name, fill: true}
            - {id: note, type: text, label: Note}
"""
)


def test_a_change_is_recorded_as_a_pending_request_then_the_build_resumes(
    tmp_path, monkeypatch, logged_in
):
    registered, launched = _recording(monkeypatch, tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(CHANGED)
    )
    system = tmp_path / "q3-review"
    system.mkdir()
    (system / "system.yaml").write_text(
        yaml.safe_dump(
            {
                "factory": {"name": "churn-review", "version": 2},
                "outputs": {"weeks": [{"name": "w1", "note": "old"}, {"name": "w2"}]},
            }
        )
    )
    run = ["factory", "run", "churn-review", "q3-review", "--change"]
    # The entry's own values are the defaults of the questions after it.
    done = CliRunner().invoke(cli, [*run, "drop-week"], input="w1\n\n")
    assert done.exit_code == 0, done.output
    [request] = yaml.safe_load((system / "system.yaml").read_text())["changes"]
    assert request.pop("at")
    assert request == {
        "id": "drop-week",
        "label": "Drop a week",
        "answers": {"week": "w1", "note": "old"},
        "instructions": "Remove the week from the review and rebuild the dashboard.",
        "status": "pending",
    }
    assert launched == ["/hops-factory-churn-review q3-review"]

    answers = tmp_path / "answers.json"
    answers.write_text(json.dumps({"week": "w9"}))
    wrong = CliRunner().invoke(cli, [*run, "drop-week", "--answers", str(answers)])
    assert wrong.exit_code != 0 and "w9 is not in system.yaml" in wrong.output
    unknown = CliRunner().invoke(cli, [*run, "rename"])
    assert unknown.exit_code != 0 and "this factory has drop-week" in unknown.output
    nowhere = CliRunner().invoke(
        cli, ["factory", "run", "churn-review", "--change", "drop-week"]
    )
    assert nowhere.exit_code != 0 and "a recorded system" in nowhere.output
    assert len(yaml.safe_load((system / "system.yaml").read_text())["changes"]) == 1


def test_a_change_form_is_checked_as_a_change():
    assert factory_spec.text_problems(CHANGED) == []
    found = factory_spec.text_problems(
        CHANGED.replace(
            "    instructions: Remove the week from the review and rebuild the dashboard.\n",
            "",
        )
    )
    assert any("instructions are required" in p for p in found)
    # An entry picks from a system that exists, so a create form cannot have one.
    found = factory_spec.text_problems(
        REVIEW.replace(
            "{id: question, type: textarea,",
            "{id: week, type: entry, label: Week, from: outputs.weeks}\n        - {id: question, type: textarea,",
        )
    )
    assert any("cannot be an entry" in p for p in found)
    assert factory_spec.items_at(
        {"marts": [{"jobs": [{"name": "a"}]}, {"jobs": [{"name": "b"}]}]}, "marts.jobs"
    ) == [{"name": "a"}, {"name": "b"}]


CLONE = """\
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: fraud-ml
title: Fraud ML system
form:
  sections:
    - id: system
      title: System
      fields:
        - {id: slug, type: slug, label: Name, required: true}
        - {id: description, type: textarea, label: "What should it predict?", required: true}
        - {id: regulator, type: text, label: Regulator to report to}
phases:
  - {key: requirements, label: Requirements}
build:
  builtin: mlsystem
  answers: {system_type: batch, repo: new}
  instructions: Write a model card for the regulator.
"""


def test_a_clone_of_a_built_in_builds_with_it_and_records_itself(
    tmp_path, monkeypatch, logged_in
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(CLONE, version=3)
    )
    seen = {}

    def built_in(ctx, answers, launch):
        seen["answers"] = answers
        seen["no_launch"] = not launch
        target = tmp_path / "fraud"
        target.mkdir()
        (target / "system.yaml").write_text(
            yaml.safe_dump({"requirements": {"status": "pending"}})
        )
        factory_spec.record_factory(ctx.meta.get(factory_spec.META), target)
        seen["factory"] = factory_spec.factory_name(ctx, "ml-batch")

    monkeypatch.setattr(factory.build, "create", built_in)
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "slug": "fraud",
                "description": "which payments are fraud",
                "regulator": "FI",
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "fraud-ml", "--answers", str(answers), "--no-launch"]
    )
    assert done.exit_code == 0, done.output
    assert seen["answers"] == {
        "slug": "fraud",
        "description": "which payments are fraud",
        "system_type": "batch",
        "repo": "new",
    }
    assert seen["no_launch"] is True
    assert seen["factory"] == "fraud-ml"
    doc = yaml.safe_load((tmp_path / "fraud" / "system.yaml").read_text())
    assert doc["factory"] == {
        "name": "fraud-ml",
        "version": 3,
        "instructions": "Write a model card for the regulator.",
    }
    assert doc["requirements"] == {"status": "pending", "extra": {"regulator": "FI"}}


# endregion
