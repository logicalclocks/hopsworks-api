"""`hops factory`: the built-in factories, the project's own, and what a factory definition may say."""

from __future__ import annotations

import json
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
        - {id: weeks, type: number, label: Weeks, min: 1, when: {field: cadence, equals: weekly}}
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
        (("field: cadence", "field: later"), "when must name an earlier field"),
        (("  instructions: Build", "  notes: Build"), "build.instructions is required"),
        (
            ("type: number", "type: component, component: shell"),
            "component must be one of",
        ),
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
    good = {"name": "q3-review", "question": "why", "cadence": "weekly", "weeks": 4}
    assert factory_spec.answer_problems(spec, good) == []
    assert factory_spec.slug_of(spec, good) == "q3-review"
    bad = {"name": "Q3", "cadence": "monthly", "weeks": 0}
    assert factory_spec.answer_problems(spec, bad) == [
        "Name must be lowercase letters, digits and hyphens, starting with a letter.",
        "What should it answer? is required.",
        "Cadence must be one of daily, weekly.",
    ]
    # A field shown only for weekly is not checked for daily.
    daily = {**good, "cadence": "daily", "weeks": 0}
    assert factory_spec.answer_problems(spec, daily) == []


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


def test_factory_help_lists_management_commands_and_no_create():
    listed = CliRunner().invoke(cli, ["factory", "--help"])
    for command in (
        "list",
        "import",
        "export",
        "clone",
        "delete",
        "mlsystem",
        "medallion",
    ):
        assert f"  {command} " in listed.output
    assert "  create " not in listed.output
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

# region A project factory's systems


def test_a_project_factory_resolves_to_its_own_commands(monkeypatch, logged_in):
    monkeypatch.setattr(factory_api, "_get", lambda name, version=None: _definition())
    listed = CliRunner().invoke(cli, ["factory", "churn-review", "--help"])
    assert listed.exit_code == 0, listed.output
    for command in ("create", "list", "status", "register", "remove", "delete"):
        assert f"  {command} " in listed.output

    def missing(name, version=None):
        raise RuntimeError("404")

    monkeypatch.setattr(factory_api, "_get", missing)
    assert CliRunner().invoke(cli, ["factory", "nope", "--help"]).exit_code != 0


def test_create_writes_the_system_and_its_build_command(
    tmp_path, monkeypatch, logged_in
):
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
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {"name": "q3-review", "question": "why", "cadence": "daily", "weeks": 9}
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "churn-review", "create", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "q3-review" / "system.yaml").read_text())
    assert doc["factory"]["name"] == "churn-review"
    assert doc["factory"]["version"] == 2
    assert [p["key"] for p in doc["factory"]["phases"]] == ["build", "verify"]
    # `weeks` is hidden for a daily cadence, so it is not a requirement.
    assert doc["requirements"] == {
        "name": "q3-review",
        "question": "why",
        "cadence": "daily",
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
        cli, ["factory", "churn-review", "create", "--answers", str(answers)]
    )
    assert done.exit_code != 0 and "Name must be lowercase" in done.output
    assert not (tmp_path / "Bad Name").exists()


CLONE = """\
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: fraud-ml
title: Fraud ML system
form:
  sections:
    - id: requirements
      title: Requirements
      fields:
        - {id: requirements, type: component, component: mlsystem.requirements, label: Requirements}
        - {id: regulator, type: text, label: Regulator to report to}
phases:
  - {key: requirements, label: Requirements}
build:
  builtin: mlsystem
  instructions: Write a model card for the regulator.
"""


def test_a_clone_of_mlsystem_builds_with_the_built_in_and_records_itself(
    tmp_path, monkeypatch, logged_in
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(CLONE, version=3)
    )
    seen = {}

    def built_in(ctx, slug, no_launch, example, answers):
        seen["answers"] = json.loads(answers.read_text())
        seen["no_launch"] = no_launch
        target = tmp_path / "fraud"
        target.mkdir()
        (target / "system.yaml").write_text(
            yaml.safe_dump({"requirements": {"status": "pending"}})
        )
        factory_spec.record_factory(ctx.meta.get(factory_spec.META), target)
        seen["factory"] = factory_spec.factory_name(ctx, "mlsystem")

    @factory.click.command()
    @factory.click.pass_context
    def stand_in(ctx, slug, no_launch, example, answers):
        built_in(ctx, slug, no_launch, example, answers)

    monkeypatch.setattr(factory.build, "create_cmd", stand_in)
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "requirements": {"slug": "fraud", "system_type": "batch"},
                "regulator": "FI",
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "fraud-ml", "create", "--answers", str(answers), "--no-launch"]
    )
    assert done.exit_code == 0, done.output
    assert seen["answers"] == {"slug": "fraud", "system_type": "batch"}
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
